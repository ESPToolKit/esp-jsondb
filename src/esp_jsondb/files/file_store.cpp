#include "file_store.h"

#include "file_store_impl.h"

#include "../db_runtime.h"
#include "../utils/fr_mutex.h"
#include "../utils/fs_utils.h"
#include "../utils/jsondb_allocator.h"

#include <StreamUtils.h>

#include <algorithm>

extern FrMutex g_fsMutex;

namespace {

std::string parentDirOf(const std::string &path) {
	auto pos = path.find_last_of('/');
	if (pos == std::string::npos || pos == 0) {
		return "/";
	}
	return path.substr(0, pos);
}

std::string fileNameOf(const std::string &path) {
	auto pos = path.find_last_of('/');
	if (pos == std::string::npos)
		return path;
	return path.substr(pos + 1);
}

struct FileEntryInfo {
	std::string path;
	bool isDirectory = false;
	size_t size = 0;
};

DbStatus statFileEntry(
    fs::FS &filesystem,
    const std::string &absolutePath,
    const std::string &relativePath,
    FileEntryInfo &out
) {
	FrLock fs(g_fsMutex);
	if (!filesystem.exists(absolutePath.c_str())) {
		return {DbStatusCode::NotFound, "file not found"};
	}
	File file = filesystem.open(absolutePath.c_str(), FILE_READ);
	if (!file) {
		return {DbStatusCode::IoError, "open file info failed"};
	}
	out.path = relativePath;
	out.isDirectory = file.isDirectory();
	out.size = out.isDirectory ? 0 : file.size();
	file.close();
	return {DbStatusCode::Ok, ""};
}

void appendFileInfoJson(JsonArray entries, const FileEntryInfo &info) {
	JsonObject entry = entries.add<JsonObject>();
	entry["path"] = info.path.c_str();
	entry["name"] = fileNameOf(info.path).c_str();
	entry["exists"] = true;
	entry["isDirectory"] = info.isDirectory;
	entry["size"] = info.size;
}

DbStatus collectDirectoryEntries(
    fs::FS &filesystem,
    const std::string &absoluteDir,
    const std::string &relativeDir,
    bool recursive,
    std::vector<FileEntryInfo> &entries
) {
	std::vector<std::pair<std::string, std::string>> pendingDirs;
	{
		FrLock fs(g_fsMutex);
		File dir = filesystem.open(absoluteDir.c_str(), FILE_READ);
		if (!dir || !dir.isDirectory()) {
			if (dir)
				dir.close();
			return {DbStatusCode::NotFound, "file not found"};
		}
		for (File child = dir.openNextFile(); child; child = dir.openNextFile()) {
			const bool isDirectory = child.isDirectory();
			String rawName = child.name();
			child.close();
			std::string segment = rawName.c_str();
			auto slash = segment.find_last_of('/');
			if (slash != std::string::npos)
				segment = segment.substr(slash + 1);
			if (segment.empty())
				continue;

			FileEntryInfo info;
			info.path = relativeDir.empty() ? segment : joinPath(relativeDir, segment);
			info.isDirectory = isDirectory;
			if (!isDirectory) {
				const std::string childPath = joinPath(absoluteDir, segment);
				File childFile = filesystem.open(childPath.c_str(), FILE_READ);
				if (!childFile) {
					dir.close();
					return {DbStatusCode::IoError, "open child file info failed"};
				}
				info.size = childFile.size();
				childFile.close();
			}
			entries.push_back(info);

			if (recursive && isDirectory) {
				pendingDirs.emplace_back(joinPath(absoluteDir, segment), info.path);
			}
		}
		dir.close();
	}

	for (const auto &pending : pendingDirs) {
		auto st = collectDirectoryEntries(filesystem, pending.first, pending.second, true, entries);
		if (!st.ok())
			return st;
	}

	return {DbStatusCode::Ok, ""};
}

DbStatus writeFromPullCb(
    fs::FS &filesystem,
    const std::string &finalPath,
    bool usePSRAMBuffers,
    const ESPJsonDBFileOptions &opts,
    const DbFileUploadPullCb &pullCb,
    size_t &totalWritten
) {
	totalWritten = 0;
	if (!pullCb) {
		return {DbStatusCode::InvalidArgument, "upload callback is required"};
	}

	const size_t chunkSize = opts.chunkSize < 32 ? 32 : opts.chunkSize;
	JsonDbVector<uint8_t> buffer{JsonDbAllocator<uint8_t>(usePSRAMBuffers)};
	buffer.resize(chunkSize);

	const std::string parentDir = parentDirOf(finalPath);
	const std::string tmpPath = finalPath + ".tmp";

	FrLock fs(g_fsMutex);
	if (!fsEnsureDir(filesystem, parentDir)) {
		return {DbStatusCode::IoError, "mkdir file parent failed"};
	}
	if (!opts.overwrite && filesystem.exists(finalPath.c_str())) {
		return {DbStatusCode::AlreadyExists, "file already exists"};
	}
	if (filesystem.exists(tmpPath.c_str())) {
		filesystem.remove(tmpPath.c_str());
	}

	File file = filesystem.open(tmpPath.c_str(), FILE_WRITE);
	if (!file) {
		return {DbStatusCode::IoError, "open file for write failed"};
	}
	WriteBufferingStream buffered(file, chunkSize);

	auto fail = [&](DbStatus st) {
		buffered.flush();
		file.close();
		filesystem.remove(tmpPath.c_str());
		return st;
	};

	for (;;) {
		size_t produced = 0;
		bool eof = false;
		auto st = pullCb(chunkSize, buffer.data(), produced, eof);
		if (!st.ok())
			return fail(st);
		if (produced > chunkSize) {
			return fail({DbStatusCode::InvalidArgument, "upload callback produced too many bytes"});
		}
		if (produced > 0) {
			size_t written = buffered.write(buffer.data(), produced);
			if (written != produced) {
				return fail({DbStatusCode::IoError, "file write failed"});
			}
			totalWritten += written;
		}
		if (eof)
			break;
		if (produced == 0) {
			return fail(
			    {DbStatusCode::InvalidArgument, "upload callback produced no bytes without eof"}
			);
		}
	}

	buffered.flush();
	file.close();

	if (opts.overwrite && filesystem.exists(finalPath.c_str()) &&
	    !filesystem.remove(finalPath.c_str())) {
		filesystem.remove(tmpPath.c_str());
		return {DbStatusCode::IoError, "remove old file failed"};
	}
	if (!filesystem.rename(tmpPath.c_str(), finalPath.c_str())) {
		filesystem.remove(tmpPath.c_str());
		return {DbStatusCode::IoError, "rename file failed"};
	}

	return {DbStatusCode::Ok, ""};
}

} // namespace

FileStoreImpl::FileStoreImpl(DbRuntime &rt)
    : _rt(&rt), uploadQueue(JsonDbAllocator<uint32_t>(rt.cfg.usePSRAMBuffers)),
      uploadJobs(std::less<uint32_t>{}, UploadJobMap::allocator_type(rt.cfg.usePSRAMBuffers)),
      terminalUploadOrder(JsonDbAllocator<uint32_t>(rt.cfg.usePSRAMBuffers)) {
}

DbStatus
FileStoreImpl::normalizePath(const std::string &rawRelativePath, std::string &normalized) const {
	normalized.clear();
	if (rawRelativePath.empty()) {
		return {DbStatusCode::InvalidArgument, "file path is empty"};
	}

	std::string segment;
	segment.reserve(rawRelativePath.size());

	auto flushSegment = [&]() -> DbStatus {
		if (segment.empty() || segment == ".") {
			segment.clear();
			return {DbStatusCode::Ok, ""};
		}
		if (segment == "..") {
			segment.clear();
			return {DbStatusCode::InvalidArgument, "path traversal is not allowed"};
		}
		if (segment.find(':') != std::string::npos) {
			segment.clear();
			return {DbStatusCode::InvalidArgument, "invalid file path segment"};
		}
		if (!normalized.empty()) {
			normalized.push_back('/');
		}
		normalized += segment;
		segment.clear();
		return {DbStatusCode::Ok, ""};
	};

	for (char c : rawRelativePath) {
		if (c == '\\')
			c = '/';
		if (c == '/') {
			auto st = flushSegment();
			if (!st.ok())
				return st;
			continue;
		}
		segment.push_back(c);
	}
	auto st = flushSegment();
	if (!st.ok())
		return st;

	if (normalized.empty()) {
		return {DbStatusCode::InvalidArgument, "file path resolves to empty"};
	}

	return {DbStatusCode::Ok, ""};
}

DbStatus FileStoreImpl::writeFileStream(
    const std::string &relativePath,
    Stream &in,
    size_t bytesToWrite,
    const ESPJsonDBFileOptions &opts
) {
	auto ready = _rt->ensureReady();
	if (!ready.ok())
		return _rt->recordStatus(ready);

	std::string normalized;
	auto nst = normalizePath(relativePath, normalized);
	if (!nst.ok())
		return _rt->recordStatus(nst);

	const std::string finalPath = joinPath(_rt->fileRootDir(), normalized);
	size_t remaining = bytesToWrite;
	DbFileUploadPullCb pullCb =
	    [&in,
	     &remaining](size_t requested, uint8_t *buffer, size_t &produced, bool &eof) -> DbStatus {
		if (!buffer) {
			return {DbStatusCode::InvalidArgument, "buffer is null"};
		}
		if (remaining == 0) {
			produced = 0;
			eof = true;
			return {DbStatusCode::Ok, ""};
		}
		size_t want = std::min(requested, remaining);
		produced = in.readBytes(reinterpret_cast<char *>(buffer), want);
		if (produced == 0) {
			eof = false;
			return {DbStatusCode::IoError, "stream ended before expected size"};
		}
		remaining -= produced;
		eof = (remaining == 0);
		return {DbStatusCode::Ok, ""};
	};

	size_t totalWritten = 0;
	auto st =
	    writeFromPullCb(*_rt->fs, finalPath, _rt->cfg.usePSRAMBuffers, opts, pullCb, totalWritten);
	if (!st.ok())
		return _rt->recordStatus(st);
	if (totalWritten != bytesToWrite) {
		return _rt->recordStatus({DbStatusCode::IoError, "written size mismatch"});
	}

	return _rt->recordStatus({DbStatusCode::Ok, ""});
}

DbStatus FileStoreImpl::writeFileStream(
    const std::string &relativePath,
    const DbFileUploadPullCb &pullCb,
    const ESPJsonDBFileOptions &opts
) {
	auto ready = _rt->ensureReady();
	if (!ready.ok())
		return _rt->recordStatus(ready);

	std::string normalized;
	auto nst = normalizePath(relativePath, normalized);
	if (!nst.ok())
		return _rt->recordStatus(nst);

	const std::string finalPath = joinPath(_rt->fileRootDir(), normalized);
	size_t totalWritten = 0;
	auto st =
	    writeFromPullCb(*_rt->fs, finalPath, _rt->cfg.usePSRAMBuffers, opts, pullCb, totalWritten);
	return _rt->recordStatus(st);
}

DbStatus FileStoreImpl::writeFileFromPath(
    const std::string &relativePath,
    const std::string &sourceFsPath,
    const ESPJsonDBFileOptions &opts
) {
	if (sourceFsPath.empty()) {
		return _rt->recordStatus({DbStatusCode::InvalidArgument, "source file path is empty"});
	}
	auto ready = _rt->ensureReady();
	if (!ready.ok())
		return _rt->recordStatus(ready);

	File source;
	{
		FrLock fs(g_fsMutex);
		source = _rt->fs->open(sourceFsPath.c_str(), FILE_READ);
	}
	if (!source) {
		return _rt->recordStatus({DbStatusCode::NotFound, "source file not found"});
	}

	const size_t bytesToWrite = source.size();
	auto st = writeFileStream(relativePath, source, bytesToWrite, opts);
	source.close();
	return st;
}

DbStatus FileStoreImpl::writeFile(
    const std::string &relativePath, const uint8_t *data, size_t size, bool overwrite
) {
	if (size > 0 && data == nullptr) {
		return _rt->recordStatus({DbStatusCode::InvalidArgument, "file data is null"});
	}

	auto ready = _rt->ensureReady();
	if (!ready.ok())
		return _rt->recordStatus(ready);

	std::string normalized;
	auto nst = normalizePath(relativePath, normalized);
	if (!nst.ok())
		return _rt->recordStatus(nst);

	const std::string finalPath = joinPath(_rt->fileRootDir(), normalized);
	const std::string parentDir = parentDirOf(finalPath);
	const std::string tmpPath = finalPath + ".tmp";

	FrLock fs(g_fsMutex);
	if (!fsEnsureDir(*_rt->fs, parentDir)) {
		return _rt->recordStatus({DbStatusCode::IoError, "mkdir file parent failed"});
	}
	if (!overwrite && _rt->fs->exists(finalPath.c_str())) {
		return _rt->recordStatus({DbStatusCode::AlreadyExists, "file already exists"});
	}
	if (_rt->fs->exists(tmpPath.c_str())) {
		_rt->fs->remove(tmpPath.c_str());
	}

	File f = _rt->fs->open(tmpPath.c_str(), FILE_WRITE);
	if (!f) {
		return _rt->recordStatus({DbStatusCode::IoError, "open file for write failed"});
	}
	WriteBufferingStream buffered(f, 256);

	size_t written = 0;
	if (size > 0) {
		written = buffered.write(data, size);
	}
	buffered.flush();
	f.close();
	if (written != size) {
		_rt->fs->remove(tmpPath.c_str());
		return _rt->recordStatus({DbStatusCode::IoError, "file write failed"});
	}

	if (overwrite && _rt->fs->exists(finalPath.c_str()) && !_rt->fs->remove(finalPath.c_str())) {
		_rt->fs->remove(tmpPath.c_str());
		return _rt->recordStatus({DbStatusCode::IoError, "remove old file failed"});
	}
	if (!_rt->fs->rename(tmpPath.c_str(), finalPath.c_str())) {
		_rt->fs->remove(tmpPath.c_str());
		return _rt->recordStatus({DbStatusCode::IoError, "rename file failed"});
	}

	return _rt->recordStatus({DbStatusCode::Ok, ""});
}

DbStatus FileStoreImpl::writeTextFile(
    const std::string &relativePath, const std::string &text, bool overwrite
) {
	return writeFile(
	    relativePath,
	    reinterpret_cast<const uint8_t *>(text.data()),
	    text.size(),
	    overwrite
	);
}

DbResult<size_t>
FileStoreImpl::readFileStream(const std::string &relativePath, Stream &out, size_t chunkSize) {
	DbResult<size_t> res{};
	auto ready = _rt->ensureReady();
	if (!ready.ok()) {
		res.status = _rt->recordStatus(ready);
		return res;
	}

	std::string normalized;
	auto nst = normalizePath(relativePath, normalized);
	if (!nst.ok()) {
		res.status = _rt->recordStatus(nst);
		return res;
	}

	if (chunkSize < 32)
		chunkSize = 32;
	JsonDbVector<uint8_t> buffer{JsonDbAllocator<uint8_t>(_rt->cfg.usePSRAMBuffers)};
	buffer.resize(chunkSize);
	const std::string path = joinPath(_rt->fileRootDir(), normalized);

	FrLock fs(g_fsMutex);
	File f = _rt->fs->open(path.c_str(), FILE_READ);
	if (!f) {
		res.status = _rt->recordStatus({DbStatusCode::NotFound, "file not found"});
		return res;
	}

	size_t total = 0;
	while (true) {
		size_t readBytes = f.read(buffer.data(), buffer.size());
		if (readBytes == 0)
			break;
		size_t written = out.write(buffer.data(), readBytes);
		if (written != readBytes) {
			f.close();
			res.status = _rt->recordStatus({DbStatusCode::IoError, "output stream write failed"});
			return res;
		}
		total += written;
	}
	f.close();

	res.status = _rt->recordStatus({DbStatusCode::Ok, ""});
	res.value = total;
	return res;
}

DbResult<std::vector<uint8_t>> FileStoreImpl::readFile(const std::string &relativePath) {
	DbResult<std::vector<uint8_t>> res{};
	auto ready = _rt->ensureReady();
	if (!ready.ok()) {
		res.status = _rt->recordStatus(ready);
		return res;
	}

	std::string normalized;
	auto nst = normalizePath(relativePath, normalized);
	if (!nst.ok()) {
		res.status = _rt->recordStatus(nst);
		return res;
	}

	const std::string path = joinPath(_rt->fileRootDir(), normalized);

	FrLock fs(g_fsMutex);
	File f = _rt->fs->open(path.c_str(), FILE_READ);
	if (!f) {
		res.status = _rt->recordStatus({DbStatusCode::NotFound, "file not found"});
		return res;
	}

	size_t sz = f.size();
	res.value.resize(sz);
	size_t readBytes = 0;
	if (sz > 0) {
		readBytes = f.read(res.value.data(), sz);
	}
	f.close();
	if (readBytes != sz) {
		res.value.clear();
		res.status = _rt->recordStatus({DbStatusCode::IoError, "file read failed"});
		return res;
	}

	res.status = _rt->recordStatus({DbStatusCode::Ok, ""});
	return res;
}

DbResult<std::string> FileStoreImpl::readTextFile(const std::string &relativePath) {
	DbResult<std::string> res{};
	auto fr = readFile(relativePath);
	if (!fr.status.ok()) {
		res.status = fr.status;
		return res;
	}
	res.value.assign(fr.value.begin(), fr.value.end());
	res.status = _rt->recordStatus({DbStatusCode::Ok, ""});
	return res;
}

DbResult<JsonDocument> FileStoreImpl::getFileInfo(const std::string &relativePath) {
	DbResult<JsonDocument> res;
	auto ready = _rt->ensureReady();
	if (!ready.ok()) {
		res.status = _rt->recordStatus(ready);
		return res;
	}

	std::string normalized;
	auto nst = normalizePath(relativePath, normalized);
	if (!nst.ok()) {
		res.status = _rt->recordStatus(nst);
		return res;
	}

	FileEntryInfo info;
	const auto st =
	    statFileEntry(*_rt->fs, joinPath(_rt->fileRootDir(), normalized), normalized, info);
	if (!st.ok()) {
		res.status = _rt->recordStatus(st);
		return res;
	}

	res.value["path"] = info.path.c_str();
	res.value["name"] = fileNameOf(info.path).c_str();
	res.value["exists"] = true;
	res.value["isDirectory"] = info.isDirectory;
	res.value["size"] = info.size;
	res.status = _rt->recordStatus({DbStatusCode::Ok, ""});
	return res;
}

DbResult<JsonDocument> FileStoreImpl::listFiles(const std::string &relativePrefix, bool recursive) {
	DbResult<JsonDocument> res;
	auto ready = _rt->ensureReady();
	if (!ready.ok()) {
		res.status = _rt->recordStatus(ready);
		return res;
	}

	std::string normalizedPrefix;
	if (!relativePrefix.empty()) {
		auto nst = normalizePath(relativePrefix, normalizedPrefix);
		if (!nst.ok()) {
			res.status = _rt->recordStatus(nst);
			return res;
		}
	}

	const std::string rootPath = _rt->fileRootDir();
	const std::string targetPath =
	    normalizedPrefix.empty() ? rootPath : joinPath(rootPath, normalizedPrefix);

	FileEntryInfo targetInfo;
	auto st = statFileEntry(*_rt->fs, targetPath, normalizedPrefix, targetInfo);
	if (!st.ok()) {
		res.status = _rt->recordStatus(st);
		return res;
	}

	std::vector<FileEntryInfo> entries;
	if (targetInfo.isDirectory) {
		st = collectDirectoryEntries(*_rt->fs, targetPath, normalizedPrefix, recursive, entries);
		if (!st.ok()) {
			res.status = _rt->recordStatus(st);
			return res;
		}
	} else {
		entries.push_back(targetInfo);
	}

	std::sort(
	    entries.begin(),
	    entries.end(),
	    [](const FileEntryInfo &lhs, const FileEntryInfo &rhs) { return lhs.path < rhs.path; }
	);

	res.value["prefix"] = normalizedPrefix.c_str();
	res.value["recursive"] = recursive;
	JsonArray entriesJson = res.value["entries"].to<JsonArray>();
	for (const auto &entry : entries) {
		appendFileInfoJson(entriesJson, entry);
	}

	res.status = _rt->recordStatus({DbStatusCode::Ok, ""});
	return res;
}

DbStatus FileStoreImpl::removeFile(const std::string &relativePath) {
	auto ready = _rt->ensureReady();
	if (!ready.ok())
		return _rt->recordStatus(ready);

	std::string normalized;
	auto nst = normalizePath(relativePath, normalized);
	if (!nst.ok())
		return _rt->recordStatus(nst);

	const std::string path = joinPath(_rt->fileRootDir(), normalized);
	FrLock fs(g_fsMutex);
	if (!_rt->fs->exists(path.c_str())) {
		return _rt->recordStatus({DbStatusCode::NotFound, "file not found"});
	}
	if (!_rt->fs->remove(path.c_str())) {
		return _rt->recordStatus({DbStatusCode::IoError, "file remove failed"});
	}
	return _rt->recordStatus({DbStatusCode::Ok, ""});
}

DbResult<bool> FileStoreImpl::fileExists(const std::string &relativePath) {
	DbResult<bool> res{};
	auto ready = _rt->ensureReady();
	if (!ready.ok()) {
		res.status = _rt->recordStatus(ready);
		return res;
	}

	std::string normalized;
	auto nst = normalizePath(relativePath, normalized);
	if (!nst.ok()) {
		res.status = _rt->recordStatus(nst);
		return res;
	}

	const std::string path = joinPath(_rt->fileRootDir(), normalized);
	FrLock fs(g_fsMutex);
	res.value = _rt->fs->exists(path.c_str());
	res.status = _rt->recordStatus({DbStatusCode::Ok, ""});
	return res;
}

DbResult<size_t> FileStoreImpl::fileSize(const std::string &relativePath) {
	DbResult<size_t> res{};
	auto ready = _rt->ensureReady();
	if (!ready.ok()) {
		res.status = _rt->recordStatus(ready);
		return res;
	}

	std::string normalized;
	auto nst = normalizePath(relativePath, normalized);
	if (!nst.ok()) {
		res.status = _rt->recordStatus(nst);
		return res;
	}

	const std::string path = joinPath(_rt->fileRootDir(), normalized);
	FrLock fs(g_fsMutex);
	File f = _rt->fs->open(path.c_str(), FILE_READ);
	if (!f) {
		res.status = _rt->recordStatus({DbStatusCode::NotFound, "file not found"});
		return res;
	}
	res.value = f.size();
	f.close();
	res.status = _rt->recordStatus({DbStatusCode::Ok, ""});
	return res;
}

bool FileStoreImpl::isUploadTerminal(DbFileUploadState state) const {
	return state == DbFileUploadState::Completed || state == DbFileUploadState::Failed ||
	       state == DbFileUploadState::Cancelled;
}

void FileStoreImpl::trackTerminalUploadLocked(const std::shared_ptr<FileUploadJob> &job) {
	if (!job || !isUploadTerminal(job->state) || job->terminalTracked)
		return;

	job->terminalTracked = true;
	terminalUploadOrder.push_back(job->id);

	while (terminalUploadOrder.size() > kMaxRetainedTerminalUploads) {
		const uint32_t expiredId = terminalUploadOrder.front();
		terminalUploadOrder.pop_front();

		auto it = uploadJobs.find(expiredId);
		if (it == uploadJobs.end())
			continue;
		if (!it->second || isUploadTerminal(it->second->state)) {
			uploadJobs.erase(it);
		}
	}
}

DbResult<uint32_t> FileStoreImpl::writeFileStreamAsync(
    const std::string &relativePath,
    const DbFileUploadPullCb &pullCb,
    const ESPJsonDBFileOptions &opts,
    const DbFileUploadDoneCb &doneCb
) {
	DbResult<uint32_t> res{};
	auto ready = _rt->ensureReady();
	if (!ready.ok()) {
		res.status = _rt->recordStatus(ready);
		return res;
	}
	if (!pullCb) {
		res.status =
		    _rt->recordStatus({DbStatusCode::InvalidArgument, "upload callback is required"});
		return res;
	}

	std::string normalized;
	auto nst = normalizePath(relativePath, normalized);
	if (!nst.ok()) {
		res.status = _rt->recordStatus(nst);
		return res;
	}

	auto job = std::make_shared<FileUploadJob>();
	job->relativePath = relativePath;
	job->normalizedPath = normalized;
	job->opts = opts;
	if (job->opts.chunkSize < 32)
		job->opts.chunkSize = 32;
	job->pullCb = pullCb;
	job->doneCb = doneCb;

	{
		FrLock lk(_rt->mu);
		job->id = nextUploadId++;
		uploadJobs[job->id] = job;
		uploadQueue.push_back(job->id);
		startTaskUnlocked();
		if (taskHandle == nullptr) {
			uploadQueue.erase(
			    std::remove(uploadQueue.begin(), uploadQueue.end(), job->id),
			    uploadQueue.end()
			);
			uploadJobs.erase(job->id);
			res.status = _rt->recordStatus({DbStatusCode::Busy, "upload worker start failed"});
			return res;
		}
	}

	res.status = _rt->recordStatus({DbStatusCode::Ok, ""});
	res.value = job->id;
	return res;
}

DbStatus FileStoreImpl::cancelUpload(uint32_t uploadId) {
	DbFileUploadDoneCb doneCb;
	DbStatus doneStatus{DbStatusCode::Busy, "upload cancelled"};
	size_t bytesWritten = 0;
	bool triggerDone = false;

	{
		FrLock lk(_rt->mu);
		auto it = uploadJobs.find(uploadId);
		if (it == uploadJobs.end()) {
			return _rt->recordStatus({DbStatusCode::NotFound, "upload not found"});
		}
		auto &job = it->second;
		if (!job) {
			return _rt->recordStatus({DbStatusCode::NotFound, "upload not found"});
		}
		if (isUploadTerminal(job->state)) {
			return _rt->recordStatus({DbStatusCode::Ok, ""});
		}

		job->cancelRequested = true;
		if (job->state == DbFileUploadState::Queued) {
			uploadQueue.erase(
			    std::remove(uploadQueue.begin(), uploadQueue.end(), uploadId),
			    uploadQueue.end()
			);
			job->state = DbFileUploadState::Cancelled;
			job->finalStatus = doneStatus;
			trackTerminalUploadLocked(job);
			doneCb = job->doneCb;
			bytesWritten = job->bytesWritten;
			triggerDone = true;
		}
	}

	if (triggerDone && doneCb) {
		doneCb(uploadId, doneStatus, bytesWritten);
	}
	return _rt->recordStatus({DbStatusCode::Ok, ""});
}

DbResult<DbFileUploadState> FileStoreImpl::getUploadState(uint32_t uploadId) {
	DbResult<DbFileUploadState> res{};
	auto ready = _rt->ensureReady();
	if (!ready.ok()) {
		res.status = _rt->recordStatus(ready);
		return res;
	}

	{
		FrLock lk(_rt->mu);
		auto it = uploadJobs.find(uploadId);
		if (it == uploadJobs.end() || !it->second) {
			res.status = _rt->recordStatus({DbStatusCode::NotFound, "upload not found"});
			return res;
		}
		res.value = it->second->state;
	}

	res.status = _rt->recordStatus({DbStatusCode::Ok, ""});
	return res;
}

void FileStoreImpl::taskThunk(void *arg) {
	auto *self = static_cast<FileStoreImpl *>(arg);
	self->taskLoop();
}

void FileStoreImpl::startTaskUnlocked() {
	if (taskHandle != nullptr)
		return;
	stopRequested.store(false, std::memory_order_release);
	taskExited.store(false, std::memory_order_release);
	TaskHandle_t handle = nullptr;
	if (_rt->createTask(taskThunk, "db.file.upload", this, handle)) {
		taskHandle = handle;
	} else {
		taskExited.store(true, std::memory_order_release);
	}
}

void FileStoreImpl::stopTask(bool cancelPending) {
	if (cancelPending) {
		for (auto &kv : uploadJobs) {
			auto &job = kv.second;
			if (!job || isUploadTerminal(job->state))
				continue;
			job->cancelRequested = true;
			job->state = DbFileUploadState::Cancelled;
			job->finalStatus = {DbStatusCode::Busy, "upload cancelled"};
		}
		uploadQueue.clear();
	}
	if (taskHandle) {
		_rt->stopTask(taskHandle, stopRequested, taskExited);
	}
	uploadJobs.clear();
	terminalUploadOrder.clear();
	nextUploadId = 1;
}

DbStatus
FileStoreImpl::runUploadJob(const std::shared_ptr<FileUploadJob> &job, size_t &bytesWritten) {
	bytesWritten = 0;
	if (!job || !job->pullCb) {
		return {DbStatusCode::InvalidArgument, "upload callback is required"};
	}

	const size_t chunkSize = job->opts.chunkSize < 32 ? 32 : job->opts.chunkSize;
	JsonDbVector<uint8_t> buffer{JsonDbAllocator<uint8_t>(_rt->cfg.usePSRAMBuffers)};
	buffer.resize(chunkSize);

	const std::string finalPath = joinPath(_rt->fileRootDir(), job->normalizedPath);
	const std::string parentDir = parentDirOf(finalPath);
	const std::string tmpPath = finalPath + ".tmp";

	{
		FrLock fs(g_fsMutex);
		if (!fsEnsureDir(*_rt->fs, parentDir)) {
			return {DbStatusCode::IoError, "mkdir file parent failed"};
		}
		if (!job->opts.overwrite && _rt->fs->exists(finalPath.c_str())) {
			return {DbStatusCode::AlreadyExists, "file already exists"};
		}
		if (_rt->fs->exists(tmpPath.c_str())) {
			_rt->fs->remove(tmpPath.c_str());
		}
	}

	File f;
	{
		FrLock fs(g_fsMutex);
		f = _rt->fs->open(tmpPath.c_str(), FILE_WRITE);
	}
	if (!f) {
		return {DbStatusCode::IoError, "open file for write failed"};
	}

	auto cleanupTmp = [&]() {
		FrLock fs(g_fsMutex);
		f.close();
		if (_rt->fs->exists(tmpPath.c_str())) {
			_rt->fs->remove(tmpPath.c_str());
		}
	};

	for (;;) {
		bool cancelled = false;
		{
			FrLock lk(_rt->mu);
			auto it = uploadJobs.find(job->id);
			cancelled = (it == uploadJobs.end() || !it->second || it->second->cancelRequested);
		}
		if (cancelled) {
			cleanupTmp();
			return {DbStatusCode::Busy, "upload cancelled"};
		}

		size_t produced = 0;
		bool eof = false;
		auto st = job->pullCb(chunkSize, buffer.data(), produced, eof);
		if (!st.ok()) {
			cleanupTmp();
			return st;
		}
		if (produced > chunkSize) {
			cleanupTmp();
			return {DbStatusCode::InvalidArgument, "upload callback produced too many bytes"};
		}

		if (produced > 0) {
			size_t written = 0;
			{
				FrLock fs(g_fsMutex);
				written = f.write(buffer.data(), produced);
			}
			if (written != produced) {
				cleanupTmp();
				return {DbStatusCode::IoError, "file write failed"};
			}
			bytesWritten += written;
		}

		if (eof) {
			break;
		}
		if (produced == 0) {
			vTaskDelay(pdMS_TO_TICKS(1));
		}
	}

	{
		FrLock fs(g_fsMutex);
		f.flush();
		f.close();
		if (job->opts.overwrite && _rt->fs->exists(finalPath.c_str()) &&
		    !_rt->fs->remove(finalPath.c_str())) {
			if (_rt->fs->exists(tmpPath.c_str())) {
				_rt->fs->remove(tmpPath.c_str());
			}
			return {DbStatusCode::IoError, "remove old file failed"};
		}
		if (!_rt->fs->rename(tmpPath.c_str(), finalPath.c_str())) {
			if (_rt->fs->exists(tmpPath.c_str())) {
				_rt->fs->remove(tmpPath.c_str());
			}
			return {DbStatusCode::IoError, "rename file failed"};
		}
	}

	return {DbStatusCode::Ok, ""};
}

void FileStoreImpl::taskLoop() {
	while (!stopRequested.load(std::memory_order_acquire)) {
		std::shared_ptr<FileUploadJob> job;
		{
			FrLock lk(_rt->mu);
			if (!uploadQueue.empty()) {
				auto id = uploadQueue.front();
				uploadQueue.pop_front();
				auto it = uploadJobs.find(id);
				if (it != uploadJobs.end()) {
					job = it->second;
					if (job && job->state == DbFileUploadState::Queued && !job->cancelRequested) {
						job->state = DbFileUploadState::Running;
					}
				}
			}
		}

		if (!job) {
			vTaskDelay(pdMS_TO_TICKS(20));
			continue;
		}

		if (job->cancelRequested) {
			DbFileUploadDoneCb doneCb;
			{
				FrLock lk(_rt->mu);
				job->state = DbFileUploadState::Cancelled;
				job->finalStatus = {DbStatusCode::Busy, "upload cancelled"};
				trackTerminalUploadLocked(job);
				doneCb = job->doneCb;
			}
			if (doneCb) {
				doneCb(job->id, job->finalStatus, job->bytesWritten);
			}
			continue;
		}

		size_t bytesWritten = 0;
		auto st = runUploadJob(job, bytesWritten);

		DbFileUploadDoneCb doneCb;
		DbStatus finalStatus = st;
		DbFileUploadState finalState = DbFileUploadState::Failed;
		{
			FrLock lk(_rt->mu);
			auto it = uploadJobs.find(job->id);
			if (it != uploadJobs.end() && it->second) {
				auto &j = it->second;
				j->bytesWritten = bytesWritten;
				if (j->cancelRequested) {
					finalState = DbFileUploadState::Cancelled;
					finalStatus = {DbStatusCode::Busy, "upload cancelled"};
				} else if (st.ok()) {
					finalState = DbFileUploadState::Completed;
				}
				j->state = finalState;
				j->finalStatus = finalStatus;
				trackTerminalUploadLocked(j);
				doneCb = j->doneCb;
			}
		}

		if (!finalStatus.ok() && finalState != DbFileUploadState::Cancelled) {
			_rt->recordStatus(finalStatus);
		}
		if (doneCb) {
			doneCb(job->id, finalStatus, bytesWritten);
		}
	}
	taskExited.store(true, std::memory_order_release);
	vTaskDelete(nullptr);
}

DbStatus FileStore::writeFileStream(
    const std::string &relativePath,
    Stream &in,
    size_t bytesToWrite,
    const ESPJsonDBFileOptions &opts
) {
	if (!_impl)
		return {DbStatusCode::NotInitialized, "file store not initialized"};
	return _impl->writeFileStream(relativePath, in, bytesToWrite, opts);
}

DbStatus FileStore::writeFileStream(
    const std::string &relativePath,
    const DbFileUploadPullCb &pullCb,
    const ESPJsonDBFileOptions &opts
) {
	if (!_impl)
		return {DbStatusCode::NotInitialized, "file store not initialized"};
	return _impl->writeFileStream(relativePath, pullCb, opts);
}

DbStatus FileStore::writeFileFromPath(
    const std::string &relativePath,
    const std::string &sourceFsPath,
    const ESPJsonDBFileOptions &opts
) {
	if (!_impl)
		return {DbStatusCode::NotInitialized, "file store not initialized"};
	return _impl->writeFileFromPath(relativePath, sourceFsPath, opts);
}

DbStatus FileStore::writeFile(
    const std::string &relativePath, const uint8_t *data, size_t size, bool overwrite
) {
	if (!_impl)
		return {DbStatusCode::NotInitialized, "file store not initialized"};
	return _impl->writeFile(relativePath, data, size, overwrite);
}

DbStatus
FileStore::writeTextFile(const std::string &relativePath, const std::string &text, bool overwrite) {
	if (!_impl)
		return {DbStatusCode::NotInitialized, "file store not initialized"};
	return _impl->writeTextFile(relativePath, text, overwrite);
}

DbResult<size_t>
FileStore::readFileStream(const std::string &relativePath, Stream &out, size_t chunkSize) {
	if (!_impl)
		return {{DbStatusCode::NotInitialized, "file store not initialized"}, 0};
	return _impl->readFileStream(relativePath, out, chunkSize);
}

DbResult<std::vector<uint8_t>> FileStore::readFile(const std::string &relativePath) {
	if (!_impl)
		return {{DbStatusCode::NotInitialized, "file store not initialized"}, {}};
	return _impl->readFile(relativePath);
}

DbResult<std::string> FileStore::readTextFile(const std::string &relativePath) {
	if (!_impl)
		return {{DbStatusCode::NotInitialized, "file store not initialized"}, {}};
	return _impl->readTextFile(relativePath);
}

DbResult<JsonDocument> FileStore::getFileInfo(const std::string &relativePath) {
	if (!_impl)
		return {{DbStatusCode::NotInitialized, "file store not initialized"}, JsonDocument{}};
	return _impl->getFileInfo(relativePath);
}

DbResult<JsonDocument> FileStore::listFiles(const std::string &relativePrefix, bool recursive) {
	if (!_impl)
		return {{DbStatusCode::NotInitialized, "file store not initialized"}, JsonDocument{}};
	return _impl->listFiles(relativePrefix, recursive);
}

DbStatus FileStore::removeFile(const std::string &relativePath) {
	if (!_impl)
		return {DbStatusCode::NotInitialized, "file store not initialized"};
	return _impl->removeFile(relativePath);
}

DbResult<bool> FileStore::fileExists(const std::string &relativePath) {
	if (!_impl)
		return {{DbStatusCode::NotInitialized, "file store not initialized"}, false};
	return _impl->fileExists(relativePath);
}

DbResult<size_t> FileStore::fileSize(const std::string &relativePath) {
	if (!_impl)
		return {{DbStatusCode::NotInitialized, "file store not initialized"}, 0};
	return _impl->fileSize(relativePath);
}

DbResult<uint32_t> FileStore::writeFileStreamAsync(
    const std::string &relativePath,
    const DbFileUploadPullCb &pullCb,
    const ESPJsonDBFileOptions &opts,
    const DbFileUploadDoneCb &doneCb
) {
	if (!_impl)
		return {{DbStatusCode::NotInitialized, "file store not initialized"}, 0};
	return _impl->writeFileStreamAsync(relativePath, pullCb, opts, doneCb);
}

DbStatus FileStore::cancelUpload(uint32_t uploadId) {
	if (!_impl)
		return {DbStatusCode::NotInitialized, "file store not initialized"};
	return _impl->cancelUpload(uploadId);
}

DbResult<DbFileUploadState> FileStore::getUploadState(uint32_t uploadId) {
	if (!_impl)
		return {
		    {DbStatusCode::NotInitialized, "file store not initialized"},
		    DbFileUploadState::Failed
		};
	return _impl->getUploadState(uploadId);
}
