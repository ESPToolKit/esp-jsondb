#include "db.h"

#include <StreamUtils.h>

#include <algorithm>

#include "utils/fs_utils.h"
#include "utils/jsondb_allocator.h"

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
			info.size = 0;
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

DbStatus
ESPJsonDB::normalizeFilePath(const std::string &rawRelativePath, std::string &normalized) const {
	normalized.clear();
	if (rawRelativePath.empty()) {
		return {DbStatusCode::InvalidArgument, "file path is empty"};
	}

	std::string segment;
	segment.reserve(rawRelativePath.size());

	auto flushSegment = [&](void) -> DbStatus {
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

DbStatus ESPJsonDB::writeFileStream(
    const std::string &relativePath,
    Stream &in,
    size_t bytesToWrite,
    const ESPJsonDBFileOptions &opts
) {
	auto ready = ensureReady();
	if (!ready.ok())
		return setLastError(ready);

	std::string normalized;
	auto nst = normalizeFilePath(relativePath, normalized);
	if (!nst.ok())
		return setLastError(nst);

	const std::string finalPath = joinPath(fileRootDir(), normalized);
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
	auto st = writeFromPullCb(*_fs, finalPath, _cfg.usePSRAMBuffers, opts, pullCb, totalWritten);
	if (!st.ok())
		return setLastError(st);
	if (totalWritten != bytesToWrite) {
		return setLastError({DbStatusCode::IoError, "written size mismatch"});
	}

	return setLastError({DbStatusCode::Ok, ""});
}

DbStatus ESPJsonDB::writeFileStream(
    const std::string &relativePath,
    const DbFileUploadPullCb &pullCb,
    const ESPJsonDBFileOptions &opts
) {
	auto ready = ensureReady();
	if (!ready.ok())
		return setLastError(ready);

	std::string normalized;
	auto nst = normalizeFilePath(relativePath, normalized);
	if (!nst.ok())
		return setLastError(nst);

	const std::string finalPath = joinPath(fileRootDir(), normalized);
	size_t totalWritten = 0;
	auto st = writeFromPullCb(*_fs, finalPath, _cfg.usePSRAMBuffers, opts, pullCb, totalWritten);
	return setLastError(st);
}

DbStatus ESPJsonDB::writeFileFromPath(
    const std::string &relativePath,
    const std::string &sourceFsPath,
    const ESPJsonDBFileOptions &opts
) {
	if (sourceFsPath.empty()) {
		return setLastError({DbStatusCode::InvalidArgument, "source file path is empty"});
	}
	auto ready = ensureReady();
	if (!ready.ok())
		return setLastError(ready);

	File source;
	{
		FrLock fs(g_fsMutex);
		source = _fs->open(sourceFsPath.c_str(), FILE_READ);
	}
	if (!source) {
		return setLastError({DbStatusCode::NotFound, "source file not found"});
	}

	const size_t bytesToWrite = source.size();
	auto st = writeFileStream(relativePath, source, bytesToWrite, opts);
	source.close();
	return st;
}

DbStatus ESPJsonDB::writeFile(
    const std::string &relativePath, const uint8_t *data, size_t size, bool overwrite
) {
	if (size > 0 && data == nullptr) {
		return setLastError({DbStatusCode::InvalidArgument, "file data is null"});
	}

	auto ready = ensureReady();
	if (!ready.ok())
		return setLastError(ready);

	std::string normalized;
	auto nst = normalizeFilePath(relativePath, normalized);
	if (!nst.ok())
		return setLastError(nst);

	const std::string finalPath = joinPath(fileRootDir(), normalized);
	const std::string parentDir = parentDirOf(finalPath);
	const std::string tmpPath = finalPath + ".tmp";

	FrLock fs(g_fsMutex);
	if (!fsEnsureDir(*_fs, parentDir)) {
		return setLastError({DbStatusCode::IoError, "mkdir file parent failed"});
	}
	if (!overwrite && _fs->exists(finalPath.c_str())) {
		return setLastError({DbStatusCode::AlreadyExists, "file already exists"});
	}
	if (_fs->exists(tmpPath.c_str())) {
		_fs->remove(tmpPath.c_str());
	}

	File f = _fs->open(tmpPath.c_str(), FILE_WRITE);
	if (!f) {
		return setLastError({DbStatusCode::IoError, "open file for write failed"});
	}
	WriteBufferingStream buffered(f, 256);

	size_t written = 0;
	if (size > 0) {
		written = buffered.write(data, size);
	}
	buffered.flush();
	f.close();
	if (written != size) {
		_fs->remove(tmpPath.c_str());
		return setLastError({DbStatusCode::IoError, "file write failed"});
	}

	if (overwrite && _fs->exists(finalPath.c_str()) && !_fs->remove(finalPath.c_str())) {
		_fs->remove(tmpPath.c_str());
		return setLastError({DbStatusCode::IoError, "remove old file failed"});
	}
	if (!_fs->rename(tmpPath.c_str(), finalPath.c_str())) {
		_fs->remove(tmpPath.c_str());
		return setLastError({DbStatusCode::IoError, "rename file failed"});
	}

	return setLastError({DbStatusCode::Ok, ""});
}

DbStatus
ESPJsonDB::writeTextFile(const std::string &relativePath, const std::string &text, bool overwrite) {
	return writeFile(
	    relativePath,
	    reinterpret_cast<const uint8_t *>(text.data()),
	    text.size(),
	    overwrite
	);
}

DbResult<size_t>
ESPJsonDB::readFileStream(const std::string &relativePath, Stream &out, size_t chunkSize) {
	DbResult<size_t> res{};
	auto ready = ensureReady();
	if (!ready.ok()) {
		res.status = setLastError(ready);
		return res;
	}

	std::string normalized;
	auto nst = normalizeFilePath(relativePath, normalized);
	if (!nst.ok()) {
		res.status = setLastError(nst);
		return res;
	}

	if (chunkSize < 32)
		chunkSize = 32;
	JsonDbVector<uint8_t> buffer{JsonDbAllocator<uint8_t>(_cfg.usePSRAMBuffers)};
	buffer.resize(chunkSize);
	const std::string path = joinPath(fileRootDir(), normalized);

	FrLock fs(g_fsMutex);
	File f = _fs->open(path.c_str(), FILE_READ);
	if (!f) {
		res.status = setLastError({DbStatusCode::NotFound, "file not found"});
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
			res.status = setLastError({DbStatusCode::IoError, "output stream write failed"});
			return res;
		}
		total += written;
	}
	f.close();

	res.status = setLastError({DbStatusCode::Ok, ""});
	res.value = total;
	return res;
}

DbResult<std::vector<uint8_t>> ESPJsonDB::readFile(const std::string &relativePath) {
	DbResult<std::vector<uint8_t>> res{};
	auto ready = ensureReady();
	if (!ready.ok()) {
		res.status = setLastError(ready);
		return res;
	}

	std::string normalized;
	auto nst = normalizeFilePath(relativePath, normalized);
	if (!nst.ok()) {
		res.status = setLastError(nst);
		return res;
	}

	const std::string path = joinPath(fileRootDir(), normalized);

	FrLock fs(g_fsMutex);
	File f = _fs->open(path.c_str(), FILE_READ);
	if (!f) {
		res.status = setLastError({DbStatusCode::NotFound, "file not found"});
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
		res.status = setLastError({DbStatusCode::IoError, "file read failed"});
		return res;
	}

	res.status = setLastError({DbStatusCode::Ok, ""});
	return res;
}

DbResult<std::string> ESPJsonDB::readTextFile(const std::string &relativePath) {
	DbResult<std::string> res{};
	auto fr = readFile(relativePath);
	if (!fr.status.ok()) {
		res.status = fr.status;
		return res;
	}
	res.value.assign(fr.value.begin(), fr.value.end());
	res.status = setLastError({DbStatusCode::Ok, ""});
	return res;
}

DbResult<JsonDocument> ESPJsonDB::getFileInfo(const std::string &relativePath) {
	DbResult<JsonDocument> res{};
	auto ready = ensureReady();
	if (!ready.ok()) {
		res.status = setLastError(ready);
		return res;
	}

	std::string normalized;
	auto nst = normalizeFilePath(relativePath, normalized);
	if (!nst.ok()) {
		res.status = setLastError(nst);
		return res;
	}

	FileEntryInfo info;
	const auto st = statFileEntry(*_fs, joinPath(fileRootDir(), normalized), normalized, info);
	if (!st.ok()) {
		res.status = setLastError(st);
		return res;
	}

	res.value["path"] = info.path.c_str();
	res.value["name"] = fileNameOf(info.path).c_str();
	res.value["exists"] = true;
	res.value["isDirectory"] = info.isDirectory;
	res.value["size"] = info.size;
	res.status = setLastError({DbStatusCode::Ok, ""});
	return res;
}

DbResult<JsonDocument>
ESPJsonDB::listFiles(const std::string &relativePrefix, bool recursive) {
	DbResult<JsonDocument> res{};
	auto ready = ensureReady();
	if (!ready.ok()) {
		res.status = setLastError(ready);
		return res;
	}

	std::string normalizedPrefix;
	if (!relativePrefix.empty()) {
		auto nst = normalizeFilePath(relativePrefix, normalizedPrefix);
		if (!nst.ok()) {
			res.status = setLastError(nst);
			return res;
		}
	}

	const std::string rootPath = fileRootDir();
	const std::string targetPath =
	    normalizedPrefix.empty() ? rootPath : joinPath(rootPath, normalizedPrefix);

	FileEntryInfo targetInfo;
	auto st = statFileEntry(*_fs, targetPath, normalizedPrefix, targetInfo);
	if (!st.ok()) {
		res.status = setLastError(st);
		return res;
	}

	std::vector<FileEntryInfo> entries;
	if (targetInfo.isDirectory) {
		st = collectDirectoryEntries(*_fs, targetPath, normalizedPrefix, recursive, entries);
		if (!st.ok()) {
			res.status = setLastError(st);
			return res;
		}
	} else {
		entries.push_back(targetInfo);
	}

	std::sort(entries.begin(), entries.end(), [](const FileEntryInfo &lhs, const FileEntryInfo &rhs) {
		return lhs.path < rhs.path;
	});

	res.value["prefix"] = normalizedPrefix.c_str();
	res.value["recursive"] = recursive;
	JsonArray entriesJson = res.value["entries"].to<JsonArray>();
	for (const auto &entry : entries) {
		appendFileInfoJson(entriesJson, entry);
	}

	res.status = setLastError({DbStatusCode::Ok, ""});
	return res;
}

DbStatus ESPJsonDB::removeFile(const std::string &relativePath) {
	auto ready = ensureReady();
	if (!ready.ok())
		return setLastError(ready);

	std::string normalized;
	auto nst = normalizeFilePath(relativePath, normalized);
	if (!nst.ok())
		return setLastError(nst);

	const std::string path = joinPath(fileRootDir(), normalized);
	FrLock fs(g_fsMutex);
	if (!_fs->exists(path.c_str())) {
		return setLastError({DbStatusCode::NotFound, "file not found"});
	}
	if (!_fs->remove(path.c_str())) {
		return setLastError({DbStatusCode::IoError, "file remove failed"});
	}
	return setLastError({DbStatusCode::Ok, ""});
}

DbResult<bool> ESPJsonDB::fileExists(const std::string &relativePath) {
	DbResult<bool> res{};
	auto ready = ensureReady();
	if (!ready.ok()) {
		res.status = setLastError(ready);
		return res;
	}

	std::string normalized;
	auto nst = normalizeFilePath(relativePath, normalized);
	if (!nst.ok()) {
		res.status = setLastError(nst);
		return res;
	}

	const std::string path = joinPath(fileRootDir(), normalized);
	FrLock fs(g_fsMutex);
	res.value = _fs->exists(path.c_str());
	res.status = setLastError({DbStatusCode::Ok, ""});
	return res;
}

DbResult<size_t> ESPJsonDB::fileSize(const std::string &relativePath) {
	DbResult<size_t> res{};
	auto ready = ensureReady();
	if (!ready.ok()) {
		res.status = setLastError(ready);
		return res;
	}

	std::string normalized;
	auto nst = normalizeFilePath(relativePath, normalized);
	if (!nst.ok()) {
		res.status = setLastError(nst);
		return res;
	}

	const std::string path = joinPath(fileRootDir(), normalized);
	FrLock fs(g_fsMutex);
	File f = _fs->open(path.c_str(), FILE_READ);
	if (!f) {
		res.status = setLastError({DbStatusCode::NotFound, "file not found"});
		return res;
	}
	res.value = f.size();
	f.close();
	res.status = setLastError({DbStatusCode::Ok, ""});
	return res;
}
