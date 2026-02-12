#include "db.h"

#include <StreamUtils.h>

#include <algorithm>

#include "utils/fs_utils.h"

namespace {

std::string parentDirOf(const std::string &path) {
	auto pos = path.find_last_of('/');
	if (pos == std::string::npos || pos == 0) {
		return "/";
	}
	return path.substr(0, pos);
}

} // namespace

DbStatus ESPJsonDB::normalizeFilePath(const std::string &rawRelativePath, std::string &normalized) const {
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
		if (c == '\\') c = '/';
		if (c == '/') {
			auto st = flushSegment();
			if (!st.ok()) return st;
			continue;
		}
		segment.push_back(c);
	}
	auto st = flushSegment();
	if (!st.ok()) return st;

	if (normalized.empty()) {
		return {DbStatusCode::InvalidArgument, "file path resolves to empty"};
	}

	return {DbStatusCode::Ok, ""};
}

DbStatus ESPJsonDB::writeFileStream(const std::string &relativePath,
										Stream &in,
										size_t bytesToWrite,
										const ESPJsonDBFileOptions &opts) {
	auto ready = ensureReady();
	if (!ready.ok()) return setLastError(ready);

	std::string normalized;
	auto nst = normalizeFilePath(relativePath, normalized);
	if (!nst.ok()) return setLastError(nst);

	const size_t chunkSize = opts.chunkSize < 32 ? 32 : opts.chunkSize;
	std::vector<uint8_t> buffer(chunkSize);

	const std::string finalPath = joinPath(fileRootDir(), normalized);
	const std::string parentDir = parentDirOf(finalPath);
	const std::string tmpPath = finalPath + ".tmp";

	FrLock fs(g_fsMutex);
	if (!fsEnsureDir(*_fs, parentDir)) {
		return setLastError({DbStatusCode::IoError, "mkdir file parent failed"});
	}
	if (!opts.overwrite && _fs->exists(finalPath.c_str())) {
		return setLastError({DbStatusCode::AlreadyExists, "file already exists"});
	}
	if (_fs->exists(tmpPath.c_str())) {
		_fs->remove(tmpPath.c_str());
	}

	File f = _fs->open(tmpPath.c_str(), FILE_WRITE);
	if (!f) {
		return setLastError({DbStatusCode::IoError, "open file for write failed"});
	}
	WriteBufferingStream buffered(f, chunkSize);

	size_t remaining = bytesToWrite;
	size_t totalWritten = 0;
	while (remaining > 0) {
		size_t want = std::min(remaining, chunkSize);
		size_t got = in.readBytes(reinterpret_cast<char *>(buffer.data()), want);
		if (got == 0) {
			buffered.flush();
			f.close();
			_fs->remove(tmpPath.c_str());
			return setLastError({DbStatusCode::IoError, "stream ended before expected size"});
		}
		size_t written = buffered.write(buffer.data(), got);
		if (written != got) {
			buffered.flush();
			f.close();
			_fs->remove(tmpPath.c_str());
			return setLastError({DbStatusCode::IoError, "file write failed"});
		}
		totalWritten += written;
		remaining -= got;
	}

	buffered.flush();
	f.close();

	if (totalWritten != bytesToWrite) {
		_fs->remove(tmpPath.c_str());
		return setLastError({DbStatusCode::IoError, "written size mismatch"});
	}

	if (opts.overwrite && _fs->exists(finalPath.c_str()) && !_fs->remove(finalPath.c_str())) {
		_fs->remove(tmpPath.c_str());
		return setLastError({DbStatusCode::IoError, "remove old file failed"});
	}
	if (!_fs->rename(tmpPath.c_str(), finalPath.c_str())) {
		_fs->remove(tmpPath.c_str());
		return setLastError({DbStatusCode::IoError, "rename file failed"});
	}

	return setLastError({DbStatusCode::Ok, ""});
}

DbStatus ESPJsonDB::writeFile(const std::string &relativePath,
								 const uint8_t *data,
								 size_t size,
								 bool overwrite) {
	if (size > 0 && data == nullptr) {
		return setLastError({DbStatusCode::InvalidArgument, "file data is null"});
	}

	auto ready = ensureReady();
	if (!ready.ok()) return setLastError(ready);

	std::string normalized;
	auto nst = normalizeFilePath(relativePath, normalized);
	if (!nst.ok()) return setLastError(nst);

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

DbStatus ESPJsonDB::writeTextFile(const std::string &relativePath,
								 const std::string &text,
								 bool overwrite) {
	return writeFile(relativePath,
					 reinterpret_cast<const uint8_t *>(text.data()),
					 text.size(),
					 overwrite);
}

DbResult<size_t> ESPJsonDB::readFileStream(const std::string &relativePath, Stream &out, size_t chunkSize) {
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

	if (chunkSize < 32) chunkSize = 32;
	std::vector<uint8_t> buffer(chunkSize);
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
		if (readBytes == 0) break;
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

DbStatus ESPJsonDB::removeFile(const std::string &relativePath) {
	auto ready = ensureReady();
	if (!ready.ok()) return setLastError(ready);

	std::string normalized;
	auto nst = normalizeFilePath(relativePath, normalized);
	if (!nst.ok()) return setLastError(nst);

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
