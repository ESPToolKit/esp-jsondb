#include "file_store.h"

#include "../db.h"

DbStatus FileStore::writeFileStream(
    const std::string &relativePath,
    Stream &in,
    size_t bytesToWrite,
    const ESPJsonDBFileOptions &opts
) {
	return _db->writeFileStream(relativePath, in, bytesToWrite, opts);
}

DbStatus FileStore::writeFileStream(
    const std::string &relativePath,
    const DbFileUploadPullCb &pullCb,
    const ESPJsonDBFileOptions &opts
) {
	return _db->writeFileStream(relativePath, pullCb, opts);
}

DbStatus FileStore::writeFileFromPath(
    const std::string &relativePath, const std::string &sourceFsPath, const ESPJsonDBFileOptions &opts
) {
	return _db->writeFileFromPath(relativePath, sourceFsPath, opts);
}

DbStatus FileStore::writeFile(
    const std::string &relativePath, const uint8_t *data, size_t size, bool overwrite
) {
	return _db->writeFile(relativePath, data, size, overwrite);
}

DbStatus FileStore::writeTextFile(
    const std::string &relativePath, const std::string &text, bool overwrite
) {
	return _db->writeTextFile(relativePath, text, overwrite);
}

DbResult<size_t>
FileStore::readFileStream(const std::string &relativePath, Stream &out, size_t chunkSize) {
	return _db->readFileStream(relativePath, out, chunkSize);
}

DbResult<std::vector<uint8_t>> FileStore::readFile(const std::string &relativePath) {
	return _db->readFile(relativePath);
}

DbResult<std::string> FileStore::readTextFile(const std::string &relativePath) {
	return _db->readTextFile(relativePath);
}

DbResult<JsonDocument> FileStore::getFileInfo(const std::string &relativePath) {
	return _db->getFileInfo(relativePath);
}

DbResult<JsonDocument> FileStore::listFiles(const std::string &relativePrefix, bool recursive) {
	return _db->listFiles(relativePrefix, recursive);
}

DbStatus FileStore::removeFile(const std::string &relativePath) {
	return _db->removeFile(relativePath);
}

DbResult<bool> FileStore::fileExists(const std::string &relativePath) {
	return _db->fileExists(relativePath);
}

DbResult<size_t> FileStore::fileSize(const std::string &relativePath) {
	return _db->fileSize(relativePath);
}

DbResult<uint32_t> FileStore::writeFileStreamAsync(
    const std::string &relativePath,
    const DbFileUploadPullCb &pullCb,
    const ESPJsonDBFileOptions &opts,
    const DbFileUploadDoneCb &doneCb
) {
	return _db->writeFileStreamAsync(relativePath, pullCb, opts, doneCb);
}

DbStatus FileStore::cancelUpload(uint32_t uploadId) {
	return _db->cancelFileUpload(uploadId);
}

DbResult<DbFileUploadState> FileStore::getUploadState(uint32_t uploadId) {
	return _db->getFileUploadState(uploadId);
}
