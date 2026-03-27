#pragma once

#include <Arduino.h>
#include <FS.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "../utils/dbTypes.h"

class ESPJsonDB;

class FileStore {
  public:
	explicit FileStore(ESPJsonDB &db) : _db(&db) {
	}

	DbStatus writeFileStream(
	    const std::string &relativePath,
	    Stream &in,
	    size_t bytesToWrite,
	    const ESPJsonDBFileOptions &opts = {}
	);
	DbStatus writeFileStream(
	    const std::string &relativePath,
	    const DbFileUploadPullCb &pullCb,
	    const ESPJsonDBFileOptions &opts = {}
	);
	DbStatus writeFileFromPath(
	    const std::string &relativePath,
	    const std::string &sourceFsPath,
	    const ESPJsonDBFileOptions &opts = {}
	);
	DbStatus writeFile(
	    const std::string &relativePath, const uint8_t *data, size_t size, bool overwrite = true
	);
	DbStatus
	writeTextFile(const std::string &relativePath, const std::string &text, bool overwrite = true);
	DbResult<size_t>
	readFileStream(const std::string &relativePath, Stream &out, size_t chunkSize = 512);
	DbResult<std::vector<uint8_t>> readFile(const std::string &relativePath);
	DbResult<std::string> readTextFile(const std::string &relativePath);
	DbResult<JsonDocument> getFileInfo(const std::string &relativePath);
	DbResult<JsonDocument> listFiles(const std::string &relativePrefix = "", bool recursive = true);
	DbStatus removeFile(const std::string &relativePath);
	DbResult<bool> fileExists(const std::string &relativePath);
	DbResult<size_t> fileSize(const std::string &relativePath);
	DbResult<uint32_t> writeFileStreamAsync(
	    const std::string &relativePath,
	    const DbFileUploadPullCb &pullCb,
	    const ESPJsonDBFileOptions &opts = {},
	    const DbFileUploadDoneCb &doneCb = {}
	);
	DbStatus cancelUpload(uint32_t uploadId);
	DbResult<DbFileUploadState> getUploadState(uint32_t uploadId);

  private:
	ESPJsonDB *_db = nullptr;
};
