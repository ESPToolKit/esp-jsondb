#pragma once

#include <Arduino.h>

#include <atomic>
#include <memory>
#include <string>

#include "../utils/dbTypes.h"
#include "../utils/jsondb_allocator.h"

class ESPJsonDB;
struct DbRuntime;

struct FileStoreImpl {
	struct FileUploadJob {
		uint32_t id = 0;
		std::string relativePath;
		std::string normalizedPath;
		ESPJsonDBFileOptions opts{};
		DbFileUploadPullCb pullCb{};
		DbFileUploadDoneCb doneCb{};
		DbFileUploadState state = DbFileUploadState::Queued;
		DbStatus finalStatus{DbStatusCode::Ok, ""};
		size_t bytesWritten = 0;
		bool cancelRequested = false;
		bool terminalTracked = false;
	};

	static constexpr size_t kMaxRetainedTerminalUploads = 64;
	using UploadIdDeque = JsonDbDeque<uint32_t>;
	using UploadJobMap = JsonDbMap<uint32_t, std::shared_ptr<FileUploadJob>>;

	FileStoreImpl(ESPJsonDB &db, DbRuntime &rt);

	DbStatus normalizePath(const std::string &rawRelativePath, std::string &normalized) const;

	DbStatus writeFileStream(
	    const std::string &relativePath,
	    Stream &in,
	    size_t bytesToWrite,
	    const ESPJsonDBFileOptions &opts
	);
	DbStatus writeFileStream(
	    const std::string &relativePath,
	    const DbFileUploadPullCb &pullCb,
	    const ESPJsonDBFileOptions &opts
	);
	DbStatus writeFileFromPath(
	    const std::string &relativePath,
	    const std::string &sourceFsPath,
	    const ESPJsonDBFileOptions &opts
	);
	DbStatus writeFile(
	    const std::string &relativePath, const uint8_t *data, size_t size, bool overwrite
	);
	DbStatus writeTextFile(const std::string &relativePath, const std::string &text, bool overwrite);
	DbResult<size_t> readFileStream(const std::string &relativePath, Stream &out, size_t chunkSize);
	DbResult<std::vector<uint8_t>> readFile(const std::string &relativePath);
	DbResult<std::string> readTextFile(const std::string &relativePath);
	DbResult<JsonDocument> getFileInfo(const std::string &relativePath);
	DbResult<JsonDocument> listFiles(const std::string &relativePrefix, bool recursive);
	DbStatus removeFile(const std::string &relativePath);
	DbResult<bool> fileExists(const std::string &relativePath);
	DbResult<size_t> fileSize(const std::string &relativePath);
	DbResult<uint32_t> writeFileStreamAsync(
	    const std::string &relativePath,
	    const DbFileUploadPullCb &pullCb,
	    const ESPJsonDBFileOptions &opts,
	    const DbFileUploadDoneCb &doneCb
	);
	DbStatus cancelUpload(uint32_t uploadId);
	DbResult<DbFileUploadState> getUploadState(uint32_t uploadId);

	void stopTask(bool cancelPending);

	bool isUploadTerminal(DbFileUploadState state) const;
	void trackTerminalUploadLocked(const std::shared_ptr<FileUploadJob> &job);
	static void taskThunk(void *arg);
	void taskLoop();
	void startTaskUnlocked();
	DbStatus runUploadJob(const std::shared_ptr<FileUploadJob> &job, size_t &bytesWritten);

	ESPJsonDB *_db = nullptr;
	DbRuntime *_rt = nullptr;
	TaskHandle_t taskHandle = nullptr;
	std::atomic<bool> stopRequested{false};
	std::atomic<bool> taskExited{true};
	uint32_t nextUploadId = 1;
	UploadIdDeque uploadQueue;
	UploadJobMap uploadJobs;
	UploadIdDeque terminalUploadOrder;
};
