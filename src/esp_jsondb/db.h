#pragma once

#include <Arduino.h>

#include <LittleFS.h>
#include <atomic>
#include <freertos/FreeRTOS.h>
#include <freertos/task.h>
#include <functional>
#include <map>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "collection/collection.h"
#include "utils/dbTypes.h"
#include "utils/fr_mutex.h"
#include "utils/schema.h"
#include <ArduinoJson.h>

class ESPJsonDB {
  public:
	~ESPJsonDB();
	DbStatus init(const char *baseDir = "/db", const ESPJsonDBConfig &cfg = {});
	void deinit();
	bool isInitialized() const {
		return _initialized.load(std::memory_order_acquire);
	}
	DbStatus registerSchema(const std::string &name, const Schema &s);
	DbStatus unRegisterSchema(const std::string &name);
	DbStatus dropCollection(const std::string &name);

	// Drop all collections and documents (clears base directory)
	DbStatus dropAll();

	// Returns collection names tracked in memory (preloaded + runtime-created)
	std::vector<std::string> getAllCollectionName();

	// Change sync configuration; restarts autosync task if needed
	DbStatus changeConfig(const ESPJsonDBConfig &cfg);

	// Register a generic DB event callback
	void onEvent(const std::function<void(DBEventType)> &cb);

	// Register callback for error notifications
	void onError(const std::function<void(const DbStatus &)> &cb);

	// Backwards-compat: register a sync-only callback
	void onSync(const std::function<void()> &cb);

	// Creates collection if missing
	DbResult<Collection *> collection(const std::string &name);
	// Arduino-friendly overload
	DbResult<Collection *> collection(const String &name);
	DbResult<Collection *> collection(const char *name);

	// Convenience: create a document in the given collection
	DbResult<std::string> create(const std::string &collectionName, JsonObjectConst doc);

	// Convenience: create from a JsonDocument; validates it's an object
	DbResult<std::string> create(const std::string &collectionName, const JsonDocument &doc);

	// Convenience: bulk create documents in a collection
	DbResult<std::vector<std::string>>
	createMany(const std::string &collectionName, JsonArrayConst arr);
	DbResult<std::vector<std::string>>
	createMany(const std::string &collectionName, const JsonDocument &arrDoc);

	// Convenience: find a document by _id in the given collection
	DbResult<DocView> findById(const std::string &collectionName, const std::string &id);

	// Convenience: find documents matching predicate in the given collection
	DbResult<std::vector<DocView>>
	findMany(const std::string &collectionName, std::function<bool(const DocView &)> pred);

	// Convenience: find the first document matching predicate in the given collection
	DbResult<DocView>
	findOne(const std::string &collectionName, std::function<bool(const DocView &)> pred);

	// Convenience: find the first document matching a JSON filter in the given collection
	DbResult<DocView> findOne(const std::string &collectionName, const JsonDocument &filter);

	// Convenience: update the first match (predicate + mutator). If create=true, creates new when
	// none found
	DbStatus updateOne(
	    const std::string &collectionName,
	    std::function<bool(const DocView &)> pred,
	    std::function<void(DocView &)> mutator,
	    bool create = false
	);

	// Convenience: update the first match (JSON filter + JSON patch). If create=true, creates new
	// when none found
	DbStatus updateOne(
	    const std::string &collectionName,
	    const JsonDocument &filter,
	    const JsonDocument &patch,
	    bool create = false
	);

	// Convenience: update a document by _id in the given collection
	DbStatus updateById(
	    const std::string &collectionName,
	    const std::string &id,
	    std::function<void(DocView &)> mutator
	);

	// Convenience: remove a document by _id in the given collection
	DbStatus removeById(const std::string &collectionName, const std::string &id);

	// Bulk operations on a collection
	template <typename Pred>
	DbResult<size_t> removeMany(const std::string &collectionName, Pred &&p);

	template <
	    typename Pred,
	    typename Mut,
	    typename = std::enable_if_t<
	        !std::is_same_v<std::decay_t<Pred>, JsonDocument> &&
	        !std::is_same_v<std::decay_t<Mut>, JsonDocument>>>
	DbResult<size_t> updateMany(const std::string &collectionName, Pred &&p, Mut &&m);

	template <
	    typename Mut,
	    typename = std::enable_if_t<!std::is_same_v<std::decay_t<Mut>, JsonDocument>>>
	DbResult<size_t> updateMany(const std::string &collectionName, Mut &&m);

	template <
	    typename Pred,
	    typename = std::enable_if_t<!std::is_same_v<std::decay_t<Pred>, JsonDocument>>>
	DbResult<size_t>
	updateMany(const std::string &collectionName, const JsonDocument &patch, Pred &&p);

	DbResult<size_t> updateMany(
	    const std::string &collectionName, const JsonDocument &patch, const JsonDocument &filter
	);

	// Manual sync (safe to call from app)
	DbStatus syncNow();

	// Retrieve last error or success status
	DbStatus lastError() const {
		return _lastError;
	}

	// Allow other components to update diagnostics/error state
	DbStatus recordStatus(const DbStatus &st) {
		return setLastError(st);
	}

	// Diagnostics: number of collections, doc counts, and config
	JsonDocument getDiag();

	// Backup/restore
	JsonDocument getSnapshot();
	DbStatus restoreFromSnapshot(const JsonDocument &snapshot);

	// Generic file-bytes helpers under <baseDir>/_files.
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

	DbStatus removeFile(const std::string &relativePath);
	DbResult<bool> fileExists(const std::string &relativePath);
	DbResult<size_t> fileSize(const std::string &relativePath);

	// Non-blocking chunked file upload worker API.
	// The pull callback runs on a background task and must fill up to `requested` bytes.
	DbResult<uint32_t> writeFileStreamAsync(
	    const std::string &relativePath,
	    const DbFileUploadPullCb &pullCb,
	    const ESPJsonDBFileOptions &opts = {},
	    const DbFileUploadDoneCb &doneCb = {}
	);
	DbStatus cancelFileUpload(uint32_t uploadId);
	DbResult<DbFileUploadState> getFileUploadState(uint32_t uploadId);

	// Emit an event to registered listeners
	void emitEvent(DBEventType ev);

	// Emit an error to registered listeners
	void emitError(const DbStatus &st);

	// Internal diagnostics counters used by Collection write paths.
	void noteDocumentCreated(const std::string &collectionName, uint32_t count = 1);
	void noteDocumentDeleted(const std::string &collectionName, uint32_t count = 1);

  private:
	std::string _baseDir;
	ESPJsonDBConfig _cfg;
	std::map<std::string, std::unique_ptr<Collection>> _cols;
	std::map<std::string, Schema> _schemas;
	std::vector<std::string> _colsToDelete;
	std::vector<std::function<void(DBEventType)>> _eventCbs;
	std::vector<std::function<void(const DbStatus &)>> _errorCbs;
	fs::FS *_fs = &LittleFS; // active filesystem
	FrMutex _mu;             // guards _cols, _schemas, _colsToDelete

	// Tracks most recent status for diagnostics/debugging
	DbStatus _lastError{DbStatusCode::Ok, ""};

	struct DiagCache {
		std::map<std::string, uint32_t> docsPerCollection;
		uint32_t collections = 0;
		uint32_t lastRefreshMs = 0; // millis when refreshed from FS
	};

	DiagCache _diagCache;          // cached diagnostics; read without touching FS
	bool _diagCachePrimed = false; // true once runtime counters are initialized
	std::atomic<bool> _initialized{false};

	// sync task
	static void syncTaskThunk(void *arg);
	void syncTaskLoop();
	DbStatus runSyncPass();
	void startSyncTaskUnlocked();
	void stopSyncTaskUnlocked();

	// Update last error/status helper
	DbStatus setLastError(const DbStatus &st) {
		_lastError = st;
		if (!st.ok())
			emitError(st);
		return st;
	}

	// fs helpers
	DbStatus ensureFsReady();
	DbStatus ensureReady() const;
	DbStatus removeCollectionDir(const std::string &name);
	bool isReservedName(const std::string &name) const;
	std::string fileRootDir() const;
	DbStatus normalizeFilePath(const std::string &rawRelativePath, std::string &normalized) const;
	void rebuildDelayedCollectionStateFromConfigLocked();
	DbStatus maybeRunDelayedPreload(bool triggeredByPeriodic);
	DbStatus preloadPendingDelayedCollectionsFromFs();
	DbStatus preloadCollectionFromFsByName(
	    const std::string &name, bool markDelayedHandled, bool *insertedOut = nullptr
	);
	bool collectionDirExistsOnFs(const std::string &name) const;

	// async file upload task
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
	static void fileUploadTaskThunk(void *arg);
	void fileUploadTaskLoop();
	void startFileUploadTaskUnlocked();
	void stopFileUploadTaskUnlocked(bool cancelPending);
	DbStatus runFileUploadJob(const std::shared_ptr<FileUploadJob> &job, size_t &bytesWritten);
	bool isUploadTerminal(DbFileUploadState state) const;
	void trackTerminalUploadLocked(const std::shared_ptr<FileUploadJob> &job);
	bool createTask(TaskFunction_t entry, const char *name, TaskHandle_t &outHandle);
	void stopTask(
	    TaskHandle_t &taskHandle, std::atomic<bool> &stopRequested, std::atomic<bool> &taskExited
	);
	static uint32_t stackBytesToWords(uint32_t stackBytes);

	// Refresh diag cache from filesystem (expensive; used only for explicit full refresh paths)
	void refreshDiagFromFs();
	DbStatus preloadCollectionsFromFs();

	// FreeRTOS task handles for autosync and async uploads
	TaskHandle_t _syncTask = nullptr;
	TaskHandle_t _fileUploadTask = nullptr;
	std::atomic<bool> _syncStopRequested{false};
	std::atomic<bool> _syncTaskExited{true};
	std::atomic<bool> _syncKickRequested{false};
	std::atomic<uint32_t> _syncRequestSeq{0};
	std::atomic<uint32_t> _syncCompletedSeq{0};
	std::map<std::string, bool> _pendingDelayedCollections;
	bool _delayedPreloadPhaseCompleted = true;
	bool _dropAllRequested = false;
	std::atomic<bool> _fileUploadStopRequested{false};
	std::atomic<bool> _fileUploadTaskExited{true};
	uint32_t _nextUploadId = 1;
	std::vector<uint32_t> _uploadQueue;
	std::map<uint32_t, std::shared_ptr<FileUploadJob>> _uploadJobs;
	std::vector<uint32_t> _terminalUploadOrder;
	static constexpr size_t kMaxRetainedTerminalUploads = 64;
};

template <typename Pred>
DbResult<size_t> ESPJsonDB::removeMany(const std::string &collectionName, Pred &&p) {
	DbResult<size_t> res{};
	auto cr = collection(collectionName);
	if (!cr.status.ok()) {
		res.status = cr.status;
		return res;
	}
	return cr.value->removeMany(std::forward<Pred>(p));
}

template <typename Pred, typename Mut, typename>
DbResult<size_t> ESPJsonDB::updateMany(const std::string &collectionName, Pred &&p, Mut &&m) {
	DbResult<size_t> res{};
	auto cr = collection(collectionName);
	if (!cr.status.ok()) {
		res.status = cr.status;
		return res;
	}
	return cr.value->updateMany(std::forward<Pred>(p), std::forward<Mut>(m));
}

template <typename Mut, typename>
DbResult<size_t> ESPJsonDB::updateMany(const std::string &collectionName, Mut &&m) {
	DbResult<size_t> res{};
	auto cr = collection(collectionName);
	if (!cr.status.ok()) {
		res.status = cr.status;
		return res;
	}
	return cr.value->updateMany(std::forward<Mut>(m));
}

template <typename Pred, typename>
DbResult<size_t>
ESPJsonDB::updateMany(const std::string &collectionName, const JsonDocument &patch, Pred &&p) {
	DbResult<size_t> res{};
	auto cr = collection(collectionName);
	if (!cr.status.ok()) {
		res.status = cr.status;
		return res;
	}
	return cr.value->updateMany(patch, std::forward<Pred>(p));
}
