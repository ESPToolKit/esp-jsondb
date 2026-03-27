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
#include "files/file_store.h"
#include "utils/dbTypes.h"
#include "utils/fr_mutex.h"
#include "utils/jsondb_allocator.h"
#include "utils/schema.h"
#include <ArduinoJson.h>

struct DbRuntime;

class ESPJsonDB {
  public:
	ESPJsonDB();
	~ESPJsonDB();
	DbStatus init(const char *baseDir = "/db", const ESPJsonDBConfig &cfg = {});
	void deinit();
	bool isInitialized() const;
	DbStatus configureCollection(const std::string &name, const CollectionConfig &cfg);
	DbStatus registerSchema(const std::string &name, const Schema &s);
	DbStatus unregisterSchema(const std::string &name);
	DbStatus dropCollection(const std::string &name);

	// Drop all collections and documents (clears base directory)
	DbStatus dropAll();

	// Returns collection names tracked in memory (preloaded + runtime-created)
	std::vector<std::string> listCollectionNames();

	// Change sync configuration; restarts autosync task if needed
	DbStatus changeConfig(const ESPJsonDBConfig &cfg);

	// Register a generic DB event callback
	void onEvent(const std::function<void(DBEventType)> &cb);

	// Register callback for error notifications
	void onError(const std::function<void(const DbStatus &)> &cb);

	// Register callback for sync status transitions.
	void onSyncStatus(const std::function<void(const DBSyncStatus &)> &cb);

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
	DbStatus lastError() const;

	// Allow other components to update diagnostics/error state
	DbStatus recordStatus(const DbStatus &st);

	// Diagnostics: number of collections, doc counts, and config
	JsonDocument getDiagnostics();

	// Backup/restore
	JsonDocument getSnapshot(SnapshotMode mode = SnapshotMode::OnDiskOnly);
	DbStatus restoreFromSnapshot(const JsonDocument &snapshot);

	FileStore &files();
	const FileStore &files() const;

	// Emit an event to registered listeners
	void emitEvent(DBEventType ev);

	// Emit an error to registered listeners
	void emitError(const DbStatus &st);

	// Internal diagnostics counters used by Collection write paths.
	void noteDocumentCreated(const std::string &collectionName, uint32_t count = 1);
	void noteDocumentDeleted(const std::string &collectionName, uint32_t count = 1);

  private:
	using CollectionMap = JsonDbMap<std::string, std::unique_ptr<Collection>>;
	using SchemaMap = JsonDbMap<std::string, Schema>;
	using StringBoolMap = JsonDbMap<std::string, bool>;
	using CollectionConfigMap = JsonDbMap<std::string, CollectionConfig>;
	using StringUint32Map = JsonDbMap<std::string, uint32_t>;
	using StringVector = JsonDbVector<std::string>;
	using EventCallbackVector = JsonDbVector<std::function<void(DBEventType)>>;
	using ErrorCallbackVector = JsonDbVector<std::function<void(const DbStatus &)>>;
	using SyncStatusCallbackVector = JsonDbVector<std::function<void(const DBSyncStatus &)>>;

	struct DiagCache {
		StringUint32Map docsPerCollection;
		uint32_t collections = 0;
		uint32_t lastRefreshMs = 0; // millis when refreshed from FS
	};

	// sync task
	static void syncTaskThunk(void *arg);
	void syncTaskLoop();
	DbStatus runSyncPass();
	void startSyncTaskUnlocked();
	void stopSyncTaskUnlocked();

	// Update last error/status helper
	DbStatus setLastError(const DbStatus &st);
	void emitSyncStatus(const DBSyncStatus &status);
	void emitSyncStatus(
	    DBSyncStage stage,
	    DBSyncSource source,
	    const std::string &collectionName,
	    uint32_t collectionsCompleted,
	    uint32_t collectionsTotal,
	    const DbStatus &result
	);

	// fs helpers
	DbStatus ensureFsReady();
	DbStatus ensureReady() const;
	void rebindAllocatorAwareStateLocked(bool preserveData);
	DbStatus removeCollectionDir(const std::string &name);
	bool isReservedName(const std::string &name) const;
	std::string fileRootDir() const;
	void rebuildDelayedCollectionStateFromConfigLocked();
	DbStatus
	maybeRunDelayedPreload(bool triggeredByPeriodic, bool emitStatus, DBSyncSource statusSource);
	DbStatus preloadPendingDelayedCollectionsFromFs(bool emitStatus, DBSyncSource statusSource);
	DbStatus preloadCollectionFromFsByName(
	    const std::string &name, bool markDelayedHandled, bool *insertedOut = nullptr
	);
	bool collectionDirExistsOnFs(const std::string &name) const;
	bool createTask(TaskFunction_t entry, const char *name, TaskHandle_t &outHandle);
	void stopTask(
	    TaskHandle_t &taskHandle, std::atomic<bool> &stopRequested, std::atomic<bool> &taskExited
	);
	static uint32_t stackBytesToWords(uint32_t stackBytes);

	// Refresh diag cache from filesystem (expensive; used only for explicit full refresh paths)
	void refreshDiagFromFs();
	DbStatus preloadCollectionsFromFs(bool emitStatus, DBSyncSource statusSource);
	friend struct DbRuntime;
	friend struct FileStoreImpl;

	std::unique_ptr<DbRuntime> _rt;
	std::string &_baseDir;
	ESPJsonDBConfig &_cfg;
	CollectionMap &_cols;
	SchemaMap &_schemas;
	CollectionConfigMap &_collectionConfigs;
	StringVector &_colsToDelete;
	EventCallbackVector &_eventCbs;
	ErrorCallbackVector &_errorCbs;
	SyncStatusCallbackVector &_syncStatusCbs;
	fs::FS *&_fs;
	FrMutex &_mu;
	DbStatus &_lastError;
	DBSyncStatus &_lastSyncStatus;
	DiagCache &_diagCache;
	bool &_diagCachePrimed;
	std::atomic<bool> &_initialized;
	TaskHandle_t &_syncTask;
	std::atomic<bool> &_syncStopRequested;
	std::atomic<bool> &_syncTaskExited;
	std::atomic<bool> &_syncKickRequested;
	std::atomic<uint32_t> &_syncRequestSeq;
	std::atomic<uint32_t> &_syncCompletedSeq;
	StringBoolMap &_pendingDelayedCollections;
	bool &_delayedPreloadPhaseCompleted;
	bool &_dropAllRequested;
	std::unique_ptr<FileStore> &_fileStore;
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
