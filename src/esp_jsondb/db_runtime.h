#pragma once

#include <LittleFS.h>

#include <atomic>
#include <freertos/FreeRTOS.h>
#include <freertos/task.h>
#include <functional>
#include <map>
#include <memory>
#include <string>

#include "files/file_store.h"
#include "utils/dbTypes.h"
#include "utils/fr_mutex.h"
#include "utils/jsondb_allocator.h"
#include "utils/schema.h"

class Collection;
class ESPJsonDB;
struct FileStoreImpl;

struct DbRuntime {
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
		uint32_t lastRefreshMs = 0;
	};

	CollectionMap cols;
	SchemaMap schemas;
	CollectionConfigMap collectionConfigs;
	StringVector colsToDelete;
	EventCallbackVector eventCbs;
	ErrorCallbackVector errorCbs;
	SyncStatusCallbackVector syncStatusCbs;
	StringBoolMap pendingDelayedCollections;
	std::string baseDir;
	ESPJsonDBConfig cfg{};
	fs::FS *fs = &LittleFS;
	FrMutex mu;
	DbStatus lastError{DbStatusCode::Ok, ""};
	DBSyncStatus lastSyncStatus{};
	DiagCache diagCache;
	bool diagCachePrimed = false;
	std::atomic<bool> initialized{false};
	TaskHandle_t syncTask = nullptr;
	std::atomic<bool> syncStopRequested{false};
	std::atomic<bool> syncTaskExited{true};
	std::atomic<bool> syncKickRequested{false};
	std::atomic<uint32_t> syncRequestSeq{0};
	std::atomic<uint32_t> syncCompletedSeq{0};
	bool delayedPreloadPhaseCompleted = true;
	bool dropAllRequested = false;
	ESPJsonDB *owner = nullptr;
	std::unique_ptr<FileStoreImpl> fileStoreImpl;
	std::unique_ptr<FileStore> fileStore;

	explicit DbRuntime(bool usePSRAMBuffers = false);
	~DbRuntime();

	DbStatus ensureReady() const;
	DbStatus recordStatus(const DbStatus &st);
	void emitEvent(DBEventType ev);
	void emitError(const DbStatus &st);
	void noteDocumentCreated(const std::string &collectionName, uint32_t count = 1);
	void noteDocumentDeleted(const std::string &collectionName, uint32_t count = 1);
	std::string fileRootDir() const;
	bool createTask(TaskFunction_t entry, const char *name, void *arg, TaskHandle_t &outHandle);
	void stopTask(
	    TaskHandle_t &taskHandle, std::atomic<bool> &stopRequested, std::atomic<bool> &taskExited
	);
	static uint32_t stackBytesToWords(uint32_t stackBytes);
};
