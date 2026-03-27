#pragma once

#include "db.h"

struct DbRuntime {
	ESPJsonDB::CollectionMap cols;
	ESPJsonDB::SchemaMap schemas;
	ESPJsonDB::CollectionConfigMap collectionConfigs;
	ESPJsonDB::StringVector colsToDelete;
	ESPJsonDB::EventCallbackVector eventCbs;
	ESPJsonDB::ErrorCallbackVector errorCbs;
	ESPJsonDB::SyncStatusCallbackVector syncStatusCbs;
	ESPJsonDB::StringBoolMap pendingDelayedCollections;
	std::string baseDir;
	ESPJsonDBConfig cfg{};
	fs::FS *fs = &LittleFS;
	FrMutex mu;
	DbStatus lastError{DbStatusCode::Ok, ""};
	DBSyncStatus lastSyncStatus{};
	ESPJsonDB::DiagCache diagCache;
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
	std::unique_ptr<FileStoreImpl> fileStoreImpl;
	std::unique_ptr<FileStore> fileStore;

	explicit DbRuntime(bool usePSRAMBuffers = false)
	    : cols(std::less<std::string>{},
	           ESPJsonDB::CollectionMap::allocator_type(usePSRAMBuffers)),
	      schemas(std::less<std::string>{},
	              ESPJsonDB::SchemaMap::allocator_type(usePSRAMBuffers)),
	      collectionConfigs(
	          std::less<std::string>{},
	          ESPJsonDB::CollectionConfigMap::allocator_type(usePSRAMBuffers)
	      ),
	      colsToDelete(JsonDbAllocator<std::string>(usePSRAMBuffers)),
	      eventCbs(JsonDbAllocator<std::function<void(DBEventType)>>(usePSRAMBuffers)),
	      errorCbs(JsonDbAllocator<std::function<void(const DbStatus &)>>(usePSRAMBuffers)),
	      syncStatusCbs(
	          JsonDbAllocator<std::function<void(const DBSyncStatus &)>>(usePSRAMBuffers)
	      ),
	      pendingDelayedCollections(
	          std::less<std::string>{},
	          ESPJsonDB::StringBoolMap::allocator_type(usePSRAMBuffers)
	      ),
	      diagCache{
	          ESPJsonDB::StringUint32Map(
	              std::less<std::string>{},
	              ESPJsonDB::StringUint32Map::allocator_type(usePSRAMBuffers)
	          ),
	          0,
	          0} {
	}

	~DbRuntime();
};
