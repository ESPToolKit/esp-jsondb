#include "db.h"
#include "db_runtime.h"
#include "files/file_store_impl.h"
#include "utils/fs_utils.h"
#include "utils/jsondb_allocator.h"
#include "utils/time_utils.h"
#include <StreamUtils.h>
#include <algorithm>
#include <cstring>

namespace {
constexpr uint32_t kTaskStopTimeoutMs = 200;
static DbStatus removeTree(fs::FS &fsImpl, const std::string &path);
using DirEntry = std::pair<std::string, bool>;
using DirEntryVector = JsonDbVector<DirEntry>;

template <typename Callback, typename... Args>
void invokeJsonDbCallback(const Callback &callback, Args... args) noexcept {
	if (!callback) {
		return;
	}

#if defined(__cpp_exceptions)
	try {
		callback(args...);
	} catch (...) {
	}
#else
	callback(args...);
#endif
}
} // namespace

FrMutex g_fsMutex; // definition of global FS mutex

DbRuntime::DbRuntime(bool usePSRAMBuffers)
    : cols(std::less<std::string>{}, DbRuntime::CollectionMap::allocator_type(usePSRAMBuffers)),
      schemas(std::less<std::string>{}, DbRuntime::SchemaMap::allocator_type(usePSRAMBuffers)),
      collectionConfigs(
          std::less<std::string>{}, DbRuntime::CollectionConfigMap::allocator_type(usePSRAMBuffers)
      ),
      colsToDelete(JsonDbAllocator<std::string>(usePSRAMBuffers)),
      eventCbs(JsonDbAllocator<std::function<void(DBEventType)>>(usePSRAMBuffers)),
      errorCbs(JsonDbAllocator<std::function<void(const DbStatus &)>>(usePSRAMBuffers)),
      syncStatusCbs(JsonDbAllocator<std::function<void(const DBSyncStatus &)>>(usePSRAMBuffers)),
      pendingDelayedCollections(
          std::less<std::string>{}, DbRuntime::StringBoolMap::allocator_type(usePSRAMBuffers)
      ),
      diagCache{
          DbRuntime::StringUint32Map(
              std::less<std::string>{}, DbRuntime::StringUint32Map::allocator_type(usePSRAMBuffers)
          ),
          0,
          0
      } {
}

DbRuntime::~DbRuntime() = default;

uint32_t DbRuntime::stackBytesToWords(uint32_t stackBytes) {
	const uint32_t wordSize = static_cast<uint32_t>(sizeof(StackType_t));
	return (stackBytes + wordSize - 1U) / wordSize;
}

DbStatus DbRuntime::ensureReady() const {
	if (!initialized.load(std::memory_order_acquire) || !fs || baseDir.empty()) {
		return {DbStatusCode::NotInitialized, "database not initialized"};
	}
	return {DbStatusCode::Ok, ""};
}

void DbRuntime::emitError(const DbStatus &st) {
	DbRuntime::ErrorCallbackVector callbacks{
	    JsonDbAllocator<std::function<void(const DbStatus &)>>(cfg.usePSRAMBuffers)
	};
	{
		FrLock lk(mu);
		callbacks.reserve(errorCbs.size());
		for (auto &cb : errorCbs) {
			callbacks.push_back(cb);
		}
	}
	for (const auto &cb : callbacks) {
		if (cb)
			invokeJsonDbCallback(cb, st);
	}
}

DbStatus DbRuntime::recordStatus(const DbStatus &st) {
	lastError = st;
	if (!st.ok()) {
		emitError(st);
	}
	return st;
}

void DbRuntime::emitEvent(DBEventType ev) {
	DbRuntime::EventCallbackVector callbacks{
	    JsonDbAllocator<std::function<void(DBEventType)>>(cfg.usePSRAMBuffers)
	};
	{
		FrLock lk(mu);
		callbacks.reserve(eventCbs.size());
		for (auto &cb : eventCbs) {
			callbacks.push_back(cb);
		}
	}
	for (const auto &cb : callbacks) {
		if (cb)
			invokeJsonDbCallback(cb, ev);
	}
}

void DbRuntime::noteDocumentCreated(const std::string &collectionName, uint32_t count) {
	if (collectionName.empty() || count == 0)
		return;
	FrLock lk(mu);
	if (!diagCachePrimed)
		return;
	uint32_t &docs = diagCache.docsPerCollection[collectionName];
	if (docs == 0) {
		++diagCache.collections;
	}
	docs += count;
	diagCache.lastRefreshMs = millis();
}

void DbRuntime::noteDocumentDeleted(const std::string &collectionName, uint32_t count) {
	if (collectionName.empty() || count == 0)
		return;
	FrLock lk(mu);
	if (!diagCachePrimed)
		return;
	auto it = diagCache.docsPerCollection.find(collectionName);
	if (it == diagCache.docsPerCollection.end())
		return;
	if (it->second <= count) {
		diagCache.docsPerCollection.erase(it);
		if (diagCache.collections > 0)
			--diagCache.collections;
	} else {
		it->second -= count;
	}
	diagCache.lastRefreshMs = millis();
}

std::string DbRuntime::fileRootDir() const {
	return joinPath(baseDir, "_files");
}

bool DbRuntime::createTask(
    TaskFunction_t entry, const char *name, void *arg, TaskHandle_t &outHandle
) {
	const uint32_t stackDepthWords = stackBytesToWords(cfg.stackSize);
	BaseType_t rc = xTaskCreatePinnedToCore(
	    entry,
	    name,
	    stackDepthWords,
	    arg,
	    cfg.priority,
	    &outHandle,
	    cfg.coreId
	);
	return rc == pdPASS;
}

void DbRuntime::stopTask(
    TaskHandle_t &taskHandle, std::atomic<bool> &stopRequested, std::atomic<bool> &taskExited
) {
	if (taskHandle == nullptr)
		return;
	stopRequested.store(true, std::memory_order_release);
	const uint32_t startMs = millis();
	while (!taskExited.load(std::memory_order_acquire)) {
		if ((millis() - startMs) >= kTaskStopTimeoutMs) {
			break;
		}
		vTaskDelay(pdMS_TO_TICKS(1));
	}
	if (!taskExited.load(std::memory_order_acquire)) {
		vTaskDelete(taskHandle);
		taskExited.store(true, std::memory_order_release);
	}
	taskHandle = nullptr;
}

ESPJsonDB::ESPJsonDB() : _rt(std::make_unique<DbRuntime>()) {
	_rt->owner = this;
	_rt->fileStoreImpl = std::make_unique<FileStoreImpl>(*_rt);
	_rt->fileStore = std::make_unique<FileStore>(_rt->fileStoreImpl.get());
}

#define _baseDir (_rt->baseDir)
#define _cfg (_rt->cfg)
#define _cols (_rt->cols)
#define _schemas (_rt->schemas)
#define _collectionConfigs (_rt->collectionConfigs)
#define _colsToDelete (_rt->colsToDelete)
#define _eventCbs (_rt->eventCbs)
#define _errorCbs (_rt->errorCbs)
#define _syncStatusCbs (_rt->syncStatusCbs)
#define _fs (_rt->fs)
#define _mu (_rt->mu)
#define _lastError (_rt->lastError)
#define _lastSyncStatus (_rt->lastSyncStatus)
#define _diagCache (_rt->diagCache)
#define _diagCachePrimed (_rt->diagCachePrimed)
#define _initialized (_rt->initialized)
#define _syncTask (_rt->syncTask)
#define _syncStopRequested (_rt->syncStopRequested)
#define _syncTaskExited (_rt->syncTaskExited)
#define _syncKickRequested (_rt->syncKickRequested)
#define _syncRequestSeq (_rt->syncRequestSeq)
#define _syncCompletedSeq (_rt->syncCompletedSeq)
#define _pendingDelayedCollections (_rt->pendingDelayedCollections)
#define _delayedPreloadPhaseCompleted (_rt->delayedPreloadPhaseCompleted)
#define _dropAllRequested (_rt->dropAllRequested)
#define _fileStore (_rt->fileStore)

bool ESPJsonDB::isInitialized() const {
	return _initialized.load(std::memory_order_acquire);
}

DbStatus ESPJsonDB::lastError() const {
	return _lastError;
}

DbStatus ESPJsonDB::recordStatus(const DbStatus &st) {
	return _rt->recordStatus(st);
}

DbStatus ESPJsonDB::setLastError(const DbStatus &st) {
	return _rt->recordStatus(st);
}

FileStore &ESPJsonDB::files() {
	return *_fileStore;
}

const FileStore &ESPJsonDB::files() const {
	return *_fileStore;
}

DbStatus ESPJsonDB::ensureFsReady() {
	_fs = _cfg.fs ? _cfg.fs : &LittleFS;
	if (!_fs) {
		return _rt->recordStatus({DbStatusCode::InvalidArgument, "filesystem handle is null"});
	}
	if (_cfg.initFileSystem && _fs == &LittleFS) {
		if (!LittleFS
		         .begin(_cfg.formatOnFail, "/littlefs", _cfg.maxOpenFiles, _cfg.partitionLabel)) {
			return _rt->recordStatus({DbStatusCode::IoError, "LittleFS.begin failed"});
		}
	}
	if (!fsEnsureDir(*_fs, _baseDir)) {
		return _rt->recordStatus({DbStatusCode::IoError, "mkdir baseDir failed"});
	}
	if (!fsEnsureDir(*_fs, fileRootDir())) {
		return _rt->recordStatus({DbStatusCode::IoError, "mkdir file root failed"});
	}
	return _rt->recordStatus({DbStatusCode::Ok, ""});
}

DbStatus ESPJsonDB::ensureReady() const {
	return _rt->ensureReady();
}

void ESPJsonDB::rebindAllocatorAwareStateLocked(bool preserveData) {
	const bool usePSRAMBuffers = _cfg.usePSRAMBuffers;

	auto oldCols = std::move(_cols);
	auto oldSchemas = std::move(_schemas);
	auto oldCollectionConfigs = std::move(_collectionConfigs);
	auto oldColsToDelete = std::move(_colsToDelete);
	auto oldEventCbs = std::move(_eventCbs);
	auto oldErrorCbs = std::move(_errorCbs);
	auto oldSyncStatusCbs = std::move(_syncStatusCbs);
	auto oldPendingDelayed = std::move(_pendingDelayedCollections);
	auto oldDiagDocs = std::move(_diagCache.docsPerCollection);
	const DBSyncStatus oldLastSyncStatus = _lastSyncStatus;

	DbRuntime::CollectionMap newCols{
	    std::less<std::string>{},
	    DbRuntime::CollectionMap::allocator_type(usePSRAMBuffers)
	};
	DbRuntime::SchemaMap newSchemas{
	    std::less<std::string>{},
	    DbRuntime::SchemaMap::allocator_type(usePSRAMBuffers)
	};
	DbRuntime::CollectionConfigMap newCollectionConfigs{
	    std::less<std::string>{},
	    DbRuntime::CollectionConfigMap::allocator_type(usePSRAMBuffers)
	};
	DbRuntime::StringVector newColsToDelete{JsonDbAllocator<std::string>(usePSRAMBuffers)};
	DbRuntime::EventCallbackVector newEventCbs{
	    JsonDbAllocator<std::function<void(DBEventType)>>(usePSRAMBuffers)
	};
	DbRuntime::ErrorCallbackVector newErrorCbs{
	    JsonDbAllocator<std::function<void(const DbStatus &)>>(usePSRAMBuffers)
	};
	DbRuntime::SyncStatusCallbackVector newSyncStatusCbs{
	    JsonDbAllocator<std::function<void(const DBSyncStatus &)>>(usePSRAMBuffers)
	};
	DbRuntime::StringBoolMap newPendingDelayed{
	    std::less<std::string>{},
	    DbRuntime::StringBoolMap::allocator_type(usePSRAMBuffers)
	};
	DbRuntime::StringUint32Map newDiagDocs{
	    std::less<std::string>{},
	    DbRuntime::StringUint32Map::allocator_type(usePSRAMBuffers)
	};

	if (preserveData) {
		newColsToDelete.reserve(oldColsToDelete.size());
		newEventCbs.reserve(oldEventCbs.size());
		newErrorCbs.reserve(oldErrorCbs.size());
		newSyncStatusCbs.reserve(oldSyncStatusCbs.size());

		for (auto &kv : oldCols) {
			newCols.emplace(kv.first, std::move(kv.second));
		}
		for (auto &kv : oldSchemas) {
			newSchemas.emplace(kv.first, kv.second);
		}
		for (auto &kv : oldCollectionConfigs) {
			newCollectionConfigs.emplace(kv.first, kv.second);
		}
		for (auto &name : oldColsToDelete) {
			newColsToDelete.push_back(std::move(name));
		}
		for (auto &cb : oldEventCbs) {
			newEventCbs.push_back(std::move(cb));
		}
		for (auto &cb : oldErrorCbs) {
			newErrorCbs.push_back(std::move(cb));
		}
		for (auto &cb : oldSyncStatusCbs) {
			newSyncStatusCbs.push_back(std::move(cb));
		}
		for (auto &kv : oldPendingDelayed) {
			newPendingDelayed.emplace(kv.first, kv.second);
		}
		for (auto &kv : oldDiagDocs) {
			newDiagDocs.emplace(kv.first, kv.second);
		}
	}

	_cols = std::move(newCols);
	_schemas = std::move(newSchemas);
	_collectionConfigs = std::move(newCollectionConfigs);
	_colsToDelete = std::move(newColsToDelete);
	_eventCbs = std::move(newEventCbs);
	_errorCbs = std::move(newErrorCbs);
	_syncStatusCbs = std::move(newSyncStatusCbs);
	_pendingDelayedCollections = std::move(newPendingDelayed);
	_diagCache.docsPerCollection = std::move(newDiagDocs);
	if (preserveData) {
		_lastSyncStatus = oldLastSyncStatus;
	} else {
		_lastSyncStatus = {DBSyncStage::Idle, DBSyncSource::Init, "", 0, 0, {DbStatusCode::Ok, ""}};
	}
}

bool ESPJsonDB::createTask(TaskFunction_t entry, const char *name, TaskHandle_t &outHandle) {
	return _rt->createTask(entry, name, this, outHandle);
}

void ESPJsonDB::stopTask(
    TaskHandle_t &taskHandle, std::atomic<bool> &stopRequested, std::atomic<bool> &taskExited
) {
	_rt->stopTask(taskHandle, stopRequested, taskExited);
}

bool ESPJsonDB::isReservedName(const std::string &name) const {
	return name == "_files";
}

std::string ESPJsonDB::fileRootDir() const {
	return _rt->fileRootDir();
}

void ESPJsonDB::rebuildDelayedCollectionStateFromConfigLocked() {
	_pendingDelayedCollections.clear();
	for (const auto &kv : _collectionConfigs) {
		if (kv.first.empty() || isReservedName(kv.first))
			continue;
		if (kv.second.loadPolicy == CollectionLoadPolicy::Delayed) {
			_pendingDelayedCollections[kv.first] = true;
		}
	}
	_delayedPreloadPhaseCompleted = _pendingDelayedCollections.empty();
}

bool ESPJsonDB::collectionDirExistsOnFs(const std::string &name) const {
	if (!_fs || name.empty())
		return false;
	const std::string dirPath = joinPath(_baseDir, name);
	FrLock fs(g_fsMutex);
	if (!_fs->exists(dirPath.c_str()))
		return false;
	File dir = _fs->open(dirPath.c_str());
	const bool exists = dir && dir.isDirectory();
	if (dir)
		dir.close();
	return exists;
}

DbStatus ESPJsonDB::preloadCollectionFromFsByName(
    const std::string &name, bool markDelayedHandled, bool *insertedOut
) {
	if (insertedOut)
		*insertedOut = false;
	if (name.empty() || isReservedName(name)) {
		return {DbStatusCode::Ok, ""};
	}

	Schema sc{};
	CollectionConfig collectionCfg{};
	{
		FrLock lk(_mu);
		auto it = _cols.find(name);
		if (it != _cols.end()) {
			if (markDelayedHandled) {
				_pendingDelayedCollections.erase(name);
				if (_pendingDelayedCollections.empty())
					_delayedPreloadPhaseCompleted = true;
			}
			return {DbStatusCode::Ok, ""};
		}
		auto sit = _schemas.find(name);
		if (sit != _schemas.end())
			sc = sit->second;
		auto cit = _collectionConfigs.find(name);
		if (cit != _collectionConfigs.end()) {
			collectionCfg = cit->second;
		} else {
			collectionCfg.loadPolicy = _cfg.defaultLoadPolicy;
		}
	}

	auto col = std::make_unique<
	    Collection>(*_rt, name, sc, _baseDir, collectionCfg, _cfg.usePSRAMBuffers, *_fs);
	auto st = col->loadFromFs(_baseDir);
	if (!st.ok())
		return st;

	bool inserted = false;
	{
		FrLock lk(_mu);
		auto [it, didInsert] = _cols.emplace(name, std::move(col));
		(void)it;
		inserted = didInsert;
		if (markDelayedHandled) {
			_pendingDelayedCollections.erase(name);
			if (_pendingDelayedCollections.empty())
				_delayedPreloadPhaseCompleted = true;
		}
	}
	if (insertedOut)
		*insertedOut = inserted;

	return {DbStatusCode::Ok, ""};
}

DbStatus
ESPJsonDB::preloadPendingDelayedCollectionsFromFs(bool emitStatus, DBSyncSource statusSource) {
	auto ready = ensureReady();
	if (!ready.ok())
		return ready;
	if (!_fs) {
		return {DbStatusCode::IoError, "filesystem not ready"};
	}

	JsonDbVector<std::string> names{JsonDbAllocator<std::string>(_cfg.usePSRAMBuffers)};
	{
		FrLock lk(_mu);
		names.reserve(_pendingDelayedCollections.size());
		for (const auto &kv : _pendingDelayedCollections) {
			names.push_back(kv.first);
		}
	}

	const uint32_t total = static_cast<uint32_t>(names.size());
	uint32_t completed = 0;
	if (emitStatus) {
		emitSyncStatus(
		    DBSyncStage::ColdSyncStarted,
		    statusSource,
		    "",
		    completed,
		    total,
		    {DbStatusCode::Ok, ""}
		);
	}

	for (const auto &name : names) {
		// Only preload existing on-disk collections in the background phase.
		if (!collectionDirExistsOnFs(name)) {
			FrLock lk(_mu);
			_pendingDelayedCollections.erase(name);
			if (_pendingDelayedCollections.empty())
				_delayedPreloadPhaseCompleted = true;
			++completed;
			continue;
		}
		if (emitStatus) {
			emitSyncStatus(
			    DBSyncStage::ColdSyncCollectionStarted,
			    statusSource,
			    name,
			    completed,
			    total,
			    {DbStatusCode::Ok, ""}
			);
		}
		auto st = preloadCollectionFromFsByName(name, true);
		if (!st.ok()) {
			if (emitStatus) {
				emitSyncStatus(DBSyncStage::SyncFailed, statusSource, name, completed, total, st);
			}
			return st;
		}
		++completed;
		if (emitStatus) {
			emitSyncStatus(
			    DBSyncStage::ColdSyncCollectionCompleted,
			    statusSource,
			    name,
			    completed,
			    total,
			    {DbStatusCode::Ok, ""}
			);
		}
	}

	if (emitStatus) {
		emitSyncStatus(
		    DBSyncStage::ColdSyncCompleted,
		    statusSource,
		    "",
		    completed,
		    total,
		    {DbStatusCode::Ok, ""}
		);
	}

	return {DbStatusCode::Ok, ""};
}

DbStatus ESPJsonDB::maybeRunDelayedPreload(
    bool triggeredByPeriodic, bool emitStatus, DBSyncSource statusSource
) {
	bool shouldRun = false;
	{
		FrLock lk(_mu);
		if (_delayedPreloadPhaseCompleted)
			return {DbStatusCode::Ok, ""};
		if (_cfg.autosync) {
			shouldRun = triggeredByPeriodic;
		} else {
			shouldRun = true;
		}
	}
	if (!shouldRun)
		return {DbStatusCode::Ok, ""};

	auto st = preloadPendingDelayedCollectionsFromFs(emitStatus, statusSource);
	if (!st.ok())
		return st;

	{
		FrLock lk(_mu);
		if (_pendingDelayedCollections.empty()) {
			_delayedPreloadPhaseCompleted = true;
		}
	}
	return {DbStatusCode::Ok, ""};
}

ESPJsonDB::~ESPJsonDB() {
	deinit();
}

void ESPJsonDB::deinit() {
	if (!isInitialized() && _syncTask == nullptr) {
		return;
	}
	_initialized.store(false, std::memory_order_release);

	{
		FrLock lk(_mu);
		if (_rt->fileStoreImpl)
			_rt->fileStoreImpl->stopTask(true);
		stopSyncTaskUnlocked();

		for (auto &kv : _cols) {
			if (kv.second)
				kv.second->markAllRemoved();
		}
		_cols.clear();
		_schemas.clear();
		_colsToDelete.clear();
		_eventCbs.clear();
		_errorCbs.clear();
		_syncStatusCbs.clear();
		_pendingDelayedCollections.clear();
		_delayedPreloadPhaseCompleted = true;
		_diagCache.docsPerCollection.clear();
		_diagCache.collections = 0;
		_diagCache.lastRefreshMs = 0;
		_diagCachePrimed = false;
		_lastSyncStatus = {DBSyncStage::Idle, DBSyncSource::Init, "", 0, 0, {DbStatusCode::Ok, ""}};
	}

	_syncStopRequested.store(false, std::memory_order_release);
	_syncTaskExited.store(true, std::memory_order_release);
	_syncKickRequested.store(false, std::memory_order_release);
	_syncRequestSeq.store(0, std::memory_order_release);
	_syncCompletedSeq.store(0, std::memory_order_release);
	_dropAllRequested = false;
	_baseDir.clear();
	_cfg = ESPJsonDBConfig{};
	_fs = _cfg.fs ? _cfg.fs : &LittleFS;
	_lastError = {DbStatusCode::Ok, ""};
	_initialized.store(false, std::memory_order_release);
}

DbStatus ESPJsonDB::init(const char *baseDir, const ESPJsonDBConfig &cfg) {
	if (isInitialized()) {
		deinit();
	}
	_initialized.store(false, std::memory_order_release);
	_baseDir = baseDir ? baseDir : std::string("/db");
	// Normalize baseDir: ensure leading '/', drop trailing '/'
	if (_baseDir.empty()) {
		_baseDir = "/db";
	}
	if (_baseDir.front() != '/') {
		_baseDir.insert(_baseDir.begin(), '/');
	}
	if (_baseDir.size() > 1 && _baseDir.back() == '/') {
		_baseDir.pop_back();
	}
	_cfg = cfg;
	_syncStopRequested.store(false, std::memory_order_release);
	_syncKickRequested.store(false, std::memory_order_release);
	_syncRequestSeq.store(0, std::memory_order_release);
	_syncCompletedSeq.store(0, std::memory_order_release);
	{
		FrLock lk(_mu);
		_lastSyncStatus = {DBSyncStage::Idle, DBSyncSource::Init, "", 0, 0, {DbStatusCode::Ok, ""}};
	}
	auto st = ensureFsReady();
	if (!st.ok())
		return st;

	{
		FrLock lk(_mu);
		if (_rt->fileStoreImpl)
			_rt->fileStoreImpl->stopTask(true);
		rebindAllocatorAwareStateLocked(true);
		_cols.clear();
		_colsToDelete.clear();
		_rt->fileStoreImpl = std::make_unique<FileStoreImpl>(*_rt);
		_fileStore = std::make_unique<FileStore>(_rt->fileStoreImpl.get());
		rebuildDelayedCollectionStateFromConfigLocked();
		_dropAllRequested = false;
		_diagCache.docsPerCollection.clear();
		_diagCache.collections = 0;
		_diagCache.lastRefreshMs = 0;
		_diagCachePrimed = true;
	}
	_initialized.store(true, std::memory_order_release);

	{
		FrLock lk(_mu);
		startSyncTaskUnlocked();
	}
	if (_syncTask == nullptr) {
		deinit();
		return setLastError({DbStatusCode::Busy, "sync task start failed"});
	}

	auto preloadStatus = preloadCollectionsFromFs(true, DBSyncSource::Init);
	if (!preloadStatus.ok()) {
		deinit();
		return setLastError(preloadStatus);
	}
	return setLastError({DbStatusCode::Ok, ""});
}

DbStatus ESPJsonDB::configureCollection(const std::string &name, const CollectionConfig &cfg) {
	if (name.empty() || isReservedName(name)) {
		return setLastError({DbStatusCode::InvalidArgument, "reserved collection name"});
	}
	FrLock lk(_mu);
	_collectionConfigs[name] = cfg;
	auto it = _cols.find(name);
	if (it != _cols.end() && it->second) {
		it->second->setConfig(cfg);
	}
	rebuildDelayedCollectionStateFromConfigLocked();
	return setLastError({DbStatusCode::Ok, ""});
}

DbStatus ESPJsonDB::registerSchema(const std::string &name, const Schema &s) {
	if (isReservedName(name)) {
		return setLastError({DbStatusCode::InvalidArgument, "reserved collection name"});
	}
	FrLock lk(_mu);
	_schemas[name] = s;
	auto it = _cols.find(name);
	if (it != _cols.end() && it->second) {
		it->second->setSchema(s);
	}
	return setLastError({DbStatusCode::Ok, ""});
}

DbStatus ESPJsonDB::unregisterSchema(const std::string &name) {
	if (isReservedName(name)) {
		return setLastError({DbStatusCode::InvalidArgument, "reserved collection name"});
	}
	FrLock lk(_mu);
	_schemas.erase(name);
	auto it = _cols.find(name);
	if (it != _cols.end() && it->second) {
		it->second->setSchema(Schema{});
	}
	return setLastError({DbStatusCode::Ok, ""});
}

void ESPJsonDB::onEvent(const std::function<void(DBEventType)> &cb) {
	FrLock lk(_mu);
	_eventCbs.push_back(cb);
}

void ESPJsonDB::onError(const std::function<void(const DbStatus &)> &cb) {
	FrLock lk(_mu);
	_errorCbs.push_back(cb);
}

void ESPJsonDB::onSyncStatus(const std::function<void(const DBSyncStatus &)> &cb) {
	if (!cb)
		return;
	DBSyncStatus snapshot;
	{
		FrLock lk(_mu);
		_syncStatusCbs.push_back(cb);
		snapshot = _lastSyncStatus;
	}
	invokeJsonDbCallback(cb, snapshot);
}

DbStatus ESPJsonDB::dropCollection(const std::string &name) {
	auto ready = ensureReady();
	if (!ready.ok()) {
		return setLastError(ready);
	}
	if (isReservedName(name)) {
		return setLastError({DbStatusCode::InvalidArgument, "reserved collection name"});
	}
	FrLock lk(_mu);
	bool removed = false;
	auto it = _cols.find(name);
	if (it != _cols.end()) {
		if (it->second) {
			// Mark all docs removed to invalidate outstanding views safely
			it->second->markAllRemoved();
		}
		_cols.erase(it);
		removed = true;
	} else {
		auto delayedIt = _pendingDelayedCollections.find(name);
		if (delayedIt != _pendingDelayedCollections.end()) {
			_pendingDelayedCollections.erase(delayedIt);
			if (_pendingDelayedCollections.empty())
				_delayedPreloadPhaseCompleted = true;
			removed = true;
		}
	}
	if (!removed) {
		return setLastError({DbStatusCode::Ok, ""});
	}
	// Update diag cache immediately to avoid reporting stale collections
	if (_diagCachePrimed) {
		auto dit = _diagCache.docsPerCollection.find(name);
		if (dit != _diagCache.docsPerCollection.end()) {
			_diagCache.docsPerCollection.erase(dit);
			if (_diagCache.collections > 0)
				--_diagCache.collections;
		}
		_diagCache.lastRefreshMs = millis();
	}
	// schedule directory removal on next sync
	_colsToDelete.push_back(name);
	// emit event outside lock to avoid callbacks under lock
	// Defer actual emit after releasing lock
	// Note: copying name not needed here; just emit generic event
	// We'll emit after lock scope ends
	// (fallthrough)
	return setLastError({DbStatusCode::Ok, ""});
}

DbResult<Collection *> ESPJsonDB::collection(const std::string &name) {
	DbResult<Collection *> res{};
	auto ready = ensureReady();
	if (!ready.ok()) {
		res.status = setLastError(ready);
		return res;
	}
	if (isReservedName(name)) {
		res.status = setLastError({DbStatusCode::InvalidArgument, "reserved collection name"});
		return res;
	}
	bool pendingDelayed = false;
	CollectionConfig collectionCfg{};
	{
		FrLock lk(_mu);
		auto it = _cols.find(name);
		if (it != _cols.end()) {
			res.status = {DbStatusCode::Ok, ""};
			res.value = it->second.get();
			return res;
		}
		pendingDelayed = _pendingDelayedCollections.find(name) != _pendingDelayedCollections.end();
		auto cit = _collectionConfigs.find(name);
		if (cit != _collectionConfigs.end()) {
			collectionCfg = cit->second;
		} else {
			collectionCfg.loadPolicy = _cfg.defaultLoadPolicy;
		}
	}

	if (pendingDelayed ||
	    (collectionCfg.loadPolicy == CollectionLoadPolicy::Lazy && collectionDirExistsOnFs(name))) {
		const bool existedOnFs = collectionDirExistsOnFs(name);
		bool inserted = false;
		auto preloadStatus = preloadCollectionFromFsByName(name, true, &inserted);
		if (!preloadStatus.ok()) {
			res.status = setLastError(preloadStatus);
			return res;
		}
		{
			FrLock lk(_mu);
			auto it = _cols.find(name);
			if (it != _cols.end()) {
				res.status = setLastError({DbStatusCode::Ok, ""});
				res.value = it->second.get();
			} else {
				res.status = setLastError({DbStatusCode::Unknown, "collection preload failed"});
			}
		}
		if (res.status.ok() && inserted && !existedOnFs)
			emitEvent(DBEventType::CollectionCreated);
		return res;
	}
	Schema sc{};
	collectionCfg = {};
	{
		FrLock lk(_mu);
		auto existing = _cols.find(name);
		if (existing != _cols.end()) {
			res.status = {DbStatusCode::Ok, ""};
			res.value = existing->second.get();
			return res;
		}
		auto sit = _schemas.find(name);
		if (sit != _schemas.end())
			sc = sit->second;
		auto cit = _collectionConfigs.find(name);
		if (cit != _collectionConfigs.end()) {
			collectionCfg = cit->second;
		} else {
			collectionCfg.loadPolicy = _cfg.defaultLoadPolicy;
		}
	}
	auto col = std::make_unique<
	    Collection>(*_rt, name, sc, _baseDir, collectionCfg, _cfg.usePSRAMBuffers, *_fs);
	Collection *ptr = nullptr;
	bool created = false;
	{
		FrLock lk(_mu);
		auto [it, inserted] = _cols.emplace(name, std::move(col));
		created = inserted;
		ptr = it->second.get();
	}
	if (created)
		emitEvent(DBEventType::CollectionCreated);
	res.status = setLastError({DbStatusCode::Ok, ""});
	res.value = ptr;
	return res;
}

// Arduino-friendly overload
DbResult<Collection *> ESPJsonDB::collection(const String &name) {
	return collection(std::string{name.c_str()});
}

DbResult<Collection *> ESPJsonDB::collection(const char *name) {
	return collection(std::string{name ? name : ""});
}

DbResult<std::string> ESPJsonDB::create(const std::string &name, JsonObjectConst doc) {
	DbResult<std::string> res{};
	auto cr = collection(name);
	if (!cr.status.ok()) {
		res.status = cr.status;
		return res;
	}
	return cr.value->create(doc);
}

DbResult<std::string> ESPJsonDB::create(const std::string &name, const JsonDocument &doc) {
	// Ensure the provided document is a JSON object (not an array/scalar)
	if (!doc.is<JsonObject>()) {
		DbResult<std::string> res{};
		res.status = setLastError({DbStatusCode::InvalidArgument, "document must be an object"});
		return res;
	}
	return create(name, doc.as<JsonObjectConst>());
}

DbResult<std::vector<std::string>>
ESPJsonDB::createMany(const std::string &name, JsonArrayConst arr) {
	DbResult<std::vector<std::string>> res{};
	auto cr = collection(name);
	if (!cr.status.ok()) {
		res.status = cr.status;
		return res;
	}
	return cr.value->createMany(arr);
}

DbResult<std::vector<std::string>>
ESPJsonDB::createMany(const std::string &name, const JsonDocument &arrDoc) {
	if (!arrDoc.is<JsonArray>()) {
		DbResult<std::vector<std::string>> res{};
		res.status =
		    setLastError({DbStatusCode::InvalidArgument, "document must be an array of objects"});
		return res;
	}
	return createMany(name, arrDoc.as<JsonArrayConst>());
}

DbResult<DocView> ESPJsonDB::findById(const std::string &name, const std::string &id) {
	auto cr = collection(name);
	if (!cr.status.ok()) {
		// Return placeholder DocView; caller should check status before use
		return {
		    cr.status,
		    DocView(
		        nullptr,
		        nullptr,
		        nullptr,
		        this,
		        nullptr,
		        nullptr,
		        nullptr,
		        nullptr,
		        nullptr,
		        false,
		        _cfg.usePSRAMBuffers
		    )
		};
	}
	return cr.value->findById(id);
}

DbResult<std::vector<DocView>>
ESPJsonDB::findMany(const std::string &name, std::function<bool(const DocView &)> pred) {
	DbResult<std::vector<DocView>> res{};
	auto cr = collection(name);
	if (!cr.status.ok()) {
		res.status = cr.status;
		return res;
	}
	return cr.value->findMany(std::move(pred));
}

DbResult<DocView>
ESPJsonDB::findOne(const std::string &name, std::function<bool(const DocView &)> pred) {
	auto cr = collection(name);
	if (!cr.status.ok()) {
		// Return placeholder DocView; caller should check status before use
		return {
		    cr.status,
		    DocView(
		        nullptr,
		        nullptr,
		        nullptr,
		        this,
		        nullptr,
		        nullptr,
		        nullptr,
		        nullptr,
		        nullptr,
		        false,
		        _cfg.usePSRAMBuffers
		    )
		};
	}
	return cr.value->findOne(std::move(pred));
}

DbResult<DocView> ESPJsonDB::findOne(const std::string &name, const JsonDocument &filter) {
	auto cr = collection(name);
	if (!cr.status.ok()) {
		// Return placeholder DocView; caller should check status before use
		return {
		    cr.status,
		    DocView(
		        nullptr,
		        nullptr,
		        nullptr,
		        this,
		        nullptr,
		        nullptr,
		        nullptr,
		        nullptr,
		        nullptr,
		        false,
		        _cfg.usePSRAMBuffers
		    )
		};
	}
	return cr.value->findOne(filter);
}

DbStatus ESPJsonDB::updateOne(
    const std::string &name,
    std::function<bool(const DocView &)> pred,
    std::function<void(DocView &)> mutator,
    bool create
) {
	auto cr = collection(name);
	if (!cr.status.ok()) {
		return cr.status;
	}
	return cr.value->updateOne(std::move(pred), std::move(mutator), create);
}

DbStatus ESPJsonDB::updateOne(
    const std::string &name, const JsonDocument &filter, const JsonDocument &patch, bool create
) {
	auto cr = collection(name);
	if (!cr.status.ok()) {
		return cr.status;
	}
	return cr.value->updateOne(filter, patch, create);
}

DbStatus ESPJsonDB::updateById(
    const std::string &name, const std::string &id, std::function<void(DocView &)> mutator
) {
	auto cr = collection(name);
	if (!cr.status.ok()) {
		return cr.status;
	}
	return cr.value->updateById(id, std::move(mutator));
}

DbStatus ESPJsonDB::removeById(const std::string &name, const std::string &id) {
	auto cr = collection(name);
	if (!cr.status.ok()) {
		return cr.status;
	}
	return cr.value->removeById(id);
}

DbResult<size_t> ESPJsonDB::updateMany(
    const std::string &collectionName, const JsonDocument &patch, const JsonDocument &filter
) {
	DbResult<size_t> res{};
	auto cr = collection(collectionName);
	if (!cr.status.ok()) {
		res.status = cr.status;
		return res;
	}
	return cr.value->updateMany(patch, filter);
}

DbStatus ESPJsonDB::syncNow() {
	auto ready = ensureReady();
	if (!ready.ok()) {
		auto st = setLastError(ready);
		emitSyncStatus(DBSyncStage::SyncFailed, DBSyncSource::SyncNow, "", 0, 0, st);
		return st;
	}
	if (_syncTask != nullptr && xTaskGetCurrentTaskHandle() == _syncTask) {
		emitSyncStatus(
		    DBSyncStage::SyncNowStarted,
		    DBSyncSource::SyncNow,
		    "",
		    0,
		    0,
		    {DbStatusCode::Ok, ""}
		);
		auto delayedStatus = maybeRunDelayedPreload(false, true, DBSyncSource::SyncNow);
		auto passStatus = runSyncPass();
		DbStatus finalStatus = delayedStatus.ok() ? passStatus : delayedStatus;
		if (!finalStatus.ok()) {
			setLastError(finalStatus);
			emitSyncStatus(DBSyncStage::SyncFailed, DBSyncSource::SyncNow, "", 0, 0, finalStatus);
			return finalStatus;
		}
		emitSyncStatus(
		    DBSyncStage::SyncNowCompleted,
		    DBSyncSource::SyncNow,
		    "",
		    0,
		    0,
		    {DbStatusCode::Ok, ""}
		);
		return finalStatus;
	}
	if (_syncTask == nullptr) {
		FrLock lk(_mu);
		startSyncTaskUnlocked();
	}
	if (_syncTask == nullptr) {
		auto st = setLastError({DbStatusCode::Busy, "sync task not running"});
		emitSyncStatus(DBSyncStage::SyncFailed, DBSyncSource::SyncNow, "", 0, 0, st);
		return st;
	}

	const uint32_t targetSeq = _syncRequestSeq.fetch_add(1, std::memory_order_acq_rel) + 1;
	_syncKickRequested.store(true, std::memory_order_release);

	const uint32_t startMs = millis();
	const uint32_t timeoutMs = std::max<uint32_t>(_cfg.intervalMs + 1000U, 60000U);
	while (_syncCompletedSeq.load(std::memory_order_acquire) < targetSeq) {
		if (_syncStopRequested.load(std::memory_order_acquire)) {
			auto st = setLastError({DbStatusCode::Busy, "sync task stopping"});
			emitSyncStatus(DBSyncStage::SyncFailed, DBSyncSource::SyncNow, "", 0, 0, st);
			return st;
		}
		if ((millis() - startMs) > timeoutMs) {
			auto st = setLastError({DbStatusCode::Busy, "sync task timeout"});
			emitSyncStatus(DBSyncStage::SyncFailed, DBSyncSource::SyncNow, "", 0, 0, st);
			return st;
		}
		vTaskDelay(pdMS_TO_TICKS(1));
	}
	return _lastError;
}

DbStatus ESPJsonDB::runSyncPass() {
	// Snapshot work under lock
	DbRuntime::StringVector colsToDrop{JsonDbAllocator<std::string>(_cfg.usePSRAMBuffers)};
	JsonDbVector<Collection *> cols{JsonDbAllocator<Collection *>(_cfg.usePSRAMBuffers)};
	bool dropAll = false;
	{
		FrLock lk(_mu);
		dropAll = _dropAllRequested;
		_dropAllRequested = false;
		colsToDrop.swap(_colsToDelete);
		cols.reserve(_cols.size());
		for (auto &kv : _cols)
			cols.push_back(kv.second.get());
	}
	bool anyChanges = false;
	DbStatus finalStatus{DbStatusCode::Ok, ""};
	if (dropAll) {
		if (_fs) {
			auto st = removeTree(*_fs, _baseDir);
			if (!st.ok()) {
				return setLastError(st);
			}
		}
		auto st = ensureFsReady();
		if (!st.ok()) {
			return setLastError(st);
		}
		anyChanges = true;
	}
	// Handle dropped collections: remove their directories
	for (const auto &n : colsToDrop) {
		auto st = removeCollectionDir(n);
		if (!st.ok()) {
			finalStatus = st;
			setLastError(st);
		} else {
			emitEvent(DBEventType::CollectionDropped);
			anyChanges = true;
		}
	}
	// Flush each collection
	for (auto *c : cols) {
		bool changed = false;
		auto st = c->flushDirtyToFs(_baseDir, changed);
		if (!st.ok()) {
			return setLastError(st);
		}
		if (changed)
			anyChanges = true;
	}
	// Only refresh diagnostics and emit Sync if there were actual changes
	if (anyChanges) {
		emitEvent(DBEventType::Sync);
	}
	if (!finalStatus.ok())
		return finalStatus;
	return setLastError({DbStatusCode::Ok, ""});
}

void ESPJsonDB::syncTaskThunk(void *arg) {
	auto *self = static_cast<ESPJsonDB *>(arg);
	self->syncTaskLoop();
}

void ESPJsonDB::syncTaskLoop() {
	uint32_t lastSyncMs = millis();
	while (!_syncStopRequested.load(std::memory_order_acquire)) {
		bool shouldRun = false;
		bool triggeredByPeriodic = false;
		if (_syncKickRequested.exchange(false, std::memory_order_acq_rel)) {
			shouldRun = true;
		}
		const uint32_t now = millis();
		if (!shouldRun && _cfg.autosync && (now - lastSyncMs) >= _cfg.intervalMs) {
			shouldRun = true;
			triggeredByPeriodic = true;
		}
		if (!shouldRun) {
			vTaskDelay(pdMS_TO_TICKS(10));
			continue;
		}
		const uint32_t targetSeq = _syncRequestSeq.load(std::memory_order_acquire);
		const uint32_t completedBefore = _syncCompletedSeq.load(std::memory_order_acquire);
		const bool isManualSyncNow = targetSeq > completedBefore;
		if (isManualSyncNow) {
			emitSyncStatus(
			    DBSyncStage::SyncNowStarted,
			    DBSyncSource::SyncNow,
			    "",
			    0,
			    0,
			    {DbStatusCode::Ok, ""}
			);
		}

		auto delayedStatus = maybeRunDelayedPreload(
		    triggeredByPeriodic,
		    isManualSyncNow && !triggeredByPeriodic,
		    DBSyncSource::SyncNow
		);
		if (!delayedStatus.ok()) {
			setLastError(delayedStatus);
		}
		lastSyncMs = now;
		auto syncStatus = runSyncPass();
		DbStatus finalStatus = delayedStatus.ok() ? syncStatus : delayedStatus;
		if (!finalStatus.ok()) {
			setLastError(finalStatus);
		}
		if (isManualSyncNow) {
			if (finalStatus.ok()) {
				emitSyncStatus(
				    DBSyncStage::SyncNowCompleted,
				    DBSyncSource::SyncNow,
				    "",
				    0,
				    0,
				    {DbStatusCode::Ok, ""}
				);
			} else {
				emitSyncStatus(
				    DBSyncStage::SyncFailed,
				    DBSyncSource::SyncNow,
				    "",
				    0,
				    0,
				    finalStatus
				);
			}
		}
		uint32_t completed = _syncCompletedSeq.load(std::memory_order_acquire);
		while (completed < targetSeq &&
		       !_syncCompletedSeq
		            .compare_exchange_weak(completed, targetSeq, std::memory_order_acq_rel)) {
		}
	}
	_syncTaskExited.store(true, std::memory_order_release);
	vTaskDelete(nullptr);
}

void ESPJsonDB::startSyncTaskUnlocked() {
	if (_syncTask != nullptr)
		return;
	_syncStopRequested.store(false, std::memory_order_release);
	_syncTaskExited.store(false, std::memory_order_release);
	_syncKickRequested.store(false, std::memory_order_release);
	TaskHandle_t handle = nullptr;
	if (createTask(syncTaskThunk, "db.sync", handle)) {
		_syncTask = handle;
	} else {
		_syncTaskExited.store(true, std::memory_order_release);
	}
}

void ESPJsonDB::stopSyncTaskUnlocked() {
	stopTask(_syncTask, _syncStopRequested, _syncTaskExited);
	_syncKickRequested.store(false, std::memory_order_release);
	_syncCompletedSeq.store(
	    _syncRequestSeq.load(std::memory_order_acquire),
	    std::memory_order_release
	);
}

namespace {
static void listDirEntries(fs::FS &fsImpl, const std::string &dir, DirEntryVector &out) {
	FrLock fs(g_fsMutex);
	if (!fsImpl.exists(dir.c_str()))
		return;
	File d = fsImpl.open(dir.c_str());
	if (!d || !d.isDirectory()) {
		if (d)
			d.close();
		return;
	}
	for (File f = d.openNextFile(); f; f = d.openNextFile()) {
		bool isDir = f.isDirectory();
		String name = f.name();
		f.close();
		std::string child = name.c_str();
		std::string full = joinPath(dir, child);
		out.emplace_back(full, isDir);
	}
	d.close();
}

static DbStatus removeTree(fs::FS &fsImpl, const std::string &path) {
	// Check if path is a directory
	bool isDir = false;
	{
		FrLock fs(g_fsMutex);
		if (!fsImpl.exists(path.c_str()))
			return {DbStatusCode::Ok, ""};
		File f = fsImpl.open(path.c_str());
		if (f) {
			isDir = f.isDirectory();
			f.close();
		} else {
			return {DbStatusCode::IoError, "open path failed during recursive remove"};
		}
	}
	if (!isDir) {
		FrLock fs(g_fsMutex);
		if (!fsImpl.remove(path.c_str())) {
			return {DbStatusCode::IoError, "remove file failed during recursive remove"};
		}
		return {DbStatusCode::Ok, ""};
	}
	// List children first without holding lock during recursion
	DirEntryVector entries{JsonDbAllocator<DirEntry>(false)};
	listDirEntries(fsImpl, path, entries);
	for (auto &e : entries) {
		if (e.second) {
			auto st = removeTree(fsImpl, e.first);
			if (!st.ok()) {
				return st;
			}
		} else {
			FrLock fs(g_fsMutex);
			if (!fsImpl.remove(e.first.c_str())) {
				return {DbStatusCode::IoError, "remove child file failed during recursive remove"};
			}
		}
	}
	// Finally remove the directory itself
	{
		FrLock fs(g_fsMutex);
		if (!fsImpl.rmdir(path.c_str())) {
			return {DbStatusCode::IoError, "remove directory failed during recursive remove"};
		}
	}
	return {DbStatusCode::Ok, ""};
}
} // namespace

DbStatus ESPJsonDB::removeCollectionDir(const std::string &name) {
	std::string dir = _baseDir;
	if (!dir.empty() && dir.back() != '/')
		dir += '/';
	dir += name;
	if (!_fs) {
		return setLastError({DbStatusCode::InvalidArgument, "filesystem handle is null"});
	}
	auto st = removeTree(*_fs, dir);
	return setLastError(st);
}

void ESPJsonDB::emitEvent(DBEventType ev) {
	std::vector<std::function<void(DBEventType)>> callbacks;
	{
		FrLock lk(_mu);
		callbacks.reserve(_eventCbs.size());
		for (auto &cb : _eventCbs) {
			callbacks.push_back(cb);
		}
	}
	for (auto &fn : callbacks) {
		if (fn)
			invokeJsonDbCallback(fn, ev);
	}
}

void ESPJsonDB::emitError(const DbStatus &st) {
	std::vector<std::function<void(const DbStatus &)>> callbacks;
	{
		FrLock lk(_mu);
		callbacks.reserve(_errorCbs.size());
		for (auto &cb : _errorCbs) {
			callbacks.push_back(cb);
		}
	}
	for (auto &fn : callbacks) {
		if (fn)
			invokeJsonDbCallback(fn, st);
	}
}

void ESPJsonDB::emitSyncStatus(const DBSyncStatus &status) {
	std::vector<std::function<void(const DBSyncStatus &)>> callbacks;
	{
		FrLock lk(_mu);
		_lastSyncStatus = status;
		callbacks.reserve(_syncStatusCbs.size());
		for (auto &cb : _syncStatusCbs) {
			callbacks.push_back(cb);
		}
	}
	for (auto &fn : callbacks) {
		if (fn)
			invokeJsonDbCallback(fn, status);
	}
}

void ESPJsonDB::emitSyncStatus(
    DBSyncStage stage,
    DBSyncSource source,
    const std::string &collectionName,
    uint32_t collectionsCompleted,
    uint32_t collectionsTotal,
    const DbStatus &result
) {
	DBSyncStatus status;
	status.stage = stage;
	status.source = source;
	status.collectionName = collectionName;
	status.collectionsCompleted = collectionsCompleted;
	status.collectionsTotal = collectionsTotal;
	status.result = result;
	emitSyncStatus(status);
}

void ESPJsonDB::noteDocumentCreated(const std::string &collectionName, uint32_t count) {
	if (collectionName.empty() || count == 0)
		return;
	FrLock lk(_mu);
	if (!_diagCachePrimed)
		return;
	uint32_t &docs = _diagCache.docsPerCollection[collectionName];
	if (docs == 0) {
		++_diagCache.collections;
	}
	docs += count;
	_diagCache.lastRefreshMs = millis();
}

void ESPJsonDB::noteDocumentDeleted(const std::string &collectionName, uint32_t count) {
	if (collectionName.empty() || count == 0)
		return;
	FrLock lk(_mu);
	if (!_diagCachePrimed)
		return;
	auto it = _diagCache.docsPerCollection.find(collectionName);
	if (it == _diagCache.docsPerCollection.end())
		return;
	if (it->second <= count) {
		_diagCache.docsPerCollection.erase(it);
		if (_diagCache.collections > 0)
			--_diagCache.collections;
	} else {
		it->second -= count;
	}
	_diagCache.lastRefreshMs = millis();
}

DbStatus ESPJsonDB::preloadCollectionsFromFs(bool emitStatus, DBSyncSource statusSource) {
	auto ready = ensureReady();
	if (!ready.ok())
		return setLastError(ready);
	if (!_fs) {
		return setLastError({DbStatusCode::IoError, "filesystem not ready"});
	}

	JsonDbVector<std::string> names{JsonDbAllocator<std::string>(_cfg.usePSRAMBuffers)};
	{
		FrLock fs(g_fsMutex);
		if (!_fs->exists(_baseDir.c_str())) {
			return setLastError({DbStatusCode::Ok, ""});
		}
		File base = _fs->open(_baseDir.c_str());
		if (!base || !base.isDirectory()) {
			if (base)
				base.close();
			return setLastError({DbStatusCode::IoError, "open base dir failed"});
		}
		for (File f = base.openNextFile(); f; f = base.openNextFile()) {
			if (!f.isDirectory()) {
				f.close();
				continue;
			}
			String raw = f.name();
			f.close();
			std::string name = raw.c_str();
			auto slash = name.find_last_of('/');
			if (slash != std::string::npos)
				name = name.substr(slash + 1);
			if (name.empty() || isReservedName(name))
				continue;
			names.push_back(name);
		}
		base.close();
	}
	std::sort(names.begin(), names.end());
	names.erase(std::unique(names.begin(), names.end()), names.end());

	JsonDbVector<std::string> preloadNames{JsonDbAllocator<std::string>(_cfg.usePSRAMBuffers)};
	{
		FrLock lk(_mu);
		preloadNames.reserve(names.size());
		for (const auto &name : names) {
			if (_cols.find(name) != _cols.end())
				continue;
			if (_pendingDelayedCollections.find(name) != _pendingDelayedCollections.end())
				continue;
			auto cit = _collectionConfigs.find(name);
			const auto policy =
			    cit != _collectionConfigs.end() ? cit->second.loadPolicy : _cfg.defaultLoadPolicy;
			if (policy == CollectionLoadPolicy::Delayed) {
				_pendingDelayedCollections[name] = true;
				_delayedPreloadPhaseCompleted = false;
				continue;
			}
			if (policy != CollectionLoadPolicy::Eager)
				continue;
			preloadNames.push_back(name);
		}
	}

	const uint32_t total = static_cast<uint32_t>(preloadNames.size());
	uint32_t completed = 0;
	if (emitStatus) {
		emitSyncStatus(
		    DBSyncStage::ColdSyncStarted,
		    statusSource,
		    "",
		    completed,
		    total,
		    {DbStatusCode::Ok, ""}
		);
	}

	for (const auto &name : preloadNames) {
		if (emitStatus) {
			emitSyncStatus(
			    DBSyncStage::ColdSyncCollectionStarted,
			    statusSource,
			    name,
			    completed,
			    total,
			    {DbStatusCode::Ok, ""}
			);
		}
		auto st = preloadCollectionFromFsByName(name, false);
		if (!st.ok()) {
			if (emitStatus) {
				emitSyncStatus(DBSyncStage::SyncFailed, statusSource, name, completed, total, st);
			}
			return setLastError(st);
		}
		++completed;
		if (emitStatus) {
			emitSyncStatus(
			    DBSyncStage::ColdSyncCollectionCompleted,
			    statusSource,
			    name,
			    completed,
			    total,
			    {DbStatusCode::Ok, ""}
			);
		}
	}
	if (emitStatus) {
		emitSyncStatus(
		    DBSyncStage::ColdSyncCompleted,
		    statusSource,
		    "",
		    completed,
		    total,
		    {DbStatusCode::Ok, ""}
		);
	}

	return setLastError({DbStatusCode::Ok, ""});
}

JsonDocument ESPJsonDB::getDiagnostics() {
	// Build diagnostics from cached FS snapshot, overlapped with live loaded collections
	// No filesystem access here.
	JsonDocument doc;
	const bool usePSRAMBuffers = _cfg.usePSRAMBuffers;

	// Snapshot state under lock
	DbRuntime::StringUint32Map cached{
	    std::less<std::string>{},
	    DbRuntime::StringUint32Map::allocator_type(usePSRAMBuffers)
	};
	DbRuntime::StringUint32Map live{
	    std::less<std::string>{},
	    DbRuntime::StringUint32Map::allocator_type(usePSRAMBuffers)
	};
	uint32_t lastRefreshMs = 0;
	// Copy of configuration for reporting
	ESPJsonDBConfig cfgCopy{};
	std::string baseDirCopy;
	{
		FrLock lk(_mu);
		cached = _diagCache.docsPerCollection; // copy
		lastRefreshMs = _diagCache.lastRefreshMs;
		for (auto &kv : _cols) {
			if (isReservedName(kv.first))
				continue;
			live[kv.first] = kv.second ? static_cast<uint32_t>(kv.second->size()) : 0u;
		}
		cfgCopy = _cfg;
		baseDirCopy = _baseDir;
	}

	// Per-collection document counts
	auto per = doc["documentsPerCollection"].to<JsonObject>();
	// Union of keys: prefer live counts for loaded collections
	DbRuntime::StringBoolMap seen{
	    std::less<std::string>{},
	    DbRuntime::StringBoolMap::allocator_type(usePSRAMBuffers)
	};
	for (auto &kv : live) {
		per[kv.first.c_str()] = kv.second;
		seen[kv.first] = true;
	}
	for (auto &kv : cached) {
		if (isReservedName(kv.first))
			continue;
		if (seen.find(kv.first) != seen.end())
			continue;
		per[kv.first.c_str()] = kv.second;
	}

	// Collections = number of unique keys
	uint32_t collections = static_cast<uint32_t>(seen.size());
	for (auto &kv : cached) {
		if (isReservedName(kv.first))
			continue;
		if (seen.find(kv.first) == seen.end())
			++collections;
	}
	doc["collections"] = collections;
	doc["lastRefreshMs"] = lastRefreshMs; // for visibility (optional)

	// Config block
	auto cfg = doc["config"].to<JsonObject>();
	cfg["baseDir"] = baseDirCopy.c_str();
	cfg["intervalMs"] = cfgCopy.intervalMs;
	cfg["autosync"] = cfgCopy.autosync;
	cfg["initFileSystem"] = cfgCopy.initFileSystem;
	cfg["formatOnFail"] = cfgCopy.formatOnFail;
	cfg["maxOpenFiles"] = static_cast<uint32_t>(cfgCopy.maxOpenFiles);
	cfg["partitionLabel"] = cfgCopy.partitionLabel ? cfgCopy.partitionLabel : nullptr;
	cfg["stackSize"] = cfgCopy.stackSize;
	cfg["priority"] = static_cast<uint32_t>(cfgCopy.priority);
	cfg["coreId"] = static_cast<int32_t>(cfgCopy.coreId);
	cfg["usePSRAMBuffers"] = cfgCopy.usePSRAMBuffers;
	cfg["defaultLoadPolicy"] = static_cast<uint8_t>(cfgCopy.defaultLoadPolicy);

	auto policies = cfg["collectionLoadPolicies"].to<JsonObject>();
	{
		FrLock lk(_mu);
		for (const auto &kv : _collectionConfigs) {
			policies[kv.first.c_str()] = static_cast<uint8_t>(kv.second.loadPolicy);
		}
	}

	setLastError({DbStatusCode::Ok, ""});
	return doc;
}

DbStatus ESPJsonDB::dropAll() {
	auto ready = ensureReady();
	if (!ready.ok()) {
		return setLastError(ready);
	}
	{
		FrLock lk(_mu);
		if (_rt->fileStoreImpl)
			_rt->fileStoreImpl->stopTask(true);

		for (auto &kv : _cols) {
			if (kv.second)
				kv.second->markAllRemoved();
		}
		_cols.clear();
		_colsToDelete.clear();
		_rt->fileStoreImpl = std::make_unique<FileStoreImpl>(*_rt);
		_fileStore = std::make_unique<FileStore>(_rt->fileStoreImpl.get());
		_pendingDelayedCollections.clear();
		_delayedPreloadPhaseCompleted = true;
		_diagCache.docsPerCollection.clear();
		_diagCache.collections = 0;
		_diagCache.lastRefreshMs = millis();
		_diagCachePrimed = true;
		_dropAllRequested = true;
	}
	_syncKickRequested.store(true, std::memory_order_release);
	return syncNow();
}

std::vector<std::string> ESPJsonDB::listCollectionNames() {
	auto ready = ensureReady();
	if (!ready.ok()) {
		setLastError(ready);
		return {};
	}
	std::vector<std::string> names;
	// Use a set to avoid duplicates
	DbRuntime::StringBoolMap seen{
	    std::less<std::string>{},
	    DbRuntime::StringBoolMap::allocator_type(_cfg.usePSRAMBuffers)
	};
	{
		FrLock lk(_mu);
		for (auto &kv : _cols) {
			if (isReservedName(kv.first))
				continue;
			seen[kv.first] = true;
		}
		for (auto &kv : _diagCache.docsPerCollection) {
			if (isReservedName(kv.first))
				continue;
			seen[kv.first] = true;
		}
	}
	names.reserve(seen.size());
	for (auto &kv : seen)
		names.push_back(kv.first);
	return names;
}

DbStatus ESPJsonDB::changeConfig(const ESPJsonDBConfig &cfg) {
	auto ready = ensureReady();
	if (!ready.ok()) {
		return setLastError(ready);
	}
	// Stop existing task if running and apply new config
	{
		FrLock lk(_mu);
		if (_rt->fileStoreImpl)
			_rt->fileStoreImpl->stopTask(true);
		stopSyncTaskUnlocked();
		_cfg = cfg;
		rebindAllocatorAwareStateLocked(true);
		_rt->fileStoreImpl = std::make_unique<FileStoreImpl>(*_rt);
		_fileStore = std::make_unique<FileStore>(_rt->fileStoreImpl.get());
		rebuildDelayedCollectionStateFromConfigLocked();
	}
	auto fsStatus = ensureFsReady();
	if (!fsStatus.ok())
		return fsStatus;
	auto preloadStatus = preloadCollectionsFromFs(false, DBSyncSource::Init);
	if (!preloadStatus.ok()) {
		return preloadStatus;
	}
	{
		FrLock lk(_mu);
		startSyncTaskUnlocked();
	}
	if (_syncTask == nullptr) {
		return setLastError({DbStatusCode::Busy, "sync task start failed"});
	}
	return setLastError({DbStatusCode::Ok, ""});
}

namespace {
DbStatus writeSnapshotBytes(Print &out, const char *data, size_t size) {
	if (!data || size == 0) {
		return {DbStatusCode::Ok, ""};
	}
	size_t written = out.write(reinterpret_cast<const uint8_t *>(data), size);
	if (written != size) {
		return {DbStatusCode::IoError, "snapshot write failed"};
	}
	return {DbStatusCode::Ok, ""};
}

DbStatus writeSnapshotBytes(Print &out, const char *text) {
	return text ? writeSnapshotBytes(out, text, std::strlen(text)) : DbStatus{DbStatusCode::Ok, ""};
}

DbStatus writeSnapshotString(Print &out, const std::string &text) {
	return writeSnapshotBytes(out, text.data(), text.size());
}

std::string snapshotCollectionName(const std::string &path) {
	auto pos = path.find_last_of('/');
	return (pos == std::string::npos) ? path : path.substr(pos + 1);
}
} // namespace

DbStatus ESPJsonDB::writeSnapshot(Stream &out, SnapshotMode mode) {
	if (mode == SnapshotMode::InMemoryConsistent) {
		auto syncStatus = syncNow();
		if (!syncStatus.ok()) {
			return setLastError(syncStatus);
		}
	}
	if (!_fs) {
		return setLastError({DbStatusCode::IoError, "filesystem not ready"});
	}

	WriteBufferingStream buffered(out, 512);
	auto writeStatus = writeSnapshotBytes(buffered, "{\"collections\":{");
	if (!writeStatus.ok()) {
		return setLastError(writeStatus);
	}

	DirEntryVector colDirs{JsonDbAllocator<DirEntry>(_cfg.usePSRAMBuffers)};
	listDirEntries(*_fs, _baseDir, colDirs);

	RecordStore store(*_fs, _cfg.usePSRAMBuffers);
	bool firstCollection = true;
	for (auto &entry : colDirs) {
		if (!entry.second) {
			continue;
		}

		const std::string colName = snapshotCollectionName(entry.first);
		if (isReservedName(colName)) {
			continue;
		}

		JsonDocument keyDoc;
		keyDoc.set(colName.c_str());
		std::string keyJson;
		serializeJson(keyDoc, keyJson);

		if (!firstCollection) {
			writeStatus = writeSnapshotBytes(buffered, ",");
			if (!writeStatus.ok()) {
				return setLastError(writeStatus);
			}
		}
		firstCollection = false;

		writeStatus = writeSnapshotString(buffered, keyJson);
		if (!writeStatus.ok()) {
			return setLastError(writeStatus);
		}
		writeStatus = writeSnapshotBytes(buffered, ":[");
		if (!writeStatus.ok()) {
			return setLastError(writeStatus);
		}

		const auto ids = store.listIds(entry.first);
		bool firstDocument = true;
		for (const auto &id : ids) {
			auto rec = store.read(entry.first, id.c_str());
			if (!rec.status.ok() || !rec.value) {
				continue;
			}

			JsonDocument payload;
			auto err =
			    deserializeMsgPack(payload, rec.value->msgpack.data(), rec.value->msgpack.size());
			if (err) {
				continue;
			}

			JsonDocument snapshotEntry;
			JsonObject obj = snapshotEntry.to<JsonObject>();
			obj.set(payload.as<JsonObjectConst>());
			obj["_id"] = rec.value->meta.id.c_str();
			auto meta = obj["_meta"].to<JsonObject>();
			meta["createdAtMs"] = rec.value->meta.createdAtMs;
			meta["updatedAtMs"] = rec.value->meta.updatedAtMs;
			meta["revision"] = rec.value->meta.revision;
			meta["flags"] = rec.value->meta.flags;

			std::string entryJson;
			serializeJson(snapshotEntry, entryJson);

			if (!firstDocument) {
				writeStatus = writeSnapshotBytes(buffered, ",");
				if (!writeStatus.ok()) {
					return setLastError(writeStatus);
				}
			}
			firstDocument = false;

			writeStatus = writeSnapshotString(buffered, entryJson);
			if (!writeStatus.ok()) {
				return setLastError(writeStatus);
			}
		}

		writeStatus = writeSnapshotBytes(buffered, "]");
		if (!writeStatus.ok()) {
			return setLastError(writeStatus);
		}
	}

	writeStatus = writeSnapshotBytes(buffered, "}}");
	if (!writeStatus.ok()) {
		return setLastError(writeStatus);
	}

	buffered.flush();
	if (buffered.getWriteError()) {
		return setLastError({DbStatusCode::IoError, "snapshot write failed"});
	}

	return setLastError({DbStatusCode::Ok, ""});
}

JsonDocument ESPJsonDB::getSnapshot(SnapshotMode mode) {
	JsonDocument snap;
	if (mode == SnapshotMode::InMemoryConsistent) {
		auto syncStatus = syncNow();
		if (!syncStatus.ok()) {
			setLastError(syncStatus);
			return snap;
		}
	}
	if (!_fs) {
		setLastError({DbStatusCode::IoError, "filesystem not ready"});
		return snap;
	}
	auto colsObj = snap["collections"].to<JsonObject>();
	RecordStore store(*_fs, _cfg.usePSRAMBuffers);

	// Scan collections dirs
	DirEntryVector colDirs{JsonDbAllocator<DirEntry>(_cfg.usePSRAMBuffers)};
	listDirEntries(*_fs, _baseDir, colDirs);
	for (auto &cd : colDirs) {
		if (!cd.second)
			continue; // not a directory
		const std::string &full = cd.first;
		auto p = full.find_last_of('/');
		std::string colName = (p == std::string::npos) ? full : full.substr(p + 1);
		if (isReservedName(colName))
			continue;

		// Iterate files in collection dir
		const auto ids = store.listIds(full);
		JsonArray arr = colsObj[colName.c_str()].to<JsonArray>();
		for (const auto &id : ids) {
			auto rec = store.read(full, id.c_str());
			if (!rec.status.ok() || !rec.value)
				continue;
			JsonDocument tmp;
			auto derr =
			    deserializeMsgPack(tmp, rec.value->msgpack.data(), rec.value->msgpack.size());
			if (derr)
				continue;
			JsonObject obj = arr.add<JsonObject>();
			obj.set(tmp.as<JsonObjectConst>());
			obj["_id"] = rec.value->meta.id.c_str();
			auto meta = obj["_meta"].to<JsonObject>();
			meta["createdAtMs"] = rec.value->meta.createdAtMs;
			meta["updatedAtMs"] = rec.value->meta.updatedAtMs;
			meta["revision"] = rec.value->meta.revision;
			meta["flags"] = rec.value->meta.flags;
		}
	}
	setLastError({DbStatusCode::Ok, ""});
	return snap;
}

DbStatus ESPJsonDB::restoreFromSnapshot(Stream &in) {
	ReadBufferingStream buffered(in, 512);
	JsonDocument snapshot;
	auto err = deserializeJson(snapshot, buffered);
	if (err) {
		return setLastError({DbStatusCode::InvalidArgument, "snapshot parse failed"});
	}
	return restoreFromSnapshot(snapshot);
}

DbStatus ESPJsonDB::restoreFromSnapshot(const JsonDocument &snapshot) {
	// Validate snapshot structure
	auto cols = snapshot["collections"].as<JsonObjectConst>();
	if (cols.isNull()) {
		return setLastError({DbStatusCode::InvalidArgument, "missing collections"});
	}
	if (!_fs) {
		return setLastError({DbStatusCode::IoError, "filesystem not ready"});
	}

	// Drop everything first
	auto st = dropAll();
	if (!st.ok())
		return st;

	// For each collection, recreate documents
	for (auto kv : cols) {
		const char *colName = kv.key().c_str();
		if (!colName || !*colName)
			continue;
		if (isReservedName(colName))
			continue;
		JsonArrayConst arr = kv.value().as<JsonArrayConst>();
		if (arr.isNull())
			continue;

		// Ensure directory exists
		std::string dir = _baseDir;
		if (!dir.empty() && dir.back() != '/')
			dir += '/';
		dir += colName;
		{
			FrLock fs(g_fsMutex);
			fsEnsureDir(*_fs, dir);
		}

		RecordStore store(*_fs, _cfg.usePSRAMBuffers);
		for (JsonObjectConst obj : arr) {
			const char *id =
			    obj["_id"].is<const char *>() ? obj["_id"].as<const char *>() : nullptr;
			if (!id || !*id)
				continue;
			if (!DocId::isHex(id, std::strlen(id))) {
				return setLastError(
				    {DbStatusCode::InvalidArgument, "snapshot contains invalid _id"}
				);
			}

			// Copy object without _id into a temp doc
			JsonDocument tmp;
			tmp.to<JsonObject>().set(obj);
			tmp.remove("_id");
			tmp.remove("_meta");

			size_t sz = measureMsgPack(tmp);
			DocumentRecord record(_cfg.usePSRAMBuffers);
			record.meta.id = DocId(id);
			record.msgpack.resize(sz);
			size_t written = serializeMsgPack(tmp, record.msgpack.data(), record.msgpack.size());
			if (written != sz)
				return setLastError({DbStatusCode::IoError, "serialize msgpack failed"});

			JsonObjectConst meta = obj["_meta"].as<JsonObjectConst>();
			if (!meta.isNull()) {
				record.meta.createdAtMs = meta["createdAtMs"] | nowUtcMs();
				record.meta.updatedAtMs = meta["updatedAtMs"] | record.meta.createdAtMs;
				record.meta.revision = meta["revision"] | 1U;
				record.meta.flags = meta["flags"] | static_cast<uint16_t>(0);
			} else {
				record.meta.createdAtMs = nowUtcMs();
				record.meta.updatedAtMs = record.meta.createdAtMs;
				record.meta.revision = 1;
				record.meta.flags = 0;
			}
			record.meta.dirty = false;
			record.meta.removed = false;
			auto stWrite = store.write(dir, record);
			if (!stWrite.ok())
				return setLastError(stWrite);
		}
	}

	// Refresh diag cache to reflect restored state
	refreshDiagFromFs();
	emitEvent(DBEventType::Sync);
	return setLastError({DbStatusCode::Ok, ""});
}

// Private: expensive FS scan; called on init and after successful sync
void ESPJsonDB::refreshDiagFromFs() {
	if (!_fs)
		return;
	DbRuntime::StringUint32Map perCol{
	    std::less<std::string>{},
	    DbRuntime::StringUint32Map::allocator_type(_cfg.usePSRAMBuffers)
	};
	uint32_t colCount = 0;
	{
		FrLock fs(g_fsMutex);
		if (!_fs->exists(_baseDir.c_str())) {
			// No base dir yet → empty
		} else {
			File base = _fs->open(_baseDir.c_str());
			if (base && base.isDirectory()) {
				for (File f = base.openNextFile(); f; f = base.openNextFile()) {
					if (!f.isDirectory()) {
						f.close();
						continue;
					}
					String colName = f.name();
					std::string cname = colName.c_str();
					auto slash = cname.find_last_of('/');
					if (slash != std::string::npos)
						cname = cname.substr(slash + 1);
					if (isReservedName(cname)) {
						f.close();
						continue;
					}
					f.close();

					// Count persisted record files
					std::string dirPath = _baseDir;
					if (!dirPath.empty() && dirPath.back() != '/')
						dirPath += '/';
					dirPath += cname;
					File colDir = _fs->open(dirPath.c_str());
					if (!colDir || !colDir.isDirectory()) {
						colDir.close();
						continue;
					}
					uint32_t cnt = 0;
					for (File df = colDir.openNextFile(); df; df = colDir.openNextFile()) {
						if (df.isDirectory()) {
							df.close();
							continue;
						}
						String fn = df.name();
						df.close();
						std::string n = fn.c_str();
						if (n.size() >= 4 && n.substr(n.size() - 4) == ".jdb")
							++cnt;
					}
					colDir.close();
					// Only include collections that currently have at least one document file
					if (cnt > 0) {
						perCol[cname] = cnt;
						++colCount;
					}
				}
				base.close();
			}
		}
	}
	{
		FrLock lk(_mu);
		_diagCache.docsPerCollection = std::move(perCol);
		_diagCache.collections = colCount;
		_diagCache.lastRefreshMs = millis();
		_diagCachePrimed = true;
	}
}
