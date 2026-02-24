#include "db.h"
#include "utils/fs_utils.h"
#include "utils/jsondb_allocator.h"
#include <StreamUtils.h>

namespace {
constexpr uint32_t kTaskStopTimeoutMs = 200;
} // namespace

FrMutex g_fsMutex; // definition of global FS mutex

DbStatus ESPJsonDB::ensureFsReady() {
	_fs = _cfg.fs ? _cfg.fs : &LittleFS;
	if (!_fs) {
		return setLastError({DbStatusCode::InvalidArgument, "filesystem handle is null"});
	}
	if (_cfg.initFileSystem && _fs == &LittleFS) {
		if (!LittleFS.begin(_cfg.formatOnFail, "/littlefs", _cfg.maxOpenFiles, _cfg.partitionLabel)) {
			return setLastError({DbStatusCode::IoError, "LittleFS.begin failed"});
		}
	}
	if (!fsEnsureDir(*_fs, _baseDir)) {
		return setLastError({DbStatusCode::IoError, "mkdir baseDir failed"});
	}
	if (!fsEnsureDir(*_fs, fileRootDir())) {
		return setLastError({DbStatusCode::IoError, "mkdir file root failed"});
	}
	return setLastError({DbStatusCode::Ok, ""});
}

DbStatus ESPJsonDB::ensureReady() const {
	if (!isInitialized() || !_fs || _baseDir.empty()) {
		return {DbStatusCode::InvalidArgument, "database not initialized"};
	}
	return {DbStatusCode::Ok, ""};
}

uint32_t ESPJsonDB::stackBytesToWords(uint32_t stackBytes) {
	const uint32_t wordSize = static_cast<uint32_t>(sizeof(StackType_t));
	return (stackBytes + wordSize - 1U) / wordSize;
}

bool ESPJsonDB::createTask(TaskFunction_t entry, const char *name, TaskHandle_t &outHandle) {
	const uint32_t stackDepthWords = stackBytesToWords(_cfg.stackSize);
	BaseType_t rc = xTaskCreatePinnedToCore(entry,
											name,
											stackDepthWords,
											this,
											_cfg.priority,
											&outHandle,
											_cfg.coreId);
	return rc == pdPASS;
}

void ESPJsonDB::stopTask(TaskHandle_t &taskHandle, std::atomic<bool> &stopRequested, std::atomic<bool> &taskExited) {
	if (taskHandle == nullptr) return;
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

bool ESPJsonDB::isReservedName(const std::string &name) const {
	return name == "_files";
}

std::string ESPJsonDB::fileRootDir() const {
	return joinPath(_baseDir, "_files");
}

ESPJsonDB::~ESPJsonDB() {
	deinit();
}

void ESPJsonDB::deinit() {
	if (!isInitialized() && _syncTask == nullptr && _fileUploadTask == nullptr) {
		return;
	}
	_initialized.store(false, std::memory_order_release);

	{
		FrLock lk(_mu);
		stopFileUploadTaskUnlocked(true);
		stopSyncTaskUnlocked();

		for (auto &kv : _cols) {
			if (kv.second) kv.second->markAllRemoved();
		}
		_cols.clear();
		_schemas.clear();
		_colsToDelete.clear();
		_eventCbs.clear();
		_errorCbs.clear();
		_uploadQueue.clear();
		_uploadJobs.clear();
		_nextUploadId = 1;
		_diagCache.docsPerCollection.clear();
		_diagCache.collections = 0;
		_diagCache.lastRefreshMs = 0;
		_diagCachePrimed = false;
	}

	_syncStopRequested.store(false, std::memory_order_release);
	_syncTaskExited.store(true, std::memory_order_release);
	_fileUploadStopRequested.store(false, std::memory_order_release);
	_fileUploadTaskExited.store(true, std::memory_order_release);
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
	_fileUploadStopRequested.store(false, std::memory_order_release);
	auto st = ensureFsReady();
	if (!st.ok()) return st;

	{
		FrLock lk(_mu);
		stopFileUploadTaskUnlocked(true);
		_uploadQueue.clear();
		_uploadJobs.clear();
		_nextUploadId = 1;
		_diagCache.docsPerCollection.clear();
		_diagCache.collections = 0;
		_diagCache.lastRefreshMs = 0;
		_diagCachePrimed = true;
	}
	_initialized.store(true, std::memory_order_release);

	if (_cfg.coldSync) {
		auto preloadStatus = preloadCollectionsFromFs();
		if (!preloadStatus.ok()) {
			deinit();
			return setLastError(preloadStatus);
		}
	}

	if (_cfg.autosync) {
		{
			FrLock lk(_mu);
			startSyncTaskUnlocked();
		}
	}
	return setLastError({DbStatusCode::Ok, ""});
}

DbStatus ESPJsonDB::registerSchema(const std::string &name, const Schema &s) {
	if (isReservedName(name)) {
		return setLastError({DbStatusCode::InvalidArgument, "reserved collection name"});
	}
	FrLock lk(_mu);
	_schemas[name] = s;
	return setLastError({DbStatusCode::Ok, ""});
}

DbStatus ESPJsonDB::unRegisterSchema(const std::string &name) {
	if (isReservedName(name)) {
		return setLastError({DbStatusCode::InvalidArgument, "reserved collection name"});
	}
	FrLock lk(_mu);
	_schemas.erase(name);
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

void ESPJsonDB::onSync(const std::function<void()> &cb) {
	// Wrap sync-only callback into event form
	if (!cb) return;
	onEvent([cb](DBEventType ev) { if (ev == DBEventType::Sync) cb(); });
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
	auto it = _cols.find(name);
	if (it != _cols.end()) {
		if (it->second) {
			// Mark all docs removed to invalidate outstanding views safely
			it->second->markAllRemoved();
		}
		_cols.erase(it);
	} else {
		return setLastError({DbStatusCode::Ok, ""});
	}
	// Update diag cache immediately to avoid reporting stale collections
	if (_diagCachePrimed) {
		auto dit = _diagCache.docsPerCollection.find(name);
		if (dit != _diagCache.docsPerCollection.end()) {
			_diagCache.docsPerCollection.erase(dit);
			if (_diagCache.collections > 0) --_diagCache.collections;
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
	{
		FrLock lk(_mu);
		auto it = _cols.find(name);
		if (it != _cols.end()) {
			res.status = {DbStatusCode::Ok, ""};
			res.value = it->second.get();
			return res;
		}
	}
	Schema sc{};
	{
		FrLock lk(_mu);
		auto sit = _schemas.find(name);
		if (sit != _schemas.end()) sc = sit->second;
	}
	auto col = std::make_unique<Collection>(*this, name, sc, _baseDir, _cfg.cacheEnabled, _cfg.usePSRAMBuffers, *_fs);
	auto st = col->loadFromFs(_baseDir);
	if (!st.ok()) {
		res.status = setLastError(st);
		return res;
	}
	Collection *ptr = col.get();
	bool created = false;
	{
		FrLock lk(_mu);
		auto [it, inserted] = _cols.emplace(name, std::move(col));
		created = inserted;
		(void)it;
	}
	if (created) emitEvent(DBEventType::CollectionCreated);
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

DbResult<std::vector<std::string>> ESPJsonDB::createMany(const std::string &name, JsonArrayConst arr) {
	DbResult<std::vector<std::string>> res{};
	auto cr = collection(name);
	if (!cr.status.ok()) {
		res.status = cr.status;
		return res;
	}
	return cr.value->createMany(arr);
}

DbResult<std::vector<std::string>> ESPJsonDB::createMany(const std::string &name, const JsonDocument &arrDoc) {
	if (!arrDoc.is<JsonArray>()) {
		DbResult<std::vector<std::string>> res{};
		res.status = setLastError({DbStatusCode::InvalidArgument, "document must be an array of objects"});
		return res;
	}
	return createMany(name, arrDoc.as<JsonArrayConst>());
}

DbResult<DocView> ESPJsonDB::findById(const std::string &name, const std::string &id) {
	auto cr = collection(name);
	if (!cr.status.ok()) {
		// Return placeholder DocView; caller should check status before use
		return {cr.status, DocView(nullptr, nullptr, nullptr, this)};
	}
	return cr.value->findById(id);
}

DbResult<std::vector<DocView>> ESPJsonDB::findMany(const std::string &name,
												  std::function<bool(const DocView &)> pred) {
	DbResult<std::vector<DocView>> res{};
	auto cr = collection(name);
	if (!cr.status.ok()) {
		res.status = cr.status;
		return res;
	}
	return cr.value->findMany(std::move(pred));
}

DbResult<DocView> ESPJsonDB::findOne(const std::string &name, std::function<bool(const DocView &)> pred) {
	auto cr = collection(name);
	if (!cr.status.ok()) {
		// Return placeholder DocView; caller should check status before use
		return {cr.status, DocView(nullptr, nullptr, nullptr, this)};
	}
	return cr.value->findOne(std::move(pred));
}

DbResult<DocView> ESPJsonDB::findOne(const std::string &name, const JsonDocument &filter) {
	auto cr = collection(name);
	if (!cr.status.ok()) {
		// Return placeholder DocView; caller should check status before use
		return {cr.status, DocView(nullptr, nullptr, nullptr, this)};
	}
	return cr.value->findOne(filter);
}

DbStatus ESPJsonDB::updateOne(const std::string &name,
							 std::function<bool(const DocView &)> pred,
							 std::function<void(DocView &)> mutator,
							 bool create) {
	auto cr = collection(name);
	if (!cr.status.ok()) {
		return cr.status;
	}
	return cr.value->updateOne(std::move(pred), std::move(mutator), create);
}

DbStatus ESPJsonDB::updateOne(const std::string &name,
							 const JsonDocument &filter,
							 const JsonDocument &patch,
							 bool create) {
	auto cr = collection(name);
	if (!cr.status.ok()) {
		return cr.status;
	}
	return cr.value->updateOne(filter, patch, create);
}

DbStatus ESPJsonDB::updateById(const std::string &name, const std::string &id, std::function<void(DocView &)> mutator) {
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

DbResult<size_t> ESPJsonDB::updateMany(const std::string &collectionName,
									  const JsonDocument &patch,
									  const JsonDocument &filter) {
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
		return setLastError(ready);
	}
	// Snapshot work under lock
	std::vector<std::string> colsToDrop;
	std::vector<Collection *> cols;
	{
		FrLock lk(_mu);
		colsToDrop.swap(_colsToDelete);
		cols.reserve(_cols.size());
		for (auto &kv : _cols)
			cols.push_back(kv.second.get());
	}
	bool anyChanges = false;
	DbStatus finalStatus{DbStatusCode::Ok, ""};
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
		if (changed) anyChanges = true;
	}
	// Only refresh diagnostics and emit Sync if there were actual changes
	if (anyChanges) {
		emitEvent(DBEventType::Sync);
	}
	if (!finalStatus.ok()) return finalStatus;
	return setLastError({DbStatusCode::Ok, ""});
}

void ESPJsonDB::syncTaskThunk(void *arg) {
	auto *self = static_cast<ESPJsonDB *>(arg);
	self->syncTaskLoop();
}

void ESPJsonDB::syncTaskLoop() {
	while (!_syncStopRequested.load(std::memory_order_acquire)) {
		vTaskDelay(pdMS_TO_TICKS(_cfg.intervalMs));
		if (_syncStopRequested.load(std::memory_order_acquire)) {
			break;
		}
		(void)syncNow();
	}
	_syncTaskExited.store(true, std::memory_order_release);
	vTaskDelete(nullptr);
}

void ESPJsonDB::startSyncTaskUnlocked() {
	if (_syncTask != nullptr) return;
	_syncStopRequested.store(false, std::memory_order_release);
	_syncTaskExited.store(false, std::memory_order_release);
	TaskHandle_t handle = nullptr;
	if (createTask(syncTaskThunk, "db.sync", handle)) {
		_syncTask = handle;
	} else {
		_syncTaskExited.store(true, std::memory_order_release);
	}
}

void ESPJsonDB::stopSyncTaskUnlocked() {
	stopTask(_syncTask, _syncStopRequested, _syncTaskExited);
}

namespace {
static void listDirEntries(fs::FS &fsImpl, const std::string &dir, std::vector<std::pair<std::string, bool>> &out) {
	FrLock fs(g_fsMutex);
	if (!fsImpl.exists(dir.c_str())) return;
	File d = fsImpl.open(dir.c_str());
	if (!d || !d.isDirectory()) {
		if (d) d.close();
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

static void removeTree(fs::FS &fsImpl, const std::string &path) {
	// Check if path is a directory
	bool isDir = false;
	{
		FrLock fs(g_fsMutex);
		if (!fsImpl.exists(path.c_str())) return;
		File f = fsImpl.open(path.c_str());
		if (f) {
			isDir = f.isDirectory();
			f.close();
		}
	}
	if (!isDir) {
		FrLock fs(g_fsMutex);
		fsImpl.remove(path.c_str());
		return;
	}
	// List children first without holding lock during recursion
	std::vector<std::pair<std::string, bool>> entries;
	listDirEntries(fsImpl, path, entries);
	for (auto &e : entries) {
		if (e.second) {
			removeTree(fsImpl, e.first);
		} else {
			FrLock fs(g_fsMutex);
			fsImpl.remove(e.first.c_str());
		}
	}
	// Finally remove the directory itself
	{
		FrLock fs(g_fsMutex);
#ifdef ARDUINO_ARCH_ESP32
		fsImpl.rmdir(path.c_str());
#endif
	}
}
} // namespace

DbStatus ESPJsonDB::removeCollectionDir(const std::string &name) {
	std::string dir = _baseDir;
	if (!dir.empty() && dir.back() != '/') dir += '/';
	dir += name;
	if (_fs) removeTree(*_fs, dir);
	return setLastError({DbStatusCode::Ok, ""});
}

void ESPJsonDB::emitEvent(DBEventType ev) {
	std::vector<std::function<void(DBEventType)>> callbacks;
	{
		FrLock lk(_mu);
		callbacks = _eventCbs; // copy snapshot
	}
	for (auto &fn : callbacks) {
		if (fn) fn(ev);
	}
}

void ESPJsonDB::emitError(const DbStatus &st) {
	std::vector<std::function<void(const DbStatus &)>> callbacks;
	{
		FrLock lk(_mu);
		callbacks = _errorCbs; // copy snapshot
	}
	for (auto &fn : callbacks) {
		if (fn) fn(st);
	}
}

void ESPJsonDB::noteDocumentCreated(const std::string &collectionName, uint32_t count) {
	if (collectionName.empty() || count == 0) return;
	FrLock lk(_mu);
	if (!_diagCachePrimed) return;
	uint32_t &docs = _diagCache.docsPerCollection[collectionName];
	if (docs == 0) {
		++_diagCache.collections;
	}
	docs += count;
	_diagCache.lastRefreshMs = millis();
}

void ESPJsonDB::noteDocumentDeleted(const std::string &collectionName, uint32_t count) {
	if (collectionName.empty() || count == 0) return;
	FrLock lk(_mu);
	if (!_diagCachePrimed) return;
	auto it = _diagCache.docsPerCollection.find(collectionName);
	if (it == _diagCache.docsPerCollection.end()) return;
	if (it->second <= count) {
		_diagCache.docsPerCollection.erase(it);
		if (_diagCache.collections > 0) --_diagCache.collections;
	} else {
		it->second -= count;
	}
	_diagCache.lastRefreshMs = millis();
}

DbStatus ESPJsonDB::preloadCollectionsFromFs() {
	auto names = getAllCollectionName();
	for (const auto &name : names) {
		if (name.empty()) continue;
		auto cr = collection(name);
		if (!cr.status.ok()) {
			return cr.status;
		}
	}
	return setLastError({DbStatusCode::Ok, ""});
}

JsonDocument ESPJsonDB::getDiag() {
	// Build diagnostics from cached FS snapshot, overlapped with live loaded collections
	// No filesystem access here.
	JsonDocument doc;

	// Snapshot state under lock
	std::map<std::string, uint32_t> cached;
	std::map<std::string, uint32_t> live;
	uint32_t lastRefreshMs = 0;
	// Copy of configuration for reporting
	ESPJsonDBConfig cfgCopy{};
	std::string baseDirCopy;
	{
		FrLock lk(_mu);
		cached = _diagCache.docsPerCollection; // copy
		lastRefreshMs = _diagCache.lastRefreshMs;
		for (auto &kv : _cols) {
			if (isReservedName(kv.first)) continue;
			live[kv.first] = kv.second ? static_cast<uint32_t>(kv.second->size()) : 0u;
		}
		cfgCopy = _cfg;
		baseDirCopy = _baseDir;
	}

	// Per-collection document counts
	auto per = doc["documentsPerCollection"].to<JsonObject>();
	// Union of keys: prefer live counts for loaded collections
	std::map<std::string, bool> seen;
	for (auto &kv : live) {
		per[kv.first.c_str()] = kv.second;
		seen[kv.first] = true;
	}
	for (auto &kv : cached) {
		if (isReservedName(kv.first)) continue;
		if (seen.find(kv.first) != seen.end()) continue;
		per[kv.first.c_str()] = kv.second;
	}

	// Collections = number of unique keys
	uint32_t collections = static_cast<uint32_t>(seen.size());
	for (auto &kv : cached) {
		if (isReservedName(kv.first)) continue;
		if (seen.find(kv.first) == seen.end()) ++collections;
	}
	doc["collections"] = collections;
	doc["lastRefreshMs"] = lastRefreshMs; // for visibility (optional)

	// Config block
	auto cfg = doc["config"].to<JsonObject>();
	cfg["baseDir"] = baseDirCopy.c_str();
	cfg["intervalMs"] = cfgCopy.intervalMs;
	cfg["autosync"] = cfgCopy.autosync;
	cfg["coldSync"] = cfgCopy.coldSync;
	cfg["cacheEnabled"] = cfgCopy.cacheEnabled;
	cfg["initFileSystem"] = cfgCopy.initFileSystem;
	cfg["formatOnFail"] = cfgCopy.formatOnFail;
	cfg["maxOpenFiles"] = static_cast<uint32_t>(cfgCopy.maxOpenFiles);
	cfg["partitionLabel"] = cfgCopy.partitionLabel ? cfgCopy.partitionLabel : nullptr;
	cfg["stackSize"] = cfgCopy.stackSize;
	cfg["priority"] = static_cast<uint32_t>(cfgCopy.priority);
	cfg["coreId"] = static_cast<int32_t>(cfgCopy.coreId);
	cfg["usePSRAMBuffers"] = cfgCopy.usePSRAMBuffers;

	setLastError({DbStatusCode::Ok, ""});
	return doc;
}

DbStatus ESPJsonDB::dropAll() {
	auto ready = ensureReady();
	if (!ready.ok()) {
		return setLastError(ready);
	}
	bool shouldRestart = false;
	{
		FrLock lk(_mu);
		// Stop autosync task to avoid races while removing files
		shouldRestart = _cfg.autosync;
		stopFileUploadTaskUnlocked(true);
		stopSyncTaskUnlocked();

		// Clear in-memory state
		for (auto &kv : _cols) {
			if (kv.second) kv.second->markAllRemoved();
		}
		_cols.clear();
		_colsToDelete.clear();
		_uploadQueue.clear();
		_uploadJobs.clear();
		_diagCache.docsPerCollection.clear();
		_diagCache.collections = 0;
		_diagCache.lastRefreshMs = millis();
		_diagCachePrimed = true;
	}

	// Remove base directory tree and recreate base dir
	{
		std::string base = _baseDir;
		if (_fs) removeTree(*_fs, base);
	}
	auto st = ensureFsReady();
	if (!st.ok()) return st;

	// Restart autosync if it was enabled
	if (shouldRestart) {
		FrLock lk(_mu);
		if (_cfg.autosync) startSyncTaskUnlocked();
	}

	// Emit a single Sync event to inform listeners state changed
	emitEvent(DBEventType::Sync);
	return setLastError({DbStatusCode::Ok, ""});
}

std::vector<std::string> ESPJsonDB::getAllCollectionName() {
	auto ready = ensureReady();
	if (!ready.ok()) {
		setLastError(ready);
		return {};
	}
	std::vector<std::string> names;
	// Use a set to avoid duplicates
	std::map<std::string, bool> seen;
	{
		FrLock lk(_mu);
		for (auto &kv : _cols) {
			if (isReservedName(kv.first)) continue;
			seen[kv.first] = true;
		}
	}
	// Scan filesystem
	std::vector<std::pair<std::string, bool>> entries;
	if (_fs) {
		listDirEntries(*_fs, _baseDir, entries);
		for (auto &e : entries) {
			if (!e.second) continue; // only directories
			const std::string &full = e.first;
			auto p = full.find_last_of('/');
			std::string name = (p == std::string::npos) ? full : full.substr(p + 1);
			if (isReservedName(name)) continue;
			seen[name] = true;
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
	bool doColdSync = cfg.coldSync;
	bool shouldStart = false;
	// Stop existing task if running and apply new config
	{
		FrLock lk(_mu);
		stopFileUploadTaskUnlocked(true);
		stopSyncTaskUnlocked();
		_cfg = cfg;
		for (auto &kv : _cols) {
			if (kv.second) kv.second->setCacheEnabled(_cfg.cacheEnabled);
		}
			shouldStart = _cfg.autosync;
	}
	auto fsStatus = ensureFsReady();
	if (!fsStatus.ok()) return fsStatus;
	if (doColdSync) {
		auto preloadStatus = preloadCollectionsFromFs();
		if (!preloadStatus.ok()) {
			return preloadStatus;
		}
	}
	if (shouldStart) {
		FrLock lk(_mu);
		startSyncTaskUnlocked();
	}
	return setLastError({DbStatusCode::Ok, ""});
}

JsonDocument ESPJsonDB::getSnapshot() {
	JsonDocument snap;
	if (!_fs) {
		setLastError({DbStatusCode::IoError, "filesystem not ready"});
		return snap;
	}
	auto colsObj = snap["collections"].to<JsonObject>();

	// Scan collections dirs
	std::vector<std::pair<std::string, bool>> colDirs;
	listDirEntries(*_fs, _baseDir, colDirs);
	for (auto &cd : colDirs) {
		if (!cd.second) continue; // not a directory
		const std::string &full = cd.first;
		auto p = full.find_last_of('/');
		std::string colName = (p == std::string::npos) ? full : full.substr(p + 1);
		if (isReservedName(colName)) continue;

		// Iterate files in collection dir
		std::vector<std::pair<std::string, bool>> files;
		listDirEntries(*_fs, full, files);
		JsonArray arr = colsObj[colName.c_str()].to<JsonArray>();
		for (auto &fe : files) {
			if (fe.second) continue; // skip subdirectories
			const std::string &fpath = fe.first;
			// expect <id>.mp
			auto dot = fpath.find_last_of('.');
			if (dot == std::string::npos || fpath.substr(dot) != ".mp") continue;
			auto slash = fpath.find_last_of('/');
			std::string fname = (slash == std::string::npos) ? fpath : fpath.substr(slash + 1);
			std::string id = fname.substr(0, fname.size() - 3);

			// Read and decode msgpack
			DeserializationError derr;
			JsonDocument tmp;
			{
				FrLock fs(g_fsMutex);
				File f = _fs->open(fpath.c_str(), FILE_READ);
				if (f) {
					derr = deserializeMsgPack(tmp, f);
					f.close();
				} else {
					derr = DeserializationError::Code::InvalidInput;
				}
			}
			if (derr) continue; // skip unreadable
			JsonObject obj = arr.add<JsonObject>();
			obj.set(tmp.as<JsonObjectConst>());
			obj["_id"] = id.c_str();
		}
	}
	setLastError({DbStatusCode::Ok, ""});
	return snap;
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
	if (!st.ok()) return st;

	// For each collection, recreate documents
	for (auto kv : cols) {
		const char *colName = kv.key().c_str();
		if (!colName || !*colName) continue;
		if (isReservedName(colName)) continue;
		JsonArrayConst arr = kv.value().as<JsonArrayConst>();
		if (arr.isNull()) continue;

		// Ensure directory exists
		std::string dir = _baseDir;
		if (!dir.empty() && dir.back() != '/') dir += '/';
		dir += colName;
		{
			FrLock fs(g_fsMutex);
			fsEnsureDir(*_fs, dir);
		}

		for (JsonObjectConst obj : arr) {
			const char *id = obj["_id"].is<const char *>() ? obj["_id"].as<const char *>() : nullptr;
			if (!id || !*id) continue;

			// Copy object without _id into a temp doc
			JsonDocument tmp;
			tmp.to<JsonObject>().set(obj);
			tmp.remove("_id");

			// Serialize to MsgPack buffer
			size_t sz = measureMsgPack(tmp);
			JsonDbVector<uint8_t> bytes{JsonDbAllocator<uint8_t>(_cfg.usePSRAMBuffers)};
			bytes.resize(sz);
			size_t written = serializeMsgPack(tmp, bytes.data(), bytes.size());
			if (written != sz) return setLastError({DbStatusCode::IoError, "serialize msgpack failed"});

			// Write file atomically
			std::string finalPath = dir + "/" + std::string(id) + ".mp";
			std::string tmpPath = finalPath + ".tmp";
			{
				FrLock fs(g_fsMutex);
				File f = _fs->open(tmpPath.c_str(), FILE_WRITE);
				if (!f) return setLastError({DbStatusCode::IoError, "open for write failed"});
				WriteBufferingStream bufferedFile(f, 256);
				size_t w = bufferedFile.write(bytes.data(), bytes.size());
				bufferedFile.flush();
				f.close();
				if (w != bytes.size()) {
					_fs->remove(tmpPath.c_str());
					return setLastError({DbStatusCode::IoError, "write failed"});
				}
				if (!_fs->rename(tmpPath.c_str(), finalPath.c_str())) {
					_fs->remove(tmpPath.c_str());
					return setLastError({DbStatusCode::IoError, "rename failed"});
				}
			}
		}
	}

	// Refresh diag cache to reflect restored state
	refreshDiagFromFs();
	emitEvent(DBEventType::Sync);
	return setLastError({DbStatusCode::Ok, ""});
}

// Private: expensive FS scan; called on init and after successful sync
void ESPJsonDB::refreshDiagFromFs() {
	if (!_fs) return;
	std::map<std::string, uint32_t> perCol;
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
					if (slash != std::string::npos) cname = cname.substr(slash + 1);
					if (isReservedName(cname)) {
						f.close();
						continue;
					}
					f.close();

					// Count .mp files in collection dir
					std::string dirPath = _baseDir;
					if (!dirPath.empty() && dirPath.back() != '/') dirPath += '/';
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
						if (n.size() >= 3 && n.substr(n.size() - 3) == ".mp") ++cnt;
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
