#include "db.h"
#include "utils/fs_utils.h"
#include <StreamUtils.h>
FrMutex g_fsMutex; // definition of global FS mutex

DbStatus DataBase::ensureFsReady() {
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
	return setLastError({DbStatusCode::Ok, ""});
}

DataBase::~DataBase() {
	stopSyncTaskUnlocked();
}

DbStatus DataBase::init(const char *baseDir, const SyncConfig &cfg) {
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
	auto st = ensureFsReady();
	if (!st.ok()) return st;

	// Initial diag refresh from FS (once). This avoids getDiag() touching FS later.
	refreshDiagFromFs();

	if (_cfg.coldSync) {
		auto preloadStatus = preloadCollectionsFromFs();
		if (!preloadStatus.ok()) return preloadStatus;
	}

	if (_cfg.autosync) {
		{
			FrLock lk(_mu);
			startSyncTaskUnlocked();
		}
	}
	return setLastError({DbStatusCode::Ok, ""});
}

DbStatus DataBase::registerSchema(const std::string &name, const Schema &s) {
	FrLock lk(_mu);
	_schemas[name] = s;
	return setLastError({DbStatusCode::Ok, ""});
}

DbStatus DataBase::unRegisterSchema(const std::string &name) {
	FrLock lk(_mu);
	_schemas.erase(name);
	return setLastError({DbStatusCode::Ok, ""});
}

void DataBase::onEvent(const std::function<void(DBEventType)> &cb) {
	FrLock lk(_mu);
	_eventCbs.push_back(cb);
}

void DataBase::onError(const std::function<void(const DbStatus &)> &cb) {
	FrLock lk(_mu);
	_errorCbs.push_back(cb);
}

void DataBase::onSync(const std::function<void()> &cb) {
	// Wrap sync-only callback into event form
	if (!cb) return;
	onEvent([cb](DBEventType ev) { if (ev == DBEventType::Sync) cb(); });
}

DbStatus DataBase::dropCollection(const std::string &name) {
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
	auto dit = _diagCache.docsPerCollection.find(name);
	if (dit != _diagCache.docsPerCollection.end()) {
		_diagCache.docsPerCollection.erase(dit);
		if (_diagCache.collections > 0) --_diagCache.collections;
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

DbResult<Collection *> DataBase::collection(const std::string &name) {
	DbResult<Collection *> res{};
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
	auto col = std::make_unique<Collection>(*this, name, sc, _baseDir, _cfg.cacheEnabled, *_fs);
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
DbResult<Collection *> DataBase::collection(const String &name) {
	return collection(std::string{name.c_str()});
}

DbResult<Collection *> DataBase::collection(const char *name) {
	return collection(std::string{name ? name : ""});
}

DbResult<std::string> DataBase::create(const std::string &name, JsonObjectConst doc) {
	DbResult<std::string> res{};
	auto cr = collection(name);
	if (!cr.status.ok()) {
		res.status = cr.status;
		return res;
	}
	return cr.value->create(doc);
}

DbResult<std::string> DataBase::create(const std::string &name, const JsonDocument &doc) {
	// Ensure the provided document is a JSON object (not an array/scalar)
	if (!doc.is<JsonObject>()) {
		DbResult<std::string> res{};
		res.status = setLastError({DbStatusCode::InvalidArgument, "document must be an object"});
		return res;
	}
	return create(name, doc.as<JsonObjectConst>());
}

DbResult<std::vector<std::string>> DataBase::createMany(const std::string &name, JsonArrayConst arr) {
	DbResult<std::vector<std::string>> res{};
	auto cr = collection(name);
	if (!cr.status.ok()) {
		res.status = cr.status;
		return res;
	}
	return cr.value->createMany(arr);
}

DbResult<std::vector<std::string>> DataBase::createMany(const std::string &name, const JsonDocument &arrDoc) {
	if (!arrDoc.is<JsonArray>()) {
		DbResult<std::vector<std::string>> res{};
		res.status = setLastError({DbStatusCode::InvalidArgument, "document must be an array of objects"});
		return res;
	}
	return createMany(name, arrDoc.as<JsonArrayConst>());
}

DbResult<DocView> DataBase::findById(const std::string &name, const std::string &id) {
	auto cr = collection(name);
	if (!cr.status.ok()) {
		// Return placeholder DocView; caller should check status before use
		return {cr.status, DocView(nullptr, nullptr, nullptr, this)};
	}
	return cr.value->findById(id);
}

DbResult<std::vector<DocView>> DataBase::findMany(const std::string &name,
												  std::function<bool(const DocView &)> pred) {
	DbResult<std::vector<DocView>> res{};
	auto cr = collection(name);
	if (!cr.status.ok()) {
		res.status = cr.status;
		return res;
	}
	return cr.value->findMany(std::move(pred));
}

DbResult<DocView> DataBase::findOne(const std::string &name, std::function<bool(const DocView &)> pred) {
	auto cr = collection(name);
	if (!cr.status.ok()) {
		// Return placeholder DocView; caller should check status before use
		return {cr.status, DocView(nullptr, nullptr, nullptr, this)};
	}
	return cr.value->findOne(std::move(pred));
}

DbResult<DocView> DataBase::findOne(const std::string &name, const JsonDocument &filter) {
	auto cr = collection(name);
	if (!cr.status.ok()) {
		// Return placeholder DocView; caller should check status before use
		return {cr.status, DocView(nullptr, nullptr, nullptr, this)};
	}
	return cr.value->findOne(filter);
}

DbStatus DataBase::updateOne(const std::string &name,
							 std::function<bool(const DocView &)> pred,
							 std::function<void(DocView &)> mutator,
							 bool create) {
	auto cr = collection(name);
	if (!cr.status.ok()) {
		return cr.status;
	}
	return cr.value->updateOne(std::move(pred), std::move(mutator), create);
}

DbStatus DataBase::updateOne(const std::string &name,
							 const JsonDocument &filter,
							 const JsonDocument &patch,
							 bool create) {
	auto cr = collection(name);
	if (!cr.status.ok()) {
		return cr.status;
	}
	return cr.value->updateOne(filter, patch, create);
}

DbStatus DataBase::updateById(const std::string &name, const std::string &id, std::function<void(DocView &)> mutator) {
	auto cr = collection(name);
	if (!cr.status.ok()) {
		return cr.status;
	}
	return cr.value->updateById(id, std::move(mutator));
}

DbStatus DataBase::removeById(const std::string &name, const std::string &id) {
	auto cr = collection(name);
	if (!cr.status.ok()) {
		return cr.status;
	}
	return cr.value->removeById(id);
}

DbResult<size_t> DataBase::updateMany(const std::string &collectionName,
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

DbStatus DataBase::syncNow() {
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
		refreshDiagFromFs();
		emitEvent(DBEventType::Sync);
	}
	if (!finalStatus.ok()) return finalStatus;
	return setLastError({DbStatusCode::Ok, ""});
}

void DataBase::syncTaskThunk(void *arg) {
	auto *self = static_cast<DataBase *>(arg);
	self->syncTaskLoop();
}

void DataBase::syncTaskLoop() {
	for (;;) {
		vTaskDelay(pdMS_TO_TICKS(_cfg.intervalMs));
		(void)syncNow();
	}
}

void DataBase::startSyncTaskUnlocked() {
	if (_syncTask != nullptr) return;
	xTaskCreatePinnedToCore(&DataBase::syncTaskThunk, "db.sync", _cfg.stackSize, this, _cfg.priority, &_syncTask, _cfg.coreId);
}

void DataBase::stopSyncTaskUnlocked() {
	if (_syncTask) {
		TaskHandle_t t = _syncTask;
		_syncTask = nullptr;
		vTaskDelete(t);
	}
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

DbStatus DataBase::removeCollectionDir(const std::string &name) {
	std::string dir = _baseDir;
	if (!dir.empty() && dir.back() != '/') dir += '/';
	dir += name;
	if (_fs) removeTree(*_fs, dir);
	return setLastError({DbStatusCode::Ok, ""});
}

void DataBase::emitEvent(DBEventType ev) {
	std::vector<std::function<void(DBEventType)>> callbacks;
	{
		FrLock lk(_mu);
		callbacks = _eventCbs; // copy snapshot
	}
	for (auto &fn : callbacks) {
		if (fn) fn(ev);
	}
}

void DataBase::emitError(const DbStatus &st) {
	std::vector<std::function<void(const DbStatus &)>> callbacks;
	{
		FrLock lk(_mu);
		callbacks = _errorCbs; // copy snapshot
	}
	for (auto &fn : callbacks) {
		if (fn) fn(st);
	}
}

DbStatus DataBase::preloadCollectionsFromFs() {
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

JsonDocument DataBase::getDiag() {
	// Build diagnostics from cached FS snapshot, overlapped with live loaded collections
	// No filesystem access here.
	JsonDocument doc;

	// Snapshot state under lock
	std::map<std::string, uint32_t> cached;
	std::map<std::string, uint32_t> live;
	uint32_t lastRefreshMs = 0;
	// Copy of configuration for reporting
	SyncConfig cfgCopy{};
	std::string baseDirCopy;
	{
		FrLock lk(_mu);
		cached = _diagCache.docsPerCollection; // copy
		lastRefreshMs = _diagCache.lastRefreshMs;
		for (auto &kv : _cols) {
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
		if (seen.find(kv.first) != seen.end()) continue;
		per[kv.first.c_str()] = kv.second;
	}

	// Collections = number of unique keys
	uint32_t collections = static_cast<uint32_t>(seen.size());
	for (auto &kv : cached) {
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

	setLastError({DbStatusCode::Ok, ""});
	return doc;
}

DbStatus DataBase::dropAll() {
	bool shouldRestart = false;
	{
		FrLock lk(_mu);
		// Stop autosync task to avoid races while removing files
		shouldRestart = _cfg.autosync;
		stopSyncTaskUnlocked();

		// Clear in-memory state
		for (auto &kv : _cols) {
			if (kv.second) kv.second->markAllRemoved();
		}
		_cols.clear();
		_colsToDelete.clear();
		_diagCache.docsPerCollection.clear();
		_diagCache.collections = 0;
	}

	// Remove base directory tree and recreate base dir
	{
		std::string base = _baseDir;
		if (_fs) removeTree(*_fs, base);
	}
	auto st = ensureFsReady();
	if (!st.ok()) return st;

	// Refresh diagnostics (should be empty)
	refreshDiagFromFs();

	// Restart autosync if it was enabled
	if (shouldRestart) {
		FrLock lk(_mu);
		if (_cfg.autosync) startSyncTaskUnlocked();
	}

	// Emit a single Sync event to inform listeners state changed
	emitEvent(DBEventType::Sync);
	return setLastError({DbStatusCode::Ok, ""});
}

std::vector<std::string> DataBase::getAllCollectionName() {
	std::vector<std::string> names;
	// Use a set to avoid duplicates
	std::map<std::string, bool> seen;
	{
		FrLock lk(_mu);
		for (auto &kv : _cols) {
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
			seen[name] = true;
		}
	}
	names.reserve(seen.size());
	for (auto &kv : seen)
		names.push_back(kv.first);
	return names;
}

DbStatus DataBase::changeConfig(const SyncConfig &cfg) {
	bool doColdSync = cfg.coldSync;
	bool shouldStart = false;
	// Stop existing task if running and apply new config
	{
		FrLock lk(_mu);
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

JsonDocument DataBase::getSnapshot() {
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

DbStatus DataBase::restoreFromSnapshot(const JsonDocument &snapshot) {
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
			std::vector<uint8_t> bytes;
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
void DataBase::refreshDiagFromFs() {
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
	}
}
