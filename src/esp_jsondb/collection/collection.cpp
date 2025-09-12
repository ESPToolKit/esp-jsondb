#include "collection.h"
#include "../db.h"
#include "../utils/fs_utils.h"
#include "../utils/time_utils.h"

Collection::Collection(const std::string &name, const Schema &schema)
	: _name(name), _schema(schema) {}

DbResult<std::string> Collection::create(JsonObjectConst data) {
	DbResult<std::string> res{};
	JsonDocument workDoc;
	workDoc.set(data);
	JsonObject obj = workDoc.as<JsonObject>();
	if (_schema.hasValidate()) {
		auto ve = _schema.runPreSave(obj);
		if (!ve.valid) {
			res.status = {DbStatusCode::ValidationFailed, ve.message};
			dbSetLastError(res.status);
			return res;
		}
	}
	bool emit = false;
	{
		FrLock lk(_mu);
		auto rec = std::make_unique<DocumentRecord>();
		rec->meta.createdAt = nowUtcMs();
		rec->meta.updatedAt = rec->meta.createdAt;
		rec->meta.id = ObjectId().toHex();
		rec->meta.dirty = true;

		// Serialize input data to MsgPack
		size_t sz = measureMsgPack(obj);
		rec->msgpack.resize(sz);
		size_t written = serializeMsgPack(obj, rec->msgpack.data(), rec->msgpack.size());
		if (written != sz) {
			res.status = {DbStatusCode::IoError, "serialize msgpack failed"};
			dbSetLastError(res.status);
			return res;
		}

		const std::string id = rec->meta.id;
		_docs.emplace(id, std::move(rec));
		_dirty = true;

		res.status = {DbStatusCode::Ok, ""};
		dbSetLastError(res.status);
		res.value = id;
		emit = true;
	}
	if (emit) db.emitEvent(DBEventType::DocumentCreated);
	return res;
}

DbResult<DocView> Collection::findById(const std::string &id) {
	FrLock lk(_mu);
	auto it = _docs.find(id);
	if (it == _docs.end()) {
		DbStatus st{DbStatusCode::NotFound, "document not found"};
		dbSetLastError(st);
		return {st, DocView(nullptr, &_schema, &_mu)};
	}
	DbStatus st{DbStatusCode::Ok, ""};
	dbSetLastError(st);
	return {st, DocView(it->second.get(), &_schema, &_mu)};
}

DbResult<std::vector<DocView>> Collection::findMany(std::function<bool(const DocView &)> pred) {
	DbResult<std::vector<DocView>> res{};
	for (auto &kv : _docs) {
		DocView v(kv.second.get(), &_schema);
		if (!pred || pred(v)) {
			res.value.emplace_back(kv.second.get(), &_schema, &_mu);
		}
	}
	res.status = {DbStatusCode::Ok, ""};
	dbSetLastError(res.status);
	return res;
}

DbStatus Collection::updateById(const std::string &id, std::function<void(DocView &)> mutator) {
	bool updated = false;
	DbStatus st{DbStatusCode::Ok, ""};
	{
		FrLock lk(_mu);
		auto it = _docs.find(id);
		if (it == _docs.end()) {
			return dbSetLastError({DbStatusCode::NotFound, "document not found"});
		}
		DocView v(it->second.get(), &_schema, nullptr); // using outer lock
		mutator(v);
		if (_schema.hasValidate()) {
			auto obj = v.asObject();
			auto ve = _schema.runPreSave(obj);
			if (!ve.valid) {
				v.discard();
				return dbSetLastError({DbStatusCode::ValidationFailed, ve.message});
			}
		}
		st = v.commit();
		if (!st.ok()) return dbSetLastError(st);
		_dirty = true;
		updated = true;
	}
	if (updated) db.emitEvent(DBEventType::DocumentUpdated);
	return dbSetLastError(st);
}

DbStatus Collection::removeById(const std::string &id) {
	bool removed = false;
	{
		FrLock lk(_mu);
		auto it = _docs.find(id);
		if (it == _docs.end()) return dbSetLastError({DbStatusCode::NotFound, "document not found"});
		_deletedIds.push_back(id); // ensure file removal on sync
		_docs.erase(it);
		_dirty = true;
		removed = true;
	}
	if (removed) db.emitEvent(DBEventType::DocumentDeleted);
	return dbSetLastError({DbStatusCode::Ok, ""});
}

DbStatus Collection::writeDocToFile(const std::string &baseDir, const DocumentRecord &r) {
	FrLock fs(g_fsMutex);
	std::string dir = joinPath(baseDir, _name);
	if (!fsEnsureDir(dir)) {
		return dbSetLastError({DbStatusCode::IoError, "mkdir failed"});
	}
	std::string finalPath = joinPath(dir, r.meta.id + ".mp");
	std::string tmpPath = finalPath + ".tmp";
	// Write to temp then rename for atomicity
	File f = LittleFS.open(tmpPath.c_str(), FILE_WRITE);
	if (!f) return {DbStatusCode::IoError, "open for write failed"};
	// Buffer writes to coalesce small chunks if any
	WriteBufferingStream bufferedFile(f, 256);
	size_t w = bufferedFile.write(r.msgpack.data(), r.msgpack.size());
	bufferedFile.flush();
	f.close();
	if (w != r.msgpack.size()) {
		LittleFS.remove(tmpPath.c_str());
		return dbSetLastError({DbStatusCode::IoError, "write failed"});
	}
	if (!LittleFS.rename(tmpPath.c_str(), finalPath.c_str())) {
		LittleFS.remove(tmpPath.c_str());
		return dbSetLastError({DbStatusCode::IoError, "rename failed"});
	}
	return dbSetLastError({DbStatusCode::Ok, ""});
}

DbResult<std::unique_ptr<DocumentRecord>> Collection::readDocFromFile(const std::string &baseDir, const std::string &id) {
	DbResult<std::unique_ptr<DocumentRecord>> res{};
	std::string path = joinPath(joinPath(baseDir, _name), id + ".mp");
	FrLock fs(g_fsMutex);
	File f = LittleFS.open(path.c_str(), FILE_READ);
	if (!f) {
		res.status = {DbStatusCode::NotFound, "file not found"};
		dbSetLastError(res.status);
		return res;
	}
	auto rec = std::make_unique<DocumentRecord>();
	rec->meta.id = id;
	rec->meta.createdAt = nowUtcMs();
	rec->meta.updatedAt = rec->meta.createdAt;
	rec->meta.dirty = false;

	size_t sz = f.size();
	rec->msgpack.resize(sz);
	size_t r = f.read(rec->msgpack.data(), sz);
	f.close();
	if (r != sz) {
		res.status = {DbStatusCode::IoError, "read failed"};
		dbSetLastError(res.status);
		return res;
	}
	res.status = {DbStatusCode::Ok, ""};
	dbSetLastError(res.status);
	res.value = std::move(rec);
	return res;
}

DbStatus Collection::loadFromFs(const std::string &baseDir) {
	// First, under FS mutex, collect the list of document IDs to load.
	std::vector<std::string> ids;
	std::string dir = joinPath(baseDir, _name);
	{
		FrLock fs(g_fsMutex);
		if (!LittleFS.exists(dir.c_str())) {
			// Nothing to load yet; create directory lazily on write
			return dbSetLastError({DbStatusCode::Ok, ""});
		}
		File d = LittleFS.open(dir.c_str());
		if (!d || !d.isDirectory()) {
			return dbSetLastError({DbStatusCode::IoError, "open dir failed"});
		}
		for (File f = d.openNextFile(); f; f = d.openNextFile()) {
			if (f.isDirectory()) continue;
			String name = f.name();
			f.close();
			std::string n = name.c_str();
			// Expect <id>.mp
			if (n.size() >= 3 && n.substr(n.size() - 3) == ".mp") {
				ids.push_back(n.substr(0, n.size() - 3));
			}
		}
	}

	// Now, outside FS mutex, read each document file (readDocFromFile acquires FS mutex per file)
	for (const auto &id : ids) {
		auto rr = readDocFromFile(baseDir, id);
		if (rr.status.ok()) {
			_docs.emplace(id, std::move(rr.value));
		}
	}
	return dbSetLastError({DbStatusCode::Ok, ""});
}

DbStatus Collection::flushDirtyToFs(const std::string &baseDir, bool &didWork) {
	didWork = false;
	// Snapshot work under lock
	std::vector<std::string> toDelete;
	struct PendingWrite {
		std::string id;
		std::vector<uint8_t> bytes;
	};
	std::vector<PendingWrite> toWrite;
	{
		FrLock lk(_mu);
		toDelete.swap(_deletedIds);
		for (auto &kv : _docs) {
			auto &rec = kv.second;
			if (rec->meta.dirty) {
				toWrite.push_back(PendingWrite{rec->meta.id, rec->msgpack});
				rec->meta.dirty = false;
			}
		}
		_dirty = false;
	}

	// Process deletions (FS serialized by global mutex)
	if (!toDelete.empty()) {
		didWork = true;
		std::string dir = joinPath(baseDir, _name);
		for (const auto &id : toDelete) {
			std::string path = joinPath(dir, id + ".mp");
			{
				FrLock fs(g_fsMutex);
				if (LittleFS.exists(path.c_str())) {
					LittleFS.remove(path.c_str());
				}
			}
		}
	}

	// Flush writes
	for (auto &pw : toWrite) {
		DocumentRecord tmp;
		tmp.meta.id = pw.id;
		tmp.msgpack = std::move(pw.bytes);
		auto st = writeDocToFile(baseDir, tmp);
		if (!st.ok()) return dbSetLastError(st);
		didWork = true;
	}
	return dbSetLastError({DbStatusCode::Ok, ""});
}
