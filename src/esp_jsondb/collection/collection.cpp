#include "collection.h"
#include "../db.h"
#include "../utils/fs_utils.h"
#include "../utils/time_utils.h"

namespace {
std::shared_ptr<DocumentRecord> makeSharedDocumentRecord(bool usePSRAMBuffers) {
	return std::allocate_shared<DocumentRecord>(
	    JsonDbAllocator<DocumentRecord>(usePSRAMBuffers), usePSRAMBuffers
	);
}
} // namespace

Collection::Collection(
    ESPJsonDB &db,
    const std::string &name,
    const Schema &schema,
    std::string baseDir,
    bool cacheEnabled,
    bool usePSRAMBuffers,
    fs::FS &fs
)
    : _db(&db), _name(name), _schema(schema),
      _docs(DocIdLess{}, DocumentMapAllocator(usePSRAMBuffers)),
      _deletedIds(JsonDbAllocator<DocId>(usePSRAMBuffers)),
      _baseDir(std::move(baseDir)), _cacheEnabled(true), _usePSRAMBuffers(usePSRAMBuffers),
      _fs(&fs) {
	(void)cacheEnabled;
}

void Collection::setCacheEnabled(bool enabled) {
	(void)enabled;
	_cacheEnabled = true;
}

DbStatus Collection::recordStatus(const DbStatus &st) const {
	return _db ? _db->recordStatus(st) : st;
}

void Collection::emitEvent(DBEventType ev) const {
	if (_db)
		_db->emitEvent(ev);
}

void Collection::noteDeletedInDiag(size_t count) const {
	if (count == 0 || !_db)
		return;
	_db->noteDocumentDeleted(_name, static_cast<uint32_t>(count));
}

DbStatus Collection::checkUniqueFieldsInCache(JsonObjectConst obj, const DocId *selfId) {
	// Scan schema for fields marked unique and ensure no other doc has same value
	for (const auto &f : _schema.fields) {
		if (!f.unique)
			continue;
		// Only enforce on scalar types
		if (f.type == FieldType::Object || f.type == FieldType::Array)
			continue;
		JsonVariantConst v = obj[f.name];
		if (v.isNull())
			continue;
		for (const auto &kv : _docs) {
			if (selfId && kv.first == *selfId)
				continue;
			DocView other(kv.second, &_schema, nullptr, _db);
			JsonVariantConst ov = other[f.name];
			if (!ov.isNull() && ov == v) {
				return recordStatus({DbStatusCode::ValidationFailed, "unique constraint violated"});
			}
		}
	}
	return recordStatus({DbStatusCode::Ok, ""});
}

DbStatus Collection::checkUniqueFieldsOnDisk(JsonObjectConst obj, const DocId *selfId) {
	bool hasUnique = false;
	for (const auto &field : _schema.fields) {
		if (field.unique) {
			hasUnique = true;
			break;
		}
	}
	if (!hasUnique) {
		return recordStatus({DbStatusCode::Ok, ""});
	}
	// Enumerate all documents on disk and compare declared unique fields
	JsonDbVector<DocId> ids{JsonDbAllocator<DocId>(_usePSRAMBuffers)};
	std::string dir = joinPath(_baseDir, _name);
	{
		FrLock fs(g_fsMutex);
		if (_fs->exists(dir.c_str())) {
			File d = _fs->open(dir.c_str());
			if (!d || !d.isDirectory()) {
				return recordStatus({DbStatusCode::IoError, "open dir failed"});
			}
			for (File f = d.openNextFile(); f; f = d.openNextFile()) {
				if (f.isDirectory())
					continue;
				String name = f.name();
				std::string fname = name.c_str();
				if (fname.size() < 3 || fname.substr(fname.size() - 3) != ".mp")
					continue;
				DocId parsedId;
				if (parsedId.assign(fname.substr(0, fname.size() - 3))) {
					ids.push_back(parsedId);
				}
			}
		}
	}
	for (const auto &docId : ids) {
		if (selfId && docId == *selfId)
			continue;
		auto rr = readDocFromFile(_baseDir, docId.c_str());
		if (!rr.status.ok()) {
			return rr.status;
		}
		DocView view(rr.value, &_schema, nullptr, _db);
		for (const auto &field : _schema.fields) {
			if (!field.unique)
				continue;
			if (field.type == FieldType::Object || field.type == FieldType::Array)
				continue;
			JsonVariantConst newVal = obj[field.name];
			if (newVal.isNull())
				continue;
			JsonVariantConst existingVal = view[field.name];
			if (!existingVal.isNull() && existingVal == newVal) {
				return recordStatus({DbStatusCode::ValidationFailed, "unique constraint violated"});
			}
		}
	}
	return recordStatus({DbStatusCode::Ok, ""});
}

DbStatus Collection::checkUniqueFields(JsonObjectConst obj, const DocId *selfId) {
	return checkUniqueFieldsInCache(obj, selfId);
}

DbResult<std::string> Collection::create(JsonObjectConst data) {
	DbResult<std::string> res{};
	JsonDocument workDoc;
	workDoc.set(data);
	JsonObject obj = workDoc.as<JsonObject>();
	if (_schema.hasValidate()) {
		auto ve = _schema.runPreSave(obj);
		if (!ve.valid) {
			res.status = {DbStatusCode::ValidationFailed, ve.message};
			recordStatus(res.status);
			return res;
		}
	}
	bool emit = false;
	std::shared_ptr<DocumentRecord> rec;
	std::string id;
	{
		FrLock lk(_mu);
		// Enforce unique constraints before creating the record
		auto ust = checkUniqueFields(obj, nullptr);
		if (!ust.ok()) {
			res.status = ust;
			recordStatus(res.status);
			return res;
		}
		rec = makeSharedDocumentRecord(_usePSRAMBuffers);
		rec->meta.createdAt = nowUtcMs();
		rec->meta.updatedAt = rec->meta.createdAt;
		rec->meta.id = ObjectId().toDocId();
		rec->meta.dirty = true;

		// Serialize input data to MsgPack
		size_t sz = measureMsgPack(obj);
		rec->msgpack.resize(sz);
		size_t written = serializeMsgPack(obj, rec->msgpack.data(), rec->msgpack.size());
		if (written != sz) {
			res.status = {DbStatusCode::IoError, "serialize msgpack failed"};
			recordStatus(res.status);
			return res;
		}

		id = rec->meta.id.c_str();
		_docs.emplace(rec->meta.id, rec);
		_dirty = true;

		res.status = {DbStatusCode::Ok, ""};
		recordStatus(res.status);
		res.value = id;
		emit = true;
	}
	if (emit) {
		if (_db)
			_db->noteDocumentCreated(_name);
		emitEvent(DBEventType::DocumentCreated);
	}
	return res;
}

DbResult<std::string> Collection::create(const JsonDocument &data) {
	// Ensure provided document is an object
	if (!data.is<JsonObject>()) {
		DbResult<std::string> res{};
		res.status = recordStatus({DbStatusCode::InvalidArgument, "document must be an object"});
		return res;
	}
	return create(data.as<JsonObjectConst>());
}

DbResult<std::vector<std::string>> Collection::createMany(JsonArrayConst arr) {
	DbResult<std::vector<std::string>> res{};
	std::vector<std::string> ids;
	ids.reserve(arr.size());

	for (auto v : arr) {
		if (!v.is<JsonObjectConst>()) {
			// Skip non-object entries
			continue;
		}
		auto cr = create(v.as<JsonObjectConst>());
		if (cr.status.ok()) {
			ids.push_back(cr.value);
		}
	}

	res.status = {DbStatusCode::Ok, ""};
	recordStatus(res.status);
	res.value = std::move(ids);
	return res;
}

DbResult<std::vector<std::string>> Collection::createMany(const JsonDocument &arrDoc) {
	if (!arrDoc.is<JsonArray>()) {
		DbResult<std::vector<std::string>> res{};
		res.status =
		    recordStatus({DbStatusCode::InvalidArgument, "document must be an array of objects"});
		return res;
	}
	return createMany(arrDoc.as<JsonArrayConst>());
}

DbResult<DocView> Collection::findById(const std::string &id) {
	DocId lookupId;
	if (!lookupId.assign(id)) {
		DbStatus st{DbStatusCode::NotFound, "document not found"};
		recordStatus(st);
		return {st, DocView(nullptr, &_schema, &_mu, _db, nullptr, _usePSRAMBuffers)};
	}
	{
		FrLock lk(_mu);
		auto it = _docs.find(lookupId);
		if (it != _docs.end()) {
			DbStatus st{DbStatusCode::Ok, ""};
			recordStatus(st);
			return {st, makeView(it->second)};
		}
	}

	DbStatus st{DbStatusCode::NotFound, "document not found"};
	recordStatus(st);
	return {st, DocView(nullptr, &_schema, &_mu, _db, nullptr, _usePSRAMBuffers)};
}

DbResult<std::vector<DocView>> Collection::findMany(std::function<bool(const DocView &)> pred) {
	DbResult<std::vector<DocView>> res{};
	FrLock lk(_mu);
	for (auto &kv : _docs) {
		DocView v(kv.second, &_schema, nullptr, _db);
		if (!pred || pred(v)) {
			res.value.emplace_back(makeView(kv.second));
		}
	}
	res.status = {DbStatusCode::Ok, ""};
	recordStatus(res.status);
	return res;
}

DbResult<DocView> Collection::findOne(std::function<bool(const DocView &)> pred) {
	FrLock lk(_mu);
	for (auto &kv : _docs) {
		DocView v(kv.second, &_schema, nullptr, _db);
		if (!pred || pred(v)) {
			DbStatus st{DbStatusCode::Ok, ""};
			recordStatus(st);
			return {st, makeView(kv.second)};
		}
	}
	DbStatus st{DbStatusCode::NotFound, "document not found"};
	recordStatus(st);
	return {st, DocView(nullptr, &_schema, &_mu, _db, nullptr, _usePSRAMBuffers)};
}

DbResult<DocView> Collection::findOne(const JsonDocument &filter) {
	// Build an inline predicate from the filter object
	auto pred = [&](const DocView &v) {
		for (auto kv : filter.as<JsonObjectConst>()) {
			if (v[kv.key().c_str()] != kv.value()) {
				return false;
			}
		}
		return true;
	};
	return findOne(std::move(pred));
}

DbStatus Collection::updateOne(
    std::function<bool(const DocView &)> pred, std::function<void(DocView &)> mutator, bool create
) {
	bool updated = false;
	bool created = false;
	DbStatus st{DbStatusCode::NotFound, "document not found"};
	FrLock lk(_mu);
	// Search existing docs first
	for (auto &kv : _docs) {
		DocView v(kv.second, &_schema, nullptr, _db); // use outer lock
		if (!pred || pred(v)) {
			mutator(v);
			if (_schema.hasValidate()) {
				auto obj = v.asObject();
				auto ve = _schema.runPreSave(obj);
				if (!ve.valid) {
					v.discard();
					return recordStatus({DbStatusCode::ValidationFailed, ve.message});
				}
				// Unique constraints
				auto ust = checkUniqueFields(obj, &kv.second->meta.id);
				if (!ust.ok()) {
					v.discard();
					return recordStatus(ust);
				}
			}
			st = v.commit();
			if (!st.ok())
				return recordStatus(st);
			// Only flag collection and emit update if record actually changed
			if (kv.second->meta.dirty) {
				_dirty = true;
				updated = true;
			}
			break;
		}
	}

	// If not found and create requested, create a new record and apply mutator
	if (!updated && create) {
		auto rec = makeSharedDocumentRecord(_usePSRAMBuffers);
		rec->meta.createdAt = nowUtcMs();
		rec->meta.updatedAt = rec->meta.createdAt;
		rec->meta.id = ObjectId().toDocId();
		rec->meta.dirty = true;

		DocView v(rec, &_schema, nullptr, _db);
		// Initialize as empty object then let mutator fill values
		v.asObject();
		mutator(v);
		if (_schema.hasValidate()) {
			auto obj = v.asObject();
			auto ve = _schema.runPreSave(obj);
			if (!ve.valid) {
				v.discard();
				return recordStatus({DbStatusCode::ValidationFailed, ve.message});
			}
			// Unique constraints
			auto ust = checkUniqueFields(obj, &rec->meta.id);
			if (!ust.ok()) {
				v.discard();
				return recordStatus(ust);
			}
		}
		st = v.commit();
		if (!st.ok())
			return recordStatus(st);
		_docs.emplace(rec->meta.id, std::move(rec));
		_dirty = true;
		created = true;
		st = {DbStatusCode::Ok, ""};
	}
	if (created) {
		if (_db)
			_db->noteDocumentCreated(_name);
		emitEvent(DBEventType::DocumentCreated);
	} else if (updated) {
		emitEvent(DBEventType::DocumentUpdated);
	}
	return recordStatus(st);
}

DbStatus Collection::updateOne(const JsonDocument &filter, const JsonDocument &patch, bool create) {
	bool updated = false;
	bool created = false;
	DbStatus st{DbStatusCode::NotFound, "document not found"};
	FrLock lk(_mu);
	// Look for first matching doc
	for (auto &kv : _docs) {
		DocView v(kv.second, &_schema, nullptr, _db);
		bool match = true;
		for (auto kvf : filter.as<JsonObjectConst>()) {
			if (v[kvf.key().c_str()] != kvf.value()) {
				match = false;
				break;
			}
		}
		if (match) {
			for (auto kvp : patch.as<JsonObjectConst>()) {
				v[kvp.key().c_str()].set(kvp.value());
			}
			if (_schema.hasValidate()) {
				auto obj = v.asObject();
				auto ve = _schema.runPreSave(obj);
				if (!ve.valid) {
					v.discard();
					return recordStatus({DbStatusCode::ValidationFailed, ve.message});
				}
				// Unique constraints
				auto ust = checkUniqueFields(obj, &kv.second->meta.id);
				if (!ust.ok()) {
					v.discard();
					return recordStatus(ust);
				}
			}
			st = v.commit();
			if (!st.ok())
				return recordStatus(st);
			if (kv.second->meta.dirty) {
				_dirty = true;
				updated = true;
			}
			break;
		}
	}

	if (!updated && create) {
		// Create a new document merging filter and patch
		auto rec = makeSharedDocumentRecord(_usePSRAMBuffers);
		rec->meta.createdAt = nowUtcMs();
		rec->meta.updatedAt = rec->meta.createdAt;
		rec->meta.id = ObjectId().toDocId();
		rec->meta.dirty = true;

		DocView v(rec, &_schema, nullptr, _db);
		auto obj = v.asObject();
		for (auto kvf : filter.as<JsonObjectConst>()) {
			obj[kvf.key().c_str()] = kvf.value();
		}
		for (auto kvp : patch.as<JsonObjectConst>()) {
			obj[kvp.key().c_str()] = kvp.value();
		}
		if (_schema.hasValidate()) {
			auto ve = _schema.runPreSave(obj);
			if (!ve.valid) {
				v.discard();
				return recordStatus({DbStatusCode::ValidationFailed, ve.message});
			}
			// Unique constraints
			auto ust = checkUniqueFields(obj, &rec->meta.id);
			if (!ust.ok()) {
				v.discard();
				return recordStatus(ust);
			}
		}
		st = v.commit();
		if (!st.ok())
			return recordStatus(st);
		_docs.emplace(rec->meta.id, std::move(rec));
		_dirty = true;
		created = true;
		st = {DbStatusCode::Ok, ""};
	}
	if (created) {
		if (_db)
			_db->noteDocumentCreated(_name);
		emitEvent(DBEventType::DocumentCreated);
	} else if (updated) {
		emitEvent(DBEventType::DocumentUpdated);
	}
	return recordStatus(st);
}

DbStatus Collection::updateById(const std::string &id, std::function<void(DocView &)> mutator) {
	bool updated = false;
	DbStatus st{DbStatusCode::Ok, ""};
	DocId lookupId;
	if (!lookupId.assign(id)) {
		return recordStatus({DbStatusCode::NotFound, "document not found"});
	}
	FrLock lk(_mu);
	auto it = _docs.find(lookupId);
	if (it == _docs.end()) {
		return recordStatus({DbStatusCode::NotFound, "document not found"});
	}
	DocView v(it->second, &_schema, nullptr, _db); // using outer lock
	mutator(v);
	if (_schema.hasValidate()) {
		auto obj = v.asObject();
		auto ve = _schema.runPreSave(obj);
		if (!ve.valid) {
			v.discard();
			return recordStatus({DbStatusCode::ValidationFailed, ve.message});
		}
		// Unique constraints
		auto ust = checkUniqueFields(obj, &it->second->meta.id);
		if (!ust.ok()) {
			v.discard();
			return recordStatus(ust);
		}
	}
	st = v.commit();
	if (!st.ok())
		return recordStatus(st);
	if (it->second->meta.dirty) {
		_dirty = true;
		updated = true;
	}
	if (updated)
		emitEvent(DBEventType::DocumentUpdated);
	return recordStatus(st);
}

DbStatus Collection::removeById(const std::string &id) {
	bool removed = false;
	DbStatus st{DbStatusCode::Ok, ""};
	DocId lookupId;
	if (!lookupId.assign(id)) {
		return recordStatus({DbStatusCode::NotFound, "document not found"});
	}
	FrLock lk(_mu);
	auto it = _docs.find(lookupId);
	if (it == _docs.end())
		return recordStatus({DbStatusCode::NotFound, "document not found"});
	// Mark record as logically removed so outstanding views fail on commit
	it->second->meta.removed = true;
	_deletedIds.push_back(it->first); // ensure file removal on sync
	_docs.erase(it);
	_dirty = true;
	removed = true;
	if (removed) {
		if (_db)
			_db->noteDocumentDeleted(_name);
		emitEvent(DBEventType::DocumentDeleted);
	}
	return recordStatus(st);
}

DbStatus Collection::writeDocToFile(const std::string &baseDir, const DocumentRecord &r) {
	FrLock fs(g_fsMutex);
	std::string dir = joinPath(baseDir, _name);
	if (!fsEnsureDir(*_fs, dir)) {
		return recordStatus({DbStatusCode::IoError, "mkdir failed"});
	}
	std::string finalPath = joinPath(dir, std::string(r.meta.id.c_str()) + ".mp");
	std::string tmpPath = finalPath + ".tmp";
	// Write to temp then rename for atomicity
	File f = _fs->open(tmpPath.c_str(), FILE_WRITE);
	if (!f)
		return recordStatus({DbStatusCode::IoError, "open for write failed"});
	// Buffer writes to coalesce small chunks if any
	WriteBufferingStream bufferedFile(f, 256);
	size_t w = bufferedFile.write(r.msgpack.data(), r.msgpack.size());
	bufferedFile.flush();
	f.close();
	if (w != r.msgpack.size()) {
		_fs->remove(tmpPath.c_str());
		return recordStatus({DbStatusCode::IoError, "write failed"});
	}
	if (!_fs->rename(tmpPath.c_str(), finalPath.c_str())) {
		_fs->remove(tmpPath.c_str());
		return recordStatus({DbStatusCode::IoError, "rename failed"});
	}
	return recordStatus({DbStatusCode::Ok, ""});
}

DbResult<std::shared_ptr<DocumentRecord>>
Collection::readDocFromFile(const std::string &baseDir, const std::string &id) {
	DbResult<std::shared_ptr<DocumentRecord>> res{};
	std::string path = joinPath(joinPath(baseDir, _name), id + ".mp");
	FrLock fs(g_fsMutex);
	File f = _fs->open(path.c_str(), FILE_READ);
	if (!f) {
		res.status = {DbStatusCode::NotFound, "file not found"};
		recordStatus(res.status);
		return res;
	}
	auto rec = makeSharedDocumentRecord(_usePSRAMBuffers);
	if (!rec->meta.id.assign(id)) {
		res.status = {DbStatusCode::Corrupted, "invalid document id"};
		recordStatus(res.status);
		return res;
	}
	rec->meta.createdAt = nowUtcMs();
	rec->meta.updatedAt = rec->meta.createdAt;
	rec->meta.dirty = false;

	size_t sz = f.size();
	rec->msgpack.resize(sz);
	size_t r = f.read(rec->msgpack.data(), sz);
	f.close();
	if (r != sz) {
		res.status = {DbStatusCode::IoError, "read failed"};
		recordStatus(res.status);
		return res;
	}
	res.status = {DbStatusCode::Ok, ""};
	recordStatus(res.status);
	res.value = std::move(rec);
	return res;
}

JsonDbVector<DocId> Collection::listDocumentIdsFromFs() const {
	JsonDbVector<DocId> ids{JsonDbAllocator<DocId>(_usePSRAMBuffers)};
	std::string dir = joinPath(_baseDir, _name);
	{
		FrLock fs(g_fsMutex);
		if (!_fs->exists(dir.c_str()))
			return ids;
		File d = _fs->open(dir.c_str());
		if (!d || !d.isDirectory())
			return ids;
		for (File f = d.openNextFile(); f; f = d.openNextFile()) {
			if (f.isDirectory())
				continue;
			String name = f.name();
			std::string fname = name.c_str();
			if (fname.size() < 3 || fname.substr(fname.size() - 3) != ".mp")
				continue;
			DocId parsedId;
			if (parsedId.assign(fname.substr(0, fname.size() - 3))) {
				ids.push_back(parsedId);
			}
		}
	}
	return ids;
}

size_t Collection::countDocumentsFromFs() const {
	return listDocumentIdsFromFs().size();
}

DbStatus Collection::persistImmediate(const std::shared_ptr<DocumentRecord> &rec) {
	if (!rec) {
		return recordStatus({DbStatusCode::InvalidArgument, "no record"});
	}
	auto st = writeDocToFile(_baseDir, *rec);
	if (!st.ok())
		return st;
	FrLock lk(_mu);
	rec->meta.dirty = false;
	rec->meta.removed = false;
	_dirty = false;
	return recordStatus({DbStatusCode::Ok, ""});
}

DocView Collection::makeView(std::shared_ptr<DocumentRecord> rec) {
	return DocView(std::move(rec), &_schema, &_mu, _db, nullptr, _usePSRAMBuffers);
}

DbStatus Collection::updateOneNoCache(
    std::function<bool(const DocView &)> pred,
    std::function<void(DocView &)> mutator,
    bool create,
    bool &created,
    bool &updated
) {
	DbStatus st{DbStatusCode::NotFound, "document not found"};
	auto ids = listDocumentIdsFromFs();
	for (const auto &id : ids) {
		auto rr = readDocFromFile(_baseDir, id.c_str());
		if (!rr.status.ok()) {
			st = rr.status;
			continue;
		}
		auto view = makeView(rr.value);
		if (!pred || pred(view)) {
			mutator(view);
			if (_schema.hasValidate()) {
				auto obj = view.asObject();
				auto ve = _schema.runPreSave(obj);
				if (!ve.valid) {
					view.discard();
					return recordStatus({DbStatusCode::ValidationFailed, ve.message});
				}
				auto ust = checkUniqueFields(obj, &rr.value->meta.id);
				if (!ust.ok()) {
					view.discard();
					return recordStatus(ust);
				}
			}
			st = view.commit();
			if (!st.ok())
				return recordStatus(st);
			updated = true;
			return recordStatus(st);
		}
	}
	if (create) {
		auto rec = makeSharedDocumentRecord(_usePSRAMBuffers);
		rec->meta.createdAt = nowUtcMs();
		rec->meta.updatedAt = rec->meta.createdAt;
		rec->meta.id = ObjectId().toDocId();
		rec->meta.dirty = true;
		auto view = makeView(rec);
		view.asObject();
		mutator(view);
		if (_schema.hasValidate()) {
			auto obj = view.asObject();
			auto ve = _schema.runPreSave(obj);
			if (!ve.valid) {
				view.discard();
				return recordStatus({DbStatusCode::ValidationFailed, ve.message});
			}
			auto ust = checkUniqueFields(obj, &rec->meta.id);
			if (!ust.ok()) {
				view.discard();
				return recordStatus(ust);
			}
		}
		st = view.commit();
		if (!st.ok())
			return recordStatus(st);
		created = true;
		return recordStatus(st);
	}
	return recordStatus(st);
}

DbStatus Collection::updateOneJsonNoCache(
    const JsonDocument &filter, const JsonDocument &patch, bool create, bool &created, bool &updated
) {
	DbStatus st{DbStatusCode::NotFound, "document not found"};
	auto ids = listDocumentIdsFromFs();
	for (const auto &id : ids) {
		auto rr = readDocFromFile(_baseDir, id.c_str());
		if (!rr.status.ok()) {
			st = rr.status;
			continue;
		}
		auto view = makeView(rr.value);
		bool match = true;
		for (auto kvf : filter.as<JsonObjectConst>()) {
			if (view[kvf.key().c_str()] != kvf.value()) {
				match = false;
				break;
			}
		}
		if (!match)
			continue;
		for (auto kvp : patch.as<JsonObjectConst>()) {
			view[kvp.key().c_str()].set(kvp.value());
		}
		if (_schema.hasValidate()) {
			auto obj = view.asObject();
			auto ve = _schema.runPreSave(obj);
			if (!ve.valid) {
				view.discard();
				return recordStatus({DbStatusCode::ValidationFailed, ve.message});
			}
			auto ust = checkUniqueFields(obj, &rr.value->meta.id);
			if (!ust.ok()) {
				view.discard();
				return recordStatus(ust);
			}
		}
		st = view.commit();
		if (!st.ok())
			return recordStatus(st);
		updated = true;
		return recordStatus(st);
	}
	if (create) {
		auto rec = makeSharedDocumentRecord(_usePSRAMBuffers);
		rec->meta.createdAt = nowUtcMs();
		rec->meta.updatedAt = rec->meta.createdAt;
		rec->meta.id = ObjectId().toDocId();
		rec->meta.dirty = true;
		auto view = makeView(rec);
		auto obj = view.asObject();
		for (auto kvf : filter.as<JsonObjectConst>()) {
			obj[kvf.key().c_str()] = kvf.value();
		}
		for (auto kvp : patch.as<JsonObjectConst>()) {
			obj[kvp.key().c_str()] = kvp.value();
		}
		if (_schema.hasValidate()) {
			auto ve = _schema.runPreSave(obj);
			if (!ve.valid) {
				view.discard();
				return recordStatus({DbStatusCode::ValidationFailed, ve.message});
			}
			auto ust = checkUniqueFields(obj, &rec->meta.id);
			if (!ust.ok()) {
				view.discard();
				return recordStatus(ust);
			}
		}
		st = view.commit();
		if (!st.ok())
			return recordStatus(st);
		created = true;
		return recordStatus(st);
	}
	return recordStatus(st);
}

DbStatus Collection::updateByIdNoCache(
    const std::string &id, std::function<void(DocView &)> mutator, bool &updated
) {
	auto rr = readDocFromFile(_baseDir, id);
	if (!rr.status.ok())
		return recordStatus(rr.status);
	auto view = makeView(rr.value);
	mutator(view);
	if (_schema.hasValidate()) {
		auto obj = view.asObject();
		auto ve = _schema.runPreSave(obj);
		if (!ve.valid) {
			view.discard();
			return recordStatus({DbStatusCode::ValidationFailed, ve.message});
		}
		auto ust = checkUniqueFields(obj, &rr.value->meta.id);
		if (!ust.ok()) {
			view.discard();
			return recordStatus(ust);
		}
	}
	auto st = view.commit();
	if (!st.ok())
		return recordStatus(st);
	updated = true;
	return recordStatus(st);
}

DbStatus Collection::removeByIdNoCache(const std::string &id, bool &removed) {
	std::string path = joinPath(joinPath(_baseDir, _name), id + ".mp");
	{
		FrLock fs(g_fsMutex);
		if (!_fs->exists(path.c_str())) {
			return recordStatus({DbStatusCode::NotFound, "document not found"});
		}
		if (!_fs->remove(path.c_str())) {
			return recordStatus({DbStatusCode::IoError, "remove failed"});
		}
	}
	removed = true;
	return recordStatus({DbStatusCode::Ok, ""});
}

DbStatus Collection::loadFromFs(const std::string &baseDir) {
	// First, under FS mutex, collect the list of document IDs to load.
	JsonDbVector<DocId> ids{JsonDbAllocator<DocId>(_usePSRAMBuffers)};
	std::string dir = joinPath(baseDir, _name);
	{
		FrLock fs(g_fsMutex);
		if (!_fs->exists(dir.c_str())) {
			// Nothing to load yet; create directory lazily on write
			return recordStatus({DbStatusCode::Ok, ""});
		}
		File d = _fs->open(dir.c_str());
		if (!d || !d.isDirectory()) {
			return recordStatus({DbStatusCode::IoError, "open dir failed"});
		}
		for (File f = d.openNextFile(); f; f = d.openNextFile()) {
			if (f.isDirectory())
				continue;
			String name = f.name();
			f.close();
			std::string n = name.c_str();
			// Expect <id>.mp
			if (n.size() >= 3 && n.substr(n.size() - 3) == ".mp") {
				DocId parsedId;
				if (parsedId.assign(n.substr(0, n.size() - 3))) {
					ids.push_back(parsedId);
				}
			}
		}
	}

	// Now, outside FS mutex, read each document file (readDocFromFile acquires FS mutex per file)
	for (const auto &id : ids) {
		auto rr = readDocFromFile(baseDir, id.c_str());
		if (rr.status.ok()) {
			_docs.emplace(rr.value->meta.id, std::move(rr.value));
		}
	}
	return recordStatus({DbStatusCode::Ok, ""});
}

DbStatus Collection::flushDirtyToFs(const std::string &baseDir, bool &didWork) {
	didWork = false;
	// Snapshot work under lock
	JsonDbVector<DocId> toDelete{JsonDbAllocator<DocId>(_usePSRAMBuffers)};
	struct PendingWrite {
		DocId id;
		JsonDbVector<uint8_t> bytes;

		explicit PendingWrite(bool usePSRAMBuffers)
		    : bytes(JsonDbAllocator<uint8_t>(usePSRAMBuffers)) {
		}
	};
	JsonDbVector<PendingWrite> toWrite{JsonDbAllocator<PendingWrite>(_usePSRAMBuffers)};
	{
		FrLock lk(_mu);
		toDelete.swap(_deletedIds);
		for (auto &kv : _docs) {
			auto &rec = kv.second;
			if (rec->meta.dirty) {
				PendingWrite pending(_usePSRAMBuffers);
				pending.id = rec->meta.id;
				pending.bytes = rec->msgpack;
				toWrite.push_back(std::move(pending));
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
			std::string path = joinPath(dir, std::string(id.c_str()) + ".mp");
			{
				FrLock fs(g_fsMutex);
				if (_fs->exists(path.c_str())) {
					_fs->remove(path.c_str());
				}
			}
		}
	}

	// Flush writes
	for (auto &pw : toWrite) {
		DocumentRecord tmp(_usePSRAMBuffers);
		tmp.meta.id = pw.id;
		tmp.msgpack = std::move(pw.bytes);
		auto st = writeDocToFile(baseDir, tmp);
		if (!st.ok())
			return recordStatus(st);
		didWork = true;
	}
	return recordStatus({DbStatusCode::Ok, ""});
}
