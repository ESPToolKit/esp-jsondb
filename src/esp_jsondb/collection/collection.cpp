#include "collection.h"
#include "../db.h"
#include "../utils/fs_utils.h"
#include "../utils/time_utils.h"

#include <cstdio>

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
    const CollectionConfig &config,
    bool usePSRAMBuffers,
    fs::FS &fs
)
    : _db(&db), _name(name), _schema(schema), _config(config),
      _docs(DocIdLess{}, DocumentMapAllocator(usePSRAMBuffers)),
      _deletedIds(JsonDbAllocator<DocId>(usePSRAMBuffers)),
      _baseDir(std::move(baseDir)), _usePSRAMBuffers(usePSRAMBuffers), _fs(&fs),
      _recordStore(fs, usePSRAMBuffers),
      _uniqueIndexes(std::less<std::string>{},
                     JsonDbAllocator<std::pair<const std::string, UniqueValueMap>>(usePSRAMBuffers)) {
}

void Collection::setConfig(const CollectionConfig &config) {
	FrLock lk(_mu);
	_config = config;
}

void Collection::setSchema(const Schema &schema) {
	FrLock lk(_mu);
	_schema = schema;
	(void)rebuildUniqueIndexesLocked();
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

std::string Collection::collectionDir() const {
	return joinPath(_baseDir, _name);
}

std::string Collection::uniqueValueKey(const SchemaField &field, JsonVariantConst value) const {
	if (value.isNull())
		return {};
	char buffer[48];
	switch (field.type) {
	case FieldType::String:
		return std::string("s:") + value.as<std::string>();
	case FieldType::Int32:
		return std::string("i32:") + std::to_string(value.as<int32_t>());
	case FieldType::Int64:
		return std::string("i64:") + std::to_string(value.as<int64_t>());
	case FieldType::UInt32:
		return std::string("u32:") + std::to_string(value.as<uint32_t>());
	case FieldType::UInt64:
		return std::string("u64:") + std::to_string(value.as<uint64_t>());
	case FieldType::Float:
		snprintf(buffer, sizeof(buffer), "f:%0.7g", static_cast<double>(value.as<float>()));
		return buffer;
	case FieldType::Double:
		snprintf(buffer, sizeof(buffer), "d:%0.17g", value.as<double>());
		return buffer;
	case FieldType::Bool:
		return value.as<bool>() ? "b:true" : "b:false";
	case FieldType::Object:
	case FieldType::Array:
		break;
	}
	return {};
}

DbStatus Collection::addUniqueValuesLocked(JsonObjectConst obj, const DocId &id) {
	for (const auto &field : _schema.fields) {
		if (!field.unique || field.type == FieldType::Object || field.type == FieldType::Array)
			continue;
		const auto key = uniqueValueKey(field, obj[field.name]);
		if (key.empty())
			continue;
		auto &fieldIndex = _uniqueIndexes[field.name ? field.name : ""];
		auto it = fieldIndex.find(key);
		if (it != fieldIndex.end() && it->second != id) {
			return {DbStatusCode::ValidationFailed, "unique constraint violated"};
		}
		fieldIndex[key] = id;
	}
	return {DbStatusCode::Ok, ""};
}

void Collection::removeUniqueValuesLocked(JsonObjectConst obj, const DocId &id) {
	for (const auto &field : _schema.fields) {
		if (!field.unique || field.type == FieldType::Object || field.type == FieldType::Array)
			continue;
		const auto fieldName = field.name ? std::string(field.name) : std::string{};
		auto fieldIt = _uniqueIndexes.find(fieldName);
		if (fieldIt == _uniqueIndexes.end())
			continue;
		const auto key = uniqueValueKey(field, obj[field.name]);
		if (key.empty())
			continue;
		auto valueIt = fieldIt->second.find(key);
		if (valueIt != fieldIt->second.end() && valueIt->second == id) {
			fieldIt->second.erase(valueIt);
		}
		if (fieldIt->second.empty()) {
			_uniqueIndexes.erase(fieldIt);
		}
	}
}

DbStatus Collection::rebuildUniqueIndexesLocked() {
	_uniqueIndexes.clear();
	for (const auto &kv : _docs) {
		JsonDocument doc;
		if (!kv.second || kv.second->msgpack.empty()) {
			continue;
		}
		auto err = deserializeMsgPack(doc, kv.second->msgpack.data(), kv.second->msgpack.size());
		if (err) {
			return {DbStatusCode::CorruptionDetected, "msgpack decode failed while rebuilding index"};
		}
		auto st = addUniqueValuesLocked(doc.as<JsonObjectConst>(), kv.first);
		if (!st.ok())
			return st;
	}
	return {DbStatusCode::Ok, ""};
}

DbStatus Collection::checkUniqueFieldsInCache(JsonObjectConst obj, const DocId *selfId) {
	for (const auto &f : _schema.fields) {
		if (!f.unique)
			continue;
		if (f.type == FieldType::Object || f.type == FieldType::Array)
			continue;
		const std::string fieldName = f.name ? std::string(f.name) : std::string{};
		auto fit = _uniqueIndexes.find(fieldName);
		if (fit == _uniqueIndexes.end())
			continue;
		const std::string key = uniqueValueKey(f, obj[f.name]);
		if (key.empty())
			continue;
		auto vit = fit->second.find(key);
		if (vit != fit->second.end() && (!selfId || vit->second != *selfId)) {
			return recordStatus({DbStatusCode::ValidationFailed, "unique constraint violated"});
		}
	}
	return recordStatus({DbStatusCode::Ok, ""});
}

DbStatus Collection::checkUniqueFieldsOnDisk(JsonObjectConst obj, const DocId *selfId) {
	return checkUniqueFieldsInCache(obj, selfId);
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
		rec->meta.createdAtMs = nowUtcMs();
		rec->meta.updatedAtMs = rec->meta.createdAtMs;
		rec->meta.id = ObjectId().toDocId();
		rec->meta.revision = 1;
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
		auto uniqueStatus = addUniqueValuesLocked(obj, rec->meta.id);
		if (!uniqueStatus.ok()) {
			_docs.erase(rec->meta.id);
			res.status = uniqueStatus;
			recordStatus(res.status);
			return res;
		}
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
			JsonDocument beforeDoc;
			beforeDoc.set(v.asObjectConst());
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
			removeUniqueValuesLocked(beforeDoc.as<JsonObjectConst>(), kv.second->meta.id);
			auto uniqueStatus = addUniqueValuesLocked(v.asObjectConst(), kv.second->meta.id);
			if (!uniqueStatus.ok())
				return recordStatus(uniqueStatus);
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
		rec->meta.createdAtMs = nowUtcMs();
		rec->meta.updatedAtMs = rec->meta.createdAtMs;
		rec->meta.id = ObjectId().toDocId();
		rec->meta.revision = 0;
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
		auto uniqueStatus = addUniqueValuesLocked(v.asObjectConst(), v.meta().id);
		if (!uniqueStatus.ok())
			return recordStatus(uniqueStatus);
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
			JsonDocument beforeDoc;
			beforeDoc.set(v.asObjectConst());
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
			removeUniqueValuesLocked(beforeDoc.as<JsonObjectConst>(), kv.second->meta.id);
			auto uniqueStatus = addUniqueValuesLocked(v.asObjectConst(), kv.second->meta.id);
			if (!uniqueStatus.ok())
				return recordStatus(uniqueStatus);
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
		rec->meta.createdAtMs = nowUtcMs();
		rec->meta.updatedAtMs = rec->meta.createdAtMs;
		rec->meta.id = ObjectId().toDocId();
		rec->meta.revision = 0;
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
		auto uniqueStatus = addUniqueValuesLocked(v.asObjectConst(), v.meta().id);
		if (!uniqueStatus.ok())
			return recordStatus(uniqueStatus);
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
	JsonDocument beforeDoc;
	beforeDoc.set(v.asObjectConst());
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
	removeUniqueValuesLocked(beforeDoc.as<JsonObjectConst>(), it->second->meta.id);
	auto uniqueStatus = addUniqueValuesLocked(v.asObjectConst(), it->second->meta.id);
	if (!uniqueStatus.ok())
		return recordStatus(uniqueStatus);
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
	DocView view(it->second, &_schema, nullptr, _db);
	removeUniqueValuesLocked(view.asObjectConst(), it->first);
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
	(void)baseDir;
	return recordStatus(_recordStore.write(collectionDir(), r));
}

DbResult<std::shared_ptr<DocumentRecord>>
Collection::readDocFromFile(const std::string &baseDir, const std::string &id) {
	(void)baseDir;
	auto res = _recordStore.read(collectionDir(), id);
	recordStatus(res.status);
	return res;
}

JsonDbVector<DocId> Collection::listDocumentIdsFromFs() const {
	return _recordStore.listIds(collectionDir());
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
		rec->meta.createdAtMs = nowUtcMs();
		rec->meta.updatedAtMs = rec->meta.createdAtMs;
		rec->meta.id = ObjectId().toDocId();
		rec->meta.revision = 0;
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
		rec->meta.createdAtMs = nowUtcMs();
		rec->meta.updatedAtMs = rec->meta.createdAtMs;
		rec->meta.id = ObjectId().toDocId();
		rec->meta.revision = 0;
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
	DocId docId;
	if (!docId.assign(id)) {
		return recordStatus({DbStatusCode::NotFound, "document not found"});
	}
	auto st = _recordStore.remove(collectionDir(), docId);
	if (!st.ok())
		return recordStatus(st);
	removed = true;
	return recordStatus({DbStatusCode::Ok, ""});
}

DbStatus Collection::loadFromFs(const std::string &baseDir) {
	// First, under FS mutex, collect the list of document IDs to load.
	JsonDbVector<DocId> ids{JsonDbAllocator<DocId>(_usePSRAMBuffers)};
	(void)baseDir;
	ids = listDocumentIdsFromFs();

	// Now, outside FS mutex, read each document file (readDocFromFile acquires FS mutex per file)
	for (const auto &id : ids) {
		auto rr = readDocFromFile(_baseDir, id.c_str());
		if (rr.status.ok()) {
			_docs.emplace(rr.value->meta.id, std::move(rr.value));
		}
	}
	{
		FrLock lk(_mu);
		auto indexStatus = rebuildUniqueIndexesLocked();
		if (!indexStatus.ok())
			return recordStatus(indexStatus);
	}
	return recordStatus({DbStatusCode::Ok, ""});
}

DbStatus Collection::flushDirtyToFs(const std::string &baseDir, bool &didWork) {
	didWork = false;
	// Snapshot work under lock
	JsonDbVector<DocId> toDelete{JsonDbAllocator<DocId>(_usePSRAMBuffers)};
	struct PendingWrite {
		DocumentMeta meta;
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
				pending.meta = rec->meta;
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
		for (const auto &id : toDelete) {
			auto st = _recordStore.remove(collectionDir(), id);
			if (!st.ok() && st.code != DbStatusCode::NotFound)
				return recordStatus({DbStatusCode::IoError, "document delete failed"});
		}
	}

	// Flush writes
	for (auto &pw : toWrite) {
		DocumentRecord tmp(_usePSRAMBuffers);
		tmp.meta = pw.meta;
		tmp.msgpack = std::move(pw.bytes);
		auto st = writeDocToFile(baseDir, tmp);
		if (!st.ok())
			return recordStatus(st);
		didWork = true;
	}
	return recordStatus({DbStatusCode::Ok, ""});
}
