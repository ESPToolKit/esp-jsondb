#include "collection.h"
#include "../db.h"
#include "../db_runtime.h"
#include "../utils/fs_utils.h"
#include "../utils/time_utils.h"

#include <algorithm>
#include <cstdio>

namespace {
std::shared_ptr<DocumentRecord> makeSharedDocumentRecord(bool usePSRAMBuffers) {
	return std::allocate_shared<DocumentRecord>(
	    JsonDbAllocator<DocumentRecord>(usePSRAMBuffers), usePSRAMBuffers
	);
}

using DocumentRecordPtr = std::shared_ptr<DocumentRecord>;
using DocumentMapValue = std::pair<const DocId, DocumentRecordPtr>;
using DocumentMapAllocator = JsonDbAllocator<DocumentMapValue>;
using DocumentMap = std::map<DocId, DocumentRecordPtr, DocIdLess, DocumentMapAllocator>;
using UniqueValueMap = std::map<std::string, DocId, std::less<std::string>,
                                JsonDbAllocator<std::pair<const std::string, DocId>>>;
using UniqueIndexMap =
    std::map<std::string,
             UniqueValueMap,
             std::less<std::string>,
             JsonDbAllocator<std::pair<const std::string, UniqueValueMap>>>;
} // namespace

struct CollectionStore {
	DocumentMap docs;
	JsonDbVector<DocId> deletedIds;
	JsonDbVector<DocId> knownIds;
	DbRuntime *rt = nullptr;
	std::string name;
	Schema schema;
	CollectionConfig config{};
	bool dirty = false;
	FrMutex mu;
	std::string baseDir;
	bool usePSRAMBuffers = false;
	fs::FS *fs = nullptr;
	RecordStore recordStore;
	UniqueIndexMap uniqueIndexes;
	uint64_t accessClock = 0;
	size_t activeDecodedViews = 0;

	CollectionStore(
	    DbRuntime &rtRef,
	    const std::string &collectionName,
	    const Schema &collectionSchema,
	    std::string baseDirValue,
	    const CollectionConfig &collectionConfig,
	    bool psram,
	    fs::FS &filesystem
	)
	    : docs(DocumentMap(DocIdLess{}, DocumentMapAllocator(psram))),
	      deletedIds(JsonDbAllocator<DocId>(psram)), knownIds(JsonDbAllocator<DocId>(psram)),
	      rt(&rtRef), name(collectionName), schema(collectionSchema), config(collectionConfig),
	      baseDir(std::move(baseDirValue)), usePSRAMBuffers(psram), fs(&filesystem),
	      recordStore(filesystem, psram),
	      uniqueIndexes(
	          std::less<std::string>{},
	          JsonDbAllocator<std::pair<const std::string, UniqueValueMap>>(psram)
	      ) {
	}
};

Collection::Collection(
    DbRuntime &rt,
    const std::string &name,
    const Schema &schema,
    std::string baseDir,
    const CollectionConfig &config,
    bool usePSRAMBuffers,
    fs::FS &fs
)
    : _store(std::make_unique<CollectionStore>(
          rt, name, schema, std::move(baseDir), config, usePSRAMBuffers, fs
      )) {
}

Collection::~Collection() = default;

#define _rt (_store->rt)
#define _name (_store->name)
#define _schema (_store->schema)
#define _config (_store->config)
#define _docs (_store->docs)
#define _dirty (_store->dirty)
#define _deletedIds (_store->deletedIds)
#define _mu (_store->mu)
#define _baseDir (_store->baseDir)
#define _usePSRAMBuffers (_store->usePSRAMBuffers)
#define _fs (_store->fs)
#define _recordStore (_store->recordStore)
#define _uniqueIndexes (_store->uniqueIndexes)

const std::string &Collection::name() const {
	return _name;
}

const CollectionConfig &Collection::config() const {
	return _config;
}

bool Collection::isDirty() const {
	return _dirty;
}

void Collection::clearDirty() {
	_dirty = false;
}

size_t Collection::size() const {
	return _docs.size();
}

void Collection::markAllRemoved() {
	FrLock lk(_mu);
	for (auto &kv : _docs) {
		kv.second->meta.removed = true;
	}
}

void Collection::setConfig(const CollectionConfig &config) {
	FrLock lk(_mu);
	_config = config;
	(void)ensureResidentCapacityLocked(0);
}

void Collection::setSchema(const Schema &schema) {
	FrLock lk(_mu);
	_schema = schema;
	(void)rebuildUniqueIndexesLocked();
}

DbStatus Collection::recordStatus(const DbStatus &st) const {
	return _rt ? _rt->recordStatus(st) : st;
}

void Collection::emitEvent(DBEventType ev) const {
	if (_rt)
		_rt->emitEvent(ev);
}

void Collection::noteDeletedInDiag(size_t count) const {
	if (count == 0 || !_rt)
		return;
	_rt->noteDocumentDeleted(_name, static_cast<uint32_t>(count));
}

bool Collection::isResidentBudgetEnforced() const {
	return _config.maxRecordsInMemory > 0 &&
	       (_config.loadPolicy == CollectionLoadPolicy::Lazy ||
	        _config.loadPolicy == CollectionLoadPolicy::Delayed);
}

bool Collection::isDecodedBudgetEnforced() const {
	return _config.maxDecodedViews > 0;
}

void Collection::touchRecordLocked(const std::shared_ptr<DocumentRecord> &rec) {
	if (!rec)
		return;
	rec->lastAccessSeq = ++_store->accessClock;
}

void Collection::rememberKnownIdLocked(const DocId &id) {
	if (!containsKnownIdLocked(id)) {
		_store->knownIds.push_back(id);
	}
}

void Collection::forgetKnownIdLocked(const DocId &id) {
	_store->knownIds.erase(
	    std::remove(_store->knownIds.begin(), _store->knownIds.end(), id),
	    _store->knownIds.end()
	);
}

bool Collection::containsKnownIdLocked(const DocId &id) const {
	return std::find(_store->knownIds.begin(), _store->knownIds.end(), id) != _store->knownIds.end();
}

DbStatus Collection::ensureResidentCapacityLocked(size_t additional, const DocId *protectId) {
	if (!isResidentBudgetEnforced())
		return {DbStatusCode::Ok, ""};

	while ((_docs.size() + additional) > _config.maxRecordsInMemory) {
		auto victimIt = _docs.end();
		uint64_t oldestSeq = UINT64_MAX;
		for (auto it = _docs.begin(); it != _docs.end(); ++it) {
			const auto &rec = it->second;
			if (!rec || rec->meta.dirty || rec->meta.removed || rec->pinCount > 0)
				continue;
			if (protectId && it->first == *protectId)
				continue;
			if (rec->lastAccessSeq < oldestSeq) {
				oldestSeq = rec->lastAccessSeq;
				victimIt = it;
			}
		}
		if (victimIt == _docs.end()) {
			return {DbStatusCode::Busy, "record memory budget exceeded"};
		}
		_docs.erase(victimIt);
	}
	return {DbStatusCode::Ok, ""};
}

DbResult<std::shared_ptr<DocumentRecord>> Collection::ensureRecordLoaded(const DocId &id) {
	DbResult<std::shared_ptr<DocumentRecord>> res{};
	{
		FrLock lk(_mu);
		auto it = _docs.find(id);
		if (it != _docs.end()) {
			touchRecordLocked(it->second);
			res.status = {DbStatusCode::Ok, ""};
			res.value = it->second;
			return res;
		}
		if (!containsKnownIdLocked(id)) {
			res.status = {DbStatusCode::NotFound, "document not found"};
			return res;
		}
	}

	auto rr = readDocFromFile(_baseDir, id.c_str());
	if (!rr.status.ok()) {
		res.status = rr.status;
		return res;
	}

	{
		FrLock lk(_mu);
		auto existing = _docs.find(id);
		if (existing != _docs.end()) {
			touchRecordLocked(existing->second);
			res.status = {DbStatusCode::Ok, ""};
			res.value = existing->second;
			return res;
		}
		auto cap = ensureResidentCapacityLocked(1, &id);
		if (!cap.ok()) {
			res.status = recordStatus(cap);
			return res;
		}
		touchRecordLocked(rr.value);
		auto [it, inserted] = _docs.emplace(id, rr.value);
		(void)inserted;
		res.status = {DbStatusCode::Ok, ""};
		res.value = it->second;
	}
	return res;
}

DbStatus Collection::pinRecord(const std::shared_ptr<DocumentRecord> &rec) {
	if (!rec)
		return {DbStatusCode::Ok, ""};
	FrLock lk(_mu);
	++rec->pinCount;
	touchRecordLocked(rec);
	return {DbStatusCode::Ok, ""};
}

void Collection::unpinRecord(const std::shared_ptr<DocumentRecord> &rec) {
	if (!rec)
		return;
	FrLock lk(_mu);
	if (rec->pinCount > 0)
		--rec->pinCount;
}

DbStatus Collection::acquireDecodedViewSlot() {
	if (!isDecodedBudgetEnforced())
		return {DbStatusCode::Ok, ""};
	FrLock lk(_mu);
	if (_store->activeDecodedViews >= _config.maxDecodedViews) {
		return {DbStatusCode::Busy, "decoded view budget exceeded"};
	}
	++_store->activeDecodedViews;
	return {DbStatusCode::Ok, ""};
}

void Collection::releaseDecodedViewSlot() {
	if (!isDecodedBudgetEnforced())
		return;
	FrLock lk(_mu);
	if (_store->activeDecodedViews > 0)
		--_store->activeDecodedViews;
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
		touchRecordLocked(rec);

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
		auto cap = ensureResidentCapacityLocked(1, &rec->meta.id);
		if (!cap.ok()) {
			res.status = recordStatus(cap);
			return res;
		}
		_docs.emplace(rec->meta.id, rec);
		rememberKnownIdLocked(rec->meta.id);
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
		if (_rt)
			_rt->noteDocumentCreated(_name);
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
		return {st,
		        DocView(nullptr,
		                &_schema,
		                nullptr,
		                _rt ? _rt->owner : nullptr,
		                nullptr,
		                nullptr,
		                nullptr,
		                nullptr,
		                nullptr,
		                false,
		                _usePSRAMBuffers)};
	}

	auto loaded = ensureRecordLoaded(lookupId);
	if (!loaded.status.ok()) {
		recordStatus(loaded.status);
		return {loaded.status,
		        DocView(nullptr,
		                &_schema,
		                nullptr,
		                _rt ? _rt->owner : nullptr,
		                nullptr,
		                nullptr,
		                nullptr,
		                nullptr,
		                nullptr,
		                false,
		                _usePSRAMBuffers)};
	}
	DbStatus st{DbStatusCode::Ok, ""};
	recordStatus(st);
	return {st, makeView(loaded.value)};
}

DbResult<std::vector<DocView>> Collection::findMany(std::function<bool(const DocView &)> pred) {
	DbResult<std::vector<DocView>> res{};
	auto idsRes = collectMatchingIds(std::move(pred));
	if (!idsRes.status.ok()) {
		res.status = idsRes.status;
		return res;
	}
	for (const auto &id : idsRes.value) {
		auto loaded = ensureRecordLoaded(id);
		if (loaded.status.ok()) {
			res.value.emplace_back(makeView(loaded.value));
		}
	}
	res.status = {DbStatusCode::Ok, ""};
	recordStatus(res.status);
	return res;
}

DbResult<DocView> Collection::findOne(std::function<bool(const DocView &)> pred) {
	auto idsRes = collectMatchingIds(std::move(pred));
	if (!idsRes.status.ok()) {
		return {idsRes.status,
		        DocView(nullptr,
		                &_schema,
		                nullptr,
		                _rt ? _rt->owner : nullptr,
		                nullptr,
		                nullptr,
		                nullptr,
		                nullptr,
		                nullptr,
		                false,
		                _usePSRAMBuffers)};
	}
	if (!idsRes.value.empty()) {
		auto loaded = ensureRecordLoaded(idsRes.value.front());
		if (loaded.status.ok()) {
			DbStatus st{DbStatusCode::Ok, ""};
			recordStatus(st);
			return {st, makeView(loaded.value)};
		}
	}
	DbStatus st{DbStatusCode::NotFound, "document not found"};
	recordStatus(st);
	return {st,
	        DocView(nullptr,
	                &_schema,
	                nullptr,
	                _rt ? _rt->owner : nullptr,
	                nullptr,
	                nullptr,
	                nullptr,
	                nullptr,
	                nullptr,
	                false,
	                _usePSRAMBuffers)};
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

DbResult<JsonDbVector<DocId>> Collection::collectMatchingIds(
    std::function<bool(const DocView &)> pred
) {
	DbResult<JsonDbVector<DocId>> res{};
	res.value = JsonDbVector<DocId>(JsonDbAllocator<DocId>(_usePSRAMBuffers));
	JsonDbVector<DocId> ids{JsonDbAllocator<DocId>(_usePSRAMBuffers)};
	{
		FrLock lk(_mu);
		ids = _store->knownIds;
	}
	for (const auto &id : ids) {
		auto loaded = ensureRecordLoaded(id);
		if (!loaded.status.ok()) {
			if (loaded.status.code == DbStatusCode::Busy) {
				res.status = loaded.status;
				return res;
			}
			continue;
		}
		bool matched = false;
		{
			FrLock lk(_mu);
			auto it = _docs.find(id);
			if (it == _docs.end())
				continue;
			touchRecordLocked(it->second);
			DocView v(it->second,
			          &_schema,
			          nullptr,
			          _rt ? _rt->owner : nullptr,
			          nullptr,
			          nullptr,
			          nullptr,
			          nullptr,
			          nullptr,
			          false,
			          _usePSRAMBuffers);
			matched = !pred || pred(v);
		}
		if (matched) {
			res.value.push_back(id);
		}
	}
	res.status = {DbStatusCode::Ok, ""};
	return res;
}

DbStatus Collection::updateOne(
    std::function<bool(const DocView &)> pred, std::function<void(DocView &)> mutator, bool create
) {
	auto matches = collectMatchingIds(std::move(pred));
	if (!matches.status.ok())
		return recordStatus(matches.status);

	bool updated = false;
	bool created = false;
	DbStatus st{DbStatusCode::NotFound, "document not found"};
	if (!matches.value.empty()) {
		st = updateByIdWithDecision(
		    matches.value.front().c_str(),
		    std::function<bool(DocView &)>([&mutator](DocView &view) {
			    mutator(view);
			    return true;
		    }),
		    updated
		);
	}

	if (!updated && create) {
		auto rec = makeSharedDocumentRecord(_usePSRAMBuffers);
		rec->meta.createdAtMs = nowUtcMs();
		rec->meta.updatedAtMs = rec->meta.createdAtMs;
		rec->meta.id = ObjectId().toDocId();
		rec->meta.revision = 1;
		rec->meta.dirty = true;
		touchRecordLocked(rec);

		DocView v(rec,
		          &_schema,
		          nullptr,
		          _rt ? _rt->owner : nullptr,
		          nullptr,
		          nullptr,
		          nullptr,
		          nullptr,
		          nullptr,
		          false,
		          _usePSRAMBuffers);
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
		FrLock lk(_mu);
		auto cap = ensureResidentCapacityLocked(1, &rec->meta.id);
		if (!cap.ok())
			return recordStatus(cap);
		_docs.emplace(rec->meta.id, std::move(rec));
		rememberKnownIdLocked(v.meta().id);
		auto uniqueStatus = addUniqueValuesLocked(v.asObjectConst(), v.meta().id);
		if (!uniqueStatus.ok())
			return recordStatus(uniqueStatus);
		_dirty = true;
		created = true;
		st = {DbStatusCode::Ok, ""};
	}
	if (created) {
		if (_rt)
			_rt->noteDocumentCreated(_name);
		emitEvent(DBEventType::DocumentCreated);
	} else if (updated) {
		emitEvent(DBEventType::DocumentUpdated);
	}
	return recordStatus(st);
}

DbStatus Collection::updateOne(const JsonDocument &filter, const JsonDocument &patch, bool create) {
	auto matches = collectMatchingIds([&filter](const DocView &v) {
		for (auto kvf : filter.as<JsonObjectConst>()) {
			if (v[kvf.key().c_str()] != kvf.value()) {
				return false;
			}
		}
		return true;
	});
	if (!matches.status.ok())
		return recordStatus(matches.status);

	bool updated = false;
	bool created = false;
	DbStatus st{DbStatusCode::NotFound, "document not found"};
	if (!matches.value.empty()) {
		st = updateByIdWithDecision(
		    matches.value.front().c_str(),
		    std::function<bool(DocView &)>([&patch](DocView &view) {
			    for (auto kvp : patch.as<JsonObjectConst>()) {
				    view[kvp.key().c_str()].set(kvp.value());
			    }
			    return true;
		    }),
		    updated
		);
	}

	if (!updated && create) {
		// Create a new document merging filter and patch
		auto rec = makeSharedDocumentRecord(_usePSRAMBuffers);
		rec->meta.createdAtMs = nowUtcMs();
		rec->meta.updatedAtMs = rec->meta.createdAtMs;
		rec->meta.id = ObjectId().toDocId();
		rec->meta.revision = 1;
		rec->meta.dirty = true;
		touchRecordLocked(rec);

		DocView v(rec,
		          &_schema,
		          nullptr,
		          _rt ? _rt->owner : nullptr,
		          nullptr,
		          nullptr,
		          nullptr,
		          nullptr,
		          nullptr,
		          false,
		          _usePSRAMBuffers);
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
		FrLock lk(_mu);
		auto cap = ensureResidentCapacityLocked(1, &rec->meta.id);
		if (!cap.ok())
			return recordStatus(cap);
		_docs.emplace(rec->meta.id, std::move(rec));
		rememberKnownIdLocked(v.meta().id);
		auto uniqueStatus = addUniqueValuesLocked(v.asObjectConst(), v.meta().id);
		if (!uniqueStatus.ok())
			return recordStatus(uniqueStatus);
		_dirty = true;
		created = true;
		st = {DbStatusCode::Ok, ""};
	}
	if (created) {
		if (_rt)
			_rt->noteDocumentCreated(_name);
		emitEvent(DBEventType::DocumentCreated);
	} else if (updated) {
		emitEvent(DBEventType::DocumentUpdated);
	}
	return recordStatus(st);
}

DbStatus Collection::updateById(const std::string &id, std::function<void(DocView &)> mutator) {
	bool updated = false;
	auto st = updateByIdWithDecision(
	    id,
	    std::function<bool(DocView &)>([&mutator](DocView &view) {
		    mutator(view);
		    return true;
	    }),
	    updated
	);
	if (updated)
		emitEvent(DBEventType::DocumentUpdated);
	return recordStatus(st);
}

DbStatus Collection::updateByIdWithDecision(
    const std::string &id, std::function<bool(DocView &)> mutator, bool &updated
) {
	updated = false;
	DocId lookupId;
	if (!lookupId.assign(id)) {
		return recordStatus({DbStatusCode::NotFound, "document not found"});
	}

	auto loaded = ensureRecordLoaded(lookupId);
	if (!loaded.status.ok())
		return recordStatus(loaded.status);

	std::shared_ptr<DocumentRecord> liveRec;
	uint32_t startRevision = 0;
	JsonDocument beforeDoc;
	{
		FrLock lk(_mu);
		auto it = _docs.find(lookupId);
		if (it == _docs.end())
			return recordStatus({DbStatusCode::NotFound, "document not found"});
		liveRec = it->second;
		startRevision = liveRec->meta.revision;
		touchRecordLocked(liveRec);
		if (!liveRec->msgpack.empty()) {
			auto err = deserializeMsgPack(beforeDoc, liveRec->msgpack.data(), liveRec->msgpack.size());
			if (err) {
				return recordStatus({DbStatusCode::CorruptionDetected, "msgpack decode failed"});
			}
		} else {
			beforeDoc.to<JsonObject>();
		}
	}

	auto candidate = makeSharedDocumentRecord(_usePSRAMBuffers);
	candidate->meta = liveRec->meta;
	candidate->msgpack = liveRec->msgpack;
	DocView working(
	    candidate,
	    &_schema,
	    nullptr,
	    _rt ? _rt->owner : nullptr,
	    nullptr,
	    nullptr,
	    nullptr,
	    nullptr,
	    nullptr,
	    false,
	    _usePSRAMBuffers
	);
	bool shouldCommit = mutator ? mutator(working) : true;
	if (!shouldCommit) {
		working.discard();
		return recordStatus({DbStatusCode::Ok, ""});
	}
	if (_schema.hasValidate()) {
		auto obj = working.asObject();
		auto ve = _schema.runPreSave(obj);
		if (!ve.valid) {
			working.discard();
			return recordStatus({DbStatusCode::ValidationFailed, ve.message});
		}
	}
	auto st = working.commit();
	if (!st.ok())
		return recordStatus(st);

	{
		FrLock lk(_mu);
		auto it = _docs.find(lookupId);
		if (it == _docs.end())
			return recordStatus({DbStatusCode::Conflict, "document changed during update"});
		if (it->second->meta.revision != startRevision) {
			return recordStatus({DbStatusCode::Conflict, "document changed during update"});
		}
		auto uniqueStatus =
		    checkUniqueFields(candidate->msgpack.empty()
		                          ? JsonObjectConst()
		                          : working.asObjectConst(),
		                      &lookupId);
		if (!uniqueStatus.ok()) {
			return recordStatus(uniqueStatus);
		}
		if (candidate->meta.revision != startRevision) {
			removeUniqueValuesLocked(beforeDoc.as<JsonObjectConst>(), lookupId);
			auto addStatus = addUniqueValuesLocked(working.asObjectConst(), lookupId);
			if (!addStatus.ok()) {
				addUniqueValuesLocked(beforeDoc.as<JsonObjectConst>(), lookupId);
				return recordStatus(addStatus);
			}
			it->second->msgpack = candidate->msgpack;
			it->second->meta.updatedAtMs = candidate->meta.updatedAtMs;
			it->second->meta.revision = candidate->meta.revision;
			it->second->meta.dirty = true;
			touchRecordLocked(it->second);
			_dirty = true;
			updated = true;
		}
	}
	return recordStatus({DbStatusCode::Ok, ""});
}

DbStatus Collection::removeById(const std::string &id) {
	bool removed = false;
	DbStatus st{DbStatusCode::Ok, ""};
	DocId lookupId;
	if (!lookupId.assign(id)) {
		return recordStatus({DbStatusCode::NotFound, "document not found"});
	}
	auto loaded = ensureRecordLoaded(lookupId);
	if (!loaded.status.ok())
		return recordStatus(loaded.status);
	{
		FrLock lk(_mu);
		auto it = _docs.find(lookupId);
		if (it == _docs.end())
			return recordStatus({DbStatusCode::NotFound, "document not found"});
		DocView view(it->second,
		             &_schema,
		             nullptr,
		             _rt ? _rt->owner : nullptr,
		             nullptr,
		             nullptr,
		             nullptr,
		             nullptr,
		             nullptr,
		             false,
		             _usePSRAMBuffers);
		removeUniqueValuesLocked(view.asObjectConst(), it->first);
		it->second->meta.removed = true;
		_deletedIds.push_back(it->first);
		forgetKnownIdLocked(it->first);
		_docs.erase(it);
		_dirty = true;
		removed = true;
	}
	if (removed) {
		if (_rt)
			_rt->noteDocumentDeleted(_name);
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
	{
		FrLock lk(_mu);
		if (rec) {
			++rec->pinCount;
			touchRecordLocked(rec);
		}
	}
	auto releasePin = [this, weakRec = std::weak_ptr<DocumentRecord>(rec)]() {
		if (auto locked = weakRec.lock()) {
			unpinRecord(locked);
		}
	};
	auto acquireDecode = [this]() { return acquireDecodedViewSlot(); };
	auto releaseDecode = [this]() { releaseDecodedViewSlot(); };
	return DocView(
	    std::move(rec),
	    &_schema,
	    nullptr,
	    _rt ? _rt->owner : nullptr,
	    nullptr,
	    acquireDecode,
	    releaseDecode,
	    nullptr,
	    releasePin,
	    true,
	    _usePSRAMBuffers
	);
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
		rec->meta.revision = 1;
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
		rec->meta.revision = 1;
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
	JsonDbVector<DocId> ids{JsonDbAllocator<DocId>(_usePSRAMBuffers)};
	(void)baseDir;
	ids = listDocumentIdsFromFs();

	{
		FrLock lk(_mu);
		_docs.clear();
		_store->knownIds = ids;
		_uniqueIndexes.clear();
	}

	for (const auto &id : ids) {
		auto rr = readDocFromFile(_baseDir, id.c_str());
		if (!rr.status.ok()) {
			continue;
		}
		JsonDocument doc;
		auto err = deserializeMsgPack(doc, rr.value->msgpack.data(), rr.value->msgpack.size());
		if (err) {
			return recordStatus({DbStatusCode::CorruptionDetected, "msgpack decode failed"});
		}
		{
			FrLock lk(_mu);
			auto uniqueStatus = addUniqueValuesLocked(doc.as<JsonObjectConst>(), rr.value->meta.id);
			if (!uniqueStatus.ok())
				return recordStatus(uniqueStatus);
			if (_config.loadPolicy == CollectionLoadPolicy::Eager &&
			    (!isResidentBudgetEnforced() || _docs.size() < _config.maxRecordsInMemory)) {
				touchRecordLocked(rr.value);
				_docs.emplace(rr.value->meta.id, rr.value);
			}
		}
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
