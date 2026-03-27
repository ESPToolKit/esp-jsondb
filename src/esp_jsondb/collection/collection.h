#pragma once

#include <Arduino.h>
#include <functional>
#include <map>
#include <memory>
#include <vector>

#include <FS.h>
#include <StreamUtils.h>

#include <ctime>
#include <type_traits>
#include <utility>

#include "../document/document.h"
#include "../utils/dbTypes.h"
#include "../utils/fr_mutex.h"
#include "../utils/jsondb_allocator.h"
#include "../utils/objectId.h"
#include "../utils/schema.h"
#include "../storage/record_store.h"

class ESPJsonDB;
struct CollectionStore;

class Collection {
  public:
	Collection(
	    ESPJsonDB &db,
	    const std::string &name,
	    const Schema &schema,
	    std::string baseDir,
	    const CollectionConfig &config,
	    bool usePSRAMBuffers,
	    fs::FS &fs
	);
	~Collection();
	const std::string &name() const {
		return _name;
	}
	const CollectionConfig &config() const {
		return _config;
	}
	void setConfig(const CollectionConfig &config);
	void setSchema(const Schema &schema);

	// Create from JsonObjectConst (validated)
	DbResult<std::string> create(JsonObjectConst data); // returns new _id

	// Convenience: create from JsonDocument; validates it's an object
	DbResult<std::string> create(const JsonDocument &data);

	// Bulk create from an array of objects. Returns list of created ids.
	DbResult<std::vector<std::string>> createMany(JsonArrayConst arr);
	DbResult<std::vector<std::string>> createMany(const JsonDocument &arrDoc);

	// Find
	DbResult<DocView> findById(const std::string &id);

	// Retrieve all documents matching predicate
	DbResult<std::vector<DocView>> findMany(std::function<bool(const DocView &)> pred);

	// Retrieve the first document matching predicate
	DbResult<DocView> findOne(std::function<bool(const DocView &)> pred);

	// Retrieve the first document matching a JSON filter (key==value pairs)
	DbResult<DocView> findOne(const JsonDocument &filter);

	// Update the first document matching predicate; optionally create if not found
	DbStatus updateOne(
	    std::function<bool(const DocView &)> pred,
	    std::function<void(DocView &)> mutator,
	    bool create
	);

	// Update the first document matching a JSON filter with a JSON patch; optionally create if not
	// found
	DbStatus updateOne(const JsonDocument &filter, const JsonDocument &patch, bool create);

	// Update single by id (mutate via view)
	DbStatus updateById(const std::string &id, std::function<void(DocView &)> mutator);

	// Remove
	DbStatus removeById(const std::string &id);

	// Bulk (cheap, flexible)
	template <typename Pred> DbResult<size_t> removeMany(Pred &&p);

	template <
	    typename Pred,
	    typename Mut,
	    typename = std::enable_if_t<
	        !std::is_same_v<std::decay_t<Pred>, JsonDocument> &&
	        !std::is_same_v<std::decay_t<Mut>, JsonDocument>>>
	DbResult<size_t> updateMany(Pred &&p, Mut &&m);

	template <
	    typename Mut,
	    typename = std::enable_if_t<!std::is_same_v<std::decay_t<Mut>, JsonDocument>>>
	DbResult<size_t> updateMany(Mut &&m);

	template <
	    typename Pred,
	    typename = std::enable_if_t<!std::is_same_v<std::decay_t<Pred>, JsonDocument>>>

	DbResult<size_t> updateMany(const JsonDocument &patch, Pred &&p);

	DbResult<size_t> updateMany(const JsonDocument &patch, const JsonDocument &filter);

	// Dirty tracking
	bool isDirty() const {
		return _dirty;
	}
	void clearDirty() {
		_dirty = false;
	}

	// Persistence hooks used by ESPJsonDB
	DbStatus loadFromFs(const std::string &baseDir);
	// Flush pending writes/deletes to FS. Sets didWork=true if any file was
	// written or removed during this call.
	DbStatus flushDirtyToFs(const std::string &baseDir, bool &didWork);

	// Optional: stats
	size_t size() const {
		return _docs.size();
	}

	// Mark all records as removed (used when dropping a collection)
	void markAllRemoved() {
		FrLock lk(_mu);
		for (auto &kv : _docs) {
			kv.second->meta.removed = true;
		}
	}

  private:
	using DocumentRecordPtr = std::shared_ptr<DocumentRecord>;
	using DocumentMapValue = std::pair<const DocId, DocumentRecordPtr>;
	using DocumentMapAllocator = JsonDbAllocator<DocumentMapValue>;
	using DocumentMap =
	    std::map<DocId, DocumentRecordPtr, DocIdLess, DocumentMapAllocator>;

	using UniqueValueMap = std::map<std::string, DocId, std::less<std::string>,
	                                JsonDbAllocator<std::pair<const std::string, DocId>>>;
	using UniqueIndexMap = std::map<
	    std::string,
	    UniqueValueMap,
	    std::less<std::string>,
	    JsonDbAllocator<std::pair<const std::string, UniqueValueMap>>>;

	std::unique_ptr<CollectionStore> _store;
	ESPJsonDB *&_db;
	std::string &_name;
	Schema &_schema;
	CollectionConfig &_config;
	DocumentMap &_docs;
	bool &_dirty;
	JsonDbVector<DocId> &_deletedIds;
	FrMutex &_mu;
	std::string &_baseDir;
	bool &_usePSRAMBuffers;
	fs::FS *&_fs;
	RecordStore &_recordStore;
	UniqueIndexMap &_uniqueIndexes;

	DbStatus writeDocToFile(const std::string &baseDir, const DocumentRecord &r);
	DbResult<std::shared_ptr<DocumentRecord>>
	readDocFromFile(const std::string &baseDir, const std::string &id);
	DbStatus checkUniqueFieldsInCache(JsonObjectConst obj, const DocId *selfId);
	DbStatus checkUniqueFieldsOnDisk(JsonObjectConst obj, const DocId *selfId);
	DbStatus checkUniqueFields(JsonObjectConst obj, const DocId *selfId);
	JsonDbVector<DocId> listDocumentIdsFromFs() const;
	DbStatus persistImmediate(const std::shared_ptr<DocumentRecord> &rec);
	size_t countDocumentsFromFs() const;
	DocView makeView(std::shared_ptr<DocumentRecord> rec);
	DbResult<JsonDbVector<DocId>> collectMatchingIds(std::function<bool(const DocView &)> pred);
	std::string collectionDir() const;
	std::string uniqueValueKey(const SchemaField &field, JsonVariantConst value) const;
	DbStatus addUniqueValuesLocked(JsonObjectConst obj, const DocId &id);
	void removeUniqueValuesLocked(JsonObjectConst obj, const DocId &id);
	DbStatus rebuildUniqueIndexesLocked();
	bool isResidentBudgetEnforced() const;
	bool isDecodedBudgetEnforced() const;
	void touchRecordLocked(const std::shared_ptr<DocumentRecord> &rec);
	void rememberKnownIdLocked(const DocId &id);
	void forgetKnownIdLocked(const DocId &id);
	bool containsKnownIdLocked(const DocId &id) const;
	DbStatus ensureResidentCapacityLocked(size_t additional, const DocId *protectId = nullptr);
	DbResult<std::shared_ptr<DocumentRecord>> ensureRecordLoaded(const DocId &id);
	DbStatus pinRecord(const std::shared_ptr<DocumentRecord> &rec);
	void unpinRecord(const std::shared_ptr<DocumentRecord> &rec);
	DbStatus acquireDecodedViewSlot();
	void releaseDecodedViewSlot();
	DbStatus updateByIdWithDecision(
	    const std::string &id,
	    std::function<bool(DocView &)> mutator,
	    bool &updated
	);
	DbStatus updateOneNoCache(
	    std::function<bool(const DocView &)> pred,
	    std::function<void(DocView &)> mutator,
	    bool create,
	    bool &created,
	    bool &updated
	);
	DbStatus updateOneJsonNoCache(
	    const JsonDocument &filter,
	    const JsonDocument &patch,
	    bool create,
	    bool &created,
	    bool &updated
	);
	DbStatus
	updateByIdNoCache(const std::string &id, std::function<void(DocView &)> mutator, bool &updated);
	DbStatus removeByIdNoCache(const std::string &id, bool &removed);

	DbStatus recordStatus(const DbStatus &st) const;
	void emitEvent(DBEventType ev) const;
	void noteDeletedInDiag(size_t count) const;
	friend struct CollectionStore;
};

template <typename Pred> DbResult<size_t> Collection::removeMany(Pred &&p) {
	DbResult<size_t> res{};
	auto matches = collectMatchingIds(std::function<bool(const DocView &)>(std::forward<Pred>(p)));
	if (!matches.status.ok()) {
		res.status = matches.status;
		return res;
	}
	for (const auto &id : matches.value) {
		auto st = removeById(id.c_str());
		if (st.ok())
			++res.value;
	}
	res.status = {DbStatusCode::Ok, ""};
	recordStatus(res.status);
	return res;
}

template <typename Pred, typename Mut, typename>
DbResult<size_t> Collection::updateMany(Pred &&p, Mut &&m) {
	DbResult<size_t> res{};
	bool sawConflict = false;
	auto matches = collectMatchingIds(std::function<bool(const DocView &)>(std::forward<Pred>(p)));
	if (!matches.status.ok()) {
		res.status = matches.status;
		return res;
	}
	for (const auto &id : matches.value) {
		bool updated = false;
		auto st = updateByIdWithDecision(
		    id.c_str(),
		    std::function<bool(DocView &)>([&m](DocView &view) {
			    m(view);
			    return true;
		    }),
		    updated
		);
		if (st.code == DbStatusCode::Conflict)
			sawConflict = true;
		if (st.ok() && updated)
			++res.value;
	}
	res.status = sawConflict ? DbStatus{DbStatusCode::Conflict, "concurrent modification"} :
	                           DbStatus{DbStatusCode::Ok, ""};
	recordStatus(res.status);
	return res;
}

template <typename Mut, typename> DbResult<size_t> Collection::updateMany(Mut &&m) {
	DbResult<size_t> res{};
	bool sawConflict = false;
	auto matches = collectMatchingIds({});
	if (!matches.status.ok()) {
		res.status = matches.status;
		return res;
	}
	for (const auto &id : matches.value) {
		bool updated = false;
		auto st = updateByIdWithDecision(
		    id.c_str(),
		    std::function<bool(DocView &)>([&m](DocView &view) {
			    using Ret = std::invoke_result_t<Mut &, DocView &>;
			    if constexpr (std::is_same_v<Ret, bool>) {
				    return m(view);
			    } else {
				    m(view);
				    return true;
			    }
		    }),
		    updated
		);
		if (st.code == DbStatusCode::Conflict)
			sawConflict = true;
		if (st.ok() && updated)
			++res.value;
	}
	res.status = sawConflict ? DbStatus{DbStatusCode::Conflict, "concurrent modification"} :
	                           DbStatus{DbStatusCode::Ok, ""};
	recordStatus(res.status);
	return res;
}

template <typename Pred, typename>
DbResult<size_t> Collection::updateMany(const JsonDocument &patch, Pred &&p) {
	auto mut = [&](DocView &v) {
		for (auto kv : patch.as<JsonObjectConst>()) {
			v[kv.key().c_str()].set(kv.value());
		}
	};
	return updateMany(std::forward<Pred>(p), mut);
}

inline DbResult<size_t>
Collection::updateMany(const JsonDocument &patch, const JsonDocument &filter) {
	auto pred = [&](const DocView &v) {
		for (auto kv : filter.as<JsonObjectConst>()) {
			if (v[kv.key().c_str()] != kv.value()) {
				return false;
			}
		}
		return true;
	};
	return updateMany(patch, pred);
}
