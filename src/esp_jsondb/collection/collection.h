#pragma once

#include <Arduino.h>
#include <functional>
#include <map>
#include <memory>
#include <vector>

#include <LittleFS.h>
#include <StreamUtils.h>

#include <ctime>
#include <type_traits>
#include <utility>

#include "../document/document.h"
#include "../utils/dbTypes.h"
#include "../utils/fr_mutex.h"
#include "../utils/objectId.h"
#include "../utils/schema.h"

class Collection {
  public:
	explicit Collection(const std::string &name, const Schema &schema);
	const std::string &name() const { return _name; }

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
	DbStatus updateOne(std::function<bool(const DocView &)> pred,
					  std::function<void(DocView &)> mutator,
					  bool create);

	// Update the first document matching a JSON filter with a JSON patch; optionally create if not found
	DbStatus updateOne(const JsonDocument &filter,
					  const JsonDocument &patch,
					  bool create);

	// Update single by id (mutate via view)
	DbStatus updateById(const std::string &id, std::function<void(DocView &)> mutator);

	// Remove
	DbStatus removeById(const std::string &id);

	// Bulk (cheap, flexible)
	template <typename Pred>
	DbResult<size_t> removeMany(Pred &&p);

	template <typename Pred, typename Mut, typename = std::enable_if_t<!std::is_same_v<std::decay_t<Pred>, JsonDocument> && !std::is_same_v<std::decay_t<Mut>, JsonDocument>>>
	DbResult<size_t> updateMany(Pred &&p, Mut &&m);

	template <typename Mut,
			  typename = std::enable_if_t<!std::is_same_v<std::decay_t<Mut>, JsonDocument>>>
	DbResult<size_t> updateMany(Mut &&m);

	template <typename Pred,
			  typename = std::enable_if_t<!std::is_same_v<std::decay_t<Pred>, JsonDocument>>>

	DbResult<size_t> updateMany(const JsonDocument &patch, Pred &&p);

	DbResult<size_t> updateMany(const JsonDocument &patch, const JsonDocument &filter);

	// Dirty tracking
	bool isDirty() const { return _dirty; }
	void clearDirty() { _dirty = false; }

	// Persistence hooks used by DataBase
	DbStatus loadFromFs(const std::string &baseDir);
	// Flush pending writes/deletes to FS. Sets didWork=true if any file was
	// written or removed during this call.
	DbStatus flushDirtyToFs(const std::string &baseDir, bool &didWork);

	// Optional: stats
	size_t size() const { return _docs.size(); }

  private:
	std::string _name;
	Schema _schema;
	std::map<std::string, std::unique_ptr<DocumentRecord>> _docs;
	bool _dirty = false;
	std::vector<std::string> _deletedIds; // files to remove on next flush
	FrMutex _mu;						  // guards _docs, _deletedIds

	DbStatus writeDocToFile(const std::string &baseDir, const DocumentRecord &r);
	DbResult<std::unique_ptr<DocumentRecord>> readDocFromFile(const std::string &baseDir, const std::string &id);
    // Enforce unique constraints declared in schema fields; excludes selfId when provided
    DbStatus checkUniqueFields(JsonObjectConst obj, const std::string &selfId);
};

template <typename Pred>
DbResult<size_t> Collection::removeMany(Pred &&p) {
	DbResult<size_t> res{};
	std::vector<std::string> toErase;
	toErase.reserve(_docs.size());
	for (auto &kv : _docs) {
		DocView v(kv.second.get(), &_schema);
		if (p(v)) {
			toErase.push_back(kv.first);
		}
	}
	for (auto &id : toErase) {
		_deletedIds.push_back(id);
		_docs.erase(id);
	}
	if (!toErase.empty()) _dirty = true;
	res.status = {DbStatusCode::Ok, ""};
	dbSetLastError(res.status);
	res.value = toErase.size();
	return res;
}

template <typename Pred, typename Mut, typename>
DbResult<size_t> Collection::updateMany(Pred &&p, Mut &&m) {
	DbResult<size_t> res{};
	size_t count = 0;
	for (auto &kv : _docs) {
		DocView v(kv.second.get(), &_schema);
		if (p(v)) {
			m(v);
				if (_schema.hasValidate()) {
					auto obj = v.asObject();
					auto ve = _schema.runPreSave(obj);
					if (!ve.valid) {
						v.discard();
						continue;
					}
                // Unique constraints
                auto ust = checkUniqueFields(obj, kv.second->meta.id);
                if (!ust.ok()) {
                    v.discard();
                    continue;
                }
				}
				auto st = v.commit();
				if (st.ok()) {
					++count;
				}
		}
	}
	if (count) _dirty = true;
	res.status = {DbStatusCode::Ok, ""};
	dbSetLastError(res.status);
	res.value = count;
	return res;
}

template <typename Mut, typename>
DbResult<size_t> Collection::updateMany(Mut &&m) {
	DbResult<size_t> res{};
	size_t count = 0;
	for (auto &kv : _docs) {
		DocView v(kv.second.get(), &_schema);
		if (m(v)) {
			if (_schema.hasValidate()) {
				auto obj = v.asObject();
				auto ve = _schema.runPreSave(obj);
				if (!ve.valid) {
					v.discard();
					continue;
				}
                // Unique constraints
                auto ust = checkUniqueFields(obj, kv.second->meta.id);
                if (!ust.ok()) {
                    v.discard();
                    continue;
                }
			}
			auto st = v.commit();
			if (st.ok()) {
				++count;
			}
		} else {
			v.discard();
		}
	}
	if (count) _dirty = true;
	res.status = {DbStatusCode::Ok, ""};
	dbSetLastError(res.status);
	res.value = count;
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

inline DbResult<size_t> Collection::updateMany(const JsonDocument &patch, const JsonDocument &filter) {
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
