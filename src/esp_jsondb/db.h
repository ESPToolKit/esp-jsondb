#pragma once

#include <Arduino.h>

#include <LittleFS.h>
#include <freertos/FreeRTOS.h>
#include <freertos/task.h>
#include <functional>
#include <map>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "collection/collection.h"
#include "utils/dbTypes.h"
#include "utils/fr_mutex.h"
#include "utils/schema.h"
#include <ArduinoJson.h>

class DataBase {
  public:
	~DataBase();
	DbStatus init(const char *baseDir = "/db", const SyncConfig &cfg = {});
	DbStatus registerSchema(const std::string &name, const Schema &s);
	DbStatus unRegisterSchema(const std::string &name);
	DbStatus dropCollection(const std::string &name);

	// Drop all collections and documents (clears base directory)
	DbStatus dropAll();

	// Returns union of loaded + on-disk collection names
	std::vector<std::string> getAllCollectionName();

	// Change sync configuration; restarts autosync task if needed
	DbStatus changeConfig(const SyncConfig &cfg);

	// Register a generic DB event callback
	void onEvent(const std::function<void(DBEventType)> &cb);

	// Register callback for error notifications
	void onError(const std::function<void(const DbStatus &)> &cb);

	// Backwards-compat: register a sync-only callback
	void onSync(const std::function<void()> &cb);

	// Creates collection if missing
	DbResult<Collection *> collection(const std::string &name);
	// Arduino-friendly overload
	DbResult<Collection *> collection(const String &name);
	DbResult<Collection *> collection(const char *name);

	// Convenience: create a document in the given collection
	DbResult<std::string> create(const std::string &collectionName, JsonObjectConst doc);

	// Convenience: create from a JsonDocument; validates it's an object
	DbResult<std::string> create(const std::string &collectionName, const JsonDocument &doc);

	// Convenience: bulk create documents in a collection
	DbResult<std::vector<std::string>> createMany(const std::string &collectionName, JsonArrayConst arr);
	DbResult<std::vector<std::string>> createMany(const std::string &collectionName, const JsonDocument &arrDoc);

	// Convenience: find a document by _id in the given collection
	DbResult<DocView> findById(const std::string &collectionName, const std::string &id);

	// Convenience: find documents matching predicate in the given collection
	DbResult<std::vector<DocView>> findMany(const std::string &collectionName,
											std::function<bool(const DocView &)> pred);

	// Convenience: find the first document matching predicate in the given collection
	DbResult<DocView> findOne(const std::string &collectionName, std::function<bool(const DocView &)> pred);

	// Convenience: find the first document matching a JSON filter in the given collection
	DbResult<DocView> findOne(const std::string &collectionName, const JsonDocument &filter);

	// Convenience: update the first match (predicate + mutator). If create=true, creates new when none found
	DbStatus updateOne(const std::string &collectionName,
					   std::function<bool(const DocView &)> pred,
					   std::function<void(DocView &)> mutator,
					   bool create = false);

	// Convenience: update the first match (JSON filter + JSON patch). If create=true, creates new when none found
	DbStatus updateOne(const std::string &collectionName,
					   const JsonDocument &filter,
					   const JsonDocument &patch,
					   bool create = false);

	// Convenience: update a document by _id in the given collection
	DbStatus updateById(const std::string &collectionName, const std::string &id, std::function<void(DocView &)> mutator);

	// Convenience: remove a document by _id in the given collection
	DbStatus removeById(const std::string &collectionName, const std::string &id);

	// Bulk operations on a collection
	template <typename Pred>
	DbResult<size_t> removeMany(const std::string &collectionName, Pred &&p);

	template <typename Pred, typename Mut, typename = std::enable_if_t<!std::is_same_v<std::decay_t<Pred>, JsonDocument> && !std::is_same_v<std::decay_t<Mut>, JsonDocument>>>
	DbResult<size_t> updateMany(const std::string &collectionName, Pred &&p, Mut &&m);

	template <typename Mut,
			  typename = std::enable_if_t<!std::is_same_v<std::decay_t<Mut>, JsonDocument>>>
	DbResult<size_t> updateMany(const std::string &collectionName, Mut &&m);

	template <typename Pred,
			  typename = std::enable_if_t<!std::is_same_v<std::decay_t<Pred>, JsonDocument>>>
	DbResult<size_t> updateMany(const std::string &collectionName, const JsonDocument &patch, Pred &&p);

	DbResult<size_t> updateMany(const std::string &collectionName, const JsonDocument &patch, const JsonDocument &filter);

	// Manual sync (safe to call from app)
	DbStatus syncNow();

	// Retrieve last error or success status
	DbStatus lastError() const { return _lastError; }

	// Allow other components to update diagnostics/error state
	DbStatus recordStatus(const DbStatus &st) { return setLastError(st); }

	// Diagnostics: number of collections, doc counts, and config
	JsonDocument getDiag();

	// Backup/restore
	JsonDocument getSnapshot();
	DbStatus restoreFromSnapshot(const JsonDocument &snapshot);

	// Emit an event to registered listeners
	void emitEvent(DBEventType ev);

	// Emit an error to registered listeners
	void emitError(const DbStatus &st);

  private:
	std::string _baseDir;
	SyncConfig _cfg;
	std::map<std::string, std::unique_ptr<Collection>> _cols;
	std::map<std::string, Schema> _schemas;
	std::vector<std::string> _colsToDelete;
	std::vector<std::function<void(DBEventType)>> _eventCbs;
	std::vector<std::function<void(const DbStatus &)>> _errorCbs;
	fs::FS *_fs = &LittleFS; // active filesystem
	FrMutex _mu; // guards _cols, _schemas, _colsToDelete

	// Tracks most recent status for diagnostics/debugging
	DbStatus _lastError{DbStatusCode::Ok, ""};

	struct DiagCache {
		std::map<std::string, uint32_t> docsPerCollection;
		uint32_t collections = 0;
		uint32_t lastRefreshMs = 0; // millis when refreshed from FS
	};

	DiagCache _diagCache; // refreshed on sync; read without touching FS

	// sync task
	static void syncTaskThunk(void *arg);
	void syncTaskLoop();
	void startSyncTaskUnlocked();
	void stopSyncTaskUnlocked();

	// Update last error/status helper
	DbStatus setLastError(const DbStatus &st) {
		_lastError = st;
		if (!st.ok()) emitError(st);
		return st;
	}

	// fs helpers
	DbStatus ensureFsReady();
	DbStatus removeCollectionDir(const std::string &name);

	// Refresh diag cache from filesystem (expensive; called during sync)
	void refreshDiagFromFs();
	DbStatus preloadCollectionsFromFs();

	// Task handle for autosync
	TaskHandle_t _syncTask = nullptr;
};

template <typename Pred>
DbResult<size_t> DataBase::removeMany(const std::string &collectionName, Pred &&p) {
	DbResult<size_t> res{};
	auto cr = collection(collectionName);
	if (!cr.status.ok()) {
		res.status = cr.status;
		return res;
	}
	return cr.value->removeMany(std::forward<Pred>(p));
}

template <typename Pred, typename Mut, typename>
DbResult<size_t> DataBase::updateMany(const std::string &collectionName, Pred &&p, Mut &&m) {
	DbResult<size_t> res{};
	auto cr = collection(collectionName);
	if (!cr.status.ok()) {
		res.status = cr.status;
		return res;
	}
	return cr.value->updateMany(std::forward<Pred>(p), std::forward<Mut>(m));
}

template <typename Mut, typename>
DbResult<size_t> DataBase::updateMany(const std::string &collectionName, Mut &&m) {
	DbResult<size_t> res{};
	auto cr = collection(collectionName);
	if (!cr.status.ok()) {
		res.status = cr.status;
		return res;
	}
	return cr.value->updateMany(std::forward<Mut>(m));
}

template <typename Pred, typename>
DbResult<size_t> DataBase::updateMany(const std::string &collectionName, const JsonDocument &patch, Pred &&p) {
	DbResult<size_t> res{};
	auto cr = collection(collectionName);
	if (!cr.status.ok()) {
		res.status = cr.status;
		return res;
	}
	return cr.value->updateMany(patch, std::forward<Pred>(p));
}
