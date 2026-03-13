#include "dbTest.h"

#include <algorithm>

namespace {
bool hasCollectionName(const std::vector<std::string> &names, const std::string &name) {
	return std::find(names.begin(), names.end(), name) != names.end();
}

std::string delayedCollectionPath(const std::string &name) {
	return std::string("/test_db/") + name;
}
} // namespace

void DbTester::delayedCollectionAccessBeforeAutosyncTickTest() {
	auto clearStatus = db.dropAll();
	if (!clearStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "dropAll() failed before delayed access test: %s", clearStatus.message);
		return;
	}

	JsonDocument immediateDoc;
	immediateDoc["kind"] = "immediate";
	auto immediateCreate = db.create("immediate_access", immediateDoc);
	if (!immediateCreate.status.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "Failed to seed immediate_access for delayed access test: %s",
		    immediateCreate.status.message
		);
		return;
	}

	JsonDocument delayedDoc;
	delayedDoc["kind"] = "delayed";
	auto delayedCreate = db.create("delayed_access", delayedDoc);
	if (!delayedCreate.status.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "Failed to seed delayed_access for delayed access test: %s",
		    delayedCreate.status.message
		);
		return;
	}

	auto syncStatus = db.syncNow();
	if (!syncStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "syncNow() failed while seeding delayed access test: %s", syncStatus.message);
		return;
	}

	db.deinit();
	ESPJsonDBConfig cfg;
	cfg.autosync = true;
	cfg.intervalMs = 60000; // Keep autosync tick away so first access path is deterministic.
	cfg.delayedCollectionSyncArray = {"delayed_access"};
	auto initStatus = db.init("/test_db", cfg);
	if (!initStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "re-init failed for delayed access test: %s", initStatus.message);
		return;
	}

	auto namesBefore = db.getAllCollectionName();
	if (hasCollectionName(namesBefore, "delayed_access")) {
		ESP_LOGE(DB_TESTER_TAG, "delayed_access was preloaded but should have been deferred");
		return;
	}

	auto immediateFind = db.findMany("immediate_access", [](const DocView &) {
		return true;
	});
	if (!immediateFind.status.ok() || immediateFind.value.size() != 1) {
		ESP_LOGE(DB_TESTER_TAG, "immediate_access was not available after init in delayed access test");
		return;
	}

	auto delayedFind = db.findMany("delayed_access", [](const DocView &) {
		return true;
	});
	if (!delayedFind.status.ok() || delayedFind.value.size() != 1) {
		ESP_LOGE(DB_TESTER_TAG, "delayed_access did not load correctly on first access");
		return;
	}

	auto namesAfter = db.getAllCollectionName();
	if (!hasCollectionName(namesAfter, "delayed_access")) {
		ESP_LOGE(DB_TESTER_TAG, "delayed_access not tracked after first access load");
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "Delayed access before autosync tick test passed");
}

void DbTester::delayedCollectionSyncNowFallbackTest() {
	auto clearStatus = db.dropAll();
	if (!clearStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "dropAll() failed before syncNow fallback test: %s", clearStatus.message);
		return;
	}

	JsonDocument delayedDoc;
	delayedDoc["mode"] = "syncNow";
	auto delayedCreate = db.create("delayed_syncnow", delayedDoc);
	if (!delayedCreate.status.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "Failed to seed delayed_syncnow for syncNow fallback test: %s",
		    delayedCreate.status.message
		);
		return;
	}

	auto seedSyncStatus = db.syncNow();
	if (!seedSyncStatus.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "syncNow() failed while seeding syncNow fallback test: %s",
		    seedSyncStatus.message
		);
		return;
	}

	db.deinit();
	ESPJsonDBConfig cfg;
	cfg.autosync = false;
	cfg.delayedCollectionSyncArray = {"delayed_syncnow"};
	auto initStatus = db.init("/test_db", cfg);
	if (!initStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "re-init failed for syncNow fallback test: %s", initStatus.message);
		return;
	}

	auto namesBefore = db.getAllCollectionName();
	if (hasCollectionName(namesBefore, "delayed_syncnow")) {
		ESP_LOGE(DB_TESTER_TAG, "delayed_syncnow was preloaded but should wait for syncNow");
		return;
	}

	auto triggerStatus = db.syncNow();
	if (!triggerStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "syncNow() trigger failed in syncNow fallback test: %s", triggerStatus.message);
		return;
	}

	auto namesAfter = db.getAllCollectionName();
	if (!hasCollectionName(namesAfter, "delayed_syncnow")) {
		ESP_LOGE(DB_TESTER_TAG, "delayed_syncnow was not loaded by syncNow fallback path");
		return;
	}

	auto delayedFind = db.findMany("delayed_syncnow", [](const DocView &) {
		return true;
	});
	if (!delayedFind.status.ok() || delayedFind.value.size() != 1) {
		ESP_LOGE(DB_TESTER_TAG, "delayed_syncnow content missing after syncNow fallback load");
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "Delayed syncNow fallback test passed");
}

void DbTester::delayedCollectionDropBeforeLoadTest() {
	auto clearStatus = db.dropAll();
	if (!clearStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "dropAll() failed before delayed drop test: %s", clearStatus.message);
		return;
	}

	JsonDocument doc;
	doc["drop"] = true;
	auto createRes = db.create("drop_before_load", doc);
	if (!createRes.status.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "Failed to seed drop_before_load for delayed drop test: %s",
		    createRes.status.message
		);
		return;
	}

	auto seedSyncStatus = db.syncNow();
	if (!seedSyncStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "syncNow() failed while seeding delayed drop test: %s", seedSyncStatus.message);
		return;
	}

	db.deinit();
	ESPJsonDBConfig cfg;
	cfg.autosync = false;
	cfg.delayedCollectionSyncArray = {"drop_before_load"};
	auto initStatus = db.init("/test_db", cfg);
	if (!initStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "re-init failed for delayed drop test: %s", initStatus.message);
		return;
	}

	auto dropStatus = db.dropCollection("drop_before_load");
	if (!dropStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "dropCollection(drop_before_load) failed: %s", dropStatus.message);
		return;
	}

	auto syncStatus = db.syncNow();
	if (!syncStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "syncNow() failed while dropping delayed collection: %s", syncStatus.message);
		return;
	}

	if (LittleFS.exists(delayedCollectionPath("drop_before_load").c_str())) {
		ESP_LOGE(DB_TESTER_TAG, "drop_before_load directory still exists after delayed drop sync");
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "Delayed drop-before-load test passed");
}

void DbTester::delayedCollectionConfigNormalizationTest() {
	auto clearStatus = db.dropAll();
	if (!clearStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "dropAll() failed before delayed normalization test: %s", clearStatus.message);
		return;
	}

	JsonDocument doc;
	doc["value"] = 42;
	auto createRes = db.create("dup_collection", doc);
	if (!createRes.status.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "Failed to seed dup_collection for delayed normalization test: %s",
		    createRes.status.message
		);
		return;
	}

	auto seedSyncStatus = db.syncNow();
	if (!seedSyncStatus.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "syncNow() failed while seeding delayed normalization test: %s",
		    seedSyncStatus.message
		);
		return;
	}

	db.deinit();
	ESPJsonDBConfig cfg;
	cfg.autosync = false;
	cfg.delayedCollectionSyncArray = {"dup_collection", "dup_collection", "", "_files"};
	auto initStatus = db.init("/test_db", cfg);
	if (!initStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "re-init failed for delayed normalization test: %s", initStatus.message);
		return;
	}

	auto namesBefore = db.getAllCollectionName();
	if (hasCollectionName(namesBefore, "dup_collection")) {
		ESP_LOGE(DB_TESTER_TAG, "dup_collection was preloaded but should have been deferred");
		return;
	}

	auto triggerStatus = db.syncNow();
	if (!triggerStatus.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "syncNow() failed while triggering delayed normalization test: %s",
		    triggerStatus.message
		);
		return;
	}

	auto namesAfter = db.getAllCollectionName();
	if (!hasCollectionName(namesAfter, "dup_collection")) {
		ESP_LOGE(DB_TESTER_TAG, "dup_collection did not load after syncNow in normalization test");
		return;
	}

	auto findRes = db.findMany("dup_collection", [](const DocView &) {
		return true;
	});
	if (!findRes.status.ok() || findRes.value.size() != 1) {
		ESP_LOGE(DB_TESTER_TAG, "dup_collection content invalid after normalization test load");
		return;
	}

	auto reservedRes = db.collection("_files");
	if (reservedRes.status.code != DbStatusCode::InvalidArgument) {
		ESP_LOGE(DB_TESTER_TAG, "reserved delayed collection name was not rejected correctly");
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "Delayed config normalization test passed");
}
