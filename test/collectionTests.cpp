#include "dbTest.h"

namespace {
bool pathExists(const std::string &path) {
	return LittleFS.exists(path.c_str());
}
}

void DbTester::simpleCollectionCreate() {
	auto result = db.collection("sensors");
	if (!result.status.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "Failed to create 'sensors' collection. Error: %s",
		    result.status.message
		);
	} else {
		ESP_LOGI(DB_TESTER_TAG, "Created 'sensors' collection");
	}
}

void DbTester::simpleCollectionRemove() {
	auto result = db.dropCollection("sensors");
	if (!result.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "Failed to drop 'sensors' collection. Error: %s", result.message);
	} else {
		ESP_LOGI(DB_TESTER_TAG, "Dropped 'sensors' collection");
	}
}

void DbTester::collectionDirectoryCleanupOnSyncTest() {
	const std::string collectionName = "sync_drop_collection";
	const std::string collectionPath = std::string("/test_db/") + collectionName;
	auto clearStatus = db.dropAll();
	if (!clearStatus.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "collectionDirectoryCleanupOnSyncTest dropAll failed: %s",
		    clearStatus.message
		);
		return;
	}

	JsonDocument doc;
	doc["cleanup"] = true;
	auto createRes = db.create(collectionName, doc.as<JsonObjectConst>());
	if (!createRes.status.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "collectionDirectoryCleanupOnSyncTest create failed: %s",
		    createRes.status.message
		);
		return;
	}

	auto seedSync = db.syncNow();
	if (!seedSync.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "collectionDirectoryCleanupOnSyncTest seed sync failed: %s",
		    seedSync.message
		);
		return;
	}

	if (!pathExists(collectionPath)) {
		ESP_LOGE(DB_TESTER_TAG, "collectionDirectoryCleanupOnSyncTest expected collection dir on disk");
		return;
	}

	auto dropStatus = db.dropCollection(collectionName);
	if (!dropStatus.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "collectionDirectoryCleanupOnSyncTest dropCollection failed: %s",
		    dropStatus.message
		);
		return;
	}

	auto cleanupSync = db.syncNow();
	if (!cleanupSync.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "collectionDirectoryCleanupOnSyncTest cleanup sync failed: %s",
		    cleanupSync.message
		);
		return;
	}

	if (pathExists(collectionPath)) {
		ESP_LOGE(DB_TESTER_TAG, "collectionDirectoryCleanupOnSyncTest collection dir still exists");
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "Collection directory cleanup on sync test passed");
}

void DbTester::multiCollectionCreate(int collNum) {
	int created = 0;
	for (int i = 0; i < collNum; i++) {
		std::string collectionName = "test_" + std::to_string(i);
		auto result = db.collection(collectionName);
		if (!result.status.ok()) {
			ESP_LOGE(DB_TESTER_TAG, "Failed to create '%s' collection", collectionName.c_str());
			continue;
		}
		created++;
	}
	ESP_LOGI(DB_TESTER_TAG, "Created %d collection", created);
}

void DbTester::allCollectionDrop() {
	auto result = db.dropAll();
	if (!result.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "Failed to drop all collections. Error: %s", result.message);
	} else {
		ESP_LOGI(DB_TESTER_TAG, "Dropped all collections");
	}
}

void DbTester::dropAllRemovesBaseDirTest() {
	const std::string docCollection = "drop_all_cleanup";
	const std::string filePath = "/test_db/_files/drop_all_cleanup/payload.txt";
	auto clearStatus = db.dropAll();
	if (!clearStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "dropAllRemovesBaseDirTest initial dropAll failed: %s", clearStatus.message);
		return;
	}

	JsonDocument doc;
	doc["dropAll"] = true;
	auto createRes = db.create(docCollection, doc.as<JsonObjectConst>());
	if (!createRes.status.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "dropAllRemovesBaseDirTest create failed: %s", createRes.status.message);
		return;
	}

	auto fileWrite = db.files().writeTextFile("drop_all_cleanup/payload.txt", "cleanup");
	if (!fileWrite.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "dropAllRemovesBaseDirTest writeTextFile failed: %s", fileWrite.message);
		return;
	}

	auto seedSync = db.syncNow();
	if (!seedSync.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "dropAllRemovesBaseDirTest seed sync failed: %s", seedSync.message);
		return;
	}

	if (!pathExists("/test_db") || !pathExists(filePath)) {
		ESP_LOGE(DB_TESTER_TAG, "dropAllRemovesBaseDirTest expected seeded baseDir contents");
		return;
	}

	auto dropStatus = db.dropAll();
	if (!dropStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "dropAllRemovesBaseDirTest dropAll failed: %s", dropStatus.message);
		return;
	}

	if (!pathExists("/test_db")) {
		ESP_LOGE(DB_TESTER_TAG, "dropAllRemovesBaseDirTest baseDir was not recreated after dropAll");
		return;
	}
	const std::string collectionPath = std::string("/test_db/") + docCollection;
	if (pathExists(filePath) || pathExists(collectionPath)) {
		ESP_LOGE(DB_TESTER_TAG, "dropAllRemovesBaseDirTest stale collection or file data remain");
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "dropAll baseDir cleanup test passed");
}
