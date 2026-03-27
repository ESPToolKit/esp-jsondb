#include "dbTest.h"

namespace {
bool isHex24(const std::string &id) {
	if (id.size() != 24)
		return false;
	for (char c : id) {
		const bool isDigit = (c >= '0' && c <= '9');
		const bool isLower = (c >= 'a' && c <= 'f');
		const bool isUpper = (c >= 'A' && c <= 'F');
		if (!isDigit && !isLower && !isUpper)
			return false;
	}
	return true;
}

std::string collectionDirPath(const std::string &collection) {
	return std::string("/test_db/") + collection;
}

std::string documentPath(const std::string &collection, const std::string &id) {
	return collectionDirPath(collection) + "/" + id + ".jdb";
}
} // namespace

void DbTester::simpleDocCreate() {
	lastNewDocId = "";
	JsonDocument newUser;
	newUser["email"] = "espjsondb@gmail.com";
	newUser["username"] = "esp-jsondb";
	auto result = db.create("users", newUser.as<JsonObjectConst>());
	if (!result.status.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "Failed to add new user to DB. Error: %s", result.status.message);
	} else {
		ESP_LOGI(DB_TESTER_TAG, "New user created");
		lastNewDocId = result.value;
	}
}

void DbTester::simpleDocRemove() {
	if (lastNewDocId.empty())
		return;
	auto result = db.removeById("users", lastNewDocId);
	if (!result.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "Failed to remove user from DB. Error: %s", result.message);
	} else {
		ESP_LOGI(DB_TESTER_TAG, "User removed");
	}
}

void DbTester::multiDocCreate(int docNum) {
	int created = 0;
	for (int index = 0; index < docNum; index++) {
		JsonDocument newUser;
		newUser["email"] = "espjsondb_" + std::to_string(index) + "_@gmail.com";
		newUser["username"] = "esp-jsondb_" + std::to_string(index);
		newUser["role"] = index % 2 ? "admin" : "user";
		auto result = db.create("users", newUser.as<JsonObjectConst>());
		if (result.status.ok()) {
			created++;
		} else {
			ESP_LOGE(
			    DB_TESTER_TAG,
			    "Failed to add new user (%s) to DB. Error: %s",
			    newUser["email"],
			    result.status.message
			);
		}
	}
	ESP_LOGI(DB_TESTER_TAG, "Created %d document", created);
}

void DbTester::multiDocRemove() {
	// Remove all admins
	auto result = db.removeMany("users", [](const DocView &doc) {
		return doc["role"].as<std::string>() == "admin";
	});
	if (!result.status.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "Failed to remove users with admin role. Error: %s",
		    result.status.message
		);
	} else {
		ESP_LOGI(DB_TESTER_TAG, "Removed %d users", result.value);
	}
}

void DbTester::idLifecycleRoundTripTest() {
	const std::string collection = "id_lifecycle";
	(void)db.dropCollection(collection);

	JsonDocument seed;
	seed["kind"] = "id_lifecycle";
	seed["value"] = 1;

	auto createRes = db.create(collection, seed.as<JsonObjectConst>());
	if (!createRes.status.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "idLifecycleRoundTripTest create failed: %s", createRes.status.message);
		return;
	}
	const std::string id = createRes.value;
	if (!isHex24(id)) {
		ESP_LOGE(DB_TESTER_TAG, "idLifecycleRoundTripTest expected 24-char hex id");
		return;
	}

	auto findRes = db.findById(collection, id);
	if (!findRes.status.ok() || findRes.value["value"].as<int>() != 1) {
		ESP_LOGE(DB_TESTER_TAG, "idLifecycleRoundTripTest findById failed");
		return;
	}

	auto updateStatus = db.updateById(collection, id, [](DocView &doc) { doc["value"] = 2; });
	if (!updateStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "idLifecycleRoundTripTest updateById failed: %s", updateStatus.message);
		return;
	}

	auto findUpdated = db.findById(collection, id);
	if (!findUpdated.status.ok() || findUpdated.value["value"].as<int>() != 2) {
		ESP_LOGE(DB_TESTER_TAG, "idLifecycleRoundTripTest update verification failed");
		return;
	}

	auto removeStatus = db.removeById(collection, id);
	if (!removeStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "idLifecycleRoundTripTest removeById failed: %s", removeStatus.message);
		return;
	}

	auto findRemoved = db.findById(collection, id);
	if (findRemoved.status.code != DbStatusCode::NotFound) {
		ESP_LOGE(DB_TESTER_TAG, "idLifecycleRoundTripTest expected NotFound after remove");
		return;
	}

	auto invalidFind = db.findById(collection, "invalid-id");
	if (invalidFind.status.code != DbStatusCode::NotFound) {
		ESP_LOGE(DB_TESTER_TAG, "idLifecycleRoundTripTest invalid-id lookup expected NotFound");
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "ID lifecycle roundtrip test passed");
}

void DbTester::snapshotRestoreIdLifecycleTest() {
	auto dropStatus = db.dropAll();
	if (!dropStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotRestoreIdLifecycleTest dropAll failed: %s", dropStatus.message);
		return;
	}

	const std::string collection = "snapshot_ids";
	std::vector<std::string> ids;
	ids.reserve(2);
	for (int i = 0; i < 2; ++i) {
		JsonDocument doc;
		doc["index"] = i;
		doc["kind"] = "snapshot";
		auto createRes = db.create(collection, doc.as<JsonObjectConst>());
		if (!createRes.status.ok()) {
			ESP_LOGE(
			    DB_TESTER_TAG,
			    "snapshotRestoreIdLifecycleTest create failed: %s",
			    createRes.status.message
			);
			return;
		}
		if (!isHex24(createRes.value)) {
			ESP_LOGE(DB_TESTER_TAG, "snapshotRestoreIdLifecycleTest invalid generated id");
			return;
		}
		ids.push_back(createRes.value);
	}

	auto syncStatus = db.syncNow();
	if (!syncStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotRestoreIdLifecycleTest sync failed: %s", syncStatus.message);
		return;
	}

	auto snapshot = db.getSnapshot(SnapshotMode::OnDiskOnly);
	JsonArrayConst arr = snapshot["collections"][collection.c_str()].as<JsonArrayConst>();
	if (arr.isNull() || arr.size() != ids.size()) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotRestoreIdLifecycleTest snapshot collection missing");
		return;
	}
	for (JsonObjectConst obj : arr) {
		const char *id = obj["_id"].as<const char *>();
		if (!id || !isHex24(id)) {
			ESP_LOGE(DB_TESTER_TAG, "snapshotRestoreIdLifecycleTest snapshot _id format invalid");
			return;
		}
		JsonObjectConst meta = obj["_meta"].as<JsonObjectConst>();
		if (meta.isNull() || meta["createdAtMs"].isNull() || meta["updatedAtMs"].isNull() ||
		    meta["revision"].isNull()) {
			ESP_LOGE(DB_TESTER_TAG, "snapshotRestoreIdLifecycleTest snapshot metadata missing");
			return;
		}
	}

	JsonDocument badSnapshot;
	badSnapshot.set(snapshot);
	badSnapshot["collections"][collection.c_str()][0]["_id"] = "invalid-id";
	auto badRestoreStatus = db.restoreFromSnapshot(badSnapshot);
	if (badRestoreStatus.code != DbStatusCode::InvalidArgument) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotRestoreIdLifecycleTest expected invalid _id restore failure");
		return;
	}

	dropStatus = db.dropAll();
	if (!dropStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotRestoreIdLifecycleTest second dropAll failed: %s", dropStatus.message);
		return;
	}

	auto restoreStatus = db.restoreFromSnapshot(snapshot);
	if (!restoreStatus.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "snapshotRestoreIdLifecycleTest restore failed: %s",
		    restoreStatus.message
		);
		return;
	}

	for (std::size_t i = 0; i < ids.size(); ++i) {
		auto findRes = db.findById(collection, ids[i]);
		if (!findRes.status.ok()) {
			ESP_LOGE(
			    DB_TESTER_TAG,
			    "snapshotRestoreIdLifecycleTest findById after restore failed: %s",
			    findRes.status.message
			);
			return;
		}
		if (findRes.value["index"].as<int>() != static_cast<int>(i)) {
			ESP_LOGE(DB_TESTER_TAG, "snapshotRestoreIdLifecycleTest restored payload mismatch");
			return;
		}
		if (findRes.value.meta().createdAtMs == 0 || findRes.value.meta().updatedAtMs == 0 ||
		    findRes.value.meta().revision == 0) {
			ESP_LOGE(DB_TESTER_TAG, "snapshotRestoreIdLifecycleTest restored metadata missing");
			return;
		}
	}

	ESP_LOGI(DB_TESTER_TAG, "Snapshot restore ID lifecycle test passed");
}

void DbTester::documentFileDeletionOnSyncTest() {
	const std::string collection = "sync_delete_files";
	auto clearStatus = db.dropAll();
	if (!clearStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "documentFileDeletionOnSyncTest dropAll failed: %s", clearStatus.message);
		return;
	}

	JsonDocument firstDoc;
	firstDoc["kind"] = "delete_one";
	auto firstCreate = db.create(collection, firstDoc.as<JsonObjectConst>());
	if (!firstCreate.status.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "documentFileDeletionOnSyncTest first create failed: %s",
		    firstCreate.status.message
		);
		return;
	}

	JsonDocument secondDoc;
	secondDoc["kind"] = "delete_many";
	auto secondCreate = db.create(collection, secondDoc.as<JsonObjectConst>());
	if (!secondCreate.status.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "documentFileDeletionOnSyncTest second create failed: %s",
		    secondCreate.status.message
		);
		return;
	}

	auto seedSync = db.syncNow();
	if (!seedSync.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "documentFileDeletionOnSyncTest seed sync failed: %s", seedSync.message);
		return;
	}

	const std::string firstPath = documentPath(collection, firstCreate.value);
	const std::string secondPath = documentPath(collection, secondCreate.value);
	if (!LittleFS.exists(firstPath.c_str()) || !LittleFS.exists(secondPath.c_str())) {
		ESP_LOGE(DB_TESTER_TAG, "documentFileDeletionOnSyncTest expected seeded files on disk");
		return;
	}

	auto removeOne = db.removeById(collection, firstCreate.value);
	if (!removeOne.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "documentFileDeletionOnSyncTest removeById failed: %s", removeOne.message);
		return;
	}

	auto removeMany = db.removeMany(collection, [](const DocView &doc) {
		return doc["kind"].as<std::string>() == "delete_many";
	});
	if (!removeMany.status.ok() || removeMany.value != 1) {
		ESP_LOGE(DB_TESTER_TAG, "documentFileDeletionOnSyncTest removeMany failed");
		return;
	}

	auto cleanupSync = db.syncNow();
	if (!cleanupSync.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "documentFileDeletionOnSyncTest cleanup sync failed: %s", cleanupSync.message);
		return;
	}

	if (LittleFS.exists(firstPath.c_str()) || LittleFS.exists(secondPath.c_str())) {
		ESP_LOGE(DB_TESTER_TAG, "documentFileDeletionOnSyncTest stale document files remain after sync");
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "Document file deletion on sync test passed");
}
