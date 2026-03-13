#include "dbTest.h"

#include <memory>
#include <vector>

namespace {
struct SyncStatusCapture {
	std::vector<DBSyncStatus> statuses;
};

std::shared_ptr<SyncStatusCapture> attachSyncStatusCapture(ESPJsonDB &db) {
	auto capture = std::make_shared<SyncStatusCapture>();
	db.onSyncStatus([capture](const DBSyncStatus &status) { capture->statuses.push_back(status); });
	return capture;
}
} // namespace

void DbTester::syncStatusColdPreloadSequenceTest() {
	auto clearStatus = db.dropAll();
	if (!clearStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "dropAll() failed before sync status cold preload test: %s", clearStatus.message);
		return;
	}

	JsonDocument alpha;
	alpha["kind"] = "alpha";
	auto alphaCreate = db.create("sync_alpha", alpha.as<JsonObjectConst>());
	if (!alphaCreate.status.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "Failed to seed sync_alpha: %s", alphaCreate.status.message);
		return;
	}

	JsonDocument beta;
	beta["kind"] = "beta";
	auto betaCreate = db.create("sync_beta", beta.as<JsonObjectConst>());
	if (!betaCreate.status.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "Failed to seed sync_beta: %s", betaCreate.status.message);
		return;
	}

	auto seedSyncStatus = db.syncNow();
	if (!seedSyncStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "syncNow() failed while seeding cold preload test: %s", seedSyncStatus.message);
		return;
	}

	db.deinit();
	auto capture = attachSyncStatusCapture(db);

	ESPJsonDBConfig cfg;
	cfg.autosync = false;
	auto initStatus = db.init("/test_db", cfg);
	if (!initStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "re-init failed for sync status cold preload test: %s", initStatus.message);
		return;
	}

	if (capture->statuses.size() < 7) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "sync status cold preload expected at least 7 entries, got %d",
		    static_cast<int>(capture->statuses.size())
		);
		return;
	}

	if (capture->statuses[0].stage != DBSyncStage::Idle) {
		ESP_LOGE(DB_TESTER_TAG, "sync status cold preload expected initial snapshot stage Idle");
		return;
	}

	const std::vector<DBSyncStage> expected = {
	    DBSyncStage::ColdSyncStarted,
	    DBSyncStage::ColdSyncCollectionStarted,
	    DBSyncStage::ColdSyncCollectionCompleted,
	    DBSyncStage::ColdSyncCollectionStarted,
	    DBSyncStage::ColdSyncCollectionCompleted,
	    DBSyncStage::ColdSyncCompleted};

	for (std::size_t i = 0; i < expected.size(); ++i) {
		if (capture->statuses[i + 1].stage != expected[i]) {
			ESP_LOGE(
			    DB_TESTER_TAG,
			    "sync status cold preload stage mismatch at index %d: %s",
			    static_cast<int>(i + 1),
			    dbSyncStageToString(capture->statuses[i + 1].stage)
			);
			return;
		}
		if (capture->statuses[i + 1].source != DBSyncSource::Init) {
			ESP_LOGE(DB_TESTER_TAG, "sync status cold preload expected Init source");
			return;
		}
	}

	const DBSyncStatus &start = capture->statuses[1];
	const DBSyncStatus &alphaStart = capture->statuses[2];
	const DBSyncStatus &alphaDone = capture->statuses[3];
	const DBSyncStatus &betaStart = capture->statuses[4];
	const DBSyncStatus &betaDone = capture->statuses[5];
	const DBSyncStatus &done = capture->statuses[6];

	if (start.collectionsTotal != 2 || start.collectionsCompleted != 0) {
		ESP_LOGE(DB_TESTER_TAG, "sync status cold preload start counters mismatch");
		return;
	}
	if (alphaStart.collectionName != "sync_alpha" || alphaDone.collectionName != "sync_alpha") {
		ESP_LOGE(DB_TESTER_TAG, "sync status cold preload first collection mismatch");
		return;
	}
	if (betaStart.collectionName != "sync_beta" || betaDone.collectionName != "sync_beta") {
		ESP_LOGE(DB_TESTER_TAG, "sync status cold preload second collection mismatch");
		return;
	}
	if (alphaDone.collectionsCompleted != 1 || betaDone.collectionsCompleted != 2) {
		ESP_LOGE(DB_TESTER_TAG, "sync status cold preload completed counters mismatch");
		return;
	}
	if (done.collectionsCompleted != 2 || done.collectionsTotal != 2 || !done.result.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "sync status cold preload completion payload mismatch");
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "Sync status cold preload sequence test passed");
}

void DbTester::syncStatusLateSubscriptionSnapshotTest() {
	auto capture = attachSyncStatusCapture(db);
	if (capture->statuses.size() != 1) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "late subscription expected immediate single snapshot, got %d",
		    static_cast<int>(capture->statuses.size())
		);
		return;
	}

	const DBSyncStatus &snapshot = capture->statuses[0];
	if (snapshot.stage != DBSyncStage::ColdSyncCompleted || snapshot.source != DBSyncSource::Init) {
		ESP_LOGE(DB_TESTER_TAG, "late subscription snapshot expected ColdSyncCompleted from Init");
		return;
	}
	if (snapshot.collectionsCompleted != 2 || snapshot.collectionsTotal != 2 || !snapshot.result.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "late subscription snapshot counters/status mismatch");
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "Sync status late subscription snapshot test passed");
}

void DbTester::syncStatusManualSyncNowTest() {
	db.deinit();
	ESPJsonDBConfig cfg;
	cfg.autosync = false;
	auto initStatus = db.init("/test_db", cfg);
	if (!initStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "re-init failed for manual sync status test: %s", initStatus.message);
		return;
	}

	auto capture = attachSyncStatusCapture(db);
	capture->statuses.clear(); // drop immediate snapshot; assert only manual transitions below

	JsonDocument doc;
	doc["kind"] = "manual_sync";
	auto createRes = db.create("sync_manual", doc.as<JsonObjectConst>());
	if (!createRes.status.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "manual sync status seed create failed: %s", createRes.status.message);
		return;
	}

	auto syncStatus = db.syncNow();
	if (!syncStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "syncNow() failed in manual sync status test: %s", syncStatus.message);
		return;
	}

	if (capture->statuses.size() != 2) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "manual sync status expected 2 transitions, got %d",
		    static_cast<int>(capture->statuses.size())
		);
		return;
	}

	const DBSyncStatus &start = capture->statuses[0];
	const DBSyncStatus &done = capture->statuses[1];
	if (start.stage != DBSyncStage::SyncNowStarted || done.stage != DBSyncStage::SyncNowCompleted) {
		ESP_LOGE(DB_TESTER_TAG, "manual sync status stage sequence mismatch");
		return;
	}
	if (start.source != DBSyncSource::SyncNow || done.source != DBSyncSource::SyncNow) {
		ESP_LOGE(DB_TESTER_TAG, "manual sync status expected SyncNow source");
		return;
	}
	if (!done.result.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "manual sync status completion expected ok result");
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "Sync status manual syncNow test passed");
}

void DbTester::syncStatusPeriodicExclusionTest() {
	db.deinit();
	ESPJsonDBConfig cfg;
	cfg.autosync = true;
	cfg.intervalMs = 50;
	auto initStatus = db.init("/test_db", cfg);
	if (!initStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "re-init failed for periodic exclusion test: %s", initStatus.message);
		return;
	}

	auto capture = attachSyncStatusCapture(db);
	capture->statuses.clear(); // ignore immediate snapshot

	JsonDocument doc;
	doc["kind"] = "periodic";
	auto createRes = db.create("sync_periodic", doc.as<JsonObjectConst>());
	if (!createRes.status.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "periodic exclusion seed create failed: %s", createRes.status.message);
		return;
	}

	delay(220);
	if (!capture->statuses.empty()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "periodic autosync should not emit sync status transitions, first=%s",
		    dbSyncStageToString(capture->statuses.front().stage)
		);
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "Sync status periodic exclusion test passed");
}
