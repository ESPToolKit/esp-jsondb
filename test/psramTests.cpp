#include "dbTest.h"

void DbTester::psramBufferWiringTest() {
	auto clearStatus = db.dropAll();
	if (!clearStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "dropAll() failed before PSRAM wiring test: %s", clearStatus.message);
		return;
	}

	db.deinit();

	ESPJsonDBConfig cfg;
	cfg.autosync = false;
	cfg.usePSRAMBuffers = true;

	auto initStatus = db.init("/test_db", cfg);
	if (!initStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "init(usePSRAMBuffers=true) failed: %s", initStatus.message);
		return;
	}

	JsonDocument seedDoc;
	seedDoc["kind"] = "seed";
	auto createRes = db.create("psram_wiring", seedDoc.as<JsonObjectConst>());
	if (!createRes.status.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "create(seed) failed in PSRAM wiring test: %s", createRes.status.message);
		return;
	}

	auto lambdaUpsert = db.updateOne(
	    "psram_wiring",
	    [](const DocView &v) {
		    return v["kind"] == "lambda_match";
	    },
	    [](DocView &v) {
		    v["kind"].set("lambda_created");
	    },
	    true
	);
	if (!lambdaUpsert.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "updateOne(lambda, create=true) failed in PSRAM wiring test: %s",
		    lambdaUpsert.message
		);
		return;
	}

	JsonDocument filterDoc;
	filterDoc["kind"] = "json_match";
	JsonDocument patchDoc;
	patchDoc["kind"] = "json_created";
	patchDoc["marker"] = true;
	auto jsonUpsert = db.updateOne("psram_wiring", filterDoc, patchDoc, true);
	if (!jsonUpsert.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "updateOne(json, create=true) failed in PSRAM wiring test: %s",
		    jsonUpsert.message
		);
		return;
	}

	auto syncStatus = db.syncNow();
	if (!syncStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "syncNow() failed in PSRAM wiring test: %s", syncStatus.message);
		return;
	}

	db.deinit();
	initStatus = db.init("/test_db", cfg);
	if (!initStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "re-init(usePSRAMBuffers=true) failed: %s", initStatus.message);
		return;
	}

	auto findRes = db.findMany("psram_wiring", [](const DocView &) {
		return true;
	});
	if (!findRes.status.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "findMany(psram_wiring) failed: %s", findRes.status.message);
		return;
	}
	if (findRes.value.size() != 3) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "PSRAM wiring preload expected 3 docs, got %d",
		    static_cast<int>(findRes.value.size())
		);
		return;
	}

	bool hasSeed = false;
	bool hasLambdaCreated = false;
	bool hasJsonCreated = false;
	for (const auto &doc : findRes.value) {
		const char *kind = doc["kind"].as<const char *>();
		if (!kind)
			continue;
		if (strcmp(kind, "seed") == 0) {
			hasSeed = true;
		} else if (strcmp(kind, "lambda_created") == 0) {
			hasLambdaCreated = true;
		} else if (strcmp(kind, "json_created") == 0) {
			hasJsonCreated = true;
		}
	}

	if (!hasSeed || !hasLambdaCreated || !hasJsonCreated) {
		ESP_LOGE(DB_TESTER_TAG, "PSRAM wiring test documents mismatch after preload");
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "PSRAM buffer wiring test passed");
}
