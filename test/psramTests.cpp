#include "dbTest.h"
#include <atomic>
#include <cstring>
#include <memory>
#include <vector>

#if defined(ARDUINO_ARCH_ESP32) && __has_include(<esp_heap_caps.h>)
#include <esp_heap_caps.h>
#define ESP_JSONDB_HAS_HEAP_CAPS 1
#else
#define ESP_JSONDB_HAS_HEAP_CAPS 0
#endif

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

void DbTester::psramMemoryBenchmarkTest() {
#if !ESP_JSONDB_HAS_HEAP_CAPS
	ESP_LOGI(DB_TESTER_TAG, "PSRAM memory benchmark skipped (heap_caps unavailable)");
	return;
#else
	struct BenchmarkResult {
		size_t preloadInternalUsed = 0;
		size_t uploadInternalPeakUsed = 0;
		bool ok = false;
	};

	auto runScenario = [this](bool usePSRAM, BenchmarkResult &out) -> bool {
		db.deinit();

		ESPJsonDBConfig cfg;
		cfg.autosync = false;
		cfg.usePSRAMBuffers = usePSRAM;

		auto initStatus = db.init("/test_db", cfg);
		if (!initStatus.ok()) {
			ESP_LOGE(DB_TESTER_TAG, "benchmark init failed: %s", initStatus.message);
			return false;
		}
		auto clearStatus = db.dropAll();
		if (!clearStatus.ok()) {
			ESP_LOGE(DB_TESTER_TAG, "benchmark dropAll failed: %s", clearStatus.message);
			return false;
		}

		const size_t preloadStartInternal = heap_caps_get_free_size(MALLOC_CAP_INTERNAL);
		for (int i = 0; i < 120; ++i) {
			JsonDocument doc;
			doc["kind"] = "bench_preload";
			doc["index"] = i;
			doc["payload"] = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
			auto createRes = db.create("bench_preload", doc.as<JsonObjectConst>());
			if (!createRes.status.ok()) {
				ESP_LOGE(DB_TESTER_TAG, "benchmark create failed: %s", createRes.status.message);
				return false;
			}
		}
		auto syncStatus = db.syncNow();
		if (!syncStatus.ok()) {
			ESP_LOGE(DB_TESTER_TAG, "benchmark preload sync failed: %s", syncStatus.message);
			return false;
		}

		db.deinit();
		initStatus = db.init("/test_db", cfg);
		if (!initStatus.ok()) {
			ESP_LOGE(DB_TESTER_TAG, "benchmark preload re-init failed: %s", initStatus.message);
			return false;
		}

		const size_t preloadEndInternal = heap_caps_get_free_size(MALLOC_CAP_INTERNAL);
		out.preloadInternalUsed =
		    preloadStartInternal > preloadEndInternal ? (preloadStartInternal - preloadEndInternal) : 0;

		struct UploadCtx {
			std::vector<uint8_t> payload;
			size_t offset = 0;
		};

		const size_t uploadStartInternal = heap_caps_get_free_size(MALLOC_CAP_INTERNAL);
		size_t minInternalFree = uploadStartInternal;

		const size_t uploadCount = 20;
		std::atomic<size_t> doneCount{0};
		volatile bool uploadFailed = false;
		std::vector<std::string> uploadPaths;
		uploadPaths.reserve(uploadCount);
		std::vector<std::shared_ptr<UploadCtx>> contexts;
		contexts.reserve(uploadCount);

		ESPJsonDBFileOptions opts;
		opts.overwrite = true;
		opts.chunkSize = 128;

		for (size_t i = 0; i < uploadCount; ++i) {
			auto ctx = std::make_shared<UploadCtx>();
			ctx->payload.resize(1024, static_cast<uint8_t>(0x40 + (i % 16)));
			contexts.push_back(ctx);

			const std::string path = "bench/upload_" + std::to_string(i) + ".bin";
			uploadPaths.push_back(path);

			DbFileUploadPullCb pullCb =
			    [ctx](size_t requested, uint8_t *buffer, size_t &produced, bool &eof) -> DbStatus {
				if (!buffer)
					return {DbStatusCode::InvalidArgument, "buffer is null"};
				if (ctx->offset >= ctx->payload.size()) {
					produced = 0;
					eof = true;
					return {DbStatusCode::Ok, ""};
				}
				const size_t remaining = ctx->payload.size() - ctx->offset;
				const size_t take = remaining < requested ? remaining : requested;
				memcpy(buffer, ctx->payload.data() + ctx->offset, take);
				ctx->offset += take;
				produced = take;
				eof = (ctx->offset >= ctx->payload.size());
				return {DbStatusCode::Ok, ""};
			};

			DbFileUploadDoneCb doneCb =
			    [&doneCount, &uploadFailed](uint32_t, const DbStatus &st, size_t bytesWritten) {
				if (!st.ok() || bytesWritten == 0) {
					uploadFailed = true;
				}
				doneCount.fetch_add(1);
			};

			auto asyncRes = db.writeFileStreamAsync(path, pullCb, opts, doneCb);
			if (!asyncRes.status.ok()) {
				ESP_LOGE(DB_TESTER_TAG, "benchmark async upload start failed: %s", asyncRes.status.message);
				return false;
			}
		}

		const uint32_t started = millis();
		while (doneCount.load() < uploadCount && (millis() - started) < 10000) {
			const size_t freeNow = heap_caps_get_free_size(MALLOC_CAP_INTERNAL);
			if (freeNow < minInternalFree)
				minInternalFree = freeNow;
			delay(5);
		}
		if (doneCount.load() != uploadCount || uploadFailed) {
			ESP_LOGE(DB_TESTER_TAG, "benchmark async uploads failed or timed out");
			return false;
		}

		out.uploadInternalPeakUsed =
		    uploadStartInternal > minInternalFree ? (uploadStartInternal - minInternalFree) : 0;

		for (const auto &path : uploadPaths) {
			(void)db.removeFile(path);
		}
		(void)db.dropAll();
		out.ok = true;
		return true;
	};

	BenchmarkResult baseline{};
	BenchmarkResult psram{};
	if (!runScenario(false, baseline))
		return;
	if (!runScenario(true, psram))
		return;

	const size_t psramBytes = heap_caps_get_total_size(MALLOC_CAP_SPIRAM);
	if (psramBytes == 0) {
		ESP_LOGI(
		    DB_TESTER_TAG,
		    "PSRAM memory benchmark ran without external PSRAM; fallback behavior verified"
		);
		return;
	}

	const float preloadImprovement = baseline.preloadInternalUsed == 0
	                                     ? 0.0f
	                                     : (100.0f *
	                                        (static_cast<float>(baseline.preloadInternalUsed) -
	                                         static_cast<float>(psram.preloadInternalUsed)) /
	                                        static_cast<float>(baseline.preloadInternalUsed));
	const float uploadImprovement = baseline.uploadInternalPeakUsed == 0
	                                    ? 0.0f
	                                    : (100.0f *
	                                       (static_cast<float>(baseline.uploadInternalPeakUsed) -
	                                        static_cast<float>(psram.uploadInternalPeakUsed)) /
	                                       static_cast<float>(baseline.uploadInternalPeakUsed));

	ESP_LOGI(
	    DB_TESTER_TAG,
	    "PSRAM benchmark: preload internal used baseline=%u psram=%u (%.1f%% improvement)",
	    static_cast<unsigned>(baseline.preloadInternalUsed),
	    static_cast<unsigned>(psram.preloadInternalUsed),
	    preloadImprovement
	);
	ESP_LOGI(
	    DB_TESTER_TAG,
	    "PSRAM benchmark: upload internal peak baseline=%u psram=%u (%.1f%% improvement)",
	    static_cast<unsigned>(baseline.uploadInternalPeakUsed),
	    static_cast<unsigned>(psram.uploadInternalPeakUsed),
	    uploadImprovement
	);

	if (preloadImprovement < 10.0f) {
		ESP_LOGE(DB_TESTER_TAG, "PSRAM benchmark preload improvement below 10%% target");
		return;
	}
	if (uploadImprovement < 8.0f) {
		ESP_LOGE(DB_TESTER_TAG, "PSRAM benchmark upload improvement below 8%% target");
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "PSRAM memory benchmark test passed");
#endif
}
