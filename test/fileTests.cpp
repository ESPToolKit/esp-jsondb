#include "dbTest.h"
#include <atomic>
#include <cstring>

namespace {
JsonObjectConst findFileEntry(JsonArrayConst entries, const char *path) {
	for (JsonObjectConst entry : entries) {
		const char *entryPath = entry["path"].as<const char *>();
		if (entryPath && strcmp(entryPath, path) == 0) {
			return entry;
		}
	}
	return JsonObjectConst();
}
} // namespace

void DbTester::fileStorageTest() {
	const std::string textPath = "docs/sample.txt";
	const std::string textPayload = "ESPJsonDB file storage test";

	auto textWrite = db.files().writeTextFile(textPath, textPayload, true);
	if (!textWrite.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "writeTextFile failed: %s", textWrite.message);
		return;
	}

	auto exists = db.files().fileExists(textPath);
	if (!exists.status.ok() || !exists.value) {
		ESP_LOGE(DB_TESTER_TAG, "fileExists failed for text payload");
		return;
	}
	auto size = db.files().fileSize(textPath);
	if (!size.status.ok() || size.value != textPayload.size()) {
		ESP_LOGE(DB_TESTER_TAG, "fileSize failed for text payload");
		return;
	}

	auto textRead = db.files().readTextFile(textPath);
	if (!textRead.status.ok() || textRead.value != textPayload) {
		ESP_LOGE(DB_TESTER_TAG, "readTextFile failed or content mismatch");
		return;
	}

	std::vector<uint8_t> binaryPayload;
	binaryPayload.reserve(64);
	for (uint8_t i = 0; i < 64; ++i) {
		binaryPayload.push_back(i);
	}

	const std::string binPath = "bin/source.bin";
	auto binWrite = db.files().writeFile(binPath, binaryPayload.data(), binaryPayload.size(), true);
	if (!binWrite.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "writeFile failed: %s", binWrite.message);
		return;
	}

	File src = LittleFS.open("/test_db/_files/bin/source.bin", FILE_READ);
	if (!src) {
		ESP_LOGE(DB_TESTER_TAG, "Failed to open source binary file for streaming");
		return;
	}

	ESPJsonDBFileOptions streamOpts;
	streamOpts.overwrite = true;
	streamOpts.chunkSize = 128;
	auto streamWrite = db.files().writeFileStream("bin/copied.bin", src, src.size(), streamOpts);
	src.close();
	if (!streamWrite.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "writeFileStream failed: %s", streamWrite.message);
		return;
	}

	auto copied = db.files().readFile("bin/copied.bin");
	if (!copied.status.ok() || copied.value != binaryPayload) {
		ESP_LOGE(DB_TESTER_TAG, "readFile failed or binary mismatch");
		return;
	}

	auto copiedFromPath = db.files().writeFileFromPath(
	    "bin/copied_from_path.bin",
	    "/test_db/_files/bin/source.bin",
	    streamOpts
	);
	if (!copiedFromPath.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "writeFileFromPath failed: %s", copiedFromPath.message);
		return;
	}

	auto copiedFromPathRead = db.files().readFile("bin/copied_from_path.bin");
	if (!copiedFromPathRead.status.ok() || copiedFromPathRead.value != binaryPayload) {
		ESP_LOGE(DB_TESTER_TAG, "writeFileFromPath readback mismatch");
		return;
	}

	struct SyncPullCtx {
		const uint8_t *data = nullptr;
		size_t size = 0;
		size_t offset = 0;
	} syncCtx;
	syncCtx.data = binaryPayload.data();
	syncCtx.size = binaryPayload.size();

	auto pullCb =
	    [&syncCtx](size_t requested, uint8_t *buffer, size_t &produced, bool &eof) -> DbStatus {
		if (!buffer)
			return {DbStatusCode::InvalidArgument, "buffer is null"};
		if (syncCtx.offset >= syncCtx.size) {
			produced = 0;
			eof = true;
			return {DbStatusCode::Ok, ""};
		}
		size_t remaining = syncCtx.size - syncCtx.offset;
		size_t take = remaining < requested ? remaining : requested;
		memcpy(buffer, syncCtx.data + syncCtx.offset, take);
		syncCtx.offset += take;
		produced = take;
		eof = (syncCtx.offset >= syncCtx.size);
		return {DbStatusCode::Ok, ""};
	};

	auto syncCbWrite =
	    db.files().writeFileStream("bin/copied_from_callback.bin", pullCb, streamOpts);
	if (!syncCbWrite.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "sync callback writeFileStream failed: %s", syncCbWrite.message);
		return;
	}

	auto copiedFromCallback = db.files().readFile("bin/copied_from_callback.bin");
	if (!copiedFromCallback.status.ok() || copiedFromCallback.value != binaryPayload) {
		ESP_LOGE(DB_TESTER_TAG, "sync callback stream readback mismatch");
		return;
	}

	auto sourceNotFound = db.files().writeFileFromPath(
	    "bin/not_created.bin",
	    "/test_db/_files/bin/missing.bin",
	    streamOpts
	);
	if (sourceNotFound.ok() || sourceNotFound.code != DbStatusCode::NotFound) {
		ESP_LOGE(DB_TESTER_TAG, "writeFileFromPath missing-source check failed");
		return;
	}

	auto invalidProducer = db.files().writeFileStream(
	    "bin/invalid_callback.bin",
	    [](size_t requested, uint8_t *, size_t &produced, bool &eof) -> DbStatus {
		    produced = requested + 1;
		    eof = false;
		    return {DbStatusCode::Ok, ""};
	    },
	    streamOpts
	);
	if (invalidProducer.ok() || invalidProducer.code != DbStatusCode::InvalidArgument) {
		ESP_LOGE(DB_TESTER_TAG, "writeFileStream invalid producer check failed");
		return;
	}

	File sink = LittleFS.open("/test_db/stream_sink.txt", FILE_WRITE);
	if (!sink) {
		ESP_LOGE(DB_TESTER_TAG, "Failed to open sink file for readFileStream");
		return;
	}
	auto streamRead = db.files().readFileStream(textPath, sink, 96);
	sink.close();
	if (!streamRead.status.ok() || streamRead.value != textPayload.size()) {
		ESP_LOGE(DB_TESTER_TAG, "readFileStream failed: %s", streamRead.status.message);
		return;
	}

	auto removeText = db.files().removeFile(textPath);
	if (!removeText.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "removeFile failed: %s", removeText.message);
		return;
	}
	(void)db.files().removeFile("bin/source.bin");
	(void)db.files().removeFile("bin/copied.bin");
	(void)db.files().removeFile("bin/copied_from_path.bin");
	(void)db.files().removeFile("bin/copied_from_callback.bin");
	(void)db.files().removeFile("bin/invalid_callback.bin");

	ESP_LOGI(DB_TESTER_TAG, "File storage test passed");
}

void DbTester::fileMetadataDiscoveryTest() {
	auto clearStatus = db.dropAll();
	if (!clearStatus.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "fileMetadataDiscoveryTest dropAll failed: %s",
		    clearStatus.message
		);
		return;
	}

	auto topWrite = db.files().writeTextFile("top.txt", "root");
	auto infoWrite = db.files().writeTextFile("docs/info.txt", "metadata");
	auto nestedWrite = db.files().writeTextFile("docs/nested/child.txt", "nested");
	if (!topWrite.ok() || !infoWrite.ok() || !nestedWrite.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "fileMetadataDiscoveryTest failed to seed files");
		return;
	}

	JsonDocument userDoc;
	userDoc["type"] = "file-listing-should-ignore-collections";
	auto createRes = db.create("users", userDoc.as<JsonObjectConst>());
	if (!createRes.status.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "fileMetadataDiscoveryTest create collection doc failed: %s",
		    createRes.status.message
		);
		return;
	}

	auto syncStatus = db.syncNow();
	if (!syncStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "fileMetadataDiscoveryTest sync failed: %s", syncStatus.message);
		return;
	}

	auto fileInfo = db.files().getFileInfo("docs/info.txt");
	if (!fileInfo.status.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "fileMetadataDiscoveryTest getFileInfo(file) failed: %s",
		    fileInfo.status.message
		);
		return;
	}
	if (strcmp(fileInfo.value["path"] | "", "docs/info.txt") != 0 ||
	    strcmp(fileInfo.value["name"] | "", "info.txt") != 0 ||
	    !fileInfo.value["exists"].as<bool>() || fileInfo.value["isDirectory"].as<bool>() ||
	    fileInfo.value["size"].as<size_t>() != std::strlen("metadata")) {
		ESP_LOGE(DB_TESTER_TAG, "fileMetadataDiscoveryTest file metadata mismatch");
		return;
	}

	auto dirInfo = db.files().getFileInfo("docs");
	if (!dirInfo.status.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "fileMetadataDiscoveryTest getFileInfo(dir) failed: %s",
		    dirInfo.status.message
		);
		return;
	}
	if (strcmp(dirInfo.value["path"] | "", "docs") != 0 ||
	    strcmp(dirInfo.value["name"] | "", "docs") != 0 || !dirInfo.value["exists"].as<bool>() ||
	    !dirInfo.value["isDirectory"].as<bool>() || dirInfo.value["size"].as<size_t>() != 0) {
		ESP_LOGE(DB_TESTER_TAG, "fileMetadataDiscoveryTest directory metadata mismatch");
		return;
	}

	auto topLevel = db.files().listFiles("", false);
	if (!topLevel.status.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "fileMetadataDiscoveryTest top-level listFiles failed: %s",
		    topLevel.status.message
		);
		return;
	}
	JsonArrayConst topEntries = topLevel.value["entries"].as<JsonArrayConst>();
	auto topTxt = findFileEntry(topEntries, "top.txt");
	auto docsDir = findFileEntry(topEntries, "docs");
	auto nestedChildAtTop = findFileEntry(topEntries, "docs/nested/child.txt");
	auto leakedCollection = findFileEntry(topEntries, "users");
	if (topEntries.isNull() || topEntries.size() != 2 || topTxt.isNull() || docsDir.isNull() ||
	    !docsDir["isDirectory"].as<bool>() || !nestedChildAtTop.isNull() ||
	    !leakedCollection.isNull()) {
		ESP_LOGE(DB_TESTER_TAG, "fileMetadataDiscoveryTest top-level listing mismatch");
		return;
	}

	auto docsRecursive = db.files().listFiles("docs", true);
	if (!docsRecursive.status.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "fileMetadataDiscoveryTest recursive listFiles failed: %s",
		    docsRecursive.status.message
		);
		return;
	}
	JsonArrayConst docsEntries = docsRecursive.value["entries"].as<JsonArrayConst>();
	if (strcmp(docsRecursive.value["prefix"] | "", "docs") != 0 ||
	    !docsRecursive.value["recursive"].as<bool>() || docsEntries.size() != 3) {
		ESP_LOGE(DB_TESTER_TAG, "fileMetadataDiscoveryTest recursive listing header mismatch");
		return;
	}
	const char *expectedOrder[] = {"docs/info.txt", "docs/nested", "docs/nested/child.txt"};
	for (size_t i = 0; i < docsEntries.size(); ++i) {
		const char *path = docsEntries[i]["path"].as<const char *>();
		if (!path || strcmp(path, expectedOrder[i]) != 0) {
			ESP_LOGE(DB_TESTER_TAG, "fileMetadataDiscoveryTest recursive listing order mismatch");
			return;
		}
	}

	auto missingInfo = db.files().getFileInfo("missing.bin");
	if (missingInfo.status.code != DbStatusCode::NotFound) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "fileMetadataDiscoveryTest expected NotFound for missing file info"
		);
		return;
	}

	auto invalidInfo = db.files().getFileInfo("../escape");
	if (invalidInfo.status.code != DbStatusCode::InvalidArgument) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "fileMetadataDiscoveryTest expected InvalidArgument for invalid file info path"
		);
		return;
	}

	auto invalidList = db.files().listFiles("../escape", true);
	if (invalidList.status.code != DbStatusCode::InvalidArgument) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "fileMetadataDiscoveryTest expected InvalidArgument for invalid listFiles path"
		);
		return;
	}

	auto missingList = db.files().listFiles("missing_prefix", true);
	if (missingList.status.code != DbStatusCode::NotFound) {
		ESP_LOGE(DB_TESTER_TAG, "fileMetadataDiscoveryTest expected NotFound for missing prefix");
		return;
	}

	(void)db.files().removeFile("top.txt");
	(void)db.files().removeFile("docs/info.txt");
	(void)db.files().removeFile("docs/nested/child.txt");
	ESP_LOGI(DB_TESTER_TAG, "File metadata discovery test passed");
}

void DbTester::asyncFileUploadTest() {
	struct UploadCtx {
		const uint8_t *data = nullptr;
		size_t size = 0;
		size_t offset = 0;
	} ctx;

	std::vector<uint8_t> payload;
	payload.reserve(513);
	for (size_t i = 0; i < 513; ++i) {
		payload.push_back(static_cast<uint8_t>(i & 0xFF));
	}
	ctx.data = payload.data();
	ctx.size = payload.size();
	ctx.offset = 0;

	volatile bool done = false;
	volatile bool doneOk = false;
	volatile size_t doneBytes = 0;

	DbFileUploadPullCb pullCb =
	    [&ctx](size_t requested, uint8_t *buffer, size_t &produced, bool &eof) -> DbStatus {
		if (!buffer)
			return {DbStatusCode::InvalidArgument, "buffer is null"};
		if (ctx.offset >= ctx.size) {
			produced = 0;
			eof = true;
			return {DbStatusCode::Ok, ""};
		}
		size_t remaining = ctx.size - ctx.offset;
		size_t take = remaining < requested ? remaining : requested;
		memcpy(buffer, ctx.data + ctx.offset, take);
		ctx.offset += take;
		produced = take;
		eof = (ctx.offset >= ctx.size);
		return {DbStatusCode::Ok, ""};
	};

	DbFileUploadDoneCb doneCb =
	    [&done, &doneOk, &doneBytes](uint32_t, const DbStatus &st, size_t bytesWritten) {
		    doneOk = st.ok();
		    doneBytes = bytesWritten;
		    done = true;
	    };

	ESPJsonDBFileOptions opts;
	opts.overwrite = true;
	opts.chunkSize = 96;
	auto asyncRes = db.files().writeFileStreamAsync("async/payload.bin", pullCb, opts, doneCb);
	if (!asyncRes.status.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "writeFileStreamAsync failed: %s", asyncRes.status.message);
		return;
	}

	uint32_t uploadId = asyncRes.value;
	const uint32_t started = millis();
	while (!done && (millis() - started) < 5000) {
		delay(10);
	}
	if (!done) {
		ESP_LOGE(DB_TESTER_TAG, "Async upload timed out");
		(void)db.files().cancelUpload(uploadId);
		return;
	}
	if (!doneOk || doneBytes != payload.size()) {
		ESP_LOGE(DB_TESTER_TAG, "Async upload completion status mismatch");
		return;
	}

	auto stateRes = db.files().getUploadState(uploadId);
	if (!stateRes.status.ok() || stateRes.value != DbFileUploadState::Completed) {
		ESP_LOGE(DB_TESTER_TAG, "Async upload state mismatch");
		return;
	}

	auto readBack = db.files().readFile("async/payload.bin");
	if (!readBack.status.ok() || readBack.value != payload) {
		ESP_LOGE(DB_TESTER_TAG, "Async upload payload mismatch");
		return;
	}

	(void)db.files().removeFile("async/payload.bin");
	ESP_LOGI(DB_TESTER_TAG, "Async file upload test passed");
}

void DbTester::asyncFileUploadRetentionBoundTest() {
	const size_t uploadCount = 80;
	std::vector<uint32_t> uploadIds;
	uploadIds.reserve(uploadCount);

	ESPJsonDBFileOptions opts;
	opts.overwrite = true;
	opts.chunkSize = 64;

	for (size_t i = 0; i < uploadCount; ++i) {
		struct UploadCtx {
			const uint8_t *data = nullptr;
			size_t size = 0;
			size_t offset = 0;
		} ctx;

		std::vector<uint8_t> payload(32, static_cast<uint8_t>(i & 0xFF));
		ctx.data = payload.data();
		ctx.size = payload.size();
		ctx.offset = 0;

		volatile bool done = false;
		volatile bool doneOk = false;
		volatile size_t doneBytes = 0;

		DbFileUploadPullCb pullCb =
		    [&ctx](size_t requested, uint8_t *buffer, size_t &produced, bool &eof) -> DbStatus {
			if (!buffer)
				return {DbStatusCode::InvalidArgument, "buffer is null"};
			if (ctx.offset >= ctx.size) {
				produced = 0;
				eof = true;
				return {DbStatusCode::Ok, ""};
			}
			size_t remaining = ctx.size - ctx.offset;
			size_t take = remaining < requested ? remaining : requested;
			memcpy(buffer, ctx.data + ctx.offset, take);
			ctx.offset += take;
			produced = take;
			eof = (ctx.offset >= ctx.size);
			return {DbStatusCode::Ok, ""};
		};

		DbFileUploadDoneCb doneCb =
		    [&done, &doneOk, &doneBytes](uint32_t, const DbStatus &st, size_t bytesWritten) {
			    doneOk = st.ok();
			    doneBytes = bytesWritten;
			    done = true;
		    };

		const std::string path = "async/retention_" + std::to_string(i) + ".bin";
		auto asyncRes = db.files().writeFileStreamAsync(path, pullCb, opts, doneCb);
		if (!asyncRes.status.ok()) {
			ESP_LOGE(
			    DB_TESTER_TAG,
			    "Retention test upload start failed at %u: %s",
			    static_cast<unsigned>(i),
			    asyncRes.status.message
			);
			return;
		}
		uploadIds.push_back(asyncRes.value);

		const uint32_t started = millis();
		while (!done && (millis() - started) < 5000) {
			delay(5);
		}
		if (!done || !doneOk || doneBytes != payload.size()) {
			ESP_LOGE(
			    DB_TESTER_TAG,
			    "Retention test upload completion mismatch at %u",
			    static_cast<unsigned>(i)
			);
			return;
		}

		auto latestState = db.files().getUploadState(asyncRes.value);
		if (!latestState.status.ok() || latestState.value != DbFileUploadState::Completed) {
			ESP_LOGE(
			    DB_TESTER_TAG,
			    "Retention test latest upload state mismatch at %u",
			    static_cast<unsigned>(i)
			);
			return;
		}

		(void)db.files().removeFile(path);
	}

	auto oldestState = db.files().getUploadState(uploadIds.front());
	if (oldestState.status.code != DbStatusCode::NotFound) {
		ESP_LOGE(DB_TESTER_TAG, "Retention test expected oldest upload state to expire");
		return;
	}

	auto newestState = db.files().getUploadState(uploadIds.back());
	if (!newestState.status.ok() || newestState.value != DbFileUploadState::Completed) {
		ESP_LOGE(DB_TESTER_TAG, "Retention test expected newest upload state to be retained");
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "Async upload retention bound test passed");
}

void DbTester::asyncFileUploadQueueOrderTest() {
	struct UploadCtx {
		std::vector<uint8_t> payload;
		size_t offset = 0;
	};

	const size_t uploadCount = 3;
	std::vector<uint32_t> uploadIds;
	uploadIds.reserve(uploadCount);
	std::vector<std::string> uploadPaths;
	uploadPaths.reserve(uploadCount);

	std::atomic<size_t> doneCount{0};
	volatile bool doneOk = true;
	uint32_t completionOrder[uploadCount]{0, 0, 0};

	ESPJsonDBFileOptions opts;
	opts.overwrite = true;
	opts.chunkSize = 64;

	std::vector<std::shared_ptr<UploadCtx>> contexts;
	contexts.reserve(uploadCount);

	for (size_t i = 0; i < uploadCount; ++i) {
		auto ctx = std::make_shared<UploadCtx>();
		ctx->payload.resize(64, static_cast<uint8_t>(0x30 + i));
		contexts.push_back(ctx);

		const std::string path = "async/order_" + std::to_string(i) + ".bin";
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
		    [&doneCount, &completionOrder, &doneOk](uint32_t uploadId, const DbStatus &st, size_t) {
			    const size_t idx = doneCount.fetch_add(1);
			    if (idx < uploadCount) {
				    completionOrder[idx] = uploadId;
			    }
			    if (!st.ok()) {
				    doneOk = false;
			    }
		    };

		auto asyncRes = db.files().writeFileStreamAsync(path, pullCb, opts, doneCb);
		if (!asyncRes.status.ok()) {
			ESP_LOGE(
			    DB_TESTER_TAG,
			    "Queue order test upload start failed: %s",
			    asyncRes.status.message
			);
			return;
		}
		uploadIds.push_back(asyncRes.value);
	}

	const uint32_t started = millis();
	while (doneCount.load() < uploadCount && (millis() - started) < 5000) {
		delay(5);
	}
	if (doneCount.load() != uploadCount || !doneOk) {
		ESP_LOGE(DB_TESTER_TAG, "Queue order test uploads did not complete cleanly");
		return;
	}

	for (size_t i = 0; i < uploadCount; ++i) {
		if (completionOrder[i] != uploadIds[i]) {
			ESP_LOGE(DB_TESTER_TAG, "Queue order test completion order mismatch");
			return;
		}
		auto state = db.files().getUploadState(uploadIds[i]);
		if (!state.status.ok() || state.value != DbFileUploadState::Completed) {
			ESP_LOGE(DB_TESTER_TAG, "Queue order test upload state mismatch");
			return;
		}
	}

	for (const auto &path : uploadPaths) {
		(void)db.files().removeFile(path);
	}

	ESP_LOGI(DB_TESTER_TAG, "Async upload queue order test passed");
}
