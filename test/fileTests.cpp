#include "dbTest.h"
#include <cstring>

void DbTester::fileStorageTest() {
	const std::string textPath = "docs/sample.txt";
	const std::string textPayload = "ESPJsonDB file storage test";

	auto textWrite = db.writeTextFile(textPath, textPayload, true);
	if (!textWrite.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "writeTextFile failed: %s", textWrite.message);
		return;
	}

	auto exists = db.fileExists(textPath);
	if (!exists.status.ok() || !exists.value) {
		ESP_LOGE(DB_TESTER_TAG, "fileExists failed for text payload");
		return;
	}
	auto size = db.fileSize(textPath);
	if (!size.status.ok() || size.value != textPayload.size()) {
		ESP_LOGE(DB_TESTER_TAG, "fileSize failed for text payload");
		return;
	}

	auto textRead = db.readTextFile(textPath);
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
	auto binWrite = db.writeFile(binPath, binaryPayload.data(), binaryPayload.size(), true);
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
	auto streamWrite = db.writeFileStream("bin/copied.bin", src, src.size(), streamOpts);
	src.close();
	if (!streamWrite.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "writeFileStream failed: %s", streamWrite.message);
		return;
	}

	auto copied = db.readFile("bin/copied.bin");
	if (!copied.status.ok() || copied.value != binaryPayload) {
		ESP_LOGE(DB_TESTER_TAG, "readFile failed or binary mismatch");
		return;
	}

	auto copiedFromPath = db.writeFileFromPath("bin/copied_from_path.bin", "/test_db/_files/bin/source.bin", streamOpts);
	if (!copiedFromPath.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "writeFileFromPath failed: %s", copiedFromPath.message);
		return;
	}

	auto copiedFromPathRead = db.readFile("bin/copied_from_path.bin");
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

	auto pullCb = [&syncCtx](size_t requested, uint8_t *buffer, size_t &produced, bool &eof) -> DbStatus {
		if (!buffer) return {DbStatusCode::InvalidArgument, "buffer is null"};
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

	auto syncCbWrite = db.writeFileStream("bin/copied_from_callback.bin", pullCb, streamOpts);
	if (!syncCbWrite.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "sync callback writeFileStream failed: %s", syncCbWrite.message);
		return;
	}

	auto copiedFromCallback = db.readFile("bin/copied_from_callback.bin");
	if (!copiedFromCallback.status.ok() || copiedFromCallback.value != binaryPayload) {
		ESP_LOGE(DB_TESTER_TAG, "sync callback stream readback mismatch");
		return;
	}

	auto sourceNotFound = db.writeFileFromPath("bin/not_created.bin", "/test_db/_files/bin/missing.bin", streamOpts);
	if (sourceNotFound.ok() || sourceNotFound.code != DbStatusCode::NotFound) {
		ESP_LOGE(DB_TESTER_TAG, "writeFileFromPath missing-source check failed");
		return;
	}

	auto invalidProducer = db.writeFileStream(
		"bin/invalid_callback.bin",
		[](size_t requested, uint8_t *, size_t &produced, bool &eof) -> DbStatus {
			produced = requested + 1;
			eof = false;
			return {DbStatusCode::Ok, ""};
		},
		streamOpts);
	if (invalidProducer.ok() || invalidProducer.code != DbStatusCode::InvalidArgument) {
		ESP_LOGE(DB_TESTER_TAG, "writeFileStream invalid producer check failed");
		return;
	}

	File sink = LittleFS.open("/test_db/stream_sink.txt", FILE_WRITE);
	if (!sink) {
		ESP_LOGE(DB_TESTER_TAG, "Failed to open sink file for readFileStream");
		return;
	}
	auto streamRead = db.readFileStream(textPath, sink, 96);
	sink.close();
	if (!streamRead.status.ok() || streamRead.value != textPayload.size()) {
		ESP_LOGE(DB_TESTER_TAG, "readFileStream failed: %s", streamRead.status.message);
		return;
	}

	auto removeText = db.removeFile(textPath);
	if (!removeText.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "removeFile failed: %s", removeText.message);
		return;
	}
	(void)db.removeFile("bin/source.bin");
	(void)db.removeFile("bin/copied.bin");
	(void)db.removeFile("bin/copied_from_path.bin");
	(void)db.removeFile("bin/copied_from_callback.bin");
	(void)db.removeFile("bin/invalid_callback.bin");

	ESP_LOGI(DB_TESTER_TAG, "File storage test passed");
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

	DbFileUploadPullCb pullCb = [&ctx](size_t requested, uint8_t *buffer, size_t &produced, bool &eof) -> DbStatus {
		if (!buffer) return {DbStatusCode::InvalidArgument, "buffer is null"};
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

	DbFileUploadDoneCb doneCb = [&done, &doneOk, &doneBytes](uint32_t, const DbStatus &st, size_t bytesWritten) {
		doneOk = st.ok();
		doneBytes = bytesWritten;
		done = true;
	};

	ESPJsonDBFileOptions opts;
	opts.overwrite = true;
	opts.chunkSize = 96;
	auto asyncRes = db.writeFileStreamAsync("async/payload.bin", pullCb, opts, doneCb);
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
		(void)db.cancelFileUpload(uploadId);
		return;
	}
	if (!doneOk || doneBytes != payload.size()) {
		ESP_LOGE(DB_TESTER_TAG, "Async upload completion status mismatch");
		return;
	}

	auto stateRes = db.getFileUploadState(uploadId);
	if (!stateRes.status.ok() || stateRes.value != DbFileUploadState::Completed) {
		ESP_LOGE(DB_TESTER_TAG, "Async upload state mismatch");
		return;
	}

	auto readBack = db.readFile("async/payload.bin");
	if (!readBack.status.ok() || readBack.value != payload) {
		ESP_LOGE(DB_TESTER_TAG, "Async upload payload mismatch");
		return;
	}

	(void)db.removeFile("async/payload.bin");
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

		DbFileUploadPullCb pullCb = [&ctx](size_t requested, uint8_t *buffer, size_t &produced, bool &eof) -> DbStatus {
			if (!buffer) return {DbStatusCode::InvalidArgument, "buffer is null"};
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

		DbFileUploadDoneCb doneCb = [&done, &doneOk, &doneBytes](uint32_t, const DbStatus &st, size_t bytesWritten) {
			doneOk = st.ok();
			doneBytes = bytesWritten;
			done = true;
		};

		const std::string path = "async/retention_" + std::to_string(i) + ".bin";
		auto asyncRes = db.writeFileStreamAsync(path, pullCb, opts, doneCb);
		if (!asyncRes.status.ok()) {
			ESP_LOGE(DB_TESTER_TAG, "Retention test upload start failed at %u: %s",
					 static_cast<unsigned>(i),
					 asyncRes.status.message);
			return;
		}
		uploadIds.push_back(asyncRes.value);

		const uint32_t started = millis();
		while (!done && (millis() - started) < 5000) {
			delay(5);
		}
		if (!done || !doneOk || doneBytes != payload.size()) {
			ESP_LOGE(DB_TESTER_TAG, "Retention test upload completion mismatch at %u", static_cast<unsigned>(i));
			return;
		}

		auto latestState = db.getFileUploadState(asyncRes.value);
		if (!latestState.status.ok() || latestState.value != DbFileUploadState::Completed) {
			ESP_LOGE(DB_TESTER_TAG, "Retention test latest upload state mismatch at %u", static_cast<unsigned>(i));
			return;
		}

		(void)db.removeFile(path);
	}

	auto oldestState = db.getFileUploadState(uploadIds.front());
	if (oldestState.status.code != DbStatusCode::NotFound) {
		ESP_LOGE(DB_TESTER_TAG, "Retention test expected oldest upload state to expire");
		return;
	}

	auto newestState = db.getFileUploadState(uploadIds.back());
	if (!newestState.status.ok() || newestState.value != DbFileUploadState::Completed) {
		ESP_LOGE(DB_TESTER_TAG, "Retention test expected newest upload state to be retained");
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "Async upload retention bound test passed");
}
