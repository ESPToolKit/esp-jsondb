#include "dbTest.h"

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

	ESP_LOGI(DB_TESTER_TAG, "File storage test passed");
}
