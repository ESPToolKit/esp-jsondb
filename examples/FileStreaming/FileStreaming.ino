#include <ESPJsonDB.h>
#include <LittleFS.h>

ESPJsonDB db;

static bool writeSeedSourceFile(const char *path) {
	if (LittleFS.exists(path)) {
		LittleFS.remove(path);
	}

	File out = LittleFS.open(path, FILE_WRITE);
	if (!out)
		return false;

	for (uint16_t i = 0; i < 1024; ++i) {
		uint8_t b = static_cast<uint8_t>(i & 0xFF);
		if (out.write(&b, 1) != 1) {
			out.close();
			return false;
		}
	}

	out.close();
	return true;
}

void setup() {
	Serial.begin(115200);

	ESPJsonDBConfig cfg;
	cfg.autosync = false;

	auto st = db.init("/stream_demo_db", cfg);
	if (!st.ok()) {
		Serial.printf("DB init failed: %s\n", st.message);
		return;
	}

	// 1) Plain text file
	st = db.writeTextFile("docs/readme.txt", "Hello from ESPJsonDB file storage.\nLine #2.");
	if (!st.ok()) {
		Serial.printf("writeTextFile(txt) failed: %s\n", st.message);
		return;
	}

	// 2) JSON file (stored as text payload)
	st = db.writeTextFile("configs/app.json", "{\"mode\":\"demo\",\"intervalMs\":1000}");
	if (!st.ok()) {
		Serial.printf("writeTextFile(json) failed: %s\n", st.message);
		return;
	}

	// 3) CSV file
	st = db.writeTextFile("exports/metrics.csv", "ts,temp,humidity\n1,21.4,48\n2,21.7,47\n");
	if (!st.ok()) {
		Serial.printf("writeTextFile(csv) failed: %s\n", st.message);
		return;
	}

	// 4) Binary file using direct byte write
	const uint8_t binPayload[] = {0x00, 0xA1, 0xFF, 0x10, 0x22, 0x33, 0x44, 0x55};
	st = db.writeFile("firmware/chunk.bin", binPayload, sizeof(binPayload));
	if (!st.ok()) {
		Serial.printf("writeFile(bin) failed: %s\n", st.message);
		return;
	}

	// 5) Custom extension file (any file type works)
	const uint8_t customPayload[] = {'M', 'O', 'D', 'L', 0x01, 0x02, 0x03, 0x04};
	st = db.writeFile("assets/model.dat", customPayload, sizeof(customPayload));
	if (!st.ok()) {
		Serial.printf("writeFile(dat) failed: %s\n", st.message);
		return;
	}

	// 6) Stream write: stream source -> DB managed file
	const char *seedPath = "/seed_source.raw";
	if (!writeSeedSourceFile(seedPath)) {
		Serial.println("Failed to prepare source stream file");
		return;
	}

	ESPJsonDBFileOptions opts;
	opts.overwrite = true;
	opts.chunkSize = 128;
	st = db.writeFileFromPath("streams/raw_capture.raw", seedPath, opts);
	if (!st.ok()) {
		Serial.printf("writeFileFromPath failed: %s\n", st.message);
		return;
	}

	// 7) Stream read: DB managed file -> sink stream
	if (LittleFS.exists("/stream_sink.raw")) {
		LittleFS.remove("/stream_sink.raw");
	}

	File sink = LittleFS.open("/stream_sink.raw", FILE_WRITE);
	if (!sink) {
		Serial.println("Failed to open sink stream file");
		return;
	}

	auto streamOut = db.readFileStream("streams/raw_capture.raw", sink, 96);
	sink.close();
	if (!streamOut.status.ok()) {
		Serial.printf("readFileStream failed: %s\n", streamOut.status.message);
		return;
	}

	// 8) Verify with helper reads
	auto txt = db.readTextFile("docs/readme.txt");
	auto jsn = db.readTextFile("configs/app.json");
	auto csv = db.readTextFile("exports/metrics.csv");
	auto bin = db.readFile("firmware/chunk.bin");
	auto modelSize = db.fileSize("assets/model.dat");

	if (!txt.status.ok() || !jsn.status.ok() || !csv.status.ok() || !bin.status.ok() ||
	    !modelSize.status.ok()) {
		Serial.println("One of the verification reads failed");
		return;
	}

	Serial.println("Stored under /stream_demo_db/_files:");
	Serial.printf("txt bytes: %u\n", static_cast<unsigned>(txt.value.size()));
	Serial.printf("json bytes: %u\n", static_cast<unsigned>(jsn.value.size()));
	Serial.printf("csv bytes: %u\n", static_cast<unsigned>(csv.value.size()));
	Serial.printf("bin bytes: %u\n", static_cast<unsigned>(bin.value.size()));
	Serial.printf("model.dat bytes: %u\n", static_cast<unsigned>(modelSize.value));
	Serial.printf("streamed raw bytes out: %u\n", static_cast<unsigned>(streamOut.value));

	auto exists = db.fileExists("streams/raw_capture.raw");
	Serial.printf(
	    "streams/raw_capture.raw exists: %s\n",
	    (exists.status.ok() && exists.value) ? "yes" : "no"
	);
}

void loop() {
}
