#include <ESPJsonDB.h>
#include <cstring>

ESPJsonDB db;

struct UploadContext {
	const uint8_t *data = nullptr;
	size_t total = 0;
	size_t offset = 0;
};

static UploadContext gUploadCtx;
static uint32_t gUploadId = 0;
static volatile bool gUploadDone = false;
static volatile bool gUploadSuccess = false;
static volatile size_t gUploadBytes = 0;

DbStatus uploadPull(size_t requested, uint8_t *buffer, size_t &produced, bool &eof) {
	if (!buffer)
		return {DbStatusCode::InvalidArgument, "buffer is null"};

	if (gUploadCtx.offset >= gUploadCtx.total) {
		produced = 0;
		eof = true;
		return {DbStatusCode::Ok, ""};
	}

	size_t remaining = gUploadCtx.total - gUploadCtx.offset;
	size_t take = remaining < requested ? remaining : requested;
	memcpy(buffer, gUploadCtx.data + gUploadCtx.offset, take);
	gUploadCtx.offset += take;

	produced = take;
	eof = (gUploadCtx.offset >= gUploadCtx.total);
	return {DbStatusCode::Ok, ""};
}

void onUploadDone(uint32_t uploadId, const DbStatus &st, size_t bytesWritten) {
	Serial.printf(
	    "Upload %lu done: %s (%u bytes)\n",
	    static_cast<unsigned long>(uploadId),
	    st.ok() ? "ok" : st.message,
	    static_cast<unsigned>(bytesWritten)
	);
	gUploadBytes = bytesWritten;
	gUploadSuccess = st.ok();
	gUploadDone = true;
}

void setup() {
	Serial.begin(115200);

	ESPJsonDBConfig cfg;
	cfg.autosync = false;

	auto initStatus = db.init("/async_file_demo", cfg);
	if (!initStatus.ok()) {
		Serial.printf("DB init failed: %s\n", initStatus.message);
		return;
	}

	static uint8_t payload[1500];
	for (size_t i = 0; i < sizeof(payload); ++i) {
		payload[i] = static_cast<uint8_t>(i & 0xFF);
	}

	gUploadCtx.data = payload;
	gUploadCtx.total = sizeof(payload);
	gUploadCtx.offset = 0;

	ESPJsonDBFileOptions opts;
	opts.chunkSize = 128;
	opts.overwrite = true;

	auto start =
	    db.files().writeFileStreamAsync("uploads/telemetry.bin", uploadPull, opts, onUploadDone);
	if (!start.status.ok()) {
		Serial.printf("writeFileStreamAsync failed: %s\n", start.status.message);
		return;
	}

	gUploadId = start.value;
	Serial.printf("Started upload %lu\n", static_cast<unsigned long>(gUploadId));

	// setup() returns immediately; upload continues on background worker.
}

void loop() {
	if (gUploadId == 0) {
		delay(200);
		return;
	}

	if (!gUploadDone) {
		auto state = db.files().getUploadState(gUploadId);
		if (state.status.ok()) {
			Serial.printf("Upload state: %u\n", static_cast<unsigned>(state.value));
		}
		delay(250);
		return;
	}

	// One-time verification after completion.
	static bool verified = false;
	if (!verified) {
		verified = true;

		if (!gUploadSuccess) {
			Serial.println("Upload failed");
			return;
		}

		auto file = db.files().readFile("uploads/telemetry.bin");
		if (!file.status.ok()) {
			Serial.printf("readFile failed: %s\n", file.status.message);
			return;
		}

		Serial.printf(
		    "readFile bytes: %u (done callback bytes: %u)\n",
		    static_cast<unsigned>(file.value.size()),
		    static_cast<unsigned>(gUploadBytes)
		);
	}

	delay(1000);
}
