#include <ESPJsonDB.h>

ESPJsonDB db;

static constexpr size_t kPayloadSize = 768 * 1024; // 768 KiB
static constexpr size_t kChunkSize = 256;
static constexpr size_t kReadChunkSize = 384;
static constexpr uint32_t kFnvOffset = 2166136261UL;
static constexpr uint32_t kFnvPrime = 16777619UL;

static uint32_t fnv1aUpdate(uint32_t hash, const uint8_t *data, size_t size) {
	for (size_t i = 0; i < size; ++i) {
		hash ^= data[i];
		hash *= kFnvPrime;
	}
	return hash;
}

static uint8_t payloadByteAt(size_t index) {
	return static_cast<uint8_t>((index * 37UL + 11UL) & 0xFFU);
}

struct UploadContext {
	size_t offset = 0;
	uint32_t hash = kFnvOffset;
};

class HashingSink : public Stream {
  public:
	size_t total = 0;
	uint32_t hash = kFnvOffset;

	int available() override {
		return 0;
	}
	int read() override {
		return -1;
	}
	int peek() override {
		return -1;
	}
	void flush() override {
	}

	size_t write(uint8_t b) override {
		hash = fnv1aUpdate(hash, &b, 1);
		total += 1;
		return 1;
	}

	size_t write(const uint8_t *buffer, size_t size) override {
		if (buffer == nullptr && size > 0)
			return 0;
		hash = fnv1aUpdate(hash, buffer, size);
		total += size;
		return size;
	}
};

static UploadContext gUploadCtx;
static uint32_t gUploadId = 0;
static volatile bool gUploadDone = false;
static volatile bool gUploadOk = false;
static volatile size_t gUploadBytes = 0;
static volatile size_t gUploadedProgress = 0;
static volatile uint32_t gExpectedHash = kFnvOffset;

DbStatus largeUploadPull(size_t requested, uint8_t *buffer, size_t &produced, bool &eof) {
	if (!buffer)
		return {DbStatusCode::InvalidArgument, "buffer is null"};

	if (gUploadCtx.offset >= kPayloadSize) {
		produced = 0;
		eof = true;
		return {DbStatusCode::Ok, ""};
	}

	size_t remaining = kPayloadSize - gUploadCtx.offset;
	size_t take = remaining < requested ? remaining : requested;
	for (size_t i = 0; i < take; ++i) {
		buffer[i] = payloadByteAt(gUploadCtx.offset + i);
	}

	gUploadCtx.hash = fnv1aUpdate(gUploadCtx.hash, buffer, take);
	gUploadCtx.offset += take;
	gUploadedProgress = gUploadCtx.offset;
	gExpectedHash = gUploadCtx.hash;

	produced = take;
	eof = (gUploadCtx.offset >= kPayloadSize);
	return {DbStatusCode::Ok, ""};
}

void onLargeUploadDone(uint32_t uploadId, const DbStatus &st, size_t bytesWritten) {
	Serial.printf(
	    "Upload %lu done: %s (%u bytes)\n",
	    static_cast<unsigned long>(uploadId),
	    st.ok() ? "ok" : st.message,
	    static_cast<unsigned>(bytesWritten)
	);
	gUploadBytes = bytesWritten;
	gUploadOk = st.ok();
	gUploadDone = true;
}

void setup() {
	Serial.begin(115200);

	ESPJsonDBConfig cfg;
	cfg.autosync = false;

	auto initStatus = db.init("/async_large_demo", cfg);
	if (!initStatus.ok()) {
		Serial.printf("DB init failed: %s\n", initStatus.message);
		return;
	}

	gUploadCtx.offset = 0;
	gUploadCtx.hash = kFnvOffset;
	gUploadDone = false;
	gUploadOk = false;
	gUploadBytes = 0;
	gUploadedProgress = 0;
	gExpectedHash = kFnvOffset;

	ESPJsonDBFileOptions opts;
	opts.chunkSize = kChunkSize;
	opts.overwrite = true;

	auto start = db.writeFileStreamAsync(
	    "uploads/large_payload.bin",
	    largeUploadPull,
	    opts,
	    onLargeUploadDone
	);
	if (!start.status.ok()) {
		Serial.printf("writeFileStreamAsync failed: %s\n", start.status.message);
		return;
	}

	gUploadId = start.value;
	Serial.printf(
	    "Started async upload %lu for %u bytes\n",
	    static_cast<unsigned long>(gUploadId),
	    static_cast<unsigned>(kPayloadSize)
	);
}

void loop() {
	if (gUploadId == 0) {
		delay(250);
		return;
	}

	if (!gUploadDone) {
		auto state = db.getFileUploadState(gUploadId);
		if (state.status.ok()) {
			Serial.printf(
			    "state=%u progress=%u/%u\n",
			    static_cast<unsigned>(state.value),
			    static_cast<unsigned>(gUploadedProgress),
			    static_cast<unsigned>(kPayloadSize)
			);
		}
		delay(500);
		return;
	}

	static bool verified = false;
	if (!verified) {
		verified = true;

		if (!gUploadOk || gUploadBytes != kPayloadSize) {
			Serial.println("Upload failed or byte count mismatch");
			return;
		}

		auto sizeRes = db.fileSize("uploads/large_payload.bin");
		if (!sizeRes.status.ok() || sizeRes.value != kPayloadSize) {
			Serial.printf(
			    "fileSize mismatch: %s size=%u\n",
			    sizeRes.status.message,
			    static_cast<unsigned>(sizeRes.value)
			);
			return;
		}

		HashingSink sink;
		auto readRes = db.readFileStream("uploads/large_payload.bin", sink, kReadChunkSize);
		if (!readRes.status.ok()) {
			Serial.printf("readFileStream failed: %s\n", readRes.status.message);
			return;
		}

		if (readRes.value != kPayloadSize || sink.total != kPayloadSize ||
		    sink.hash != gExpectedHash) {
			Serial.printf(
			    "verify failed read=%u sink=%u expected=%u hash=0x%08lX/0x%08lX\n",
			    static_cast<unsigned>(readRes.value),
			    static_cast<unsigned>(sink.total),
			    static_cast<unsigned>(kPayloadSize),
			    static_cast<unsigned long>(sink.hash),
			    static_cast<unsigned long>(gExpectedHash)
			);
			return;
		}

		Serial.printf(
		    "Async large file verified: %u bytes, hash=0x%08lX\n",
		    static_cast<unsigned>(sink.total),
		    static_cast<unsigned long>(sink.hash)
		);
	}

	delay(1000);
}
