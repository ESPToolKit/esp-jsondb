#include <ESPJsonDB.h>

ESPJsonDB db;

static constexpr size_t kPayloadSize = 512 * 1024; // 512 KiB
static constexpr size_t kWriteChunkSize = 256;
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

struct PayloadProducer {
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

void setup() {
	Serial.begin(115200);

	ESPJsonDBConfig cfg;
	cfg.autosync = false;

	auto st = db.init("/large_stream_db", cfg);
	if (!st.ok()) {
		Serial.printf("DB init failed: %s\n", st.message);
		return;
	}

	PayloadProducer producer;
	DbFileUploadPullCb pullCb =
	    [&producer](size_t requested, uint8_t *buffer, size_t &produced, bool &eof) -> DbStatus {
		if (!buffer)
			return {DbStatusCode::InvalidArgument, "buffer is null"};

		if (producer.offset >= kPayloadSize) {
			produced = 0;
			eof = true;
			return {DbStatusCode::Ok, ""};
		}

		size_t remaining = kPayloadSize - producer.offset;
		size_t take = remaining < requested ? remaining : requested;
		for (size_t i = 0; i < take; ++i) {
			buffer[i] = payloadByteAt(producer.offset + i);
		}

		producer.hash = fnv1aUpdate(producer.hash, buffer, take);
		producer.offset += take;
		produced = take;
		eof = (producer.offset >= kPayloadSize);

		if ((producer.offset % (64 * 1024)) == 0 || eof) {
			Serial.printf(
			    "write progress: %u / %u bytes\n",
			    static_cast<unsigned>(producer.offset),
			    static_cast<unsigned>(kPayloadSize)
			);
		}

		return {DbStatusCode::Ok, ""};
	};

	ESPJsonDBFileOptions opts;
	opts.overwrite = true;
	opts.chunkSize = kWriteChunkSize;

	const char *path = "firmware/large_payload.bin";
	st = db.files().writeFileStream(path, pullCb, opts);
	if (!st.ok()) {
		Serial.printf("writeFileStream failed: %s\n", st.message);
		return;
	}

	auto sizeRes = db.files().fileSize(path);
	if (!sizeRes.status.ok() || sizeRes.value != kPayloadSize) {
		Serial.printf(
		    "fileSize mismatch: status=%s size=%u\n",
		    sizeRes.status.message,
		    static_cast<unsigned>(sizeRes.value)
		);
		return;
	}

	HashingSink sink;
	auto readRes = db.files().readFileStream(path, sink, kReadChunkSize);
	if (!readRes.status.ok()) {
		Serial.printf("readFileStream failed: %s\n", readRes.status.message);
		return;
	}

	if (readRes.value != kPayloadSize || sink.total != kPayloadSize || sink.hash != producer.hash) {
		Serial.printf(
		    "verification failed (read=%u sink=%u expected=%u)\n",
		    static_cast<unsigned>(readRes.value),
		    static_cast<unsigned>(sink.total),
		    static_cast<unsigned>(kPayloadSize)
		);
		return;
	}

	Serial.printf(
	    "Large chunked stream verified: %u bytes, hash=0x%08lX\n",
	    static_cast<unsigned>(sink.total),
	    static_cast<unsigned long>(sink.hash)
	);
}

void loop() {
}
