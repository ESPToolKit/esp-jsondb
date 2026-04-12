#pragma once

#include <Arduino.h>
#include <ArduinoJson.h>

#include <cstdint>
#include <ctime>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "../utils/dbTypes.h"
#include "../utils/doc_id.h"
#include "../utils/fr_mutex.h"
#include "../utils/jsondb_allocator.h"
#include "../utils/refs.h"
#include "../utils/schema.h"

class ESPJsonDB;

#if defined(ARDUINOJSON_VERSION_MAJOR) && (ARDUINOJSON_VERSION_MAJOR >= 7)
#define ESP_JSONDB_HAS_JSONDOC_ALLOCATOR 1
#else
#define ESP_JSONDB_HAS_JSONDOC_ALLOCATOR 0
#endif

#if ESP_JSONDB_HAS_JSONDOC_ALLOCATOR
class JsonDbDocAllocator : public ArduinoJson::Allocator {
  public:
	explicit JsonDbDocAllocator(bool usePSRAMBuffers = false) : _usePSRAMBuffers(usePSRAMBuffers) {
	}

	void *allocate(size_t size) override {
		return jsondb_allocator_detail::allocate(size, _usePSRAMBuffers);
	}

	void deallocate(void *ptr) override {
		jsondb_allocator_detail::deallocate(ptr);
	}

	void *reallocate(void *ptr, size_t new_size) override {
		return jsondb_allocator_detail::reallocate(ptr, new_size, _usePSRAMBuffers);
	}

	void setUsePSRAMBuffers(bool enabled) {
		_usePSRAMBuffers = enabled;
	}

  private:
	bool _usePSRAMBuffers = false;
};
#endif

/**
 * IMPORTANT: The database uses system UTC time for timestamps (ISO 8601) milliseconds.
 * You must call `configTime(...)` and ensure system time is synced (e.g. via NTP)
 * before creating or updating documents.
 * The database does not manage or check time synchronization.
 */
struct DocumentMeta {
	DocId id;                 // 24-hex ObjectId
	uint64_t createdAtMs = 0; // UTC milliseconds
	uint64_t updatedAtMs = 0; // UTC milliseconds
	uint32_t revision = 0;
	uint16_t flags = 0;
	bool dirty = false;   // needs flush to FS
	bool removed = false; // logically deleted; DocView::commit should fail
};

// Internal storage unit (owned by Collection)
struct DocumentRecord {
	explicit DocumentRecord(bool usePSRAMBuffers = false)
	    : msgpack(JsonDbAllocator<uint8_t>(usePSRAMBuffers)) {
	}

	DocumentMeta meta;
	JsonDbVector<uint8_t> msgpack; // authoritative source
	uint32_t pinCount = 0;
	uint64_t lastAccessSeq = 0;
	// Optional decoded cache; created on demand and freed when view
	// destroyed Decoding/encoding uses ArduinoJson.
};

// A short-lived, RAII view for convenient operator[] access
// - On creation: deserialize MessagePack into JsonDocument
// - On commit(): reserialize to MessagePack and mark dirty
class DocView {
  public:
	DocView(
	    std::shared_ptr<DocumentRecord> rec,
	    const Schema *schema = nullptr,
	    FrMutex *mu = nullptr,
	    ESPJsonDB *db = nullptr,
	    std::function<DbStatus(const std::shared_ptr<DocumentRecord> &)> commitSink = nullptr,
	    std::function<DbStatus()> decodeAcquire = nullptr,
	    std::function<void()> decodeRelease = nullptr,
	    std::function<DbStatus()> pinAcquire = nullptr,
	    std::function<void()> pinRelease = nullptr,
	    bool pinHeld = false,
	    bool usePSRAMBuffers = false
	);
	~DocView();

	// non-copyable, movable
	DocView(const DocView &) = delete;
	DocView &operator=(const DocView &) = delete;
	DocView(DocView &&other) noexcept;
	DocView &operator=(DocView &&other) noexcept;

	// Read-only or mutable variant access (ArduinoJson API)
	JsonVariant operator[](const char *key);
	JsonVariant operator[](const String &key);
	JsonVariant operator[](int index);

	JsonVariantConst operator[](const char *key) const;
	JsonVariantConst operator[](const String &key) const;
	JsonVariantConst operator[](int index) const;

	JsonObject asObject();
	JsonObjectConst asObjectConst() const;

	// Convenience: read a field or return a default if absent/invalid
	template <typename T> T getOr(const char *field, T def) const;

	// DocRef helpers
	DocRef getRef(const char *field) const;
	DocView populate(const char *field, uint8_t maxDepth = 4) const;

	// persist changes back to record (MsgPack)
	DbStatus commit(); // serialize -> msgpack; set dirty+updatedAt only if bytes changed
	void discard();    // drop changes, keep msgpack

	const DocumentMeta &meta() const {
		static const DocumentMeta kEmptyMeta{};
		return _rec ? _rec->meta : kEmptyMeta;
	}

  private:
	std::shared_ptr<DocumentRecord> _rec; // shared lifetime with collection
	const Schema *_schema = nullptr;
	std::unique_ptr<JsonDocument> _doc; // decoded pool
	bool _dirtyLocally = false;
	FrMutex *_mu = nullptr; // optional: used when called without external lock
	ESPJsonDB *_db = nullptr;
	std::function<DbStatus(const std::shared_ptr<DocumentRecord> &)> _commitSink;
	std::function<DbStatus()> _decodeAcquire;
	std::function<void()> _decodeRelease;
	std::function<void()> _pinRelease;
	bool _usePSRAMBuffers = false;
	bool _decodeReserved = false;
	bool _pinHeld = false;
#if ESP_JSONDB_HAS_JSONDOC_ALLOCATOR
	JsonDbDocAllocator _docAllocator;
#endif
	DbStatus decode();
	DbStatus encode();
	DbStatus recordStatus(const DbStatus &st) const;
	void releaseResources();
};

template <typename T> T DocView::getOr(const char *field, T def) const {
	if (!_doc) {
		// Need to const_cast to decode lazily
		auto self = const_cast<DocView *>(this);
		if (!self->decode().ok())
			return def;
	}
	JsonVariantConst v = _doc->as<JsonVariantConst>()[field];
	if (v.isNull())
		return def;
	if (!v.template is<T>())
		return def;
	return v.as<T>();
}
