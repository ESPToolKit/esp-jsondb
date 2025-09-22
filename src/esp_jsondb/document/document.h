#pragma once

#include <Arduino.h>
#include <ArduinoJson.h>

#include <cstdint>
#include <ctime>
#include <memory>
#include <string>
#include <functional>
#include <vector>

#include "../utils/dbTypes.h"
#include "../utils/fr_mutex.h"
#include "../utils/refs.h"
#include "../utils/schema.h"

/**
 * IMPORTANT: The database uses system UTC time for timestamps (ISO 8601) milliseconds.
 * You must call `configTime(...)` and ensure system time is synced (e.g. via NTP)
 * before creating or updating documents.
 * The database does not manage or check time synchronization.
 */
struct DocumentMeta {
    uint32_t createdAt = 0; // UTC milliseconds
    uint32_t updatedAt = 0; // UTC milliseconds
    std::string id;			// 24-hex ObjectId
    bool dirty = false;		// needs flush to FS
    bool removed = false;     // logically deleted; DocView::commit should fail
};

// Internal storage unit (owned by Collection)
struct DocumentRecord {
	DocumentMeta meta;
	std::vector<uint8_t> msgpack; // authoritative source
								  // Optional decoded cache; created on demand and freed when view destroyed
								  // Decoding/encoding uses ArduinoJson.
};

// A short-lived, RAII view for convenient operator[] access
// - On creation: deserialize MessagePack into JsonDocument
// - On commit(): reserialize to MessagePack and mark dirty
class DocView {
  public:
	DocView(std::shared_ptr<DocumentRecord> rec,
			const Schema *schema = nullptr,
			FrMutex *mu = nullptr,
			std::function<DbStatus(const std::shared_ptr<DocumentRecord>&)> commitSink = nullptr);
	~DocView(); // optional auto-commit if enabled

	// non-copyable, movable
	DocView(const DocView &) = delete;
	DocView &operator=(const DocView &) = delete;
	DocView(DocView &&) noexcept = default;
	DocView &operator=(DocView &&) noexcept = default;

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
	template <typename T>
	T getOr(const char *field, T def) const;

	// DocRef helpers
	DocRef getRef(const char *field) const;
	DocView populate(const char *field, uint8_t maxDepth = 4) const;

	// persist changes back to record (MsgPack)
	DbStatus commit(); // serialize -> msgpack; set dirty+updatedAt only if bytes changed
	void discard();	   // drop changes, keep msgpack

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
	std::function<DbStatus(const std::shared_ptr<DocumentRecord>&)> _commitSink;
	DbStatus decode();
	DbStatus encode();
};

template <typename T>
T DocView::getOr(const char *field, T def) const {
	if (!_doc) {
		// Need to const_cast to decode lazily
		auto self = const_cast<DocView *>(this);
		if (!self->decode().ok()) return def;
	}
	JsonVariantConst v = _doc->as<JsonVariantConst>()[field];
	if (v.isNull()) return def;
	if (!v.template is<T>()) return def;
	return v.as<T>();
}
