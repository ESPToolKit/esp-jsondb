#pragma once

#include <ArduinoJson.h>

#include <cstddef>
#include <cstdint>

#include "../document/document.h"
#include "../utils/dbTypes.h"
#include "../utils/jsondb_allocator.h"

struct RecordHeader {
	DocId id;
	uint64_t createdAtMs = 0;
	uint64_t updatedAtMs = 0;
	uint32_t revision = 0;
	uint32_t payloadCrc32 = 0;
	uint16_t flags = 0;
};

class DocCodec {
  public:
	static constexpr const char *kRecordExtension = ".jdb";

	static uint32_t crc32(const uint8_t *data, size_t size);
	static DbStatus encodeRecord(
	    const RecordHeader &header, const JsonDbVector<uint8_t> &payload, JsonDbVector<uint8_t> &out
	);
	static DbStatus decodeRecord(
	    const uint8_t *data,
	    size_t size,
	    RecordHeader &header,
	    JsonDbVector<uint8_t> &payload,
	    bool usePSRAMBuffers
	);
};
