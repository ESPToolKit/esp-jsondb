#include "doc_codec.h"

#include <cstring>

namespace {
constexpr uint8_t kMagic[4] = {'J', 'D', 'B', '2'};
constexpr uint16_t kVersionWithDuplicatedFlags = 1;
constexpr uint16_t kVersionPrefixFlagsOnly = 2;
constexpr uint32_t kHeaderSizeV1 = 24 + 8 + 8 + 4 + 4 + 2;
constexpr uint32_t kHeaderSizeV2 = 24 + 8 + 8 + 4 + 4;
constexpr uint32_t kPrefixSize = 4 + 2 + 2 + 4 + 4;

void appendU16(JsonDbVector<uint8_t> &out, uint16_t value) {
	out.push_back(static_cast<uint8_t>(value & 0xFFu));
	out.push_back(static_cast<uint8_t>((value >> 8) & 0xFFu));
}

void appendU32(JsonDbVector<uint8_t> &out, uint32_t value) {
	out.push_back(static_cast<uint8_t>(value & 0xFFu));
	out.push_back(static_cast<uint8_t>((value >> 8) & 0xFFu));
	out.push_back(static_cast<uint8_t>((value >> 16) & 0xFFu));
	out.push_back(static_cast<uint8_t>((value >> 24) & 0xFFu));
}

void appendU64(JsonDbVector<uint8_t> &out, uint64_t value) {
	for (uint8_t shift = 0; shift < 8; ++shift) {
		out.push_back(static_cast<uint8_t>((value >> (shift * 8U)) & 0xFFu));
	}
}

bool readU16(const uint8_t *data, size_t size, size_t &offset, uint16_t &value) {
	if (offset + 2 > size)
		return false;
	value = static_cast<uint16_t>(data[offset]) |
	        (static_cast<uint16_t>(data[offset + 1]) << 8);
	offset += 2;
	return true;
}

bool readU32(const uint8_t *data, size_t size, size_t &offset, uint32_t &value) {
	if (offset + 4 > size)
		return false;
	value = static_cast<uint32_t>(data[offset]) |
	        (static_cast<uint32_t>(data[offset + 1]) << 8) |
	        (static_cast<uint32_t>(data[offset + 2]) << 16) |
	        (static_cast<uint32_t>(data[offset + 3]) << 24);
	offset += 4;
	return true;
}

bool readU64(const uint8_t *data, size_t size, size_t &offset, uint64_t &value) {
	if (offset + 8 > size)
		return false;
	value = 0;
	for (uint8_t shift = 0; shift < 8; ++shift) {
		value |= static_cast<uint64_t>(data[offset + shift]) << (shift * 8U);
	}
	offset += 8;
	return true;
}
} // namespace

uint32_t DocCodec::crc32(const uint8_t *data, size_t size) {
	uint32_t crc = 0xFFFFFFFFu;
	for (size_t i = 0; i < size; ++i) {
		crc ^= static_cast<uint32_t>(data[i]);
		for (uint8_t bit = 0; bit < 8; ++bit) {
			const uint32_t mask = static_cast<uint32_t>(-(static_cast<int32_t>(crc & 1u)));
			crc = (crc >> 1) ^ (0xEDB88320u & mask);
		}
	}
	return ~crc;
}

DbStatus DocCodec::encodeRecord(
    const RecordHeader &header, const JsonDbVector<uint8_t> &payload, JsonDbVector<uint8_t> &out
) {
	if (!header.id.valid()) {
		return {DbStatusCode::InvalidArgument, "record id is invalid"};
	}

	const uint32_t payloadCrc = crc32(payload.data(), payload.size());
	out.clear();
	out.reserve(kPrefixSize + kHeaderSizeV2 + payload.size() + sizeof(uint32_t));
	out.insert(out.end(), kMagic, kMagic + sizeof(kMagic));
	appendU16(out, kVersionPrefixFlagsOnly);
	appendU16(out, header.flags);
	appendU32(out, kHeaderSizeV2);
	appendU32(out, static_cast<uint32_t>(payload.size()));

	for (size_t idx = 0; idx < DocId::kHexLength; ++idx) {
		out.push_back(static_cast<uint8_t>(header.id.c_str()[idx]));
	}
	appendU64(out, header.createdAtMs);
	appendU64(out, header.updatedAtMs);
	appendU32(out, header.revision);
	appendU32(out, payloadCrc);

	out.insert(out.end(), payload.begin(), payload.end());
	appendU32(out, payloadCrc);
	return {DbStatusCode::Ok, ""};
}

DbStatus DocCodec::decodeRecord(
    const uint8_t *data,
    size_t size,
    RecordHeader &header,
    JsonDbVector<uint8_t> &payload,
    bool usePSRAMBuffers
) {
	payload = JsonDbVector<uint8_t>(JsonDbAllocator<uint8_t>(usePSRAMBuffers));
	if (!data || size < (kPrefixSize + kHeaderSizeV2 + sizeof(uint32_t))) {
		return {DbStatusCode::CorruptionDetected, "record too small"};
	}
	if (std::memcmp(data, kMagic, sizeof(kMagic)) != 0) {
		return {DbStatusCode::CorruptionDetected, "record magic mismatch"};
	}

	size_t offset = sizeof(kMagic);
	uint16_t version = 0;
	uint16_t prefixFlags = 0;
	uint32_t headerSize = 0;
	uint32_t payloadSize = 0;
	if (!readU16(data, size, offset, version) || !readU16(data, size, offset, prefixFlags) ||
	    !readU32(data, size, offset, headerSize) || !readU32(data, size, offset, payloadSize)) {
		return {DbStatusCode::CorruptionDetected, "record header truncated"};
	}
	const bool isV1 = version == kVersionWithDuplicatedFlags && headerSize == kHeaderSizeV1;
	const bool isV2 = version == kVersionPrefixFlagsOnly && headerSize == kHeaderSizeV2;
	if (!isV1 && !isV2) {
		return {DbStatusCode::SchemaMismatch, "unsupported record version"};
	}
	if (size != (kPrefixSize + headerSize + payloadSize + sizeof(uint32_t))) {
		return {DbStatusCode::CorruptionDetected, "record size mismatch"};
	}

	char idBuffer[DocId::kStorageLength];
	if (offset + DocId::kHexLength > size) {
		return {DbStatusCode::CorruptionDetected, "record id truncated"};
	}
	std::memcpy(idBuffer, data + offset, DocId::kHexLength);
	idBuffer[DocId::kHexLength] = '\0';
	offset += DocId::kHexLength;
	if (!header.id.assign(idBuffer)) {
		return {DbStatusCode::CorruptionDetected, "record id invalid"};
	}
	if (!readU64(data, size, offset, header.createdAtMs) ||
	    !readU64(data, size, offset, header.updatedAtMs) ||
	    !readU32(data, size, offset, header.revision) ||
	    !readU32(data, size, offset, header.payloadCrc32)) {
		return {DbStatusCode::CorruptionDetected, "record header payload truncated"};
	}
	if (isV1) {
		uint16_t legacyFlags = 0;
		if (!readU16(data, size, offset, legacyFlags)) {
			return {DbStatusCode::CorruptionDetected, "record header payload truncated"};
		}
	}

	payload.resize(payloadSize);
	if (payloadSize > 0) {
		std::memcpy(payload.data(), data + offset, payloadSize);
	}
	offset += payloadSize;

	uint32_t trailerCrc = 0;
	if (!readU32(data, size, offset, trailerCrc)) {
		return {DbStatusCode::CorruptionDetected, "record crc truncated"};
	}

	const uint32_t actualCrc = crc32(payload.data(), payload.size());
	if (actualCrc != header.payloadCrc32 || actualCrc != trailerCrc) {
		return {DbStatusCode::CorruptionDetected, "record crc mismatch"};
	}
	header.flags = prefixFlags;
	return {DbStatusCode::Ok, ""};
}
