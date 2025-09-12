#include "objectId.h"

namespace {
static uint32_t readEpochSeconds() {
	time_t now = time(nullptr);
	if (now < 0) now = 0;
	return static_cast<uint32_t>(now);
}

static void writeU32BE(uint8_t *out, uint32_t v) {
	out[0] = static_cast<uint8_t>((v >> 24) & 0xFF);
	out[1] = static_cast<uint8_t>((v >> 16) & 0xFF);
	out[2] = static_cast<uint8_t>((v >> 8) & 0xFF);
	out[3] = static_cast<uint8_t>((v) & 0xFF);
}

static uint8_t hexNibble(char c, bool *ok) {
	if (c >= '0' && c <= '9') return static_cast<uint8_t>(c - '0');
	if (c >= 'a' && c <= 'f') return static_cast<uint8_t>(10 + (c - 'a'));
	if (c >= 'A' && c <= 'F') return static_cast<uint8_t>(10 + (c - 'A'));
	if (ok) *ok = false;
	return 0;
}
} // namespace

ObjectId::ObjectId() {
	// 4 bytes: epoch seconds (big-endian)
	const uint32_t secs = readEpochSeconds();
	writeU32BE(_b.data(), secs);

	// 5 bytes: device/random
	uint8_t dev[5]{};
#ifdef ESP32
	uint64_t mac = ESP.getEfuseMac();
	// Use lower 5 bytes of MAC as device component
	dev[0] = static_cast<uint8_t>((mac >> 0) & 0xFF);
	dev[1] = static_cast<uint8_t>((mac >> 8) & 0xFF);
	dev[2] = static_cast<uint8_t>((mac >> 16) & 0xFF);
	dev[3] = static_cast<uint8_t>((mac >> 24) & 0xFF);
	dev[4] = static_cast<uint8_t>((mac >> 32) & 0xFF);
#else
	// Fallback: pseudo-random bytes
	for (int i = 0; i < 5; ++i)
		dev[i] = static_cast<uint8_t>(random(0, 256));
#endif
	memcpy(_b.data() + 4, dev, 5);

	// 3 bytes: counter (24-bit, big-endian)
	const uint32_t c = nextCounter() & 0xFFFFFFu;
	_b[9] = static_cast<uint8_t>((c >> 16) & 0xFF);
	_b[10] = static_cast<uint8_t>((c >> 8) & 0xFF);
	_b[11] = static_cast<uint8_t>((c) & 0xFF);
}

std::string ObjectId::toHex() const {
	static const char *kHex = "0123456789abcdef";
	std::string out;
	out.resize(24);
	for (size_t i = 0; i < _b.size(); ++i) {
		out[i * 2 + 0] = kHex[(_b[i] >> 4) & 0xF];
		out[i * 2 + 1] = kHex[_b[i] & 0xF];
	}
	return out;
}

ObjectId ObjectId::fromHex(const std::string &hex, bool *ok) {
	if (ok) *ok = true;
	ObjectId out;
	if (hex.size() != 24) {
		if (ok) *ok = false;
		return out;
	}
	for (size_t i = 0; i < 12; ++i) {
		uint8_t hi = hexNibble(hex[i * 2], ok);
		uint8_t lo = hexNibble(hex[i * 2 + 1], ok);
		out._b[i] = static_cast<uint8_t>((hi << 4) | lo);
	}
	return out;
}

uint32_t ObjectId::nextCounter() {
	static uint32_t c = 0;
	// Simple increment; wrap at 24 bits
	c = (c + 1) & 0xFFFFFFu;
	if (c == 0) c = 1; // avoid 0
	return c;
}
