#pragma once

#include <Arduino.h>
#include <ctime>

inline uint32_t nowUtcMs() {
	time_t s = time(nullptr);
	if (s < 0) s = 0;
	uint64_t ms = static_cast<uint64_t>(s) * 1000ULL;
	return static_cast<uint32_t>(ms);
}
