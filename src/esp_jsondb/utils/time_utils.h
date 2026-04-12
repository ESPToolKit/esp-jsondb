#pragma once

#include <Arduino.h>
#include <ctime>
#include <sys/time.h>

inline uint64_t nowUtcMs() {
	timeval tv{};
	if (gettimeofday(&tv, nullptr) == 0) {
		const uint64_t seconds = tv.tv_sec < 0 ? 0ULL : static_cast<uint64_t>(tv.tv_sec);
		const uint64_t micros = tv.tv_usec < 0 ? 0ULL : static_cast<uint64_t>(tv.tv_usec);
		return seconds * 1000ULL + (micros / 1000ULL);
	}

	time_t seconds = time(nullptr);
	if (seconds < 0)
		seconds = 0;
	return static_cast<uint64_t>(seconds) * 1000ULL;
}
