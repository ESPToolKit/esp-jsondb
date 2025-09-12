#pragma once

#include <Arduino.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <esp_system.h>
#include <string>

/*
	ObjectId-style IDs (12-byte → 24-hex)
	Layout: 4 bytes seconds since epoch, 5 bytes device/random, 3 bytes counter.
	Nice compromise between uniqueness and cost.
*/
class ObjectId {
  public:
	ObjectId(); // fills from rtc/time + chip data + counter
	std::string toHex() const;
	static ObjectId fromHex(const std::string &hex, bool *ok);

  private:
	std::array<uint8_t, 12> _b{};
	static uint32_t nextCounter();
};
