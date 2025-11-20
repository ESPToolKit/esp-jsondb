#pragma once

#include <Arduino.h>
#include <ArduinoJson.h>
#include <FS.h>
#include <freertos/FreeRTOS.h>

#include <cstdint>
#include <string>

enum class DbStatusCode : uint8_t {
	Ok = 0,
	NotFound,
	AlreadyExists,
	InvalidArgument,
	ValidationFailed,
	IoError,
	Corrupted,
	Busy,
	Unknown
};

struct SyncConfig {
	uint32_t intervalMs = 2000;
	uint16_t stackSize = static_cast<uint16_t>(4096 * sizeof(StackType_t));
	UBaseType_t priority = 2;
	BaseType_t coreId = tskNO_AFFINITY;
	bool autosync = true;
	bool coldSync = false;
	bool cacheEnabled = true;
	fs::FS *fs = nullptr; // optional external filesystem handle
	bool initFileSystem = true;
	bool formatOnFail = true;
	uint8_t maxOpenFiles = 10;
	const char *partitionLabel = "spiffs";
};

enum class DBEventType : uint8_t {
	Sync = 0,
	CollectionCreated,
	CollectionDropped,
	DocumentCreated,
	DocumentUpdated,
	DocumentDeleted
};

// Human-readable descriptions for DBEventType values.
// Embedded-friendly: constexpr data and inline accessor (no dynamic alloc, no exceptions)
static constexpr const char *kDBEventTypeDescriptions[] = {
	"Sync completed",
	"Collection created",
	"Collection dropped",
	"Document created",
	"Document updated",
	"Document deleted"};

inline const char *dbEventTypeToString(DBEventType ev) {
	const auto idx = static_cast<uint8_t>(ev);
	const auto count = static_cast<uint8_t>(sizeof(kDBEventTypeDescriptions) / sizeof(kDBEventTypeDescriptions[0]));
	return (idx < count) ? kDBEventTypeDescriptions[idx] : "Unknown";
}

// Human-readable descriptions for DbStatusCode values.
// Embedded-friendly: constexpr data and inline accessor (no dynamic alloc, no exceptions)
static constexpr const char *kDbStatusCodeDescriptions[] = {
	"Ok",
	"Not found",
	"Already exists",
	"Invalid argument",
	"Validation failed",
	"I/O error",
	"Corrupted",
	"Busy",
	"Unknown",
};

inline const char *dbStatusCodeToString(DbStatusCode code) {
	const auto idx = static_cast<uint8_t>(code);
	const auto count = static_cast<uint8_t>(sizeof(kDbStatusCodeDescriptions) / sizeof(kDbStatusCodeDescriptions[0]));
	return (idx < count) ? kDbStatusCodeDescriptions[idx] : "Unknown";
}

struct DbStatus {
    DbStatusCode code = DbStatusCode::Ok;
    const char *message = "";
    // Explicit default and convenience constructor to allow brace assignments
    DbStatus() = default;
    DbStatus(DbStatusCode c, const char *msg) : code(c), message(msg) {}
    bool ok() const { return code == DbStatusCode::Ok; }
};

template <typename T>
struct DbResult {
	DbStatus status;
	T value;
};
