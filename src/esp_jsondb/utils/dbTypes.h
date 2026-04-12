#pragma once

#include <Arduino.h>
#include <ArduinoJson.h>
#include <FS.h>
#include <freertos/FreeRTOS.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <vector>

enum class DbStatusCode : uint8_t {
	Ok = 0,
	NotFound,
	AlreadyExists,
	InvalidArgument,
	ValidationFailed,
	IoError,
	CorruptionDetected,
	Busy,
	NotInitialized,
	Conflict,
	Timeout,
	Unsupported,
	SchemaMismatch,
	Unknown,
	Corrupted = CorruptionDetected
};

enum class SnapshotMode : uint8_t { InMemoryConsistent = 0, OnDiskOnly };

enum class CollectionLoadPolicy : uint8_t { Eager = 0, Lazy, Delayed };

struct CollectionConfig {
	CollectionLoadPolicy loadPolicy = CollectionLoadPolicy::Eager;
	size_t maxDecodedViews = 0;
	size_t maxRecordsInMemory = 0;
};

struct ESPJsonDBConfig {
	uint32_t intervalMs = 2000;
	uint16_t stackSize = static_cast<uint16_t>(4096 * sizeof(StackType_t));
	UBaseType_t priority = 2;
	BaseType_t coreId = tskNO_AFFINITY;
	bool autosync = true;
	fs::FS *fs = nullptr; // optional external filesystem handle
	bool initFileSystem = true;
	bool formatOnFail = true;
	uint8_t maxOpenFiles = 10;
	const char *partitionLabel = "spiffs";
	bool usePSRAMBuffers = false; // Prefer PSRAM for internal byte buffers when available
	CollectionLoadPolicy defaultLoadPolicy = CollectionLoadPolicy::Eager;
};

struct ESPJsonDBFileOptions {
	bool overwrite = true;
	size_t chunkSize = 512;
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
    "Document deleted"
};

inline const char *dbEventTypeToString(DBEventType ev) {
	const auto idx = static_cast<uint8_t>(ev);
	const auto count = static_cast<uint8_t>(
	    sizeof(kDBEventTypeDescriptions) / sizeof(kDBEventTypeDescriptions[0])
	);
	return (idx < count) ? kDBEventTypeDescriptions[idx] : "Unknown";
}

enum class DBSyncStage : uint8_t {
	Idle = 0,
	ColdSyncStarted,
	ColdSyncCollectionStarted,
	ColdSyncCollectionCompleted,
	ColdSyncCompleted,
	SyncNowStarted,
	SyncNowCompleted,
	SyncFailed
};

enum class DBSyncSource : uint8_t { Init = 0, SyncNow };

// Human-readable descriptions for DBSyncStage values.
static constexpr const char *kDBSyncStageDescriptions[] = {
    "Idle",
    "Cold sync started",
    "Cold sync collection started",
    "Cold sync collection completed",
    "Cold sync completed",
    "syncNow started",
    "syncNow completed",
    "Sync failed",
};

inline const char *dbSyncStageToString(DBSyncStage stage) {
	const auto idx = static_cast<uint8_t>(stage);
	const auto count = static_cast<uint8_t>(
	    sizeof(kDBSyncStageDescriptions) / sizeof(kDBSyncStageDescriptions[0])
	);
	return (idx < count) ? kDBSyncStageDescriptions[idx] : "Unknown";
}

// Human-readable descriptions for DBSyncSource values.
static constexpr const char *kDBSyncSourceDescriptions[] = {"Init", "syncNow"};

inline const char *dbSyncSourceToString(DBSyncSource source) {
	const auto idx = static_cast<uint8_t>(source);
	const auto count = static_cast<uint8_t>(
	    sizeof(kDBSyncSourceDescriptions) / sizeof(kDBSyncSourceDescriptions[0])
	);
	return (idx < count) ? kDBSyncSourceDescriptions[idx] : "Unknown";
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
    "Corruption detected",
    "Busy",
    "Not initialized",
    "Conflict",
    "Timeout",
    "Unsupported",
    "Schema mismatch",
    "Unknown",
};

inline const char *dbStatusCodeToString(DbStatusCode code) {
	const auto idx = static_cast<uint8_t>(code);
	const auto count = static_cast<uint8_t>(
	    sizeof(kDbStatusCodeDescriptions) / sizeof(kDbStatusCodeDescriptions[0])
	);
	return (idx < count) ? kDbStatusCodeDescriptions[idx] : "Unknown";
}

struct DbStatus {
	DbStatusCode code = DbStatusCode::Ok;
	const char *message = "";
	// Explicit default and convenience constructor to allow brace assignments
	DbStatus() = default;
	DbStatus(DbStatusCode c, const char *msg) : code(c), message(msg) {
	}
	bool ok() const {
		return code == DbStatusCode::Ok;
	}
};

struct DBSyncStatus {
	DBSyncStage stage = DBSyncStage::Idle;
	DBSyncSource source = DBSyncSource::Init;
	std::string collectionName;
	uint32_t collectionsCompleted = 0;
	uint32_t collectionsTotal = 0;
	DbStatus result{DbStatusCode::Ok, ""};
};

enum class DbFileUploadState : uint8_t { Queued = 0, Running, Completed, Failed, Cancelled };

using DbFileUploadPullCb =
    std::function<DbStatus(size_t requested, uint8_t *buffer, size_t &produced, bool &eof)>;

using DbFileUploadDoneCb =
    std::function<void(uint32_t uploadId, const DbStatus &status, size_t bytesWritten)>;

template <typename T> struct DbResult {
	DbStatus status;
	T value;
};
