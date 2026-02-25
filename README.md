# ESPJsonDB

A lightweight document database for ESP32 devices. ESPJsonDB borrows the ergonomics of MongoDB/Mongoose while embracing embedded constraints: collections live as JSON on LittleFS, memory use is capped through an optional cache, and every API leans on ArduinoJson types so you can stay inside a single document representation.

## CI / Release / License
[![CI](https://github.com/ESPToolKit/esp-jsondb/actions/workflows/ci.yml/badge.svg)](https://github.com/ESPToolKit/esp-jsondb/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/ESPToolKit/esp-jsondb?sort=semver)](https://github.com/ESPToolKit/esp-jsondb/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE.md)

## Features
- Simple, mongoose-like API for embedded projects (create/update/remove/find with predicates or JSON filters).
- Optional in-memory cache with dirty-document tracking and change detection to avoid needless flash I/O.
- Automatic LittleFS synchronisation on a background FreeRTOS task (`ESPJsonDBConfig` controls interval, stack, priority, and core affinity).
- MessagePack compression + StreamUtils for efficient read/write pipelines.
- Schema registry with required fields, defaults, type validation, and collection-level unique constraints.
- Event + error callbacks so firmware can observe sync cycles or take action when validation fails.
- Snapshot/restore helpers for backups plus diagnostics that report per-collection counts and config details.
- Generic file storage helpers under `/<baseDir>/_files` with chunked stream read/write for any file type (text, binary, etc.).

## Examples
Quick start:

```cpp
#include <ESPJsonDB.h>

ESPJsonDB db;

void setup() {
    Serial.begin(115200);

    ESPJsonDBConfig cfg;
    cfg.intervalMs = 3000;  // autosync every 3s
    cfg.autosync = true;
    cfg.usePSRAMBuffers = true; // optional: prefer PSRAM for internal byte buffers

    if (!db.init("/test_db", cfg).ok()) {
        Serial.println("DB init failed");
        return;
    }

    db.onEvent([](DBEventType evt){
        Serial.printf("Event: %s\n", dbEventTypeToString(evt));
    });
    db.onError([](const DbStatus &st){
        Serial.printf("Error: %s\n", st.message);
    });
}

void loop() {
    // Call db.deinit() before shutting down the feature/task that owns the DB.
}
```

Working with documents is intentionally `JsonDocument`-centric:

```cpp
JsonDocument doc;
doc["email"] = "user@example.com";
doc["role"] = "admin";
auto createRes = db.create("users", doc.as<JsonObjectConst>());

if (createRes.status.ok()) {
    const std::string& id = createRes.value;
    auto found = db.findById("users", id);
    if (found.status.ok()) {
        Serial.printf("Role: %s\n", found.value["role"].as<const char*>());
    }
    db.updateById("users", id, [](DocView& view){
        view["role"].set("owner");
    });
    db.removeById("users", id);
}
```

See the sketches under `examples/` for end-to-end flows:
- `QuickStart` – database initialisation and simple CRUD.
- `Collections` – create/drop collections at runtime.
- `CacheDisabled` – migration note for the removed cache-disabled mode.
- `BulkOperations` – batch inserts, updates, and queries.
- `SchemaValidation` – enforce required fields and custom validators.
- `UniqueFields` – per-collection uniqueness guarantees.
- `References` – store one-to-many relations and populate them lazily.
- `FileStreaming` – store and stream `txt` / `json` / `csv` / `bin` / custom extension payloads.
- `LargeFileStreaming` – chunked upload + chunked verification for a large binary payload without full-buffer RAM copies.
- `AsyncFileUpload` – non-blocking, callback-driven chunk upload on a background task.
- `AsyncLargeFileUpload` – background chunk upload for a large binary payload with progress polling and streaming hash verification.

File storage example:

```cpp
ESPJsonDBFileOptions fileOpts;
fileOpts.chunkSize = 256;
db.writeTextFile("notes/readme.txt", "hello from esp-jsondb");

db.writeFileFromPath("firmware/chunk.bin", "/fw/chunk.bin", fileOpts);

db.writeFileStream(
    "firmware/chunk_cb.bin",
    [](size_t requested, uint8_t* buffer, size_t& produced, bool& eof) -> DbStatus {
        // fill `buffer` with up to `requested` bytes, set produced/eof
        produced = 0;
        eof = true;
        return {DbStatusCode::Ok, ""};
    },
    fileOpts
);
```

## Gotchas
- Each collection lives in RAM; add PSRAM when handling large documents.
- All payloads are JSON; converting to structs is optional but deserialisation still costs memory—size your `JsonDocument` objects carefully.
- Sync callbacks run on the background task; keep them short to avoid blocking periodic flushes.
- Unique constraints and validators run inside write operations. Long-running validators will increase latency for the calling task.
- `writeFileStream()` and `readFileStream()` hold the filesystem lock while processing the stream; use reasonable chunk sizes and avoid blocking stream sources/sinks.
- `writeFileStreamAsync()` runs producer callbacks on a background task; callbacks must be short and thread-safe.
- `getFileUploadState(uploadId)` retains terminal states for a bounded number of recent uploads; older upload IDs eventually return `NotFound`.
- `/_files` is an internal reserved directory used for file storage and cannot be used as a collection name.
- `getSnapshot()` and `restoreFromSnapshot()` currently cover document collections only; file storage under `/_files` is not included.
- `usePSRAMBuffers` affects ESPJsonDB-owned byte buffers and decoded `DocView` `JsonDocument` pools on ArduinoJson v7. Public return containers like `readFile()` still use the existing API types.

## API Reference
- `DbStatus init(const char* baseDir = "/db", const ESPJsonDBConfig& cfg = {})` – mount LittleFS (`cfg.initFileSystem`), preload collections into RAM cache, and start the sync worker task.
- `void deinit()` – stop background tasks, cancel pending async uploads, and release runtime state. Safe before `init()` and safe to call repeatedly.
- `bool isInitialized() const` – reports whether this instance is initialized and ready for DB operations.
- `void onEvent(std::function<void(DBEventType)>)` / `void onError(std::function<void(const DbStatus&)>)` – receive sync, CRUD, and validation events.
- Collection management: `collection(name)`, `dropCollection(name)`, `dropAll()`, `getAllCollectionName()`.
- Document helpers:
  - Create: `create`, `createMany` (JSON array) plus direct `Collection::create*` variants.
  - Read: `findById`, `findOne`, `findMany` (predicate or JSON filter) returning `DocView` so you can read/write lazily.
  - Update/delete: `updateOne`, `updateById`, `updateMany`, `removeById`, `removeMany` (predicate or JSON filter).
- Schemas: `registerSchema(name, Schema)`, `unRegisterSchema(name)`; `Schema` exposes fields with type/default/unique flags plus optional custom `validate` callables.
- References: store `{ "collection": "authors", "_id": "..." }` inside a document and call `DocView::populate(fieldName)` to expand the reference into an embedded object.
- Sync + diagnostics: `syncNow()`, `getDiag()` (JSON summary), `getSnapshot()` / `restoreFromSnapshot()` for backups.
  - `getDiag()` does not touch the filesystem; it reports cached counters overlaid with currently loaded collection sizes.
- File storage:
  - `writeFileStream(path, in, bytesToWrite, opts)` / `readFileStream(path, out, chunkSize)` for chunked stream transfer.
  - `writeFileStream(path, pullCb, opts)` for synchronous callback-driven chunk production.
  - `writeFileFromPath(path, sourceFsPath, opts)` to copy a source file path into DB-managed file storage.
  - `writeFileStreamAsync(path, pullCb, opts, doneCb)` for non-blocking producer-driven uploads.
  - `cancelFileUpload(uploadId)`, `getFileUploadState(uploadId)` for async job control (terminal states are retained for a bounded recent window).
  - `writeFile(path, data, size)` / `readFile(path)` for direct byte buffers.
  - `writeTextFile(path, text)` / `readTextFile(path)` for UTF-8 or plain text payloads.
  - `fileExists(path)`, `fileSize(path)`, `removeFile(path)` for file lifecycle utilities.
  - File paths are relative to `/<baseDir>/_files` and path traversal segments are rejected.
  - `ESPJsonDBFileOptions`: `overwrite` and `chunkSize` controls for stream writes.
  - `DbFileUploadPullCb`: callback receives `(requested, buffer, produced, eof)` and fills bytes into `buffer`.

`ESPJsonDBConfig` knobs:
- `intervalMs`, `stackSize`, `priority`, `coreId` – background autosync cadence & FreeRTOS tuning.
- `autosync`, `coldSync`, `cacheEnabled` – sync behavior. `cacheEnabled=false` is rejected so writes stay on the sync task; init always preloads collections into cache.
- `fs`, `initFileSystem`, `formatOnFail`, `partitionLabel`, `maxOpenFiles` – file system integration; pass your own `fs::FS` if you mount LittleFS elsewhere.
- `usePSRAMBuffers` – prefer PSRAM for internal msgpack + file stream byte buffers and decoded `DocView` `JsonDocument` pools (ArduinoJson v7), with safe fallback to default heap. Task stacks are always created from internal RAM.

Stack sizes are expressed in bytes.

## Restrictions
- Designed for ESP32 + LittleFS. Other platforms/FSes are untested.
- Large documents are only practical on boards with PSRAM when the cache is enabled.
- Requires ArduinoJson 6+, StreamUtils, and a FreeRTOS-capable environment (Arduino-ESP32 or ESP-IDF with C++17).

## Tests
An integration harness (`test/`) runs CRUD, bulk, schema, reference, and diagnostic scenarios via the `DbTester` class. Build it as a PlatformIO test or ESP-IDF component (include `test/dbTest.cpp` in your project) and run it on hardware to validate changes. Contributions that expand automated coverage are welcome.

## License
MIT — see [LICENSE.md](LICENSE.md).

## ESPToolKit
- Check out other libraries: <https://github.com/orgs/ESPToolKit/repositories>
- Hang out on Discord: <https://discord.gg/WG8sSqAy>
- Support the project: <https://ko-fi.com/esptoolkit>
- Visit the website: <https://www.esptoolkit.hu/>
