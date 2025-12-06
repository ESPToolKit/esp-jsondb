# ESPJsonDB

A lightweight document database for ESP32 devices. ESPJsonDB borrows the ergonomics of MongoDB/Mongoose while embracing embedded constraints: collections live as JSON on LittleFS, memory use is capped through an optional cache, and every API leans on ArduinoJson types so you can stay inside a single document representation.

## CI / Release / License
[![CI](https://github.com/ESPToolKit/esp-jsondb/actions/workflows/ci.yml/badge.svg)](https://github.com/ESPToolKit/esp-jsondb/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/ESPToolKit/esp-jsondb?sort=semver)](https://github.com/ESPToolKit/esp-jsondb/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE.md)

## Features
- Simple, mongoose-like API for embedded projects (create/update/remove/find with predicates or JSON filters).
- Optional in-memory cache with dirty-document tracking and change detection to avoid needless flash I/O.
- Automatic LittleFS synchronisation on a background FreeRTOS task (`SyncConfig` controls interval, stack, priority, and cache usage).
- MessagePack compression + StreamUtils for efficient read/write pipelines.
- Schema registry with required fields, defaults, type validation, and collection-level unique constraints.
- Event + error callbacks so firmware can observe sync cycles or take action when validation fails.
- Snapshot/restore helpers for backups plus diagnostics that report per-collection counts and config details.

## Examples
Quick start:

```cpp
#include <ESPJsonDB.h>

ESPJsonDB db;

void setup() {
    Serial.begin(115200);

    SyncConfig cfg;
    cfg.intervalMs = 3000;  // autosync every 3s
    cfg.autosync = true;

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
- `CacheDisabled` – run without the RAM cache.
- `BulkOperations` – batch inserts, updates, and queries.
- `SchemaValidation` – enforce required fields and custom validators.
- `UniqueFields` – per-collection uniqueness guarantees.
- `References` – store one-to-many relations and populate them lazily.

## Gotchas
- Each collection lives in RAM when the cache is enabled; disable the cache or add PSRAM when handling large documents.
- All payloads are JSON; converting to structs is optional but deserialisation still costs memory—size your `JsonDocument` objects carefully.
- Sync callbacks run on the background task; keep them short to avoid blocking periodic flushes.
- Unique constraints and validators run inside write operations. Long-running validators will increase latency for the calling task.

## API Reference
- `DbStatus init(const char* baseDir = "/db", const SyncConfig& cfg = {})` – mount LittleFS (`cfg.initFileSystem`), create the autosync task (optional), and prime diagnostics.
- `void onEvent(std::function<void(DBEventType)>)` / `void onError(std::function<void(const DbStatus&)>)` – receive sync, CRUD, and validation events.
- Collection management: `collection(name)`, `dropCollection(name)`, `dropAll()`, `getAllCollectionName()`.
- Document helpers:
  - Create: `create`, `createMany` (JSON array) plus direct `Collection::create*` variants.
  - Read: `findById`, `findOne`, `findMany` (predicate or JSON filter) returning `DocView` so you can read/write lazily.
  - Update/delete: `updateOne`, `updateById`, `updateMany`, `removeById`, `removeMany` (predicate or JSON filter).
- Schemas: `registerSchema(name, Schema)`, `unRegisterSchema(name)`; `Schema` exposes fields with type/default/unique flags plus optional custom `validate` callables.
- References: store `{ "collection": "authors", "_id": "..." }` inside a document and call `DocView::populate(fieldName)` to expand the reference into an embedded object.
- Sync + diagnostics: `syncNow()`, `getDiag()` (JSON summary), `getSnapshot()` / `restoreFromSnapshot()` for backups.

`SyncConfig` knobs:
- `intervalMs`, `stackSize`, `priority`, `coreId` – background autosync cadence & FreeRTOS tuning.
- `autosync`, `coldSync`, `cacheEnabled` – enable/disable timers and caches.
- `fs`, `initFileSystem`, `formatOnFail`, `partitionLabel`, `maxOpenFiles` – file system integration; pass your own `fs::FS` if you mount LittleFS elsewhere.

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
