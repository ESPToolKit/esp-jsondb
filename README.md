# ESPJsonDB

ESPJsonDB is an embedded document database for ESP32 boards. Version 2 stores document payloads as MessagePack inside durable `.jdb` records, keeps document metadata on disk, and separates document storage from generic file storage.

## CI / Release / License
[![CI](https://github.com/ESPToolKit/esp-jsondb/actions/workflows/ci.yml/badge.svg)](https://github.com/ESPToolKit/esp-jsondb/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/ESPToolKit/esp-jsondb?sort=semver)](https://github.com/ESPToolKit/esp-jsondb/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE.md)

## What Changed In v2
- Documents are persisted as `.jdb` records with durable `_id`, `createdAtMs`, `updatedAtMs`, `revision`, and `flags`.
- Collection loading is policy-driven with `Eager`, `Lazy`, and `Delayed` modes.
- Snapshots are explicit: `SnapshotMode::OnDiskOnly` or `SnapshotMode::InMemoryConsistent`.
- Schema fields support first-class `required`, `unique`, and typed defaults.
- Generic file storage is accessed through `db.files()`.
- The old `cacheEnabled`, `coldSync`, and `delayedCollectionSyncArray` config knobs are gone.

## Features
- MessagePack payload storage with lazy `DocView` decoding.
- Durable per-document metadata and revision counters.
- Background sync worker for record flush and collection cleanup.
- Per-collection load policy configuration via `configureCollection()`.
- Schema validation with typed defaults and required fields.
- Unique field enforcement backed by in-memory indexes.
- Snapshot / restore for document collections.
- Async file uploads and chunked file I/O through `FileStore`.
- PSRAM-aware internal allocators for payload and buffer-heavy paths.

## Quick Start
```cpp
#include <ESPJsonDB.h>

ESPJsonDB db;

void setup() {
    Serial.begin(115200);

    ESPJsonDBConfig cfg;
    cfg.intervalMs = 2000;
    cfg.autosync = true;
    cfg.defaultLoadPolicy = CollectionLoadPolicy::Eager;

    db.configureCollection("audit", CollectionConfig{CollectionLoadPolicy::Delayed, 0, 0});

    if (!db.init("/jsondb_v2", cfg).ok()) {
        Serial.println("DB init failed");
        return;
    }

    Schema users;
    users.fields = {
        SchemaField{"email", FieldType::String, std::string("a@b.c")},
        SchemaField{"username", FieldType::String},
        SchemaField{"role", FieldType::String, std::string("user")},
        SchemaField{"password", FieldType::String},
        SchemaField{"age", FieldType::Int32},
    };
    users.fields[1].required = true;
    users.fields[3].required = true;
    db.registerSchema("users", users);

    JsonDocument doc;
    doc["username"] = "esp-jsondb";
    doc["password"] = "secret";
    auto created = db.create("users", doc.as<JsonObjectConst>());
    if (!created.status.ok()) {
        Serial.printf("Create failed: %s\n", created.status.message);
        return;
    }

    auto found = db.findById("users", created.value);
    if (found.status.ok()) {
        Serial.printf(
            "revision=%lu createdAtMs=%llu\n",
            static_cast<unsigned long>(found.value.meta().revision),
            static_cast<unsigned long long>(found.value.meta().createdAtMs)
        );
    }

    auto snap = db.getSnapshot(SnapshotMode::InMemoryConsistent);
    serializeJsonPretty(snap, Serial);
}

void loop() {
}
```

## File Storage
Document records and arbitrary file blobs are separate subsystems.

```cpp
ESPJsonDBFileOptions opts;
opts.chunkSize = 256;

db.files().writeTextFile("notes/readme.txt", "hello");

auto uploadId = db.files().writeFileStreamAsync(
    "firmware/chunk.bin",
    [](size_t requested, uint8_t* buffer, size_t& produced, bool& eof) -> DbStatus {
        produced = 0;
        eof = true;
        return {DbStatusCode::Ok, ""};
    }
);

auto info = db.files().getFileInfo("notes/readme.txt");
```

## Core API
- `DbStatus init(const char* baseDir = "/db", const ESPJsonDBConfig& cfg = {})`
- `DbStatus configureCollection(const std::string& name, const CollectionConfig& cfg)`
- `DbResult<Collection*> collection(name)`
- `DbStatus registerSchema(name, schema)`
- `DbStatus unregisterSchema(name)`
- `JsonDocument getDiagnostics()`
- `JsonDocument getSnapshot(SnapshotMode mode = SnapshotMode::OnDiskOnly)`
- `DbStatus restoreFromSnapshot(const JsonDocument& snapshot)`
- `FileStore& files()`

## Snapshot Format
Snapshots are document-only and exclude `/_files`.

```json
{
  "collections": {
    "users": [
      {
        "_id": "0123456789abcdef01234567",
        "_meta": {
          "createdAtMs": 1743100000000,
          "updatedAtMs": 1743100005000,
          "revision": 2,
          "flags": 0
        },
        "username": "esp-jsondb"
      }
    ]
  }
}
```

## Notes
- `SnapshotMode::InMemoryConsistent` triggers `syncNow()` before reading persisted state.
- `CollectionLoadPolicy::Lazy` loads a collection on first access; `Delayed` defers load to background sync or explicit access.
- `DocView::commit()` is the only write intent; metadata returned by `meta()` is durable record metadata.
- `/_files` remains reserved and is not a valid collection name.
- v2 is a breaking release and does not read legacy v1 `.mp` files directly.

## Tests
The hardware-oriented test harness under `test/` exercises CRUD, schema validation, delayed loading, snapshots, diagnostics, and file storage. Run it in the same environment used for the library examples.

## License
MIT — see [LICENSE.md](LICENSE.md).

## ESPToolKit
- Website: <https://www.esptoolkit.hu/>
- GitHub: <https://github.com/ESPToolKit>
- Ko-Fi: <https://ko-fi.com/esptoolkit>
