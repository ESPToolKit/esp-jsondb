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
- The current `.jdb` writer uses a prefix-authoritative record envelope and still reads the interim duplicated-`flags` v2 envelope for compatibility.
- Background sync worker for record flush and collection cleanup.
- Per-collection load policy configuration via `configureCollection()`.
- Schema validation with typed defaults and required fields.
- Unique field enforcement backed by in-memory indexes.
- Snapshot / restore for document collections.
- Stream-based snapshot export / import for backup pipelines without a full intermediate JSON string.
- Optional `ESPCompressor` bridge for native compressed snapshot export / restore without adding a hard dependency.
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
- `DbStatus writeSnapshot(Stream& out, SnapshotMode mode = SnapshotMode::OnDiskOnly)`
- `JsonDocument getSnapshot(SnapshotMode mode = SnapshotMode::OnDiskOnly)`
- `DbStatus restoreFromSnapshot(Stream& in)`
- `DbStatus restoreFromSnapshot(const JsonDocument& snapshot)`
- `FileStore& files()`

If `ESPCompressor` is installed and `ESPJsonDBCompressor.h` is included, these bridge APIs are also available:

- `DbStatus writeCompressedSnapshot(ESPCompressor&, CompressionSink&, SnapshotMode mode = SnapshotMode::OnDiskOnly, ProgressCallback = nullptr, const CompressionJobOptions& = {})`
- `DbStatus restoreCompressedSnapshot(ESPCompressor&, CompressionSource&, ProgressCallback = nullptr, const CompressionJobOptions& = {})`

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

## Snapshot Streaming
Use the stream APIs when you want snapshot transport without materializing a full serialized JSON string first.

```cpp
File snapshotFile = LittleFS.open("/backups/snapshot.json", FILE_WRITE);
if (!snapshotFile) {
    return;
}

DbStatus st = db.writeSnapshot(snapshotFile, SnapshotMode::InMemoryConsistent);
snapshotFile.close();
if (!st.ok()) {
    Serial.printf("snapshot export failed: %s\n", st.message);
    return;
}
```

Restore can read the same JSON snapshot back from any Arduino `Stream`:

```cpp
File snapshotFile = LittleFS.open("/backups/snapshot.json", FILE_READ);
if (!snapshotFile) {
    return;
}

DbStatus st = db.restoreFromSnapshot(snapshotFile);
snapshotFile.close();
```

## Optional ESPCompressor Bridge
`ESPJsonDB` stays independent from `ESPCompressor`, but when both libraries are present you can use `ESPJsonDBCompressor.h` for native compressed snapshot flows.

```cpp
#include <ESPJsonDBCompressor.h>

ESPJsonDB db;
ESPCompressor compressor;

void backupNow() {
    if (compressor.init() != CompressionError::Ok) {
        return;
    }

    FileSink sink(LittleFS, "/backups/latest.esc");
    DbStatus st = db.writeCompressedSnapshot(
        compressor,
        sink,
        SnapshotMode::InMemoryConsistent
    );
    if (!st.ok()) {
        Serial.printf("compressed backup failed: %s\n", st.message);
    }
}
```

For restore, stage the compressed payload before destructive replacement when the backup itself is stored under `db.files()`:

```cpp
auto backup = db.files().readFile("backups/latest.esc");
if (!backup.status.ok()) {
    return;
}

BufferSource source(backup.value.data(), backup.value.size());
DbStatus st = db.restoreCompressedSnapshot(compressor, source);
```

This keeps backup payloads as files while letting the app track backup metadata separately in normal collections if needed.
Internally, `restoreCompressedSnapshot()` first decompresses into a temporary snapshot file outside the DB root so `dropAll()` cannot erase the staged restore input.

## Notes
- `SnapshotMode::InMemoryConsistent` triggers `syncNow()` before reading persisted state.
- `writeSnapshot(Stream&)` and `restoreFromSnapshot(Stream&)` preserve the existing snapshot JSON wire shape used by `getSnapshot()` and `restoreFromSnapshot(const JsonDocument&)`.
- `CollectionLoadPolicy::Lazy` loads a collection on first access; `Delayed` defers load to background sync or explicit access.
- `DocView::commit()` is the only write intent; metadata returned by `meta()` is durable record metadata.
- New `.jdb` writes use the current v2 envelope; decode also accepts the earlier unreleased duplicated-`flags` envelope variant.
- `/_files` remains reserved and is not a valid collection name.
- If compressed backups are stored in `db.files()`, read or copy the backup payload before restore because `dropAll()` clears `/_files`.
- v2 is a breaking release and does not read legacy v1 `.mp` files directly.

## Tests
The hardware-oriented test harness under `test/` exercises CRUD, schema validation, delayed loading, snapshots, diagnostics, and file storage. Run it in the same environment used for the library examples.

## License
MIT — see [LICENSE.md](LICENSE.md).

## ESPToolKit
- Website: <https://www.esptoolkit.hu/>
- GitHub: <https://github.com/ESPToolKit>
- Ko-Fi: <https://ko-fi.com/esptoolkit>
