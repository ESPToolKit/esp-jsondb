# Changelog

All notable changes to this project are documented in this file.

The format follows Keep a Changelog and the project adheres to Semantic Versioning.

## [Unreleased]
### Added
- `ESPJsonDBConfig::usePSRAMBuffers` to prefer PSRAM for ESPJsonDB-owned byte buffers when available (with automatic heap fallback).
- `ESPJsonDBConfig::delayedCollectionSyncArray` to defer selected collection preloads at boot and load them later.
- Background async file upload API:
  - `writeFileStreamAsync(path, pullCb, opts, doneCb)`
  - `cancelFileUpload(uploadId)`
  - `getFileUploadState(uploadId)`
- New `examples/AsyncFileUpload` sketch.
- Synchronous file streaming convenience APIs:
  - `writeFileStream(path, pullCb, opts)` for callback-driven producers.
  - `writeFileFromPath(path, sourceFsPath, opts)` to avoid manual source `File` management.
- New `examples/LargeFileStreaming` sketch showing chunked large-binary streaming with streaming hash verification.
- New `examples/AsyncLargeFileUpload` sketch showing background large-binary upload with progress polling and hash verification.

### Changed
- `init()` now skips collections listed in `delayedCollectionSyncArray` during eager preload; deferred collections are loaded on first periodic autosync tick (or first `syncNow()` when `autosync=false`) and still load immediately on first explicit `collection(name)` access.
- Replaced `ESPWorker` task creation with native FreeRTOS task handling for autosync and async upload workers.
- Task stacks for ESPJsonDB background tasks are now always allocated from internal RAM (never PSRAM/external task stacks).
- Routed internal byte-buffer-heavy paths through `ESPBufferManager` policy:
  - document msgpack storage (`DocumentRecord::msgpack`)
  - sync/async file upload and file streaming chunk buffers
  - snapshot restore msgpack serialization buffers
  - dirty flush pending-write byte snapshots
- Routed decoded `DocView` `JsonDocument` pools through `ESPBufferManager` when building with ArduinoJson v7 allocator support, with automatic fallback to default allocator paths when unavailable.
- `getDiag()` now reports `config.usePSRAMBuffers`.
- `dropAll()`, `changeConfig()`, and `init()` now cancel pending/running async uploads before reconfiguring filesystem state.
- Async upload state retention is now bounded: terminal upload states are kept only for a recent window of upload IDs.
- Updated `examples/FileStreaming` and file storage tests to cover callback/path convenience write flows.

## [1.1.0] - 2026-02-12
### Added
- Generic file storage APIs under the internal `/<baseDir>/_files` tree:
  - `writeFileStream`, `readFileStream` for chunked streaming.
  - `writeFile`, `readFile` for byte buffers.
  - `writeTextFile`, `readTextFile` for text payloads.
  - `fileExists`, `fileSize`, `removeFile` utility helpers.
- New `examples/FileStreaming` sketch covering stream in/out and multiple file types (`txt`, `json`, `csv`, `bin`, `dat`).

### Changed
- Removed eager diagnostics scanning from `init()` to reduce startup latency.
- Switched diagnostics maintenance to incremental updates for create/delete/drop flows so regular write and sync paths avoid repeated full filesystem rescans.
- Made `getDiag()` fully non-filesystem to keep it safe for async/task contexts (for example web server callback tasks); diagnostics now remain in-memory and lightweight.
- Reserved `/_files` as an internal folder and excluded it from collection discovery/snapshot flows.
- `getSnapshot()` / `restoreFromSnapshot()` continue to operate on JSON document collections only (they do not include `/_files` payloads).

## [1.0.5] - 2026-02-09
### Changed
- Renamed the public database configuration type to `ESPJsonDBConfig` for naming consistency across ESPToolKit libraries.
- Updated all public API signatures, examples, tests, and documentation to use `ESPJsonDBConfig`.

## [1.0.4] - 2025-09-25
### Added
- `ESPJsonDBConfig::cacheEnabled` toggle and the supporting persistence path so the database can run without the RAM cache; includes a `CacheDisabled` example sketch.
- `ESPJsonDBConfig::coldSync` option to preload collections from the filesystem during `init()` and `changeConfig()` when warm cache is disabled.
- Extended `ESPJsonDBConfig` with LittleFS controls (`initFileSystem`, `formatOnFail`, `maxOpenFiles`, `partitionLabel`) and the ability to supply an external `fs::FS` handle; added the `ExternalFs` example to demonstrate SPIFFS-backed storage.

### Changed
- Reworked collection locking and commit flow to improve thread-safety and keep cache-less configurations consistent with on-disk state.
- Diagnostics and runtime config reporting now include the new synchronization and filesystem fields.

### Fixed
- Unique field validation when reading from disk now uses the correct field name string, preventing missed conflicts when operating without the cache.

## [1.0.3] - 2025-09-15
### Added
- `SchemaField::unique` flag with enforcement across `create`, `update*`, and bulk write helpers for both cached and disk-backed collections.
- Example sketches for unique constraints (`examples/UniqueFields`) and additional CRUD helpers (`examples/ConfigManagement`, `CreateMany`, `FindOne`, `UpdateOne`).

### Changed
- Collection persistence paths were updated to call the shared unique-field checks so violations raise `DbStatusCode::ValidationFailed`.

## [1.0.2] - 2025-09-15
### Changed
- Updated the release workflow to publish artifacts and notes automatically from version tags.

## [1.0.1] - 2025-09-15
### Added
- Initial public release of ESPJsonDB with a MongoDB-inspired API for collections, documents, and queries.
- Schema registration with per-field validation hooks, default values, and reference population helpers.
- Bulk operations (`createMany`, `updateMany`, `removeMany`, `findMany`) and utility helpers such as `findOne`, `getOr`, and `updateOne`.
- Configurable autosync task to flush collections from RAM to LittleFS, plus manual `syncNow()` support.
- Event callbacks, diagnostics reporting, and automatic `createdAt` / `updatedAt` timestamps on documents.
- Example sketches covering quick start, collections, bulk operations, schema validation, and references.

[Unreleased]: https://github.com/ESPToolKit/esp-jsondb/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/ESPToolKit/esp-jsondb/releases/tag/v1.1.0
[1.0.5]: https://github.com/ESPToolKit/esp-jsondb/releases/tag/v1.0.5
[1.0.4]: https://github.com/ESPToolKit/esp-jsondb/releases/tag/v1.0.4
[1.0.3]: https://github.com/ESPToolKit/esp-jsondb/releases/tag/v1.0.3
[1.0.2]: https://github.com/ESPToolKit/esp-jsondb/releases/tag/v1.0.2
[1.0.1]: https://github.com/ESPToolKit/esp-jsondb/releases/tag/v1.0.1
