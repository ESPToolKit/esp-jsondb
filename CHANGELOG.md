# Changelog

All notable changes to this project are documented in this file.

The format follows Keep a Changelog and the project adheres to Semantic Versioning.

## [Unreleased]
- No changes yet.

## [1.0.4] - 2025-09-25
### Added
- `SyncConfig::cacheEnabled` toggle and the supporting persistence path so the database can run without the RAM cache; includes a `CacheDisabled` example sketch.
- `SyncConfig::coldSync` option to preload collections from the filesystem during `init()` and `changeConfig()` when warm cache is disabled.
- Extended `SyncConfig` with LittleFS controls (`initFileSystem`, `formatOnFail`, `maxOpenFiles`, `partitionLabel`) and the ability to supply an external `fs::FS` handle; added the `ExternalFs` example to demonstrate SPIFFS-backed storage.

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

[Unreleased]: https://github.com/ESPToolKit/esp-jsondb/compare/v1.0.4...HEAD
[1.0.4]: https://github.com/ESPToolKit/esp-jsondb/releases/tag/v1.0.4
[1.0.3]: https://github.com/ESPToolKit/esp-jsondb/releases/tag/v1.0.3
[1.0.2]: https://github.com/ESPToolKit/esp-jsondb/releases/tag/v1.0.2
[1.0.1]: https://github.com/ESPToolKit/esp-jsondb/releases/tag/v1.0.1
