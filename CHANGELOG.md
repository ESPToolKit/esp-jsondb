# Changelog

All notable changes to this project are documented in this file.

The format follows Keep a Changelog and the project adheres to Semantic Versioning.

## [Unreleased]
- Add more examples and ESP-IDF component packaging.
- Improve docs site and troubleshooting guides.
### Added
- SchemaField: `bool unique` flag (default `false`) to enforce per-collection uniqueness for scalar fields.
- New example: `examples/UniqueFields` demonstrating unique constraints on create and update.
- `SyncConfig::cacheEnabled` flag to disable the in-memory document cache for low-memory deployments.
### Changed
- Enforce unique constraints during `create`, `updateOne` (both overloads), `updateById`, and `updateMany`.
  Violations return `DbStatusCode::ValidationFailed` with message `"unique constraint violated"`.
  Unique checks skip arrays/objects and exclude the current document on updates.

## [1.0.1] - 2025-09-15
### Fixed
- Ensure C++17 is used in CI for PlatformIO to support `enable_if_t`, `is_same_v`, and `make_unique`.
- Add `DbStatus` convenience constructor to allow brace-style initialization/assignment.
- Add Arduino `String`/`const char*` overloads for `DataBase::collection()` to match example usage.
- Update examples to include `<ESPJsonDB.h>` for reliable Arduino CLI discovery.
- Adjust Arduino CLI workflow to stage the library into the sketchbook and fix `StreamUtils` name.

## [1.0.0] - 2025-09-15
### Added
- Initial public release of ESPJsonDB, a lightweight JSON document database for ESP32.
- MongoDB/Mongoose-like API for collections and documents.
- Schema validation with custom validators and default values.
- Document references with `populate` helper.
- Bulk operations: `createMany`, `updateMany`, `removeMany`, `findMany`.
- Autosync configuration and `syncNow()` for manual flushes to LittleFS.
- Automatic `createdAt` and `updatedAt` timestamps.
- Dirty tracking to avoid no-op writes and reduce flash wear.
- Examples: QuickStart, Collections, BulkOperations, SchemaValidation, References.
- Arduino library metadata (`library.properties`) and PlatformIO metadata (`library.json`).
- CI workflow to build examples across multiple ESP32 boards.

[Unreleased]: https://github.com/ESPToolKit/esp-jsondb/compare/v1.0.1...HEAD
[1.0.1]: https://github.com/ESPToolKit/esp-jsondb/releases/tag/v1.0.1
[1.0.0]: https://github.com/ESPToolKit/esp-jsondb/releases/tag/v1.0.0
