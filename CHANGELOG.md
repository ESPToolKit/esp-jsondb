# Changelog

All notable changes to this project are documented in this file.

The format follows Keep a Changelog and the project adheres to Semantic Versioning.

## [Unreleased]
- Add more examples and ESP-IDF component packaging.
- Improve docs site and troubleshooting guides.

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

[Unreleased]: https://github.com/esp-jsondb/esp-jsondb/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/esp-jsondb/esp-jsondb/releases/tag/v1.0.0

