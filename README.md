# esp-jsondb

[![CI](https://github.com/esp-jsondb/esp-jsondb/actions/workflows/ci.yml/badge.svg)](https://github.com/esp-jsondb/esp-jsondb/actions/workflows/ci.yml) [![Release](https://img.shields.io/github/v/release/esp-jsondb/esp-jsondb)](https://github.com/esp-jsondb/esp-jsondb/releases)

esp-jsondb is a lightweight document database for ESP32 devices.  It is inspired by the workflow of MongoDB/Mongoose but tailored for resource‑constrained environments.  Documents are stored as JSON using ArduinoJson and collections are flushed to the filesystem on demand.

## Goals
- Provide a simple, mongoose-like API for embedded projects.
- Keep code exception free and friendly to C++17 embedded toolchains.
- Offer optional autosync between memory and flash storage.

## Limitations
- The database keeps collections in RAM. Large documents are only practical on boards with PSRAM.
- Designed for ESP32 + LittleFS. Other platforms or filesystems have not been tested.

## Dependencies
- [ArduinoStreamUtils](https://github.com/bblanchon/ArduinoStreamUtils) (`ArduinoStreamUtils.h`)
- [ArduinoJson](https://github.com/bblanchon/ArduinoJson) (`ArduinoJson.h`)
- [LittleFS](https://github.com/lorol/LittleFS) (`LittleFS.h`)

## Installation

### Arduino IDE
- Library Manager: Search for "ESPJsonDB" in Library Manager. If not yet indexed, use manual install.
- Manual install: Download this repository as ZIP and import via
  `Sketch → Include Library → Add .ZIP Library…`.
- Include in your sketch:
  ```cpp
  #include <ESPJsonDB.h>
  ```

### PlatformIO (and pioarduino)
Add the libraries to your `platformio.ini` under `lib_deps`. You can reference
this library and StreamUtils directly by Git URL; ArduinoJson can be pulled from the registry.

Minimal example:
```ini
[env:esp32dev]
platform = espressif32
framework = arduino
board = esp32dev

lib_deps =
    ArduinoJson
    https://github.com/bblanchon/ArduinoStreamUtils
    https://github.com/ESPToolKit/esp-jsondb.git
```

Then include the umbrella header in your code:
```cpp
#include <ESPJsonDB.h>
```

Notes:
- If you are using pioarduino, you can add the same Git URLs to `lib_deps`.
- Ensure LittleFS support is available for your ESP32 Arduino core; initialize it before `db.init()`.

After publishing to the PlatformIO Registry, users can install by name:
```ini
lib_deps =
  ESPJsonDB
```

## Examples
Ready-to-run sketches are available in the `examples` directory:

- `QuickStart` - basic database initialization and callbacks
- `Collections` - create and drop collections
- `BulkOperations` - batch inserts, updates, and queries
- `SchemaValidation` - register schemas and validate documents
- `References` - store and populate document references

## Quick start
```cpp
#include <esp_jsondb/db.h>

void setup() {
    Serial.begin(115200);

    SyncConfig syncCfg;
    syncCfg.intervalMs = 3000;  // autosync every 3s
    syncCfg.autosync = true;

    // baseDir is normalized to start with '/' and no trailing '/'
    // e.g. "test_db" becomes "/test_db"
    if (!db.init("/test_db", syncCfg).ok()) {
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

## Working with documents
Create and remove documents inside a collection:
```cpp
JsonDocument userDoc;
userDoc["email"] = "espjsondb@gmail.com";
userDoc["username"] = "esp-jsondb";
auto createRes = db.create("users", userDoc.as<JsonObjectConst>());
if (createRes.status.ok()) {
    Serial.printf("Created user %s\n", createRes.value.c_str());
    // Read with default using DocView::getOr
    auto findRes = db.findById("users", createRes.value);
    if (findRes.status.ok()) {
        const char *ssid = findRes.value.getOr<const char *>("ssid", SSID);
        Serial.printf("SSID: %s\n", ssid);
    }
    db.removeById("users", createRes.value);   // delete it again
}
```

You can also pass a `JsonDocument` directly; it must be an object:
```cpp
JsonDocument userDoc2;
userDoc2["email"] = "second@example.com";
userDoc2["username"] = "second-user";
auto createRes2 = db.create("users", userDoc2); // validated: must be an object
```

Add several documents and delete those matching a predicate:
```cpp
for (int i = 0; i < 10; ++i) {
    JsonDocument userDoc;
    userDoc["email"] = "espjsondb_" + String(i) + "@gmail.com";
    userDoc["role"]  = i % 2 ? "admin" : "user";
    db.create("users", userDoc.as<JsonObjectConst>());
}

auto removed = db.removeMany("users", [](const DocView &doc){
    return doc["role"].as<std::string>() == "admin";
});
Serial.printf("Removed %d admins\n", removed.value);
```

Bulk insert with Collection::createMany:
```cpp
auto colRes = db.collection("users");
if (colRes.status.ok()) {
    JsonDocument batch;
    JsonArray arr = batch.to<JsonArray>();

    JsonObject u1 = arr.add<JsonObject>();
    u1["email"] = "user1@example.com";
    u1["role"] = "user";

    JsonObject u2 = arr.add<JsonObject>();
    u2["email"] = "user2@example.com";
    u2["role"] = "admin";

    auto many = colRes.value->createMany(batch); // validates: must be an array of objects
    // many.value is a vector<string> of created _id values
}
```

Or do it via the database directly:
```cpp
JsonDocument batch2;
JsonArray arr2 = batch2.to<JsonArray>();
JsonObject u3 = arr2.add<JsonObject>();
u3["email"] = "user3@example.com";
u3["role"] = "user";
JsonObject u4 = arr2.add<JsonObject>();
u4["email"] = "user4@example.com";
u4["role"] = "admin";

auto many2 = db.createMany("users", batch2); // returns vector of _id
```

## Bulk updates and searches
Update many documents at once using a patch + filter:
```cpp
JsonDocument patch;
patch["role"] = "admin";
JsonDocument filter;
filter["role"] = "user";
db.updateMany("users", patch, filter);
```

Or perform the mutation directly in code:
```cpp
db.updateMany("users", [](DocView &doc){
    if (doc["role"].as<std::string>() == "user") {
        doc["role"].set("admin");
        return true;    // count this document
    }
    return false;
});

auto found = db.findMany("users", [](const DocView &doc){
    return doc["role"].as<std::string>() == "admin";
});
Serial.printf("Found %d admins\n", found.value.size());
```

Find a single document with findOne:
```cpp
// 1) Using a predicate (lambda)
auto firstAdmin = db.findOne("users", [](const DocView &doc){
    return doc["role"].as<std::string>() == "admin";
});
if (firstAdmin.status.ok()) {
    Serial.printf("First admin: %s\n", firstAdmin.value["email"].as<const char *>());
} else {
    Serial.println("No admin found");
}

// 2) Using a JSON filter (key == value pairs)
JsonDocument userFilter;
userFilter["role"] = "user";
auto firstUser = db.findOne("users", userFilter);
if (firstUser.status.ok()) {
    Serial.printf("First user: %s\n", firstUser.value["email"].as<const char *>());
}
```

Update a single document with updateOne (optionally create if missing):
```cpp
// 1) Using a predicate + mutator
db.updateOne("users",
    [](const DocView &doc){
        return doc["email"].as<std::string>() == "user3@example.com";
    },
    [](DocView &doc){
        doc["role"].set("admin");
    }
);

// 2) Using a JSON filter + JSON patch
JsonDocument patch;
patch["role"] = "admin";
JsonDocument filter;
filter["email"] = "user5@example.com";
// create=true will create the document if none match filter (upsert)
db.updateOne("users", filter, patch, /*create=*/true);
```

## References
Collections can store references to each other. The `populate` helper resolves them:
```cpp
JsonDocument authorDoc;
authorDoc["name"] = "John Doe";
auto authorCreateRes = db.create("authors", authorDoc.as<JsonObjectConst>());

DocRef authorRef{"authors", authorCreateRes.value};
JsonDocument book;
book["title"] = "Example Book";
JsonObject authorRefObj = book["author"].to<JsonObject>();
authorRefObj["collection"] = authorRef.collection;
authorRefObj["_id"] = authorRef.id;
db.create("books", book.as<JsonObjectConst>());

auto bookFindRes = db.findById("books", authorCreateRes.value);
auto populated = bookFindRes.value.populate("author");
Serial.println(populated["name"].as<const char*>());  // "John Doe"
```

## Schemas
Schemas validate incoming documents and enforce field types:
```cpp
static ValidationError usersValidate(const JsonObjectConst &doc) {
    if (doc["username"].isNull() || doc["password"].isNull())
        return {false, "username and password are required"};
    return {true, ""};
}

Schema userSchema;
userSchema.fields = {
    {"email", FieldType::String, "a@b.c"},
    {"username", FieldType::String},
    {"role", FieldType::String, "user"},
    {"password", FieldType::String},
    {"age", FieldType::Int}
};
userSchema.validate = usersValidate;

db.registerSchema("users", userSchema);
```

## Timestamps (createdAt / updatedAt)

Every document automatically records its creation and last update time in
UTC milliseconds (`createdAt` and `updatedAt`).  
These values are generated at runtime using the ESP32 system clock.

⚠️ **Important:** You must ensure that the ESP32 system time is properly set
before creating or updating any documents.  
Call [`configTime(...)`](https://docs.espressif.com/projects/esp-idf/en/latest/esp32/api-reference/system/system_time.html)
and synchronize time (e.g. via NTP) during startup.  

The database itself does not manage or verify time synchronization.

## Diagnostics & Sync
Call `db.getDiag()` to obtain collection/document counts and the active sync config. Use `syncNow()` for manual flushes when autosync is disabled.

## Dirty Tracking and Change Detection
- On edits, documents are re‑serialized to MessagePack. The library compares the new bytes against the in‑memory version before marking a document dirty.
- If bytes are identical: it does not set `dirty`, does not bump `updatedAt`, and no `DocumentUpdated` event is emitted. This avoids unnecessary filesystem writes during sync.
- If bytes differ: it updates the record buffer, sets `dirty = true`, updates `updatedAt`, and the collection becomes dirty for the next sync.
- The comparison is streaming and memory‑efficient; it does not allocate a second buffer for unchanged documents.

Example (no‑op update does nothing):
```cpp
// Create a document
JsonDocument user;
user["email"] = "noop@example.com";
user["role"] = "admin";
auto created = db.create("users", user.as<JsonObjectConst>());

// Capture updatedAt
auto before = db.findById("users", created.value);
uint32_t t0 = before.value.meta().updatedAt;

// Attempt a no-op update (set the same value)
db.updateById("users", created.value, [](DocView &doc){
    doc["role"].set("admin"); // same as before
});

// Re-read and compare updatedAt — unchanged means no write/event was triggered
auto after = db.findById("users", created.value);
uint32_t t1 = after.value.meta().updatedAt;
Serial.printf("No-op avoided write: %s\n", (t0 == t1 ? "true" : "false"));

// Optionally force a sync and observe no DocumentUpdated event is emitted
db.syncNow();
```


## Strengths
- Familiar document/collection model inspired by MongoDB.
- Compact C++17 implementation with no exceptions.
- In-memory caching for speed with optional autosync to flash.
- Schema validation, references and bulk operations out of the box.

## Contributing
Contributions are very welcome! Please read our [Contribution Guide](CONTRIBUTING.md) to get started.

## Acknowledgments
- Huge thanks to [Benoît Blanchon](https://github.com/bblanchon), the creator of
  [ArduinoJson](https://github.com/bblanchon/ArduinoJson) and
  [ArduinoStreamUtils](https://github.com/bblanchon/ArduinoStreamUtils).  
  This project builds directly on top of his excellent work.

## License
This project is released under the MIT License.

See also: `CHANGELOG.md` for version history and notable changes.

## Publishing

- Arduino Library Manager:
  - Ensure `library.properties` is valid and the repo is public with a semver tag (e.g., `v1.0.0`).
  - Submit the repository URL to the Arduino Library Registry following their submission guide: https://github.com/arduino/library-registry
  - After indexing, users install via Library Manager by searching “ESPJsonDB”.

- PlatformIO Registry:
  - Ensure `library.json` exists and push a semver tag (e.g., `v1.0.0`).
  - Publish the library by adding your GitHub repo in the PlatformIO Registry UI: https://platformio.org/lib
  - Once indexed, users can add to `platformio.ini` with `lib_deps = ESPJsonDB`.
