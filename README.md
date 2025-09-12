# esp-jsondb

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

## Working with documents
Create and remove documents inside a collection:
```cpp
JsonDocument user;
user["email"] = "espjsondb@gmail.com";
user["username"] = "esp-jsondb";
auto res = db.create("users", user.as<JsonObjectConst>());
if (res.status.ok()) {
    Serial.printf("Created user %s\n", res.value.c_str());
    db.removeById("users", res.value);   // delete it again
}
```

Add several documents and delete those matching a predicate:
```cpp
for (int i = 0; i < 10; ++i) {
    JsonDocument u;
    u["email"] = "espjsondb_" + String(i) + "@gmail.com";
    u["role"]  = i % 2 ? "admin" : "user";
    db.create("users", u.as<JsonObjectConst>());
}

auto removed = db.removeMany("users", [](const DocView &d){
    return d["role"].as<std::string>() == "admin";
});
Serial.printf("Removed %d admins\n", removed.value);
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
db.updateMany("users", [](DocView &d){
    if (d["role"].as<std::string>() == "user") {
        d["role"].set("admin");
        return true;    // count this document
    }
    return false;
});

auto found = db.findMany("users", [](const DocView &d){
    return d["role"].as<std::string>() == "admin";
});
Serial.printf("Found %d admins\n", found.value.size());
```

## References
Collections can store references to each other. The `populate` helper resolves them:
```cpp
JsonDocument author;
author["name"] = "John Doe";
auto a = db.create("authors", author.as<JsonObjectConst>());

DocRef authRef{"authors", a.value};
JsonDocument book;
book["title"] = "Example Book";
JsonObject ref = book["author"].to<JsonObject>();
ref["collection"] = authRef.collection;
ref["_id"] = authRef.id;
db.create("books", book.as<JsonObjectConst>());

auto fr = db.findById("books", a.value);
auto populated = fr.value.populate("author");
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
