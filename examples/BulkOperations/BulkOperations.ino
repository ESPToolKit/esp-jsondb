#include <ESPJsonDB.h>

void setup() {
    Serial.begin(115200);

    if (!db.init("/bulk_db").ok()) {
        Serial.println("DB init failed");
        return;
    }

    for (int index = 0; index < 10; ++index) {
        JsonDocument userDoc;
        userDoc["email"] = "espjsondb_" + String(index) + "@gmail.com";
        userDoc["role"]  = index % 2 ? "admin" : "user";
        db.create("users", userDoc.as<JsonObjectConst>());
    }

    auto removed = db.removeMany("users", [](const DocView &doc){
        return doc["role"].as<std::string>() == "admin";
    });
    Serial.printf("Removed %d admins\n", removed.value);

    JsonDocument patch;
    patch["role"] = "admin";
    JsonDocument filter;
    filter["role"] = "user";
    db.updateMany("users", patch, filter);

    auto found = db.findMany("users", [](const DocView &doc){
        return doc["role"].as<std::string>() == "admin";
    });
    Serial.printf("Found %d admins\n", found.value.size());

    // Find first matching document using a predicate
    auto firstAdmin = db.findOne("users", [](const DocView &doc){
        return doc["role"].as<std::string>() == "admin";
    });
    if (firstAdmin.status.ok()) {
        Serial.printf("First admin email: %s\n", firstAdmin.value["email"].as<const char *>());
    } else {
        Serial.println("No admin found");
    }

    // Or find using a JSON filter (key == value pairs)
    JsonDocument userFilter;
    userFilter["role"] = "user";
    auto firstUser = db.findOne("users", userFilter);
    if (firstUser.status.ok()) {
        Serial.printf("First user email: %s\n", firstUser.value["email"].as<const char *>());
    }
}

void loop() {
}
