#include <esp_jsondb/db.h>

void setup() {
    Serial.begin(115200);

    if (!db.init("/bulk_db").ok()) {
        Serial.println("DB init failed");
        return;
    }

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

    JsonDocument patch;
    patch["role"] = "admin";
    JsonDocument filter;
    filter["role"] = "user";
    db.updateMany("users", patch, filter);

    auto found = db.findMany("users", [](const DocView &d){
        return d["role"].as<std::string>() == "admin";
    });
    Serial.printf("Found %d admins\n", found.value.size());
}

void loop() {
}
