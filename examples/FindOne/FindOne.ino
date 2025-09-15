#include <ESPJsonDB.h>

// Demonstrates findOne with predicate and with a JSON filter.

void setup() {
    Serial.begin(115200);

    if (!db.init("/findone_db").ok()) {
        Serial.println("DB init failed");
        return;
    }

    // Create a few docs
    for (int i = 0; i < 4; ++i) {
        JsonDocument d;
        d["name"] = String("device-") + i;
        d["status"] = (i % 2) ? "offline" : "online";
        db.create("devices", d.as<JsonObjectConst>());
    }

    // 1) findOne with predicate
    auto online = db.findOne("devices", [](const DocView &doc) {
        return doc["status"].as<std::string>() == std::string("online");
    });
    if (online.status.ok()) {
        Serial.printf("First online: %s\n", online.value["name"].as<const char*>());
    }

    // 2) findOne with JSON filter
    JsonDocument filter;
    filter["status"] = "offline";
    auto offline = db.findOne("devices", filter);
    if (offline.status.ok()) {
        Serial.printf("First offline: %s\n", offline.value["name"].as<const char*>());
    }
}

void loop() {}

