#include <ESPJsonDB.h>

/**
 * Demonstrates running esp-jsondb with the in-memory cache disabled.
 *
 * In this mode collections are loaded lazily from LittleFS and each commit
 * writes straight to flash, which can reduce RAM usage at the cost of IO.
 */
void setup() {
    Serial.begin(115200);

    SyncConfig cfg;
    cfg.autosync = false;      // not needed because writes go straight to disk
    cfg.cacheEnabled = false;  // disable RAM cache

    if (!db.init("/nocache_db", cfg).ok()) {
        Serial.println("DB init failed");
        return;
    }

    auto events = db.collection("events");
    if (!events.status.ok()) {
        Serial.printf("Failed to get events collection: %s\n", events.status.message);
        return;
    }

    JsonDocument eventDoc;
    eventDoc["type"] = "boot";
    eventDoc["ts"] = millis();
    auto created = db.create("events", eventDoc.as<JsonObjectConst>());
    if (!created.status.ok()) {
        Serial.printf("Create failed: %s\n", created.status.message);
        return;
    }
    Serial.printf("Created event %s (written directly to flash)\n", created.value.c_str());

    // Because the cache is disabled, the following lookup reloads from LittleFS.
    auto fetched = db.findById("events", created.value);
    if (fetched.status.ok()) {
        const char *kind = fetched.value["type"].as<const char*>();
        Serial.printf("Reloaded event type: %s\n", kind ? kind : "(null)");
    } else {
        Serial.printf("Reload failed: %s\n", fetched.status.message);
    }
}

void loop() {}
