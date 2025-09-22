#include <SPIFFS.h>
#include <ESPJsonDB.h>

void setup() {
    Serial.begin(115200);
    while (!Serial && millis() < 2000) {
        delay(10);
    }

    Serial.println();
    Serial.println("[ExternalFs] Mounting SPIFFS...");

    if (!SPIFFS.begin(true)) {
        Serial.println("SPIFFS mount failed. Check partition table.");
        return;
    }

    SyncConfig syncCfg;
    syncCfg.fs = &SPIFFS;        // use the already-mounted SPIFFS instance
    syncCfg.initFileSystem = false; // skip internal LittleFS.begin()
    syncCfg.autosync = false;   // we'll call syncNow() manually for clarity

    auto initStatus = db.init("/external_db", syncCfg);
    if (!initStatus.ok()) {
        Serial.printf("DB init failed: %s\n", initStatus.message);
        return;
    }

    Serial.println("Database ready (SPIFFS backend).");

    JsonDocument doc;
    doc["name"] = "custom-fs";
    doc["timestamp"] = millis();

    auto createRes = db.create("settings", doc.as<JsonObjectConst>());
    if (!createRes.status.ok()) {
        Serial.printf("Create failed: %s\n", createRes.status.message);
        return;
    }

    Serial.printf("Created doc id: %s\n", createRes.value.c_str());

    auto syncStatus = db.syncNow();
    if (!syncStatus.ok()) {
        Serial.printf("syncNow failed: %s\n", syncStatus.message);
        return;
    }

    auto findRes = db.findById("settings", createRes.value);
    if (findRes.status.ok()) {
        Serial.print("Fetched doc: ");
        serializeJsonPretty(findRes.value.as<JsonObjectConst>(), Serial);
        Serial.println();
    } else {
        Serial.printf("Lookup failed: %s\n", findRes.status.message);
    }
}

void loop() {
}
