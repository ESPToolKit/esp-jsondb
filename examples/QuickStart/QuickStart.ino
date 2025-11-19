#include <ESPJsonDB.h>

static DataBase db;

void setup() {
    Serial.begin(115200);

    SyncConfig syncCfg;
    syncCfg.intervalMs = 3000;  // autosync every 3s
    syncCfg.autosync = true;

    if (!db.init("/example_db", syncCfg).ok()) {
        Serial.println("DB init failed");
        return;
    }

    db.onEvent([](DBEventType event){
        Serial.printf("Event: %s\n", dbEventTypeToString(event));
    });

    db.onError([](const DbStatus &status){
        Serial.printf("Error: %s\n", status.message);
    });

    JsonDocument userDoc;
    userDoc["email"] = "espjsondb@gmail.com";
    userDoc["username"] = "esp-jsondb";
    auto createRes = db.create("users", userDoc.as<JsonObjectConst>());
    if (createRes.status.ok()) {
        Serial.printf("Created user %s\n", createRes.value.c_str());
        db.removeById("users", createRes.value);
    }
}

void loop() {
}
