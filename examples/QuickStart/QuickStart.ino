#include <esp_jsondb/db.h>

void setup() {
    Serial.begin(115200);

    SyncConfig cfg;
    cfg.intervalMs = 3000;  // autosync every 3s
    cfg.autosync = true;

    if (!db.init("/example_db", cfg).ok()) {
        Serial.println("DB init failed");
        return;
    }

    db.onEvent([](DBEventType evt){
        Serial.printf("Event: %s\n", dbEventTypeToString(evt));
    });

    db.onError([](const DbStatus &st){
        Serial.printf("Error: %s\n", st.message);
    });

    JsonDocument user;
    user["email"] = "espjsondb@gmail.com";
    user["username"] = "esp-jsondb";
    auto res = db.create("users", user.as<JsonObjectConst>());
    if (res.status.ok()) {
        Serial.printf("Created user %s\n", res.value.c_str());
        db.removeById("users", res.value);
    }
}

void loop() {
}
