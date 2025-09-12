#include <esp_jsondb/db.h>

void setup() {
    Serial.begin(115200);

    if (!db.init("/collections_db").ok()) {
        Serial.println("DB init failed");
        return;
    }

    if (db.collection("sensors").status.ok()) {
        Serial.println("Created 'sensors' collection");
    }
    db.dropCollection("sensors");

    for (int i = 0; i < 3; ++i) {
        db.collection("test_" + String(i));
    }
    db.dropAll();
}

void loop() {
}
