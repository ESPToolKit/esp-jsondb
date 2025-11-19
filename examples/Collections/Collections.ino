#include <ESPJsonDB.h>

static DataBase db;

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

    for (int index = 0; index < 3; ++index) {
        db.collection("test_" + String(index));
    }
    db.dropAll();
}

void loop() {
}
