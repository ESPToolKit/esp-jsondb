#include <ESPJsonDB.h>

static DataBase db;

// Demonstrates bulk insert with createMany and reading returned ids.

void setup() {
    Serial.begin(115200);

    if (!db.init("/createmany_db").ok()) {
        Serial.println("DB init failed");
        return;
    }

    // Prepare an array of documents
    JsonDocument arrDoc;
    JsonArray arr = arrDoc.to<JsonArray>();
    for (int i = 0; i < 5; ++i) {
        JsonObject o = arr.add<JsonObject>();
        o["email"] = String("bulk_") + i + "@example.com";
        o["index"] = i;
        o["active"] = (i % 2) == 0;
    }

    auto res = db.createMany("users", arrDoc);
    if (!res.status.ok()) {
        Serial.printf("createMany failed: %s\n", res.status.message);
        return;
    }

    Serial.printf("Inserted %u documents\n", (unsigned)res.value.size());
    for (auto &id : res.value) {
        Serial.printf(" - _id: %s\n", id.c_str());
    }

    // Find one of them by filter to confirm
    JsonDocument filter;
    filter["index"] = 3;
    auto f = db.findOne("users", filter);
    if (f.status.ok()) {
        std::string email = f.value["email"].as<std::string>();
        Serial.printf("Found index=3: %s\n", email.c_str());
    }
}

void loop() {}
