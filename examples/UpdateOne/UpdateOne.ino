#include <ESPJsonDB.h>

static DataBase db;

// Demonstrates updateOne with both predicate+mutator and JSON filter+patch.

void setup() {
    Serial.begin(115200);

    if (!db.init("/updateone_db").ok()) {
        Serial.println("DB init failed");
        return;
    }

    // Seed a document
    JsonDocument seed;
    seed["email"] = "user@example.com";
    seed["visits"] = 1;
    auto idRes = db.create("users", seed.as<JsonObjectConst>());
    if (!idRes.status.ok()) {
        Serial.printf("Seed failed: %s\n", idRes.status.message);
        return;
    }

    // 1) updateOne with predicate + mutator (increment a counter)
    auto st1 = db.updateOne(
        "users",
        [&](const DocView &v) {
            return v["email"].as<std::string>() == std::string("user@example.com"); // match by email
        },
        [&](DocView &v) {
            int visits = v["visits"].as<int>();
            v["visits"].set(visits + 1);
        }
    );
    Serial.printf("updateOne (predicate/mutator): %s\n", st1.ok() ? "OK" : st1.message);

    // 2) updateOne with JSON filter + patch (upsert=true)
    JsonDocument filter;
    filter["email"] = "newuser@example.com";

    JsonDocument patch;
    patch["email"] = "newuser@example.com";
    patch["visits"] = 1;
    patch["role"] = "user";

    auto st2 = db.updateOne("users", filter, patch, /*create=*/true);
    Serial.printf("updateOne (filter/patch upsert): %s\n", st2.ok() ? "OK" : st2.message);

    // Verify results
    auto foundExisting = db.findOne("users", [&](const DocView &v){ return v["email"].as<std::string>() == std::string("user@example.com"); });
    if (foundExisting.status.ok()) {
        Serial.printf("Existing visits: %d\n", foundExisting.value["visits"].as<int>());
    }

    JsonDocument f2;
    f2["email"] = "newuser@example.com";
    auto foundUpserted = db.findOne("users", f2);
    if (foundUpserted.status.ok()) {
        std::string role = foundUpserted.value["role"].as<std::string>();
        Serial.printf("Upserted role: %s\n", role.c_str());
    }
}

void loop() {}
