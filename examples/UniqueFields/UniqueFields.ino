#include <ESPJsonDB.h>

// Demonstrates schema-level unique fields on create and update.

void setup() {
    Serial.begin(115200);

    if (!db.init("/unique_db").ok()) {
        Serial.println("DB init failed");
        return;
    }

    // Define a schema where email and username must be unique
    Schema userSchema;
    userSchema.fields = {
        {"email",    FieldType::String, nullptr, true}, // unique
        {"username", FieldType::String, nullptr, true}, // unique
        {"role",     FieldType::String, "user"}
    };
    db.registerSchema("users", userSchema);

    // Create first user
    JsonDocument u1;
    u1["email"] = "a@x.com";
    u1["username"] = "alpha";
    auto r1 = db.create("users", u1.as<JsonObjectConst>());
    Serial.printf("Create u1: %s\n", r1.status.ok() ? r1.value.c_str() : r1.status.message);

    // Attempt to create a duplicate email (should fail)
    JsonDocument dup;
    dup["email"] = "a@x.com";
    dup["username"] = "alpha2";
    auto rd = db.create("users", dup.as<JsonObjectConst>());
    Serial.printf("Create duplicate email: %s\n", rd.status.ok() ? "OK (unexpected)" : rd.status.message);

    // Create second unique user
    JsonDocument u2;
    u2["email"] = "b@x.com";
    u2["username"] = "bravo";
    auto r2 = db.create("users", u2.as<JsonObjectConst>());
    Serial.printf("Create u2: %s\n", r2.status.ok() ? r2.value.c_str() : r2.status.message);

    if (!r2.status.ok()) return;

    // Try to update u2's email to an existing one (should fail unique check)
    auto stFail = db.updateById("users", r2.value, [](DocView &v){
        v["email"].set("a@x.com");
    });
    Serial.printf("Update to duplicate email: %s\n", stFail.ok() ? "OK (unexpected)" : stFail.message);

    // Update u2's email to a new unique value (should succeed)
    auto stOk = db.updateById("users", r2.value, [](DocView &v){
        v["email"].set("c@x.com");
    });
    Serial.printf("Update to new email: %s\n", stOk.ok() ? "OK" : stOk.message);

    // Verify with a findOne filter
    JsonDocument filter;
    filter["email"] = "c@x.com";
    auto found = db.findOne("users", filter);
    if (found.status.ok()) {
        Serial.printf("Found updated user: %s\n", found.value["username"].as<const char*>());
    }
}

void loop() {}

