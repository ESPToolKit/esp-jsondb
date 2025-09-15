#include <esp_jsondb/db.h>

static ValidationError usersValidate(const JsonObjectConst &doc) {
    if (doc["username"].isNull() || doc["password"].isNull())
        return {false, "username and password are required"};
    return {true, ""};
}

void setup() {
    Serial.begin(115200);

    if (!db.init("/schema_db").ok()) {
        Serial.println("DB init failed");
        return;
    }

    Schema userSchema;
    userSchema.fields = {
        {"email", FieldType::String, "a@b.c"},
        {"username", FieldType::String},
        {"role", FieldType::String, "user"},
        {"password", FieldType::String},
        {"age", FieldType::Int}
    };
    userSchema.validate = usersValidate;
    db.registerSchema("users", userSchema);

    JsonDocument userDoc;
    userDoc["username"] = "esp-jsondb";
    userDoc["password"] = "secret";
    auto createRes = db.create("users", userDoc.as<JsonObjectConst>());
    if (createRes.status.ok()) {
        Serial.printf("Created user %s\n", createRes.value.c_str());
    }
}

void loop() {
}
