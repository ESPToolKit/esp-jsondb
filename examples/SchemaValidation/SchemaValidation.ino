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

    JsonDocument user;
    user["username"] = "esp-jsondb";
    user["password"] = "secret";
    auto res = db.create("users", user.as<JsonObjectConst>());
    if (res.status.ok()) {
        Serial.printf("Created user %s\n", res.value.c_str());
    }
}

void loop() {
}
