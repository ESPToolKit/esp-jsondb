#include <ESPJsonDB.h>

ESPJsonDB db;

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
	    SchemaField{"email", FieldType::String, std::string("a@b.c")},
	    SchemaField{"username", FieldType::String},
	    SchemaField{"role", FieldType::String, std::string("user")},
	    SchemaField{"password", FieldType::String},
	    SchemaField{"age", FieldType::Int32}
	};
	userSchema.fields[1].required = true;
	userSchema.fields[3].required = true;
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
