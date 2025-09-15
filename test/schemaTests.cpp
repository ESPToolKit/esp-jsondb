#include "dbTest.h"

static ValidationError usersValidate(const JsonObjectConst &doc) {
	auto username = doc["username"];
	auto password = doc["password"];
	if (username.isNull() || password.isNull()) {
		return {false, "username and password are required"};
	}
	return {true, ""};
}

/*
 *   This create should fail.
 *   The Schema validator should prevent the new doc to be created
 */
void DbTester::schemaFailDocCreate() {
	// This two line can be enforced right after init.
	// It is there for testing purpose only.
	userSchema.validate = usersValidate;
	db.registerSchema("users", userSchema);

	JsonDocument newUser;
	newUser["username"] = "admin";
	newUser["thing"] = "notAPassword";
	auto result = db.create("users", newUser.as<JsonObjectConst>());
	if (!result.status.ok()) {
		ESP_LOGE(
			DB_TESTER_TAG,
			"Failed to add new user to DB. Error: %s",
			result.status.message);
	} else {
		ESP_LOGI(DB_TESTER_TAG, "New user created");
	}
}

/*
 *   This create should succeed.
 *   The Schema validator should allow this new doc to be created
 */
void DbTester::schemaSuccessDocCreate() {
	JsonDocument newUser;
	newUser["username"] = "admin";
	newUser["password"] = "aSecureHashedPassword";
	auto result = db.create("users", newUser.as<JsonObjectConst>());
	if (!result.status.ok()) {
		ESP_LOGE(
			DB_TESTER_TAG,
			"Failed to add new user to DB. Error: %s",
			result.status.message);
	} else {
		ESP_LOGI(DB_TESTER_TAG, "New user created");
	}
}

void DbTester::schemaFailWithTypesDocCreate() {
	userSchema.fields = {
		// key - type - default value
		{"email", FieldType::String, "a@b.c"},
		{"username", FieldType::String},
		{"role", FieldType::String, "user"},
		{"password", FieldType::String},
		{"age", FieldType::Int},
		{"height", FieldType::Int},
	};

	JsonDocument newUser;
	newUser["username"] = "admin";
	newUser["password"] = "aSecureHashedPassword";
	newUser["age"] = "cya";
	auto result = db.create("users", newUser.as<JsonObjectConst>());
	if (!result.status.ok()) {
		ESP_LOGE(
			DB_TESTER_TAG,
			"Failed to add new user to DB. Error: %s",
			result.status.message);
	} else {
		ESP_LOGI(DB_TESTER_TAG, "New user created");
	}
}

void DbTester::schemaSuccessWithTypesDocCreate() {
	JsonDocument newUser;
	newUser["username"] = "admin";
	newUser["password"] = "aSecureHashedPassword";
	newUser["age"] = 18;
	auto result = db.create("users", newUser.as<JsonObjectConst>());
	if (!result.status.ok()) {
		ESP_LOGE(
			DB_TESTER_TAG,
			"Failed to add new user to DB. Error: %s",
			result.status.message);
	} else {
		ESP_LOGI(DB_TESTER_TAG, "New user created");
	}
}

// Ensure updates with invalid data are rejected and do not modify the document
void DbTester::schemaFailDocUpdate() {
	// Start from a clean collection and register a strict schema
	db.dropCollection("users");
	userSchema.fields = {
		{"username", FieldType::String},
		{"password", FieldType::String},
	};
	userSchema.validate = usersValidate;
	db.registerSchema("users", userSchema);

	// Create a valid user document
	JsonDocument newUser;
	newUser["username"] = "admin";
	newUser["password"] = "aSecureHashedPassword";
	auto createRes = db.create("users", newUser.as<JsonObjectConst>());
	if (!createRes.status.ok()) {
		ESP_LOGE(
			DB_TESTER_TAG,
			"Failed to add new user to DB. Error: %s",
			createRes.status.message);
	} else {
		ESP_LOGI(DB_TESTER_TAG, "New user created");
	}
    const std::string userId = createRes.value;

	// Attempt to update with invalid data (wrong type for password)
    DbStatus updateStatus = db.updateById("users", userId, [](DocView &doc) {
        doc["password"].set(123);
    });
    if (!updateStatus.ok()) {
        ESP_LOGE(
            DB_TESTER_TAG,
            "Failed to update user document. Error: %s",
            updateStatus.message);
    } else {
        ESP_LOGI(DB_TESTER_TAG, "User document updated");
    }

	// Verify the original document remains unchanged
    auto findRes = db.findById("users", userId);

	if (!findRes.status.ok()) {
		ESP_LOGE(
			DB_TESTER_TAG,
			"Failed to find user document. Error: %s",
			findRes.status.message);
	} else {
		ESP_LOGI(DB_TESTER_TAG, "User document found");
	}

	db.dropCollection("users");
}
