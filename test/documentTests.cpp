#include "dbTest.h"

void DbTester::simpleDocCreate() {
	lastNewDocId = "";
	JsonDocument newUser;
	newUser["email"] = "espjsondb@gmail.com";
	newUser["username"] = "esp-jsondb";
	auto result = db.create("users", newUser.as<JsonObjectConst>());
	if (!result.status.ok()) {
		ESP_LOGE(
            DB_TESTER_TAG,
            "Failed to add new user to DB. Error: %s",
            result.status.message
        );
	}else{
        ESP_LOGI(DB_TESTER_TAG, "New user created");
		lastNewDocId = result.value;
    }
}

void DbTester::simpleDocRemove(){
	if( lastNewDocId.empty() ) return;
	auto result = db.removeById("users", lastNewDocId);
	if( !result.ok() ){
		ESP_LOGE(DB_TESTER_TAG, "Failed to remove user from DB. Error: %s", result.message);
	}else{
		ESP_LOGI(DB_TESTER_TAG, "User removed");
	}
}

void DbTester::multiDocCreate(int docNum){
	int created = 0;
	for (int i = 0; i < docNum; i++) {
		JsonDocument newUser;
		newUser["email"] = "espjsondb_" + std::to_string(i) + "_@gmail.com";
		newUser["username"] = "esp-jsondb_" + std::to_string(i);
		newUser["role"] = i % 2 ? "admin" : "user";
		auto result = db.create("users", newUser.as<JsonObjectConst>());
		if (result.status.ok()) {
			created++;
		}else{
			ESP_LOGE(
				DB_TESTER_TAG,
				"Failed to add new user (%s) to DB. Error: %s",
				newUser["email"],
				result.status.message
			);
		}
	}
	ESP_LOGI(
		DB_TESTER_TAG,
		"Created %d document",
		created
	);
}

void DbTester::multiDocRemove(){
	// Remove all admins
	auto result = db.removeMany("users", [](const DocView &doc) {
		return doc["role"].as<std::string>() == "admin";
	});
	if (!result.status.ok()) {
		ESP_LOGE(
			DB_TESTER_TAG,
			"Failed to remove users with admin role. Error: %s",
			result.status.message
		);
	}else{
		ESP_LOGI(DB_TESTER_TAG, "Removed %d users", result.value);
	}
}