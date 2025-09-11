#include "dbTest.h"

void DbTester::updateManyFilter(){
    // Create multiple documents so we can update them.
    multiDocCreate(20);

    // Update role
    JsonDocument updatedDoc;
	updatedDoc["role"] = "admin";

    // Filter by role
	JsonDocument filterDoc;
	filterDoc["role"] = "user";

    // Promote each user to admin.
	auto result = db.updateMany("users", updatedDoc, filterDoc);

    if( result.status.ok() ){
        ESP_LOGI(DB_TESTER_TAG, "Updated %d users", result.value);
    }else{
        ESP_LOGE(DB_TESTER_TAG, "Failed to update users. Error: %s", result.status.message);
    }

    db.dropCollection("users");
}

void DbTester::updateManyLambdaFilter(){
    // Create multiple documents so we can update them.
    multiDocCreate(20);

    // Update role
    JsonDocument updatedDoc;
	updatedDoc["role"] = "admin";

    // Promote each user to admin.
    auto result = db.updateMany("users", updatedDoc, [](const DocView &doc) {
		return doc["role"].as<std::string>() == "user";
	});

    if( result.status.ok() ){
        ESP_LOGI(DB_TESTER_TAG, "Updated %d users", result.value);
    }else{
        ESP_LOGE(DB_TESTER_TAG, "Failed to update users. Error: %s", result.status.message);
    }

    db.dropCollection("users");
}

void DbTester::updateManyCombined(){
    // Create multiple documents so we can update them.
    multiDocCreate(20);

    // Promote each user to admin.
    auto result = db.updateMany("users", [](DocView &doc) {
		if (doc["role"].as<std::string>() == "user") {
			doc["role"].set("admin");
			return true;
		}
		return false;
	});

    if( result.status.ok() ){
        ESP_LOGI(DB_TESTER_TAG, "Updated %d users", result.value);
    }else{
        ESP_LOGE(DB_TESTER_TAG, "Failed to update users. Error: %s", result.status.message);
    }

    db.dropCollection("users");
}

void DbTester::findMany(){
    // Create multiple documents so we can find them.
    multiDocCreate(20);

    // Search for admins
    auto result = db.findMany("users", [](const DocView &doc) {
        return doc["role"].as<std::string>() == "admin";
    });

    if( result.status.ok() ){
        ESP_LOGI(DB_TESTER_TAG, "Found %d users", result.value.size());
    }else{
        ESP_LOGE(DB_TESTER_TAG, "Failed to find users. Error: %s", result.status.message);
    }

    db.dropCollection("users");
}
