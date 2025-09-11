#include "dbTest.h"

void DbTester::simpleCollectionCreate(){
    auto result = db.collection("sensors");
    if( !result.status.ok() ){
        ESP_LOGE(
            DB_TESTER_TAG,
            "Failed to create 'sensors' collection. Error: %s",
            result.status.message
        );
    }else{
        ESP_LOGI(DB_TESTER_TAG, "Created 'sensors' collection");
    }
}

void DbTester::simpleCollectionRemove(){
    auto result = db.dropCollection("sensors");
    if( !result.ok() ){
        ESP_LOGE(
            DB_TESTER_TAG,
            "Failed to drop 'sensors' collection. Error: %s",
            result.message
        );
    }else{
        ESP_LOGI(DB_TESTER_TAG, "Dropped 'sensors' collection");
    }
}

void DbTester::multiCollectionCreate(int collNum){
    int created = 0;
    for (int i = 0; i < collNum; i++) {
        std::string collectionName = "test_" + std::to_string(i);
        auto result = db.collection(collectionName);
        if (!result.status.ok()) {
            ESP_LOGE(
                DB_TESTER_TAG,
                "Failed to create '%s' collection",
                collectionName.c_str());
            continue;
        }
        created++;
    }
    ESP_LOGI(
        DB_TESTER_TAG,
        "Created %d collection",
        created);
}

void DbTester::allCollectionDrop(){
    auto result = db.dropAll();
    if( !result.ok() ){
        ESP_LOGE(
            DB_TESTER_TAG,
            "Failed to drop all collections. Error: %s",
            result.message
        );
    }else{
        ESP_LOGI(DB_TESTER_TAG, "Dropped all collections");
    }
}
