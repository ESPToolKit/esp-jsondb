#include "dbTest.h"

void DbTester::init() {
	// cfg is opional
	SyncConfig cfg;
	cfg.intervalMs = 3000;
	cfg.autosync = true;

	// initialize the db instance
	if (!db.init("/test_db", cfg).ok()) {
		ESP_LOGE(DB_TESTER_TAG, "DB Initialization failed");
		return;
	}

	// Clear the DB first
	db.dropAll();

	// Bind private event handler methods
	// This could be an inline lambda or an external function as well
	db.onEvent(std::bind(&DbTester::dbEventHandler, this, std::placeholders::_1));
	db.onError(std::bind(&DbTester::dbErrorHandler, this, std::placeholders::_1));

	// Run tests
	run();
}

void DbTester::run() {
	printDBDiag();
	// Document tests
	simpleDocCreate();
	simpleDocRemove();
	multiDocCreate(10);
	multiDocRemove();
	refPopulateTest();
	printDBDiag();
	// Collection tests
	simpleCollectionCreate();
	simpleCollectionRemove();
	multiCollectionCreate(10);
	allCollectionDrop();
	printDBDiag();
	// Bulk tests
	updateManyFilter();
	updateManyLambdaFilter();
	updateManyCombined();
	findMany();
	printDBDiag();
	// Schema tests
	schemaFailDocCreate();
	schemaSuccessDocCreate();
	schemaFailWithTypesDocCreate();
	schemaSuccessWithTypesDocCreate();
	schemaFailDocUpdate();
	printDBDiag();
}

void DbTester::dbEventHandler(DBEventType evt) {
	ESP_LOGI(DB_TESTER_TAG, "%s", dbEventTypeToString(evt));
}

void DbTester::dbErrorHandler(const DbStatus &st) {
	ESP_LOGE(DB_TESTER_TAG, "%s: %s", dbStatusCodeToString(st.code), st.message);
}

void DbTester::printDBDiag() {
	JsonDocument diagDoc = db.getDiag();
	ESP_LOGI(DB_TESTER_TAG, "DB Diagnostics");
	serializeJsonPretty(diagDoc, Serial);
	ESP_LOGI(DB_TESTER_TAG, "");
}