#include "../src/esp_jsondb/db.h"
#include <Arduino.h>
#include <ArduinoJson.h>

#define DB_TESTER_TAG "DB_Tester"

class DbTester {
  public:
	void init();

  private:
	ESPJsonDB db;
	void run();
	// Event handlers
	void dbEventHandler(DBEventType evt);
	void dbErrorHandler(const DbStatus &st);

	// Document tests
	std::string lastNewDocId;
	void simpleDocCreate();
	void simpleDocRemove();
	void multiDocCreate(int docNum);
	void multiDocRemove();
	void refPopulateTest();
	void idLifecycleRoundTripTest();
	void snapshotRestoreIdLifecycleTest();
	void fileStorageTest();
	void asyncFileUploadTest();
	void asyncFileUploadRetentionBoundTest();
	void asyncFileUploadQueueOrderTest();
	// Collection tests
	void simpleCollectionCreate();
	void simpleCollectionRemove();
	void multiCollectionCreate(int collNum);
	void allCollectionDrop();
	// Bulk tests
	void updateManyFilter();
	void updateManyLambdaFilter();
	void updateManyCombined();
	void findMany();
	// Schema tests
	Schema userSchema;
	void schemaFailDocCreate();
	void schemaSuccessDocCreate();
	void schemaFailWithTypesDocCreate();
	void schemaSuccessWithTypesDocCreate();
	void schemaFailDocUpdate();
	// Delayed preload tests
	void delayedCollectionAccessBeforeAutosyncTickTest();
	void delayedCollectionSyncNowFallbackTest();
	void delayedCollectionDropBeforeLoadTest();
	void delayedCollectionConfigNormalizationTest();
	void psramBufferWiringTest();
	void psramMemoryBenchmarkTest();
	// Utils
	void printDBDiag();
	void teardownLifecycle();
};

inline DbTester dbTester;
