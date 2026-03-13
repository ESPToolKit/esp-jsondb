#include <ESPJsonDB.h>

ESPJsonDB db;

void setup() {
	Serial.begin(115200);

	ESPJsonDBConfig syncCfg;
	syncCfg.intervalMs = 3000; // autosync every 3s
	syncCfg.autosync = true;

	if (!db.init("/example_db", syncCfg).ok()) {
		Serial.println("DB init failed");
		return;
	}

	db.onEvent([](DBEventType event) { Serial.printf("Event: %s\n", dbEventTypeToString(event)); });

	db.onSyncStatus([](const DBSyncStatus &status) {
		Serial.printf(
		    "Sync: %s (%s) collection=%s %lu/%lu\n",
		    dbSyncStageToString(status.stage),
		    dbSyncSourceToString(status.source),
		    status.collectionName.c_str(),
		    static_cast<unsigned long>(status.collectionsCompleted),
		    static_cast<unsigned long>(status.collectionsTotal)
		);
	});

	db.onError([](const DbStatus &status) { Serial.printf("Error: %s\n", status.message); });

	JsonDocument userDoc;
	userDoc["email"] = "espjsondb@gmail.com";
	userDoc["username"] = "esp-jsondb";
	auto createRes = db.create("users", userDoc.as<JsonObjectConst>());
	if (createRes.status.ok()) {
		Serial.printf("Created user %s\n", createRes.value.c_str());
		db.removeById("users", createRes.value);
	}
}

void loop() {
	// Call db.deinit() before shutting down the DB-owning feature.
}
