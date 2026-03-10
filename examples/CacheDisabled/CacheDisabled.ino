#include <ESPJsonDB.h>

ESPJsonDB db;

/**
 * Cache-disabled mode was removed to keep filesystem writes on the sync task.
 * This sketch now demonstrates low-frequency autosync with the cache enabled.
 */
void setup() {
	Serial.begin(115200);

	ESPJsonDBConfig cfg;
	cfg.autosync = true;
	cfg.intervalMs = 3000;

	if (!db.init("/nocache_db", cfg).ok()) {
		Serial.println("DB init failed");
		return;
	}

	auto events = db.collection("events");
	if (!events.status.ok()) {
		Serial.printf("Failed to get events collection: %s\n", events.status.message);
		return;
	}

	JsonDocument eventDoc;
	eventDoc["type"] = "boot";
	eventDoc["ts"] = millis();
	auto created = db.create("events", eventDoc.as<JsonObjectConst>());
	if (!created.status.ok()) {
		Serial.printf("Create failed: %s\n", created.status.message);
		return;
	}
	Serial.printf("Created event %s (queued for sync task flush)\n", created.value.c_str());

	// Reads are served from in-memory cache.
	auto fetched = db.findById("events", created.value);
	if (fetched.status.ok()) {
		std::string kind = fetched.value["type"].as<std::string>();
		Serial.printf("Reloaded event type: %s\n", kind.empty() ? "(null)" : kind.c_str());
	} else {
		Serial.printf("Reload failed: %s\n", fetched.status.message);
	}
}

void loop() {
}
