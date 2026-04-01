#include "dbTest.h"
#include "../src/esp_jsondb/storage/doc_codec.h"
#include "../src/esp_jsondb/utils/objectId.h"

#if __has_include(<ESPJsonDBCompressor.h>)
#include <ESPJsonDBCompressor.h>
#endif

namespace {
constexpr uint8_t kLegacyMagic[4] = {'J', 'D', 'B', '2'};
constexpr uint16_t kLegacyVersion = 1;
constexpr uint32_t kLegacyHeaderSize = 24 + 8 + 8 + 4 + 4 + 2;

bool isHex24(const std::string &id) {
	if (id.size() != 24)
		return false;
	for (char c : id) {
		const bool isDigit = (c >= '0' && c <= '9');
		const bool isLower = (c >= 'a' && c <= 'f');
		const bool isUpper = (c >= 'A' && c <= 'F');
		if (!isDigit && !isLower && !isUpper)
			return false;
	}
	return true;
}

std::string collectionDirPath(const std::string &collection) {
	return std::string("/test_db/") + collection;
}

std::string documentPath(const std::string &collection, const std::string &id) {
	return collectionDirPath(collection) + "/" + id + ".jdb";
}

void appendU16(JsonDbVector<uint8_t> &out, uint16_t value) {
	out.push_back(static_cast<uint8_t>(value & 0xFFu));
	out.push_back(static_cast<uint8_t>((value >> 8) & 0xFFu));
}

void appendU32(JsonDbVector<uint8_t> &out, uint32_t value) {
	out.push_back(static_cast<uint8_t>(value & 0xFFu));
	out.push_back(static_cast<uint8_t>((value >> 8) & 0xFFu));
	out.push_back(static_cast<uint8_t>((value >> 16) & 0xFFu));
	out.push_back(static_cast<uint8_t>((value >> 24) & 0xFFu));
}

void appendU64(JsonDbVector<uint8_t> &out, uint64_t value) {
	for (uint8_t shift = 0; shift < 8; ++shift) {
		out.push_back(static_cast<uint8_t>((value >> (shift * 8U)) & 0xFFu));
	}
}

JsonDbVector<uint8_t> encodeLegacyRecord(
    const RecordHeader &header, const JsonDbVector<uint8_t> &payload
) {
	JsonDbVector<uint8_t> encoded{JsonDbAllocator<uint8_t>(false)};
	const uint32_t payloadCrc = DocCodec::crc32(payload.data(), payload.size());
	encoded.insert(encoded.end(), kLegacyMagic, kLegacyMagic + sizeof(kLegacyMagic));
	appendU16(encoded, kLegacyVersion);
	appendU16(encoded, header.flags);
	appendU32(encoded, kLegacyHeaderSize);
	appendU32(encoded, static_cast<uint32_t>(payload.size()));
	for (size_t idx = 0; idx < DocId::kHexLength; ++idx) {
		encoded.push_back(static_cast<uint8_t>(header.id.c_str()[idx]));
	}
	appendU64(encoded, header.createdAtMs);
	appendU64(encoded, header.updatedAtMs);
	appendU32(encoded, header.revision);
	appendU32(encoded, payloadCrc);
	appendU16(encoded, header.flags);
	encoded.insert(encoded.end(), payload.begin(), payload.end());
	appendU32(encoded, payloadCrc);
	return encoded;
}
} // namespace

void DbTester::simpleDocCreate() {
	lastNewDocId = "";
	JsonDocument newUser;
	newUser["email"] = "espjsondb@gmail.com";
	newUser["username"] = "esp-jsondb";
	auto result = db.create("users", newUser.as<JsonObjectConst>());
	if (!result.status.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "Failed to add new user to DB. Error: %s", result.status.message);
	} else {
		ESP_LOGI(DB_TESTER_TAG, "New user created");
		lastNewDocId = result.value;
	}
}

void DbTester::simpleDocRemove() {
	if (lastNewDocId.empty())
		return;
	auto result = db.removeById("users", lastNewDocId);
	if (!result.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "Failed to remove user from DB. Error: %s", result.message);
	} else {
		ESP_LOGI(DB_TESTER_TAG, "User removed");
	}
}

void DbTester::multiDocCreate(int docNum) {
	int created = 0;
	for (int index = 0; index < docNum; index++) {
		JsonDocument newUser;
		newUser["email"] = "espjsondb_" + std::to_string(index) + "_@gmail.com";
		newUser["username"] = "esp-jsondb_" + std::to_string(index);
		newUser["role"] = index % 2 ? "admin" : "user";
		auto result = db.create("users", newUser.as<JsonObjectConst>());
		if (result.status.ok()) {
			created++;
		} else {
			ESP_LOGE(
			    DB_TESTER_TAG,
			    "Failed to add new user (%s) to DB. Error: %s",
			    newUser["email"],
			    result.status.message
			);
		}
	}
	ESP_LOGI(DB_TESTER_TAG, "Created %d document", created);
}

void DbTester::multiDocRemove() {
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
	} else {
		ESP_LOGI(DB_TESTER_TAG, "Removed %d users", result.value);
	}
}

void DbTester::idLifecycleRoundTripTest() {
	const std::string collection = "id_lifecycle";
	(void)db.dropCollection(collection);

	JsonDocument seed;
	seed["kind"] = "id_lifecycle";
	seed["value"] = 1;

	auto createRes = db.create(collection, seed.as<JsonObjectConst>());
	if (!createRes.status.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "idLifecycleRoundTripTest create failed: %s", createRes.status.message);
		return;
	}
	const std::string id = createRes.value;
	if (!isHex24(id)) {
		ESP_LOGE(DB_TESTER_TAG, "idLifecycleRoundTripTest expected 24-char hex id");
		return;
	}

	auto findRes = db.findById(collection, id);
	if (!findRes.status.ok() || findRes.value["value"].as<int>() != 1) {
		ESP_LOGE(DB_TESTER_TAG, "idLifecycleRoundTripTest findById failed");
		return;
	}

	auto updateStatus = db.updateById(collection, id, [](DocView &doc) { doc["value"].set(2); });
	if (!updateStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "idLifecycleRoundTripTest updateById failed: %s", updateStatus.message);
		return;
	}

	auto findUpdated = db.findById(collection, id);
	if (!findUpdated.status.ok() || findUpdated.value["value"].as<int>() != 2) {
		ESP_LOGE(DB_TESTER_TAG, "idLifecycleRoundTripTest update verification failed");
		return;
	}

	auto removeStatus = db.removeById(collection, id);
	if (!removeStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "idLifecycleRoundTripTest removeById failed: %s", removeStatus.message);
		return;
	}

	auto findRemoved = db.findById(collection, id);
	if (findRemoved.status.code != DbStatusCode::NotFound) {
		ESP_LOGE(DB_TESTER_TAG, "idLifecycleRoundTripTest expected NotFound after remove");
		return;
	}

	auto invalidFind = db.findById(collection, "invalid-id");
	if (invalidFind.status.code != DbStatusCode::NotFound) {
		ESP_LOGE(DB_TESTER_TAG, "idLifecycleRoundTripTest invalid-id lookup expected NotFound");
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "ID lifecycle roundtrip test passed");
}

void DbTester::snapshotRestoreIdLifecycleTest() {
	auto dropStatus = db.dropAll();
	if (!dropStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotRestoreIdLifecycleTest dropAll failed: %s", dropStatus.message);
		return;
	}

	const std::string collection = "snapshot_ids";
	std::vector<std::string> ids;
	ids.reserve(2);
	for (int i = 0; i < 2; ++i) {
		JsonDocument doc;
		doc["index"] = i;
		doc["kind"] = "snapshot";
		auto createRes = db.create(collection, doc.as<JsonObjectConst>());
		if (!createRes.status.ok()) {
			ESP_LOGE(
			    DB_TESTER_TAG,
			    "snapshotRestoreIdLifecycleTest create failed: %s",
			    createRes.status.message
			);
			return;
		}
		if (!isHex24(createRes.value)) {
			ESP_LOGE(DB_TESTER_TAG, "snapshotRestoreIdLifecycleTest invalid generated id");
			return;
		}
		ids.push_back(createRes.value);
	}

	auto syncStatus = db.syncNow();
	if (!syncStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotRestoreIdLifecycleTest sync failed: %s", syncStatus.message);
		return;
	}

	auto snapshot = db.getSnapshot(SnapshotMode::OnDiskOnly);
	JsonArrayConst arr = snapshot["collections"][collection.c_str()].as<JsonArrayConst>();
	if (arr.isNull() || arr.size() != ids.size()) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotRestoreIdLifecycleTest snapshot collection missing");
		return;
	}
	for (JsonObjectConst obj : arr) {
		const char *id = obj["_id"].as<const char *>();
		if (!id || !isHex24(id)) {
			ESP_LOGE(DB_TESTER_TAG, "snapshotRestoreIdLifecycleTest snapshot _id format invalid");
			return;
		}
		JsonObjectConst meta = obj["_meta"].as<JsonObjectConst>();
		if (meta.isNull() || meta["createdAtMs"].isNull() || meta["updatedAtMs"].isNull() ||
		    meta["revision"].isNull()) {
			ESP_LOGE(DB_TESTER_TAG, "snapshotRestoreIdLifecycleTest snapshot metadata missing");
			return;
		}
	}

	JsonDocument badSnapshot;
	badSnapshot.set(snapshot);
	badSnapshot["collections"][collection.c_str()][0]["_id"] = "invalid-id";
	auto badRestoreStatus = db.restoreFromSnapshot(badSnapshot);
	if (badRestoreStatus.code != DbStatusCode::InvalidArgument) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotRestoreIdLifecycleTest expected invalid _id restore failure");
		return;
	}

	dropStatus = db.dropAll();
	if (!dropStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotRestoreIdLifecycleTest second dropAll failed: %s", dropStatus.message);
		return;
	}

	auto restoreStatus = db.restoreFromSnapshot(snapshot);
	if (!restoreStatus.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "snapshotRestoreIdLifecycleTest restore failed: %s",
		    restoreStatus.message
		);
		return;
	}

	for (std::size_t i = 0; i < ids.size(); ++i) {
		auto findRes = db.findById(collection, ids[i]);
		if (!findRes.status.ok()) {
			ESP_LOGE(
			    DB_TESTER_TAG,
			    "snapshotRestoreIdLifecycleTest findById after restore failed: %s",
			    findRes.status.message
			);
			return;
		}
		if (findRes.value["index"].as<int>() != static_cast<int>(i)) {
			ESP_LOGE(DB_TESTER_TAG, "snapshotRestoreIdLifecycleTest restored payload mismatch");
			return;
		}
		if (findRes.value.meta().createdAtMs == 0 || findRes.value.meta().updatedAtMs == 0 ||
		    findRes.value.meta().revision == 0) {
			ESP_LOGE(DB_TESTER_TAG, "snapshotRestoreIdLifecycleTest restored metadata missing");
			return;
		}
	}

	ESP_LOGI(DB_TESTER_TAG, "Snapshot restore ID lifecycle test passed");
}

void DbTester::snapshotStreamRoundTripTest() {
	auto dropStatus = db.dropAll();
	if (!dropStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotStreamRoundTripTest dropAll failed: %s", dropStatus.message);
		return;
	}

	const std::string collection = "snapshot_stream";
	std::vector<std::string> ids;
	ids.reserve(2);
	for (int i = 0; i < 2; ++i) {
		JsonDocument doc;
		doc["index"] = i;
		doc["kind"] = "stream";
		auto createRes = db.create(collection, doc.as<JsonObjectConst>());
		if (!createRes.status.ok()) {
			ESP_LOGE(DB_TESTER_TAG, "snapshotStreamRoundTripTest create failed: %s", createRes.status.message);
			return;
		}
		ids.push_back(createRes.value);
	}

	auto syncStatus = db.syncNow();
	if (!syncStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotStreamRoundTripTest sync failed: %s", syncStatus.message);
		return;
	}

	JsonDocument expected = db.getSnapshot(SnapshotMode::OnDiskOnly);
	const char *snapshotPath = "/snapshot_stream_roundtrip.json";
	(void)LittleFS.remove(snapshotPath);
	File out = LittleFS.open(snapshotPath, FILE_WRITE);
	if (!out) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotStreamRoundTripTest open write file failed");
		return;
	}
	auto writeStatus = db.writeSnapshot(out, SnapshotMode::OnDiskOnly);
	out.close();
	if (!writeStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotStreamRoundTripTest writeSnapshot failed: %s", writeStatus.message);
		(void)LittleFS.remove(snapshotPath);
		return;
	}

	File in = LittleFS.open(snapshotPath, FILE_READ);
	if (!in) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotStreamRoundTripTest open read file failed");
		(void)LittleFS.remove(snapshotPath);
		return;
	}
	JsonDocument actual;
	auto parseErr = deserializeJson(actual, in);
	in.close();
	if (parseErr) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotStreamRoundTripTest parse failed");
		(void)LittleFS.remove(snapshotPath);
		return;
	}

	JsonArrayConst expectedArr = expected["collections"][collection.c_str()].as<JsonArrayConst>();
	JsonArrayConst actualArr = actual["collections"][collection.c_str()].as<JsonArrayConst>();
	if (expectedArr.isNull() || actualArr.isNull() || expectedArr.size() != actualArr.size()) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotStreamRoundTripTest snapshot shape mismatch");
		(void)LittleFS.remove(snapshotPath);
		return;
	}

	dropStatus = db.dropAll();
	if (!dropStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotStreamRoundTripTest second dropAll failed: %s", dropStatus.message);
		(void)LittleFS.remove(snapshotPath);
		return;
	}

	in = LittleFS.open(snapshotPath, FILE_READ);
	if (!in) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotStreamRoundTripTest reopen read file failed");
		(void)LittleFS.remove(snapshotPath);
		return;
	}
	auto restoreStatus = db.restoreFromSnapshot(in);
	in.close();
	(void)LittleFS.remove(snapshotPath);
	if (!restoreStatus.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "snapshotStreamRoundTripTest restoreFromSnapshot(Stream) failed: %s",
		    restoreStatus.message
		);
		return;
	}

	for (std::size_t i = 0; i < ids.size(); ++i) {
		auto findRes = db.findById(collection, ids[i]);
		if (!findRes.status.ok() || findRes.value["index"].as<int>() != static_cast<int>(i)) {
			ESP_LOGE(DB_TESTER_TAG, "snapshotStreamRoundTripTest restore verification failed");
			return;
		}
		if (findRes.value.meta().createdAtMs == 0 || findRes.value.meta().revision == 0) {
			ESP_LOGE(DB_TESTER_TAG, "snapshotStreamRoundTripTest restored metadata missing");
			return;
		}
	}

	ESP_LOGI(DB_TESTER_TAG, "Snapshot stream roundtrip test passed");
}

void DbTester::snapshotStreamInvalidJsonTest() {
	auto dropStatus = db.dropAll();
	if (!dropStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotStreamInvalidJsonTest dropAll failed: %s", dropStatus.message);
		return;
	}

	JsonDocument seed;
	seed["kind"] = "invalid_snapshot";
	seed["value"] = 7;
	auto createRes = db.create("invalid_snapshot", seed.as<JsonObjectConst>());
	if (!createRes.status.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotStreamInvalidJsonTest create failed: %s", createRes.status.message);
		return;
	}

	const char *snapshotPath = "/snapshot_invalid.json";
	(void)LittleFS.remove(snapshotPath);
	File out = LittleFS.open(snapshotPath, FILE_WRITE);
	if (!out) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotStreamInvalidJsonTest open write file failed");
		return;
	}
	out.print("{invalid");
	out.close();

	File in = LittleFS.open(snapshotPath, FILE_READ);
	if (!in) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotStreamInvalidJsonTest open read file failed");
		(void)LittleFS.remove(snapshotPath);
		return;
	}
	auto restoreStatus = db.restoreFromSnapshot(in);
	in.close();
	(void)LittleFS.remove(snapshotPath);
	if (restoreStatus.code != DbStatusCode::InvalidArgument) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotStreamInvalidJsonTest expected InvalidArgument");
		return;
	}

	auto findRes = db.findById("invalid_snapshot", createRes.value);
	if (!findRes.status.ok() || findRes.value["value"].as<int>() != 7) {
		ESP_LOGE(DB_TESTER_TAG, "snapshotStreamInvalidJsonTest mutated DB on parse failure");
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "Snapshot stream invalid JSON test passed");
}

#if __has_include(<ESPJsonDBCompressor.h>)
void DbTester::compressedSnapshotRoundTripTest() {
	auto dropStatus = db.dropAll();
	if (!dropStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotRoundTripTest dropAll failed: %s", dropStatus.message);
		return;
	}

	const std::string collection = "compressed_snapshot";
	std::vector<std::string> ids;
	ids.reserve(2);
	for (int i = 0; i < 2; ++i) {
		JsonDocument doc;
		doc["index"] = i;
		doc["kind"] = "compressed";
		auto createRes = db.create(collection, doc.as<JsonObjectConst>());
		if (!createRes.status.ok()) {
			ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotRoundTripTest create failed: %s", createRes.status.message);
			return;
		}
		ids.push_back(createRes.value);
	}

	ESPCompressor compressor;
	if (compressor.init() != CompressionError::Ok) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotRoundTripTest compressor init failed");
		return;
	}

	std::vector<uint8_t> compressed;
	DynamicBufferSink sink(compressed);
	auto writeStatus = db.writeCompressedSnapshot(
	    compressor, sink, SnapshotMode::OnDiskOnly, nullptr, CompressionJobOptions{}
	);
	if (!writeStatus.ok() || compressed.empty()) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotRoundTripTest writeCompressedSnapshot failed: %s", writeStatus.message);
		return;
	}

	auto clearStatus = db.dropAll();
	if (!clearStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotRoundTripTest second dropAll failed: %s", clearStatus.message);
		return;
	}

	BufferSource source(compressed.data(), compressed.size());
	auto restoreStatus = db.restoreCompressedSnapshot(
	    compressor, source, nullptr, CompressionJobOptions{}
	);
	if (!restoreStatus.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "compressedSnapshotRoundTripTest restoreCompressedSnapshot failed: %s",
		    restoreStatus.message
		);
		return;
	}

	for (std::size_t i = 0; i < ids.size(); ++i) {
		auto findRes = db.findById(collection, ids[i]);
		if (!findRes.status.ok() || findRes.value["index"].as<int>() != static_cast<int>(i)) {
			ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotRoundTripTest restore verification failed");
			return;
		}
		if (findRes.value.meta().createdAtMs == 0 || findRes.value.meta().revision == 0) {
			ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotRoundTripTest restored metadata missing");
			return;
		}
	}

	ESP_LOGI(DB_TESTER_TAG, "Compressed snapshot buffer roundtrip test passed");
}

void DbTester::compressedSnapshotFileRoundTripTest() {
	auto dropStatus = db.dropAll();
	if (!dropStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotFileRoundTripTest dropAll failed: %s", dropStatus.message);
		return;
	}

	JsonDocument doc;
	doc["kind"] = "file_roundtrip";
	doc["value"] = 11;
	auto createRes = db.create("compressed_files", doc.as<JsonObjectConst>());
	if (!createRes.status.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotFileRoundTripTest create failed: %s", createRes.status.message);
		return;
	}

	ESPCompressor compressor;
	if (compressor.init() != CompressionError::Ok) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotFileRoundTripTest compressor init failed");
		return;
	}

	const char *compressedPath = "/snapshot_roundtrip.esc";
	(void)LittleFS.remove(compressedPath);
	FileSink sink(LittleFS, compressedPath, true);
	auto writeStatus = db.writeCompressedSnapshot(
	    compressor, sink, SnapshotMode::OnDiskOnly, nullptr, CompressionJobOptions{}
	);
	if (!writeStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotFileRoundTripTest write failed: %s", writeStatus.message);
		(void)LittleFS.remove(compressedPath);
		return;
	}

	auto clearStatus = db.dropAll();
	if (!clearStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotFileRoundTripTest second dropAll failed: %s", clearStatus.message);
		(void)LittleFS.remove(compressedPath);
		return;
	}

	FileSource source(LittleFS, compressedPath);
	auto restoreStatus = db.restoreCompressedSnapshot(
	    compressor, source, nullptr, CompressionJobOptions{}
	);
	(void)LittleFS.remove(compressedPath);
	if (!restoreStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotFileRoundTripTest restore failed: %s", restoreStatus.message);
		return;
	}

	auto findRes = db.findById("compressed_files", createRes.value);
	if (!findRes.status.ok() || findRes.value["value"].as<int>() != 11) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotFileRoundTripTest restore verification failed");
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "Compressed snapshot file roundtrip test passed");
}

void DbTester::compressedSnapshotDbFilesRoundTripTest() {
	auto dropStatus = db.dropAll();
	if (!dropStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotDbFilesRoundTripTest dropAll failed: %s", dropStatus.message);
		return;
	}

	JsonDocument doc;
	doc["kind"] = "db_files_backup";
	doc["value"] = 23;
	auto createRes = db.create("compressed_db_files", doc.as<JsonObjectConst>());
	if (!createRes.status.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotDbFilesRoundTripTest create failed: %s", createRes.status.message);
		return;
	}

	ESPCompressor compressor;
	if (compressor.init() != CompressionError::Ok) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotDbFilesRoundTripTest compressor init failed");
		return;
	}

	std::vector<uint8_t> compressed;
	DynamicBufferSink sink(compressed);
	auto writeStatus = db.writeCompressedSnapshot(
	    compressor, sink, SnapshotMode::OnDiskOnly, nullptr, CompressionJobOptions{}
	);
	if (!writeStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotDbFilesRoundTripTest write failed: %s", writeStatus.message);
		return;
	}

	auto storeStatus =
	    db.files().writeFile("backups/latest.esc", compressed.data(), compressed.size(), true);
	if (!storeStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotDbFilesRoundTripTest store failed: %s", storeStatus.message);
		return;
	}

	auto stagedBackup = db.files().readFile("backups/latest.esc");
	if (!stagedBackup.status.ok() || stagedBackup.value.empty()) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotDbFilesRoundTripTest stage read failed");
		return;
	}

	BufferSource source(stagedBackup.value.data(), stagedBackup.value.size());
	auto restoreStatus = db.restoreCompressedSnapshot(
	    compressor, source, nullptr, CompressionJobOptions{}
	);
	if (!restoreStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotDbFilesRoundTripTest restore failed: %s", restoreStatus.message);
		return;
	}

	auto findRes = db.findById("compressed_db_files", createRes.value);
	if (!findRes.status.ok() || findRes.value["value"].as<int>() != 23) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotDbFilesRoundTripTest restore verification failed");
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "Compressed snapshot db.files() staging test passed");
}

void DbTester::compressedSnapshotCorruptionTest() {
	auto dropStatus = db.dropAll();
	if (!dropStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotCorruptionTest dropAll failed: %s", dropStatus.message);
		return;
	}

	JsonDocument doc;
	doc["kind"] = "corruption_guard";
	doc["value"] = 99;
	auto createRes = db.create("corruption_guard", doc.as<JsonObjectConst>());
	if (!createRes.status.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotCorruptionTest create failed: %s", createRes.status.message);
		return;
	}

	ESPCompressor compressor;
	if (compressor.init() != CompressionError::Ok) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotCorruptionTest compressor init failed");
		return;
	}

	const std::vector<uint8_t> corrupt{'n', 'o', 't', '-', 'e', 's', 'c'};
	BufferSource source(corrupt.data(), corrupt.size());
	auto restoreStatus = db.restoreCompressedSnapshot(
	    compressor, source, nullptr, CompressionJobOptions{}
	);
	if (restoreStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotCorruptionTest expected restore failure");
		return;
	}

	auto findRes = db.findById("corruption_guard", createRes.value);
	if (!findRes.status.ok() || findRes.value["value"].as<int>() != 99) {
		ESP_LOGE(DB_TESTER_TAG, "compressedSnapshotCorruptionTest mutated DB on corrupt input");
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "Compressed snapshot corruption test passed");
}
#endif

void DbTester::docCodecCompatibilityTest() {
	RecordHeader header;
	header.id = ObjectId().toDocId();
	header.createdAtMs = 123456789ULL;
	header.updatedAtMs = 123456999ULL;
	header.revision = 7;
	header.flags = 0x002A;

	JsonDocument payloadDoc;
	payloadDoc["kind"] = "codec";
	payloadDoc["value"] = 42;
	JsonDbVector<uint8_t> payload{JsonDbAllocator<uint8_t>(false)};
	const size_t payloadSize = measureMsgPack(payloadDoc);
	payload.resize(payloadSize);
	if (serializeMsgPack(payloadDoc, payload.data(), payload.size()) != payloadSize) {
		ESP_LOGE(DB_TESTER_TAG, "docCodecCompatibilityTest payload serialization failed");
		return;
	}

	JsonDbVector<uint8_t> encoded{JsonDbAllocator<uint8_t>(false)};
	auto encodeStatus = DocCodec::encodeRecord(header, payload, encoded);
	if (!encodeStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "docCodecCompatibilityTest encode failed: %s", encodeStatus.message);
		return;
	}

	RecordHeader decoded{};
	JsonDbVector<uint8_t> decodedPayload{JsonDbAllocator<uint8_t>(false)};
	auto decodeStatus =
	    DocCodec::decodeRecord(encoded.data(), encoded.size(), decoded, decodedPayload, false);
	if (!decodeStatus.ok() || decoded.flags != header.flags || decoded.revision != header.revision ||
	    decodedPayload != payload) {
		ESP_LOGE(DB_TESTER_TAG, "docCodecCompatibilityTest v2 decode verification failed");
		return;
	}

	auto legacy = encodeLegacyRecord(header, payload);
	RecordHeader legacyDecoded{};
	JsonDbVector<uint8_t> legacyPayload{JsonDbAllocator<uint8_t>(false)};
	auto legacyStatus =
	    DocCodec::decodeRecord(legacy.data(), legacy.size(), legacyDecoded, legacyPayload, false);
	if (!legacyStatus.ok() || legacyDecoded.flags != header.flags || legacyPayload != payload) {
		ESP_LOGE(DB_TESTER_TAG, "docCodecCompatibilityTest legacy decode failed");
		return;
	}

	legacy[4] = 0x63;
	legacy[5] = 0x00;
	RecordHeader badHeader{};
	JsonDbVector<uint8_t> badPayload{JsonDbAllocator<uint8_t>(false)};
	auto badStatus =
	    DocCodec::decodeRecord(legacy.data(), legacy.size(), badHeader, badPayload, false);
	if (badStatus.code != DbStatusCode::SchemaMismatch) {
		ESP_LOGE(DB_TESTER_TAG, "docCodecCompatibilityTest expected SchemaMismatch for bad version");
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "DocCodec compatibility test passed");
}

void DbTester::optimisticConflictTest() {
	auto clearStatus = db.dropAll();
	if (!clearStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "optimisticConflictTest dropAll failed: %s", clearStatus.message);
		return;
	}

	JsonDocument seed;
	seed["count"] = 0;
	auto createRes = db.create("conflict_docs", seed.as<JsonObjectConst>());
	if (!createRes.status.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "optimisticConflictTest create failed: %s", createRes.status.message);
		return;
	}

	const std::string id = createRes.value;
	auto updateStatus = db.updateById("conflict_docs", id, [&](DocView &doc) {
		auto nested =
		    db.updateById("conflict_docs", id, [](DocView &inner) { inner["count"].set(2); });
		if (!nested.ok()) {
			ESP_LOGE(DB_TESTER_TAG, "optimisticConflictTest nested update failed: %s", nested.message);
			return;
		}
		doc["count"].set(1);
	});
	if (updateStatus.code != DbStatusCode::Conflict) {
		ESP_LOGE(DB_TESTER_TAG, "optimisticConflictTest expected Conflict, got %s", updateStatus.message);
		return;
	}

	auto findRes = db.findById("conflict_docs", id);
	if (!findRes.status.ok() || findRes.value["count"].as<int>() != 2) {
		ESP_LOGE(DB_TESTER_TAG, "optimisticConflictTest final document state mismatch");
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "Optimistic conflict test passed");
}

void DbTester::documentFileDeletionOnSyncTest() {
	const std::string collection = "sync_delete_files";
	auto clearStatus = db.dropAll();
	if (!clearStatus.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "documentFileDeletionOnSyncTest dropAll failed: %s", clearStatus.message);
		return;
	}

	JsonDocument firstDoc;
	firstDoc["kind"] = "delete_one";
	auto firstCreate = db.create(collection, firstDoc.as<JsonObjectConst>());
	if (!firstCreate.status.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "documentFileDeletionOnSyncTest first create failed: %s",
		    firstCreate.status.message
		);
		return;
	}

	JsonDocument secondDoc;
	secondDoc["kind"] = "delete_many";
	auto secondCreate = db.create(collection, secondDoc.as<JsonObjectConst>());
	if (!secondCreate.status.ok()) {
		ESP_LOGE(
		    DB_TESTER_TAG,
		    "documentFileDeletionOnSyncTest second create failed: %s",
		    secondCreate.status.message
		);
		return;
	}

	auto seedSync = db.syncNow();
	if (!seedSync.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "documentFileDeletionOnSyncTest seed sync failed: %s", seedSync.message);
		return;
	}

	const std::string firstPath = documentPath(collection, firstCreate.value);
	const std::string secondPath = documentPath(collection, secondCreate.value);
	if (!LittleFS.exists(firstPath.c_str()) || !LittleFS.exists(secondPath.c_str())) {
		ESP_LOGE(DB_TESTER_TAG, "documentFileDeletionOnSyncTest expected seeded files on disk");
		return;
	}

	auto removeOne = db.removeById(collection, firstCreate.value);
	if (!removeOne.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "documentFileDeletionOnSyncTest removeById failed: %s", removeOne.message);
		return;
	}

	auto removeMany = db.removeMany(collection, [](const DocView &doc) {
		return doc["kind"].as<std::string>() == "delete_many";
	});
	if (!removeMany.status.ok() || removeMany.value != 1) {
		ESP_LOGE(DB_TESTER_TAG, "documentFileDeletionOnSyncTest removeMany failed");
		return;
	}

	auto cleanupSync = db.syncNow();
	if (!cleanupSync.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "documentFileDeletionOnSyncTest cleanup sync failed: %s", cleanupSync.message);
		return;
	}

	if (LittleFS.exists(firstPath.c_str()) || LittleFS.exists(secondPath.c_str())) {
		ESP_LOGE(DB_TESTER_TAG, "documentFileDeletionOnSyncTest stale document files remain after sync");
		return;
	}

	ESP_LOGI(DB_TESTER_TAG, "Document file deletion on sync test passed");
}
