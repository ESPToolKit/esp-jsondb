#include <ESPJsonDB.h>
#include <ctype.h>

static ESPJsonDB db;

// Realistic configuration management example demonstrating:
// - Complex schema with validation
// - findOne using JSON filter
// - getOr<T> with sensible defaults
// - updateOne with upsert=true to create or update config

static const char* CONF_COLLECTION = "config";

// Simple validation helpers
static bool validHostname(const char* s) {
    if (!s) return false;
    size_t n = strlen(s);
    if (n == 0 || n > 32) return false;
    for (size_t i = 0; i < n; ++i) {
        char c = s[i];
        if (!(isalnum(c) || c == '-' || c == '_')) return false;
    }
    return true;
}

static bool validSsid(const char* s) {
    if (!s) return false;
    size_t n = strlen(s);
    return n > 0 && n <= 32;
}

static bool validPassword(const char* s) {
    if (!s) return false;
    size_t n = strlen(s);
    // allow open networks (empty) or WPA(8..63)
    return n == 0 || (n >= 8 && n <= 63);
}

static ValidationError netConfigValidate(const JsonObjectConst &doc) {
    // Require a confType matching this schema
    const char* confType = doc["confType"].as<const char*>();
    if (!confType || strcmp(confType, "netConf") != 0) {
        return {false, "confType must be 'netConf'"};
    }

    const char* ssid = doc["ssid"].as<const char*>();
    if (!validSsid(ssid)) return {false, "ssid invalid"};

    const char* password = doc["password"].as<const char*>();
    if (!validPassword(password)) return {false, "password invalid"};

    const char* hostname = doc["hostname"].as<const char*>();
    if (!validHostname(hostname)) return {false, "hostname invalid"};

    // Optional nested object: ipConfig { mode: dhcp|static, ip, gw, mask }
    auto ipCfg = doc["ipConfig"];
    if (!ipCfg.isNull()) {
        const char* mode = ipCfg["mode"].as<const char*>();
        if (!mode || (strcmp(mode, "dhcp") != 0 && strcmp(mode, "static") != 0)) {
            return {false, "ipConfig.mode must be 'dhcp' or 'static'"};
        }
        if (strcmp(mode, "static") == 0) {
            if (!ipCfg["ip"].is<const char*>() || !ipCfg["gw"].is<const char*>() || !ipCfg["mask"].is<const char*>()) {
                return {false, "static ipConfig requires ip, gw, mask"};
            }
        }
    }
    return {true, ""};
}

void setup() {
    Serial.begin(115200);

    if (!db.init("/config_db").ok()) {
        Serial.println("DB init failed");
        return;
    }

    // Register a schema for network configuration
    Schema netSchema;
    netSchema.fields = {
        {"confType", FieldType::String, "netConf"},
        {"ssid", FieldType::String, ""},
        {"password", FieldType::String, ""},
        {"hostname", FieldType::String, "ESP_DEVICE"},
        {"autoReconnect", FieldType::Bool, "true"},
        // Ensure object exists by providing any defaultValue (unused for Object)
        {"ipConfig", FieldType::Object, ""}
    };
    netSchema.validate = netConfigValidate;
    db.registerSchema(CONF_COLLECTION, netSchema);

    // Upsert a configuration document using updateOne(filter, patch, true)
    JsonDocument filter;
    filter["confType"] = "netConf";

    JsonDocument patch; // supply values typically sourced from user input
    patch["confType"] = "netConf";
    patch["ssid"] = "MyWiFi";
    patch["password"] = "supersecret";
    patch["hostname"] = "ESP_MAIN";
    patch["autoReconnect"] = true;
    JsonObject ip = patch["ipConfig"].to<JsonObject>();
    ip["mode"] = "dhcp"; // or "static" with ip/gw/mask

    auto up = db.updateOne(CONF_COLLECTION, filter, patch, /*create=*/true);
    Serial.printf("Config upsert: %s\n", up.ok() ? "OK" : up.message);

    // Retrieve and read using getOr with defaults
    JsonDocument findFilter;
    findFilter["confType"] = "netConf";
    auto confResult = db.findOne(CONF_COLLECTION, findFilter);

    const char *ssid = confResult.value.getOr<const char *>("ssid", "FallbackSSID");
    const char *password = confResult.value.getOr<const char *>("password", "");
    const char *hostname = confResult.value.getOr<const char *>("hostname", "ESP_DEVICE");
    bool autoReconnect = confResult.value.getOr<bool>("autoReconnect", true);

    if (!confResult.status.ok()) {
        Serial.println("Config not found. Using defaults.");
    }

    Serial.printf("SSID: %s\n", ssid);
    Serial.printf("Hostname: %s\n", hostname);
    Serial.printf("AutoReconnect: %s\n", autoReconnect ? "true" : "false");
}

void loop() {}
