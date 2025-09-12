#pragma once

#include <ArduinoJson.h>
#include <string>

// Reference to another document in a collection
struct DocRef {
	std::string collection;
	std::string id; // target _id

	bool valid() const { return !collection.empty() && !id.empty(); }
};

inline DocRef docRefFromJson(JsonVariantConst v) {
	DocRef r{};
	if (!v.is<JsonObjectConst>()) return r;
	JsonObjectConst obj = v.as<JsonObjectConst>();
	const char *col = obj["collection"].as<const char *>();
	const char *id = obj["_id"].as<const char *>();
	if (col) r.collection = col;
	if (id) r.id = id;
	return r;
}
