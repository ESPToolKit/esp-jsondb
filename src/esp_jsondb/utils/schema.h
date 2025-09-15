#pragma once

#include <Arduino.h>
#include <ArduinoJson.h>

#include <cstdlib>
#include <cstring>
#include <functional>
#include <string>
#include <type_traits>
#include <vector>

struct ValidationError {
	bool valid;
	const char *message; // lifetime must be static or managed externally
};

using ValidateFn = std::function<ValidationError(const JsonObjectConst &)>;
using PreSaveFn = std::function<ValidationError(JsonObject &)>;
using PostLoadFn = std::function<void(JsonObject &)>;

enum class FieldType {
	String,
	Int,
	Float,
	Bool,
	Object,
	Array,
};

struct SchemaField {
	const char *name;
	FieldType type;
	const char *defaultValue = nullptr;
	bool unique = false; // enforce per-collection uniqueness when true
};

struct Schema {
	std::vector<SchemaField> fields;
	PreSaveFn preSave{};
	PostLoadFn postLoad{};
	ValidateFn validate{};

	Schema() = default;

	inline bool hasValidate() const { return validate != nullptr || preSave != nullptr || !fields.empty(); }

	inline void applyDefaults(JsonObject obj) const {
		for (const auto &f : fields) {
			JsonVariant v = obj[f.name];
			if (!v && f.defaultValue) {
				switch (f.type) {
				case FieldType::String:
					obj[f.name] = f.defaultValue;
					break;
				case FieldType::Int:
					obj[f.name] = atoi(f.defaultValue);
					break;
				case FieldType::Float:
					obj[f.name] = atof(f.defaultValue);
					break;
				case FieldType::Bool:
					obj[f.name] = (strcmp(f.defaultValue, "true") == 0 || strcmp(f.defaultValue, "1") == 0);
					break;
				case FieldType::Object:
					obj[f.name].to<JsonObject>();
					break;
				case FieldType::Array:
					obj[f.name].to<JsonArray>();
					break;
				}
			}
		}
	}

	inline bool validateTypes(JsonObjectConst obj) const {
		for (const auto &f : fields) {
			JsonVariantConst v = obj[f.name];
			if (!v.isNull()) {
				switch (f.type) {
				case FieldType::String:
					if (!v.is<const char *>() && !v.is<std::string>() && !v.is<String>()) return false;
					break;
				case FieldType::Int:
					if (!v.is<int>()) return false;
					break;
				case FieldType::Float:
					if (!v.is<float>()) return false;
					break;
				case FieldType::Bool:
					if (!v.is<bool>()) return false;
					break;
				case FieldType::Object:
					if (!v.is<JsonObjectConst>()) return false;
					break;
				case FieldType::Array:
					if (!v.is<JsonArrayConst>()) return false;
					break;
				}
			}
		}
		return true;
	}

	inline ValidationError runPreSave(JsonObject &o) const {
		applyDefaults(o);
		if (!validateTypes(o)) return {false, "schema: invalid type"};
		if (preSave) return preSave(o);
		if (validate) return validate(o);
		return {true, ""};
	}

	inline ValidationError runValidate(const JsonObjectConst &o) const {
		if (!validateTypes(o)) return {false, "schema: invalid type"};
		if (validate) return validate(o);
		return {true, ""};
	}

	inline void runPostLoad(JsonObject &o) const {
		if (postLoad) postLoad(o);
	}
};
