#pragma once

#include <Arduino.h>
#include <ArduinoJson.h>

#include <cstdint>
#include <functional>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

struct ValidationError {
	bool valid = true;
	const char *message = "";
};

using ValidateFn = std::function<ValidationError(const JsonObjectConst &)>;
using PreSaveFn = std::function<ValidationError(JsonObject &)>;
using PostLoadFn = std::function<void(JsonObject &)>;

enum class FieldType {
	String,
	Int32,
	Int64,
	UInt32,
	UInt64,
	Float,
	Double,
	Bool,
	Object,
	Array,
	Int = Int32,
};

struct EmptyObjectTag {};
struct EmptyArrayTag {};

using JsonDefaultValue = std::variant<
    std::monostate,
    std::string,
    int32_t,
    int64_t,
    uint32_t,
    uint64_t,
    float,
    double,
    bool,
    EmptyObjectTag,
    EmptyArrayTag>;

struct SchemaField {
	const char *name = nullptr;
	FieldType type = FieldType::String;
	bool required = false;
	bool unique = false;
	bool hasDefault = false;
	JsonDefaultValue defaultValue{};

	SchemaField() = default;

	SchemaField(const char *fieldName, FieldType fieldType)
	    : name(fieldName), type(fieldType) {
	}

	SchemaField(const char *fieldName, FieldType fieldType, const char *defaultString)
	    : name(fieldName), type(fieldType), hasDefault(defaultString != nullptr),
	      defaultValue(defaultString ? JsonDefaultValue(std::string(defaultString))
	                                 : JsonDefaultValue(std::monostate{})) {
	}

	SchemaField(const char *fieldName, FieldType fieldType, const char *defaultString, bool uniqueFlag)
	    : name(fieldName), type(fieldType), unique(uniqueFlag),
	      hasDefault(defaultString != nullptr),
	      defaultValue(defaultString ? JsonDefaultValue(std::string(defaultString))
	                                 : JsonDefaultValue(std::monostate{})) {
	}

	SchemaField(const char *fieldName, FieldType fieldType, JsonDefaultValue value, bool uniqueFlag = false)
	    : name(fieldName), type(fieldType), unique(uniqueFlag), hasDefault(true),
	      defaultValue(std::move(value)) {
	}
};

inline bool schemaFieldTypeMatches(JsonVariantConst value, FieldType type) {
	switch (type) {
	case FieldType::String:
		return value.is<const char *>() || value.is<std::string>() || value.is<String>();
	case FieldType::Int32:
		return value.is<int32_t>() || value.is<int>();
	case FieldType::Int64:
		return value.is<int64_t>();
	case FieldType::UInt32:
		return value.is<uint32_t>();
	case FieldType::UInt64:
		return value.is<uint64_t>();
	case FieldType::Float:
		return value.is<float>();
	case FieldType::Double:
		return value.is<double>() || value.is<float>();
	case FieldType::Bool:
		return value.is<bool>();
	case FieldType::Object:
		return value.is<JsonObjectConst>();
	case FieldType::Array:
		return value.is<JsonArrayConst>();
	}
	return false;
}

inline void schemaApplyDefaultValue(JsonObject obj, const SchemaField &field) {
	if (!field.name || !field.hasDefault)
		return;

	if (std::holds_alternative<std::string>(field.defaultValue)) {
		obj[field.name] = std::get<std::string>(field.defaultValue).c_str();
		return;
	}
	if (std::holds_alternative<int32_t>(field.defaultValue)) {
		obj[field.name] = std::get<int32_t>(field.defaultValue);
		return;
	}
	if (std::holds_alternative<int64_t>(field.defaultValue)) {
		obj[field.name] = std::get<int64_t>(field.defaultValue);
		return;
	}
	if (std::holds_alternative<uint32_t>(field.defaultValue)) {
		obj[field.name] = std::get<uint32_t>(field.defaultValue);
		return;
	}
	if (std::holds_alternative<uint64_t>(field.defaultValue)) {
		obj[field.name] = std::get<uint64_t>(field.defaultValue);
		return;
	}
	if (std::holds_alternative<float>(field.defaultValue)) {
		obj[field.name] = std::get<float>(field.defaultValue);
		return;
	}
	if (std::holds_alternative<double>(field.defaultValue)) {
		obj[field.name] = std::get<double>(field.defaultValue);
		return;
	}
	if (std::holds_alternative<bool>(field.defaultValue)) {
		obj[field.name] = std::get<bool>(field.defaultValue);
		return;
	}
	if (std::holds_alternative<EmptyObjectTag>(field.defaultValue)) {
		obj[field.name].to<JsonObject>();
		return;
	}
	if (std::holds_alternative<EmptyArrayTag>(field.defaultValue)) {
		obj[field.name].to<JsonArray>();
	}
}

struct Schema {
	std::vector<SchemaField> fields;
	PreSaveFn preSave{};
	PostLoadFn postLoad{};
	ValidateFn validate{};

	Schema() = default;

	inline bool hasValidate() const {
		return validate != nullptr || preSave != nullptr || !fields.empty();
	}

	inline void applyDefaults(JsonObject obj) const {
		for (const auto &field : fields) {
			JsonVariant value = obj[field.name];
			if (value.isNull() && field.hasDefault) {
				schemaApplyDefaultValue(obj, field);
			}
		}
	}

	inline ValidationError validateFields(JsonObjectConst obj) const {
		for (const auto &field : fields) {
			if (!field.name || !*field.name)
				continue;
			JsonVariantConst value = obj[field.name];
			if (value.isNull()) {
				if (field.required) {
					return {false, "schema: required field missing"};
				}
				continue;
			}
			if (!schemaFieldTypeMatches(value, field.type)) {
				return {false, "schema: invalid type"};
			}
		}
		return {true, ""};
	}

	inline ValidationError runPreSave(JsonObject &o) const {
		applyDefaults(o);
		auto fieldStatus = validateFields(o);
		if (!fieldStatus.valid)
			return fieldStatus;
		if (preSave)
			return preSave(o);
		if (validate)
			return validate(o);
		return {true, ""};
	}

	inline ValidationError runValidate(const JsonObjectConst &o) const {
		auto fieldStatus = validateFields(o);
		if (!fieldStatus.valid)
			return fieldStatus;
		if (validate)
			return validate(o);
		return {true, ""};
	}

	inline void runPostLoad(JsonObject &o) const {
		if (postLoad)
			postLoad(o);
	}
};
