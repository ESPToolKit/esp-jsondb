#include "document.h"
#include "../db.h"
#include "../utils/refs.h"
#include "../utils/time_utils.h"
#include <utility>

DocView::DocView(DocumentRecord *rec, const Schema *schema, FrMutex *mu)
	: _rec(rec), _schema(schema), _mu(mu) {}

DocView::~DocView() {
	// no auto-commit by default; discard decoded state
}

DbStatus DocView::decode() {
	std::unique_ptr<FrLock> guard;
	if (_mu) guard = std::make_unique<FrLock>(*_mu);
	if (_doc) return dbSetLastError({DbStatusCode::Ok, ""});
	_doc = std::make_unique<JsonDocument>();
	// If there is no backing record (e.g., NotFound), treat as empty object
	if (_rec == nullptr) {
		_doc->to<JsonObject>();
		return dbSetLastError({DbStatusCode::Ok, ""});
	}
	DeserializationError err = DeserializationError::Ok;
	if (_rec->msgpack.empty()) {
		// Start with empty object
		_doc->to<JsonObject>();
	} else {
		err = deserializeMsgPack(*_doc, _rec->msgpack.data(), _rec->msgpack.size());
		if (err) {
			_doc.reset();
			return dbSetLastError({DbStatusCode::Corrupted, "msgpack decode failed"});
		}
	}
	if (_schema) {
		auto obj = _doc->as<JsonObject>();
		_schema->runPostLoad(obj);
	}
	return dbSetLastError({DbStatusCode::Ok, ""});
}

DbStatus DocView::encode() {
    std::unique_ptr<FrLock> guard;
    if (_mu) guard = std::make_unique<FrLock>(*_mu);
    if (!_doc) return dbSetLastError({DbStatusCode::InvalidArgument, "no decoded doc"});
    if (_rec == nullptr) return dbSetLastError({DbStatusCode::InvalidArgument, "no backing record"});
    // measure and serialize to vector buffer
    size_t sz = measureMsgPack(_doc->as<JsonVariantConst>());
    _rec->msgpack.resize(sz);
	size_t written = serializeMsgPack(_doc->as<JsonVariantConst>(), _rec->msgpack.data(), _rec->msgpack.size());
	if (written != sz) {
		return dbSetLastError({DbStatusCode::IoError, "serialize msgpack size mismatch"});
	}
	_rec->meta.updatedAt = nowUtcMs();
	_rec->meta.dirty = true;
	_dirtyLocally = false;
	return dbSetLastError({DbStatusCode::Ok, ""});
}

JsonVariant DocView::operator[](const char *key) {
	if (!decode().ok()) return JsonVariant();
	_dirtyLocally = true;
	return (*_doc)[key];
}

JsonVariant DocView::operator[](const String &key) {
	if (!decode().ok()) return JsonVariant();
	_dirtyLocally = true;
	return (*_doc)[key];
}

JsonVariant DocView::operator[](int index) {
	if (!decode().ok()) return JsonVariant();
	_dirtyLocally = true;
	return (*_doc)[index];
}

JsonVariantConst DocView::operator[](const char *key) const {
	if (!_doc) {
		auto self = const_cast<DocView *>(this);
		if (!self->decode().ok()) return JsonVariantConst();
	}
	return _doc->as<JsonVariantConst>()[key];
}

JsonVariantConst DocView::operator[](const String &key) const {
	if (!_doc) {
		auto self = const_cast<DocView *>(this);
		if (!self->decode().ok()) return JsonVariantConst();
	}
	return _doc->as<JsonVariantConst>()[key];
}

JsonVariantConst DocView::operator[](int index) const {
	if (!_doc) {
		auto self = const_cast<DocView *>(this);
		if (!self->decode().ok()) return JsonVariantConst();
	}
	return _doc->as<JsonVariantConst>()[index];
}

JsonObject DocView::asObject() {
	if (!decode().ok()) return JsonObject();
	return _doc->as<JsonObject>();
}

JsonObjectConst DocView::asObjectConst() const {
	if (!_doc) {
		// Need to const_cast to decode lazily
		auto self = const_cast<DocView *>(this);
		if (!self->decode().ok()) return JsonObjectConst();
	}
	return _doc->as<JsonObjectConst>();
}

DbStatus DocView::commit() {
	if (!_doc) return dbSetLastError({DbStatusCode::Ok, "no changes"});
	return encode();
}

void DocView::discard() {
	_doc.reset();
	_dirtyLocally = false;
}

DocRef DocView::getRef(const char *field) const {
	if (!_doc) {
		auto self = const_cast<DocView *>(this);
		if (!self->decode().ok()) return {};
	}
	return docRefFromJson(_doc->as<JsonVariantConst>()[field]);
}

DocView DocView::populate(const char *field, uint8_t maxDepth) const {
	if (maxDepth == 0) {
		dbSetLastError({DbStatusCode::InvalidArgument, "max depth reached"});
		return DocView(nullptr);
	}
	auto ref = getRef(field);
	if (!ref.valid()) {
		dbSetLastError({DbStatusCode::InvalidArgument, "field not DocRef"});
		return DocView(nullptr);
	}
	auto fr = db.findById(ref.collection, ref.id);
	if (!fr.status.ok()) return DocView(nullptr);
	if (maxDepth > 1) {
		for (auto kv : fr.value.asObjectConst()) {
			auto nested = docRefFromJson(kv.value());
			if (nested.valid()) {
				fr.value.populate(kv.key().c_str(), maxDepth - 1);
			}
		}
	}
	return std::move(fr.value);
}
