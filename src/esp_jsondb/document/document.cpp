#include "document.h"
#include "../db.h"
#include "../utils/refs.h"
#include "../utils/time_utils.h"
#include <utility>

DocView::DocView(std::shared_ptr<DocumentRecord> rec,
				 const Schema *schema,
				 FrMutex *mu,
				 DataBase *db,
				 std::function<DbStatus(const std::shared_ptr<DocumentRecord>&)> commitSink)
	: _rec(std::move(rec)), _schema(schema), _mu(mu), _db(db), _commitSink(std::move(commitSink)) {}

DocView::~DocView() {
	// no auto-commit by default; discard decoded state
}

DbStatus DocView::decode() {
	std::unique_ptr<FrLock> guard;
	if (_mu) guard = std::make_unique<FrLock>(*_mu);
	if (_doc) return recordStatus({DbStatusCode::Ok, ""});
	_doc = std::make_unique<JsonDocument>();
	// If there is no backing record (e.g., NotFound), treat as empty object
	if (!_rec) {
		_doc->to<JsonObject>();
		return recordStatus({DbStatusCode::Ok, ""});
	}
	DeserializationError err = DeserializationError::Ok;
	if (_rec->msgpack.empty()) {
		// Start with empty object
		_doc->to<JsonObject>();
	} else {
		err = deserializeMsgPack(*_doc, _rec->msgpack.data(), _rec->msgpack.size());
		if (err) {
			_doc.reset();
			return recordStatus({DbStatusCode::Corrupted, "msgpack decode failed"});
		}
	}
	if (_schema) {
		auto obj = _doc->as<JsonObject>();
		_schema->runPostLoad(obj);
	}
	return recordStatus({DbStatusCode::Ok, ""});
}

namespace {
struct CompareToBufferPrint : public Print {
	const uint8_t *ref;
	size_t size;
	size_t index;
	bool equal;
	CompareToBufferPrint(const uint8_t *r, size_t s) : ref(r), size(s), index(0), equal(true) {}
	size_t write(uint8_t b) override {
		if (index >= size) {
			equal = false;
			++index; // still advance to reflect extra bytes
			return 1;
		}
		if (ref[index] != b) equal = false;
		++index;
		return 1;
	}
	size_t write(const uint8_t *buffer, size_t len) override {
		for (size_t i = 0; i < len; ++i)
			write(buffer[i]);
		return len;
	}
};
} // namespace

DbStatus DocView::recordStatus(const DbStatus &st) const {
	return _db ? _db->recordStatus(st) : st;
}

DbStatus DocView::encode() {
    std::unique_ptr<FrLock> guard;
    if (_mu) guard = std::make_unique<FrLock>(*_mu);
    if (!_doc) return recordStatus({DbStatusCode::InvalidArgument, "no decoded doc"});
    if (!_rec) return recordStatus({DbStatusCode::InvalidArgument, "no backing record"});
    if (_rec->meta.removed) return recordStatus({DbStatusCode::NotFound, "document removed"});

	// First, measure the size of the new serialization
	size_t sz = measureMsgPack(_doc->as<JsonVariantConst>());

	// If sizes match, stream-compare bytes without allocating
	if (sz == _rec->msgpack.size()) {
		CompareToBufferPrint cmp(_rec->msgpack.data(), _rec->msgpack.size());
		size_t written = serializeMsgPack(_doc->as<JsonVariantConst>(), cmp);
		if (written != sz) {
			return recordStatus({DbStatusCode::IoError, "serialize msgpack size mismatch"});
		}
		if (cmp.equal) {
			_dirtyLocally = false;
			return recordStatus({DbStatusCode::Ok, ""});
		}
		// else: fall through to write new bytes
	}

	// Allocate and write new bytes
	_rec->msgpack.resize(sz);
	size_t written = serializeMsgPack(_doc->as<JsonVariantConst>(), _rec->msgpack.data(), _rec->msgpack.size());
	if (written != sz) {
		return recordStatus({DbStatusCode::IoError, "serialize msgpack size mismatch"});
	}
    _rec->meta.updatedAt = nowUtcMs();
    _rec->meta.dirty = true;
    _dirtyLocally = false;
    return recordStatus({DbStatusCode::Ok, ""});
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
	if (!_doc) return recordStatus({DbStatusCode::Ok, "no changes"});
	auto st = encode();
	if (!st.ok()) return st;
	if (_commitSink && _rec) {
		st = _commitSink(_rec);
	}
	return st;
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
		recordStatus({DbStatusCode::InvalidArgument, "max depth reached"});
		return DocView(nullptr, nullptr, nullptr, _db);
	}
	auto ref = getRef(field);
	if (!ref.valid()) {
		recordStatus({DbStatusCode::InvalidArgument, "field not DocRef"});
		return DocView(nullptr, nullptr, nullptr, _db);
	}
	if (!_db) {
		recordStatus({DbStatusCode::InvalidArgument, "database context unavailable"});
		return DocView(nullptr, nullptr, nullptr, _db);
	}
	auto fr = _db->findById(ref.collection, ref.id);
	if (!fr.status.ok()) return DocView(nullptr, nullptr, nullptr, _db);
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
