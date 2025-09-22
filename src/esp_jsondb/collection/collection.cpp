#include "collection.h"
#include "../db.h"
#include "../utils/fs_utils.h"
#include "../utils/time_utils.h"

Collection::Collection(const std::string &name, const Schema &schema, std::string baseDir, bool cacheEnabled)
    : _name(name), _schema(schema), _baseDir(std::move(baseDir)), _cacheEnabled(cacheEnabled) {}

void Collection::setCacheEnabled(bool enabled) {
	if (_cacheEnabled == enabled) return;
	if (!enabled) {
		bool didWork = false;
		auto st = flushDirtyToFs(_baseDir, didWork);
		if (!st.ok()) {
			return;
		}
		{
			FrLock lk(_mu);
			_docs.clear();
			_deletedIds.clear();
			_dirty = false;
		}
	}
	_cacheEnabled = enabled;
}

DbStatus Collection::checkUniqueFieldsInCache(JsonObjectConst obj, const std::string &selfId) {
    // Scan schema for fields marked unique and ensure no other doc has same value
    for (const auto &f : _schema.fields) {
        if (!f.unique) continue;
        // Only enforce on scalar types
        if (f.type == FieldType::Object || f.type == FieldType::Array) continue;
        JsonVariantConst v = obj[f.name];
        if (v.isNull()) continue;
        for (const auto &kv : _docs) {
            if (!selfId.empty() && kv.first == selfId) continue;
            DocView other(kv.second, &_schema);
            JsonVariantConst ov = other[f.name];
            if (!ov.isNull() && ov == v) {
                return dbSetLastError({DbStatusCode::ValidationFailed, "unique constraint violated"});
            }
        }
    }
    return dbSetLastError({DbStatusCode::Ok, ""});
}

DbStatus Collection::checkUniqueFieldsOnDisk(JsonObjectConst obj, const std::string &selfId) {
	bool hasUnique = false;
	for (const auto &field : _schema.fields) {
		if (field.unique) {
			hasUnique = true;
			break;
		}
	}
	if (!hasUnique) {
		return dbSetLastError({DbStatusCode::Ok, ""});
	}
	// Enumerate all documents on disk and compare declared unique fields
	std::vector<std::string> ids;
	std::string dir = joinPath(_baseDir, _name);
	{
		FrLock fs(g_fsMutex);
		if (LittleFS.exists(dir.c_str())) {
			File d = LittleFS.open(dir.c_str());
			if (!d || !d.isDirectory()) {
				return dbSetLastError({DbStatusCode::IoError, "open dir failed"});
			}
			for (File f = d.openNextFile(); f; f = d.openNextFile()) {
				if (f.isDirectory()) continue;
				String name = f.name();
				std::string fname = name.c_str();
				if (fname.size() < 3 || fname.substr(fname.size() - 3) != ".mp") continue;
				ids.push_back(fname.substr(0, fname.size() - 3));
			}
		}
	}
	for (const auto &docId : ids) {
		if (!selfId.empty() && docId == selfId) continue;
		auto rr = readDocFromFile(_baseDir, docId);
		if (!rr.status.ok()) {
			return rr.status;
		}
		DocView view(rr.value, &_schema, nullptr);
		for (const auto &field : _schema.fields) {
			if (!field.unique) continue;
			if (field.type == FieldType::Object || field.type == FieldType::Array) continue;
			JsonVariantConst newVal = obj[field.name];
			if (newVal.isNull()) continue;
			JsonVariantConst existingVal = view[field.name.c_str()];
			if (!existingVal.isNull() && existingVal == newVal) {
				return dbSetLastError({DbStatusCode::ValidationFailed, "unique constraint violated"});
			}
		}
	}
	return dbSetLastError({DbStatusCode::Ok, ""});
}

DbStatus Collection::checkUniqueFields(JsonObjectConst obj, const std::string &selfId) {
	if (_cacheEnabled) {
		return checkUniqueFieldsInCache(obj, selfId);
	}
	return checkUniqueFieldsOnDisk(obj, selfId);
}

DbResult<std::string> Collection::create(JsonObjectConst data) {
    DbResult<std::string> res{};
    JsonDocument workDoc;
    workDoc.set(data);
    JsonObject obj = workDoc.as<JsonObject>();
    if (_schema.hasValidate()) {
        auto ve = _schema.runPreSave(obj);
        if (!ve.valid) {
            res.status = {DbStatusCode::ValidationFailed, ve.message};
            dbSetLastError(res.status);
            return res;
        }
    }
    bool emit = false;
    std::shared_ptr<DocumentRecord> rec;
    std::string id;
    {
        FrLock lk(_mu);
        // Enforce unique constraints before creating the record
        auto ust = checkUniqueFields(obj, "");
        if (!ust.ok()) {
            res.status = ust;
            dbSetLastError(res.status);
            return res;
        }
		rec = std::make_shared<DocumentRecord>();
		rec->meta.createdAt = nowUtcMs();
		rec->meta.updatedAt = rec->meta.createdAt;
		rec->meta.id = ObjectId().toHex();
		rec->meta.dirty = true;

		// Serialize input data to MsgPack
		size_t sz = measureMsgPack(obj);
		rec->msgpack.resize(sz);
		size_t written = serializeMsgPack(obj, rec->msgpack.data(), rec->msgpack.size());
		if (written != sz) {
			res.status = {DbStatusCode::IoError, "serialize msgpack failed"};
			dbSetLastError(res.status);
			return res;
		}

		id = rec->meta.id;
		if (_cacheEnabled) {
			_docs.emplace(id, rec);
			_dirty = true;
		}

		res.status = {DbStatusCode::Ok, ""};
		dbSetLastError(res.status);
		res.value = id;
		emit = true;
    }
	if (!_cacheEnabled) {
		auto st = persistImmediate(rec);
		if (!st.ok()) {
			res.status = st;
			res.value.clear();
			return res;
		}
	}
	if (emit) db.emitEvent(DBEventType::DocumentCreated);
	return res;
}

DbResult<std::string> Collection::create(const JsonDocument &data) {
	// Ensure provided document is an object
	if (!data.is<JsonObject>()) {
		DbResult<std::string> res{};
		res.status = dbSetLastError({DbStatusCode::InvalidArgument, "document must be an object"});
		return res;
	}
	return create(data.as<JsonObjectConst>());
}

DbResult<std::vector<std::string>> Collection::createMany(JsonArrayConst arr) {
	DbResult<std::vector<std::string>> res{};
	std::vector<std::string> ids;
	ids.reserve(arr.size());

	for (auto v : arr) {
		if (!v.is<JsonObjectConst>()) {
			// Skip non-object entries
			continue;
		}
		auto cr = create(v.as<JsonObjectConst>());
		if (cr.status.ok()) {
			ids.push_back(cr.value);
		}
	}

	res.status = {DbStatusCode::Ok, ""};
	dbSetLastError(res.status);
	res.value = std::move(ids);
	return res;
}

DbResult<std::vector<std::string>> Collection::createMany(const JsonDocument &arrDoc) {
	if (!arrDoc.is<JsonArray>()) {
		DbResult<std::vector<std::string>> res{};
		res.status = dbSetLastError({DbStatusCode::InvalidArgument, "document must be an array of objects"});
		return res;
	}
	return createMany(arrDoc.as<JsonArrayConst>());
}

DbResult<DocView> Collection::findById(const std::string &id) {
    {
        FrLock lk(_mu);
        auto it = _docs.find(id);
        if (it != _docs.end()) {
            DbStatus st{DbStatusCode::Ok, ""};
            dbSetLastError(st);
            return {st, makeView(it->second)};
        }
    }

    auto rr = readDocFromFile(_baseDir, id);
    if (!rr.status.ok()) {
        return {rr.status, DocView(nullptr, &_schema, &_mu)};
    }

    if (_cacheEnabled) {
        FrLock lk(_mu);
        _docs.emplace(id, rr.value);
    }

    DbStatus st{DbStatusCode::Ok, ""};
    dbSetLastError(st);
    return {st, makeView(std::move(rr.value))};
}

DbResult<std::vector<DocView>> Collection::findMany(std::function<bool(const DocView &)> pred) {
    DbResult<std::vector<DocView>> res{};
    if (_cacheEnabled) {
        FrLock lk(_mu);
        for (auto &kv : _docs) {
            DocView v(kv.second, &_schema, nullptr);
            if (!pred || pred(v)) {
                res.value.emplace_back(makeView(kv.second));
            }
        }
    } else {
        auto ids = listDocumentIdsFromFs();
        for (const auto &id : ids) {
            auto rr = readDocFromFile(_baseDir, id);
            if (!rr.status.ok()) continue;
            auto view = makeView(rr.value);
            if (!pred || pred(view)) {
                res.value.emplace_back(std::move(view));
            }
        }
    }
    res.status = {DbStatusCode::Ok, ""};
    dbSetLastError(res.status);
    return res;
}

DbResult<DocView> Collection::findOne(std::function<bool(const DocView &)> pred) {
    // Iterate over in-memory docs and return the first matching view
    if (_cacheEnabled) {
        FrLock lk(_mu);
        for (auto &kv : _docs) {
            DocView v(kv.second, &_schema, nullptr);
            if (!pred || pred(v)) {
                DbStatus st{DbStatusCode::Ok, ""};
                dbSetLastError(st);
                return {st, makeView(kv.second)};
            }
        }
    } else {
        auto ids = listDocumentIdsFromFs();
        for (const auto &id : ids) {
            auto rr = readDocFromFile(_baseDir, id);
            if (!rr.status.ok()) continue;
            auto view = makeView(rr.value);
            if (!pred || pred(view)) {
                DbStatus st{DbStatusCode::Ok, ""};
                dbSetLastError(st);
                return {st, std::move(view)};
            }
        }
    }
    DbStatus st{DbStatusCode::NotFound, "document not found"};
    dbSetLastError(st);
    return {st, DocView(nullptr, &_schema, &_mu)};
}

DbResult<DocView> Collection::findOne(const JsonDocument &filter) {
	// Build an inline predicate from the filter object
	auto pred = [&](const DocView &v) {
		for (auto kv : filter.as<JsonObjectConst>()) {
			if (v[kv.key().c_str()] != kv.value()) {
				return false;
			}
		}
		return true;
	};
	return findOne(std::move(pred));
}

DbStatus Collection::updateOne(std::function<bool(const DocView &)> pred,
							   std::function<void(DocView &)> mutator,
							   bool create) {
    bool updated = false;
    bool created = false;
    DbStatus st{DbStatusCode::NotFound, "document not found"};
	if (!_cacheEnabled) {
		st = updateOneNoCache(std::move(pred), std::move(mutator), create, created, updated);
	} else {
		FrLock lk(_mu);
		// Search existing docs first
		for (auto &kv : _docs) {
			DocView v(kv.second, &_schema, nullptr); // use outer lock
			if (!pred || pred(v)) {
				mutator(v);
				if (_schema.hasValidate()) {
					auto obj = v.asObject();
					auto ve = _schema.runPreSave(obj);
					if (!ve.valid) {
						v.discard();
						return dbSetLastError({DbStatusCode::ValidationFailed, ve.message});
					}
					// Unique constraints
					auto ust = checkUniqueFields(obj, kv.second->meta.id);
					if (!ust.ok()) {
						v.discard();
						return dbSetLastError(ust);
					}
				}
				st = v.commit();
				if (!st.ok()) return dbSetLastError(st);
				// Only flag collection and emit update if record actually changed
				if (kv.second->meta.dirty) {
					_dirty = true;
					updated = true;
				}
				break;
			}
		}

		// If not found and create requested, create a new record and apply mutator
		if (!updated && create) {
			auto rec = std::make_shared<DocumentRecord>();
			rec->meta.createdAt = nowUtcMs();
			rec->meta.updatedAt = rec->meta.createdAt;
			rec->meta.id = ObjectId().toHex();
			rec->meta.dirty = true;

			DocView v(rec, &_schema, nullptr);
			// Initialize as empty object then let mutator fill values
			v.asObject();
			mutator(v);
			if (_schema.hasValidate()) {
				auto obj = v.asObject();
				auto ve = _schema.runPreSave(obj);
				if (!ve.valid) {
					v.discard();
					return dbSetLastError({DbStatusCode::ValidationFailed, ve.message});
				}
				// Unique constraints
				auto ust = checkUniqueFields(obj, rec->meta.id);
				if (!ust.ok()) {
					v.discard();
					return dbSetLastError(ust);
				}
			}
			st = v.commit();
			if (!st.ok()) return dbSetLastError(st);
			const std::string id = rec->meta.id;
			_docs.emplace(id, std::move(rec));
			_dirty = true;
			created = true;
			st = {DbStatusCode::Ok, ""};
		}
	}
	if (created)
		db.emitEvent(DBEventType::DocumentCreated);
	else if (updated)
		db.emitEvent(DBEventType::DocumentUpdated);
	return dbSetLastError(st);
}

DbStatus Collection::updateOne(const JsonDocument &filter,
							   const JsonDocument &patch,
							   bool create) {
	bool updated = false;
	bool created = false;
	DbStatus st{DbStatusCode::NotFound, "document not found"};
	if (!_cacheEnabled) {
		st = updateOneJsonNoCache(filter, patch, create, created, updated);
	} else {
		FrLock lk(_mu);
		// Look for first matching doc
        for (auto &kv : _docs) {
            DocView v(kv.second, &_schema, nullptr);
            bool match = true;
            for (auto kvf : filter.as<JsonObjectConst>()) {
                if (v[kvf.key().c_str()] != kvf.value()) {
                    match = false;
                    break;
                }
            }
            if (match) {
                for (auto kvp : patch.as<JsonObjectConst>()) {
                    v[kvp.key().c_str()].set(kvp.value());
                }
                if (_schema.hasValidate()) {
                    auto obj = v.asObject();
                    auto ve = _schema.runPreSave(obj);
                    if (!ve.valid) {
                        v.discard();
                        return dbSetLastError({DbStatusCode::ValidationFailed, ve.message});
                    }
                    // Unique constraints
                    auto ust = checkUniqueFields(obj, kv.second->meta.id);
                    if (!ust.ok()) {
                        v.discard();
                        return dbSetLastError(ust);
                    }
                }
                st = v.commit();
                if (!st.ok()) return dbSetLastError(st);
                if (kv.second->meta.dirty) {
                    _dirty = true;
                    updated = true;
                }
                break;
            }
        }

		if (!updated && create) {
            // Create a new document merging filter and patch
            auto rec = std::make_shared<DocumentRecord>();
            rec->meta.createdAt = nowUtcMs();
            rec->meta.updatedAt = rec->meta.createdAt;
            rec->meta.id = ObjectId().toHex();
            rec->meta.dirty = true;

            DocView v(rec, &_schema, nullptr);
            auto obj = v.asObject();
            for (auto kvf : filter.as<JsonObjectConst>()) {
                obj[kvf.key().c_str()] = kvf.value();
            }
            for (auto kvp : patch.as<JsonObjectConst>()) {
                obj[kvp.key().c_str()] = kvp.value();
            }
            if (_schema.hasValidate()) {
                auto ve = _schema.runPreSave(obj);
                if (!ve.valid) {
                    v.discard();
                    return dbSetLastError({DbStatusCode::ValidationFailed, ve.message});
                }
                // Unique constraints
                auto ust = checkUniqueFields(obj, rec->meta.id);
                if (!ust.ok()) {
                    v.discard();
                    return dbSetLastError(ust);
                }
            }
            st = v.commit();
            if (!st.ok()) return dbSetLastError(st);
            const std::string id = rec->meta.id;
            _docs.emplace(id, std::move(rec));
            _dirty = true;
            created = true;
            st = {DbStatusCode::Ok, ""};
        }
	}
	if (created)
		db.emitEvent(DBEventType::DocumentCreated);
	else if (updated)
		db.emitEvent(DBEventType::DocumentUpdated);
	return dbSetLastError(st);
}

DbStatus Collection::updateById(const std::string &id, std::function<void(DocView &)> mutator) {
    bool updated = false;
    DbStatus st{DbStatusCode::Ok, ""};
    if (!_cacheEnabled) {
        st = updateByIdNoCache(id, std::move(mutator), updated);
    } else {
        FrLock lk(_mu);
        auto it = _docs.find(id);
        if (it == _docs.end()) {
            return dbSetLastError({DbStatusCode::NotFound, "document not found"});
        }
        DocView v(it->second, &_schema, nullptr); // using outer lock
        mutator(v);
        if (_schema.hasValidate()) {
            auto obj = v.asObject();
            auto ve = _schema.runPreSave(obj);
            if (!ve.valid) {
                v.discard();
                return dbSetLastError({DbStatusCode::ValidationFailed, ve.message});
            }
            // Unique constraints
            auto ust = checkUniqueFields(obj, it->second->meta.id);
            if (!ust.ok()) {
                v.discard();
                return dbSetLastError(ust);
            }
        }
        st = v.commit();
        if (!st.ok()) return dbSetLastError(st);
        if (it->second->meta.dirty) {
            _dirty = true;
            updated = true;
        }
    }
	if (updated) db.emitEvent(DBEventType::DocumentUpdated);
	return dbSetLastError(st);
}

DbStatus Collection::removeById(const std::string &id) {
    bool removed = false;
    DbStatus st{DbStatusCode::Ok, ""};
    if (!_cacheEnabled) {
        st = removeByIdNoCache(id, removed);
    } else {
        FrLock lk(_mu);
        auto it = _docs.find(id);
        if (it == _docs.end()) return dbSetLastError({DbStatusCode::NotFound, "document not found"});
        // Mark record as logically removed so outstanding views fail on commit
        it->second->meta.removed = true;
        _deletedIds.push_back(id); // ensure file removal on sync
        _docs.erase(it);
        _dirty = true;
        removed = true;
    }
	if (removed) db.emitEvent(DBEventType::DocumentDeleted);
	return dbSetLastError(st);
}

DbStatus Collection::writeDocToFile(const std::string &baseDir, const DocumentRecord &r) {
    FrLock fs(g_fsMutex);
	std::string dir = joinPath(baseDir, _name);
	if (!fsEnsureDir(dir)) {
		return dbSetLastError({DbStatusCode::IoError, "mkdir failed"});
	}
	std::string finalPath = joinPath(dir, r.meta.id + ".mp");
	std::string tmpPath = finalPath + ".tmp";
	// Write to temp then rename for atomicity
	File f = LittleFS.open(tmpPath.c_str(), FILE_WRITE);
	if (!f) return {DbStatusCode::IoError, "open for write failed"};
	// Buffer writes to coalesce small chunks if any
	WriteBufferingStream bufferedFile(f, 256);
	size_t w = bufferedFile.write(r.msgpack.data(), r.msgpack.size());
	bufferedFile.flush();
	f.close();
	if (w != r.msgpack.size()) {
		LittleFS.remove(tmpPath.c_str());
		return dbSetLastError({DbStatusCode::IoError, "write failed"});
	}
	if (!LittleFS.rename(tmpPath.c_str(), finalPath.c_str())) {
		LittleFS.remove(tmpPath.c_str());
		return dbSetLastError({DbStatusCode::IoError, "rename failed"});
	}
	return dbSetLastError({DbStatusCode::Ok, ""});
}

DbResult<std::shared_ptr<DocumentRecord>> Collection::readDocFromFile(const std::string &baseDir, const std::string &id) {
    DbResult<std::shared_ptr<DocumentRecord>> res{};
    std::string path = joinPath(joinPath(baseDir, _name), id + ".mp");
    FrLock fs(g_fsMutex);
    File f = LittleFS.open(path.c_str(), FILE_READ);
    if (!f) {
        res.status = {DbStatusCode::NotFound, "file not found"};
        dbSetLastError(res.status);
        return res;
    }
    auto rec = std::make_shared<DocumentRecord>();
    rec->meta.id = id;
    rec->meta.createdAt = nowUtcMs();
    rec->meta.updatedAt = rec->meta.createdAt;
    rec->meta.dirty = false;

	size_t sz = f.size();
	rec->msgpack.resize(sz);
	size_t r = f.read(rec->msgpack.data(), sz);
	f.close();
	if (r != sz) {
		res.status = {DbStatusCode::IoError, "read failed"};
		dbSetLastError(res.status);
		return res;
	}
    res.status = {DbStatusCode::Ok, ""};
    dbSetLastError(res.status);
    res.value = std::move(rec);
    return res;
}

std::vector<std::string> Collection::listDocumentIdsFromFs() const {
	std::vector<std::string> ids;
	std::string dir = joinPath(_baseDir, _name);
	{
		FrLock fs(g_fsMutex);
		if (!LittleFS.exists(dir.c_str())) return ids;
		File d = LittleFS.open(dir.c_str());
		if (!d || !d.isDirectory()) return ids;
		for (File f = d.openNextFile(); f; f = d.openNextFile()) {
			if (f.isDirectory()) continue;
			String name = f.name();
			std::string fname = name.c_str();
			if (fname.size() < 3 || fname.substr(fname.size() - 3) != ".mp") continue;
			ids.push_back(fname.substr(0, fname.size() - 3));
		}
	}
	return ids;
}

size_t Collection::countDocumentsFromFs() const {
	return listDocumentIdsFromFs().size();
}

DbStatus Collection::persistImmediate(const std::shared_ptr<DocumentRecord> &rec) {
	if (!rec) {
		return dbSetLastError({DbStatusCode::InvalidArgument, "no record"});
	}
	auto st = writeDocToFile(_baseDir, *rec);
	if (!st.ok()) return st;
	FrLock lk(_mu);
	rec->meta.dirty = false;
	rec->meta.removed = false;
	_dirty = false;
	return dbSetLastError({DbStatusCode::Ok, ""});
}

DocView Collection::makeView(std::shared_ptr<DocumentRecord> rec) {
	if (_cacheEnabled) {
		return DocView(std::move(rec), &_schema, &_mu);
	}
	auto sink = [this](const std::shared_ptr<DocumentRecord> &record) {
		return persistImmediate(record);
	};
	return DocView(std::move(rec), &_schema, &_mu, sink);
}

DbStatus Collection::updateOneNoCache(std::function<bool(const DocView &)> pred,
									  std::function<void(DocView &)> mutator,
									  bool create,
									  bool &created,
									  bool &updated) {
	DbStatus st{DbStatusCode::NotFound, "document not found"};
	auto ids = listDocumentIdsFromFs();
	for (const auto &id : ids) {
		auto rr = readDocFromFile(_baseDir, id);
		if (!rr.status.ok()) {
			st = rr.status;
			continue;
		}
		auto view = makeView(rr.value);
		if (!pred || pred(view)) {
			mutator(view);
			if (_schema.hasValidate()) {
				auto obj = view.asObject();
				auto ve = _schema.runPreSave(obj);
				if (!ve.valid) {
					view.discard();
					return dbSetLastError({DbStatusCode::ValidationFailed, ve.message});
				}
				auto ust = checkUniqueFields(obj, rr.value->meta.id);
				if (!ust.ok()) {
					view.discard();
					return dbSetLastError(ust);
				}
			}
			st = view.commit();
			if (!st.ok()) return dbSetLastError(st);
			updated = true;
			return dbSetLastError(st);
		}
	}
	if (create) {
		auto rec = std::make_shared<DocumentRecord>();
		rec->meta.createdAt = nowUtcMs();
		rec->meta.updatedAt = rec->meta.createdAt;
		rec->meta.id = ObjectId().toHex();
		rec->meta.dirty = true;
		auto view = makeView(rec);
		view.asObject();
		mutator(view);
		if (_schema.hasValidate()) {
			auto obj = view.asObject();
			auto ve = _schema.runPreSave(obj);
			if (!ve.valid) {
				view.discard();
				return dbSetLastError({DbStatusCode::ValidationFailed, ve.message});
			}
			auto ust = checkUniqueFields(obj, rec->meta.id);
			if (!ust.ok()) {
				view.discard();
				return dbSetLastError(ust);
			}
		}
		st = view.commit();
		if (!st.ok()) return dbSetLastError(st);
		created = true;
		return dbSetLastError(st);
	}
	return dbSetLastError(st);
}

DbStatus Collection::updateOneJsonNoCache(const JsonDocument &filter,
										  const JsonDocument &patch,
										  bool create,
										  bool &created,
										  bool &updated) {
	DbStatus st{DbStatusCode::NotFound, "document not found"};
	auto ids = listDocumentIdsFromFs();
	for (const auto &id : ids) {
		auto rr = readDocFromFile(_baseDir, id);
		if (!rr.status.ok()) {
			st = rr.status;
			continue;
		}
		auto view = makeView(rr.value);
		bool match = true;
		for (auto kvf : filter.as<JsonObjectConst>()) {
			if (view[kvf.key().c_str()] != kvf.value()) {
				match = false;
				break;
			}
		}
		if (!match) continue;
		for (auto kvp : patch.as<JsonObjectConst>()) {
			view[kvp.key().c_str()].set(kvp.value());
		}
		if (_schema.hasValidate()) {
			auto obj = view.asObject();
			auto ve = _schema.runPreSave(obj);
			if (!ve.valid) {
				view.discard();
				return dbSetLastError({DbStatusCode::ValidationFailed, ve.message});
			}
			auto ust = checkUniqueFields(obj, rr.value->meta.id);
			if (!ust.ok()) {
				view.discard();
				return dbSetLastError(ust);
			}
		}
		st = view.commit();
		if (!st.ok()) return dbSetLastError(st);
		updated = true;
		return dbSetLastError(st);
	}
	if (create) {
		auto rec = std::make_shared<DocumentRecord>();
		rec->meta.createdAt = nowUtcMs();
		rec->meta.updatedAt = rec->meta.createdAt;
		rec->meta.id = ObjectId().toHex();
		rec->meta.dirty = true;
		auto view = makeView(rec);
		auto obj = view.asObject();
		for (auto kvf : filter.as<JsonObjectConst>()) {
			obj[kvf.key().c_str()] = kvf.value();
		}
		for (auto kvp : patch.as<JsonObjectConst>()) {
			obj[kvp.key().c_str()] = kvp.value();
		}
		if (_schema.hasValidate()) {
			auto ve = _schema.runPreSave(obj);
			if (!ve.valid) {
				view.discard();
				return dbSetLastError({DbStatusCode::ValidationFailed, ve.message});
			}
			auto ust = checkUniqueFields(obj, rec->meta.id);
			if (!ust.ok()) {
				view.discard();
				return dbSetLastError(ust);
			}
		}
		st = view.commit();
		if (!st.ok()) return dbSetLastError(st);
		created = true;
		return dbSetLastError(st);
	}
	return dbSetLastError(st);
}

DbStatus Collection::updateByIdNoCache(const std::string &id,
									  std::function<void(DocView &)> mutator,
									  bool &updated) {
	auto rr = readDocFromFile(_baseDir, id);
	if (!rr.status.ok()) return dbSetLastError(rr.status);
	auto view = makeView(rr.value);
	mutator(view);
	if (_schema.hasValidate()) {
		auto obj = view.asObject();
		auto ve = _schema.runPreSave(obj);
		if (!ve.valid) {
			view.discard();
			return dbSetLastError({DbStatusCode::ValidationFailed, ve.message});
		}
		auto ust = checkUniqueFields(obj, id);
		if (!ust.ok()) {
			view.discard();
			return dbSetLastError(ust);
		}
	}
	auto st = view.commit();
	if (!st.ok()) return dbSetLastError(st);
	updated = true;
	return dbSetLastError(st);
}

DbStatus Collection::removeByIdNoCache(const std::string &id, bool &removed) {
	std::string path = joinPath(joinPath(_baseDir, _name), id + ".mp");
	{
		FrLock fs(g_fsMutex);
		if (!LittleFS.exists(path.c_str())) {
			return dbSetLastError({DbStatusCode::NotFound, "document not found"});
		}
		if (!LittleFS.remove(path.c_str())) {
			return dbSetLastError({DbStatusCode::IoError, "remove failed"});
		}
	}
	removed = true;
	return dbSetLastError({DbStatusCode::Ok, ""});
}

DbStatus Collection::loadFromFs(const std::string &baseDir) {
	if (!_cacheEnabled) {
		return dbSetLastError({DbStatusCode::Ok, ""});
	}
	// First, under FS mutex, collect the list of document IDs to load.
	std::vector<std::string> ids;
	std::string dir = joinPath(baseDir, _name);
	{
		FrLock fs(g_fsMutex);
		if (!LittleFS.exists(dir.c_str())) {
			// Nothing to load yet; create directory lazily on write
			return dbSetLastError({DbStatusCode::Ok, ""});
		}
		File d = LittleFS.open(dir.c_str());
		if (!d || !d.isDirectory()) {
			return dbSetLastError({DbStatusCode::IoError, "open dir failed"});
		}
		for (File f = d.openNextFile(); f; f = d.openNextFile()) {
			if (f.isDirectory()) continue;
			String name = f.name();
			f.close();
			std::string n = name.c_str();
			// Expect <id>.mp
			if (n.size() >= 3 && n.substr(n.size() - 3) == ".mp") {
				ids.push_back(n.substr(0, n.size() - 3));
			}
		}
	}

	// Now, outside FS mutex, read each document file (readDocFromFile acquires FS mutex per file)
    for (const auto &id : ids) {
        auto rr = readDocFromFile(baseDir, id);
        if (rr.status.ok()) {
            _docs.emplace(id, std::move(rr.value));
        }
    }
    return dbSetLastError({DbStatusCode::Ok, ""});
}

DbStatus Collection::flushDirtyToFs(const std::string &baseDir, bool &didWork) {
	didWork = false;
	if (!_cacheEnabled) {
		return dbSetLastError({DbStatusCode::Ok, ""});
	}
	// Snapshot work under lock
	std::vector<std::string> toDelete;
	struct PendingWrite {
		std::string id;
		std::vector<uint8_t> bytes;
	};
	std::vector<PendingWrite> toWrite;
	{
		FrLock lk(_mu);
		toDelete.swap(_deletedIds);
		for (auto &kv : _docs) {
			auto &rec = kv.second;
			if (rec->meta.dirty) {
				toWrite.push_back(PendingWrite{rec->meta.id, rec->msgpack});
				rec->meta.dirty = false;
			}
		}
		_dirty = false;
	}

	// Process deletions (FS serialized by global mutex)
	if (!toDelete.empty()) {
		didWork = true;
		std::string dir = joinPath(baseDir, _name);
		for (const auto &id : toDelete) {
			std::string path = joinPath(dir, id + ".mp");
			{
				FrLock fs(g_fsMutex);
				if (LittleFS.exists(path.c_str())) {
					LittleFS.remove(path.c_str());
				}
			}
		}
	}

	// Flush writes
    for (auto &pw : toWrite) {
        DocumentRecord tmp;
        tmp.meta.id = pw.id;
        tmp.msgpack = std::move(pw.bytes);
        auto st = writeDocToFile(baseDir, tmp);
        if (!st.ok()) return dbSetLastError(st);
        didWork = true;
    }
    return dbSetLastError({DbStatusCode::Ok, ""});
}
