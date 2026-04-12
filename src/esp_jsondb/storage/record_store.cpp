#include "record_store.h"

#include <StreamUtils.h>

#include <cstring>

#include "../storage/doc_codec.h"
#include "../utils/fr_mutex.h"
#include "../utils/fs_utils.h"
#include "../utils/jsondb_allocator.h"

namespace {
std::string recordPathFor(const std::string &collectionDir, const std::string &id) {
	return joinPath(collectionDir, id + DocCodec::kRecordExtension);
}
} // namespace

DbStatus RecordStore::write(const std::string &collectionDir, const DocumentRecord &record) {
	if (!_fs) {
		return {DbStatusCode::IoError, "filesystem not ready"};
	}
	if (!record.meta.id.valid()) {
		return {DbStatusCode::InvalidArgument, "record id is invalid"};
	}

	JsonDbVector<uint8_t> encoded{JsonDbAllocator<uint8_t>(_usePSRAMBuffers)};
	RecordHeader header;
	header.id = record.meta.id;
	header.createdAtMs = record.meta.createdAtMs;
	header.updatedAtMs = record.meta.updatedAtMs;
	header.revision = record.meta.revision;
	header.flags = record.meta.flags;
	auto encodeStatus = DocCodec::encodeRecord(header, record.msgpack, encoded);
	if (!encodeStatus.ok())
		return encodeStatus;

	const std::string finalPath = recordPathFor(collectionDir, record.meta.id.c_str());
	const std::string tmpPath = finalPath + ".tmp";

	FrLock fs(g_fsMutex);
	if (!fsEnsureDir(*_fs, collectionDir)) {
		return {DbStatusCode::IoError, "mkdir failed"};
	}
	File file = _fs->open(tmpPath.c_str(), FILE_WRITE);
	if (!file) {
		return {DbStatusCode::IoError, "open for write failed"};
	}
	WriteBufferingStream buffered(file, 256);
	const size_t written = buffered.write(encoded.data(), encoded.size());
	buffered.flush();
	file.close();
	if (written != encoded.size()) {
		_fs->remove(tmpPath.c_str());
		return {DbStatusCode::IoError, "write failed"};
	}
	if (_fs->exists(finalPath.c_str()) && !_fs->remove(finalPath.c_str())) {
		_fs->remove(tmpPath.c_str());
		return {DbStatusCode::IoError, "replace old record failed"};
	}
	if (!_fs->rename(tmpPath.c_str(), finalPath.c_str())) {
		_fs->remove(tmpPath.c_str());
		return {DbStatusCode::IoError, "rename failed"};
	}
	return {DbStatusCode::Ok, ""};
}

DbResult<std::shared_ptr<DocumentRecord>>
RecordStore::read(const std::string &collectionDir, const std::string &id) const {
	DbResult<std::shared_ptr<DocumentRecord>> result{};
	if (!_fs) {
		result.status = {DbStatusCode::IoError, "filesystem not ready"};
		return result;
	}

	const std::string path = recordPathFor(collectionDir, id);
	JsonDbVector<uint8_t> encoded{JsonDbAllocator<uint8_t>(_usePSRAMBuffers)};
	{
		FrLock fs(g_fsMutex);
		File file = _fs->open(path.c_str(), FILE_READ);
		if (!file) {
			result.status = {DbStatusCode::NotFound, "file not found"};
			return result;
		}
		const size_t size = file.size();
		encoded.resize(size);
		const size_t readSize = file.read(encoded.data(), size);
		file.close();
		if (readSize != size) {
			result.status = {DbStatusCode::IoError, "read failed"};
			return result;
		}
	}

	auto record = std::allocate_shared<DocumentRecord>(
	    JsonDbAllocator<DocumentRecord>(_usePSRAMBuffers),
	    _usePSRAMBuffers
	);
	RecordHeader header;
	auto decodeStatus = DocCodec::decodeRecord(
	    encoded.data(),
	    encoded.size(),
	    header,
	    record->msgpack,
	    _usePSRAMBuffers
	);
	if (!decodeStatus.ok()) {
		result.status = decodeStatus;
		return result;
	}
	record->meta.id = header.id;
	record->meta.createdAtMs = header.createdAtMs;
	record->meta.updatedAtMs = header.updatedAtMs;
	record->meta.revision = header.revision;
	record->meta.flags = header.flags;
	record->meta.dirty = false;
	record->meta.removed = false;
	result.status = {DbStatusCode::Ok, ""};
	result.value = std::move(record);
	return result;
}

JsonDbVector<DocId> RecordStore::listIds(const std::string &collectionDir) const {
	JsonDbVector<DocId> ids{JsonDbAllocator<DocId>(_usePSRAMBuffers)};
	if (!_fs)
		return ids;

	FrLock fs(g_fsMutex);
	if (!_fs->exists(collectionDir.c_str()))
		return ids;
	File dir = _fs->open(collectionDir.c_str());
	if (!dir || !dir.isDirectory()) {
		if (dir)
			dir.close();
		return ids;
	}
	for (File file = dir.openNextFile(); file; file = dir.openNextFile()) {
		if (file.isDirectory()) {
			file.close();
			continue;
		}
		String rawName = file.name();
		file.close();
		std::string name = rawName.c_str();
		const auto slash = name.find_last_of('/');
		if (slash != std::string::npos)
			name = name.substr(slash + 1);
		if (name.size() <= std::strlen(DocCodec::kRecordExtension))
			continue;
		if (name.substr(name.size() - std::strlen(DocCodec::kRecordExtension)) !=
		    DocCodec::kRecordExtension)
			continue;
		DocId parsed;
		if (parsed.assign(name.substr(0, name.size() - std::strlen(DocCodec::kRecordExtension)))) {
			ids.push_back(parsed);
		}
	}
	dir.close();
	return ids;
}

DbStatus RecordStore::remove(const std::string &collectionDir, const DocId &id) const {
	if (!_fs) {
		return {DbStatusCode::IoError, "filesystem not ready"};
	}
	const std::string path = recordPathFor(collectionDir, id.c_str());
	FrLock fs(g_fsMutex);
	if (!_fs->exists(path.c_str())) {
		return {DbStatusCode::NotFound, "file not found"};
	}
	if (!_fs->remove(path.c_str())) {
		return {DbStatusCode::IoError, "remove failed"};
	}
	return {DbStatusCode::Ok, ""};
}
