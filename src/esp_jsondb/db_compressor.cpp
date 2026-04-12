#include "db.h"

#if __has_include(<ESPCompressor.h>)

#include "db_runtime.h"
#include "utils/fs_utils.h"

namespace {

DbStatus compressionStatus(CompressionError error) {
	switch (error) {
	case CompressionError::Ok:
		return {DbStatusCode::Ok, ""};
	case CompressionError::NotInitialized:
		return {DbStatusCode::NotInitialized, compressionErrorToString(error)};
	case CompressionError::Busy:
	case CompressionError::Cancelled:
		return {DbStatusCode::Busy, compressionErrorToString(error)};
	case CompressionError::InvalidArgument:
		return {DbStatusCode::InvalidArgument, compressionErrorToString(error)};
	case CompressionError::CorruptData:
		return {DbStatusCode::CorruptionDetected, compressionErrorToString(error)};
	case CompressionError::UnsupportedVersion:
	case CompressionError::UnsupportedAlgorithm:
		return {DbStatusCode::Unsupported, compressionErrorToString(error)};
	case CompressionError::NoMemory:
		return {DbStatusCode::Unknown, compressionErrorToString(error)};
	case CompressionError::OpenFailed:
	case CompressionError::ReadFailed:
	case CompressionError::WriteFailed:
	case CompressionError::OutputOverflow:
	case CompressionError::InternalError:
	default:
		return {DbStatusCode::IoError, compressionErrorToString(error)};
	}
}

std::string sanitizeTempToken(const std::string &input) {
	std::string out;
	out.reserve(input.size());
	for (char c : input) {
		if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
			out.push_back(c);
		} else {
			out.push_back('_');
		}
	}
	return out.empty() ? std::string("db") : out;
}

std::string snapshotTempPath(const std::string &baseDir, const char *suffix) {
	return "/" + sanitizeTempToken(baseDir) + suffix;
}

void removeTempFile(fs::FS &filesystem, const std::string &path) {
	FrLock fs(g_fsMutex);
	if (filesystem.exists(path.c_str())) {
		filesystem.remove(path.c_str());
	}
	if (filesystem.exists((path + ".tmp").c_str())) {
		filesystem.remove((path + ".tmp").c_str());
	}
}

DbStatus openFileForWrite(fs::FS &filesystem, const std::string &path, File &outFile) {
	FrLock fs(g_fsMutex);
	outFile = filesystem.open(path.c_str(), FILE_WRITE);
	if (!outFile) {
		return {DbStatusCode::IoError, "open snapshot staging file failed"};
	}
	return {DbStatusCode::Ok, ""};
}

DbStatus openFileForRead(fs::FS &filesystem, const std::string &path, File &outFile) {
	FrLock fs(g_fsMutex);
	outFile = filesystem.open(path.c_str(), FILE_READ);
	if (!outFile) {
		return {DbStatusCode::IoError, "open snapshot staging file failed"};
	}
	return {DbStatusCode::Ok, ""};
}

} // namespace

DbStatus ESPJsonDB::writeCompressedSnapshot(
    ESPCompressor &compressor,
    CompressionSink &sink,
    SnapshotMode mode,
    ProgressCallback onProgress,
    const CompressionJobOptions &options
) {
	auto ready = ensureReady();
	if (!ready.ok()) {
		return setLastError(ready);
	}

	fs::FS *filesystem = _rt->fs;
	const std::string tempPath = snapshotTempPath(_rt->baseDir, "_snapshot_export.json");
	removeTempFile(*filesystem, tempPath);

	File snapshotFile;
	auto openStatus = openFileForWrite(*filesystem, tempPath, snapshotFile);
	if (!openStatus.ok()) {
		return setLastError(openStatus);
	}

	auto snapshotStatus = writeSnapshot(snapshotFile, mode);
	snapshotFile.close();
	if (!snapshotStatus.ok()) {
		removeTempFile(*filesystem, tempPath);
		return snapshotStatus;
	}

	FileSource source(*filesystem, tempPath.c_str());
	auto result = compressor.compress(source, sink, onProgress, options);
	removeTempFile(*filesystem, tempPath);
	if (!result.ok()) {
		return setLastError(compressionStatus(result.error));
	}
	return setLastError({DbStatusCode::Ok, ""});
}

DbStatus ESPJsonDB::restoreCompressedSnapshot(
    ESPCompressor &compressor,
    CompressionSource &source,
    ProgressCallback onProgress,
    const CompressionJobOptions &options
) {
	auto ready = ensureReady();
	if (!ready.ok()) {
		return setLastError(ready);
	}

	fs::FS *filesystem = _rt->fs;
	const std::string tempPath = snapshotTempPath(_rt->baseDir, "_snapshot_restore.json");
	removeTempFile(*filesystem, tempPath);

	FileSink sink(*filesystem, tempPath.c_str(), true);
	auto result = compressor.decompress(source, sink, onProgress, options);
	if (!result.ok()) {
		removeTempFile(*filesystem, tempPath);
		return setLastError(compressionStatus(result.error));
	}

	File snapshotFile;
	auto openStatus = openFileForRead(*filesystem, tempPath, snapshotFile);
	if (!openStatus.ok()) {
		removeTempFile(*filesystem, tempPath);
		return setLastError(openStatus);
	}

	auto restoreStatus = restoreFromSnapshot(snapshotFile);
	snapshotFile.close();
	removeTempFile(*filesystem, tempPath);
	return restoreStatus;
}

#endif
