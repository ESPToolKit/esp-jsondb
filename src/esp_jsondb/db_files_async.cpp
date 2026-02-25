#include "db.h"

#include <algorithm>

#include "utils/fs_utils.h"
#include "utils/jsondb_allocator.h"

namespace {

std::string parentDirForAsyncUpload(const std::string &path) {
	auto pos = path.find_last_of('/');
	if (pos == std::string::npos || pos == 0) {
		return "/";
	}
	return path.substr(0, pos);
}

} // namespace

bool ESPJsonDB::isUploadTerminal(DbFileUploadState state) const {
	return state == DbFileUploadState::Completed ||
		   state == DbFileUploadState::Failed ||
		   state == DbFileUploadState::Cancelled;
}

void ESPJsonDB::trackTerminalUploadLocked(const std::shared_ptr<FileUploadJob> &job) {
	if (!job || !isUploadTerminal(job->state) || job->terminalTracked) return;

	job->terminalTracked = true;
	_terminalUploadOrder.push_back(job->id);

	while (_terminalUploadOrder.size() > kMaxRetainedTerminalUploads) {
		const uint32_t expiredId = _terminalUploadOrder.front();
		_terminalUploadOrder.erase(_terminalUploadOrder.begin());

		auto it = _uploadJobs.find(expiredId);
		if (it == _uploadJobs.end()) continue;
		if (!it->second || isUploadTerminal(it->second->state)) {
			_uploadJobs.erase(it);
		}
	}
}

DbResult<uint32_t> ESPJsonDB::writeFileStreamAsync(const std::string &relativePath,
														const DbFileUploadPullCb &pullCb,
														const ESPJsonDBFileOptions &opts,
														const DbFileUploadDoneCb &doneCb) {
	DbResult<uint32_t> res{};
	auto ready = ensureReady();
	if (!ready.ok()) {
		res.status = setLastError(ready);
		return res;
	}
	if (!pullCb) {
		res.status = setLastError({DbStatusCode::InvalidArgument, "upload callback is required"});
		return res;
	}

	std::string normalized;
	auto nst = normalizeFilePath(relativePath, normalized);
	if (!nst.ok()) {
		res.status = setLastError(nst);
		return res;
	}

	auto job = std::make_shared<FileUploadJob>();
	job->relativePath = relativePath;
	job->normalizedPath = normalized;
	job->opts = opts;
	if (job->opts.chunkSize < 32) job->opts.chunkSize = 32;
	job->pullCb = pullCb;
	job->doneCb = doneCb;

	{
		FrLock lk(_mu);
		job->id = _nextUploadId++;
		_uploadJobs[job->id] = job;
		_uploadQueue.push_back(job->id);
		startFileUploadTaskUnlocked();
		if (_fileUploadTask == nullptr) {
			_uploadQueue.erase(std::remove(_uploadQueue.begin(), _uploadQueue.end(), job->id), _uploadQueue.end());
			_uploadJobs.erase(job->id);
			res.status = setLastError({DbStatusCode::Busy, "upload worker start failed"});
			return res;
		}
	}

	res.status = setLastError({DbStatusCode::Ok, ""});
	res.value = job->id;
	return res;
}

DbStatus ESPJsonDB::cancelFileUpload(uint32_t uploadId) {
	DbFileUploadDoneCb doneCb;
	DbStatus doneStatus{DbStatusCode::Busy, "upload cancelled"};
	size_t bytesWritten = 0;
	bool triggerDone = false;

	{
		FrLock lk(_mu);
		auto it = _uploadJobs.find(uploadId);
		if (it == _uploadJobs.end()) {
			return setLastError({DbStatusCode::NotFound, "upload not found"});
		}
		auto &job = it->second;
		if (!job) {
			return setLastError({DbStatusCode::NotFound, "upload not found"});
		}
		if (isUploadTerminal(job->state)) {
			return setLastError({DbStatusCode::Ok, ""});
		}

		job->cancelRequested = true;
		if (job->state == DbFileUploadState::Queued) {
			_uploadQueue.erase(std::remove(_uploadQueue.begin(), _uploadQueue.end(), uploadId), _uploadQueue.end());
			job->state = DbFileUploadState::Cancelled;
			job->finalStatus = doneStatus;
			trackTerminalUploadLocked(job);
			doneCb = job->doneCb;
			bytesWritten = job->bytesWritten;
			triggerDone = true;
		}
	}

	if (triggerDone && doneCb) {
		doneCb(uploadId, doneStatus, bytesWritten);
	}
	return setLastError({DbStatusCode::Ok, ""});
}

DbResult<DbFileUploadState> ESPJsonDB::getFileUploadState(uint32_t uploadId) {
	DbResult<DbFileUploadState> res{};
	auto ready = ensureReady();
	if (!ready.ok()) {
		res.status = setLastError(ready);
		return res;
	}

	{
		FrLock lk(_mu);
		auto it = _uploadJobs.find(uploadId);
		if (it == _uploadJobs.end() || !it->second) {
			res.status = setLastError({DbStatusCode::NotFound, "upload not found"});
			return res;
		}
		res.value = it->second->state;
	}

	res.status = setLastError({DbStatusCode::Ok, ""});
	return res;
}

void ESPJsonDB::fileUploadTaskThunk(void *arg) {
	auto *self = static_cast<ESPJsonDB *>(arg);
	self->fileUploadTaskLoop();
}

void ESPJsonDB::startFileUploadTaskUnlocked() {
	if (_fileUploadTask != nullptr) return;
	_fileUploadStopRequested.store(false, std::memory_order_release);
	_fileUploadTaskExited.store(false, std::memory_order_release);
	TaskHandle_t handle = nullptr;
	if (createTask(fileUploadTaskThunk, "db.file.upload", handle)) {
		_fileUploadTask = handle;
	} else {
		_fileUploadTaskExited.store(true, std::memory_order_release);
	}
}

void ESPJsonDB::stopFileUploadTaskUnlocked(bool cancelPending) {
	if (cancelPending) {
		for (auto &kv : _uploadJobs) {
			auto &job = kv.second;
			if (!job || isUploadTerminal(job->state)) continue;
			job->cancelRequested = true;
			job->state = DbFileUploadState::Cancelled;
			job->finalStatus = {DbStatusCode::Busy, "upload cancelled"};
		}
		_uploadQueue.clear();
	}
	if (_fileUploadTask) {
		stopTask(_fileUploadTask, _fileUploadStopRequested, _fileUploadTaskExited);
	}
}

DbStatus ESPJsonDB::runFileUploadJob(const std::shared_ptr<FileUploadJob> &job, size_t &bytesWritten) {
	bytesWritten = 0;
	if (!job || !job->pullCb) {
		return {DbStatusCode::InvalidArgument, "upload callback is required"};
	}

	const size_t chunkSize = job->opts.chunkSize < 32 ? 32 : job->opts.chunkSize;
	JsonDbVector<uint8_t> buffer{JsonDbAllocator<uint8_t>(_cfg.usePSRAMBuffers)};
	buffer.resize(chunkSize);

	const std::string finalPath = joinPath(fileRootDir(), job->normalizedPath);
	const std::string parentDir = parentDirForAsyncUpload(finalPath);
	const std::string tmpPath = finalPath + ".tmp";

	{
		FrLock fs(g_fsMutex);
		if (!fsEnsureDir(*_fs, parentDir)) {
			return {DbStatusCode::IoError, "mkdir file parent failed"};
		}
		if (!job->opts.overwrite && _fs->exists(finalPath.c_str())) {
			return {DbStatusCode::AlreadyExists, "file already exists"};
		}
		if (_fs->exists(tmpPath.c_str())) {
			_fs->remove(tmpPath.c_str());
		}
	}

	File f;
	{
		FrLock fs(g_fsMutex);
		f = _fs->open(tmpPath.c_str(), FILE_WRITE);
	}
	if (!f) {
		return {DbStatusCode::IoError, "open file for write failed"};
	}

	auto cleanupTmp = [&]() {
		FrLock fs(g_fsMutex);
		f.close();
		if (_fs->exists(tmpPath.c_str())) {
			_fs->remove(tmpPath.c_str());
		}
	};

	for (;;) {
		bool cancelled = false;
		{
			FrLock lk(_mu);
			auto it = _uploadJobs.find(job->id);
			cancelled = (it == _uploadJobs.end() || !it->second || it->second->cancelRequested);
		}
		if (cancelled) {
			cleanupTmp();
			return {DbStatusCode::Busy, "upload cancelled"};
		}

		size_t produced = 0;
		bool eof = false;
		auto st = job->pullCb(chunkSize, buffer.data(), produced, eof);
		if (!st.ok()) {
			cleanupTmp();
			return st;
		}
		if (produced > chunkSize) {
			cleanupTmp();
			return {DbStatusCode::InvalidArgument, "upload callback produced too many bytes"};
		}

		if (produced > 0) {
			size_t written = 0;
			{
				FrLock fs(g_fsMutex);
				written = f.write(buffer.data(), produced);
			}
			if (written != produced) {
				cleanupTmp();
				return {DbStatusCode::IoError, "file write failed"};
			}
			bytesWritten += written;
		}

		if (eof) {
			break;
		}
		if (produced == 0) {
			vTaskDelay(pdMS_TO_TICKS(1));
		}
	}

	{
		FrLock fs(g_fsMutex);
		f.flush();
		f.close();
		if (job->opts.overwrite && _fs->exists(finalPath.c_str()) && !_fs->remove(finalPath.c_str())) {
			if (_fs->exists(tmpPath.c_str())) {
				_fs->remove(tmpPath.c_str());
			}
			return {DbStatusCode::IoError, "remove old file failed"};
		}
		if (!_fs->rename(tmpPath.c_str(), finalPath.c_str())) {
			if (_fs->exists(tmpPath.c_str())) {
				_fs->remove(tmpPath.c_str());
			}
			return {DbStatusCode::IoError, "rename file failed"};
		}
	}

	return {DbStatusCode::Ok, ""};
}

void ESPJsonDB::fileUploadTaskLoop() {
	while (!_fileUploadStopRequested.load(std::memory_order_acquire)) {
		std::shared_ptr<FileUploadJob> job;
		{
			FrLock lk(_mu);
			if (!_uploadQueue.empty()) {
				auto id = _uploadQueue.front();
				_uploadQueue.erase(_uploadQueue.begin());
				auto it = _uploadJobs.find(id);
				if (it != _uploadJobs.end()) {
					job = it->second;
					if (job && job->state == DbFileUploadState::Queued && !job->cancelRequested) {
						job->state = DbFileUploadState::Running;
					}
				}
			}
		}

		if (!job) {
			vTaskDelay(pdMS_TO_TICKS(20));
			continue;
		}

		if (job->cancelRequested) {
			DbFileUploadDoneCb doneCb;
			{
				FrLock lk(_mu);
				job->state = DbFileUploadState::Cancelled;
				job->finalStatus = {DbStatusCode::Busy, "upload cancelled"};
				trackTerminalUploadLocked(job);
				doneCb = job->doneCb;
			}
			if (doneCb) {
				doneCb(job->id, job->finalStatus, job->bytesWritten);
			}
			continue;
		}

		size_t bytesWritten = 0;
		auto st = runFileUploadJob(job, bytesWritten);

		DbFileUploadDoneCb doneCb;
		DbStatus finalStatus = st;
		DbFileUploadState finalState = DbFileUploadState::Failed;
		{
			FrLock lk(_mu);
			auto it = _uploadJobs.find(job->id);
			if (it != _uploadJobs.end() && it->second) {
				auto &j = it->second;
				j->bytesWritten = bytesWritten;
				if (j->cancelRequested) {
					finalState = DbFileUploadState::Cancelled;
					finalStatus = {DbStatusCode::Busy, "upload cancelled"};
				} else if (st.ok()) {
					finalState = DbFileUploadState::Completed;
					finalStatus = st;
				} else {
					finalState = DbFileUploadState::Failed;
					finalStatus = st;
				}
				j->state = finalState;
				j->finalStatus = finalStatus;
				trackTerminalUploadLocked(j);
				doneCb = j->doneCb;
			}
		}

		if (!finalStatus.ok() && finalState != DbFileUploadState::Cancelled) {
			setLastError(finalStatus);
		}
		if (doneCb) {
			doneCb(job->id, finalStatus, bytesWritten);
		}
	}
	_fileUploadTaskExited.store(true, std::memory_order_release);
	vTaskDelete(nullptr);
}
