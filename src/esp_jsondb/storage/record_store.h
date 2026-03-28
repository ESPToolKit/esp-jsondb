#pragma once

#include <FS.h>

#include <memory>
#include <string>

#include "../document/document.h"
#include "../utils/dbTypes.h"
#include "../utils/jsondb_allocator.h"

class RecordStore {
  public:
	RecordStore(fs::FS &fs, bool usePSRAMBuffers = false) : _fs(&fs), _usePSRAMBuffers(usePSRAMBuffers) {
	}

	DbStatus write(const std::string &collectionDir, const DocumentRecord &record);
	DbResult<std::shared_ptr<DocumentRecord>>
	read(const std::string &collectionDir, const std::string &id) const;
	JsonDbVector<DocId> listIds(const std::string &collectionDir) const;
	DbStatus remove(const std::string &collectionDir, const DocId &id) const;

  private:
	fs::FS *_fs = nullptr;
	bool _usePSRAMBuffers = false;
};
