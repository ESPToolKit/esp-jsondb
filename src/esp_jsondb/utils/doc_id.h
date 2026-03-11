#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

struct DocId {
	static const std::size_t kHexLength = 24;
	static const std::size_t kStorageLength = kHexLength + 1;

	DocId() {
		clear();
	}

	explicit DocId(const char *id) {
		clear();
		assign(id);
	}

	explicit DocId(const std::string &id) {
		clear();
		assign(id);
	}

	DocId &operator=(const std::string &id) {
		assign(id);
		return *this;
	}

	DocId &operator=(const char *id) {
		assign(id);
		return *this;
	}

	bool assign(const char *id) {
		if (!id) {
			clear();
			return false;
		}
		const std::size_t len = std::strlen(id);
		return assign(id, len);
	}

	bool assign(const std::string &id) {
		return assign(id.c_str(), id.size());
	}

	bool assign(const char *id, std::size_t len) {
		if (!id || len != kHexLength || !isHex(id, len)) {
			clear();
			return false;
		}
		std::memcpy(_data, id, kHexLength);
		_data[kHexLength] = '\0';
		_size = static_cast<uint8_t>(kHexLength);
		return true;
	}

	void setHexUnchecked(const char *id) {
		if (!id) {
			clear();
			return;
		}
		std::memcpy(_data, id, kHexLength);
		_data[kHexLength] = '\0';
		_size = static_cast<uint8_t>(kHexLength);
	}

	void clear() {
		_data[0] = '\0';
		_size = 0;
	}

	bool valid() const {
		return _size == kHexLength && isHex(_data, _size);
	}

	bool empty() const {
		return _size == 0;
	}

	std::size_t size() const {
		return _size;
	}

	const char *c_str() const {
		return _data;
	}

	std::string str() const {
		return std::string(_data, _size);
	}

	operator std::string() const {
		return str();
	}

	int compare(const DocId &other) const {
		return compareRaw(_data, _size, other._data, other._size);
	}

	int compare(const std::string &other) const {
		return compareRaw(_data, _size, other.c_str(), other.size());
	}

	int compare(const char *other) const {
		if (!other) {
			return compareRaw(_data, _size, "", 0);
		}
		return compareRaw(_data, _size, other, std::strlen(other));
	}

	bool operator==(const DocId &other) const {
		return _size == other._size && (_size == 0 || std::memcmp(_data, other._data, _size) == 0);
	}

	bool operator!=(const DocId &other) const {
		return !(*this == other);
	}

	bool operator==(const std::string &other) const {
		return compare(other) == 0;
	}

	bool operator==(const char *other) const {
		return compare(other) == 0;
	}

	static bool isHex(const char *id, std::size_t len) {
		if (!id || len != kHexLength)
			return false;
		for (std::size_t i = 0; i < len; ++i) {
			const char c = id[i];
			const bool isDigit = c >= '0' && c <= '9';
			const bool isLower = c >= 'a' && c <= 'f';
			const bool isUpper = c >= 'A' && c <= 'F';
			if (!isDigit && !isLower && !isUpper)
				return false;
		}
		return true;
	}

  private:
	static int compareRaw(const char *lhs, std::size_t lhsLen, const char *rhs, std::size_t rhsLen) {
		const std::size_t minLen = lhsLen < rhsLen ? lhsLen : rhsLen;
		if (minLen > 0) {
			const int cmp = std::memcmp(lhs, rhs, minLen);
			if (cmp != 0)
				return cmp;
		}
		if (lhsLen < rhsLen)
			return -1;
		if (lhsLen > rhsLen)
			return 1;
		return 0;
	}

	char _data[kStorageLength];
	uint8_t _size = 0;
};

struct DocIdLess {
	typedef void is_transparent;

	bool operator()(const DocId &lhs, const DocId &rhs) const {
		return lhs.compare(rhs) < 0;
	}

	bool operator()(const DocId &lhs, const std::string &rhs) const {
		return lhs.compare(rhs) < 0;
	}

	bool operator()(const std::string &lhs, const DocId &rhs) const {
		return rhs.compare(lhs) > 0;
	}

	bool operator()(const DocId &lhs, const char *rhs) const {
		return lhs.compare(rhs) < 0;
	}

	bool operator()(const char *lhs, const DocId &rhs) const {
		return rhs.compare(lhs) > 0;
	}
};
