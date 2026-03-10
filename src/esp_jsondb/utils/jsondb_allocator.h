#pragma once

#if __has_include(<ESPBufferManager.h>)
#include <ESPBufferManager.h>
#define ESP_JSONDB_HAS_BUFFER_MANAGER 1
#elif __has_include(<esp_buffer_manager/buffer_manager.h>)
#include <esp_buffer_manager/buffer_manager.h>
#define ESP_JSONDB_HAS_BUFFER_MANAGER 1
#else
#define ESP_JSONDB_HAS_BUFFER_MANAGER 0
#endif

#if defined(ARDUINO_ARCH_ESP32) && __has_include(<esp_heap_caps.h>)
#include <esp_heap_caps.h>
#define ESP_JSONDB_HAS_ESP32_HEAP_CAPS 1
#else
#define ESP_JSONDB_HAS_ESP32_HEAP_CAPS 0
#endif

#include <cstddef>
#include <cstdlib>
#include <limits>
#include <new>
#include <vector>

namespace jsondb_allocator_detail {

inline void *allocate(std::size_t bytes, bool usePSRAMBuffers) noexcept {
#if ESP_JSONDB_HAS_BUFFER_MANAGER
	return ESPBufferManager::allocate(bytes, usePSRAMBuffers);
#else
#if ESP_JSONDB_HAS_ESP32_HEAP_CAPS
	if (usePSRAMBuffers) {
		if (void *psramMemory = heap_caps_malloc(bytes, MALLOC_CAP_SPIRAM)) {
			return psramMemory;
		}
	}
#else
	(void)usePSRAMBuffers;
#endif
	return std::malloc(bytes);
#endif
}

inline void deallocate(void *ptr) noexcept {
#if ESP_JSONDB_HAS_BUFFER_MANAGER
	ESPBufferManager::deallocate(ptr);
#else
	std::free(ptr);
#endif
}

inline void *reallocate(void *ptr, std::size_t bytes, bool usePSRAMBuffers) noexcept {
#if ESP_JSONDB_HAS_BUFFER_MANAGER
	return ESPBufferManager::reallocate(ptr, bytes, usePSRAMBuffers);
#else
#if ESP_JSONDB_HAS_ESP32_HEAP_CAPS
	if (usePSRAMBuffers) {
		if (void *psramMemory = heap_caps_realloc(ptr, bytes, MALLOC_CAP_SPIRAM)) {
			return psramMemory;
		}
	}
#else
	(void)usePSRAMBuffers;
#endif
	return std::realloc(ptr, bytes);
#endif
}

} // namespace jsondb_allocator_detail

template <typename T> class JsonDbAllocator {
  public:
	using value_type = T;

	JsonDbAllocator() noexcept = default;
	explicit JsonDbAllocator(bool usePSRAMBuffers) noexcept : _usePSRAMBuffers(usePSRAMBuffers) {
	}

	template <typename U>
	JsonDbAllocator(const JsonDbAllocator<U> &other) noexcept
	    : _usePSRAMBuffers(other.usePSRAMBuffers()) {
	}

	T *allocate(std::size_t n) {
		if (n == 0)
			return nullptr;
		if (n > (std::numeric_limits<std::size_t>::max() / sizeof(T))) {
#if defined(__cpp_exceptions)
			throw std::bad_alloc();
#else
			std::abort();
#endif
		}

		void *memory = jsondb_allocator_detail::allocate(n * sizeof(T), _usePSRAMBuffers);
		if (memory == nullptr) {
#if defined(__cpp_exceptions)
			throw std::bad_alloc();
#else
			std::abort();
#endif
		}
		return static_cast<T *>(memory);
	}

	void deallocate(T *ptr, std::size_t) noexcept {
		jsondb_allocator_detail::deallocate(ptr);
	}

	bool usePSRAMBuffers() const noexcept {
		return _usePSRAMBuffers;
	}

	template <typename U> bool operator==(const JsonDbAllocator<U> &other) const noexcept {
		return _usePSRAMBuffers == other.usePSRAMBuffers();
	}

	template <typename U> bool operator!=(const JsonDbAllocator<U> &other) const noexcept {
		return !(*this == other);
	}

  private:
	template <typename> friend class JsonDbAllocator;

	bool _usePSRAMBuffers = false;
};

template <typename T> using JsonDbVector = std::vector<T, JsonDbAllocator<T>>;
