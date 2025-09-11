#pragma once

#include <freertos/FreeRTOS.h>
#include <freertos/semphr.h>

struct FrMutex {
	SemaphoreHandle_t h{nullptr};
	FrMutex() { h = xSemaphoreCreateMutex(); }
	~FrMutex() {
		if (h) vSemaphoreDelete(h);
	}
	// non-copyable
	FrMutex(const FrMutex &) = delete;
	FrMutex &operator=(const FrMutex &) = delete;
};

struct FrLock {
	FrMutex &m;
	explicit FrLock(FrMutex &mtx) : m(mtx) { xSemaphoreTake(m.h, portMAX_DELAY); }
	~FrLock() { xSemaphoreGive(m.h); }
	// non-copyable
	FrLock(const FrLock &) = delete;
	FrLock &operator=(const FrLock &) = delete;
};

// Global FS mutex to serialize LittleFS operations across tasks
extern FrMutex g_fsMutex;
