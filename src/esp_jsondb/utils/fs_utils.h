#pragma once

#include <Arduino.h>
#include <LittleFS.h>
#include <string>

std::string joinPath(const std::string &a, const std::string &b);

inline bool fsEnsureDir(const std::string &path) {
	if (path.empty() || path == "/") return true;
	if (LittleFS.exists(path.c_str())) return true;
	size_t slash = path.rfind('/');
	if (slash != std::string::npos && slash > 0) {
		std::string parent = path.substr(0, slash);
		if (!fsEnsureDir(parent)) return false;
	}
	return LittleFS.mkdir(path.c_str());
}

inline std::string joinPath(const std::string &a, const std::string &b) {
	if (!b.empty() && b.front() == '/') return b;
	if (a.empty()) return b;
	if (a.back() == '/') {
		return b.empty() ? a : a + b;
	}
	if (b.empty()) return a;
	return a + "/" + b;
}