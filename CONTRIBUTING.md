# Contributing to esp-jsondb (VS Code + PIOArduino)

First off—**thank you** for your interest! Contributions are very welcome.  
This project targets **ESP32 + Arduino core** and is primarily developed in **VS Code** using **PIOArduino** (a fork of PlatformIO).

---

## Table of contents
- [Quick start (VS Code + PIOArduino)](#quick-start-vs-code--pioarduino)
- [PlatformIO environments](#platformio-environments)
- [Filesystem & partitions](#filesystem--partitions)
- [Project constraints & invariants](#project-constraints--invariants)
- [Coding style](#coding-style)
- [Commit messages & branches](#commit-messages--branches)
- [Pull request checklist](#pull-request-checklist)
- [Adding/Running examples](#addingrunning-examples)
- [Reporting bugs & proposing features](#reporting-bugs--proposing-features)
- [License](#license)

---

## Quick start (VS Code + PIOArduino)

1. **Install VS Code & PIOArduino**
   - Install the **PIOArduino** extension (PlatformIO fork) in VS Code.
2. **Open the project**
   - `File → Open Folder…` and select the repository root.
3. **Dependencies**
   - Handled by PlatformIO (see `platformio.ini`). We depend on:
     - **ArduinoJson**
     - **ArduinoStreamUtils**
     - **LittleFS** (from ESP32 core, or `lorol/LittleFS` if your toolchain does not bundle it)
4. **Select an environment**
   - Use the VS Code status bar (PlatformIO env selector) to choose your board env (e.g., `esp32dev` or `esp32-s3`).
5. **Build / Upload / Monitor**
   - **Build:** `PlatformIO: Build`
   - **Upload:** `PlatformIO: Upload`
   - **Serial Monitor:** `PlatformIO: Monitor` (or add `monitor_speed` in `platformio.ini`)

A minimal `platformio.ini` you can adapt:

```ini
[env]
framework = arduino
platform = espressif32
build_flags =
  -std=gnu++17
  -D ARDUINOJSON_USE_LONG_LONG=1
  -D ARDUINOJSON_ENABLE_STD_STRING=1
  -D ESP_JSONDB_ENABLE_AUTOSYNC=1
monitor_speed = 115200

lib_deps =
  bblanchon/ArduinoJson
  bblanchon/ArduinoStreamUtils
  lorol/LittleFS_esp32 @ ^1.0.6

; ---- Example boards ----

[env:esp32dev]
board = esp32dev
board_build.filesystem = littlefs
build_flags =
  ${env.build_flags}
  -D CONFIG_ARDUHAL_LOG_COLORS=1

[env:esp32s3]
board = esp32-s3-devkitc-1
board_build.filesystem = littlefs
board_build.partitions = partitions.csv   ; see the Partitions section
build_flags =
  ${env.build_flags}
  -D BOARD_HAS_PSRAM
  -D CONFIG_SPIRAM_SUPPORT=1
```

> **Tip:** Boards with **PSRAM** are recommended if you plan to store larger documents or many collections.

---

## PlatformIO environments

- Each supported board should have its own `[env:...]` with the right `board`, `board_build.filesystem`, and optional `board_build.partitions`.
- Keep common options under the shared `[env]` section.
- Prefer **C++17** via `-std=gnu++17` in `build_flags`.
- Add feature flags as `-D` defines (e.g., autosync toggles, debug logs).

---

## Filesystem & partitions

- The DB stores documents as **MessagePack** files in **LittleFS** under a base directory (e.g., `/jsondb`).  
- For S3 or projects needing more FS space, commit a custom `partitions.csv` and point to it from your env:

Example `partitions.csv` (adjust to your flash size):
```csv
# Name,   Type, SubType, Offset,  Size,   Flags
nvs,      data, nvs,     0x9000,  0x5000,
otadata,  data, ota,     0xE000,  0x2000,
app0,     app,  ota_0,   0x10000, 0x190000,
app1,     app,  ota_1,   0x1A0000,0x190000,
littlefs, data, spiffs,  0x330000,0x0D0000,
```

- You can format LittleFS from a sketch once (e.g., `LittleFS.format()` for dev) or use a simple helper tool.  
- **All LittleFS access in the library must hold the global FS mutex** (see constraints).

---

## Project constraints & invariants

Please keep these in mind when contributing:

1. **C++17, no exceptions**  
   - Use explicit status codes via `DbStatus{code, message}` and `DbResult<T>` — do **not** `throw`.
2. **Time sync requirement**  
   - Timestamps are stored in **UTC milliseconds**. The application (sketch) must call `configTime(...)` before creating/updating documents.
3. **Filesystem access is serialized**  
   - LittleFS operations must be guarded by the **global FS mutex** (use `FrLock` with `g_fsMutex` when touching FS).
4. **Autosync task (FreeRTOS)**  
   - The DB may run a background task that flushes dirty documents every `intervalMs`. Keep callbacks **non-blocking**; tune stack/priority/core via `SyncConfig`.
5. **On-disk layout & atomic writes**  
   - Documents are saved as `<id>.mp` under `/baseDir/<collection>/`. Writes should be **atomic**: write to `*.tmp` then `rename()`.
6. **Validation hooks**  
   - Collections may have a `Schema` validator. Mutations should run pre-save validation and fail with a clear status when invalid.

---

## Coding style

- **Language/Std**: C++17 (embedded-friendly). Prefer `std::unique_ptr`, `std::vector`, `std::string`.
- **Error handling**: Return `DbStatus` / `DbResult<T>`; never throw. Keep messages short, static strings where possible.
- **Thread-safety**: Guard shared structures with `FrMutex`/`FrLock`. All LittleFS calls must hold the global FS mutex.
- **I/O**: Use `StreamUtils::WriteBufferingStream` for buffered writes.
- **Validation**: Run schema hooks on create/update; on failure revert and return a validation error.
- **Naming**: lowerCamelCase for methods/vars, UpperCamelCase for types, ALL_CAPS for simple constants/enums.
- **Formatting**: Use a consistent `clang-format` (LLVM/Google); keep lines ≤ 120 cols.
- **Allocations**: Avoid hidden allocations in hot paths and inside event callbacks & sync loops.

Optional `.clang-format` starter (Google-like):
```yaml
BasedOnStyle: Google
IndentWidth: 4
ColumnLimit: 120
DerivePointerAlignment: false
PointerAlignment: Left
AllowShortFunctionsOnASingleLine: Empty
```

---

## Commit messages & branches

- Create **feature branches** from `dev` (e.g., `feat/bulk-update`, `fix/atomic-write`).
- Prefer **Conventional Commits**:
  - `feat: add bulk update with filter`
  - `fix: guard LittleFS rename with mutex`
  - `docs: explain time sync requirement`
  - `perf: buffer file writes with StreamUtils`
- Keep PRs **small and focused** with a clear description & rationale.

---

## Pull request checklist

Before opening a PR, verify:

- [ ] Builds in **PIOArduino** for at least one ESP32 board env.
- [ ] **Examples** build (QuickStart, Collections, BulkOperations, etc.).
- [ ] No exceptions, no RTTI-specific tricks, no `assert()` crashes in production code.
- [ ] All FS access under the **global FS mutex**; no races in multi-task contexts.
- [ ] Validation hooks called on create/update; failures return a proper status.
- [ ] Autosync task (`SyncConfig`) defaults sane; no blocking inside event callbacks.
- [ ] Docs updated: README snippets, new example usage if API changed.
- [ ] Added/updated an example when introducing a new feature.
- [ ] If partitions changed, include `partitions.csv` and update `platformio.ini`.

---

## Adding/Running examples

- Examples live under `examples/YourExample/YourExample.ino` (Arduino format).  
- You can also create **PlatformIO example projects** under `examples/pio/YourExample/` with their own `platformio.ini` if needed.
- Keep examples small, focused, and runnable:
  - Minimal board setup (`Serial`, LittleFS begin), DB init with `SyncConfig`.
  - One clear concept (schema validation, bulk updates, references).
  - Short serial logs to demonstrate output.
  - A header comment with purpose and steps.

---

## Reporting bugs & proposing features

- **Bugs**: include board model, ESP32 core version, PIOArduino version, and a minimal sketch/project that reproduces the issue.
- **Features**: describe the use case, constraints, and any API sketches or example code. Be explicit about memory/sync implications.

---

## License

By contributing, you agree that your contributions will be licensed under the **MIT License** (same as the project).

Thanks again for helping make **esp-jsondb** better! 🚀
