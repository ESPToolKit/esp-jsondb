#!/usr/bin/env bash

set -euo pipefail

_repo_root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
_clang_format="${_repo_root}/.vscode/bin/clang-format"

if [ ! -x "${_clang_format}" ]; then
    echo "clang-format wrapper not found: ${_clang_format}" >&2
    exit 1
fi

mapfile -d '' _format_files < <(
    git -C "${_repo_root}" ls-files -z -- '*.c' '*.cc' '*.cpp' '*.h' '*.hpp' '*.ino'
)

if [ "${#_format_files[@]}" -eq 0 ]; then
    echo "No tracked C/C++/INO files found to format."
    exit 0
fi

"${_clang_format}" -i --style=file "${_format_files[@]}"

echo "Formatted ${#_format_files[@]} files."
