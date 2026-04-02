#!/usr/bin/env python3

import argparse
import re
import subprocess
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = PROJECT_DIR / "release-changelog.md"


def _run_git(args):
    completed = subprocess.run(
        ["git", *args],
        cwd=PROJECT_DIR,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return False, (completed.stderr or "").strip()
    return True, completed.stdout.strip()


def _require_git(args, error_message):
    ok, output = _run_git(args)
    if not ok:
        print(error_message)
        print(output)
        raise RuntimeError(error_message)
    return output


def _resolve_current_tag(target_ref):
    output = _require_git(
        ["tag", "--points-at", target_ref, "--list", "v*", "--sort=-v:refname"],
        f"[release_changelog] failed to resolve tags for ref '{target_ref}'",
    )
    tags = [line.strip() for line in output.splitlines() if line.strip()]
    if tags:
        return tags[0]
    return ""


def _resolve_previous_tag(target_ref):
    ok, output = _run_git(["describe", "--tags", "--abbrev=0", "--match", "v*", f"{target_ref}^"])
    if not ok:
        return ""
    return output.strip()


def _resolve_commits(range_spec, max_commits):
    output = _require_git(
        [
            "log",
            "--no-merges",
            f"--max-count={max_commits}",
            "--pretty=format:%h%x09%s",
            range_spec,
        ],
        "[release_changelog] failed to resolve commit log",
    )
    commits = []
    for line in output.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t", 1)
        short_hash = parts[0].strip()
        subject = parts[1].strip() if len(parts) > 1 else ""
        commits.append((short_hash, subject))
    return commits


CONVENTIONAL_SUBJECT_RE = re.compile(r"^(?P<type>[a-zA-Z]+)(\([^)]+\))?!?:\s*(?P<body>.+)$")
FIX_SUBJECT_RE = re.compile(r"\b(fix|fixes|fixed|bug|bugs|hotfix|patch|resolve|resolved)\b")
FEATURE_SUBJECT_RE = re.compile(
    r"\b(feat|feature|features|add|adds|added|implement|implemented|introduce|introduced|support|supported|improve|improved|enhance|enhanced)\b"
)


def _classify_subject(subject):
    normalized = subject.strip().lower()
    conventional_match = CONVENTIONAL_SUBJECT_RE.match(normalized)
    if conventional_match:
        commit_type = conventional_match.group("type")
        if commit_type in {"feat", "feature"}:
            return "features"
        if commit_type == "fix":
            return "fixes"

    if FIX_SUBJECT_RE.search(normalized):
        return "fixes"
    if FEATURE_SUBJECT_RE.search(normalized):
        return "features"
    return "other"


def _append_section(lines, section_title, entries):
    lines.append(f"## {section_title}")
    if not entries:
        lines.append("- none")
        lines.append("")
        return

    for short_hash, subject in entries:
        message = subject.strip() or "(no subject)"
        lines.append(f"- {message} (`{short_hash}`)")
    lines.append("")


def _render_markdown(*, display_tag, commits):
    grouped = {"features": [], "fixes": [], "other": []}
    for short_hash, subject in commits:
        section_key = _classify_subject(subject)
        grouped[section_key].append((short_hash, subject))

    lines = []
    lines.append(f"# Release Changelog: {display_tag}")
    lines.append("")
    _append_section(lines, "Features", grouped["features"])
    _append_section(lines, "Fixes", grouped["fixes"])
    _append_section(lines, "Other", grouped["other"])

    return "\n".join(lines)


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Generate release-changelog.md from git changes."
    )
    parser.add_argument(
        "--target-ref",
        default="HEAD",
        help="Target git ref for this release (default: HEAD).",
    )
    parser.add_argument(
        "--tag-name",
        default="",
        help="Display tag name override (default: auto resolve from target ref).",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Output markdown file path.",
    )
    parser.add_argument(
        "--max-commits",
        type=int,
        default=100,
        help="Maximum number of commits to list in highlights.",
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    target_ref = args.target_ref.strip() or "HEAD"
    output_path = Path(args.output).resolve()

    _require_git(
        ["rev-parse", "--verify", f"{target_ref}^{{commit}}"],
        f"[release_changelog] target ref '{target_ref}' is not a valid commit",
    )

    current_tag = _resolve_current_tag(target_ref)
    display_tag = args.tag_name.strip() or current_tag or target_ref
    previous_tag = _resolve_previous_tag(target_ref)
    range_spec = f"{previous_tag}..{target_ref}" if previous_tag else target_ref

    commits = _resolve_commits(range_spec, max(args.max_commits, 1))

    markdown = _render_markdown(
        display_tag=display_tag,
        commits=commits,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(markdown, encoding="utf-8")
    print(f"[release_changelog] wrote {output_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError:
        raise SystemExit(1)
