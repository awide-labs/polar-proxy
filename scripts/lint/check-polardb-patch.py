#!/usr/bin/env python3
"""Fast guardrails for PolarDB patch shape.

This is intentionally narrower than a full formatter. The ProxySQL tree has
mixed legacy style, so this script checks only high-signal cases that repeatedly
cause mistakes in the PolarDB work:

* Git whitespace errors in the changed patch.
* Counter-list entries that are over-indented inside PgSQL_PolarDB_Counters.h.
* Counter metadata entries missing the matching HGM status storage field.
* Added C/C++ lines whose indentation is deeper than the enclosing brace level.

Use --cached from pre-commit hooks and --worktree before proposing a patch.
Use --mode full-tree-polardb when you want a slower sweep over PolarDB-owned
C/C++ files before submitting a change.
"""

from __future__ import annotations

import argparse
import difflib
import fnmatch
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path


CPP_SUFFIXES = {".c", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp", ".hxx"}
COUNTERS = Path("include/PgSQL_PolarDB_Counters.h")
HGM = Path("include/PgSQL_HostGroups_Manager.h")
HOOK_STATE_ROOT = Path(os.environ.get("TMPDIR", "/tmp")) / "codex-polardb-patch-check"
POLARDB_FULL_TREE_PATTERNS = (
    "include/PgSQL_PolarDB*.h",
    "lib/PgSQL_PolarDB*.cpp",
    "test/polardb/test-unit/*polardb*_unit-t.cpp",
)


def run_git(repo: Path, args: list[str], check: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=str(repo),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=check,
    )


def repo_root() -> Path:
    proc = run_git(Path.cwd(), ["rev-parse", "--show-toplevel"], check=True)
    return Path(proc.stdout.strip())


def diff_args(mode: str) -> list[str]:
    if mode == "cached":
        return ["diff", "--cached"]
    if mode == "worktree":
        return ["diff"]
    raise ValueError(mode)


def check_diff_whitespace(repo: Path, mode: str) -> list[str]:
    proc = run_git(repo, [*diff_args(mode), "--check"])
    if proc.returncode == 0:
        return []
    output = (proc.stdout + proc.stderr).strip()
    return [f"{mode}: git diff --check failed", output] if output else [
        f"{mode}: git diff --check failed"
    ]


def changed_cpp_lines(repo: Path, mode: str) -> dict[Path, set[int]]:
    proc = run_git(repo, [*diff_args(mode), "--unified=80", "--no-ext-diff"])
    result: dict[Path, set[int]] = {}
    current: Path | None = None
    new_line = 0

    for raw in proc.stdout.splitlines():
        if raw.startswith("+++ b/"):
            current = Path(raw[6:])
            if current.suffix not in CPP_SUFFIXES:
                current = None
            elif current not in result:
                result[current] = set()
            continue
        if raw.startswith("@@"):
            match = re.search(r"\+(\d+)(?:,(\d+))?", raw)
            if match:
                new_line = int(match.group(1))
            continue
        if current is None:
            continue
        if raw.startswith("+") and not raw.startswith("+++"):
            result[current].add(new_line)
            new_line += 1
        elif raw.startswith("-") and not raw.startswith("---"):
            continue
        else:
            new_line += 1

    return {path: lines for path, lines in result.items() if lines}


def full_tree_polardb_cpp_lines(repo: Path) -> dict[Path, set[int]]:
    """Return all lines from PolarDB-owned C/C++ files for optional strict lint."""

    proc = run_git(repo, ["ls-files"], check=True)
    result: dict[Path, set[int]] = {}
    for raw in proc.stdout.splitlines():
        relpath = Path(raw)
        if relpath.suffix not in CPP_SUFFIXES:
            continue
        normalized = relpath.as_posix()
        if not any(fnmatch.fnmatch(normalized, pattern) for pattern in POLARDB_FULL_TREE_PATTERNS):
            continue
        path = repo / relpath
        if not path.exists() or not path.is_file():
            continue
        line_count = len(path.read_text(errors="ignore").splitlines())
        if line_count:
            result[relpath] = set(range(1, line_count + 1))
    return result


def parse_apply_patch_paths(command: str) -> set[Path]:
    paths: set[Path] = set()
    for raw in command.splitlines():
        for marker in (
            "*** Add File: ",
            "*** Update File: ",
            "*** Delete File: ",
            "*** Move to: ",
        ):
            if raw.startswith(marker):
                paths.add(Path(raw[len(marker) :].strip()))
                break
    return paths


def changed_lines_between(before: list[str], after: list[str]) -> set[int]:
    changed: set[int] = set()
    matcher = difflib.SequenceMatcher(a=before, b=after, autojunk=False)
    for tag, _i1, _i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            continue
        changed.update(range(j1 + 1, j2 + 1))
    return changed


def hook_state_dir(event: dict) -> Path:
    session_id = re.sub(r"[^A-Za-z0-9_.-]", "_", str(event.get("session_id", "unknown")))
    tool_use_id = re.sub(r"[^A-Za-z0-9_.-]", "_", str(event.get("tool_use_id", "unknown")))
    return HOOK_STATE_ROOT / session_id / tool_use_id


def snapshot_files(repo: Path, event: dict) -> int:
    tool_input = event.get("tool_input") or {}
    command = tool_input.get("command") if isinstance(tool_input, dict) else ""
    paths = sorted(parse_apply_patch_paths(command or ""))
    state_dir = hook_state_dir(event)
    if state_dir.exists():
        shutil.rmtree(state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)

    manifest = []
    for relpath in paths:
        if relpath.is_absolute() or ".." in relpath.parts:
            continue
        source = repo / relpath
        item = {"path": relpath.as_posix(), "exists": source.exists()}
        target = state_dir / relpath
        if source.exists() and source.is_file():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
        manifest.append(item)

    (state_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return 0


def changed_cpp_lines_from_hook_snapshot(repo: Path, event: dict) -> dict[Path, set[int]]:
    state_dir = hook_state_dir(event)
    manifest_path = state_dir / "manifest.json"
    if not manifest_path.exists():
        return {}

    try:
        manifest = json.loads(manifest_path.read_text())
    except Exception:
        return {}

    changed: dict[Path, set[int]] = {}
    for item in manifest:
        relpath = Path(str(item.get("path", "")))
        if relpath.suffix not in CPP_SUFFIXES:
            continue
        current_path = repo / relpath
        if not current_path.exists() or not current_path.is_file():
            continue
        before_path = state_dir / relpath
        before = (
            before_path.read_text(errors="ignore").splitlines()
            if before_path.exists()
            else []
        )
        after = current_path.read_text(errors="ignore").splitlines()
        lines = changed_lines_between(before, after)
        if lines:
            changed[relpath] = lines

    shutil.rmtree(state_dir, ignore_errors=True)
    return changed


def strip_strings_and_comments(line: str, in_block_comment: bool) -> tuple[str, bool]:
    out: list[str] = []
    i = 0
    quote: str | None = None
    escaped = False
    while i < len(line):
        ch = line[i]
        nxt = line[i + 1] if i + 1 < len(line) else ""

        if in_block_comment:
            if ch == "*" and nxt == "/":
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue

        if quote:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == quote:
                quote = None
            out.append(" ")
            i += 1
            continue

        if ch == "/" and nxt == "*":
            in_block_comment = True
            i += 2
            continue
        if ch == "/" and nxt == "/":
            break
        if ch in {"'", '"'}:
            quote = ch
            out.append(" ")
            i += 1
            continue

        out.append(ch)
        i += 1

    return "".join(out), in_block_comment


def is_namespace_open(code: str) -> bool:
    stripped = code.strip()
    return bool(re.match(r"^(inline\s+)?namespace(\s+[A-Za-z_][A-Za-z_0-9]*)?\s*\{", stripped))


def count_leading_closes(code: str) -> int:
    stripped = code.lstrip()
    count = 0
    for ch in stripped:
        if ch == "}":
            count += 1
        elif ch.isspace():
            continue
        else:
            break
    return count


def continuation_line(prev: str, stripped: str) -> bool:
    if not prev:
        return False
    prev = prev.rstrip()
    if prev.endswith(("{", "}", ";")):
        return False
    if prev.endswith((",", "(", "[", "=", "&&", "||", "?", ":", "+", "-", "*", "/")):
        return True
    if stripped.startswith((".", "->", "&&", "||", ",", ":", "?", "+", "-", "*", "/")):
        return True
    return False


def check_cpp_added_indentation(
    repo: Path,
    mode: str,
    changed: dict[Path, set[int]] | None = None,
    enforce_tabs: bool = True,
) -> list[str]:
    changed = changed_cpp_lines(repo, mode) if changed is None else changed
    problems: list[str] = []
    for relpath, added_lines in sorted(changed.items()):
        path = repo / relpath
        if not path.exists():
            continue
        lines = path.read_text(errors="ignore").splitlines()
        indent_depth = 0
        namespace_depth = 0
        block_comment = False
        macro_continuation = False
        prev_significant = ""

        for index, line in enumerate(lines, start=1):
            code, block_comment = strip_strings_and_comments(line, block_comment)
            stripped = code.strip()
            in_macro = macro_continuation or stripped.startswith("#")
            macro_continuation = code.rstrip().endswith("\\")

            leading_close = count_leading_closes(code)
            effective_depth = max(0, indent_depth - leading_close - namespace_depth)

            if index in added_lines and stripped and not in_macro:
                raw_indent = line[: len(line) - len(line.lstrip("\t "))]
                tab_count = len(raw_indent) - len(raw_indent.replace("\t", ""))
                has_space_indent = " " in raw_indent
                is_cont = continuation_line(prev_significant, stripped)

                if enforce_tabs and has_space_indent and not is_cont:
                    problems.append(
                        f"{relpath}:{index}: added C/C++ code uses space indentation"
                    )
                elif tab_count > effective_depth and not is_cont:
                    problems.append(
                        f"{relpath}:{index}: added line is indented {tab_count} tabs, "
                        f"but enclosing brace depth is {effective_depth}"
                    )

            if stripped:
                prev_significant = stripped

            namespace_open = is_namespace_open(code)
            opens = code.count("{")
            closes = code.count("}")
            indent_depth = max(0, indent_depth + opens - closes)
            if namespace_open:
                namespace_depth += 1
            if stripped.startswith("} // namespace") or stripped == "}":
                namespace_depth = max(0, min(namespace_depth, indent_depth))

    return [f"{mode}: indentation check failed", *problems] if problems else []


def hook_event_from_stdin() -> dict:
    try:
        raw = sys.stdin.read()
        return json.loads(raw) if raw.strip() else {}
    except Exception:
        return {}


def run_hook() -> int:
    event = hook_event_from_stdin()
    repo = Path(event.get("cwd") or repo_root()).resolve()
    if not (repo / ".git").exists():
        repo = repo_root()

    event_name = event.get("hook_event_name")
    tool_name = event.get("tool_name")
    if tool_name not in {"apply_patch", "Edit", "Write"}:
        return 0

    if event_name == "PreToolUse":
        return snapshot_files(repo, event)
    if event_name != "PostToolUse":
        return 0

    changed = changed_cpp_lines_from_hook_snapshot(repo, event)
    errors: list[str] = []
    errors.extend(check_diff_whitespace(repo, "worktree"))
    errors.extend(check_cpp_added_indentation(repo, "current apply_patch", changed))
    errors.extend(check_counter_macro_indentation(repo))
    errors.extend(check_counter_storage(repo))

    if errors:
        print("PolarDB patch check failed after apply_patch:", file=sys.stderr)
        for item in errors:
            print(item, file=sys.stderr)
        return 2
    return 0


def check_counter_macro_indentation(repo: Path) -> list[str]:
    path = repo / COUNTERS
    problems: list[str] = []
    for index, line in enumerate(path.read_text().splitlines(), start=1):
        if re.match(r"^\s+[TG]\(", line) and not re.match(r"^\t[TG]\(", line):
            problems.append(
                f"{COUNTERS}:{index}: counter-list entry must start with exactly one tab"
            )
    return problems


def extract_counter_names(text: str) -> set[str]:
    names: set[str] = set()
    for line in text.splitlines():
        match = re.match(r"\s*[TG]\(([a-z0-9_]+)\s*,", line)
        if match:
            names.add(match.group(1))
    return names


def check_counter_storage(repo: Path) -> list[str]:
    counter_text = (repo / COUNTERS).read_text()
    hgm_text = (repo / HGM).read_text()
    missing: list[str] = []
    for name in sorted(extract_counter_names(counter_text)):
        field = f"polardb_{name}"
        if not re.search(rf"\b{re.escape(field)}\b", hgm_text):
            missing.append(
                f"{COUNTERS}: counter '{name}' is missing HGM status field '{field}'"
            )
    return missing


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=("cached", "worktree", "both", "full-tree-polardb"),
        default="worktree",
        help="patch to check; pre-commit should use --mode=cached",
    )
    parser.add_argument(
        "--hook",
        action="store_true",
        help="read Codex hook JSON from stdin and check only the current tool call",
    )
    args = parser.parse_args()

    if args.hook:
        return run_hook()

    repo = repo_root()
    errors: list[str] = []

    if args.mode == "full-tree-polardb":
        errors.extend(
            check_cpp_added_indentation(
                repo,
                "full-tree-polardb",
                full_tree_polardb_cpp_lines(repo),
                enforce_tabs=False,
            )
        )
    else:
        modes = ["cached", "worktree"] if args.mode == "both" else [args.mode]
        for mode in modes:
            errors.extend(check_diff_whitespace(repo, mode))
            errors.extend(check_cpp_added_indentation(repo, mode))

    errors.extend(check_counter_macro_indentation(repo))
    errors.extend(check_counter_storage(repo))

    if errors:
        print("PolarDB patch check failed:", file=sys.stderr)
        for item in errors:
            print(item, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
