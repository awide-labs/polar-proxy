#!/usr/bin/env python3
"""Summarize gcov implementation coverage for PolarDB review controls."""

import argparse
import re
import sys
from pathlib import Path


FILE_RE = re.compile(r"^File '([^']+)'")
METRIC_RE = re.compile(r"^(Lines executed|Branches executed|Taken at least once|Calls executed):([0-9.]+)%")
SOURCE_RE = re.compile(r"^\s*-:\s*0:Source:(.+)$")
FUNCTION_RE = re.compile(
    r"^function (.+) called ([0-9#]+) returned ([0-9.]+)% blocks executed ([0-9.]+)%")
FUNCTION_FALLBACK_RE = re.compile(
    r"^function (.+) called ([0-9#]+) returned ([0-9.]+)%")
EXECUTED_LINE_RE = re.compile(r"^\s*(#####|={5}|[0-9]+[*]?):\s*[0-9]+:")
BRANCH_RE = re.compile(r"^\s*branch\s+[0-9]+\s+(?:taken\s+([^ ]+)|never executed)")
CALL_RE = re.compile(r"^\s*call\s+[0-9]+\s+(?:returned\s+([^ ]+)|never executed)")

DEFAULT_FUNCTION_INCLUDE = (
    "polardb|polar|rfq|build_simple_query_packet|record_wait_latency|"
    "clear_pending_notices|enqueue_pending_notice|finalize_wait_timeout_injection|"
    "fail_wait_wrap_finalize|build_wrapped_wait_query|"
    "handle_async_check_cont|get_task_query|perf_readonly_actions"
)
NOISE_FUNCTION_PREFIXES = (
    "std::",
    "__gnu_cxx::",
    "nlohmann::",
    "boost::",
    "void std::",
    "std::_",
)


def parse_gcov(path):
    files = {}
    current = None
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        file_match = FILE_RE.match(line)
        if file_match:
            current = file_match.group(1)
            files.setdefault(current, {})
            continue

        metric_match = METRIC_RE.match(line)
        if metric_match and current:
            files[current][metric_match.group(1)] = float(metric_match.group(2))
    return files


def branch_or_call_taken(value):
    if value is None:
        return False
    value = value.rstrip("%")
    try:
        return float(value) > 0.0
    except ValueError:
        return False


def function_is_noise(name):
    return any(name.startswith(prefix) for prefix in NOISE_FUNCTION_PREFIXES)


def function_is_included(name, include_re):
    return include_re.search(name) is not None and not function_is_noise(name)


def flush_function(functions, current):
    if not current:
        return
    branches = current["branches"]
    calls = current["calls"]
    executable_lines = current["executable_lines"]
    current["line_taken_pct"] = (
        (current["executed_lines"] * 100.0 / executable_lines)
        if executable_lines else None)
    current["branch_taken_pct"] = (
        (current["branches_taken"] * 100.0 / branches) if branches else None)
    current["call_taken_pct"] = (
        (current["calls_taken"] * 100.0 / calls) if calls else None)
    functions.append(current)


def parse_function_gcov(gcov_dir, source_files, include_pattern):
    include_re = re.compile(include_pattern, re.IGNORECASE)
    source_set = set(source_files)
    functions = []

    for source_name in source_files:
        gcov_path = gcov_dir / f"{Path(source_name).name}.gcov"
        if not gcov_path.exists():
            continue

        source = source_name
        current = None
        for line in gcov_path.read_text(encoding="utf-8", errors="replace").splitlines():
            source_match = SOURCE_RE.match(line)
            if source_match:
                source = Path(source_match.group(1)).name
                continue

            function_match = FUNCTION_RE.match(line) or FUNCTION_FALLBACK_RE.match(line)
            if function_match:
                flush_function(functions, current)
                name = function_match.group(1)
                if source not in source_set or not function_is_included(name, include_re):
                    current = None
                    continue
                current = {
                    "source": source,
                    "name": name,
                    "called": function_match.group(2),
                    "returned_pct": float(function_match.group(3)),
                    "blocks_pct": float(function_match.group(4))
                    if function_match.lastindex and function_match.lastindex >= 4 else None,
                    "executable_lines": 0,
                    "executed_lines": 0,
                    "branches": 0,
                    "branches_taken": 0,
                    "calls": 0,
                    "calls_taken": 0,
                }
                continue

            if not current:
                continue

            line_match = EXECUTED_LINE_RE.match(line)
            if line_match:
                current["executable_lines"] += 1
                if line_match.group(1)[0].isdigit():
                    current["executed_lines"] += 1
                continue

            branch_match = BRANCH_RE.match(line)
            if branch_match:
                current["branches"] += 1
                if branch_or_call_taken(branch_match.group(1)):
                    current["branches_taken"] += 1
                continue

            call_match = CALL_RE.match(line)
            if call_match:
                current["calls"] += 1
                if branch_or_call_taken(call_match.group(1)):
                    current["calls_taken"] += 1

        flush_function(functions, current)

    functions.sort(key=lambda f: (f["source"], f["name"]))
    return functions


def function_threshold_excluded(function, exclude_patterns):
    full_name = f"{function['source']}::{function['name']}"
    return any(pattern.search(full_name) for pattern in exclude_patterns)


def format_pct(value):
    return "n/a" if value is None else f"{value:.2f}%"


def main():
    parser = argparse.ArgumentParser(
        description="Summarize PolarDB implementation gcov output.")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--function-output", type=Path,
                        help="optional output for filtered PolarDB function coverage")
    parser.add_argument("--gcov-dir", type=Path,
                        help="directory containing generated .gcov files")
    parser.add_argument("--function-include", default=DEFAULT_FUNCTION_INCLUDE,
                        help="case-insensitive regex for functions included in the function report")
    parser.add_argument("--min-taken", type=float, default=0.0,
                        help="minimum Taken at least once percentage for threshold files")
    parser.add_argument("--min-function-taken", type=float, default=0.0,
                        help="minimum branch-taken percentage for filtered functions")
    parser.add_argument("--min-functions", type=int, default=0,
                        help="minimum number of filtered functions that must be reported")
    parser.add_argument("--function-threshold-exclude", action="append", default=[],
                        help="regex for filtered functions exempt from --min-function-taken")
    parser.add_argument("--required-file", action="append", default=[],
                        help="implementation source file that must appear in gcov output")
    parser.add_argument("--threshold-file", action="append", default=[],
                        help="required source file subject to --min-taken")
    args = parser.parse_args()

    files = parse_gcov(args.input)
    missing = [name for name in args.required_file if name not in files]
    threshold_files = set(args.threshold_file or args.required_file)
    failures = []
    lines = []

    for name in args.required_file:
        metrics = files.get(name, {})
        taken = metrics.get("Taken at least once")
        branch = metrics.get("Branches executed")
        line = metrics.get("Lines executed")
        call = metrics.get("Calls executed")
        if name in missing:
            continue
        if taken is None:
            failures.append(f"{name}: missing 'Taken at least once' metric")
            taken_text = "n/a"
        else:
            taken_text = f"{taken:.2f}%"
            if name in threshold_files and args.min_taken > 0.0 and taken < args.min_taken:
                failures.append(
                    f"{name}: Taken at least once {taken:.2f}% < {args.min_taken:.2f}%")
        lines.append(
            f"{name}: lines={line if line is not None else 'n/a'}% "
            f"branches={branch if branch is not None else 'n/a'}% "
            f"taken={taken_text} calls={call if call is not None else 'n/a'}%")

    if missing:
        failures.extend(f"{name}: missing from gcov output" for name in missing)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("\n".join(lines) + ("\n" if lines else ""),
                           encoding="utf-8")

    if args.function_output:
        if not args.gcov_dir:
            failures.append("--function-output requires --gcov-dir")
        else:
            exclude_patterns = [
                re.compile(pattern, re.IGNORECASE)
                for pattern in args.function_threshold_exclude
            ]
            functions = parse_function_gcov(
                args.gcov_dir, args.required_file, args.function_include)
            if not functions:
                failures.append("no functions matched the function coverage filter")
            elif args.min_functions > 0 and len(functions) < args.min_functions:
                failures.append(
                    f"matched {len(functions)} functions < required "
                    f"{args.min_functions}")
            function_lines = []
            for function in functions:
                branch_taken = function["branch_taken_pct"]
                function_lines.append(
                    f"{function['source']}::{function['name']}: "
                    f"lines={format_pct(function['line_taken_pct'])} "
                    f"blocks={format_pct(function['blocks_pct'])} "
                    f"branches={function['branches']} "
                    f"taken={format_pct(branch_taken)} "
                    f"calls={function['calls']} "
                    f"calls_taken={format_pct(function['call_taken_pct'])}")
                if (args.min_function_taken > 0.0 and branch_taken is not None
                        and branch_taken < args.min_function_taken
                        and not function_threshold_excluded(function, exclude_patterns)):
                    failures.append(
                        f"{function['source']}::{function['name']}: "
                        f"branch taken {branch_taken:.2f}% < "
                        f"{args.min_function_taken:.2f}%")
            args.function_output.parent.mkdir(parents=True, exist_ok=True)
            args.function_output.write_text(
                "\n".join(function_lines) + ("\n" if function_lines else ""),
                encoding="utf-8")

    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
