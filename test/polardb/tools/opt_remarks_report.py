#!/usr/bin/env python3
"""Summarize PolarDB compiler/BOLT optimization diagnostics.

The script is intentionally tolerant: it reports whatever artifacts exist after
a remarks build instead of requiring every compiler mode to be present. Typical
inputs are:

  build/polardb-remarks/*.log          compiler build logs
  lib/obj/*.opt.yaml, src/obj/*.yaml   clang optimization records
  build/polardb-remarks/lto.*          ThinLTO remarks/stats/time trace
  build/polardb-pgo/*.profdata         clang PGO profile
  build/polardb-pgo-cs/*.profdata      clang CS-PGO profile
  build/polardb-bolt/bolt-optimize.log BOLT optimization diagnostics
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


POLARDB_SYMBOL_RE = re.compile(
    r"polardb|PolarDB|PgSQL_Session|PgSQL_Connection|PgSQL_HostGroups|"
    r"pgsql_query_digest|PgSQL_Query_Info|Query_Processor",
    re.IGNORECASE,
)

LOG_INTEREST_RE = re.compile(
    r"remark:|missed:|optimized:|Inlining|inline|profile|PGO|hotness|"
    r"coverage-mismatch|missing profile|BOLT-|dyno|reorder|split|cache",
    re.IGNORECASE,
)


@dataclass
class OptRecord:
    source: Path
    kind: str
    opt_pass: str = ""
    name: str = ""
    function: str = ""
    hotness: int = 0
    debug_file: str = ""
    debug_line: str = ""
    args: list[str] = field(default_factory=list)

    @property
    def text(self) -> str:
        parts = [
            self.kind,
            self.opt_pass,
            self.name,
            self.function,
            self.debug_file,
            self.debug_line,
            *self.args,
        ]
        return " ".join(p for p in parts if p)


def rel(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def first_tool(*names: str) -> str | None:
    for name in names:
        if "/" in name and Path(name).exists():
            return name
        found = shutil.which(name)
        if found:
            return found
    return None


def run_cmd(args: list[str], limit: int = 12000) -> str:
    try:
        proc = subprocess.run(
            args,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
    except OSError as exc:
        return f"command failed to start: {exc}"
    out = proc.stdout or ""
    if len(out) > limit:
        out = out[:limit] + "\n... truncated ...\n"
    return out.rstrip()


def find_opt_records(root: Path, remarks_dir: Path) -> list[Path]:
    candidates: list[Path] = []
    for base in (
        root / "lib" / "obj",
        root / "src" / "obj",
        root / "src",
        remarks_dir,
    ):
        if base.exists():
            candidates.extend(base.rglob("*.opt.yaml"))
            candidates.extend(base.rglob("*.opt.yml"))
    seen: set[Path] = set()
    out: list[Path] = []
    for path in candidates:
        resolved = path.resolve()
        if resolved not in seen:
            seen.add(resolved)
            out.append(path)
    return sorted(out)


def parse_scalar(line: str, key: str) -> str | None:
    prefix = f"{key}:"
    if not line.startswith(prefix):
        return None
    value = line[len(prefix) :].strip()
    if len(value) >= 2 and value[0] in "'\"" and value[-1] == value[0]:
        value = value[1:-1]
    return value


def parse_opt_records(paths: Iterable[Path]) -> list[OptRecord]:
    records: list[OptRecord] = []

    def finish(current: OptRecord | None) -> None:
        if current is not None:
            records.append(current)

    for path in paths:
        current: OptRecord | None = None
        in_args = False
        for raw in path.read_text(errors="replace").splitlines():
            line = raw.strip()
            match = re.match(r"^--- !([A-Za-z_][A-Za-z0-9_]*)", line)
            if match:
                finish(current)
                current = OptRecord(source=path, kind=match.group(1))
                in_args = False
                continue
            if current is None:
                continue
            if line.startswith("Args:"):
                in_args = True
                continue
            if line.startswith("DebugLoc:"):
                f = re.search(r"File:\s*([^,}]+)", line)
                ln = re.search(r"Line:\s*([0-9]+)", line)
                if f:
                    current.debug_file = f.group(1).strip().strip("'\"")
                if ln:
                    current.debug_line = ln.group(1)
                continue
            for key, attr in (
                ("Pass", "opt_pass"),
                ("Name", "name"),
                ("Function", "function"),
                ("Hotness", "hotness"),
            ):
                value = parse_scalar(line, key)
                if value is None:
                    continue
                if attr == "hotness":
                    try:
                        current.hotness = int(value)
                    except ValueError:
                        current.hotness = 0
                else:
                    setattr(current, attr, value)
                break
            else:
                if in_args and line.startswith("- "):
                    current.args.append(line[2:].strip())
        finish(current)
    return records


def counter_table(counter: collections.Counter[str], limit: int = 20) -> list[str]:
    if not counter:
        return ["  none"]
    width = max(len(str(v)) for v in counter.values())
    return [f"  {count:>{width}}  {key}" for key, count in counter.most_common(limit)]


def top_records(records: list[OptRecord], limit: int = 20) -> list[str]:
    if not records:
        return ["  none"]
    lines: list[str] = []
    for record in sorted(records, key=lambda r: r.hotness, reverse=True)[:limit]:
        loc = ""
        if record.debug_file:
            loc = f" {record.debug_file}"
            if record.debug_line:
                loc += f":{record.debug_line}"
        args = " ".join(record.args[:4])
        if len(args) > 180:
            args = args[:177] + "..."
        lines.append(
            f"  hot={record.hotness:<10} {record.kind:<8} "
            f"{record.opt_pass}:{record.name} {record.function}{loc} {args}".rstrip()
        )
    return lines


def summarize_opt_records(records: list[OptRecord], root: Path) -> list[str]:
    lines: list[str] = []
    lines.append("## Clang Optimization Records")
    lines.append("")
    lines.append(f"record_count: {len(records)}")
    if not records:
        lines.append("")
        lines.append("No clang `.opt.yaml` records found.")
        return lines

    by_kind = collections.Counter(r.kind for r in records)
    by_pass = collections.Counter(r.opt_pass for r in records if r.opt_pass)
    by_name = collections.Counter(f"{r.opt_pass}:{r.name}" for r in records if r.opt_pass or r.name)

    lines.append("")
    lines.append("### By Record Kind")
    lines.extend(counter_table(by_kind))
    lines.append("")
    lines.append("### By Pass")
    lines.extend(counter_table(by_pass))
    lines.append("")
    lines.append("### By Pass/Decision")
    lines.extend(counter_table(by_name))

    inline_records = [r for r in records if "inline" in r.opt_pass.lower() or "inline" in r.name.lower()]
    passed_inline = [r for r in inline_records if r.kind.lower() == "passed"]
    missed_inline = [r for r in inline_records if r.kind.lower() in ("missed", "analysis")]
    profile_records = [
        r
        for r in records
        if "pgo" in r.opt_pass.lower()
        or "profile" in r.opt_pass.lower()
        or "icall" in r.opt_pass.lower()
        or "pgo" in r.name.lower()
    ]
    vector_records = [
        r
        for r in records
        if "vector" in r.opt_pass.lower()
        or "vector" in r.name.lower()
        or "slp" in r.opt_pass.lower()
    ]
    polardb_records = [r for r in records if POLARDB_SYMBOL_RE.search(r.text)]

    for title, subset in (
        ("### Top Hot Passed Inline Decisions", passed_inline),
        ("### Top Hot Missed/Analyzed Inline Decisions", missed_inline),
        ("### Top Hot Profile-Guided Records", profile_records),
        ("### Top Hot Vectorization Records", vector_records),
        ("### Top Hot ProxySQL/PolarDB-Related Records", polardb_records),
    ):
        lines.append("")
        lines.append(title)
        lines.extend(top_records(subset))

    sources = collections.Counter(rel(r.source, root) for r in records)
    lines.append("")
    lines.append("### Record Files")
    lines.extend(counter_table(sources, limit=30))
    return lines


def interesting_log_lines(paths: Iterable[Path], limit: int = 120) -> list[str]:
    lines: list[str] = []
    for path in sorted(paths):
        try:
            text = path.read_text(errors="replace")
        except OSError:
            continue
        matched = [line for line in text.splitlines() if LOG_INTEREST_RE.search(line)]
        if not matched:
            continue
        lines.append(f"### {path}")
        for line in matched[:limit]:
            line = line.rstrip()
            if len(line) > 240:
                line = line[:237] + "..."
            lines.append(f"  {line}")
        if len(matched) > limit:
            lines.append(f"  ... {len(matched) - limit} more matching lines ...")
        lines.append("")
    return lines or ["  none"]


def summarize_logs(remarks_dir: Path, bolt_dir: Path) -> list[str]:
    log_paths: list[Path] = []
    for base in (remarks_dir, bolt_dir):
        if base.exists():
            log_paths.extend(base.glob("*.log"))
            log_paths.extend(base.glob("*.txt"))
    lines = ["## Build/BOLT Logs", ""]
    lines.extend(interesting_log_lines(log_paths))
    return lines


def summarize_profdata(pgo_dir: Path, cs_dir: Path) -> list[str]:
    lines = ["## LLVM Profile Data", ""]
    tool = first_tool(os.environ.get("LLVM_PROFDATA", ""), "llvm-profdata")
    if not tool:
        lines.append("llvm-profdata not found.")
        return lines

    profiles = [
        ("clang", pgo_dir / "clang.profdata"),
        ("clang-cs", cs_dir / "clang-cs.profdata"),
    ]
    for label, profile in profiles:
        lines.append(f"### {label}: {profile}")
        if not profile.exists():
            lines.append("  missing")
            lines.append("")
            continue
        lines.append(f"  size_bytes: {profile.stat().st_size}")
        out = run_cmd(
            [
                tool,
                "show",
                "--summary",
                "--detailed-summary",
                "--topn=40",
                "--counts",
                str(profile),
            ],
            limit=20000,
        )
        for line in out.splitlines()[:220]:
            lines.append(f"  {line}")
        lines.append("")
    return lines


def summarize_bolt(bolt_dir: Path) -> list[str]:
    lines = ["## BOLT Artifacts", ""]
    if not bolt_dir.exists():
        lines.append("BOLT directory missing.")
        return lines

    for name in ("proxysql.fdata", "proxysql.merged.fdata", "bolt-optimize.log"):
        path = bolt_dir / name
        if path.exists():
            lines.append(f"- {path}: {path.stat().st_size} bytes")
        else:
            lines.append(f"- {path}: missing")

    bolt_log = bolt_dir / "bolt-optimize.log"
    if bolt_log.exists():
        lines.append("")
        lines.append("### BOLT Decision Lines")
        for line in interesting_log_lines([bolt_log], limit=180):
            lines.append(line)
    return lines


def summarize_time_traces(root: Path, remarks_dir: Path) -> list[str]:
    paths: list[Path] = []
    for base in (root / "lib" / "obj", root / "src" / "obj", remarks_dir):
        if base.exists():
            paths.extend(base.rglob("*.json"))
    lines = ["## Time Trace Files", ""]
    if not paths:
        lines.append("  none")
        return lines
    lines.append(f"file_count: {len(paths)}")
    largest = sorted(paths, key=lambda p: p.stat().st_size if p.exists() else 0, reverse=True)[:20]
    for path in largest:
        size = path.stat().st_size
        label = ""
        try:
            data = json.loads(path.read_text(errors="replace"))
            label = data.get("traceEvents", [{}])[0].get("name", "")
        except Exception:
            label = ""
        suffix = f" {label}" if label else ""
        lines.append(f"  {size:>10}  {rel(path, root)}{suffix}")
    return lines


def build_report(args: argparse.Namespace) -> str:
    root = args.root.resolve()
    remarks_dir = args.remarks_dir.resolve()
    pgo_dir = args.pgo_dir.resolve()
    cs_dir = args.cs_dir.resolve()
    bolt_dir = args.bolt_dir.resolve()

    opt_paths = find_opt_records(root, remarks_dir)
    records = parse_opt_records(opt_paths)

    sections: list[list[str]] = []
    sections.append(
        [
            "# PolarDB Optimization Remarks Report",
            "",
            f"root: {root}",
            f"remarks_dir: {remarks_dir}",
            f"pgo_dir: {pgo_dir}",
            f"cs_dir: {cs_dir}",
            f"bolt_dir: {bolt_dir}",
            "",
        ]
    )
    sections.append(summarize_opt_records(records, root))
    sections.append(summarize_logs(remarks_dir, bolt_dir))
    sections.append(summarize_profdata(pgo_dir, cs_dir))
    sections.append(summarize_bolt(bolt_dir))
    sections.append(summarize_time_traces(root, remarks_dir))

    return "\n".join("\n".join(section).rstrip() for section in sections) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--remarks-dir", type=Path, default=Path("build/polardb-remarks"))
    parser.add_argument("--pgo-dir", type=Path, default=Path("build/polardb-pgo"))
    parser.add_argument("--cs-dir", type=Path, default=Path("build/polardb-pgo-cs"))
    parser.add_argument("--bolt-dir", type=Path, default=Path("build/polardb-bolt"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    report = build_report(args)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report)
    else:
        sys.stdout.write(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
