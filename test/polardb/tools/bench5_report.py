#!/usr/bin/env python3
"""Print aligned reports from bench5 consistency-shape benchmark runs.

Usage examples:
  test/polardb/tools/bench5_report.py 554
  test/polardb/tools/bench5_report.py 554 --view winners
  test/polardb/tools/bench5_report.py run_0554_bench5_consistency_shapes --case txn- --sort speedup
  make -C test/polardb bench5-report ARGS="554 --view all --sort speedup"
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable


SCRIPT = Path(__file__).resolve()
POLARDB_TEST_DIR = SCRIPT.parents[1]
DEFAULT_OUTPUT_DIR = POLARDB_TEST_DIR / "test_output"


INT_FIELDS = (
    "fresh",
    "stale",
    "bad",
    "error",
    "elapsed_ms",
    "tps",
    "writer_q",
    "reader_q",
    "waits",
    "split_total",
    "split_success",
    "split_fallback",
    "warmup_requested",
    "warmup_created",
    "warmup_avg_us",
)


@dataclass
class BenchRow:
    run_id: str
    run_dir: Path
    case: str
    shape: str
    mode: str
    warmup: str
    values: dict[str, int]
    primary_elapsed_ms: int | None = None

    @property
    def speedup(self) -> float | None:
        if not self.primary_elapsed_ms or self.values["elapsed_ms"] <= 0:
            return None
        return self.primary_elapsed_ms / self.values["elapsed_ms"]

    @property
    def is_primary(self) -> bool:
        return self.mode == "primary"

    @property
    def is_winner(self) -> bool:
        return not self.is_primary and self.speedup is not None and self.speedup > 1.0


def run_id_from_dir(path: Path) -> str:
    match = re.search(r"run_(\d+)", path.name)
    if match:
        return str(int(match.group(1)))
    return path.name


def resolve_run_arg(arg: str, output_dir: Path) -> Path:
    path = Path(arg).expanduser()
    if path.exists():
        if path.is_file():
            if path.name != "bench5_summary.tsv":
                raise SystemExit(f"{arg}: expected bench5_summary.tsv or a run dir")
            return path.parent
        if (path / "bench5_summary.tsv").exists():
            return path
        raise SystemExit(f"{arg}: no bench5_summary.tsv found")

    candidates: list[Path] = []
    if arg.isdigit():
        wanted = int(arg)
        for run_dir in output_dir.glob("run_*"):
            if not (run_dir / "bench5_summary.tsv").exists():
                continue
            match = re.search(r"run_(\d+)", run_dir.name)
            if match and int(match.group(1)) == wanted:
                candidates.append(run_dir)
    else:
        for run_dir in output_dir.glob(f"*{arg}*"):
            if (run_dir / "bench5_summary.tsv").exists():
                candidates.append(run_dir)

    if not candidates:
        raise SystemExit(f"{arg}: no bench5 run found under {output_dir}")
    if len(candidates) > 1:
        joined = "\n  ".join(str(p) for p in sorted(candidates))
        raise SystemExit(f"{arg}: ambiguous bench5 run:\n  {joined}")
    return candidates[0]


def parse_case(case: str) -> tuple[str, str, str]:
    parts = case.split(":")
    if len(parts) == 2:
        return parts[0], parts[1], "default"
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    raise SystemExit(f"invalid bench5 case key: {case}")


def read_run(run_dir: Path) -> list[BenchRow]:
    summary = run_dir / "bench5_summary.tsv"
    run_id = run_id_from_dir(run_dir)
    rows: list[BenchRow] = []

    with summary.open(newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        missing = [field for field in ("case", *INT_FIELDS) if field not in (reader.fieldnames or ())]
        if missing:
            raise SystemExit(f"{summary}: missing fields: {', '.join(missing)}")
        for raw in reader:
            shape, mode, warmup = parse_case(raw["case"])
            rows.append(
                BenchRow(
                    run_id=run_id,
                    run_dir=run_dir,
                    case=raw["case"],
                    shape=shape,
                    mode=mode,
                    warmup=warmup,
                    values={field: int(raw[field]) for field in INT_FIELDS},
                )
            )

    primary_by_shape = {
        row.shape: row.values["elapsed_ms"]
        for row in rows
        if row.mode == "primary"
    }
    for row in rows:
        row.primary_elapsed_ms = primary_by_shape.get(row.shape)
    return rows


def compile_filter(pattern: str | None) -> Callable[[str], bool]:
    if not pattern:
        return lambda _value: True
    regex = re.compile(pattern)
    return lambda value: regex.search(value) is not None


def apply_filters(rows: Iterable[BenchRow], args: argparse.Namespace) -> list[BenchRow]:
    case_match = compile_filter(args.case)
    shape_match = compile_filter(args.shape)
    modes = set(args.mode or [])
    warmups = set(args.warmup or [])
    filtered: list[BenchRow] = []

    for row in rows:
        if not case_match(row.case):
            continue
        if not shape_match(row.shape):
            continue
        if modes and row.mode not in modes:
            continue
        if warmups and row.warmup not in warmups:
            continue
        if args.min_speedup is not None:
            if row.speedup is None or row.speedup < args.min_speedup:
                continue
        if args.max_speedup is not None:
            if row.speedup is None or row.speedup > args.max_speedup:
                continue
        filtered.append(row)
    return filtered


def sort_rows(rows: list[BenchRow], args: argparse.Namespace) -> list[BenchRow]:
    if args.sort == "input":
        return rows

    def speed(row: BenchRow) -> float:
        return row.speedup if row.speedup is not None else -1.0

    key_funcs: dict[str, Callable[[BenchRow], object]] = {
        "run": lambda r: (int(r.run_id) if r.run_id.isdigit() else r.run_id, r.case),
        "case": lambda r: r.case,
        "shape": lambda r: (r.shape, r.mode, r.warmup),
        "mode": lambda r: (r.mode, r.shape, r.warmup),
        "warmup": lambda r: (r.warmup, r.shape, r.mode),
        "elapsed": lambda r: r.values["elapsed_ms"],
        "tps": lambda r: r.values["tps"],
        "speedup": speed,
        "reader": lambda r: r.values["reader_q"],
        "split": lambda r: r.values["split_success"],
        "fallback": lambda r: r.values["split_fallback"],
    }
    return sorted(rows, key=key_funcs[args.sort], reverse=args.desc)


def fmt_speedup(row: BenchRow) -> str:
    if row.speedup is None:
        return "-"
    return f"{row.speedup:.2f}x"


def table(rows: list[list[object]], labels: list[str]) -> str:
    text_rows = [[str(value) for value in row] for row in rows]
    widths = [len(label) for label in labels]
    for row in text_rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))

    out = [
        "  ".join(labels[index].ljust(widths[index]) for index in range(len(labels))),
        "  ".join("-" * widths[index] for index in range(len(labels))),
    ]
    left_aligned = {"run", "case"}
    for row in text_rows:
        cells = []
        for index, value in enumerate(row):
            if labels[index] in left_aligned:
                cells.append(value.ljust(widths[index]))
            else:
                cells.append(value.rjust(widths[index]))
        out.append("  ".join(cells))
    return "\n".join(out)


def full_table(rows: list[BenchRow], show_run: bool) -> str:
    labels = [
        "case", "fresh", "stale", "bad", "err", "ms", "tps", "writer",
        "reader", "waits", "split", "ok", "fallback", "wreq", "wnew",
        "wavg_us", "vs_primary",
    ]
    body = []
    for row in rows:
        body.append([
            row.case,
            row.values["fresh"],
            row.values["stale"],
            row.values["bad"],
            row.values["error"],
            row.values["elapsed_ms"],
            row.values["tps"],
            row.values["writer_q"],
            row.values["reader_q"],
            row.values["waits"],
            row.values["split_total"],
            row.values["split_success"],
            row.values["split_fallback"],
            row.values["warmup_requested"],
            row.values["warmup_created"],
            row.values["warmup_avg_us"],
            fmt_speedup(row),
        ])
    if show_run:
        labels.insert(0, "run")
        for row, out_row in zip(rows, body):
            out_row.insert(0, row.run_id)
    return table(body, labels)


def winners_table(rows: list[BenchRow], show_run: bool, winners: bool) -> str:
    selected = [row for row in rows if row.is_winner] if winners else [
        row for row in rows if not row.is_primary and not row.is_winner
    ]
    selected = sorted(selected, key=lambda row: row.speedup or -1.0, reverse=winners)
    labels = ["case", "primary_ms", "mode_ms", "speedup", "reader", "waits", "split_ok", "fallback"]
    body = []
    for row in selected:
        body.append([
            row.case,
            row.primary_elapsed_ms if row.primary_elapsed_ms is not None else "-",
            row.values["elapsed_ms"],
            fmt_speedup(row),
            row.values["reader_q"],
            row.values["waits"],
            row.values["split_success"],
            row.values["split_fallback"],
        ])
    if show_run:
        labels.insert(0, "run")
        for row, out_row in zip(selected, body):
            out_row.insert(0, row.run_id)
    return table(body, labels)


def print_section(title: str, content: str) -> None:
    print(title)
    print(content if content.strip() else "(no rows)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print aligned reports from bench5_summary.tsv files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("runs", nargs="+",
                        help="Run IDs, run directory names, run directories, or bench5_summary.tsv paths")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR,
                        help="Directory containing run_* benchmark output directories")
    parser.add_argument("--view", choices=("full", "winners", "neutral", "all"),
                        default="full", help="Report section to print")
    parser.add_argument("--case", help="Regex filter for the full case key")
    parser.add_argument("--shape", help="Regex filter for the shape prefix")
    parser.add_argument("--mode", action="append", choices=("primary", "lsn", "split", "off"),
                        help="Keep only this mode; can be passed more than once")
    parser.add_argument("--warmup", action="append",
                        choices=("default", "off", "demand", "begin", "both"),
                        help="Keep only this warmup mode; can be passed more than once")
    parser.add_argument("--min-speedup", type=float,
                        help="Keep rows with vs_primary at or above this value")
    parser.add_argument("--max-speedup", type=float,
                        help="Keep rows with vs_primary at or below this value")
    parser.add_argument("--sort",
                        choices=("input", "run", "case", "shape", "mode", "warmup", "elapsed",
                                 "tps", "speedup", "reader", "split", "fallback"),
                        default="input", help="Sort key for the full table")
    parser.add_argument("--desc", action="store_true",
                        help="Reverse the selected full-table sort")
    parser.add_argument("--run-column", choices=("auto", "always", "never"), default="auto",
                        help="Control whether output includes a run column")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_dirs = [resolve_run_arg(run, args.output_dir) for run in args.runs]
    rows: list[BenchRow] = []
    for run_dir in run_dirs:
        rows.extend(read_run(run_dir))
    rows = apply_filters(rows, args)
    rows = sort_rows(rows, args)

    show_run = args.run_column == "always" or (
        args.run_column == "auto" and len(run_dirs) > 1
    )

    if args.view in ("full", "all"):
        print_section("Full Table", full_table(rows, show_run))
    if args.view == "all":
        print()
    if args.view in ("winners", "all"):
        print_section("Winners", winners_table(rows, show_run, winners=True))
    if args.view == "all":
        print()
    if args.view in ("neutral", "all"):
        print_section("Neutral Or Slower", winners_table(rows, show_run, winners=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
