#!/usr/bin/env python3
"""
Audit PolarDB LSN-only integration run traces.

This script validates the persisted logs produced by the integration cases when
ProxySQL is built with POLARDB_DEBUG=1 (`make polardb-debug`). It checks the
combined PolarDB request-flow and FSM trace in proxysql_debug.log, with
proxysql.log retained as the full source log.

Usage:
  python3 test/polardb/tools/trace_analyzer.py
  python3 test/polardb/tools/trace_analyzer.py test/polardb/test_output/<run_dir>
  python3 test/polardb/tools/trace_analyzer.py --latest 6
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path


# ---------------------------------------------------------------------------
# Hostgroup identifiers
# ---------------------------------------------------------------------------
# The PolarDB integration topology pins the writer to one hostgroup and the
# reader (replica) to another. The defaults (writer=10, reader=11) match the
# admin config used by the TAP harness; override via env to track a different
# topology without editing the trace assertions below.
WRITER_HG = int(os.environ.get("POLARDB_WRITER_HG", "10"))
READER_HG = int(os.environ.get("POLARDB_READER_HG", "11"))


# ---------------------------------------------------------------------------
# PolarDB trace markers
# ---------------------------------------------------------------------------
# Every POLARDB_DEBUG=1 trace substring that the audit asserts on lives here so
# the trace-marker contract is auditable in one place and resistant to
# silent drift. Entries keyed by a short check name; hostgroup numbers are
# interpolated from WRITER_HG / READER_HG so they stay consistent with the
# topology constants above. Behavior is identical to the previous inline
# literals -- this is purely a centralization of the strings.
TRACE = {
    # Top-level request-flow markers (always expected / counted in the report).
    "collect": "PolarDB COLLECT:",
    "plan": "PolarDB PLAN:",
    "execute": "PolarDB EXECUTE:",
    "wait": "PolarDB WAIT:",
    "process_result": "PolarDB PROCESS_RESULT:",
    "conninfo_protocol_re": r"PolarDB CONNINFO: .*protocol=([A-Za-z0-9_]+)",

    # Startup-variable hygiene (must be absent).
    "invalid_value": "Invalid value",
    "stale_split_mode": "polardb_split_mode",

    # FSM trace tags (filtered view of proxysql.log).
    "fsm_handler": "[H",
    "fsm_runquery": "[RQ",
    "fsm_session_handler": "[SH",
    "fsm_wire": "[W",

    # Wait-timeout TAP markers.
    "wrap_finalize_stmts3": "PolarDB WRAP FINALIZE: wrapper_stmts=3",
    "wait_timeout_accounted": "PolarDB WAIT: timeout accounted",
    "writer_retry": "retrying original query on writer_hg=",

    # OFF mode.
    "mode_off_final": "consistency_mode: off",
    "plan_off_passthrough": "PolarDB PLAN: mode=OFF -> PASSTHROUGH",
    "replica_with_wait": "REPLICA_WITH_WAIT",
    "wait_wrap": "PolarDB WAIT WRAP:",

    # PRIMARY mode.
    "mode_primary_final": "consistency_mode: primary",
    "plan_primary_force": "PolarDB PLAN: mode=PRIMARY -> FORCE_PRIMARY",
    "execute_force_primary": f"PolarDB EXECUTE: FORCE_PRIMARY writer={WRITER_HG}",

    # LSN mode.
    "mode_lsn_final": "consistency_mode: lsn",
    "conninfo_reader_params_re": (
        rf"PolarDB CONNINFO: adding startup params to HG {READER_HG} "
        r"protocol=(v15|legacy)"),
    "plan_replica_with_wait": f"PolarDB PLAN: REPLICA_WITH_WAIT reader={READER_HG}",
    "execute_replica_prepared": "PolarDB EXECUTE: REPLICA_WITH_WAIT prepared",
    "wait_wrap_built": "PolarDB WAIT WRAP: built wrapped_query_len",
    "query_start_stmt_total3": "PolarDB QUERY_START: begin wrapped result, stmt_total=3",

    # LSN-mode strict / best_effort injection.
    "set_mode_strict": "SET polar_consistency_mode = 'strict'",
    "inject_strict": "injecting strict mode",
    "set_mode_best_effort": "SET polar_consistency_mode = 'best_effort'",
    "inject_best_effort": "injecting best_effort mode",

    # LSN-mode timeout outcomes.
    "best_effort_notice_save": (
        "PolarDB WAIT: saving timeout notice to session->pending_notices"),
    "sqlstate_57014": "[57014] LSN wait timeout",
    "reader_query_error": f"Error during query on ({READER_HG},",
    "backend_warning_100ms": "WARNING:  LSN wait timeout after 100 ms",
    "backend_error_100ms": "ERROR:  LSN wait timeout after 100 ms",
}

# Test-log markers asserted by the wait-timeout audit.
TEST_LOG = {
    "timeout_cleanup_done": "timeout cleanup assertions completed: failed=0",
    "client_warning_once": "client-visible 'LSN wait timeout' WARNING count = 1",
    "timeout0_best_effort": "timeout 0/best_effort emits no timeout warning",
    "timeout0_strict": "timeout 0/strict emits no timeout error",
}


LSN_WAIT_RE = re.compile(
    r"PROCESS_RESULT: write query digest='INSERT[^\n]*session_write_lsn=(\d+)"
)


@dataclass(frozen=True)
class RunKind:
    mode: str
    wait_mode: str
    outcome: str

    @property
    def lsn_mode(self) -> bool:
        return self.mode == "lsn"

    @property
    def failure(self) -> bool:
        return self.outcome == "failure"

    @property
    def strict(self) -> bool:
        return self.wait_mode == "strict"

    @property
    def best_effort(self) -> bool:
        return self.wait_mode == "best_effort"


class Audit:
    def __init__(self, run_dir: Path) -> None:
        self.run_dir = run_dir
        self.failures: list[str] = []
        self.test_log = self._read("test.log")
        self.proxy_debug = self._read("proxysql_debug.log")
        self.proxy_log = self._read("proxysql.log")
        self.replica_log = self._read("polardb_replica.log", required=False)
        self.is_wait_timeout = "timeout cleanup assertions completed" in self.test_log
        self.kind = self._detect_kind()

    def _read(self, name: str, required: bool = True) -> str:
        path = self.run_dir / name
        if not path.exists():
            if required:
                self.failures.append(f"missing {name}")
            return ""
        return path.read_text(errors="replace")

    def _detect_kind(self) -> RunKind:
        mode = self._match(r"pgsql-polardb_consistency_mode\|([A-Za-z_]+)", "unknown")
        wait_mode = self._match(r"pgsql-polardb_wait_timeout_mode\|([A-Za-z_]+)", "unknown")
        outcome = self._match(r"polar_mode=[A-Za-z_]+ outcome=([A-Za-z_]+)", "unknown")
        if self.is_wait_timeout:
            mode = "mixed"
            wait_mode = self._match(r"wait_mode=([A-Za-z_]+)", "mixed")
            outcome = "success"
        return RunKind(mode=mode, wait_mode=wait_mode, outcome=outcome)

    def _match(self, pattern: str, default: str) -> str:
        match = re.search(pattern, self.test_log)
        return match.group(1) if match else default

    def check(self, cond: bool, msg: str) -> None:
        if not cond:
            self.failures.append(msg)

    def count(self, text: str, needle: str) -> int:
        return text.count(needle)

    def startup_protocols(self) -> set[str]:
        return set(re.findall(TRACE["conninfo_protocol_re"], self.proxy_debug))

    def _test_passed(self) -> bool:
        return (
            "RESULT: PASS" in self.test_log
            or f"# {TEST_LOG['timeout_cleanup_done']}" in self.test_log
            or re.search(r"Failed=0", self.test_log) is not None
        )

    def audit(self) -> None:
        self.check(self._test_passed(), "test.log does not report a current PASS marker")
        self.check(TRACE["invalid_value"] not in self.proxy_debug, "invalid startup variable in proxysql_debug.log")
        self.check(TRACE["stale_split_mode"] not in self.proxy_debug, "stale polardb_split_mode in proxysql_debug.log")

        self.check(TRACE["collect"] in self.proxy_debug, "missing COLLECT trace")
        self.check(TRACE["plan"] in self.proxy_debug, "missing PLAN trace")
        self.check(TRACE["process_result"] in self.proxy_debug, "missing PROCESS_RESULT trace")
        self.check(bool(self.startup_protocols()), "missing startup-profile trace")

        self._audit_fsm_trace()

        if self.is_wait_timeout:
            self._audit_wait_timeout()
            return
        if self.kind.mode == "off":
            self._audit_off()
        elif self.kind.mode == "primary":
            self._audit_primary()
        elif self.kind.mode == "lsn":
            self._audit_lsn()
        else:
            self.failures.append(f"unknown consistency mode {self.kind.mode!r}")

    def _audit_fsm_trace(self) -> None:
        # proxysql_debug.log is a filtered view of proxysql.log and should carry
        # the PolarDB request-flow lines plus the local FSM tags.
        self.check(TRACE["fsm_handler"] in self.proxy_debug,
                   "missing [H] connection-handler FSM trace in proxysql_debug.log")
        self.check(TRACE["fsm_runquery"] in self.proxy_debug,
                   "missing [RQ] RunQuery FSM trace in proxysql_debug.log")
        self.check(TRACE["fsm_session_handler"] in self.proxy_debug,
                   "missing [SH] session-handler trace in proxysql_debug.log")
        self.check(TRACE["fsm_wire"] in self.proxy_debug,
                   "missing [W] wire-entry trace in proxysql_debug.log")


    def _audit_wait_timeout(self) -> None:
        self.check(TEST_LOG["timeout_cleanup_done"] in self.test_log,
                   "wait-timeout TAP did not finish with failed=0")
        self.check(TEST_LOG["client_warning_once"] in self.test_log,
                   "missing exact-once client warning assertion")
        self.check(TEST_LOG["timeout0_best_effort"] in self.test_log,
                   "missing timeout 0 best_effort assertion")
        self.check(TEST_LOG["timeout0_strict"] in self.test_log,
                   "missing timeout 0 strict assertion")
        self.check(TRACE["wrap_finalize_stmts3"] in self.proxy_debug,
                   "missing consistency wrapper finalize trace")
        self.check(TRACE["wait_timeout_accounted"] in self.proxy_debug,
                   "missing timeout accounting trace")
        self.check(TRACE["writer_retry"] in self.proxy_debug,
                   "missing wait-read writer retry trace")

    def _hostgroup_queries(self, hg: int) -> int | None:
        match = re.search(rf"^{hg}\s+\S+\s+(\d+)\s+", self.test_log, re.MULTILINE)
        return int(match.group(1)) if match else None

    def _audit_off(self) -> None:
        self.check(TRACE["mode_off_final"] in self.proxy_debug, "missing final off mode")
        self.check(TRACE["plan_off_passthrough"] in self.proxy_debug,
                   "missing mode OFF passthrough plan")
        self.check(TRACE["replica_with_wait"] not in self.proxy_debug, "off mode unexpectedly used REPLICA_WITH_WAIT")
        self.check(TRACE["wait_wrap"] not in self.proxy_debug, "off mode unexpectedly wrapped a wait")
        self.check(self._hostgroup_queries(READER_HG) == 0,
                   f"off mode used reader HG{READER_HG}")

    def _audit_primary(self) -> None:
        self.check(TRACE["mode_primary_final"] in self.proxy_debug, "missing final primary mode")
        self.check(TRACE["plan_primary_force"] in self.proxy_debug,
                   "missing primary FORCE_PRIMARY plan")
        self.check(TRACE["execute_force_primary"] in self.proxy_debug,
                   "missing primary FORCE_PRIMARY execute")
        self.check(TRACE["replica_with_wait"] not in self.proxy_debug,
                   "primary mode unexpectedly used REPLICA_WITH_WAIT")
        self.check(TRACE["wait_wrap"] not in self.proxy_debug,
                   "primary mode unexpectedly wrapped a wait")
        self.check(self._hostgroup_queries(READER_HG) == 0,
                   f"primary mode used reader HG{READER_HG}")

    def _audit_lsn(self) -> None:
        self.check(TRACE["mode_lsn_final"] in self.proxy_debug, "missing final lsn mode")
        self.check(re.search(TRACE["conninfo_reader_params_re"],
                             self.proxy_debug) is not None,
                   f"missing HG{READER_HG} startup params")
        self.check(TRACE["plan_replica_with_wait"] in self.proxy_debug,
                   "missing REPLICA_WITH_WAIT plan")
        self.check(TRACE["execute_replica_prepared"] in self.proxy_debug,
                   "missing REPLICA_WITH_WAIT execute")
        self.check(TRACE["wait_wrap_built"] in self.proxy_debug,
                   "missing built wrapped query")
        self.check(TRACE["wrap_finalize_stmts3"] in self.proxy_debug,
                   "missing wrap finalize stmt count")
        self.check(TRACE["query_start_stmt_total3"] in self.proxy_debug,
                   "missing wrapped-result begin")

        for pending in ("pending=2", "pending=1", "pending=0"):
            self.check(pending in self.proxy_debug, f"missing wrapper result consumption {pending}")

        write_lsn = self._extract_write_lsn()
        if write_lsn:
            self.check(f"write_lsn={write_lsn}" in self.proxy_debug,
                       f"COLLECT did not carry write_lsn {write_lsn}")
            self.check(f"wait_target={write_lsn}" in self.proxy_debug,
                       f"PLAN did not use wait_target {write_lsn}")
            self.check(f"prepared target={write_lsn}" in self.proxy_debug,
                       f"EXECUTE did not prepare target {write_lsn}")
            self.check(f"polar_xact_split_wait_lsn = '{write_lsn}'" in self.proxy_debug,
                       f"WRAP query missing target {write_lsn}")
        else:
            self.failures.append("missing INSERT process_result session_write_lsn")

        if self.kind.strict:
            self.check(TRACE["set_mode_strict"] in self.proxy_debug,
                       "missing strict mode SET in wrapped query")
            self.check(TRACE["inject_strict"] in self.proxy_debug,
                       "missing strict injection trace")
        elif self.kind.best_effort:
            self.check(TRACE["set_mode_best_effort"] in self.proxy_debug,
                       "missing best_effort mode SET in wrapped query")
            self.check(TRACE["inject_best_effort"] in self.proxy_debug,
                       "missing best_effort injection trace")

        if self.kind.failure and self.kind.best_effort:
            self.check(TRACE["best_effort_notice_save"] in self.proxy_debug,
                       "missing best-effort timeout notice save")
            self.check(TRACE["sqlstate_57014"] not in self.proxy_debug,
                       "best-effort unexpectedly logged SQLSTATE 57014")
            self.check(TRACE["backend_warning_100ms"] in self.replica_log,
                       "best-effort backend warning missing")

        if self.kind.failure and self.kind.strict:
            self.check(TRACE["sqlstate_57014"] in self.proxy_debug,
                       "strict timeout missing SQLSTATE 57014 in ProxySQL trace")
            self.check(TRACE["reader_query_error"] in self.proxy_debug,
                       "strict timeout missing reader error log")
            self.check(TRACE["backend_error_100ms"] in self.replica_log,
                       "strict backend ERROR missing")

    def _extract_write_lsn(self) -> str | None:
        match = LSN_WAIT_RE.search(self.proxy_debug)
        return match.group(1) if match else None

    def report(self) -> bool:
        status = "PASS" if not self.failures else "FAIL"
        print(f"{status} {self.run_dir.name}: mode={self.kind.mode} "
              f"wait_mode={self.kind.wait_mode} outcome={self.kind.outcome}")
        print(f"  debug: COLLECT={self.count(self.proxy_debug, TRACE['collect'])} "
              f"PLAN={self.count(self.proxy_debug, TRACE['plan'])} "
              f"EXECUTE={self.count(self.proxy_debug, TRACE['execute'])} "
              f"WRAP={self.count(self.proxy_debug, TRACE['wait_wrap'])} "
              f"PROCESS_RESULT={self.count(self.proxy_debug, TRACE['process_result'])} "
              f"WAIT={self.count(self.proxy_debug, TRACE['wait'])}")
        print(f"  fsm: H={self.count(self.proxy_debug, TRACE['fsm_handler'])} "
              f"RQ={self.count(self.proxy_debug, TRACE['fsm_runquery'])} "
              f"SH={self.count(self.proxy_debug, TRACE['fsm_session_handler'])} "
              f"W={self.count(self.proxy_debug, TRACE['fsm_wire'])}")
        for failure in self.failures:
            print(f"  - {failure}")
        return not self.failures


def default_output_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "test_output"


def latest_runs(n: int, output_dir: Path) -> list[Path]:
    runs = [p for p in output_dir.glob("run_*") if p.is_dir()]
    return sorted(runs, key=lambda p: p.stat().st_mtime, reverse=True)[:n][::-1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runs", nargs="*", type=Path,
                        help="Run directories to audit. Defaults to --latest 6.")
    parser.add_argument("--latest", type=int, default=6,
                        help="Audit the newest N run directories when no runs are supplied.")
    parser.add_argument("--output-dir", type=Path, default=default_output_dir(),
                        help="Integration test output directory.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runs = args.runs or latest_runs(args.latest, args.output_dir)
    if not runs:
        print(f"no run directories found under {args.output_dir}", file=sys.stderr)
        return 2

    ok = True
    for run in runs:
        audit = Audit(run)
        audit.audit()
        ok = audit.report() and ok
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
