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
# The PolarDB integration topology assigns the writer to one hostgroup and the
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
    "writer_retry": (
        "PolarDB WAIT: strict wait timeout before user result; redirecting original"
    ),

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
        "PolarDB WAIT: saving timeout notice to session notice queue"),
    "sqlstate_57014": "[57014] LSN wait timeout",
    "reader_query_error": f"Error during query on ({READER_HG},",
    "backend_warning_100ms": "WARNING:  LSN wait timeout after 100 ms",
    "backend_error_100ms": "ERROR:  LSN wait timeout after 100 ms",

    # Transaction-split routing and retry markers.
    "plan_txn_split": f"PolarDB PLAN: REPLICA_TXN_SPLIT planned reader={READER_HG}",
    "txn_split_prepared": "PolarDB TXN_SPLIT: prepared",
    "txn_split_completed": "PolarDB TXN_SPLIT: completed split read",
    "txn_split_observed_primary": "PolarDB TXN_SPLIT: observed primary RFQ status=T",
    "txn_split_locking_read": "reason=split_locking_read",
    "txn_split_route_primary": "PolarDB PLAN: route=primary hint -> FORCE_PRIMARY",
    "txn_wait_plan": "PolarDB PLAN: transaction pre-write read -> reader wait path",
    "txn_wait_prepared": "PolarDB TXN_WAIT: prepared",
    "txn_wait_blocked_isolation": (
        "PolarDB TXN_WAIT: pre-write reader waits blocked by isolation"),
    "txn_wait_blocked_local_state": (
        "PolarDB TXN_WAIT: pre-write reader waits blocked by transaction local state"),
    "txn_wait_plan_blocked_rc": (
        "PolarDB PLAN: transaction pre-write reader wait blocked read_committed=0"),
    "txn_wait_plan_blocked_local_state": (
        "PolarDB PLAN: transaction pre-write reader wait blocked read_committed=1 local_state_clean=0"),
    "txn_split_warmup_queued": "PolarDB WARMUP: queued split pool request",
    "txn_split_warmup_added": "PolarDB WARMUP: added connected split pool connection",
    "txn_split_warmup_disabled": "PolarDB WARMUP: split lazy warmup disabled; skip request",
    "txn_split_warmup_suppressed_off": "PolarDB WARMUP: demand request suppressed mode=off",
    "txn_split_warmup_request_demand": "PolarDB WARMUP: requested split pool reason=demand",
    "txn_split_warmup_request_begin": "PolarDB WARMUP: requested split pool reason=begin",
    "set_warmup_off": "PolarDB SET: txn_split_warmup_mode=off",
    "set_warmup_demand": "PolarDB SET: txn_split_warmup_mode=demand",
    "set_warmup_begin": "PolarDB SET: txn_split_warmup_mode=begin",
    "set_warmup_both": "PolarDB SET: txn_split_warmup_mode=both",
    "txn_split_failure_policy_retry": "PolarDB FAILURE: policy action=retry",
    "txn_split_failure_retry_writer": "PolarDB FAILURE: retry split read on writer_hg",
    "txn_split_wrapper_result_retry": "PolarDB FAILURE: wrapper-result handler action=retry",
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
    split: bool = False
    split_variant: str = ""

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
        split = re.search(r"mode=\d+\s+split=1\s+xact=1", self.test_log) is not None
        split_variant = self._detect_split_variant() if split else ""
        if self.is_wait_timeout:
            mode = "mixed"
            wait_mode = self._match(r"wait_mode=([A-Za-z_]+)", "mixed")
            outcome = "success"
            split = False
            split_variant = ""
        return RunKind(mode=mode, wait_mode=wait_mode, outcome=outcome,
                       split=split, split_variant=split_variant)

    def _detect_split_variant(self) -> str:
        case_line_match = re.search(r"CASE \d+: ([^\n]+)", self.test_log)
        case_line = case_line_match.group(1).lower() if case_line_match else ""
        if "timeout" in case_line:
            return "timeout"
        if "warmup mode off" in case_line:
            return "warmup_off"
        if "warmup mode demand" in case_line:
            return "warmup_demand"
        if "warmup mode begin" in case_line:
            return "warmup_begin"
        if "warmup mode both" in case_line:
            return "warmup_both"
        if "lazy split warmup disabled" in case_line:
            return "lazy_disabled"
        if "lazy split warmup" in case_line:
            return "lazy"
        if "locking read veto" in case_line:
            return "for_update"
        if "repeatable read veto" in case_line:
            return "readonly_repeatable"
        if "set local veto" in case_line:
            return "readonly_set_local"
        if "read-only transaction" in case_line:
            return "readonly"
        if "route primary" in case_line:
            return "route_primary"
        if "pre-write" in case_line:
            return "prewrite"
        if "multi split" in case_line:
            return "multi"
        if "combined" in case_line:
            return "combined"
        return "basic"

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
        elif self.kind.mode == "lsn" and self.kind.split:
            self._audit_split()
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

    def _audit_split(self) -> None:
        self.check(TRACE["mode_lsn_final"] in self.proxy_debug, "missing final lsn mode")
        self.check(TRACE["txn_split_observed_primary"] in self.proxy_debug,
                   "missing primary RFQ observation for split run")

        variant = self.kind.split_variant
        if variant in ("basic", "multi", "combined"):
            self._check_count_eq(TRACE["plan_txn_split"], 2,
                                 "split plan count")
            self._check_count_eq(TRACE["txn_split_prepared"], 2,
                                 "split prepared count")
            self._check_count_eq(TRACE["txn_split_completed"], 2,
                                 "split completion count")
        elif variant == "prewrite":
            self._check_count_eq(TRACE["plan_txn_split"], 2,
                                 "split plan count")
            self._check_count_eq(TRACE["txn_split_prepared"], 2,
                                 "split prepared count")
            self._check_count_eq(TRACE["txn_split_completed"], 2,
                                 "split completion count")
            self._check_count_ge(TRACE["txn_wait_plan"], 1,
                                 "pre-write reader-wait plan count")
            self._check_count_ge(TRACE["txn_wait_prepared"], 1,
                                 "pre-write reader-wait prepared count")
        elif variant in ("lazy", "warmup_demand", "warmup_begin", "warmup_both"):
            self._check_count_ge(TRACE["txn_split_warmup_queued"], 1,
                                 "split warmup queue count")
            self._check_count_ge(TRACE["txn_split_warmup_added"], 1,
                                 "split warmup add count")
            self._check_count_ge(TRACE["txn_split_prepared"], 1,
                                 "lazy split prepared count")
            self._check_count_ge(TRACE["txn_split_completed"], 1,
                                 "lazy split completion count")
            if variant == "warmup_demand":
                self.check(TRACE["set_warmup_demand"] in self.proxy_debug,
                           "missing session warmup mode=demand SET trace")
                self.check(TRACE["txn_split_warmup_request_demand"] in self.proxy_debug,
                           "missing demand warmup request trace")
            elif variant == "warmup_begin":
                self.check(TRACE["set_warmup_begin"] in self.proxy_debug,
                           "missing session warmup mode=begin SET trace")
                self.check(TRACE["txn_split_warmup_request_begin"] in self.proxy_debug,
                           "missing begin warmup request trace")
            elif variant == "warmup_both":
                self.check(TRACE["set_warmup_both"] in self.proxy_debug,
                           "missing session warmup mode=both SET trace")
                self.check(TRACE["txn_split_warmup_request_begin"] in self.proxy_debug,
                           "missing begin warmup request trace")
        elif variant in ("lazy_disabled", "warmup_off"):
            if variant == "warmup_off":
                self.check(TRACE["set_warmup_off"] in self.proxy_debug,
                           "missing session warmup mode=off SET trace")
                self.check(TRACE["txn_split_warmup_suppressed_off"] in self.proxy_debug,
                           "missing mode=off warmup suppression trace")
                self.check(TRACE["txn_split_warmup_queued"] not in self.proxy_debug,
                           "mode=off unexpectedly queued warmup")
            else:
                self.check(TRACE["txn_split_warmup_disabled"] in self.proxy_debug,
                           "missing lazy-warmup-disabled trace")
            self.check(TRACE["txn_split_warmup_added"] not in self.proxy_debug,
                       "disabled/off warmup unexpectedly added a connection")
            self.check(TRACE["txn_split_prepared"] not in self.proxy_debug,
                       "disabled/off warmup unexpectedly prepared split read")
        elif variant == "for_update":
            self.check(TRACE["txn_split_locking_read"] in self.proxy_debug,
                       "missing locking-read veto trace")
            self.check(TRACE["plan_txn_split"] not in self.proxy_debug,
                       "locking-read case unexpectedly planned split")
        elif variant == "readonly":
            self._check_count_ge(TRACE["txn_wait_plan"], 1,
                                 "read-only transaction reader-wait plan count")
            self._check_count_ge(TRACE["txn_wait_prepared"], 1,
                                 "read-only transaction reader-wait prepared count")
            self.check(TRACE["plan_txn_split"] not in self.proxy_debug,
                       "readonly transaction unexpectedly planned split")
        elif variant == "readonly_repeatable":
            self.check(TRACE["txn_wait_blocked_isolation"] in self.proxy_debug,
                       "missing repeatable-read isolation veto trace")
            self.check(TRACE["txn_wait_plan_blocked_rc"] in self.proxy_debug,
                       "missing repeatable-read primary plan trace")
            self.check(TRACE["txn_wait_prepared"] not in self.proxy_debug,
                       "repeatable-read case unexpectedly prepared reader wait")
            self.check(TRACE["plan_txn_split"] not in self.proxy_debug,
                       "repeatable-read case unexpectedly planned split")
        elif variant == "readonly_set_local":
            self.check(TRACE["txn_wait_blocked_local_state"] in self.proxy_debug,
                       "missing SET LOCAL veto trace")
            self.check(TRACE["txn_wait_plan_blocked_local_state"] in self.proxy_debug,
                       "missing SET LOCAL primary plan trace")
            self.check(TRACE["txn_wait_prepared"] not in self.proxy_debug,
                       "SET LOCAL case unexpectedly prepared reader wait")
            self.check(TRACE["plan_txn_split"] not in self.proxy_debug,
                       "SET LOCAL case unexpectedly planned split")
        elif variant == "route_primary":
            self.check(TRACE["txn_split_route_primary"] in self.proxy_debug,
                       "missing route=primary split veto trace")
            self.check(TRACE["plan_txn_split"] not in self.proxy_debug,
                       "route=primary case unexpectedly planned split")
        elif variant == "timeout":
            self._check_count_eq(TRACE["plan_txn_split"], 1,
                                 "timeout split plan count")
            self._check_count_eq(TRACE["txn_split_wrapper_result_retry"], 1,
                                 "timeout wrapper-result retry trace count")
            self._check_count_eq(TRACE["txn_split_failure_policy_retry"], 1,
                                 "timeout retry policy trace count")
            self._check_count_eq(TRACE["txn_split_failure_retry_writer"], 1,
                                 "timeout writer retry trace count")
        else:
            self.failures.append(f"unknown split variant {variant!r}")

    def _check_count_eq(self, needle: str, expected: int, label: str) -> None:
        actual = self.count(self.proxy_debug, needle)
        self.check(actual == expected,
                   f"{label}: expected {expected}, got {actual}")

    def _check_count_ge(self, needle: str, expected: int, label: str) -> None:
        actual = self.count(self.proxy_debug, needle)
        self.check(actual >= expected,
                   f"{label}: expected >= {expected}, got {actual}")

    def _extract_write_lsn(self) -> str | None:
        match = LSN_WAIT_RE.search(self.proxy_debug)
        return match.group(1) if match else None

    def report(self) -> bool:
        status = "PASS" if not self.failures else "FAIL"
        split = f" split={self.kind.split_variant}" if self.kind.split else ""
        print(f"{status} {self.run_dir.name}: mode={self.kind.mode} "
              f"wait_mode={self.kind.wait_mode} outcome={self.kind.outcome}{split}")
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
