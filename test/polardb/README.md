# PolarDB LSN Integration Test Framework

## Table of Contents

- [1. Purpose](#1-purpose)
- [2. Environment Contract](#2-environment-contract)
- [3. Topology Selection](#3-topology-selection)
- [4. DCS / Replay-Lag Control](#4-dcs--replay-lag-control)
- [5. C Helper Tests](#5-c-helper-tests)
- [6. TAP Tests](#6-tap-tests)
- [7. Unit And Coverage Targets](#7-unit-and-coverage-targets)
- [8. Optional Benchmarks](#8-optional-benchmarks)
- [9. Output And Logs](#9-output-and-logs)
- [10. Prerequisites](#10-prerequisites)

## 1. Purpose

This directory holds the manual and integration tests for ProxySQL's PolarDB
LSN-only session-consistency feature — the behavior a compile can't check on its
own:

- the primary reports its LSN, and the session tracks it so a client reads its own writes;
- simple-query reads get wrapped with a wait, and the wrapper's own results are hidden;
- timeouts behave correctly for `best_effort`, `strict`, and timeout `0`;
- a real backend timeout is told apart from a user warning that just looks like one;
- reads respect the byte-lag cap when a replica is chosen;
- only simple-query reads are wrapped (extended protocol is out of scope here);
- PolarDB config fields survive a save and reload.

These tests need a live PolarDB primary/replica setup and a ProxySQL binary built
with `POLARDB_PROXY=1`. They aren't generic CI tests unless the CI runner provides
a PolarDB topology.

## 2. Environment Contract

The committed scripts are environment-neutral. Local endpoints, credentials,
DCS commands, and optional backend-log paths belong in an untracked env file.

Recommended setup:

```bash
cp test/polardb/.env.example test/polardb/.env
# edit test/polardb/.env for the local PolarDB/ProxySQL environment
```

All Makefile-driven runners and TAP scripts source `test/polardb/common/env.sh`. The loader reads, in order:

1. `POLARDB_ENV_FILE`, if set;
2. otherwise `test/polardb/.env`, if present.

The real `.env` file is ignored by git.

Directory layout:

- `common/env.sh` loads the single untracked `test/polardb/.env`.
- `lib/tap_core.sh` provides shared TAP output and prerequisite-skip helpers.
- `lib/scenario_harness.sh` provides shared topology, DCS/GUC, ProxySQL admin, logging, counter, and scenario-runner helpers.
- CSN and transaction-split helpers inside `lib/scenario_harness.sh` are postponed scaffold for the next feature round. v1 tests exercise only `off`, `lsn`, and `primary` LSN behavior.
- `lib/bench_harness.sh` provides benchmark-only worker, stats, and pgbench helpers.
- `test-c/run_helper.sh` is the only C-helper runner; use `make -C test/polardb run-c-*` targets in normal workflows.
- `tools/trace_analyzer.py` and `tools/coverage_summary.py` are standalone review tooling.
- `test-tap/` contains TAP integration tests.
- `test-bench/` contains manual benchmark scripts.

## 3. Topology Selection

Preferred input is a neutral endpoint list:

```bash
POLARDB_AUTODETECT=1
POLARDB_ENDPOINTS="<primary-host>:<primary-port> <replica-host>:<replica-port>"
```

The harness probes every endpoint with `SELECT pg_is_in_recovery()` and selects
exactly one primary plus the first available replica. This works for both
same-host/multi-port docker layouts and multi-host hardware layouts.

Manual override is also supported:

```bash
POLARDB_AUTODETECT=0
PRIMARY_HOST=<primary-host>
PRIMARY_PORT=<primary-port>
REPLICA_HOST=<replica-host>
REPLICA_PORT=<replica-port>
```

By default the manual endpoints are role-checked. Set
`POLARDB_SKIP_ROLE_CHECK=1` only for unusual environments where direct role
checking is not possible.

## 4. DCS / Replay-Lag Control

Timeout-edge tests need a way to change `polar_replay_min_lag_size`. Configure
one of these modes in `.env`.

Polarctl-style environment:

```bash
POLARDB_DCS_MODE=polarctl
POLARDB_DCS_BIN=polarctl
POLARDB_DCS_SCOPE=<cluster-scope>
POLARDB_DCS_SUDO=0
```

Docker-style environment:

```bash
POLARDB_DCS_MODE=docker
POLARDB_DOCKER_BIN=docker
POLARDB_DOCKER_CONTAINER=<container-name>
POLARDB_DOCKER_SUDO=0
```

If the environment cannot provide DCS control, set:

```bash
POLARDB_DCS_MODE=none
```

Tests that require replay-lag mutation will then fail or skip according to their
own prerequisite checks.

## 5. C Helper Tests

The C helpers read normal environment variables. Build the patched vendored
libpq, bundled pgbench, and helper binaries first:

```bash
make polardb-libpq
```

Then use the PolarDB test Makefile so the command names are discoverable:

```bash
make -C test/polardb help
make -C test/polardb run-c-libpq-lsn
make -C test/polardb run-c-extended-protocol QUERY="SELECT 1"
```

`run-c-extended-protocol` is a protocol probe for an already running and
configured ProxySQL frontend. Normal end-to-end coverage for that helper is in
`lsn_session_consistency_tap.sh`, which starts and configures ProxySQL before
calling the helper.

`make -C test/polardb build` delegates to `make -C test polardb`, which checks for PolarDB LSN symbols in the vendored libpq before compiling bundled pgbench and the helpers. If the check fails, rebuild with
`make polardb-libpq`; the helpers must not fall back to system libpq.

`test/polardb/test-c/run_helper.sh` owns the shared C-helper mechanics: `.env` loading, helper
build, vendored libpq `LD_LIBRARY_PATH`, and execution. For `libpq_lsn`, it also
maps the detected direct topology and direct credentials to the C test's
`POLARDB_HOST`, `POLARDB_PORT`, `POLARDB_REPLICA_HOST`,
`POLARDB_REPLICA_PORT`, `POLARDB_USER`, `POLARDB_PASSWORD`, and `POLARDB_DB`
variables.

The lower-level C-helper runner is `test/polardb/test-c/run_helper.sh`; prefer the Makefile targets for normal use.

The TAP scripts share `lib/tap_core.sh`, so missing local prerequisites are reported as valid TAP `SKIP` lines with a concrete build command instead of a shell traceback.

## 6. TAP Tests

### `config_roundtrip_tap.sh`

A standalone admin/config test. It starts its own ProxySQL (via
`common/proxysql_lifecycle.sh`) and checks that:

- the `replica_eligible` column in `pgsql_query_rules` survives a save and reload;
- the columns after `replica_eligible` keep their position (nothing shifts);
- the PolarDB policy columns in `pgsql_replication_hostgroups` survive a save and reload;
- the admin and cluster views list the columns in the same order.

### `lsn_session_consistency_tap.sh`

The main end-to-end test. It starts ProxySQL, sets up a primary hostgroup and a
replica hostgroup, and runs through the areas below. This README is the grouped
review catalog; the script keeps short comments next to each `case_*` function
for maintainers reading the executable test body.

- **Config round-trip and wrapper safety** — query-rule columns survive save/load; an unbuildable wait wrapper stops the read before dispatch instead of sending an unwrapped replica read.
- **RYW and basic routing modes** — the session moves forward only: reads are monotonic (read-after-read never goes backward) and honor the session's own writes (read-after-write); plus `off`/`primary` modes and manual routes.
- **Extended protocol routing** — Parse/Bind/Execute reads are not wait-wrapped in this version; manual routes are still honored.
- **Proxy-protocol RFQ scope** — `v15`/`legacy`/`off` and hostgroup-override negotiation of RFQ-LSN capability.
- **Startup identity fallback** — where the RFQ startup client address comes from, and rejecting the backend connection when it cannot be formed.
- **RFQ availability policy** — `strict` vs `best_effort` when no RFQ LSN is available, and the missing-LSN latches.
- **Route hints and session overrides** — `route=primary`, multi-statement queries, the session-level mode override, and `RESET ALL`.
- **Replica acquisition faults** — debug-fault coverage of the busy and unknown-LSN fallback paths.
- **Monitor LSN updates** — the background monitor LSN cache, kept separate from the per-query RFQ path.
- **Lag-cap and freshness routing** — the byte-lag cap, freshness gating, and cold-pool replica creation.
- **RFQ connection-profile reuse** — pooled replica connections reused or evicted by protocol compatibility.
- **Wrapper error attribution** — ordinary SQL errors and look-alike warnings are not mistaken for LSN wait timeouts.

Timeout-edge scenarios change the replica's replay lag, so they run only when you
set `POLARDB_TIMEOUT_EDGE_TESTS=1`.

### `wait_timeout_cleanup_tap.sh`

A focused test for timeout and notice behavior. It checks:

- `best_effort` with a finite timeout — both when the replica catches up in time and
  when it times out;
- `strict` with a finite timeout — both success and timeout;
- timeout `0`, which waits until the replica catches up;
- that exactly one timeout warning reaches the client after the backend consumes the
  wait target;
- that the timeout counter goes up exactly once;
- that the read is not retried when its result has already started streaming, when
  there is no known primary to fall back to, or when the primary connection is busy;
- that a warning whose text merely looks like a timeout is not counted as one unless
  the backend sends the structured timeout marker.

## 7. Unit And Coverage Targets

The PolarDB unit tests live under `test/polardb/test-unit/` and are built by the
PolarDB-owned Makefile target:

- `polardb_routing_lsn_unit-t` — monotonic session-target, wait-plan, baseline,
  RFQ-unavailable route policy, query-shape guards, writer-scope matrix
- `polardb_protocol_parse_unit-t` — node-type name mapping, monitor-health parsers,
  multi-statement detection
- `polardb_status_policy_unit-t` — route-action / reader-status names,
  NoticeResponse packet helpers, wrapper-error accounting, server-LSN cache reset
- `polardb_query_state_unit-t` — query-state named-reset subsets
- `polardb_startup_profile_unit-t`
- `polardb_hgm_lsn_unit-t`

The protocol-parse unit covers the monitor health parsing helpers and the
monitor-LSN update gate. Live monitor scheduling, invalid health roles/values, and
availability-driven shun behavior are covered by the LSN TAP.

Run the full PolarDB coverage checkpoint with:

```bash
make -C test/polardb coverage
```

The coverage target builds the dedicated `-O0` gcov/debug binary, runs the TAP
suite and PolarDB units, analyzes debug traces, and writes focused reports under
`COVERAGE_DIR` (default `/tmp/proxysql-polardb-coverage`).
Coverage percentage gates default to report-only during development. Set
`POLARDB_DEDICATED_TAKEN_MIN` or `POLARDB_FUNCTION_TAKEN_MIN` explicitly when a
branch wants enforced coverage floors.

## 8. Optional Benchmarks

The benchmark directory contains two different classes of manual scripts.

Short test scenarios are the small committed baseline set and are run by
`make -C test/polardb test-cases`:

- `test-case1.sh` - eventual-consistency baseline.
- `test-case2.sh` - session-LSN mode, including best_effort warning timeout and strict writer-retry timeout variants.
- `test-case5.sh` - primary-only baseline.

`make -C test/polardb bench` is kept as a compatibility alias for the short
scenario set. Heavy `bench*` targets remain separate.

Heavier benchmark/stress scripts are explicit targets and are not part of the
short `bench` target:

- `bench1_lsn_stress.sh` - mixed-load RYW stress benchmark.
- `bench2_lsn_offload.sh` - off/lsn/primary read-after-write offload comparison.
- `bench3_replica_lag.sh` - replica-lag correctness and lag-cap benchmark.
- `bench4_loaded_primary.sh` - loaded primary vs LSN offload benchmark.

Run them individually:

```bash
make -C test/polardb bench1
make -C test/polardb bench2
make -C test/polardb bench3
make -C test/polardb bench4
```

Or run all four heavy benchmarks:

```bash
make -C test/polardb bench-heavy
```

The heavy benchmarks are manual performance tools, not PR acceptance tests. They
should be run only in an environment where replay-lag DCS controls, backend log
paths, and bundled `pgbench` are configured.

Heavy benchmark routing defaults to writer (primary) HG `10`, reader (replica) HG `11`, and query-rule ID `10000`. Override `POLARDB_WRITER_HG`, `POLARDB_READER_HG`, or `POLARDB_SELECT_RULE_ID` in `.env` only when the local ProxySQL topology uses different IDs.

They use `PGBENCH_BIN` from `.env` only when explicitly overridden. By default,
the harness uses bundled PostgreSQL 16 `pgbench` built under
`deps/postgresql/postgresql/src/bin/pgbench/pgbench` by
`make -C test/polardb build`, so benchmark runs use the same patched libpq tree
as the PolarDB C helpers. Do not commit a local `pgbench` binary.

`test-bench/TEST_FRAMEWORK.md` documents the benchmark-specific purpose, knobs, and
promotion rule for adding or changing heavy benchmark scripts.

## 9. Output And Logs

TAP and benchmark output is written under `test/polardb/test_output/`, which is
ignored by git. ProxySQL logs default to the wrapper data directory and can be
overridden with `PROXYSQL_LOG`.

Backend log extraction is optional. Set `POLARDB_LOG_DIR` or explicit
`POLARDB_PRIMARY_LOG` / `POLARDB_REPLICA_LOG` in `.env` only when the local test
environment exposes backend logs on the filesystem.

## 10. Prerequisites

Meaningful end-to-end runs require a PolarDB backend with:

- RFQ-LSN support for `_polar_send_lsn` connections;
- `polar_xact_split_wait_lsn`;
- `polar_consistency_mode`;
- `polar_proxy_wait_timeout_ms`;
- structured timeout detail marker `polar_proxy_lsn_wait_timeout`;
- one-wait-per-read target consumption on supported PolarDB proxy builds.
