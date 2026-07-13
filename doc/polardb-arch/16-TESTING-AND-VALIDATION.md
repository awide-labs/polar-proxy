# 16 - Testing and Validation

> Scope: committed test coverage for the PolarDB LSN-only feature, how to run it neutrally, and which checks remain environment-dependent. | Audience: M/C | Status: stable | Prereqs: [02-BUILD-TOGGLE-AND-LIBPQ.md](02-BUILD-TOGGLE-AND-LIBPQ.md), [06-ROUTING-PIPELINE.md](06-ROUTING-PIPELINE.md), [08-WAIT-TIMEOUT-AND-NOTICES.md](08-WAIT-TIMEOUT-AND-NOTICES.md), [14-INVARIANTS-AND-FAILURE-MODES.md](14-INVARIANTS-AND-FAILURE-MODES.md)

## 1. Committed Test Assets

| Test asset | Purpose | Backend needed? | ProxySQL needed? |
|---|---|---:|---:|
| `scripts/verify-polardb-libpq-lsn-patch.sh` | Verifies the PolarDB LSN/Xact libpq patch applies cleanly after the upstream libpq patches and rejects CSN symbols. | no | no |
| `test/polardb/Makefile` | Discoverable entry point for C helpers, TAP tests, committed benchmarks, cleanup, and trace analysis. | target-dependent | target-dependent |
| `test/polardb/test-c/libpq_lsn_test.c` | Standalone C smoke test for patched libpq RFQ-LSN API and the backend timeout detail marker. | optional; skips gracefully when no backend is reachable | no |
| `test/polardb/test-c/libpq_xact_test.c` | Standalone C smoke test for staged RFQ transaction-split libpq accessors, plus strict probes for backend `w` RFQ marker and isolation `ParameterStatus` support; it does not enable ProxySQL split routing. | optional; strict probes require matching backend support | no |
| `test/polardb/test-c/run_helper.sh` | Common C-helper runner; loads `.env`, builds the selected helper against vendored patched libpq, sets `LD_LIBRARY_PATH`, and executes it. | helper-dependent | helper-dependent |
| `test/polardb/test-c/proxysql_extended_protocol_test.c` | Small libpq client that sends the final query through extended protocol. | no direct backend | yes |
| `test/polardb/test-c/libpq_row_run_test.c` | Offline C unit test for the patched-libpq DataRow row-run API (`PSpeekRowRun` / `PSadvanceInput` / `PSdetachRowRun`); builds a minimal receive buffer and validates the accessors before ProxySQL consumes them. | no | no |
| `test/polardb/test-c/polardb_lsn_trace.c` | Direct-to-PolarDB LSN trace shim (libpq preload) that reads `PQhasLSN()`/`PQgetLSN()` after each ReadyForQuery and prints one parseable, client-visible consistency verdict per request. | yes | no |
| `test/polardb/test-tap/config_roundtrip_tap.sh` | ProxySQL config/admin round-trip TAP for `replica_eligible` and PolarDB hostgroup policy columns. | no | yes |
| `test/polardb/test-tap/lsn_session_consistency_tap.sh` | Main end-to-end LSN session-consistency TAP. | yes | yes |
| `test/polardb/test-tap/wait_timeout_cleanup_tap.sh` | Focused wait-timeout/notice/structured-marker TAP. | yes | yes |
| `test/polardb/test-tap/global_lsn_consistency_tap.sh` | GLOBAL_LSN-mode end-to-end TAP: every automatic read waits on `max(session target, writer mirror LSN)`, a missing writer mirror fails closed to the writer, and non-READ COMMITTED transactions stay on the writer. | yes | yes |
| `test/polardb/test-tap/rfq_lsn_lifecycle_tap.sh` | Focused RFQ-LSN capture lifecycle TAP: an RFQ payload of value 0 on setup statements is not collapsed into "payload absent", so the session is not poisoned as missing-LSN and later reads still offload. | yes | yes |
| `test/polardb/test-tap/txn_split_tap.sh` | Transaction-split read routing TAP; a thin adapter over `lib/scenario_harness.sh`, which owns ProxySQL lifecycle, split SQL/WAL generation, pgbench execution, and routing evidence. | yes | yes |
| `test/polardb/test-tap/txn_split_failure_policy_tap.sh` | Split reader-failure policy TAP driven by DEBUG-only fault injection: retry/forward/terminate classification, retry-decline cleanup, writer-state loss, and the writer route overriding a later manual reader route. | yes | yes |
| `test/polardb/test-unit/polardb_routing_lsn_unit-t.cpp` | Monotonic-LSN routing core: SESSION_LSN monotonic target and wait-plan construction, first-read baseline seeding, RFQ-unavailable route policy, query-shape checks, and the positioned-RFQ source / writer-scope match matrix. | no | no |
| `test/polardb/test-unit/polardb_protocol_parse_unit-t.cpp` | PolarDB protocol/string parsing helpers: node-type name mapping and writer/reader classification, monitor-health parse helpers, and simple-query multi-statement detection. | no | no |
| `test/polardb/test-unit/polardb_query_state_unit-t.cpp` | `PolarDB_QueryState` named-reset semantics: scoped per-query reset helpers (reset_reader_target, reset_wait, reset_dispatch_wrapper, reset_for_new_query) and which fields each clears versus preserves. | no | no |
| `test/polardb/test-unit/polardb_status_policy_unit-t.cpp` | Status-name and policy helpers: route-action-reason names and NoticeResponse helpers, reader-status names and writer-redirect policy, wrapper-error accounting policy, and server-LSN cache reset. | no | no |
| `test/polardb/test-unit/polardb_startup_profile_unit-t.cpp` | PolarDB startup-profile request semantics: profile request bits and protocol mapping, fallback-identity validation, sockaddr identity resolution, and profile string converters. | no | no |
| `test/polardb/test-unit/polardb_hgm_lsn_unit-t.cpp` | PolarDB HostGroups Manager LSN state: counter metadata and thread-counter aggregation, writer-epoch LSN-cache reset, and thread-local fresh-LSN reader targeting. | no | no |
| `test/polardb/test-unit/pgsql_status_variables_unit-t.cpp` | PgSQL generic thread status-variable storage sizing: keeps PgSQL `stvar[]` sized to the shared `st_var` index range so high-index generic counters do not write past the array. | no | no |
| `test/polardb/test-unit/polardb_client_rfq_unit-t.cpp` | Client-facing ReadyForQuery LSN packet building: the RFQ packet stays byte-for-byte unchanged unless the client opted in and the backend RFQ carried a PolarDB LSN, in which case one uint64 LSN is appended after the transaction-status byte. | no | no |
| `test/polardb/lib/tap_core.sh` | Common TAP writer and prerequisite-skip helpers (clean SKIP output). | no | no |
| `test/polardb/lib/tap_polardb.sh` | Shared PolarDB TAP primitives (admin/proxy SQL wrappers, stats-counter readers, query-rule and poller helpers). | no | yes |
| `test/polardb/lib/bench_harness.sh` | Benchmark-only helpers for PolarDB scenarios. | yes for most scenarios | yes |
| `test/polardb/lib/scenario_harness.sh` | Shared PolarDB scenario test harness (topology, ProxySQL, stats, logging, scenario helpers). | yes for most scenarios | yes |
| `test/polardb/tools/trace_analyzer.py` | Optional debug-trace validator for logs captured by integration runs (invoked via the `trace-analyze`/`analyze-traces` Makefile targets). | no | captured logs |

The committed benchmark scripts under `test/polardb/test-bench/` are manual tools,
not mandatory PR acceptance tests. Only case1, case2, and case5 are part of the
current PR surface; heavier stress/offload/phase benchmarks are postponed until
their setup and assertions are cleaned up.

## 2. Environment Model

The committed tests are environment-neutral. Real endpoints and credentials must
come from environment variables or an untracked `.env` file.

Recommended setup:

```bash
cp test/polardb/.env.example test/polardb/.env
# edit test/polardb/.env for the local PolarDB and ProxySQL endpoints
```

All Makefile-driven runners and TAP scripts source `test/polardb/common/env.sh`. The loader
reads `POLARDB_ENV_FILE` if set; otherwise it reads `test/polardb/.env`.

The preferred topology input is:

```bash
POLARDB_AUTODETECT=1
POLARDB_ENDPOINTS="<writer-host>:<writer-port> <reader-host>:<reader-port>"
```

The harness probes each endpoint with `SELECT pg_is_in_recovery()` and selects
one writer plus the first reader. Manual `PRIMARY_*` / `REPLICA_*` variables are
also supported when autodetection is disabled.

Replay-lag tests require one DCS mode in `.env`:

```bash
POLARDB_DCS_MODE=polarctl
POLARDB_DCS_BIN=polarctl
POLARDB_DCS_SCOPE=<cluster-scope>
POLARDB_DCS_SUDO=0
```

or:

```bash
POLARDB_DCS_MODE=docker
POLARDB_DOCKER_BIN=docker
POLARDB_DOCKER_CONTAINER=<container-name>
POLARDB_DOCKER_SUDO=0
```

Private hosts, local filesystem paths, and local DCS scope names must stay in
`.env` or external runbooks, not in committed docs or scripts.

## 3. Patch-Apply Check

`scripts/verify-polardb-libpq-lsn-patch.sh` validates the patch stack without
building ProxySQL:

1. Find the vendored PostgreSQL tarball.
2. Apply the upstream libpq patches in the required order.
3. Dry-run apply `deps/postgresql/polardb_libpq.patch` last.
4. Reject fuzz, offset, failed hunks, and `.orig` residue.
5. Grep the PolarDB patch for required LSN/Xact symbols and reject CSN symbols.

A passing run shows the libpq extension still applies cleanly as the staged
LSN/Xact patch. It does not show runtime RYW behavior or transaction-split
routing.

## 4. libpq LSN Smoke Test

Build the patched vendored libpq, bundled pgbench, and helper binaries first:

```bash
make polardb-libpq
```

Then use the PolarDB test Makefile so the command names are discoverable:

```bash
make -C test/polardb help
make -C test/polardb run-c-libpq-lsn
make -C test/polardb run-c-extended-protocol QUERY="SELECT 1"
```

`run-c-extended-protocol` expects an already running and configured ProxySQL
frontend. The normal end-to-end coverage for that helper is the main LSN TAP,
which starts ProxySQL, configures writer/reader hostgroups, then calls the
helper to force the final query through extended protocol.

`make -C test/polardb build` delegates to `make -C test polardb`, which builds bundled pgbench and the helper binaries. If that reports missing vendored PostgreSQL headers or libpq, run `make polardb-libpq`; the standalone helper requires the PolarDB
LSN-patched libpq and must not silently fall back to system libpq.

The underlying C test connects directly to PolarDB, not through ProxySQL. It
verifies:

1. `_polar_send_lsn` / `_polar_proxy_send_lsn` connection-string options require
   proxy identity and are accepted with one.
2. `PQgetLSN()` / `PQhasLSN()` expose an RFQ LSN after a query.
3. LSN does not regress across a write.
4. `PQsetPolarSendLSN()` is callable.
5. A replica accepts `SET polar_xact_split_wait_lsn = '<lsn>'` when reachable.
6. A strict wait timeout exposes
   `PG_DIAG_MESSAGE_DETAIL = polar_proxy_lsn_wait_timeout`.

Missing backend/replica cases are skipped rather than hard-failed.

## 5. ProxySQL Integration TAPs

### `config_roundtrip_tap.sh`

Validates admin/config persistence:

- `pgsql_query_rules.replica_eligible` survives config save/load;
- later query-rule fields are not shifted;
- `pgsql_replication_hostgroups` PolarDB policy columns survive config save/load;
- the cluster/admin surfaces preserve the same field order.

### `lsn_session_consistency_tap.sh`

Validates the main runtime data path:

- writer query advances session LSN;
- following simple-query read routes to reader with wait wrapping;
- wrapper SET results are hidden from the client;
- no-write reads, `off`, and `primary` modes behave correctly;
- `RESET ALL` clears staged query state but keeps session LSN evidence for the
  next protected read;
- manual hostgroup routing bypasses automatic PolarDB planning;
- explicit transaction reads route to the writer instead of a replica;
- monitor-disabled mode separates RFQ and monitor counters;
- `replica_eligible` round-trip is preserved;
- byte lag cap and cold reader pool behavior are covered;
- RFQ-unavailable strict and `best_effort` routing behavior are covered;
- startup identity safety is covered by a debug one-shot fault:
  an RFQ-startup connection attempt with no usable identity is rejected, the
  error is logged, and ProxySQL continues serving after the fault clears;
- route-primary hint, multi-statement, explicit transaction, and session
  override transitions are covered;
- debug fault injection covers reader-busy retry handoff and
  reader-LSN-unknown writer fallback;
- debug monitor-health injection covers invalid-role (unroutable node_type) and
  invalid availability/LSN-value handling, valid unavailable-row shunning, and
  route restoration after shun;
- extended protocol follows this feature's policy: manual reader route is honored, automatic
  no-write extended read may use reader, automatic after-write extended read
  uses writer without wrapper.

Timeout-edge checks are opt-in with `POLARDB_TIMEOUT_EDGE_TESTS=1` because they
mutate replay lag.

### `wait_timeout_cleanup_tap.sh`

Validates timeout and notice behavior:

- finite `best_effort` success and timeout;
- finite `strict` success and timeout;
- timeout `0` waits until catchup or external interruption;
- backend timeout warning reaches the client once;
- timeout counters increment exactly once;
- timed-out wait reads retry once on the writer when safe;
- no-retry predicate outcomes are covered for result already started, unknown
  fallback writer, and busy writer stream;
- user warnings that contain timeout-looking text are not counted without the
  structured backend detail marker.

## 6. Optional Trace and Benchmark Tools

`trace_analyzer.py` checks captured debug logs when ProxySQL is built with
PolarDB debug traces. It is useful for reviewer evidence but is not required for
normal runs.

The committed benchmark scripts are manual performance tools:

- `test-case1.sh` - eventual-consistency baseline.
- `test-case2.sh` - session-LSN mode with best_effort/strict success and timeout variants through `ARGS`.
- `test-case5.sh` - primary-only baseline.

These case scripts are committed under `test/polardb/test-bench/` (Makefile
targets `test-case1`/`test-case2`/`test-case5`). The heavier stress/offload/lag
benchmarks are committed as `bench1_lsn_stress.sh`, `bench2_lsn_offload.sh`,
`bench3_replica_lag.sh`, `bench4_loaded_primary.sh`, and
`bench5_consistency_shapes.sh` under
`test/polardb/test-bench/` (Makefile targets `bench1`..`bench5`); they are
postponed outside the committed PR surface until their scenario setup and
assertions are cleaned up.

They use `PGBENCH_BIN` from `.env` only when explicitly overridden. By default,
the harness uses bundled PostgreSQL 16 `pgbench` built under
`deps/postgresql/postgresql/src/bin/pgbench/pgbench` by
`make -C test/polardb build`, so benchmark runs use the same patched libpq tree
as the PolarDB C helpers.

## 7. Coverage Improvement Plan

Line coverage alone is not enough for this feature. Recent coverage runs showed
that the dedicated PolarDB files have strong line coverage, but several
important decisions are still only exercised on one side. For review purposes,
track **branches taken at least once**, not only **branches executed**. A branch
can be "executed" while only one outcome is ever taken, which hides missing
negative and error-path coverage.

Coverage reports must also include the PolarDB code that lives in shared files:

- `PgSQL_Session.cpp`: wait-read retry capture, retry predicate, reader release,
  writer re-dispatch, and session cleanup.
- `PgSQL_Connection.cpp`: wrapper-result filtering, structured timeout-error
  detection, and connection-owned wrap state.
- `PgSQL_HostGroups_Manager.cpp`: reader acquisition status selection and lag
  enforcement.
- `PgSQL_Monitor.cpp`: producer-side health sampling for node type,
  availability, and server LSN positions.
- `PgSQL_Thread.cpp`: status-counter export and configuration conversion.

Whole-file percentages for these shared files are not acceptance criteria,
because most of each file is unrelated to PolarDB. The coverage job should either
filter to PolarDB functions/ranges or keep a separate raw `gcov` summary for
those functions. The committed coverage target writes both:

- `implementation-summary.txt`: file-level line, branch, taken-at-least-once,
  and call coverage for every required implementation file.
- `function-summary.txt`: filtered function-level coverage for PolarDB-named
  functions in shared files and for the PolarDB-specific helpers in dedicated
  files.

When generating raw implementation `gcov` files, run `gcov` from the source
directory that contains the `.cpp` files; otherwise some tools can emit
header-only stubs and hide the implementation lines.

### 7.1 First Priority: Make Coverage Reproducible

Add one discoverable test target that performs the full coverage run:

1. Build with `POLARDB_PROXY=1`, `POLARDB_DEBUG=1`, `-O0`, and gcov
   instrumentation. The `-O0` build keeps function attribution meaningful; an
   optimized build can inline small helpers and make per-function coverage
   misleading.
2. Run the config TAP, LSN TAP, wait-timeout TAP, and PolarDB unit tests.
3. Run the trace analyzer against the newest debug TAP output.
4. Run the `coverage_summary.py` parser self-test (`make -C test/polardb
   coverage-parser-selftest`, which executes `tools/tests/coverage_summary_unit.py`)
   before trusting generated coverage data.
5. Generate a focused `gcovr` summary and a raw implementation summary.
6. Report line coverage, branches executed, and branches taken at least once.
7. Fail if raw `.cpp` coverage is missing for any PolarDB implementation file.

The target should write artifacts under a caller-selected directory, defaulting
to a temporary path outside the source tree. Generated `.gcov` files must not be
left in the repository working tree.

Use `make -C test/polardb coverage` for the repeatable run. Set
`COVERAGE_DIR` to keep artifacts in a chosen directory. The default build target
is `POLARDB_COVERAGE_BUILD_TARGET=polardb-coverage-debug`; override it only when
intentionally comparing another build mode.

The coverage target reports coverage by default and keeps percentage controls
disabled unless the caller opts in. During development, this keeps the run useful
even when known branches are still being staged. To turn the report into a condition,
set these variables explicitly:

- `POLARDB_DEDICATED_TAKEN_MIN`: minimum "taken at least once" percentage for the
  dedicated PolarDB implementation files.
- `POLARDB_FUNCTION_TAKEN_MIN`: minimum branch-taken percentage for filtered
  PolarDB functions in shared files and dedicated helpers.
- `POLARDB_FUNCTION_MIN`: minimum number of filtered functions required in the
  function report.

`POLARDB_TAKEN_MIN` remains as a compatibility alias for the dedicated-file
threshold. `POLARDB_FUNCTION_TAKEN_EXCLUDES` names staged scenarios to exempt
when an opt-in function condition is enabled. The staged exceptions should shrink as
their deterministic TAP or debug-injection tests are added. Do not apply one
whole-file threshold to the shared files; their useful signal is the filtered
function report, not the file-level percentage.

Every coverage-closing test must assert behavior, not just execute code. Prefer
client-visible results plus status counters. Trace strings are useful debug-tier
evidence, but they are not the primary acceptance signal.

### 7.2 First Priority: Close Confirmed Untested Decisions

The normal TAP/unit suite now covers the deterministic cases that can be staged
without timing-dependent failures. Prefer asserting status counters and
client-visible behavior over matching trace text; use trace checks only as
additional evidence in debug builds.

1. **RFQ-unavailable routing policy.**
   The LSN TAP covers effective proxy protocol `off`, strict routing to the
   writer, `best_effort` reader degradation with one NoticeResponse, plan-stage
   degradation after a missing writer RFQ LSN, and incompatible pooled-reader
   replacement after the protocol profile changes. It also verifies that the
   RFQ-capable replacement is reused without another profile skip or eviction on
   a later protected read.

2. **Reader acquisition status branches.**
   Induce status outcomes with configuration rather than timing races. The suite
   covers:
   - `RFQ_UNAVAILABLE`: reader lacks usable RFQ LSN support.
   - `PRIMARY_LSN_UNKNOWN`: primary baseline is requested but no primary LSN
     sample exists.
   - `READER_LSN_STALE`: reader sample is older than the freshness window.
   - `READER_LAG_EXCEEDED`: `max_lag_bytes` is lower than the measured byte lag.

   A debug-injection test covers the session policy for `READER_BUSY`: the first
   reader acquisition reports busy, the query stays in ProxySQL's normal
   no-connection retry path, and a later acquisition succeeds. Another
   debug-injection test covers `READER_LSN_UNKNOWN`: the selector reports an
   unknown reader position, the read is redirected to the writer, no wait wrapper
   is sent, and the writer-fallback counter increments once. True capacity
   contention remains a stress/manual case.

3. **Missing-RFQ flags.**
   The LSN TAP drives writer and tracked-reader results that complete without an
   RFQ LSN and asserts:
   - `PolarDB_Write_Missing_LSN` or `PolarDB_Read_Missing_LSN` increments.
   - the session missing-LSN flag changes the next automatic protected read
     according to `pgsql-polardb_route_rfq_policy`.

   A focused check that a later positioned primary RFQ clears the missing-LSN
   flag is still a useful extension.

4. **Route-transition reasons.**
   The suite covers:
   - multi-statement read -> writer, no wait wrapper;
   - `/* route=primary */` hint -> writer, no wait wrapper;
   - explicit transaction read -> writer, no wait wrapper;
   - write-LSN-unknown and observed-LSN-unknown -> policy-controlled route.

5. **Session LSN lifetime across RESET.**
   The LSN TAP advances the session write/observed LSN, runs `RESET ALL`, and
   then asserts the next protected read still waits and can use the reader.
   `RESET` clears staged per-query state and client overrides; it does not clear
   read-your-writes evidence for the still-open session.

6. **Protocol setting matrix.**
   `pgsql-polardb_proxy_protocol` is a test dimension. The LSN TAP runs core
   routing/profile checks for:
   - `off`;
   - `legacy`;
   - `v15`;
   - `default`.

   The matrix covers global setting, hostgroup override precedence, startup
   parameter emission, and RFQ behavior. `default` is the inheritance path: a
   hostgroup set to `default` resolves through the global variable. This prevents
   a v15-only test from hiding regressions in the legacy, disabled, or inherited
   profiles.

### 7.3 Retry Negative Outcomes

The wait-timeout TAP shows the main retry behavior and the most important
retry predicate false outcomes:

1. strict wait timeout on a reusable reader -> return reader to pool and retry
   the original query on the writer;
2. reader connection loss before any result reaches the client -> destroy the
   reader connection and retry on the writer;
3. ordinary SQL error on a reusable reader -> do not retry;
4. any user result already started -> do not retry;
5. unknown fallback writer hostgroup -> do not retry;
6. non-idle writer stream at retry time -> do not retry.

Each case asserts both the client-visible result and
`PolarDB_Wait_Reads_Retried_On_Writer`. The no-retry cases verify the retry
counter does not move.

### 7.4 Next Priority: Protocol And Transport Variants

Run the main LSN TAP under frontend SSL. The TAP scripts already accept
`PGSSLMODE`; the coverage requirement is to execute the existing checks with SSL
enabled and keep the same status-counter assertions.

Backend SSL is a separate environment requirement. Add it only after the test
cluster is configured to accept SSL backend connections.

### 7.5 Later Or Manual Coverage

Some useful branches need controlled faults or live topology changes and should
not be part of the fast suite:

1. real reader-capacity contention under concurrent load;
2. stale writer-epoch check across a real failover;
3. mid-stream reader death after rows have reached the client;
4. allocation-failure paths in NoticeResponse construction;
5. long-running replay-lag mutation tests that pause or delay a replica.

These should remain manual or debug-injection tests until they can be made
repeatable without timing dependence.

### 7.6 Small Unit-Test Additions

Use unit tests for pure helpers so TAP scripts do not have to manufacture every
policy state through a live cluster:

- `PolarDB_SessionConsistency::target_with_baseline()`: observed baseline,
  primary baseline, and unknown primary.
- `PolarDB_Query_WaitPlan::build_consistency()`: off, primary-only,
  session-LSN with target, and session-LSN with zero target.
- `PolarDB_Query_RoutePlan::rfq_unavailable()`: strict, best-effort, and
  best-effort disabled for extended protocol.
- `PolarDB_HG_Policy::parse_node_type()`: primary, master, replica, standby,
  null, and unknown strings.
- monitor health parsing helpers: availability parsing, LSN parsing, and the
  rules for accepting monitor LSN updates.
- route-action-reason names and reader-status names for every enum value.

## 8. Backend Prerequisites

Runtime tests assume a PolarDB backend with:

- RFQ LSN support enabled by the libpq patch and connection option;
- `polar_xact_split_wait_lsn` support;
- `polar_consistency_mode` and `polar_proxy_wait_timeout_ms` support;
- structured wait-timeout detail marker `polar_proxy_lsn_wait_timeout`;
- one-wait-per-read behavior for the LSN wait target on supported PolarDB 15
  proxy builds.

Without these prerequisites, the feature should fail safely, but end-to-end RYW
validation is not meaningful.
