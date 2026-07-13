# PolarDB Benchmark Suite

## Purpose

`test/polardb/test-bench/` contains manual benchmark and stress scripts for the
PolarDB LSN session-consistency feature. These scripts are not the same class of
coverage as the behavior scenarios `test-case1.sh`, `test-case2.sh`, and
`test-case5.sh`.

The short `test-case*` scripts are committed scenario checks:

- `test-case1.sh` validates the eventual-consistency baseline.
- `test-case2.sh` validates session-LSN behavior and wait-timeout variants.
- `test-case5.sh` validates primary-only routing.

Run the whole short scenario matrix with:

```bash
make -C test/polardb test-cases
```

That target runs `test-case2.sh` in all supported wait-mode/outcome variants:
best_effort success, best_effort timeout with a client WARNING, strict success,
and strict timeout with writer retry.
`make -C test/polardb bench` is kept as a compatibility alias for this short
scenario set.

The `bench*` scripts are heavier performance/correctness probes. They run more
workers, measure elapsed time and throughput, and compare writer/reader routing
under load. They are explicit Makefile targets and are not included in the
normal `bench` target.

## Shared Harness

All scripts in this directory use the committed shared support under
`test/polardb/common/` and `test/polardb/lib/`:

- `common/env.sh` loads the untracked `.env` file and exposes topology,
  credentials, DCS controls, and the bundled `PGBENCH_BIN`.
- `lib/scenario_harness.sh` provides shared topology, DCS/GUC, ProxySQL admin,
  logging, counter, rule setup, warmup, and scenario-runner helpers.
- `lib/bench_harness.sh` provides benchmark-specific helpers for stats,
  connection-pool counters, table cleanup, WAL generator lifecycle, proxy
  script execution, worker fan-out, shared failure accounting, RYW worker SQL
  generation/result capture, benchmark mode configuration, backend port checks,
  and patched-libpq runtime path.

There must be no benchmark-local copies of `lib.sh`, `test_common.sh`, or
ProxySQL lifecycle code. If a benchmark needs shared behavior, add it to
`lib/bench_harness.sh` or `lib/scenario_harness.sh`.

CSN helpers in `lib/scenario_harness.sh` remain postponed scaffold. LSN
transaction split is active and may be used by benchmark scripts when the script
also verifies split routing and counters.

## Output

All benchmark output goes under:

```text
test/polardb/test_output/run_<id>_<name>/
```

Each run captures:

- `test.log` for benchmark output;
- `proxysql.log` and `proxysql_debug.log` when the ProxySQL log is available;
- optional PolarDB primary/replica logs when `.env` provides backend log paths.

## Environment

Create and edit an untracked `.env` file before running benchmarks:

```bash
cp test/polardb/.env.example test/polardb/.env
```

The same `.env` supports docker-style and hardware-style clusters. Endpoint and
role selection should stay in `.env`; benchmark scripts should not hardcode
hosts, ports, local paths, DCS scopes, or credentials.

Benchmark hostgroup/rule defaults can be overridden from `.env` or the shell:

- `POLARDB_WRITER_HG` defaults to `10`.
- `POLARDB_READER_HG` defaults to `11`.
- `POLARDB_SELECT_RULE_ID` defaults to `10000`.

The benchmark scripts should use these variables instead of hardcoding
hostgroup IDs or query-rule IDs.

The bundled pgbench is used by default:

```text
deps/postgresql/postgresql/src/bin/pgbench/pgbench
```

Build it with:

```bash
make polardb-libpq
# or
make -C test/polardb build
```

Do not commit a local `pgbench` binary.

## Benchmark Targets

### `bench1_lsn_stress.sh`

Mixed-load RYW stress test. It runs background read/write load while multiple
long-lived sessions repeatedly perform:

```text
INSERT unique marker -> SELECT marker, inet_server_port()
```

The script validates that every RYW read returns the marker just written and,
when configured, that those reads are served by the reader backend.

Useful knobs:

- `BENCH1_RYW_CLIENTS`
- `BENCH1_RYW_ITERS`
- `BENCH1_ADHOC_CLIENTS`
- `BENCH1_TXN_CLIENTS`
- `BENCH1_DURATION_SEC`
- `BENCH1_WAIT_MODE`
- `BENCH1_WAIT_TIMEOUT_MS`

### `bench2_lsn_offload.sh`

Compares three autocommit read-after-write modes under replay lag and writer
query delay:

- `off`: forced reader route, no LSN wait, stale reads may appear;
- `lsn`: reader route with LSN wait;
- `primary`: writer-only reads.

The script reports freshness, stale rows, errors, elapsed time, TPS, hostgroup
query deltas, and LSN wait counters.

Useful knobs:

- `BENCH2_CLIENTS`
- `BENCH2_ITERS`
- `BENCH2_PRIMARY_DELAY_US`
- `BENCH2_REPLAY_LAG_BYTES`
- `BENCH2_WAIT_MODE`
- `BENCH2_WAIT_TIMEOUT_MS`

### `bench3_replica_lag.sh`

Replica-lag correctness benchmark. It compares:

- `eventual`: forced reader route, stale reads expected under lag;
- `session_smart`: LSN mode with a byte-lag cap, forcing writer when the reader
  is outside policy;
- `session`: LSN mode without lag cap, using reader waits;
- `primary_only`: writer-only reads.

Useful knobs:

- `BENCH3_CLIENTS`
- `BENCH3_ITERS`
- `BENCH3_REPLAY_LAG_BYTES`
- `BENCH3_SMART_MAX_LAG_BYTES`
- `BENCH3_WAIT_MODE`
- `BENCH3_WAIT_TIMEOUT_MS`

### `bench4_loaded_primary.sh`

Loaded-writer benchmark. It compares primary-only reads against LSN offload when
writer reads have `polar_query_delay_us` and the reader has controlled replay
lag. It validates that LSN offload remains fresh and should be faster than
writer-only reads in the intended environment.

Useful knobs:

- `BENCH4_CLIENTS`
- `BENCH4_ITERS`
- `BENCH4_PRIMARY_DELAY_US`
- `BENCH4_REPLAY_LAG_BYTES`
- `BENCH4_WAIT_MODE`
- `BENCH4_WAIT_TIMEOUT_MS`

### `bench5_consistency_shapes.sh`

Synthetic scenario-shape benchmark. It validates the SQL shapes used to compare
primary-only consistency, session-LSN offload, and transaction split before those
shapes are moved to larger real datasets.

Supported shapes:

- `session-readonly`
- `session-write-read`
- `session-read-write-read`
- `session-write-many-reads`
- `session-mixed`
- `txn-readonly`
- `txn-write-read`
- `txn-read-write-read`
- `txn-write-many-reads`
- `txn-locking`
- `txn-mixed`

Supported modes:

- `primary`: writer-only consistency baseline.
- `lsn`: session LSN consistency, transaction split disabled.
- `split`: session LSN consistency, transaction split enabled.
- `off`: eventual-consistency control with forced reader routing.

Run one isolated case:

```bash
BENCH5_CASES="session-write-read:lsn" make -C test/polardb bench5
```

Run a small transaction-split comparison:

```bash
BENCH5_CASES="txn-write-read:primary txn-write-read:split" make -C test/polardb bench5
```

Useful knobs:

- `BENCH5_CASES`
- `BENCH5_CLIENTS`
- `BENCH5_ITERS`
- `BENCH5_PRIMARY_DELAY_US`
- `BENCH5_REPLAY_LAG_BYTES`
- `BENCH5_WAL_GENERATOR`
- `BENCH5_MAX_LAG_BYTES`
- `BENCH5_WAIT_MODE`
- `BENCH5_WAIT_TIMEOUT_MS`
- `BENCH5_LAZY_WARMUP_SPLIT`
- `BENCH5_PROXY_IDENTITY_MODE`
- `BENCH5_TXN_SPLIT_SELECT_WAIT_MS`
- `BENCH5_SPLIT_WARMUP_WAIT_SEC`

`BENCH5_WAL_GENERATOR=0` is the default. The success-path shape matrix should
exercise the backend RFQ WAL-pending marker and per-session LSN behavior without
background WAL movement. Set it to `1` only for deliberate lag/load experiments.
`BENCH5_REPLAY_LAG_BYTES=0` is the default so shape comparisons start from a
no-lag baseline. Set it explicitly when testing delayed replicas.
`BENCH5_PROXY_IDENTITY_MODE` maps to
`pgsql-polardb_proxy_identity_mode` and defaults to `proxy`. Use `client` only
when the benchmark intentionally requires backend-visible per-client identity.
`BENCH5_TXN_SPLIT_SELECT_WAIT_MS=0` and `BENCH5_SPLIT_WARMUP_WAIT_SEC=0` are
the defaults so split performance runs do not inject benchmark-side waits.
Set them only for targeted diagnostics. Do not use `BENCH5_WAIT_TIMEOUT_MS` for
warmup timing: the LSN wait timeout should continue to expose replica lag or
missing WAL movement.

## Promotion Rule

A new benchmark should be committed only after it:

- sources only the shared common harness;
- uses neutral `.env` configuration;
- writes all generated files under `test_output/`;
- has a Makefile target and README entry;
- passes `bash -n` and a real run in the supported test environment.
