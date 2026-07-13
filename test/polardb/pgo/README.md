# PolarDB PGO/BOLT Training Workloads

This directory contains workloads used to train optimized PolarDB builds. The
workloads intentionally exercise ProxySQL core plus PolarDB paths only. Do not
add ClickHouse or unrelated ProxySQL feature coverage here.

The training entry points are:

- `train.sh`
  Runs a complete PGO/BOLT training profile: short correctness/setup coverage
  plus the hot-path worker.
- `hotpath_worker.sh`
  Runs the representative mixed hot-path profile through `bench5`. This is the main
  source of steady-state profile weight.

## Why The Hot-Path Worker Exists

Correctness TAPs and small scenario scripts are useful to touch setup, failure,
and edge branches, but they are not representative enough for final PGO/BOLT
profile quality. The reference mixed workload showed that steady-state cost
is driven by high-volume query intake plus PolarDB routing and split logic, not
by the short tests themselves.

The reference workload shape was:

- high-concurrency OLTP plus concurrent OLAP
- session LSN consistency enabled
- transaction split enabled
- heavy RFQ LSN/XID publication
- reader offload present but much smaller than writer traffic
- no wait-timeout path during the measured profile

The resulting profile should include substantial activity in RFQ LSN updates,
XID tracking, splittable transactions, split reads, wait wrapping, and split
warmup. Check these counters after training to make sure one path did not
accidentally dominate or disappear.

The CPU profile showed generic ProxySQL packet/query intake as the largest
single cost, with PolarDB-specific cost concentrated in:

- `PgSQL_Session::polardb_execute`
- `PgSQL_Session::polardb_prepare_txn_wait_read`
- `PgSQL_Session::polardb_prepare_txn_split_read`
- `PgSQL_HostGroups_Manager::get_MyConn_polardb_reader`
- `polardb_try_weighted_rfq_candidates`
- `polardb_get_rfq_profile_compatible_conn`
- `PgSQL_Connection::has_same_connection_options`
- `PgSQL_Session::polardb_collect`
- `PgSQL_Session::polardb_process_result`
- `PgSQL_Session::polardb_observe_transaction_split`
- split warmup compatibility checks

That is why the hot-path profile favors transaction split, RFQ-compatible reader
selection, session wait wrapping, and result processing over generic read-only
offload.

## Training Patterns To Emphasize

Use these shapes as the main profile source:

- `txn-write-many-reads:split`
  Main transaction-split shape. Exercises write RFQ, XID tracking, reader
  selection, split dispatch, result observation, and cleanup.
- `txn-read-write-read:split`
  Covers read-before-write and read-after-write transitions.
- `txn-leading-reads-write-read:split`
  Covers transactions that start read-heavy and later become write-containing.
- `session-write-many-reads:lsn`
  Main session RYW wrapper path. Exercises wait wrap prepared/sent/bypassed
  branches.
- `session-mixed:lsn`
  Provides more parser, route-plan, and session-mode variety than one fixed
  query shape.
- `txn-locking:split`
  Low-weight split veto coverage.
- `session-write-read:primary`
  Low-weight writer-only baseline.

Use these only as low-weight edge coverage:

- finite best-effort and strict timeout cases
- reader failure or no-backend fallback cases
- explicit eventual-consistency/off baseline
- long transaction split variants

Avoid making timeout/fault paths dominate the profile. They matter for
correctness, but the reference steady-state run did not spend profile weight there.

## Hot-Path Profiles

Run the worker directly when you want only the hot-path profile:

```sh
make -C test/polardb pgo-hotpath-smoke
make -C test/polardb pgo-hotpath-core
make -C test/polardb pgo-hotpath-full
make -C test/polardb pgo-hotpath-edge
```

For BOLT instrumentation:

```sh
make -C test/polardb bolt-hotpath-core
```

Default profile sizes:

- `smoke`: small sanity run, `2` clients, `4` iterations.
- `core`: primary profile run, `16` clients, `100` iterations.
- `full`: larger profile run, `32` clients, `300` iterations.
- `edge`: small low-weight fallback/veto run, `4` clients, `20` iterations.

Useful overrides:

```sh
POLARDB_HOTPATH_CLIENTS=64 \
POLARDB_HOTPATH_ITERS=500 \
make -C test/polardb pgo-hotpath-core
```

Profile-specific overrides are also supported:

- `POLARDB_HOTPATH_SMOKE_CLIENTS`
- `POLARDB_HOTPATH_SMOKE_ITERS`
- `POLARDB_HOTPATH_CORE_CLIENTS`
- `POLARDB_HOTPATH_CORE_ITERS`
- `POLARDB_HOTPATH_FULL_CLIENTS`
- `POLARDB_HOTPATH_FULL_ITERS`
- `POLARDB_HOTPATH_EDGE_CLIENTS`
- `POLARDB_HOTPATH_EDGE_ITERS`

Shape overrides:

- `POLARDB_HOTPATH_CASES`
- `POLARDB_HOTPATH_CORE_CASES`
- `POLARDB_HOTPATH_FULL_CASES`
- `POLARDB_HOTPATH_EDGE_CASES`

Policy overrides:

- `POLARDB_HOTPATH_WAIT_MODE`
- `POLARDB_HOTPATH_WAIT_TIMEOUT_MS`
- `POLARDB_HOTPATH_MAX_LAG_BYTES`
- `POLARDB_HOTPATH_LAZY_WARMUP_SPLIT`
- `POLARDB_HOTPATH_PROXY_IDENTITY_MODE`
- `POLARDB_HOTPATH_TXN_SPLIT_WARMUP_MODE`
- `POLARDB_HOTPATH_TXN_SPLIT_SELECT_WAIT_MS`

## GCC PGO Flow

```sh
make -j128 polardb-pgo-gcc-generate
make -C test/polardb pgo-train-core
make -j128 polardb-pgo-gcc-use
```

For PGO plus LTO:

```sh
make -j128 polardb-pgo-gcc-generate
make -C test/polardb pgo-train-core
make -j128 polardb-pgo-lto-gcc
```

## Clang IR PGO Flow

```sh
LLVM_BIN=/path/to/llvm/bin
env PATH="$LLVM_BIN:$PATH" make -j128 polardb-pgo-clang-generate
make -C test/polardb pgo-train-core
env PATH="$LLVM_BIN:$PATH" make polardb-pgo-clang-merge
env PATH="$LLVM_BIN:$PATH" make -j128 polardb-pgo-clang-use
```

## Clang Context-Sensitive PGO Flow

Clang also supports context-sensitive IR PGO. Use it as a second training pass
after the base clang profile has been collected and merged:

```sh
LLVM_BIN=/path/to/llvm/bin
env PATH="$LLVM_BIN:$PATH" make -j128 polardb-pgo-clang-generate
make -C test/polardb pgo-train-core
env PATH="$LLVM_BIN:$PATH" make polardb-pgo-clang-merge
env PATH="$LLVM_BIN:$PATH" make -j128 polardb-pgo-clang-cs-generate
make -C test/polardb pgo-train-core
env PATH="$LLVM_BIN:$PATH" make polardb-pgo-clang-cs-merge
env PATH="$LLVM_BIN:$PATH" make -j128 polardb-pgo-clang-cs-use
```

The context-sensitive pass separates hot and cold caller contexts for the same
callee. That can improve ThinLTO inlining and indirect-call decisions when a
shared helper is hot only from specific PolarDB/core call chains. It is more
expensive than normal PGO because it requires a second instrumented build and a
second training run, so compare it against the regular clang PGO+ThinLTO build.

## BOLT Flow

On AMD systems without usable LBR, use BOLT instrumentation rather than
`perf2bolt` sampling. The flow builds a relocation-capable binary, instruments
it, runs the instrumented ProxySQL binary through the PolarDB workload, then
optimizes the original binary using the generated `.fdata` profile:

```sh
make -j128 polardb-bolt-build
make polardb-bolt-instrument
make -C test/polardb bolt-train-core
make polardb-bolt-optimize
```

Use the same workload shape for PGO and BOLT unless there is a measured reason
to diverge. BOLT benefits from basic-block and branch layout information, so the
steady-state hot-path worker is more useful than short correctness TAPs.

## Profile Hygiene

- Keep the training set focused on core plus PolarDB paths.
- Make steady-state transaction split and session RYW paths dominate.
- Keep timeout/fault paths low weight.
- Use enough clients and iterations that startup/config noise is diluted.
- Re-run the final optimized binary against the same benchmark used to collect
  the profile. Profile quality is only useful if it improves the real workload.

## Optimization Remarks And Decision Logs

Profile quality also needs compiler-pass evidence. The `*-remarks` targets run
the profile-use build with compiler optimization diagnostics enabled, then write
a single summarized report:

```sh
LLVM_BIN=/path/to/llvm/bin
env PATH="$LLVM_BIN:$PATH" make -j128 polardb-pgo-clang-use-remarks
env PATH="$LLVM_BIN:$PATH" make -j128 polardb-pgo-clang-cs-use-remarks
make -j128 polardb-pgo-gcc-use-remarks
make -j128 polardb-pgo-lto-gcc-remarks
```

The report is written to:

```text
build/polardb-remarks/summary.md
```

For clang builds, the remarks mode enables:

- `-Rpass`, `-Rpass-missed`, and `-Rpass-analysis` for selected optimization
  passes;
- hotness annotations from the PGO profile;
- clang optimization records (`*.opt.yaml`);
- compile time traces (`*.json`);
- ThinLTO remarks, stats, and time trace output.

For GCC builds, the remarks mode enables:

- `-fopt-info`;
- `-fprofile-report`;
- `-ftime-report`.

For BOLT, use:

```sh
make polardb-bolt-optimize-report
```

That runs BOLT with profile/layout diagnostics:

- `-print-profile-stats`;
- `-print-function-statistics=100`;
- `-print-cache-metrics`;
- `-print-large-functions`;
- `-time-opts`.

You can regenerate the combined report without rebuilding:

```sh
make polardb-opt-report
```

The report extracts the useful parts:

- top hot successful inlines;
- top hot missed/analyzed inline decisions;
- profile-guided optimization records;
- vectorization records;
- ProxySQL/PolarDB-related records;
- build-log optimization remarks;
- clang profile summaries via `llvm-profdata`;
- BOLT profile/layout diagnostics;
- largest time-trace files.

Use this to guide code shape. If a hot helper is repeatedly missed for inlining
because it is too large, split cold/error/logging paths away from the fast path.
If BOLT shows stale or missing profile coverage for hot functions, improve the
training workload before changing code. If only the synthetic hot-path profile
improves but the reference benchmark does not, the profile is too narrow.
