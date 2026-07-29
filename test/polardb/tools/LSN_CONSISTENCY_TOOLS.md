# PolarDB RFQ-LSN Consistency Tools

These tools show what an application can verify when it is linked with the
PolarDB-patched libpq and connects through ProxySQL with:

```text
_polar_proxy_send_lsn=true
_polar_proxy_client_host=...
_polar_proxy_client_port=...
```

The current client API exposes:

- `PQhasLSN(conn)`: last ReadyForQuery carried an LSN payload
- `PQgetLSN(conn)`: LSN value from the last ReadyForQuery
- `PQsetPolarSendLSN(conn, 1)`: request RFQ LSN parsing on an existing connection

## What The Check Shows

The tracer tracks two contexts per client connection:

```text
session context, alive until disconnect:
  session_target_lsn = max(session_write_lsn, session_observed_lsn)

transaction context, alive from BEGIN until COMMIT/ROLLBACK:
  txn_start_target_lsn = session_target_lsn captured before BEGIN
  txn_target_lsn = max(txn_start_target_lsn, txn_write_lsn, txn_observed_lsn)
```

For each read outside a transaction, if the RFQ LSN returned by libpq is
greater than or equal to `session_target_lsn`, the tracer prints:

```text
scope=session verdict=CONSISTENT
```

For each read inside a transaction, the comparison uses `txn_target_lsn`:

```text
scope=txn verdict=CONSISTENT
```

This is the consistency property visible to a client from RFQ LSN alone:
the read response reached the session LSN target known before the read.

PostgreSQL/libpq has one ordered protocol stream per connection. Normal sync
and async use have one active command at a time. Pipeline mode can queue
multiple commands, but results and ReadyForQuery still arrive in order; when
more than one command is pending at a single RFQ boundary, the tracer prints
`batch=1` and reports a boundary-level verdict instead of inventing one RFQ per
queued command.

## What It Does Not Show

RFQ LSN alone does not expose ProxySQL's internal route decision. A
`CONSISTENT` read might have been served by:

- a replica after ProxySQL injected an LSN wait,
- the primary because the planner forced primary routing,
- a transaction-split read that completed on a replica.

To expose that stronger statement to applications, ProxySQL would need an
additional client-visible RFQ metadata payload or a diagnostic notice/API with
at least:

- route action (`primary`, `replica_with_wait`, `txn_split`, `degraded`)
- wait type and target LSN
- whether the wait was applied or skipped
- whether the route was best-effort degraded

Without that metadata, the application can verify the RFQ LSN result, not the
planner branch that produced it.


## Native Context Tracking

The LD_PRELOAD tracer maintains two consistency contexts automatically:

```text
session context, survives until disconnect:
  session_target_lsn = max(session_write_lsn, session_observed_lsn)

transaction context, starts at BEGIN and clears at COMMIT/ROLLBACK:
  txn_start_target_lsn = session_target_lsn captured before BEGIN
  txn_target_lsn = max(txn_start_target_lsn, txn_write_lsn, txn_observed_lsn)
```

The native sysbench patch now exposes the same policy as a Lua connection
wrapper. Workloads use explicit statement kinds instead of SQL parsing:

```lua
con:polar_reset_context()
con:polar_query("BEGIN", "begin")
con:polar_query("INSERT ...", "write")
local rs, report = con:polar_query("SELECT ...", "read")
con:polar_query("COMMIT", "commit")
```

For pgbench, the native patch intentionally stays smaller than the sysbench Lua
API: it exposes `\polar_lsn prefix` primitives after statement boundaries.
Equivalent two-context tracking can be done with pgbench variables, or we can
add dedicated pgbench context meta-commands in a later patch.

## pgbench Check

Build the native RFQ-LSN pgbench binary and run:

```bash
make -C test/polardb build-pgbench-polar-rfq
make -C test/polardb run-pgbench-lsn
```

Useful knobs:

```bash
CLIENTS=4 TXNS=100 make -C test/polardb run-pgbench-lsn
POLARDB_ENV_FILE=/path/to/env make -C test/polardb run-pgbench-lsn
```

The output includes lines like:

```text
PGBENCH_POLAR_LSN verdict CONSISTENT write_lsn 12345 read_lsn 12345
```

### pgbench Integration Points

Native pgbench invocation:

```bash
test/polardb/bin/pgbench-polar-rfq \
  -n -M simple -c 1 -t 3 -f script.sql \
  "host=127.0.0.1 port=16433 user=postgres password=postgres dbname=postgres \
   _polar_proxy_send_lsn=true \
   _polar_proxy_client_host=192.0.2.10 \
   _polar_proxy_client_port=54321"
```

The integration points are:

- `dbname`/conninfo: carries `_polar_proxy_send_lsn=true` to ProxySQL.
- `pgbench-polar-rfq`: has patched `libpq.a` linked into the binary and does
  not need this tree's `libpq.so` at runtime.
- `\polar_lsn [prefix]`: copies the latest RFQ LSN from patched libpq into
  pgbench variables.

The patch adds a `\polar_lsn [prefix]` meta-command. It reads the latest RFQ
LSN already consumed by patched libpq and stores two variables:

```text
\polar_lsn read
```

creates:

```text
:read_has_lsn
:read_lsn
```

Example script fragment:

```text
BEGIN;
INSERT INTO polardb_lsn_check(id, payload) VALUES (1, 'x')
  ON CONFLICT (id) DO UPDATE SET payload = excluded.payload;
\polar_lsn write
SELECT count(*) FROM polardb_lsn_check WHERE id = 1;
\polar_lsn read
\if :read_has_lsn
  \if :read_lsn >= :write_lsn
    \set polar_consistent 1
  \else
    \set polar_consistent 0
  \endif
\endif
COMMIT;
```

The patch is maintained as a standalone file so the vendored PostgreSQL source
tree is not modified in place. For distribution testing, use the native
`pgbench-polar-rfq` binary produced by the build target below.

For a reproducible PolarDB distribution binary without modifying the vendored
PostgreSQL source tree in place, use:

```bash
make -C test/polardb build-pgbench-polar-rfq
```

This copies the configured PostgreSQL tree into a build scratch directory,
applies `pgbench-polar-lsn-native.patch`, builds pgbench there with patched
`libpq.a` linked statically, and emits:

```text
test/polardb/bin/pgbench-polar-rfq
```

The binary identifies itself with a PolarDB suffix:

```text
pgbench (PostgreSQL) 16.10-polar-rfq-lsn
```

It has no dynamic `libpq.so` dependency and no companion script directory is
required. Pgbench custom workloads are ordinary SQL script files passed with
`-f`.

## sysbench Lua Check

The sysbench check uses the optional `sysbench-polar-rfq` submodule binary by
default. Normal ProxySQL builds, PolarDB builds, and test-helper builds do not
require this submodule. It is skipped by default during recursive submodule
initialization because its repository may not be available to every clone.

Initialize it only when the native sysbench RFQ-LSN check is needed:

```bash
git submodule update --init --checkout deps/sysbench-polar-rfq
```
This native mode is required for repository validation. Set
`SYSBENCH_RFQ_MODE=preload` only when diagnosing an unmodified system sysbench
with `LD_PRELOAD` tracing.

The submodule-built binary is linked with the patched `libpq.a` statically, so it
does not require this tree's `libpq.so` at runtime. Lua workloads are still
normal sysbench assets and are copied next to the binary:

```text
test/polardb/bin/sysbench-polar-rfq
test/polardb/bin/sysbench-polar-rfq.d/sysbench_lsn_consistency.lua
test/polardb/bin/sysbench-polar-rfq.d/lua/*.lua
```

Use explicit script paths when running packaged workloads outside the source
tree. Example:

```bash
test/polardb/bin/sysbench-polar-rfq \
  test/polardb/bin/sysbench-polar-rfq.d/lua/oltp_read_write.lua \
  --db-driver=pgsql ...
```

Preloading patched libpq is intentional. Some sysbench builds link against a
distro-private soname such as `libpq.so.private13-5`, which `LD_LIBRARY_PATH`
cannot override by filename. The wrapper uses:

```bash
LD_PRELOAD="polardb_lsn_trace.so:.../libpq.so.5"
```

so sysbench resolves libpq entry points from the patched library first, while
the tracer still interposes completed requests.

```bash
make -C test/polardb build-sysbench-polar-rfq
make -C test/polardb run-sysbench-lsn
```

Useful knobs:

```bash
THREADS=4 EVENTS=100 SYSBENCH_BIN=/path/to/sysbench \
  make -C test/polardb run-sysbench-lsn
```

### sysbench Integration Points

Preferred native path:

```bash
make -C test/polardb build-sysbench-polar-rfq
SYSBENCH_BIN=test/polardb/bin/sysbench-polar-rfq \
  make -C test/polardb run-sysbench-lsn
```

Fallback preload path for an unmodified system sysbench:

```bash
SYSBENCH_RFQ_MODE=preload \
LD_LIBRARY_PATH="$PWD/deps/postgresql/postgresql/src/interfaces/libpq" \
LD_PRELOAD="$PWD/test/polardb/bin/polardb_lsn_trace.so:$PWD/deps/postgresql/postgresql/src/interfaces/libpq/libpq.so.5" \
POLARDB_LSN_TRACE=1 \
sysbench test/polardb/tools/sysbench_lsn_consistency.lua \
  --db-driver=pgsql \
  --pgsql-host=127.0.0.1 \
  --pgsql-port=16433 \
  --pgsql-user=postgres \
  --pgsql-password=postgres \
  --pgsql-db="postgres _polar_proxy_send_lsn=true _polar_proxy_client_host=192.0.2.10 _polar_proxy_client_port=54321" \
  --pgsql-sslmode=disable \
  --threads=1 \
  --events=3 \
  run
```

The shared integration points are:

- `--pgsql-db`: sysbench's PostgreSQL driver passes this as libpq `dbname`; when
  it contains conninfo tokens, patched libpq forwards `_polar_proxy_send_lsn`.
- `sysbench-polar-rfq`: built from the `deps/sysbench-polar-rfq` submodule,
  uses native Lua methods from the patch, and has patched `libpq.a` linked
  into the binary.
- `sysbench-polar-rfq.d/`: companion Lua workloads for the native binary.
- diagnostic `LD_PRELOAD=.../libpq.so.5`: needed for system sysbench builds
  linked against distro-private libpq sonames that `LD_LIBRARY_PATH` cannot
  override.
- diagnostic `LD_PRELOAD=polardb_lsn_trace.so`: prints RFQ LSN verdicts without
  changing Lua scripts.

For native Lua access in this repository, build the patched sysbench submodule:

```bash
make -C test/polardb build-sysbench-polar-rfq
```

The underlying source is maintained as a Git submodule:

```text
deps/sysbench-polar-rfq
```

The dependency is based on the sysbench 1.1.0 development snapshot at `3ceba0b`
(`AC_INIT([sysbench],[1.1.0])`) plus the PolarDB RFQ-LSN Lua/libpq hook commit.

The patch adds generic DB API helpers and wires them in the PostgreSQL driver
after `PQexec()`/`PQexecPrepared()`. Lua scripts get low-level methods:

```lua
con:polar_has_lsn()
con:polar_lsn()       -- exact decimal string, or nil
con:polar_lsn_u64()   -- LuaJIT uint64_t cdata, or nil
```

and the two-context wrapper methods:

```lua
con:polar_reset_context()          -- reset session and txn targets
con:polar_context()                -- read current context snapshot
con:polar_record(kind)             -- record last RFQ LSN
con:polar_query(sql, kind)         -- query + record in one call
```

Supported `kind` values are `begin`, `write`, `read`, `commit`, `rollback`,
and `observe`. The wrapper deliberately does not parse SQL in the hot path.

Example transaction fragment:

```lua
con:polar_reset_context()
con:polar_query("BEGIN", "begin")
con:polar_query("INSERT INTO polardb_lsn_check(id, payload) " ..
                "VALUES (1, 'x') ON CONFLICT (id) DO UPDATE " ..
                "SET payload = EXCLUDED.payload", "write")
local rs, report = con:polar_query(
   "SELECT count(*) FROM polardb_lsn_check WHERE id = 1", "read")
print(string.format("read verdict=%s lsn=%s target=%s",
                    report.verdict, report.lsn, report.target_lsn))
con:polar_query("COMMIT", "commit")
```

The preload tracer is still useful because it checks the client-visible result
with an unmodified sysbench binary and ordinary Lua SQL.

## Repository Validation

Run the local checks in this order:

```bash
make -C test check-polardb-libpq
scripts/verify-polardb-libpq-lsn-patch.sh .
test/polardb/bin/polardb_client_rfq_unit-t
make -C test/polardb -j128 rfq-bench-tools-check
```

The final target builds both native tools and checks the pgbench RFQ version,
the linked libpq RFQ symbol, the sysbench RFQ and primary-LSN functions, and
the copied Lua workloads. Live database checks use `run-pgbench-lsn` and
`run-sysbench-lsn` with `POLARDB_ENV_FILE`.

The external `polar-rfq` branch is the reusable source branch. ProxySQL keeps
only the submodule pointer, so internal tests do not depend on an in-tree
sysbench archive or patch copy.

## Optional Native Patch Files

The repository carries two native-integration patches:

```text
test/polardb/tools/patches/pgbench-polar-lsn-native.patch
deps/sysbench-polar-rfq (sysbench 1.1.0 dev + RFQ-LSN)
```

Use the pgbench patch when you want pgbench script variables populated from
patched libpq's `PQhasLSN()` / `PQgetLSN()` APIs.

Use the sysbench dependency target when you want Lua scripts to call native
connection methods instead of reading stderr from the preload tracer.

Both patches assume the target binary is built against the PolarDB-patched
libpq headers/library that expose:

```c
int PQhasLSN(const PGconn *conn);
uint64_t PQgetLSN(const PGconn *conn);
int PQsetPolarSendLSN(PGconn *conn, int enabled);
```

## Direct Tracer Use

The tracer can be used with any libpq-based program that dynamically links
libpq:

```bash
LD_LIBRARY_PATH="$PWD/deps/postgresql/postgresql/src/interfaces/libpq" \
LD_PRELOAD="$PWD/test/polardb/bin/polardb_lsn_trace.so" \
POLARDB_LSN_TRACE=1 \
your-libpq-program ...
```

It is a diagnostic tool only. Do not use it for benchmark latency numbers: it
prints one line per completed request and therefore adds logging overhead.
