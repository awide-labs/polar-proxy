# 02 — Build, Compile Toggle, and libpq RFQ-LSN Patch

> Scope: how the LSN-only PolarDB feature is controlled at build time (`POLARDB_PROXY`), why the empty stub translation unit exists, the design contract that both build tiers are equivalent, and the patched libpq that carries the WAL LSN on ReadyForQuery (which parts of the patch are used vs accepted-but-unused, and how the patch is verified/regenerated). | Audience: M (ProxySQL maintainer), C (future contributor) | Status: stable | Prereqs: [01-BACKGROUND-AND-DESIGN.md](01-BACKGROUND-AND-DESIGN.md), [03-TYPES-AND-ENUMS.md](03-TYPES-AND-ENUMS.md) | Verified against: this branch

---

## 1. What this document covers

This document explains two related things:

1. **The compile toggle.** The entire PolarDB feature is behind one switch named `POLARDB_PROXY`. This section shows where the switch lives, how it turns into a C++ macro, how every PolarDB line of code is protected by it, and why a build with the feature turned off is meant to behave exactly like normal (upstream) ProxySQL.
2. **The libpq patch.** PolarDB read-your-writes consistency needs the proxy to learn the backend's WAL LSN (Log Sequence Number) without sending an extra query. ProxySQL gets it by patching its bundled copy of libpq (the PostgreSQL client C library) so the backend appends the LSN to the ReadyForQuery wire message. The same patch also carries the xact RFQ accessors used for transaction-split observation. This section goes through the whole patch, separates what the proxy actually uses from what is staged or accepted-but-unused, and covers the scripts that regenerate and verify the patch.

### 1.1 Terms used in this document (defined on first use)

| Term | Meaning |
|---|---|
| **PolarDB** | An Alibaba PostgreSQL-compatible database with one primary (writer) node and read replicas. The feature in this tree adds read-your-writes routing for it. |
| **LSN (Log Sequence Number)** | A 64-bit position in PostgreSQL's write-ahead log (WAL). A larger LSN means "more recent". A replica that has replayed up to LSN X can serve any read whose data was written at or before X. |
| **WAL (Write-Ahead Log)** | PostgreSQL/PolarDB's append-only log of all changes. Replicas replay it to catch up to the primary. An LSN is a position in this log. |
| **RYW (read-your-writes)** | The guarantee that after a session writes, its own later reads see that write, even when the read goes to a replica. |
| **RFQ (ReadyForQuery)** | The PostgreSQL wire-protocol message a backend sends after each command to say "ready for the next query". The PolarDB patch makes the backend append its current WAL LSN to this message. |
| **libpq** | The official PostgreSQL client C library. ProxySQL bundles its own copy under `deps/postgresql/` and links against it to talk to PostgreSQL/PolarDB backends. |
| **TU (translation unit)** | One `.cpp` source file compiled on its own into one object file. |
| **`POLARDB_PROXY`** | The compile-time switch (a make variable and a C++ macro) that turns the whole PolarDB feature on or off. Default is on (`1`). |
| **conninfo / startup packet** | The connection settings libpq sends to the backend when it opens a connection. The PolarDB params are added to these settings. |
| **GUC** | "Grand Unified Configuration" variable — a PostgreSQL runtime setting changed with `SET name = value`. |

---

## 2. The `POLARDB_PROXY` build toggle

### 2.1 One switch, default on

The whole PolarDB feature is enabled by a single make variable. Its default is **on**:

```make
POLARDB_PROXY ?= 1
```

This default lives in the top-level Makefile at `Makefile:149`, with a comment that explains the off case: building with `POLARDB_PROXY=0` compiles the PolarDB code to no-op stubs with "no behavior change vs upstream" (`Makefile:147-148`).

To compile the feature out, build with:

```sh
make POLARDB_PROXY=0
```

### 2.2 How the make variable becomes a C++ macro

The make variable does not directly affect the source. Each compile stage turns `POLARDB_PROXY=1` into the C++ define `-DPOLARDB_PROXY`. A bare `-DPOLARDB_PROXY` defines the macro to the value `1`, which is what the source tests for with `#if POLARDB_PROXY`.

There are two compile stages that build PolarDB code, and each has the same small block:

| Stage | File:line | What it does |
|---|---|---|
| Library compile (`libproxysql.a`) | `lib/Makefile:56-59` | `ifeq ($(POLARDB_PROXY),1)` → set `PSQLPOLAR := -DPOLARDB_PROXY` |
| Binary compile (`proxysql`) | `src/Makefile:76-79` | `ifeq ($(POLARDB_PROXY),1)` → set `PSQLPOLAR := -DPOLARDB_PROXY` |

In the library stage, `PSQLPOLAR` is added to the C++ flags at `lib/Makefile:100` (the `MYCXXFLAGS` line lists `$(PSQLPOLAR)` among the other feature flags). The same pattern wires it into the `src` stage flags.

There is also an optional verbose-trace switch. Setting `POLARDB_DEBUG=1` appends `-DPOLARDB_DEBUG=1` to the same `PSQLPOLAR` flags (`lib/Makefile:60-62`, `src/Makefile:80-82`). That only controls extra tracing (the `POLARDB_TRACE` macro); it does not change behavior and is independent of whether the feature itself is on. This document does not cover the trace output further.

Two additional **diagnostic** build flags exist, both **default off**, wired the same way in `lib/Makefile:63-68`:

| Flag | Make block | What it adds |
|---|---|---|
| `POLARDB_PROFILE=1` | `lib/Makefile:63-65` → `-DPOLARDB_PROFILE=1` | pool-lock and idle-ping timing counters, plus wrap/reader-acquire/split timing and reader-target / RFQ-candidate diagnostic breakdowns (the `POLARDB_PROFILE_*_COUNTER_LIST` sublists in `include/PgSQL_PolarDB_Counters.h`) |
| `POLARDB_PERF_DEBUG=1` | `lib/Makefile:66-68` → `-DPOLARDB_PERF_DEBUG=1` | direct-frontend writev / plain-send byte/iov/packet histograms and writev skip-reason counters (the `POLARDB_PERF_DEBUG_*_COUNTER_LIST` sublist) |

Neither flag is set in default or production builds, so their counters are compiled out unless you opt in. They only add observability; they do not change routing behavior. The counters each one adds are catalogued in [12-THREADVARS-AND-OBSERVABILITY.md](12-THREADVARS-AND-OBSERVABILITY.md) (profile / perf-debug diagnostic counters); the always-on counter surface is unaffected.

### 2.3 The build pipeline forwards the flag

ProxySQL builds in three stages: `deps` (vendored libraries, including libpq), then `lib` (the core library), then `src` (the final binary). The top-level Makefile passes `POLARDB_PROXY` down into all three so the whole tree builds consistently:

| Forwarded into | File:line |
|---|---|
| `deps` (release / debug) | `Makefile:400`, `Makefile:404` |
| `lib` (release / debug) | `Makefile:408`, `Makefile:412` |
| `src` (release / debug) | `Makefile:416`, `Makefile:422` |

Each of those lines passes `POLARDB_PROXY=$(POLARDB_PROXY)` so the value chosen at the top flows everywhere.

### 2.4 Helper targets

The top-level Makefile adds a few convenience targets:

| Target | File:line | What it does |
|---|---|---|
| `polardb` | `Makefile:441-447` | Build the binary with `POLARDB_PROXY=1` (the release tier, optimized, no trace). The target is named `polardb` (comment at `Makefile:441-443`, recipe at `:444-447`). |
| `polardb-debug` | `Makefile:432-439` | Build with `POLARDB_PROXY=1 POLARDB_DEBUG=1` (verbose PolarDB trace enabled). |
| `polardb-check` | `Makefile:449-462` | Clean-build **both** tiers in sequence and leave the tree at `POLARDB_PROXY=1`. |
| `polardb-libpq` | `Makefile:464-474` | Re-extract PostgreSQL, re-apply the libpq patch stack, rebuild libpq, build bundled PostgreSQL 16 pgbench, and build the PolarDB C helper tests. |

The `polardb-check` target is the one that exercises tier equivalence at the build level. It runs three clean builds: `POLARDB_PROXY=1`, then `POLARDB_PROXY=0` (described in the echo as "stubs"), then `POLARDB_PROXY=1` again to restore the working tree (`Makefile:453-461`). Each build is preceded by `make clean` so objects from one tier never leak into the other. It shows both tiers **compile and link**; it does not byte-compare the two binaries (see §4 for what equivalence is and is not).

ASCII view of the toggle wiring:

```
                      make POLARDB_PROXY=0|1   (default 1, Makefile:149)
                                  |
        +-------------------------+-------------------------+
        |                         |                         |
     deps stage               lib stage                 src stage
  (Makefile:400/404)      (Makefile:408/412)        (Makefile:416/422)
        |                         |                         |
   deps/Makefile             lib/Makefile               src/Makefile
   :424 ifeq ==1             :57 ifeq ==1               :77 ifeq ==1
        |                    -> -DPOLARDB_PROXY          -> -DPOLARDB_PROXY
   apply libpq patch              |                          |
   (:425)                    #if POLARDB_PROXY          #if POLARDB_PROXY
                             in every PolarDB .cpp/.h   in core call sites
```

---

## 3. How the code is controlled, and the empty stub TU

### 3.1 Every declaration and every call site is protected

The feature uses **one** checking strategy everywhere: both the PolarDB **declarations** (types, struct members, method prototypes) and every core **call site** are wrapped in `#if POLARDB_PROXY`.

- The seven PolarDB feature source files check their **entire body**. With `POLARDB_PROXY=0` each one compiles to an empty object file:

| Feature TU | Whole-body check at | Lines |
|---|---|---|
| `lib/PgSQL_PolarDB.cpp` | `:17` | 359 |
| `lib/PgSQL_PolarDB_Consistency.cpp` | `:35` | 276 |
| `lib/PgSQL_PolarDB_Wrap.cpp` | `:48` | 559 |
| `lib/PgSQL_PolarDB_Flow.cpp` | `:41` | 1951 |
| `lib/PgSQL_PolarDB_Notices.cpp` | `:27` | 329 |
| `lib/PgSQL_PolarDB_Failure.cpp` | `:39` | 1449 |
| `lib/PgSQL_PolarDB_Split.cpp` | `:26` | 942 |

- The PolarDB declarations in the shared headers (`include/PgSQL_PolarDB.h`, plus the PolarDB members added to `include/PgSQL_Session.h`, `include/PgSQL_Connection.h`, `include/PgSQL_HostGroups_Manager.h`) are themselves under `#if POLARDB_PROXY`. When the feature is off, the PolarDB types and the PolarDB struct members do not exist.
- Every place in the always-compiled core (for example the route hook in `PgSQL_Session.cpp` and the connect/result-processing hooks in `PgSQL_Connection.cpp`) wraps its PolarDB calls in `#if POLARDB_PROXY` too, so those calls disappear when the feature is off.

The list of files carrying `#if POLARDB_PROXY` and the exact hook lines are inventoried in [POLARDB_ARCHITECTURE.md](POLARDB_ARCHITECTURE.md) and [10-SESSION-INTEGRATION.md](10-SESSION-INTEGRATION.md) / [11-CONNECTION-AND-LIBPQ.md](11-CONNECTION-AND-LIBPQ.md). This document only needs the rule itself: **declarations and call sites are protected together.**

### 3.2 Why there is a stub TU, and why it is empty

`lib/PgSQL_PolarDB_Stubs.cpp` is an eighth PolarDB source file. It is compiled in **both** tiers (it is listed in the always-built object list at `lib/Makefile:119`). Its job is to hold link-time **no-op stubs** for any PolarDB symbol that the always-compiled core might reference when the feature is off.

The pattern such a stub file uses: if some core code calls a PolarDB function **without** wrapping the call in `#if POLARDB_PROXY`, then with `POLARDB_PROXY=0` the linker would still need a definition of that function or the binary would not link. The stub file would provide an empty (do-nothing) definition under `#if !POLARDB_PROXY`.

In this tree that situation does not arise, because of the rule in §3.1: every call site is protected, so there is no unguarded reference left over when the feature is off. As a result the stub file's body is **intentionally empty**. Its whole active region is:

```cpp
#if !POLARDB_PROXY

// Intentionally empty: ... no always-compiled seam symbol that needs a link-time stub.

#endif // !POLARDB_PROXY
```

That check and comment are at `lib/PgSQL_PolarDB_Stubs.cpp:28-34`. The file's header comment (`:1-24`) states the contract in full: because both the declarations and every core call site are protected, the `POLARDB_PROXY=0` build has "no always-compiled seam symbol to stub today," and the build is meant to be byte-for-byte upstream non-PolarDB behavior.

The header also records the **maintenance rule** for the future (`:18-23`): if a later commit ever adds an **unguarded** core reference to a PolarDB symbol, its no-op stub must be added to this file — and that stub must **not** name any PolarDB type, because PolarDB types are undeclared when `POLARDB_PROXY=0`.

ASCII view of the two tiers:

```
POLARDB_PROXY=1  (feature ON)
  PgSQL_PolarDB.cpp ............ full body compiles  (check #if at :17)
  PgSQL_PolarDB_Consistency.cpp  full body           (:35)
  PgSQL_PolarDB_Wrap.cpp ....... full body           (:48)
  PgSQL_PolarDB_Flow.cpp ....... full body           (:41)
  PgSQL_PolarDB_Notices.cpp .... full body           (:27)
  PgSQL_PolarDB_Failure.cpp .... full body           (:39)
  PgSQL_PolarDB_Split.cpp ...... full body           (:26)
  PgSQL_PolarDB_Stubs.cpp ...... empty (#if !POLARDB_PROXY false)
  headers ...................... PolarDB types + members declared
  core hooks ................... PolarDB calls compiled in

POLARDB_PROXY=0  (feature OFF)
  PgSQL_PolarDB*.cpp (the 7) ... compile to EMPTY objects
  PgSQL_PolarDB_Stubs.cpp ...... still empty (nothing to stub)
  headers ...................... PolarDB types + members NOT declared
  core hooks ................... PolarDB calls compiled OUT
  => intended result: upstream non-PolarDB ProxySQL
```

---

## 4. Both-tier equivalence is a design contract, not a tested fact

The claim "a `POLARDB_PROXY=0` build is byte-equivalent to upstream non-PolarDB ProxySQL" is stated in the source itself (`lib/PgSQL_PolarDB_Stubs.cpp:17` and the surrounding header comment `:10-23`). It is worth being precise about what kind of claim this is.

| Claim | Status |
|---|---|
| Every PolarDB declaration is `#if POLARDB_PROXY` protected | Design rule, visible in the headers and the stub TU comment. |
| Every PolarDB call site in core code is `#if POLARDB_PROXY` protected | Design rule, stated in `Stubs.cpp:10-15`. |
| The stub TU is empty because there is no unguarded seam to stub | True in this tree (`Stubs.cpp:28-34`); verified by reading the file. |
| Both tiers **compile and link** | Checked structurally by the `polardb-check` target (`Makefile:449-462`); it clean-builds both. |
| The `POLARDB_PROXY=0` binary is **byte-for-byte** identical to upstream | **Stated as a design contract, NOT build/diff-tested.** No step in this tree compiles both and byte-compares the outputs, and no CI job records such a comparison. |

So: the **mechanism** for equivalence (everything `#if`-protected plus an empty stub TU) is real and verifiable from source. The **byte-for-byte result** is a contract the code asserts, not something confirmed by a build-and-diff in this tree. A maintainer who needs byte-equivalence as a hard fact should build both tiers and diff the binaries; the foundation analysis explicitly did not do that build/diff.

Also note: §3.2's "intentionally empty" is true **today**. If a future change introduces an unguarded core reference, the stub file would gain content and the off-tier would differ from a pristine upstream by exactly those no-op stubs — still functionally equivalent, but no longer literally byte-identical. The contract is written to keep that from happening silently.

---

## 5. The libpq RFQ-LSN patch

### 5.1 Where the patch lives and when it is applied

The patch file is `deps/postgresql/polardb_libpq.patch`. ProxySQL bundles its own PostgreSQL source under `deps/postgresql/` and applies a chain of patches to libpq during the `deps` build. The PolarDB patch is applied **last** in that chain, and **only** when `POLARDB_PROXY=1`:

```make
ifeq ($(POLARDB_PROXY),1)
	cd postgresql/postgresql && patch -p0 < ../polardb_libpq.patch
endif
```

This block is at `deps/Makefile:424-426`. The comment there (`:420-423`) states the two facts that matter: the patch is applied **after** `sslkeylogfile.patch` (the last upstream patch, applied at `:419`), and the `ifeq` check keeps a `POLARDB_PROXY=0` build on **vanilla libpq**. So when the feature is off, libpq is unpatched and there is no PolarDB LSN behavior in the client library at all.

The upstream patch chain that runs before it (in order) is, per `deps/Makefile`: `get_result_from_pgconn`, `handle_row_data`, `fmt_err_msg` (`:416`), `bind_fmt_text` (`:417`), `pqsendpipelinesync` (`:418`), `sslkeylogfile` (`:419`), and then the PolarDB patch (`:425`).

### 5.2 What the patch changes (file by file)

The patch touches six libpq files. The table summarizes each; the detailed description follows.

| libpq file patched | What the patch adds | Patch lines |
|---|---|---|
| `exports.txt` | Exports 7 new public functions as ordinals 188-194 | `:1-14` |
| `libpq-fe.h` | `#include <stdint.h>`; declares the LSN and xact RFQ functions | `:346-371` |
| `libpq-int.h` | New fields on `struct pg_conn`: runtime LSN state, runtime xact RFQ state, and 13 conninfo option strings | `:374-405` |
| `fe-connect.c` | Registers the 13 conninfo options; converts send-LSN/send-xact strings to bools; frees new strings/state; writes params into the startup packet | `:17-126`, `:302-333` |
| `fe-exec.c` | Adds the row-run accessors (`PSpeekRowRun`/`PSrowRunPending`/`PSadvanceInput`/`PSdetachRowRun`) | `:209-238` |
| `fe-protocol3.c` | The core change: `getReadyForQuery()` reads the LSN appended to RFQ, optionally parses xact markers/XIDs, and skips any trailing bytes | `:207-298` |

### 5.3 The public functions

The patch exports three LSN functions used by this branch's active RYW path:

| Function | Signature | What it does | Reads/writes (on `struct pg_conn`) | Impl at |
|---|---|---|---|---|
| `PQgetLSN` | `uint64_t PQgetLSN(const PGconn*)` | Return the LSN captured from the most recent RFQ; 0 if none or no connection | reads `polar_last_lsn` | `:121-127` |
| `PQhasLSN` | `int PQhasLSN(const PGconn*)` | Was an LSN present in the most recent RFQ? (1/0) | reads `polar_has_lsn` | `:130-136` |
| `PQsetPolarSendLSN` | `void PQsetPolarSendLSN(PGconn*, int enable)` | Turn the runtime flag that makes the RFQ parser look for an appended LSN on/off | writes `polar_proxy_send_lsn` | `:139-145` |

It also exports four transaction-split RFQ helpers used by the transaction-split path:

| Function | Signature | What it does | Reads/writes (on `struct pg_conn`) | Impl at |
|---|---|---|---|---|
| `PQgetXactSplitXids` | `const char *PQgetXactSplitXids(const PGconn*)` | Return the XID list captured from the most recent RFQ, or `NULL` if absent | reads `polar_xact_xids` | `:170-177` |
| `PQisXactSplittable` | `int PQisXactSplittable(const PGconn*)` | Whether the backend reported the open transaction as replica-splittable (`'x'` marker) | reads `polar_xact_splittable` | `:179-186` |
| `PQisXactWalPending` | `int PQisXactWalPending(const PGconn*)` | Whether the backend supplied XIDs but WAL is not safe for replica split yet (`'w'` marker) | reads `polar_xact_wal_pending` | `:188-195` |
| `PQsetPolarSendXact` | `void PQsetPolarSendXact(PGconn*, int enable)` | Turn the runtime flag that makes the RFQ parser look for xact metadata on/off | writes `polar_proxy_send_xact` | `:197-203` |

The header also adds `#include <stdint.h>` so `uint64_t` is available (`:236`). It is `<stdint.h>` (the C header), **not** `<cstdint>`, because `libpq-fe.h` is a C header used by C code.

How ProxySQL uses these (the proxy side, not the patch):

- After a successful connect to a PolarDB hostgroup, ProxySQL calls `PQsetPolarSendLSN(pgsql_conn, 1)` to enable LSN parsing for that connection. The enabling function `polardb_init_connection_tracking()` is **defined** at `lib/PgSQL_Connection.cpp:1882`, and the `PQsetPolarSendLSN(pgsql_conn, 1)` **call** is at `lib/PgSQL_Connection.cpp:1887`. (These are two different lines: `:1882` is the function, `:1887` is the call inside it.) The function only enables LSN parsing when this connection's startup profile requested RFQ LSN (`:1886`).
- On the response path, ProxySQL reads the LSN with no extra round-trip via `PgSQL_Connection::get_polardb_lsn()` (`lib/PgSQL_Connection.cpp:1897-1908`), which calls `PQhasLSN()` then `PQgetLSN()` (`:1903-1904`).
- The xact helpers back the active transaction-split path: ProxySQL calls `PQsetPolarSendXact` / `PQgetXactSplitXids` / `PQisXactSplittable` / `PQisXactWalPending` (`lib/PgSQL_Connection.cpp:1890-1941`) when `txn_split_enabled=1`.

The full connection/result-processing integration is in [11-CONNECTION-AND-LIBPQ.md](11-CONNECTION-AND-LIBPQ.md) and [09-PUBLISH-AND-WRITE-TRACKING.md](09-PUBLISH-AND-WRITE-TRACKING.md).

### 5.4 The new `struct pg_conn` fields

The patch adds fields to libpq's internal connection struct (`libpq-int.h`). There are three groups.

**Runtime LSN state (3 fields):**

| Field | Type | Purpose |
|---|---|---|
| `polar_proxy_send_lsn` | `bool` | When true, `getReadyForQuery()` looks for an appended LSN |
| `polar_last_lsn` | `uint64` | The LSN read from the last RFQ |
| `polar_has_lsn` | `bool` | Whether the last RFQ carried an LSN |

**Runtime xact RFQ state (4 fields):**

| Field | Type | Purpose |
|---|---|---|
| `polar_proxy_send_xact` | `bool` | When true, `getReadyForQuery()` looks for xact metadata after the LSN bytes |
| `polar_xact_xids` | `char *` | XID list copied from the last RFQ, when present |
| `polar_xact_splittable` | `bool` | RFQ marker `'x'`: WAL is safe for a replica split read |
| `polar_xact_wal_pending` | `bool` | RFQ marker `'w'`: XIDs are known, but WAL is not safe for a replica split read yet |

**Connection-string option strings (13 fields):**

| Group | Fields |
|---|---|
| PG11-style names (work with PolarDB 11 and 15) | `_polar_send_lsn`, `_polar_send_xact`, `_polar_origin_client_ip`, `_polar_origin_client_port` |
| PG15 aliases | `_polar_proxy_client_host`, `_polar_proxy_client_port`, `_polar_proxy_send_lsn`, `_polar_proxy_send_xact` |
| PG15-only metadata | `_polar_proxy_session_id`, `_polar_proxy_cancel_key`, `_polar_proxy_use_ssl`, `_polar_proxy_ssl_version`, `_polar_proxy_ssl_cipher_name` |

### 5.5 Conninfo registration and lifecycle (`fe-connect.c`)

The 13 option strings are wired into libpq's normal connection-option machinery:

1. **Registered in the option table.** All 13 are added to `PQconninfoOptions[]`. Each entry maps an option name (for example `_polar_send_lsn`) to its field offset in `struct pg_conn`.
2. **Converted to runtime bools.** In `connectOptions2()`, the send-LSN string is turned into `polar_proxy_send_lsn`, and the send-xact string is turned into `polar_proxy_send_xact`. The PG11 name wins over the PG15 alias when both are present. Values are true only when the string equals `"true"`.
3. **Freed on close.** All 13 strings are freed in `freePGconn()`, and the copied `polar_xact_xids` buffer is freed there as connection-owned RFQ state.

### 5.6 Startup-packet injection (`fe-connect.c`)

When libpq builds the startup packet, the PolarDB params are written as **direct startup options**, not inside the normal options block, so PolarDB's server-side `ProcessStartupPacket()` can read them. The pattern is "prefer the PG11 name; fall back to the PG15 alias" for the send-LSN flag, the send-xact flag, the client host, and the client port, and a plain "emit if set" for the PG15-only metadata fields. A field is only written if it is non-empty.

### 5.7 The `getReadyForQuery()` change — the core of the feature

The one behavioral change that makes RYW possible is in `getReadyForQuery()` in `fe-protocol3.c`. After libpq's normal RFQ parsing, the patch adds these steps:

1. **Compute the true message end.** It reads the 4-byte network-order length field at `inStart+1` and computes `msg_end = inStart + 1 (type byte) + length_value`. It defines a helper `MSG_REMAINING()` = `msg_end - inCursor`.
2. **Parse the LSN if requested and present.** It clears `polar_has_lsn`, then — only if `polar_proxy_send_lsn` is set **and** at least 8 bytes remain — reads an 8-byte network-order `uint64`, byte-swaps it with `pg_ntoh64`, stores it in `polar_last_lsn`, and sets `polar_has_lsn = true`.
3. **Parse xact metadata if requested and present.** It clears the previous xact flags and frees the previous XID buffer. If `polar_proxy_send_xact` is enabled and the RFQ has a marker byte, marker `'x'` means the transaction is replica-splittable and marker `'w'` means WAL is still pending; in both cases a NUL-terminated XID list may follow and is copied into connection-owned memory.
4. **Skip any unparsed trailing bytes.** If any bytes remain after the parsed fields, it advances the cursor to the message end: `conn->inCursor = msg_end`.

Step 4 is the important safety step. The boundary check (compute `msg_end` from the length field, then skip to it) lets libpq handle a backend that appends **more or fewer** bytes than the proxy expected, without getting out of sync on the next message. The patch comment names the two cases it handles: the backend appending xact data the proxy did not ask for, and future PolarDB protocol changes.

ASCII view of the RFQ parse with the patch:

```
RFQ wire message on a PolarDB backend (read-your-writes enabled):

  +------+------------------+-------------------+------------------+-----------------------+
  | 'Z'  | length (4 bytes) | xact status byte  | LSN (8 bytes)... | xact marker/XIDs ...  |
  +------+------------------+-------------------+------------------+-----------------------+
   type   includes itself     normal RFQ field    PolarDB extra     staged split metadata

getReadyForQuery() after the patch:
  1. msg_end = inStart + 1 + length
  2. if polar_proxy_send_lsn && >=8 left:
        read 8 bytes -> pg_ntoh64 -> polar_last_lsn ; polar_has_lsn=true
  3. if polar_proxy_send_xact && marker is 'x' or 'w':
        copy NUL-terminated XID list and set the matching xact flag
  4. if any bytes left:  inCursor = msg_end  (skip the rest)
```

### 5.8 Used vs accepted-but-unused

The patch is broad on purpose: it **accepts** all 13 startup params and both spellings of the send-LSN and send-xact flags, but only a small part of that is emitted by this branch. There are two layers to look at.

**Inside libpq** (what the patch acts on):

| Surface | Behavioral effect inside libpq? |
|---|---|
| `_polar_send_lsn` / `_polar_proxy_send_lsn` | YES — converted to `polar_proxy_send_lsn` (patch `:81-84`), which controls the RFQ LSN parse (patch `:167`). |
| `_polar_send_xact` / `_polar_proxy_send_xact` | YES — converted to `polar_proxy_send_xact`, which controls the RFQ xact marker/XID parse. ProxySQL emits it for every non-`off` PolarDB profile; `txn_split_enabled` decides whether result processing observes it for split routing. |
| The other 9 option strings | NO — they are written verbatim into the startup packet and never read back by libpq. They exist so a PolarDB backend can read them server-side. |

**What this implementation actually emits** when connecting to a PolarDB backend is profile-driven. The function that writes the params is `PgSQL_Connection::append_polardb_startup_params()` (`lib/PgSQL_Connection.cpp:1809-1877`), called from the connect path after it resolves the per-hostgroup/global `proxy_protocol` startup profile:

| Param | Accepted by the patch | Emitted by this tree | Note |
|---|---|---|---|
| `_polar_send_lsn=true` | yes | **YES**, when effective protocol is `legacy` (`PgSQL_Connection.cpp:1868`) | Requests RFQ LSN using the legacy startup dialect. |
| `_polar_origin_client_ip` | yes | **YES**, when effective protocol is `legacy` (`:1865`) | Client/fallback identity passthrough. |
| `_polar_origin_client_port` | yes | **YES**, when effective protocol is `legacy` (`:1866`) | Client/fallback identity passthrough. |
| `_polar_proxy_send_lsn=true` (PG15 alias) | yes | **YES**, when effective protocol is `v15` (`:1856`) | Requests RFQ LSN using the v15 startup dialect. |
| `_polar_proxy_client_host` / `_polar_proxy_client_port` | yes | **YES**, when effective protocol is `v15` (`:1853-1854`) | Client/fallback identity passthrough using v15 names. |
| `_polar_send_xact=true` / `_polar_proxy_send_xact=true` | yes | when effective protocol is `legacy` or `v15` | Requests transaction-split RFQ evidence at startup; `txn_split_enabled` controls its use in planning and split-read dispatch. |
| `_polar_proxy_session_id` / `_polar_proxy_cancel_key` | yes | no | Cancel-routing metadata; unused here. |
| `_polar_proxy_use_ssl` / `_polar_proxy_ssl_version` / `_polar_proxy_ssl_cipher_name` | yes | no | SSL passthrough metadata; unused here. |

So the proxy emits one identity/LSN dialect per RFQ-requesting connection: either v15 (`_polar_proxy_client_host`, `_polar_proxy_client_port`, `_polar_proxy_send_lsn=true`) or legacy (`_polar_origin_client_ip`, `_polar_origin_client_port`, `_polar_send_lsn=true`). When the PolarDB hostgroup has `txn_split_enabled=1`, it adds the matching xact request key (`_polar_proxy_send_xact=true` or `_polar_send_xact=true`) so result processing can observe split-readable transaction state. The remaining metadata parameters are accepted by the patch but never emitted here.

Why the patch keeps the unused metadata params: it was written once to support both PolarDB 11 and PolarDB 15 startup styles and a larger proxy feature set. This implementation emits the active LSN/client-identity dialect selected by the startup profile, while leaving cancel-routing and SSL metadata for future features.

### 5.9 Related deferred items in this tree (cross-reference)

Two deferred areas connect to the patch's metadata params. They are listed here
for completeness and tracked in
[15-LIMITATIONS-AND-ROADMAP.md](15-LIMITATIONS-AND-ROADMAP.md):

- This implementation has no session-side mirror fields for `_polar_proxy_session_id` /
  `_polar_proxy_cancel_key`. Those parameters are accepted by the libpq patch
  but never emitted by ProxySQL here; a future PolarDB15 cancel-session
  extension should add the session state, generators, startup emission, cancel
  request flow, tests, and docs together.
- The **DEFERRED** lag item elsewhere in the feature is the admin knob `pgsql-polardb_lag_ms`: it has no producer today and accepts only `0`. `PolarDB_LSN_Stale_Count` is active for the separate byte-lag safety path when `max_lag_bytes` is enabled. Neither item is part of the build/libpq surface; details are in [04-ADMIN-SCHEMA-AND-CONFIG.md](04-ADMIN-SCHEMA-AND-CONFIG.md), [12-THREADVARS-AND-OBSERVABILITY.md](12-THREADVARS-AND-OBSERVABILITY.md), and [15-LIMITATIONS-AND-ROADMAP.md](15-LIMITATIONS-AND-ROADMAP.md).

---

## 6. Verifying and regenerating the patch

### 6.1 The verification script

The script `scripts/verify-polardb-libpq-lsn-patch.sh` checks that the PolarDB patch still applies cleanly as the **last** patch on top of the vendored PostgreSQL source plus all upstream libpq patches. It is a standalone tool: a grep across `Makefile`, `deps/Makefile`, `lib/Makefile`, `src/Makefile`, `test/Makefile`, and `.github/` finds **no reference** to it, so nothing in the build or CI runs it automatically — it is meant to be run by hand (for example after changing libpq patches or bumping the PostgreSQL version).

What the script does, in order (`scripts/verify-polardb-libpq-lsn-patch.sh`):

| Step | Lines | Action |
|---|---|---|
| Resolve the tree root | `:22` | Defaults to the parent of `scripts/`; an optional first argument overrides it. |
| Sanity-check inputs | `:40-48` | The patch file, a `postgresql-*.tar.gz` tarball, and all six upstream patch files must exist. |
| Extract to a scratch dir | `:50-62` | Untar PostgreSQL into a temp dir and locate the source root that contains `src/interfaces/libpq`. |
| Apply the 6 upstream patches in order | `:64-70` | `get_result_from_pgconn`, `handle_row_data`, `fmt_err_msg`, `bind_fmt_text`, `pqsendpipelinesync`, `sslkeylogfile` (the order is fixed at `:29-36`). |
| Clean `.orig` residue | `:72-75` | Remove `.orig` files the upstream patches left, so the next check only sees the PolarDB patch's effect. |
| Dry-run the PolarDB patch | `:77-88` | `patch --dry-run`; **fail** if it reports any `fuzz`, `offset`, `FAILED`, or `hunk` problem. The patch must apply perfectly clean. |
| Real-apply the PolarDB patch | `:90-96` | Apply for real and **fail** if it produced any `.orig` file (which would mean fuzz). |
| CSN rejection | `:98-101` | **Fail** if the patch contains deferred CSN tokens (`PQgetCSN`, `_polar_send_csn`, `polar_last_csn`, and related names). |
| Required LSN/xact token check | `:103-121` | **Fail** if the patch is missing the active LSN API or xact RFQ API. |
| Result | `:123-124` | Print `PASS` and exit 0, or `fail()` (`:38`) prints `FAIL: ...` and exits non-zero. |

The "must apply with zero fuzz/offset" rule is stricter than a normal `patch` run. It guarantees the committed patch matches the exact upstream-patched baseline it was generated against, so an accidental drift (e.g. a renumbered hunk) is caught.

### 6.2 Regenerating the patch and rebuilding libpq

The committed regeneration path is:

```bash
scripts/regenerate-polardb-libpq-patch.sh --verify
```

The wrapper builds the same upstream-patched PostgreSQL baseline as the verifier, diffs that baseline against the current expanded `deps/postgresql/postgresql` libpq files, replaces `deps/postgresql/polardb_libpq.patch`, and optionally runs the verifier. Use this after editing the expanded vendored libpq files; do not hand-maintain patch hunks.

The `polardb-libpq` make target (`Makefile:464-474`) is the rebuild path:

1. Remove the previously extracted PostgreSQL tree (`Makefile:469`).
2. Rebuild `postgresql` with `POLARDB_PROXY=1`, which re-extracts the tarball and re-applies the whole patch chain including the PolarDB patch (`Makefile:470`; the patch application itself is `deps/Makefile:425`).
3. Build bundled PostgreSQL 16 pgbench and the PolarDB C helper tests (`Makefile:470-471`), then print the `test/polardb/Makefile` run targets for live-cluster execution (`Makefile:473-476`).

The C helpers it builds include `test/polardb/bin/libpq_lsn_test`, `test/polardb/bin/libpq_xact_test`, and `test/polardb/bin/proxysql_extended_protocol_test` (compiled from `test/polardb/test-c/*.c`, linked against the patched `-lpq`). The direct libpq helpers exercise the new libpq functions against a real PolarDB cluster using the `POLARDB_*` environment variables. The test details are in [16-TESTING-AND-VALIDATION.md](16-TESTING-AND-VALIDATION.md).

ASCII view of patch verify vs rebuild:

```
verify (manual, not in CI):                 rebuild (make polardb-libpq):
  scripts/verify-polardb-libpq-lsn-patch.sh    Makefile:464-474
    extract tarball                              rm extracted tree (:469)
    apply 6 upstream patches (:64-70)            make -C deps POLARDB_PROXY=1 postgresql (:470)
    dry-run + real-apply PolarDB patch             -> re-applies whole chain
       must be zero fuzz/offset (:77-96)            incl. polardb_libpq.patch (deps/Makefile:425)
    CSN rejected; LSN/xact tokens required       build bundled pgbench + test helpers (:470-471)
    PASS / FAIL                                  print make targets (:473-476)
```

---

## 7. Build commands and the deployment requirement

### 7.1 Common commands

| Goal | Command |
|---|---|
| Build with the feature on (default) | `make` |
| Build with the feature off (stubs) | `make POLARDB_PROXY=0` |
| Build both tiers and leave on | `make polardb-check` |
| Build release tier explicitly | `make polardb` |
| Build with verbose PolarDB trace | `make polardb-debug` |
| Rebuild patched libpq, bundled pgbench, and PolarDB C helpers | `make polardb-libpq` |
| Regenerate `deps/postgresql/polardb_libpq.patch` from the expanded vendored source | `scripts/regenerate-polardb-libpq-patch.sh --verify` |
| Verify the patch still applies clean | `scripts/verify-polardb-libpq-lsn-patch.sh` |

### 7.2 The patched libpq is mandatory for RYW

The read-your-writes guarantee depends on the LSN arriving on RFQ, which only happens with the patched libpq. The build wiring makes this exact:

- With `POLARDB_PROXY=1`, the patch is applied (`deps/Makefile:424-426`) and the proxy enables LSN parsing per PolarDB connection (`PgSQL_Connection.cpp:1887`).
- With `POLARDB_PROXY=0`, the `ifeq` check skips the patch (`deps/Makefile:424-426`), so libpq is **vanilla** and has no PolarDB LSN/xact API and no RFQ parsing extension. The whole feature is compiled out anyway.

There is no middle state in a normal build: you cannot get the PolarDB C++ code without the patched libpq, because both are enabled by the same `POLARDB_PROXY` switch and built from the same tree. The operational consequence — what RYW needs at deploy time (a genuine PolarDB backend plus this patched libpq) and what happens without it — is covered in [17-OPERATOR-GUIDE.md](17-OPERATOR-GUIDE.md) and [14-INVARIANTS-AND-FAILURE-MODES.md](14-INVARIANTS-AND-FAILURE-MODES.md).

---

## 8. Quick reference

| Fact | Where |
|---|---|
| Toggle default (`POLARDB_PROXY ?= 1`) | `Makefile:149` |
| `-DPOLARDB_PROXY` define (lib / src) | `lib/Makefile:57-58`, `src/Makefile:77-78` |
| Flag forwarded to deps/lib/src | `Makefile:400-422` |
| `polardb-check` (both tiers, clean) | `Makefile:449-462` |
| Stub TU, intentionally empty | `lib/PgSQL_PolarDB_Stubs.cpp:28-34` (contract `:10-23`) |
| Stub TU in always-built object list | `lib/Makefile:119` |
| Byte-equivalence stated as contract | `lib/PgSQL_PolarDB_Stubs.cpp:17` |
| Patch applied last, only if on | `deps/Makefile:424-426` |
| 7 new libpq functions exported | LSN functions are active; xact functions back the active transaction split |
| New `pg_conn` fields | patch `:256-279` |
| RFQ LSN parse + skip-remaining | patch `:146-191` (skip `:185-186`) |
| Proxy emits one LSN/identity dialect of 13 params | `lib/PgSQL_Connection.cpp:1809` |
| `PQsetPolarSendLSN` definition vs call | def `lib/PgSQL_Connection.cpp:1882`, call `:1887` |
| `get_polardb_lsn()` uses `PQhasLSN`/`PQgetLSN` | `lib/PgSQL_Connection.cpp:1903-1904` |
| Patch verify script | `scripts/verify-polardb-libpq-lsn-patch.sh` (not wired into any build/CI) |
| Patch rebuild target | `Makefile:464-474` |

---

## Appendix: Mermaid diagrams

### A. Toggle wiring (make variable → C++ macro → checks)

```mermaid
flowchart TD
  A["make POLARDB_PROXY=0|1<br/>default 1 (Makefile:149)"] --> B["deps stage<br/>(Makefile:400/404)"]
  A --> C["lib stage<br/>(Makefile:408/412)"]
  A --> D["src stage<br/>(Makefile:416/422)"]
  B --> B1["deps/Makefile:424 ifeq ==1<br/>apply libpq patch (:425)"]
  C --> C1["lib/Makefile:57 ifeq ==1<br/>-DPOLARDB_PROXY"]
  D --> D1["src/Makefile:77 ifeq ==1<br/>-DPOLARDB_PROXY"]
  C1 --> E["#if POLARDB_PROXY<br/>in every PolarDB .cpp/.h"]
  D1 --> E
```

### B. Two build tiers

```mermaid
flowchart LR
  subgraph ON["POLARDB_PROXY=1 (feature ON)"]
    O1["7 PolarDB .cpp: full body<br/>checks at :17/:35/:48/:41/:27/:39/:26"]
    O2["Stubs.cpp: empty"]
    O3["headers: PolarDB types + members declared"]
    O4["core hooks: PolarDB calls compiled in"]
  end
  subgraph OFF["POLARDB_PROXY=0 (feature OFF)"]
    F1["7 PolarDB .cpp: EMPTY objects"]
    F2["Stubs.cpp: still empty (nothing to stub)"]
    F3["headers: PolarDB types NOT declared"]
    F4["core hooks: PolarDB calls compiled out"]
    F5["=> intended: upstream non-PolarDB ProxySQL<br/>(design contract, not byte-diff tested)"]
  end
```

### C. RFQ parse with the libpq patch

```mermaid
flowchart TD
  R["RFQ message 'Z' + length + xact status (+ optional 8-byte LSN)"] --> S1["msg_end = inStart + 1 + length<br/>(patch :157-161)"]
  S1 --> S2{"polar_proxy_send_lsn<br/>&& >= 8 bytes left?"}
  S2 -- yes --> S3["read 8 bytes -> pg_ntoh64<br/>polar_last_lsn = lsn<br/>polar_has_lsn = true (patch :165-178)"]
  S2 -- no --> S4["polar_has_lsn = false"]
  S3 --> S5{"any bytes left?"}
  S4 --> S5
  S5 -- yes --> S6["inCursor = msg_end (skip remaining)<br/>(patch :180-186)"]
  S5 -- no --> S7["done"]
  S6 --> S7
```

### D. Patch verify vs rebuild

```mermaid
flowchart LR
  subgraph V["verify (manual, not in CI)"]
    V1["extract tarball"] --> V2["apply 6 upstream patches (:64-70)"]
    V2 --> V3["dry-run + real-apply PolarDB patch<br/>zero fuzz/offset (:77-96)"]
    V3 --> V4["CSN rejected<br/>LSN/xact tokens required"]
    V4 --> V5["PASS / FAIL (:108-109)"]
  end
  subgraph R["make polardb-libpq (Makefile:464-474)"]
    R1["rm extracted tree (:469)"] --> R2["make -C deps POLARDB_PROXY=1 postgresql (:470)<br/>re-applies chain incl. polardb_libpq.patch"]
    R2 --> R3["build bundled pgbench + test helpers (:470-471)"]
    R3 --> R4["print test/polardb make targets (:473-476)"]
  end
```

---

Verified against this branch.
