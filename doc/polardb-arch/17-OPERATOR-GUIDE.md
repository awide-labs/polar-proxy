# 17 — Operator Guide

> Scope: how to deploy, configure, and troubleshoot the PolarDB LSN-only read-your-writes (RYW) feature in production, including this feature's RFQ startup/routing policy. | Audience: R/M/O/C (operator-first) | Status: stable | Prereqs: [01-BACKGROUND-AND-DESIGN.md](01-BACKGROUND-AND-DESIGN.md), [04-ADMIN-SCHEMA-AND-CONFIG.md](04-ADMIN-SCHEMA-AND-CONFIG.md), [12-THREADVARS-AND-OBSERVABILITY.md](12-THREADVARS-AND-OBSERVABILITY.md) | Verified against: this branch

---

## 1. What this guide is for

This guide tells a ProxySQL operator how to:

1. Meet the deployment requirements so the feature can work at all (Section 3).
2. Turn on read-your-writes (RYW) consistency for PolarDB read traffic (Section 5).
3. Pick a config recipe for your workload (Section 6).
4. Avoid the one routing trap that silently disables RYW (Section 7, the manual-route caveat).
5. Diagnose problems using the stat counters and the proxy log (Section 8).

It does not re-explain the design. For *why* the feature works the way it does, read [01-BACKGROUND-AND-DESIGN.md](01-BACKGROUND-AND-DESIGN.md). For the full counter semantics, read [12-THREADVARS-AND-OBSERVABILITY.md](12-THREADVARS-AND-OBSERVABILITY.md).

### Terms used in this guide (defined once, used the same way everywhere)

| Term | Plain definition |
|------|------------------|
| **PolarDB** | An Alibaba PostgreSQL-compatible database. It has one **writer** (primary) node that accepts writes, and one or more **reader** (replica) nodes that replay the writer's changes and may lag behind. |
| **LSN** (Log Sequence Number) | A 64-bit number marking a position in PostgreSQL's write-ahead log (WAL). Bigger means newer. A reader that has "replayed up to LSN X" can serve any read whose data was written at or before X. |
| **RYW** (read-your-writes) | The guarantee that after a client writes, its own later reads see that write, even when the read is sent to a reader. |
| **writer / primary** | The node that takes writes. Always up to date. We say "writer" throughout. |
| **reader / replica** | A read-only node that replays the writer's WAL and may lag. We say "reader" throughout. |
| **hostgroup (HG)** | A numbered group of backend servers in ProxySQL. A PolarDB pair links a writer HG and a reader HG in the `pgsql_replication_hostgroups` table. |
| **RFQ** (ReadyForQuery) | The PostgreSQL message a backend sends after each query. With the patched libpq, a PolarDB backend appends its current LSN to RFQ, so ProxySQL learns the LSN with no extra query. |
| **wait wrapper** | A reader-bound read that ProxySQL prefixes with three `SET` statements so the reader blocks until it has replayed past the client's last write LSN before answering. |
| **GUC** | A PostgreSQL runtime setting changed with `SET name = value`. |
| **patched libpq** | The PostgreSQL client library built with ProxySQL's `deps/postgresql/polardb_libpq.patch`. Without it, ProxySQL cannot read the LSN from RFQ. |
| **safe writer fallback** | When any precondition for a safe replica read is missing, ProxySQL does not continue in a weaker mode. It sends the read to the writer, which is always consistent, so RYW is never silently broken. |

---

## 2. The one-paragraph mental model

A client connects through ProxySQL to a PolarDB writer/reader pair. Backend startup requests RFQ LSN evidence according to `proxy_protocol`. ProxySQL records positioned write and observed LSNs monotonically. A `replica_eligible=1` read uses the common consistency planner: simple query sends an SQL wait wrapper, while eligible extended protocol sends binary `W` immediately before Parse or Bind/Execute on a negotiated `v15_wait` connection. Both use the same target and one backend flush; a target-ready reader sends neither wait form. Independent actions choose writer fallback, warning, error, or disconnect when evidence, capacity, or the wait fails.

```
client writes ──► writer ──RFQ carries LSN──► ProxySQL records session write LSN
client reads  ──► (replica_eligible + consistency on) ──► reader
                       simple: SQL wait wrapper; extended: W + semantic command
                       reader blocks until caught up, then answers ──► client sees its write
```

---

## 3. Deployment requirements

RYW needs three things in place. If any is missing, the feature degrades in a specific, documented way (Section 4).

| Requirement | Why it is needed | How to confirm it |
|---|---|---|
| **A. ProxySQL built with `POLARDB_PROXY=1`** | PolarDB routing, configuration, counters, startup negotiation, and `W` require this build tier. Off mode retains generic extended-protocol correctness fixes but exposes no PolarDB runtime surface. | `POLARDB_PROXY ?= 1` is the default (`Makefile:149`). A `POLARDB_PROXY=1` build has the `pgsql-polardb_*` variables and `replica_eligible`; an off build has neither and links vanilla libpq. |
| **B. Patched libpq linked into ProxySQL** | RYW reads the writer's LSN from the RFQ message. Vanilla libpq throws those trailing bytes away. The patch adds `PQgetLSN`/`PQhasLSN`/`PQsetPolarSendLSN` and the `getReadyForQuery()` change that captures the LSN. | The patch is applied during the deps build, and **only** when `POLARDB_PROXY=1`: `deps/Makefile:386-388` (`ifeq ($(POLARDB_PROXY),1) ... patch -p0 < ../polardb_libpq.patch`). A `POLARDB_PROXY=0` build links vanilla libpq. |
| **C. Backends are genuine PolarDB** | The `SET polar_xact_split_wait_lsn` GUC and the LSN-on-RFQ behavior exist only in the PolarDB server. A plain PostgreSQL server does not block on that GUC and does not append the LSN. | The monitor's `polardb` health probe reads `polar_node_type()` and the backend LSN (`lib/PgSQL_Monitor.cpp:62`, sent from `get_task_query()` at `:1022`). On a real PolarDB backend the per-query path sees an LSN on RFQ; you can confirm with the counter `PolarDB_Server_LSN_Updates_From_RFQ` growing under stable traffic (Section 8). |

Requirements A and B normally travel together: the default build (`make`) sets `POLARDB_PROXY=1`, which both compiles the feature in and applies the libpq patch. The risk is a **mismatch** — for example a binary built `POLARDB_PROXY=1` but linked against a separately built vanilla libpq. Section 4 describes exactly what that looks like.

### Build commands (operator reference)

```
make                      # release build, POLARDB_PROXY=1 by default -> feature ON + libpq patched
make POLARDB_PROXY=0      # build with the feature compiled out + vanilla libpq
make polardb-check        # clean-builds BOTH tiers, leaves the tree at POLARDB_PROXY=1
```

(The `POLARDB_PROXY` flag becomes `-DPOLARDB_PROXY` in each compile stage: `lib/Makefile:57-58`, `src/Makefile:77-78`. The top-level Makefile forwards it into deps/lib/src.)

---

## 4. What happens WITHOUT each requirement

This is the most important table for an operator. Each missing requirement is handled safely — RYW either does not exist (`POLARDB_PROXY=0`) or the read uses the writer when an automatic LSN-mode read cannot be protected. Manual reader routes remain the operator's responsibility.

| Missing | Symptom | Why | Net effect |
|---|---|---|---|
| **A. `POLARDB_PROXY=0` binary** | No `pgsql-polardb_*` admin variables; `pgsql_replication_hostgroups` has only the 4 upstream columns; `pgsql_query_rules` has no `replica_eligible` column. | PolarDB routing/configuration/counters and patched libpq are absent. Generic extended frame/error/RFQ correctness fixes remain shared. | Reads use ordinary ProxySQL policy with no PolarDB RYW. Behavioral compatibility is required; byte identity with upstream is not claimed. |
| **B. Vanilla libpq (feature compiled in, but LSN parse missing)** | Counter `PolarDB_Server_LSN_Updates_From_RFQ` stays at 0 even under stable traffic. `PolarDB_Write_Missing_LSN` or `PolarDB_Read_Missing_LSN` may increment. | `get_polardb_lsn()` calls `PQhasLSN()`/`PQgetLSN()`; vanilla libpq does not export those / never captures the LSN. | Automatic LSN-mode reads follow `pgsql-polardb_action_missing_lsn` after missing RFQ LSN. **Fix: link the patched libpq and confirm `PolarDB_Server_LSN_Updates_From_RFQ` grows under stable traffic.** |
| **C. Plain PostgreSQL backend (not PolarDB)** | The `SET polar_xact_split_wait_lsn` is an unknown GUC; the backend errors on the wrapper SET, or simply does not block; no LSN on RFQ. | Plain PostgreSQL has none of the PolarDB GUCs or the RFQ-LSN behavior. | RYW cannot work. If the wrapper SET errors, the read errors (the wrapper SET ERROR flows to the client). Do not point this feature at non-PolarDB backends. Use `check_type='read_only'` instead, which disables PolarDB routing for that pair. |

**Key safety property:** in every "missing requirement" case, ProxySQL never *claims* RYW it cannot deliver by silently sending unwrapped reads to a reader when a wait *was required*. The execute stage forces the writer whenever a planned wait cannot be prepared (`polardb_wait_disabled` in `polardb_execute()`), and a failed wrapper build sends a clean error rather than running the read unwrapped (`lib/PgSQL_Session.cpp:3607`). A stale read remains possible only when the operator explicitly bypasses the automatic planner, for example with a manual reader hostgroup route. Manual routes are authoritative and are not given an LSN wait wrapper.

---

## 5. Enabling RYW — step by step

RYW needs three configuration pieces working together: a PolarDB hostgroup pair, a consistency mode, and at least one query rule that marks reads `replica_eligible=1`.

### Step 1 — Define the PolarDB hostgroup pair

Insert a row into `pgsql_replication_hostgroups` with `check_type='polardb'`. Only `check_type='polardb'` turns the LSN/RFQ columns on for that pair. `txn_split_enabled=1` requests and observes transaction XID RFQs and enables eligible simple-query transaction reads on a temporary reader; extended transaction split remains unsupported.

| Column | Type / allowed values | Default | Meaning |
|---|---|---|---|
| `writer_hostgroup` | INT, `>=0`, PRIMARY KEY | — | writer (primary) HG id |
| `reader_hostgroup` | INT, `<> writer`, `>=0`, UNIQUE | — | reader (replica) HG id |
| `check_type` | `'read_only'` or `'polardb'` | `'read_only'` | **must be `'polardb'`** for RYW |
| `consistency_mode` | `'default'`, `'off'`, `'eventual'`, `'session_lsn'`, `'global_lsn'` | `'default'` | per-pair consistency policy; `'default'` defers to the global knob |
| `max_lag_bytes` | INT | `-1` | per-pair reader lag cap, bytes; `-1` = inherit global, `0` = off, `>0` = cap |
| `lsn_wait_timeout_ms` | INT | `-1` | per-pair wait timeout, ms; `-1` = inherit global, `0` = wait forever, `>0` = explicit |
| `proxy_protocol` | `'default'`, `'v15_wait'`, `'v15'`, `'legacy'`, `'off'` | `'default'` | startup protocol; `v15_wait` negotiates extended `W` |
| `comment` | VARCHAR | `''` | free text |

```sql
INSERT INTO pgsql_replication_hostgroups
  (writer_hostgroup, reader_hostgroup, check_type, txn_split_enabled,
   consistency_mode, proxy_protocol)
VALUES (0, 1, 'polardb', 0, 'session_lsn', 'v15_wait');
LOAD PGSQL SERVERS TO RUNTIME;
SAVE PGSQL SERVERS TO DISK;
```

Once at least one `check_type='polardb'` pair is loaded to runtime, the master condition `polardb_active` flips true (set from whether the PolarDB hostgroup set is non-empty, `lib/PgSQL_HostGroups_Manager.cpp:1864`). Every PolarDB code path checks this condition first; with no PolarDB pair configured the feature is inert and cheap.

### Step 2 — Choose the consistency mode

The effective mode for each query is resolved by precedence: **session override > per-HG `consistency_mode` > global `pgsql-polardb_consistency_mode`** (resolver `polardb_resolve_consistency_mode`, `include/PgSQL_PolarDB.h:1277`; wired through `polardb_collect()`). A value of `-1`/`'default'` at any non-global tier means "not set, fall through to the next tier".

| Mode | Word value | What it does to reads |
|---|---|---|
| **off** | `off` (int 0) | PolarDB routing does nothing; query rules decide routing as usual. No RYW. |
| **session LSN** | `session_lsn` (int 1) | Eligible reads use the session's monotonic write/observed target. |
| **global_lsn** | `global_lsn` (int 2) | Committed-state RYW: eligible reads wrap with `max(session target, writer mirror LSN)`. Stronger than lsn, still reader-offloaded. |
| **eventual** | `eventual` (int 3) | Reader placement without an LSN wait. |

To enable RYW globally:

```sql
SET pgsql-polardb_consistency_mode = 'session_lsn';
SET pgsql-polardb_read_target = 'replica';
LOAD PGSQL VARIABLES TO RUNTIME;
SAVE PGSQL VARIABLES TO DISK;
```

(Integer mode `2` is `global_lsn` above; only CSN remains a gap. See Section 9.)

### Step 3 — Mark reads `replica_eligible`

A read only enters the PolarDB auto-routing pipeline when a query rule sets `replica_eligible=1` on it. This is a tri-state per-rule integer: `-1` = unset, `0` = force the writer, `1` = opt into reader routing (`include/PgSQL_Query_Processor.h:11`; schema `replica_eligible INT CHECK (replica_eligible IN (-1,0,1)) NOT NULL DEFAULT -1` at `include/ProxySQL_Admin_Tables_Definitions.h:295`).

```sql
INSERT INTO pgsql_query_rules
  (rule_id, active, match_digest, replica_eligible, apply)
VALUES (10, 1, '^SELECT', 1, 1);
LOAD PGSQL QUERY RULES TO RUNTIME;
SAVE PGSQL QUERY RULES TO DISK;
```

Only `replica_eligible=1` opts a read into auto reader routing. Anything else (`-1` unset or `0`) leaves the read on the writer path. A client can also force the writer for a single statement with the SQL comment `/* route=primary */` at the start of the query (`force_primary_hint`, `include/PgSQL_Query_Processor.h:84-88`); there is no `/* route=replica */` hint.

### Putting it together — the enable checklist

```
[ ] ProxySQL built POLARDB_PROXY=1 (default)            -> Requirement A
[ ] Patched libpq linked (deps built with the flag)     -> Requirement B
[ ] Backends are genuine PolarDB                         -> Requirement C
[ ] pgsql_replication_hostgroups row, check_type='polardb'  -> polardb_active = true
[ ] consistency_mode resolves to 'session_lsn' or 'global_lsn'
[ ] read_target resolves to 'replica'
[ ] proxy_protocol resolves to 'v15_wait' for extended target-bearing reads
[ ] >=1 query rule with replica_eligible=1 matching your reads
[ ] LOAD ... TO RUNTIME for servers, variables, and query rules
```

---

## 6. Current profiles and tuning

Prefer a named profile for coherent behavior. Each profile atomically sets consistency, placement, fallback actions, startup protocol, monitor LSN updates, and split warmup. Changing one owned setting converts the bundle to `custom`.

| Profile | Consistency and outcome |
|---|---|
| `off` | no PolarDB routing surface for queries |
| `eventual` | replica placement without an LSN wait |
| `session_warning` | SESSION_LSN; missing target or timeout may return a warned stale simple-query result |
| `session_fallback` | SESSION_LSN; use the writer when target/wait/reader fails; factory default |
| `session_error` | SESSION_LSN; return errors instead of weaker placement |
| `global_fallback` | GLOBAL_LSN; use writer when the group target cannot be enforced |
| `global_error` | GLOBAL_LSN; fail rather than fall back |
| `custom` | individually configured settings below |

### Main policy settings

| Admin variable | Accepted values | Factory default |
|---|---|---|
| `pgsql-polardb_profile` | named profiles above, `custom` | `session_fallback` |
| `pgsql-polardb_consistency_mode` | `off`, `eventual`, `session_lsn`, `global_lsn` | `session_lsn` |
| `pgsql-polardb_read_target` | `primary`, `replica` | `replica` |
| `pgsql-polardb_action_read_fallback` | `primary`, `error` | `primary` |
| `pgsql-polardb_action_missing_lsn` | `primary`, `warning`, `error` | `primary` |
| `pgsql-polardb_action_lsn_timeout` | `warning`, `primary`, `error`, `disconnect` | `primary` |
| `pgsql-polardb_action_replica_loss` | `replica_then_primary`, `replica_then_error`, `primary`, `error`, `disconnect` | `replica_then_primary` |
| `pgsql-polardb_action_replica_error` | `primary`, `error`, `disconnect` | `primary` |
| `pgsql-polardb_proxy_protocol` | `v15_wait`, `v15`, `legacy`, `off` | `v15` |
| `pgsql-polardb_lsn_wait_timeout_ms` | `0..60000`; zero has no PolarDB deadline | `1000` |
| `pgsql-polardb_max_reader_lsn_gap_bytes` | nonnegative; zero disables byte cap | `0` |
| `pgsql-polardb_reader_lsn_max_age_ms` | cached reader-LSN age bound | `5000` |
| `pgsql-polardb_lag_cap_freshness_ms` | extra freshness bound under finite wait plus byte cap | `250` |

The default protocol is `v15`, which supplies RFQ LSN/XID evidence but does not negotiate extended `W`. Set global or per-hostgroup `proxy_protocol='v15_wait'` before expecting target-bearing extended reads to use readers. The `_pq_.polar_proxy_wait_v1=1` startup field is capability metadata, not authentication; network and HBA policy must prevent untrusted direct backend access.

There is no session-baseline variable. A target-free SESSION_LSN read may use a reader without waiting and learns its first observed LSN from RFQ. GLOBAL_LSN requires the current group observation from its first protected read.

### Recipe A: default safe fallback with extended W

```sql
SET pgsql-polardb_profile = 'session_fallback';
SET pgsql-polardb_proxy_protocol = 'v15_wait';
LOAD PGSQL VARIABLES TO RUNTIME;
SAVE PGSQL VARIABLES TO DISK;
```

A named-profile-owned override converts the result to `custom`; this is expected because `v15_wait` differs from the profile's `v15` startup protocol.

### Recipe B: availability-first warned reads

```sql
SET pgsql-polardb_profile = 'session_warning';
SET pgsql-polardb_proxy_protocol = 'v15_wait';
LOAD PGSQL VARIABLES TO RUNTIME;
```

Only use this when the application accepts explicitly warned stale results. GLOBAL_LSN rejects warning degradation, and extended unknown-target reads still fail closed because no `W` target exists.

### Recipe C: fail rather than use a weaker route

```sql
SET pgsql-polardb_profile = 'session_error';
SET pgsql-polardb_proxy_protocol = 'v15_wait';
LOAD PGSQL VARIABLES TO RUNTIME;
```

### Recipe D: force eligible reads to the writer

```sql
SET pgsql-polardb_read_target = 'primary';
LOAD PGSQL VARIABLES TO RUNTIME;
```

Writer placement is independent of consistency mode and records `READ_TARGET_PRIMARY`.

### Wait and lag tuning

`lsn_wait_timeout_ms` resolves per hostgroup, then globally. A small value reduces reader tail latency but increases the selected timeout action. The byte-lag cap is a selection safety bound, not the RYW condition: `max_reader_lsn_gap_bytes=0` leaves correctness to the backend wait; a positive value excludes readers too far behind or without trustworthy samples. `max_reader_lag_ms` is reserved and currently accepts only zero.

### Query cache and transactions

The query cache is bypassed when an active policy requires writer routing, a GLOBAL_LSN group target, or a SESSION_LSN target/missing-evidence decision. EVENTUAL and target-free SESSION_LSN requests retain ordinary cache behavior where safe.

Transaction split is simple-query only. With `txn_split_enabled=1`, complete primary RFQ XID/LSN evidence, READ COMMITTED isolation, and a safe SELECT shape, one transaction read may use a temporary reader while the transaction-owning backend remains the writer. Extended in-transaction requests stay on the writer.

## 7. The manual-route caveat (important)

**If a query rule sets a `destination_hostgroup` but leaves `replica_eligible` unset (`-1`), the PolarDB pipeline is skipped entirely for that query — including RYW.**

This is "manual mode". The route hook computes:

```c
int  re   = qpo ? qpo->replica_eligible : -1;
int  dest = qpo ? qpo->destination_hostgroup : -1;
bool manual_mode = (re < 0 && dest >= 0);
if (manual_mode) { /* PolarDB routing skipped */ }
```

(`lib/PgSQL_Session.cpp:2635-2660`; the manual scope is resolved by `polardb_manual_route_scope()`, and a rule locked on a hostgroup (`locked_on_hostgroup >= 0`) is handled separately.) When `manual_mode` is true, `polardb_collect`/`plan`/`execute` never run, so the read is sent wherever the rule's `destination_hostgroup` points — with **neither the simple-query SQL wait wrapper nor extended-protocol `W`**, and with **no writer-fallback safety check**. If that destination is a reader, the read can be stale.

### What this means for an operator

- A rule that routes a read to a hostgroup **and** wants RYW must set `replica_eligible` explicitly:
  - `replica_eligible=1` -> the read enters the PolarDB pipeline (RYW honored).
  - `replica_eligible=0` -> the read is forced to the writer (safe).
- A rule with only `destination_hostgroup` (and `replica_eligible=-1`) bypasses PolarDB. This is fine if the destination is the writer, but **dangerous if it is a reader**, because no catch-up wait is applied.

### Rule of thumb

| Rule sets | `replica_eligible` | Result |
|---|---|---|
| `destination_hostgroup = <writer>` | `-1` (unset) | manual mode; read on writer; safe (writer is consistent) |
| `destination_hostgroup = <reader>` | `-1` (unset) | **manual mode; read on a reader with NO wait — possible stale read** |
| `destination_hostgroup = <reader>` | `0` | PolarDB pipeline runs, forces writer (eligible=0) — safe |
| `match_digest` only, no destination | `1` | PolarDB pipeline runs, RYW honored — the intended setup |

When you mix manual destination rules with PolarDB, audit every rule that points reads at a reader HG and make sure it sets `replica_eligible` to a value you intend. There is currently no counter for "reads that bypassed PolarDB via manual mode"; see [15-LIMITATIONS-AND-ROADMAP.md](15-LIMITATIONS-AND-ROADMAP.md).

---

## 8. Troubleshooting

### 8.1 First, look at the counters

All PolarDB counters live in the admin table `stats_pgsql_global` and are also
exported to Prometheus as `proxysql_polardb_*_total`. SQL values are filled by
`polardb_export_stats()`; Prometheus values are refreshed from the same sources
during metrics collection. Read the SQL view with:

```sql
SELECT * FROM stats_pgsql_global WHERE Variable_Name LIKE 'PolarDB_%';
```

There are **299 exported stat counters** (252 thread-backed plus 47 global-only). The internal `polardb_active` condition and `PolarDB_Warmup_Pending` gauge are not counters. The table below highlights the ones an operator watches most:

| Counter | Increment site | What a non-zero / growing value tells you |
|---|---|---|
| `PolarDB_Server_LSN_Updates_From_RFQ` | process_result | ProxySQL is reading LSNs off RFQ and accepting them for the current writer group+epoch. Should grow under stable PolarDB traffic that returns RFQ LSN. **Stuck at 0 = no LSN on RFQ, patched libpq not active, startup profile did not request RFQ LSN, backends not PolarDB, or result processing rejected stale/missing writer group/epoch RFQs.** |
| `PolarDB_LSN_Updates_From_Monitor` | `lib/PgSQL_Monitor.cpp:1947` | The monitor is refreshing per-server LSNs between queries. Growing = healthy background refresh. 0 = monitor LSN updates off, or no LSN advance seen, or no PolarDB HG. |
| `PolarDB_Monitor_Health_Invalid_Role` | `lib/PgSQL_Monitor.cpp:838` | A monitor `polar_proxy_status`/health row reported a role ProxySQL cannot route to (node_type UNKNOWN, including PolarDB `POLAR_UNKNOWN`/`POLAR_STANDALONE_DATAMAX`). Growing = nodes without an established routable role; the monitor applies fail-safe defaults and logs a `proxy_warning`. |
| `PolarDB_Monitor_Health_Invalid_Values` | `lib/PgSQL_Monitor.cpp:842` | A monitor health row had invalid availability or LSN text. Growing = invalid or unexpected health output from a backend; the monitor applies fail-safe defaults and logs a `proxy_warning`. |
| `PolarDB_LSN_Stale_Count` | `lib/PgSQL_HostGroups_Manager.cpp` reader acquisition | Active for byte-lag enforcement. It increments when an enabled `max_lag_bytes` check sees missing or stale group/reader LSN state. The deferred millisecond-lag branch is still protected by `POLARDB_PROXY_TODO`. |
| `PolarDB_Write_Missing_LSN` | process_result | Writer query completed without RFQ LSN; automatic LSN-mode reads in that session follow `action_missing_lsn` until a primary-sourced RFQ clears the flag. |
| `PolarDB_Read_Missing_LSN` | process_result | Tracked SESSION_LSN read completed without RFQ LSN; automatic LSN-mode reads in that session follow `action_missing_lsn` until a primary-sourced RFQ clears the flag. |
| `PolarDB_RFQ_Best_Effort_Degraded_Routes` | plan | `action_missing_lsn=warning` allowed an eligible simple-query reader route without an RFQ-derived wait target. Growing means degraded consistency was explicitly served; clients also receive a WARNING `NoticeResponse` before the result. |
| `PolarDB_Consistency_Writer_Fallback` | dispatch | A consistency read failed closed to the writer during reader acquisition. Growing means offload was lost because readers were missing/stale/over-lagged, the primary mirror was unknown under a cap, or strict RFQ requirements were not met. |
| `PolarDB_Wait_Reads_Retried_On_Writer` | session dispatch | A wait-wrapped reader query was retried once on the writer after strict wait timeout or reader connection loss, before any user result reached the client. Growing means reader-side failures were recovered by the writer. |
| `PolarDB_Wait_Error_Connection_Lost` | session dispatch | A wait-wrapped reader lost its backend connection before any user result reached the client. Growing means reader connections are failing while serving protected reads. |
| `PolarDB_RFQ_Profile_Skipped` | pool acquisition | A pooled connection whose startup profile did not request RFQ LSN was skipped for a read requiring RFQ LSN. |
| `PolarDB_RFQ_Profile_Evicted` | pool acquisition | Incompatible free pooled connections were evicted to make room for newly created RFQ-LSN-capable backends. |
| `PolarDB_Target_LSN_Preferred` | reader acquisition | A reader with fresh cached LSN at or beyond the target was preferred and acquired. |
| `PolarDB_Target_LSN_Fallback_Wait` | reader acquisition | Selection fell back to the full candidate set and relied on the wait wrapper. |
| `PolarDB_Session_Target_Epoch_Reset` | collect/process_result | A session observed a writer group or epoch change and discarded existing LSN targets or missing-LSN flags from the old scope. |
| `PolarDB_Session_LSN_Routing` | `lib/PgSQL_PolarDB_Flow.cpp:1367` | Reads are being routed to readers with a wait. Growing = RYW routing is happening. 0 with read traffic = reads not getting the wait path (no write LSN yet, consistency off, or all reads forced to writer). |
| `PolarDB_Wait_Wrap_Prepared` | `lib/PgSQL_PolarDB_Flow.cpp:1380` | The wait-wrapper intent was prepared. Should track `PolarDB_Session_LSN_Routing` 1:1. |
| `PolarDB_Wait_Wrap_Bypassed` | backend acquisition | The selected reader already reached the consistency target, so ProxySQL cleared the staged wait and did not build the wrapper. Growing means healthy readers are avoiding wait latency. |
| `PolarDB_Wait_LSN_Sent` | `lib/PgSQL_PolarDB_Wrap.cpp:380` | The wait wrapper was actually built and put on the wire. `Prepared - Sent` includes bypassed waits plus any safety aborts. |
| `PolarDB_Wait_LSN_Sum_Us` | `lib/PgSQL_PolarDB_Wrap.cpp:418` | Total microseconds spent in LSN waits. Divide by `PolarDB_Wait_LSN_Sent` for average wait latency. A rising average means readers are lagging. |
| `PolarDB_Wait_Wrap_Safety_Abort` | `lib/PgSQL_PolarDB_Wrap.cpp:281` | **Should be 0.** Non-zero = the wrapper could not be built and the read failed closed to the writer. Each one is also logged via `proxy_error`. Investigate the proxy log. |
| `PolarDB_Wait_Error_Timeout` | `lib/PgSQL_PolarDB_Wrap.cpp:493` | A genuine wait timeout was charged (reader did not catch up within the timeout). best_effort: stale data served + WARNING. strict: writer retry when safe, otherwise error. Rising = replication lag or too-tight timeout. |
| `PolarDB_Wait_Error_LSN_Wait_Timeout` | `lib/PgSQL_PolarDB_Wrap.cpp:495` | The LSN subset of the timeout total. In this LSN-only build it equals `PolarDB_Wait_Error_Timeout`. |

For full counter semantics and lockstep relationships, see [12-THREADVARS-AND-OBSERVABILITY.md](12-THREADVARS-AND-OBSERVABILITY.md).

### 8.2 The troubleshooting table (symptom -> counter/log -> cause -> fix)

| Symptom | Counter / log signal | Likely cause | Fix |
|---|---|---|---|
| Reads return stale data; client does not see its own writes | RFQ updates stay at zero, missing-LSN counters grow with `action_missing_lsn=warning`, or clients see a degraded warning | RFQ LSN is absent or warned degradation is configured | Verify patched libpq/backend startup; use `action_missing_lsn='primary'` or `read_target='primary'` for fail-closed placement |
| Reads return stale data; LSN counters DO grow | `PolarDB_Server_LSN_Updates_From_RFQ` grows but `PolarDB_Session_LSN_Routing` does not | consistency is `off`/`eventual`, no `replica_eligible=1` rule, or manual routing bypasses policy | Set `consistency_mode='session_lsn'`; add an automatic rule; audit manual reader routes |
| RYW routing happens but `Sent + Bypassed` lags `Prepared` | `PolarDB_Wait_Wrap_Prepared` > `PolarDB_Wait_LSN_Sent + PolarDB_Wait_Wrap_Bypassed`; `PolarDB_Wait_Wrap_Safety_Abort` > 0; `proxy_error` "PolarDB WRAP: finalize failed" (`lib/PgSQL_PolarDB_Wrap.cpp:186`) | The wait wrapper could not be built (missing backend conn, empty query); the session forces the writer instead of sending an unwrapped replica read | Check the proxy error log for the printed reason. These reads went to the writer safely; investigate why the wrapper build failed |
| Protected reads add writer load | `PolarDB_Wait_Reads_Retried_On_Writer` rising with `PolarDB_Wait_Error_Timeout` or `PolarDB_Wait_Error_Connection_Lost` | Readers cannot catch up within `pgsql-polardb_lsn_wait_timeout_ms`, or reader connections are failing during protected reads; safe reads are retried on the writer | Reduce replication lag, raise `pgsql-polardb_lsn_wait_timeout_ms`, investigate reader connection stability, or temporarily force reads to writer if the retry load is too high |
| Clients get errors on strict reads | `PolarDB_Wait_Error_Timeout` rising without matching `PolarDB_Wait_Reads_Retried_On_Writer`, or writer execution errors after retry | The reader timed out but retry was unsafe/unavailable, or the writer query itself failed | Check writer availability, whether results had already started, and whether the writer hostgroup was known; then tune lag or timeout |
| Average read latency climbing | wait latency per sent wait rises | Readers are lagging | Address replay lag, apply a byte cap, or temporarily set `read_target='primary'` |
| Some reads suddenly all go to the writer | `PolarDB_Consistency_Writer_Fallback` grows, possibly with `PolarDB_LSN_Stale_Count`; `PolarDB_Session_LSN_Routing` may flatten | A reader exceeded `max_lag_bytes`, had a stale/missing LSN under an enabled cap, group LSN was unknown, or strict RFQ requirements could not be satisfied | Expected safety behavior. Reduce lag, raise/disable `max_lag_bytes`, fix RFQ capability/profile issues, or accept the writer fallback until readers recover |
| High `PolarDB_RFQ_Profile_Skipped` | counter grows during RFQ-required reads | Pool contains connections created with protocol `off` or old profile settings | If `PolarDB_RFQ_Profile_Evicted` also grows, the pool is converging under demand. If only skipped grows, creation may be throttled, pooled-only acquisition may be in use, or compatible capacity may already exist. Verify per-HG/global `proxy_protocol` |
| High `PolarDB_Target_LSN_Fallback_Wait` | counter grows while wait latency also grows | No fresh cached reader at target was acquired; correctness relies on backend wait | Expected under lag. Investigate replica lag, freshness window, and monitor/RFQ LSN updates |
| `SET` errors / read errors on every eligible read | client sees an ERROR on a `SET polar_*` statement | Backends are not genuine PolarDB (Requirement C); they reject the PolarDB GUCs | Point the pair at real PolarDB backends, or set the pair's `check_type='read_only'` to disable PolarDB routing |
| `PolarDB_LSN_Updates_From_Monitor` = 0 although traffic flows | monitor LSN counter flat | `pgsql-polardb_monitor_lsn_updates=0`, or the monitor sees no LSN advance, or no PolarDB HG configured | Set `pgsql-polardb_monitor_lsn_updates=1`; confirm a `check_type='polardb'` pair is loaded to runtime |
| `PolarDB_LSN_Stale_Count` grows | counter rising with `max_lag_bytes` enabled | Reader acquisition rejected a primary/reader sample because the LSN was missing or stale | Check monitor/RFQ LSN flow, freshness window, and whether replicas are reporting current LSNs |
| A policy word is rejected | validation error lists allowed values | Removed or misspelled setting/value | Use the canonical table in Section 6; writer placement is `read_target=primary`, not a consistency mode |
| LOAD rejects `session_lsn` with `proxy_protocol=off` | validation names the hostgroup policy | SESSION_LSN cannot learn RFQ targets with protocol off | Use `v15_wait`, `v15`, or `legacy`; use `v15_wait` for extended waits |
| Feature seems entirely absent (no `pgsql-polardb_*` vars, no `replica_eligible` column) | admin tables lack the columns | Binary built `POLARDB_PROXY=0` (Requirement A) | Rebuild with the default `make` (`POLARDB_PROXY=1`) |

### 8.3 Reading the proxy log

Two `proxy_error` log lines are the most useful for operators (they fire regardless of trace level):

- **Wrapper build failure:** `"PolarDB WRAP: finalize failed: <reason>, sess=<ptr>"` (`lib/PgSQL_PolarDB_Wrap.cpp:186`). Paired with `PolarDB_Wait_Wrap_Safety_Abort++`. The `<reason>` tells you why (missing connection, empty query, etc.).
- **Knob rejected on SET:** the validation message uses the canonical values from Section 6 and rejects incomplete named profiles or invalid identity host/port pairs.
- **Config load rejection:** `session_lsn` with effective `proxy_protocol=off`, or `global_lsn` with a warning degradation action, is rejected before publication.

The result-processing path's "no LSN on RFQ" message (the requirement-B signal) is **not** an always-on log: it goes through the `POLARDB_TRACE` macro, which expands to `proxy_info()` only in a `POLARDB_DEBUG=1` build and to nothing in a release build (`include/PgSQL_PolarDB.h:69-80`; the trace itself at `lib/PgSQL_PolarDB_Flow.cpp:522-530`). In a normal release build the main signal for requirement B is `PolarDB_Server_LSN_Updates_From_RFQ` staying at 0 while missing-LSN counters move (Section 8.1). All detailed per-query routing decisions are likewise `POLARDB_TRACE` and off by default; build a trace binary with `make polardb-debug` (= `POLARDB_PROXY=1 POLARDB_DEBUG=1`) if you need them.

---

## 9. Notes, scope, and what is NOT here

- **CSN is not in this build.** The schema accepts `default`/`off`/`lsn`/`global_lsn`/`primary` (integer consistency mode `2` = `global_lsn`, with `lsn_global`/`global` as accepted aliases); the word `csn` and a session/global CSN mode are not present. CSN is a future, experimental mode that requires PolarDB backend support and does not exist here. See [15-LIMITATIONS-AND-ROADMAP.md](15-LIMITATIONS-AND-ROADMAP.md) and [18-FUTURE-CSN-DESIGN.md](18-FUTURE-CSN-DESIGN.md).
- **Transaction split, reader-failure actions, split warmup, and extended autocommit waits are present.** Transaction split remains simple-query only. Reader failures use `action_replica_loss`, `action_replica_error`, and `action_lsn_timeout`; marker-less `57014` cancellation is never replayed. Extended autocommit reads use in-band `W` only on `v15_wait`; unsupported profiles and unknown targets use the writer or configured error policy.
- **`pgsql-polardb_max_reader_lag_ms` is inert today** (no producer). `PolarDB_LSN_Stale_Count` is active for byte-lag stale/missing samples when `max_lag_bytes` is enabled; do not use it as a millisecond-lag signal (Section 6 note; `include/PgSQL_PolarDB.h:504`, `:536`).
- **Observability gaps to be aware of:** there is now a counter for consistency reader-acquisition fallback to writer and a counter for selected-reader wait bypass, but there is still no generic FORCE_PRIMARY reason breakdown, no counter for manual-mode bypass, no explicit wait-success counter beyond `PolarDB_Wait_LSN_Sent`, and no per-server LSN/lag gauge in stats. Some of these are tracked as roadmap items in [15-LIMITATIONS-AND-ROADMAP.md](15-LIMITATIONS-AND-ROADMAP.md).

---

## Appendix: Mermaid diagrams

### Diagram 1 — RYW data flow (Section 2)

```mermaid
flowchart LR
  C1[client write] --> W[writer]
  W -- RFQ carries LSN --> PX[ProxySQL records session write LSN]
  C2[client read] --> G{replica_eligible=1<br/>AND consistency=lsn?}
  G -- yes --> R[reader]
  R -- "SET polar_xact_split_wait_lsn = LSN; read" --> R2[reader blocks until caught up, then answers]
  R2 --> C3[client sees its own write]
  G -- no --> W
```

### Diagram 2 — Enable checklist as a decision flow (Sections 3 and 5)

```mermaid
flowchart TD
  A{Built POLARDB_PROXY=1?} -- no --> AX[Feature compiled out: rebuild with default make]
  A -- yes --> B{Patched libpq linked?}
  B -- no --> BX[No LSN captured: link patched libpq, or use consistency=primary]
  B -- yes --> C{Backends are genuine PolarDB?}
  C -- no --> CX[GUCs rejected: use real PolarDB or check_type=read_only]
  C -- yes --> D{pgsql_replication_hostgroups row check_type=polardb?}
  D -- no --> DX[polardb_active stays false: add the pair]
  D -- yes --> E{consistency_mode is session_lsn or global_lsn?}
  E -- no --> EX[No LSN wait: select an LSN mode]
  E -- yes --> F{Query rule replica_eligible=1 matches reads?}
  F -- no --> FX[Reads stay on writer: add a replica_eligible=1 rule]
  F -- yes --> OK[RYW enabled]
```

### Diagram 3 — The manual-route caveat (Section 7)

```mermaid
flowchart TD
  Q[incoming read] --> M{replica_eligible < 0<br/>AND destination_hostgroup >= 0?}
  M -- "yes (manual mode)" --> SKIP[PolarDB pipeline SKIPPED:<br/>route to destination_hostgroup<br/>NO wait, NO writer-fallback check]
  SKIP --> DEST{destination is a reader?}
  DEST -- yes --> STALE[possible stale read]
  DEST -- no --> SAFEW[writer: safe]
  M -- no --> PIPE[PolarDB collect/plan/execute runs:<br/>RYW honored or writer fallback used]
```

### Diagram 4 — best_effort vs strict on a wait timeout (Section 6)

```mermaid
flowchart TD
  RD[reader runs wrapped read] --> T{caught up before timeout?}
  T -- yes --> OK[return fresh rows]
  T -- "no (timeout)" --> MODE{polar_consistency_mode}
  MODE -- best_effort --> BE[return stale rows + WARNING<br/>PolarDB_Wait_Error_Timeout++]
  MODE -- strict --> ST[ERROR on reader, retry writer when safe<br/>PolarDB_Wait_Error_Timeout++]
```

---

Verified against this branch.
