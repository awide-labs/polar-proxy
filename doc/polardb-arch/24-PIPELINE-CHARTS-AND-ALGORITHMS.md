# 24 — Request / Result / Retry Pipeline — Charts & Algorithm Reference

| | |
|---|---|
| **Status** | Reference (explanatory; charts + tables for the implemented surface) |
| **Scope** | The end-to-end PolarDB request, result, and reader-failure-retry pipeline for the three routing modes now implemented — **regular** (pass-through), **session-consistency (LSN read-your-writes)**, and **transaction split** — plus the knobs, structures, and enums that drive them and how they interconnect. |
| **Audience** | Engineers and reviewers who want a visual mental model of the pipeline and a single place to look up the knobs/structs/enums. |
| **Companion** | [06-ROUTING-PIPELINE.md](06-ROUTING-PIPELINE.md) is the prose flow reference with stage IDs and a function index. This doc is the **visual + reference** layer over it. Wrapping detail: [07-QUERY-WRAPPING.md](07). Wait/timeout: [08](08-WAIT-TIMEOUT-AND-NOTICES.md). Publish/observe: [09](09-PUBLISH-AND-WRITE-TRACKING.md). Reader-failure design: [20](20-FUTURE-READER-FAILURE-RETRY-DESIGN.md). |
| **Verified against** | Current implementation with transaction split, background pool fill, retry, reader failure handling, and performance cleanup. Enum and structure references use `include/PgSQL_PolarDB.h`; pipeline references use `lib/PgSQL_PolarDB_*.cpp`. |

---

## Table of contents

1. [Orientation — three modes, one pipeline](#1-orientation)
2. [The pipeline at a glance](#2-the-pipeline-at-a-glance)
3. [Request path (per mode)](#3-request-path)
4. [The planner decision tree](#4-the-planner-decision-tree)
5. [Result path (process_result → observe → publish)](#5-result-path)
6. [Retry path (reader failure → policy → redispatch)](#6-retry-path)
7. [State machines (transaction split, ReaderFailureRoute lifecycle)](#7-state-machines)
8. [Knobs reference](#8-knobs-reference)
9. [Enums reference](#9-enums-reference)
10. [Structures reference](#10-structures-reference)
11. [How it all interconnects](#11-how-it-all-interconnects)

---

## 1. Orientation

PolarDB routing decides, per query, **where a read runs** and **what consistency it must satisfy**. Three modes coexist; the *resolved consistency mode* + the *transaction state* pick which one applies:

| Mode | When | What it does | RYW guarantee |
|---|---|---|---|
| **Regular** | non-PolarDB HG, `mode=off`, or no obligation | pass-through; normal ProxySQL routing | none (not needed) |
| **Session-consistency (LSN)** | `mode=lsn`, autocommit, simple-query read with a session target | route to a replica, prepend a `polar_xact_split_wait_lsn` wait so the replica blocks until it replays the session's target LSN | read-your-writes within the session |
| **Transaction split** | `txn_split_enabled=1`, inside an open splittable txn, eligible read | take a replica connection from the pool for one wrapped read, then return it | read-your-writes within the open txn |

The **single safety rule** across all modes: *anything uncertain fails closed to the writer* (no enforceable target, unknown LSN, unsafe query shape, build failure). A read is offloaded to a replica only when its consistency target can be enforced by the backend wait — or confirmed already satisfied (wait-bypass).

```
                       resolve consistency mode (session > HG > global)
                                        |
                 +----------------------+----------------------+
                 |                      |                      |
              mode=off            mode=lsn / primary       txn_split_enabled
                 |                      |                  & inside open txn
            [REGULAR]          [SESSION-CONSISTENCY]         [SPLIT]
```

---

## 2. The pipeline at a glance

Every query flows through four pipeline stages in `lib/PgSQL_PolarDB_Flow.cpp` — **collect → plan → execute → (dispatch) → result → publish** — orchestrated from `PgSQL_Session`. The result path feeds session state back so the *next* query plans correctly. A reader failure on the dispatched read enters the **retry path** before ProxySQL's generic error handling.

```
   CLIENT
     | SQL
     v
 +-------------------------------------------------------------------------------+
 |  REQUEST PATH                                  lib/PgSQL_PolarDB_Flow.cpp     |
 |                                                                               |
 |  collect() ---> plan() ---> execute() ---> [acquire reader] -> [wrap] ------+ |
 |  gather facts   decide      realize the     get_MyConn_*      build SET     | |
 |  (mode, txn,    RoutePlan    plan: set HG,   (RFQ profile +   wrapper or    | |
 |   query shape,  (RouteAction  acquire,        identity/SSL     bypass       | |
 |   session LSN,  + reason)     wrap/bypass     compatible)      (07)         | |
 |   reader plan)                                                              | |
 +--------------------------------------------------------------------|----------+
                                                                       | wire packet
                                                                       v
                                            +---------------------------------------+
                                            |  BACKEND (writer HG or reader HG)     |
                                            |  PolarDB executes; ReadyForQuery (RFQ)|
                                            |  carries LSN / XIDs / txn status      |
                                            +---------------------------------------+
                                                                       | result + RFQ
     +-----------------------------------------------------------------|----------+
     |  RESULT PATH                                  process_result()   v         |
     |                                                                            |
     |  has_lsn ? --yes--> positioned RFQ: advance session LSN, observe txn       |
     |     |               split state, clear unknown flags            -> publish |
     |     +-----no------> missing RFQ: set write_unknown/observed_unknown,       |
     |                     observe(lsn=0) for txn and route cleanup    -> publish|
     +-----------------------------------------------------------------|----------+
                                                                       | rows
                          (on dispatched-read failure: rc == -1)       v
     +-----------------------------------------------------------------+     CLIENT
     |  RETRY PATH                       lib/PgSQL_PolarDB_Failure.cpp  |
     |  polardb_on_failure(): classify -> decide action+target+route ->   |
     |  redispatch (writer | other reader) | forward | terminate        |
     +------------------------------------------------------------------+
```

Fast-bypass: when `POLARDB_PROXY=0`, `!polardb_active`, or a non-PolarDB HG, the whole block is a no-op and routing is exactly upstream.

---

## 3. Request path

### 3.1 Regular (pass-through)

```
collect() --> route is non-PolarDB HG  OR  resolved mode == OFF  OR  no obligation
                                   |
                              plan(): RouteAction = PASSTHROUGH
                                   |
                              execute(): keep current/target HG, no wrap
                                   |
                              dispatch unchanged  ----> backend
```
No wait, no wrapper, no reader filtering. PolarDB counters stay flat.

### 3.2 Session-consistency (LSN autocommit read) — the core case

```
collect()                              plan()                         execute()
--------                               ------                         --------
mode = lsn                             target = max(write_lsn,        acquire reader:
autocommit, simple-query SELECT          observed_lsn)                  get_MyConn_polardb_reader
replica_eligible                       target > 0 ?                     (RFQ-LSN profile +
session target = max(write,           +-- yes: REPLICA_WITH_WAIT        DB/identity/SSL match,
  observed) [+ baseline seed]         |        wait_spec{LSN, mode,     byte-lag cap)
                                      |        timeout}                       |
                                      +-- no (first read):              status ?
                                              baseline=primary ?         +-- ACQUIRED -> reader is
                                                seed from writer mirror  |     confirmed caught up?
                                                else PASSTHROUGH/reader  |       yes: wait-BYPASS
                                                                         |       no : emit wait SET
                                                                         +-- *_LSN_* / LAG -> FORCE
                                                                         |     PRIMARY (writer)
                                                                         +-- RFQ_UNAVAILABLE ->
                                                                               route_rfq_policy:
                                                                               strict=writer /
                                                                               best_effort=reader,
                                                                               degraded (warn+count)
                                                       |
                                                  [wrap] build_wrapped_wait_query (07):
                                                  "SET polar_xact_split_wait_lsn=<t>; <query>"
                                                       |
                                                  dispatch to reader HG --> backend waits, replays
                                                  to <= target, then runs the read
```

The wait `SET` is the **RYW enforcement point**. The replica cannot answer the read until it has replayed to the target LSN (`strict` → error on timeout; `best_effort` → stale rows + one warning). The wrapper is skipped only when selection confirms the chosen reader's fresh cached LSN already reached the target and acquisition remains on that reader (`wait_bypass_allowed`) — see [06 §E.3].

### 3.3 Transaction split (in-transaction read)

Inside an open transaction the autocommit LSN path does not apply; instead the **transaction-split state machine** (§7) decides whether the txn is split-readable, and the planner runs the **split eligibility check**.

```
collect() (in_transaction)            plan(): in-txn branch              execute()
--------                              ------                             --------
split state from session              polardb_txn_split_rejection_reason  prepare split read:
  (stage, xids, primary_lsn,            (txn_split_enabled,                 acquire pooled-only
   splittable, wal_pending,             split state, write_unknown,         reader (same DB/user,
   blocked)                             observed_unknown, multi_stmt,       RFQ-capable, NOT a
query shape (locking? safe select?)     extended, safe_read, locking_read)  skipped reader)
                                              |                                   |
                                        reason == NONE ?                    pool hit -> use for one read
                                        +-- no: FORCE_PRIMARY(reason)       pool empty -> queue
                                        |       (route to writer)             lazy warmup +
                                        +-- yes: REPLICA_TXN_SPLIT            FORCE_PRIMARY this read
                                                 wait_spec.target =                |
                                                 split.primary_lsn          [wrap] xids head +
                                                 xids = split.xids          polar_xact_split_wait_lsn
                                                       |                          |
                                                       +--------------------> dispatch to reader;
                                                                              restore primary backend
                                                                              after the read
```

Split is **opportunistic**: if no compatible pooled reader is free it does *not* connect inline — it declines the split (routes the read to the primary) and queues a warmup so a future read can be offloaded. The temporary reader gets the **same** XIDs + LSN wait, so RYW holds inside the txn.

---

## 4. The planner decision tree

`polardb_plan()` produces one `PolarDB_Query_RoutePlan` with a `RouteAction` and, on a writer route, a `RouteActionReason`. The order matters — earlier checks win.

```
plan(route_ctx):

  if reader_failure_route == FORCE_WRITER          -> FORCE_PRIMARY (READER_FAILURE_FORCE_WRITER)
  if hint == /* route=primary */                 -> FORCE_PRIMARY (HINT_PRIMARY)
  if resolved mode == PRIMARY_ONLY               -> FORCE_PRIMARY (MODE_PRIMARY)
  if resolved mode == OFF                         -> PASSTHROUGH

  if in_transaction:
      reason = txn_split_rejection_reason(...)         # the split eligibility check, in order:
         txn_split_enabled? ............. else HG_SPLIT_DISABLED
         multi_statement? ............... MULTI_STATEMENT
         extended_protocol? ............. EXTENDED_PROTOCOL
         locking_read (FOR UPDATE/SHARE)? SPLIT_LOCKING_READ
         not a safe SELECT (incl. WITH)?  SPLIT_NOT_SELECT
         write_unknown? ................. SPLIT_WRITE_LSN_UNKNOWN     <-- RYW check
         observed_unknown? .............. SPLIT_OBSERVED_LSN_UNKNOWN  <-- RYW check
         split.blocked? ................. SPLIT_BLOCKED
         split.wal_pending? ............. WAL_PENDING
         stage != TXN_SPLITTABLE? ....... IN_TRANSACTION
         xids empty? .................... INVARIANT_VIOLATION
         primary_lsn == 0? .............. NO_TXN_LSN
      reason == NONE ?  REPLICA_TXN_SPLIT (wait on split.primary_lsn, carry xids)
                  else  FORCE_PRIMARY(reason)

  # autocommit path (not in txn)
  if multi_statement / extended-with-target       -> FORCE_PRIMARY (MULTI_STATEMENT / EXTENDED_PROTOCOL)
  if write_unknown / observed_unknown             -> FORCE_PRIMARY (WRITE/OBSERVED_LSN_UNKNOWN)
  if session target > 0                            -> REPLICA_WITH_WAIT (wait on target)
  else (first read):
      baseline == PRIMARY & mirror known ?  REPLICA_WITH_WAIT (seed from mirror)
      baseline == PRIMARY & mirror unknown? FORCE_PRIMARY (PRIMARY_LSN_UNKNOWN)
      else                                  PASSTHROUGH / reader (no target yet)

  # reader acquisition (execute) can still downgrade a replica plan:
      ACQUIRED ........................... keep replica (wrap or bypass)
      *_LSN_UNKNOWN / *_LSN_STALE / LAG ... FORCE_PRIMARY (safety)
      RFQ_UNAVAILABLE .................... route_rfq_policy: strict=writer / best_effort=degraded reader
      READER_UNAVAILABLE / READER_BUSY ... keep ProxySQL's normal no-conn retry (not kept to writer)
```

`RouteAction` ∈ {`PASSTHROUGH`, `REPLICA_WITH_WAIT`, `FORCE_PRIMARY`, `REPLICA_TXN_SPLIT`}.

### 4.1 Reader load balancing

Reader selection happens before connection reuse and uses only topology, status,
configured weight, LSN/lag state, and globally visible active load. Worker-local
connections cannot choose or change the server.

```
select_reader(servers, request):
  if there is one reader:
      use it when eligible

  if there are exactly two readers:
      choose with the hostgroup weighted sequence
      use the peer when the first reader is unusable or the request needs it

  if there are three or more readers and the request is an ordinary read with:
       equal positive weights
       no LSN target
       no lag cap
       no excluded reader
       reset/create allowed:
      sample two distinct healthy readers
      choose the lower globally visible active count
      break an exact tie randomly

  otherwise:
      scan the complete candidate list
      apply status, LSN, lag, exclusion, and pooled-only checks
      use weighted selection and compare eligible candidates by normalized load
```

The direct two-reader sample is an optimization for the common equal-weight
three-or-more-reader case. Any special policy or unusable sample returns to the
complete selection path. `PolarDB_Reader_Pool_P2C_*` counters report sampled
decisions and whether active load or a random tie-break chose the result.

---

## 5. Result path

`process_result()` runs after the backend's `ReadyForQuery` and feeds session state forward. The single fork is **has_lsn** (did the RFQ carry an LSN?).

```
process_result(backend result + RFQ):

  has_lsn ?
   |
   +-- YES  positioned RFQ  -> polardb_process_positioned_rfq():
   |          if rfq from primary:
   |             clear write_unknown / observed_unknown sticky flags
   |             observe transaction split (advance state, primary_lsn monotonic)
   |          observed_lsn = max(observed_lsn, lsn)              # read-after-read monotonic
   |          if is_write: write_lsn = max(write_lsn, lsn)       # read-your-writes
   |
   +-- NO   missing RFQ     -> polardb_process_missing_rfq():
              if writer-scope matches:
                 if is_write:  write_unknown = true   (+warn, +PolarDB_Write_Missing_LSN)
                 else (tracked read): observed_unknown = true (+PolarDB_Read_Missing_LSN)
              # observer unification: still maintain transaction and route cleanup
              observe_transaction_split(conn, lsn=0, primary_source=true)
              #  -> on txn close ('I'): reset split state + clear ReaderFailureRoute
              #  -> primary_lsn is monotonic: lsn=0 never clobbers a valid LSN

  -> publish (RequestEnd): persist session LSN; per-query wait/wrapper/split state resets
```

Key invariants: session **write/observed LSN never regress**; a missing-LSN write sets `write_unknown` so the *next* read routes to the writer when the target LSN is unavailable (autocommit) or is rejected by the split eligibility check (in-txn); the missing-RFQ path still drives the observer so transaction-split state and the reader-failure route are correctly cleared at transaction close.

---

## 6. Retry path

A reader failure on a dispatched consistency/split read enters `polardb_on_failure()` **before** ProxySQL's generic `rc==-1` retry. One handler covers both wait-reads and split reads; one selector maps the failure to a policy.

```
rc == -1 on a PolarDB-dispatched read
        |
        v
 polardb_on_failure(outcome):
   failure = reader_failure_view(outcome)   # unified PolarDB_ReaderFailure:
   |                                        #   {split_read, kind inputs, result_started,
   |                                        #    reader id, retry_pkt, wait_spec, txn_xids,
   |                                        #    reader_plan, fallback_writer_hg}
   |
   decision = reader_decision_for(failure):
      kind = classify(failure)              # CONNECTION_LOST | WAIT_TIMEOUT | REUSABLE_ERROR
      action = knob[kind]                   # reader_death / reader_timeout / reader_error _action
                                            #   -> RETRY | FORWARD | TERMINATE
      if action==RETRY & split & kind==CONNECTION_LOST & attempts < BUDGET(=1):
           target = OTHER_READER ;  route = SKIP_READER
      else if action != TERMINATE:
           target = WRITER       ;  route = FORCE_WRITER
   |
   +-- TERMINATE ----------------> close session
   +-- RETRY, target==OTHER_READER:
   |     try_redispatch_to_other_reader(failure)         # declines unless every precondition is met
   |        success -> re-run on a different replica with the SAME wait_spec+xids; skip failed reader
   |        decline -> fall through to writer
   +-- RETRY, target==WRITER:
   |     try_redispatch_to_writer(failure)               # rebuild unwrapped query on writer
   +-- FORWARD ------------------> surface reader error, keep txn alive on writer
   +-- PASSTHROUGH --------------> hand back to generic rc==-1 (non-PolarDB / ordinary SQL error)

   apply_reader_failure_route(route)  ->  FORCE_WRITER keeps the rest of the transaction on the writer
                              SKIP_READER excludes the failed reader from acquisition
```

### 6.1 Policy matrix (failure kind × knob → action → target/route)

| Failure kind | Knob | `retry` → | `forward` → | `terminate` → |
|---|---|---|---|---|
| `CONNECTION_LOST` (dead/lost reader) | `reader_death_action` (default retry) | split: **OTHER_READER**+SKIP_READER (budget 1) then WRITER; else WRITER+FORCE_WRITER | FORWARD | TERMINATE |
| `WAIT_TIMEOUT` (strict timeout) | `reader_timeout_action` (default retry) | **WRITER**+FORCE_WRITER (never another reader — avoids a second tail timeout) | FORWARD | TERMINATE |
| `REUSABLE_ERROR` (SQL error, conn reusable) | `reader_error_action` (default forward) | WRITER+FORCE_WRITER | FORWARD (surface the real error) | TERMINATE |

### 6.2 `reader_retry` (OTHER_READER) preconditions — all required, else decline to writer

```
try_redispatch_to_other_reader declines unless ALL hold:
  failure.split_read                       # only split reads
  failure.retry_pkt.ptr                     # have the original packet
  !failure.result_started                   # no client bytes sent yet  (no retry-after-partial)
  reader id present                          # so the failed reader can be skipped
  wait_spec.has_wait() && !txn_xids.empty()  # the consistency target + XIDs survived capture
  reader_plan.has_consistency_target_lsn()
  reader_retry_attempts < POLARDB_READER_RETRY_BUDGET (=1)   # one attempt per statement
  build_txn_split_wrapped_query(...) succeeds
-> then rebuild the wrapper with the SAME captured wait_spec  => RYW preserved on the new reader
```

The budget (1) + per-statement reset (`reset_for_new_query`) prevents a retry loop between readers; a declined reader-retry falls through to the writer cleanly.

---

## 7. State machines

### 7.1 Transaction-split state machine (`PolarDB_TransactionSplitStage`)

Driven by `observe_primary_rfq()` from the primary's RFQ (txn status + split flags):

```
                    txn opens on primary
   NONE  ---------------------------------->  TXN_ON_PRIMARY
    ^                                              |
    | txn status 'I' (commit/rollback)             | RFQ: splittable && !wal_pending && xids
    | OR !split_enabled  (reset)                   v
    |                                         TXN_SPLITTABLE  <----+
    |                                              |              | RFQ still splittable
    |                                              | split read   |
    |                                              v dispatched    |
    +-------------------------------------  TXN_SPLIT_READ_ACTIVE -+
                                                   |
                       split read fails -> blocked=true; stage falls back to TXN_ON_PRIMARY
                       (eligibility check then returns SPLIT_BLOCKED for the rest of the txn)
```
Notes: `'E'` (aborted txn) holds at `TXN_ON_PRIMARY` (never splittable). `primary_lsn` advances **monotonically** (a missing-LSN `observe(0)` never lowers it). On commit/rollback the state resets *and* the reader-failure ReaderFailureRoute is cleared (observer-unification).

### 7.2 ReaderFailureRoute lifecycle (`PolarDB_ReaderFailureRoute` + skipped-reader identity)

```
        (no failure)                    reader failure, action=RETRY
   NONE ------------------+        +-----------> target WRITER  -> FORCE_WRITER (writer_hg set)
    ^                     |        |                                   | rest of txn -> writer
    |                     +--------+
    | cleared by:                  +-----------> target OTHER_READER -> SKIP_READER
    |  - observer txn close ('I')                  (skipped_reader = failed reader id)
    |  - !split_enabled observe                       | next split acquisition excludes it
    |  - session reset / RequestEnd                    | (one statement; budget 1)
    +--------------------------------------------------+
```
`FORCE_WRITER` clears any skipped-reader identity; `SKIP_READER` records the failed reader's hg/addr/port and feeds the acquisition filter. Both are transaction-scoped and cleared at txn close or session reset.

---

## 8. Knobs reference

Thread variables (`pgsql-polardb_*`), resolved per query. Consistency mode resolves **session-override > hostgroup policy > global**.

| Knob | Values (default) | Drives | Read at |
|---|---|---|---|
| `consistency_mode` | `off` / `lsn` / `primary` (off) | the routing mode; `PolarDB_ConsistencyMode` | plan |
| `route_rfq_policy` | `strict` / `best_effort` (strict) | what to do when no enforceable RFQ target; `PolarDB_RfqRoutePolicy` | plan/execute |
| `session_lsn_baseline` | `observed` / `primary` (observed) | first-read target source; `PolarDB_SessionLsnBaseline` | plan |
| `wait_timeout_mode` | `strict` / `best_effort` | replica wait behavior on timeout; `PolarDB_WaitMode` | wrap |
| `lazy_warmup_split` | `true` / `false` | enable demand-driven split-reader pool warmup | warmup enqueue/drain |
| `reader_death_action` | `retry` / `forward` / `terminate` (retry) | policy for `CONNECTION_LOST` | retry path |
| `reader_timeout_action` | `retry` / `forward` / `terminate` (retry) | policy for `WAIT_TIMEOUT` | retry path |
| `reader_error_action` | `retry` / `forward` / `terminate` (forward) | policy for `REUSABLE_ERROR` | retry path |
| `lag_bytes` | bytes (cap) | byte-lag safety cap for reader acquisition | execute (reader filter) |
| `lag_wait_ms` | ms | wait-timeout duration for the replica wait | wrap |
| `lsn_freshness_ms` | ms | how long a cached reader LSN sample is trusted for the lag cap | execute |
| `lag_ms` | ms | reserved (inert — no ms-lag producer yet) | — |
| `monitor_lsn_updates` | on / off (on) | let the monitor feed the per-server LSN cache | monitor |
| `proxy_protocol` | `off` / `v15` / `legacy` | PolarDB startup dialect; `PolarDB_ProxyProtocol` | connection build |
| `proxy_identity_host` | string | fallback advertised client host (proxy-mode startup) | connection build |
| `proxy_identity_port` | int | fallback advertised client port | connection build |
| (per-HG) `txn_split_enabled` | 0 / 1 | enable transaction split on a replication-hostgroup | plan (split eligibility check) |

controls above all knobs: compile-time `POLARDB_PROXY` and runtime `PgHGM->status.polardb_active`.

---

## 9. Enums reference

| Enum | Values | Role |
|---|---|---|
| `PolarDB_ConsistencyMode` | OFF=0, SESSION_LSN=1, PRIMARY_ONLY=3 | resolved routing mode |
| `PolarDB_WaitType` | NONE=0, LSN=2 | wait kind in a plan (only LSN today) |
| `PolarDB_WaitMode` | BEST_EFFORT=1, STRICT=2 | replica wait timeout behavior |
| `PolarDB_RfqRoutePolicy` | BEST_EFFORT=1, STRICT=2 | fallback when no enforceable RFQ target |
| `PolarDB_SessionLsnBaseline` | OBSERVED=1, PRIMARY=2 | first-read target source |
| `PolarDB_ReaderStatus` | ACQUIRED, READER_UNAVAILABLE, READER_BUSY, RFQ_UNAVAILABLE, PRIMARY_LSN_UNKNOWN, READER_LSN_UNKNOWN, READER_LSN_STALE, READER_LAG_EXCEEDED | reader-acquisition outcome (`redirects_to_writer()` flags the safety subset) |
| `RouteAction` | PASSTHROUGH, REPLICA_WITH_WAIT, FORCE_PRIMARY, REPLICA_TXN_SPLIT | the planner's decision |
| `RouteActionReason` | NONE, EXTENDED_PROTOCOL, IN_TRANSACTION, MULTI_STATEMENT, MODE_PRIMARY, HINT_PRIMARY, WRITE_LSN_UNKNOWN, OBSERVED_LSN_UNKNOWN, PRIMARY_LSN_UNKNOWN, READER_FAILURE_FORCE_WRITER, WAL_PENDING, SPLIT_BLOCKED, SPLIT_NOT_SELECT, SPLIT_LOCKING_READ, SPLIT_WRITE_LSN_UNKNOWN, SPLIT_OBSERVED_LSN_UNKNOWN, NO_TXN_LSN, INVARIANT_VIOLATION, HG_SPLIT_DISABLED | why a read was forced to the writer (diagnostics) |
| `PolarDB_TransactionSplitStage` | NONE, TXN_ON_PRIMARY, TXN_SPLITTABLE, TXN_SPLIT_READ_ACTIVE | transaction-split state machine |
| `PolarDB_FailureAction` | PASSTHROUGH, RETRY, FORWARD, TERMINATE | resume verb returned by the failure handler |
| `PolarDB_ReaderAction` | RETRY=0, FORWARD=1, TERMINATE=2 | operator policy (what the knob selects) |
| `PolarDB_ReaderFailureKind` | CONNECTION_LOST=0, WAIT_TIMEOUT=1, REUSABLE_ERROR=2 | failure class → which knob applies |
| `PolarDB_RetryTarget` | WRITER=0, OTHER_READER=1 | redispatch destination when action=RETRY |
| `PolarDB_ReaderFailureRoute` | NONE=0, FORCE_WRITER=1, SKIP_READER=2 | transaction route after a handled failure |
| `PolarDB_WriterState` | LIVE=0, NOT_STARTED=1, LOST=2 | writer-txn state at failure time |
| `PolarDB_Query_WrapperKind` | (see 07) | which wrapper a query carries |
| `PolarDB_ProxyProtocol` | OFF, V15, LEGACY | startup dialect |
| `PolarDB_NodeType` | UNKNOWN, PRIMARY, REPLICA, STANDBY | topology role |

**Three failure-related enums, three roles** (do not conflate): `ReaderFailureKind` = *what failed*; `ReaderAction` = *policy verb* (the setting); `FailureAction` = *session-resume verb* returned to the rc==-1 site; `RetryTarget`/`ReaderFailureRoute` = *where to send the retry* / *how to route later statements*.

---

## 10. Structures reference

| Struct | Lifetime | Holds | Produced / consumed |
|---|---|---|---|
| `PolarDB_Query_RoutePlan` | per query (request-stack) | `RouteAction` + reason, `wait_spec`, `reader` plan, `target_hg`, `txn_xids` | `plan()` → `execute()` |
| `PolarDB_WaitSpec` | per query | wait `type`(LSN), `target` LSN, `timeout_ms`, `mode` | plan → wrap |
| `PolarDB_Query_ReaderPlan` | per query | reader HG, `consistency_target_lsn`, `fallback_writer_hg`, route policy, lag inputs | plan → reader acquisition |
| `PolarDB_TransactionSplitState` | per session | split `stage`, `xids`, `primary_lsn`, `splittable`/`wal_pending`/`blocked` flags | result `observe` → plan eligibility check / split execute |
| session consistency state | per session | `write_lsn`, `observed_lsn` (monotonic), `write_unknown`/`observed_unknown` sticky flags | result → plan; the RYW source of truth |
| `PolarDB_ReaderFailure` | per failure | unified failure view: kind inputs, `result_started`, reader id, `retry_pkt`, `wait_spec`, `txn_xids`, `reader_plan`, `fallback_writer_hg` | `reader_failure_view()` → decision + redispatch |
| `PolarDB_ReaderFailureDecision` | per failure | `kind`, `action`, `retry_target`, `reader_failure_route`, `reason` | `reader_decision_for()` → on_failure |
| `PolarDB_StartupClientContext` | per backend conn | advertised `identity`, `frontend_ssl`/`ssl_version`/`ssl_cipher`, proxy-session fields; `compatible_for_reuse()` | connection build + pool/warmup matching |
| per-query state (`reset_for_new_query`) | per query | `reader_retry_attempts`, wait/wrapper/dispatch transient state | reset at RequestEnd / new query |
| ReaderFailureRoute + skipped-reader fields (on session) | per txn | `polardb_txn_reader_failure.route`, `polardb_txn_writer_hg`, `polardb_txn_skipped_reader_{hg,address,port}` | retry path set → plan/acquisition read; cleared by observer/reset |

---

## 11. How it all interconnects

```
 KNOBS (thread vars, per-HG)                         SESSION STATE (per client)
  consistency_mode -----+                             write_lsn / observed_lsn (monotonic)
  route_rfq_policy      |                             write_unknown / observed_unknown (sticky flags)
  session_lsn_baseline  |                             TransactionSplitState (state machine)
  wait_timeout_mode     |                             ReaderFailureRoute + skipped-reader identity
  reader_*_action       |                                   |
  lag_bytes / *_ms      |                                   |
  txn_split_enabled     |                                   |
        |               v                                   v
        |        +-------------+      reads      +----------------------+
        +------> |  collect()  | -------------> |  plan()              |
                 +-------------+  route_ctx     |  -> RoutePlan        |
                                                |     {RouteAction,     |
                                                |      reason,wait_spec,|
                                                |      reader, xids}    |
                                                +----------+------------+
                                                           |
                                                +----------v------------+
                                                |  execute()            |
                                                |  acquire reader       |
                                                |  (ReaderStatus) +     |
                                                |  wrap / bypass (07)   |
                                                +----------+------------+
                                                           | dispatch
                                                           v
                                                      BACKEND + RFQ
                                                           |
                          +--------------------------------+-----------------------------------+
                          | result OK                                          | rc == -1   |
                          v                                                    v            |
                 +-----------------+                                  +------------------+   --+
                 | process_result  |  has_lsn? -> advance session     | polardb_on_        |   |
                 | / observe       |  LSN + split state, set/clear     | failure()         |   |
                 | / publish (09)  | unknown; observer maintains route | view->decision   |   |
                 +--------+--------+                                   |  -> redispatch /  |   |
                          | feeds the NEXT query's plan                |     forward /     |   |
                          +--------------------------------------------+     terminate;    |
                                                                       |     set ReaderFailureRoute  |
                                                                       +---------+-------------+
                                                                                 | retry feeds
                                                                                 v
                                                                          (writer | other reader)
```

The loop closes through **session state**: a write advances `write_lsn`; the next read's `plan()` reads that and emits a wait; a reader failure sets a `ReaderFailureRoute` that the next `plan()` follows; result processing clears the route at transaction close. Settings only configure each stage; they never carry state between queries. All carried state lives in the per-session structures above, so read-your-writes and reader-failure routing are handled per session.
