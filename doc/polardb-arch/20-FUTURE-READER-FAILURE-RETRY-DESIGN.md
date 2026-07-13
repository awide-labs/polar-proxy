# 20 — Reader-Failure Recovery and Retry

> Scope: how a failed replica read is recovered — RETRY (re-run on a safe target), FORWARD (send the real error but keep the transaction alive), TERMINATE (close the session), and writer-loss poison (reserved for the writer's own death). | Audience: R/M/O/C | Status: active for autocommit wait reads and simple-query transaction-split reads; roadmap for advanced policies | Prereqs: [01-BACKGROUND-AND-DESIGN.md](01-BACKGROUND-AND-DESIGN.md), [06-ROUTING-PIPELINE.md](06-ROUTING-PIPELINE.md), [10-SESSION-INTEGRATION.md](10-SESSION-INTEGRATION.md), [14-INVARIANTS-AND-FAILURE-MODES.md](14-INVARIANTS-AND-FAILURE-MODES.md), [15-LIMITATIONS-AND-ROADMAP.md](15-LIMITATIONS-AND-ROADMAP.md), [19-FUTURE-TXN-SPLIT-DESIGN.md](19-FUTURE-TXN-SPLIT-DESIGN.md) | Verified against: this branch

## 0. Read this first: current branch vs remaining roadmap

This document now has two layers. The current branch implements the common reader-failure policy for:

- autocommit wait-wrapped reads, where retry targets the writer before any user result is sent; and
- simple-query transaction-split reads, where policy can retry, forward, or terminate. For split connection loss, retry first tries another compatible reader with the same XID/LSN wrapper before falling back to the writer. For wait timeouts, retry targets the writer to avoid spending a second tail timeout on another lagging replica.

The remaining roadmap in this document is advanced policy work: per-error-class policy tables, retry budgets, reader circuit-breaker / quarantine, and broader ranking integration. CSN/global consistency remains outside this document's active scope.

This document was originally written while comparing this branch with the full
prototype. It now describes the active code in this branch. Line anchors are a
review aid, not a stable API; prefer symbol search when the tree has moved.

### 0.1 The hard prerequisite

The split side of this feature is useful around transaction split (see [19-FUTURE-TXN-SPLIT-DESIGN.md](19-FUTURE-TXN-SPLIT-DESIGN.md)). The reason is simple:

- This implementation can route an eligible simple-query in-transaction read to a replica after primary RFQ XID evidence is complete.
- Reader-failure recovery keeps a **live transaction** alive when one of its offloaded reads fails on a replica, then chooses retry, forward, or terminate behavior.

The two active reader-failure families are therefore: an **autocommit wait read** that can retry on the writer when no user result has started, and a **split read** that can retry/forward/terminate under policy while preserving the writer transaction.

---

## 1. The problem in one paragraph

When a read that ProxySQL **offloaded to a replica** fails, the proxy must decide what the client sees, **without lying about the writer's transaction**. The design treats the proxy as a **transparent forwarder**: the proxy decides only *routing* (which backend runs a statement); the client owns *transaction control* (the `BEGIN` / `COMMIT` / `ROLLBACK` commands). A reader failure is **not** transaction-dooming — the offloaded read runs on a separate replica session, and its error does not abort the writer's transaction. (This is verified against the PolarDB database kernel; the design records it as the core finding at `doc/polardb-arch/23-READ-SIDE-FAILURE-RECOVERY.md:184` (full implementation).) So the proxy has **three honest outcomes**, plus **one synthetic outcome** reserved for when the writer connection itself dies.

---

## 2. Vocabulary (defined on first use)

| Term | Meaning |
|---|---|
| **writer** (primary) | The only backend that can *hold* a transaction. All writes and explicit transactions live here. |
| **reader** (replica) | A backend that runs *offloaded reads* only. It never holds the transaction. |
| **offloaded read** | A read the proxy sent to a replica instead of the writer. Two kinds: split read and wait read (below). |
| **wait read** | A read prefixed with `SET polar_xact_split_wait_lsn=...` so the replica waits for the writer's WAL position before answering (read-your-writes). **This implementation has this, but autocommit-only.** |
| **split read** | A simple-query in-transaction read run on a replica that *mimics* the writer's transaction by importing its transaction IDs (XIDs). This implementation supports the simple-query split path; extended-protocol split remains outside the active scope. |
| **rc == -1** | The session handler's "the backend query failed" branch (full implementation `lib/PgSQL_Session.cpp:4061` onward). This is the single seam the feature hooks. |
| **RFQ** (ReadyForQuery) | The PostgreSQL wire message a backend sends after a command. It carries a transaction-status byte. |
| **RFQ('T') / RFQ('E') / RFQ('I')** | The transaction-status byte in RFQ: `'T'` = the client is in a live transaction; `'E'` = the transaction is in PostgreSQL's aborted state; `'I'` = idle (no transaction). The proxy must send the **truthful** byte. |
| **tx_poisoned** | An existing session flag in this implementation and upstream. When a backend dies mid-transaction, the proxy synthesizes `ERROR 25P02` (current transaction is aborted) + `RFQ('E')` and keeps the client alive so it can `ROLLBACK`. **Shared base; reused unchanged by this feature.** |
| **reusable** | `is_connection_in_reusable_state()` = `!(PQTRANS_UNKNOWN || PQTRANS_ACTIVE)`. It is the **death-vs-error discriminator**: a connection **death** maps to *not reusable*; a plain **SQL error** or a **strict wait-timeout** maps to *reusable* (full implementation `lib/PgSQL_Connection.cpp:2178`). |
| **writer-only route** | Session state set after one reader failure that forces the **rest of the transaction** onto the writer. |
| **25P02** | The PostgreSQL SQLSTATE `ERRCODE_IN_FAILED_SQL_TRANSACTION` ("current transaction is aborted"). Used **only** by the writer-loss poison path, never by reader recovery (constant at full implementation `include/PgSQL_Error_Helper.h:506`; this implementation `include/PgSQL_Error_Helper.h:506`). |
| **08006** | The PostgreSQL SQLSTATE `ERRCODE_CONNECTION_FAILURE`. Synthesized when a reader **died with no result** and the proxy must forward something. |

---

## 3. The model — three honest outcomes plus one reserved synthetic outcome

When a reader (offloaded) read fails (`rc == -1`), the feature classifies the failure, resolves the writer's state, and picks one of four outcomes.

```
 a reader (offloaded) read FAILS  (rc == -1)
            |
            v
   classify the failure   ---------------------------------------------------+
            |                                                                 |
   resolve WRITER STATE (LIVE / NOT_STARTED / LOST)                           |
            |                                                                 |
   +--------+------------------+---------------------+--------------------+    |
   v                           v                     v                    v    |
 RETRY                      FORWARD               TERMINATE        (writer-loss |
 redispatch on             forward the REAL      tear the          POISON --    |
 the WRITER                error + RFQ('T')       session down      NOT reached  |
 (no rows sent yet)        keep the txn ALIVE    (opt-in)           from here)   |
   |                           |                     |                    |    |
 writer runs it:            client sees the       writer rolled       only the  |
  success -> data,           failed read,          back on            WRITER's  |
   RFQ('T'), continue        stays in live txn,    teardown           own death |
  error   -> native 'E',     decides next          no orphan          triggers  |
   forwarded, client         (re-issue / ROLLBACK)                     this <----+
   decides
```

A reader failure **never** poisons. Poison is reserved strictly for the **writer's own** death.

### 3.1 Outcome 1 — RETRY (redispatch on the writer)

Re-run the **client's own** query on the writer. This is allowed only when **both** are true:

1. the writer is `LIVE`, and
2. **no client-visible rows were sent yet** (otherwise a redispatch would double-send rows to the client).

Both possible results are correct and handled by the normal path:

- **success** returns the data with `RFQ('T')` and the transaction continues;
- a **genuine SQL error** puts the writer into PostgreSQL's native aborted state, and the proxy forwards that REAL error + `RFQ('E')`.

The proxy issues **no** `ROLLBACK`. The client's next command (`ROLLBACK` / `ROLLBACK TO SAVEPOINT` / `COMMIT`) routes to the writer, and PostgreSQL recovers natively. This is why redispatch (not a proxy-issued rollback) is the recovery mechanism: **the proxy must never guess the client's intent against a live transaction.**

Implementation: `polardb_try_redispatch_to_writer` (full implementation `lib/PgSQL_PolarDB_Failure.cpp:352`).

### 3.2 Outcome 2 — FORWARD (forward the real error, keep the transaction alive)

Reached when redispatch is **not** chosen (rows were already streamed to the client, or the policy for this failure type is FORWARD). The proxy forwards the replica's **REAL** `ErrorResponse` (or a synthesized `08006 connection_failure` if the replica died with no result), followed by **`RFQ('T')`**, and then releases the reader. The writer's transaction is genuinely alive, so `'T'` is the truth; sending `'E'` would lie. The client decides what to do next: re-issue the read (now kept to the writer), continue, or `ROLLBACK`.

Implementation: `polardb_forward_and_continue` and `polardb_forward_reader_error` (full implementation `lib/PgSQL_PolarDB_Failure.cpp:391` and `:311`).

### 3.3 Outcome 3 — TERMINATE (fail-fast)

Opt-in. The proxy tears the session down. PostgreSQL rolls back the writer transaction on disconnect (every active-transaction backend is physically destroyed, never returned to the pool). Used in two cases: (a) the operator set a `TERMINATE` policy for this failure type, or (b) the writer turned out to be `LOST` (see §6).

Implementation: `polardb_terminate_reader` (full implementation `lib/PgSQL_PolarDB_Failure.cpp:993`).

### 3.4 Reserved — writer-loss POISON

This is the **only** place the proxy fabricates transaction control. It is triggered **only** by the *writer's own* `rc == -1` (the connection holding the transaction died), which is the existing `tx_poisoned` path — `handler_minus1_PoisonTransaction` (full implementation `lib/PgSQL_Session.cpp:3020`; the same function exists unchanged in this implementation at `lib/PgSQL_Session.cpp:2838`, see §11). A reader failure **never reaches this path**. If a reader failure *discovers* that the writer is already gone (writer state `LOST`), it `TERMINATE`s instead of poisoning — see §6.

### 3.5 The four outcomes at a glance

| Outcome | When | What the client sees | Transaction state afterwards | Who fabricates RFQ? |
|---|---|---|---|---|
| **RETRY** | writer `LIVE` and no rows sent yet, and the policy for this failure type is RETRY | the writer's real result (data, or the writer's native SQL error + `RFQ('E')`) | continues (or is in PostgreSQL's native aborted state, the truth) | no — the writer emits the real RFQ |
| **FORWARD** | redispatch not chosen (rows already sent, or policy = FORWARD), writer `LIVE` or `NOT_STARTED` | the reader's **real** error (or synthesized `08006`) + **`RFQ('T')`** | stays alive on the writer | yes, but **only the RFQ byte** — the error body is the reader's real error |
| **TERMINATE** | policy = TERMINATE, **or** writer `LOST` | connection closed | rolled back by PostgreSQL on disconnect | no |
| **writer-loss POISON** | **writer's own** `rc == -1` (not reachable from a reader failure) | synthesized `ERROR 25P02` + `RFQ('E')` | client kept alive to `ROLLBACK` | yes — this is the one place the proxy fabricates transaction control |

---

## 4. Where it plugs into this implementation — the single seam

The whole feature attaches at one place: the session handler's `rc == -1` branch. In the full implementation this is the dispatch block at `lib/PgSQL_Session.cpp:4061-4130`. It calls two functions and then switches on the result.

```cpp
// full implementation lib/PgSQL_Session.cpp:4066-4079 (control flow only)
PolarDB_RequestOutcome polardb_out = polardb_capture_outcome(active_backend(), /*ok=*/false);
const PolarDB_FailureAction polardb_act = polardb_on_failure(polardb_out);
if      (polardb_act == PolarDB_FailureAction::RETRY)     { NEXT_IMMEDIATE(CONNECTING_SERVER); }   // redispatch on the writer
else if (polardb_act == PolarDB_FailureAction::TERMINATE) { handler_ret = -1; return handler_ret; } // tear the session down
else if (polardb_act == PolarDB_FailureAction::FORWARD)   { RequestEnd(myds, true); }               // error on the wire; txn alive
else { /* PASSTHROUGH -> fall through to the existing upstream rc==-1 handling (incl. writer-loss poison) */ }
```

Two design rules govern this seam:

1. **The decision function holds no control flow.** `polardb_on_failure(out)` classifies and applies side effects, then returns a `PolarDB_FailureAction` value. The **handler** owns every control-flow macro (`NEXT_IMMEDIATE`, `RequestEnd`, the `return`). This keeps the decision logic unit-testable and the handler the single owner of session lifetime.
2. **This dispatch sits ABOVE the upstream retry logic.** The upstream `query_retries_on_failure` retry path must not run for a handled reader failure. RETRY is **single-shot**: the writer-only route (§5) blocks any second attempt, so a handled reader failure and an upstream retry are mutually exclusive.

The dispatch's `PASSTHROUGH` value falls through to the **unchanged** upstream `rc == -1` handling, which still owns writer-loss poison and the non-PolarDB cases.

---

## 5. Reader-failure routes after a reader failure

After **one** reader failure, the transaction records an explicit reader-failure route so
the next routing decision cannot silently choose the same unsafe path again.

There are two route forms:

- `FORCE_WRITER`: set after writer-targeted retry, forward, or a retry decline.
  Later statements in the same transaction stay on the writer.
- `SKIP_READER`: set after a successful retry on another compatible reader.
  The failed reader endpoint is excluded while the transaction remains eligible
  for split reads on a different reader.

The route is set centrally in `polardb_on_failure` through
`polardb_apply_reader_failure_route_state`. The writer-only route stores the writer
hostgroup; the reader-skip route stores the failed reader hostgroup/address/port.

The route is **authoritative** — it beats query rules and the query cache. It is
honored at the routing layers below:

| Layer | Site (full implementation) | Why it is needed |
|---|---|---|
| qpo handler | `lib/PgSQL_Session.cpp:5721` | Runs first. Placed **after** the rule-driven returns but **before** the query cache, the destination override, and the hostgroup-lock — so a kept read never serves from cache and is immune to a hostgroup lock. Returns `false` (it does **not** short-circuit the rest of routing). |
| manual-mode backstop | `lib/PgSQL_Session.cpp:2723` | Covers paths that skip the PolarDB planner (manual query-rule routing). |
| planner rule | `lib/PgSQL_PolarDB_Flow.cpp` | **Required**, because the writer hostgroup is itself a PolarDB hostgroup, so the planner re-runs on the redispatched query and would otherwise overwrite the target. A writer-only route forces `target_hg = txn_writer_hg` and records action reason `READER_FAILURE_FORCE_WRITER`; a reader-skip route excludes the failed reader from split acquisition. |

The route is **cleared** on transaction end (`COMMIT` / `ROLLBACK`) and on
session reset, inside `polardb_clear_txn_xid_state`.

The action reason `READER_FAILURE_FORCE_WRITER` lives alongside the existing
LSN-policy reasons (`WRITE_LSN_UNKNOWN`, `OBSERVED_LSN_UNKNOWN`, and
`PRIMARY_LSN_UNKNOWN`) and records the writer-only-route override in traces/counters.

---

## 6. Writer state is tri-state, not a boolean

A reader failure must classify the writer **before** deciding. A boolean "is there a live writer" is not enough: it makes a **pre-write** transaction look like writer loss and wrongly poisons it. The required model is a three-valued state, resolved by `polardb_resolve_writer_state` (full implementation `lib/PgSQL_PolarDB_Failure.cpp:248`).

| State | When (writer-side evidence only) | Action |
|---|---|---|
| **LIVE** | a connected writer backend holds the open transaction (validated: `is_connected()` **and** `IsKnownActiveTransaction()`) | RETRY or FORWARD; set the writer-only route |
| **NOT_STARTED** | the client `BEGIN` is tracked but it is pre-write — **no XIDs, no persistent binding** yet | FORWARD + route to the writer hostgroup; the client's next statement opens the transaction on the writer. **Never poison** — nothing is orphaned |
| **LOST** | a writer existed (XIDs present *or* `transaction_persistent_hostgroup != -1`) but its connection died | **TERMINATE** the session |

Two correctness rules govern the resolution:

1. **Evidence rule** (full implementation `lib/PgSQL_PolarDB_Failure.cpp:257-259`): use **only** XIDs imported from a write **or** a persistent writer binding (`transaction_persistent_hostgroup != -1`). Do **not** use `active_transactions`, because a failed reader could have set that, and that would make a pre-write transaction look like writer loss.
2. **LIVE probe excludes the failed reader** (full implementation `polardb_find_live_writer`, `lib/PgSQL_PolarDB_Failure.cpp:221-245`): the probe passes the failed reader as `exclude_reader` and **validates every candidate** with `is_connected() && IsKnownActiveTransaction()`. This is needed because the underlying `FindOneActiveTransaction(true)` is unguarded and can return a dead or unknown backend. A split read always implies a prior write (XIDs present), so "split read + no live writer" is always `LOST`, never `NOT_STARTED`.

### 6.1 The LOST -> TERMINATE subtlety (the one rule that is easy to get wrong)

`polardb_on_failure` runs **only** for reader failures, so the failing stream is the **reader**. If the writer turns out to be `LOST`, the proxy **cannot** hand this to the upstream writer-loss poison path — that path keys off the **writer's own** `rc == -1`. Handing a *reusable* reader to the upstream `rc == -1` logic would make it **FORWARD the reader's error** instead of poisoning, which would **orphan the dead writer's transaction**.

So when the writer is `LOST`, the reader stage **TERMINATE**s (full implementation `lib/PgSQL_PolarDB_Failure.cpp:160-161`). This is clean: PostgreSQL rolls the dead writer back on disconnect. Writer-loss poison stays strictly the writer's-own-failure path (design reference `doc/polardb-arch/23-READ-SIDE-FAILURE-RECOVERY.md:160` and `:1092` (full implementation)).

---

## 7. Per-failure policy — three tri-state knobs

The failure **type** picks the knob; the knob picks RETRY / FORWARD / TERMINATE. **None of the three knobs can poison.** The resolver is `polardb_reader_action_for` (full implementation `lib/PgSQL_PolarDB_Failure.cpp:272`); defaults are set in `lib/PgSQL_Thread.cpp:1222-1224` (full implementation).

| Failure type | Discriminator (in code, full implementation `Failure.cpp:272-275`) | Knob | Default |
|---|---|---|---|
| replica **died** | `!fail.reusable` | `pgsql-polardb_reader_death_action` | **RETRY** (0) |
| strict **timeout** (LSN/CSN wait) | `fail.timeout` | `pgsql-polardb_reader_timeout_action` | **RETRY** (0) |
| reusable **SQL error** | else | `pgsql-polardb_reader_error_action` | **FORWARD** (1) |

Values: `0 = RETRY`, `1 = FORWARD`, `2 = TERMINATE` (enum `PolarDB_ReaderAction`, full implementation `include/PgSQL_PolarDB.h:299`). The three knobs are registered as string variables taking `retry` / `forward` / `terminate` (full implementation `lib/PgSQL_Thread.cpp:2377-2379`) and refreshed into the thread-local snapshot (full implementation `lib/PgSQL_Thread.cpp:4148-4150`).

**Rationale for the defaults.** A death or a timeout can often be re-run on the writer and succeed (the client sees no error at all), so the default is RETRY. A deterministic SQL error usually fails the same way on the writer, so re-running it would waste work; the default is therefore FORWARD. Either way the client gets the **real** SQLSTATE — never a generic `25P02` (that is writer-loss only).

---

## 8. The decision function and its helpers

`polardb_on_failure(out)` (full implementation `lib/PgSQL_PolarDB_Failure.cpp:94`) is a pure classify-and-side-effect stage. It returns a `PolarDB_FailureAction { PASSTHROUGH, RETRY, FORWARD, TERMINATE }` (enum at full implementation `include/PgSQL_PolarDB.h:290`) and runs **no** handler control flow.

### 8.1 The ordered steps inside `polardb_on_failure`

```
build the failure view   (polardb_reader_failure_view, Failure.cpp:185)
        |
        v
not a PolarDB reader op?  -> return PASSTHROUGH         (leaves the connection LIVE for upstream)
        |
        v
record the existing split / wait error stats           (timeout classification by raw error text)
        |
        v
resolve the WRITER STATE  (LIVE / NOT_STARTED / LOST)   (polardb_resolve_writer_state, Failure.cpp:248)
        |
        v
ALWAYS, before any terminal return:
   record reader-failure status   (Failure.cpp:153)
   tear down split or wait op state (Failure.cpp:154-155)
        |
        v
writer LOST          -> return polardb_terminate_reader(...)    (Failure.cpp:160-161)
        |
        v
knob == TERMINATE    -> return polardb_terminate_reader(...)    (Failure.cpp:165-166)
        |
        v
set the writer-only route (for BOTH RETRY and FORWARD)    (Failure.cpp:169)
        |
        v
RETRY eligible? (knob==RETRY && writer LIVE && !rows_sent && redispatch succeeds)
        |                                  yes -> return RETRY   (Failure.cpp:173-177)
        v  no
return polardb_forward_and_continue(...)  -> FORWARD     (Failure.cpp:180)
```

Note the **"always, before any terminal return"** step. Status accounting and state teardown run for **every** handled outcome (including `LOST -> TERMINATE`), so `polardb_terminate_reader`'s contract holds: by the time it runs, status and teardown are already done (full implementation `lib/PgSQL_PolarDB_Failure.cpp:151-155`).

### 8.2 The helper functions

| Function (full implementation) | file:line | One-line responsibility |
|---|---|---|
| `polardb_capture_outcome(on, ok)` | `Failure.cpp:44` | Read the failed backend's connection **exactly once** into a `PolarDB_RequestOutcome` (error code/message, `reusable`, whether rows started, reader hostgroup/address/port). Later stages read the snapshot, never the connection again. |
| `polardb_on_failure(out)` | `Failure.cpp:94` | The decision tree above. Returns a `PolarDB_FailureAction`. |
| `polardb_reader_failure_view(out)` | `Failure.cpp:185` | Build a `PolarDB_ReaderFailure` view: error/identity from the snapshot, plus classification (`split_read` / `wait_read`) and the retry packet captured from live session state before any teardown frees it. |
| `polardb_writer_hgid()` | `Failure.cpp:215` | The configured writer hostgroup (the `NOT_STARTED` target). Thin wrapper over the same topology lookup `polardb_collect` uses. |
| `polardb_find_live_writer(...)` | `Failure.cpp:221` | The LIVE-writer probe. Excludes the failed reader; validates every candidate's liveness. |
| `polardb_resolve_writer_state(...)` | `Failure.cpp:248` | Resolve LIVE / NOT_STARTED / LOST from writer-side evidence only. |
| `polardb_reader_decision_for(fail)` | `Failure.cpp` | Map the failure **type** to an action, retry target, and reader-failure route. |
| `polardb_apply_reader_failure_route_state(route, ...)` | `Failure.cpp` | Apply the writer-only or reader-skip route used by later routing decisions. |
| `polardb_record_reader_failure_status(fail)` | `Failure.cpp:327` | Error accounting + mark a died reader so it is destroyed, not pooled. Does **no** upstream retry. |
| `polardb_finish_reader_failure_wait_op(fail)` | `Failure.cpp:343` | Wait-op state teardown (latency, plan reset, drop staged notices). RETRY skips `RequestEnd`, so the teardown must happen here. |
| `polardb_try_redispatch_to_writer(...)` | `Failure.cpp:352` | RETRY mechanics: re-point both `mybe` and `current_hostgroup` at the writer, move the client's own query packet to the writer stream, and reset `CurrentQuery` from that packet. Preconditions are checked first with no side effects. |
| `polardb_forward_and_continue(fail)` | `Failure.cpp:391` | FORWARD mechanics: emit the real error + `RFQ('T')`, release the reader, bump the forward counter. |
| `polardb_forward_reader_error(fail, rfq)` | `Failure.cpp:311` | Emit `ErrorResponse(real error OR 08006)` + `ReadyForQuery(rfq)` onto the client output. Never synthesizes `25P02`; never routed through `generate_error_packet()` (which would hardcode `RFQ('I')`). |
| `polardb_terminate_reader(...)` | `Failure.cpp:993` | TERMINATE mechanics: free the retry packet, release (destroy) the reader, return TERMINATE. |

Split-domain teardown helpers it leans on — `polardb_release_reader_backend`,
`polardb_clear_txn_xid_state`, `polardb_finish_reader_failure_split_op` — live
in `lib/PgSQL_PolarDB_Split.cpp`.

### 8.3 How RETRY rewires the request (mechanics)

`polardb_try_redispatch_to_writer` (full implementation `Failure.cpp:352-388`) does the following, **only after** all preconditions pass (writer connected, known active transaction, `async_state_machine == ASYNC_IDLE`, no rows started, a retry packet exists):

1. release the reader backend (with reuse if the reader is reusable);
2. **re-point both** `current_hostgroup = writer_hg` **and** `mybe = writer_mybe` — re-pointing `current_hostgroup` alone would be inert, because `CONNECTING_SERVER` drives `mybe->server_myds` and never re-resolves `mybe`;
3. move the client's own query packet into the writer's data stream (`pgsql_real_query`);
4. bump the retry counter (`polardb_split_reads_retried`; wait read retry reuses or generalizes the current `polardb_wait_reads_retried_on_writer`);
5. reset `CurrentQuery` from the writer's query packet and call `set_previous_status_mode3()` so the handler resumes correctly.

The handler then runs `NEXT_IMMEDIATE(CONNECTING_SERVER)` and the writer executes the query. The writer-only route (already set by `polardb_on_failure`) prevents a second offload.

---

## 9. New state, knobs, and counters introduced (the delta)

### 9.1 New source file and types

| New item | Where (full implementation) |
|---|---|
| Translation unit `lib/PgSQL_PolarDB_Failure.cpp` (1449 lines, this branch) | the whole feature body |
| enum `PolarDB_FailureAction { PASSTHROUGH, RETRY, FORWARD, TERMINATE }` | `include/PgSQL_PolarDB.h:290` |
| enum `PolarDB_ReaderAction { RETRY=0, FORWARD=1, TERMINATE=2 }` | `include/PgSQL_PolarDB.h:299` |
| enum `PolarDB_WriterState { LIVE, NOT_STARTED, LOST }` | `include/PgSQL_PolarDB.h:311` |
| struct `PolarDB_RequestOutcome` (captured once per failed request) | `include/PgSQL_PolarDB.h:254` |
| struct `PolarDB_ReaderFailure` (the working view) | full implementation header (consumed throughout `Failure.cpp`) |
| `RouteActionReason::READER_FAILURE_FORCE_WRITER` (added alongside the existing reasons) | `include/PgSQL_PolarDB.h:721` |
| session fields `polardb_txn_reader_failure.route`, `polardb_txn_writer_hg`, `polardb_txn_skipped_reader_*` | `include/PgSQL_Session.h` |

### 9.2 New configuration knobs (three)

All three are **string** thread variables taking `retry` / `forward` / `terminate` (defaults `retry` / `retry` / `forward`, `lib/PgSQL_Thread.cpp:1284-1286`), mapping onto `RETRY=0, FORWARD=1, TERMINATE=2` in `PolarDB_ReaderAction`.

| Knob | Default | Fires when the failure is… |
|---|---|---|
| `pgsql-polardb_reader_death_action` | RETRY (0) | a replica **death** (not reusable) |
| `pgsql-polardb_reader_timeout_action` | RETRY (0) | a strict **wait timeout** (LSN/CSN) |
| `pgsql-polardb_reader_error_action` | FORWARD (1) | a reusable **SQL error** |

Registered by this implementation as `pgsql-polardb_reader_death_action`, `pgsql-polardb_reader_timeout_action`, and `pgsql-polardb_reader_error_action`. They select the common reader-failure policy for split reads and wait reads.

### 9.3 Counters

This branch exports the wait-read retry/error counters and the split reader-failure counters from `include/PgSQL_PolarDB_Counters.h`, including `PolarDB_Split_Reads_Retried`, `PolarDB_Split_Reads_Retried_On_Reader`, `PolarDB_Split_Reads_Forwarded`, and `PolarDB_Reader_Terminations`.

### 9.3.1 Remaining counter guidance

New counters should be added to the shared PolarDB counter metadata so both
`stats_pgsql_global` and Prometheus remain covered. Keep the taxonomy
non-overlapping: `split_reads_fallback` means a split read was never dispatched,
`split_reads_retried` means redispatched on the writer,
`split_reads_retried_on_reader` means redispatched on another replica,
`split_reads_forwarded` means the real reader error was forwarded, and
`reader_terminations` means the session was closed.

> **Reminder on counters in this implementation.** The current stat surface is generated from `include/PgSQL_PolarDB_Counters.h`; `polardb_active` is an internal enabled check, not an exported counter. The deferred millisecond-lag knob (`pgsql-polardb_lag_ms`) and the byte-lag stale-sample counter (`PolarDB_LSN_Stale_Count`) are unrelated to this feature; see [15-LIMITATIONS-AND-ROADMAP.md](15-LIMITATIONS-AND-ROADMAP.md).

### 9.4 Hooks in this implementation that this feature extends (already present in this implementation / upstream)

| Existing hook | What this feature does to it |
|---|---|
| the `rc == -1` handler branch | adds the `polardb_capture_outcome` + `polardb_on_failure` dispatch above the upstream retry logic |
| the `tx_poisoned` writer-loss poison (`handler_minus1_PoisonTransaction`) + recovery (`handler_poisoned_simple_query`) | **reused unchanged**, plus one protected `polardb_cleanup_txn_split_state()` call on the poison path (full implementation `lib/PgSQL_Session.cpp:3044`, inside `#if POLARDB_PROXY`) for when the writer dies while holding an open split |
| the `reusable` discriminator (`is_connection_in_reusable_state`) | reused as the death-vs-error classifier |
| the qpo handler and the PolarDB planner / `RouteActionReason` | extended with the writer-only route override and the new action reason |

---

## 10. CSN note (scope boundary)

The reader-failure model handles **both** LSN-wait and CSN-wait timeouts in its timeout classification (it matches `"LSN wait timeout"` / `"polar_wait_lsn timeout"` and `"CSN wait timeout"` / `"polar_wait_csn timeout"` in the raw backend error text — full implementation `lib/PgSQL_PolarDB_Failure.cpp:64-67` and `:117-143`). However:

- **CSN itself is a separate future feature, and it is incomplete and experimental.** CSN (Commit Sequence Number, a global per-commit counter) requires PolarDB backend support, applies only in global-consistency mode, and its wait behavior is **not reliably verified**. Several CSN paths in the full implementation are explicit no-ops today. See [18-FUTURE-CSN-DESIGN.md](18-FUTURE-CSN-DESIGN.md) and mark CSN experimental wherever it appears.
- This document's reader-failure logic does not depend on CSN being complete. The CSN-timeout branch is simply already wired so that, once CSN lands, a CSN-wait timeout on a replica is recovered the same way an LSN-wait timeout is.

So: the reader-failure model is **LSN-first** in practice (the only wait type this implementation produces), with CSN-timeout handling **pre-wired but inert** until the experimental CSN feature is finished.

---

## 11. Concrete delta from this implementation (side-by-side)

| Active in this branch | Remaining advanced roadmap |
|---|---|
| Simple-query split reads can be offloaded after primary RFQ XID evidence is complete | Advanced split ranking and CSN/global consistency remain outside this feature |
| `rc == -1` has a common reader-failure hook for wait reads and split reads | Future policy tables can refine the current action knobs by failure class and retry budget |
| RETRY/FORWARD/TERMINATE are implemented for the active wait/split paths | Future reader circuit-breakers can feed failure history back into reader selection |
| `RouteActionReason` includes reader-failure and split rejection reasons | Future routing can add richer reasons only when they are visible in traces/counters |
| `tx_poisoned` writer-loss poison + recovery exist as the shared base: the poison-set helper `handler_minus1_PoisonTransaction` (this implementation `lib/PgSQL_Session.cpp:2838`, called at `:2940` and `:3012`), the recovery helper `handler_poisoned_simple_query` (this implementation `:480`), the shared wire-write helper `write_tx_poisoned_error` (this implementation `:130`), and the reset clear (this implementation `:399`) | **reused unchanged**, plus one protected `polardb_cleanup_txn_split_state()` on the poison path |
| Reader-failure knobs and counters are active | Future additions should keep the shared counter metadata and trace taxonomy non-overlapping |

**Scope caveat for manually-routed extended-protocol reads.** The "no orphaned writer" guarantee holds only for reads routed by the **PolarDB pipeline** on the simple-query protocol (the `'Q'` message). A read manually routed to a replica by a query rule on the **extended protocol** inside a transaction **bypasses** the pipeline; the failure of its first statement is a documented follow-up, not covered by this model (design reference `doc/polardb-arch/23-READ-SIDE-FAILURE-RECOVERY.md:359` (full implementation)).

---

## 12. Worked traces

Each trace lists the backend involved, the outcome chosen, and the bytes the
client receives. These describe the active behavior for autocommit wait reads
and simple-query transaction-split reads in this branch.

### Trace A — wait read times out, writer LIVE, no rows sent -> RETRY (transparent success)

```
1. client (in txn) issues a read; pipeline offloads it to a replica with a wait wrapper
2. replica wait times out strictly -> backend ErrorResponse "LSN wait timeout"; reusable=true; rc==-1
3. polardb_capture_outcome: reusable=true, result_started=false, err="LSN wait timeout" -> is_timeout=true
4. polardb_on_failure:
      writer state = LIVE  (a connected writer holds the txn)
      knob = reader_timeout_action = RETRY (default)
      set writer-only route
      RETRY eligible (LIVE && !rows_sent && redispatch ok) -> redispatch the client's own query on the writer
5. handler: NEXT_IMMEDIATE(CONNECTING_SERVER); writer runs the query -> rows + RFQ('T')
RESULT: client sees the real data and RFQ('T'); the failure was transparent. The wait read retry counter moves.
```

### Trace B — split read returns a SQL error, rows already streamed -> FORWARD (txn alive)

```
1. client (in txn, splittable) issues a read offloaded to a replica as a split read
2. replica streams some rows, then errors (e.g. 42P01 undefined_table); reusable=true; rc==-1
3. polardb_capture_outcome: reusable=true, result_started=true, real SQLSTATE captured
4. polardb_on_failure:
      writer state = LIVE
      knob = reader_error_action = FORWARD (default)  (and rows already sent -> RETRY ineligible anyway)
      set writer-only route
      FORWARD -> emit the REAL ErrorResponse(42P01) + RFQ('T'); release the reader
RESULT: client sees the real 42P01 error and RFQ('T'); the txn is still live on the writer.
        The next statement routes to the writer. polardb_split_reads_forwarded++.
```

### Trace C — reader dies, writer LOST -> TERMINATE (clean rollback)

```
1. client (in txn, after a write -> XIDs present) has a split read on a replica
2. the replica connection dies; simultaneously the writer connection is also gone; rc==-1 on the reader
3. polardb_capture_outcome: reusable=false (death), no result
4. polardb_on_failure:
      writer state = LOST  (XIDs present => writer existed; probe finds no live writer)
      status + teardown run first
      writer LOST -> terminate_reader  (must NOT hand a reusable reader to upstream poison)
5. handler: handler_ret = -1; return  -> session torn down
RESULT: PostgreSQL rolls back the dead writer's txn on disconnect. No orphan. No 25P02 fabricated here.
```

### Trace D — writer's OWN connection dies mid-transaction -> writer-loss POISON (not a reader failure)

```
1. the WRITER connection dies while holding an open txn; the writer's own query gets rc==-1
2. polardb_on_failure is for READER failures: this is the writer, so the failure view has
   neither split_read nor wait_read -> returns PASSTHROUGH
3. control falls through to the existing upstream rc==-1 handling
4. tx_poisoned path: handler_minus1_PoisonTransaction synthesizes ERROR 25P02 + RFQ('E');
   one protected polardb_cleanup_txn_split_state() also runs on the PolarDB path
RESULT: the client gets 25P02 + RFQ('E') and stays alive to ROLLBACK. This is the ONLY fabricated
        transaction control, and it is reached only by the writer's own death.
```

---

## 13. Notes for reviewers (subtleties and gotchas)

- **The proxy never fabricates transaction control on a live transaction.** RETRY and FORWARD always tell the truth: RETRY lets the writer emit the real RFQ; FORWARD sends the reader's real error with `RFQ('T')` because the transaction really is alive. Only writer-loss poison fabricates (`25P02` + `RFQ('E')`), and only on the writer's own death.
- **Tri-state writer resolution is the important rule.** The single most important correctness rule is the LIVE/NOT_STARTED/LOST distinction (§6) using writer-side evidence only, never `active_transactions`. A boolean live-writer check cannot distinguish pre-write transactions from writer loss.
- **LOST -> TERMINATE, never poison from the reader stage** (§6.1). Handing a reusable reader to the upstream poison path would FORWARD the reader's error and orphan the dead writer.
- **The route must beat the cache.** The qpo-handler override is placed deliberately before the query-cache / destination-override / hostgroup-lock, so a kept read never serves from cache (full implementation `Session.cpp:5721`).
- **RETRY is single-shot.** The route (set before redispatch) prevents a second offload, and the dispatch sits above the upstream retry logic, so a handled reader failure and an upstream retry are mutually exclusive.
- **Capture once.** `polardb_capture_outcome` reads the failed connection exactly once into a snapshot; later stages never re-read a connection that may already be released. The reader address is **copied** while the connection is live for later error accounting (full implementation `Failure.cpp:58`, `:331-333`).
- **Status + teardown run before any terminal return** so every outcome — including `LOST -> TERMINATE` — has consistent accounting and clean state (full implementation `Failure.cpp:151-155`).
- **The manually-routed extended-protocol carve-out is a known hole** (§11): an extended-protocol read manually routed to a replica inside a transaction bypasses the pipeline and is not covered.

---

## 14. Status summary

| Item | Status |
|---|---|
| Reader-failure recovery (RETRY / FORWARD / TERMINATE) | **Active** for autocommit wait reads and simple-query transaction-split reads. |
| Writer-loss poison (`tx_poisoned`) | **Present** as the shared base; reused unchanged. A reader failure never reaches it. |
| Transaction split prerequisite | **Satisfied for simple-query split reads.** Extended-protocol split and CSN/global consistency remain outside this document's active scope. |
| CSN-timeout handling inside this model | Pre-wired but **inert / experimental** until the CSN feature ([18-FUTURE-CSN-DESIGN.md](18-FUTURE-CSN-DESIGN.md)) is finished. CSN is incomplete and experimental: it needs PolarDB backend support, applies only in global-consistency mode, and its wait behavior is not reliably verified. |
| Manually-routed extended-protocol carve-out | Documented open follow-up; not covered by the no-orphan guarantee. |

For the full out-of-scope list and the roadmap for remaining advanced policy
work, see [15-LIMITATIONS-AND-ROADMAP.md](15-LIMITATIONS-AND-ROADMAP.md).

---

## Appendix: Mermaid diagrams

### A.1 The three honest outcomes + reserved poison

```mermaid
flowchart TD
    F["reader (offloaded) read FAILS (rc == -1)"] --> C["classify the failure"]
    C --> W["resolve WRITER STATE<br/>LIVE / NOT_STARTED / LOST"]
    W --> RETRY["RETRY<br/>redispatch on the WRITER<br/>(no rows sent yet)"]
    W --> FWD["FORWARD<br/>real error + RFQ('T')<br/>keep txn ALIVE"]
    W --> TERM["TERMINATE<br/>tear session down<br/>(opt-in, or writer LOST)"]
    POISON["writer-loss POISON<br/>25P02 + RFQ('E')"]:::reserved
    WD["WRITER's OWN rc == -1"] --> POISON
    RETRY --> RR["writer runs it:<br/>success -> data + RFQ('T')<br/>error -> native 'E' forwarded"]
    FWD --> FR["client sees failed read,<br/>stays in live txn,<br/>decides next"]
    TERM --> TR["PostgreSQL rolls back<br/>the writer on disconnect"]
    classDef reserved fill:#fee,stroke:#900;
```

### A.2 The single seam in the handler (`rc == -1` dispatch)

```mermaid
flowchart TD
    H["handler rc == -1 branch<br/>(full implementation Session.cpp:4061-4130)"] --> CAP["polardb_capture_outcome(active_backend, ok=false)"]
    CAP --> ON["polardb_on_failure(out)"]
    ON --> SW{"PolarDB_FailureAction"}
    SW -->|RETRY| NI["NEXT_IMMEDIATE(CONNECTING_SERVER)"]
    SW -->|TERMINATE| TT["handler_ret = -1; return"]
    SW -->|FORWARD| RE["RequestEnd(myds, true)"]
    SW -->|PASSTHROUGH| UP["fall through to upstream rc==-1<br/>(incl. writer-loss poison)"]
```

### A.3 Decision flow inside `polardb_on_failure`

```mermaid
flowchart TD
    V["build failure view<br/>(reader_failure_view)"] --> NP{"PolarDB reader op?<br/>(split_read or wait_read)"}
    NP -->|no| PT["return PASSTHROUGH<br/>(connection stays LIVE)"]
    NP -->|yes| ST["record split/wait error stats"]
    ST --> WS["resolve WRITER STATE<br/>(resolve_writer_state)"]
    WS --> ACC["ALWAYS: record status + teardown split/wait op<br/>(before any terminal return)"]
    ACC --> L{"writer == LOST?"}
    L -->|yes| T1["terminate_reader -> TERMINATE"]
    L -->|no| K{"knob == TERMINATE?"}
    K -->|yes| T2["terminate_reader -> TERMINATE"]
    K -->|no| ROUTE["set writer-only route<br/>(for BOTH RETRY and FORWARD)"]
    ROUTE --> RE{"knob==RETRY && writer LIVE<br/>&& no rows sent<br/>&& redispatch ok?"}
    RE -->|yes| RR["return RETRY"]
    RE -->|no| FF["forward_and_continue -> FORWARD"]
```

### A.4 Writer state tri-state

```mermaid
flowchart TD
    P["reader failed -> classify writer"] --> LP["LIVE probe<br/>(excludes failed reader,<br/>validates each candidate)"]
    LP -->|connected + known active txn| LIVE["LIVE<br/>RETRY or FORWARD + writer-only route"]
    LP -->|none live| EV{"writer-side evidence?<br/>(XIDs OR persistent binding)"}
    EV -->|yes, writer existed| LOST["LOST<br/>TERMINATE"]
    EV -->|no, but client BEGIN tracked| NS["NOT_STARTED<br/>FORWARD + route to writer_hg<br/>never poison"]
    EV -->|no evidence, no BEGIN| LOST2["LOST (stop safely)<br/>TERMINATE"]
```

### A.5 The writer-only route honored at three layers

```mermaid
flowchart TD
    SET["route set in polardb_on_failure<br/>(apply_reader_failure_route_state)"] --> Q["qpo handler<br/>before cache / override / lock"]
    SET --> M["manual-mode backstop<br/>(full implementation Session.cpp:2723)"]
    SET --> PL["planner rule<br/>writer-only route sets target_hg = txn_writer_hg<br/>reader-skip route excludes failed reader"]
    Q --> CLR["cleared on COMMIT/ROLLBACK/reset<br/>(clear_txn_xid_state)"]
    M --> CLR
    PL --> CLR
```

---

*This document describes the active reader-failure recovery shape in this
branch. Remaining work is advanced policy refinement, not the base retry /
forward / terminate model.*

Verified against this branch.
