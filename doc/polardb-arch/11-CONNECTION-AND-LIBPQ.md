# 11 — Connection and libpq Integration

> Scope: how the backend-connection layer (`PgSQL_Connection`) carries the PolarDB LSN feature — startup profiles, proxy protocol parameters, the `is_polardb_enabled` enable flag (fresh and pooled), the ReadyForQuery (RFQ) LSN accessor, the notice receiver, the `dispatch_state` session-to-connection bridge, and the `WrapState` filter. | Audience: R/M/O/C | Status: stable | Prereqs: [10-SESSION-INTEGRATION.md](10-SESSION-INTEGRATION.md), [07-QUERY-WRAPPING.md](07-QUERY-WRAPPING.md), [08-WAIT-TIMEOUT-AND-NOTICES.md](08-WAIT-TIMEOUT-AND-NOTICES.md), [09-PUBLISH-AND-WRITE-TRACKING.md](09-PUBLISH-AND-WRITE-TRACKING.md), [02-BUILD-TOGGLE-AND-LIBPQ.md](02-BUILD-TOGGLE-AND-LIBPQ.md) | Verified against: this branch

---

## 1. Scope and status

This document covers the parts of the PolarDB read-your-writes (RYW) feature that live on the **backend-connection** layer, the class `PgSQL_Connection`. A `PgSQL_Connection` is one TCP connection from ProxySQL to a PostgreSQL/PolarDB backend server, wrapped around a libpq `PGconn`.

The connection layer does four PolarDB jobs:

1. **Request PolarDB RFQ payloads on the connection.** When the backend belongs to a PolarDB hostgroup, ProxySQL resolves a startup profile from per-HG/global `proxy_protocol`, emits the matching PolarDB proxy startup parameters, and records the profile on the connection.
2. **Read the LSN back with no extra query.** A pure accessor, `get_polardb_lsn()`, returns the LSN that libpq parsed from the most recent ReadyForQuery (RFQ) message.
3. **Filter the wrapped-read result.** A consistency read is sent as several `SET` statements glued in front of the user query. The connection layer silently drops the leading `SET` result sets and forwards only the user's result. It is the **sole owner** of that filtering.
4. **Capture the wait-timeout notice.** The libpq notice receiver always forwards backend notices to the PolarDB handler, so a best-effort wait-timeout WARNING is counted and re-queued even when the generic result object (`query_result`) has already been recycled and set to null.

All of this code is controlled behind the compile flag `POLARDB_PROXY`. With `POLARDB_PROXY=0` every block below compiles out and the connection layer behaves exactly like upstream ProxySQL (see [02-BUILD-TOGGLE-AND-LIBPQ.md](02-BUILD-TOGGLE-AND-LIBPQ.md)).

**Status:** stable. Everything in this document is implemented and active in this branch.

### 1.1 Terms used in this document (defined once)

| Term | Plain meaning |
|------|---------------|
| **LSN** (Log Sequence Number) | A 64-bit number marking a position in PostgreSQL's write-ahead log (WAL). Bigger means newer. A replica that has replayed up to LSN X can serve any read whose data was written at or before X. |
| **RYW** (read-your-writes) | The guarantee that after a session writes, its own later reads see that write, even when the read goes to a replica. |
| **RFQ** (ReadyForQuery) | The PostgreSQL wire message a backend sends after each command to say "ready for the next query". A patched PolarDB backend appends its current LSN to this message. |
| **conninfo** | The libpq connection string (a space-separated list of `key=value` options) used to open a backend connection. |
| **startup parameter** | An option sent in the PostgreSQL startup packet at connect time. This implementation emits either v15 names (`_polar_proxy_client_host`, `_polar_proxy_client_port`, `_polar_proxy_send_lsn`) or legacy names (`_polar_origin_client_ip`, `_polar_origin_client_port`, `_polar_send_lsn`). |
| **startup profile** | Connection-local record of the protocol and `REQUEST_RFQ_*` bits ProxySQL requested. It is requested capability, not RFQ confirmation. |
| **GUC** | A PostgreSQL server setting changed with `SET name = value`. |
| **wrapped read** | A replica-eligible read that ProxySQL rewrites by prepending three `SET` statements (mode, timeout, wait-LSN) so the replica blocks until it has replayed past the session's last write LSN. |
| **wrapper SET** | One of those three prepended `SET` statements. Each returns its own result set the connection layer must drop. |
| **HG** (hostgroup) | A numbered group of backend servers in ProxySQL. A PolarDB pair has a writer HG (the primary) and a reader HG (replicas). |
| **pooled connection** | A backend connection reused from ProxySQL's connection pool. It does not run the connect path again. |

---

## 2. Overview — where the connection layer sits in the pipeline

The PolarDB feature has four integration hooks across the request/response cycle. The connection layer owns parts of **Hook 1** (connect/enable) and all of **Hook 3** (the wire-level wrap-state filter and the notice receiver). It also supplies the RFQ LSN accessor that **Hook 4** (`polardb_process_result`) reads. The session layer (see [10-SESSION-INTEGRATION.md](10-SESSION-INTEGRATION.md)) owns Hook 2 (routing) and drives Hooks 1 and 4.

```
                  PgSQL_Session                         PgSQL_Connection (this doc)
                  -------------                         --------------------------
client read  ─►  route pipeline (Hook 2)
                 polardb_execute: prepare wait,
                 snapshot user query
                       │
                 ASYNC_IDLE:
                 finalize_wait_timeout_injection
                 builds "SET;SET;SET;<query>"  ───────► dispatch_state snapshot
                 sets polardb_dispatch_wrapper_*        (ASYNC_IDLE, Connection.cpp:2357)
                                                              │
                                                        query_start():
                                                        begin(n) consumer  (2027)
                                                              │
                                                        backend runs 3 SETs + query
                                                              │
                                                        WIRE filter (Hook 3):
                                                        drop 3 SET results,        (775)
                                                        forward only user result
                                                              │
                                                        notice_handler_cb (Hook 3):
                                                        always call               (3365)
                                                        polardb_handle_notice
                       ◄──────────────────────────── user result + pending notice
                 RequestEnd success (Hook 4):
                 polardb_process_result reads ───────────────► get_polardb_lsn()  (1897)
                 the RFQ LSN                             (PQhasLSN / PQgetLSN)
```

Two things connect at the conninfo step (Hook 1): the conninfo may gain profile-driven PolarDB proxy startup parameters, and the session is marked PolarDB-enabled so the result-processing stage will run later. A successful startup only shows the server accepted the parameters; RFQ LSN is confirmed later when result RFQs actually carry LSN.

---

## 3. Functions and members — signature, responsibility, file:line

This section lists every PolarDB item the connection layer adds, in the order a request meets them. All declarations are in `include/PgSQL_Connection.h`; all definitions are in `lib/PgSQL_Connection.cpp`.

### 3.1 Public methods

| Member | Declared | Defined | Responsibility |
|--------|----------|---------|----------------|
| `void polardb_init_connection_tracking()` | `PgSQL_Connection.h` | `PgSQL_Connection.cpp` | After a successful connect, turn on libpq LSN parsing only when the recorded startup profile requested `REQUEST_RFQ_LSN`. No-op without a live connection or without that request bit. |
| `uint64_t get_polardb_lsn()` | `PgSQL_Connection.h:935` | `PgSQL_Connection.cpp:1897` | Return the LSN libpq cached from the last RFQ. Pure accessor; issues no SQL query. Returns 0 if the RFQ carried no LSN or there is no connection. |

### 3.2 Private method

| Member | Declared | Defined | Responsibility |
|--------|----------|---------|----------------|
| `PolarDB_StartupProfile build_polardb_startup_profile(unsigned int hid) const` | `PgSQL_Connection.h` | `PgSQL_Connection.cpp` | Resolve the effective startup protocol from per-HG `proxy_protocol` over global `pgsql-polardb_proxy_protocol`. |
| `bool append_polardb_startup_params(std::ostringstream& conninfo, PolarDB_StartupProfile& profile, unsigned int hid)` | `PgSQL_Connection.h` | `PgSQL_Connection.cpp` | Append profile-driven PolarDB startup parameters. Fails connection creation early when an RFQ-requesting profile has no usable identity. |
| `PolarDB_StartupIdentity resolve_polardb_startup_identity(...) const` | `PgSQL_Connection.h` | `PgSQL_Connection.cpp` | Choose client endpoint, listener/proxy endpoint, or configured fallback identity for startup parameters. Returns `NONE` when no usable identity exists. |

### 3.3 The libpq notice receiver

| Member | Declared | Defined | Responsibility |
|--------|----------|---------|----------------|
| `static void notice_handler_cb(void* arg, const PGresult* result)` | `PgSQL_Connection.h:985` | `PgSQL_Connection.cpp:3339` | libpq notice receiver. Registered with `PQsetNoticeReceiver` in `query_start()` (`PgSQL_Connection.cpp:2044`). Records the notice in the active result if there is one, and **always** falls through to `polardb_handle_notice()`. |

### 3.4 File-local helpers (in `lib/PgSQL_Connection.cpp`)

These two are file-static, not class members. They live only in the `.cpp`.

| Helper | Defined | Responsibility |
|--------|---------|----------------|
| `static bool polardb_is_lsn_wait_timeout_result(const PGresult* result)` | `PgSQL_Connection.cpp:166` | Return true when the result's structured detail field `PG_DIAG_MESSAGE_DETAIL` equals the marker `POLARDB_LSN_WAIT_TIMEOUT_DETAIL`. This is how a strict-mode wait timeout is identified without matching human-readable text. |
| `static void polardb_account_wrapper_set_error(PgSQL_Connection* conn, const PGresult* result, const char* msg)` | `PgSQL_Connection.cpp:182` | When a wrapper SET (or the user query after wrapper consumption) errors, decide whether it is a PolarDB wait timeout and, if so, account it; then mark the wrap-state state as failed so consumption stops. |

`polardb_handle_notice()` itself is **declared** in this file (`PgSQL_Connection.cpp:38`) but **defined** in `lib/PgSQL_PolarDB_Notices.cpp:225` (see [08-WAIT-TIMEOUT-AND-NOTICES.md](08-WAIT-TIMEOUT-AND-NOTICES.md)).

### 3.5 Persistent connection-local state

| Member | Type | Declared | Purpose |
|--------|------|----------|---------|
| `dispatch_state` | `PolarDB_Query_DispatchState { uint32_t wrapper_stmts; PolarDB_Query_WrapperKind wrapper_kind; }` | `PgSQL_Connection.h:701` (struct `:691`) | Per-dispatch handoff: how many wrapper SET results to skip and which wrapper kind. Snapshotted from the session at dispatch, consumed by `query_start()`. |
| `polardb_query_wrap_state` | `PolarDB_Query_WrapState` | `PgSQL_Connection.h:840` (struct `:778`) | Per-query countdown of wrapper result sets. Drops the leading SETs and forwards only the user result. `was_wrapped`/`stmt_failed` are sticky (survive consumption). |
| `polardb_startup_profile` | `PolarDB_StartupProfile` | `PgSQL_Connection.h` | Protocol and RFQ request bits requested when this backend connection was created. |

Both are **connection-confined**: a `PgSQL_Connection` is driven by exactly one worker thread at a time on the hot path, so there is no locking on either field. See [10-SESSION-INTEGRATION.md](10-SESSION-INTEGRATION.md) for the matching session-side fields, and `POLARDB_STRUCTURES.md` for the full field inventory.

---

## 4. Hook 1 — enabling PolarDB on the connection

There are three distinct enable sites. The first two are on the connection layer (conninfo build and post-connect). The third is on the session layer (pooled/fresh attach) and is summarized here because a pooled connection never runs the connection-layer connect path.

### 4.1 conninfo: resolve startup profile and mark the session enabled (fresh connect)

When `connect_start()` builds the conninfo for a **client-backed** session connection, it checks whether the backend's hostgroup is a PolarDB hostgroup. If so, it builds a startup profile, appends the profile-driven PolarDB startup parameters, records that profile on the connection, and sets the session enable flag.

The check and the two actions are at `PgSQL_Connection.cpp:1491-1595`:

| Step | What it does | file:line |
|------|--------------|-----------|
| `is_polar = parent && PgHGM->is_polardb_hostgroup(parent->myhgc->hid)` | Ask the HostGroups Manager if this HG is a PolarDB HG. | `PgSQL_Connection.cpp:1492` |
| `append_polardb_startup_params(conninfo, profile, ...)` | Append profile-driven v15 or legacy startup params, or none for `off`. | `PgSQL_Connection.cpp` |
| `myds->sess->polardb_config.is_polardb_enabled = true` | Mark the session PolarDB-enabled so the response-path result-processing stage runs. | `PgSQL_Connection.cpp:1595` |

There is a second conninfo branch for **non-client** connections (monitor / internal) to a PolarDB hostgroup. It uses the same profile builder. If the profile requests RFQ LSN, a configured fallback identity is normally required because there is no real client endpoint. It does **not** set any session enable flag, because there is no client session yet:

```
PgSQL_Connection.cpp:1633-1637
  if (!(myds && myds->sess && myds->sess->client_myds) &&
      parent && PgHGM->is_polardb_hostgroup(parent->myhgc->hid)) {
      append_polardb_startup_params(conninfo, polardb_startup_profile, parent->myhgc->hid);
  }
```

### 4.2 `append_polardb_startup_params` — what it actually emits

The effective protocol comes from per-HG `pgsql_replication_hostgroups.proxy_protocol` when it is not `default`, otherwise from global `pgsql-polardb_proxy_protocol`.

| Protocol | Emitted parameters |
|---|---|
| `v15` | `_polar_proxy_client_host=<host>`, `_polar_proxy_client_port=<port>`, `_polar_proxy_send_lsn=true`, `_polar_proxy_send_xact=true` |
| `legacy` | `_polar_origin_client_ip=<host>`, `_polar_origin_client_port=<port>`, `_polar_send_lsn=true`, `_polar_send_xact=true` |
| `off` | no PolarDB proxy startup parameters |

Notes:

- These must be **top-level** connection parameters, not inside the libpq `options` block, so PolarDB's `ProcessStartupPacket()` reads them. The conninfo `options` block is closed (the trailing `'`) at `PgSQL_Connection.cpp:1578`, before these parameters are appended at `:1580+`.
- The function is called only for PolarDB hostgroups. A vanilla PostgreSQL backend would reject an unknown startup option, so checking the call keeps non-PolarDB connections safe (`PgSQL_Connection.cpp:1584`).
- The identity is selected from the client endpoint, then listener/proxy endpoint, then `pgsql-polardb_proxy_identity_host` plus `pgsql-polardb_proxy_identity_port`.
- RFQ-requesting profiles require valid identity. Empty, invalid, wildcard, or unset identity fails connection creation before `PQconnectStart()`. The configured fallback is validated on `SET`: host must be empty or a non-wildcard IP literal, and a completed fallback pair requires port `1..65535`.
- The request bit names are `REQUEST_RFQ_LSN`, `REQUEST_RFQ_CSN`, and `REQUEST_RFQ_XID`. Current `v15` and `legacy` profiles request `REQUEST_RFQ_LSN` and `REQUEST_RFQ_XID`; `off` requests none. XID is requested at startup for capability, while `txn_split_enabled` controls whether result processing uses the XID evidence for split routing.

### 4.3 `polardb_init_connection_tracking` — turn on requested libpq RFQ parsing

After a successful connect (the `ASYNC_CONNECT_SUCCESSFUL` branch of `connect_cont`), the connection calls `polardb_init_connection_tracking()`:

```
PgSQL_Connection.cpp:655-658
  #if POLARDB_PROXY
      // Turn on requested PolarDB RFQ parsing on this backend ...
      polardb_init_connection_tracking();
  #endif
```

The function itself checks the live connection and then enables each parser whose
startup bit was requested:

| Step | Behavior | file:line |
|------|----------|-----------|
| check: live connection | Return early if `pgsql_conn` is null or `PQstatus(...) != CONNECTION_OK`. | `PgSQL_Connection.cpp:1883-1884` |
| LSN action | If `polardb_startup_profile.has_rfq_lsn()`, call `PQsetPolarSendLSN(pgsql_conn, 1)` so `getReadyForQuery()` parses the appended LSN when present. | `PgSQL_Connection.cpp` |
| XID action | If `polardb_startup_profile.has_rfq_xid()`, call `PQsetPolarSendXact(pgsql_conn, 1)` so libpq parses transaction-split RFQ metadata when present. | `PgSQL_Connection.cpp` |

Citation precision: `PQsetPolarSendLSN` and `PQsetPolarSendXact` are **defined** in the libpq patch at `polardb_libpq.patch` (function definitions; see [02-BUILD-TOGGLE-AND-LIBPQ.md](02-BUILD-TOGGLE-AND-LIBPQ.md)). The call sites in this tree are inside `polardb_init_connection_tracking()`.

This is still not capability confirmation. It only tells patched libpq to parse RFQ payloads if the backend later sends them. Missing RFQ LSN is handled in result processing and routing policy; missing transaction-split RFQ metadata simply leaves the observed split state primary-only.

### 4.4 Pooled vs fresh enable (session layer, summarized)

A pooled backend connection is attached to a session **without** running `connect_start()`, so the conninfo enable at §4.1 never runs for it. The session layer therefore sets `is_polardb_enabled` itself when it gets or creates a backend for a PolarDB hostgroup. Both sites first check the flag is not already set and that the HG is a PolarDB HG:

| Path | Action | file:line |
|------|--------|-----------|
| Pooled (got a pooled connection) | `polardb_config.is_polardb_enabled = true` | `PgSQL_Session.cpp:6302` |
| Fresh (no pooled connection; will create new) | `polardb_config.is_polardb_enabled = true` | `PgSQL_Session.cpp:6316` |

Net effect: whichever way the session obtains a backend for a PolarDB HG, `is_polardb_enabled` is true by the time the request runs, so the result-processing stage will execute. The flag is **never reset to false** during the session — it only ever flips on. Full detail is in [10-SESSION-INTEGRATION.md](10-SESSION-INTEGRATION.md).

---

## 5. Hook 3 — the wrap-state filter and the notice receiver

This is the connection layer's largest PolarDB responsibility. A consistency read arrives at the backend as one simple-query packet containing three wrapper SETs followed by the user query (built by `finalize_wait_timeout_injection`, see [07-QUERY-WRAPPING.md](07-QUERY-WRAPPING.md)). The backend returns one result set per statement, in order:

```
[ SET ok ][ SET ok ][ SET ok ][ user SELECT result ]
  drop      drop      drop       forward to client
```

The connection layer must drop the first three and forward only the fourth. It is the **sole owner** of this filtering; the session and client only ever see the user result.

### 5.1 The dispatch_state bridge (session → connection, same thread)

The count of SETs to drop travels from the session to the connection in two hops, both on the same worker thread, so no locking is needed.

**Hop 1 — snapshot at dispatch (`ASYNC_IDLE`).** When the query is submitted, the connection snapshots the session's handoff fields into its own `dispatch_state` and zeroes the session fields so they cannot be reused:

```
PgSQL_Connection.cpp:2357-2363  (inside the ASYNC_IDLE case of async_query)
  dispatch_state.reset();
  if (!extended_query_info && myds && myds->sess &&
      myds->sess->polardb_query.dispatch_wrapper_stmts > 0) {
      dispatch_state.wrapper_stmts = myds->sess->polardb_query.dispatch_wrapper_stmts;
      dispatch_state.wrapper_kind  = myds->sess->polardb_query.dispatch_wrapper_kind;
      myds->sess->polardb_query.dispatch_wrapper_stmts = 0;
      myds->sess->polardb_query.dispatch_wrapper_kind  = PolarDB_Query_WrapperKind::NONE;
  }
```

Two design points:

- Only **simple queries** are wrapped. The `!extended_query_info` check at `PgSQL_Connection.cpp:2358` skips the snapshot for extended-protocol (Parse/Bind/Execute) queries, which are never wrapped.
- The session fields are zeroed here so a later query on the same session cannot accidentally re-skip results.

**Hop 2 — consume at `query_start()`.** When the query starts, the connection turns the snapshot into the per-query countdown and then resets the snapshot:

```
PgSQL_Connection.cpp:2025-2041
  polardb_query_wrap_state.clear();
  if (dispatch_state.wrapper_stmts > 0) {
      polardb_query_wrap_state.begin(dispatch_state.wrapper_stmts,
                                         dispatch_state.wrapper_kind);
      ...
  }
  ...
  dispatch_state.reset();   // Consumed — prevent stale reuse
```

`begin(n, kind)` (defined inline in `PgSQL_Connection.h:807`) sets `was_wrapped = (n>0)`, `stmt_total = n`, `stmt_pending = n`, and the wrapper kind. For an ordinary, non-wrapped query, `wrapper_stmts` is 0, so `begin()` is never called and `polardb_query_wrap_state` stays empty — the result path then skips the filter entirely.

`dispatch_state` is also reset on connection cleanup (`PgSQL_Connection.cpp:3730`), and `polardb_query_wrap_state` is cleared on the same cleanup (`PgSQL_Connection.cpp:3731`), so a connection returned to the pool carries no stale wrapper state.

### 5.2 The leading-skip consume loop (the wire filter)

The actual filtering runs in the result loop of `PgSQL_Connection::handler()`, in the `ASYNC_USE_RESULT_CONT` state. For each backend result, while there are still wrapper results pending and we are at the simple-query end state:

```
PgSQL_Connection.cpp:775-819
  if (polardb_query_wrap_state.has_pending() &&
      fetch_result_end_st == ASYNC_QUERY_END) {
      if (exec_status_type == PGRES_COMMAND_OK ||
          exec_status_type == PGRES_EMPTY_QUERY) {
          polardb_query_wrap_state.stmt_pending--;          // one SET done
          // recycle this result's buffer, null query_result, fetch next
          if (query_result) {
              if (query_result_reuse) delete query_result_reuse;
              query_result_reuse = query_result;
              query_result = nullptr;
          }
          NEXT_IMMEDIATE(ASYNC_USE_RESULT_START);                // next result
      } else if (exec_status_type == PGRES_FATAL_ERROR ||
                 exec_status_type == PGRES_NONFATAL_ERROR ||
                 exec_status_type == PGRES_BAD_RESPONSE) {
          // a wrapper SET errored (e.g. strict-mode wait timeout as ERROR)
          polardb_account_wrapper_set_error(this, result.get(),
                                            PQresultErrorMessage(result.get()));
          // fall through: let the error flow to the client
      }
  }
```

Two branches:

| Result status | Meaning | Connection action | file:line |
|---------------|---------|-------------------|-----------|
| `PGRES_COMMAND_OK` or `PGRES_EMPTY_QUERY` | A wrapper SET finished cleanly. | Decrement `stmt_pending`, recycle the result buffer into `query_result_reuse`, null `query_result`, and immediately fetch the next result. The dropped SET result never reaches the client. | `PgSQL_Connection.cpp:777-798` |
| `PGRES_FATAL_ERROR`, `PGRES_NONFATAL_ERROR`, or `PGRES_BAD_RESPONSE` | A wrapper SET itself errored (for example, a strict-mode wait timeout surfaces as an ERROR). | Call `polardb_account_wrapper_set_error()` (accounts a confirmed timeout, marks the connection WrapState failed), then **stop** consuming so the error flows to the client through the normal path below. | `PgSQL_Connection.cpp:799-818` |

The condition `has_pending()` (`stmt_pending > 0`, defined in `PgSQL_Connection.h:787`) plus the `fetch_result_end_st == ASYNC_QUERY_END` check at `PgSQL_Connection.cpp:776` mean the filter only runs while wrapper SETs are still pending and only for simple queries (the only thing that can be wrapped). After `stmt_pending` reaches 0, the next result — the user query — falls through to the normal result-forwarding code and reaches the client.

### 5.3 The other two error-account sites

A wrapper SET error can also surface outside the consume loop, on paths where libpq reports the connection as already in error. Both call the same helper so a confirmed timeout is still accounted:

| Site | When | file:line |
|------|------|-----------|
| `ASYNC_USE_RESULT_START`, error present before fetching | The fetch start sees an error already set; account it before ending the result. | `PgSQL_Connection.cpp:734` |
| `ASYNC_USE_RESULT_CONT`, FATAL / bad-response default branch | A FATAL/non-fatal/bad result after the error is set from the result; account it. | `PgSQL_Connection.cpp:930` |

There is also a trace-only block in the `ASYNC_QUERY_START`/`ASYNC_QUERY_CONT` early-error paths (`PgSQL_Connection.cpp:692-697` and `:712-719`); those only log (under `POLARDB_DEBUG`) and do not account anything.

### 5.4 `polardb_account_wrapper_set_error` — the accounting decision

Defined at `PgSQL_Connection.cpp:182`. It is the single place that decides whether a wrapper-path error is a PolarDB wait timeout to charge. Its logic, step by step:

| Step | Behavior | file:line |
|------|----------|-----------|
| check: was wrapped | If there is no connection or the connection's result was not wrapped (`!was_wrapped`), do nothing. | `PgSQL_Connection.cpp:186-194` |
| check: wait active | If there is no session or the session's wait is not active (`!polardb_wait_active()`), just mark the connection WrapState failed and return (no accounting). | `PgSQL_Connection.cpp:214-231` |
| Classify | `is_lsn_timeout = polardb_is_lsn_wait_timeout_result(result)` — check the structured marker. | `PgSQL_Connection.cpp:205-206` |
| Classify for retry | If `is_lsn_timeout`, set the query wait state's timeout flag. The session failure path uses this flag to decide whether the original read can be retried on the writer. | `PgSQL_Connection.cpp:246-248` |
| Account | If `is_lsn_timeout`, call `sess->polardb_account_wait_timeout("result-error")`. | `PgSQL_Connection.cpp:249-251` |
| Mark failed | If still consuming a wrapper SET, mark the connection WrapState failed (stops consumption). | `PgSQL_Connection.cpp:253-256` |

The comment at `PgSQL_Connection.cpp:241-245` explains the "wait active" condition: in strict mode the timeout can surface after all wrapper SET results were already consumed (the wait was started by `SET polar_xact_split_wait_lsn`, then the backend raises ERROR before running the user SELECT). Charging only the structured-marker event while the wait is active prevents an ordinary user-query error — whose text might coincidentally look like a timeout — from being miscounted. The timeout flag is separate from accounting because accounting is idempotent and may consume the latency timer before the session's `rc == -1` branch runs. The counters touched by `polardb_account_wait_timeout()` and by retry are documented in [08-WAIT-TIMEOUT-AND-NOTICES.md](08-WAIT-TIMEOUT-AND-NOTICES.md) and [12-THREADVARS-AND-OBSERVABILITY.md](12-THREADVARS-AND-OBSERVABILITY.md).

### 5.5 The structured timeout marker (no text matching)

ProxySQL does **not** match human-readable WARNING/ERROR text, because user SQL could fabricate it. It checks the structured PostgreSQL diagnostic field `PG_DIAG_MESSAGE_DETAIL` against a fixed constant:

```
include/PgSQL_PolarDB.h:186-187
  static constexpr const char* POLARDB_LSN_WAIT_TIMEOUT_DETAIL =
      "polar_proxy_lsn_wait_timeout";
```

The connection-side check is `polardb_is_lsn_wait_timeout_result()`:

```
PgSQL_Connection.cpp:166-169
  const char* detail = result ? PQresultErrorField(result, PG_DIAG_MESSAGE_DETAIL) : nullptr;
  return detail && strcmp(detail, POLARDB_LSN_WAIT_TIMEOUT_DETAIL) == 0;
```

The PolarDB backend emits this exact detail (via `errdetail_internal()`) only from its proxy LSN-wait path, so the marker is trustworthy. The same marker is checked on the notice path inside `polardb_handle_notice()` (`lib/PgSQL_PolarDB_Notices.cpp`). See [08-WAIT-TIMEOUT-AND-NOTICES.md](08-WAIT-TIMEOUT-AND-NOTICES.md) for both paths together.

### 5.6 The notice receiver — inline `add_notice` vs `pending_notices`

A best-effort wait timeout arrives as a backend WARNING/NOTICE **while the wrapped SELECT obtains its snapshot**. By that point the leading SET results may already have been consumed: the connection recycled their result buffers and set its generic `query_result` pointer to null (see §5.2). So `query_result` can be null when the notice arrives. The capture path must still work in that case.

libpq's notice receiver is `notice_handler_cb()`, registered per query in `query_start()` (`PgSQL_Connection.cpp:2044`). Its body, at `PgSQL_Connection.cpp:3339`:

```
PgSQL_Connection.cpp:3344-3366
  if (conn->query_result != nullptr) {
      // Generic upstream path: store the notice inline in the active result so
      // its NoticeResponse is forwarded with that result and its bytes accounted.
      const unsigned int bytes_recv = conn->query_result->add_notice(result);
      conn->update_bytes_recv(bytes_recv);
  } else {
      // No active query_result (e.g. a wrapped SET was just consumed, or during
      // RESET SESSION). MUST NOT return — fall through so the PolarDB handler
      // can still owe timeout accounting and a separately-forwarded NoticeResponse.
      proxy_debug(...);   // debug log only
  }

#if POLARDB_PROXY
  // Runs regardless of query_result so wrapped-wait timeout notices are never dropped.
  polardb_handle_notice(conn, result);
#endif
```

Two paths, and why there is no client duplicate:

| Situation | What `notice_handler_cb` does | Where the NoticeResponse goes to the client |
|-----------|------------------------------|---------------------------------------------|
| There **is** an active `query_result` (a normal notice during a result) | `query_result->add_notice(result)` stores the notice **inline** in that result object. Then it still calls `polardb_handle_notice`. | The notice is forwarded as part of that result. `polardb_handle_notice` ignores it unless it is a PolarDB wait-timeout marker during an active wait (see below), so it does not also queue it. **No duplicate.** |
| There is **no** active `query_result` (the wrapped-wait case: SETs consumed, `query_result` set to null) | The inline branch is skipped. `polardb_handle_notice` runs and, only for a marker notice during an active wait, builds a fresh NoticeResponse packet and enqueues it on the session's `pending_notices`. | The session flushes `pending_notices` once, ahead of the user result. The inline path was skipped, so there is **no duplicate**. |

The key is the `if/else` on `query_result`: at most one of "store inline" and "queue in `pending_notices`" stores the notice for the client. `polardb_handle_notice()` adds to `pending_notices` only when (a) the notice carries the timeout marker and (b) the wait is active and the connection WrapState is a consistency wait (`is_consistency_wait()`); otherwise it leaves the notice to the generic path (`lib/PgSQL_PolarDB_Notices.cpp:235-273`). So a normal notice that already went inline is never re-queued, and a marker notice that arrived with no `query_result` is captured exactly once. The session-side queue (`pending_notices`, `enqueue_pending_notice`, `clear_pending_notices`) and the forward-once flush are covered in [08-WAIT-TIMEOUT-AND-NOTICES.md](08-WAIT-TIMEOUT-AND-NOTICES.md) and [10-SESSION-INTEGRATION.md](10-SESSION-INTEGRATION.md).

`add_notice` itself is a method of `PgSQL_Query_Result` (declared `include/PgSQL_Protocol.h:517`); it returns the byte count so the connection can update its received-bytes statistic.

---

## 6. Hook 4 (read side) — `get_polardb_lsn`, the RFQ-only accessor

The result-processing stage (Hook 4) needs the backend's WAL LSN after a successful query. It reads it through `get_polardb_lsn()`, which is **RFQ-only**: it returns the LSN libpq already parsed from the most recent ReadyForQuery message and issues **no** SQL query.

```
PgSQL_Connection.cpp:1897-1908
  uint64_t PgSQL_Connection::get_polardb_lsn() {
      if (!pgsql_conn || PQstatus(pgsql_conn) != CONNECTION_OK) {
          return 0;
      }
      if (PQhasLSN(pgsql_conn)) {           // did the last RFQ carry an LSN?
          return PQgetLSN(pgsql_conn);      // 64-bit LSN
      }
      return 0;                             // RFQ carried no LSN
  }
```

| Property | Detail | file:line |
|----------|--------|-----------|
| No round-trip | Calls only the native libpq accessors `PQhasLSN()` / `PQgetLSN()`. Safe on the hot request/result-processing path. | `PgSQL_Connection.cpp:1903-1904` |
| Returns 0 when absent | No connection, connection not OK, or the RFQ carried no LSN -> returns 0. | `PgSQL_Connection.cpp:1898-1900`, `:1907` |
| Enabled by | The per-connection LSN parsing must have been turned on by `PQsetPolarSendLSN(conn,1)` in `polardb_init_connection_tracking()` (`PgSQL_Connection.cpp:1887`). Without it, `PQhasLSN()` is always false. | — |

The caller is `polardb_process_result()` (defined in `lib/PgSQL_PolarDB_Flow.cpp`), reached from `PgSQL_Session::RequestEnd()` on the success path. Everything the result-processing stage does with the returned LSN — advancing session write/observed LSN state, maintaining missing-LSN flags, and refreshing the per-server cache — is documented in [09-PUBLISH-AND-WRITE-TRACKING.md](09-PUBLISH-AND-WRITE-TRACKING.md). The deliberate "RFQ-only, never SQL" rule keeps all SQL-based LSN probing in the monitor; see [05-MONITOR-AND-HGM-LSN-STATE.md](05-MONITOR-AND-HGM-LSN-STATE.md).

> **The LSN a client reads is not always the raw backend LSN.** `get_polardb_lsn()` above reads what the *backend* reported on ProxySQL's connection to it. The LSN ProxySQL then forwards to an LSN-aware *client* — the value that client's patched libpq exposes through `PQhasLSN()` / `PQgetLSN()` — is chosen separately by `PgSQL_Session::polardb_client_ready_lsn()`. It may be **raised** above the backend value to a writer-confirmed session target or a successful wait target, so the client's LSN stream never moves backwards when consecutive queries use different backend connections. That behavior and the `PolarDB_Client_RFQ_LSN_Raised_{To_Target,By_Writer,By_Wait}` counters are documented in [09-PUBLISH-AND-WRITE-TRACKING.md §6](09-PUBLISH-AND-WRITE-TRACKING.md).

---

## 7. The libpq patch surface this layer depends on

The connection layer depends on the PolarDB-specific libpq patch: active LSN functions, xact RFQ functions, and accepted startup parameters. None exist in stock libpq; they are all added by the patch `deps/postgresql/polardb_libpq.patch`, which is applied only when `POLARDB_PROXY=1` (see [02-BUILD-TOGGLE-AND-LIBPQ.md](02-BUILD-TOGGLE-AND-LIBPQ.md) for the full patch review). This connection layer consumes LSN functions for consistency and xact RFQ functions for transaction-split observation.

| libpq item | Kind | Used by connection layer at | What it does |
|------------|------|------------------------------|--------------|
| `PQsetPolarSendLSN(PGconn*, int enable)` | function | `PgSQL_Connection.cpp:1887` | Turn on the runtime flag that makes libpq's RFQ parser look for an appended LSN. |
| `PQhasLSN(const PGconn*)` | function | `PgSQL_Connection.cpp:1903` | Was an LSN present in the last RFQ? |
| `PQgetLSN(const PGconn*)` | function | `PgSQL_Connection.cpp:1904` | Return the LSN captured from the last RFQ (0 if none). |
| `PQgetXactSplitXids` / `PQisXactSplittable` / `PQisXactWalPending` / `PQsetPolarSendXact` | functions | connection/result processing, planner inputs, split-read dispatch | Request and read transaction-split RFQ evidence; the planner may name a split route, and execution can take a compatible replica connection from the pool for that read. |
| `_polar_send_lsn` / `_polar_proxy_send_lsn` | startup parameter | emitted according to the resolved startup profile | Ask the backend to append its WAL LSN to every RFQ. |
| `_polar_send_xact` / `_polar_proxy_send_xact` | startup parameter | emitted for non-`off` PolarDB profiles | Ask the backend to append transaction-split metadata to RFQ; result processing observes it only when `txn_split_enabled=1`, and the planner can recognize split-readable state. |

### 7.1 Accepted startup parameters and emitted subset

The patch accepts more parameters than this tree emits. This implementation emits the LSN, XID, and client-identity parameters for the selected proxy protocol, and leaves cancel-routing / SSL metadata unused.

| Capability accepted by the patch | Emitted by this tree? | Note |
|----------------------------------|-----------------------|------|
| `_polar_send_lsn` / `_polar_origin_client_ip` / `_polar_origin_client_port` (legacy names) | **yes, when effective protocol is `legacy`** | Legacy startup dialect. |
| `_polar_proxy_send_lsn` / `_polar_proxy_client_host` / `_polar_proxy_client_port` (v15 names) | **yes, when effective protocol is `v15`** | Default startup dialect. |
| `_polar_send_xact` / `_polar_proxy_send_xact` | **yes, when effective protocol is `legacy` or `v15`** | Request transaction-split RFQ evidence at startup; `txn_split_enabled` decides whether result processing observes it for split routing. |
| `_polar_proxy_session_id` / `_polar_proxy_cancel_key` | no | Cancel-routing metadata; not used here. |
| `_polar_proxy_use_ssl` / `_polar_proxy_ssl_version` / `_polar_proxy_ssl_cipher_name` | no | SSL passthrough metadata; not used here. |

The patch registers the legacy names, the v15 names, xact RFQ names, cancel metadata, SSL metadata, the three LSN functions (`PQgetLSN`, `PQhasLSN`, `PQsetPolarSendLSN`), and the four xact functions.

**Not present at all in this tree's patch: CSN.** The commit-sequence-number (CSN) parameter `_polar_send_csn` and the functions `PQgetCSN` / `PQhasCSN` are a **future** feature and are **not** in the final-tree patch. The final-tree patch `deps/postgresql/polardb_libpq.patch` contains no CSN block; that block exists only in the full implementation's copy of the patch (full implementation). CSN is also experimental: it needs PolarDB backend support, it applies only in global-consistency mode, and its wait behavior is not reliably verified. See §9 and [18-FUTURE-CSN-DESIGN.md](18-FUTURE-CSN-DESIGN.md).

This implementation has no session-side mirror fields for `_polar_proxy_session_id` /
`_polar_proxy_cancel_key`. Those accepted-but-unused params are reserved for a
future PolarDB15 cancel-session extension.

---

## 8. Notes for reviewers (subtleties and gotchas)

- **The connection layer is the sole owner of `WrapState` filtering.** The session and client never see the dropped SET results. If a future change moved this filtering elsewhere, the count must stay consistent with the count copied through `dispatch_state` (`PgSQL_Connection.cpp:2357-2363`).
- **The filter is bounded by `stmt_pending` and the simple-query end state.** The check `has_pending() && fetch_result_end_st == ASYNC_QUERY_END` (`PgSQL_Connection.cpp:775-776`) means a stray extra result after `stmt_pending` reaches 0 is **not** dropped — it would be forwarded to the client. The header comment (`PgSQL_Connection.h:734-736`) documents this: a `SET` appended *after* the user query is not supported because there is no trailing-skip counter. The wrap point only ever prepends, so this is safe today.
- **`get_polardb_lsn()` is intentionally side-effect-free.** It never issues SQL. This is what makes it safe to call on the result-processing hot path. Any future LSN probe that needs SQL must go in the monitor, not here.
- **Enable is one-way and multi-sourced.** `is_polardb_enabled` is set in three places (conninfo `:1595`, pooled `:6302`, fresh `:6316`) and never cleared. A pooled connection cannot bypass the result-processing stage, because the session enable is set on attach even though the connection-layer connect path did not run.
- **The notice `if/else` is essential to correctness.** The inline `add_notice` and the `pending_notices` queue are mutually exclusive for a given notice, which is why the client never sees a duplicate NoticeResponse. Removing the `else`/fall-through, or returning early when `query_result` is null, would drop wrapped-wait timeout notices (`PgSQL_Connection.cpp:3344-3366`).
- **Structured marker, not text.** Timeout classification uses `PG_DIAG_MESSAGE_DETAIL == "polar_proxy_lsn_wait_timeout"` (`PgSQL_Connection.cpp:166-169`, marker `include/PgSQL_PolarDB.h:186-187`), so user SQL cannot fake a timeout. This is a correctness property, not a convenience.
- **Both wrap-state and dispatch state are cleared on cleanup.** `PgSQL_Connection.cpp:3730-3731` resets `dispatch_state` and clears `polardb_query_wrap_state` so a connection returning to the pool carries no stale wrapper state into the next session.

---

## 9. Status and deferred items

### 9.1 Implemented in this branch

| Capability | Status |
|------------|--------|
| Profile-driven startup params (`v15`, `legacy`, `off`) | implemented |
| Connection-local `polardb_startup_profile` with `REQUEST_RFQ_LSN` controlling | implemented |
| Session enable on fresh connect, pooled attach, and fresh attach | implemented (`:1595`, `PgSQL_Session.cpp:6302`, `:6316`) |
| `polardb_init_connection_tracking` / `PQsetPolarSendLSN` | implemented (`PgSQL_Connection.cpp:1882`, `:1887`) |
| `get_polardb_lsn` RFQ-only accessor | implemented (`PgSQL_Connection.cpp:1897`) |
| Wrap-state filter (drop N SETs, forward user result) | implemented (`PgSQL_Connection.cpp:775-819`) |
| Wrapper-error accounting via structured marker | implemented (`PgSQL_Connection.cpp:182`, `:250`) |
| Notice receiver fall-through to `polardb_handle_notice` | implemented (`PgSQL_Connection.cpp:3365`) |
| `dispatch_state` session→connection bridge | implemented (`PgSQL_Connection.cpp:2357-2363`, `:2025-2041`) |

### 9.2 Deferred / not present in this branch

These belong to future features (see [15-LIMITATIONS-AND-ROADMAP.md](15-LIMITATIONS-AND-ROADMAP.md) and the future-design docs):

- **CSN over the connection layer.** `_polar_send_csn`, `PQgetCSN`/`PQhasCSN`, and `get_polardb_csn()` are **not** present here. They are also **not** in this tree's libpq patch: the final-tree patch `deps/postgresql/polardb_libpq.patch` has no CSN block; the CSN libpq block lives only in the full implementation's patch (full implementation). CSN is a future and experimental feature: it requires PolarDB backend support, it applies only in global-consistency mode, and its wait behavior is not reliably verified. See [18-FUTURE-CSN-DESIGN.md](18-FUTURE-CSN-DESIGN.md). (CSN is experimental throughout — treat any CSN reference as future/unverified.)
- **Cancel and SSL metadata parameters.** Accepted by the patch but not emitted here (§7.1).
- **No session-side cancel mirror fields.** `_polar_proxy_session_id` and
  `_polar_proxy_cancel_key` are accepted by the patch but ProxySQL does not emit
  them and keeps no session-side state for them in this implementation.
- **Trailing-skip support.** The wrap-state filter has no counter for a SET *after* the user query (`PgSQL_Connection.h:734-736`); not needed today because the wrap only prepends.

---

## Appendix: Mermaid diagrams

### A. Hook 1 — conninfo enable and post-connect LSN parsing

```mermaid
flowchart TD
  A[connect_start builds conninfo] --> B{parent HG is PolarDB?<br/>PgHGM->is_polardb_hostgroup<br/>Connection.cpp:1492}
  B -- no --> C[plain conninfo, no PolarDB params]
  B -- yes --> D[build startup profile<br/>per-HG proxy_protocol > global]
  D --> E{effective protocol}
  E -- v15 --> F["emit _polar_proxy_client_host / _port<br/>+ _polar_proxy_send_lsn=true"]
  E -- legacy --> G["emit _polar_origin_client_ip / _port<br/>+ _polar_send_lsn=true"]
  E -- off --> X["emit no PolarDB proxy params"]
  F --> SE[set session is_polardb_enabled=true<br/>Connection.cpp:1595]
  G --> SE
  X --> SE
  C --> PQ[PQconnectStart]
  SE --> PQ
  PQ --> I{connect OK?}
  I -- yes --> J[polardb_init_connection_tracking<br/>Connection.cpp:658]
  J --> K{live conn AND<br/>REQUEST_RFQ_LSN requested?}
  K -- yes --> L[PQsetPolarSendLSN conn,1<br/>Connection.cpp:1887]
  K -- no --> M[no-op]
```

### B. Hook 3 — dispatch_state bridge and the wrap-state filter

```mermaid
flowchart TD
  S[session finalize_wait_timeout_injection<br/>sets polardb_query.dispatch_wrapper_stmts=3] --> A{ASYNC_IDLE, simple query?<br/>Connection.cpp:2358}
  A -- yes --> B[snapshot into dispatch_state<br/>zero session fields<br/>Connection.cpp:2360-2362]
  A -- no/extended --> Z[no snapshot]
  B --> C[query_start: begin n,kind<br/>Connection.cpp:2027]
  C --> D[backend runs 3 SETs + user query]
  D --> E{result while has_pending AND<br/>simple-query end? Connection.cpp:775}
  E -- COMMAND_OK / EMPTY_QUERY --> F[stmt_pending--, recycle buffer,<br/>fetch next Connection.cpp:777-798]
  F --> E
  E -- error status --> G[polardb_account_wrapper_set_error<br/>stop consuming Connection.cpp:799-818]
  E -- pending==0 --> H[forward user result to client]
  G --> H
```

### C. Hook 3 — notice receiver (inline vs pending_notices, no duplicate)

```mermaid
flowchart TD
  N[backend NOTICE/WARNING] --> CB[notice_handler_cb<br/>Connection.cpp:3339]
  CB --> Q{query_result != null?<br/>Connection.cpp:3344}
  Q -- yes --> I[query_result->add_notice<br/>store inline Connection.cpp:3347]
  Q -- no --> D[debug log only, do NOT return<br/>Connection.cpp:3353]
  I --> P[polardb_handle_notice<br/>Connection.cpp:3365]
  D --> P
  P --> M{marker AND wait active AND<br/>is_consistency_wait?<br/>Notices.cpp:235-273}
  M -- yes, captured with no query_result --> Z[enqueue on pending_notices<br/>forwarded once before result]
  M -- no --> L[leave to generic path]
```

### D. Hook 4 (read side) — RFQ-only LSN accessor

```mermaid
flowchart TD
  A[RequestEnd success path] --> B[polardb_process_result<br/>]
  B --> C[get_polardb_lsn<br/>Connection.cpp:1897]
  C --> D{conn OK?}
  D -- no --> Z[return 0]
  D -- yes --> E{PQhasLSN?<br/>Connection.cpp:1903}
  E -- yes --> F[return PQgetLSN<br/>Connection.cpp:1904]
  E -- no --> Z
```

---

Verified against this branch.
