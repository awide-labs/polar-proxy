/**
 * @file PgSQL_PolarDB.h
 * @brief PolarDB-specific support for ProxySQL (LSN session-consistency subset)
 *
 * This header defines the structures and free functions for the PolarDB
 * read-your-writes (RYW) consistency feature, built around the WAL log sequence
 * number (LSN) that PolarDB reports in the extended ReadyForQuery message. This
 * message is abbreviated "RFQ" throughout this file and the .cpp files:
 *   - health check via polar_node_type() / polar_is_available()
 *   - per-session write-LSN tracking for read-your-writes consistency
 *   - a prepended SET block (consistency mode, wait timeout, and the
 *     `SET polar_xact_split_wait_lsn` wait condition) on replica-eligible autocommit
 *     reads
 *
 * Build switch: everything PolarDB-specific is compiled only under
 * POLARDB_PROXY (see Makefiles). With POLARDB_PROXY=0 the feature compiles to
 * no-op stubs in PgSQL_PolarDB_Stubs.cpp, leaving ProxySQL behavior unchanged.
 *
 * This header contains the complete LSN session-consistency surface: monitor
 * health parsing, per-server LSN cache helpers, query wait state, routing
 * context/plan types, wait wrapping, notice capture and forwarding, and
 * response-side LSN result processing. A replica read is allowed only when its
 * required wait prefix can be attached before dispatch.
 */

#ifndef __CLASS_PGSQL_POLARDB_H
#define __CLASS_PGSQL_POLARDB_H

#include <array>
#include <limits>

class PgSQL_Connection;
class PgSQL_Backend;
class PgSQL_Data_Stream;
class PgSQL_SrvC;
class PgSQL_Session;

// ===========================================================================
// PolarDB tracing facility  (debugging map)
// ===========================================================================
//
// Two layers of diagnostics, both routed through POLARDB_TRACE() (defined below)
// and so compiled in only under POLARDB_DEBUG. Always-on operational events
// (connect/enable, per-server LSN cache, errors) stay on plain proxy_info()/
// proxy_error() and appear in every build.
//
// 1) RYW request-flow traces -- the LSN routing logic:
//      "PolarDB CONNINFO ..."   connect_start: is_polardb_hostgroup, _polar_send_lsn,
//                               session enable (fresh + pooled backend attach)
//      "PolarDB COLLECT ..."    route context (hg / writer / reader / mode / eligible / ...)
//      "PolarDB PLAN ..."       routing decision, one line per branch
//                               (PASSTHROUGH / FORCE_PRIMARY / REPLICA_WITH_WAIT /
//                                mode=PRIMARY|OFF / in_transaction / multi-statement /
//                                extended / no-write-no-wait / lag-over-cap)
//      "PolarDB EXECUTE ..."    side effects (final target hg, wait prepared)
//      "PolarDB WRAP ..."       SET polar_xact_split_wait_lsn wrap + wrapper_stmts
//      "PolarDB PROCESS_RESULT ..."  RFQ payloads -> session LSNs / cache
//      "PolarDB LSN CACHE ..."       per-server LSN cache ENTER/EXIT
//      "PolarDB WAIT ..."       wait-timeout NOTICE capture/forward
//
// 2) POLARDB_DEBUG numbered state-machine/request traces -- OPT-IN (off by default). Build a
//    full-trace binary with `make polardb-debug` (== POLARDB_PROXY=1 POLARDB_DEBUG=1),
//    or compile with -DPOLARDB_DEBUG=1. Each tag carries a per-call counter for
//    correlating one request through the dispatch state machine:
//      [H...]  PgSQL_Connection::handler()       ENTRY + handler_again STATE on every
//                                                async-state transition (full state flow)
//      [RQ...] PgSQL_Session::RunQuery()         ENTRY + query ptr/len + async_query call/return
//      [SH...] PgSQL_Session::handler()          ENTRY
//      [S...]  session RC branch                 rc disposition after RunQuery
//      [W...]  PgSQL_Result_to_PgSQL_wire()      ENTRY (wait_active / stage / type / write_lsn)
//    End-to-end order for a wrapped LSN read:
//      [H] connect -> [RQ] query -> PLAN -> WRAP -> [H] SET-skip -> [W] wire result -> [S] rc
//    The per-call counter/local setup stays under #if POLARDB_PROXY && POLARDB_DEBUG
//    (controlling that trace-only state); the log call itself uses POLARDB_TRACE.
//
// POLARDB_DEBUG defaults OFF in release builds.
#ifndef POLARDB_DEBUG
#define POLARDB_DEBUG 0
#endif

// POLARDB_PROFILE defaults OFF in release builds. Enable it for benchmark builds
// that need per-phase latency counters in the query path.
#ifndef POLARDB_PROFILE
#define POLARDB_PROFILE 0
#endif

// Compile switch for PolarDB code paths that are designed but not yet active.
// Off by default. It keeps this not-yet-finished code visible in the source
// without letting it affect runtime behavior.
#ifndef POLARDB_PROXY_TODO
#define POLARDB_PROXY_TODO 0
#endif

// Unified PolarDB diagnostic facility. Expands to proxy_info() in a POLARDB_DEBUG
// build and to nothing otherwise (its args are discarded, so it costs nothing in
// a release build). `make polardb-debug` (POLARDB_PROXY=1 POLARDB_DEBUG=1, or
// -DPOLARDB_DEBUG=1) yields an optimized binary that carries the full
// request-flow + state-machine tracing below.
#if POLARDB_PROXY && POLARDB_DEBUG
#define POLARDB_TRACE(fmt, ...) proxy_info(fmt, ##__VA_ARGS__)
#else
#define POLARDB_TRACE(fmt, ...) do {} while (0)
#endif

#if POLARDB_PROXY

#include <atomic>
#include <cassert>
#include <arpa/inet.h>
#include <cctype>
#include <cstdint>
#include <cstdio>   // sscanf in parse_lsn
#include <cstdlib>  // std::getenv in polardb_debug_read_fault_file
#include <cstring>  // strcasecmp in parse_node_type / parse_is_available
#include <memory>
#include <netinet/in.h>
#include <algorithm>
#include <string>
#include <string_view>
#include <vector>
#include <sys/socket.h>
#include <utility>

// PolarDB type suffix conventions.
//
// Keep new structs aligned with these meanings:
//   *Ctx    : immutable collected input for one stage; never mutable runtime state.
//   *Plan   : planner/helper output describing how to perform an action.
//   *Spec   : immutable payload format; holds no mutable runtime state and makes
//             no routing decision.
//   *Scope  : identity/validity boundary for topology-epoch-bound state.
//   *State  : mutable runtime workspace reset at a query/operation boundary.
//   *Result : one-shot operation outcome; not retained as mutable state.
//   *Type   : protocol/domain category with external semantic meaning.
//   *Kind   : internal variant discriminator for a local state machine.
//
// The PolarDB_Query_ infix marks query-stage owners such as RouteCtx, RoutePlan,
// WaitPlan, WaitState, and QueryState. Shared leaf value objects such as
// WriterScope and WaitSpec omit the Query_ infix even when a query-stage owner
// embeds them.
//
// Durable session truth may be named by domain instead of using *State
// (for example PolarDB_SessionConsistency). Do not use *Ctx for stored decisions
// or mutable runtime. Keep the detailed structure notes in the PolarDB
// architecture documents under doc/polardb-arch/.

static inline const char* polardb_skip_sql_space(const char* query) {
    while (query && *query && std::isspace((unsigned char)*query)) {
        query++;
    }
    return query;
}

static inline bool polardb_starts_with_sql_keyword(
        const char* query, const char* keyword) {
    query = polardb_skip_sql_space(query);
    if (!query || !keyword || !*keyword) {
        return false;
    }
    const size_t len = strlen(keyword);
    if (strncasecmp(query, keyword, len) != 0) {
        return false;
    }
    const unsigned char next = (unsigned char)query[len];
    return next == '\0' || std::isspace(next) || next == ';';
}

/**
 * @brief Classify the LSN payload carried by a backend ReadyForQuery message.
 *
 * The split between MISSING and ZERO is load-bearing. MISSING means the backend
 * appended no LSN payload at all, so nothing can be concluded about the session
 * write position. ZERO means a payload was present but the reported position is
 * 0, which only counts as "no wait target needed" for the read-only and
 * session-state statements listed in
 * polardb_zero_lsn_payload_can_skip_wait_target(). For DML/DDL a ZERO payload
 * must still be treated as an unknown write: set the session sticky flag rather
 * than clear the wait target.
 *
 * POSITIONED means the payload carries a real WAL position that can be used
 * directly as a session or wait target.
 */
enum class PolarDB_RfqLsnPayloadState : uint8_t {
    MISSING = 0,
    ZERO = 1,
    POSITIONED = 2
};

static inline PolarDB_RfqLsnPayloadState polardb_rfq_lsn_payload_state(
        bool payload_present, uint64_t lsn) {
    if (!payload_present) {
        return PolarDB_RfqLsnPayloadState::MISSING;
    }
    return lsn == 0 ? PolarDB_RfqLsnPayloadState::ZERO
                    : PolarDB_RfqLsnPayloadState::POSITIONED;
}

static inline bool polardb_zero_lsn_payload_can_skip_wait_target(
        const char* query) {
    // A PolarDB RFQ LSN payload can be present with value 0 before the backend has
    // a useful session WAL position. That is valid for read-only and session-state
    // statements, but not for DML/DDL where the session must retain an unknown
    // write position if no usable write LSN was reported. SELECT/SHOW/EXPLAIN are intentionally not in
    // this list: the caller has already established that ordinary reads are
    // safe, and locking SELECT statements must be treated as writes.
    static const char* const safe_zero_keywords[] = {
        "SET", "RESET", "DISCARD",
        "BEGIN", "START", "COMMIT", "END", "ROLLBACK",
        "SAVEPOINT", "RELEASE", "DEALLOCATE", "CLOSE"
    };
    for (const char* keyword : safe_zero_keywords) {
        if (polardb_starts_with_sql_keyword(query, keyword)) {
            return true;
        }
    }
    return false;
}

// Stable PolarDB15 errdetail_internal() marker emitted by the backend for
// proxy LSN wait timeouts. ProxySQL uses this structured field instead of
// matching human-readable WARNING/ERROR text.
static constexpr const char* POLARDB_LSN_WAIT_TIMEOUT_DETAIL =
    "polar_proxy_lsn_wait_timeout";
static constexpr unsigned int POLARDB_REPLICA_FAILURE_ERROR_CODE = 9999;

// =============================================================================
// PolarDB DEBUG fault-injection catalog (test hooks; release builds compile out)
// =============================================================================
//
// Everything below is enabled by `#if POLARDB_PROXY && POLARDB_DEBUG` and exists
// only to let the TAP suite drive deterministic failures. None of it compiles
// into a non-DEBUG (release) build. This block is the single reference for "how
// to drive PolarDB fault injection" — env var name, mechanism, and accepted
// values for every fault hook.
//
// Two mechanisms are used:
//
//   (A) Atomic environment one-shots — read an environment variable once and fire
//       exactly once per process via std::atomic<bool> CAS. Set BEFORE ProxySQL
//       starts. These do NOT use the file helper.
//
//         POLARDB_DEBUG_FAIL_WRAP_FINALIZE_ONCE = "1"
//             Force one wrapper-build finalize failure; the query must stop instead
//             of sending an unwrapped replica read.
//             (PgSQL_PolarDB_Wrap.cpp: polardb_debug_fail_wrap_finalize_once)
//
//         POLARDB_DEBUG_WAIT_RETRY_FALLBACK_UNKNOWN_ONCE = "1"
//         POLARDB_DEBUG_WAIT_RETRY_RESULT_STARTED_ONCE   = "1"
//         POLARDB_DEBUG_WAIT_RETRY_WRITER_BUSY_ONCE      = "1"
//             Force one wait-retry fault of the named kind (ENV path of
//             polardb_debug_consume_wait_retry_fault in
//             PgSQL_PolarDB_Failure.cpp).
//
//   (B) File-based fault files — reusable without restart. Write a value into
//       the file path named by the env var; the fault fires once and the file is
//       cleared (truncated) so it can trigger again when the test rewrites it. The
//       read mechanics (read first line, strip trailing CR/LF) are shared via
//       polardb_debug_read_fault_file() below, and the truncation via
//       polardb_debug_clear_fault_file(); each call site owns its own
//       match/parse of the consumed line. The clear is match-limited (the call
//       site truncates only after it confirms a hit), so a fault file that several
//       readers share — e.g. the wait-retry and reader-acquire files, which are
//       probed once per candidate fault name — survives a non-matching probe and
//       still fires for the matching one.
//
//         POLARDB_DEBUG_WRAP_SET_ERROR_FILE
//             Line == "1" -> next wrapped read emits an invalid wrapper SET.
//             (PgSQL_PolarDB_Wrap.cpp: polardb_debug_fail_wait_set_once)
//
//         POLARDB_DEBUG_WAIT_RETRY_FAULT_FILE
//             Line == one of: "fallback_unknown" | "result_started" |
//             "writer_busy" -> force that wait-retry fault (FILE path of
//             polardb_debug_consume_wait_retry_fault in
//             PgSQL_PolarDB_Failure.cpp).
//
//         POLARDB_DEBUG_SPLIT_FAILURE_FAULT_FILE
//             Line == one of: "death" | "sql_error" | "result_started" |
//             "no_retry_packet" | "writer_busy" | "writer_lost" |
//             "writer_not_started" -> force that transaction-split
//             reader-failure branch. "death_twice" injects two reader deaths
//             in one statement to verify the reader-retry limit falls back to
//             the writer instead of oscillating between replicas.
//             (PgSQL_PolarDB_Failure.cpp:
//              polardb_debug_consume_split_failure_fault)
//
//         POLARDB_DEBUG_READER_ACQUIRE_FAULT_FILE
//             Line == "reader_busy" | "reader_lsn_unknown" -> force that reader
//             acquisition status once. "reader_busy_until_deadline" keeps
//             returning reader_busy until the test clears the file, allowing
//             deterministic coverage of the connection-deadline action.
//             "reset_timeout" forces the timeout branch after a compatible
//             reader connection enters PostgreSQL session-state reset.
//             "writer_changed_after_reader_acquire" changes the captured writer
//             epoch after a split retry acquires a replacement reader, proving
//             the final reader-to-writer mapping check rejects dispatch.
//             (PgSQL_PolarDB_ReaderPool.cpp:
//             polardb_debug_reader_acquire_fault; PgSQL_Session.cpp:
//             polardb_debug_reset_timeout; PgSQL_PolarDB_Failure.cpp:
//             polardb_debug_writer_changed_after_reader_acquire)
//
//         POLARDB_DEBUG_STARTUP_IDENTITY_FILE
//             Line == any non-empty token ("none" | "listener_proxy" |
//             "configured_fallback" are the ones the caller acts on) -> force
//             that startup-identity resolution outcome. (PgSQL_Connection.cpp:
//             polardb_debug_startup_identity_fault)
//
//         POLARDB_DEBUG_POST_SEND_OFFLINE_FILE
//             Line == "offline_no_error" -> after an automatically routed
//             replica query is sent, mark that replica OFFLINE_HARD before its
//             result is consumed. This drives the local no-backend-error
//             connection-loss path for ordinary and transaction reads.
//             (PgSQL_Connection.cpp: polardb_debug_post_send_offline)
//
//         POLARDB_DEBUG_MONITOR_HEALTH_FILE
//             Line == "node|avail|lsn" where node is "*" or "addr:port" ->
//             override the monitor health row for the matching endpoint.
//             (PgSQL_Monitor.cpp: polardb_debug_monitor_health_override)
//
// =============================================================================
#if POLARDB_PROXY && POLARDB_DEBUG
/**
 * @brief Common read/strip mechanics for a file-based DEBUG fault injector.
 *
 * Shared by every file-based fault hook so their read/strip semantics are
 * byte-identical. This helper does ONLY the read mechanics; the caller owns all
 * match/parse of the consumed line and is responsible for clearing the file (via
 * polardb_debug_clear_fault_file()) once it confirms a match:
 *   1. getenv(@p env_name); if unset or empty, return false (no fault configured).
 *   2. fopen(path, "r"); if it cannot be opened, return false.
 *   3. fgets one line into @p line_out (bounded by @p line_sz).
 *   4. Strip the first trailing CR/LF via strcspn (the rest of @p line_out keeps
 *      whatever fgets wrote / left zero-initialized by the caller).
 *   5. fclose the reader.
 *
 * The clear is deliberately NOT done here: some files are read by several
 * candidate probes in a row (wait-retry, reader-acquire), so a non-matching
 * probe must leave the file intact for the matching one. The caller calls
 * polardb_debug_clear_fault_file() after a successful match.
 *
 * @param env_name  Name of the env var holding the fault-file path.
 * @param line_out  Caller buffer to receive the (CR/LF-stripped) first line.
 *                  Must not be null.
 * @param line_sz   Size of @p line_out in bytes. Must be greater than zero.
 * @return true if a line was read; false if @p line_out is null or @p line_sz
 *         is 0, if the env var was unset/empty, if the file could not be
 *         opened, or if no line could be read. On false, @p line_out is left as
 *         the caller set it.
 */
static inline bool polardb_debug_read_fault_file(
        const char* env_name, char* line_out, size_t line_sz) {
    if (!line_out || line_sz == 0) {
        return false;
    }
    const char* fault_file = std::getenv(env_name);
    if (!fault_file || fault_file[0] == '\0') {
        return false;
    }

    FILE* f = fopen(fault_file, "r");
    if (!f) {
        return false;
    }

    bool consumed = false;
    if (fgets(line_out, (int)line_sz, f)) {
        line_out[strcspn(line_out, "\r\n")] = '\0';
        consumed = true;
    }
    fclose(f);
    return consumed;
}

/**
 * @brief Truncate a fault file so its one-shot can be enabled again.
 *
 * Re-opens the path named by @p env_name in "w" mode and immediately closes it,
 * which truncates the file to zero length. The caller invokes this only after it
 * has confirmed the consumed line matched its expected fault, mirroring the
 * match-limited clear each original injector performed. A no-op if the env var is
 * unset/empty or the file cannot be opened for writing.
 */
static inline void polardb_debug_clear_fault_file(const char* env_name) {
    const char* fault_file = std::getenv(env_name);
    if (!fault_file || fault_file[0] == '\0') {
        return;
    }
    FILE* clear_file = fopen(fault_file, "w");
    if (clear_file) {
        fclose(clear_file);
    }
}

/**
 * @brief Probe whether a fault file currently holds a non-empty first line.
 *
 * Despite calling polardb_debug_read_fault_file(), this is non-destructive:
 * it never calls polardb_debug_clear_fault_file(), so the fault stays active for
 * the hook that will actually match it. Use it only to test for presence, not
 * to read the configured value: the line is read into a 63-character buffer and
 * anything longer is truncated, so it cannot be compared against exact tokens.
 *
 * @param env_name  Name of the env var holding the fault-file path.
 * @return true when the env var names a readable file whose first line is not
 *         empty, false otherwise.
 */
static inline bool polardb_debug_fault_file_is_set(const char* env_name) {
    char buf[64] = {0};
	return polardb_debug_read_fault_file(env_name, buf, sizeof(buf)) &&
        buf[0] != '\0';
}
#endif // POLARDB_PROXY && POLARDB_DEBUG

#ifndef POLARDB_XLOGREC_PTR_DEFINED
#define POLARDB_XLOGREC_PTR_DEFINED
// PostgreSQL WAL position type. PolarDB reports it as a 64-bit value in the
// extended ReadyForQuery message; ProxySQL carries it through unchanged.
typedef uint64_t XLogRecPtr;
#define InvalidXLogRecPtr 0
static inline bool XLogRecPtrIsInvalid(XLogRecPtr lsn) { return lsn == 0; }
#endif

/**
 * @brief PolarDB node types returned by polar_node_type().
 */
enum class PolarDB_NodeType {
    UNKNOWN = 0,
    PRIMARY = 1,    // Writer node
    REPLICA = 2,    // Read-only replica (shared storage)
    STANDBY = 3     // Standby node (separate storage)
};

/**
 * @brief Type of consistency wait to apply before a read query.
 *
 * Only NONE and LSN exist; the wait wrapper renders LSN as a
 * SET polar_xact_split_wait_lsn statement. The value 1 is left unassigned so the
 * stored integer stays stable if another wait kind is added later.
 */
enum class PolarDB_WaitType : uint8_t {
    NONE = 0,  // No wait needed
    LSN  = 2   // Wait for a WAL log sequence number (polar_xact_split_wait_lsn)
};

/**
 * @brief Backend wire mode used while waiting for an LSN.
 *
 * This is an internal protocol choice, not an independent operator setting.
 * PolarDB_LsnWaitTimeoutAction determines which mode is sent and what ProxySQL
 * does if the backend reports a strict timeout.
 */
enum class PolarDB_WaitMode : uint8_t {
    BEST_EFFORT = 1,     // Wait up to timeout, then return stale data
    STRICT = 2           // Wait up to timeout, then error if not reached
};

/**
 * @brief Action when ProxySQL cannot build an enforceable LSN wait target.
 */
enum class PolarDB_MissingLsnAction : uint8_t {
    PRIMARY = 0,
    WARNING = 1,
    ERROR = 2
};

/**
 * @brief Complete client-visible outcome when an LSN wait reaches its deadline.
 */
enum class PolarDB_LsnWaitTimeoutAction : uint8_t {
    WARNING = 0,
    PRIMARY = 1,
    ERROR = 2,
    DISCONNECT = 3
};

/**
 * @brief Client outcome when a reader connection disappears before a result.
 *
 * The two retry actions try at most one different eligible reader when the
 * retained request can be sent again. If that reader is unavailable or also
 * fails, they continue with the terminal outcome named by the enum member.
 */
enum class PolarDB_ReplicaLossAction : uint8_t {
    REPLICA_THEN_PRIMARY = 0,
    REPLICA_THEN_ERROR = 1,
    PRIMARY = 2,
    ERROR = 3,
    DISCONNECT = 4
};

/**
 * @brief Action when a reusable reader returns a SQL/backend error.
 */
enum class PolarDB_ReplicaErrorAction : uint8_t {
    PRIMARY = 0,
    ERROR = 1,
    DISCONNECT = 2
};

/**
 * @brief Outcome of acquiring a reader connection for an LSN-targeted read.
 *
 * Each value names the routing fact the caller acts on. Only consistency-specific
 * failures redirect this query to the writer. Plain availability or capacity
 * failures keep ProxySQL's normal no-connection retry behavior.
 *
 * RFQ_UNAVAILABLE is a third case and is deliberately not part of
 * polardb_reader_status_redirects_to_writer(), which returns false for it. Its
 * disposition is selected at the session layer by
 * pgsql-polardb_action_missing_lsn. PRIMARY redirects to the primary,
 * WARNING degrades to a replica with no wait, and ERROR ends
 * the request. Do not read the redirect predicate as the complete disposition
 * table for this enum.
 */
enum class PolarDB_ReaderStatus : uint8_t {
    ACQUIRED = 0,           // Got a usable reader connection
    READER_UNAVAILABLE,     // No reader online/usable (plain availability)
    READER_BUSY,            // The policy-selected reader is at capacity
    READER_GROUP_BUSY,      // A retry checked every eligible reader and found no capacity
    RETRY_AFTER_CONFIG_CHANGE, // Topology or startup configuration changed
    RFQ_UNAVAILABLE,        // No free reader with an RFQ-LSN startup profile
    GROUP_LSN_UNKNOWN,      // Lag cap on, but the group LSN sample is missing
    READER_LSN_UNKNOWN,     // Lag cap on, but the reader LSN sample is missing
    READER_LSN_STALE,       // Lag cap on, but the reader LSN sample is too old to trust
    READER_LAG_EXCEEDED,    // Reader is further behind the group LSN than the byte cap allows
};

static inline int polardb_reader_status_priority(
    PolarDB_ReaderStatus status) {
    // Preserve the most actionable cause: RFQ policy first, then
    // consistency-safety failures, then ordinary capacity/availability.
    switch (status) {
    case PolarDB_ReaderStatus::RFQ_UNAVAILABLE:
        return 80;
    case PolarDB_ReaderStatus::GROUP_LSN_UNKNOWN:
        return 70;
    case PolarDB_ReaderStatus::READER_LSN_UNKNOWN:
        return 60;
    case PolarDB_ReaderStatus::READER_LSN_STALE:
        return 50;
    case PolarDB_ReaderStatus::READER_LAG_EXCEEDED:
        return 40;
    case PolarDB_ReaderStatus::READER_BUSY:
        return 30;
    case PolarDB_ReaderStatus::READER_GROUP_BUSY:
        return 30;
    case PolarDB_ReaderStatus::RETRY_AFTER_CONFIG_CHANGE:
        return 20;
    case PolarDB_ReaderStatus::READER_UNAVAILABLE:
        return 10;
    case PolarDB_ReaderStatus::ACQUIRED:
        return 0;
    }
    return 0;
}

static inline PolarDB_ReaderStatus polardb_reader_status_prefer(
    PolarDB_ReaderStatus current,
    PolarDB_ReaderStatus next) {
    return polardb_reader_status_priority(next) >
        polardb_reader_status_priority(current) ? next : current;
}

/// @brief Short stable name for a reader status, for logs and counters.
static inline const char* polardb_reader_status_name(
    PolarDB_ReaderStatus status) {
    switch (status) {
    case PolarDB_ReaderStatus::ACQUIRED:
        return "acquired";
    case PolarDB_ReaderStatus::READER_UNAVAILABLE:
        return "reader_unavailable";
    case PolarDB_ReaderStatus::READER_BUSY:
        return "reader_busy";
    case PolarDB_ReaderStatus::READER_GROUP_BUSY:
        return "reader_group_busy";
    case PolarDB_ReaderStatus::RETRY_AFTER_CONFIG_CHANGE:
        return "retry_after_config_change";
    case PolarDB_ReaderStatus::RFQ_UNAVAILABLE:
        return "rfq_unavailable";
    case PolarDB_ReaderStatus::GROUP_LSN_UNKNOWN:
        return "group_lsn_unknown";
    case PolarDB_ReaderStatus::READER_LSN_UNKNOWN:
        return "reader_lsn_unknown";
    case PolarDB_ReaderStatus::READER_LSN_STALE:
        return "reader_lsn_stale";
    case PolarDB_ReaderStatus::READER_LAG_EXCEEDED:
        return "reader_lag_exceeded";
    }
    return "unknown";
}

static inline bool polardb_reader_capacity_scope_is_blocked(
		const std::vector<uint64_t>& blocked_scopes, uint64_t scope_hash) {
	return std::find(blocked_scopes.begin(), blocked_scopes.end(), scope_hash) !=
		blocked_scopes.end();
}

/**
 * @brief Record a scope that a group-wide capacity sweep found full.
 *
 * Only READER_GROUP_BUSY is recorded, because only that status means every
 * eligible reader in the scope was checked and none had capacity. Every other
 * status — including READER_BUSY, which names one reader and not the scope —
 * leaves @p blocked_scopes untouched, so the vector must not be read as the set
 * of all capacity failures.
 *
 * @param blocked_scopes  Caller-owned vector of scope hashes already known to be
 *                        full. Appended to in place, de-duplicated against the
 *                        entries it already holds.
 * @param scope_hash      Hash of the reader scope the attempt ran against.
 * @param status          Outcome of that attempt.
 */
static inline void polardb_record_reader_group_busy_scope(
		std::vector<uint64_t>& blocked_scopes, uint64_t scope_hash,
		PolarDB_ReaderStatus status) {
	if (status == PolarDB_ReaderStatus::READER_GROUP_BUSY &&
			!polardb_reader_capacity_scope_is_blocked(blocked_scopes, scope_hash)) {
		blocked_scopes.push_back(scope_hash);
	}
}

static inline uint64_t polardb_reader_capacity_retry_delay_us(int delay_ms) {
	// poll() has millisecond resolution. Zero means the next worker tick, not a
	// nonblocking loop.
	return static_cast<uint64_t>(delay_ms > 0 ? delay_ms : 1) * 1000;
}

/**
 * @brief Shorten the worker poll timeout so a pending capacity retry is not missed.
 *
 * All three values are microseconds. Pass the timeout the worker has selected so
 * far as @p current_timeout_us; the result is never larger than it, so folding in
 * several retries keeps the earliest deadline.
 *
 * Two conventions matter. @p current_timeout_us == 0 means "no timeout selected
 * yet", not "poll without blocking", so a zero input is always replaced by the
 * candidate — never route a genuine nonblocking-poll intent through this helper.
 * The candidate is floored at 1000 us because poll() has millisecond resolution,
 * so a retry deadline that has already elapsed also yields 1000 us rather than 0.
 *
 * @param retry_at_us        Absolute time the retry becomes due.
 * @param now_us             Current time.
 * @param current_timeout_us Timeout selected so far, or 0 when none is selected.
 * @return The poll timeout to use, at least 1000 us and never longer than a
 *         non-zero @p current_timeout_us.
 */
static inline unsigned int polardb_reader_capacity_retry_timeout_us(
		uint64_t retry_at_us, uint64_t now_us,
		unsigned int current_timeout_us) {
	uint64_t remaining = retry_at_us > now_us
		? retry_at_us - now_us : 1000;
	remaining = std::max<uint64_t>(remaining, 1000);
	const unsigned int candidate = static_cast<unsigned int>(
		std::min<uint64_t>(remaining,
			std::numeric_limits<unsigned int>::max()));
	return current_timeout_us == 0 || candidate < current_timeout_us
		? candidate : current_timeout_us;
}

/**
 * @brief Where a reader connection matching a waiting session already lives on
 *        this worker.
 *
 * This is the check for cross-worker capacity requests. Only NONE may register a
 * pool-capacity request through
 * polardb_reader_pool_capacity_request_should_register(); LOCAL and ACTIVE mean
 * the connection is already on this worker, so the session adopts retention with
 * polardb_reader_adopt_retention() and waits for it instead of asking other
 * workers for one.
 *
 * Values are ordered by strength and the strongest that applies is reported:
 *   RESERVATION - this worker holds a reservation carrying a connection for the
 *                 session's scope and identity on a still-eligible server;
 *   LOCAL       - a matching connection sits in this worker's cached connections;
 *   ACTIVE      - a matching connection is in use by another session on this
 *                 worker;
 *   NONE        - nothing on this worker matches, or the session has no active
 *                 wait.
 */
enum class PolarDB_ReaderOwnership : uint8_t {
	NONE = 0,
	LOCAL,
	ACTIVE,
	RESERVATION
};

/**
 * @brief Outcome of offering a returning retained connection to another worker's
 *        pending capacity request.
 *
 * Only CONNECTION_INVALID says the connection failed the retention check.
 * NOT_ATTEMPTED tells the caller to use its ordinary return path.
 *   CONNECTION_RESERVED - the connection was handed to another worker's pending
 *                         request and this thread must not touch it again;
 *   NO_REMOTE_REQUEST   - no other worker was waiting; retain it locally;
 *   CONNECTION_INVALID  - classify it through the ordinary return path;
 *   NOT_ATTEMPTED       - no matching retention/request was present.
 */
enum class PolarDB_ReaderRemoteReservationResult : uint8_t {
	CONNECTION_RESERVED = 0,
	NO_REMOTE_REQUEST,
	CONNECTION_INVALID,
	NOT_ATTEMPTED
};

static inline bool polardb_reader_pool_capacity_request_should_register(
		PolarDB_ReaderStatus status, PolarDB_ReaderOwnership ownership) {
	return status == PolarDB_ReaderStatus::READER_GROUP_BUSY &&
		ownership == PolarDB_ReaderOwnership::NONE;
}

/**
 * @brief Whether this reader status should send the query to the writer instead.
 *
 * Only consistency/safety failures (a missing or stale LSN sample, or lag over
 * the cap) redirect the read to the writer. Plain availability or capacity
 * failures return false here so the query keeps ProxySQL's normal
 * no-connection retry behavior instead of being kept on the writer.
 */
static inline bool polardb_reader_status_redirects_to_writer(
    PolarDB_ReaderStatus status) {
    switch (status) {
    case PolarDB_ReaderStatus::GROUP_LSN_UNKNOWN:
    case PolarDB_ReaderStatus::READER_LSN_UNKNOWN:
    case PolarDB_ReaderStatus::READER_LSN_STALE:
    case PolarDB_ReaderStatus::READER_LAG_EXCEEDED:
        return true;
    default:
        return false;
    }
}

/**
 * @brief Whether transaction-split demand warmup can help this acquire failure.
 *
 * Warmup can create or prepare RFQ-compatible reader backends. It cannot fix a
 * policy refusal caused by missing/stale LSN samples or byte lag above the cap;
 * those readers must stay rejected until monitor/RFQ freshness catches up.
 */
static inline bool polardb_reader_status_split_warmup_can_help(
	PolarDB_ReaderStatus status) {
	switch (status) {
	case PolarDB_ReaderStatus::READER_UNAVAILABLE:
	case PolarDB_ReaderStatus::READER_BUSY:
	case PolarDB_ReaderStatus::READER_GROUP_BUSY:
	case PolarDB_ReaderStatus::RFQ_UNAVAILABLE:
		return true;
	default:
		return false;
	}
}

/// @brief Map a configured int to a missing-LSN action.
static inline PolarDB_MissingLsnAction polardb_missing_lsn_action_from_int(int v) {
    switch (v) {
    case static_cast<int>(PolarDB_MissingLsnAction::WARNING):
        return PolarDB_MissingLsnAction::WARNING;
    case static_cast<int>(PolarDB_MissingLsnAction::ERROR):
        return PolarDB_MissingLsnAction::ERROR;
    default:
        return PolarDB_MissingLsnAction::PRIMARY;
    }
}

/// @brief Map a configured int to an LSN-wait timeout action.
static inline PolarDB_LsnWaitTimeoutAction
polardb_lsn_wait_timeout_action_from_int(int v) {
    switch (v) {
    case static_cast<int>(PolarDB_LsnWaitTimeoutAction::WARNING):
        return PolarDB_LsnWaitTimeoutAction::WARNING;
    case static_cast<int>(PolarDB_LsnWaitTimeoutAction::PRIMARY):
        return PolarDB_LsnWaitTimeoutAction::PRIMARY;
    case static_cast<int>(PolarDB_LsnWaitTimeoutAction::DISCONNECT):
        return PolarDB_LsnWaitTimeoutAction::DISCONNECT;
    default:
        return PolarDB_LsnWaitTimeoutAction::ERROR;
    }
}

static inline const char* polardb_lsn_wait_timeout_action_name(
        PolarDB_LsnWaitTimeoutAction action) {
    switch (action) {
    case PolarDB_LsnWaitTimeoutAction::WARNING:
        return "warning";
    case PolarDB_LsnWaitTimeoutAction::PRIMARY:
        return "primary";
    case PolarDB_LsnWaitTimeoutAction::ERROR:
        return "error";
    case PolarDB_LsnWaitTimeoutAction::DISCONNECT:
        return "disconnect";
    }
    return "unknown";
}

/// @brief Derive the backend GUC mode from the complete timeout action.
static inline PolarDB_WaitMode polardb_wait_mode_for_timeout_action(
        PolarDB_LsnWaitTimeoutAction action) {
    return action == PolarDB_LsnWaitTimeoutAction::WARNING
        ? PolarDB_WaitMode::BEST_EFFORT
        : PolarDB_WaitMode::STRICT;
}

/// @brief GLOBAL_LSN cannot provide its guarantee by returning stale data.
static inline bool polardb_timeout_action_allows_global_lsn(
        PolarDB_LsnWaitTimeoutAction action) {
    return action != PolarDB_LsnWaitTimeoutAction::WARNING;
}

/**
 * Transaction-split reads cannot return a stale result while their primary
 * transaction remains open on another backend. When warning cannot be honored,
 * action_read_fallback decides between the primary and an error. The three
 * strict timeout actions retain their meaning.
 */
static inline PolarDB_LsnWaitTimeoutAction
polardb_transaction_split_timeout_action(
        PolarDB_LsnWaitTimeoutAction action,
        bool allow_primary_fallback) {
    return action != PolarDB_LsnWaitTimeoutAction::WARNING
        ? action
        : (allow_primary_fallback
            ? PolarDB_LsnWaitTimeoutAction::PRIMARY
            : PolarDB_LsnWaitTimeoutAction::ERROR);
}

/// @brief Map a configured int to a replica-connection-loss action.
static inline PolarDB_ReplicaLossAction
polardb_replica_loss_action_from_int(int v) {
    switch (v) {
    case static_cast<int>(
            PolarDB_ReplicaLossAction::REPLICA_THEN_PRIMARY):
        return PolarDB_ReplicaLossAction::REPLICA_THEN_PRIMARY;
    case static_cast<int>(
            PolarDB_ReplicaLossAction::REPLICA_THEN_ERROR):
        return PolarDB_ReplicaLossAction::REPLICA_THEN_ERROR;
    case static_cast<int>(PolarDB_ReplicaLossAction::PRIMARY):
        return PolarDB_ReplicaLossAction::PRIMARY;
    case static_cast<int>(PolarDB_ReplicaLossAction::DISCONNECT):
        return PolarDB_ReplicaLossAction::DISCONNECT;
    default:
        return PolarDB_ReplicaLossAction::ERROR;
    }
}

/// @brief Map a configured int to a reusable-replica-error action.
static inline PolarDB_ReplicaErrorAction
polardb_replica_error_action_from_int(int v) {
    switch (v) {
    case static_cast<int>(PolarDB_ReplicaErrorAction::PRIMARY):
        return PolarDB_ReplicaErrorAction::PRIMARY;
    case static_cast<int>(PolarDB_ReplicaErrorAction::DISCONNECT):
        return PolarDB_ReplicaErrorAction::DISCONNECT;
    default:
        return PolarDB_ReplicaErrorAction::ERROR;
    }
}

// Bits that say which extra payloads ProxySQL asks the PolarDB backend to append
// to ReadyForQuery in the startup handshake. LSN is needed for consistency
// routing. XID is also requested for every non-OFF PolarDB profile because it is
// a startup-only capability: txn_split_enabled can change while pooled backend
// connections already exist. That policy still controls whether the session
// observes/uses XID RFQ data. CSN remains reserved for a later feature. Request
// bits state intent only: the backend may still omit a payload from an
// individual RFQ.
constexpr uint32_t REQUEST_RFQ_LSN = 1u << 0;
constexpr uint32_t REQUEST_RFQ_CSN = 1u << 1;
constexpr uint32_t REQUEST_RFQ_XID = 1u << 2;

/**
 * @brief PolarDB proxy startup-parameter dialect for a backend connection.
 *
 * Selects which PolarDB-specific keys ProxySQL sends in the startup packet to
 * identify itself as a proxy and request that the backend append the RFQ LSN.
 * Resolved per replication-hostgroup over the global default.
 */
enum class PolarDB_ProxyProtocol : uint8_t {
    OFF = 0,        // Send no PolarDB proxy startup keys
    LEGACY = 1,     // Older key names (_polar_origin_client_ip/port, _polar_send_lsn)
    V15 = 2         // Newer key names (_polar_proxy_client_host/port, _polar_proxy_send_lsn)
};

/// @brief Map a configured int to PolarDB_ProxyProtocol; unknown values map to OFF.
static inline PolarDB_ProxyProtocol polardb_proxy_protocol_from_int(int protocol) {
    switch (protocol) {
    case static_cast<int>(PolarDB_ProxyProtocol::V15):
        return PolarDB_ProxyProtocol::V15;
    case static_cast<int>(PolarDB_ProxyProtocol::LEGACY):
        return PolarDB_ProxyProtocol::LEGACY;
    case static_cast<int>(PolarDB_ProxyProtocol::OFF):
    default:
        return PolarDB_ProxyProtocol::OFF;
    }
}

static inline const char* polardb_proxy_protocol_config_name(
        PolarDB_ProxyProtocol protocol) {
    switch (protocol) {
    case PolarDB_ProxyProtocol::OFF:
        return "off";
    case PolarDB_ProxyProtocol::LEGACY:
        return "legacy";
    case PolarDB_ProxyProtocol::V15:
        return "v15";
    }
    return "off";
}

/**
 * @brief Where the client endpoint sent in the PolarDB startup params came from.
 *
 * The legacy/v15 startup keys carry an origin host and port. This records which
 * source supplied them, so an RFQ-requesting connection can be rejected when no
 * usable identity is available.
 */
enum class PolarDB_StartupIdentitySource : uint8_t {
    NONE = 0,                   // No identity available
    CLIENT = 1,                 // The real client endpoint
    LISTENER_PROXY = 2,         // The local listener/proxy endpoint
    CONFIGURED_FALLBACK = 3     // A configured fallback endpoint
};

/**
 * @brief What ProxySQL requested from a PolarDB backend at startup (Spec).
 *
 * Connection-local record of the chosen proxy protocol and the RFQ payload bits
 * sent in the startup packet. The request bits state intent only: a requested
 * RFQ-LSN bit records what ProxySQL asked for and never shows whether the
 * backend will actually return LSNs. That is only known once result RFQs carry
 * an LSN payload.
 *
 * The profile is also the pool-compatibility key. A pooled connection may be
 * reused for a request when polardb_startup_profile_compatible_for_reuse()
 * accepts the two profiles and the connection's stored generation() token
 * matches the requested one.
 */
struct PolarDB_StartupProfile {
    PolarDB_ProxyProtocol protocol{PolarDB_ProxyProtocol::OFF};
    uint32_t request_bits{0};   // OR of REQUEST_RFQ_* bits requested

    /// @brief Build the default profile for a protocol. Any non-OFF protocol
    /// requests RFQ LSN and XID payloads; OFF requests nothing.
    static PolarDB_StartupProfile from_protocol(PolarDB_ProxyProtocol protocol) {
        PolarDB_StartupProfile profile;
        profile.protocol = protocol;
        if (protocol == PolarDB_ProxyProtocol::LEGACY ||
            protocol == PolarDB_ProxyProtocol::V15) {
            profile.request_bits = REQUEST_RFQ_LSN | REQUEST_RFQ_XID;
        }
        return profile;
    }

    /// @brief True if the given REQUEST_RFQ_* bit was requested.
    bool requests(uint32_t bit) const {
        return (request_bits & bit) != 0;
    }

    /// @brief True if this profile asked the backend to append the RFQ LSN.
	bool requests_rfq_lsn() const {
        return requests(REQUEST_RFQ_LSN);
    }

    /// @brief Add the transaction XID RFQ request to a non-OFF profile.
    void request_rfq_xid() {
        if (protocol != PolarDB_ProxyProtocol::OFF) {
            request_bits |= REQUEST_RFQ_XID;
        }
    }

    /// @brief True if this profile asked the backend to append transaction XIDs.
	bool requests_rfq_xid() const {
        return requests(REQUEST_RFQ_XID);
    }

    /// @brief True if this profile causes any PolarDB startup keys to be sent.
    bool emits_startup_params() const {
        return protocol != PolarDB_ProxyProtocol::OFF && request_bits != 0;
    }

    /**
     * @brief Build the compatibility token a pooled connection is matched on.
     *
     * Packs the startup decision into one word: the proxy protocol in bits
     * 31..24, the identity mode in bits 23..22 (zero unless
     * emits_startup_params() is true, since a connection that sends no startup
     * keys carries no identity), and the RFQ request bits in bits 21..0. The
     * token is stored on the connection and compared by
     * polardb_startup_profile_matches_request_generation() and the
     * polardb_startup_config_generation checks that reject stale pooled
     * backends.
     *
     * @param identity_mode  Resolved PolarDB_ProxyIdentityMode as an int.
     * @return The packed generation token.
     */
    uint32_t generation(int identity_mode) const {
        const uint32_t identity_bits = emits_startup_params()
            ? (static_cast<uint32_t>(identity_mode) & 0x3u)
            : 0;
        return (static_cast<uint32_t>(protocol) << 24) |
            (identity_bits << 22) |
            (request_bits & 0x003fffffu);
    }
};

static inline bool polardb_startup_profile_compatible_for_reuse(
        const PolarDB_StartupProfile& pooled,
        const PolarDB_StartupProfile& requested) {
    return pooled.protocol == requested.protocol &&
        pooled.request_bits == requested.request_bits;
}

static inline bool polardb_startup_profile_matches_request_generation(
        const PolarDB_StartupProfile& pooled,
        uint32_t pooled_generation,
        const PolarDB_StartupProfile& requested,
        int requested_identity_mode) {
    return pooled_generation == requested.generation(requested_identity_mode) &&
        polardb_startup_profile_compatible_for_reuse(pooled, requested);
}

static inline bool polardb_startup_profile_requests_rfq_lsn_xid(
        const PolarDB_StartupProfile& profile) {
	return profile.requests_rfq_lsn() && profile.requests_rfq_xid();
}

enum class PolarDB_PoolProfile : uint8_t {
    BASE = 0,
    RFQ = 1
};

/**
 * @brief Identity a pooled backend connection is matched on.
 *
 * The three members are FNV-1a hashes of the inputs that must agree before a
 * pooled connection can serve a request: the authentication credentials, the
 * PolarDB startup identity (see polardb_startup_client_reuse_key()), and the
 * session options sent at startup. Equality compares the hashes only, so a hash
 * collision is accepted as identity; the members are digests, not values that
 * can be compared field by field.
 *
 * empty() means the key has not been computed yet, which is not the same as
 * "every input was empty". A request whose key is empty must not be used for
 * matching — polardb_register_reader_capacity_request() rejects it for
 * exactly that reason.
 */
struct PolarDB_PoolKey {
    uint64_t auth_hash{0};
    uint64_t startup_identity_hash{0};
    uint64_t startup_options_hash{0};

    bool empty() const {
        return auth_hash == 0 && startup_identity_hash == 0 &&
            startup_options_hash == 0;
    }

    bool operator==(const PolarDB_PoolKey& other) const {
        return auth_hash == other.auth_hash &&
            startup_identity_hash == other.startup_identity_hash &&
            startup_options_hash == other.startup_options_hash;
    }

    bool operator!=(const PolarDB_PoolKey& other) const {
        return !(*this == other);
    }
};

/**
 * @brief Disposition of a worker-held reader connection on the return path.
 *
 * Returned by PgSQL_PolarDB_ReaderPool::local_return_decision(); each value tells
 * the caller who owns the connection next:
 *   USE_SHARED_POOL             - hand it back to the shared server pool;
 *   USE_SHARED_POOL_REUSE_CHECKED - hand it back to the shared server pool, and
 *                                   do not repeat the completed reuse check;
 *   KEEP_WITH_WORKER            - the worker retains it locally instead of
 *                                 returning it to the shared pool;
 *   REMOVE_CONNECTION           - it failed the reuse or online check and the
 *                                 caller must destroy it.
 */
enum class PolarDB_ReaderLocalReturn : uint8_t {
    USE_SHARED_POOL = 0,
    USE_SHARED_POOL_REUSE_CHECKED,
    KEEP_WITH_WORKER,
    REMOVE_CONNECTION
};

enum class PolarDB_ReaderConnectionReturnStatus : uint8_t {
    RETURNABLE = 0,
    NOT_MANAGED,
    OFFLINE,
    CLIENT_IDENTITY,
    UNUSABLE,
    EMPTY_KEY
};

struct PolarDB_ReaderLocalReturnDecision {
    PolarDB_ReaderLocalReturn action{
        PolarDB_ReaderLocalReturn::USE_SHARED_POOL};
    PolarDB_ReaderConnectionReturnStatus connection_status{
        PolarDB_ReaderConnectionReturnStatus::NOT_MANAGED};
};

static inline bool polardb_reader_uses_shared_pool(
        PolarDB_ReaderLocalReturn decision) {
    return decision == PolarDB_ReaderLocalReturn::USE_SHARED_POOL ||
        decision == PolarDB_ReaderLocalReturn::USE_SHARED_POOL_REUSE_CHECKED;
}

static inline uint64_t polardb_pool_hash_bytes(
        uint64_t hash, const void* data, size_t len) {
    const unsigned char* bytes = static_cast<const unsigned char*>(data);
    for (size_t i = 0; i < len; i++) {
        hash ^= bytes[i];
        hash *= 1099511628211ULL;
    }
    return hash;
}

static inline uint64_t polardb_pool_hash_u64(uint64_t hash, uint64_t value) {
    return polardb_pool_hash_bytes(hash, &value, sizeof(value));
}

static inline uint64_t polardb_pool_hash_i32(uint64_t hash, int value) {
    return polardb_pool_hash_bytes(hash, &value, sizeof(value));
}

static inline uint64_t polardb_pool_hash_string(
        uint64_t hash, const std::string& value) {
    hash = polardb_pool_hash_u64(hash, value.size());
    return polardb_pool_hash_bytes(hash, value.data(), value.size());
}

static inline uint64_t polardb_pool_hash_cstr(
        uint64_t hash, const char* value) {
    const size_t len = value ? strlen(value) : 0;
    hash = polardb_pool_hash_u64(hash, len);
    return len ? polardb_pool_hash_bytes(hash, value, len) : hash;
}

static inline const char* polardb_pool_profile_name(
        PolarDB_PoolProfile profile) {
    switch (profile) {
    case PolarDB_PoolProfile::BASE:
        return "base";
    case PolarDB_PoolProfile::RFQ:
        return "rfq";
    }
    return "unknown";
}

static inline PolarDB_PoolProfile polardb_pool_profile_from_startup_profile(
        const PolarDB_StartupProfile& profile) {
    return profile.emits_startup_params()
        ? PolarDB_PoolProfile::RFQ
        : PolarDB_PoolProfile::BASE;
}

static inline bool polardb_pool_profile_matches_startup_profile(
        const PolarDB_StartupProfile& pooled,
        PolarDB_PoolProfile expected_profile) {
    return polardb_pool_profile_from_startup_profile(pooled) ==
        expected_profile;
}

/**
 * @brief Complete request for matching or opening one backend connection.
 *
 * `expected_profile` segregates the pool: BASE accepts only connections that send
 * no PolarDB startup keys, RFQ only those that do, and a connection on the wrong
 * side is refused rather than reset. `only_pooled` forbids opening a new backend
 * connection, so the request must fail when the pool has no match.
 */
struct PolarDB_PoolRequest {
    PolarDB_PoolKey key;
    PolarDB_StartupProfile startup_profile;
    int startup_identity_mode;
    PolarDB_PoolProfile expected_profile;
    bool only_pooled;

    PolarDB_PoolRequest(
            const PolarDB_PoolKey& request_key,
            const PolarDB_StartupProfile& request_profile,
            int request_identity_mode,
            PolarDB_PoolProfile request_expected_profile,
            bool request_only_pooled)
        : key(request_key)
        , startup_profile(request_profile)
        , startup_identity_mode(request_identity_mode)
        , expected_profile(request_expected_profile)
        , only_pooled(request_only_pooled)
    {}

    bool ready_for_matching() const {
        return startup_identity_mode >= 0 && !key.empty();
    }
};

enum class PolarDB_PoolReuseState : uint8_t {
    EXACT = 0,
    NEEDS_RESET = 1,
    NEEDS_VARIABLE_UPDATE = 2,
    BAD_CONTEXT = 3,
    PROFILE_MISMATCH = 4,
    AUTH_MISMATCH = 5,
    IDENTITY_MISMATCH = 6,
    SESSION_STATE_MISMATCH = 7
};

static inline const char* polardb_pool_reuse_state_name(
        PolarDB_PoolReuseState state) {
    switch (state) {
    case PolarDB_PoolReuseState::EXACT:
        return "exact";
    case PolarDB_PoolReuseState::NEEDS_RESET:
        return "needs_reset";
    case PolarDB_PoolReuseState::NEEDS_VARIABLE_UPDATE:
        return "needs_variable_update";
    case PolarDB_PoolReuseState::BAD_CONTEXT:
        return "bad_context";
    case PolarDB_PoolReuseState::PROFILE_MISMATCH:
        return "profile_mismatch";
    case PolarDB_PoolReuseState::AUTH_MISMATCH:
        return "auth_mismatch";
    case PolarDB_PoolReuseState::IDENTITY_MISMATCH:
        return "identity_mismatch";
    case PolarDB_PoolReuseState::SESSION_STATE_MISMATCH:
        return "session_state_mismatch";
    }
    return "unknown";
}

struct PolarDB_PoolReuseClassification {
    PolarDB_PoolReuseState state{PolarDB_PoolReuseState::BAD_CONTEXT};
    unsigned int matching_session_variables{0};
};

static inline bool polardb_pool_startup_profile_compatible_for_reuse(
        const PolarDB_StartupProfile& pooled,
        const PolarDB_PoolRequest& request) {
    return polardb_pool_profile_matches_startup_profile(
            pooled, request.expected_profile) &&
        polardb_startup_profile_compatible_for_reuse(
            pooled, request.startup_profile);
}

static inline bool polardb_startup_parameter_consumed_by_proxy(
        const std::string& key_lowercase) {
    return key_lowercase == "_polar_send_lsn" ||
        key_lowercase == "_polar_proxy_send_lsn" ||
        key_lowercase == "_polar_send_xact" ||
        key_lowercase == "_polar_proxy_send_xact" ||
        key_lowercase == "_polar_origin_client_ip" ||
        key_lowercase == "_polar_origin_client_port" ||
        key_lowercase == "_polar_proxy_client_host" ||
        key_lowercase == "_polar_proxy_client_port";
}

/**
 * @brief True if the host is empty or an "any" address.
 *
 * An empty or null host is treated as a wildcard because it carries no specific
 * client endpoint to send to the backend. A wildcard cannot identify a client,
 * so RFQ-requesting connections reject it.
 */
inline bool polardb_identity_host_is_wildcard(const char* host) {
    if (!host || host[0] == '\0') {
        return true;
    }

    struct in_addr addr4;
    if (inet_pton(AF_INET, host, &addr4) == 1) {
        return addr4.s_addr == INADDR_ANY;
    }

    struct in6_addr addr6;
    if (inet_pton(AF_INET6, host, &addr6) == 1) {
        if (IN6_IS_ADDR_UNSPECIFIED(&addr6)) {
            return true;
        }
        return IN6_IS_ADDR_V4MAPPED(&addr6) &&
            addr6.s6_addr[12] == 0 &&
            addr6.s6_addr[13] == 0 &&
            addr6.s6_addr[14] == 0 &&
            addr6.s6_addr[15] == 0;
    }

    return false;
}

/// @brief True if the host string is a literal IPv4 or IPv6 address (not a name).
inline bool polardb_identity_host_is_ip(const char* host) {
    if (!host || host[0] == '\0') {
        return false;
    }

    struct in_addr addr4;
    if (inet_pton(AF_INET, host, &addr4) == 1) {
        return true;
    }

    struct in6_addr addr6;
    if (inet_pton(AF_INET6, host, &addr6) == 1) {
        return true;
    }

    return false;
}

/**
 * @brief Resolved client endpoint to advertise in PolarDB startup params.
 *
 * Holds the host, port, and where they came from. A connection that requests the
 * RFQ LSN must have a valid, non-wildcard IP identity before it is created.
 */
struct PolarDB_StartupIdentity {
    std::string host;
    int port = 0;
    PolarDB_StartupIdentitySource source{PolarDB_StartupIdentitySource::NONE};

    PolarDB_StartupIdentity() = default;

    PolarDB_StartupIdentity(
        const char* host_value,
        int port_value,
        PolarDB_StartupIdentitySource source_value = PolarDB_StartupIdentitySource::NONE)
        : host(host_value ? host_value : "")
        , port(port_value)
        , source(source_value) {}

    PolarDB_StartupIdentity(
        std::string host_value,
        int port_value,
        PolarDB_StartupIdentitySource source_value = PolarDB_StartupIdentitySource::NONE)
        : host(std::move(host_value))
        , port(port_value)
        , source(source_value) {}

    bool host_is_wildcard() const {
        return polardb_identity_host_is_wildcard(host.c_str());
    }

    bool host_is_ip() const {
        return polardb_identity_host_is_ip(host.c_str());
    }

    /**
     * @brief Whether this identity is usable in startup params.
     * @param reject_wildcard If true, an "any" address also fails.
     * @return true only for a literal IP host with a valid port (1..65535).
     */
    bool valid(bool reject_wildcard) const {
        if (host.empty() || port <= 0 || port > 65535) {
            return false;
        }
        if (!host_is_ip()) {
            return false;  // hostnames are not accepted; only literal IPs
        }
        return !reject_wildcard || !host_is_wildcard();
    }
};

/**
 * @brief Startup-client metadata tied to one backend connection.
 *
 * Today only @ref identity is emitted in PolarDB startup parameters. The SSL and
 * proxy session/cancel fields are intentionally reserved placeholders: PolarDB
 * has backend support for these startup keys, but ProxySQL does not send them
 * yet. Keeping them here gives pooling and warmup one compatibility key to
 * extend later instead of scattering new checks through the split path.
 */
struct PolarDB_StartupClientContext {
    PolarDB_StartupIdentity identity;
    bool frontend_ssl = false;
    std::string ssl_version;
    std::string ssl_cipher;
    bool has_proxy_session = false;
    uint64_t proxy_session_id = 0;
    uint32_t proxy_cancel_key = 0;

    bool identity_valid_for_startup(bool reject_wildcard) const {
        return identity.valid(reject_wildcard);
    }

    bool has_strict_metadata() const {
        return frontend_ssl || has_proxy_session;
    }

    /**
     * @brief Return whether every startup-context field permits reuse.
     */
    bool compatible_for_reuse(
            const PolarDB_StartupClientContext& other) const {
        return identity.source == other.identity.source &&
            identity.host == other.identity.host &&
            identity.port == other.identity.port &&
            frontend_ssl == other.frontend_ssl &&
            ssl_version == other.ssl_version &&
            ssl_cipher == other.ssl_cipher &&
            has_proxy_session == other.has_proxy_session &&
            proxy_session_id == other.proxy_session_id &&
            proxy_cancel_key == other.proxy_cancel_key;
    }

};

/**
 * @brief Check whether a pooled backend has the startup settings required now.
 *
 * This is shared by the worker-local cache and the core shared pool. It compares
 * the stored startup generation, protocol, RFQ request bits, identity mode, and
 * startup client values. It does not take a pool or hostgroup lock.
 */
bool polardb_connection_startup_settings_match(
    const PgSQL_Connection* conn,
    const PolarDB_StartupProfile& startup_profile,
    const PolarDB_StartupClientContext* startup_client,
    uint64_t current_startup_generation,
    int current_identity_mode);

bool polardb_startup_client_from_session(
    PgSQL_Session* sess, PolarDB_StartupClientContext* startup_client);

/**
 * @brief Convert a raw socket address into a startup identity.
 *
 * The raw client address fallback uses valid(false): it accepts any literal
 * socket address observed for the client side. The later listener-proxy fallback
 * applies valid(true) because a wildcard listener is not a real client identity.
 */
inline bool polardb_startup_identity_from_sockaddr(
        const struct sockaddr* addr,
        PolarDB_StartupIdentity* identity,
        PolarDB_StartupIdentitySource source = PolarDB_StartupIdentitySource::CLIENT) {
    if (identity) {
        *identity = PolarDB_StartupIdentity{};
    }
    if (!addr || !identity) {
        return false;
    }

    char buf[INET6_ADDRSTRLEN] = "";
    int port = 0;
    if (addr->sa_family == AF_INET) {
        const struct sockaddr_in* addr4 =
            reinterpret_cast<const struct sockaddr_in*>(addr);
        if (!inet_ntop(AF_INET, &addr4->sin_addr, buf, sizeof(buf))) {
            return false;
        }
        port = ntohs(addr4->sin_port);
    } else if (addr->sa_family == AF_INET6) {
        const struct sockaddr_in6* addr6 =
            reinterpret_cast<const struct sockaddr_in6*>(addr);
        if (!inet_ntop(AF_INET6, &addr6->sin6_addr, buf, sizeof(buf))) {
            return false;
        }
        port = ntohs(addr6->sin6_port);
    } else {
        return false;
    }

    PolarDB_StartupIdentity resolved{buf, port, source};
    if (!resolved.valid(false)) {
        return false;
    }
    *identity = std::move(resolved);
    return true;
}

/**
 * @brief Validate a configured identity override before it is stored.
 *
 * An empty host is accepted: it means no override is set, which is valid. A set
 * host must be a non-wildcard literal IP. When @p allow_incomplete_port is true,
 * the host may be validated while the port is still unset (0), so the two config
 * fields can be checked as they are entered.
 *
 * @param host                 Configured host, or empty/null for "no override".
 * @param port                 Configured port (0 = not yet set).
 * @param allow_incomplete_port Permit a host with port still 0.
 * @return true if the override is acceptable.
 */
inline bool polardb_identity_config_valid(
        const char* host, int port, bool allow_incomplete_port) {
    if (!host || host[0] == '\0') {
        return true;  // unset override is valid
    }
    if (!polardb_identity_host_is_ip(host)) {
        return false;
    }
    if (allow_incomplete_port && port == 0) {
        return !polardb_identity_host_is_wildcard(host);
    }
    return PolarDB_StartupIdentity{host, port}.valid(true);
}

static constexpr uint32_t POLARDB_DEFAULT_WAIT_TIMEOUT_MS = 1000;
static constexpr uint32_t POLARDB_TXN_SPLIT_RESET_WRAPPER_SET_COUNT = 1;  // clear stale split XIDs

/*
 * Timeout policy for PolarDB consistency waits:
 *
 * ProxySQL always sends a concrete timeout SET as part of the consistency
 * wrapper:
 *
 *     SET polar_proxy_wait_timeout_ms = <resolved_ms>;
 *
 * The resolved timeout is chosen by ProxySQL:
 *  - pgsql_replication_hostgroups.lsn_wait_timeout_ms > 0:
 *      use that per-HG value.
 *  - pgsql_replication_hostgroups.lsn_wait_timeout_ms == 0:
 *      wait indefinitely for this HG.
 *  - pgsql_replication_hostgroups.lsn_wait_timeout_ms == -1:
 *      inherit pgsql-polardb_lsn_wait_timeout_ms.
 *
 * The global pgsql-polardb_lsn_wait_timeout_ms default is 1000 ms. Setting the global
 * value to 0 is an explicit request to wait indefinitely for HGs that inherit it.
 *
 * PolarDB backend behavior:
 *  - polar_proxy_wait_timeout_ms > 0:
 *      wait up to that many milliseconds. If the target is still not reached,
 *      polar_consistency_mode selects the result: best_effort emits WARNING and
 *      returns stale data; strict raises ERROR and aborts the query.
 *  - polar_proxy_wait_timeout_ms == 0:
 *      the timeout check is disabled. The query waits until the replica reaches
 *      the requested LSN. In this case best_effort and strict behave the same for
 *      the wait itself, because the timeout branch is never reached.
 *
 * Operational note:
 *  - Indefinite waits are strongest for RYW but can hold a client and backend
 *    connection if the replica is stuck or far behind. Use max_lag_bytes and/or
 *    statement_timeout when configuring timeout 0 in a deployed proxy.
 */

/**
 * @brief Which family of prepended SET statements precedes the user query.
 *
 * Tells the connection layer what it is consuming ahead of the user result, so
 * those wrapper SET results are dropped and never forwarded to the client.
 */
enum class PolarDB_Query_WrapperKind : uint8_t {
    NONE = 0,             // No PolarDB wrapper SET results to consume
    CONSISTENCY_WAIT = 1, // Mode + timeout + wait-target SETs before the user query
    TXN_SPLIT_WAIT = 2,   // XID SET plus mode + timeout + wait-target SETs
    TXN_SPLIT_XIDS_RESET = 3 // Empty XID SET used to clean a reused reader
};

/**
 * @brief Consistency routing mode for a PolarDB session.
 *
 * Resolved from the three-level config hierarchy: session override > hostgroup >
 * global. Unsupported integer values map to OFF.
 */
enum class PolarDB_ConsistencyMode : uint8_t {
    OFF = 0,            // Bypass PolarDB query routing
    SESSION_LSN = 1,    // Wait on max(session write LSN, observed LSN)
    GLOBAL_LSN = 2,     // Wait on max(session target, latest group LSN)
    EVENTUAL = 3        // PolarDB placement without an LSN wait
};

/// @brief Validate the actions that may knowingly return a degraded reader result.
static inline const char* polardb_consistency_policy_error(
        PolarDB_ConsistencyMode mode,
        PolarDB_MissingLsnAction missing_lsn_action,
        PolarDB_LsnWaitTimeoutAction timeout_action) {
    if (mode != PolarDB_ConsistencyMode::GLOBAL_LSN) {
        return nullptr;
    }
    if (missing_lsn_action == PolarDB_MissingLsnAction::WARNING) {
        return "global_lsn cannot use a reader without an LSN target";
    }
    if (!polardb_timeout_action_allows_global_lsn(timeout_action)) {
        return "global_lsn cannot return stale data after an LSN wait timeout";
    }
    return nullptr;
}

/// @brief Convert a resolved consistency-mode int (session > HG > global) to the
/// typed enum. Must NOT be called on the unresolved -1 sentinel (asserts v >= 0).
/// Any unsupported value maps to OFF.
inline PolarDB_ConsistencyMode polardb_consistency_from_int(int v) {
    assert(v >= 0);
    switch (v) {
        case static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN):
            return PolarDB_ConsistencyMode::SESSION_LSN;
        case static_cast<int>(PolarDB_ConsistencyMode::GLOBAL_LSN):
            return PolarDB_ConsistencyMode::GLOBAL_LSN;
        case static_cast<int>(PolarDB_ConsistencyMode::EVENTUAL):
            return PolarDB_ConsistencyMode::EVENTUAL;
        default:
            return PolarDB_ConsistencyMode::OFF;
    }
}

/// @brief Map a config string to the consistency-mode int. Accepted values are
/// "off", "eventual", "session_lsn", and "global_lsn"; matching is
/// case-insensitive.
/// Null, empty, "default", or any unknown value returns @p default_value.
static inline int polardb_consistency_mode_from_string(
    const char* value,
    int default_value) {
    if (!value || value[0] == '\0' || strcasecmp(value, "default") == 0) {
        return default_value;
    }
    if (strcasecmp(value, "off") == 0) {
        return static_cast<int>(PolarDB_ConsistencyMode::OFF);
    }
    if (strcasecmp(value, "session_lsn") == 0) {
        return static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN);
    }
    if (strcasecmp(value, "global_lsn") == 0) {
        return static_cast<int>(PolarDB_ConsistencyMode::GLOBAL_LSN);
    }
    if (strcasecmp(value, "eventual") == 0) {
        return static_cast<int>(PolarDB_ConsistencyMode::EVENTUAL);
    }
    return default_value;
}

static inline bool polardb_consistency_mode_uses_lsn_wait(
    PolarDB_ConsistencyMode mode) {
    return mode == PolarDB_ConsistencyMode::SESSION_LSN ||
        mode == PolarDB_ConsistencyMode::GLOBAL_LSN;
}

/**
 * @brief Validate the LSN source required by an effective hostgroup policy.
 *
 * SESSION_LSN learns the session target from RFQ replies. Without an RFQ
 * capable startup protocol the first read has no target, and its missing RFQ
 * reply makes every later read take the configured missing-LSN action. Reject
 * that configuration instead of allowing routing to change after one query.
 *
 * GLOBAL_LSN is deliberately not rejected here: it can use the group LSN
 * supplied by the monitor, and its missing-target action already fails closed.
 */
static inline const char* polardb_hostgroup_lsn_source_error(
        PolarDB_ConsistencyMode mode,
        PolarDB_ProxyProtocol protocol) {
    if (mode == PolarDB_ConsistencyMode::SESSION_LSN &&
            protocol == PolarDB_ProxyProtocol::OFF) {
        return "session_lsn requires proxy_protocol v15 or legacy";
    }
    return nullptr;
}

/**
 * @brief Whether this consistency mode refuses a degraded (no-wait, best-effort)
 *        reader route.
 *
 * The asymmetry against polardb_consistency_mode_uses_lsn_wait() is intentional:
 * both LSN modes use an LSN wait, but only GLOBAL_LSN forbids degrading to a
 * reader without one. A global target covers writes the session never made, so
 * there is nothing to approximate it with; a SESSION_LSN target only covers the
 * session's own writes and may still degrade under a best-effort policy.
 *
 * @param mode  Resolved consistency mode.
 * @return true only for GLOBAL_LSN.
 */
static inline bool polardb_consistency_mode_disallows_degraded_reader(
    PolarDB_ConsistencyMode mode) {
    return mode == PolarDB_ConsistencyMode::GLOBAL_LSN;
}

static inline const char* polardb_consistency_mode_name(
    PolarDB_ConsistencyMode mode) {
    switch (mode) {
    case PolarDB_ConsistencyMode::OFF:
        return "off";
    case PolarDB_ConsistencyMode::EVENTUAL:
        return "eventual";
    case PolarDB_ConsistencyMode::SESSION_LSN:
        return "session_lsn";
    case PolarDB_ConsistencyMode::GLOBAL_LSN:
        return "global_lsn";
    }
    return "unknown";
}

/**
 * @brief Normal destination for a replica-eligible read.
 *
 * This setting answers only where to try first. The independent read-fallback
 * action says what to do when the selected target cannot serve the request.
 */
enum class PolarDB_ReadTarget : uint8_t {
    PRIMARY = 0,
    REPLICA = 1
};

static inline PolarDB_ReadTarget polardb_read_target_from_int(int v) {
    return v == static_cast<int>(PolarDB_ReadTarget::REPLICA)
        ? PolarDB_ReadTarget::REPLICA
        : PolarDB_ReadTarget::PRIMARY;
}

static inline int polardb_read_target_from_string(
        const char* value, int default_value) {
    if (!value || value[0] == '\0' || strcasecmp(value, "default") == 0) {
        return default_value;
    }
    if (strcasecmp(value, "primary") == 0) {
        return static_cast<int>(PolarDB_ReadTarget::PRIMARY);
    }
    if (strcasecmp(value, "replica") == 0) {
        return static_cast<int>(PolarDB_ReadTarget::REPLICA);
    }
    return default_value;
}

static inline const char* polardb_read_target_name(PolarDB_ReadTarget target) {
    return target == PolarDB_ReadTarget::PRIMARY ? "primary" : "replica";
}

/**
 * @brief Final result when a read targeted at a replica cannot acquire one.
 */
enum class PolarDB_ReadFallbackAction : uint8_t {
    PRIMARY = 0,
    ERROR = 1
};

static inline PolarDB_ReadFallbackAction
polardb_read_fallback_action_from_int(int v) {
    return v == static_cast<int>(PolarDB_ReadFallbackAction::ERROR)
        ? PolarDB_ReadFallbackAction::ERROR
        : PolarDB_ReadFallbackAction::PRIMARY;
}

static inline int polardb_read_fallback_action_from_string(
        const char* value, int default_value) {
    if (!value || value[0] == '\0' || strcasecmp(value, "default") == 0) {
        return default_value;
    }
    if (strcasecmp(value, "primary") == 0) {
        return static_cast<int>(PolarDB_ReadFallbackAction::PRIMARY);
    }
    if (strcasecmp(value, "error") == 0) {
        return static_cast<int>(PolarDB_ReadFallbackAction::ERROR);
    }
    return default_value;
}

static inline const char* polardb_read_fallback_action_name(
        PolarDB_ReadFallbackAction action) {
    return action == PolarDB_ReadFallbackAction::ERROR ? "error" : "primary";
}

enum class PolarDB_Profile : uint8_t {
    OFF = 0,
    EVENTUAL,
    SESSION_WARNING,
    SESSION_FALLBACK,
    SESSION_ERROR,
    GLOBAL_FALLBACK,
    GLOBAL_ERROR,
    CUSTOM
};

enum class PolarDB_ReaderAcquireAction : uint8_t {
    RETRY_READER = 0,
    USE_PRIMARY = 1,
    RETURN_ERROR = 2
};

/**
 * @brief Apply placement policy after reader selection cannot complete.
 *
 * RFQ_UNAVAILABLE is handled by missing_lsn_action before this function.
 * Capacity-related states keep waiting until the existing request deadline.
 * Other failures use the placement policy to choose the writer or an error.
 */
static inline PolarDB_ReaderAcquireAction polardb_reader_acquire_action(
        PolarDB_ReadFallbackAction fallback,
        PolarDB_ReaderStatus status) {
    if (status == PolarDB_ReaderStatus::READER_BUSY ||
            status == PolarDB_ReaderStatus::READER_GROUP_BUSY ||
            status == PolarDB_ReaderStatus::RETRY_AFTER_CONFIG_CHANGE ||
            status == PolarDB_ReaderStatus::RFQ_UNAVAILABLE ||
            status == PolarDB_ReaderStatus::ACQUIRED) {
        return PolarDB_ReaderAcquireAction::RETRY_READER;
    }
    return fallback == PolarDB_ReadFallbackAction::ERROR
        ? PolarDB_ReaderAcquireAction::RETURN_ERROR
        : PolarDB_ReaderAcquireAction::USE_PRIMARY;
}

/**
 * @brief Raise a session wait target to the group-wide position under GLOBAL_LSN.
 *
 * The return value alone is not the whole answer, and reading it that way is a
 * correctness bug. A return of 0 with *group_lsn_unknown set to true means the
 * group observation is missing, so global consistency cannot be confirmed — it
 * does NOT mean "no wait is needed". A caller that only tests the return value
 * would send a GLOBAL_LSN read to a replica with no wait attached. Always check
 * the out-param before treating a zero result as "nothing to wait for".
 *
 * @param local_target_lsn     Wait target derived from the session's own writes.
 * @param group_lsn            Highest accepted LSN from the group, or 0 when
 *                             no value is available.
 * @param group_lsn_unknown    Receives whether the group LSN was
 *                             missing. Always written when non-null (cleared on
 *                             entry, then set only for the missing case). May be
 *                             null, in which case the caller loses the ability
 *                             to distinguish the two meanings of a zero return.
 * @return max(@p local_target_lsn, @p group_lsn), or 0 when @p group_lsn is 0.
 */
static inline uint64_t polardb_target_with_global_lsn(
    uint64_t local_target_lsn,
    uint64_t group_lsn,
    bool* group_lsn_unknown) {
    if (group_lsn_unknown) {
        *group_lsn_unknown = false;
    }
    if (group_lsn == 0) {
        if (group_lsn_unknown) {
            *group_lsn_unknown = true;
        }
        return 0;
    }
    return local_target_lsn > group_lsn ? local_target_lsn : group_lsn;
}

/**
 * @brief Session policy for when transaction-split reader warmup is requested.
 *
 * Split execution never opens a socket on the query path: it only temporarily uses an
 * already-pooled compatible reader. This mode controls when the session asks the
 * HGM maintenance pass to create that pooled reader in the background.
 */
enum class PolarDB_TxnSplitWarmupMode : uint8_t {
    OFF = 0,     // Never queue warmup from this session
    DEMAND = 1,  // Queue after a split-readable transaction read misses the pool
    BEGIN = 2,   // Queue at BEGIN / START TRANSACTION only
    BOTH = 3     // Queue at BEGIN and also after a later pool miss
};

enum class PolarDB_ProxyIdentityMode : uint8_t {
    CLIENT = 0,
    PROXY = 1
};

/**
 * @brief Coherent parsed values that affect PolarDB backend startup packets.
 *
 * The thread handler owns the canonical value. Workers copy it while holding
 * the existing thread-variable lock and then use their thread-local fields.
 *
 * `generation` is the recycle token for pooled backends. The thread handler
 * bumps it on commit only when polardb_same_startup_config_inputs() reports that
 * the startup-affecting inputs changed, so an unchanged commit leaves pooled
 * connections usable. Any pooled connection whose stored
 * polardb_startup_config_generation differs from the current one was opened with
 * different startup parameters and must be rejected for reuse rather than reset.
 */
struct PolarDB_StartupConfigValue {
    uint64_t generation{1};
    PolarDB_ProxyProtocol proxy_protocol{PolarDB_ProxyProtocol::V15};
    PolarDB_ProxyIdentityMode identity_mode{PolarDB_ProxyIdentityMode::PROXY};
    PolarDB_StartupIdentity configured_identity;
};

static inline int polardb_proxy_protocol_from_string(
    const char* value,
    int default_value);
static inline int polardb_missing_lsn_action_from_string(
    const char* value,
    int default_value);
static inline int polardb_lsn_wait_timeout_action_from_string(
    const char* value,
    int default_value);
static inline int polardb_replica_loss_action_from_string(
    const char* value,
    int default_value);
static inline int polardb_replica_error_action_from_string(
    const char* value,
    int default_value);

struct PolarDB_ProfileDefinition {
    PolarDB_Profile profile;
    const char* name;
    PolarDB_ConsistencyMode consistency;
    PolarDB_ReadTarget read_target;
    PolarDB_ReadFallbackAction read_fallback;
    PolarDB_MissingLsnAction missing_lsn;
    PolarDB_LsnWaitTimeoutAction lsn_timeout;
    PolarDB_ReplicaErrorAction replica_error;
    PolarDB_ReplicaLossAction replica_loss;
    PolarDB_ProxyProtocol rfq_protocol;
    bool monitor_lsn_updates;
    bool split_warmup;
};

inline constexpr PolarDB_ProfileDefinition
POLARDB_DEFAULT_PROFILE_DEFINITION{
    PolarDB_Profile::SESSION_FALLBACK,
    "session_fallback",
    PolarDB_ConsistencyMode::SESSION_LSN,
    PolarDB_ReadTarget::REPLICA,
    PolarDB_ReadFallbackAction::PRIMARY,
    PolarDB_MissingLsnAction::PRIMARY,
    PolarDB_LsnWaitTimeoutAction::PRIMARY,
    PolarDB_ReplicaErrorAction::PRIMARY,
    PolarDB_ReplicaLossAction::REPLICA_THEN_PRIMARY,
    PolarDB_ProxyProtocol::V15,
    true,
    true
};

/**
 * @brief Committed snapshot of the global PolarDB configuration.
 *
 * `consistency_mode` is stored as an integer because configuration uses -1 as
 * an inheritance marker. Convert a resolved value with
 * polardb_consistency_from_int() before comparing it with the enum.
 *
 * The snapshot must be read under the thread-variable lock.
 * get_polardb_global_config() takes that lock and returns a copy;
 * get_polardb_global_config_unlocked() does not, so its caller must already hold the
 * lock and must not retain a reference past it.
 */
struct PolarDB_ParsedGlobalConfigValue {
    int profile{
        static_cast<int>(POLARDB_DEFAULT_PROFILE_DEFINITION.profile)};
    int consistency_mode{
        static_cast<int>(POLARDB_DEFAULT_PROFILE_DEFINITION.consistency)};
    int read_target{
        static_cast<int>(POLARDB_DEFAULT_PROFILE_DEFINITION.read_target)};
    int read_fallback_action{
        static_cast<int>(POLARDB_DEFAULT_PROFILE_DEFINITION.read_fallback)};
    int missing_lsn_action{
        static_cast<int>(POLARDB_DEFAULT_PROFILE_DEFINITION.missing_lsn)};
    int lsn_wait_timeout_action{
        static_cast<int>(POLARDB_DEFAULT_PROFILE_DEFINITION.lsn_timeout)};
    int replica_loss_action{
        static_cast<int>(POLARDB_DEFAULT_PROFILE_DEFINITION.replica_loss)};
    int replica_error_action{
        static_cast<int>(POLARDB_DEFAULT_PROFILE_DEFINITION.replica_error)};
    int lsn_wait_timeout_ms{1000};
    int split_warmup_max_connections_per_request{1};
    bool monitor_lsn_updates{
        POLARDB_DEFAULT_PROFILE_DEFINITION.monitor_lsn_updates};
    bool split_warmup{POLARDB_DEFAULT_PROFILE_DEFINITION.split_warmup};
    PolarDB_StartupConfigValue startup;
};

/**
 * @brief Complete policy presets.
 *
 * This is the single definition of profile behavior. Parsing, display, runtime
 * expansion and tests all use this table. Profiles deliberately leave topology,
 * lag/capacity limits, startup identity and generic I/O tuning unchanged.
 */
static inline const std::array<PolarDB_ProfileDefinition, 7>&
polardb_profile_definitions() {
    using P = PolarDB_Profile;
    using C = PolarDB_ConsistencyMode;
    using T = PolarDB_ReadTarget;
    using F = PolarDB_ReadFallbackAction;
    using M = PolarDB_MissingLsnAction;
    using W = PolarDB_LsnWaitTimeoutAction;
    using E = PolarDB_ReplicaErrorAction;
    using L = PolarDB_ReplicaLossAction;
    using R = PolarDB_ProxyProtocol;
    static const std::array<PolarDB_ProfileDefinition, 7> definitions{{
        {P::OFF, "off", C::OFF, T::PRIMARY, F::PRIMARY,
            M::PRIMARY, W::PRIMARY, E::PRIMARY, L::REPLICA_THEN_PRIMARY,
            R::OFF, false, false},
        {P::EVENTUAL, "eventual", C::EVENTUAL, T::REPLICA, F::PRIMARY,
            M::PRIMARY, W::PRIMARY, E::PRIMARY, L::REPLICA_THEN_PRIMARY,
            R::OFF, false, false},
        {P::SESSION_WARNING, "session_warning", C::SESSION_LSN, T::REPLICA, F::PRIMARY,
            M::WARNING, W::WARNING, E::PRIMARY, L::REPLICA_THEN_PRIMARY,
            R::V15, true, true},
        POLARDB_DEFAULT_PROFILE_DEFINITION,
        {P::SESSION_ERROR, "session_error", C::SESSION_LSN, T::REPLICA, F::ERROR,
            M::ERROR, W::ERROR, E::ERROR, L::REPLICA_THEN_ERROR,
            R::V15, true, true},
        {P::GLOBAL_FALLBACK, "global_fallback", C::GLOBAL_LSN, T::REPLICA, F::PRIMARY,
            M::PRIMARY, W::PRIMARY, E::PRIMARY, L::REPLICA_THEN_PRIMARY,
            R::V15, true, true},
        {P::GLOBAL_ERROR, "global_error", C::GLOBAL_LSN, T::REPLICA, F::ERROR,
            M::ERROR, W::ERROR, E::ERROR, L::REPLICA_THEN_ERROR,
            R::V15, true, true}
    }};
    return definitions;
}

static inline int polardb_profile_from_string(
        const char* value, int default_value) {
    if (!value || value[0] == '\0' || strcasecmp(value, "default") == 0) {
        return default_value;
    }
    if (strcasecmp(value, "custom") == 0) {
        return static_cast<int>(PolarDB_Profile::CUSTOM);
    }
    for (const PolarDB_ProfileDefinition& definition :
            polardb_profile_definitions()) {
        if (strcasecmp(value, definition.name) == 0) {
            return static_cast<int>(definition.profile);
        }
    }
    return default_value;
}

static inline PolarDB_Profile polardb_profile_from_int(int value) {
    if (value == static_cast<int>(PolarDB_Profile::CUSTOM)) {
        return PolarDB_Profile::CUSTOM;
    }
    for (const PolarDB_ProfileDefinition& definition :
            polardb_profile_definitions()) {
        if (value == static_cast<int>(definition.profile)) {
            return definition.profile;
        }
    }
    return PolarDB_Profile::CUSTOM;
}

static inline const char* polardb_profile_name(PolarDB_Profile profile) {
    for (const PolarDB_ProfileDefinition& definition :
            polardb_profile_definitions()) {
        if (profile == definition.profile) {
            return definition.name;
        }
    }
    return "custom";
}

static inline bool polardb_profile_is_named(PolarDB_Profile profile) {
    return profile != PolarDB_Profile::CUSTOM;
}

static inline const PolarDB_ProfileDefinition*
polardb_profile_definition(PolarDB_Profile profile) {
    for (const PolarDB_ProfileDefinition& definition :
            polardb_profile_definitions()) {
        if (definition.profile == profile) {
            return &definition;
        }
    }
    return nullptr;
}

static inline bool polardb_apply_profile(
        PolarDB_Profile profile,
        PolarDB_ParsedGlobalConfigValue* config) {
    if (!config) {
        return false;
    }
    const PolarDB_ProfileDefinition* definition =
        polardb_profile_definition(profile);
    if (!definition) {
        return false;
    }
    config->profile = static_cast<int>(profile);
    config->consistency_mode = static_cast<int>(definition->consistency);
    config->read_target = static_cast<int>(definition->read_target);
    config->read_fallback_action =
        static_cast<int>(definition->read_fallback);
    config->missing_lsn_action =
        static_cast<int>(definition->missing_lsn);
    config->lsn_wait_timeout_action =
        static_cast<int>(definition->lsn_timeout);
    config->replica_error_action =
        static_cast<int>(definition->replica_error);
    config->replica_loss_action =
        static_cast<int>(definition->replica_loss);
    config->monitor_lsn_updates = definition->monitor_lsn_updates;
    config->split_warmup = definition->split_warmup;
    config->startup.proxy_protocol = definition->rfq_protocol;
    return true;
}

static inline bool polardb_same_profile_owned_settings(
        const PolarDB_ParsedGlobalConfigValue& lhs,
        const PolarDB_ParsedGlobalConfigValue& rhs) {
    return lhs.consistency_mode == rhs.consistency_mode &&
        lhs.read_target == rhs.read_target &&
        lhs.read_fallback_action == rhs.read_fallback_action &&
        lhs.missing_lsn_action == rhs.missing_lsn_action &&
        lhs.lsn_wait_timeout_action == rhs.lsn_wait_timeout_action &&
        lhs.replica_error_action == rhs.replica_error_action &&
        lhs.replica_loss_action == rhs.replica_loss_action &&
        lhs.monitor_lsn_updates == rhs.monitor_lsn_updates &&
        lhs.split_warmup == rhs.split_warmup &&
        lhs.startup.proxy_protocol == rhs.startup.proxy_protocol;
}

static inline bool polardb_config_matches_profile(
        const PolarDB_ParsedGlobalConfigValue& config,
        PolarDB_Profile profile) {
    PolarDB_ParsedGlobalConfigValue expected = config;
    return polardb_apply_profile(profile, &expected) &&
        polardb_same_profile_owned_settings(config, expected);
}

static inline const char* polardb_missing_lsn_action_name(
        PolarDB_MissingLsnAction action) {
    switch (action) {
    case PolarDB_MissingLsnAction::PRIMARY:
        return "primary";
    case PolarDB_MissingLsnAction::WARNING:
        return "warning";
    case PolarDB_MissingLsnAction::ERROR:
        return "error";
    }
    return "unknown";
}

static inline const char* polardb_replica_error_action_name(
        PolarDB_ReplicaErrorAction action) {
    switch (action) {
    case PolarDB_ReplicaErrorAction::PRIMARY:
        return "primary";
    case PolarDB_ReplicaErrorAction::ERROR:
        return "error";
    case PolarDB_ReplicaErrorAction::DISCONNECT:
        return "disconnect";
    }
    return "unknown";
}

static inline const char* polardb_replica_loss_action_name(
        PolarDB_ReplicaLossAction action) {
    switch (action) {
    case PolarDB_ReplicaLossAction::REPLICA_THEN_PRIMARY:
        return "replica_then_primary";
    case PolarDB_ReplicaLossAction::REPLICA_THEN_ERROR:
        return "replica_then_error";
    case PolarDB_ReplicaLossAction::PRIMARY:
        return "primary";
    case PolarDB_ReplicaLossAction::ERROR:
        return "error";
    case PolarDB_ReplicaLossAction::DISCONNECT:
        return "disconnect";
    }
    return "unknown";
}

enum class PolarDB_ProfileSettingKey : uint8_t {
    PROFILE = 0,
    CONSISTENCY_MODE,
    READ_TARGET,
    READ_FALLBACK,
    MISSING_LSN,
    LSN_TIMEOUT,
    REPLICA_ERROR,
    REPLICA_LOSS,
    PROXY_PROTOCOL,
    MONITOR_LSN_UPDATES,
    SPLIT_WARMUP
};

struct PolarDB_ProfileSettingDefinition {
    PolarDB_ProfileSettingKey key;
    const char* name;
    const char* allowed_values;
};

static inline const std::array<PolarDB_ProfileSettingDefinition, 11>&
polardb_profile_settings() {
    using K = PolarDB_ProfileSettingKey;
    static const std::array<PolarDB_ProfileSettingDefinition, 11> settings{{
        {K::PROFILE, "polardb_profile",
            "off, eventual, session_warning, session_fallback, "
            "session_error, global_fallback, global_error, custom"},
        {K::CONSISTENCY_MODE, "polardb_consistency_mode",
            "off, eventual, session_lsn, global_lsn"},
        {K::READ_TARGET, "polardb_read_target", "primary, replica"},
        {K::READ_FALLBACK, "polardb_action_read_fallback",
            "primary, error"},
        {K::MISSING_LSN, "polardb_action_missing_lsn",
            "primary, warning, error"},
        {K::LSN_TIMEOUT, "polardb_action_lsn_timeout",
            "warning, primary, error, disconnect"},
        {K::REPLICA_ERROR, "polardb_action_replica_error",
            "primary, error, disconnect"},
        {K::REPLICA_LOSS, "polardb_action_replica_loss",
            "replica_then_primary, replica_then_error, primary, "
            "error, disconnect"},
        {K::PROXY_PROTOCOL, "polardb_proxy_protocol", "v15, legacy, off"},
        {K::MONITOR_LSN_UPDATES, "polardb_monitor_lsn_updates",
            "true, false"},
        {K::SPLIT_WARMUP, "polardb_lazy_warmup_split", "true, false"}
    }};
    return settings;
}

static inline const PolarDB_ProfileSettingDefinition*
polardb_find_profile_setting(const char* name) {
    if (!name) {
        return nullptr;
    }
    for (const PolarDB_ProfileSettingDefinition& setting :
            polardb_profile_settings()) {
        if (strcasecmp(name, setting.name) == 0) {
            return &setting;
        }
    }
    return nullptr;
}

static inline bool polardb_profile_owns_setting(const char* name) {
    return polardb_find_profile_setting(name) != nullptr;
}

static inline const char* polardb_profile_setting_allowed_values(
        const char* name) {
    const PolarDB_ProfileSettingDefinition* setting =
        polardb_find_profile_setting(name);
    return setting ? setting->allowed_values : "";
}

enum class PolarDB_ProfileSettingResult : uint8_t {
    NOT_PROFILE_SETTING = 0,
    UPDATED,
    INVALID
};

static inline bool polardb_parse_profile_bool(
        const char* value, bool* parsed) {
    if (!value || !parsed) {
        return false;
    }
    if (strcasecmp(value, "true") == 0 || strcmp(value, "1") == 0) {
        *parsed = true;
        return true;
    }
    if (strcasecmp(value, "false") == 0 || strcmp(value, "0") == 0) {
        *parsed = false;
        return true;
    }
    return false;
}

/**
 * @brief Parse one profile-owned setting into a complete candidate bundle.
 *
 * The caller decides whether an individual change turns a named profile into
 * CUSTOM. This helper only parses canonical public values and updates the
 * candidate. A named profile expands immediately from the single profile table.
 */
static inline PolarDB_ProfileSettingResult
polardb_update_profile_setting(
        PolarDB_ParsedGlobalConfigValue* config,
        const char* name,
        const char* value) {
    const PolarDB_ProfileSettingDefinition* setting =
        polardb_find_profile_setting(name);
    if (!config || !setting) {
        return PolarDB_ProfileSettingResult::NOT_PROFILE_SETTING;
    }
    int parsed = -1;
    switch (setting->key) {
    case PolarDB_ProfileSettingKey::PROFILE: {
        parsed = polardb_profile_from_string(value, -1);
        if (parsed < 0) {
            return PolarDB_ProfileSettingResult::INVALID;
        }
        const PolarDB_Profile profile = polardb_profile_from_int(parsed);
        if (profile == PolarDB_Profile::CUSTOM) {
            config->profile = parsed;
            return PolarDB_ProfileSettingResult::UPDATED;
        }
        return polardb_apply_profile(profile, config)
            ? PolarDB_ProfileSettingResult::UPDATED
            : PolarDB_ProfileSettingResult::INVALID;
    }
    case PolarDB_ProfileSettingKey::CONSISTENCY_MODE:
        parsed = polardb_consistency_mode_from_string(value, -1);
        if (parsed >= 0) config->consistency_mode = parsed;
        break;
    case PolarDB_ProfileSettingKey::READ_TARGET:
        parsed = polardb_read_target_from_string(value, -1);
        if (parsed >= 0) config->read_target = parsed;
        break;
    case PolarDB_ProfileSettingKey::READ_FALLBACK:
        parsed = polardb_read_fallback_action_from_string(value, -1);
        if (parsed >= 0) config->read_fallback_action = parsed;
        break;
    case PolarDB_ProfileSettingKey::MISSING_LSN:
        parsed = polardb_missing_lsn_action_from_string(value, -1);
        if (parsed >= 0) config->missing_lsn_action = parsed;
        break;
    case PolarDB_ProfileSettingKey::LSN_TIMEOUT:
        parsed = polardb_lsn_wait_timeout_action_from_string(value, -1);
        if (parsed >= 0) config->lsn_wait_timeout_action = parsed;
        break;
    case PolarDB_ProfileSettingKey::REPLICA_ERROR:
        parsed = polardb_replica_error_action_from_string(value, -1);
        if (parsed >= 0) config->replica_error_action = parsed;
        break;
    case PolarDB_ProfileSettingKey::REPLICA_LOSS:
        parsed = polardb_replica_loss_action_from_string(value, -1);
        if (parsed >= 0) config->replica_loss_action = parsed;
        break;
    case PolarDB_ProfileSettingKey::PROXY_PROTOCOL:
        parsed = polardb_proxy_protocol_from_string(value, -1);
        if (parsed >= 0) {
            config->startup.proxy_protocol =
                polardb_proxy_protocol_from_int(parsed);
        }
        break;
    case PolarDB_ProfileSettingKey::MONITOR_LSN_UPDATES: {
        bool enabled = false;
        if (polardb_parse_profile_bool(value, &enabled)) {
            config->monitor_lsn_updates = enabled;
            parsed = enabled ? 1 : 0;
        }
        break;
    }
    case PolarDB_ProfileSettingKey::SPLIT_WARMUP: {
        bool enabled = false;
        if (polardb_parse_profile_bool(value, &enabled)) {
            config->split_warmup = enabled;
            parsed = enabled ? 1 : 0;
        }
        break;
    }
    }
    return parsed >= 0
        ? PolarDB_ProfileSettingResult::UPDATED
        : PolarDB_ProfileSettingResult::INVALID;
}

/// @brief Return the reason a complete global policy is invalid, or nullptr.
static inline const char* polardb_global_policy_error(
        const PolarDB_ParsedGlobalConfigValue& config) {
    const PolarDB_ConsistencyMode mode =
        polardb_consistency_from_int(config.consistency_mode);
    if (const char* error = polardb_consistency_policy_error(
            mode,
            polardb_missing_lsn_action_from_int(config.missing_lsn_action),
            polardb_lsn_wait_timeout_action_from_int(
                config.lsn_wait_timeout_action))) {
        return error;
    }
    const PolarDB_Profile profile =
        polardb_profile_from_int(config.profile);
    if (polardb_profile_is_named(profile) &&
            !polardb_config_matches_profile(config, profile)) {
        return "a named PolarDB profile must match its complete policy bundle";
    }
    return nullptr;
}

/**
 * @brief Compare only the inputs that change what a backend startup packet says.
 *
 * Compares the proxy protocol, the identity mode and the configured identity
 * host/port. PolarDB_StartupConfigValue::generation is deliberately excluded:
 * this is the test the caller applies before bumping that generation, so
 * including it would make every comparison unequal. This is not full struct
 * equality.
 *
 * @param lhs  One startup configuration.
 * @param rhs  The other startup configuration.
 * @return true when the startup-affecting inputs are identical; false when the
 *         caller must bump the generation and stop reusing older pooled backends.
 */
static inline bool polardb_same_startup_config_inputs(
        const PolarDB_StartupConfigValue& lhs,
        const PolarDB_StartupConfigValue& rhs) {
    return lhs.proxy_protocol == rhs.proxy_protocol &&
        lhs.identity_mode == rhs.identity_mode &&
        lhs.configured_identity.host == rhs.configured_identity.host &&
        lhs.configured_identity.port == rhs.configured_identity.port;
}

static inline int polardb_proxy_identity_mode_from_string(
        const char* value,
        int default_value) {
    if (!value || value[0] == '\0' || strcasecmp(value, "default") == 0) {
        return default_value;
    }
    if (strcasecmp(value, "client") == 0) {
        return static_cast<int>(PolarDB_ProxyIdentityMode::CLIENT);
    }
    if (strcasecmp(value, "proxy") == 0) {
        return static_cast<int>(PolarDB_ProxyIdentityMode::PROXY);
    }
    return default_value;
}

/**
 * @brief Short stable name for a proxy identity mode, for logs and counters.
 *
 * Unlike the other name helpers in this header, there is no "unknown" label:
 * PROXY renders as "proxy" and every other value, including out-of-range ones,
 * renders as "client". A corrupt or unmapped mode is therefore logged as a valid
 * mode, so do not use this output to validate the input.
 *
 * @param mode  Identity mode as an int.
 * @return "proxy" for PolarDB_ProxyIdentityMode::PROXY, "client" otherwise.
 */
static inline const char* polardb_proxy_identity_mode_name(int mode) {
    switch (mode) {
    case static_cast<int>(PolarDB_ProxyIdentityMode::PROXY):
        return "proxy";
    case static_cast<int>(PolarDB_ProxyIdentityMode::CLIENT):
    default:
        return "client";
    }
}

static inline bool polardb_proxy_identity_mode_uses_client_identity(int mode) {
    return mode == static_cast<int>(PolarDB_ProxyIdentityMode::CLIENT);
}

/**
 * @brief Hash a startup client context into a pool identity key.
 *
 * The result is never 0: a hash that lands on zero is returned as 1, so callers
 * may reserve 0 to mean "no key computed yet".
 *
 * The hash must cover exactly the fields that
 * PolarDB_StartupClientContext::compatible_for_reuse() compares —
 * identity source/host/port, the SSL flag/version/cipher, and the proxy session
 * id and cancel key. The SSL and proxy-session fields are reserved placeholders
 * and are still hashed, so that the key and the comparison stay in agreement.
 * Keep the two in sync: a field added to one must be added to the other, or two
 * incompatible connections will share a key.
 *
 * @param startup_client  Startup context to hash.
 * @return A non-zero FNV-1a key.
 */
static inline uint64_t polardb_startup_client_reuse_key(
        const PolarDB_StartupClientContext& startup_client) {
    uint64_t hash = 1469598103934665603ULL;
    hash = polardb_pool_hash_string(hash, startup_client.identity.host);
    hash = polardb_pool_hash_i32(
        hash, static_cast<int>(startup_client.identity.source));
    hash = polardb_pool_hash_i32(hash, startup_client.identity.port);
    hash = polardb_pool_hash_u64(hash, startup_client.frontend_ssl ? 1 : 0);
    hash = polardb_pool_hash_string(hash, startup_client.ssl_version);
    hash = polardb_pool_hash_string(hash, startup_client.ssl_cipher);
    hash = polardb_pool_hash_u64(hash, startup_client.has_proxy_session ? 1 : 0);
    hash = polardb_pool_hash_u64(hash, startup_client.proxy_session_id);
    hash = polardb_pool_hash_u64(hash, startup_client.proxy_cancel_key);
    return hash ? hash : 1;
}

static inline bool polardb_startup_client_compatible_for_warmup(
        const PolarDB_StartupClientContext& pooled,
        const PolarDB_StartupClientContext& requested) {
    return pooled.compatible_for_reuse(requested);
}

static inline int polardb_txn_split_warmup_mode_from_string(
    const char* value,
    int default_value) {
    if (!value || value[0] == '\0' || strcasecmp(value, "default") == 0) {
        return default_value;
    }
    if (strcasecmp(value, "off") == 0) {
        return static_cast<int>(PolarDB_TxnSplitWarmupMode::OFF);
    }
    if (strcasecmp(value, "demand") == 0 || strcasecmp(value, "lazy") == 0) {
        return static_cast<int>(PolarDB_TxnSplitWarmupMode::DEMAND);
    }
    if (strcasecmp(value, "begin") == 0 ||
            strcasecmp(value, "transaction_begin") == 0) {
        return static_cast<int>(PolarDB_TxnSplitWarmupMode::BEGIN);
    }
    if (strcasecmp(value, "both") == 0) {
        return static_cast<int>(PolarDB_TxnSplitWarmupMode::BOTH);
    }
    return default_value;
}

static inline const char* polardb_txn_split_warmup_mode_name(int mode) {
    switch (mode) {
    case static_cast<int>(PolarDB_TxnSplitWarmupMode::OFF):
        return "off";
    case static_cast<int>(PolarDB_TxnSplitWarmupMode::DEMAND):
        return "demand";
    case static_cast<int>(PolarDB_TxnSplitWarmupMode::BEGIN):
        return "begin";
    case static_cast<int>(PolarDB_TxnSplitWarmupMode::BOTH):
        return "both";
    default:
        return "default";
    }
}

/// @brief Map a config string ("off"/"legacy"/"v15") to the proxy-protocol int.
/// Null, empty, "default", or any unknown value returns @p default_value.
static inline int polardb_proxy_protocol_from_string(
    const char* value,
    int default_value) {
    if (!value || value[0] == '\0' || strcasecmp(value, "default") == 0) {
        return default_value;
    }
    if (strcasecmp(value, "off") == 0) {
        return static_cast<int>(PolarDB_ProxyProtocol::OFF);
    }
    if (strcasecmp(value, "legacy") == 0) {
        return static_cast<int>(PolarDB_ProxyProtocol::LEGACY);
    }
    if (strcasecmp(value, "v15") == 0) {
        return static_cast<int>(PolarDB_ProxyProtocol::V15);
    }
    return default_value;
}

/**
 * @brief Common words shared by the event-specific failure settings.
 *
 * The settings keep separate typed enums because a missing LSN, a wait timeout,
 * and a lost connection have different legal outcomes. This parser owns the
 * four outcome words they share so their public spelling cannot drift.
 */
enum class PolarDB_CommonFailureOutcome : int8_t {
    UNRECOGNIZED = -1,
    PRIMARY = 0,
    WARNING = 1,
    ERROR = 2,
    DISCONNECT = 3
};

static inline PolarDB_CommonFailureOutcome
polardb_common_failure_outcome_from_string(const char* value) {
    if (!value) {
        return PolarDB_CommonFailureOutcome::UNRECOGNIZED;
    }
    if (strcasecmp(value, "primary") == 0) {
        return PolarDB_CommonFailureOutcome::PRIMARY;
    }
    if (strcasecmp(value, "warning") == 0) {
        return PolarDB_CommonFailureOutcome::WARNING;
    }
    if (strcasecmp(value, "error") == 0) {
        return PolarDB_CommonFailureOutcome::ERROR;
    }
    if (strcasecmp(value, "disconnect") == 0) {
        return PolarDB_CommonFailureOutcome::DISCONNECT;
    }
    return PolarDB_CommonFailureOutcome::UNRECOGNIZED;
}

/// @brief Parse primary, warning or error.
static inline int polardb_missing_lsn_action_from_string(
    const char* value,
    int default_value) {
    if (!value) {
        return default_value;
    }
    switch (polardb_common_failure_outcome_from_string(value)) {
    case PolarDB_CommonFailureOutcome::PRIMARY:
        return static_cast<int>(PolarDB_MissingLsnAction::PRIMARY);
    case PolarDB_CommonFailureOutcome::WARNING:
        return static_cast<int>(
            PolarDB_MissingLsnAction::WARNING);
    case PolarDB_CommonFailureOutcome::ERROR:
        return static_cast<int>(PolarDB_MissingLsnAction::ERROR);
    default:
        return default_value;
    }
}

/// @brief Parse the complete outcome of an expired LSN wait.
static inline int polardb_lsn_wait_timeout_action_from_string(
        const char* value,
        int default_value) {
    if (!value) {
        return default_value;
    }
    switch (polardb_common_failure_outcome_from_string(value)) {
    case PolarDB_CommonFailureOutcome::WARNING:
        return static_cast<int>(
            PolarDB_LsnWaitTimeoutAction::WARNING);
    case PolarDB_CommonFailureOutcome::PRIMARY:
        return static_cast<int>(PolarDB_LsnWaitTimeoutAction::PRIMARY);
    case PolarDB_CommonFailureOutcome::ERROR:
        return static_cast<int>(PolarDB_LsnWaitTimeoutAction::ERROR);
    case PolarDB_CommonFailureOutcome::DISCONNECT:
        return static_cast<int>(
            PolarDB_LsnWaitTimeoutAction::DISCONNECT);
    default:
        return default_value;
    }
}

/// @brief Parse the recovery action for a lost replica connection.
static inline int polardb_replica_loss_action_from_string(
        const char* value,
        int default_value) {
    if (!value) {
        return default_value;
    }
    if (strcasecmp(value, "replica_then_primary") == 0) {
        return static_cast<int>(
            PolarDB_ReplicaLossAction::REPLICA_THEN_PRIMARY);
    }
    if (strcasecmp(value, "replica_then_error") == 0) {
        return static_cast<int>(
            PolarDB_ReplicaLossAction::REPLICA_THEN_ERROR);
    }
    switch (polardb_common_failure_outcome_from_string(value)) {
    case PolarDB_CommonFailureOutcome::PRIMARY:
        return static_cast<int>(
            PolarDB_ReplicaLossAction::PRIMARY);
    case PolarDB_CommonFailureOutcome::ERROR:
        return static_cast<int>(
            PolarDB_ReplicaLossAction::ERROR);
    case PolarDB_CommonFailureOutcome::DISCONNECT:
        return static_cast<int>(
            PolarDB_ReplicaLossAction::DISCONNECT);
    default:
        return default_value;
    }
}

/// @brief Parse the recovery action for a reusable replica error.
static inline int polardb_replica_error_action_from_string(
        const char* value,
        int default_value) {
    if (!value) {
        return default_value;
    }
    switch (polardb_common_failure_outcome_from_string(value)) {
    case PolarDB_CommonFailureOutcome::PRIMARY:
        return static_cast<int>(PolarDB_ReplicaErrorAction::PRIMARY);
    case PolarDB_CommonFailureOutcome::ERROR:
        return static_cast<int>(PolarDB_ReplicaErrorAction::ERROR);
    case PolarDB_CommonFailureOutcome::DISCONNECT:
        return static_cast<int>(
            PolarDB_ReplicaErrorAction::DISCONNECT);
    default:
        return default_value;
    }
}

/**
 * @brief Consistency-policy routing suggestion for one query.
 *
 * This is not the final routing decision. It is the recommendation produced by
 * the consistency wait planner and consumed by PolarDB_Query_RoutePlan. Final
 * routing still belongs to polardb_plan(), which also applies query-rule,
 * transaction, protocol form, reader availability, and lag/freshness checks.
 *
 * Values:
 *  - NONE: the helper has no reader recommendation.
 *  - REPLICA: the helper allows a reader if the planner's later checks allow
 *    it. This is informational; placement still makes the final decision.
 */
enum class PolarDB_Query_ConsistencyRouteHint : uint8_t {
    NONE = 0,
    REPLICA = 1
};

/**
 * @brief Tracks whether a consistency wait is currently in flight.
 */
enum class PolarDB_WaitStage : uint8_t {
    IDLE = 0,     // No wait active
    WAITING = 1   // Wait prefix staged; filtering SET results before the user result
};

/**
 * @brief Outcome of finalizing the wait wrapper before backend dispatch.
 *
 * CONTINUE means the query is safe to run: either no wait wrapper was needed,
 * or the wrapper was successfully attached. FAILED means the planner chose a
 * replica-with-wait route but the wrapper could not be attached, so the caller
 * must not run the original query unwrapped on the replica.
 */
enum class PolarDB_WrapFinalizeResult : uint8_t {
    CONTINUE = 0,
    FAILED = 1
};

/**
 * @brief Result of a PolarDB health check (node_type + availability + LSN).
 *
 * Populated from POLARDB_CHECK_WITH_LSN_QUERY:
 * - node_type    : polar_node_type()              (col 0)
 * - is_available : polar_is_available()           (col 1); false in maintenance mode
 * - current_lsn  : pg_current_wal_lsn / replay_lsn (col 2)
 *
 * @note A standard PostgreSQL backend (no PolarDB) has no polar_is_available()
 * maintenance concept, so the availability column is absent and is_available
 * defaults to true (parse_is_available(nullptr) returns true).
 */
struct PolarDB_HealthCheck {
    PolarDB_NodeType node_type{PolarDB_NodeType::UNKNOWN};
    bool is_available{true};      ///< false = node in maintenance mode
    bool role_valid{false};
    bool availability_valid{false};
    bool lsn_valid{false};
    uint64_t current_lsn{0};      ///< WAL position from the checked node
};

/**
 * @brief Whether a backend belongs to the configured writer hostgroup.
 *
 * This checks hostgroup identity only. It does not verify an RFQ payload, LSN,
 * server role, or writer epoch.
 */
static inline bool polardb_backend_is_writer_hostgroup(
    bool is_polardb_hostgroup,
    int backend_hostgroup,
    int writer_hostgroup) {
    return is_polardb_hostgroup &&
        backend_hostgroup >= 0 &&
        writer_hostgroup >= 0 &&
        backend_hostgroup == writer_hostgroup;
}

/**
 * @brief Identity of the current writer: hostgroup plus a monotonic epoch (Scope).
 *
 * The epoch counts changes of the writer hostgroup's topology (for example a
 * failover). Stored LSN state is only valid while it matches the writer scope it
 * was recorded under; a scope mismatch means the topology moved and the old LSN
 * must be discarded. The epoch is meaningful only together with its hostgroup,
 * because each writer hostgroup numbers its own epochs independently.
 */
struct PolarDB_WriterScope {
    int hg = -1;
    uint64_t epoch = 0;

    /// @brief A scope is valid once it names a real writer hostgroup (hg >= 0).
    bool valid() const { return hg >= 0; }
    void reset() { hg = -1; epoch = 0; }

    /**
     * @brief Whether two writer hostgroup/epoch scopes identify the same writer.
     *
     * Epoch values are scoped by writer hostgroup. Matching only the scalar epoch
     * can accept state from a different PolarDB group whose epoch number happens
     * to be the same.
     */
    bool matches(const PolarDB_WriterScope& current) const {
        return valid() &&
            current.valid() &&
            hg == current.hg &&
            epoch == current.epoch;
    }
};

/**
 * @brief Per-session PolarDB consistency state.
 *
 * The write component tracks this session's positioned writes. The observed
 * component tracks every positioned result this client has seen. SESSION_LSN
 * reads wait on max(write_lsn, observed_lsn), so a later read never goes behind
 * either its own writes or a fresher replica result it already observed.
 *
 * Missing-LSN sticky flags are event-attributed: a write result without RFQ LSN sets
 * write_unknown, while a tracked read without RFQ LSN sets observed_unknown.
 * The two flags drive the same policy but keep operator diagnostics distinct.
 */
struct PolarDB_SessionConsistency {
    uint64_t write_lsn = 0;
    uint64_t observed_lsn = 0;
#if POLARDB_PROFILE
    uintptr_t observed_lsn_source_server_token = 0;
#endif // POLARDB_PROFILE
    bool write_unknown = false;
    bool observed_unknown = false;
    PolarDB_WriterScope writer_scope;

    bool has_lsn_state() const {
        return write_lsn != 0 ||
            observed_lsn != 0 ||
            write_unknown ||
            observed_unknown;
    }

    // A new session has no target. Its first reader RFQ establishes
    // observed_lsn, after which every SESSION_LSN read is monotonic.
    uint64_t target() const {
        return write_lsn > observed_lsn ? write_lsn : observed_lsn;
    }

    /**
     * @brief GLOBAL_LSN target for a committed-state read.
     *
     * The caller must provide the current writer-scope group LSN observation. A
     * missing observation is unknown rather than "no wait": global consistency
     * cannot be confirmed without a shared target. When present, wait on the max
     * of this session's own monotonic target and the group observation.
     */
    uint64_t target_with_global_lsn(
        uint64_t group_lsn,
        bool* group_lsn_unknown) const {
        return polardb_target_with_global_lsn(
            target(),
            group_lsn,
            group_lsn_unknown);
    }

    /**
     * @brief Drop this session's LSN state without dropping its writer binding.
     *
     * Clears both LSN values and both missing-LSN sticky flags. `writer_scope` is
     * deliberately preserved: this is not a full reset. The caller re-stamps the
     * scope for the new topology epoch immediately afterwards, so clearing it
     * here would lose epoch attribution, while treating this as a full reset
     * would leave the session bound to a stale writer scope with no LSN state.
     */
    void reset_lsn_state() {
        write_lsn = 0;
        observed_lsn = 0;
#if POLARDB_PROFILE
        observed_lsn_source_server_token = 0;
#endif // POLARDB_PROFILE
        write_unknown = false;
        observed_unknown = false;
    }
};

struct PolarDB_ClientRfqDecision {
    bool include_lsn = false;
    uint64_t lsn = 0;
    bool raised_to_target = false;
    bool raised_by_writer = false;
    bool raised_by_wait = false;
};

/**
 * @brief Select the LSN value ProxySQL can expose in a client ReadyForQuery.
 *
 * The backend RFQ payload is the default. Read-only transaction state may instead
 * preserve the saved session target because the pooled writer connection's RFQ can
 * describe unrelated work. ProxySQL may raise the selected value only when it has
 * an independent confirmation that this response satisfied a higher frontend-session
 * target:
 *   - writer response: the query ran on the current writer hostgroup, so the
 *     result is at least as fresh as this session's already observed/written
 *     target even if that backend connection reports a lower local RFQ value.
 *   - confirmed read target: ProxySQL either completed the LSN wait or selected
 *     a backend already known to have reached that target.
 *
 * No-wait replica responses pass through the backend RFQ payload unchanged; this
 * keeps client-side stale detection meaningful for intentionally eventual routes.
 * Missing backend RFQ payloads are not fabricated. Raise attribution always compares
 * the final client value with the raw backend value, not with the selected baseline.
 *
 * When the client did not ask for an LSN, or the backend supplied no payload, the
 * result is default-constructed: include_lsn false, lsn 0, and every attribution
 * flag false. Callers must test include_lsn before emitting anything.
 *
 * Attribution is recorded only when the final value exceeds @p backend_lsn.
 * raised_by_writer is set when the response came from the writer and the session
 * target is exactly the value chosen; raised_by_wait is set when
 * @p completed_wait_target is non-zero and equals the required target that drove
 * the raise. Both may be false on a raise, and both may be true.
 *
 * @param client_requested_lsn    Whether the client asked for an LSN in its RFQ.
 * @param backend_payload_present Whether the backend RFQ carried an LSN payload.
 * @param backend_lsn             Raw LSN from that payload; the attribution baseline.
 * @param session_target          This session's monotonic target.
 * @param session_lsn_is_baseline Whether the saved session target, rather than the
 *                                backend payload, is the correct starting value —
 *                                set for read-only transaction state where the
 *                                pooled writer RFQ can describe unrelated work.
 * @param response_from_writer    Whether the query ran on the current writer
 *                                hostgroup.
 * @param confirmed_read_target   Target ProxySQL independently confirmed the
 *                                responding backend had reached, or 0.
 * @param completed_wait_target   Target of an LSN wait that completed for this
 *                                query, or 0 when no wait completed. Used only to
 *                                set raised_by_wait.
 * @return The decision. Meaningful only when include_lsn is true.
 */
static inline PolarDB_ClientRfqDecision polardb_client_rfq_decision(
        bool client_requested_lsn,
        bool backend_payload_present,
        uint64_t backend_lsn,
        uint64_t session_target,
        bool session_lsn_is_baseline,
        bool response_from_writer,
        uint64_t confirmed_read_target,
        uint64_t completed_wait_target = 0) {
    PolarDB_ClientRfqDecision decision;
    if (!client_requested_lsn || !backend_payload_present) {
        return decision;
    }

    decision.include_lsn = true;
    decision.lsn = session_lsn_is_baseline && response_from_writer &&
            session_target > 0
        ? session_target : backend_lsn;

    uint64_t required_target = 0;
    if (response_from_writer && session_target > required_target) {
        required_target = session_target;
    }
    if (confirmed_read_target > required_target) {
        required_target = confirmed_read_target;
    }

    if (required_target > decision.lsn) {
        decision.lsn = required_target;
    }

    if (decision.lsn > backend_lsn) {
        decision.raised_to_target = true;
        decision.raised_by_writer =
            response_from_writer && session_target == decision.lsn;
        decision.raised_by_wait =
            completed_wait_target > 0 &&
            completed_wait_target == required_target;
    }
    return decision;
}

/**
 * @brief True when RFQ marks an active transaction with no assigned write XID.
 *
 * `x` and `w` name fields of the RFQ payload, not values of the status byte: the
 * backend sends `x` with an empty XID list for the pre-write/read-only state,
 * and `w` reports session WAL that is not yet usable by a replica. Data and
 * sequence WAL allocate an XID. This helper only classifies those already-read
 * fields; callers still validate the writer.
 *
 * All four conditions must hold, not just the XID emptiness: the transaction
 * must be open and not failed, and the caller must have already found the
 * statement splittable.
 *
 * @param transaction_status  ReadyForQuery status byte. Only 'T' qualifies; 'I'
 *                            (idle) and 'E' (failed transaction) return false.
 * @param transaction_xids    RFQ XID list. Must be empty to qualify; a null
 *                            pointer returns false.
 * @param splittable          Caller's splittable decision. Must be true.
 * @param wal_pending         RFQ pending-WAL marker. Must be false.
 * @return true only when all four conditions hold.
 */
inline bool polardb_rfq_is_prewrite_split_candidate(
        char transaction_status, const char* transaction_xids,
        bool splittable, bool wal_pending) {
    return transaction_status == 'T' && splittable && !wal_pending &&
        transaction_xids && transaction_xids[0] == '\0';
}

/**
 * @brief Stage of transaction-split tracking for a client transaction, from no
 *        tracked transaction through an active split read on a replica.
 *
 * This records RFQ transaction evidence and the session-side state machine shape.
 * The planner can recognize split-readable state; execution temporarily uses a
 * replica backend for one split read and then restores the primary backend.
 */
enum class PolarDB_TransactionSplitStage : uint8_t {
    NONE = 0,                   // No split-capable transaction is being tracked
    TXN_ON_PRIMARY = 1,         // Transaction is open on the primary
    TXN_SPLITTABLE = 2,         // Backend RFQ reports the transaction is eligible for replica reads
    TXN_SPLIT_READ_ACTIVE = 3   // One split read is active on a replica
};

/**
 * @brief Session-scoped transaction-split state.
 *
 * Stores RFQ transaction evidence separately from the existing RYW LSN state.
 * Result processing updates it after an accepted primary RFQ. The planner reads
 * it to recognize split-readable transactions, and split execution marks the
 * transient active/blocked states.
 */
struct PolarDB_TransactionSplitState {
    PolarDB_TransactionSplitStage stage = PolarDB_TransactionSplitStage::NONE;
    std::string xids;          // RFQ transaction ID list for future replica import
    uint64_t primary_lsn = 0;  // LSN observed for this transaction's primary side
    bool splittable = false;   // RFQ reports the transaction is eligible for split reads
    bool wal_pending = false;  // RFQ reports WAL is pending; do not split
    bool blocked = false;      // A prior split fault blocks further split attempts
    bool was_splittable = false; // Transaction was split-readable at least once
    bool did_split = false;      // At least one split read completed in this transaction

    bool active() const {
        return stage != PolarDB_TransactionSplitStage::NONE;
    }

    bool has_backend_evidence() const {
        return !xids.empty() || primary_lsn != 0 || splittable || wal_pending;
    }

    /**
     * @brief Fold one primary ReadyForQuery into the transaction-split state
     *        machine.
     *
     * The caller must have already validated that this RFQ came from the current
     * writer scope. This method trusts its arguments and cannot detect an RFQ
     * from a stale primary or from a replica; feeding it one silently authorizes
     * split reads against evidence that does not describe the client's
     * transaction.
     *
     * Branch rules, in the order applied:
     *   - @p split_enabled false, or @p transaction_status 'I': full reset(),
     *     which also clears `blocked`, `was_splittable` and `did_split`;
     *   - any status other than 'T' or 'E': nothing is touched, so an
     *     unrecognised status byte never corrupts the state;
     *   - `primary_lsn` only moves forward — a lower reported LSN is ignored;
     *   - 'E' force-clears `splittable`, because a failed transaction cannot
     *     authorize reads on another backend even when its RFQ still carries the
     *     split marker;
     *   - a `blocked` transaction, and any 'E' status, pins the stage to
     *     TXN_ON_PRIMARY;
     *   - otherwise the stage becomes TXN_SPLITTABLE only when `splittable` is
     *     set, `wal_pending` is clear and the XID list is non-empty; failing that
     *     an existing TXN_SPLITTABLE falls back to TXN_ON_PRIMARY.
     *
     * @param transaction_status  ReadyForQuery status byte ('I', 'T' or 'E').
     * @param rfq_xids            RFQ XID list. A null pointer leaves the stored
     *                            list unchanged rather than clearing it.
     * @param rfq_splittable      RFQ split marker.
     * @param rfq_wal_pending     RFQ pending-WAL marker.
     * @param rfq_primary_lsn     LSN reported for the primary side.
     * @param split_enabled       Whether transaction split is enabled for this
     *                            hostgroup and session.
     */
    void observe_primary_rfq(char transaction_status,
        const char* rfq_xids,
        bool rfq_splittable,
        bool rfq_wal_pending,
        uint64_t rfq_primary_lsn,
        bool split_enabled) {
        if (!split_enabled) {
            reset();
            return;
        }

        if (transaction_status == 'I') {
            reset();
            return;
        }
        if (transaction_status != 'T' && transaction_status != 'E') {
            return;
        }

        if (stage == PolarDB_TransactionSplitStage::NONE) {
            stage = PolarDB_TransactionSplitStage::TXN_ON_PRIMARY;
        }
        if (rfq_primary_lsn > primary_lsn) {
            primary_lsn = rfq_primary_lsn;
        }
        if (rfq_xids) {
            xids = rfq_xids;
        }
        splittable = rfq_splittable;
        wal_pending = rfq_wal_pending;

        // A failed transaction cannot authorize reads on another backend,
        // even if its RFQ still carries an otherwise valid split marker.
        if (transaction_status == 'E') {
            splittable = false;
        }
        if (blocked || transaction_status == 'E') {
            stage = PolarDB_TransactionSplitStage::TXN_ON_PRIMARY;
            return;
        }
        if (splittable && !wal_pending && !xids.empty()) {
            stage = PolarDB_TransactionSplitStage::TXN_SPLITTABLE;
            was_splittable = true;
        } else if (stage == PolarDB_TransactionSplitStage::TXN_SPLITTABLE) {
            stage = PolarDB_TransactionSplitStage::TXN_ON_PRIMARY;
        }
    }

    void reset() {
        stage = PolarDB_TransactionSplitStage::NONE;
        xids.clear();
        primary_lsn = 0;
        splittable = false;
        wal_pending = false;
        blocked = false;
        was_splittable = false;
        did_split = false;
    }
};

/**
 * @brief Immutable transaction-split inputs captured for one routing decision.
 *
 * The session state above owns the RFQ XID string and is mutable across queries.
 * Routing needs only a synchronous view of that string plus the scalar evidence;
 * keeping this separate avoids constructing an unused std::string in every
 * PolarDB_Query_RouteCtx.
 */
struct PolarDB_TransactionSplitSnapshot {
    std::string_view xids;
    uint64_t primary_lsn = 0;
    PolarDB_TransactionSplitStage stage = PolarDB_TransactionSplitStage::NONE;
    bool splittable = false;
    bool wal_pending = false;
    bool blocked = false;
    bool was_splittable = false;
    bool did_split = false;
};

/**
 * @brief Capture the transaction-split evidence one routing decision needs.
 *
 * The returned snapshot does not own its XID list: PolarDB_TransactionSplitSnapshot::xids
 * is a std::string_view aliasing @p state.xids. The snapshot is stored in
 * PolarDB_Query_RouteCtx and propagated into PolarDB_Query_RoutePlan::txn_xids,
 * so the borrow outlives this call and the caller carries the lifetime
 * obligation.
 *
 * The view stays valid only while @p state is alive and unmodified — that is,
 * within one synchronous collect/plan/execute pass. observe_primary_rfq()
 * assigns to `xids` and reset() clears it, so any RFQ processed between capture
 * and use dangles the view, as does holding the snapshot across an async
 * boundary. Copy the string if it must outlive the pass.
 *
 * @param state  Session split state to read. Must outlive every use of the result.
 * @return A snapshot whose scalars are copies and whose XID list is a borrowed view.
 */
static inline PolarDB_TransactionSplitSnapshot
polardb_transaction_split_snapshot(const PolarDB_TransactionSplitState& state) {
    PolarDB_TransactionSplitSnapshot snapshot;
    snapshot.xids = state.xids;
    snapshot.primary_lsn = state.primary_lsn;
    snapshot.stage = state.stage;
    snapshot.splittable = state.splittable;
    snapshot.wal_pending = state.wal_pending;
    snapshot.blocked = state.blocked;
    snapshot.was_splittable = state.was_splittable;
    snapshot.did_split = state.did_split;
    return snapshot;
}

/**
 * @brief Immutable wait payload for one routed query (Spec: payload only).
 *
 * Routing is selected by PolarDB_Query_RoutePlan. This object describes only the
 * consistency-wait payload that the plan carries into PolarDB_Query_WaitState
 * before dispatch, and that is later rendered as wrapper SET statements.
 *
 * Today the only wait kind is an LSN target, rendered as
 * SET polar_xact_split_wait_lsn. Keeping a separate wait-spec object is
 * intentional: when CSN support is added later, the same boundary can carry the
 * CSN wait target and mode without changing the query route plan or the
 * connection-level wrapper consumer (PolarDB_Query_WrapState).
 *
 * Transaction split is not modeled by this structure. Split also needs XID
 * propagation and its own wrapper payload, but it can reuse
 * PolarDB_Query_WrapState for consuming wrapper statement result sets.
 */
struct PolarDB_WaitSpec {
    PolarDB_WaitType type = PolarDB_WaitType::NONE; // LSN target, or NONE
    uint64_t target = 0;                            // the LSN to wait for (0 = none); widens to a CSN target later
    uint32_t timeout_ms = POLARDB_DEFAULT_WAIT_TIMEOUT_MS;
    PolarDB_WaitMode mode = PolarDB_WaitMode::BEST_EFFORT;

    bool has_wait() const {
        return type != PolarDB_WaitType::NONE;
    }

    void reset() {
        type = PolarDB_WaitType::NONE;
        target = 0;
        timeout_ms = POLARDB_DEFAULT_WAIT_TIMEOUT_MS;
        mode = PolarDB_WaitMode::BEST_EFFORT;
    }

    /**
     * @brief Build a wait spec for an LSN target.
     *
     * A zero @p lsn does not produce an error: it yields a spec whose type stays
     * NONE, so has_wait() is false while @p timeout_ms and @p mode are still
     * applied. Callers must test has_wait() before treating the read as
     * consistency-protected — a spec built from a zero session target and then
     * dispatched carries no wait at all, which is the unwrapped replica read the
     * consistency path exists to prevent.
     *
     * @param lsn         WAL position to wait for. 0 means no wait target.
     * @param timeout_ms  Wait timeout, applied whether or not a target is set.
     * @param mode        BEST_EFFORT or STRICT, applied whether or not a target
     *                    is set.
     * @return The spec. has_wait() is true only when @p lsn is non-zero.
     */
    static PolarDB_WaitSpec from_lsn(
        uint64_t lsn,
        uint32_t timeout_ms,
        PolarDB_WaitMode mode) {
        PolarDB_WaitSpec spec;
        if (lsn > 0) {
            spec.type = PolarDB_WaitType::LSN;
            spec.target = lsn;
        }
        spec.timeout_ms = timeout_ms;
        spec.mode = mode;
        return spec;
    }
};

/**
 * @brief Consistency helper output before final routing.
 *
 * route_hint is the consistency helper's recommendation only. REPLICA means
 * the helper permits a reader, but placement and safety checks still select the
 * final route.
 */
struct PolarDB_Query_WaitPlan {
    PolarDB_WaitSpec spec;
    PolarDB_Query_ConsistencyRouteHint route_hint = PolarDB_Query_ConsistencyRouteHint::NONE; // Helper hint; the final route comes from Query_RoutePlan

    bool has_wait() const {
        return spec.has_wait();
    }

    void reset() {
        spec.reset();
        route_hint = PolarDB_Query_ConsistencyRouteHint::NONE;
    }

    /**
     * @brief Derive the wait payload and route hint from the consistency mode.
     *
     * Under SESSION_LSN or GLOBAL_LSN with @p session_lsn == 0, the returned
     * plan keeps a REPLICA hint while
     * spec.has_wait() is false: the hint alone never implies an enforceable wait,
     * so callers must test has_wait() separately before treating the read as
     * consistency-protected.
     *
     * @param mode            Resolved consistency mode.
     * @param session_lsn     Wait target for the LSN modes; 0 means no target.
     * @param wait_timeout_ms Wait timeout, applied to the spec regardless of mode.
     * @param wait_mode       BEST_EFFORT or STRICT, applied regardless of mode.
     * @param prefer_replica  Whether placement selected a replica.
     * @return The wait plan: a spec that may carry no wait, plus a route hint.
     */
    static PolarDB_Query_WaitPlan build_consistency(
        PolarDB_ConsistencyMode mode,
        uint64_t session_lsn,
        uint32_t wait_timeout_ms,
        PolarDB_WaitMode wait_mode,
        bool prefer_replica) {
        PolarDB_Query_WaitPlan wait_plan;
        wait_plan.spec.timeout_ms = wait_timeout_ms;
        wait_plan.spec.mode = wait_mode;
        wait_plan.route_hint = prefer_replica
            ? PolarDB_Query_ConsistencyRouteHint::REPLICA
            : PolarDB_Query_ConsistencyRouteHint::NONE;

        switch (mode) {
        case PolarDB_ConsistencyMode::SESSION_LSN:
        case PolarDB_ConsistencyMode::GLOBAL_LSN:
            wait_plan.spec = PolarDB_WaitSpec::from_lsn(
                session_lsn,
                wait_timeout_ms,
                wait_mode);
            break;
        case PolarDB_ConsistencyMode::OFF:
        case PolarDB_ConsistencyMode::EVENTUAL:
        default:
            break;
        }

        return wait_plan;
    }
};

/**
 * @brief Wait state for an in-flight query.
 *
 * Prepared from the route plan's wait spec after the selected reader is known
 * to be behind the target and before query dispatch. Tracks the active wait
 * through wrapper finalization and timeout, and drives wrapper-result filtering
 * once the wait wrapper prepends SET statements before the user query.
 */
struct PolarDB_Query_WaitState {
    PolarDB_WaitSpec spec;
    PolarDB_WaitStage wait_stage = PolarDB_WaitStage::IDLE; // IDLE or WAITING
    uint32_t wrapper_stmts = 0;      // SET results to skip before the user result
    uint64_t wait_started_at_us = 0;             // Monotonic start time for latency tracking
    bool wrapper_finalized = false;         // True after the wait wrapper is finalized
    bool timeout_error = false;             // True after a structured strict wait-timeout ERROR
    int fallback_writer_hg = -1;            // Writer HG for retrying this failed wait read

    void reset() {
        spec.reset();
        wait_stage = PolarDB_WaitStage::IDLE;
        wrapper_stmts = 0;
        wait_started_at_us = 0;
        wrapper_finalized = false;
        timeout_error = false;
        fallback_writer_hg = -1;
    }

    void prepare_from_spec(const PolarDB_WaitSpec& wait_spec) {
        spec = wait_spec;
        wait_stage = PolarDB_WaitStage::IDLE;
        wrapper_stmts = 0;
        wait_started_at_us = 0;
        wrapper_finalized = false;
        timeout_error = false;
        fallback_writer_hg = -1;
    }
};

#if POLARDB_PROFILE
enum class PolarDB_ConsistencyTraceEvent : uint64_t {
    RFQ_ACCEPTED = 1,
    RFQ_REJECTED = 2,
    READ_WAIT_PLANNED = 3,
    READER_WAIT_BYPASSED = 4,
    READER_WAIT_REQUIRED = 5,
    WAIT_WRAPPER_INSTALLED = 6,
};

enum PolarDB_ConsistencyTraceFlag : uint32_t {
    POLARDB_CONSISTENCY_TRACE_WRITE = 1U << 0,
    POLARDB_CONSISTENCY_TRACE_PRIMARY = 1U << 1,
    POLARDB_CONSISTENCY_TRACE_KEEP_SESSION_LSN = 1U << 2,
    POLARDB_CONSISTENCY_TRACE_SELECTED_LSN_FRESH = 1U << 3,
    POLARDB_CONSISTENCY_TRACE_BEST_LSN_FRESH = 1U << 4,
};

static inline uint64_t polardb_consistency_trace_detail(
        int hostgroup_id, uint32_t flags) {
    return (static_cast<uint64_t>(static_cast<uint32_t>(hostgroup_id)) << 32) |
        flags;
}

/**
 * Stable profile-only uprobe point. Arguments are event, session ID, target
 * LSN, event LSN, auxiliary LSN, and packed hostgroup/flags.
 */
extern "C" void proxysql_polardb_consistency_trace(
    uint64_t event, uint64_t session_id, uint64_t target_lsn,
    uint64_t event_lsn, uint64_t auxiliary_lsn, uint64_t detail);

enum class PolarDB_WaitProfileContext : uint8_t {
    UNKNOWN = 0,
    ORDINARY,
    TXN_PREWRITE,
    TXN_SPLIT,
};

enum class PolarDB_WaitProfileTargetSource : uint8_t {
    UNKNOWN = 0,
    WRITE,
    OBSERVED,
    SESSION_EQUAL,
    GLOBAL,
    TXN_PRIMARY,
};

/**
 * @brief Profile-only correlation state for one LSN-wrapped reader query.
 *
 * The release build contains none of this state. Profiling builds carry the
 * reader observation selected during acquisition through wrapper completion so
 * target gaps can be correlated with the time spent in the wrapper prefix.
 */
struct PolarDB_WaitProfileState {
    uint64_t prepared_at_us = 0;
    uint64_t dispatched_at_us = 0;
    uint64_t wait_set_completed_at_us = 0;
    uint64_t selected_gap_bytes = 0;
    uint64_t selection_loss_bytes = 0;
    uint64_t selected_lsn_age_us = 0;
    uintptr_t observed_source_server_token = 0;
    uintptr_t selected_server_token = 0;
    PolarDB_WaitProfileContext context =
        PolarDB_WaitProfileContext::UNKNOWN;
    PolarDB_WaitProfileTargetSource target_source =
        PolarDB_WaitProfileTargetSource::UNKNOWN;
    bool active = false;
    bool target_mismatch = false;
    bool selection_recorded = false;
    bool selected_lsn_known = false;
    bool selected_lsn_fresh = false;
    bool selection_compared = false;
    bool selected_behind_best = false;
    bool selected_lsn_age_known = false;

    void reset() {
        prepared_at_us = 0;
        dispatched_at_us = 0;
        wait_set_completed_at_us = 0;
        selected_gap_bytes = 0;
        selection_loss_bytes = 0;
        selected_lsn_age_us = 0;
        observed_source_server_token = 0;
        selected_server_token = 0;
        context = PolarDB_WaitProfileContext::UNKNOWN;
        target_source = PolarDB_WaitProfileTargetSource::UNKNOWN;
        active = false;
        target_mismatch = false;
        selection_recorded = false;
        selected_lsn_known = false;
        selected_lsn_fresh = false;
        selection_compared = false;
        selected_behind_best = false;
        selected_lsn_age_known = false;
    }
};
#endif // POLARDB_PROFILE

/**
 * @brief Actions to apply after an error while processing a wrapped query.
 *
 * This is the accounting policy PgSQL_Connection's result loop uses, kept
 * side-effect-free so unit tests exercise the same decision table without
 * constructing a live connection, session, or PGresult. The caller still owns
 * the actual state mutation and counter updates.
 */
struct PolarDB_WrapperErrorAccounting {
    bool mark_wrapper_failed = false;
    bool mark_timeout_error = false;
    bool account_wait_timeout = false;
};

/**
 * @brief Return how to account a backend error from a wrapped query.
 *
 * The two timeout outputs have different tests and must not be conflated.
 * mark_timeout_error is set whenever the query was wrapped and the backend
 * supplied the structured timeout marker, regardless of whether the wait is
 * still active — the error is a wait timeout either way. account_wait_timeout
 * additionally requires @p wait_active, so a timeout observed after the wait
 * state was cleared is reported but not counted twice.
 *
 * A wrapper SET failure is marked only while wrapper SET results are still
 * being consumed. Once the user query is running, its error must remain a user
 * error even if the session wait state was already cleared.
 *
 * @param query_was_wrapped     Whether wait wrapper statements were prepended.
 *                              When false every output stays false.
 * @param wait_active           Whether the PolarDB wait state is still active.
 * @param lsn_timeout           Whether the backend carried the structured
 *                              wait-timeout marker.
 * @param consuming_wrapper_set Whether the failing result belongs to a wrapper
 *                              SET rather than the user query.
 * @return The accounting decision. The caller performs the state changes and
 *         counter updates itself.
 */
inline PolarDB_WrapperErrorAccounting polardb_wrapper_error_accounting(
        bool query_was_wrapped,
        bool wait_active,
        bool lsn_timeout,
        bool consuming_wrapper_set) {
    PolarDB_WrapperErrorAccounting accounting;
    if (!query_was_wrapped) {
        return accounting;
    }
    if (lsn_timeout) {
        accounting.mark_timeout_error = true;
        accounting.account_wait_timeout = wait_active;
    }
    if (consuming_wrapper_set) {
        accounting.mark_wrapper_failed = true;
    }
    return accounting;
}

/**
 * @brief Clear one per-server LSN cache cell.
 *
 * A writer-epoch change invalidates all cached WAL positions for the affected
 * writer/reader pair, so both halves of the cell are cleared: leaving either
 * half set would let later reader selection treat a stale position as current.
 *
 * The two stores use the same ordering as the code that records a new
 * observation. @p current_lsn is cleared with memory_order_relaxed and
 * @p updated_at with memory_order_release, so a thread that acquire-loads the
 * timestamp and then relaxed-loads the LSN never observes a fresh timestamp
 * paired with an older LSN.
 *
 * The pair is not atomic as a whole. A concurrent reader can acquire-load a
 * timestamp that has not been cleared yet and then load an already-cleared
 * lsn == 0, so correctness depends on every reader treating a zero LSN as
 * unknown rather than as a valid position.
 *
 * No external lock is required. Callers that hold the hostgroup-manager lock
 * hold it to walk the server list, not to protect these stores.
 *
 * @param current_lsn  Cached WAL position cell to clear.
 * @param updated_at   Cached observation timestamp cell to clear.
 */
inline void polardb_reset_server_lsn_cache(
        std::atomic<uint64_t>& current_lsn,
        std::atomic<unsigned long long>& updated_at) {
    current_lsn.store(0, std::memory_order_relaxed);
    updated_at.store(0, std::memory_order_release);
}

// ===========================================================================
// LSN policy decision helpers
// ===========================================================================
//
// Keep these helpers header-inline and side-effect free, matching the core
// ProxySQL pattern used by HostgroupRouting / MonitorHealthDecision. Unit tests
// exercise the same policy code the release path uses without requiring a
// POLARDB_PROXY=1 libproxysql.a.

/**
 * @brief Resolve the effective wait timeout from HG and global config.
 *
 * HG lsn_wait_timeout_ms semantics:
 *   -1 = inherit global
 *    0 = wait indefinitely inside the PolarDB wait loop; PostgreSQL
 *        statement_timeout, client cancel, administrator termination, or
 *        connection loss can still interrupt the statement
 *   >0 = explicit HG value
 *
 * @param hg_timeout_ms     HG-level timeout.
 * @param global_timeout_ms Runtime global timeout.
 * @return Resolved timeout in milliseconds (0 = wait indefinitely, subject to
 *         PostgreSQL statement interruption).
 */
static inline uint32_t polardb_resolve_wait_timeout_ms(int hg_timeout_ms,
                                                       int global_timeout_ms) {
    if (hg_timeout_ms > 0) return (uint32_t)hg_timeout_ms;
    if (hg_timeout_ms == 0) return 0;
    if (global_timeout_ms >= 0) return (uint32_t)global_timeout_ms;
    return POLARDB_DEFAULT_WAIT_TIMEOUT_MS;
}

/**
 * @brief Resolve the effective consistency mode from the 3-tier config.
 *
 * Priority: session override > hostgroup config > global thread variable.
 *
 * @param session_override    Session-level override (-1 = not set).
 * @param hg_consistency_mode Hostgroup config value (-1 = not set).
 * @param global_mode         Global thread variable value.
 * @return Resolved consistency mode, or -1 if all inputs are -1.
 */
static inline int polardb_resolve_consistency_mode(
    int session_override, int hg_consistency_mode, int global_mode) {
    if (session_override >= 0) return session_override;
    if (hg_consistency_mode >= 0) return hg_consistency_mode;
    return global_mode;
}

/**
 * @brief Lag controls supported by LSN-only session consistency.
 *
 * LSN-only session consistency currently uses three supported lag controls:
 *
 * 1. LSN byte distance (`polardb_max_reader_lsn_gap_bytes` / per-HG `max_lag_bytes`).
 * 2. Cached-LSN age (`polardb_reader_lsn_max_age_ms`).
 * 3. Wait timeout around `polar_xact_split_wait_lsn`.
 *
 * Millisecond replica lag is intentionally deferred. The PgSQL/PolarDB path does
 * not currently produce a real per-reader time-lag value; the full PolarDB tree
 * has the same gap. Do not treat `polardb_max_reader_lag_ms` as a supported routing condition
 * until a PgSQL monitor/session producer is defined and tested.
 *
 * Future direction:
 *  - Do not reuse the MySQL/Aurora `aws_aurora_current_lag_us` field as-is.
 *  - Keep monitor samples of each reader's replay LSN and sample time.
 *  - Estimate recent replay rate from consecutive samples:
 *
 *        replay_bytes_per_ms =
 *            (replay_lsn_now - replay_lsn_prev) / (sample_ms_now - sample_ms_prev)
 *
 *  - For a candidate reader, estimate catch-up time from the current byte gap:
 *
 *        estimated_catchup_ms = byte_lag / recent_replay_bytes_per_ms
 *
 *  - `pgsql-polardb_max_reader_lag_ms` can then reject readers whose estimated catch-up
 *    time is above the configured cap. Missing samples, stale samples, or zero
 *    replay rate under an enabled cap should reject the reader and let
 *    the caller use the writer.
 */
/// Default for pgsql-polardb_reader_lsn_max_age_ms (max age of a cached per-server
/// LSN to trust), used when that runtime knob is unset or non-positive.
static constexpr int POLARDB_LSN_FRESHNESS_MS_DEFAULT = 5000;
static constexpr uint32_t POLARDB_LSN_FRESHNESS_WAIT_FRACTION_DIVISOR = 4;

struct PolarDB_ReaderLsnSample {
	uint64_t lsn{0};
	bool fresh{false};
};

/**
 * @brief Whether a cached per-server LSN sample is still young enough to trust.
 *
 * The units are mixed on purpose to match their sources: the two timestamps are
 * microseconds from the monotonic clock, while the allowance is milliseconds
 * from pgsql-polardb_reader_lsn_max_age_ms.
 *
 * Two edge cases are deliberate. A zero @p updated_at_us means the server has
 * never been sampled and is never fresh. A @p now_us earlier than
 * @p updated_at_us is treated as fresh rather than invalid: a backwards reading
 * is clock skew across the sampling path, and rejecting the sample for it would
 * push reads to the writer for no consistency benefit.
 *
 * @param updated_at_us  Time the sample was taken, in microseconds. 0 means never.
 * @param now_us         Current time, in microseconds.
 * @param freshness_ms   Maximum accepted sample age, in milliseconds.
 * @return true when the sample is within the allowance or the clock ran
 *         backwards, false when it is too old or was never taken.
 */
static inline bool polardb_lsn_cache_is_fresh(uint64_t updated_at_us,
                                           uint64_t now_us,
                                           uint32_t freshness_ms) {
	if (updated_at_us == 0) return false;
	if (now_us < updated_at_us) return true;  // monotonic clock skew check
	return (now_us - updated_at_us) <= ((uint64_t)freshness_ms * 1000ULL);
}

/*
 * Effective LSN-cache freshness for reader acquisition.
 *
 * The configured pgsql-polardb_reader_lsn_max_age_ms is the general maximum age of a
 * cached per-server LSN. When a byte-lag cap and a finite backend wait timeout
 * are both active, the configured age can be too loose: a reader that was within
 * max_lag_bytes several seconds ago can drift far outside the cap before the
 * backend gets only wait_timeout_ms to catch up.
 *
 * v1 uses a conservative pseudo-dynamic clamp:
 *   - no byte cap, or timeout 0 (wait indefinitely): use configured freshness;
 *   - finite wait + byte cap: trust cached LSNs for at most one quarter of the
 *     wait timeout, optionally capped by pgsql-polardb_lag_cap_freshness_ms.
 *
 * A future rate-based policy can replace this clamp after we store writer WAL
 * generation rate and per-reader replay rate by writer epoch. Then freshness can
 * be derived from:
 *
 *     allowed_age_ms ~= remaining_lag_bytes / observed_growth_bytes_per_ms
 *
 * @param configured_freshness_ms Configured pgsql-polardb_reader_lsn_max_age_ms; a
 *                                non-positive value selects
 *                                POLARDB_LSN_FRESHNESS_MS_DEFAULT.
 * @param wait_timeout_ms         Resolved backend wait timeout. 0 (wait
 *                                indefinitely) disables the clamp.
 * @param max_lag_bytes           Byte-lag cap. Non-positive disables the clamp.
 * @param lag_cap_freshness_ms    Optional upper bound on the clamped value; a
 *                                non-positive value applies no extra bound.
 * @param clamped                 Optional out-param. When non-null it is set to
 *                                false on entry, so it is written on every path
 *                                including the early returns, and set to true
 *                                only when the configured value was actually
 *                                reduced to the wait fraction.
 * @return The freshness allowance to use, in milliseconds.
 */
static inline uint32_t polardb_effective_lsn_freshness_ms(
		int configured_freshness_ms,
		uint32_t wait_timeout_ms,
		int max_lag_bytes,
		int lag_cap_freshness_ms,
		bool* clamped = nullptr) {
	uint32_t effective =
		configured_freshness_ms > 0
			? (uint32_t)configured_freshness_ms
			: (uint32_t)POLARDB_LSN_FRESHNESS_MS_DEFAULT;
	if (clamped) *clamped = false;

	if (max_lag_bytes <= 0 || wait_timeout_ms == 0) {
		return effective;
	}

	uint32_t wait_fraction_ms =
		wait_timeout_ms / POLARDB_LSN_FRESHNESS_WAIT_FRACTION_DIVISOR;
	if (wait_fraction_ms == 0) {
		wait_fraction_ms = 1;
	}
	if (lag_cap_freshness_ms > 0 &&
			wait_fraction_ms > (uint32_t)lag_cap_freshness_ms) {
		wait_fraction_ms = (uint32_t)lag_cap_freshness_ms;
	}
	if (effective > wait_fraction_ms) {
		effective = wait_fraction_ms;
		if (clamped) *clamped = true;
	}
	return effective;
}

/**
 * @brief Whether an LSN wait timeout may be retried on the writer.
 *
 * Every condition must hold at once; a single false input makes the result
 * false. The arguments are six adjacent bools and transposing them is silent,
 * so pass them by position with care.
 *
 * The load-bearing term is the negated one. The caller acts on a true result by
 * zeroing the query's remaining retries and re-dispatching it to the writer,
 * which is only safe while no result bytes have reached the client. Once the
 * result transfer has started the client has already seen part of the answer and
 * the statement can no longer be replayed, so this returns false.
 *
 * @param simple_query_result Whether the result belongs to a simple-protocol query.
 * @param wait_active         Whether the PolarDB wait state is still active.
 * @param timeout_error       Whether the backend reported the structured strict
 *                            wait-timeout error.
 * @param wrapper_finalized   Whether the wait wrapper finished, so the error
 *                            belongs to the user query and not to a wrapper SET.
 * @param consistency_wait    Whether this query carried a consistency wait.
 * @param result_started      Whether result bytes have already been sent to the
 *                            client. Must be false.
 * @return true only when all five positive conditions hold and
 *         @p result_started is false.
 */
static inline bool polardb_should_handle_wait_timeout_result(
		bool simple_query_result,
		bool wait_active,
		bool timeout_error,
		bool wrapper_finalized,
		bool consistency_wait,
		bool result_started) {
	return simple_query_result &&
		wait_active &&
		timeout_error &&
		wrapper_finalized &&
		consistency_wait &&
		!result_started;
}

/**
 * @brief Whether a reader is close enough behind the best candidate to be treated
 *        as equivalent to it.
 *
 * The @p range_bytes convention here is the opposite of the lag caps elsewhere in
 * this header. PolarDB_Query_ReaderPlan::within_byte_cap() and
 * polardb_reader_lag_ms_within_cap() read a non-positive cap as "check disabled, always
 * true"; here a non-positive @p range_bytes means no tolerance at all, so only an
 * exact LSN match qualifies. This is a tie-break between two candidates, not a
 * safety cap, so an unset tolerance must narrow the tie rather than widen it.
 *
 * The comparison is one-sided: a reader strictly ahead of @p best_lsn is not "in
 * range" unless the two are exactly equal.
 *
 * @param reader_lsn  Candidate reader's WAL position. 0 (no sample) returns false.
 * @param best_lsn    Best candidate's WAL position. 0 (no sample) returns false.
 * @param range_bytes Tolerated distance behind @p best_lsn. Non-positive means
 *                    exact equality only.
 * @return true when the two positions are equal, or when @p reader_lsn is behind
 *         @p best_lsn by at most @p range_bytes.
 */
static inline bool polardb_reader_lsn_in_best_behind_range(
		uint64_t reader_lsn, uint64_t best_lsn, int range_bytes) {
	if (reader_lsn == 0 || best_lsn == 0) return false;
	if (reader_lsn == best_lsn) return true;
	if (range_bytes <= 0 || reader_lsn > best_lsn) return false;
	return (best_lsn - reader_lsn) <= (uint64_t)range_bytes;
}

#if POLARDB_PROXY_TODO
/**
 * @brief Predicate for a future monitor-observed time-lag cap.
 *
 * TODO: wire only after PgSQL/PolarDB has a real millisecond-lag producer.
 * The intended producer is an estimated catch-up time, not the PostgreSQL
 * integer-second replication-lag check and not the MySQL/Aurora lag field:
 *
 *     estimated_catchup_ms = byte_lag / recent_replay_bytes_per_ms
 *
 * Today the supported LSN-only lag check is `polardb_max_reader_lsn_gap_bytes`, not this helper.
 */
static inline bool polardb_reader_lag_ms_within_cap(uint64_t lag_us, int max_lag_ms) {
    if (max_lag_ms <= 0) return true;
    return (lag_us / 1000ULL) <= (uint64_t)max_lag_ms;
}
#endif

/**
 * @brief Whether a monitor health result should update the per-server LSN cache.
 */
static inline bool polardb_should_update_monitor_lsn(bool monitor_lsn_updates,
                                                     uint64_t observed_lsn) {
    return monitor_lsn_updates && observed_lsn > 0;
}

/**
 * @brief PolarDB protocol/parsing helpers.
 *
 * Health-check parsing, write-query classification, and wait-SET builders used
 * by the PolarDB LSN session-consistency path.
 */
class PolarDB_Protocol {
public:
    /**
     * @brief Parse PolarDB node type from a polar_node_type() result string.
     */
    static PolarDB_NodeType parse_node_type(const char* node_type) {
        if (node_type == nullptr) {
            return PolarDB_NodeType::UNKNOWN;
        }
        if (strcasecmp(node_type, "primary") == 0 ||
            strcasecmp(node_type, "master") == 0) {
            return PolarDB_NodeType::PRIMARY;
        } else if (strcasecmp(node_type, "replica") == 0) {
            return PolarDB_NodeType::REPLICA;
        } else if (strcasecmp(node_type, "standby") == 0) {
            return PolarDB_NodeType::STANDBY;
        }
        return PolarDB_NodeType::UNKNOWN;
    }

    /**
     * @brief True if this node type is a writer (primary).
     */
    static bool is_writer(PolarDB_NodeType node_type) {
        return node_type == PolarDB_NodeType::PRIMARY;
    }

    /**
     * @brief True if this node type is a reader (replica or standby).
     */
    static bool is_reader(PolarDB_NodeType node_type) {
        return node_type == PolarDB_NodeType::REPLICA ||
               node_type == PolarDB_NodeType::STANDBY;
    }

    /**
     * @brief Convert node type to the hostgroup-manager read_only value.
     * @return 0 for primary, 1 for replica/standby/unknown.
     *
     * UNKNOWN is not a writer role. Monitor callers count it separately and
     * fail it closed through availability; this conversion must never promote
     * POLAR_UNKNOWN or POLAR_STANDALONE_DATAMAX to writer.
     */
    static int node_type_to_read_only(PolarDB_NodeType node_type) {
        return is_writer(node_type) ? 0 : 1;
    }

    /**
     * @brief Parse availability from a polar_is_available() result ('t'/'f').
     * @return true only for true values; a missing column defaults to available.
     */
    static bool parse_is_available(const char* is_available) {
        if (is_available == nullptr) {
            return true;  // Assume available on error
        }
        return (is_available[0] == 't' || is_available[0] == 'T');
    }

    /**
     * @brief Parse a PostgreSQL LSN string (e.g. "0/1234ABCD") into a 64-bit value.
     *
     * Format: "logid/offset" where logid is the high 32 bits and offset the low
     * 32 bits. The whole string must be consumed: trailing text after the offset
     * rejects the parse.
     *
     * @param lsn_str Text to parse. May be null.
     * @param lsn     Parsed value, reset to 0 on failure.
     * @return true only when the whole string is a valid LSN. A valid "0/0"
     *         returns true with @p lsn set to 0.
     */
    static bool parse_lsn(const char* lsn_str, uint64_t& lsn) {
        lsn = 0;
        if (lsn_str == nullptr) return false;
        uint32_t logid = 0;
        uint32_t offset = 0;
        int parsed_len = 0;
        // Require a full "logid/offset" parse, then pack PostgreSQL's numeric
        // LSN as logid in the high 32 bits and offset in the low 32 bits.
        if (sscanf(lsn_str, "%X/%X%n", &logid, &offset, &parsed_len) == 2 &&
                lsn_str[parsed_len] == '\0') {
            lsn = (static_cast<uint64_t>(logid) << 32) | offset;
            return true;
        }
        return false;
    }

    /**
     * @brief Parse and validate one full PolarDB monitor row.
     */
    static PolarDB_HealthCheck parse_health_check(
            const char* node_type_str,
            const char* is_available_str,
            const char* lsn_str) {
        PolarDB_HealthCheck health;
        health.node_type = parse_node_type(node_type_str);
        health.role_valid =
            health.node_type != PolarDB_NodeType::UNKNOWN;
        health.availability_valid =
            is_available_str &&
            (is_available_str[0] == 't' || is_available_str[0] == 'T' ||
             is_available_str[0] == 'f' || is_available_str[0] == 'F') &&
            is_available_str[1] == '\0';
        health.is_available = health.availability_valid
            ? parse_is_available(is_available_str) : true;
        health.lsn_valid = parse_lsn(lsn_str, health.current_lsn);
        return health;
    }

    /**
     * @brief Text-only write fallback when no command classification exists.
     *
     * SELECT/SHOW/EXPLAIN are reads; SELECT ... FOR UPDATE/SHARE and everything
     * else are writes. This makes WITH conservative when the caller has only
     * text. When ProxySQL's parser supplies PGSQL_QUERY_SELECT, callers use the
     * overload below instead, and that command type wins over the text.
     *
     * @param query SQL query text (or digest).
     * @return true if the query likely modifies data.
     */
    static bool is_write_query(const char* query);

    /**
     * @brief Write detection using the caller's command classification.
     *
     * The command type wins, not the query text. PGSQL_QUERY_SELECT is a write
     * only when @p query carries a row-locking FOR clause; PGSQL_QUERY_SHOW and
     * PGSQL_QUERY_EXPLAIN are reads; every other recognised command type is a
     * write. Only the three unclassified types — PGSQL_QUERY_UNKNOWN,
     * PGSQL_QUERY__UNINITIALIZED and PGSQL_QUERY___NONE — fall back to the
     * text heuristic of the single-argument overload; pass one of those when the
     * command type is not known.
     *
     * The text overload's conservative treatment of WITH therefore does not
     * apply here: a parser that classifies `WITH ...` as PGSQL_QUERY_SELECT
     * reaches this overload as a SELECT and is reported as a read unless it also
     * has a locking FOR clause. For a data-modifying CTE, the positioned RFQ
     * still advances observed_lsn and preserves SESSION_LSN monotonicity, but
     * write_lsn is not advanced. Keep those statements on the primary until a
     * fast top-level CTE classifier is available.
     *
     * @param query        SQL query text (or digest). Used for the SELECT locking
     *                     check and for the unclassified fallback.
     * @param command_type PGSQL_QUERY_* classification of the top-level command.
     * @return true if the query must be treated as a write.
     */
    static bool is_write_query(const char* query, int command_type);

    /**
     * @brief True when a completed statement can advance the session write LSN.
     *
     * This is narrower than routing write detection. PostgreSQL locking reads
     * (SELECT ... FOR UPDATE/SHARE) must stay on the writer for routing, but
     * their RFQ LSN is still an observed read position, not an own-write
     * position.
     */
	static bool should_advance_session_write_lsn(const char* query);

    /**
     * @brief Write-LSN detection using the caller's command classification.
     *
     * Applies the same narrower classification as the text-only overload — a
     * locking SELECT is a write for routing but still advances no write LSN — and
     * the command type wins over the query text. PGSQL_QUERY_SELECT,
     * PGSQL_QUERY_SHOW and PGSQL_QUERY_EXPLAIN advance nothing, and the query
     * text is not consulted for them. Every other recognised command type
     * advances the write LSN. Only PGSQL_QUERY_UNKNOWN,
     * PGSQL_QUERY__UNINITIALIZED and PGSQL_QUERY___NONE fall back to the text
     * heuristic; pass one of those when the command type is not known.
     * A data-modifying CTE classified as PGSQL_QUERY_SELECT therefore does not
     * advance write_lsn; its RFQ still advances observed_lsn.
     *
     * @param query        SQL query text (or digest). Used only for the
     *                     unclassified fallback.
     * @param command_type PGSQL_QUERY_* classification of the top-level command.
     * @return true when this statement's completion must advance the session
     *         write LSN.
     */
	static bool should_advance_session_write_lsn(const char* query, int command_type);

    /**
     * @brief True when transaction split may dispatch this query to a replica.
     *
     * Split is deliberately narrower than generic read detection: only a
     * top-level SELECT without a locking FOR clause is eligible. SHOW, EXPLAIN,
     * WITH, and ambiguous shapes stay on the primary until explicitly supported.
     */
    static bool is_txn_split_safe_read(const char* query);

    /**
     * @brief Fast split eligibility for the hot routing path.
     *
     * ProxySQL already classified the top-level command while running query
     * rules. Reuse that result instead of scanning the first keyword again; only
     * the row-locking clause check still needs SQL text.
     */
    static bool is_txn_split_safe_select(
            bool is_top_level_select, const char* query);

    /**
     * @brief Find a PostgreSQL row-locking FOR clause in a classified SELECT.
     *
     * The caller supplies the top-level SELECT classification. This method
     * deliberately scans the complete statement without rechecking its first
     * keyword, so leading comments and WITH ... SELECT forms remain safe.
     */
    static bool is_locking_select_query(const char* query);

    /**
     * @brief Append a single `SET polar_xact_split_wait_lsn = '<target>'; ` statement.
     *
     * Centralizes the GUC name and numeric formatting so the wait wrapper stays
     * consistent.
     *
     * @return true if a statement was appended, false for NONE/zero target.
     */
    static inline bool append_polar_wait_set(PolarDB_WaitType type, uint64_t target, std::string& out) {
        if (type != PolarDB_WaitType::LSN || target == 0) {
            return false;
        }
        char buf[24];
        snprintf(buf, sizeof(buf), "%lu", (unsigned long)target);
        out.append("SET polar_xact_split_wait_lsn = '");
        out.append(buf);
        out.append("'; ");
        return true;
    }

    /**
     * @brief Append `SET polar_proxy_wait_timeout_ms = <ms>; `.
     *
     * Always emits a concrete value, including 0, so a pooled connection never
     * keeps a stale timeout. A value of 0 disables only the PolarDB wait-timeout
     * branch; PostgreSQL statement_timeout/cancel/terminate can still interrupt
     * the statement. The caller is responsible for the capability check.
     */
    static inline void append_polar_timeout_set(uint32_t timeout_ms, std::string& out) {
        char buf[16];
        snprintf(buf, sizeof(buf), "%u", timeout_ms);
        out.append("SET polar_proxy_wait_timeout_ms = ");
        out.append(buf);
        out.append("; ");
    }

};

// ======================================================================
// Route pipeline structs (collect -> plan -> execute)
// ======================================================================

/*
 * Protocol scope:
 *
 * ProxySQL PostgreSQL query-rule routing supports both simple-query protocol and
 * extended protocol (Parse/Bind/Execute). The PolarDB LSN consistency feature
 * supports read-your-writes only for simple-query protocol, because the wait step
 * is injected as wrapper SQL text:
 *
 *   SET polar_xact_split_wait_lsn = ...; <user query>
 *
 * That wrapper cannot be safely inserted into a backend extended-protocol stream
 * without a separate Parse/Bind/Execute wrapper and result-consumption model.
 *
 * Therefore:
 *   - simple-query + replica_eligible=1 may route to a reader with a wait wrapper;
 *   - manual destination_hostgroup rules remain authoritative, including manual
 *     extended-protocol routing to a reader; ProxySQL does not add a wait wrapper
 *     to that manually selected backend;
 *   - automatic replica_eligible=1 extended-protocol reads with no session LSN
 *     target may route to a reader without a wrapper;
 *   - automatic replica_eligible=1 extended-protocol reads after the session has
 *     a wait target, or when write/observed LSN state is unknown, force the
 *     writer. Sending those reads to a replica without an extended-protocol wait
 *     wrapper could serve stale data.
 */

/// All inputs needed for the routing decision. Filled once per query by
/// polardb_collect(); immutable after collection. Request-stack scoped only —
/// never persisted across async boundaries.
struct PolarDB_Query_RouteCtx {
    PolarDB_WriterScope writer_scope;         // replication-group writer identity
    PolarDB_SessionConsistency session;       // session LSN/sticky-flag snapshot
    // Observed primary RFQ split state. The snapshot views the session-owned XID
    // string; collect->plan is synchronous and never crosses an async boundary.
    PolarDB_TransactionSplitSnapshot transaction_split;

    // --- 32-bit fields ---
    uint32_t wait_timeout_ms = POLARDB_DEFAULT_WAIT_TIMEOUT_MS; // resolved wait timeout (HG or global)
    int reader_hg = -1;                    // reader hostgroup (-1 if none configured)
    int effective_consistency_mode = 0;    // resolved once: session > HG > global
    int read_target = static_cast<int>(
        POLARDB_DEFAULT_PROFILE_DEFINITION.read_target);
    int read_fallback_action = static_cast<int>(
        POLARDB_DEFAULT_PROFILE_DEFINITION.read_fallback);
    int lsn_wait_timeout_action = static_cast<int>(
        POLARDB_DEFAULT_PROFILE_DEFINITION.lsn_timeout);
    int missing_lsn_action = static_cast<int>(
        POLARDB_DEFAULT_PROFILE_DEFINITION.missing_lsn);
    int replica_loss_action = static_cast<int>(
        POLARDB_DEFAULT_PROFILE_DEFINITION.replica_loss);
    int replica_error_action = static_cast<int>(
        POLARDB_DEFAULT_PROFILE_DEFINITION.replica_error);
    int max_lag_bytes = -1;                // from HG policy, -1 = use thread default

    // --- bool fields (packed) ---
    bool is_polar_hg = false;              // from HGM cache (resolved once)
    bool replica_eligible = false;         // from qpo->replica_eligible (policy condition)
    bool is_multi_statement = false;       // semicolon scan (hard safety check)
    bool is_extended_protocol = false;     // Parse/Bind/Execute: regular ProxySQL routing only; no PolarDB wait wrapper
    bool in_transaction = false;           // session is inside an explicit transaction
    bool txn_split_enabled = false;        // LSN mode and per-hostgroup split permission are both required
    bool is_txn_split_safe_read = false;   // split: top-level SELECT with no locking clause
    bool is_txn_split_locking_read = false; // split: SELECT ... FOR UPDATE/SHARE-style lock
    bool txn_reader_wait_isolation_read_committed = true; // pre-write reader waits require READ COMMITTED
    bool txn_reader_wait_local_state_clean = true; // false after in-txn SET/SET LOCAL
    bool force_primary_hint = false;       // /* route=primary */ first-comment hint (plan L0)
    // Transaction-scoped route set after a split-reader failure. Checked by the
    // normal planner plus manual/qpo paths that may bypass planning.
    bool txn_force_writer_after_reader_failure = false;
    int txn_writer_hg = -1;
};

/**
 * @brief Per-query inputs for picking a reader connection (Plan).
 *
 * Built by the planner and consumed when a reader connection is acquired. It
 * carries reader-selection parameters: group LSN for the byte-lag cap,
 * fallback writer hostgroup, missing-LSN action, timeout action, and consistency
 * mode. The wait target and timeout duration live in
 * PolarDB_Query_RoutePlan::wait_spec and are passed beside this record to reader
 * acquisition.
 *
 * The wait target is normally enforced by the SET polar_xact_split_wait_lsn
 * statement on the wire. Backend acquisition may skip that wrapper only after
 * the selected reader is confirmed to have a fresh cached LSN at or beyond the
 * target from the wait spec.
 */
struct PolarDB_Query_ReaderPlan {
    // Factory helpers construct this record before the planner copies the
    // query's complete policy into it. These neutral values keep standalone
    // helpers safe; no production reader plan leaves polardb_plan() with them.
    uint64_t group_lsn = 0;      // highest accepted group LSN for the byte-lag cap
    int max_lag_bytes = -1;      // reader lag cap in WAL bytes; <=0 = disabled
    int fallback_writer_hg = -1; // writer hostgroup to fall back to
    int missing_lsn_action = static_cast<int>(
        PolarDB_MissingLsnAction::PRIMARY);
    int lsn_wait_timeout_action = static_cast<int>(
        PolarDB_LsnWaitTimeoutAction::PRIMARY);
    int read_target = static_cast<int>(PolarDB_ReadTarget::PRIMARY);
    int read_fallback_action = static_cast<int>(
        PolarDB_ReadFallbackAction::PRIMARY);
    int replica_loss_action = static_cast<int>(
        PolarDB_ReplicaLossAction::REPLICA_THEN_PRIMARY);
    int replica_error_action = static_cast<int>(
        PolarDB_ReplicaErrorAction::PRIMARY);
    PolarDB_ConsistencyMode consistency_mode = PolarDB_ConsistencyMode::OFF;
    bool allow_reader_without_target = true;
    bool require_replica = false; // transaction split cannot use the active writer endpoint

    /// @brief True if the byte-lag safety cap is enabled for this query.
    bool lag_cap_enabled() const { return max_lag_bytes > 0; }
    /**
     * @brief True if this reader is within the byte-lag cap.
     *
     * Rejects the reader when the cap is enabled but either LSN sample is missing (0):
     * an unknown gap must not be treated as acceptable. A replica at or ahead of
     * the recorded group LSN is always within the cap.
     */
    bool within_byte_cap(uint64_t replica_lsn) const {
        if (max_lag_bytes <= 0) return true;
        if (group_lsn == 0 || replica_lsn == 0) return false;
        if (group_lsn <= replica_lsn) return true;
        return (group_lsn - replica_lsn) <= (uint64_t)max_lag_bytes;
    }
    void reset() {
        *this = PolarDB_Query_ReaderPlan{};
    }
};

/**
 * @brief Per-query PolarDB routing/wait state.
 *
 * These fields are consumed across session state-machine stages: planning records the
 * reader/wait intent, backend acquisition consumes the reader target, wrapping
 * consumes the wait state, and the connection layer copies dispatch-wrapper
 * metadata. The reset methods intentionally clear different subsets; do not
 * replace them with one broad reset.
 */
struct PolarDB_QueryState {
    PolarDB_WriterScope request_writer_scope;
    PolarDB_Query_ReaderPlan reader_plan;
    // Selection input for an ordinary consistency read. It stays separate from
    // wait so a target-ready reader can dispatch the original query without
    // ever activating wrapper state.
    PolarDB_WaitSpec reader_wait_spec;
    PolarDB_Query_WaitState wait;
#if POLARDB_PROFILE
    PolarDB_WaitProfileState wait_profile;
#endif // POLARDB_PROFILE
    // Stable original SQL for a wrapped simple query. CurrentQuery points into
    // this string after the backend packet is replaced and until RequestEnd
    // finishes query logging.
    std::string original_query;
    std::string wrapped_query_buf;
    // Policy captured before dispatch and kept until RequestEnd. Runtime
    // configuration may change while the backend is executing, but the response
    // must finish under the same consistency and split policy as the request.
    int effective_consistency_mode =
        static_cast<int>(PolarDB_ConsistencyMode::OFF);
    uint8_t reader_retry_attempts = 0;
    uint32_t dispatch_wrapper_stmts = 0;
    bool profile_enabled = false;
    bool txn_split_enabled = false;
    bool backend_isolation_status_needed = false;
    // Use the existing session LSN when this RFQ ends a read-only transaction.
    bool keep_session_lsn = false;
    // Wait target already satisfied by the selected backend.
    uint64_t wait_bypass_target = 0;
    PolarDB_Query_WrapperKind dispatch_wrapper_kind =
        PolarDB_Query_WrapperKind::NONE;

    bool data_path_enabled() const {
        return profile_enabled &&
            polardb_consistency_from_int(effective_consistency_mode) !=
                PolarDB_ConsistencyMode::OFF;
    }

    void reset_reader_plan() {
        reader_plan.reset();
        reader_wait_spec.reset();
    }

    void reset_wait() {
        wait.reset();
#if POLARDB_PROFILE
        wait_profile.reset();
#endif // POLARDB_PROFILE
    }

    void clear_reader_route() {
        reset_reader_plan();
        reset_wait();
        wait_bypass_target = 0;
    }

    void reset_dispatch_wrapper() {
        dispatch_wrapper_stmts = 0;
        dispatch_wrapper_kind = PolarDB_Query_WrapperKind::NONE;
    }

    /**
     * @brief Clear one completed extended-protocol step without losing the
     *        policy already selected for the pending Execute.
     *
     * Parse and Describe may complete before Bind/Execute from the same Sync
     * frame. Keep only the route and response-policy snapshot; clear wait,
     * wrapper, retry, result and notice-related state exactly as a normal
     * request end does.
     */
    void reset_between_extended_messages() {
        const PolarDB_WriterScope writer_scope = request_writer_scope;
        const PolarDB_Query_ReaderPlan plan = reader_plan;
        const PolarDB_WaitSpec wait_spec = reader_wait_spec;
        const int consistency_mode = effective_consistency_mode;
        const bool enabled = profile_enabled;
        const bool split_enabled = txn_split_enabled;
        const bool isolation_status_needed =
            backend_isolation_status_needed;

        reset_for_new_query();

        request_writer_scope = writer_scope;
        reader_plan = plan;
        reader_wait_spec = wait_spec;
        effective_consistency_mode = consistency_mode;
        profile_enabled = enabled;
        txn_split_enabled = split_enabled;
        backend_isolation_status_needed = isolation_status_needed;
    }

    void reset_for_new_query() {
        clear_reader_route();
        request_writer_scope.reset();
        original_query.clear();
        wrapped_query_buf.clear();
        effective_consistency_mode =
            static_cast<int>(PolarDB_ConsistencyMode::OFF);
        reader_retry_attempts = 0;
        profile_enabled = false;
        txn_split_enabled = false;
        backend_isolation_status_needed = false;
        keep_session_lsn = false;
        reset_dispatch_wrapper();
    }
};

/**
 * @brief Outcome of acquiring a reader connection (Result).
 *
 * Returned by the HostGroups_Manager reader-acquisition path. @ref status says
 * what happened, and the caller maps a failure to either a writer fallback or
 * ProxySQL's normal no-connection retry (see PolarDB_ReaderStatus and
 * polardb_reader_status_redirects_to_writer).
 *
 * Ownership is not uniform across the members. @ref conn is owned by the caller
 * on a successful acquire. @ref srv and @ref selected_pool_miss_server are
 * non-owning back-pointers and are populated on failure results too: the caller
 * uses them to drive connection creation and warmup for a reader it could not
 * take from the pool. They stay valid only while @ref selected_server_snapshot,
 * or an equivalent snapshot the caller holds, is alive — do not store them past
 * that.
 *
 * On a successful acquire @ref selected_server_snapshot is moved into the
 * connection, so this result's copy is empty afterwards. A caller that still
 * needs @ref srv past that point must keep its own snapshot reference.
 */
struct PolarDB_ReaderResult {
    PgSQL_Connection* conn = nullptr;
    PgSQL_SrvC* srv = nullptr;
    // First routing-selected server whose locked exact pooled lookup missed.
    // A successful pooled-only lookup on another reader uses this to replenish
    // the selected server asynchronously.
    PgSQL_SrvC* selected_pool_miss_server = nullptr;
    // Keeps srv valid between selection and connection retrieval.
    std::shared_ptr<const void> selected_server_snapshot;
    PolarDB_ReaderStatus status =
        PolarDB_ReaderStatus::READER_UNAVAILABLE;
    bool wait_bypass_allowed = false;  // selected reader already reached consistency target
    bool server_saturated = false;     // selected snapshot had no free or open capacity
#if POLARDB_PROFILE
    bool exact_match_reserved = false;  // an eligible exact FREE match was reserved by a reservation
#endif // POLARDB_PROFILE
    // Cached LSN observations used only for post-acquisition diagnostics. The
    // best value is the freshest eligible reader considered by this lookup,
    // not an assertion about readers that routing did not inspect.
    uint64_t selected_reader_lsn = 0;
    uint64_t best_considered_reader_lsn = 0;
    bool selected_reader_lsn_fresh = false;
    bool best_considered_reader_lsn_fresh = false;
    // A compact worker-local pacing scope. Hash equality is used only to defer
    // another retry until the next worker pass; pool identity still uses the
    // complete collision-safe match key.
    uint64_t retry_scope_hash = 0;
    uint32_t reservation_profile_generation = 0;
    PolarDB_PoolKey reservation_pool_key;

    /// @brief True only when a usable reader connection was obtained.
    bool acquired() const {
        return status == PolarDB_ReaderStatus::ACQUIRED && conn != nullptr;
    }
};

/**
 * @brief The routing decision for one query (Plan).
 *
 * Produced by polardb_plan() and consumed by polardb_execute(). Request-stack
 * scoped only; never persisted across an async boundary. @ref action is what to
 * do; @ref action_reason records why, for diagnostics.
 */
struct PolarDB_Query_RoutePlan {
    /// @brief The route the planner selected for this query.
    enum class RouteAction : uint8_t {
        PASSTHROUGH = 0,       // No PolarDB override, use the default/target HG
        REPLICA_WITH_WAIT,     // Route to a replica and prepend SET polar_xact_split_wait_lsn
        FORCE_PRIMARY,         // Override to primary (consistency or safety reason)
        REPLICA_TXN_SPLIT,     // In-transaction read dispatched to a replica with XIDs + LSN wait
        RETURN_ERROR,          // End the request because the required LSN is unavailable
    };

    /**
     * @brief Why the planner did not take the straightforward replica route.
     *
     * A reason is recorded whenever the planner has a diagnostic one, not only on
     * FORCE_PRIMARY plans. missing_lsn() sets a reason and may still produce a
     * PASSTHROUGH reader plan with degraded_rfq_route set; the operator warning
     * for that reader-with-warning route reads the reason from the passthrough
     * plan. Look for a reason on any plan.
     */
    enum class RouteActionReason : uint8_t {
        NONE = 0,
        EXTENDED_PROTOCOL,     // extended-protocol read has a session wait target; the wait wrapper is simple-query only
        IN_TRANSACTION,        // transaction state is not eligible for the safe pre-write/XID split route
        MULTI_STATEMENT,       // multi-statement read — never offloaded to a replica
        READ_TARGET_PRIMARY,   // read target is primary
        HINT_PRIMARY,          // /* route=primary */ per-query hint
        WRITE_LSN_UNKNOWN,     // a prior write completed but RFQ carried no LSN
        OBSERVED_LSN_UNKNOWN,  // a prior tracked read completed but RFQ carried no LSN
        GROUP_LSN_UNKNOWN,     // GLOBAL_LSN requested but the group LSN observation is unknown
        INVALID_POLICY,        // configured policy would weaken the selected consistency mode
        READER_RFQ_UNAVAILABLE,// selected reader cannot provide the RFQ LSN needed for the wait
        READ_FALLBACK_ERROR,   // replica was unavailable and fallback is error
        READER_FAILURE_FORCE_WRITER, // full retry: rest of txn stays on writer after reader failure
        WAL_PENDING,           // split: transaction WAL is not yet replay-safe
        SPLIT_BLOCKED,         // split: a prior split read failed in this transaction
        SPLIT_NOT_SELECT,      // split: only top-level SELECT is eligible
        SPLIT_LOCKING_READ,    // split: locking SELECT must stay on the primary
        SPLIT_WRITE_LSN_UNKNOWN,    // split: a prior write RFQ missed its LSN target
        SPLIT_OBSERVED_LSN_UNKNOWN, // split: a tracked read RFQ missed its LSN
        NO_TXN_LSN,            // split: no transaction-scoped LSN wait target is available
        INVARIANT_VIOLATION,   // split: defensive fallback for impossible state
        HG_SPLIT_DISABLED,     // split: replication-hostgroup policy disables transaction split
    };

    // --- wait/reader fields ---
    PolarDB_WaitSpec wait_spec;
    PolarDB_Query_ReaderPlan reader;      // per-query reader acquisition plan
    std::string_view txn_xids;            // split-read XIDs; backed by session state during plan/execute

    // --- 32-bit fields ---
    int target_hg = -1;                    // final hostgroup (-1 = caller keeps current)

    // --- small fields (packed) ---
    RouteAction action = RouteAction::PASSTHROUGH;
    RouteActionReason action_reason = RouteActionReason::NONE;
    RouteActionReason split_reason = RouteActionReason::NONE;
    bool degraded_rfq_route : 1;           // best-effort route without enforceable RFQ target
    bool txn_wait_read : 1;                // pre-write transaction read using a temporary reader
    bool split_checked : 1;                // transaction-split checks ran for this plan

    PolarDB_Query_RoutePlan()
        : degraded_rfq_route(false), txn_wait_read(false), split_checked(false) {}

    /// @brief Build a plan that forces the read to the writer hostgroup.
    static PolarDB_Query_RoutePlan force_primary(int writer_hg,
        RouteActionReason reason) {
        PolarDB_Query_RoutePlan plan;
        plan.action = RouteAction::FORCE_PRIMARY;
        plan.target_hg = writer_hg;
        plan.action_reason = reason;
        return plan;
    }

    /// @brief Build a planned transaction-split replica read.
    static PolarDB_Query_RoutePlan replica_txn_split(int reader_hg,
        int writer_hg,
        const PolarDB_WaitSpec& wait_spec,
        std::string_view txn_xids) {
        PolarDB_Query_RoutePlan plan;
        plan.action = RouteAction::REPLICA_TXN_SPLIT;
        plan.target_hg = reader_hg;
        plan.wait_spec = wait_spec;
        plan.txn_xids = txn_xids;
        plan.reader.fallback_writer_hg = writer_hg;
        plan.reader.require_replica = true;
        return plan;
    }

    /**
     * @brief Build the plan for when no enforceable RFQ wait target is available.
     *
     * WARNING routes to the reader with no wait and marks the plan
     * degraded_rfq_route. PRIMARY routes to the primary. ERROR ends the
     * request without contacting a backend. The missing-LSN action owns the
     * event; action_read_fallback is consulted only when WARNING cannot safely
     * use a reader, and then decides between the primary and an error.
     */
    static PolarDB_Query_RoutePlan missing_lsn(
        RouteActionReason reason,
        int missing_lsn_action,
        int read_fallback_action,
        int writer_hg,
        int reader_hg,
        bool allow_reader_without_target = true) {
        PolarDB_Query_RoutePlan plan;
        plan.action_reason = reason;

        const PolarDB_MissingLsnAction action =
            polardb_missing_lsn_action_from_int(missing_lsn_action);
        if (action == PolarDB_MissingLsnAction::ERROR) {
            plan.action = RouteAction::RETURN_ERROR;
        } else if (action ==
                PolarDB_MissingLsnAction::WARNING &&
                allow_reader_without_target) {
            plan.action = RouteAction::PASSTHROUGH;
            plan.target_hg = reader_hg;
            plan.degraded_rfq_route = true;
        } else if (action == PolarDB_MissingLsnAction::PRIMARY ||
                polardb_read_fallback_action_from_int(
                    read_fallback_action) ==
                    PolarDB_ReadFallbackAction::PRIMARY) {
            plan.action = RouteAction::FORCE_PRIMARY;
            plan.target_hg = writer_hg;
        } else {
            plan.action = RouteAction::RETURN_ERROR;
        }

        return plan;
    }
};

/**
 * @brief Explain why the current transaction cannot be planned as a split read.
 *
 * NONE means the observed transaction state has enough RFQ evidence for the
 * planner to name a split-read route. The executor may still decline the plan
 * if a suitable replica connection cannot be obtained for this query.
 *
 * The checks run in a fixed precedence order and the first match wins, so only
 * one reason is ever reported even when several apply. Two consequences matter
 * when reading the result: @p txn_split_enabled false masks every other reason as
 * HG_SPLIT_DISABLED, and a transaction whose stage is not TXN_SPLITTABLE is
 * reported as the generic IN_TRANSACTION rather than as a split-specific reason.
 *
 * Six of the arguments are adjacent bools and transpose silently; pass them by
 * position with care.
 *
 * @param txn_split_enabled        Whether the replication hostgroup allows split.
 * @param transaction_split        Snapshot of the session's split state. Its
 *                                 xids view must still be valid.
 * @param write_lsn_unknown        A prior write completed with no RFQ LSN.
 * @param observed_lsn_unknown     A prior tracked read completed with no RFQ LSN.
 * @param is_multi_statement       The query carries more than one statement.
 * @param is_extended_protocol     The query arrived over the extended protocol.
 * @param is_txn_split_safe_read   The query shape is eligible for a split read.
 * @param is_txn_split_locking_read The query is a locking SELECT.
 * @return NONE when a split read may be planned, otherwise the first rejection
 *         reason in precedence order.
 */
static inline PolarDB_Query_RoutePlan::RouteActionReason polardb_txn_split_rejection_reason(
    bool txn_split_enabled,
    const PolarDB_TransactionSplitSnapshot& transaction_split,
    bool write_lsn_unknown,
    bool observed_lsn_unknown,
    bool is_multi_statement,
    bool is_extended_protocol,
    bool is_txn_split_safe_read,
    bool is_txn_split_locking_read) {
    using RAR = PolarDB_Query_RoutePlan::RouteActionReason;

    if (!txn_split_enabled) return RAR::HG_SPLIT_DISABLED;
    if (is_multi_statement) return RAR::MULTI_STATEMENT;
    if (is_extended_protocol) return RAR::EXTENDED_PROTOCOL;
    if (is_txn_split_locking_read) return RAR::SPLIT_LOCKING_READ;
    if (!is_txn_split_safe_read) return RAR::SPLIT_NOT_SELECT;
    if (write_lsn_unknown) return RAR::SPLIT_WRITE_LSN_UNKNOWN;
    if (observed_lsn_unknown) return RAR::SPLIT_OBSERVED_LSN_UNKNOWN;
    if (transaction_split.blocked) return RAR::SPLIT_BLOCKED;
    if (transaction_split.wal_pending) return RAR::WAL_PENDING;
    if (transaction_split.stage != PolarDB_TransactionSplitStage::TXN_SPLITTABLE) {
        return RAR::IN_TRANSACTION;
    }
    if (transaction_split.xids.empty()) return RAR::INVARIANT_VIOLATION;
    if (transaction_split.primary_lsn == 0) return RAR::NO_TXN_LSN;

    return RAR::NONE;
}

/// @brief Short stable name for a route-action reason, for logs and traces.
static inline const char* polardb_route_action_reason_name(
    PolarDB_Query_RoutePlan::RouteActionReason reason) {
    using RAR = PolarDB_Query_RoutePlan::RouteActionReason;
    switch (reason) {
    case RAR::NONE:
        return "none";
    case RAR::EXTENDED_PROTOCOL:
        return "extended_protocol";
    case RAR::IN_TRANSACTION:
        return "in_transaction";
    case RAR::MULTI_STATEMENT:
        return "multi_statement";
    case RAR::READ_TARGET_PRIMARY:
        return "read_target_primary";
    case RAR::HINT_PRIMARY:
        return "hint_primary";
    case RAR::WRITE_LSN_UNKNOWN:
        return "write_lsn_unknown";
    case RAR::OBSERVED_LSN_UNKNOWN:
        return "observed_lsn_unknown";
    case RAR::GROUP_LSN_UNKNOWN:
        return "group_lsn_unknown";
    case RAR::INVALID_POLICY:
        return "invalid_policy";
    case RAR::READER_RFQ_UNAVAILABLE:
        return "reader_rfq_unavailable";
    case RAR::READ_FALLBACK_ERROR:
        return "read_fallback_error";
    case RAR::READER_FAILURE_FORCE_WRITER:
        return "reader_failure_force_writer";
    case RAR::WAL_PENDING:
        return "wal_pending";
    case RAR::SPLIT_BLOCKED:
        return "split_blocked";
    case RAR::SPLIT_NOT_SELECT:
        return "split_not_select";
    case RAR::SPLIT_LOCKING_READ:
        return "split_locking_read";
    case RAR::SPLIT_WRITE_LSN_UNKNOWN:
        return "split_write_lsn_unknown";
    case RAR::SPLIT_OBSERVED_LSN_UNKNOWN:
        return "split_observed_lsn_unknown";
    case RAR::NO_TXN_LSN:
        return "no_txn_lsn";
    case RAR::INVARIANT_VIOLATION:
        return "invariant_violation";
    case RAR::HG_SPLIT_DISABLED:
        return "hg_split_disabled";
    }
    return "unknown";
}

/**
 * @brief Captured outcome of one backend request.
 *
 * Populated only on the failure path. It snapshots the failed backend identity
 * and a few stable state bits before the normal rc==-1 path may release or
 * destroy the connection. Split and full reader-failure recovery will extend how
 * this snapshot is consumed; CSN feedback is intentionally not part of this
 * LSN-only branch.
 */
struct PolarDB_RequestOutcome {
    bool result_started = false;
    bool reusable = false;
    bool connected = false;
    bool timeout_error = false;
    bool timeout_already_accounted = false;
    bool wrapper_set_failure = false;

    int backend_hg = -1;
    std::string backend_address;
    int backend_port = 0;
    int backend_error_code = -1;
    std::string error_message;

    PgSQL_Backend* backend = nullptr;          // non-owning; valid only while the rc==-1 path runs
    PgSQL_Data_Stream* backend_myds = nullptr; // non-owning; valid only while the rc==-1 path runs
};

/**
 * @brief Action returned by the PolarDB rc==-1 failure stage.
 *
 * The rc==-1 handler consumes these before ProxySQL's generic retry/error path.
 */
enum class PolarDB_FailureAction : uint8_t {
    PASSTHROUGH = 0,   // Use ProxySQL's normal rc==-1 path
    RETRY,             // The original query has already been redispatched to the
                       // resolved retry target — another reader or the writer,
                       // chosen by the target resolver. The rc==-1 handler must
                       // not run a retry of its own.
    FORWARD,           // Forward reader error and keep txn alive on writer
    TERMINATE,         // Close the session
};

static inline const char* polardb_failure_action_name(
        PolarDB_FailureAction action) {
    switch (action) {
    case PolarDB_FailureAction::PASSTHROUGH:
        return "passthrough";
    case PolarDB_FailureAction::RETRY:
        return "retry";
    case PolarDB_FailureAction::FORWARD:
        return "forward";
    case PolarDB_FailureAction::TERMINATE:
        return "terminate";
    }
    return "unknown";
}

/**
 * @brief Operator policy applied when PolarDB recovers a replica-reader failure.
 *
 * Public settings use event-specific action enums. This compact internal result
 * tells the common failure executor whether to retry, return an error, or close
 * the client. RETRY carries an explicit retry_target in the decision.
 */
enum class PolarDB_ReaderAction : uint8_t {
    RETRY = 0,
    RETURN_ERROR = 1,
    DISCONNECT_CLIENT = 2
};

/// @brief Reader failure category used before applying operator policy knobs.
enum class PolarDB_ReaderFailureKind : uint8_t {
    CONNECTION_LOST = 0,
    WAIT_TIMEOUT = 1,
    REUSABLE_ERROR = 2
};

/// @brief Destination selected when a reader-failure policy chooses RETRY.
enum class PolarDB_RetryTarget : uint8_t {
    WRITER = 0,
    OTHER_READER = 1
};

/// @brief Transaction-scoped routing state installed after a recovered reader failure.
enum class PolarDB_ReaderFailureRoute : uint8_t {
    NONE = 0,
    FORCE_WRITER = 1,
    SKIP_READER = 2
};

/// @brief State of the writer transaction when a replica-reader failure is seen.
enum class PolarDB_WriterState : uint8_t {
    LIVE = 0,        // A connected writer backend is already inside the txn
    NOT_STARTED,    // BEGIN exists client-side, but no writer backend exists yet
    LOST = 2         // Writer evidence existed but no live writer can be found
};

static inline const char* polardb_reader_action_name(
        PolarDB_ReaderAction action) {
    switch (action) {
    case PolarDB_ReaderAction::RETRY:
        return "retry";
    case PolarDB_ReaderAction::RETURN_ERROR:
        return "error";
    case PolarDB_ReaderAction::DISCONNECT_CLIENT:
        return "disconnect";
    }
    return "unknown";
}

static inline const char* polardb_reader_failure_kind_name(
        PolarDB_ReaderFailureKind kind) {
    switch (kind) {
    case PolarDB_ReaderFailureKind::CONNECTION_LOST:
        return "connection_lost";
    case PolarDB_ReaderFailureKind::WAIT_TIMEOUT:
        return "wait_timeout";
    case PolarDB_ReaderFailureKind::REUSABLE_ERROR:
        return "reusable_error";
    }
    return "unknown";
}

static inline const char* polardb_retry_target_name(
        PolarDB_RetryTarget target) {
    switch (target) {
    case PolarDB_RetryTarget::WRITER:
        return "writer";
    case PolarDB_RetryTarget::OTHER_READER:
        return "other_reader";
    }
    return "unknown";
}

static inline const char* polardb_reader_failure_route_name(
        PolarDB_ReaderFailureRoute route) {
    switch (route) {
    case PolarDB_ReaderFailureRoute::NONE:
        return "none";
    case PolarDB_ReaderFailureRoute::FORCE_WRITER:
        return "force_writer";
    case PolarDB_ReaderFailureRoute::SKIP_READER:
        return "skip_reader";
    }
    return "unknown";
}

/**
 * @brief Byte size of the PostgreSQL NoticeResponse packet for these fields.
 *
 * Used to size the buffer before polardb_write_notice_response_packet() fills
 * it. Each present field adds its type byte, its value, and a NUL terminator. A
 * null field pointer is skipped (the field is omitted from the message).
 */
static inline unsigned int polardb_notice_response_packet_size(
    const char* severity,
    const char* sqlstate,
    const char* primary,
    const char* detail = nullptr,
    const char* severity_nonlocalized = nullptr) {
    unsigned int size = 1 + 4 + 1; // type + length + field-list terminator
    if (severity) size += strlen(severity) + 2; // 'S' + value + NUL
    if (severity_nonlocalized) size += strlen(severity_nonlocalized) + 2; // 'V' + value + NUL
    if (sqlstate) size += strlen(sqlstate) + 2; // 'C' + value + NUL
    if (primary) size += strlen(primary) + 2;   // 'M' + value + NUL
    if (detail) size += strlen(detail) + 2;     // 'D' + value + NUL
    return size;
}

/**
 * @brief Serialize a PostgreSQL NoticeResponse ('N') message into @p pkt.
 *
 * Writes a wire-format NoticeResponse with optional S/V/C/M/D fields. The
 * length prefix counts itself but excludes the leading type byte, per protocol.
 *
 * @param pkt      Destination buffer.
 * @param capacity Size of @p pkt in bytes.
 * @return Bytes written, or 0 if @p pkt is null or too small for the message.
 */
static inline unsigned int polardb_write_notice_response_packet(
    unsigned char* pkt,
    unsigned int capacity,
    const char* severity,
    const char* sqlstate,
    const char* primary,
    const char* detail = nullptr,
    const char* severity_nonlocalized = nullptr) {
    const unsigned int size = polardb_notice_response_packet_size(
        severity, sqlstate, primary, detail, severity_nonlocalized);
    if (!pkt || capacity < size) {
        return 0;
    }

    unsigned char* write_cursor = pkt;
    *write_cursor++ = 'N';
    const uint32_t packet_len = htonl(size - 1); // includes length, excludes type byte
    memcpy(write_cursor, &packet_len, 4);
    write_cursor += 4;

    auto write_field = [&write_cursor](unsigned char code, const char* value) {
        if (!value) {
            return;
        }
        *write_cursor++ = code;
        const size_t value_len = strlen(value) + 1;
        memcpy(write_cursor, value, value_len);
        write_cursor += value_len;
    };

    write_field('S', severity);
    write_field('V', severity_nonlocalized);
    write_field('C', sqlstate);
    write_field('M', primary);
    write_field('D', detail);
    *write_cursor++ = '\0';
    return size;
}

/**
 * @brief Pick the route-action reason for a query shape that forces the writer.
 *
 * Three query shapes cannot take the replica-with-wait path, so the read goes to
 * the writer. The checks are evaluated in a fixed precedence order and the first
 * match wins:
 *   @p in_transaction     -> IN_TRANSACTION
 *   @p is_multi_statement -> MULTI_STATEMENT
 *   @p is_locking_select  -> SPLIT_LOCKING_READ
 *
 * @param in_transaction     Caller-resolved: the query is in a transaction that
 *                           is not eligible for a transaction wait read. Being
 *                           in a transaction is not on its own disqualifying —
 *                           the caller folds the wait-read eligibility policy
 *                           into this argument.
 * @param is_multi_statement The simple-query buffer holds more than one statement.
 * @param is_locking_select  The query is a SELECT with a locking FOR clause.
 * @return The first matching reason, or NONE when none of the three applies and
 *         the query shape does not by itself require the writer.
 */
static inline PolarDB_Query_RoutePlan::RouteActionReason polardb_writer_required_reason(
    bool in_transaction,
    bool is_multi_statement,
    bool is_locking_select) {
    using RAR = PolarDB_Query_RoutePlan::RouteActionReason;
    if (in_transaction) return RAR::IN_TRANSACTION;
    if (is_multi_statement) return RAR::MULTI_STATEMENT;
    if (is_locking_select) return RAR::SPLIT_LOCKING_READ;
    return RAR::NONE;
}

/**
 * @brief Return true when a simple-query buffer contains more than one statement.
 *
 * PgSQL_Query_Info stores PostgreSQL simple-query text with the wire terminator
 * included in QueryLength. Treat that trailing NUL like trailing whitespace,
 * then ignore one final semicolon before scanning for any remaining semicolon.
 * The remaining separator means the wait wrapper would receive more user result
 * sets than it can safely account for, so the query must stay on the writer.
 */
static inline bool polardb_query_has_multiple_statements(
    const char* query,
    size_t query_len) {
    if (!query || query_len == 0) {
        return false;
    }

    const char* query_begin = query;
    const char* query_end = query + query_len;

    while (query_begin < query_end &&
            isspace(static_cast<unsigned char>(*query_begin))) {
        query_begin++;
    }
    while (query_end > query_begin &&
            (*(query_end - 1) == '\0' ||
                isspace(static_cast<unsigned char>(*(query_end - 1))))) {
        query_end--;
    }
    if (query_end > query_begin && *(query_end - 1) == ';') {
        query_end--;
    }

    return memchr(query_begin, ';', query_end - query_begin) != nullptr;
}

/// Result of polardb_execute(). Owns the final hostgroup after any execute-time
/// fallback (execute may override plan.target_hg).
struct PolarDB_Query_ExecuteResult {
    int final_target_hg = -1;
    bool return_error = false;
};

#endif // POLARDB_PROXY

#endif // __CLASS_PGSQL_POLARDB_H
