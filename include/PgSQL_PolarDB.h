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
 *     `SET polar_xact_split_wait_lsn` wait gate) on replica-eligible autocommit
 *     reads
 *
 * Build switch: everything PolarDB-specific is compiled only under
 * POLARDB_PROXY (see Makefiles). With POLARDB_PROXY=0 the feature compiles to
 * no-op stubs in PgSQL_PolarDB_Stubs.cpp, leaving ProxySQL behavior unchanged.
 *
 * This header contains the complete LSN session-consistency surface: monitor
 * health parsing, per-server LSN cache helpers, query wait state, routing
 * context/plan types, wait wrapping, notice handling, and response-side LSN
 * result processing. A replica read is allowed only when its required wait prefix can
 * be attached before dispatch.
 */

#ifndef __CLASS_PGSQL_POLARDB_H
#define __CLASS_PGSQL_POLARDB_H

class PgSQL_Connection;
class PgSQL_Backend;
class PgSQL_Data_Stream;
class PgSQL_SrvC;

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
//      [S...]  session RC handling               rc disposition after RunQuery
//      [W...]  PgSQL_Result_to_PgSQL_wire()      ENTRY (wait_active / stage / type / write_lsn)
//    End-to-end order for a wrapped LSN read:
//      [H] connect -> [RQ] query -> PLAN -> WRAP -> [H] SET-skip -> [W] wire result -> [S] rc
//    The per-call counter/local setup stays under #if POLARDB_PROXY && POLARDB_DEBUG
//    (gating that trace-only state); the log call itself uses POLARDB_TRACE.
//
// POLARDB_DEBUG defaults OFF for production.
#ifndef POLARDB_DEBUG
#define POLARDB_DEBUG 0
#endif

// POLARDB_PROFILE defaults OFF for production. Enable it for benchmark builds
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
#include <cstdio>   // sscanf in parse_lsn_string
#include <cstdlib>  // std::getenv in polardb_debug_consume_fault_file
#include <cstring>  // strcasecmp in parse_node_type / parse_is_available
#include <memory>
#include <netinet/in.h>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <utility>

// PolarDB type suffix contract.
//
// Keep new structs aligned with these lifecycle meanings:
//   *Ctx    : immutable collected input for one stage; never mutable runtime state.
//   *Plan   : planner/helper output describing how to perform an action.
//   *Spec   : immutable payload format; no runtime lifecycle or routing decision.
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

static inline bool polardb_zero_lsn_payload_can_skip_wait_target(
        const char* query) {
    // A PolarDB RFQ LSN payload can be present with value 0 before the backend has
    // a useful session WAL position. That is valid for read-only and session-state
    // statements, but not for DML/DDL where the session must fail closed if no
    // usable write LSN was reported. SELECT/SHOW/EXPLAIN are intentionally not in
    // this list: the caller already knows ordinary reads are safe, and locking
    // SELECT statements must be handled as writes.
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
// Everything below is gated by `#if POLARDB_PROXY && POLARDB_DEBUG` and exists
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
//             polardb_debug_once_enabled in PgSQL_PolarDB_Failure.cpp).
//
//   (B) File-based fault files — re-armable without restart. Write a value into
//       the file path named by the env var; the fault fires once and the file is
//       cleared (truncated) so it re-arms when the test writes to it again. All
//       file mechanics (read first line, strip trailing CR/LF, then clear the
//       file once the caller confirms a match) are shared via
//       polardb_debug_consume_fault_file() below; each call site owns its own
//       match/parse of the consumed line. The clear is match-limited (the helper
//       clears only when the caller signals a hit), so a fault file that several
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
//             polardb_debug_once_enabled in PgSQL_PolarDB_Failure.cpp).
//
//         POLARDB_DEBUG_SPLIT_FAILURE_FAULT_FILE
//             Line == one of: "death" | "sql_error" | "result_started" |
//             "no_retry_packet" | "writer_busy" | "writer_lost" |
//             "writer_not_started" -> force that transaction-split
//             reader-failure branch. "death_twice" injects two reader deaths
//             in one statement to verify the reader-retry budget falls back to
//             the writer instead of oscillating between replicas.
//             (PgSQL_PolarDB_Failure.cpp: polardb_debug_split_failure_fault_is)
//
//         POLARDB_DEBUG_READER_ACQUIRE_FAULT_FILE
//             Line == "reader_busy" | "reader_lsn_unknown" -> force that reader
//             acquisition status. (PgSQL_HostGroups_Manager.cpp:
//             polardb_debug_reader_acquire_fault)
//
//         POLARDB_DEBUG_STARTUP_IDENTITY_FILE
//             Line == any non-empty token ("none" | "listener_proxy" |
//             "configured_fallback" are the ones the caller acts on) -> force
//             that startup-identity resolution outcome. (PgSQL_Connection.cpp:
//             polardb_debug_startup_identity_fault)
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
 * Shared by all five file-based fault hooks so their read/strip semantics are
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
 * The clear is deliberately NOT done here: each original injector truncated the
 * file only on a match, and some files are read by several candidate probes in a
 * row (wait-retry, reader-acquire), so a non-matching probe must leave the file
 * intact for the matching one. The caller calls polardb_debug_clear_fault_file()
 * after a successful match, exactly as before.
 *
 * @param env_name  Name of the env var holding the fault-file path.
 * @param line_out  Caller buffer to receive the (CR/LF-stripped) first line.
 * @param line_sz   Size of @p line_out in bytes.
 * @return true if a line was read; false if the env var was unset/empty, the
 *         file could not be opened, or no line could be read. On false,
 *         @p line_out is left as the caller set it.
 */
static inline bool polardb_debug_consume_fault_file(
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
 * @brief Truncate (clear) a fault file so its one-shot re-arms.
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
#endif // POLARDB_PROXY && POLARDB_DEBUG

#ifndef POLARDB_XLOGREC_PTR_DEFINED
#define POLARDB_XLOGREC_PTR_DEFINED
// PostgreSQL WAL position type. PolarDB reports it as a 64-bit value in the
// extended ReadyForQuery message; we carry it through unchanged.
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
 * @brief Wait behavior when the target LSN is not yet visible on the replica.
 */
enum class PolarDB_WaitMode : uint8_t {
    BEST_EFFORT = 1,     // Wait up to timeout, then return stale data
    STRICT = 2           // Wait up to timeout, then error if not reached
};

/**
 * @brief Policy for reads when ProxySQL cannot build an RFQ-derived wait target.
 *
 * Values match the hot-path integer mapping for
 * pgsql-polardb_route_rfq_policy.
 */
enum class PolarDB_RfqRoutePolicy : uint8_t {
    BEST_EFFORT = 1,     // Route to a reader without a wait and record degradation
    STRICT = 2           // Use the writer when no RFQ wait target can be enforced
};

/**
 * @brief Outcome of acquiring a reader connection for an LSN-targeted read.
 *
 * Each value names the routing fact the caller acts on. Only consistency-specific
 * failures redirect this query to the writer. Plain availability or capacity
 * failures keep ProxySQL's normal no-connection retry behavior.
 */
enum class PolarDB_ReaderStatus : uint8_t {
    ACQUIRED = 0,           // Got a usable reader connection
    READER_UNAVAILABLE,     // No reader online/usable (plain availability)
    READER_BUSY,            // Readers exist but all are at capacity (plain capacity)
    RFQ_UNAVAILABLE,        // No free reader with an RFQ-LSN startup profile
    PRIMARY_LSN_UNKNOWN,    // Lag cap on, but the primary LSN sample is missing
    READER_LSN_UNKNOWN,     // Lag cap on, but the reader LSN sample is missing
    READER_LSN_STALE,       // Lag cap on, but the reader LSN sample is too old to trust
    READER_LAG_EXCEEDED,    // Reader is further behind the primary than the byte cap allows
};

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
    case PolarDB_ReaderStatus::RFQ_UNAVAILABLE:
        return "rfq_unavailable";
    case PolarDB_ReaderStatus::PRIMARY_LSN_UNKNOWN:
        return "primary_lsn_unknown";
    case PolarDB_ReaderStatus::READER_LSN_UNKNOWN:
        return "reader_lsn_unknown";
    case PolarDB_ReaderStatus::READER_LSN_STALE:
        return "reader_lsn_stale";
    case PolarDB_ReaderStatus::READER_LAG_EXCEEDED:
        return "reader_lag_exceeded";
    }
    return "unknown";
}

/**
 * @brief Whether this reader status should send the query to the writer instead.
 *
 * Only consistency/safety failures (a missing or stale LSN sample, or lag over
 * the cap) redirect the read to the writer. Plain availability or capacity
 * failures return false here so the query keeps ProxySQL's normal
 * no-connection retry behavior instead of being pinned to the writer.
 */
static inline bool polardb_reader_status_redirects_to_writer(
    PolarDB_ReaderStatus status) {
    switch (status) {
    case PolarDB_ReaderStatus::PRIMARY_LSN_UNKNOWN:
    case PolarDB_ReaderStatus::READER_LSN_UNKNOWN:
    case PolarDB_ReaderStatus::READER_LSN_STALE:
    case PolarDB_ReaderStatus::READER_LAG_EXCEEDED:
        return true;
    default:
        return false;
    }
}

/**
 * @brief Initial SESSION_LSN target source for a first read-only session.
 *
 * Values match the hot-path integer mapping for
 * pgsql-polardb_session_lsn_baseline.
 */
enum class PolarDB_SessionLsnBaseline : uint8_t {
    OBSERVED = 1,        // Use only positions observed by this client session
    PRIMARY = 2          // Seed first read target from the writer mirror
};

/// @brief Map a configured int to PolarDB_RfqRoutePolicy; anything not
/// BEST_EFFORT is treated as STRICT, the safe default that routes to
/// the writer when no RFQ wait target can be enforced.
static inline PolarDB_RfqRoutePolicy polardb_rfq_route_policy_from_int(int v) {
    return v == static_cast<int>(PolarDB_RfqRoutePolicy::BEST_EFFORT)
        ? PolarDB_RfqRoutePolicy::BEST_EFFORT
        : PolarDB_RfqRoutePolicy::STRICT;
}

/// @brief Map a configured int to PolarDB_SessionLsnBaseline; anything not
/// PRIMARY is treated as OBSERVED (the session-only default).
static inline PolarDB_SessionLsnBaseline polardb_session_lsn_baseline_from_int(int v) {
    return v == static_cast<int>(PolarDB_SessionLsnBaseline::PRIMARY)
        ? PolarDB_SessionLsnBaseline::PRIMARY
        : PolarDB_SessionLsnBaseline::OBSERVED;
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
 * Selects which PolarDB-specific keys ProxySQL sends in the startup packet so
 * the backend knows it is talking to a proxy and should append the RFQ LSN.
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
 * sent in the startup packet. It states intent only: a requested RFQ-LSN bit is
 * not proof the backend will return LSNs. That is confirmed later when result
 * RFQs actually carry an LSN.
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
    bool has_rfq_lsn() const {
        return requests(REQUEST_RFQ_LSN);
    }

    /// @brief Add the transaction XID RFQ request to a non-OFF profile.
    void request_rfq_xid() {
        if (protocol != PolarDB_ProxyProtocol::OFF) {
            request_bits |= REQUEST_RFQ_XID;
        }
    }

    /// @brief True if this profile asked the backend to append transaction XIDs.
    bool has_rfq_xid() const {
        return requests(REQUEST_RFQ_XID);
    }

    /// @brief True if this profile causes any PolarDB startup keys to be sent.
    bool emits_startup_params() const {
        return protocol != PolarDB_ProxyProtocol::OFF && request_bits != 0;
    }
};

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

    bool compatible_for_reuse_strict(
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

    bool compatible_for_reuse(const PolarDB_StartupClientContext& other) const {
        return compatible_for_reuse_strict(other);
    }
};

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
static constexpr uint32_t POLARDB_WAIT_WRAPPER_SET_COUNT = 3;  // mode SET + timeout SET + wait SET
static constexpr uint32_t POLARDB_TXN_SPLIT_WRAPPER_SET_COUNT =
    POLARDB_WAIT_WRAPPER_SET_COUNT + 1;  // XID SET + wait-wrapper SETs
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
 *      inherit pgsql-polardb_lag_wait_ms.
 *
 * The global pgsql-polardb_lag_wait_ms default is 1000 ms. Setting the global
 * value to 0 is an explicit request to wait indefinitely for HGs that inherit it.
 *
 * PolarDB backend behavior:
 *  - polar_proxy_wait_timeout_ms > 0:
 *      wait up to that many milliseconds. If the target is still not reached,
 *      polar_consistency_mode decides the result: best_effort emits WARNING and
 *      returns stale data; strict raises ERROR and aborts the query.
 *  - polar_proxy_wait_timeout_ms == 0:
 *      the timeout check is disabled. The query waits until the replica reaches
 *      the requested LSN. In this case best_effort and strict behave the same for
 *      the wait itself, because the timeout branch is never reached.
 *
 * Operational note:
 *  - Indefinite waits are strongest for RYW but can hold a client and backend
 *    connection if the replica is stuck or far behind. Use max_lag_bytes and/or
 *    statement_timeout when configuring timeout 0 in production.
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
 * global. Values match POLARDB_CONSISTENCY_* in PgSQL_Thread.h; unsupported
 * integer values map to OFF.
 */
enum class PolarDB_ConsistencyMode : uint8_t {
    OFF = 0,            // No consistency routing; reads go to default hostgroup
    SESSION_LSN = 1,    // Track per-session write LSN, wait on replica before reads
    PRIMARY_ONLY = 3    // Route all reads to primary (no replica reads)
};

/// @brief Convert a resolved consistency-mode int (session > HG > global) to the
/// typed enum. Must NOT be called on the unresolved -1 sentinel (asserts v >= 0).
/// Any unsupported value maps to OFF.
inline PolarDB_ConsistencyMode polardb_consistency_from_int(int v) {
    assert(v >= 0);
    switch (v) {
        case static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN):
            return PolarDB_ConsistencyMode::SESSION_LSN;
        case static_cast<int>(PolarDB_ConsistencyMode::PRIMARY_ONLY):
            return PolarDB_ConsistencyMode::PRIMARY_ONLY;
        default:
            return PolarDB_ConsistencyMode::OFF;
    }
}

/// @brief Map a config string ("off"/"lsn"/"primary") to the consistency-mode
/// int. Null, empty, "default", or any unknown value returns @p default_value.
static inline int polardb_consistency_mode_from_string(
    const char* value,
    int default_value) {
    if (!value || value[0] == '\0' || strcasecmp(value, "default") == 0) {
        return default_value;
    }
    if (strcasecmp(value, "off") == 0) {
        return static_cast<int>(PolarDB_ConsistencyMode::OFF);
    }
    if (strcasecmp(value, "lsn") == 0) {
        return static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN);
    }
    if (strcasecmp(value, "primary") == 0) {
        return static_cast<int>(PolarDB_ConsistencyMode::PRIMARY_ONLY);
    }
    return default_value;
}

/**
 * @brief Session policy for when transaction-split reader warmup is requested.
 *
 * Split execution never opens a socket on the query path: it only borrows an
 * already-pooled compatible reader. This mode controls when the session asks the
 * HGM maintenance pass to create that pooled reader in the background.
 */
enum class PolarDB_TxnSplitWarmupMode : uint8_t {
    OFF = 0,     // Never queue warmup from this session
    DEMAND = 1,  // Queue after a split-readable transaction read misses the pool
    BEGIN = 2,   // Queue at BEGIN / START TRANSACTION only
    BOTH = 3     // Queue at BEGIN and also after a later pool miss
};

/**
 * @brief How split warmup matches pooled reader startup identity.
 *
 * strict keeps the backend-visible client host/port exact. The looser modes are
 * opt-in throughput modes for deployments that do not depend on per-client
 * backend-visible identity. SSL/session-id metadata always forces strict
 * matching because those fields describe one frontend connection.
 */
enum class PolarDB_SplitWarmupIdentity : uint8_t {
    STRICT = 0,       // user/db + exact startup identity
    CLIENT_IP = 1,    // user/db + client IP/source, ignore client port
    AUTH_PROFILE = 2  // user/db only, unless strict metadata is present
};

static inline int polardb_split_warmup_identity_from_string(
    const char* value,
    int default_value) {
    if (!value || value[0] == '\0' || strcasecmp(value, "default") == 0) {
        return default_value;
    }
    if (strcasecmp(value, "strict") == 0) {
        return static_cast<int>(PolarDB_SplitWarmupIdentity::STRICT);
    }
    if (strcasecmp(value, "client_ip") == 0 ||
            strcasecmp(value, "client-ip") == 0) {
        return static_cast<int>(PolarDB_SplitWarmupIdentity::CLIENT_IP);
    }
    if (strcasecmp(value, "auth_profile") == 0 ||
            strcasecmp(value, "auth-profile") == 0) {
        return static_cast<int>(PolarDB_SplitWarmupIdentity::AUTH_PROFILE);
    }
    return default_value;
}

static inline const char* polardb_split_warmup_identity_name(int mode) {
    switch (mode) {
    case static_cast<int>(PolarDB_SplitWarmupIdentity::STRICT):
        return "strict";
    case static_cast<int>(PolarDB_SplitWarmupIdentity::CLIENT_IP):
        return "client_ip";
    case static_cast<int>(PolarDB_SplitWarmupIdentity::AUTH_PROFILE):
        return "auth_profile";
    default:
        return "strict";
    }
}

static inline bool polardb_startup_client_compatible_for_warmup(
        const PolarDB_StartupClientContext& pooled,
        const PolarDB_StartupClientContext& requested,
        int mode) {
    // SSL and proxy session-id fields identify one frontend connection, not a
    // reusable auth profile. When either side has them, loose warmup modes are
    // deliberately ignored.
    if (pooled.has_strict_metadata() || requested.has_strict_metadata()) {
        return pooled.compatible_for_reuse_strict(requested);
    }
    switch (mode) {
    case static_cast<int>(PolarDB_SplitWarmupIdentity::AUTH_PROFILE):
        return true;
    case static_cast<int>(PolarDB_SplitWarmupIdentity::CLIENT_IP):
        return pooled.identity.source == requested.identity.source &&
            pooled.identity.host == requested.identity.host;
    case static_cast<int>(PolarDB_SplitWarmupIdentity::STRICT):
    default:
        return pooled.compatible_for_reuse_strict(requested);
    }
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

/// @brief Map a config string ("best_effort"/"strict") to the wait-mode int.
/// A null or unknown value returns @p default_value.
static inline int polardb_wait_mode_from_string(
    const char* value,
    int default_value) {
    if (!value) {
        return default_value;
    }
    if (strcasecmp(value, "best_effort") == 0) {
        return static_cast<int>(PolarDB_WaitMode::BEST_EFFORT);
    }
    if (strcasecmp(value, "strict") == 0) {
        return static_cast<int>(PolarDB_WaitMode::STRICT);
    }
    return default_value;
}

/// @brief Map a config string ("best_effort"/"strict") to the route-policy int.
/// A null or unknown value returns @p default_value (STRICT, the safe
/// default that does not degrade without an explicit best_effort policy).
static inline int polardb_route_rfq_policy_from_string(
    const char* value,
    int default_value = static_cast<int>(PolarDB_RfqRoutePolicy::STRICT)) {
    if (!value) {
        return default_value;
    }
    if (strcasecmp(value, "best_effort") == 0) {
        return static_cast<int>(PolarDB_RfqRoutePolicy::BEST_EFFORT);
    }
    if (strcasecmp(value, "strict") == 0) {
        return static_cast<int>(PolarDB_RfqRoutePolicy::STRICT);
    }
    return default_value;
}

/// @brief Map a config string ("primary"/"observed") to the baseline int.
/// A null or unknown value returns @p default_value (OBSERVED, the
/// session-only default).
static inline int polardb_session_lsn_baseline_from_string(
    const char* value,
    int default_value = static_cast<int>(PolarDB_SessionLsnBaseline::OBSERVED)) {
    if (!value) {
        return default_value;
    }
    if (strcasecmp(value, "primary") == 0) {
        return static_cast<int>(PolarDB_SessionLsnBaseline::PRIMARY);
    }
    if (strcasecmp(value, "observed") == 0) {
        return static_cast<int>(PolarDB_SessionLsnBaseline::OBSERVED);
    }
    return default_value;
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
 *  - NONE: the helper has no route requirement; the planner keeps deciding.
 *  - PRIMARY: the consistency mode itself requires the writer.
 *  - REPLICA: the helper allows a reader if the planner's later checks allow
 *    it. This is informational; it is not the final route decision.
 */
enum class PolarDB_Query_ConsistencyRouteHint : uint8_t {
    NONE = 0,    // Consistency policy has no routing preference
    PRIMARY,     // Consistency policy requires primary/writer
    REPLICA      // Consistency policy allows a replica with the wait payload
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
    PolarDB_NodeType node_type;
    bool is_available;           ///< false = node in maintenance mode (polar_is_available() = 'f')
    uint64_t current_lsn;        ///< Current WAL position from health check query

    PolarDB_HealthCheck()
        : node_type(PolarDB_NodeType::UNKNOWN)
        , is_available(true)
        , current_lsn(0)
    {}
};

/**
 * @brief Whether an RFQ result that carried an LSN is proven to come from the
 * primary (writer).
 *
 * The caller supplies values from the PolarDB topology snapshot for the backend
 * hostgroup that produced the result. An LSN seen from a replica still advances
 * the session's observed LSN, but only an LSN seen from the writer is allowed to
 * clear the "missing LSN" sticky flags (see PolarDB_SessionConsistency).
 */
static inline bool polardb_positioned_rfq_from_primary(
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
 * @brief Short-lived reader preference proven by this client session.
 *
 * A successful backend wait proves that one reader reached at least
 * last_reached_lsn. Replica replay LSNs only move forward, so the session can
 * safely reuse that reader for later reads whose target is not higher than the
 * proven value, even if the shared server LSN sample has aged out.
 *
 * This is a wait-avoidance hint, not a correctness requirement. If any check
 * fails, reader acquisition falls back to the normal weighted selector.
 */
struct PolarDB_ReaderAffinity {
    int reader_hg = -1;
    std::string address;
    int port = -1;
    uint64_t valid_until_us = 0;
    uint32_t uses_left = 0;
    uint64_t last_reached_lsn = 0;
    PolarDB_WriterScope writer_scope;

    bool active() const {
        return reader_hg >= 0 && port >= 0 && !address.empty() &&
            uses_left > 0 && last_reached_lsn > 0;
    }

    void clear() {
        reader_hg = -1;
        address.clear();
        port = -1;
        valid_until_us = 0;
        uses_left = 0;
        last_reached_lsn = 0;
        writer_scope.reset();
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
 * They share policy handling but keep operator diagnostics distinct.
 */
struct PolarDB_SessionConsistency {
    uint64_t write_lsn = 0;
    uint64_t observed_lsn = 0;
    bool write_unknown = false;
    bool observed_unknown = false;
    PolarDB_WriterScope writer_scope;

    bool has_lsn_state() const {
        return write_lsn != 0 ||
            observed_lsn != 0 ||
            write_unknown ||
            observed_unknown;
    }

    uint64_t target() const {
        return write_lsn > observed_lsn ? write_lsn : observed_lsn;
    }

    /**
     * @brief SESSION_LSN target after applying the first-read baseline.
     *
     * OBSERVED preserves the session-only target. PRIMARY only takes effect for
     * a first read-only session with no known write or observed LSN. The caller
     * supplies the current primary mirror; this method does not read or mutate
     * external state.
     */
    uint64_t target_with_baseline(
        int session_lsn_baseline,
        uint64_t primary_lsn,
        bool* primary_lsn_unknown) const {
        if (primary_lsn_unknown) {
            *primary_lsn_unknown = false;
        }

        uint64_t current = target();
        if (current > 0) {
            return current;
        }

        if (polardb_session_lsn_baseline_from_int(session_lsn_baseline) !=
            PolarDB_SessionLsnBaseline::PRIMARY) {
            return 0;
        }

        if (primary_lsn == 0) {
            if (primary_lsn_unknown) {
                *primary_lsn_unknown = true;
            }
            return 0;
        }

        return primary_lsn;
    }

    void reset_lsn_state() {
        write_lsn = 0;
        observed_lsn = 0;
        write_unknown = false;
        observed_unknown = false;
    }
};

/**
 * @brief Transaction-split lifecycle stage for a client transaction.
 *
 * This records RFQ transaction evidence and the session-side state machine shape.
 * The planner can recognize split-readable state; execution temporarily borrows
 * a replica backend for one split read and then restores the primary backend.
 */
enum class PolarDB_TransactionSplitStage : uint8_t {
    NONE = 0,                   // No split-capable transaction is being tracked
    TXN_ON_PRIMARY = 1,         // Transaction is open on the primary
    TXN_SPLITTABLE = 2,         // Backend RFQ says replica reads may be considered
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
    bool splittable = false;   // RFQ says the transaction can consider split reads
    bool wal_pending = false;  // RFQ says WAL is pending; do not split
    bool blocked = false;      // A prior split fault blocks further split attempts
    bool was_splittable = false; // Transaction was split-readable at least once
    bool did_split = false;      // At least one split read completed in this transaction

    bool active() const {
        return stage != PolarDB_TransactionSplitStage::NONE;
    }

    bool has_backend_evidence() const {
        return !xids.empty() || primary_lsn != 0 || splittable || wal_pending;
    }

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
        if (rfq_xids && rfq_xids[0]) {
            xids = rfq_xids;
        }
        splittable = rfq_splittable;
        wal_pending = rfq_wal_pending;

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
 * @brief Immutable wait payload for one routed query (Spec: payload only).
 *
 * Routing is decided by PolarDB_Query_RoutePlan. This object describes only the
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

    static PolarDB_WaitSpec lsn(
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
 * route_hint is the consistency helper's recommendation only. PRIMARY is a
 * hard requirement from the consistency mode; REPLICA means the helper permits
 * a reader but the planner still decides after query-shape, reader-availability,
 * lag/freshness, and safety checks.
 */
struct PolarDB_Query_WaitPlan {
    PolarDB_WaitSpec spec;
    PolarDB_Query_ConsistencyRouteHint route_hint = PolarDB_Query_ConsistencyRouteHint::NONE; // Helper hint; production routing uses Query_RoutePlan

    bool has_wait() const {
        return spec.has_wait();
    }

    void reset() {
        spec.reset();
        route_hint = PolarDB_Query_ConsistencyRouteHint::NONE;
    }

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
        case PolarDB_ConsistencyMode::PRIMARY_ONLY:
            wait_plan.route_hint = PolarDB_Query_ConsistencyRouteHint::PRIMARY;
            break;
        case PolarDB_ConsistencyMode::SESSION_LSN:
            wait_plan.spec = PolarDB_WaitSpec::lsn(
                session_lsn,
                wait_timeout_ms,
                wait_mode);
            break;
        case PolarDB_ConsistencyMode::OFF:
        default:
            break;
        }

        return wait_plan;
    }
};

/**
 * @brief Wait state for an in-flight query.
 *
 * Prepared from the route plan's wait spec (PolarDB_WaitSpec) before query
 * dispatch. Tracks the query's LSN-wait lifecycle and drives wrapper-result
 * filtering at the connection layer once the wait wrapper prepends SET
 * statements before the user query.
 */
struct PolarDB_Query_WaitState {
    PolarDB_WaitSpec spec;
    PolarDB_WaitStage wait_stage = PolarDB_WaitStage::IDLE; // IDLE or WAITING
    uint32_t wrapper_stmts = 0;      // SET results to skip before the user result
    uint64_t wait_started_at_us = 0;             // Monotonic start time for latency tracking
    bool wrapper_finalized = false;         // True after the wait wrapper is finalized
    bool timeout_error = false;             // True after a structured strict wait-timeout ERROR
    int fallback_writer_hg = -1;            // Writer HG for retrying this failed wait read
    std::string original_query;        // Original user query (captured before wrapping)

    void reset() {
        spec.reset();
        wait_stage = PolarDB_WaitStage::IDLE;
        wrapper_stmts = 0;
        wait_started_at_us = 0;
        wrapper_finalized = false;
        timeout_error = false;
        fallback_writer_hg = -1;
        original_query.clear();
    }

    void prepare_from_spec(const PolarDB_WaitSpec& wait_spec) {
        spec = wait_spec;
        wait_stage = PolarDB_WaitStage::IDLE;
        wrapper_stmts = 0;
        wait_started_at_us = 0;
        wrapper_finalized = false;
        timeout_error = false;
        fallback_writer_hg = -1;
        original_query.clear();
    }
};

/**
 * @brief Actions to apply after an error while processing a wrapped query.
 *
 * This is production accounting policy used by PgSQL_Connection's result loop,
 * kept side-effect-free so unit tests exercise the same decision table without
 * constructing a live connection, session, or PGresult. The caller still owns
 * the actual state mutation and counter updates.
 */
struct PolarDB_WrapperErrorAccounting {
    bool mark_wrapper_failed = false;
    bool mark_timeout_error = false;
    bool account_wait_timeout = false;
};

/**
 * @brief Decide how to account a backend error from a wrapped query.
 *
 * A strict LSN timeout is counted only when the query was wrapped, a PolarDB
 * wait is still active, and the backend supplied the structured timeout marker.
 * A wrapper SET failure is marked when the error happens before all wrapper SET
 * results have been consumed, or when there is no active wait to account.
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
    if (!wait_active) {
        accounting.mark_wrapper_failed = true;
        return accounting;
    }
    if (lsn_timeout) {
        accounting.mark_timeout_error = true;
        accounting.account_wait_timeout = true;
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
 * writer/reader pair. Clearing both the LSN and the freshness timestamp is one
 * operation: leaving either half set would let later reader selection treat a
 * stale position as current.
 */
inline void polardb_reset_server_lsn_cache(
        std::atomic<uint64_t>& current_lsn,
        std::atomic<unsigned long long>& updated_at) {
    current_lsn.store(0, std::memory_order_relaxed);
    updated_at.store(0, std::memory_order_relaxed);
}

// ===========================================================================
// LSN policy decision helpers
// ===========================================================================
//
// Keep these helpers header-inline and side-effect free, matching the core
// ProxySQL pattern used by HostgroupRouting / MonitorHealthDecision. Unit tests
// exercise the same policy code production uses without requiring a
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
 * @brief Whether a cached LSN timestamp is still fresh.
 *
 * LSN-only session consistency currently uses three supported lag controls:
 *
 * 1. LSN byte distance (`polardb_lag_bytes` / per-HG `max_lag_bytes`).
 * 2. Cached-LSN age (`polardb_lsn_freshness_ms`).
 * 3. Wait timeout around `polar_xact_split_wait_lsn`.
 *
 * Millisecond replica lag is intentionally deferred. The PgSQL/PolarDB path does
 * not currently produce a real per-reader time-lag value; the full PolarDB tree
 * has the same gap. Do not treat `polardb_lag_ms` as a supported routing gate
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
 *  - `pgsql-polardb_lag_ms` can then reject readers whose estimated catch-up
 *    time is above the configured cap. Missing samples, stale samples, or zero
 *    replay rate under an enabled cap should reject the reader and let
 *    the caller use the writer.
 */
/// Default for pgsql-polardb_lsn_freshness_ms (max age of a cached per-server
/// LSN to trust), used when that runtime knob is unset or non-positive.
static constexpr int POLARDB_LSN_FRESHNESS_MS_DEFAULT = 5000;

static inline bool polardb_lsn_cache_fresh(uint64_t updated_at_us,
                                           uint64_t now_us,
                                           uint32_t freshness_ms) {
    if (updated_at_us == 0) return false;
    if (now_us < updated_at_us) return true;  // monotonic clock skew guard
    return (now_us - updated_at_us) <= ((uint64_t)freshness_ms * 1000ULL);
}

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
 * Today the supported LSN-only lag gate is `polardb_lag_bytes`, not this helper.
 */
static inline bool polardb_lag_ms_within_cap(uint64_t lag_us, int max_lag_ms) {
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

static inline bool polardb_split_ascii_space(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

static inline const char* polardb_split_skip_leading_space(const char* query) {
    if (!query) {
        return nullptr;
    }
    while (*query && polardb_split_ascii_space(*query)) {
        query++;
    }
    return query;
}

static inline char polardb_split_ascii_lower(char c) {
    return c >= 'A' && c <= 'Z' ? static_cast<char>(c - 'A' + 'a') : c;
}

static inline const char* polardb_split_next_word(
        const char* p, char* out, size_t out_len) {
    if (!p || !out || out_len == 0) {
        return nullptr;
    }
    while (*p && polardb_split_ascii_space(*p)) {
        p++;
    }
    if (!*p) {
        return nullptr;
    }

    size_t len = 0;
    while (p[len] && !polardb_split_ascii_space(p[len]) &&
            p[len] != ';' && p[len] != ',' &&
            p[len] != '(' && p[len] != ')') {
        if (len + 1 < out_len) {
            out[len] = polardb_split_ascii_lower(p[len]);
        }
        len++;
    }
    const size_t copied = len < out_len ? len : out_len - 1;
    out[copied] = '\0';
    return p + len;
}

static inline bool polardb_split_has_locking_for_clause(const char* query) {
    if (!query) {
        return false;
    }

    for (const char* p = query; *p; ++p) {
        const bool left_ok = p == query || polardb_split_ascii_space(*(p - 1));
        if (!left_ok || strncasecmp(p, "FOR", 3) != 0) {
            continue;
        }
        if (p[3] != '\0' && !polardb_split_ascii_space(p[3])) {
            continue;
        }

        char first[8];
        char second[8];
        char third[8];
        const char* next = polardb_split_next_word(p + 3, first, sizeof(first));
        if (!next) {
            return false;
        }
        if (strcmp(first, "update") == 0 || strcmp(first, "share") == 0) {
            return true;
        }

        next = polardb_split_next_word(next, second, sizeof(second));
        if (!next) {
            return false;
        }
        if (strcmp(first, "key") == 0 && strcmp(second, "share") == 0) {
            return true;
        }
        if (strcmp(first, "no") == 0 && strcmp(second, "key") == 0) {
            next = polardb_split_next_word(next, third, sizeof(third));
            return next && strcmp(third, "update") == 0;
        }
    }
    return false;
}

static inline bool polardb_split_starts_with_select(const char* query) {
    query = polardb_split_skip_leading_space(query);
    return query && strncasecmp(query, "SELECT", 6) == 0 &&
        (query[6] == '\0' || polardb_split_ascii_space(query[6]));
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
     * 32 bits.
     */
    static uint64_t parse_lsn_string(const char* lsn_str) {
        if (lsn_str == nullptr) {
            return 0;
        }
        uint32_t logid = 0;
        uint32_t offset = 0;
        int parsed_len = 0;
        // Require a full "logid/offset" parse, then pack PostgreSQL's numeric
        // LSN as logid in the high 32 bits and offset in the low 32 bits.
        if (sscanf(lsn_str, "%X/%X%n", &logid, &offset, &parsed_len) == 2 &&
                lsn_str[parsed_len] == '\0') {
            return (static_cast<uint64_t>(logid) << 32) | offset;
        }
        return 0;
    }

    /**
     * @brief Heuristic write detection for routing decisions.
     *
     * Automatic PolarDB consistency treats SELECT/SHOW/EXPLAIN as reads and
     * everything else as a write. WITH is intentionally conservative: CTEs can
     * hide data-modifying statements, so they record a writer LSN and route like
     * writes in the automatic planner. Treating WITH as a write may over-wait for
     * read-only CTEs, but it preserves read-your-writes after data-modifying CTEs.
     * SELECT ... FOR UPDATE/SHARE is also treated as a write because it takes
     * locks. Manual hard routing (query-rule destination hostgroup or hostgroup
     * hint) bypasses this automatic planner; users may still explicitly send a
     * WITH query to a replica, without a PolarDB RYW guarantee.
     *
     * @param query SQL query text (or digest).
     * @return true if the query likely modifies data.
     */
    static bool is_write_query(const char* query);

    /**
     * @brief True when transaction split may dispatch this query to a replica.
     *
     * Split is deliberately narrower than generic read detection: only a
     * top-level SELECT without a locking FOR clause is eligible. SHOW, EXPLAIN,
     * WITH, and ambiguous shapes stay on the primary until explicitly supported.
     */
    static bool is_txn_split_safe_read(const char* query) {
        return polardb_split_starts_with_select(query) &&
            !is_locking_select_query(query);
    }

    /**
     * @brief Fast split eligibility for the hot routing path.
     *
     * ProxySQL already classified the top-level command while running query
     * rules. Reuse that result instead of scanning the first keyword again; only
     * the row-locking clause check still needs SQL text.
     */
    static bool is_txn_split_safe_select(
            bool is_top_level_select, const char* query) {
        return is_top_level_select && !is_locking_select_query(query);
    }

    /**
     * @brief True for SELECT statements with a PostgreSQL row-locking FOR clause.
     */
    static bool is_locking_select_query(const char* query) {
        query = polardb_split_skip_leading_space(query);
        return polardb_split_starts_with_select(query) &&
            polardb_split_has_locking_for_clause(query);
    }

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

/**
 * @brief Resolve the effective wait timeout in ms from tri-state HG config.
 *
 * HG lsn_wait_timeout_ms semantics:
 *   -1 = inherit global (pgsql_thread___polardb_lag_wait_ms)
 *    0 = wait indefinitely inside the PolarDB wait loop; PostgreSQL
 *        statement_timeout/cancel/terminate can still interrupt the statement
 *   >0 = explicit HG value
 */
uint32_t polardb_resolve_wait_timeout_ms(int hg_timeout_ms);

/**
 * @brief Parse full PolarDB health check result (node_type + availability + LSN).
 *
 * Called from PgSQL_Monitor for POLARDB_CHECK_WITH_LSN_QUERY rows. Centralizes the
 * parsing of polar_node_type(), polar_is_available() and the LSN text column into a
 * PolarDB_HealthCheck.
 *
 * @param node_type_str    Node type string (col 0).
 * @param is_available_str Availability string 't'/'f' (col 1).
 * @param lsn_str          LSN string "logid/offset" (col 2).
 * @param health           Output: populated health structure.
 * @return true (always succeeds; unknown strings get safe defaults).
 */
bool parse_polardb_full_health_check(const char* node_type_str, const char* is_available_str,
                                     const char* lsn_str, PolarDB_HealthCheck& health);

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
    // Observed primary RFQ split state. XIDs are carried separately as a view so
    // collect->plan does not copy the transaction XID string on every query.
    PolarDB_TransactionSplitState transaction_split;
    std::string_view transaction_split_xids;

    // --- 32-bit fields ---
    uint32_t wait_timeout_ms = POLARDB_DEFAULT_WAIT_TIMEOUT_MS; // resolved wait timeout (HG or global)
    int reader_hg = -1;                    // reader hostgroup (-1 if none configured)
    int effective_consistency_mode = 0;    // resolved once: session > HG > global
    int wait_timeout_mode = (int)PolarDB_WaitMode::BEST_EFFORT; // BEST_EFFORT / STRICT
    int route_rfq_policy = (int)PolarDB_RfqRoutePolicy::STRICT; // BEST_EFFORT / STRICT
    int session_lsn_baseline = (int)PolarDB_SessionLsnBaseline::OBSERVED; // OBSERVED / PRIMARY
    int max_lag_bytes = -1;                // from HG policy, -1 = use thread default

    // --- bool fields (packed) ---
    bool is_polar_hg = false;              // from HGM cache (resolved once)
    bool replica_eligible = false;         // from qpo->replica_eligible (policy gate)
    bool is_multi_statement = false;       // semicolon scan (hard safety guard)
    bool is_extended_protocol = false;     // Parse/Bind/Execute: regular ProxySQL routing only; no PolarDB wait wrapper
    bool in_transaction = false;           // session is inside an explicit transaction
    bool txn_split_enabled = false;        // HG policy: request/observe split RFQ and allow planning
    bool is_txn_split_safe_read = false;   // split: top-level SELECT with no locking clause
    bool is_txn_split_locking_read = false; // split: SELECT ... FOR UPDATE/SHARE-style lock
    bool txn_reader_wait_isolation_read_committed = true; // pre-write reader waits require READ COMMITTED
    bool txn_reader_wait_local_state_clean = true; // false after in-txn SET/SET LOCAL
    bool force_primary_hint = false;       // /* route=primary */ first-comment hint (plan L0)
    // Transaction-scoped pin set after a split-reader failure. Checked by the
    // normal planner plus manual/qpo paths that may bypass planning.
    bool txn_force_writer_after_reader_failure = false;
    int txn_writer_hg = -1;
};

/**
 * @brief Per-query inputs for picking a reader connection (Plan).
 *
 * Built by the planner and consumed when a reader connection is acquired. It
 * carries the consistency target LSN so acquisition can prefer a reader whose
 * cached replay LSN already reaches it, plus the optional byte-lag safety cap
 * and the fallback policy for when no suitable reader is available.
 *
 * The consistency target is normally enforced by the
 * SET polar_xact_split_wait_lsn statement on the wire. Backend acquisition may
 * skip that wrapper only after the selected reader is proven to have a fresh
 * cached LSN at or beyond this target.
 */
struct PolarDB_Query_ReaderPlan {
    uint64_t consistency_target_lsn = 0;   // target LSN for this read; 0 = no target
    uint64_t primary_lsn = 0;    // writer position, for the byte-lag cap
    int max_lag_bytes = -1;      // reader lag cap in WAL bytes; <=0 = disabled
    int fallback_writer_hg = -1; // writer hostgroup to fall back to
    int route_rfq_policy = (int)PolarDB_RfqRoutePolicy::STRICT;
    bool allow_best_effort_degrade = true;

    /// @brief True if this read has a consistency target LSN.
    bool has_consistency_target_lsn() const { return consistency_target_lsn > 0; }
    /// @brief True if the byte-lag safety cap is enabled for this query.
    bool lag_cap_enabled() const { return max_lag_bytes > 0; }
    /// @brief True if this reader's cached LSN reaches the consistency target.
    bool reader_lsn_reaches_consistency_target(uint64_t reader_lsn) const {
        return consistency_target_lsn > 0 && reader_lsn >= consistency_target_lsn;
    }
    /**
     * @brief True if this replica is within the byte-lag cap behind the primary.
     *
     * Rejects the reader when the cap is enabled but either LSN sample is missing (0):
     * an unknown gap must not be treated as acceptable. A replica at or ahead of
     * the recorded primary position is always within the cap.
     */
    bool within_byte_cap(uint64_t replica_lsn) const {
        if (max_lag_bytes <= 0) return true;
        if (primary_lsn == 0 || replica_lsn == 0) return false;
        if (primary_lsn <= replica_lsn) return true;
        return (primary_lsn - replica_lsn) <= (uint64_t)max_lag_bytes;
    }
    void reset() {
        consistency_target_lsn = 0;
        primary_lsn = 0;
        max_lag_bytes = -1;
        fallback_writer_hg = -1;
        route_rfq_policy = (int)PolarDB_RfqRoutePolicy::STRICT;
        allow_best_effort_degrade = true;
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
    PolarDB_Query_WaitState wait;
    std::string wrapped_query_buf;
    uint8_t reader_retry_attempts = 0;
    uint32_t dispatch_wrapper_stmts = 0;
    PolarDB_Query_WrapperKind dispatch_wrapper_kind =
        PolarDB_Query_WrapperKind::NONE;

    void reset_reader_target() {
        reader_plan.reset();
    }

    void reset_wait() {
        wait.reset();
    }

    void reset_dispatch_wrapper() {
        dispatch_wrapper_stmts = 0;
        dispatch_wrapper_kind = PolarDB_Query_WrapperKind::NONE;
    }

    void reset_for_new_query() {
        reset_reader_target();
        request_writer_scope.reset();
        reset_wait();
        wrapped_query_buf.clear();
        reader_retry_attempts = 0;
        reset_dispatch_wrapper();
    }
};

/**
 * @brief Outcome of acquiring a reader connection (Result).
 *
 * Returned by the HostGroups_Manager reader-acquisition path. On success it owns
 * the chosen connection and server; on failure @ref status says why, which the
 * caller maps to either a writer fallback or ProxySQL's normal no-connection
 * retry (see PolarDB_ReaderStatus and polardb_reader_status_redirects_to_writer).
 */
struct PolarDB_ReaderResult {
    PgSQL_Connection* conn = nullptr;
    PgSQL_SrvC* srv = nullptr;
    PolarDB_ReaderStatus status =
        PolarDB_ReaderStatus::READER_UNAVAILABLE;
    bool wait_bypass_allowed = false;  // selected reader already reached consistency target

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
    /// @brief What the planner decided to do with this query.
    enum class RouteAction : uint8_t {
        PASSTHROUGH = 0,       // No PolarDB override, use the default/target HG
        REPLICA_WITH_WAIT,     // Route to a replica and prepend SET polar_xact_split_wait_lsn
        FORCE_PRIMARY,         // Override to primary (consistency or safety reason)
        REPLICA_TXN_SPLIT,     // In-transaction read dispatched to a replica with XIDs + LSN wait
    };

    /// @brief Why a replica route was rejected (recorded on a FORCE_PRIMARY plan).
    enum class RouteActionReason : uint8_t {
        NONE = 0,
        EXTENDED_PROTOCOL,     // extended-protocol read has a session wait target; the wait wrapper is simple-query only
        IN_TRANSACTION,        // inside an explicit transaction; LSN consistency applies to autocommit reads only
        MULTI_STATEMENT,       // multi-statement read — never offloaded to a replica
        MODE_PRIMARY,          // consistency mode = PRIMARY
        HINT_PRIMARY,          // /* route=primary */ per-query hint
        WRITE_LSN_UNKNOWN,     // a prior write completed but RFQ carried no LSN
        OBSERVED_LSN_UNKNOWN,  // a prior tracked read completed but RFQ carried no LSN
        PRIMARY_LSN_UNKNOWN,   // primary baseline requested but writer mirror is unknown
        READER_FAILURE_FORCE_WRITER, // full retry: rest of txn is pinned to writer after reader failure
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
    bool degraded_rfq_route = false;       // best-effort route without enforceable RFQ target
    bool txn_wait_read = false;            // pre-write in-transaction read using a temporary reader backend

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
        plan.reader.consistency_target_lsn = wait_spec.target;
        plan.reader.fallback_writer_hg = writer_hg;
        return plan;
    }

    /**
     * @brief Build the plan for when no enforceable RFQ wait target is available.
     *
     * The route_rfq_policy decides the fallback: BEST_EFFORT routes to the reader
     * with no wait and marks the plan @ref degraded_rfq_route; STRICT (and any
     * caller that disallows degrading) routes to the writer instead of using
     * a reader without an enforceable wait target. STRICT is the safe default.
     */
    static PolarDB_Query_RoutePlan rfq_unavailable(
        RouteActionReason reason,
        int route_rfq_policy,
        int writer_hg,
        int reader_hg,
        bool allow_best_effort_degrade = true) {
        PolarDB_Query_RoutePlan plan;
        plan.action_reason = reason;

        if (polardb_rfq_route_policy_from_int(route_rfq_policy) ==
            PolarDB_RfqRoutePolicy::BEST_EFFORT &&
            allow_best_effort_degrade) {
            plan.action = RouteAction::PASSTHROUGH;
            plan.target_hg = reader_hg;
            plan.degraded_rfq_route = true;
        } else {
            plan.action = RouteAction::FORCE_PRIMARY;
            plan.target_hg = writer_hg;
        }

        return plan;
    }
};

/**
 * @brief Explain why the current transaction cannot be planned as a split read.
 *
 * NONE means the observed transaction state has enough RFQ evidence for the
 * planner to name a split-read route. The executor may still decline it if a
 * suitable replica connection cannot be obtained for this query.
 */
static inline PolarDB_Query_RoutePlan::RouteActionReason polardb_txn_split_rejection_reason(
    bool txn_split_enabled,
    const PolarDB_TransactionSplitState& transaction_split,
    std::string_view transaction_split_xids,
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
    if (transaction_split_xids.empty()) return RAR::INVARIANT_VIOLATION;
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
    case RAR::MODE_PRIMARY:
        return "mode_primary";
    case RAR::HINT_PRIMARY:
        return "hint_primary";
    case RAR::WRITE_LSN_UNKNOWN:
        return "write_lsn_unknown";
    case RAR::OBSERVED_LSN_UNKNOWN:
        return "observed_lsn_unknown";
    case RAR::PRIMARY_LSN_UNKNOWN:
        return "primary_lsn_unknown";
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
 * Stage-1 extraction uses this only for the failure path. It snapshots the
 * failed backend identity and a few stable state bits before normal rc==-1
 * handling may release or destroy the connection. Split and full reader-failure
 * recovery will extend how this snapshot is consumed; CSN feedback is
 * intentionally not part of this LSN-only branch.
 */
struct PolarDB_RequestOutcome {
    bool ok = false;
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

    PgSQL_Backend* backend = nullptr;          // non-owning; valid only during rc==-1 handling
    PgSQL_Data_Stream* backend_myds = nullptr; // non-owning; valid only during rc==-1 handling
};

/**
 * @brief Action returned by the PolarDB rc==-1 failure stage.
 *
 * The rc==-1 handler consumes these before ProxySQL's generic retry/error path.
 */
enum class PolarDB_FailureAction : uint8_t {
    PASSTHROUGH = 0,   // Keep existing ProxySQL rc==-1 handling
    RETRY,             // Redispatch the original query on the writer
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
 * @brief Operator policy for a handled replica-reader failure.
 *
 * The failure class picks which configured knob applies: non-reusable/dead
 * reader, strict wait timeout, or reusable SQL error. RETRY means "try a safe
 * redispatch target"; the target resolver chooses writer vs another reader
 * from the failure kind and query shape.
 */
enum class PolarDB_ReaderAction : uint8_t {
    RETRY = 0,
    FORWARD = 1,
    TERMINATE = 2
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

/// @brief Transaction-scoped route pin installed after handled reader failures.
enum class PolarDB_RoutePin : uint8_t {
    NONE = 0,
    FORCE_WRITER = 1,
    SHUN_READER = 2
};

/// @brief State of the writer transaction when a replica-reader failure is seen.
enum class PolarDB_WriterState : uint8_t {
    LIVE = 0,        // A connected writer backend is already inside the txn
    NOT_STARTED,    // BEGIN exists client-side, but no writer backend exists yet
    LOST = 2         // Writer evidence existed but no live writer can be found
};

static inline int polardb_reader_action_from_string(
    const char* value,
    int default_value) {
    if (!value) {
        return default_value;
    }
    if (strcasecmp(value, "retry") == 0) {
        return static_cast<int>(PolarDB_ReaderAction::RETRY);
    }
    if (strcasecmp(value, "forward") == 0) {
        return static_cast<int>(PolarDB_ReaderAction::FORWARD);
    }
    if (strcasecmp(value, "terminate") == 0) {
        return static_cast<int>(PolarDB_ReaderAction::TERMINATE);
    }
    return default_value;
}

static inline const char* polardb_reader_action_name(
        PolarDB_ReaderAction action) {
    switch (action) {
    case PolarDB_ReaderAction::RETRY:
        return "retry";
    case PolarDB_ReaderAction::FORWARD:
        return "forward";
    case PolarDB_ReaderAction::TERMINATE:
        return "terminate";
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

static inline const char* polardb_route_pin_name(PolarDB_RoutePin pin) {
    switch (pin) {
    case PolarDB_RoutePin::NONE:
        return "none";
    case PolarDB_RoutePin::FORCE_WRITER:
        return "force_writer";
    case PolarDB_RoutePin::SHUN_READER:
        return "shun_reader";
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
 * An explicit transaction or a multi-statement query cannot take the
 * replica-with-wait path, so the read goes to the writer. Returns the matching
 * reason for diagnostics (transaction is reported first), or NONE if neither
 * applies.
 */
static inline PolarDB_Query_RoutePlan::RouteActionReason polardb_writer_required_reason(
    bool in_transaction,
    bool is_multi_statement) {
    using RAR = PolarDB_Query_RoutePlan::RouteActionReason;
    if (in_transaction) return RAR::IN_TRANSACTION;
    if (is_multi_statement) return RAR::MULTI_STATEMENT;
    return RAR::NONE;
}

/**
 * @brief Return true when a simple-query buffer contains more than one statement.
 *
 * PgSQL_Query_Info stores PostgreSQL simple-query text with the wire terminator
 * included in QueryLength. Treat that trailing NUL like trailing whitespace,
 * then ignore one final semicolon before scanning for any remaining semicolon.
 * The remaining separator means the wait wrapper would see more user result
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
};

#endif // POLARDB_PROXY

#endif // __CLASS_PGSQL_POLARDB_H
