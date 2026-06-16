/**
 * @file PgSQL_PolarDB.h
 * @brief PolarDB configuration enums and string converters.
 *
 * The configuration commit uses these enum values and inline converters while
 * loading and validating admin variables. Later commits extend this header with
 * the routing, wait, RFQ-result, and counter-support types.
 */

#ifndef __CLASS_PGSQL_POLARDB_H
#define __CLASS_PGSQL_POLARDB_H

#if POLARDB_PROXY

#include <cstdint>
#include <strings.h>

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
 * @brief Initial SESSION_LSN target source for a first read-only session.
 *
 * Values match the hot-path integer mapping for
 * pgsql-polardb_session_lsn_baseline.
 */
enum class PolarDB_SessionLsnBaseline : uint8_t {
    OBSERVED = 1,        // Use only positions observed by this client session
    PRIMARY = 2          // Seed first read target from the writer mirror
};

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

static inline bool polardb_string_is_default_or_empty(const char* value) {
    return value == nullptr || *value == '\0' || strcasecmp(value, "default") == 0;
}

static inline int polardb_consistency_mode_from_string(
    const char* value,
    int default_value) {
    if (polardb_string_is_default_or_empty(value)) return default_value;
    if (strcasecmp(value, "off") == 0) return static_cast<int>(PolarDB_ConsistencyMode::OFF);
    if (strcasecmp(value, "lsn") == 0) return static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN);
    if (strcasecmp(value, "primary") == 0) return static_cast<int>(PolarDB_ConsistencyMode::PRIMARY_ONLY);
    return default_value;
}

static inline int polardb_proxy_protocol_from_string(
    const char* value,
    int default_value) {
    if (polardb_string_is_default_or_empty(value)) return default_value;
    if (strcasecmp(value, "off") == 0) return static_cast<int>(PolarDB_ProxyProtocol::OFF);
    if (strcasecmp(value, "legacy") == 0) return static_cast<int>(PolarDB_ProxyProtocol::LEGACY);
    if (strcasecmp(value, "v15") == 0) return static_cast<int>(PolarDB_ProxyProtocol::V15);
    return default_value;
}

static inline int polardb_wait_mode_from_string(
    const char* value,
    int default_value) {
    if (value == nullptr || *value == '\0') return default_value;
    if (strcasecmp(value, "best_effort") == 0) return static_cast<int>(PolarDB_WaitMode::BEST_EFFORT);
    if (strcasecmp(value, "strict") == 0) return static_cast<int>(PolarDB_WaitMode::STRICT);
    return default_value;
}

static inline int polardb_route_rfq_policy_from_string(
    const char* value,
    int default_value = static_cast<int>(PolarDB_RfqRoutePolicy::STRICT)) {
    if (value == nullptr || *value == '\0') return default_value;
    if (strcasecmp(value, "best_effort") == 0) return static_cast<int>(PolarDB_RfqRoutePolicy::BEST_EFFORT);
    if (strcasecmp(value, "strict") == 0) return static_cast<int>(PolarDB_RfqRoutePolicy::STRICT);
    return default_value;
}

static inline int polardb_session_lsn_baseline_from_string(
    const char* value,
    int default_value = static_cast<int>(PolarDB_SessionLsnBaseline::OBSERVED)) {
    if (value == nullptr || *value == '\0') return default_value;
    if (strcasecmp(value, "observed") == 0) return static_cast<int>(PolarDB_SessionLsnBaseline::OBSERVED);
    if (strcasecmp(value, "primary") == 0) return static_cast<int>(PolarDB_SessionLsnBaseline::PRIMARY);
    return default_value;
}

#endif // POLARDB_PROXY

#endif // __CLASS_PGSQL_POLARDB_H
