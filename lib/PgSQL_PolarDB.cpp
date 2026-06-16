/**
 * @file PgSQL_PolarDB.cpp
 * @brief PolarDB-specific support implementation for ProxySQL (LSN subset)
 *
 * Core PolarDB helpers:
 *   - health check via polar_node_type() / polar_is_available() / LSN
 *   - write-query heuristic that the response path uses to decide whether the
 *     finished query advanced this session's write LSN
 *
 * When POLARDB_PROXY=0, PolarDB declarations and call sites compile out.
 */

#include "PgSQL_PolarDB.h"
#include "proxysql.h"
#include "cpp.h"

#if POLARDB_PROXY

static inline bool polardb_ascii_space(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

static inline char polardb_ascii_lower(char c) {
    return c >= 'A' && c <= 'Z' ? (char)(c - 'A' + 'a') : c;
}

static bool polardb_has_token(const char* query, const char* token) {
    if (!query || !token || !*token) {
        return false;
    }

    const size_t token_len = strlen(token);
    for (const char* p = query; *p; ++p) {
        if (!polardb_ascii_space(*p)) {
            continue;
        }
        const char* q = p + 1;
        size_t i = 0;
        for (; i < token_len && q[i]; ++i) {
            if (polardb_ascii_lower(q[i]) != polardb_ascii_lower(token[i])) {
                break;
            }
        }
        if (i == token_len && polardb_ascii_space(q[token_len])) {
            return true;
        }
    }
    return false;
}

/**
 * @brief Parse full PolarDB health check result (node_type + availability + LSN).
 *
 * Expected result format (from POLARDB_CHECK_WITH_LSN_QUERY):
 * - Col 0: polar_node_type()
 * - Col 1: polar_is_available()
 * - Col 2: pg_current_wal_lsn() or pg_last_wal_replay_lsn() as text
 *
 * @see PolarDB_Protocol::parse_node_type / parse_is_available / parse_lsn_string
 */
bool parse_polardb_full_health_check(const char* node_type_str, const char* is_available_str,
                                     const char* lsn_str, PolarDB_HealthCheck& health) {
    health.node_type = PolarDB_Protocol::parse_node_type(node_type_str);
    health.is_available = PolarDB_Protocol::parse_is_available(is_available_str);
    health.current_lsn = PolarDB_Protocol::parse_lsn_string(lsn_str);
    // Always succeeds: each helper maps an unrecognized or null string to a safe
    // default (UNKNOWN node type, available=true, LSN 0), so there is no error path.
    return true;
}

// See the declaration in PgSQL_PolarDB.h for the @brief and the full read/write
// classification contract. Rationale: see
// doc/polardb-arch/09-PUBLISH-AND-WRITE-TRACKING.md.
//
// The argument is the query digest text, so only the leading keyword and a
// coarse whitespace-delimited FOR token scan are needed to classify it.
bool PolarDB_Protocol::is_write_query(const char* query) {
    if (query == nullptr) {
        return false;
    }

    // The digest can keep leading whitespace, so advance to the first keyword
    // before matching prefixes.
    while (*query && polardb_ascii_space(*query)) {
        query++;
    }

    // Only these prefixes are treated as reads. Anything else (including WITH,
    // whose CTE body may modify data) falls through to the write branch below so
    // that its result advances this session's write LSN.
    if (strncasecmp(query, "SELECT", 6) == 0 ||
        strncasecmp(query, "SHOW", 4) == 0 ||
        strncasecmp(query, "EXPLAIN", 7) == 0) {

        // SELECT ... FOR UPDATE/SHARE takes row locks, so treat it as a write.
        // This is a coarse token scan: a literal or identifier containing a
        // whitespace-delimited FOR can also match, which only over-classifies a
        // read as a write.
        if (strncasecmp(query, "SELECT", 6) == 0) {
            if (polardb_has_token(query, "FOR")) {
                return true;
            }
        }
        return false;
    }

    // Everything else is considered a write.
    return true;
}

#endif // POLARDB_PROXY
