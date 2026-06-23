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

static const char* polardb_skip_leading_space(const char* query) {
    if (!query) {
        return nullptr;
    }
    while (*query && polardb_ascii_space(*query)) {
        query++;
    }
    return query;
}

static bool polardb_is_ident_char(char c) {
    return (c >= 'A' && c <= 'Z') ||
        (c >= 'a' && c <= 'z') ||
        (c >= '0' && c <= '9') ||
        c == '_';
}

static bool polardb_token_equals(const char* token, size_t len, const char* word) {
    const size_t word_len = strlen(word);
    if (len != word_len) {
        return false;
    }
    for (size_t i = 0; i < len; i++) {
        if (polardb_ascii_lower(token[i]) != polardb_ascii_lower(word[i])) {
            return false;
        }
    }
    return true;
}

static const char* polardb_skip_sql_literal(const char* p) {
    p++;
    while (*p) {
        if (*p == '\'' && p[1] == '\'') {
            p += 2;
            continue;
        }
        if (*p++ == '\'') {
            break;
        }
    }
    return p;
}

static const char* polardb_skip_quoted_identifier(const char* p) {
    p++;
    while (*p) {
        if (*p == '"' && p[1] == '"') {
            p += 2;
            continue;
        }
        if (*p++ == '"') {
            break;
        }
    }
    return p;
}

static const char* polardb_skip_dollar_quote(const char* p) {
    if (*p != '$') {
        return p;
    }
    const char* tag_end = p + 1;
    while (*tag_end && polardb_is_ident_char(*tag_end)) {
        tag_end++;
    }
    if (*tag_end != '$') {
        return p;
    }
    const size_t tag_len = static_cast<size_t>(tag_end - p + 1);
    const char* body = tag_end + 1;
    while (*body) {
        if (strncmp(body, p, tag_len) == 0) {
            return body + tag_len;
        }
        body++;
    }
    return body;
}

static const char* polardb_next_sql_token(
        const char* p, const char** token, size_t* token_len) {
    *token = nullptr;
    *token_len = 0;
    while (p && *p) {
        if (polardb_ascii_space(*p) || *p == ';' || *p == ',' ||
                *p == '(' || *p == ')') {
            p++;
            continue;
        }
        if (*p == '-' && p[1] == '-') {
            p += 2;
            while (*p && *p != '\n' && *p != '\r') {
                p++;
            }
            continue;
        }
        if (*p == '/' && p[1] == '*') {
            p += 2;
            while (*p && !(p[0] == '*' && p[1] == '/')) {
                p++;
            }
            if (*p) {
                p += 2;
            }
            continue;
        }
        if (*p == '\'') {
            p = polardb_skip_sql_literal(p);
            continue;
        }
        if (*p == '"') {
            p = polardb_skip_quoted_identifier(p);
            continue;
        }
        if (*p == '$') {
            const char* after = polardb_skip_dollar_quote(p);
            if (after != p) {
                p = after;
                continue;
            }
        }
        if (polardb_is_ident_char(*p)) {
            const char* start = p;
            while (polardb_is_ident_char(*p)) {
                p++;
            }
            *token = start;
            *token_len = static_cast<size_t>(p - start);
            return p;
        }
        p++;
    }
    return p;
}

static bool polardb_select_has_locking_for_clause(const char* query) {
    if (!query) {
        return false;
    }
    const char* p = query;
    const char* token = nullptr;
    size_t len = 0;
    while ((p = polardb_next_sql_token(p, &token, &len))) {
        if (!token) {
            break;
        }
        if (!polardb_token_equals(token, len, "for")) {
            continue;
        }
        const char* next = nullptr;
        size_t next_len = 0;
        p = polardb_next_sql_token(p, &next, &next_len);
        if (!next) {
            return false;
        }
        if (polardb_token_equals(next, next_len, "update") ||
                polardb_token_equals(next, next_len, "share")) {
            return true;
        }
        if (polardb_token_equals(next, next_len, "no")) {
            const char* key = nullptr;
            size_t key_len = 0;
            p = polardb_next_sql_token(p, &key, &key_len);
            const char* update = nullptr;
            size_t update_len = 0;
            p = polardb_next_sql_token(p, &update, &update_len);
            if (key && update &&
                    polardb_token_equals(key, key_len, "key") &&
                    polardb_token_equals(update, update_len, "update")) {
                return true;
            }
        } else if (polardb_token_equals(next, next_len, "key")) {
            const char* share = nullptr;
            size_t share_len = 0;
            p = polardb_next_sql_token(p, &share, &share_len);
            if (share && polardb_token_equals(share, share_len, "share")) {
                return true;
            }
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
// The argument is normally query digest text, but can be raw query text when
// digests are disabled. Locking SELECT detection must therefore ignore SQL
// literals and comments.
bool PolarDB_Protocol::is_write_query(const char* query) {
    if (query == nullptr) {
        return false;
    }

    // The digest can keep leading whitespace, so advance to the first keyword
    // before matching prefixes.
    query = polardb_skip_leading_space(query);

    // Only these prefixes are treated as reads. Anything else (including WITH,
    // whose CTE body may modify data) falls through to the write branch below so
    // that its result advances this session's write LSN.
    if (strncasecmp(query, "SELECT", 6) == 0 ||
        strncasecmp(query, "SHOW", 4) == 0 ||
        strncasecmp(query, "EXPLAIN", 7) == 0) {

        // SELECT ... FOR UPDATE/SHARE takes row locks, so treat it as a write.
        // The scanner skips literals/comments/quoted identifiers so ordinary
        // expressions such as substring(x from ? for ?) remain reads.
        if (strncasecmp(query, "SELECT", 6) == 0) {
            if (polardb_select_has_locking_for_clause(query)) {
                return true;
            }
        }
        return false;
    }

    // Everything else is considered a write.
    return true;
}

bool PolarDB_Protocol::is_write_query(
        const char* query, int command_type) {
    switch (command_type) {
    case PGSQL_QUERY_SELECT:
        return polardb_select_has_locking_for_clause(query);
    case PGSQL_QUERY_SHOW:
    case PGSQL_QUERY_EXPLAIN:
        return false;
    case PGSQL_QUERY_UNKNOWN:
    case PGSQL_QUERY__UNINITIALIZED:
    case PGSQL_QUERY___NONE:
        return is_write_query(query);
    default:
        return true;
    }
}

#endif // POLARDB_PROXY
