/**
 * @file PgSQL_PolarDB_Protocol.cpp
 * @brief PolarDB SQL classification and startup identity helpers.
 *
 * The write-query heuristic lets the response path decide whether the finished
 * query advanced this session's write LSN. Startup helpers build and compare the
 * client identity sent to PolarDB backends.
 *
 * When POLARDB_PROXY=0, PolarDB declarations and call sites compile out.
 */

#include "PgSQL_PolarDB.h"
#include "PgSQL_Data_Stream.h"
#include "PgSQL_Session.h"
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

static bool polardb_starts_with_keyword(const char* query, const char* word) {
    query = polardb_skip_leading_space(query);
    if (!query) {
        return false;
    }
    const size_t word_len = strlen(word);
    if (!polardb_token_equals(query, word_len, word)) {
        return false;
    }
    return query[word_len] == '\0' || !polardb_is_ident_char(query[word_len]);
}

static const char* polardb_skip_single_quoted(const char* p, bool backslash_escapes) {
    p++;
    while (*p) {
        if (backslash_escapes && *p == '\\' && p[1] != '\0') {
            p += 2;
            continue;
        }
        if (*p == '\'') {
            p++;
            if (*p == '\'') {
                p++;
                continue;
            }
            return p;
        }
        p++;
    }
    return p;
}

static const char* polardb_skip_double_quoted(const char* p) {
    p++;
    while (*p) {
        if (*p == '"') {
            p++;
            if (*p == '"') {
                p++;
                continue;
            }
            return p;
        }
        p++;
    }
    return p;
}

static const char* polardb_skip_line_comment(const char* p) {
    p += 2;
    while (*p && *p != '\n' && *p != '\r') {
        p++;
    }
    return p;
}

static const char* polardb_skip_block_comment(const char* p) {
    p += 2;
    while (*p) {
        if (p[0] == '*' && p[1] == '/') {
            return p + 2;
        }
        p++;
    }
    return p;
}

static const char* polardb_skip_dollar_quoted(const char* p) {
    if (*p != '$') {
        return p;
    }
    const char* tag_end = p + 1;
    while (polardb_is_ident_char(*tag_end)) {
        tag_end++;
    }
    if (*tag_end != '$') {
        return p;
    }
    const size_t delim_len = static_cast<size_t>(tag_end - p + 1);
    const char* body = tag_end + 1;
    while (*body) {
        if (*body == '$' && strncmp(body, p, delim_len) == 0) {
            return body + delim_len;
        }
        body++;
    }
    return body;
}

/**
 * @brief Scan forward to the next bare identifier word in SQL text.
 *
 * Whitespace, line and block comments, single-quoted and E'' strings,
 * double-quoted identifiers and dollar-quoted bodies are all skipped without
 * producing a token, so only unquoted words outside comments are ever reported.
 *
 * The return value is not a termination signal: for a non-null @p p this always
 * returns a non-null cursor, and at end of input that cursor points at the
 * terminating NUL. Callers must loop on *token instead — when no further
 * identifier exists, @p token is set to nullptr and @p token_len to 0.
 *
 * @param p          Cursor into a NUL-terminated SQL string. May be null.
 * @param token      Set to the start of the identifier found, or nullptr when the
 *                   input is exhausted. Points into the caller's buffer; not
 *                   NUL-terminated at the token end.
 * @param token_len  Set to the identifier length in bytes, or 0.
 * @return Cursor positioned just past the reported identifier, or at the
 *         terminating NUL when none was found.
 */
static const char* polardb_next_sql_word(
        const char* p, const char** token, size_t* token_len) {
    *token = nullptr;
    *token_len = 0;
    while (p && *p) {
        if (polardb_ascii_space(*p)) {
            p++;
            continue;
        }
        if (p[0] == '-' && p[1] == '-') {
            p = polardb_skip_line_comment(p);
            continue;
        }
        if (p[0] == '/' && p[1] == '*') {
            p = polardb_skip_block_comment(p);
            continue;
        }
        if ((p[0] == 'e' || p[0] == 'E') && p[1] == '\'') {
            p = polardb_skip_single_quoted(p + 1, true);
            continue;
        }
        if (*p == '\'') {
            p = polardb_skip_single_quoted(p, false);
            continue;
        }
        if (*p == '"') {
            p = polardb_skip_double_quoted(p);
            continue;
        }
        if (*p == '$') {
            const char* after_dollar = polardb_skip_dollar_quoted(p);
            if (after_dollar != p) {
                p = after_dollar;
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

/**
 * @brief Detect a PostgreSQL row-locking FOR clause anywhere in a statement.
 *
 * Scans the whole text with polardb_next_sql_word(), so quoted literals and
 * comments are skipped and a match is found at any nesting level — inside a
 * subquery or a CTE body just as much as at the top level. Returns on the first
 * match of FOR UPDATE, FOR SHARE, FOR NO KEY UPDATE or FOR KEY SHARE. A trailing
 * `FOR` with nothing after it ends the scan and reports no match.
 *
 * This check keeps row-locking SELECTs off replicas, so it reports a
 * match only for those four exact forms: any other shape returns false. A false
 * result therefore means "none of these four forms was found", not "this
 * statement takes no locks" — callers must not treat it as proof that a SELECT is
 * lock-free.
 *
 * @param query  NUL-terminated statement text. A null pointer reports no match.
 * @return true when a row-locking FOR clause was found, false otherwise.
 */
static bool polardb_select_has_locking_for_clause(const char* query) {
    if (!query) {
        return false;
    }
    const char* p = query;
    const char* token = nullptr;
    size_t len = 0;
    while ((p = polardb_next_sql_word(p, &token, &len))) {
        if (!token) {
            break;
        }
        if (!polardb_token_equals(token, len, "for")) {
            continue;
        }
        const char* next = nullptr;
        size_t next_len = 0;
        const char* lookahead = polardb_next_sql_word(p, &next, &next_len);
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
            lookahead = polardb_next_sql_word(lookahead, &key, &key_len);
            const char* update = nullptr;
            size_t update_len = 0;
            lookahead = polardb_next_sql_word(lookahead, &update, &update_len);
            if (key && update &&
                    polardb_token_equals(key, key_len, "key") &&
                    polardb_token_equals(update, update_len, "update")) {
                return true;
            }
        } else if (polardb_token_equals(next, next_len, "key")) {
            const char* share = nullptr;
            size_t share_len = 0;
            lookahead = polardb_next_sql_word(lookahead, &share, &share_len);
            if (share && polardb_token_equals(share, share_len, "share")) {
                return true;
            }
        }
    }
    return false;
}

/**
 * @brief Derive the proxy-side startup identity for a client stream.
 *
 * Tries the listener proxy address the client connected to first, then falls back
 * to the configured pgsql-polardb_proxy_identity_host / _port pair. Both
 * candidates are accepted only if they pass valid(true), which rejects wildcard
 * listener addresses — a wildcard would collapse distinct listeners into one
 * pool key.
 *
 * @p identity is overwritten on every path, including when this returns false, so
 * any identity the caller had already placed there is destroyed by the call.
 *
 * @param client_myds  Client data stream whose listener address is preferred. May
 *                     be null, in which case only the configured fallback is
 *                     tried.
 * @param identity     Filled with the derived identity. Must not be null.
 *                     Overwritten regardless of the return value.
 * @return true when a usable identity was produced, false when neither candidate
 *         is valid (and @p identity then holds the rejected fallback).
 */
static bool polardb_proxy_identity_from_client_stream(
        PgSQL_Data_Stream* client_myds,
        PolarDB_StartupIdentity* identity) {
    if (!identity) {
        return false;
    }
    if (client_myds) {
        *identity = PolarDB_StartupIdentity{
            client_myds->proxy_addr.addr,
            client_myds->proxy_addr.port,
            PolarDB_StartupIdentitySource::LISTENER_PROXY};
        if (identity->valid(true)) {
            return true;
        }
    }
    *identity = PolarDB_StartupIdentity{
        pgsql_thread___polardb_proxy_identity_host,
        pgsql_thread___polardb_proxy_identity_port,
        PolarDB_StartupIdentitySource::CONFIGURED_FALLBACK};
    return identity->valid(true);
}

/**
 * @brief Derive the startup identity that keys this session's ReaderPool entry.
 *
 * @p startup_client is reset to a default-constructed value on entry and stays
 * that way on every failure path, so a false return always leaves it empty.
 *
 * Which identity is used depends on the runtime setting
 * pgsql-polardb_proxy_identity_mode. In client-identity mode the client's own
 * address is taken, checked with valid(false), and retried from the raw sockaddr
 * when the cached text address is unusable. Otherwise the identity comes from the
 * listener proxy address or the configured fallback, checked with valid(true) so
 * wildcard listener addresses are rejected.
 *
 * @param sess            Session to derive from. Must have a client data stream.
 * @param startup_client  Receives the derived identity. Must not be null.
 * @return true when a usable identity was derived; false when none could be —
 *         the caller must not pool by identity in that case.
 */
bool polardb_startup_client_from_session(
        PgSQL_Session* sess, PolarDB_StartupClientContext* startup_client) {
    if (startup_client) {
        *startup_client = PolarDB_StartupClientContext{};
    }
    if (!sess || !sess->client_myds || !startup_client) {
        return false;
    }

    PgSQL_Data_Stream* client_myds = sess->client_myds;
    const bool use_client_identity =
        polardb_proxy_identity_mode_uses_client_identity(
            pgsql_thread___polardb_proxy_identity_mode);
    PolarDB_StartupIdentity identity{
        client_myds->addr.addr,
        client_myds->addr.port,
        PolarDB_StartupIdentitySource::CLIENT};
    if (use_client_identity) {
        if (identity.valid(false)) {
            startup_client->identity = std::move(identity);
            return true;
        }

        if (polardb_startup_identity_from_sockaddr(
                client_myds->client_addr,
                &identity,
                PolarDB_StartupIdentitySource::CLIENT)) {
            startup_client->identity = std::move(identity);
            return true;
        }

        return false;
    }

	if (polardb_proxy_identity_from_client_stream(client_myds, &identity)) {
        startup_client->identity = std::move(identity);
        return true;
    }

    return false;
}

// See the declaration in PgSQL_PolarDB.h for the @brief and the full read/write
// classification rules. Rationale: see
// doc/polardb-arch/09-RESULT-AND-LSN-TRACKING.md.
//
// The argument is normally query digest text, but can be raw query text when
// digests are disabled. Locking SELECT detection uses a deliberately small word
// scanner. Ambiguous text can over-classify as a write, which keeps routing on
// the conservative side.
bool PolarDB_Protocol::is_write_query(const char* query) {
    if (query == nullptr) {
        return false;
    }

    // Only these prefixes are treated as reads. Anything else (including WITH,
    // whose CTE body may modify data) falls through to the write branch below so
    // that its result advances this session's write LSN.
    if (polardb_starts_with_keyword(query, "select") ||
        polardb_starts_with_keyword(query, "show") ||
        polardb_starts_with_keyword(query, "explain")) {

        // SELECT ... FOR UPDATE/SHARE takes row locks, so treat it as a write.
        if (polardb_starts_with_keyword(query, "select")) {
            if (polardb_select_has_locking_for_clause(query)) {
                return true;
            }
        }
        return false;
    }

    // Everything else is considered a write.
    return true;
}

bool PolarDB_Protocol::is_txn_split_safe_read(const char* query) {
    return polardb_starts_with_keyword(query, "select") &&
        !polardb_select_has_locking_for_clause(query);
}

bool PolarDB_Protocol::is_txn_split_safe_select(
        bool is_top_level_select, const char* query) {
    return is_top_level_select && !polardb_select_has_locking_for_clause(query);
}

bool PolarDB_Protocol::is_locking_select_query(const char* query) {
    // The caller already has ProxySQL's PGSQL_QUERY_SELECT classification.
    // Scan only for the locking clause so leading comments and WITH ... SELECT
    // forms do not bypass the writer-only decision.
    return polardb_select_has_locking_for_clause(query);
}

/**
 * @brief Classify a statement as a write using the caller-supplied command type.
 *
 * The command type selects the outcome, not the text: PGSQL_QUERY_SELECT counts
 * as a write only when @p query carries a row-locking FOR clause;
 * PGSQL_QUERY_SHOW and PGSQL_QUERY_EXPLAIN are reads; PGSQL_QUERY_UNKNOWN,
 * PGSQL_QUERY__UNINITIALIZED and PGSQL_QUERY___NONE fall back to the
 * single-argument text heuristic; every other command type is a write.
 *
 * Note that the "WITH routes like a write" behaviour of the text heuristic does
 * NOT hold here. With pgsql-query_processor_parser enabled the parser classifies
 * `WITH ...` as PGSQL_QUERY_SELECT, so a data-modifying CTE reaches this overload
 * as a SELECT and is reported as a read unless it also has a locking FOR clause.
 *
 * @param query         Statement text, normally the digest. Used only for the
 *                      SELECT locking check and the fallback heuristic.
 * @param command_type  PGSQL_QUERY_* classification supplied by the caller.
 * @return true when the statement must be treated as a write, false for a read.
 */
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

bool PolarDB_Protocol::should_advance_session_write_lsn(const char* query) {
    if (query == nullptr) {
        return false;
    }

    if (polardb_starts_with_keyword(query, "select") ||
        polardb_starts_with_keyword(query, "show") ||
        polardb_starts_with_keyword(query, "explain")) {
        return false;
    }

    return true;
}

/**
 * @brief Return whether a statement's completion must advance the session write
 *        LSN, using the caller-supplied command type.
 *
 * The command type selects the outcome, not the text: PGSQL_QUERY_SELECT,
 * PGSQL_QUERY_SHOW and PGSQL_QUERY_EXPLAIN record no write LSN;
 * PGSQL_QUERY_UNKNOWN, PGSQL_QUERY__UNINITIALIZED and PGSQL_QUERY___NONE fall
 * back to the single-argument text heuristic; every other command type records
 * one.
 *
 * The read/write split here is broader than is_write_query(): a locking SELECT is
 * a write for routing but still records no write LSN, because it commits nothing
 * a later read has to return.
 *
 * Note the parser interaction: with pgsql-query_processor_parser enabled a
 * `WITH ... UPDATE/INSERT` is classified PGSQL_QUERY_SELECT and therefore does
 * not advance the diagnostic write LSN. Its positioned RFQ still advances the
 * session observed LSN, so SESSION_LSN keeps the next read monotonic. With that
 * variable disabled the tokenizer yields PGSQL_QUERY_UNKNOWN for the same
 * statement and the text heuristic records it as a write.
 *
 * @param query         Statement text, used only for the fallback heuristic.
 * @param command_type  PGSQL_QUERY_* classification supplied by the caller.
 * @return true when the session write LSN must be advanced after this statement,
 *         false otherwise.
 */
bool PolarDB_Protocol::should_advance_session_write_lsn(
        const char* query, int command_type) {
    switch (command_type) {
    case PGSQL_QUERY_SELECT:
    case PGSQL_QUERY_SHOW:
    case PGSQL_QUERY_EXPLAIN:
        return false;
    case PGSQL_QUERY_UNKNOWN:
    case PGSQL_QUERY__UNINITIALIZED:
    case PGSQL_QUERY___NONE:
        return should_advance_session_write_lsn(query);
    default:
        return true;
    }
}

#endif // POLARDB_PROXY
