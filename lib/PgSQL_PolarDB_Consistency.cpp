/**
 * @file PgSQL_PolarDB_Consistency.cpp
 * @brief Small PolarDB consistency helpers used by the routing pipeline.
 *
 * Out-of-line definitions for the helpers consumed by
 * PgSQL_PolarDB_Flow.cpp, among them:
 *   - PgSQL_Session::polardb_set_session_consistency_mode(): stores the per-session
 *     consistency-mode override (top tier of mode resolution).
 *   - PgSQL_Session::polardb_set_txn_split_warmup_mode(): stores the
 *     per-session timing policy for transaction-split pool warmup.
 *
 * The pure, side-effect-free policy functions that share this domain
 * (polardb_resolve_wait_timeout_ms(), the three-tier
 * polardb_resolve_consistency_mode() with priority session > hostgroup > global,
 * and PolarDB_Query_WaitPlan::build_consistency() for the LSN wait spec) are
 * defined inline in PgSQL_PolarDB.h so unit tests can exercise them without
 * linking the full PolarDB build.
 */

#include "PgSQL_Session.h"
#include "PgSQL_Connection.h"
#include "PgSQL_PolarDB.h"
#include "proxysql.h"
#include "cpp.h"
#include "../deps/json/json.hpp"

#include <atomic>
#include <cctype>
#include <string>

extern PgSQL_HostGroups_Manager* PgHGM;

#if POLARDB_PROXY

namespace {

bool polardb_value_is_read_committed(const char* raw) {
	if (!raw || !raw[0]) {
		return false;
	}
	std::string normalized;
	for (const unsigned char ch : std::string(raw)) {
		if (std::isalnum(ch)) {
			normalized.push_back((char)std::tolower(ch));
		}
	}
	return normalized == "readcommitted";
}

} // namespace

// Store the per-session consistency-mode override (@brief on the declaration in
// PgSQL_Session.h). This value is the top tier of mode resolution
// (session override > hostgroup > global): a value >= 0 forces that mode for
// the session; -1 means "no override", so resolution falls through to the
// hostgroup and global settings. Set when the client runs
// `SET proxysql.polardb_consistency_mode = <value>`; `= default` (or -1 here)
// clears the override. It is also cleared by RESET proxysql.polardb_consistency_mode,
// RESET ALL, DISCARD ALL, and RESET CONNECTION.
void PgSQL_Session::polardb_set_session_consistency_mode(int mode) {
	polardb_config.session_consistency_mode = mode;
	POLARDB_TRACE("PolarDB SET: session_consistency_mode=%d\n", mode);
}

// Store the per-session transaction-split warmup timing override. -1 means
// "use the built-in default", currently DEMAND: warmup is queued only after a
// split-readable transaction read misses the pool.
void PgSQL_Session::polardb_set_txn_split_warmup_mode(int mode) {
	polardb_config.txn_split_warmup_mode = mode;
	POLARDB_TRACE(
		"PolarDB SET: txn_split_warmup_mode=%s (%d)\n",
		polardb_txn_split_warmup_mode_name(mode), mode);
}

// Return the active warmup timing policy. Keeping the default here lets the
// parser store only a session override and keeps call sites branch-free.
int PgSQL_Session::polardb_effective_txn_split_warmup_mode() const {
	if (polardb_config.txn_split_warmup_mode >= 0) {
		return polardb_config.txn_split_warmup_mode;
	}
	return static_cast<int>(PolarDB_TxnSplitWarmupMode::DEMAND);
}

/**
 * @brief Apply the PolarDB-relevant part of pgsql_users.attributes to this session.
 *
 * Only the "default-transaction_isolation" key is consumed. It seeds the tracked
 * session default isolation, which controls whether pre-write transaction reads
 * may be served by a reader at all. The two "no usable value" cases fail in
 * opposite directions on purpose: an absent or empty attribute string means the
 * operator configured nothing, so the PostgreSQL default READ COMMITTED is
 * assumed and pre-write reader waits stay available; a malformed attribute string
 * means the intent cannot be known, so it fails closed and pre-write reads stay
 * on the primary.
 *
 * Also clears the backend-default-seen marker, so the next
 * default_transaction_isolation report from a backend is treated as first-seen.
 *
 * @param attributes  Raw JSON attributes string for the user; may be null or empty.
 */
void PgSQL_Session::polardb_apply_user_attributes(const char* attributes) {
	bool read_committed = true;
	if (attributes && attributes[0]) {
		try {
			nlohmann::json parsed = nlohmann::json::parse(attributes);
			auto default_isolation =
				parsed.find("default-transaction_isolation");
			if (default_isolation != parsed.end()) {
				const std::string value = default_isolation->get<std::string>();
				read_committed = polardb_value_is_read_committed(value.c_str());
			}
		} catch (...) {
			// Authentication normally validates attributes before a session
			// receives them. If a malformed value still reaches this point, keep
			// pre-write transaction reads on the primary.
			read_committed = false;
		}
	}
	polardb_config.txn_reader_wait_default_read_committed = read_committed;
	polardb_config.txn_reader_wait_backend_default_seen = false;
	POLARDB_TRACE(
		"PolarDB USER: default_transaction_isolation_read_committed=%d\n",
		read_committed ? 1 : 0);
}

void PgSQL_Session::polardb_reapply_user_attributes_after_reset() {
	polardb_apply_user_attributes(user_attributes);
}

/**
 * @brief Apply the transaction isolation a backend reports through parameter status.
 *
 * The two reports carry different authority. A default_transaction_isolation
 * report overwrites the tracked session isolation in both directions, so it can
 * both enable and disable pre-write reader waits. A current transaction_isolation
 * report can only latch the sticky non-READ-COMMITTED block through
 * polardb_note_txn_non_read_committed(), which blocks pre-write reader waits for
 * the rest of the transaction; it is consulted only while the backend reports
 * PQTRANS_INTRANS or PQTRANS_INERROR, because outside an active transaction that
 * value can be stale on older backend patches.
 *
 * @param conn    Backend connection to read parameter status from. A null
 *                connection is a silent no-op.
 * @param reason  Trace-only label naming the call site; it affects no decision.
 */
void PgSQL_Session::polardb_apply_backend_isolation_status(
		PgSQL_Connection* conn, const char* reason) {
	if (!conn) {
		return;
	}

	const char* default_isolation =
		conn->get_pg_parameter_status("default_transaction_isolation");
	if (default_isolation && default_isolation[0]) {
		const bool read_committed =
			polardb_value_is_read_committed(default_isolation);
		if (!polardb_config.txn_reader_wait_backend_default_seen ||
				polardb_config.txn_reader_wait_default_read_committed !=
					read_committed) {
			POLARDB_TRACE(
				"PolarDB TXN_WAIT: backend default_transaction_isolation "
				"read_committed=%d reason=%s\n",
				read_committed ? 1 : 0,
				reason ? reason : "unknown");
		}
		polardb_config.txn_reader_wait_backend_default_seen = true;
		polardb_config.txn_reader_wait_default_read_committed = read_committed;
	}

	const char* current_isolation =
		conn->get_pg_parameter_status("transaction_isolation");
	if (!current_isolation || !current_isolation[0]) {
		return;
	}

	// A current-isolation report can be stale outside an active transaction on
	// older backend patches. Use it only to keep an active non-READ-COMMITTED
	// transaction on the primary; the default-isolation report above is the
	// source that enables pre-write reader waits.
	const PGTransactionStatusType tx_status = conn->get_pg_transaction_status();
	if ((tx_status == PQTRANS_INTRANS || tx_status == PQTRANS_INERROR) &&
			!polardb_value_is_read_committed(current_isolation)) {
		polardb_note_txn_non_read_committed("backend_transaction_isolation");
	}
}

/**
 * @brief Mark the current transaction unsafe for pre-write reader waits.
 *
 * The flag latches and is one-way: it stays set for the rest of the transaction
 * and is cleared only when polardb_teardown_transaction_reader_state() resets the
 * wait-safety flags, that is at transaction close, when split observation is
 * disabled, or on a writer-scope change. Statement boundaries do not clear it.
 *
 * @param reason  Trace-only label naming what set the flag.
 */
void PgSQL_Session::polardb_note_txn_non_read_committed(const char* reason) {
	if (!polardb_txn_wait_safety.non_read_committed) {
		POLARDB_TRACE(
			"PolarDB TXN_WAIT: pre-write reader waits blocked by isolation "
			"reason=%s\n",
			reason ? reason : "unknown");
	}
	polardb_txn_wait_safety.non_read_committed = true;
}

/**
 * @brief Mark the current transaction as carrying primary-local state changes.
 *
 * Set when a statement changes state that lives only on the primary transaction
 * connection, for example SET LOCAL, which a reader cannot reproduce. Like
 * polardb_note_txn_non_read_committed() the flag latches until
 * polardb_teardown_transaction_reader_state() resets the wait-safety flags at
 * transaction close, split-observation disable, or a writer-scope change.
 *
 * @param reason  Trace-only label naming what set the flag.
 */
void PgSQL_Session::polardb_note_txn_local_state_change(const char* reason) {
	if (!polardb_txn_wait_safety.local_state_changed) {
		POLARDB_TRACE(
			"PolarDB TXN_WAIT: pre-write reader waits blocked by transaction "
			"local state reason=%s\n",
			reason ? reason : "unknown");
	}
	polardb_txn_wait_safety.local_state_changed = true;
}

bool PgSQL_Session::polardb_txn_wait_uses_read_committed() const {
	if (polardb_txn_wait_safety.non_read_committed) {
		return false;
	}
	return polardb_config.txn_reader_wait_default_read_committed;
}

/**
 * @brief Drop all durable transaction-split state and any temporary split backend.
 *
 * When a split read or a pre-write wait read is still active this routine unwinds
 * it itself: polardb_release_txn_reader_backend() calls
 * polardb_reset_txn_reader_request(), which restores mybe from the saved primary
 * backend, frees the saved original client packet, resets the reader stream's
 * pgsql_real_query and drops pending notices. It then resets the transaction-split
 * state, the reader-failure writer route and the transaction wait-safety flags, so
 * the session is left with no transaction-split knowledge at all.
 *
 * @param reason      Trace-only label naming the call site.
 * @param want_reuse  true to normalize the temporary reader connection and return
 *                    it to the pool, false to destroy it. Pass false whenever the
 *                    connection state is unknown or the session is being torn down.
 */
void PgSQL_Session::polardb_teardown_transaction_reader_state(
	const char* reason, bool want_reuse) {
	if (polardb_transaction_split.active() ||
			polardb_transaction_split.has_backend_evidence() ||
			polardb_txn_reader_failure.active()) {
		POLARDB_TRACE(
			"PolarDB TXN_SPLIT: clear state reason=%s\n",
			reason ? reason : "unspecified");
	}
	polardb_release_txn_reader_backend(want_reuse);
	polardb_transaction_split.reset();
	polardb_txn_reader_failure.clear();
	polardb_txn_wait_safety.clear();
}

/**
 * @brief Advance the transaction-split state machine from a primary RFQ.
 *
 * Call this after Flow.cpp has validated the request writer scope. A positioned
 * RFQ passes its primary LSN; the zero-payload and missing-payload paths pass 0
 * because only the transaction status byte and the split markers are usable
 * there, and the session write_unknown/observed_unknown sticky flags stay the
 * routing source of truth for the LSN itself.
 *
 * Beyond the split stage this always refreshes polardb_txn_has_no_write_xids,
 * which the planner and the client RFQ translation both read as a routing input.
 * Two cases tear state down instead of advancing it: @p split_enabled false, and a
 * transaction status of 'I' meaning the transaction closed. Both call
 * polardb_teardown_transaction_reader_state() with reuse, which also clears the
 * reader-failure writer route and the wait-safety flags and releases the temporary
 * split reader connection back to the pool. Callers that derive @p split_enabled
 * from policy therefore release that connection here when the policy flips.
 *
 * @param conn            Backend connection carrying the RFQ. Null is a no-op.
 * @param primary_lsn     Primary LSN from a positioned RFQ, 0 when unknown.
 * @param split_enabled   false to disable observation and clear all split state.
 * @param primary_source  true only when the RFQ came from the writer hostgroup;
 *                        false is a no-op.
 */
void PgSQL_Session::polardb_observe_transaction_split(PgSQL_Connection* conn,
	uint64_t primary_lsn, bool split_enabled, bool primary_source) {
	if (!primary_source || !conn) {
		return;
	}

	const char transaction_status = conn->get_transaction_status_char();
	const char* xids = conn->get_polardb_txn_xids();
	const bool splittable = conn->is_polardb_txn_splittable();
	const bool wal_pending = conn->is_polardb_txn_wal_pending();
	polardb_txn_has_no_write_xids =
		polardb_rfq_is_prewrite_split_candidate(
			transaction_status, xids, splittable, wal_pending);

	if (!split_enabled) {
		polardb_teardown_transaction_reader_state("observation_disabled", true);
		return;
	}

	if (transaction_status == 'I') {
		if (polardb_transaction_split.did_split) {
			POLARDB_THREAD_COUNT_ONE(thread, txn_committed_with_split);
		} else if (polardb_transaction_split.was_splittable) {
			POLARDB_THREAD_COUNT_ONE(thread, txn_committed_no_split);
		}
		polardb_teardown_transaction_reader_state("primary_idle", true);
		return;
	}

	const PolarDB_TransactionSplitStage old_stage = polardb_transaction_split.stage;
	const bool old_was_split_readable =
		old_stage == PolarDB_TransactionSplitStage::TXN_SPLITTABLE;

	polardb_transaction_split.observe_primary_rfq(
		transaction_status, xids, splittable, wal_pending, primary_lsn, split_enabled);

	if (xids && xids[0]) {
		POLARDB_THREAD_COUNT_ONE(thread, xids_received);
	}
	if (old_stage != PolarDB_TransactionSplitStage::TXN_SPLITTABLE &&
			polardb_transaction_split.stage ==
				PolarDB_TransactionSplitStage::TXN_SPLITTABLE) {
		POLARDB_THREAD_COUNT_ONE(thread, txn_became_splittable);
	} else if (old_was_split_readable &&
			polardb_transaction_split.stage !=
				PolarDB_TransactionSplitStage::TXN_SPLITTABLE) {
		POLARDB_THREAD_COUNT_ONE(thread, txn_lost_splittable);
	}

	if (old_stage != polardb_transaction_split.stage || xids || splittable || wal_pending) {
		POLARDB_TRACE(
			"PolarDB TXN_SPLIT: observed primary RFQ status=%c split_enabled=%d "
			"stage=%d->%d lsn=%lu xids='%s' splittable=%d wal_pending=%d\n",
			transaction_status,
			split_enabled ? 1 : 0,
			static_cast<int>(old_stage),
			static_cast<int>(polardb_transaction_split.stage),
			(unsigned long)primary_lsn,
			xids ? xids : "",
			splittable ? 1 : 0,
			wal_pending ? 1 : 0);
	}
}

#endif // POLARDB_PROXY
