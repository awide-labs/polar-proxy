/**
 * @file PgSQL_PolarDB_Consistency.cpp
 * @brief Small PolarDB consistency helpers used by the routing pipeline.
 *
 * Out-of-line definitions for two thin helpers consumed by
 * PgSQL_PolarDB_Flow.cpp:
 *   - polardb_resolve_wait_timeout_ms(int): the one-argument overload that
 *     supplies the global default and forwards to the header-inline resolver.
 *   - PgSQL_Session::polardb_set_session_override(): stores the per-session
 *     consistency-mode override (top tier of mode resolution).
 *
 * The pure, side-effect-free policy functions that share this domain
 * (the two-argument polardb_resolve_wait_timeout_ms(), the three-tier
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

#include <atomic>
#include <cctype>
#include <string>

extern PgSQL_HostGroups_Manager* PgHGM;

#if POLARDB_PROXY

// Resolves the effective wait timeout from a hostgroup value, inheriting the
// global default (@brief on the declaration in PgSQL_PolarDB.h).
uint32_t polardb_resolve_wait_timeout_ms(int hg_timeout_ms) {
	// One-argument form: bind the global default to the current thread's
	// runtime value, then defer all tri-state logic to the header-inline
	// two-argument resolver so production and unit tests share one decision.
	return polardb_resolve_wait_timeout_ms(
		hg_timeout_ms, pgsql_thread___polardb_lag_wait_ms);
}

// Stores the per-session consistency-mode override (@brief on the declaration in
// PgSQL_Session.h). This value is the top tier of mode resolution
// (session override > hostgroup > global): a value >= 0 forces that mode for
// the session; -1 means "no override", so resolution falls through to the
// hostgroup and global settings. Set when the client runs
// `SET proxysql.polardb_consistency_mode = <value>`; `= default` (or -1 here)
// clears the override. It is also cleared by RESET proxysql.polardb_consistency_mode,
// RESET ALL, DISCARD ALL, and RESET CONNECTION.
void PgSQL_Session::polardb_set_session_override(int mode) {
	polardb_config.session_consistency_mode = mode;
	POLARDB_TRACE("PolarDB SET: session_consistency_mode=%d\n", mode);
}

// Stores the per-session transaction-split warmup timing override. -1 means
// "use the built-in default", currently demand warmup for compatibility with
// the original lazy behavior.
void PgSQL_Session::polardb_set_txn_split_warmup_mode(int mode) {
	polardb_config.txn_split_warmup_mode = mode;
	POLARDB_TRACE(
		"PolarDB SET: txn_split_warmup_mode=%s (%d)\n",
		polardb_txn_split_warmup_mode_name(mode), mode);
}

// Returns the active warmup timing policy. Keeping the default here lets the
// parser store only a session override and keeps call sites branch-free.
int PgSQL_Session::polardb_effective_txn_split_warmup_mode() const {
	if (polardb_config.txn_split_warmup_mode >= 0) {
		return polardb_config.txn_split_warmup_mode;
	}
	return static_cast<int>(PolarDB_TxnSplitWarmupMode::DEMAND);
}

// Observes transaction-split RFQ metadata only after Flow.cpp accepted the LSN
// for the current writer scope. This is state collection; the later planner and
// executor decide whether one in-transaction read may borrow a replica backend.
void PgSQL_Session::polardb_observe_transaction_split(PgSQL_Connection* conn,
	uint64_t primary_lsn, bool split_enabled, bool primary_source) {
	if (!primary_source || !conn) {
		return;
	}

	if (!split_enabled) {
		if (polardb_transaction_split.active() ||
				polardb_transaction_split.has_backend_evidence()) {
			POLARDB_TRACE(
				"PolarDB TXN_SPLIT: cleared observed RFQ state because "
				"txn_split_enabled=0\n");
		}
		polardb_release_txn_split_backend(/*want_reuse=*/true);
		polardb_transaction_split.reset();
		return;
	}

	const char transaction_status = conn->get_transaction_status_char();
	if (transaction_status == 'I') {
		if (polardb_transaction_split.did_split) {
			POLARDB_THREAD_COUNT_ONE(thread, txn_committed_with_split);
		} else if (polardb_transaction_split.was_splittable) {
			POLARDB_THREAD_COUNT_ONE(thread, txn_committed_no_split);
		}
		polardb_release_txn_split_backend(/*want_reuse=*/true);
		polardb_transaction_split.reset();
		return;
	}

	const char* xids = conn->get_polardb_txn_xids();
	const bool splittable = conn->is_polardb_txn_splittable();
	const bool wal_pending = conn->is_polardb_txn_wal_pending();
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
