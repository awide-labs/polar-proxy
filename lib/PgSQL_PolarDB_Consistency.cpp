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
			// Authentication normally validates attributes before a session sees
			// them. If a malformed value still reaches this point, keep
			// pre-write transaction reads on the primary.
			read_committed = false;
		}
	}
	polardb_config.txn_reader_wait_default_read_committed = read_committed;
	POLARDB_TRACE(
		"PolarDB USER: default_transaction_isolation_read_committed=%d\n",
		read_committed ? 1 : 0);
}

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
	// older backend patches. Use it only as a fail-closed blocker while the
	// backend says a transaction is open; the default-isolation report above is
	// the source that enables pre-write reader waits.
	const PGTransactionStatusType tx_status = conn->get_pg_transaction_status();
	if ((tx_status == PQTRANS_INTRANS || tx_status == PQTRANS_INERROR) &&
			!polardb_value_is_read_committed(current_isolation)) {
		polardb_note_txn_non_read_committed("backend_transaction_isolation");
	}
}

void PgSQL_Session::polardb_note_txn_non_read_committed(const char* reason) {
	if (!polardb_txn_non_read_committed) {
		POLARDB_TRACE(
			"PolarDB TXN_WAIT: pre-write reader waits blocked by isolation "
			"reason=%s\n",
			reason ? reason : "unknown");
	}
	polardb_txn_non_read_committed = true;
}

void PgSQL_Session::polardb_note_txn_local_state_change(const char* reason) {
	if (!polardb_txn_local_state_changed) {
		POLARDB_TRACE(
			"PolarDB TXN_WAIT: pre-write reader waits blocked by transaction "
			"local state reason=%s\n",
			reason ? reason : "unknown");
	}
	polardb_txn_local_state_changed = true;
}

bool PgSQL_Session::polardb_txn_reader_wait_isolation_read_committed() {
	if (polardb_txn_non_read_committed) {
		return false;
	}
	return polardb_config.txn_reader_wait_default_read_committed;
}

// Clears durable transaction-split state and any borrowed split backend. The
// active split-read path restores mybe before calling this; this routine owns
// only the persistent transaction-split state, route pin, and borrowed backend slot.
void PgSQL_Session::polardb_clear_transaction_split_state(
	const char* reason, bool want_reuse) {
	if (polardb_transaction_split.active() ||
			polardb_transaction_split.has_backend_evidence() ||
			polardb_txn_reader_failure_pin != PolarDB_RoutePin::NONE) {
		POLARDB_TRACE(
			"PolarDB TXN_SPLIT: clear state reason=%s\n",
			reason ? reason : "unspecified");
	}
	polardb_release_txn_split_backend(want_reuse);
	polardb_transaction_split.reset();
	polardb_txn_reader_failure_pin = PolarDB_RoutePin::NONE;
	polardb_txn_writer_hg = -1;
	polardb_txn_shunned_reader_hg = -1;
	polardb_txn_shunned_reader_address.clear();
	polardb_txn_shunned_reader_port = -1;
	polardb_txn_non_read_committed = false;
	polardb_txn_local_state_changed = false;
}

// Observes transaction-split RFQ metadata after Flow.cpp has validated the
// writer scope. Positioned RFQ passes its primary LSN; missing-LSN RFQ passes 0
// only to process transaction status/split flags and to clear writer pins on
// transaction close. Session write_unknown/observed_unknown sticky flags remain the
// routing source of truth when the LSN itself is missing.
void PgSQL_Session::polardb_observe_transaction_split(PgSQL_Connection* conn,
	uint64_t primary_lsn, bool split_enabled, bool primary_source) {
	if (!primary_source || !conn) {
		return;
	}

	if (!split_enabled) {
		polardb_clear_transaction_split_state("observation_disabled", true);
		return;
	}

	const char transaction_status = conn->get_transaction_status_char();
	if (transaction_status == 'I') {
		if (polardb_transaction_split.did_split) {
			POLARDB_THREAD_COUNT_ONE(thread, txn_committed_with_split);
		} else if (polardb_transaction_split.was_splittable) {
			POLARDB_THREAD_COUNT_ONE(thread, txn_committed_no_split);
		}
		polardb_clear_transaction_split_state("primary_idle", true);
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
