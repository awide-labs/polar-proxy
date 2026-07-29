
#include <fcntl.h>
#include <sstream>
#include <atomic>
#include <memory>
#include <cstring>

#include "../deps/json/json.hpp"
using json = nlohmann::json;
#define PROXYJSON
#include "PgSQL_HostGroups_Manager.h"
#include "PgSQL_Monitor.hpp"
#include "proxysql.h"
#include "cpp.h"
#include "PgSQL_PreparedStatement.h"
#include "PgSQL_Data_Stream.h"
#include "PgSQL_Query_Processor.h"
#include "PgSQL_Variables.h"
#include "PgSQL_Thread.h"
#include "PgSQL_Extended_Query_Message.h"
#include "PgSQL_PolarDB.h"

#if POLARDB_PROXY
// PolarDB_ProxyProtocol is the enum used in the connection code; the same values
// are also defined as plain integer constants (POLARDB_PROXY_PROTOCOL_*) used by
// configuration. Keep the two in lock-step so a cast between them is always valid.
static_assert(POLARDB_PROXY_PROTOCOL_OFF == static_cast<int>(PolarDB_ProxyProtocol::OFF),
	"PolarDB proxy protocol OFF constant mismatch");
static_assert(POLARDB_PROXY_PROTOCOL_LEGACY == static_cast<int>(PolarDB_ProxyProtocol::LEGACY),
	"PolarDB proxy protocol LEGACY constant mismatch");
static_assert(POLARDB_PROXY_PROTOCOL_V15 == static_cast<int>(PolarDB_ProxyProtocol::V15),
	"PolarDB proxy protocol V15 constant mismatch");

// Defined in PgSQL_PolarDB_Notices.cpp. Handles the backend's LSN wait-timeout
// notice. It must run even when this connection has no active result object,
// because the wrapped wait recycles that object while consuming the prepended
// SET results (see notice_handler_cb below).
void polardb_handle_notice(PgSQL_Connection* conn, const PGresult* result);

/// @brief Human-readable name of a proxy protocol value, for log and trace lines.
static const char* polardb_proxy_protocol_name(PolarDB_ProxyProtocol protocol) {
	switch (protocol) {
	case PolarDB_ProxyProtocol::V15:
		return "v15";
	case PolarDB_ProxyProtocol::LEGACY:
		return "legacy";
	case PolarDB_ProxyProtocol::OFF:
	default:
		return "off";
	}
}

#if POLARDB_PROXY && POLARDB_DEBUG
static bool polardb_debug_startup_identity_fault(char* out, size_t out_size) {
	if (out && out_size > 0) {
		out[0] = '\0';
	}
	// A non-empty first line names the forced identity path. Clear the file only
	// after consuming a real request so a blank file is harmless.
	bool found = false;
	if (polardb_debug_consume_fault_file(
			"POLARDB_DEBUG_STARTUP_IDENTITY_FILE", out, out_size)) {
		found = out[0] != '\0';
	}

	if (found) {
		polardb_debug_clear_fault_file("POLARDB_DEBUG_STARTUP_IDENTITY_FILE");
	}
	return found;
}

static bool polardb_debug_post_send_offline(PgSQL_Connection* conn) {
	if (!conn || !conn->myds || !conn->myds->sess ||
			!conn->myds->sess->polardb_txn_reader.active() ||
			!conn->myds->sess->polardb_txn_reader.backend ||
			conn->myds->sess->polardb_txn_reader.backend->server_myds != conn->myds) {
		return false;
	}
	char fault[64] = {0};
	if (!polardb_debug_consume_fault_file(
			"POLARDB_DEBUG_POST_SEND_OFFLINE_FILE", fault, sizeof(fault)) ||
			strcmp(fault, "offline_no_error") != 0) {
		return false;
	}
	polardb_debug_clear_fault_file("POLARDB_DEBUG_POST_SEND_OFFLINE_FILE");
	return true;
}
#endif // POLARDB_PROXY && POLARDB_DEBUG

#if POLARDB_PROXY
static PgSQL_Thread* polardb_row_run_counter_thread(PgSQL_Connection* conn) {
	if (conn && conn->myds && conn->myds->sess) {
		return conn->myds->sess->thread;
	}
	return nullptr;
}

#define POLARDB_ROW_RUN_COUNT(conn, name, value) do { \
		if (PgHGM) { \
			POLARDB_THREAD_COUNT(polardb_row_run_counter_thread((conn)), name, (value)); \
		} \
	} while (0)

static inline bool polardb_row_run_enabled(const PgSQL_Connection* conn) {
	if (!pgsql_thread___polardb_result_fast_forward || !conn || conn->is_copy_out) {
		return false;
	}
	if (conn->new_result || !conn->query_result || !conn->pgsql_conn) {
		return false;
	}
	if (conn->query.extended_query_info || conn->processing_multi_statement) {
		return false;
	}
	if (conn->polardb_query_wrap_state.has_pending()) {
		return false;
	}
	return true;
}

static inline bool polardb_try_add_row_run(PgSQL_Connection* conn) {
	char* owner = nullptr;
	const char* data = nullptr;
	size_t len = 0;
	int frames = 0;

	if (!polardb_row_run_enabled(conn)) {
		return false;
	}
	if (!PSrowRunPending(conn->pgsql_conn)) {
		POLARDB_ROW_RUN_COUNT(conn, result_row_run_not_candidate, 1);
		return false;
	}

	POLARDB_ROW_RUN_COUNT(conn, result_row_run_attempts, 1);
	const int rc = PSdetachRowRun(conn->pgsql_conn, &owner, &data, &len, &frames, 0);
	if (rc == 0 && owner && data && len > 0 && frames > 0) {
		const auto bytes_recv = conn->query_result->add_row_run_borrowed(owner,
			data, static_cast<unsigned int>(len), static_cast<unsigned int>(frames));
		conn->polardb_row_run_last_bytes = bytes_recv;
		conn->result_type = 3;
		POLARDB_ROW_RUN_COUNT(conn, result_row_run_used, 1);
		POLARDB_ROW_RUN_COUNT(conn, result_row_run_frames, frames);
		POLARDB_ROW_RUN_COUNT(conn, result_row_run_bytes, bytes_recv);
		POLARDB_TRACE("PolarDB RESULT: row_run conn=%p frames=%d bytes=%u\n",
			(void*)conn, frames, bytes_recv);
		return true;
	}

	if (rc == EOF) {
		POLARDB_ROW_RUN_COUNT(conn, result_row_run_partial, 1);
	} else {
		POLARDB_ROW_RUN_COUNT(conn, result_row_run_unavailable, 1);
	}
	return false;
}

#undef POLARDB_ROW_RUN_COUNT
#endif // POLARDB_PROXY

/// @brief Is this error/notice the PolarDB LSN wait-timeout?
///
/// The PolarDB backend tags its wait-timeout with a fixed marker in the
/// structured DETAIL diagnostic field. We match that field, never the
/// human-readable message text, so a user query that happens to contain the
/// same words cannot be mistaken for a wait timeout.
static bool polardb_is_lsn_wait_timeout_result(const PGresult* result) {
	const char* detail = result ? PQresultErrorField(result, PG_DIAG_MESSAGE_DETAIL) : nullptr;
	return detail && strcmp(detail, POLARDB_LSN_WAIT_TIMEOUT_DETAIL) == 0;
}

/// @brief Account a wrapper-statement error and stop consuming wrapper results.
///
/// A consistency read is sent as several SET statements glued in front of the
/// user query (see PgSQL_PolarDB_Wrap.cpp). If one of those wrapper statements
/// errors, this decides whether the error is a PolarDB wait-timeout to count,
/// then marks this connection's wrap-state failed so result consumption stops
/// and the error flows to the client through the normal path.
///
/// @param conn   the backend connection whose wrapped read errored.
/// @param result the libpq result carrying the error (may be null on some paths).
/// @param msg    error text, for trace output only.
static void polardb_account_wrapper_set_error(PgSQL_Connection* conn, const PGresult* result, const char* msg) {
	POLARDB_TRACE("PolarDB WAIT: account_wrapper_set_error enter conn=%p msg='%s'\n",
		(void*)conn, msg ? msg : "");
	// Nothing to account if this query was never sent wrapped.
	if (!conn || !conn->polardb_query_wrap_state.was_wrapped) {
		POLARDB_TRACE("PolarDB WAIT: skip account, no conn or not consuming wrapper SET "
			"conn=%p was_wrapped=%d pending=%u failed=%d\n",
			(void*)conn,
			conn && conn->polardb_query_wrap_state.was_wrapped ? 1 : 0,
			conn ? conn->polardb_query_wrap_state.stmt_pending : 0,
			conn && conn->polardb_query_wrap_state.wrapper_set_failed() ? 1 : 0);
		return;
	}

	// Ordinary consistency waits keep session wait state active until result
	// handling completes. Transaction split stores its wait target on the split
	// backend wrapper instead, so polardb_wait_active() is false even when the
	// backend returns the same structured LSN-timeout marker. Count that marker
	// against split counters while still using the generic wrapper-failed flag to
	// stop SET consumption.
	PgSQL_Session* sess = conn->myds ? conn->myds->sess : nullptr;
	const bool wait_active = sess && sess->polardb_wait_active();
	const bool txn_split_wait = conn->polardb_query_wrap_state.is_txn_split_wait();
	const bool is_lsn_timeout =
		(wait_active || txn_split_wait) && polardb_is_lsn_wait_timeout_result(result);
	const bool txn_split_lsn_timeout = txn_split_wait && is_lsn_timeout;
	const PolarDB_WrapperErrorAccounting accounting =
		polardb_wrapper_error_accounting(
			conn->polardb_query_wrap_state.was_wrapped,
			wait_active,
			is_lsn_timeout,
			conn->polardb_query_wrap_state.consuming_wrapper_set());
	if (!wait_active) {
		if (txn_split_lsn_timeout && sess) {
			sess->polardb_account_txn_split_wait_timeout("result-error");
		}
		POLARDB_TRACE("PolarDB WAIT: mark wrapper failed outside consistency wait "
			"sess=%p wait_active=%d txn_split_wait=%d lsn_timeout=%d pending=%u\n",
			(void*)sess, wait_active ? 1 : 0,
			txn_split_wait ? 1 : 0,
			is_lsn_timeout ? 1 : 0,
			conn->polardb_query_wrap_state.stmt_pending);
		// A transaction-split wait timeout belongs to the prepended wrapper
		// even after all SET results were consumed; the session failure-policy
		// path depends on this flag to avoid completing the split read.
		if (txn_split_lsn_timeout || accounting.mark_wrapper_failed) {
			conn->polardb_query_wrap_state.mark_wrapper_set_failed();
		}
		return;
	}

	POLARDB_TRACE("PolarDB WAIT: account check sess=%p wait_active=%d "
		"wait_type=%d wait_started=%lu pending=%u is_lsn_timeout=%d\n",
		(void*)sess, sess->polardb_wait_active() ? 1 : 0,
		(int)sess->polardb_query.wait.spec.type,
		(unsigned long)sess->polardb_query.wait.wait_started_at_us,
		conn->polardb_query_wrap_state.stmt_pending,
		is_lsn_timeout ? 1 : 0);

	// In strict mode the timeout can arrive after every wrapper SET result has
	// already been consumed: the wait is armed by SET polar_xact_split_wait_lsn,
	// then the backend raises ERROR before running the user SELECT. Count it only
	// when the structured marker is present AND the wait is still active, so an
	// ordinary user-query error that happens to resemble a timeout is not counted.
	if (accounting.mark_timeout_error) {
		sess->polardb_query.wait.timeout_error = true;
	}
	if (accounting.account_wait_timeout) {
		sess->polardb_account_wait_timeout("result-error");
	}

	if (accounting.mark_wrapper_failed) {
		conn->polardb_query_wrap_state.mark_wrapper_set_failed();
		POLARDB_TRACE("PolarDB WAIT: wrapper SET failure marked\n");
	}
}
#endif

extern char * binary_sha1;

#include "proxysql_find_charset.h"

void PgSQL_Variable::fill_server_internal_session(json &j, int conn_num, int idx) {
	j[conn_num]["conn"][pgsql_tracked_variables[idx].set_variable_name] = std::string(value?value:"");
}

void PgSQL_Variable::fill_client_internal_session(json &j, int idx) {
	j["conn"][pgsql_tracked_variables[idx].set_variable_name] = value?value:"";
}

PgSQL_Connection_userinfo::PgSQL_Connection_userinfo() {
	username=NULL;
	password=NULL;
	sha1_pass=NULL;
	dbname=NULL;
	fe_username=NULL;
	hash=0;
}

PgSQL_Connection_userinfo::~PgSQL_Connection_userinfo() {
	if (username) free(username);
	if (fe_username) free(fe_username);
	if (password) free(password);
	if (sha1_pass) free(sha1_pass);
	if (dbname) free(dbname);
}

uint64_t PgSQL_Connection_userinfo::compute_hash() {
	int l=0;
	if (username)
		l+=strlen(username);
	if (password)
		l+=strlen(password);
	if (dbname)
		l+=strlen(dbname);
// two random seperator
#define _COMPUTE_HASH_DEL1_	"-ujhtgf76y576574fhYTRDF345wdt-"
#define _COMPUTE_HASH_DEL2_	"-8k7jrhtrgJHRgrefgreyhtRFewg6-"
	l+=strlen(_COMPUTE_HASH_DEL1_);
	l+=strlen(_COMPUTE_HASH_DEL2_);
	char *buf=(char *)malloc(l+1);
	l=0;
	if (username) {
		strcpy(buf+l,username);
		l+=strlen(username);
	}
	strcpy(buf+l,_COMPUTE_HASH_DEL1_);
	l+=strlen(_COMPUTE_HASH_DEL1_);
	if (password) {
		strcpy(buf+l,password);
		l+=strlen(password);
	}
	if (dbname) {
		strcpy(buf+l, dbname);
		l+=strlen(dbname);
	}
	strcpy(buf+l,_COMPUTE_HASH_DEL2_);
	l+=strlen(_COMPUTE_HASH_DEL2_);
	hash=SpookyHash::Hash64(buf,l,0);
	free(buf);
	return hash;
}

void PgSQL_Connection_userinfo::set(char *user, char *pass, char *db, char *sh1) {
	if (user) {
		if (username) {
			if (strcmp(user,username)) {
				free(username);
				username=strdup(user);
			}
		} else {
			username=strdup(user);
		}
	}
	if (pass) {
		if (password) {
			if (strcmp(pass,password)) {
				free(password);
				password=strdup(pass);
			}
		} else {
			password=strdup(pass);
		}
	}
	if (db) {
		if (dbname) { 
			if (strcmp(db,dbname)) {
				free(dbname);
				dbname=strdup(db);
			}
		} else {
			dbname=strdup(db);
		}
	}
	if (sh1) {
		if (sha1_pass) {
			free(sha1_pass);
		}
		sha1_pass=strdup(sh1);
	}
	compute_hash();
}

void PgSQL_Connection_userinfo::set(PgSQL_Connection_userinfo *ui) {
	set(ui->username, ui->password, ui->dbname, ui->sha1_pass);
}

bool PgSQL_Connection_userinfo::set_dbname(const char* db) {
	assert(db);
	const int new_db_len = db ? strlen(db) : 0;
	const int old_db_len = dbname ? strlen(dbname) : 0;

	if (old_db_len == 0 || old_db_len != new_db_len || strcmp(db, dbname)) {
		if (dbname) {
			free(dbname);
		}
		dbname = (char*)malloc(new_db_len + 1);
		// Copy string including null terminator
		memcpy(dbname, db, new_db_len + 1);
		compute_hash();
		return true;
	}
	return false;
}

void print_backtrace(void);

#define NEXT_IMMEDIATE(new_st) do { async_state_machine = new_st; goto handler_again; } while (0)

PgSQL_Connection::PgSQL_Connection(bool is_client_conn) {
	proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 4, "Creating new PgSQL_Connection %p\n", this);
	is_client_connection = is_client_conn;
	pgsql_conn = NULL;
	result_type = 0;
	pgsql_result = NULL;
	query_result = NULL;
	query_result_reuse = NULL;
	//stmt_metadata_result = NULL;
	myds = NULL;
	parent = NULL;
	fd = -1;
	status_flags = 0;
	largest_query_length = 0;
	bytes_info.bytes_recv = 0;
	bytes_info.bytes_sent = 0;
	statuses.questions = 0;
	statuses.pgconnpoll_get = 0;
	statuses.pgconnpoll_put = 0;
	unknown_transaction_status = false;
	send_quit = true;
	reusable = false;
	multiplex_delayed = false;
	processing_multi_statement = false;
	async_state_machine = ASYNC_CONNECT_START;
	last_time_used = 0;
	creation_time = 0;
	auto_increment_delay_token = 0;
	query.ptr = NULL;
	query.length = 0;
	options.init_connect = NULL;
	options.init_connect_sent = false;
	userinfo = new PgSQL_Connection_userinfo();
	local_stmts = new PgSQL_STMT_Local(false); // false by default, it is a backend

	//for (int i = 0; i < PGSQL_NAME_LAST_HIGH_WM; i++) {
	//	variables[i].value = NULL;
	//	var_hash[i] = 0;
	//}

	new_result = true;
	is_copy_out = false;
	exit_pipeline_mode = false;
	resync_failed = false;
#if POLARDB_PROXY
	polardb_parent_bytes_recv_pending = 0;
	polardb_parent_bytes_sent_pending = 0;
	polardb_parent_queries_sent_pending = 0;
	polardb_parent_query_batch_count = 0;
	polardb_startup_profile_generation = 0;
	polardb_startup_identity_mode =
		static_cast<int>(PolarDB_ProxyIdentityMode::PROXY);
	polardb_forced_startup_identity = PolarDB_StartupIdentity{};
	polardb_forced_startup_parameters = false;
	polardb_startup_client = PolarDB_StartupClientContext{};
	polardb_pool_key = PolarDB_PoolKey{};
	polardb_core_pool_position = UINT32_MAX;
#endif // POLARDB_PROXY
	reset_error();
	memset(&connected_host_details, 0, sizeof(connected_host_details));
}

PgSQL_Connection::~PgSQL_Connection() {
	proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 4, "Destroying PgSQL_Connection %p\n", this);
#if POLARDB_PROXY
	polardb_flush_parent_bytes(PolarDB_ParentBytesFlushReason::Destructor);
#endif
	if (userinfo) {
		delete userinfo;
		userinfo = NULL;
	}
	if (pgsql_result) {
		PQclear(pgsql_result);
		pgsql_result = NULL;
	}
	if (local_stmts) {
		delete local_stmts;
		local_stmts = NULL;
	}
	if (pgsql_conn) {
		if (is_connected())  
			__sync_fetch_and_sub(&PgHGM->status.server_connections_connected, 1);
		async_free_result();
		PQfinish(pgsql_conn);
		pgsql_conn = NULL;
	}
	if (query_result) {
		delete query_result;
		query_result = NULL;
	}
	if (query_result_reuse) {
		delete query_result_reuse;
		query_result_reuse = NULL;
	}

	/*if (stmt_metadata_result) {
		delete stmt_metadata_result;
		stmt_metadata_result = NULL;
	}*/

	if (connected_host_details.hostname) {
		free(connected_host_details.hostname);
		connected_host_details.hostname = NULL;
	}
	if (connected_host_details.ip) {
		free(connected_host_details.ip);
		connected_host_details.hostname = NULL;
	}

	if (options.init_connect) free(options.init_connect);

	for (int i = 0; i < PGSQL_NAME_LAST_HIGH_WM; ++i) {
		if (variables[i].value) {
			free(variables[i].value);
			variables[i].value = NULL;
			var_hash[i] = 0;
		}
	}

	for (int i = 0; i < PGSQL_NAME_LAST_HIGH_WM; ++i) {
		if (startup_parameters[i]) {
			free(startup_parameters[i]);
			startup_parameters[i] = nullptr;
			startup_parameters_hash[i] = 0;
		}
	}
	reset_error_info(error_info, true);
}

void PgSQL_Connection::next_event(PG_ASYNC_ST new_st) {
#ifdef DEBUG
	int fd;
#endif /* DEBUG */
	wait_events = 0;

	if (async_exit_status & PG_EVENT_READ)
		wait_events |= POLLIN;
	if (async_exit_status & PG_EVENT_WRITE)
		wait_events |= POLLOUT;
	if (wait_events)
#ifdef DEBUG
		fd = PQsocket(pgsql_conn);
#else
		PQsocket(pgsql_conn);
#endif /* DEBUG */
	else
#ifdef DEBUG
		fd = -1;
#endif /* DEBUG */

	proxy_debug(PROXY_DEBUG_NET, 8, "fd=%d, wait_events=%d , old_ST=%d, new_ST=%d\n", fd, wait_events, async_state_machine, new_st);
	async_state_machine = new_st;
};


PG_ASYNC_ST PgSQL_Connection::handler(short event) {
#if POLARDB_PROXY && POLARDB_DEBUG
	{
		static std::atomic<int> _h_count{0};
		int _hc = _h_count.fetch_add(1, std::memory_order_relaxed) + 1;
		int _conn_id = pgsql_conn ? PQsocket(pgsql_conn) : -1;
		POLARDB_TRACE("[H%03d.000|C%d] ======== HANDLER ENTRY ======== async_state=%d event=%d\n",
			_hc, _conn_id, (int)async_state_machine, (int)event);
	}
#endif // POLARDB_PROXY && POLARDB_DEBUG
#if ENABLE_TIMER
	Timer timer(myds->sess->thread->Timers.Connections_Handlers);
#endif // ENABLE_TIMER
	uint64_t processed_bytes = 0;	// issue #527 : this variable will store the amount of bytes processed during this event
	if (pgsql_conn == NULL) {
		// it is the first time handler() is being called
		async_state_machine = ASYNC_CONNECT_START;
		myds->wait_until = myds->sess->thread->curtime + pgsql_thread___connect_timeout_server * 1000;
		if (myds->max_connect_time) {
			if (myds->wait_until > myds->max_connect_time) {
				myds->wait_until = myds->max_connect_time;
			}
		}
	}
handler_again:
#if POLARDB_PROXY && POLARDB_DEBUG
	POLARDB_TRACE("[H.010] ---- handler_again state=%d event=%d ----\n", (int)async_state_machine, (int)event);
#endif
	proxy_debug(PROXY_DEBUG_MYSQL_PROTOCOL, 6, "async_state_machine=%d\n", async_state_machine);
	switch (async_state_machine) {
	case ASYNC_CONNECT_START:
		connect_start();
#if POLARDB_PROXY
		if (is_error_present() && pgsql_conn == NULL) {
			NEXT_IMMEDIATE(ASYNC_CONNECT_FAILED);
		}
#endif // POLARDB_PROXY
		if (async_exit_status) {
			next_event(ASYNC_CONNECT_CONT);
		}
		else {
			NEXT_IMMEDIATE(ASYNC_CONNECT_END);
		}
		break;
	case ASYNC_CONNECT_CONT:
		if (event) {
			connect_cont(event);
		}
		if (async_exit_status) {
			if (myds->sess->thread->curtime >= myds->wait_until) {
				NEXT_IMMEDIATE(ASYNC_CONNECT_TIMEOUT);
			}
			next_event(ASYNC_CONNECT_CONT);
		} else {
			NEXT_IMMEDIATE(ASYNC_CONNECT_END);
		}
		break;
	case ASYNC_CONNECT_END:
		if (myds) {
			if (myds->sess) {
				if (myds->sess->thread) {
					unsigned long long curtime = monotonic_time();
					myds->sess->thread->atomic_curtime = curtime;
				}
			}
		}
		if (is_error_present()) {
			// always increase the counter
			const int conn_fd = pgsql_conn ? PQsocket(pgsql_conn) : -1;
			const int myds_fd = myds ? myds->fd : -1;
			proxy_error("Failed to PQconnectStart() on %u:%s:%d , FD (Conn:%d , MyDS:%d) , %s.\n", parent->myhgc->hid, parent->address, parent->port, conn_fd, myds_fd, get_error_code_with_message().c_str());
			NEXT_IMMEDIATE(ASYNC_CONNECT_FAILED);
		} else {
			if (PQisnonblocking(pgsql_conn) == false) {
				// Set non-blocking mode
				if (PQsetnonblocking(pgsql_conn, 1) != 0) {
					set_error_from_PQerrorMessage();
					proxy_error("Failed to set non-blocking mode: %s\n", get_error_code_with_message().c_str());
					NEXT_IMMEDIATE(ASYNC_CONNECT_FAILED);
				}
			}
			NEXT_IMMEDIATE(ASYNC_CONNECT_SUCCESSFUL);
		}
		break;
	case ASYNC_CONNECT_SUCCESSFUL:
		if (!is_connected()) 
			assert(0); // shouldn't ever reach here, we have messed up the state machine
		
		if (get_pg_ssl_in_use()) {
			if (myds && myds->sess && myds->sess->session_fast_forward) {
				assert(myds->ssl == NULL);
				SSL* ssl_obj = get_pg_ssl_object();
				if (ssl_obj != NULL) {
					myds->encrypted = true;
					myds->ssl = ssl_obj;
					myds->rbio_ssl = BIO_new(BIO_s_mem());
					myds->wbio_ssl = BIO_new(BIO_s_mem());
					SSL_set_bio(myds->ssl, myds->rbio_ssl, myds->wbio_ssl);
				}
				else {
					// it means that ProxySQL tried to use SSL to connect to the backend
					// but the backend didn't support SSL				
				}
			}
		}
		__sync_fetch_and_add(&PgHGM->status.server_connections_connected, 1);
		__sync_fetch_and_add(&parent->connect_OK, 1);
		// Seed the PgSQL DNS cache from the just-established connection so
		// the next connect for this hostname can skip getaddrinfo even if
		// the background resolver loop hasn't visited it yet.
		PgSQL_Monitor::update_dns_cache_from_pgsql_conn(pgsql_conn);
#if POLARDB_PROXY
		// Turn on PolarDB WAL-LSN reporting on this backend so a later writer
		// query can expose its LSN via PQgetLSN() with no extra round-trip.
		polardb_init_connection_tracking();
		if (myds && myds->sess) {
			myds->sess->polardb_apply_backend_isolation_status(this, "connect");
		}
#endif
		break;
	case ASYNC_CONNECT_FAILED:
		//PQfinish(pgsql_conn);//release connection even on error
		//pgsql_conn = NULL;
		PgHGM->p_update_pgsql_error_counter(p_pgsql_error_type::pgsql, parent->myhgc->hid, parent->address, parent->port, 9999 /* TODO: fix this mysql_errno(pgsql) */);
		parent->connect_error(9999 /* TODO: fix this mysql_errno(pgsql)*/);
		break;
	case ASYNC_CONNECT_TIMEOUT:
		// to fix
		//PQfinish(pgsql_conn);//release connection
		//pgsql_conn = NULL;
		proxy_error("Connect timeout on %s:%d : exceeded by %lluus\n", parent->address, parent->port, myds->sess->thread->curtime - myds->wait_until);
		PgHGM->p_update_pgsql_error_counter(p_pgsql_error_type::pgsql, parent->myhgc->hid, parent->address, parent->port, 9999/* TODO: fix this mysql_errno(pgsql)*/);
		parent->connect_error(9999 /* TODO: fix this mysql_errno(pgsql)*/);
		break;
	case ASYNC_QUERY_START:
		query_start();
#if POLARDB_PROXY
		update_queries_sent();
#else
		__sync_fetch_and_add(&parent->queries_sent, 1);
#endif
		update_bytes_sent(query.length + 5);
		statuses.questions++;
		if (async_exit_status) {
			next_event(ASYNC_QUERY_CONT);
		} else {
			if (is_error_present()) {
#if POLARDB_PROXY
				POLARDB_TRACE("PolarDB WAIT: ASYNC_QUERY_START early error "
					"wrapper_pending=%u wait_active=%d msg='%s'\n",
					polardb_query_wrap_state.stmt_pending,
					myds && myds->sess && myds->sess->polardb_wait_active() ? 1 : 0,
					get_error_message().c_str());
#endif
				NEXT_IMMEDIATE(ASYNC_QUERY_END);
			}
			NEXT_IMMEDIATE(ASYNC_USE_RESULT_START);
		}
		break;
	case ASYNC_QUERY_CONT:
		if (event) {
			query_cont(event);
		}
		if (async_exit_status) {
			next_event(ASYNC_QUERY_CONT);
		} else {
			if (is_error_present() || 
				!set_single_row_mode()) {
#if POLARDB_PROXY
				POLARDB_TRACE("PolarDB WAIT: ASYNC_QUERY_CONT early end "
					"is_error=%d wrapper_pending=%u wait_active=%d msg='%s'\n",
					is_error_present() ? 1 : 0,
					polardb_query_wrap_state.stmt_pending,
					myds && myds->sess && myds->sess->polardb_wait_active() ? 1 : 0,
					get_error_message().c_str());
#endif
				NEXT_IMMEDIATE(ASYNC_QUERY_END);
			}
			set_fetch_result_end_state(ASYNC_QUERY_END);
			NEXT_IMMEDIATE(ASYNC_USE_RESULT_START);
		}
		break;
	case ASYNC_USE_RESULT_START:
		fetch_result_start();
		if (async_exit_status == PG_EVENT_NONE) {
			if (is_error_present()) {
#if POLARDB_PROXY
				// The fetch start already failed, so there is no per-result PGresult
				// to classify here; pass null and let the helper decide from session
				// wait state whether a wrapper wait timeout still needs accounting.
				polardb_account_wrapper_set_error(this, nullptr, get_error_message().c_str());
#endif
				NEXT_IMMEDIATE(fetch_result_end_st);
			}
			init_query_result();
			NEXT_IMMEDIATE(ASYNC_USE_RESULT_CONT);
		} else {
			assert(0); // shouldn't ever reach here
		}
		break;
	case ASYNC_USE_RESULT_CONT:
	{
		if (myds->sess && myds->sess->client_myds && myds->sess->mirror == false) { // see issue#4072
			const unsigned int buffered_data = myds->sess->client_myds->PSarrayOUT->len * PGSQL_RESULTSET_BUFLEN;
			if (buffered_data > overflow_safe_multiply<8,unsigned int>(pgsql_thread___threshold_resultset_size)) {
				next_event(ASYNC_USE_RESULT_CONT); // we temporarily pause . See #1232
				break;
			}
		}

		fetch_result_cont(event);
		if (async_exit_status) {
			next_event(ASYNC_USE_RESULT_CONT);
			break;
		}

		if (result_type == 1) {
			std::unique_ptr<PGresult, decltype(&PQclear)> result(get_result(), PQclear);

			if (result) {

				const ExecStatusType exec_status_type = PQresultStatus(result.get());

#if POLARDB_PROXY
				// Consume the wrapper SET results inline. A wrapped LSN-wait read
				// prepends N SET statements; each completes as PGRES_COMMAND_OK or
				// PGRES_EMPTY_QUERY. We silently discard those results (do NOT buffer
				// them to the client) and forward only the (N+1)th result, the user's
				// query. The connection is the sole owner of SET consumption, so the
				// session and client only ever see the user result. Wrapped reads are
				// simple queries, so this only triggers at the simple-query end state.
				if (polardb_query_wrap_state.has_pending() &&
					fetch_result_end_st == ASYNC_QUERY_END) {
					if (exec_status_type == PGRES_COMMAND_OK ||
						exec_status_type == PGRES_EMPTY_QUERY) {
						const bool consumed_xids_reset =
							polardb_query_wrap_state.consuming_txn_split_xids_reset();
						polardb_query_wrap_state.consume_successful_wrapper_set();
						if (consumed_xids_reset) {
							// Clear the dirty flag later, when the whole reset-wrapped
							// query completes without error. Until then the physical
							// backend must still be treated as needing cleanup.
							polardb_txn_split_xids_reset_consumed = true;
							polardb_query_wrap_state.txn_split_xids_reset_pending = false;
						}
						POLARDB_TRACE("PolarDB: wrapper result consumed: status=%d pending=%u\n",
							(int)exec_status_type, polardb_query_wrap_state.stmt_pending);
						// Discard the SET result and reuse its buffer for the next
						// result (init_query_result() picks up query_result_reuse).
						if (query_result) {
							if (query_result_reuse) delete query_result_reuse;
							query_result_reuse = query_result;
							query_result = nullptr;
						}
						NEXT_IMMEDIATE(ASYNC_USE_RESULT_START);
					} else if (exec_status_type == PGRES_FATAL_ERROR ||
						exec_status_type == PGRES_NONFATAL_ERROR ||
						exec_status_type == PGRES_BAD_RESPONSE) {
						// A wrapper statement itself errored (e.g. strict-mode wait timeout
						// surfaced as ERROR). Strict wait errors are normal PostgreSQL
						// ErrorResponse packets, not dead-connection rc=-1 failures, so
						// account them here while we still know the error belongs to a
						// wrapper SET result. Then stop consuming and let the error flow
						// to the client through the normal path below.
						if (polardb_query_wrap_state.consuming_txn_split_xids_reset()) {
							reusable = false;
							polardb_txn_split_xids_reset_consumed = false;
							POLARDB_TRACE(
								"PolarDB TXN_SPLIT: xids reset SET failed conn=%p\n",
								(void*)this);
						}
						POLARDB_TRACE("PolarDB: wrapper result error: status=%d pending=%u\n",
							(int)exec_status_type, polardb_query_wrap_state.stmt_pending);
						polardb_account_wrapper_set_error(this, result.get(), PQresultErrorMessage(result.get()));
					}
				}
#endif // POLARDB_PROXY

				// Multi-statements are supported only in simple queries
				if (fetch_result_end_st == ASYNC_QUERY_END &&
					(query_result->get_result_packet_type() & (PGSQL_QUERY_RESULT_COMMAND | PGSQL_QUERY_RESULT_EMPTY | PGSQL_QUERY_RESULT_ERROR))) {
					next_multi_statement_result(result.release());
					next_event(ASYNC_USE_RESULT_START);
					break;
				}

				switch (exec_status_type) {
				case PGRES_COMMAND_OK:
					{
						unsigned int bytes_recv = 0;
						switch (fetch_result_end_st)
						{
						case ASYNC_STMT_PREPARE_END:
							bytes_recv = query_result->add_parse_completion();
							break;
						case ASYNC_STMT_DESCRIBE_END:
							bytes_recv = query_result->add_describe_completion(result.get(), query.extended_query_info->stmt_type);
							break;
						case ASYNC_STMT_EXECUTE_END:
							// PQsendQueryPrepared sends the sequence BIND -> DESCRIBE(PORTAL) -> EXECUTE -> SYNC
							// Since libpq does not indicate whether the DESCRIBE PORTAL step produced a
							// NoData packet for commands such as INSERT, DELETE, or UPDATE.
							// In these cases, libpq returns PGRES_COMMAND_OK (whereas SELECT statements
							// yield PGRES_SINGLE_TUPLE or PGRES_TUPLES_OK). Therefore, it is safe to
							// explicitly append a NoData packet to the result.
							if ((query.extended_query_info->flags & PGSQL_EXTENDED_QUERY_FLAG_DESCRIBE_PORTAL) != 0) {
								bytes_recv = query_result->add_no_data();
							}
							// fallthrough
						default:
							bytes_recv += query_result->add_command_completion(result.get());
							break;
						}
						update_bytes_recv(bytes_recv);
					}
					NEXT_IMMEDIATE(ASYNC_USE_RESULT_CONT);
					break;
				case PGRES_EMPTY_QUERY:
					{
						unsigned int bytes_recv = 0;

						if (fetch_result_end_st == ASYNC_STMT_EXECUTE_END) {
							if ((query.extended_query_info->flags & PGSQL_EXTENDED_QUERY_FLAG_DESCRIBE_PORTAL) != 0) {
								bytes_recv = query_result->add_no_data();
							}
						}
						bytes_recv += query_result->add_empty_query_response(result.get());
						update_bytes_recv(bytes_recv);
					}
					NEXT_IMMEDIATE(ASYNC_USE_RESULT_CONT);
					break;
				case PGRES_TUPLES_OK:
				case PGRES_SINGLE_TUPLE:
					break;
				case PGRES_COPY_OUT:
					if (handle_copy_out(result.get(), &processed_bytes) == false) {
						next_event(ASYNC_USE_RESULT_CONT);
						return async_state_machine; // Threashold for result size reached. Pause temporarily
					}
					NEXT_IMMEDIATE(ASYNC_USE_RESULT_CONT);
					break;
				case PGRES_COPY_IN:
				case PGRES_COPY_BOTH:
					// disconnect client session (and backend connection) if COPY (STDIN) command bypasses the initial checks.
					// This scenario should be handled in fast-forward mode and should never occur at this point.
					if (myds && myds->sess) {
						proxy_warning("Unable to process the '%s' command from client %s:%d. Please report a bug for future enhancements.\n", 
							myds->sess->CurrentQuery.QueryParserArgs.digest_text ? myds->sess->CurrentQuery.QueryParserArgs.digest_text : "COPY",
							myds->sess->client_myds->addr.addr, myds->sess->client_myds->addr.port);
					} else {
						proxy_warning("Unable to process the 'COPY' command. Please report a bug for future enhancements.\n");
					}
					set_error(PGSQL_ERROR_CODES::ERRCODE_RAISE_EXCEPTION, "Unable to process 'COPY' command", true);
					NEXT_IMMEDIATE(fetch_result_end_st);
					break;
				case PGRES_PIPELINE_SYNC:
					// backend connection is in Ready for Query state, we can now safely exit pipeline mode
					exit_pipeline_mode = true;
					NEXT_IMMEDIATE(ASYNC_USE_RESULT_CONT);
					break;
				case PGRES_PIPELINE_ABORTED:
					// received an extended query immediately after an error was triggered by a previous query (before sync).
					// In ProxySQL this should never happen, since the extended query frame is reset after an error.
					// However, it may rarely occur if an error is raised during the "describe portal" phase (while executing).
					// In that case, we continue until PGRES_PIPELINE_SYNC (Ready for Query state) is received, then safely exit pipeline mode.
					NEXT_IMMEDIATE(ASYNC_USE_RESULT_CONT);
					break;
				case PGRES_BAD_RESPONSE:
				case PGRES_NONFATAL_ERROR:
				case PGRES_FATAL_ERROR:
				default:
					// if on previous call we encountered a FATAL error, we will not process the result, as it will contain residual protocol messages
					// from the broken connection
					if (is_error_present() == true && get_error_severity() == PGSQL_ERROR_SEVERITY::ERRSEVERITY_FATAL) {
						NEXT_IMMEDIATE(ASYNC_USE_RESULT_CONT);
					}

					// we don't have a command completion, empty query responseor error packet in the result. This check is here to 
					// handle internal cleanup of libpq that might return residual protocol messages from the broken connection and 
					// may add multiple final packets.
					//if ((query_result->get_result_packet_type() & (PGSQL_QUERY_RESULT_COMMAND | PGSQL_QUERY_RESULT_EMPTY | PGSQL_QUERY_RESULT_ERROR)) == 0) {
					set_error_from_result(result.get(), PGSQL_ERROR_FIELD_ALL);
					assert(is_error_present());
#if POLARDB_PROXY
					// Catch-all error path for a wrapper read that reached the generic
					// FATAL/bad-response handling: account a wrapper wait timeout here too.
					polardb_account_wrapper_set_error(this, result.get(), PQresultErrorMessage(result.get()));
#endif

					// we will not send FATAL error messages to the client
					const PGSQL_ERROR_SEVERITY severity = get_error_severity();
					if (severity == PGSQL_ERROR_SEVERITY::ERRSEVERITY_ERROR ||
						severity == PGSQL_ERROR_SEVERITY::ERRSEVERITY_WARNING ||
						severity == PGSQL_ERROR_SEVERITY::ERRSEVERITY_NOTICE) {

						const unsigned int bytes_recv = query_result->add_error(result.get());
						update_bytes_recv(bytes_recv);
					}

					const PGSQL_ERROR_CATEGORY error_category = get_error_category();
					if (error_category != PGSQL_ERROR_CATEGORY::ERRCATEGORY_SYNTAX_ERROR &&
						error_category != PGSQL_ERROR_CATEGORY::ERRCATEGORY_STATUS &&
						error_category != PGSQL_ERROR_CATEGORY::ERRCATEGORY_DATA_ERROR) {
						proxy_error("Error: %s, Multi-Statement: %d\n", get_error_code_with_message().c_str(), processing_multi_statement);
					}
					NEXT_IMMEDIATE(ASYNC_USE_RESULT_CONT);
				}

				if (new_result == true) {
					bool should_add_row_description = true;

					// In extended query mode, we should add RowDescription only if the DESCRIBE PORTAL message was sent
					// before the EXECUTE message.
					if (fetch_result_end_st == ASYNC_STMT_EXECUTE_END) {
						should_add_row_description =
							(query.extended_query_info->flags & PGSQL_EXTENDED_QUERY_FLAG_DESCRIBE_PORTAL) != 0;
					}

					if (should_add_row_description) {
						const auto bytes_recv = query_result->add_row_description(result.get());
						update_bytes_recv(bytes_recv);
					} else {
						query_result->num_fields = PQnfields(result.get());
					}

					new_result = false;
				}

				if (PQntuples(result.get()) > 0) {
					const unsigned int bytes_recv = query_result->add_row(result.get());
					update_bytes_recv(bytes_recv);
					processed_bytes += bytes_recv;	// issue #527 : this variable will store the amount of bytes processed during this event
					
					bool suspend_resultset_fetch = (processed_bytes > overflow_safe_multiply<8,unsigned int>(pgsql_thread___threshold_resultset_size));
					 
					if (suspend_resultset_fetch == true && myds->sess && myds->sess->qpo && myds->sess->qpo->cache_ttl > 0) {
						suspend_resultset_fetch = (processed_bytes > ((uint64_t)pgsql_thread___query_cache_size_MB) * 1024ULL * 1024ULL);
					}
					
					if (
						suspend_resultset_fetch
						||
						(pgsql_thread___throttle_ratio_server_to_client && pgsql_thread___throttle_max_bytes_per_second_to_client && (processed_bytes > (unsigned long long)pgsql_thread___throttle_max_bytes_per_second_to_client / 10 * (unsigned long long)pgsql_thread___throttle_ratio_server_to_client))
						) {
						next_event(ASYNC_USE_RESULT_CONT); // we temporarily pause
						break;
					} else {
						NEXT_IMMEDIATE(ASYNC_USE_RESULT_CONT); // we continue looping 
					}
				} else {
					const unsigned int bytes_recv=query_result->add_command_completion(result.get(), false);
					update_bytes_recv(bytes_recv);
					NEXT_IMMEDIATE(ASYNC_USE_RESULT_CONT);
				}
			}
		} else if (result_type == 2) {
			if (ps_result.id == 'D') {
				unsigned int bytes_recv=query_result->add_row(&ps_result);
				update_bytes_recv(bytes_recv);
				processed_bytes += bytes_recv;	// issue #527 : this variable will store the amount of bytes processed during this event

				bool suspend_resultset_fetch = (processed_bytes > overflow_safe_multiply<8,unsigned int>(pgsql_thread___threshold_resultset_size));

				if (suspend_resultset_fetch == true && myds->sess && myds->sess->qpo && myds->sess->qpo->cache_ttl > 0) {
					suspend_resultset_fetch = (processed_bytes > ((uint64_t)pgsql_thread___query_cache_size_MB) * 1024ULL * 1024ULL);
				}

				if (
					suspend_resultset_fetch
					||
					(pgsql_thread___throttle_ratio_server_to_client && pgsql_thread___throttle_max_bytes_per_second_to_client && (processed_bytes > (unsigned long long)pgsql_thread___throttle_max_bytes_per_second_to_client / 10 * (unsigned long long)pgsql_thread___throttle_ratio_server_to_client))
					) {
					next_event(ASYNC_USE_RESULT_CONT); // we temporarily pause
					break;
				} else {
					NEXT_IMMEDIATE(ASYNC_USE_RESULT_CONT); // we continue looping
				}
			} else {
				assert(0);
			}
#if POLARDB_PROXY
		} else if (result_type == 3) {
			const unsigned int bytes_recv = polardb_row_run_last_bytes;
			polardb_row_run_last_bytes = 0;
			assert(bytes_recv > 0);
			update_bytes_recv(bytes_recv);
			processed_bytes += bytes_recv;	// issue #527 : this variable will store the amount of bytes processed during this event

			bool suspend_resultset_fetch = (processed_bytes > overflow_safe_multiply<8,unsigned int>(pgsql_thread___threshold_resultset_size));

			if (suspend_resultset_fetch == true && myds->sess && myds->sess->qpo && myds->sess->qpo->cache_ttl > 0) {
				suspend_resultset_fetch = (processed_bytes > ((uint64_t)pgsql_thread___query_cache_size_MB) * 1024ULL * 1024ULL);
			}

			if (
				suspend_resultset_fetch
				||
				(pgsql_thread___throttle_ratio_server_to_client && pgsql_thread___throttle_max_bytes_per_second_to_client && (processed_bytes > (unsigned long long)pgsql_thread___throttle_max_bytes_per_second_to_client / 10 * (unsigned long long)pgsql_thread___throttle_ratio_server_to_client))
				) {
				next_event(ASYNC_USE_RESULT_CONT); // we temporarily pause
				break;
			} else {
				NEXT_IMMEDIATE(ASYNC_USE_RESULT_CONT); // we continue looping
			}
#endif // POLARDB_PROXY
		} else {
			assert(0);
		}

		// if we arrive here via async_perform_resync, the connection is in "Ready for Query" state,  
		// but query_result will be empty. In this case, we check exit_pipeline_mode; if it is true,  
		// it indicates a non-error scenario and we skip this check.
		if (exit_pipeline_mode == false &&
			(query_result->get_result_packet_type() & (PGSQL_QUERY_RESULT_COMMAND | PGSQL_QUERY_RESULT_EMPTY | PGSQL_QUERY_RESULT_ERROR)) == 0) {
			// if we reach here we assume that error_info is already set in previous call
			if (!is_error_present())
				assert(0); // we might have missed setting error_info in previous call

			query_result->add_error(NULL);
		}

		if (fetch_result_end_st != ASYNC_QUERY_END) {
			bool has_error = (query_result->get_result_packet_type() & PGSQL_QUERY_RESULT_ERROR) != 0;

			// Normally, ReadyForQuery is not sent immediately if we are in extended query mode
			// and there are pending messages in the queue, as it will be sent once the entire
			// extended query frame has been processed.
			//
			// Edge case: if a message fails with an error while the queue still contains pending
			// messages, the queue will be cleared later in the session. In this situation,
			// ReadyForQuery would never be sent because the pending messages are discarded.
			//
			// Fix: if the result indicates an error, explicitly send ReadyForQuery immediately.
			// The extended query frame will still be reset later in the session.
			if (!myds->sess->is_extended_query_ready_for_query() && !has_error) {
				// Skip sending ReadyForQuery if there are still extended query messages pending in the queue
				NEXT_IMMEDIATE(fetch_result_end_st);
			}

			// An error has occurred while executing extended query sequence,  
			// and connection is not in 'Ready for Query' state, i.e., unsynchronized.  
			// To recover, we must resync by sending a SYNC to the backend connection.
			if (!exit_pipeline_mode && has_error) {
				NEXT_IMMEDIATE(ASYNC_RESYNC_START);
			}
		}

#if POLARDB_PROXY
		if (myds && myds->sess &&
				myds->sess->polardb_query.backend_isolation_status_needed) {
			myds->sess->polardb_apply_backend_isolation_status(
				this, "query_result");
		}
#endif // POLARDB_PROXY

		// finally add ready for query packet
#if POLARDB_PROXY
		/*
		 * PolarDB client RFQ-LSN passthrough.
		 *
		 * Backend side: ProxySQL requests RFQ LSN from PolarDB backends through
		 * its backend startup profile. The patched libpq parser caches the LSN
		 * from the last backend ReadyForQuery; has_polardb_lsn_payload() tells us
		 * whether the backend RFQ actually carried the extension, and
		 * get_polardb_lsn() returns the cached value.
		 *
		 * Frontend side: a PolarDB-aware client can opt in with the same startup
		 * keys used by PolarDB libpq (_polar_send_lsn=true or
		 * _polar_proxy_send_lsn=true). PgSQL_Protocol consumes those keys during
		 * client startup and sets polardb_route_state.client_rfq_lsn_requested, so they do not
		 * leak into generic PostgreSQL startup-parameter handling.
		 *
		 * Client-visible flow examples:
		 *  - Autocommit, one query per protocol message:
		 *      SELECT ...
		 *    Each backend-backed query ends with ReadyForQuery. If the client
		 *    startup requested _polar_send_lsn=true or _polar_proxy_send_lsn=true,
		 *    ProxySQL appends an LSN to that client RFQ when the backend carried
		 *    the RFQ-LSN payload. No-wait replica responses expose the backend
		 *    value unchanged; writer responses and successful LSN-wait responses
		 *    may be translated to a higher confirmed frontend target so the
		 *    client-visible RFQ stream does not move backwards when ProxySQL
		 *    reuses another backend connection.
		 *
		 *  - Explicit transaction, separate statements:
		 *      BEGIN;
		 *      INSERT ...;
		 *      SELECT ...;
		 *      COMMIT;
		 *    Each statement gets its own backend RFQ and client RFQ:
		 *      BEGIN  -> RFQ status T + LSN if backend sent it
		 *      INSERT -> RFQ status T + updated LSN if WAL advanced
		 *      SELECT -> RFQ status T + backend RFQ LSN, or the session target
		 *                if the writer/wait result is higher
		 *      COMMIT -> RFQ status I + commit RFQ LSN
		 *
		 *  - Multi-statement in one Simple Query packet:
		 *      BEGIN; INSERT ...; SELECT ...; COMMIT;
		 *    PostgreSQL protocol emits only one ReadyForQuery at the end of the
		 *    packet, so the client gets one final RFQ LSN, not one LSN per
		 *    semicolon-delimited statement.
		 *
		 *  - Extended protocol:
		 *    ReadyForQuery is emitted after Sync, not after every
		 *    Parse/Bind/Execute message, so LSN availability follows Sync
		 *    boundaries. RFQ-LSN forwarding is supported for this backend-backed
		 *    extended path, but PolarDB wait/split handling is routing-only for
		 *    extended protocol: it does not inject LSN-wait or split-XID SQL
		 *    wrappers into Parse/Bind/Execute streams. If an extended-protocol read
		 *    needs a wait, the PolarDB extended route helper forces the writer
		 *    instead of trying to wrap the extended message flow.
		 *
		 *  - Proxy-local responses:
		 *    If ProxySQL generates a response locally without a backend RFQ, it
		 *    sends standard RFQ and does not fabricate an LSN. A patched libpq
		 *    client should therefore report PQhasLSN() == 0. If a backend-backed
		 *    response reaches this path but the backend RFQ carried no LSN payload,
		 *    ProxySQL also sends standard RFQ, even when it has a session target.
		 *
		 * Query cache is disabled for LSN-aware clients because cached wire bytes
		 * would otherwise replay a stale or missing ReadyForQuery LSN.
		 */
		bool include_client_lsn = false;
		uint64_t client_lsn = 0;
		if (myds && myds->sess) {
			const bool backend_payload_present = has_polardb_lsn_payload();
			const uint64_t backend_lsn = backend_payload_present
				? get_polardb_lsn()
				: 0;
			include_client_lsn = myds->sess->polardb_client_ready_lsn(
				this, backend_payload_present, backend_lsn, &client_lsn);
		}
		const unsigned int ready_bytes = query_result->add_ready_status(
			PQtransactionStatus(pgsql_conn), include_client_lsn, client_lsn);
		update_bytes_recv(ready_bytes);
#else
		query_result->add_ready_status(PQtransactionStatus(pgsql_conn));
		update_bytes_recv(6);
#endif // POLARDB_PROXY
		//processing_multi_statement = false;
		NEXT_IMMEDIATE(fetch_result_end_st);
	}
	break;

	case ASYNC_STMT_PREPARE_START:
		stmt_prepare_start();
#if POLARDB_PROXY
		update_queries_sent();
#else
		__sync_fetch_and_add(&parent->queries_sent, 1);
#endif
		update_bytes_sent(query.length + 5);
		statuses.questions++;
		if (async_exit_status) {
			next_event(ASYNC_STMT_PREPARE_CONT);
		} else {
			NEXT_IMMEDIATE(ASYNC_STMT_PREPARE_END);
		}
		break;
	case ASYNC_STMT_PREPARE_CONT:
		if (event) {
			stmt_prepare_cont(event);
		}
		if (async_exit_status) {
			next_event(ASYNC_STMT_PREPARE_CONT);
		} else {
			if (is_error_present()) {
				NEXT_IMMEDIATE(ASYNC_STMT_PREPARE_END);
			}
			set_fetch_result_end_state(ASYNC_STMT_PREPARE_END);
			NEXT_IMMEDIATE(ASYNC_USE_RESULT_START);
		}
		break;

	case ASYNC_STMT_DESCRIBE_START:
	{
		stmt_describe_start();
#if POLARDB_PROXY
		update_queries_sent();
#else
		__sync_fetch_and_add(&parent->queries_sent, 1);
#endif
		size_t bytes_sent = 7 + 5; // 7 for DESCRIBE header, 5 for SYNC/FLUSH
		if (query.extended_query_info->stmt_type == 'P') {
			bytes_sent += query.extended_query_info->stmt_client_portal_name ? (strlen(query.extended_query_info->stmt_client_portal_name) + 1) : 0;
		} else {
			bytes_sent += query.backend_stmt_name ? (strlen(query.backend_stmt_name) + 1) : 0;
		}
		update_bytes_sent(bytes_sent);
		statuses.questions++;
		if (async_exit_status) {
			next_event(ASYNC_STMT_DESCRIBE_CONT);
		} else {
			NEXT_IMMEDIATE(ASYNC_STMT_DESCRIBE_END);
		}
	}
	break;
	case ASYNC_STMT_DESCRIBE_CONT:
		if (event) {
			stmt_describe_cont(event);
		}
		if (async_exit_status) {
			next_event(ASYNC_STMT_DESCRIBE_CONT);
		} else {
			if (is_error_present()) {
				NEXT_IMMEDIATE(ASYNC_STMT_DESCRIBE_END);
			}
			set_fetch_result_end_state(ASYNC_STMT_DESCRIBE_END);
			NEXT_IMMEDIATE(ASYNC_USE_RESULT_START);
		}
		break;

	case ASYNC_STMT_EXECUTE_START:
		stmt_execute_start();
#if POLARDB_PROXY
		update_queries_sent();
#else
		__sync_fetch_and_add(&parent->queries_sent, 1);
#endif
		update_bytes_sent(query.extended_query_info->bind_msg->get_raw_pkt().size + 5);
		statuses.questions++;
		if (async_exit_status) {
			next_event(ASYNC_STMT_EXECUTE_CONT);
		} else {
			NEXT_IMMEDIATE(ASYNC_STMT_EXECUTE_END);
		}
		break;
	case ASYNC_STMT_EXECUTE_CONT:
		if (event) {
			stmt_execute_cont(event);
		}
		if (async_exit_status) {
			next_event(ASYNC_STMT_EXECUTE_CONT);
		} else {
			if (is_error_present() ||
				!set_single_row_mode()) {
				NEXT_IMMEDIATE(ASYNC_STMT_EXECUTE_END);
			}
			set_fetch_result_end_state(ASYNC_STMT_EXECUTE_END);
			NEXT_IMMEDIATE(ASYNC_USE_RESULT_START);
		}
		break;

	case ASYNC_RESYNC_END:
		// if we reach here, it means that the connection is now synchronized
		if (resync_failed) {
			// if resync failed
			set_error(PGSQL_ERROR_CODES::ERRCODE_RAISE_EXCEPTION, "Failed to synchronize connection", false);
		}
		// fall through
	case ASYNC_QUERY_END:
	case ASYNC_STMT_PREPARE_END:
	case ASYNC_STMT_DESCRIBE_END:
	case ASYNC_STMT_EXECUTE_END:
		PROXY_TRACE2();

		if (is_error_present()) {
			compute_unknown_transaction_status();
		} else {
			unknown_transaction_status = false;
		}

		PQsetNoticeReceiver(pgsql_conn, &PgSQL_Connection::unhandled_notice_cb, this);

		// we check exit_pipeline_mode to ensure it is safe to exit pipeline mode
		if (exit_pipeline_mode &&
			PQpipelineStatus(pgsql_conn) == PQ_PIPELINE_ON) {
			if (PQexitPipelineMode(pgsql_conn) == 0) {
				set_error_from_PQerrorMessage();
				proxy_error("Failed to exit pipeline mode. %s\n", get_error_code_with_message().c_str());
			}
			exit_pipeline_mode = false;
		}
		// should be NULL
		assert(!pgsql_result);
		assert(!is_copy_out);
		break;

	case ASYNC_RESYNC_START:
		if (PQpipelineStatus(pgsql_conn) == PQ_PIPELINE_OFF) {
			proxy_warning("Resync not required - connection already synchronized.\n");
			NEXT_IMMEDIATE(ASYNC_RESYNC_END);
		}
		resync_start();
		update_bytes_sent(5); // SYNC message
		if (async_exit_status) {
			next_event(ASYNC_RESYNC_CONT);
		} else {
			NEXT_IMMEDIATE(ASYNC_RESYNC_END);
		}
		break;
	case ASYNC_RESYNC_CONT:
		if (event) {
			resync_cont(event);
		}
		if (async_exit_status) {
			if (myds->wait_until != 0 && myds->sess->thread->curtime >= myds->wait_until) {
				proxy_error("Timeout waiting for pipeline sync to complete.\n");
				resync_failed = true;
				NEXT_IMMEDIATE(ASYNC_RESYNC_END);
			}
			next_event(ASYNC_RESYNC_CONT);
			break;
		} else {
			if (resync_failed == true) {
				NEXT_IMMEDIATE(ASYNC_RESYNC_END);
			}
			if (query_result && query_result->result_packet_type != PGSQL_QUERY_RESULT_NO_DATA) {
				// we have already have some result set, so we just continue
				NEXT_IMMEDIATE(ASYNC_USE_RESULT_CONT);
			} else {
				set_fetch_result_end_state(ASYNC_RESYNC_END);
				NEXT_IMMEDIATE(ASYNC_USE_RESULT_START);
			}
		}
		break;		

	case ASYNC_RESET_SESSION_START:
		reset_session_start();
		if (reset_session_in_pipeline) {
			update_bytes_sent(5);
		}
		else {
			update_bytes_sent((reset_session_in_txn == false ? (sizeof("DISCARD ALL") + 5) : (sizeof("ROLLBACK") + 5)));
		}
		if (async_exit_status) {
			next_event(ASYNC_RESET_SESSION_CONT);
		}
		else {
			if (is_error_present()) {
				NEXT_IMMEDIATE(ASYNC_RESET_SESSION_END);
			}
			NEXT_IMMEDIATE(ASYNC_RESET_SESSION_CONT);
		}
		break;
	case ASYNC_RESET_SESSION_CONT:
	{
		if (event) {
			reset_session_cont(event);
		}
		if (async_exit_status) {
			if (myds->wait_until != 0 && myds->sess->thread->curtime >= myds->wait_until) {
				NEXT_IMMEDIATE(ASYNC_RESET_SESSION_TIMEOUT);
			}
			next_event(ASYNC_RESET_SESSION_CONT);
			break;
		}
		if (is_error_present()) {
			NEXT_IMMEDIATE(ASYNC_RESET_SESSION_END);
		}
		PGresult* result = get_result();
		if (result) {
			if (PQresultStatus(result) != PGRES_COMMAND_OK &&
				PQresultStatus(result) != PGRES_PIPELINE_SYNC) {
				set_error_from_result(result, PGSQL_ERROR_FIELD_ALL);
				assert(is_error_present());
			}
			PQclear(result);
			NEXT_IMMEDIATE(ASYNC_RESET_SESSION_CONT);
		}
		if (reset_session_in_pipeline) {
			if (PQexitPipelineMode(pgsql_conn) == 0) {
				set_error_from_PQerrorMessage();
				proxy_error("Failed to exit pipeline mode. %s\n", get_error_code_with_message().c_str());
				NEXT_IMMEDIATE(ASYNC_RESET_SESSION_END);
			}
			reset_session_in_pipeline = false;
			NEXT_IMMEDIATE(ASYNC_RESET_SESSION_START);
		}
		if (reset_session_in_txn) {
			reset_session_in_txn = false;
			NEXT_IMMEDIATE(ASYNC_RESET_SESSION_START);
		}
		NEXT_IMMEDIATE(ASYNC_RESET_SESSION_END);
	}
	break;
	case ASYNC_RESET_SESSION_END:
		PQsetNoticeReceiver(pgsql_conn, &PgSQL_Connection::unhandled_notice_cb, this);
		if (is_error_present()) {
			NEXT_IMMEDIATE(ASYNC_RESET_SESSION_FAILED);
		}
		NEXT_IMMEDIATE(ASYNC_RESET_SESSION_SUCCESSFUL);
		break;
	case ASYNC_RESET_SESSION_FAILED:
	case ASYNC_RESET_SESSION_SUCCESSFUL:
	case ASYNC_RESET_SESSION_TIMEOUT:
		break;

	default:
		// not implemented yet
		assert(0); 
	}
	return async_state_machine;
}

static void append_conninfo_param(std::ostringstream& conninfo, const char* key, char* val) {
	if (!val) return;
	char* escaped_str = escape_string_single_quotes_and_backslashes(val, false);
	conninfo << key << "='" << escaped_str << "' ";
	if (escaped_str != val) {
		free(escaped_str);
	}
}

std::string PgSQL_Connection::connect_start_DNS_lookup() {
	// PgSQL_Monitor::dns_lookup() returns an IP on cache hit, or empty
	// on miss / when 'parent->address' is itself an IP / when the cache is
	// disabled.  Empty result means "don't pass hostaddr to libpq" so the
	// existing behavior (libpq does getaddrinfo) is preserved.
	const std::string ip = PgSQL_Monitor::dns_lookup(parent->address,
		/*return_hostname_if_lookup_fails=*/false);
	return ip;
}

void PgSQL_Connection::connect_start() {
	PROXY_TRACE();
	assert(pgsql_conn == NULL); // already there is a connection
	reset_error();
	async_exit_status = PG_EVENT_NONE;

	std::ostringstream conninfo;
	append_conninfo_param(conninfo, "user", userinfo->username); // username
	append_conninfo_param(conninfo, "password", userinfo->password); // password
	append_conninfo_param(conninfo, "dbname", userinfo->dbname); // dbname
	append_conninfo_param(conninfo, "host", parent->address); // backend address
	// If the DNS cache has resolved this hostname already, also pass
	// hostaddr=<ip>.  libpq documents this combo specifically to skip name
	// resolution while keeping the hostname for TLS verification and error
	// messages.  Empty IP -> cache miss / IP literal / disabled, leave as-is.
	{
		const std::string ip = connect_start_DNS_lookup();
		if (!ip.empty() && ip != std::string(parent->address)) {
		append_conninfo_param(conninfo, "hostaddr", const_cast<char*>(ip.c_str()));
		}
	}
	// port=0 means hostname is a Unix-domain socket path; libpq rejects
	// "port=0" with "invalid port number: \"0\"".
	if (parent->port != 0) {
		conninfo << "port=" << parent->port << " ";
	}
	conninfo << "application_name=proxysql "; // application name
	//conninfo << "require_auth=" << AUTHENTICATION_METHOD_STR[pgsql_thread___authentication_method]; // authentication method
#if POLARDB_PROXY
	// Resolve the startup profile once and record it on the connection. The
	// actual startup parameters are appended further down (after the options
	// block is closed); the recorded profile is also read later by
	// polardb_init_connection_tracking() once the connect succeeds.
	const unsigned int polardb_hid = parent && parent->myhgc ? parent->myhgc->hid : 0;
	const bool is_polardb_hg =
		parent && parent->myhgc && PgHGM->is_polardb_hostgroup(polardb_hid);
	const bool client_backed = myds && myds->sess && myds->sess->client_myds;
	polardb_startup_profile = build_polardb_startup_profile(polardb_hid);
	polardb_startup_profile_generation =
		polardb_startup_profile.generation(
			pgsql_thread___polardb_proxy_identity_mode);
	POLARDB_TRACE("PolarDB CONNINFO: hg=%u is_polardb=%d protocol=%s request_bits=0x%x\n",
		polardb_hid, is_polardb_hg ? 1 : 0,
		polardb_proxy_protocol_name(polardb_startup_profile.protocol),
		polardb_startup_profile.request_bits);
#endif // POLARDB_PROXY
	if (parent->use_ssl) {
		conninfo << "sslmode='require' "; // SSL required
		std::unique_ptr<PgSQLServers_SslParams> ssl_params {
			PgHGM->get_Server_SSL_Params(parent->address, parent->port, userinfo->username)
		};
		if (ssl_params != nullptr) {
			// Use per-server SSL params
			if (ssl_params->ssl_key.length() > 0)
				append_conninfo_param(conninfo, "sslkey", (char*)ssl_params->ssl_key.c_str());
			if (ssl_params->ssl_cert.length() > 0)
				append_conninfo_param(conninfo, "sslcert", (char*)ssl_params->ssl_cert.c_str());
			if (ssl_params->ssl_ca.length() > 0)
				append_conninfo_param(conninfo, "sslrootcert", (char*)ssl_params->ssl_ca.c_str());
			if (ssl_params->ssl_crl.length() > 0)
				append_conninfo_param(conninfo, "sslcrl", (char*)ssl_params->ssl_crl.c_str());
			if (ssl_params->ssl_crlpath.length() > 0)
				append_conninfo_param(conninfo, "sslcrldir", (char*)ssl_params->ssl_crlpath.c_str());
			// ssl_protocol_version_range was pre-parsed at PgSQLServers_SslParams
			// construction time (see parse_tls_version()). Empty min/max means
			// either unset or malformed — in both cases libpq defaults apply.
			if (ssl_params->ssl_min_protocol_version.length() > 0)
				append_conninfo_param(conninfo, "ssl_min_protocol_version", (char*)ssl_params->ssl_min_protocol_version.c_str());
			if (ssl_params->ssl_max_protocol_version.length() > 0)
				append_conninfo_param(conninfo, "ssl_max_protocol_version", (char*)ssl_params->ssl_max_protocol_version.c_str());
		} else {
			// Fall back to global SSL settings
			append_conninfo_param(conninfo, "sslkey", pgsql_thread___ssl_p2s_key);
			append_conninfo_param(conninfo, "sslcert", pgsql_thread___ssl_p2s_cert);
			append_conninfo_param(conninfo, "sslrootcert", pgsql_thread___ssl_p2s_ca);
			append_conninfo_param(conninfo, "sslcrl", pgsql_thread___ssl_p2s_crl);
			append_conninfo_param(conninfo, "sslcrldir", pgsql_thread___ssl_p2s_crlpath);
		}
	} else {
		conninfo << "sslmode='disable' "; // not supporting SSL
	}

	if (myds && myds->sess && myds->sess->client_myds) {
		// Client Encoding should be always set
		const char* client_charset = pgsql_variables.client_get_value(myds->sess, PGSQL_CLIENT_ENCODING);
		assert(client_charset);
		uint32_t client_charset_hash = pgsql_variables.client_get_hash(myds->sess, PGSQL_CLIENT_ENCODING);
		assert(client_charset_hash);
		const char* escaped_str = escape_string_backslash_spaces(client_charset);
		conninfo << "client_encoding='" << escaped_str << "' ";
		if (escaped_str != client_charset)
			free((char*)escaped_str);

		// charset validation is already done 
		pgsql_variables.server_set_hash_and_value(myds->sess, PGSQL_CLIENT_ENCODING, client_charset, client_charset_hash);

		// optimized way to set client parameters on backend connection when creating a new connection
		// Join the "-c key=value" tokens with a leading separator so the options value has
		// no trailing space before the closing quote. PgBouncer rejects a startup packet
		// whose options value ends in whitespace (#5801).
		conninfo << "options='";
		const char* separator = "";
		// excluding client_encoding, which is already set above
		for (int idx = 1; idx < PGSQL_NAME_LAST_LOW_WM; idx++) {
			const char* value = pgsql_variables.client_get_value(myds->sess, idx);
			const char* escaped_str = escape_string_backslash_spaces(value);
			conninfo << separator << "-c " << pgsql_tracked_variables[idx].set_variable_name << "=" << escaped_str;
			separator = " ";
			if (escaped_str != value)
				free((char*)escaped_str);

			const uint32_t hash = pgsql_variables.client_get_hash(myds->sess, idx);
			pgsql_variables.server_set_hash_and_value(myds->sess, idx, value, hash);
		}

		myds->sess->mybe->server_myds->myconn->copy_pgsql_variables_to_startup_parameters(true);

		// if there are untracked parameters, the session should lock on the host group
		if (myds->sess->untracked_option_parameters.empty() == false) {
			conninfo << separator << myds->sess->untracked_option_parameters;
		}
		conninfo << "'";

#if POLARDB_PROXY
		// Append the PolarDB startup parameters now that the options block above
		// is closed: they must be top-level conninfo keys, not options entries.
		// The profile decides whether v15, legacy, or no parameters are emitted.
		if (is_polardb_hg) {
			POLARDB_TRACE("PolarDB CONNINFO: client-backed sess=%p is_polardb_enabled=%d session_consistency_mode=%d\n",
				myds->sess,
				myds->sess ? (myds->sess->polardb_config.is_polardb_enabled ? 1 : 0) : -1,
				myds->sess ? myds->sess->polardb_config.session_consistency_mode : -1);
			if (!append_polardb_startup_params(conninfo, polardb_startup_profile, polardb_hid)) {
				return;
			}
			// Mark the session PolarDB-enabled whenever its backend is in a PolarDB
			// hostgroup. This flag gates the response-side LSN result processing;
			// it is only ever turned on, never cleared, for the life of the session.
			myds->sess->polardb_config.is_polardb_enabled = true;
		}
#endif // POLARDB_PROXY
	}

#if POLARDB_PROXY
	else if (!client_backed && polardb_forced_startup_parameters) {
		const char* client_charset = startup_parameters[PGSQL_CLIENT_ENCODING]
			? startup_parameters[PGSQL_CLIENT_ENCODING]
			: pgsql_tracked_variables[PGSQL_CLIENT_ENCODING].default_value;
		const char* escaped_charset =
			escape_string_backslash_spaces(client_charset);
		conninfo << "client_encoding='" << escaped_charset << "' ";
		if (escaped_charset != client_charset) {
			free((char*)escaped_charset);
		}

		conninfo << "options='";
		const char* separator = "";
		for (int idx = 1; idx < PGSQL_NAME_LAST_LOW_WM; idx++) {
			const char* value = startup_parameters[idx]
				? startup_parameters[idx]
				: pgsql_tracked_variables[idx].default_value;
			const char* escaped_str = escape_string_backslash_spaces(value);
			conninfo << separator << "-c "
				<< pgsql_tracked_variables[idx].set_variable_name
				<< "=" << escaped_str;
			separator = " ";
			if (escaped_str != value) {
				free((char*)escaped_str);
			}
		}
		conninfo << "'";
	}

	// Non-client (monitor / internal) connections use the same profile builder.
	// If RFQ LSN is requested here, configured fallback identity is normally
	// required because there is no real client address.
	if (!client_backed && is_polardb_hg) {
		if (!append_polardb_startup_params(conninfo, polardb_startup_profile, polardb_hid)) {
			return;
		}
	}
#endif // POLARDB_PROXY

	/*conninfo << "postgres://";
	 conninfo << userinfo->username << ":" << userinfo->password; // username and password
	 conninfo << "@";
	 conninfo << parent->address << ":" << parent->port; // backend address and port
	 conninfo << "/";
	 conninfo << userinfo->schemaname; // currently schemaname consists of datasename (have to improve this in future). In PostgreSQL database and schema are NOT the same.
	 conninfo << "?";
	 //conninfo << "require_auth=" << AUTHENTICATION_METHOD_STR[pgsql_thread___authentication_method]; // authentication method
	 conninfo << "application_name=proxysql";
	*/

	const std::string& conninfo_str = conninfo.str();
	pgsql_conn = PQconnectStart(conninfo_str.c_str());

	// introduced a new, formatted error verbosity type.
	PQsetErrorVerbosity(pgsql_conn, PSERRORS_FORMATTED_DEFAULT);
	//PQsetErrorContextVisibility(pgsql_conn, PQSHOW_CONTEXT_ERRORS);

	if (pgsql_conn == NULL || PQstatus(pgsql_conn) == CONNECTION_BAD) {
		if (pgsql_conn) {
			set_error_from_PQerrorMessage();
		} else {
			set_error(PGSQL_GET_ERROR_CODE_STR(ERRCODE_OUT_OF_MEMORY), "Out of memory", false);
		}
		proxy_error("Connect failed. %s\n", get_error_code_with_message().c_str());
		return;
	}
	if (PQsetnonblocking(pgsql_conn, 1) != 0) {
		set_error_from_PQerrorMessage();
		proxy_error("Failed to set non-blocking mode: %s\n", get_error_code_with_message().c_str());
		return;
	}
	fd = PQsocket(pgsql_conn);
	async_exit_status = PG_EVENT_WRITE;
}

#if POLARDB_PROXY
PolarDB_StartupProfile PgSQL_Connection::build_polardb_startup_profile(unsigned int hid) const {
	// A non-PolarDB hostgroup never gets PolarDB startup parameters.
	if (!parent || !parent->myhgc || !PgHGM->is_polardb_hostgroup(hid)) {
		return PolarDB_StartupProfile::from_protocol(PolarDB_ProxyProtocol::OFF);
	}

	// Per-hostgroup proxy_protocol wins when set (>= 0); otherwise fall back to the
	// global pgsql-polardb_proxy_protocol default.
	PgSQL_HostGroups_Manager::PolarDB_HG_Policy policy = PgHGM->get_polardb_hg_policy(hid);
	int protocol = policy.proxy_protocol >= 0 ? policy.proxy_protocol : pgsql_thread___polardb_proxy_protocol;
	return PolarDB_StartupProfile::from_protocol(
		polardb_proxy_protocol_from_int(protocol));
}

/// @brief Pick the client identity to advertise in PolarDB startup parameters.
///
/// PolarDB uses this host/port to identify the request source behind the proxy.
/// In client mode this must be the real client endpoint. In proxy mode it is the
/// ProxySQL listener endpoint, with the configured fallback for internal paths.
/// Returns an identity with source NONE when nothing usable is found; the caller
/// treats that as a hard failure for any profile that requests an RFQ LSN.
PolarDB_StartupIdentity PgSQL_Connection::resolve_polardb_startup_identity(
	const PolarDB_StartupProfile& profile) const {
	(void)profile;
#if POLARDB_PROXY && POLARDB_DEBUG
	char debug_identity_fault[64] = {0};
	polardb_debug_startup_identity_fault(
		debug_identity_fault, sizeof(debug_identity_fault));
	if (strcmp(debug_identity_fault, "none") == 0) {
		POLARDB_TRACE("PolarDB CONNINFO: debug forced missing startup identity\n");
		return PolarDB_StartupIdentity{};
	}
	const bool debug_use_listener_proxy =
		strcmp(debug_identity_fault, "listener_proxy") == 0;
	const bool debug_use_configured_fallback =
		strcmp(debug_identity_fault, "configured_fallback") == 0;
#else
	const bool debug_use_listener_proxy = false;
	const bool debug_use_configured_fallback = false;
#endif
	const int identity_mode = pgsql_thread___polardb_proxy_identity_mode;
	const bool use_client_identity =
		polardb_proxy_identity_mode_uses_client_identity(identity_mode);
	const bool use_proxy_identity =
		!use_client_identity || debug_use_listener_proxy ||
		debug_use_configured_fallback;
	if (polardb_forced_startup_identity.source !=
			PolarDB_StartupIdentitySource::NONE) {
		const bool forced_client_identity =
			polardb_forced_startup_identity.source ==
			PolarDB_StartupIdentitySource::CLIENT;
		if (forced_client_identity != use_client_identity) {
			POLARDB_TRACE("PolarDB CONNINFO: forced startup identity rejected by mode=%s source=%d host=%s port=%d\n",
				polardb_proxy_identity_mode_name(identity_mode),
				static_cast<int>(polardb_forced_startup_identity.source),
				polardb_forced_startup_identity.host.c_str(),
				polardb_forced_startup_identity.port);
			return PolarDB_StartupIdentity{};
		}
		const bool reject_wildcard =
			polardb_forced_startup_identity.source !=
			PolarDB_StartupIdentitySource::CLIENT;
		if (polardb_forced_startup_identity.valid(reject_wildcard)) {
			return polardb_forced_startup_identity;
		}
		POLARDB_TRACE("PolarDB CONNINFO: forced startup identity is invalid "
			"source=%d host=%s port=%d\n",
			static_cast<int>(polardb_forced_startup_identity.source),
			polardb_forced_startup_identity.host.c_str(),
			polardb_forced_startup_identity.port);
		return PolarDB_StartupIdentity{};
	}

	const PgSQL_Data_Stream* client_myds =
		(myds && myds->sess) ? myds->sess->client_myds : nullptr;
	if (client_myds) {
		if (!use_proxy_identity) {
			// First choice: the client endpoint already resolved on the session.
			PolarDB_StartupIdentity identity{
				client_myds->addr.addr,
				client_myds->addr.port,
				PolarDB_StartupIdentitySource::CLIENT
			};
			if (!debug_use_listener_proxy &&
					!debug_use_configured_fallback &&
					identity.valid(false)) {
				return identity;
			}
			// Second: derive it straight from the raw client socket address.
			PolarDB_StartupIdentity raw_identity;
			if (!debug_use_listener_proxy &&
					!debug_use_configured_fallback &&
					polardb_startup_identity_from_sockaddr(
						client_myds->client_addr,
						&raw_identity,
						PolarDB_StartupIdentitySource::CLIENT)) {
				return raw_identity;
			}
			return PolarDB_StartupIdentity{};
		}
		// ProxySQL's own listener address. valid(true) additionally
		// rejects a wildcard (any-address) listener, which would not identify a
		// real endpoint; ordinary client checks above use valid(false) and
		// tolerate it because they represent the actual client socket.
		PolarDB_StartupIdentity identity{
			client_myds->proxy_addr.addr,
			client_myds->proxy_addr.port,
			PolarDB_StartupIdentitySource::LISTENER_PROXY
		};
		if (!debug_use_configured_fallback && identity.valid(true)) {
			return identity;
		}
	}

	// Last resort: the operator-configured fallback identity. This is the only
	// path available to non-client (monitor / internal) connections.
	PolarDB_StartupIdentity fallback_identity{
		pgsql_thread___polardb_proxy_identity_host,
		pgsql_thread___polardb_proxy_identity_port,
		PolarDB_StartupIdentitySource::CONFIGURED_FALLBACK
	};
	if (fallback_identity.valid(true)) {
		return fallback_identity;
	}

	// Nothing usable: default-constructed identity has source NONE.
	return PolarDB_StartupIdentity{};
}

// These parameters must be top-level conninfo keys (NOT inside the `options`
// block) so the PolarDB backend's startup-packet handler sees them. See the
// declaration in PgSQL_Connection.h for the full contract.
bool PgSQL_Connection::append_polardb_startup_params(std::ostringstream& conninfo,
	const PolarDB_StartupProfile& profile, unsigned int hid) {
	polardb_startup_client = PolarDB_StartupClientContext{};
	polardb_startup_identity_mode = pgsql_thread___polardb_proxy_identity_mode;
	// Profiles that do not ask for RFQ payloads need no startup parameters at all.
	if (!profile.emits_startup_params()) {
		POLARDB_TRACE("PolarDB CONNINFO: no RFQ startup request for HG %u protocol=%s\n",
			hid, polardb_proxy_protocol_name(profile.protocol));
		return true;
	}

	// RFQ startup safety: a profile that requests RFQ payloads but has no client
	// identity to advertise cannot work, so refuse the connection rather than
	// connect without the parameters and silently lose read-your-writes.
	PolarDB_StartupIdentity identity = resolve_polardb_startup_identity(profile);
	if (identity.source == PolarDB_StartupIdentitySource::NONE) {
		std::string msg = "PolarDB proxy_protocol=";
		msg += polardb_proxy_protocol_name(profile.protocol);
		msg += " requests RFQ payloads but no startup identity is available ";
		msg += "(identity_mode=";
		msg += polardb_proxy_identity_mode_name(
			pgsql_thread___polardb_proxy_identity_mode);
		msg += "); ";
		msg += "use a client-backed connection, listener/proxy address, or configure ";
		msg += "pgsql-polardb_proxy_identity_host and pgsql-polardb_proxy_identity_port";
		set_error(PGSQL_ERROR_CODES::ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
			msg.c_str(), true);
		proxy_error("Cannot create PolarDB backend connection for HG %u: %s\n",
			hid, msg.c_str());
		return false;
	}
	polardb_startup_client.identity = identity;

	POLARDB_TRACE("PolarDB CONNINFO: adding startup params to HG %u protocol=%s request_bits=0x%x identity_source=%d identity=%s:%d\n",
		hid, polardb_proxy_protocol_name(profile.protocol), profile.request_bits,
		static_cast<int>(identity.source), identity.host.c_str(), identity.port);

	// Two PolarDB startup dialects carry the same information under different
	// parameter names. _polar_*_send_lsn asks for WAL LSN; _polar_*_send_xact
	// asks for transaction split evidence (XIDs and split markers). XID RFQ is
	// negotiated on every non-OFF PolarDB connection because the startup profile
	// is fixed for the lifetime of a pooled backend; txn_split_enabled decides
	// later whether routing uses it.
	if (profile.protocol == PolarDB_ProxyProtocol::V15) {
		conninfo << " _polar_proxy_client_host=" << identity.host;
		conninfo << " _polar_proxy_client_port=" << identity.port;
		if (profile.has_rfq_lsn()) {
			conninfo << " _polar_proxy_send_lsn=true";
		}
		if (profile.has_rfq_xid()) {
			conninfo << " _polar_proxy_send_xact=true";
		}
		return true;
	}

	if (profile.protocol == PolarDB_ProxyProtocol::LEGACY) {
		conninfo << " _polar_origin_client_ip=" << identity.host;
		conninfo << " _polar_origin_client_port=" << identity.port;
		if (profile.has_rfq_lsn()) {
			conninfo << " _polar_send_lsn=true";
		}
		if (profile.has_rfq_xid()) {
			conninfo << " _polar_send_xact=true";
		}
		return true;
	}

	return true;
}

// See PgSQL_Connection.h for the @brief. PQsetPolarSend* turns on libpq parsing
// for payloads this connection requested at startup, so later hot-path accessors
// can read cached RFQ state with no extra round-trip.
void PgSQL_Connection::polardb_init_connection_tracking() {
	if (!pgsql_conn || PQstatus(pgsql_conn) != CONNECTION_OK) {
		return;
	}
	if (polardb_startup_profile.has_rfq_lsn()) {
		PQsetPolarSendLSN(pgsql_conn, 1);
	}
	if (polardb_startup_profile.has_rfq_xid()) {
		PQsetPolarSendXact(pgsql_conn, 1);
	}
}

// See PgSQL_Connection.h for the @brief. This is a pure accessor: it reads the
// LSN libpq already cached from the last ReadyForQuery and issues no SQL, which
// is what makes it safe on the hot result-processing path.
uint64_t PgSQL_Connection::get_polardb_lsn() {
	if (!pgsql_conn || PQstatus(pgsql_conn) != CONNECTION_OK) {
		return 0;
	}

	// Returns 0 when the last ReadyForQuery carried no non-zero LSN.
	if (PQhasLSN(pgsql_conn)) {
		return PQgetLSN(pgsql_conn);
	}

	return 0;
}

// See PgSQL_Connection.h for the @brief. This deliberately reports payload
// presence, not whether the payload is a non-zero wait target.
bool PgSQL_Connection::has_polardb_lsn_payload() {
	if (!pgsql_conn || PQstatus(pgsql_conn) != CONNECTION_OK) {
		return false;
	}
	return PQhasLSN(pgsql_conn) != 0;
}

// See PgSQL_Connection.h for the @brief. This is a pure RFQ accessor: libpq
// already cached the payload from the last ReadyForQuery.
const char* PgSQL_Connection::get_polardb_txn_xids() {
	if (!pgsql_conn || PQstatus(pgsql_conn) != CONNECTION_OK) {
		return nullptr;
	}
	return PQgetXactSplitXids(pgsql_conn);
}

// See PgSQL_Connection.h for the @brief.
bool PgSQL_Connection::is_polardb_txn_splittable() {
	if (!pgsql_conn || PQstatus(pgsql_conn) != CONNECTION_OK) {
		return false;
	}
	return PQisXactSplittable(pgsql_conn) != 0;
}

// See PgSQL_Connection.h for the @brief.
bool PgSQL_Connection::is_polardb_txn_wal_pending() {
	if (!pgsql_conn || PQstatus(pgsql_conn) != CONNECTION_OK) {
		return false;
	}
	return PQisXactWalPending(pgsql_conn) != 0;
}
#endif // POLARDB_PROXY

void PgSQL_Connection::connect_cont(short event) {
	PROXY_TRACE();
	assert(pgsql_conn);
	reset_error();
	async_exit_status = PG_EVENT_NONE;

// For troubleshooting connection issue
#if 0
	const char* message = nullptr;
	switch (PQstatus(pgsql_conn))
	{
	case CONNECTION_STARTED:
		message = "Connecting...";
		break;

	case CONNECTION_MADE:
		message = "Connected to server (waiting to send) ...";
		break;

	case CONNECTION_AWAITING_RESPONSE:
		message = "Waiting for a response from the server...";
		break;

	case CONNECTION_AUTH_OK:
		message = "Received authentication; waiting for backend start - up to finish...";
		break;

	case CONNECTION_SSL_STARTUP:
		message = "Negotiating SSL encryption...";
		break;
	
	case CONNECTION_SETENV:
		message = "Negotiating environment-driven parameter settings...";
		break;

	default:
		message = "Connecting...";
	}

	proxy_info("Connection status: %d %s\n", PQsocket(pgsql_conn), message);
#endif

	PostgresPollingStatusType poll_res = PQconnectPoll(pgsql_conn);
	switch (poll_res) {
	case PGRES_POLLING_WRITING:
		async_exit_status = PG_EVENT_WRITE;
		break;
	case PGRES_POLLING_ACTIVE: // Not used
	case PGRES_POLLING_READING:
		async_exit_status = PG_EVENT_READ;
		break;
	case PGRES_POLLING_OK:
		async_exit_status = PG_EVENT_NONE;
		break;
	//case PGRES_POLLING_FAILED:
	default:
		set_error_from_PQerrorMessage();
		proxy_error("Connect failed. %s\n", get_error_code_with_message().c_str());
	}
	int current_fd = PQsocket(pgsql_conn);
	if (current_fd != fd) {
		proxy_warning("PgSQL Connection FD has been changed by PQconnectPoll(). oldFD:%d newFD:%d\n", fd, current_fd);
		proxy_debug(PROXY_DEBUG_MYSQL_CONNECTION, 5, "PgSQL Connection FD has been changed by PQconnectPoll()"
			"Session=%p, Conn=%p, myds=%p, oldFD=%d, newFD=%d\n",
			myds ? myds->sess : nullptr, this, myds, fd, current_fd);
		fd = current_fd;
	}
}

void PgSQL_Connection::query_start() {
	PROXY_TRACE();
	reset_error();
	processing_multi_statement = false;
#if POLARDB_PROXY
	// Begin consuming wrapper result sets from the dispatch state.
	// async_query() (ASYNC_IDLE) copied the wrapper-statement count into
	// dispatch_state.wrapper_stmts from the session field that
	// finalize_wait_timeout_injection() recorded. The handler then counts those
	// result sets down via stmt_pending, dropping each until only the user
	// query's result remains.
	polardb_query_wrap_state.clear();
	if (dispatch_state.wrapper_stmts > 0) {
		polardb_query_wrap_state.begin(dispatch_state.wrapper_stmts,
			dispatch_state.wrapper_kind, dispatch_state.txn_split_xids_reset);
		POLARDB_TRACE("PolarDB QUERY_START: begin wrapped result, "
			"stmt_total=%u kind=%d reset_xids=%d\n",
			dispatch_state.wrapper_stmts, (int)dispatch_state.wrapper_kind,
			dispatch_state.txn_split_xids_reset ? 1 : 0);
	}
	POLARDB_TRACE("PolarDB QUERY_START: wrapper_stmts=%u kind=%d pending=%u reset_xids=%d "
		"wait_active=%d query='%s'\n",
		dispatch_state.wrapper_stmts, (int)dispatch_state.wrapper_kind,
		polardb_query_wrap_state.stmt_pending,
		dispatch_state.txn_split_xids_reset ? 1 : 0,
		myds && myds->sess && myds->sess->polardb_wait_active() ? 1 : 0,
		query.ptr ? query.ptr : "");
	dispatch_state.reset();  // Consumed — prevent stale reuse
#endif // POLARDB_PROXY
	async_exit_status = PG_EVENT_NONE;
	PQsetNoticeReceiver(pgsql_conn, &PgSQL_Connection::notice_handler_cb, this);

	if (PQsendQuery(pgsql_conn, query.ptr) == 0) {
		set_error_from_PQerrorMessage();
		proxy_error("Failed to send query. %s\n", get_error_code_with_message().c_str());
		return;
	}
#if POLARDB_PROXY && POLARDB_DEBUG
	if (polardb_debug_post_send_offline(this)) {
		parent->status = MYSQL_SERVER_STATUS_OFFLINE_HARD;
		POLARDB_TRACE(
			"PolarDB FAILURE DEBUG: reader marked OFFLINE_HARD after query send\n");
	}
#endif
	flush();
}

void PgSQL_Connection::query_cont(short event) {
	PROXY_TRACE();
	proxy_debug(PROXY_DEBUG_MYSQL_PROTOCOL, 6, "event=%d\n", event);
	async_exit_status = PG_EVENT_NONE;
	if (event & POLLOUT) {
		flush();
	}
}

void PgSQL_Connection::fetch_result_start() {
	PROXY_TRACE();
	reset_error();
	async_exit_status = PG_EVENT_NONE;
}

void PgSQL_Connection::fetch_result_cont(short event) {
	PROXY_TRACE();
	async_exit_status = PG_EVENT_NONE;

	// Avoid fetching a new result if one is already available. 
	// This situation can happen when a multi-statement query has been executed.
	if (pgsql_result)
		return;
	
	if (is_copy_out == false) {
#if POLARDB_PROXY
		if (pgsql_thread___polardb_result_fast_forward && polardb_try_add_row_run(this)) {
			return;
		}
#endif // POLARDB_PROXY
		switch (PShandleRowData(pgsql_conn, new_result, &ps_result)) {
		case 0:
			result_type = 2;
			return;
		case 1:
			// we already have data available in buffer
			if (PQisBusy(pgsql_conn) == 0) {
				result_type = 1;
				pgsql_result = PQgetResult(pgsql_conn);

				if (!pgsql_result &&
					query.extended_query_info &&
					(query.extended_query_info->flags & PGSQL_EXTENDED_QUERY_FLAG_SYNC) != 0) {
					pgsql_result = PQgetResult(pgsql_conn);
				}
				return;
			}
			break;
		}
	}

	if (PQconsumeInput(pgsql_conn) == 0) {
		/* We will only set the error if we didn't capture error in last call. If is_error_present is true,
		 * it indicates that an error was already captured during a previous PQconsumeInput call,
		 * and we do not want to overwrite that information.
		 */
		if (is_error_present() == false) {
			set_error_from_PQerrorMessage();
			proxy_error("Failed to consume input. %s\n", get_error_code_with_message().c_str());
		}
		return;
	}

#if POLARDB_PROXY
	if (pgsql_thread___polardb_result_fast_forward &&
		!is_copy_out && polardb_try_add_row_run(this)) {
		return;
	}
#endif // POLARDB_PROXY
	switch (PShandleRowData(pgsql_conn, new_result, &ps_result)) {
	case 0:
		result_type = 2;
		return;
	case 1:
		if (PQisBusy(pgsql_conn)) {
			async_exit_status = PG_EVENT_READ;
			return;
		}
		break;
	default:
		async_exit_status = PG_EVENT_READ;
		return;
	}
	result_type = 1;
	pgsql_result = PQgetResult(pgsql_conn);

	if (!pgsql_result &&
		query.extended_query_info &&
		(query.extended_query_info->flags & PGSQL_EXTENDED_QUERY_FLAG_SYNC) != 0) {
		pgsql_result = PQgetResult(pgsql_conn);
	}
}

void PgSQL_Connection::flush(bool is_resync) {
	int res = PQflush(pgsql_conn);

	if (res > 0) {
		async_exit_status = PG_EVENT_WRITE;
	}
	else if (res == 0) {
		async_exit_status = PG_EVENT_READ;
	}
	else {
		if (!is_resync) {
			set_error_from_PQerrorMessage();
		} else {
			resync_failed = true;
		}
		proxy_error("Failed to flush data to backend. %s\n", get_error_code_with_message().c_str());
		async_exit_status = PG_EVENT_NONE;
	}
}

int PgSQL_Connection::async_connect(short event) {
	PROXY_TRACE();
	if (pgsql_conn == NULL && async_state_machine != ASYNC_CONNECT_START) {
#if POLARDB_PROXY
		// PolarDB can fail before PQconnectStart() when RFQ startup parameters
		// are required but no valid startup identity exists. The debug fault
		// only makes this production path deterministic in tests; the
		// connection is rejected rather than opened without RFQ LSN support.
		if (async_state_machine == ASYNC_CONNECT_FAILED) {
			return -1;
		}
		if (async_state_machine == ASYNC_CONNECT_TIMEOUT) {
			return -2;
		}
#endif // POLARDB_PROXY
		// LCOV_EXCL_START
		assert(0);
		// LCOV_EXCL_STOP
	}
	if (async_state_machine == ASYNC_IDLE) {
		myds->wait_until = 0;
		return 0;
	}
	if (async_state_machine == ASYNC_CONNECT_SUCCESSFUL) {
		compute_unknown_transaction_status();
		async_state_machine = ASYNC_IDLE;
		myds->wait_until = 0;
		creation_time = monotonic_time();
		return 0;
	}
	handler(event);
	switch (async_state_machine) {
	case ASYNC_CONNECT_SUCCESSFUL:
		compute_unknown_transaction_status();
		async_state_machine = ASYNC_IDLE;
		myds->wait_until = 0;
		return 0;
	case ASYNC_CONNECT_FAILED:
		return -1;
	case ASYNC_CONNECT_TIMEOUT:
		return -2;
	default:
		break;
	}
	return 1;
}

bool PgSQL_Connection::is_connected() const {
	if (pgsql_conn == nullptr || PQstatus(pgsql_conn) != CONNECTION_OK) {
		return false;
	}
	return true;
}

void PgSQL_Connection::compute_unknown_transaction_status() {
	
	if (pgsql_conn) {
		// make sure we have not missed even a single error
		if (is_error_present() == false) {
			unknown_transaction_status = false;
			return;
		}

		// On a broken backend, PQtransactionStatus() returns PQTRANS_UNKNOWN
		// even if a transaction was active — libpq has no cached INTRANS bit
		// equivalent to MySQL's server_status & SERVER_STATUS_IN_TRANS. Force
		// unknown_transaction_status=true so IsActiveTransaction() still
		// reports true and the retry path does not replay inside-tx statements
		// on a fresh connection (which would run them as autocommit).
		if (is_connected() == false) {
			unknown_transaction_status = true;
			return;
		}

		switch (PQtransactionStatus(pgsql_conn)) {
		case PQTRANS_INTRANS:
		case PQTRANS_INERROR:
		case PQTRANS_ACTIVE:
			unknown_transaction_status = true;
			break;
		case PQTRANS_UNKNOWN:
		default:
			//unknown_transaction_status = false;
			break;
		}
	}
}

void PgSQL_Connection::async_free_result() {
	PROXY_TRACE();
	//assert(pgsql_conn);

	if (query.ptr) {
		query.ptr = NULL;
		query.length = 0;
	}
#if POLARDB_PROXY
	if (polardb_txn_split_xids_reset_consumed) {
		if (!is_error_present()) {
			polardb_txn_split_xids_dirty = false;
			POLARDB_TRACE("PolarDB TXN_SPLIT: xids reset committed on conn=%p\n",
				(void*)this);
		} else {
			POLARDB_TRACE(
				"PolarDB TXN_SPLIT: keep xids dirty after reset-wrapped query error "
				"conn=%p msg='%s'\n",
				(void*)this, get_error_message().c_str());
		}
		polardb_txn_split_xids_reset_consumed = false;
	}
	polardb_txn_split_xids_reset_query_buf.clear();
#endif // POLARDB_PROXY
	if (userinfo) {
		// if userinfo is NULL , the connection is being destroyed
		// because it is reset on destructor ( ~PgSQL_Connection() )
		// therefore this section is skipped completely
		// this should prevent bug #1046
		//if (query.stmt) {
		//	if (query.stmt->mysql) {
		//		if (query.stmt->mysql == pgsql) { // extra check
		//			mysql_stmt_free_result(query.stmt);
		//		}
		//	}
		//	// If we reached here from 'ASYNC_STMT_PREPARE_FAILED', the
		//	// prepared statement was never added to 'local_stmts', thus
		//	// it will never be freed when 'local_stmts' are purged. If
		//	// initialized, it must be freed. For more context see #3525.
		//	if (this->async_state_machine == ASYNC_STMT_PREPARE_FAILED) {
		//		if (query.stmt != NULL) {
		//			proxy_mysql_stmt_close(query.stmt);
		//		}
		//	}
		//	query.stmt = NULL;
		//}
	}
	if (pgsql_result) {
		PQclear(pgsql_result);
		pgsql_result = NULL;
	}
	compute_unknown_transaction_status();
	async_state_machine = ASYNC_IDLE;
	if (query_result) {
		if (query_result_reuse) {
			delete (query_result_reuse);
		}
		query_result_reuse = query_result;
		query_result = NULL;
	}
	new_result = false;
}

// Returns:
// 0 when the query is completed
// 1 when the query is not completed
// the calling function should check pgsql error in pgsql struct
int PgSQL_Connection::async_query(short event, const char* stmt, unsigned long length, const char* backend_stmt_name, 
	PgSQL_Extended_Query_Type type, const PgSQL_Extended_Query_Info* extended_query_info) {
	PROXY_TRACE();
	PROXY_TRACE2();
	assert(pgsql_conn);

	server_status = parent->status; // we copy it here to avoid race condition. The caller will see this
	if (IsServerOffline())
		return -1;

	if (myds) {
		if (myds->DSS != STATE_MARIADB_QUERY) {
			myds->DSS = STATE_MARIADB_QUERY;
		}
	}
	switch (async_state_machine) {
	case ASYNC_STMT_EXECUTE_END:
	case ASYNC_QUERY_END:
		processing_multi_statement = false;	// no matter if we are processing a multi statement or not, we reached the end
		return 0;
		break;
	case ASYNC_IDLE:
	{
#if POLARDB_PROXY
		// Snapshot the wrapper-statement count from session state at dispatch time
		// so query_start() consumes dispatch_state and does not re-read the session.
		// Only simple queries can be wrapped; extended queries never are. Clearing
		// the session fields here stops a later query from re-skipping these results.
		dispatch_state.reset();
		if (!extended_query_info && myds && myds->sess &&
			myds->sess->polardb_query.dispatch_wrapper_stmts > 0) {
			dispatch_state.wrapper_stmts = myds->sess->polardb_query.dispatch_wrapper_stmts;
			dispatch_state.wrapper_kind = myds->sess->polardb_query.dispatch_wrapper_kind;
			myds->sess->polardb_query.reset_dispatch_wrapper();
		}
		const char* dispatch_stmt = stmt;
		unsigned long dispatch_length = length;
		if (!extended_query_info && polardb_txn_split_xids_dirty &&
				dispatch_state.wrapper_kind != PolarDB_Query_WrapperKind::TXN_SPLIT_WAIT) {
			// A pooled reader that previously ran transaction split can carry the
			// backend-local split-XID context. Clear it in the same leading-SET
			// wrapper path used by LSN waits, before any unrelated user query runs.
			polardb_txn_split_xids_reset_query_buf.assign(
				"SET polar_xact_split_xids = ''; ");
			if (stmt && length > 0) {
				polardb_txn_split_xids_reset_query_buf.append(stmt, length);
			}
			dispatch_stmt = polardb_txn_split_xids_reset_query_buf.c_str();
			dispatch_length =
				(unsigned long)polardb_txn_split_xids_reset_query_buf.size();
			dispatch_state.wrapper_stmts +=
				POLARDB_TXN_SPLIT_RESET_WRAPPER_SET_COUNT;
			dispatch_state.txn_split_xids_reset = true;
			if (dispatch_state.wrapper_kind == PolarDB_Query_WrapperKind::NONE) {
				dispatch_state.wrapper_kind =
					PolarDB_Query_WrapperKind::TXN_SPLIT_XIDS_RESET;
			}
			POLARDB_TRACE(
				"PolarDB TXN_SPLIT: prepended xids reset conn=%p "
				"wrapper_stmts=%u kind=%d\n",
				(void*)this, dispatch_state.wrapper_stmts,
				(int)dispatch_state.wrapper_kind);
		} else if (!extended_query_info) {
			polardb_txn_split_xids_reset_query_buf.clear();
		} else if (polardb_txn_split_xids_dirty) {
			// Extended protocol cannot be SQL-text wrapped here. Keep the dirty
			// marker and avoid returning this backend as clean if the query path
			// proceeds despite the outstanding reset requirement.
			reusable = false;
			POLARDB_TRACE(
				"PolarDB TXN_SPLIT: dirty xids on extended query conn=%p\n",
				(void*)this);
		}
#endif // POLARDB_PROXY
		if (myds && myds->sess) {
			if (myds->sess->active_transactions == 0) {
				// every time we start a query (no matter if COM_QUERY, STMT_PREPARE or otherwise)
				// also a transaction starts, even if in autocommit mode
				myds->sess->active_transactions = 1;
				myds->sess->transaction_started_at = myds->sess->thread->curtime;
			}
		}
		if (!extended_query_info) {
			async_state_machine = ASYNC_QUERY_START;
		} else {
			if (type == PGSQL_EXTENDED_QUERY_TYPE_PARSE) {
				async_state_machine = ASYNC_STMT_PREPARE_START;
			} else if (type == PGSQL_EXTENDED_QUERY_TYPE_DESCRIBE) {
				async_state_machine = ASYNC_STMT_DESCRIBE_START;
			} else if (type == PGSQL_EXTENDED_QUERY_TYPE_EXECUTE) {
				async_state_machine = ASYNC_STMT_EXECUTE_START;
			} else {
				assert(0); // should never reach here
			}
		}
#if POLARDB_PROXY
		set_query(dispatch_stmt, dispatch_length, backend_stmt_name, extended_query_info);
#else
		set_query(stmt, length, backend_stmt_name, extended_query_info);
#endif // POLARDB_PROXY
	}
	default:
		handler(event);
		break;
	}

	if (async_state_machine == ASYNC_QUERY_END ||
		async_state_machine == ASYNC_STMT_EXECUTE_END ||
		async_state_machine == ASYNC_STMT_DESCRIBE_END ||
		async_state_machine == ASYNC_STMT_PREPARE_END ||
		async_state_machine == ASYNC_RESYNC_END) {
		PROXY_TRACE2();
		compute_unknown_transaction_status();
		if (is_error_present()) {
			return -1;
		} else {
			return 0;
		}
	}

	if (async_state_machine == ASYNC_USE_RESULT_START) {
		// if we reached this point it measn we are processing a multi-statement
		// and we need to exit to give control to PgSQL_Session
		processing_multi_statement = true;
		return 2;
	}
	if (processing_multi_statement == true) {
		// we are in the middle of processing a multi-statement
		return 3;
	}
	return 1;
}

// Returns:
// 0 when the query is completed
// 1 when the query is not completed
// the calling function should check pgsql error in pgsql struct
int PgSQL_Connection::async_reset_session(short event) {
	PROXY_TRACE();
	PROXY_TRACE2();
	assert(pgsql_conn);

	server_status = parent->status; // we copy it here to avoid race condition. The caller will see this
	if (IsServerOffline())
		return -1;

	/*if (myds) {
		if (myds->DSS != STATE_MARIADB_QUERY) {
			myds->DSS = STATE_MARIADB_QUERY;
		}
	}*/

	switch (async_state_machine) {
	case ASYNC_RESET_SESSION_SUCCESSFUL:
		unknown_transaction_status = false;
		async_state_machine = ASYNC_IDLE;
		return 0;
		break;
	case ASYNC_RESET_SESSION_FAILED:
		return -1;
		break;
	case ASYNC_RESET_SESSION_TIMEOUT:
		return -2;
		break;
	case ASYNC_IDLE:
		if (myds && myds->sess) {
			if (myds->sess->active_transactions == 0) {
				myds->sess->active_transactions = 1;
				myds->sess->transaction_started_at = myds->sess->thread->curtime;
			}
		}
		async_state_machine = ASYNC_RESET_SESSION_START;
	default:
		handler(event);
		break;
	}

	switch (async_state_machine) {
	case ASYNC_RESET_SESSION_SUCCESSFUL:
		if (myds && myds->sess) {
			if (myds->sess->active_transactions != 0) {
				myds->sess->active_transactions = 0;
				myds->sess->transaction_started_at = 0;
			}
		}
		unknown_transaction_status = false;
		async_state_machine = ASYNC_IDLE;
		return 0;
		break;
	case ASYNC_RESET_SESSION_FAILED:
		if (myds && myds->sess) {
			if (myds->sess->active_transactions != 0) {
				myds->sess->active_transactions = 0;
				myds->sess->transaction_started_at = 0;
			}
		}
		return -1;
		break;
	case ASYNC_RESET_SESSION_TIMEOUT:
		if (myds && myds->sess) {
			if (myds->sess->active_transactions != 0) {
				myds->sess->active_transactions = 0;
				myds->sess->transaction_started_at = 0;
			}
		}
		return -2;
		break;
	default:
		break;
	}
	return 1;
}

// Returns:
// 0 when the ping is completed successfully
// -1 when the ping is completed not successfully
// 1 when the ping is not completed
// -2 on timeout
// the calling function should check pgsql error in pgsql struct
int PgSQL_Connection::async_ping(short event) {
	PROXY_TRACE();
	assert(pgsql_conn);
	switch (async_state_machine) {
	case ASYNC_PING_SUCCESSFUL:
		unknown_transaction_status = false;
		async_state_machine = ASYNC_IDLE;
		return 0;
		break;
	case ASYNC_PING_FAILED:
		return -1;
		break;
	case ASYNC_PING_TIMEOUT:
		return -2;
		break;
	case ASYNC_IDLE:
		async_state_machine = ASYNC_PING_START;
	default:
		//handler(event);
		async_state_machine = ASYNC_PING_SUCCESSFUL;
		break;
	}

	// check again
	switch (async_state_machine) {
	case ASYNC_PING_SUCCESSFUL:
		unknown_transaction_status = false;
		async_state_machine = ASYNC_IDLE;
		return 0;
		break;
	case ASYNC_PING_FAILED:
		return -1;
		break;
	case ASYNC_PING_TIMEOUT:
		return -2;
		break;
	default:
		return 1;
		break;
	}
	return 1;
}

bool PgSQL_Connection::IsKnownActiveTransaction() {
	if (!pgsql_conn) return false;

	PGTransactionStatusType status = PQtransactionStatus(pgsql_conn);
	if (status == PQTRANS_INTRANS || status == PQTRANS_INERROR) {
		return true;
	}

	// In pipeline mode, libpq status may be stale because ReadyForQuery hasn't been processed yet
	// Use the session's transaction state manager which tracks BEGIN/COMMIT/ROLLBACK via SQL parsing
	if (PQpipelineStatus(pgsql_conn) == PQ_PIPELINE_ON && myds && myds->sess) {
		return myds->sess->is_in_transaction();
	}

	return false;
}

bool PgSQL_Connection::IsActiveTransaction() {
	// First check known state
	if (IsKnownActiveTransaction()) {
		return true;
	}

	// Check unknown transaction status flag
	if (is_error_present() && unknown_transaction_status) {
		return true;
	}

	return false;
}

bool PgSQL_Connection::IsServerOffline() {
	bool ret = false;
	if (parent == NULL)
		return ret;
	server_status = parent->status; // we copy it here to avoid race condition. The caller will see this
	if (
		(server_status == MYSQL_SERVER_STATUS_OFFLINE_HARD) // the server is OFFLINE as specific by the user
		||
		(server_status == MYSQL_SERVER_STATUS_SHUNNED && parent->shunned_automatic == true && parent->shunned_and_kill_all_connections == true) // the server is SHUNNED due to a serious issue
		||
		(server_status == MYSQL_SERVER_STATUS_SHUNNED_REPLICATION_LAG) // slave is lagging! see #774
		) {
		ret = true;
	}
	return ret;
}

void PgSQL_Connection::set_is_client() {
	local_stmts->set_is_client(myds->sess);
}

bool PgSQL_Connection::is_connection_in_reusable_state() const {
	PGTransactionStatusType txn_status = PQtransactionStatus(pgsql_conn);
	bool conn_usable = !(txn_status == PQTRANS_UNKNOWN || txn_status == PQTRANS_ACTIVE);
	assert(!(conn_usable == false && is_error_present() == false));
	return conn_usable;
}

PGresult* PgSQL_Connection::get_result() {
	PGresult* result_tmp = pgsql_result;
	pgsql_result = nullptr;
	return result_tmp;
}

bool PgSQL_Connection::set_single_row_mode() {
	assert(pgsql_conn);
	if (PQsetSingleRowMode(pgsql_conn) == 0) {
		set_error_from_PQerrorMessage();
		proxy_error("Failed to set single row mode. %s\n", get_error_code_with_message().c_str());
		return false;
	}
	return true;
}

void PgSQL_Connection::next_multi_statement_result(PGresult* result) {
	// set unprocessed result to pgsql_result
	pgsql_result = result;
	// copy buffer to PSarrayOut
	query_result->buffer_to_PSarrayOut();
}

void PgSQL_Connection::stmt_prepare_start() {
	PROXY_TRACE();
	reset_error();
	processing_multi_statement = false;
	async_exit_status = PG_EVENT_NONE;

	if (PQpipelineStatus(pgsql_conn) == PQ_PIPELINE_OFF) {
		if (PQenterPipelineMode(pgsql_conn) == 0) {
			set_error_from_PQerrorMessage();
			proxy_error("Failed to enter pipeline mode. %s\n", get_error_code_with_message().c_str());
			return;
		}
	}
	
	PQsetNoticeReceiver(pgsql_conn, &PgSQL_Connection::notice_handler_cb, this);

	const PgSQL_Extended_Query_Info* extended_query_info = query.extended_query_info;
	const Parse_Param_Types& parse_param_types = extended_query_info->parse_param_types;

	if (PQsendPrepare(pgsql_conn, query.backend_stmt_name, query.ptr, parse_param_types.size(), parse_param_types.data()) == 0) {
		set_error_from_PQerrorMessage();
		proxy_error("Failed to send prepare. %s\n", get_error_code_with_message().c_str());
		return;
	}

	// Send a Flush if this is not the last extended query message in the sequence/frame (or is an implicit prepared);  
	// otherwise, send a SYNC.
	if ((extended_query_info->flags & PGSQL_EXTENDED_QUERY_FLAG_IMPLICIT_PREPARE) != 0 ||
		(extended_query_info->flags & PGSQL_EXTENDED_QUERY_FLAG_SYNC) == 0) {
		if (PQsendFlushRequest(pgsql_conn) == 0) {
			set_error_from_PQerrorMessage();
			proxy_error("Failed to send flush request. %s\n", get_error_code_with_message().c_str());
			return;
		}
	} else {
		if (PQsendPipelineSync(pgsql_conn) == 0) {
			set_error_from_PQerrorMessage();
			proxy_error("Failed to send pipeline sync. %s\n", get_error_code_with_message().c_str());
			return;
		}
	}
	flush();
}

void PgSQL_Connection::stmt_prepare_cont(short event) {
	PROXY_TRACE();
	proxy_debug(PROXY_DEBUG_MYSQL_PROTOCOL, 6, "event=%d\n", event);
	async_exit_status = PG_EVENT_NONE;
	if (event & POLLOUT) {
		flush();
	}
}

void PgSQL_Connection::stmt_describe_start() {
	PROXY_TRACE();
	reset_error();
	processing_multi_statement = false;
	async_exit_status = PG_EVENT_NONE;

	if (PQpipelineStatus(pgsql_conn) == PQ_PIPELINE_OFF) {
		if (PQenterPipelineMode(pgsql_conn) == 0) {
			set_error_from_PQerrorMessage();
			proxy_error("Failed to enter pipeline mode. %s\n", get_error_code_with_message().c_str());
			return;
		}
	}

	PQsetNoticeReceiver(pgsql_conn, &PgSQL_Connection::notice_handler_cb, this);

	const PgSQL_Extended_Query_Info* extended_query_info = query.extended_query_info;

	switch (extended_query_info->stmt_type) {
	case 'P': // Portal
		if (PQsendDescribePortal(pgsql_conn, extended_query_info->stmt_client_portal_name) == 0) {
			set_error_from_PQerrorMessage();
			proxy_error("Failed to send describe portal message. %s\n", get_error_code_with_message().c_str());
			return;
		}
		break;
	case 'S': // Prepared Statement
		if (PQsendDescribePrepared(pgsql_conn, query.backend_stmt_name) == 0) {
			set_error_from_PQerrorMessage();
			proxy_error("Failed to send describe prepared statement. %s\n", get_error_code_with_message().c_str());
			return;
		}
		break;
	default:
		set_error(PGSQL_ERROR_CODES::ERRCODE_INVALID_PARAMETER_VALUE, "Invalid statement type for describe", false);
		proxy_error("Failed to send describe message. %s\n", get_error_code_with_message().c_str());
		return;
	}

	// Send a Flush if this is not the last extended query message in the sequence/frame;  
	// otherwise, send a SYNC.
	if ((extended_query_info->flags & PGSQL_EXTENDED_QUERY_FLAG_SYNC) == 0) {
		if (PQsendFlushRequest(pgsql_conn) == 0) {
			set_error_from_PQerrorMessage();
			proxy_error("Failed to send flush request. %s\n", get_error_code_with_message().c_str());
			return;
		}
	} else {
		if (PQsendPipelineSync(pgsql_conn) == 0) {
			set_error_from_PQerrorMessage();
			proxy_error("Failed to send pipeline sync. %s\n", get_error_code_with_message().c_str());
			return;
		}
	}
	flush();
}

void PgSQL_Connection::stmt_describe_cont(short event) {
	PROXY_TRACE();
	proxy_debug(PROXY_DEBUG_MYSQL_PROTOCOL, 6, "event=%d\n", event);
	async_exit_status = PG_EVENT_NONE;
	if (event & POLLOUT) {
		flush();
	}
}

void PgSQL_Connection::resync_start() {
	PROXY_TRACE();
	async_exit_status = PG_EVENT_NONE;

	PQsetNoticeReceiver(pgsql_conn, &PgSQL_Connection::notice_handler_cb, this);

	if (PQsendPipelineSync(pgsql_conn) == 0) {
		proxy_error("Failed to send pipeline sync.\n");
		resync_failed = true;
		return;
	}
	flush(true);
}

void PgSQL_Connection::resync_cont(short event) {
	PROXY_TRACE();
	proxy_debug(PROXY_DEBUG_MYSQL_PROTOCOL, 6, "event=%d\n", event);
	async_exit_status = PG_EVENT_NONE;
	if (event & POLLOUT) {
		flush(true);
	}
}

void PgSQL_Connection::stmt_execute_start() {
	PROXY_TRACE();
	reset_error();
	processing_multi_statement = false;
	async_exit_status = PG_EVENT_NONE;

	if (PQpipelineStatus(pgsql_conn) == PQ_PIPELINE_OFF) {
		if (PQenterPipelineMode(pgsql_conn) == 0) {
			set_error_from_PQerrorMessage();
			proxy_error("Failed to enter pipeline mode. %s\n", get_error_code_with_message().c_str());
			return;
		}
	}

	PQsetNoticeReceiver(pgsql_conn, &PgSQL_Connection::notice_handler_cb, this);

	const PgSQL_Extended_Query_Info* extended_query_info = query.extended_query_info;
	const PgSQL_Bind_Message* bind_msg = extended_query_info->bind_msg;
	assert(bind_msg); // should never be null
	const PgSQL_Bind_Data& bind_data = bind_msg->data(); // will always have valid data

	std::vector<const char*> param_values;
	std::vector<int> param_lengths;
	std::vector<int> param_formats;
	std::vector<int> result_formats;

	if (bind_data.num_param_values > 0) {
		auto param_value_reader = bind_msg->get_param_value_reader();

		param_values.resize(bind_data.num_param_values);
		param_lengths.resize(bind_data.num_param_values);

		for (int i = 0; i < bind_data.num_param_values; ++i) {
			PgSQL_Param_Value param_val;
			if (!param_value_reader.next(&param_val)) {
				proxy_error("Failed to read param value at index %u\n", i);
				set_error(PGSQL_ERROR_CODES::ERRCODE_INVALID_PARAMETER_VALUE,
					"Failed to read param value", false);
				return;
			}

			param_values[i] = (reinterpret_cast<const char*>(param_val.value));
			param_lengths[i] = param_val.len;
		}
	}

	if (bind_data.num_param_formats > 0) {
		auto param_fmt_reader = bind_msg->get_param_format_reader();

		param_formats.resize(bind_data.num_param_formats);

		for (int i = 0; i < bind_data.num_param_formats; ++i) {
			uint16_t format;
			if (!param_fmt_reader.next(&format)) {
				proxy_error("Failed to read param format at index %u\n", i);
				set_error(PGSQL_ERROR_CODES::ERRCODE_INVALID_PARAMETER_VALUE,
					"Failed to read param format", false);
				return;
			}
			param_formats[i] = format; // 0 = text, 1 = binary
		}
	}

	// Normalize param formats for libpq:
	// According to the PostgreSQL Bind message specification:
	// https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-BIND
	//  - num_param_formats = 0 -> all parameters are TEXT
	//  - num_param_formats = 1 -> the single format applies to all parameters
	//  - num_param_formats = num_param_values -> formats are applied per-parameter in order
	// Any other number of parameter formats is a protocol error.
	if (!param_formats.empty()) {
		if (param_formats.size() == 1 && param_values.size() > 1) {
			// PostgreSQL protocol allows 1 format for all params,
			// libpq DOES NOT, we must expand
			int fmt = param_formats[0];
			param_formats.resize(param_values.size(), fmt);
		} else if (param_formats.size() != param_values.size()) {
			proxy_error("Invalid param format count: got %zu, expected %zu\n",
				param_formats.size(), param_values.size());
			set_error(PGSQL_ERROR_CODES::ERRCODE_INVALID_PARAMETER_VALUE,
				"Invalid parameter format count", false);
			return;
		}
	}

	if (bind_data.num_result_formats > 0) {
		auto result_fmt_reader = bind_msg->get_result_format_reader();
		result_formats.resize(bind_data.num_result_formats);
		for (int i = 0; i < bind_data.num_result_formats; ++i) {
			uint16_t format;
			if (!result_fmt_reader.next(&format)) {
				proxy_error("Failed to read result format at index %u\n", i);
				set_error(PGSQL_ERROR_CODES::ERRCODE_INVALID_PARAMETER_VALUE,
					"Failed to read result format", false);
				return;
			}
			result_formats[i] = format;
		}
	}

	// If the client did not send any parameter formats (num_param_formats = 0),
	// PostgreSQL protocol defines this as "all parameters are TEXT".
	// libpq represents this case by passing paramFormats = nullptr.
	const int* param_formats_data = (param_formats.empty() == false ? param_formats.data() : nullptr);

	if (PQsendQueryPrepared(pgsql_conn, query.backend_stmt_name, param_values.size(),
		param_values.data(), param_lengths.data(), param_formats_data,
		(result_formats.size() > 0) ? result_formats[0] : 0) == 0) {
		set_error_from_PQerrorMessage();
		proxy_error("Failed to send execute prepared statement. %s\n", get_error_code_with_message().c_str());
		return;
	}

	// Send a Flush if this is not the last extended query message in the sequence/frame;  
	// otherwise, send a SYNC.
	if ((extended_query_info->flags & PGSQL_EXTENDED_QUERY_FLAG_SYNC) == 0) {
		if (PQsendFlushRequest(pgsql_conn) == 0) {
			set_error_from_PQerrorMessage();
			proxy_error("Failed to send flush request. %s\n", get_error_code_with_message().c_str());
			return;
		}
	} else {
		if (PQsendPipelineSync(pgsql_conn) == 0) {
			set_error_from_PQerrorMessage();
			proxy_error("Failed to send pipeline sync. %s\n", get_error_code_with_message().c_str());
			return;
		}
	}
	flush();
}

void PgSQL_Connection::stmt_execute_cont(short event) {
	PROXY_TRACE();
	proxy_debug(PROXY_DEBUG_MYSQL_PROTOCOL, 6, "event=%d\n", event);
	async_exit_status = PG_EVENT_NONE;
	if (event & POLLOUT) {
		flush();
	}
}

void PgSQL_Connection::reset_session_start() {
	PROXY_TRACE();
	assert(pgsql_conn);
	reset_error();
	async_exit_status = PG_EVENT_NONE;
	PQsetNoticeReceiver(pgsql_conn, &PgSQL_Connection::notice_handler_cb, this);

	reset_session_in_pipeline = is_pipeline_active();
	if (reset_session_in_pipeline) {
		if (PQsendPipelineSync(pgsql_conn) == 0) {
			set_error_from_PQerrorMessage();
			proxy_error("Failed to send pipeline sync. %s\n", get_error_code_with_message().c_str());
			return;
		}
	} else {
		reset_session_in_txn = IsKnownActiveTransaction();
		if (PQsendQuery(pgsql_conn, (reset_session_in_txn == false ? "DISCARD ALL" : "ROLLBACK")) == 0) {
			set_error_from_PQerrorMessage();
			proxy_error("Failed to send query. %s\n", get_error_code_with_message().c_str());
			return;
		}
	}
	flush();
}

void PgSQL_Connection::reset_session_cont(short event) {
	PROXY_TRACE();
	proxy_debug(PROXY_DEBUG_MYSQL_PROTOCOL, 6, "event=%d\n", event);
	async_exit_status = PG_EVENT_NONE;
	if (event & POLLOUT) {
		flush();
		return;
	}

	if (PQconsumeInput(pgsql_conn) == 0) {
		/* We will only set the error if we didn't capture error in last call. If is_error_present is true,
		 * it indicates that an error was already captured during a previous PQconsumeInput call,
		 * and we do not want to overwrite that information.
		 */
		if (is_error_present() == false) {
			set_error_from_PQerrorMessage();
			proxy_error("Failed to consume input. %s\n", get_error_code_with_message().c_str());
		}
		return;
	}

	if (PQisBusy(pgsql_conn)) {
		async_exit_status = PG_EVENT_READ;
		return;
	}

	pgsql_result = PQgetResult(pgsql_conn);
}

bool PgSQL_Connection::requires_RESETTING_CONNECTION(const PgSQL_Connection* client_conn) {
	for (auto i = 0; i < PGSQL_NAME_LAST_LOW_WM; i++) {
		if (client_conn->var_hash[i] == 0) {
			if (var_hash[i]) {
				// this connection has a variable set that the
				// client connection doesn't have.
				// Since connection cannot be unset , this connection
				// needs to be reset 
				return true;
			}
		}
	}
	if (client_conn->dynamic_variables_idx.size() < dynamic_variables_idx.size()) {
		// the server connection has more variables set than the client
		return true;
	}
	std::vector<uint32_t>::const_iterator it_c = client_conn->dynamic_variables_idx.begin(); // client connection iterator
	std::vector<uint32_t>::const_iterator it_s = dynamic_variables_idx.begin();              // server connection iterator
	for (; it_s != dynamic_variables_idx.end(); it_s++) {
		while (it_c != client_conn->dynamic_variables_idx.end() && (*it_c < *it_s)) {
			it_c++;
		}
		if (it_c != client_conn->dynamic_variables_idx.end() && *it_c == *it_s) {
			// the backend variable idx matches the frontend variable idx
		}
		else {
			// we are processing a backend variable but there are
			// no more frontend variables
			return true;
		}
	}
	return false;
}

bool PgSQL_Connection::has_same_connection_options(const PgSQL_Connection* client_conn) {
	if (userinfo->hash != client_conn->userinfo->hash) {
		if (strcmp(userinfo->username, client_conn->userinfo->username)) {
			return false;
		}
		if (strcmp(userinfo->dbname, client_conn->userinfo->dbname)) {
			return false;
		}
	}
	return true;
}

unsigned int PgSQL_Connection::get_memory_usage() const {
	// TODO: need to create new function in libpq
	unsigned int memory_bytes = (16 * 1024) * 2; //PSgetMemoryUsage(pgsql_conn);
	return /*sizeof(PGconn) +*/ memory_bytes;
}

char PgSQL_Connection::get_transaction_status_char() {
	char txn_status;
	switch (get_pg_transaction_status()) {
	case PQTRANS_IDLE:
		txn_status = 'I';
		break;
	case PQTRANS_ACTIVE:
	case PQTRANS_INTRANS:
		txn_status = 'T';
		break;
	case PQTRANS_INERROR:
		txn_status = 'E';
		break;
	case PQTRANS_UNKNOWN:
	default:
		txn_status = 'U';
	}
	return txn_status;
}

#if POLARDB_PROXY
static constexpr uint64_t POLARDB_PARENT_BYTES_FLUSH_THRESHOLD = 64 * 1024;
static constexpr uint64_t POLARDB_PARENT_QUERIES_FLUSH_THRESHOLD = 64;

static PgSQL_Thread* polardb_parent_bytes_counter_thread(PgSQL_Connection* conn) {
	if (conn && conn->myds && conn->myds->sess) {
		return conn->myds->sess->thread;
	}
	return nullptr;
}

#define POLARDB_PARENT_BYTES_COUNT(thread, name, value) do { \
		if (PgHGM) { \
			POLARDB_THREAD_COUNT((thread), name, (value)); \
		} \
	} while (0)

#if POLARDB_DEBUG
static const char* polardb_parent_bytes_flush_reason_name(
		PolarDB_ParentBytesFlushReason reason) {
	switch (reason) {
	case PolarDB_ParentBytesFlushReason::ThresholdRecv:
		return "threshold_recv";
	case PolarDB_ParentBytesFlushReason::ThresholdSent:
		return "threshold_sent";
	case PolarDB_ParentBytesFlushReason::ThresholdQueries:
		return "threshold_queries";
	case PolarDB_ParentBytesFlushReason::Detach:
		return "detach";
	case PolarDB_ParentBytesFlushReason::Destructor:
		return "destructor";
	case PolarDB_ParentBytesFlushReason::Manual:
		return "manual";
	default:
		return "unknown";
	}
}
#endif // POLARDB_DEBUG

void PgSQL_Connection::polardb_flush_parent_bytes(PolarDB_ParentBytesFlushReason reason) {
	const uint64_t recv_pending = polardb_parent_bytes_recv_pending;
	const uint64_t sent_pending = polardb_parent_bytes_sent_pending;
	const uint64_t queries_pending = polardb_parent_queries_sent_pending;
	if (recv_pending == 0 && sent_pending == 0 && queries_pending == 0) {
		return;
	}

	PgSQL_Thread* counter_thread = polardb_parent_bytes_counter_thread(this);
#if POLARDB_DEBUG
	POLARDB_TRACE("PolarDB PARENT_BYTES: flush conn=%p parent=%p reason=%s recv=%lu sent=%lu queries=%lu\n",
		(void*)this, (void*)parent,
		polardb_parent_bytes_flush_reason_name(reason),
		(unsigned long)recv_pending, (unsigned long)sent_pending,
		(unsigned long)queries_pending);
#endif // POLARDB_DEBUG
	switch (reason) {
		case PolarDB_ParentBytesFlushReason::ThresholdRecv:
			POLARDB_PARENT_BYTES_COUNT(counter_thread, parent_bytes_flush_threshold_recv, 1);
			break;
		case PolarDB_ParentBytesFlushReason::ThresholdSent:
			POLARDB_PARENT_BYTES_COUNT(counter_thread, parent_bytes_flush_threshold_sent, 1);
			break;
		case PolarDB_ParentBytesFlushReason::ThresholdQueries:
			break;
		case PolarDB_ParentBytesFlushReason::Detach:
			POLARDB_PARENT_BYTES_COUNT(counter_thread, parent_bytes_flush_detach, 1);
			break;
		case PolarDB_ParentBytesFlushReason::Destructor:
			POLARDB_PARENT_BYTES_COUNT(counter_thread, parent_bytes_flush_destructor, 1);
			break;
		case PolarDB_ParentBytesFlushReason::Manual:
			break;
	}

	if (!parent) {
		POLARDB_PARENT_BYTES_COUNT(counter_thread, parent_bytes_flush_no_parent, 1);
		polardb_parent_bytes_recv_pending = 0;
		polardb_parent_bytes_sent_pending = 0;
		polardb_parent_queries_sent_pending = 0;
		polardb_parent_query_batch_count = 0;
		return;
	}
	if (recv_pending) {
		__sync_fetch_and_add(&parent->bytes_recv, recv_pending);
		POLARDB_PARENT_BYTES_COUNT(counter_thread, parent_bytes_flush_recv_atomic, 1);
		POLARDB_PARENT_BYTES_COUNT(counter_thread, parent_bytes_flush_recv_bytes, recv_pending);
		polardb_parent_bytes_recv_pending = 0;
	}
	if (sent_pending) {
		__sync_fetch_and_add(&parent->bytes_sent, sent_pending);
		POLARDB_PARENT_BYTES_COUNT(counter_thread, parent_bytes_flush_sent_atomic, 1);
		POLARDB_PARENT_BYTES_COUNT(counter_thread, parent_bytes_flush_sent_bytes, sent_pending);
		polardb_parent_bytes_sent_pending = 0;
	}
	if (queries_pending) {
		__sync_fetch_and_add(&parent->queries_sent, queries_pending);
		polardb_parent_queries_sent_pending = 0;
	}
	polardb_parent_query_batch_count = 0;
}

void PgSQL_Connection::polardb_flush_parent_queries() {
	const uint64_t queries_pending = polardb_parent_queries_sent_pending;
	if (queries_pending == 0) {
		return;
	}
	if (parent) {
		__sync_fetch_and_add(&parent->queries_sent, queries_pending);
	}
	polardb_parent_queries_sent_pending = 0;
}

void PgSQL_Connection::update_queries_sent() {
	polardb_parent_queries_sent_pending++;
	polardb_parent_query_batch_count++;
	if (polardb_parent_query_batch_count >=
			POLARDB_PARENT_QUERIES_FLUSH_THRESHOLD) {
		polardb_flush_parent_bytes(PolarDB_ParentBytesFlushReason::ThresholdQueries);
	}
}

#undef POLARDB_PARENT_BYTES_COUNT
#endif // POLARDB_PROXY

void PgSQL_Connection::update_bytes_recv(uint64_t bytes_recv) {
#if POLARDB_PROXY
	polardb_parent_bytes_recv_pending += bytes_recv;
	if (polardb_parent_bytes_recv_pending >=
			POLARDB_PARENT_BYTES_FLUSH_THRESHOLD) {
		polardb_flush_parent_bytes(PolarDB_ParentBytesFlushReason::ThresholdRecv);
	}
#else
	__sync_fetch_and_add(&parent->bytes_recv, bytes_recv);
#endif // POLARDB_PROXY
	myds->sess->thread->status_variables.stvar[st_var_queries_backends_bytes_recv] += bytes_recv;
	myds->bytes_info.bytes_recv += bytes_recv;
	bytes_info.bytes_recv += bytes_recv;
}

void PgSQL_Connection::update_bytes_sent(uint64_t bytes_sent) {
#if POLARDB_PROXY
	polardb_parent_bytes_sent_pending += bytes_sent;
	if (polardb_parent_bytes_sent_pending >=
			POLARDB_PARENT_BYTES_FLUSH_THRESHOLD) {
		polardb_flush_parent_bytes(PolarDB_ParentBytesFlushReason::ThresholdSent);
	}
#else
	__sync_fetch_and_add(&parent->bytes_sent, bytes_sent);
#endif // POLARDB_PROXY
	myds->sess->thread->status_variables.stvar[st_var_queries_backends_bytes_sent] += bytes_sent;
	myds->bytes_info.bytes_sent += bytes_sent;
	bytes_info.bytes_sent += bytes_sent;
}

const char* PgSQL_Connection::get_pg_server_version_str(char* buff, int buff_size) {
	const int postgresql_version = get_pg_server_version();
	snprintf(buff, buff_size, "%d.%d.%d", postgresql_version / 10000, (postgresql_version / 100) % 100, postgresql_version % 100);
	return buff;
}

const char* PgSQL_Connection::get_pg_connection_status_str() {
	switch (get_pg_connection_status()) {
	case CONNECTION_OK:
		return "OK";
	case CONNECTION_BAD:
		return "BAD";
	case CONNECTION_STARTED:
		return "STARTED";
	case CONNECTION_MADE:
		return "MADE";
	case CONNECTION_AWAITING_RESPONSE:
		return "AWAITING_RESPONSE";
	case CONNECTION_AUTH_OK:
		return "AUTH_OK";
	case CONNECTION_SETENV:
		return "SETENV";
	case CONNECTION_SSL_STARTUP:
		return "SSL_STARTUP";
	case CONNECTION_NEEDED:
		return "NEEDED";
	case CONNECTION_CHECK_WRITABLE:
		return "CHECK_WRITABLE";
	case CONNECTION_CONSUME:
		return "CONSUME";
	case CONNECTION_GSS_STARTUP:
		return "GSS_STARTUP";
	case CONNECTION_CHECK_TARGET:
		return "CHECK_TARGET";
	case CONNECTION_CHECK_STANDBY:
		return "CHECK_STANDBY";
	}
	return "UNKNOWN";
}

const char* PgSQL_Connection::get_pg_transaction_status_str() {
	switch (get_pg_transaction_status()) {
	case PQTRANS_IDLE:
		return "IDLE";
	case PQTRANS_ACTIVE:
		return "ACTIVE";
	case PQTRANS_INTRANS:
		return "IN-TRANSACTION";
	case PQTRANS_INERROR:
		return "IN-ERROR-TRANSACTION";
	case PQTRANS_UNKNOWN:
		return "UNKNOWN";
	}
	return "INVALID";
}

const char* PgSQL_Connection::get_pg_backend_state() const {
	if (PQstatus(pgsql_conn) != CONNECTION_OK)
		return "disconnected";

	switch (PQtransactionStatus(pgsql_conn)) {
	case PQTRANS_IDLE:
		return "idle";
	case PQTRANS_ACTIVE:
		return "active";
	case PQTRANS_INTRANS:
		return "idle in transaction";
	case PQTRANS_INERROR:
		return "idle in transaction (aborted)";
	case PQTRANS_UNKNOWN:
	default:
		return "unknown";
	}
}

bool PgSQL_Connection::handle_copy_out(const PGresult* result, uint64_t* processed_bytes) {

	if (new_result == true) {
		const unsigned int bytes_recv = query_result->add_copy_out_response_start(result);
		update_bytes_recv(bytes_recv);
		new_result = false;
		is_copy_out = true;
	}

	char* buffer = NULL;
	int copy_data_len = 0;

	while ((copy_data_len = PQgetCopyData(pgsql_conn, &buffer, 1)) > 0) {
		const unsigned int bytes_recv = query_result->add_copy_out_row(buffer, copy_data_len);
		update_bytes_recv(bytes_recv);
		PQfreemem(buffer);
		buffer = NULL;
		*processed_bytes += bytes_recv;	// issue #527 : this variable will store the amount of bytes processed during this event
		if (
			(*processed_bytes > (unsigned int)pgsql_thread___threshold_resultset_size * 8)
			||
			(pgsql_thread___throttle_ratio_server_to_client && pgsql_thread___throttle_max_bytes_per_second_to_client && (*processed_bytes > (uint64_t)pgsql_thread___throttle_max_bytes_per_second_to_client / 10 * (uint64_t)pgsql_thread___throttle_ratio_server_to_client))
			) 
		{
			return false;
		}
	}

	if (copy_data_len == -1) {
		const unsigned int bytes_recv = query_result->add_copy_out_response_end();
		update_bytes_recv(bytes_recv);
		is_copy_out = false;
	} else if (copy_data_len < 0) {
		if (is_error_present() == false) {
			set_error_from_PQerrorMessage();
			proxy_error("PQgetCopyData failed. %s\n", get_error_code_with_message().c_str());
		}
		is_copy_out = false;
	}

	return true;
}

void PgSQL_Connection::notice_handler_cb(void* arg, const PGresult* result) {
	assert(arg);
	PgSQL_Connection* conn = (PgSQL_Connection*)arg;
	if (!result) return;

	if (conn->query_result != nullptr) {
		// Generic upstream path: record the notice in the active result so its
		// NoticeResponse is forwarded inline and its bytes are accounted.
		const unsigned int bytes_recv = conn->query_result->add_notice(result);
		conn->update_bytes_recv(bytes_recv);
	} else {
		// No active query_result to store into. This can happen when a notice
		// arrives outside a result boundary, for example during RESET SESSION
		// (DISCARD ALL / ROLLBACK).
		proxy_debug(PROXY_DEBUG_MYSQL_COM, 5, "Notice received without active query_result [State: %d, FD: %d]: %s\n",
			(int)conn->async_state_machine,
			conn->get_pg_socket_fd(),
			PQresultErrorMessage(result));
	}

#if POLARDB_PROXY
	// Do not return before this block: a wrapped LSN wait can receive its timeout
	// notice while hidden wrapper SET results are being consumed and query_result
	// is not available for normal notice forwarding. polardb_handle_notice()
	// ignores anything that is not an LSN wait-timeout notice during an active
	// PolarDB wait.
	polardb_handle_notice(conn, result);
#endif
}

void PgSQL_Connection::unhandled_notice_cb(void* arg, const PGresult* result) {
	assert(arg);
	PgSQL_Connection* conn = (PgSQL_Connection*)arg;
	proxy_error("Unhandled notice: '%s' received from backend [PID: %d] (Host: %s, Port: %d, User: %s, FD: %d, State: %d). Please report this issue for further investigation and enhancements.\n",
		PQresultErrorMessage(result), conn->get_pg_backend_pid(), conn->get_pg_host(), atoi(conn->get_pg_port()), conn->get_pg_user(), conn->get_pg_socket_fd(), (int)conn->async_state_machine);
#ifdef DEBUG
	assert(0);
#endif
}

void PgSQL_Connection::ProcessQueryAndSetStatusFlags(const char* query_digest_text, int savepoint_count) {
	if (query_digest_text == NULL) return;
	// unknown what to do with multiplex
	int mul = -1;
	if (myds) {
		if (myds->sess) {
			if (myds->sess->qpo) {
				mul = myds->sess->qpo->multiplex;
				if (mul == 0) {
					set_status(true, STATUS_PGSQL_CONNECTION_NO_MULTIPLEX);
				} else {
					if (mul == 1) {
						set_status(false, STATUS_PGSQL_CONNECTION_NO_MULTIPLEX);
					}
				}
			}
		}
	}

	if (get_status(STATUS_PGSQL_CONNECTION_USER_VARIABLE) == false) { // we search for variables only if not already set
		if (strncasecmp(query_digest_text, "SET ", 4) == 0) {
			// For issue #555 , multiplexing is disabled if --safe-updates is used (see session_vars definition)
			int sqloh = pgsql_thread___set_query_lock_on_hostgroup;
			switch (sqloh) {
			case 0: // old algorithm
				if (mul != 2) {
					if (index(query_digest_text, '.')) { // mul = 2 has a special meaning : do not disable multiplex for variables in THIS QUERY ONLY
						if (!IsKeepMultiplexEnabledVariables(query_digest_text)) {
							set_status(true, STATUS_PGSQL_CONNECTION_USER_VARIABLE);
						}
					}
				}
				break;
			case 1: // new algorithm
				if (myds->sess->locked_on_hostgroup > -1) {
					// locked_on_hostgroup was set, so some variable wasn't parsed
					set_status(true, STATUS_PGSQL_CONNECTION_USER_VARIABLE);
				}
				break;
			default:
				break;
			}
		} else {
			if (mul != 2 && index(query_digest_text, '.')) { // mul = 2 has a special meaning : do not disable multiplex for variables in THIS QUERY ONLY
				if (!IsKeepMultiplexEnabledVariables(query_digest_text)) {
					set_status(true, STATUS_PGSQL_CONNECTION_USER_VARIABLE);
				}
			}
		}
	}
	if (get_status(STATUS_PGSQL_CONNECTION_PREPARED_STATEMENT) == false) { // we search if prepared was already executed
		if (!strncasecmp(query_digest_text, "PREPARE ", strlen("PREPARE "))) {
			set_status(true, STATUS_PGSQL_CONNECTION_PREPARED_STATEMENT);
		}
	}

	// CREATE TEMP TABLE creates a session-scoped temporary table.
	// It exists only for the duration of the session and is automatically dropped when the session ends.
	// Since we are not tracking individual temp tables, the status will be reset only on DISCARD TEMP.
	if (get_status(STATUS_PGSQL_CONNECTION_TEMPORARY_TABLE) == false) { // we search for temporary if not already set
		if (!strncasecmp(query_digest_text, "CREATE TEMPORARY TABLE ", strlen("CREATE TEMPORARY TABLE ")) || 
			!strncasecmp(query_digest_text, "CREATE TEMP TABLE ", strlen("CREATE TEMP TABLE "))) {
			set_status(true, STATUS_PGSQL_CONNECTION_TEMPORARY_TABLE);
		}
	} else { // we search for temporary if not already set
		if (!strncasecmp(query_digest_text, "DISCARD TEMP", strlen("DISCARD TEMP"))) {
			set_status(false, STATUS_PGSQL_CONNECTION_TEMPORARY_TABLE);
		}
	}

	// LOCK TABLE is transaction-scoped:
	// The lock is released automatically when the transaction ends
	// (either COMMIT or ROLLBACK). It cannot persist beyond the transaction.
	if (get_status(STATUS_PGSQL_CONNECTION_LOCK_TABLES) == false) { // we search for lock tables only if not already set
		if (IsKnownActiveTransaction() == true && 
			!strncasecmp(query_digest_text, "LOCK TABLE", strlen("LOCK TABLE"))) {
			set_status(true, STATUS_PGSQL_CONNECTION_LOCK_TABLES);
		}
	} else {
		if (IsKnownActiveTransaction() == false) {
			set_status(false, STATUS_PGSQL_CONNECTION_LOCK_TABLES);
		}
	}

	// pg_advisory_xact_lock is transaction-scoped:
	// The advisory lock is automatically released at the end of the current transaction
	// (either COMMIT or ROLLBACK). It does not persist beyond the transaction.
	if (get_status(STATUS_PGSQL_CONNECTION_ADVISORY_XACT_LOCK) == false) {
		if (IsKnownActiveTransaction() == true && 
			!strncasecmp(query_digest_text, "SELECT pg_advisory_xact_lock", sizeof("SELECT pg_advisory_xact_lock") - 1)) {
			set_status(true, STATUS_PGSQL_CONNECTION_ADVISORY_XACT_LOCK);
		}
	} else {
		if (IsKnownActiveTransaction() == false) {
			set_status(false, STATUS_PGSQL_CONNECTION_ADVISORY_XACT_LOCK);
		}
	}

	// pg_advisory_lock is session-level:
	// In ProxySQL, as we are not tracking individual Advisory Locks, we will reset the status only 
	// when we see pg_advisory_unlock_all, which releases all session-level advisory locks.
	if (get_status(STATUS_PGSQL_CONNECTION_ADVISORY_LOCK) == false) { // we search for pg_advisory_lock* if not already set
		if (!strncasecmp(query_digest_text, "SELECT pg_advisory_lock", sizeof("SELECT pg_advisory_lock")-1)) {
			set_status(true, STATUS_PGSQL_CONNECTION_ADVISORY_LOCK);
		}
	} else { 
		if (!strncasecmp(query_digest_text, "SELECT pg_advisory_unlock_all", sizeof("SELECT pg_advisory_unlock_all") - 1)) {
			set_status(false, STATUS_PGSQL_CONNECTION_ADVISORY_LOCK);
		}
	}

	// CREATE SEQUENCE vs CREATE TEMP SEQUENCE:
	/// - CREATE SEQUENCE: Persistent; survives across sessions until explicitly dropped.
	// - CREATE TEMP SEQUENCE: Session-scoped; automatically dropped when the session ends.
	// Since we are not tracking individual sequences, the status will not be reset on DROP SEQUENCE.
	// Instead, it will be reset on DISCARD SEQUENCES, which removes all session-scoped sequences.
	if (get_status(STATUS_PGSQL_CONNECTION_HAS_SEQUENCES) == false) { // we search for sequences only if not already set
		if (!strncasecmp(query_digest_text, "CREATE ", sizeof("CREATE ") - 1) &&
			(!strncasecmp(query_digest_text + sizeof("CREATE ") - 1, "SEQUENCE", sizeof("SEQUENCE") - 1) ||
				!strncasecmp(query_digest_text + sizeof("CREATE ") - 1, "TEMP SEQUENCE", sizeof("TEMP SEQUENCE") - 1) ||
				!strncasecmp(query_digest_text + sizeof("CREATE ") - 1, "TEMPORARY SEQUENCE", sizeof("TEMPORARY SEQUENCE") - 1))) {
			set_status(true, STATUS_PGSQL_CONNECTION_HAS_SEQUENCES);
		}
	} else { // we search for sequences only if not already set
		if (!strncasecmp(query_digest_text, "DISCARD SEQUENCES", sizeof("DISCARD SEQUENCES")-1)) {
			set_status(false, STATUS_PGSQL_CONNECTION_HAS_SEQUENCES);
		}
	}

	// SAVEPOINT is transaction-scoped:
	// The savepoint is automatically released at the end of the current transaction
	// (either COMMIT or ROLLBACK). It does not persist beyond the transaction.
	// If the savepoint count is -1, it means we are not sure if we are in a transaction or not.
	// If the savepoint count is > 0, it means we are in a transaction and have savepoints.
	// If the savepoint count is 0, it means we are not in a transaction and have no savepoints.
	if (get_status(STATUS_PGSQL_CONNECTION_HAS_SAVEPOINT) == false) {
		if (savepoint_count > 0) {
			set_status(true, STATUS_PGSQL_CONNECTION_HAS_SAVEPOINT);
		} else if (savepoint_count == -1) {
			if (IsKnownActiveTransaction() == true && 
				!strncasecmp(query_digest_text, "SAVEPOINT ", sizeof("SAVEPOINT ")-1)) {
					set_status(true, STATUS_PGSQL_CONNECTION_HAS_SAVEPOINT);
			}
		}
	} else {
		if (savepoint_count == 0) {
			set_status(false, STATUS_PGSQL_CONNECTION_HAS_SAVEPOINT);
		} else if (savepoint_count == -1) {
			if ((IsKnownActiveTransaction() == false) /* ||
				(strncasecmp(query_digest_text, "COMMIT", strlen("COMMIT")) == 0) ||
				(strncasecmp(query_digest_text, "ROLLBACK", strlen("ROLLBACK")) == 0) ||
				(strncasecmp(query_digest_text, "ABORT", strlen("ABORT")) == 0)*/) {
				set_status(false, STATUS_PGSQL_CONNECTION_HAS_SAVEPOINT);
			}
		} 
	}
}

// this function is identical to async_query() , with the only exception that query_result should never contain PGSQL_QUERY_RESULT_TUPLE
int PgSQL_Connection::async_send_simple_command(short event, char* stmt, unsigned long length) {
	PROXY_TRACE();
	PROXY_TRACE2();
	assert(pgsql_conn);

	server_status = parent->status; // we copy it here to avoid race condition. The caller will see this
	if (IsServerOffline())
		return -1;

	switch (async_state_machine) {
	case ASYNC_QUERY_END:
		processing_multi_statement = false;	// no matter if we are processing a multi statement or not, we reached the end
		//return 0; <= bug. Do not return here, because we need to reach the if (async_state_machine==ASYNC_QUERY_END) few lines below
		break;
	case ASYNC_IDLE:
		set_query(stmt, length);
		async_state_machine = ASYNC_QUERY_START;
	default:
		handler(event);
		break;
	}
	if (query_result && (query_result->get_result_packet_type() & PGSQL_QUERY_RESULT_TUPLE)) {
		// this is a severe mistake, we shouldn't have reach here
		// for now we do not assert but report the error
		// PMC-10003: Retrieved a resultset while running a simple command using async_send_simple_command() .
		// async_send_simple_command() is used by ProxySQL to configure the connection, thus it
		// shouldn't retrieve any resultset.
		// A common issue for triggering this error is to have configure pgsql-init_connect to
		// run a statement that returns a resultset.
		proxy_error("Retrieved a resultset while running a simple command '%s'\n", stmt);
		return -2;
	}
	if (async_state_machine == ASYNC_QUERY_END) {
		// We just needed to know if the query was successful, not. 
		// We discard the result.
		if (query_result) {
			assert(!query_result_reuse);
			query_result->clear();
			query_result_reuse = query_result;
			query_result = NULL;
		}
		compute_unknown_transaction_status();
		if (is_error_present()) {
			return -1;
		} else {
			async_state_machine = ASYNC_IDLE;
			return 0;
		}
	}

	if (async_state_machine == ASYNC_USE_RESULT_START) {
		// if we reached this point it measn we are processing a multi-statement
		// and we need to exit to give control to MySQL_Session
		processing_multi_statement = true;
		return 2;
	}
	if (processing_multi_statement == true) {
		// we are in the middle of processing a multi-statement
		return 3;
	}

	return 1;
}

int PgSQL_Connection::async_perform_resync(short event) {
	PROXY_TRACE();
	PROXY_TRACE2();
	assert(pgsql_conn);

	server_status = parent->status; // we copy it here to avoid race condition. The caller will see this
	if (IsServerOffline())
		return -1;

	switch (async_state_machine) {
	case ASYNC_RESYNC_END:
		processing_multi_statement = false;
		break;
	case ASYNC_IDLE:
		if (myds && myds->sess) {
			if (myds->sess->active_transactions == 0) {
				myds->sess->active_transactions = 1;
				myds->sess->transaction_started_at = myds->sess->thread->curtime;
			}
		}
		async_state_machine = ASYNC_RESYNC_START;
	default:
		handler(event);
		break;
	}
	if (async_state_machine == ASYNC_RESYNC_END) {
		if (myds && myds->sess) {
			if (myds->sess->active_transactions != 0) {
				myds->sess->active_transactions = 0;
				myds->sess->transaction_started_at = 0;
			}
		}
		// We just needed to know if the query was successful, not. 
		// We discard the result.
		if (query_result) {
			assert(!query_result_reuse);
			query_result->clear();
			query_result_reuse = query_result;
			query_result = NULL;
		}
		compute_unknown_transaction_status();
		if (resync_failed) {
			return -1;
		} else {
			async_state_machine = ASYNC_IDLE;
			return 0;
		}
	}
	return 1;
}

unsigned int PgSQL_Connection::reorder_dynamic_variables_idx() {
#if POLARDB_PROXY
	polardb_pool_key = PolarDB_PoolKey{};
#endif // POLARDB_PROXY
	dynamic_variables_idx.clear();
	// note that we are inserting the index already ordered
	for (auto i = PGSQL_NAME_LAST_LOW_WM + 1; i < PGSQL_NAME_LAST_HIGH_WM; i++) {
		if (var_hash[i] != 0) {
			dynamic_variables_idx.push_back(i);
		}
	}
	unsigned int r = dynamic_variables_idx.size();
	return r;
}

unsigned int PgSQL_Connection::number_of_matching_session_variables(const PgSQL_Connection* client_conn, unsigned int& not_matching) {
	unsigned int ret = 0;
	for (auto i = 0; i < PGSQL_NAME_LAST_LOW_WM; i++) {
		if (client_conn->var_hash[i]) { // client has a variable set
			if (var_hash[i] == client_conn->var_hash[i]) { // server conection has the variable set to the same value
				ret++;
			}
			else {
				not_matching++;
			}
		}
	}
	// increse not_matching y the sum of client and server variables
	// when a match is found the counter will be reduced by 2
	not_matching += client_conn->dynamic_variables_idx.size();
	not_matching += dynamic_variables_idx.size();
	std::vector<uint32_t>::const_iterator it_c = client_conn->dynamic_variables_idx.begin(); // client connection iterator
	std::vector<uint32_t>::const_iterator it_s = dynamic_variables_idx.begin();              // server connection iterator
	for (; it_c != client_conn->dynamic_variables_idx.end() && it_s != dynamic_variables_idx.end(); it_c++) {
		while (it_s != dynamic_variables_idx.end() && *it_s < *it_c) {
			it_s++;
		}
		if (it_s != dynamic_variables_idx.end()) {
			if (*it_s == *it_c) {
				if (var_hash[*it_s] == client_conn->var_hash[*it_c]) { // server conection has the variable set to the same value
					// when a match is found the counter is reduced by 2
					not_matching -= 2;
					ret++;
				}
			}
		}
	}
	return ret;
}

void PgSQL_Connection::reset() {
	bool old_no_multiplex_hg = get_status(STATUS_PGSQL_CONNECTION_NO_MULTIPLEX_HG);
	bool old_compress = get_status(STATUS_PGSQL_CONNECTION_COMPRESSION);
	status_flags = 0;
	// reconfigure STATUS_PGSQL_CONNECTION_NO_MULTIPLEX_HG
	set_status(old_no_multiplex_hg, STATUS_PGSQL_CONNECTION_NO_MULTIPLEX_HG);
	// reconfigure STATUS_PGSQL_CONNECTION_COMPRESSION
	set_status(old_compress, STATUS_PGSQL_CONNECTION_COMPRESSION);
	reusable = true;
	creation_time = monotonic_time();
	delete local_stmts;
	local_stmts = new PgSQL_STMT_Local(false);

	// reset all variables
	for (int i = 0; i < PGSQL_NAME_LAST_HIGH_WM; i++) {
		var_hash[i] = 0;
		if (variables[i].value) {
			free(variables[i].value);
			variables[i].value = NULL;
		}
	}
	dynamic_variables_idx.clear();

#if POLARDB_PROXY
	// Server connections reach reset() after async_reset_session() succeeds, so
	// backend-local split-XID state is clean again. Wrapper bookkeeping is local
	// to this PgSQL_Connection and must not survive that reset boundary.
	dispatch_state.reset();
	polardb_query_wrap_state.clear();
	polardb_txn_split_xids_dirty = false;
	polardb_txn_split_xids_reset_consumed = false;
	polardb_txn_split_xids_reset_query_buf.clear();
	polardb_pool_key = PolarDB_PoolKey{};
#endif // POLARDB_PROXY

	// We need to copy the startup parameters:
	// For client connections, we copy all startup parameters
	// For server connections, we copy only copy critical parameters
	copy_startup_parameters_to_pgsql_variables(/*copy_only_critical_param=*/!is_client_connection);

	if (options.init_connect) {
		free(options.init_connect);
		options.init_connect = NULL;
		options.init_connect_sent = false;
	}
	auto_increment_delay_token = 0;	
	exit_pipeline_mode = false;
	resync_failed = false;
#ifdef DEBUG
	if (pgsql_conn)
		assert(PQpipelineStatus(pgsql_conn) == PQ_PIPELINE_OFF);
#endif
}

void PgSQL_Connection::set_status(bool set, uint32_t status_flag) {
	if (set) {
		this->status_flags |= status_flag;
	} else {
		this->status_flags &= ~status_flag;
	}
}

bool PgSQL_Connection::get_status(uint32_t status_flag) {
	return this->status_flags & status_flag;
}

bool PgSQL_Connection::MultiplexDisabled(bool check_delay_token) {
	// status_flags stores information about the status of the connection
	// can be used to determine if multiplexing can be enabled or not
	bool ret = false;
	if (status_flags & (STATUS_PGSQL_CONNECTION_USER_VARIABLE | STATUS_PGSQL_CONNECTION_PREPARED_STATEMENT |
		STATUS_PGSQL_CONNECTION_LOCK_TABLES | STATUS_PGSQL_CONNECTION_TEMPORARY_TABLE | STATUS_PGSQL_CONNECTION_ADVISORY_LOCK | 
		STATUS_PGSQL_CONNECTION_NO_MULTIPLEX | STATUS_PGSQL_CONNECTION_HAS_SEQUENCES | STATUS_PGSQL_CONNECTION_ADVISORY_XACT_LOCK | 
		STATUS_PGSQL_CONNECTION_NO_MULTIPLEX_HG | STATUS_PGSQL_CONNECTION_HAS_SAVEPOINT 
		/*| STATUS_PGSQL_CONNECTION_HAS_WARNINGS*/ )) {
		ret = true;
	}
	if (check_delay_token && auto_increment_delay_token) return true;
	return ret;
}

void PgSQL_Connection::set_query(const char* stmt, unsigned long length, const char* _backend_stmt_name, const PgSQL_Extended_Query_Info* extended_query_info) {
	query.length = length;
	query.ptr = stmt;
	if (length > largest_query_length) {
		largest_query_length = length;
	}
	query.backend_stmt_name = _backend_stmt_name;
	query.extended_query_info = extended_query_info;
}

bool PgSQL_Connection::IsKeepMultiplexEnabledVariables(const char* query_digest_text) {

	return true;
	/* TODO: fix this
	if (query_digest_text == NULL) return true;

	char* query_digest_text_filter_select = NULL;
	unsigned long query_digest_text_len = strlen(query_digest_text);
	if (strncasecmp(query_digest_text, "SELECT ", strlen("SELECT ")) == 0) {
		query_digest_text_filter_select = (char*)malloc(query_digest_text_len - 7 + 1);
		memcpy(query_digest_text_filter_select, &query_digest_text[7], query_digest_text_len - 7);
		query_digest_text_filter_select[query_digest_text_len - 7] = '\0';
	}
	else {
		return false;
	}
	//filter @@session., @@local. and @@
	char* match = NULL;
	char* last_pos = NULL;
	const int at_session_offset = strlen("@@session.");
	const int at_local_offset = strlen("@@local."); // Alias of session
	const int double_at_offset = strlen("@@");
	while (query_digest_text_filter_select && (match = strcasestr(query_digest_text_filter_select, "@@session."))) {
		memmove(match, match + at_session_offset, strlen(match) - at_session_offset);
		last_pos = match + strlen(match) - at_session_offset;
		*last_pos = '\0';
	}
	while (query_digest_text_filter_select && (match = strcasestr(query_digest_text_filter_select, "@@local."))) {
		memmove(match, match + at_local_offset, strlen(match) - at_local_offset);
		last_pos = match + strlen(match) - at_local_offset;
		*last_pos = '\0';
	}
	while (query_digest_text_filter_select && (match = strcasestr(query_digest_text_filter_select, "@@"))) {
		memmove(match, match + double_at_offset, strlen(match) - double_at_offset);
		last_pos = match + strlen(match) - double_at_offset;
		*last_pos = '\0';
	}

	std::vector<char*>query_digest_text_filter_select_v;
	char* query_digest_text_filter_select_tok = NULL;
	char* save_query_digest_text_ptr = NULL;
	if (query_digest_text_filter_select) {
		query_digest_text_filter_select_tok = strtok_r(query_digest_text_filter_select, ",", &save_query_digest_text_ptr);
	}
	while (query_digest_text_filter_select_tok) {
		//filter "as"/space/alias,such as select @@version as a, @@version b
		while (1) {
			char c = *query_digest_text_filter_select_tok;
			if (!isspace(c)) {
				break;
			}
			query_digest_text_filter_select_tok++;
		}
		char* match_as;
		match_as = strcasestr(query_digest_text_filter_select_tok, " ");
		if (match_as) {
			query_digest_text_filter_select_tok[match_as - query_digest_text_filter_select_tok] = '\0';
			query_digest_text_filter_select_v.push_back(query_digest_text_filter_select_tok);
		}
		else {
			query_digest_text_filter_select_v.push_back(query_digest_text_filter_select_tok);
		}
		query_digest_text_filter_select_tok = strtok_r(NULL, ",", &save_query_digest_text_ptr);
	}

	std::vector<char*>keep_multiplexing_variables_v;
	char* keep_multiplexing_variables_tmp;
	char* save_keep_multiplexing_variables_ptr = NULL;
	unsigned long keep_multiplexing_variables_len = strlen(pgsql_thread___keep_multiplexing_variables);
	keep_multiplexing_variables_tmp = (char*)malloc(keep_multiplexing_variables_len + 1);
	memcpy(keep_multiplexing_variables_tmp, pgsql_thread___keep_multiplexing_variables, keep_multiplexing_variables_len);
	keep_multiplexing_variables_tmp[keep_multiplexing_variables_len] = '\0';
	char* keep_multiplexing_variables_tok = strtok_r(keep_multiplexing_variables_tmp, " ,", &save_keep_multiplexing_variables_ptr);
	while (keep_multiplexing_variables_tok) {
		keep_multiplexing_variables_v.push_back(keep_multiplexing_variables_tok);
		keep_multiplexing_variables_tok = strtok_r(NULL, " ,", &save_keep_multiplexing_variables_ptr);
	}

	for (std::vector<char*>::iterator it = query_digest_text_filter_select_v.begin(); it != query_digest_text_filter_select_v.end(); it++) {
		bool is_match = false;
		for (std::vector<char*>::iterator it1 = keep_multiplexing_variables_v.begin(); it1 != keep_multiplexing_variables_v.end(); it1++) {
			//printf("%s,%s\n",*it,*it1);
			if (strncasecmp(*it, *it1, strlen(*it1)) == 0) {
				is_match = true;
				break;
			}
		}
		if (is_match) {
			is_match = false;
			continue;
		}
		else {
			free(query_digest_text_filter_select);
			free(keep_multiplexing_variables_tmp);
			return false;
		}
	}
	free(query_digest_text_filter_select);
	free(keep_multiplexing_variables_tmp);
	return true;
	*/
}

bool PgSQL_Connection::is_valid_formatted_pq_error_header(const std::string& s, size_t pos) {
	if (pos >= s.size() || !std::isupper(s[pos])) return false;
	size_t prefix_end = pos;
	while (prefix_end < s.size() && std::isupper(s[prefix_end])) prefix_end++;
	if (prefix_end >= s.size() || s[prefix_end] != ':') return false;
	size_t size_start = prefix_end + 1;
	if (size_start >= s.size()) return false;

	// Check valid size format
	size_t size_end = size_start;
	if (size_end >= s.size() || !std::isdigit(s[size_end])) return false;
	while (size_end < s.size() && std::isdigit(s[size_end])) size_end++;
	return (size_end < s.size() && s[size_end] == ':');
}

std::map<std::string, std::vector<std::string>> PgSQL_Connection::parse_pq_error_message(const std::string& error_str) {
	std::map<std::string, std::vector<std::string>> components;
	size_t pos = 0;

	while (pos < error_str.size()) {
		if (is_valid_formatted_pq_error_header(error_str, pos)) {
			std::string prefix;
			int size = 0;
			std::string value;

			// Extract prefix
			size_t prefix_end = pos;
			while (prefix_end < error_str.size() && std::isupper(error_str[prefix_end]))
				prefix_end++;
			prefix = error_str.substr(pos, prefix_end - pos);
			pos = prefix_end + 1;

			// Parse size
			size_t size_start = pos;
			while (pos < error_str.size() && std::isdigit(error_str[pos])) pos++;
			std::string size_str = error_str.substr(size_start, pos - size_start);
			bool valid_size = true;

			if (size_str.empty()) {
				valid_size = false;
			} else {
				size = 0;
				for (char c : size_str) {
					if (!std::isdigit(c)) {
						valid_size = false;
						break;
					}
					int digit = c - '0';
					if (size > (INT_MAX - digit) / 10) {
						valid_size = false;
						break;
					}
					size = size * 10 + digit;
				}
			}
			if (!valid_size || size < 0) {
				pos = size_start;
				continue;
			}
			pos++;
			// Extract value
			size_t value_start = pos;
			size_t value_end;
			value_end = value_start + size;
			if (value_end > error_str.size()) {
				pos = value_start;
				continue;
			}

			value = trim(error_str.substr(value_start, value_end - value_start));
			components[prefix].push_back(value);
			pos = value_end;
		}
		else {
			size_t le_start = pos;
			while (pos < error_str.size() && !is_valid_formatted_pq_error_header(error_str, pos))
				pos++;
			std::string le_value = error_str.substr(le_start, pos - le_start);
			le_value = trim(le_value);
			if (!le_value.empty()) {
				components["LE"].push_back(le_value);
			}
		}
	}

	return components;
}

void PgSQL_Connection::set_error_from_PQerrorMessage() {
	const char* raw_msg = PQerrorMessage(pgsql_conn);
	if (raw_msg == nullptr) {
		PgSQL_Error_Helper::fill_error_info(error_info, PGSQL_ERROR_CODES::ERRCODE_INTERNAL_ERROR, "Unknown error",
			PGSQL_ERROR_SEVERITY::ERRSEVERITY_FATAL);
		return;
	}

	std::string org_msg(raw_msg);

	proxy_debug(PROXY_DEBUG_MYSQL_PROTOCOL, 6,
		"Session=%p, Conn=%p, myds=%p. Error message: '%s' received from backend (Host: %s, Port: %d, User: %s, FD: %d)\n",
		myds->sess, this, myds, org_msg.c_str(), parent->address, parent->port, userinfo->username, get_pg_socket_fd());

	const auto error_field_map = parse_pq_error_message(org_msg);

	auto lookup = [&error_field_map](const char* key, std::string_view fallback) -> std::string_view {
		auto it = error_field_map.find(key);
		if (it != error_field_map.end() && !it->second.empty())
			return it->second.back();
		return fallback;
	};

	std::string_view severity = lookup("S", PgSQL_Error_Helper::get_severity(PGSQL_ERROR_SEVERITY::ERRSEVERITY_FATAL));
	std::string_view sqlstate = lookup("C", PgSQL_Error_Helper::get_error_code(PGSQL_ERROR_CODES::ERRCODE_RAISE_EXCEPTION));
	std::string_view primary_msg = lookup("M", "");
	std::string_view lib_errmsg = lookup("LE", "");

	// we are currently distinguishing between server errors and library-generated errors. 
	// A library-generated error is only set when a server error is not available.
	const std::string_view& full_msg = !primary_msg.empty() ? primary_msg : lib_errmsg;
	PgSQL_Error_Helper::fill_error_info(error_info, sqlstate.data(), full_msg.data(), severity.data());
}

std::pair<const char*, uint32_t> PgSQL_Connection::get_startup_parameter_and_hash(enum pgsql_variable_name idx) {
	// within valid range?
	assert(idx >= 0 && idx < PGSQL_NAME_LAST_HIGH_WM);

	// Attempt to retrieve value from default startup parameters
	if (startup_parameters_hash[idx] != 0) {
		assert(startup_parameters[idx]);
		return { startup_parameters[idx], startup_parameters_hash[idx] };
	}
	assert(!(idx < PGSQL_NAME_LAST_LOW_WM));
	return { "", 0};
}

void PgSQL_Connection::copy_pgsql_variables_to_startup_parameters(bool copy_only_critical_param) {

	//memcpy(startup_parameters_hash, var_hash, sizeof(uint32_t) * PGSQL_NAME_LAST_LOW_WM);
	for (int i = 0; i < PGSQL_NAME_LAST_LOW_WM; ++i) {
		assert(var_hash[i]);
		assert(variables[i].value);
		startup_parameters_hash[i] = var_hash[i];
		free(startup_parameters[i]);
		startup_parameters[i] = strdup(variables[i].value);
	}

	if (copy_only_critical_param) return;

	for (int i = PGSQL_NAME_LAST_LOW_WM + 1; i < PGSQL_NAME_LAST_HIGH_WM; i++) {
		if (var_hash[i] != 0) {
			startup_parameters_hash[i] = var_hash[i];
			free(startup_parameters[i]);
			startup_parameters[i] = strdup(variables[i].value);
		} else {
			startup_parameters_hash[i] = 0;
			free(startup_parameters[i]);
			startup_parameters[i] = nullptr;
		}
	}
}

void PgSQL_Connection::copy_startup_parameters_to_pgsql_variables(bool copy_only_critical_param) {

	//memcpy(var_hash, startup_parameters_hash, sizeof(uint32_t) * PGSQL_NAME_LAST_LOW_WM);
#if POLARDB_PROXY
	polardb_pool_key = PolarDB_PoolKey{};
#endif // POLARDB_PROXY
	for (int i = 0; i < PGSQL_NAME_LAST_LOW_WM; i++) {
		assert(startup_parameters_hash[i]);
		assert(startup_parameters[i]);
		var_hash[i] = startup_parameters_hash[i];
		free(variables[i].value);
		variables[i].value = strdup(startup_parameters[i]);
	}

	if (copy_only_critical_param) return;

	for (int i = PGSQL_NAME_LAST_LOW_WM + 1; i < PGSQL_NAME_LAST_HIGH_WM; i++) {
		if (startup_parameters_hash[i]) {
			var_hash[i] = startup_parameters_hash[i];
			free(variables[i].value);
			variables[i].value = strdup(startup_parameters[i]);
		} else {
			var_hash[i] = 0;
			free(variables[i].value);
			variables[i].value = nullptr;
		}
	}
}

void PgSQL_Connection::init_startup_parameters_from_server() {
	if (!pgsql_conn) return;

	static const char* param_names[] = {
		"client_encoding",
		"DateStyle",
		"IntervalStyle",
		"standard_conforming_strings",
		"TimeZone",
	};

	for (int i = 0; i < PGSQL_NAME_LAST_LOW_WM; i++) {
		const char* value = PQparameterStatus(pgsql_conn, param_names[i]);
		if (!value) {
			value = pgsql_tracked_variables[i].default_value;
		}
		free(startup_parameters[i]);
		startup_parameters[i] = strdup(value);
		startup_parameters_hash[i] = SpookyHash::Hash32(value, strlen(value), 10);
		free(variables[i].value);
		variables[i].value = strdup(value);
		var_hash[i] = startup_parameters_hash[i];
	}
}

void PgSQL_Connection::init_query_result() {
	if (!query_result_reuse) {
		if (query_result) {
#ifdef DEBUG
			assert(!query_result);
#endif
			delete query_result;
			query_result = nullptr;
		}
		query_result = new PgSQL_Query_Result();
	} else {
		query_result = query_result_reuse;
		query_result_reuse = nullptr;
	}

	if (myds->sess->mirror == false) {
		query_result->init(&myds->sess->client_myds->myprot, myds, this);
	}
	else {
		query_result->init(NULL, myds, this);
	}
	new_result = true;
}

PgSQL_Backend_Kill_Args::PgSQL_Backend_Kill_Args(PGconn* conn, const char* user, const char* pass, const char* db, const char* host,
	unsigned int p, unsigned int hid, bool ssl, TYPE typ, PgSQL_Thread* thd) {

	if (typ == TYPE::CANCEL_QUERY)
		cancel_conn = PQgetCancel(conn);
	else {
		cancel_conn = nullptr;
	}
	username = strdup(user);
	password = strdup(pass);
	hostname = strdup(host);
	dbname = strdup(db);
	port = p;
	hostgroup_id = hid;
	type = typ;
	pgsql_thd = thd;
	backend_pid = PQbackendPID(conn);
	ssl_config.use_ssl = ssl;
	if (ssl) {
		std::unique_ptr<PgSQLServers_SslParams> params {
			PgHGM->get_Server_SSL_Params(hostname, port, username)
		};
		if (params != nullptr) {
			ssl_config.sslkey = params->ssl_key.length() > 0 ? strdup(params->ssl_key.c_str()) : nullptr;
			ssl_config.sslcert = params->ssl_cert.length() > 0 ? strdup(params->ssl_cert.c_str()) : nullptr;
			ssl_config.sslrootcert = params->ssl_ca.length() > 0 ? strdup(params->ssl_ca.c_str()) : nullptr;
			ssl_config.sslcrl = params->ssl_crl.length() > 0 ? strdup(params->ssl_crl.c_str()) : nullptr;
			ssl_config.sslcrldir = params->ssl_crlpath.length() > 0 ? strdup(params->ssl_crlpath.c_str()) : nullptr;
			ssl_config.ssl_min_protocol_version = params->ssl_min_protocol_version.length() > 0 ? strdup(params->ssl_min_protocol_version.c_str()) : nullptr;
			ssl_config.ssl_max_protocol_version = params->ssl_max_protocol_version.length() > 0 ? strdup(params->ssl_max_protocol_version.c_str()) : nullptr;
		} else {
			ssl_config.sslkey = pgsql_thread___ssl_p2s_key ? strdup(pgsql_thread___ssl_p2s_key) : nullptr;
			ssl_config.sslcert = pgsql_thread___ssl_p2s_cert ? strdup(pgsql_thread___ssl_p2s_cert) : nullptr;
			ssl_config.sslrootcert = pgsql_thread___ssl_p2s_ca ? strdup(pgsql_thread___ssl_p2s_ca) : nullptr;
			ssl_config.sslcrl = pgsql_thread___ssl_p2s_crl ? strdup(pgsql_thread___ssl_p2s_crl) : nullptr;
			ssl_config.sslcrldir = pgsql_thread___ssl_p2s_crlpath ? strdup(pgsql_thread___ssl_p2s_crlpath) : nullptr;
			ssl_config.ssl_min_protocol_version = nullptr;
			ssl_config.ssl_max_protocol_version = nullptr;
		}
	} else {
		ssl_config.sslkey = nullptr;
		ssl_config.sslcert = nullptr;
		ssl_config.sslrootcert = nullptr;
		ssl_config.sslcrl = nullptr;
		ssl_config.sslcrldir = nullptr;
		ssl_config.ssl_min_protocol_version = nullptr;
		ssl_config.ssl_max_protocol_version = nullptr;
	}
}

PgSQL_Backend_Kill_Args::~PgSQL_Backend_Kill_Args() {
	free(username);
	free(password);
	free(hostname);
	free(dbname);
	free(ssl_config.sslkey);
	free(ssl_config.sslcert);
	free(ssl_config.sslrootcert);
	free(ssl_config.sslcrl);
	free(ssl_config.sslcrldir);
	free(ssl_config.ssl_min_protocol_version);
	free(ssl_config.ssl_max_protocol_version);
	if (cancel_conn)
		PQfreeCancel(cancel_conn);
}

void* PgSQL_backend_kill_thread(void* arg) {
	assert(arg);
	PgSQL_Backend_Kill_Args* backend_kill_args = static_cast<PgSQL_Backend_Kill_Args*>(arg);

	if (backend_kill_args->type == PgSQL_Backend_Kill_Args::TYPE::CANCEL_QUERY) {
		if (!backend_kill_args->cancel_conn) {
			proxy_error("Failed to cancel query on %s:%d with backend PID %d\n", backend_kill_args->hostname, 
				backend_kill_args->port, backend_kill_args->backend_pid);
			PgHGM->p_update_pgsql_error_counter(p_pgsql_error_type::pgsql, backend_kill_args->hostgroup_id, 
				backend_kill_args->hostname, backend_kill_args->port, 999);
			goto __exit;
		}

		if (backend_kill_args->pgsql_thd) backend_kill_args->pgsql_thd->status_variables.stvar[st_var_killed_queries]++;

		char errbuf[256];
		if (!PQcancel(backend_kill_args->cancel_conn, errbuf, sizeof(errbuf))) {
			proxy_error("Failed to cancel query on %s:%d with backend PID %d: %s\n", backend_kill_args->hostname, 
				backend_kill_args->port, backend_kill_args->backend_pid, errbuf);
			PgHGM->p_update_pgsql_error_counter(p_pgsql_error_type::pgsql, backend_kill_args->hostgroup_id, 
				backend_kill_args->hostname, backend_kill_args->port, 999);
		} else {
			proxy_warning("Canceled query on %s:%d with backend PID %d successfully\n", backend_kill_args->hostname,
				backend_kill_args->port, backend_kill_args->backend_pid);
		}
	} else if (backend_kill_args->type == PgSQL_Backend_Kill_Args::TYPE::TERMINATE_CONNECTION) {

		std::ostringstream conninfo;
		append_conninfo_param(conninfo, "user", backend_kill_args->username); // username
		append_conninfo_param(conninfo, "password", backend_kill_args->password); // password
		append_conninfo_param(conninfo, "dbname", backend_kill_args->dbname); // dbname
		append_conninfo_param(conninfo, "host", backend_kill_args->hostname); // backend address
		// port=0 means hostname is a Unix-domain socket path; libpq rejects
		// "port=0" with "invalid port number: \"0\"".
		if (backend_kill_args->port != 0) {
			conninfo << "port=" << backend_kill_args->port << " ";
		}
		conninfo << "application_name=proxysql "; // application name
		
		if (backend_kill_args->ssl_config.use_ssl) {
			conninfo << "sslmode='require' "; // SSL required
			append_conninfo_param(conninfo, "sslkey", backend_kill_args->ssl_config.sslkey);
			append_conninfo_param(conninfo, "sslcert", backend_kill_args->ssl_config.sslcert);
			append_conninfo_param(conninfo, "sslrootcert", backend_kill_args->ssl_config.sslrootcert);
			append_conninfo_param(conninfo, "sslcrl", backend_kill_args->ssl_config.sslcrl);
			append_conninfo_param(conninfo, "sslcrldir", backend_kill_args->ssl_config.sslcrldir);
			// Per-server TLS protocol pinning was pre-parsed from
			// ssl_protocol_version_range when the Kill_Args struct was built.
			append_conninfo_param(conninfo, "ssl_min_protocol_version", backend_kill_args->ssl_config.ssl_min_protocol_version);
			append_conninfo_param(conninfo, "ssl_max_protocol_version", backend_kill_args->ssl_config.ssl_max_protocol_version);
		} else {
			conninfo << "sslmode='disable' "; // not supporting SSL
		}

		const std::string& conninfo_str = conninfo.str();
		PGconn* kill_conn = PQconnectdb(conninfo_str.c_str());

		if (PQstatus(kill_conn) != CONNECTION_OK) {
			proxy_error("Connection failed: %s\n", PQerrorMessage(kill_conn));
			PQfinish(kill_conn);
			goto __exit;
		}

		if (backend_kill_args->pgsql_thd) backend_kill_args->pgsql_thd->status_variables.stvar[st_var_killed_connections]++;

		char query[128];
		snprintf(query, sizeof(query), "SELECT pg_terminate_backend(%d)", backend_kill_args->backend_pid);

		PGresult* res = PQexec(kill_conn, query);
		if (PQresultStatus(res) != PGRES_TUPLES_OK) {
			proxy_error("Terminate failed: %s\n", PQerrorMessage(kill_conn));
		}
		PQclear(res);


		//proxy_warning("Terminating connection on %s:%d with backend PID %d\n", ka->hostname, ka->port, ka->backend_pid);
	}
__exit:
	delete backend_kill_args;
	return NULL;
}
