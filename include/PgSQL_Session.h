#ifdef CLASS_BASE_SESSION_H

#ifndef PROXYSQL_PGSQL_SESSION_H
#define PROXYSQL_PGSQL_SESSION_H

#include <functional>
#include <vector>
#include <variant>
#include "proxysql.h"
#include "Base_Session.h"
#include "cpp.h"
#include "PgSQL_Error_Helper.h"
#include "PgSQL_Variables.h"
#include "PgSQL_Variables_Validator.h"
#include "PgSQL_PolarDB.h"   // PolarDB LSN types (no-op when POLARDB_PROXY=0)

class PgSQL_Query_Result;
class PgSQL_ExplicitTxnStateMgr;
class PgSQL_Parse_Message;
class PgSQL_Describe_Message;
class PgSQL_Close_Message;
class PgSQL_Bind_Message;
class PgSQL_Execute_Message;
struct PgSQL_Param_Value;

#ifndef PROXYJSON
#define PROXYJSON
#include "../deps/json/json_fwd.hpp"
#endif // PROXYJSON

extern class PgSQL_Variables pgsql_variables;

enum PgSQL_Extended_Query_Type : uint8_t {
	PGSQL_EXTENDED_QUERY_TYPE_NOT_SET			 = 0x00,
	PGSQL_EXTENDED_QUERY_TYPE_PARSE				 = 0x01,
	PGSQL_EXTENDED_QUERY_TYPE_DESCRIBE			 = 0x02,
	PGSQL_EXTENDED_QUERY_TYPE_EXECUTE			 = 0x04,
	PGSQL_EXTENDED_QUERY_TYPE_BIND				 = 0x08,
	PGSQL_EXTENDED_QUERY_TYPE_CLOSE				 = 0x10,
};

/* Enumerated types for output format and date order */
typedef enum {
	DATESTYLE_FORMAT_NONE = 0,
	DATESTYLE_FORMAT_ISO,
	DATESTYLE_FORMAT_SQL,
	DATESTYLE_FORMAT_POSTGRES,
	DATESTYLE_FORMAT_GERMAN
} PgSQL_DateStyleFormat_t;

typedef enum {
	DATESTYLE_ORDER_NONE = 0,
	DATESTYLE_ORDER_MDY,
	DATESTYLE_ORDER_DMY,
	DATESTYLE_ORDER_YMD
} PgSQL_DateStyleOrder_t;

/* Structure to hold the parsed DateStyle */
typedef struct {
	PgSQL_DateStyleFormat_t format;
	PgSQL_DateStyleOrder_t order;
} PgSQL_DateStyle_t;

// Utility class for handling PostgreSQL DateStyle
class PgSQL_DateStyle_Util {
private:
	/**
	  * @brief Splits DateStyle string into tokens
	  *
	  * This function takes a DateStyle string as input and splits it into tokens.
	  * It trims leading and trailing whitespace from each token and returns a vector containing the tokens.
	  * If the input string contains more than one comma, an error is logged, and an empty vector is returned.
	  *
	  * @param input A string_view representing the DateStyle input to be split.
	  * @return A vector of strings containing the split tokens. If the input is invalid, an empty vector is returned.
	  *
	  */
	static std::vector<std::string> split_datestyle(std::string_view input);

public:
	/**
	  * @brief Parses the given DateStyle string and returns the corresponding DateStyle format and order.
	  *
	  * This function splits the input string into tokens and processes
	  * each token to identify the DateStyle format and order. If conflicting styles or orders are found, the
	  * function returns a default DateStyle with none format and order.
	  *
	  * @param input A string_view representing the DateStyle input to be parsed.
	  * @return A PgSQL_DateStyle_t structure containing the parsed DateStyle format and order.
	  *
	  */
	static PgSQL_DateStyle_t parse_datestyle(std::string_view input);

	/**
	  * @brief Converts a PgSQL_DateStyle_t structure to a string representation.
	  *
	  * This function takes PgSQL_DateStyle_t structure and converts it to a string representation.
	  * If the format or order in the provided datestyle is not set (DATESTYLE_FORMAT_NONE or DATESTYLE_ORDER_NONE),
	  * it uses the corresponding values from the default_datestyle.
	  *
	  * @param datestyle The PgSQL_DateStyle_t structure to be converted to a string.
	  * @param default_datestyle The default PgSQL_DateStyle_t structure to use if the provided datestyle is incomplete.
	  * @return A string representation of the PgSQL_DateStyle_t structure.
	  *
	  */
	static std::string datestyle_to_string(PgSQL_DateStyle_t datestyle, const PgSQL_DateStyle_t& default_datestyle);

	/**
	  * @brief Converts a DateStyle string to its string representation using a default DateStyle.
	  *
	  * This function takes a DateStyle string as input, parses it, and converts it to a string representation.
	  * If the input DateStyle string is incomplete, the function uses the provided default DateStyle
	  * to fill in the missing parts.
	  *
	  * @param input A string_view representing the DateStyle input to be converted.
	  * @param default_datestyle A PgSQL_DateStyle_t structure representing the default DateStyle to use if the input is incomplete.
	  * @return A string representation of DateStyle.
	  *
	  */
	static std::string datestyle_to_string(std::string_view input, const PgSQL_DateStyle_t& default_datestyle);
};

class PgSQL_STMT_Global_info;
using Parse_Param_Types = std::vector<uint32_t>; // Vector of parameter types for prepared statements

enum PgSQL_Extended_Query_Flags : uint8_t {
	PGSQL_EXTENDED_QUERY_FLAG_NONE				= 0x00,
	PGSQL_EXTENDED_QUERY_FLAG_DESCRIBE_PORTAL	= 0x01,
	PGSQL_EXTENDED_QUERY_FLAG_SYNC				= 0x02,
	PGSQL_EXTENDED_QUERY_FLAG_IMPLICIT_PREPARE  = 0x04,
};

enum ExtendedQueryPhase : uint8_t {
	EXTQ_PHASE_IDLE						= 0x00,	// No extended query activity
	EXTQ_PHASE_BUILDING					= 0x01,	// Collecting extended query messages (Parse/Bind/etc.)
	EXTQ_PHASE_EXECUTING_SYNC_CLIENT	= 0x02,	// Executing after client-initiated Sync
	EXTQ_PHASE_EXECUTING_SYNC_IMPLICIT	= 0x04,	// Executing after implicit Sync (injected)
	EXTQ_PHASE_PROCESSING_PARSE			= 0x08,	// Processing Parse message after Sync
	EXTQ_PHASE_PROCESSING_DESCRIBE		= 0x10,	// Processing Describe message after Sync
	EXTQ_PHASE_PROCESSING_CLOSE			= 0x20,	// Processing Close message after Sync
	EXTQ_PHASE_PROCESSING_BIND			= 0x40,	// Processing Bind message after Sync
	EXTQ_PHASE_PROCESSING_EXECUTE		= 0x80	// Processing Execute message after Sync
};

#define EXTQ_PHASE_PROCESSING_MASK \
    (EXTQ_PHASE_PROCESSING_PARSE | EXTQ_PHASE_PROCESSING_DESCRIBE | \
     EXTQ_PHASE_PROCESSING_CLOSE | EXTQ_PHASE_PROCESSING_BIND | \
     EXTQ_PHASE_PROCESSING_EXECUTE)

struct PgSQL_Extended_Query_Info {
	const char* stmt_client_name;
	const char* stmt_client_portal_name;
	const PgSQL_Bind_Message* bind_msg;
	const PgSQL_STMT_Global_info* stmt_info;
	uint64_t stmt_global_id;
	uint32_t stmt_backend_id;
	uint8_t stmt_type;
	uint8_t flags;
	Parse_Param_Types parse_param_types;
};

class PgSQL_Query_Info {
public:
	unsigned long long start_time;
	unsigned long long end_time;
	uint64_t affected_rows;
	uint64_t rows_sent;
	uint64_t waiting_since;

	PgSQL_Extended_Query_Info extended_query_info;
	PgSQL_Session* sess;
	unsigned char* QueryPointer;
	SQP_par_t QueryParserArgs;
	int QueryLength;
	enum PGSQL_QUERY_command PgQueryCmd;

	bool have_affected_rows;

	PgSQL_Query_Info();
	~PgSQL_Query_Info();
	
	void query_parser_init();
	enum PGSQL_QUERY_command query_parser_command_type();
	void query_parser_free();
	unsigned long long query_parser_update_counters();
	void begin(unsigned char* _p, int len, bool header = false);
	void end();
	char* get_digest_text();
	void set_end_time(unsigned long long time);

private:
	void reset_extended_query_info();
	void init(unsigned char* _p, int len, bool header = false);
};

/**
 * @brief Assigns query end time.
 * @details In addition to being a setter for end_time member variable, this
 * method ensures that end_time is always greater than or equal to start_time.
 * Refer https://github.com/sysown/proxysql/issues/4950 for more details.
 * @param time query end time
 */
inline void PgSQL_Query_Info::set_end_time(unsigned long long time) {
	end_time = time;

#ifndef CLOCK_MONOTONIC_RAW
	if (start_time <= end_time)
		return;

	// If start_time is greater than end_time, assign current monotonic time
	end_time = monotonic_time();
	if (start_time <= end_time)
		return;

	// If start_time is still greater than end_time, set the difference to 0
	end_time = start_time;
#endif // CLOCK_MONOTONIC_RAW
}

class TrafficObserver;

class PgSQL_Session : public Base_Session<PgSQL_Session, PgSQL_Data_Stream, PgSQL_Backend, PgSQL_Thread> {
private:
	using PktType = std::variant<std::unique_ptr<PgSQL_Parse_Message>,std::unique_ptr<PgSQL_Describe_Message>,
		std::unique_ptr<PgSQL_Close_Message>, std::unique_ptr<PgSQL_Bind_Message>, std::unique_ptr<PgSQL_Execute_Message>>;

	bool extended_query_exec_qp { false };
	uint8_t extended_query_phase { EXTQ_PHASE_IDLE };
	std::queue<PktType> extended_query_frame;
	std::unique_ptr<const PgSQL_Bind_Message> bind_waiting_for_execute;

	//int handler_ret;
	void handler___status_CONNECTING_CLIENT___STATE_SERVER_HANDSHAKE(PtrSize_t*, bool*);

	//	void handler___status_CHANGING_USER_CLIENT___STATE_CLIENT_HANDSHAKE(PtrSize_t *, bool *);
#if 0
	void handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___MYSQL_COM_FIELD_LIST(PtrSize_t*);
	void handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___MYSQL_COM_INIT_DB(PtrSize_t*);
	/**
	 * @brief Handles 'COM_QUERIES' holding 'USE DB' statements.
	 *
	 * @param pkt The packet being processed.
	 * @param query_digest The query digest returned by the 'QueryProcessor'
	 *   holding the 'USE' statement without the initial comment.
	 *
	 * @details NOTE: This function used to be called from 'handler_special_queries'.
	 *   But since it was change for handling 'USE' statements which are preceded by
	 *   comments, it's called after 'QueryProcessor' has processed the query.
	 */
	void handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___MYSQL_COM_QUERY_USE_DB(PtrSize_t* pkt);
	void handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___MYSQL_COM_PING(PtrSize_t*);

	void handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___MYSQL_COM_CHANGE_USER(PtrSize_t*, bool*);
	/**
	 * @brief Handles the command 'COM_RESET_CONNECTION'.
	 * @param pkt Pointer to packet received holding the 'COM_RESET_CONNECTION'.
	 * @details 'COM_RESET_CONNECTION' command is currently supported only for 'sesssion_types':
	 *   - 'PROXYSQL_SESSION_MYSQL'.
	 *   - 'PROXYSQL_SESSION_SQLITE'.
	 *  If the command is received for other sessions, the an error packet with error '1047' is sent to the
	 *  client. If the session is supported, it performs the following operations over the current session:
	 *   1. Store the current relevent session variables to be recovered after the 'RESET'.
	 *   2. Perform a reset and initialization of current session.
	 *   3. Recover the relevant session variables and other initial state associated with the current session
	 *      user.
	 *   4. Respond to client with 'OK' packet.
	 */
	void handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___MYSQL_COM_RESET_CONNECTION(PtrSize_t* pkt);

	void handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___MYSQL_COM_SET_OPTION(PtrSize_t*);

	void handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___MYSQL_COM_STATISTICS(PtrSize_t*);
	void handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___MYSQL_COM_PROCESS_KILL(PtrSize_t*);
#endif

	void handler___client_DSS_QUERY_SENT___server_DSS_NOT_INITIALIZED__get_connection();

	bool is_multi_statement_command(const char* cmd);
	bool handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___handle_SET_command(const char* dig, bool* lock_hostgroup);
	bool handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___handle_RESET_command(const char* dig, bool* lock_hostgroup);
	bool handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___handle_DISCARD_command(const char* dig);
	bool handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___handle_DEALLOCATE_command(const char* dig);
	bool handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___handle_special_commands(const char* dig, bool* lock_hostgroup);
	bool handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___PGSQL_QUERY_qpo(PtrSize_t*, bool* lock_hostgroup, 
		PgSQL_Extended_Query_Type stmt_type = PGSQL_EXTENDED_QUERY_TYPE_NOT_SET);
	bool handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___PGSQL_PARSE(PtrSize_t& pkt);
	bool handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___PGSQL_DESCRIBE(PtrSize_t& pkt);
	bool handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___PGSQL_CLOSE(PtrSize_t& pkt);
	bool handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___PGSQL_BIND(PtrSize_t& pkt);
	bool handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___PGSQL_EXECUTE(PtrSize_t& pkt);
	int handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___PGSQL_SYNC();
	bool handler___rc0_PROCESSING_STMT_PREPARE(enum session_status& st, PgSQL_Data_Stream* myds);
	// FIXME: unused. Remove in next iteration
	//void handler___rc0_PROCESSING_STMT_DESCRIBE_PREPARE(PgSQL_Data_Stream* myds);
	int handler___status_PROCESSING_EXTENDED_QUERY_SYNC();
	int handle_post_sync_parse_message(PgSQL_Parse_Message* parse_msg);
	int handle_post_sync_describe_message(PgSQL_Describe_Message* describe_msg);
	int handle_post_sync_close_message(PgSQL_Close_Message* close_msg);
	int handle_post_sync_bind_message(PgSQL_Bind_Message* bind_msg);
	int handle_post_sync_execute_message(PgSQL_Execute_Message* execute_msg);
	void handle_post_sync_error(PGSQL_ERROR_CODES errcode, const char* errmsg, bool fatal);
	void handle_post_sync_locked_on_hostgroup_error(const char* query, int query_len);
	void reset_extended_query_frame();


	//void return_proxysql_internal(PtrSize_t*);
	bool handler_special_queries(PtrSize_t*, bool* lock_hostgroup);
	//bool handler_special_queries_STATUS(PtrSize_t*);
	/**
	 * @brief Handles 'COMMIT|ROLLBACK' commands.
	 * @details Forwarding the packet is required when there are active transactions. Since we are limited to
	 *  forwarding just one 'COMMIT|ROLLBACK', we work under the assumption that we only have one active
	 *  transaction. If more transactions are simultaneously open for the session, more 'COMMIT|ROLLBACK'.
	 *  commands are required to be issued by the client, so they could be forwarded to the corresponding
	 *  backend connections.
	 * @param The received packet to be handled.
	 * @return 'true' if the packet is intercepted and never forwarded to the client, 'false' otherwise.
	 */
	bool handler_CommitRollback(PtrSize_t*);
	/**
	 * @brief Should execute most of the commands executed when a request is finalized.
	 * @details Cleanup of current session state, and required operations to the supplied 'PgSQL_Data_Stream'
	 *   for further queries processing. Takes care of the following actions:
	 *   - Update the status of the backend connection (if supplied), with previous query actions.
	 *   - Log the query for the required statuses.
	 *   - Cleanup the previous Query_Processor output.
	 *   - Free the resources of the backend connection (if supplied).
	 *   - Reset all the required session status flags. E.g:
	 *       + status
	 *       + client_myds::DSS
	 *       + started_sending_data_to_client
	 *       + previous_hostgroup
	 *   NOTE: Should become the place to hook other functions.
	 * @param myds If not null, should point to a PgSQL_Data_Stream (backend connection) which connection status
	 *   should be updated, and previous query resources cleanup.
	 */
	void RequestEnd(PgSQL_Data_Stream*, bool called_on_failure);
	void LogQuery(PgSQL_Data_Stream*);

	void handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___MYSQL_COM_QUERY___create_mirror_session();
	int handler_again___status_PINGING_SERVER();
	int handler_again___status_RESETTING_CONNECTION();
	int handler_again___status_RESYNCHRONIZING_CONNECTION();

	/**
	 * @brief Initiates a new thread to kill current running query.
	 *
	 * The handler_again___new_thread_to_cancel_query() method creates a new thread to initiate 
	 * the cancellation of the current running query.
	 *
	 */
	void handler_again___new_thread_to_cancel_query();

	bool handler_again___verify_init_connect();
#if 0
	bool handler_again___verify_ldap_user_variable();
	bool handler_again___verify_backend_autocommit();
	bool handler_again___verify_backend_session_track_gtids();
	bool handler_again___verify_backend_multi_statement();
#endif // 0
	bool handler_again___verify_backend_user_db();
	bool handler_again___status_SETTING_INIT_CONNECT(int*);
#if 0
	bool handler_again___status_SETTING_LDAP_USER_VARIABLE(int*);
	bool handler_again___status_SETTING_SQL_MODE(int*);
	bool handler_again___status_SETTING_SESSION_TRACK_GTIDS(int*);
	bool handler_again___status_CHANGING_SCHEMA(int*);
#endif // 0
	bool handler_again___status_CONNECTING_SERVER(int*);
	bool handler_again___status_RESETTING_CONNECTION(int*);
	//bool handler_again___status_CHANGING_AUTOCOMMIT(int*);
#if 0
	bool handler_again___status_SETTING_MULTI_STMT(int* _rc);
#endif // 0
	bool handler_again___multiple_statuses(int* rc);
	//void init();
	void reset();
#if 0
	void add_ldap_comment_to_pkt(PtrSize_t*);
	/**
	 * @brief Performs the required housekeeping operations over the session and its connections before
	 *  performing any processing on received client packets.
	 */
	void housekeeping_before_pkts();
#endif // 0
	int get_pkts_from_client(bool&, PtrSize_t&);
	// these functions have code that used to be inline, and split into functions for readibility
	int handler_ProcessingQueryError_CheckBackendConnectionStatus(PgSQL_Data_Stream* myds);
	void SetQueryTimeout();
	bool handler_minus1_ClientLibraryError(PgSQL_Data_Stream* myds);
	// Synthesize ErrorResponse(25P02) + NoticeResponse(backend text, no 57P01) +
	// ReadyForQuery('E') to the client, destroy the backend pool connection, set
	// tx_poisoned=true. Returns true if poison was applied; false means the
	// caller must fall back to the current terminate-the-session flow (e.g.
	// admin var off, result transfer already started, or a preflight failed).
	bool handler_minus1_PoisonTransaction(PgSQL_Data_Stream* myds);
	// While tx_poisoned, classify a 'Q' packet and either clear the poison
	// and synthesize a ROLLBACK response (for plain whole-transaction
	// ROLLBACK / COMMIT / ABORT / END) or reject with ERROR 25P02
	// (anything else, including ROLLBACK TO SAVEPOINT).
	// Returns true if the packet was handled here. Increments the
	// pgsql_tx_poisoned_{recovered,rejected_statements}_total counters.
	bool handler_poisoned_simple_query(PtrSize_t* pkt);
	void handler_minus1_LogErrorDuringQuery(PgSQL_Connection* myconn);
	bool handler_minus1_HandleErrorCodes(PgSQL_Data_Stream* myds, int& handler_ret);
	void handler_minus1_GenerateErrorMessage(PgSQL_Data_Stream* myds, bool& wrong_pass);
	void handler_minus1_HandleBackendConnection(PgSQL_Data_Stream* myds);
	int RunQuery(PgSQL_Data_Stream* myds, PgSQL_Connection* myconn);
	void handler___status_WAITING_CLIENT_DATA();
	void handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___MYSQL_COM_INIT_DB_replace_CLICKHOUSE(PtrSize_t& pkt);
	void handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___MYSQL_COM_QUERY___not_mysql(PtrSize_t& pkt);
	bool handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___MYSQL_COM_QUERY_detect_SQLi();
#if 0
	bool handler___status_WAITING_CLIENT_DATA___STATE_SLEEP_MULTI_PACKET(PtrSize_t& pkt);
	bool handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___MYSQL_COM__various(PtrSize_t* pkt, bool* wrong_pass);
#endif
	void handler___status_WAITING_CLIENT_DATA___default();
	void handler___status_NONE_or_default(PtrSize_t& pkt);

	void handler_WCD_SS_MCQ_qpo_QueryRewrite(PtrSize_t* pkt);
	void handler_WCD_SS_MCQ_qpo_OK_msg(PtrSize_t* pkt);
	void handler_WCD_SS_MCQ_qpo_error_msg(PtrSize_t* pkt);
	void handler_WCD_SS_MCQ_qpo_LargePacket(PtrSize_t* pkt);

	/**
	 * @brief Switches session from normal mode to fast forward mode.
	 *
	 * This method transitions the session to fast forward mode based on session type.
	 * (Currently only supports SESSION_FORWARD_TYPE_TEMPORARY and extended types)
	 *
	 * @param pkt Used solely to push the packet back to client_myds PSarrayIN,
	 *			allowing it to be forwarded to the backend via the fast forward session
	 * @param command Command that causes the session to switch to fast forward mode.
	 * @param session_type SESSION_FORWARD_TYPE indicating the type of session.
	 *
	 * @return bool.
	 */
	bool switch_normal_to_fast_forward_mode(PtrSize_t& pkt, std::string_view command, SESSION_FORWARD_TYPE session_type);

	/**
	 * @brief Switches session from fast forward mode to normal mode.
	 *
	 * This method is used to revert session from fast forward mode back to normal mode.
	 * 
	 */
	void switch_fast_forward_to_normal_mode();

public:
	void handle_transaction_state();

	inline bool is_extended_query_frame_empty() const {
		return extended_query_frame.empty();
	}

	inline uint8_t get_extended_query_phase() const {
		return extended_query_phase;
	}

	inline bool is_extended_query_ready_for_query() const {
		return extended_query_frame.empty() &&
			((extended_query_phase & EXTQ_PHASE_EXECUTING_SYNC_IMPLICIT) == 0);
	}

	bool handler_again___status_SETTING_GENERIC_VARIABLE(int* _rc, const char* var_name, const char* var_value, bool no_quote = false, bool set_transaction = false);
#if 0
	bool handler_again___status_SETTING_SQL_LOG_BIN(int*);
#endif // 0
	std::stack<enum session_status> previous_status;

	PgSQL_Query_Info CurrentQuery;
	PtrSize_t pkt;
	std::string untracked_option_parameters;
	PgSQL_DateStyle_t current_datestyle = {};
	uint32_t cancel_secret_key;

#if POLARDB_PROXY
	// ---- PolarDB LSN session-consistency ----
	// All PolarDB session state lives here, grouped into small value structs so the
	// routing, result-processing, and reset paths share one set of fields and one
	// reset contract instead of scattering loose flags across the session.
	// Every field is session-confined: only the one thread currently driving this
	// PgSQL_Session touches it, so none of these fields use locks or atomics.

	/**
	 * @brief PolarDB per-session configuration.
	 *
	 * Optional per-session consistency-mode override. The resolved mode is
	 * request-local state in PolarDB_Query_RouteCtx, because per-hostgroup or
	 * global policy can change between queries.
	 *
	 * Supported values for the client SET that drives this override:
	 *   default      clear the session override and inherit per-HG / global config
	 *   off          disable PolarDB consistency routing for this session
	 *   lsn/session  use per-session write LSN read-your-writes routing
	 *   primary      force reads to the primary
	 */
	struct PgSQL_PolarDB_Config {
		// Set true once this session attaches to a backend in a PolarDB-enabled
		// hostgroup (on fresh connect, or when a pooled connection is reused). It
		// gates the response-path result processing. Only ever turned on: a session
		// that has talked to a PolarDB backend keeps capturing LSNs for its life.
		bool is_polardb_enabled = false;
		// Per-session consistency-mode override, and the top tier of mode resolution
		// (session override > hostgroup > global). -1 means "no override", so the
		// resolved mode falls through to the per-hostgroup and global settings.
		int session_consistency_mode = -1;
	} polardb_config;

	// Durable per-session consistency truth: the read-your-writes LSN target
	// (write and observed positions), the missing-LSN latches, and the writer
	// hostgroup/epoch scope those values belong to. This lives for the whole
	// session: the write LSN deliberately survives RESET, because a RESET clears
	// session settings but does not undo writes the client already made. Only a
	// new client connection (a fresh session object) starts it at zero.
	// See doc/polardb-arch/10-SESSION-INTEGRATION.md section 6.4.
	PolarDB_SessionConsistency polardb_session_consistency;

	// Mutable state for the one query in flight: the reader acquisition plan, the
	// wait state, the wrapped-query buffer, and the request writer scope. Reset
	// before each query and at query end, so nothing leaks into the next query.
	PolarDB_QueryState polardb_query;

	// Safety latch. When set, this session forces the writer instead of sending an
	// unwrapped read to a replica. It is set when a wait wrapper cannot be built,
	// which would otherwise break read-your-writes silently. Cleared on RESET.
	bool polardb_wait_disabled = false;

	// Queue of NoticeResponse packets captured from a wrapped consistency read,
	// flushed to the client just ahead of the user result so the client sees the
	// same warning-before-result ordering it would without wrapping (e.g. a
	// best_effort LSN wait-timeout WARNING, or a synthetic degraded-route WARNING).
	// This is the only heap-owned PolarDB session field: it is allocated lazily on
	// the first captured notice and freed by clear_pending_notices(). The flush
	// path hands the bytes to the client output array and clears without freeing;
	// every other teardown path frees. See doc/polardb-arch/10-SESSION-INTEGRATION.md
	// section 6.3.
	PtrSizeArray* pending_notices = nullptr;
	// One-shot guard so the per-session "RFQ route degraded" log line is written at
	// most once while a degradation stays active. The client NoticeResponse and the
	// counters are still emitted for every degraded route; only the log line is
	// rate-limited. Re-armed (set back to false) when the degradation clears.
	bool polardb_rfq_degraded_route_warning_sent = false;
#endif // POLARDB_PROXY

	// Describe mode state for \d tablename meta command
	bool describe_mode{ false };
	char describe_table_name[256]{ 0 };

	// When a backend connection breaks mid-transaction AND the admin var
	// pgsql-preserve_client_on_broken_backend_in_tx is on, we synthesize an
	// ERROR 25P02 (current transaction is aborted) + ReadyForQuery('E') to
	// the client, destroy the backend pool connection, and set this flag
	// true instead of tearing down the client session. While this is true,
	// the query intake path short-circuits before query rules:
	//   plain ROLLBACK / ABORT -> synthesize
	//     CommandComplete('ROLLBACK') + ReadyForQuery('I'), clear flag.
	//   plain COMMIT / END -> same ROLLBACK response + NoticeResponse carrying the
	//     "there is no transaction in progress" warning, clear flag.
	//   anything else (including ROLLBACK TO SAVEPOINT and RELEASE SAVEPOINT)
	//     -> reply ERROR 25P02 + ReadyForQuery('E'), stay poisoned.
	bool tx_poisoned{ false };

#ifdef DEBUG
	PgSQL_Connection* dbg_extended_query_backend_conn = nullptr;
#endif

#if 0
	// uint64_t
	unsigned long long start_time;
	unsigned long long pause_until;

	unsigned long long idle_since;
	unsigned long long transaction_started_at;

	// pointers
	PgSQL_Thread* thread;
#endif // 0
	PgSQL_Query_Processor_Output* qpo;
	StatCounters* command_counters;
#if 0
	PgSQL_Backend* mybe;
	PtrArray* mybes;
	PgSQL_Data_Stream* client_myds;
#endif // 0
	PgSQL_Data_Stream* server_myds;
	PgSQL_ExplicitTxnStateMgr* transaction_state_manager;
#if 0
	/*
	 * @brief Store the hostgroups that hold connections that have been flagged as 'expired' by the
	 *  maintenance thread. These values will be used to release the retained connections in the specific
	 *  hostgroups in housekeeping operations, before client packet processing. Currently 'housekeeping_before_pkts'.
	 */
	std::vector<int32_t> hgs_expired_conns{};
	char* default_schema;
	char* user_attributes;

	//this pointer is always initialized inside handler().
	// it is an attempt to start simplifying the complexing of handler()

	uint32_t thread_session_id;
	unsigned long long last_insert_id;
	int last_HG_affected_rows;
	enum session_status status;
	int healthy;
	int user_max_connections;
	int current_hostgroup;
	int default_hostgroup;
	int previous_hostgroup;
	int locked_on_hostgroup;
	int next_query_flagIN;
	int mirror_hostgroup;
	int mirror_flagOUT;
	unsigned int active_transactions;
	int transaction_persistent_hostgroup;
	int to_process;
	enum proxysql_session_type session_type;
	

	// bool
	bool autocommit;
	bool autocommit_handled;
	bool sending_set_autocommit;
	bool killed;
	bool locked_on_hostgroup_and_all_variables_set;
	//bool admin;
	bool max_connections_reached;
	bool client_authenticated;
	bool connections_handler;
	bool mirror;
	//bool stats;
	bool schema_locked;
	bool transaction_persistent;
	bool session_fast_forward;
	//bool started_sending_data_to_client; // this status variable tracks if some result set was sent to the client, or if proxysql is still buffering everything
	bool use_ssl;
#endif // 0

//	MySQL_STMTs_meta* sess_STMTs_meta;
//	StmtLongDataHandler* SLDH;

	Session_Regex** match_regexes;
#ifdef PROXYSQLFFTO
	std::unique_ptr<TrafficObserver> m_ffto;
	bool ffto_bypassed { false };
	void observe_ffto_client_packet(const PtrSize_t& pkt);
	void observe_ffto_server_packet(const PtrSize_t& pkt);
#endif
	CopyCmdMatcher* copy_cmd_matcher;

	ProxySQL_Node_Address* proxysql_node_address; // this is used ONLY for Admin, and only if the other party is another proxysql instance part of a cluster
	bool use_ldap_auth;

	// this variable is relevant only if status == SETTING_VARIABLE
	enum pgsql_variable_name changing_variable_idx;

	PgSQL_Session();
	~PgSQL_Session();

	//void set_unhealthy();

	void set_status(enum session_status e);
	int handler();

	void (*handler_function) (PgSQL_Session* sess, void*, PtrSize_t* pkt);
	//PgSQL_Backend* find_backend(int);
	//PgSQL_Backend* create_backend(int, PgSQL_Data_Stream* _myds = NULL);
	//PgSQL_Backend* find_or_create_backend(int, PgSQL_Data_Stream* _myds = NULL);

	void SQLite3_to_MySQL(SQLite3_result*, char*, int, MySQL_Protocol*, bool in_transaction = false, bool deprecate_eof_active = false) override;
	void PgSQL_Result_to_PgSQL_wire(PgSQL_Connection* conn, PgSQL_Data_Stream* _myds = nullptr);
	
	//unsigned int NumActiveTransactions(bool check_savpoint = false);
	//bool HasOfflineBackends();
	//bool SetEventInOfflineBackends();
	/**
	 * @brief Finds one active transaction in the current backend connections.
	 * @details Since only one connection is returned, if the session holds multiple backend connections with
	 *  potential transactions, the priority is:
	 *   1. Connections flagged with 'SERVER_STATUS_IN_TRANS', or 'autocommit=0' in combination with
	 *      'autocommit_false_is_transaction'.
	 *   2. Connections with 'autocommit=0' holding a 'SAVEPOINT'.
	 *   3. Connections with 'unknown transaction status', e.g: connections with errors.
	 * @param check_savepoint Used to also check for connections holding savepoints. See MySQL bug
	 *  https://bugs.mysql.com/bug.php?id=107875.
	 * @returns The hostgroup in which the connection was found, -1 in case no connection is found.
	 */
	//int FindOneActiveTransaction(bool check_savepoint = false);
	unsigned long long IdleTime();

	//void reset_all_backends();
	//void writeout();
	void Memory_Stats();
	void create_new_session_and_reset_connection(PgSQL_Data_Stream* _myds) override;
	bool handle_command_query_kill(PtrSize_t*);

	//void update_expired_conns(const std::vector<std::function<bool(PgSQL_Connection*)>>&);
	/**
	 * @brief Performs the final operations after current query has finished to be executed. It updates the session
	 *  'transaction_persistent_hostgroup', and updates the 'PgSQL_Data_Stream' and 'PgSQL_Connection' before
	 *  returning the connection back to the connection pool. After this operation the session should be ready
	 *  for handling new client connections.
	 *
	 * @param myds The 'PgSQL_Data_Stream' which status should be updated.
	 * @param myconn The 'PgSQL_Connection' which status should be updated, and which should be returned to
	 *   the connection pool.
	 * @param prepared_stmt_with_no_params specifies if the processed query was a prepared statement with no
	 *   params.
	 */
	void finishQuery(PgSQL_Data_Stream* myds, PgSQL_Connection* myconn, bool sticky_backend_connection);
	void generate_proxysql_internal_session_json(nlohmann::json&) override;
	bool known_query_for_locked_on_hostgroup(uint64_t);
	void unable_to_parse_set_statement(bool*);
	//bool has_any_backend();
	void detected_broken_connection(const char* file, unsigned int line, const char* func, const char* action, PgSQL_Connection* myconn, bool verbose = false);
	void generate_status_one_hostgroup(int hid, std::string& s);
	void set_previous_status_mode3(bool allow_execute = true);
	char* get_current_query(int max_length = -1);
	bool is_in_transaction() const;

#if POLARDB_PROXY
	// ---- PolarDB LSN routing pipeline ----

	/**
	 * @brief Collect all routing inputs for one query into an immutable snapshot.
	 *
	 * Reads session, HostGroups_Manager, and thread state into @p route_ctx, which
	 * polardb_plan() then decides from. One side effect: if the replication group's
	 * writer scope changed since this session last collected, it first clears the
	 * session's now-stale LSN targets and latches before copying them in.
	 */
	void polardb_collect(PolarDB_Query_RouteCtx& route_ctx, int current_hg, int qpo_replica_eligible, bool qpo_force_primary_hint);
	/**
	 * @brief Decide the route for one query from the collected snapshot.
	 *
	 * Deterministic and side-effect free apart from read-only HGM snapshots. The
	 * LSN consistency feature applies to autocommit, simple-query reads only;
	 * explicit transactions, multi-statement, and extended-protocol queries are
	 * routed to the writer rather than offloaded with a wait wrapper.
	 */
	PolarDB_Query_RoutePlan polardb_plan(const PolarDB_Query_RouteCtx& route_ctx);
	/** @brief Update counters and emit the rate-limited log for a produced route plan. */
	void polardb_account_route_plan(const PolarDB_Query_RoutePlan& plan,
		const PolarDB_Query_RouteCtx& route_ctx);
	/**
	 * @brief Apply the route plan's side effects and stage wait state if needed.
	 *
	 * Resolves the final target hostgroup. For a replica-with-wait plan it prepares
	 * the per-query wait state and snapshots the original query text; the wrapped
	 * query itself is not built here but later, once, by
	 * finalize_wait_timeout_injection() at ASYNC_IDLE.
	 */
	PolarDB_Query_ExecuteResult polardb_execute(const PolarDB_Query_RoutePlan& plan,
		const PolarDB_Query_RouteCtx& route_ctx, PtrSize_t& pkt);
	/**
	 * @brief Detect whether a query rule is doing manual destination-hostgroup routing.
	 *
	 * Manual means the rule set a destination hostgroup but did not opt into
	 * automatic replica routing. In that case the PolarDB planner must not override
	 * the route; it only stages the writer scope for result attribution. Returns
	 * true and reports that scope hostgroup via @p scope_hg; the optional outputs
	 * return the rule's raw destination and replica-eligible values.
	 */
	bool polardb_manual_route_scope(int* scope_hg, int* dest_hg = nullptr,
		int* replica_eligible = nullptr) const;
	/**
	 * @brief Record which writer scope this request runs under, for LSN attribution.
	 *
	 * Snapshots the writer hostgroup and epoch for @p scope_hg into the per-query
	 * state. Used on the manual-routing path, which skips collect() but still needs
	 * the scope so the response path can tell whether an RFQ LSN belongs to the
	 * current writer timeline. Returns false if @p scope_hg is not a PolarDB
	 * hostgroup.
	 */
	bool polardb_capture_request_writer_scope(int scope_hg);
	/**
	 * @brief Apply automatic routing for an extended-protocol (Parse/Bind/Execute)
	 *        query, without a wait wrapper.
	 *
	 * The wait is injected as SQL text, which cannot be safely spliced into an
	 * extended-protocol stream, so this is routing only. Manual destination rules
	 * are left untouched. An automatic replica-eligible read may use a reader only
	 * when the session has no wait target; otherwise it is pinned to the writer.
	 */
	void polardb_apply_extended_route();
	/**
	 * @brief Attach lag-cap inputs (primary LSN mirror and byte cap) to the reader plan.
	 *
	 * No-op when no byte cap is configured. When a cap applies, backend acquisition
	 * later enforces the per-reader byte lag, so the chosen reader is one within the
	 * cap.
	 */
	void polardb_reader_lag_plan(PolarDB_Query_RoutePlan& plan,
		const PolarDB_Query_RouteCtx& route_ctx);
	/**
	 * @brief True when query cache must not serve or store the current query.
	 *
	 * Automatic PolarDB consistency reads must pass through routing so the
	 * session LSN policy can choose the writer or add a replica wait.
	 */
	bool polardb_query_cache_disabled_for_current_rule() const;
	/**
	 * @brief Send the in-flight query to the writer instead of a replica.
	 *
	 * Clears the staged reader target and wait, sets current_hostgroup to
	 * @p writer_hg, and acquires a backend there. Used when a consistency-safe
	 * reader cannot be obtained; the query uses the writer instead of an
	 * unprotected replica read. Returns false (no change)
	 * if @p writer_hg is negative.
	 */
	bool polardb_redirect_to_writer(int writer_hg, const char* reason);
	/**
	 * @brief Update session consistency state from the finished query's result.
	 *
	 * Runs on the success path. Reads the WAL LSN the backend appended to its
	 * ReadyForQuery message (no extra round-trip), advances the session's observed
	 * and write LSN positions, maintains the missing-LSN latches, and refreshes the
	 * per-server LSN cache. Writer-epoch-stale RFQs are rejected first.
	 */
	void polardb_process_result(PgSQL_Data_Stream* myds, const char* query_digest_text);

	/** @brief Set the per-session consistency mode override (-1 clears it). */
	void polardb_set_session_override(int mode);

	// ---- PolarDB wait wrapping and notices ----

	/** @brief Whether an LSN wait is prepared for the in-flight query. */
	inline bool polardb_wait_active() const {
		return polardb_query.wait.wait_stage == PolarDB_WaitStage::WAITING;
	}
	/**
	 * @brief Return the `SET polar_consistency_mode = '...'` statement for a wait mode.
	 *
	 * This is the first SET in the wait wrapper; it tells the replica what to do
	 * when the wait times out (best_effort returns stale data with a WARNING,
	 * strict raises an ERROR). The two statements are fixed text, so this returns a
	 * reference to a long-lived static string. @p wait_mode is the timeout-behavior
	 * knob, not the routing consistency mode. Do not free or modify the result.
	 */
	const std::string& build_polar_consistency_mode_set(PolarDB_WaitMode wait_mode);
	/**
	 * @brief Assemble the wrapped wait query (mode + timeout + wait SETs, then the
	 *        user query) into @p out.
	 *
	 * Safety contract: @p out is left empty when there is nothing safe to wrap
	 * (empty query, no wait, or an LSN target of 0). The caller treats an empty
	 * buffer as a build failure and must not run the read unwrapped on a replica.
	 *
	 * @param prefix The consistency-mode SET statement to place first, from
	 *               build_polar_consistency_mode_set(). The timeout and wait SETs
	 *               are appended after it, then the user query.
	 */
	void build_wrapped_wait_query(const char* orig_query, size_t orig_len,
		const PolarDB_Query_WaitState& wait_state, const std::string& prefix, std::string& out);
	/**
	 * @brief Build the wrapped wait query and install it into the outgoing packet.
	 *
	 * The single point where the wrapper is applied, called once at ASYNC_IDLE
	 * after the backend connection exists. Idempotent (a second call after success
	 * is a no-op). If a needed wrapper cannot be built it latches the session to
	 * the writer and returns FAILED, and the caller must abort the query
	 * rather than send it unwrapped.
	 */
	PolarDB_WrapFinalizeResult finalize_wait_timeout_injection(PgSQL_Connection* conn, PgSQL_Data_Stream* myds);

	/**
	 * @brief Add the in-flight wait's elapsed time to the latency counter and clear
	 *        its start timer.
	 *
	 * No-op if no wait was active. Zeroing the start timer also disarms timeout
	 * accounting, so the same backend wait cannot be counted twice. Must run before
	 * the per-query wait state is reset, or the elapsed time is lost.
	 */
	void record_wait_latency(PolarDB_Query_WaitState& state);
	/**
	 * @brief Count one proven PolarDB wait timeout and its elapsed latency.
	 *
	 * Precondition: the caller has already proven the backend event is a PolarDB
	 * proxy wait timeout (for example by matching the timeout detail marker).
	 * Consumes the wait start timer through record_wait_latency(), so a repeated
	 * observation of the same event does not double-count. Returns false (no count)
	 * if no wait is active or the timer was already consumed.
	 */
	bool polardb_account_wait_timeout(const char* source);

	/**
	 * @brief Empty and free the captured-notice queue.
	 *
	 * Idempotent (safe when the queue was never allocated). Pass
	 * free_buffers=false only on the flush path, where the packet bytes have been
	 * handed to the client output array, which then owns them; every other path
	 * passes true to free the buffers.
	 */
	void clear_pending_notices(bool free_buffers = false);
	/**
	* @brief Move queued NoticeResponse packets to the client output array.
	 *
	 * Used before normal result forwarding and before the first streamed result
	 * chunk, so best_effort wait warnings stay ahead of rows.
	*/
	void polardb_flush_pending_notices_to_client();
	/**
	* @brief Take ownership of a NoticeResponse packet to forward with the next result.
	 *
	 * Allocates the queue lazily on the first captured notice, so a session that
	 * never hits a wait timeout never allocates it. @p pkt becomes session-owned.
	 */
	void enqueue_pending_notice(unsigned char* pkt, unsigned int size);
	/** @brief Build and enqueue a NoticeResponse packet. */
	bool polardb_enqueue_notice_packet(const char* severity, const char* sqlstate,
		const char* primary, const char* detail = nullptr,
		const char* severity_nonlocalized = nullptr);
	/** @brief Queue a synthetic client-visible warning for a degraded RFQ route. */
	void polardb_enqueue_degraded_rfq_notice(const char* reason,
		int reader_hg, int writer_hg);
	/** @brief Same, deriving the reason and hostgroups from the route plan/context. */
	void polardb_enqueue_degraded_rfq_notice(const PolarDB_Query_RoutePlan& plan,
		const PolarDB_Query_RouteCtx& route_ctx);

	/**
	 * @brief Clear staged per-query wait/notice state on a RESET-family command.
	 *
	 * Handles RESET / RESET ALL / DISCARD ALL / RESET CONNECTION. Clears the
	 * per-query wait, notices, and wrapper state, and lifts the wait-disabled
	 * latch. It deliberately does NOT clear the durable session write/observed
	 * LSNs: a RESET clears session settings, not the fact that the client has
	 * written, so read-your-writes must survive it. @p reset_override additionally
	 * clears the per-session consistency-mode override.
	 */
	void polardb_clear_staged_wait_state_for_reset(bool reset_override);
#endif // POLARDB_PROXY

private:
#if POLARDB_PROXY
	/**
	 * @brief Captured state for one failed wait-wrapped replica read.
	 *
	 * This snapshot is collected before normal error handling mutates the
	 * replica data stream. Retry code uses it to decide whether one autocommit
	 * wait-wrapped read can run again on the primary after a strict wait timeout
	 * or a lost replica connection. It does not cover ordinary SQL errors,
	 * transaction retry, or split reads.
	 */
	struct PolarDB_WaitReadFailure {
		PgSQL_Data_Stream* failed_myds = NULL;
		bool wait_read = false;
		bool result_started = false;
		bool wrapper_set_failure = false;
		bool timeout_error = false;
		bool connection_lost = false;
		bool can_return_to_pool = false;
		int reader_hg = -1;
		std::string reader_address;
		int reader_port = 0;
		int fallback_writer_hg = -1;
		std::string original_query;
	};

	/**
	 * @brief Stop safely when the wait wrapper cannot be built or installed.
	 *
	 * Counts the abort, logs @p reason, latches polardb_wait_disabled so later
	 * reads in this session go to the writer until RESET, and clears the half-built
	 * wrapper and wait state. Always returns PolarDB_WrapFinalizeResult::FAILED; the
	 * caller must stop before running the query and return a clean error.
	 */
	PolarDB_WrapFinalizeResult fail_wait_wrap_finalize(const char* reason);
	/** @brief Capture retry-relevant details from a failed wait-wrapped replica read. */
	PolarDB_WaitReadFailure polardb_capture_wait_read_failure(PgSQL_Data_Stream* failed_myds);
	/**
	 * @brief Handle a failed wait-wrapped read without redispatching wrapper SETs.
	 *
	 * A safe pre-result replica failure is redirected to the primary as the
	 * original SQL. Otherwise the wrapper packet is removed and the normal
	 * error path returns a clean client error.
	 */
	bool polardb_handle_failed_wait_read(const PolarDB_WaitReadFailure& failure);
	/** @brief Build a PostgreSQL simple-query packet owned by the caller. */
	void build_simple_query_packet(const std::string& sql, PtrSize_t& out);
#endif // POLARDB_PROXY

	int32_t extract_pid_from_param(const PgSQL_Param_Value& param, uint16_t format) const;
	void send_parameter_error_response(const char* error_message, PGSQL_ERROR_CODES code = PGSQL_ERROR_CODES::ERRCODE_INVALID_TEXT_REPRESENTATION);
	bool handle_kill_success(int32_t pid, int tki, const char* digest_text, PgSQL_Connection* mc, PtrSize_t* pkt);
	bool handle_literal_kill_query(PtrSize_t* pkt, PgSQL_Connection* mc);

#if defined(__clang__)
	template<typename SESS, typename DS, typename BE, typename THD>
	friend class Base_Session;
#else
	friend class Base_Session<PgSQL_Session, PgSQL_Data_Stream, PgSQL_Backend, PgSQL_Thread>;
#endif
};



#endif /* PROXYSQL_PGSQL_SESSION_H */
#endif // CLASS_BASE_SESSION_H
