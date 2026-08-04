#ifdef CLASS_BASE_SESSION_H

#ifndef PROXYSQL_PGSQL_SESSION_H
#define PROXYSQL_PGSQL_SESSION_H

#include <functional>
#include <vector>
#include <variant>
#include <string_view>
#include "proxysql.h"
#include "Base_Session.h"
#include "cpp.h"
#include "PgSQL_Error_Helper.h"
#include "PgSQL_Variables.h"
#include "PgSQL_Variables_Validator.h"
#include "PgSQL_PolarDB.h"   // PolarDB LSN types (no-op when POLARDB_PROXY=0)

class PgSQL_Query_Result;
class PgSQL_ExplicitTxnStateMgr;
class PgSQL_Connection;
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
#if POLARDB_PROXY
	struct PolarDB_ExtendedRouteState {
		bool execute_pending{false};

		void reset() {
			execute_pending = false;
		}
	} polardb_extended_route;

	inline bool polardb_extended_request_continues(
			bool called_on_failure, bool result_has_error) const {
		return !called_on_failure &&
			!result_has_error &&
			!extended_query_frame.empty() &&
			(extended_query_phase &
				(EXTQ_PHASE_PROCESSING_PARSE |
				 EXTQ_PHASE_PROCESSING_DESCRIBE));
	}
#endif // POLARDB_PROXY

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

	bool handler___client_DSS_QUERY_SENT___server_DSS_NOT_INITIALIZED__get_connection();

	bool is_multi_statement_command(const char* cmd);
	bool handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___handle_SET_command(const char* dig, bool* lock_hostgroup);
	bool handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___handle_RESET_command(const char* dig, bool* lock_hostgroup);
	bool handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___handle_DISCARD_command(const char* dig);
	bool handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___handle_DEALLOCATE_command(const char* dig);
	bool handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___handle_special_commands(const char* dig, bool* lock_hostgroup);
	void reapply_user_attributes_after_reset();
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
	// routing, result-processing, and reset paths share one set of fields cleared in
	// one place instead of scattering loose flags across the session.
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
	 *   eventual     route without an LSN wait
	 *   session_lsn  wait on this session's write/observed LSN target
	 *   global_lsn   wait on max(session target, group LSN)
	 *
	 * Supported values for SET proxysql.polardb_txn_split_warmup:
	 *   default  use the built-in default (demand)
	 *   off      never request split reader warmup from this session
	 *   demand   request warmup after a split read misses the reader pool
	 *   begin    request warmup at BEGIN / START TRANSACTION only
	 *   both     request at BEGIN and after a later split-read pool miss
	 */
	struct PolarDB_SessionConfig {
		// Set true once this session attaches to a backend in a PolarDB-enabled
		// hostgroup (on fresh connect, or when a pooled connection is reused). It
		// controls the response-path result processing. Only ever turned on: once a
		// session has used a PolarDB backend it keeps capturing LSNs for its life.
		bool is_polardb_enabled = false;
		// Per-session consistency-mode override, and the top tier of mode resolution
		// (session override > hostgroup > global). -1 means "no override", so the
		// resolved mode falls through to the per-hostgroup and global settings.
		int session_consistency_mode = -1;
		// Per-session transaction-split warmup timing override. -1 means use the
		// built-in default: demand warmup, requested only after a split read misses
		// the reader pool.
		int txn_split_warmup_mode = -1;
		// Operator-declared transaction isolation for pre-write transaction reads.
		// PostgreSQL tracked variables do not currently include transaction
		// isolation, so this is seeded from pgsql_users.attributes, adjusted by
		// explicit BEGIN/SET isolation statements, and overridden by backend
		// ParameterStatus when a PolarDB backend reports transaction isolation.
		bool txn_reader_wait_default_read_committed = true;
		bool txn_reader_wait_backend_default_seen = false;
	} polardb_config;

	// Durable per-session consistency truth: the read-your-writes LSN target
	// (write and observed positions), the missing-LSN sticky flags, and the writer
	// hostgroup/epoch scope those values belong to. This lives for the whole
	// session: both positions deliberately survive RESET, because a RESET does
	// not erase what this client has already written or observed. Only a new
	// client connection starts them at zero.
	// See doc/polardb-arch/10-SESSION-INTEGRATION.md section 6.4.
	PolarDB_SessionConsistency polardb_session_consistency;
	// True when the writer reports T + x + an explicit empty XID list.
	bool polardb_txn_has_no_write_xids = false;

	// Observed transaction-split RFQ state. The planner reads it when determining
	// whether one in-transaction read can be sent to a replica with exported
	// transaction XIDs and an LSN wait.
	PolarDB_TransactionSplitState polardb_transaction_split;

	/**
	 * @brief Session-owned state for one temporary transaction reader request.
	 *
	 * Both transaction-split reads and pre-write transaction wait reads move mybe
	 * to a reader while the writer transaction remains open on the primary backend.
	 */
	struct PolarDB_TxnReaderState {
		PgSQL_Backend* backend = nullptr;
		PgSQL_Backend* primary_backend = nullptr;
		bool split_active = false;
		bool wait_read_active = false;
		// Set when the backend reports a wait timeout. Latency accounting clears
		// wait_start_us before failure handling reads this state.
		bool wait_timeout_error = false;
		std::string wrapped_query;
		PtrSize_t original_pkt{};
		PolarDB_WaitSpec wait_spec;
		PolarDB_WriterScope writer_scope;
		// Writer scope for the retained reader's last RFQ LSN.
		PolarDB_WriterScope rfq_writer_scope;
		unsigned long long read_start_us = 0;
		unsigned long long wait_start_us = 0;

		bool active() const {
			return split_active || wait_read_active;
		}

		bool has_primary_backend() const {
			return primary_backend != nullptr;
		}

		void clear_request_state() {
			wrapped_query.clear();
			original_pkt = {};
			wait_spec.reset();
			writer_scope.reset();
			read_start_us = 0;
			wait_start_us = 0;
			wait_timeout_error = false;
			primary_backend = nullptr;
			split_active = false;
			wait_read_active = false;
		}

		void take_original_packet(PtrSize_t& packet) {
			original_pkt = packet;
			packet = {};
		}

		PtrSize_t release_original_packet() {
			PtrSize_t packet = original_pkt;
			original_pkt = {};
			return packet;
		}

		void clear_backend() {
			backend = nullptr;
			rfq_writer_scope.reset();
		}
	};

	// Temporary state for one in-flight transaction reader read. Split reads add
	// exported XIDs; pre-write consistency reads only use the normal wait wrapper.
	PolarDB_TxnReaderState polardb_txn_reader;
	// Pre-write reads stay on the primary unless the transaction is READ COMMITTED
	// and has no transaction-local backend state. Higher isolation levels keep the
	// transaction snapshot fixed, and an in-transaction SET may create state that
	// is absent from the temporary reader.
	struct PolarDB_TxnReaderWaitSafetyState {
		bool non_read_committed = false;
		bool local_state_changed = false;

		void clear() {
			non_read_committed = false;
			local_state_changed = false;
		}
	};
	PolarDB_TxnReaderWaitSafetyState polardb_txn_wait_safety;
	struct PolarDB_TxnReaderFailureState {
		PolarDB_ReaderFailureRoute route = PolarDB_ReaderFailureRoute::NONE;
		int writer_hg = -1;
		int reader_hg = -1;
		std::string reader_address;
		int reader_port = -1;

		bool active() const {
			return route != PolarDB_ReaderFailureRoute::NONE;
		}

		int forced_writer_hg() const {
			return route == PolarDB_ReaderFailureRoute::FORCE_WRITER &&
				writer_hg >= 0 ? writer_hg : -1;
		}

		bool matches_skipped_reader(int target_hg, const char** out_address,
				int* out_port) const {
			const bool skip =
				route == PolarDB_ReaderFailureRoute::SKIP_READER &&
				reader_hg == target_hg &&
				!reader_address.empty() &&
				reader_port >= 0;
			if (skip) {
				if (out_address) {
					*out_address = reader_address.c_str();
				}
				if (out_port) {
					*out_port = reader_port;
				}
			}
			return skip;
		}

		void clear() {
			route = PolarDB_ReaderFailureRoute::NONE;
			writer_hg = -1;
			reader_hg = -1;
			reader_address.clear();
			reader_port = -1;
		}

		void set_force_writer(int hg) {
			clear();
			route = PolarDB_ReaderFailureRoute::FORCE_WRITER;
			writer_hg = hg;
		}

		void set_reader_skip(int hg, const std::string& address, int port) {
			clear();
			route = PolarDB_ReaderFailureRoute::SKIP_READER;
			reader_hg = hg;
			reader_address = address;
			reader_port = port;
		}
	};

	// Transaction-scoped reader-failure route state. FORCE_WRITER keeps the
	// remainder of the transaction on the writer even on paths that bypass the
	// normal planner, such as manual route rules and query-cache lookup.
	// SKIP_READER keeps split eligible but excludes one failed endpoint.
	PolarDB_TxnReaderFailureState polardb_txn_reader_failure;

	// Mutable state for the one query in flight: the reader acquisition plan, the
	// wait state, the wrapped-query buffer, and the request writer scope. Reset
	// before each query and at query end, so nothing leaks into the next query.
	PolarDB_QueryState polardb_query;

	/**
	 * @brief Per-session state for one wait on reader-pool capacity.
	 *
	 * reservation_server is a raw pointer into the hostgroup server list and is
	 * valid only while reservation_server_snapshot is held: the snapshot shared_ptr
	 * is the sole thing keeping that server alive here. Drop the snapshot and the
	 * pointer must not be read again.
	 *
	 * active means polardb_enter_reader_capacity_wait() registered a
	 * reader-pool reservation for this session on PgSQL_Thread. Release it with
	 * polardb_leave_reader_capacity_wait(), which cancels the thread-side
	 * reservation and records the wait latency. reset() only wipes the session
	 * fields, so calling it on an active wait leaves the thread-side reservation
	 * in place and it is never cancelled.
	 */
	struct PolarDB_ReaderCapacityWaitState {
		uint64_t started_at_us = 0;
		uint64_t scope_hash = 0;
		PolarDB_PoolCapacityTarget target =
			PolarDB_PoolCapacityTarget::READER;
		PolarDB_Query_ReaderPlan reservation_plan;
		PolarDB_WaitSpec reservation_wait_spec;
		PgSQL_SrvC* reservation_server = nullptr;
		std::shared_ptr<const void> reservation_server_snapshot;
		uint32_t reservation_profile_generation = 0;
		PolarDB_PoolKey reservation_pool_key;
		PolarDB_ReaderStatus last_status =
			PolarDB_ReaderStatus::READER_UNAVAILABLE;
		bool active = false;
		bool retry_admitted = false;
		bool result_valid = false;

		void reset() {
			started_at_us = 0;
			scope_hash = 0;
			target = PolarDB_PoolCapacityTarget::READER;
			reservation_plan.reset();
			reservation_wait_spec.reset();
			reservation_server = nullptr;
			reservation_server_snapshot.reset();
			reservation_profile_generation = 0;
			reservation_pool_key = PolarDB_PoolKey{};
			last_status = PolarDB_ReaderStatus::READER_UNAVAILABLE;
			active = false;
			retry_admitted = false;
			result_valid = false;
		}
	};
	PolarDB_ReaderCapacityWaitState polardb_reader_capacity_wait;

	struct PolarDB_SessionRouteState {
		// When set, this session forces the writer instead of sending an
		// unwrapped read to a replica. It is set when a wait wrapper cannot be
		// built, which would otherwise break read-your-writes silently.
		bool wait_disabled = false;
		// Frontend capability negotiated from client startup parameters. When
		// true, ProxySQL appends the backend RFQ LSN to ReadyForQuery packets it
		// sends to the client, matching PolarDB's extended RFQ layout.
		bool client_rfq_lsn_requested = false;
		// One-shot flag so the per-session "RFQ route degraded" log line is
		// written at most once while a degradation stays active. The client
		// NoticeResponse and counters are still emitted for every degraded route.
		bool rfq_degraded_route_warning_sent = false;
		// Cached per-session startup identity key for ReaderPool lookups. The
		// endpoint is fixed for a frontend connection; the mode check below
		// invalidates it if the runtime identity policy changes.
		uint64_t reader_pool_startup_identity_hash = 0;
		int reader_pool_startup_identity_mode = -1;

		void clear_resettable() {
			wait_disabled = false;
			rfq_degraded_route_warning_sent = false;
		}

		void clear_session() {
			clear_resettable();
			client_rfq_lsn_requested = false;
			reader_pool_startup_identity_hash = 0;
			reader_pool_startup_identity_mode = -1;
		}
	};
	PolarDB_SessionRouteState polardb_route_state;

	struct PolarDB_NoticeQueueState {
		PtrSizeArray* pending = nullptr;

		bool empty() const {
			return pending == nullptr || pending->len == 0;
		}

		unsigned int len() const {
			return pending ? pending->len : 0;
		}

		void add(unsigned char* pkt, unsigned int size);

	private:
		friend class PgSQL_Session;
		void clear(bool free_buffers);
	};

	// NoticeResponse packets captured from a wrapped consistency read, flushed to
	// the client just ahead of the user result so the client receives the same
	// warning-before-result ordering it would without wrapping.
	// See doc/polardb-arch/10-SESSION-INTEGRATION.md section 6.3.
	PolarDB_NoticeQueueState polardb_notices;
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
	 * Reads session, HostGroups_Manager, and thread state into a new context, which
	 * polardb_plan() then routes from. Call polardb_observe_route_inputs() first
	 * so durable session state is already reconciled before the snapshot is copied.
	 */
	PolarDB_Query_RouteCtx polardb_collect(int current_hg, int qpo_replica_eligible,
		bool qpo_force_primary_hint) const;
	/**
	 * @brief Apply durable session observations needed before building a route snapshot.
	 *
	 * Updates request writer scope, writer-epoch-scoped session LSN state, and
	 * transaction reader-wait safety flags from the current query text.
	 */
	void polardb_observe_route_inputs(int current_hg);
	/**
	 * @brief Select the route for one query from the collected snapshot.
	 *
	 * Makes no routing, session, or counter changes and reads HGM only for
	 * snapshots. The LSN consistency feature applies to autocommit, simple-query
	 * reads only; multi-statement and extended-protocol queries are routed to the
	 * writer.
	 * Explicit transactions stay on the writer unless the transaction-split policy
	 * and primary RFQ evidence allow one read to take a replica connection from
	 * the pool and return it after that read.
	 */
	PolarDB_Query_RoutePlan polardb_plan(
		const PolarDB_Query_RouteCtx& route_ctx) const;
	/** @brief Update route counters, enqueue a client notice, and rate-limit its warning. */
	void polardb_report_route_result(const PolarDB_Query_RoutePlan& plan,
		const PolarDB_Query_RouteCtx& route_ctx);
	/** @brief Count a manual query-rule/sticky-hostgroup route that bypassed the planner. */
	void polardb_account_manual_route(int effective_hg, bool forced_writer);
	/** @brief Apply writer routing required after a transaction reader failure. */
	bool polardb_apply_reader_failure_writer_route(const char* stage);
	/**
	 * @brief Apply the route plan's side effects and retain reader-selection input.
	 *
	 * Resolves the final target hostgroup. A plan with txn_wait_read set takes
	 * priority: it is the pre-write in-transaction consistency read, and it prepares a
	 * temporary reader through polardb_prepare_txn_wait_read(). When the session has
	 * waits disabled, or that prepare declines, the read falls back to the writer
	 * hostgroup in @p route_ctx and no reader is staged. For an ordinary
	 * replica-with-wait plan it retains only the wait target used to select a
	 * reader. A target-ready reader dispatches the original packet directly;
	 * otherwise backend acquisition activates wait state and
	 * polardb_install_wait_wrapper() builds the wrapper once at ASYNC_IDLE. For a transaction split
	 * read it prepares the temporary replica backend immediately and preserves the
	 * primary backend for the open transaction.
	 *
	 * @param plan       Routing plan from polardb_plan().
	 * @param route_ctx  Routing context from polardb_collect().
	 * @param pkt        Client simple-query packet. Replica-with-wait leaves
	 *                   ownership with the caller; the transaction
	 *                   split and transaction wait-read branches move ownership to
	 *                   the reader cleanup path once their wrapper is installed, so
	 *                   the caller must not free or reuse it afterwards.
	 * @return Execution result carrying the final target hostgroup.
	 */
	PolarDB_Query_ExecuteResult polardb_execute(const PolarDB_Query_RoutePlan& plan,
		const PolarDB_Query_RouteCtx& route_ctx, PtrSize_t& pkt);
	/**
	 * @brief End the current request with a clear consistency-policy error.
	 *
	 * Used when an action is configured as error and no backend result
	 * exists to forward. The client receives one PostgreSQL ErrorResponse and
	 * the request is completed normally, so the connection remains usable.
	 */
	void polardb_return_consistency_error(
		PolarDB_Query_RoutePlan::RouteActionReason reason);
	struct PolarDB_ManualRoute {
		int scope_hg{-1};
		int destination_hg{-1};
		int replica_eligible{-1};

		bool is_manual() const {
			return replica_eligible < 0 && destination_hg >= 0;
		}
	};
	/**
	 * @brief Return the current query rule's manual-route values.
	 */
	PolarDB_ManualRoute polardb_manual_route() const;
	/**
	 * @brief Record which writer scope this request runs under, for LSN attribution.
	 *
	 * Snapshots the writer hostgroup and epoch for @p scope_hg into the per-query
	 * state. Used on the manual-routing path, which skips collect() but still needs
	 * the scope so the response path can determine whether an RFQ LSN belongs to the
	 * current writer timeline.
	 *
	 * The per-query request writer scope is cleared on entry, so any scope captured
	 * earlier for this request is discarded even when the new capture fails.
	 *
	 * @param scope_hg  Hostgroup whose writer scope this request runs under.
	 * @return true when a valid scope was captured. false when @p scope_hg is not a
	 *         PolarDB hostgroup, which leaves the request with no writer scope: the
	 *         response path then treats every backend RFQ LSN as out of scope and
	 *         drops it, so no LSN attribution happens for this request.
	 */
	bool polardb_capture_request_writer_scope(int scope_hg);
	/**
	 * @brief Apply automatic routing for an extended-protocol request.
	 *
	 * Manual destination rules are left untouched. Parse, Bind, and Describe stay
	 * on the writer. Execute may select a reader directly when it is target-ready,
	 * or carry a protocol-level wait to a v15_wait reader when it is behind.
	 */
	bool polardb_apply_extended_route(PgSQL_Extended_Query_Type stmt_type);
	/**
	 * @brief Attach lag-cap inputs (group LSN and byte cap) to the reader plan.
	 *
	 * No-op when no byte cap is configured. When a cap applies, backend acquisition
	 * later enforces the per-reader byte lag, so the chosen reader is one within the
	 * cap.
	 */
	void polardb_attach_reader_lag_cap(PolarDB_Query_RoutePlan& plan,
		const PolarDB_Query_RouteCtx& route_ctx) const;
	/**
	 * @brief True when query cache must not serve or store the current query.
	 *
	 * Two independent reasons. First, once the client negotiated the RFQ-LSN
	 * extension the cache is off for every query for the rest of the session: a
	 * cached result carries no backend RFQ LSN, so there would be nothing to append
	 * to the client ReadyForQuery. Second, automatic PolarDB consistency reads must
	 * pass through routing so the session LSN policy can choose the writer or add a
	 * replica wait.
	 */
	bool polardb_query_cache_is_disabled() const;
	/**
	 * @brief Send the in-flight query to the writer instead of a replica.
	 *
	 * Clears the staged reader target and wait, sets current_hostgroup to
	 * @p writer_hg, and acquires a backend there. Used when a consistency-safe
	 * reader cannot be obtained; the query uses the writer instead of an
	 * unprotected replica read.
	 *
	 * When this changes backend streams, the pending simple-query packet moves with
	 * the query: any packet already attached to the writer stream is freed, then the
	 * previous stream's pgsql_real_query is moved onto the writer stream. After a
	 * true return the previous stream holds no packet and the caller must neither
	 * re-install nor free it.
	 *
	 * @param writer_hg  Writer hostgroup to send this one query to.
	 * @param reason     Trace-only description of why the redirect happened.
	 * @return true when the redirect was applied. false when @p writer_hg is
	 *         negative, in which case nothing changed and the caller keeps the
	 *         normal pool path.
	 */
	bool polardb_redirect_to_writer(int writer_hg, const char* reason);
	/**
	 * @brief Keep a retry consistent after skipping a reader wait.
	 *
	 * Ordinary retries remain unchanged. A query whose selected reader already
	 * satisfied the wait target cannot retry an unchecked reader. The captured
	 * replica-loss action either permits a primary retry or leaves the original
	 * connection failure to the normal client error/close path.
	 */
	bool polardb_redirect_wait_retry_to_writer(
		bool retry_conn, const char* reason);
	/**
	 * @brief Update session consistency state from the finished query's result.
	 *
	 * Runs on the success path. Reads the WAL LSN the backend appended to its
	 * ReadyForQuery message (no extra round-trip), advances the session's observed
	 * and write LSN positions, maintains the missing-LSN sticky flags, and refreshes the
	 * per-server LSN cache. Writer-epoch-stale RFQs are rejected first.
	 */
	void polardb_process_result(PgSQL_Data_Stream* myds, const char* query_text,
		PGSQL_QUERY_command query_cmd);
	/**
	 * @brief Prepare the PolarDB LSN appended to a client ReadyForQuery packet.
	 *
	 * Called from the backend connection result path after libpq consumed the
	 * backend RFQ, but before RequestEnd() updates response-side session state.
	 * It preserves raw backend RFQ values for no-wait replica responses, while
	 * translating writer or successful-wait responses to a confirmed frontend target
	 * when the selected backend's local RFQ value is lower. It also records whether
	 * the following polardb_process_result() call must preserve the prior session
	 * LSN. Per-query state reset clears that decision before the next result. It
	 * never fabricates an LSN when the backend RFQ payload is absent.
	 *
	 * @param conn                    Backend connection whose result is being
	 *                                completed.
	 * @param backend_payload_present True when the backend ReadyForQuery actually
	 *                                carried an LSN payload.
	 * @param backend_lsn             LSN read from that payload; meaningless when
	 *                                @p backend_payload_present is false.
	 * @param client_lsn              Out parameter receiving the LSN to append.
	 * @return true when *client_lsn holds the LSN to append to the client
	 *         ReadyForQuery. false when nothing must be appended: a null
	 *         @p client_lsn returns false without touching any state, while every
	 *         other false path zeroes *client_lsn and may already have recorded the
	 *         keep-session-LSN decision for the following polardb_process_result().
	 */
	bool polardb_prepare_client_ready_lsn(PgSQL_Connection* conn,
		bool backend_payload_present, uint64_t backend_lsn,
		uint64_t* client_lsn);

	/**
	 * @brief Observe transaction-split RFQ metadata from an accepted primary result.
	 *
	 * Reads the transaction-status byte, XID list, splittable flag, and WAL-pending
	 * flag that libpq cached from ReadyForQuery. This updates only
	 * polardb_transaction_split; the planner selects the route later.
	 */
	void polardb_observe_transaction_split(PgSQL_Connection* conn,
		uint64_t primary_lsn, bool split_enabled, bool primary_source);
	/**
	 * @brief Release any temporary split backend and clear transaction-split state.
	 *
	 * Used when split observation is disabled, a transaction closes, or the
	 * writer scope changes. The reason is trace-only.
	 */
	void polardb_teardown_transaction_reader_state(const char* reason, bool want_reuse);

	/** @brief Set the per-session consistency mode override (-1 clears it). */
	void polardb_set_session_consistency_mode(int mode);
	/** @brief Set the per-session transaction-split warmup mode override. */
	void polardb_set_txn_split_warmup_mode(int mode);
	/** @brief Return the active transaction-split warmup mode for this session. */
	int polardb_effective_txn_split_warmup_mode() const;
	/** @brief Apply PolarDB-relevant pgsql_users.attributes to this session. */
	void polardb_apply_user_attributes(const char* attributes);
	/** @brief Re-apply user attributes after a full session reset command. */
	void polardb_reapply_user_attributes_after_reset();
	/** @brief Apply backend-reported transaction isolation, when available. */
	void polardb_apply_backend_isolation_status(PgSQL_Connection* conn,
		const char* reason);
	/** @brief Queue transaction-split reader warmup for this session, if possible. */
	void polardb_request_txn_split_warmup(int reader_hg, const char* reason,
		const PgSQL_SrvC* target_server = nullptr);
	/** @brief Mark the current transaction unsafe for pre-write reader waits. */
	void polardb_note_txn_non_read_committed(const char* reason);
	/** @brief Mark primary-only transaction-local state for pre-write reads. */
	void polardb_note_txn_local_state_change(const char* reason);
	/** @brief True when the tracked transaction isolation is READ COMMITTED. */
	bool polardb_txn_wait_uses_read_committed() const;

	// ---- PolarDB wait wrapping and notices ----

	/** @brief Whether an LSN wait is prepared for the in-flight query. */
	inline bool polardb_wait_active() const {
		return polardb_query.wait.wait_stage == PolarDB_WaitStage::WAITING;
	}
	/** @brief Whether the current query is running as a transaction split read. */
	inline bool polardb_txn_split_read_active() const {
		return polardb_txn_reader.split_active;
	}
	/** @brief Whether mybe is temporarily swapped to a transaction reader. */
	inline bool polardb_txn_reader_read_active() const {
		return polardb_txn_reader.active();
	}
	/**
	 * @brief Return the `SET polar_consistency_mode = '...'` statement for a wait mode.
	 *
	 * This is the first SET in the wait wrapper; it selects what the replica does
	 * when the wait times out (best_effort returns stale data with a WARNING,
	 * strict raises an ERROR). The two statements are fixed text, so this returns a
	 * reference to a long-lived static string. @p wait_mode is the timeout-behavior
	 * knob, not the routing consistency mode. Do not free or modify the result.
	 */
	const std::string& polardb_wait_mode_set_statement(PolarDB_WaitMode wait_mode);
	/**
	 * @brief Append the wait wrapper (mode + timeout + wait SETs, then the user
	 *        query) to the end of @p out.
	 *
	 * @p out is appended to, never cleared first, so a caller may place its own
	 * prefix statements in the buffer beforehand. Emptiness of @p out therefore says
	 * nothing about success; use the return value. On failure @p out is unchanged.
	 *
	 * On a 0 return the caller must not run the read unwrapped on a replica.
	 *
	 * @param orig_query Original user query text.
	 * @param orig_len   Length of @p orig_query in bytes.
	 * @param wait_state Wait state for this query (wait type, LSN target, timeout).
	 * @param prefix The consistency-mode SET statement to place first, from
	 *               polardb_wait_mode_set_statement(). The timeout and wait SETs
	 *               are appended after it, then the user query.
	 * @param out    Buffer the wrapper is appended to.
	 * @return Number of wrapper SET results to skip, or 0 if no wrapper was built.
	 */
	uint32_t append_wrapped_wait_query(const char* orig_query, size_t orig_len,
		const PolarDB_Query_WaitState& wait_state, const std::string& prefix, std::string& out);
	/**
	 * @brief Build the wait wrapper as the whole content of @p out.
	 *
	 * Clear @p out, then delegate to append_wrapped_wait_query() with the same
	 * arguments. Because the buffer starts empty, for this variant only an empty
	 * @p out on return means no wrapper was produced, matching the 0 return.
	 *
	 * @param orig_query Original user query text.
	 * @param orig_len   Length of @p orig_query in bytes.
	 * @param wait_state Wait state for this query (wait type, LSN target, timeout).
	 * @param prefix     Consistency-mode SET statement placed first.
	 * @param out        Buffer that receives the complete wrapped query.
	 * @return Number of wrapper SET results to skip, or 0 if no wrapper was built.
	 */
	uint32_t build_wrapped_wait_query(const char* orig_query, size_t orig_len,
		const PolarDB_Query_WaitState& wait_state, const std::string& prefix, std::string& out);
	/**
	 * @brief Build the wrapped wait query and install it into the outgoing packet.
	 *
	 * The single point where the wrapper is applied, called once at ASYNC_IDLE
	 * after the backend connection exists. Idempotent (a second call after success
	 * is a no-op). If a needed wrapper cannot be built it records that later reads
	 * in this session must use the writer and returns FAILED, and the caller must
	 * abort the query rather than send it unwrapped.
	 */
	PolarDB_WrapFinalizeResult polardb_install_wait_wrapper(
		PgSQL_Connection* conn, PgSQL_Data_Stream* myds);
	/**
	 * @brief Finish reader selection for an ordinary consistency read.
	 *
	 * A reader whose fresh cached LSN already reached @p wait_spec is ready for
	 * direct dispatch: record the satisfied target and leave the wrapper inactive.
	 * Otherwise activate the wait wrapper immediately before the selected
	 * connection continues through backend setup.
	 *
	 * @return true when a real wait wrapper was activated; false for direct
	 *         dispatch or for a request with no wait target.
	 */
	bool polardb_finish_reader_wait_selection(
		const PolarDB_WaitSpec& wait_spec, bool target_reached,
		int fallback_writer_hg);

	/**
	 * @brief Finish a wait, update the reader LSN after success, and stop timing.
	 *
	 * Pass the backend stream only after a successful request. Pass null after a
	 * failure or timeout.
	 */
	void polardb_finish_wait(PgSQL_Data_Stream* myds);
#if POLARDB_PROFILE
	void polardb_profile_prepare_wait(
		const PolarDB_Query_RoutePlan& plan,
		const PolarDB_Query_RouteCtx& route_ctx,
		PolarDB_WaitProfileContext context);
	void polardb_profile_note_reader_selection(
		const PolarDB_WaitSpec& wait_spec,
		const PolarDB_ReaderResult& result);
	void polardb_profile_note_reader_connection(
		const PolarDB_WaitSpec& wait_spec,
		const PolarDB_Query_ReaderPlan& reader_plan,
		PgSQL_Connection* conn);
	void polardb_profile_note_wait_dispatched(
		PolarDB_Query_WrapperKind wrapper_kind);
	void polardb_profile_note_wait_set_completed(
		PolarDB_Query_WrapperKind wrapper_kind);
	void polardb_profile_record_wait_completion(
		unsigned long long fallback_elapsed_us);
#endif // POLARDB_PROFILE
	/**
	 * @brief Count one confirmed PolarDB wait timeout and its elapsed latency.
	 *
	 * Precondition: the caller has already confirmed the backend event is a PolarDB
	 * proxy wait timeout (for example by matching the timeout detail marker).
	 * Consumes the wait start timer through polardb_finish_wait(), so a repeated
	 * observation of the same event does not double-count. Returns false (no count)
	 * if no wait is active or the timer was already consumed.
	 */
	bool polardb_account_wait_timeout(const char* source);
	/**
	 * @brief Count one confirmed transaction-split LSN wait timeout.
	 *
	 * Used from the common NoticeResponse handler after it has matched the
	 * structured PolarDB wait-timeout detail marker. Split waits keep separate
	 * counters from autocommit/session waits, but share the same notice path.
	 */
	bool polardb_account_txn_split_wait_timeout(const char* source);

	/** @brief Free all queued notice packets. */
	void discard_pending_notices();
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
	/** @brief Build and enqueue a NoticeResponse packet; return its wire size. */
	unsigned int polardb_enqueue_notice_packet(const char* severity, const char* sqlstate,
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
	 * Covers RESET / RESET ALL / DISCARD ALL / RESET CONNECTION. Clears the
	 * per-query wait, notices, and wrapper state, and lifts the wait-disabled
	 * sticky flag. It deliberately does NOT clear the durable session write/observed
	 * LSNs: a RESET clears session settings, not the fact that the client has
	 * written, so read-your-writes must survive it. @p reset_override additionally
	 * clears per-session PolarDB overrides.
	 */
	void polardb_clear_staged_wait_state_for_reset(bool reset_override);
	/**
	 * @brief Mark this session as waiting for reader-pool capacity.
	 *
	 * Idempotent per scope. The first call starts the wait timer, counts the enter,
	 * and registers a reader-pool reservation for this session on PgSQL_Thread. A
	 * later call while the wait is still active only updates the scope hash and, if
	 * the scope changed, reports that change to the thread; it neither restarts the
	 * timer nor counts a second enter.
	 *
	 * The four reservation arguments are retained only when @p status is
	 * READER_GROUP_BUSY and all of them are usable — a non-null
	 * @p reservation_server, a held @p reservation_server_snapshot, a non-zero
	 * @p reservation_profile_generation, and a non-empty @p reservation_pool_key.
	 * Any other combination is ignored silently. The snapshot is what keeps
	 * @p reservation_server alive for the duration of the wait.
	 *
	 * Every enter must be matched by polardb_leave_reader_capacity_wait(); without
	 * it the thread-side reservation stays registered.
	 *
	 * @param scope_hash                      Hash identifying the reader pool scope
	 *                                        being waited on.
	 * @param status                          Reader acquisition status that caused
	 *                                        the wait; recorded as the last status.
	 * @param reservation_server              Server the reservation is held against.
	 * @param reservation_server_snapshot     Server-list snapshot that keeps
	 *                                        @p reservation_server alive.
	 * @param reservation_profile_generation  Generation of the reader profile the
	 *                                        reservation was made under.
	 * @param reservation_pool_key            Pool key the reservation applies to;
	 *                                        copied, not retained by pointer.
	 */
	void polardb_enter_reader_capacity_wait(
		uint64_t scope_hash, PolarDB_ReaderStatus status,
		PgSQL_SrvC* reservation_server = nullptr,
		std::shared_ptr<const void> reservation_server_snapshot = nullptr,
		uint32_t reservation_profile_generation = 0,
		const PolarDB_PoolKey* reservation_pool_key = nullptr,
		PolarDB_PoolCapacityTarget target =
			PolarDB_PoolCapacityTarget::READER,
		const PolarDB_Query_ReaderPlan* reservation_plan = nullptr,
		const PolarDB_WaitSpec* reservation_wait_spec = nullptr);
	/**
	 * @brief End a reader-pool capacity wait and release its thread-side reservation.
	 *
	 * Always cancels this session's reader-pool reservation on PgSQL_Thread, even
	 * when no wait is active, so it is safe to call unconditionally — callers such
	 * as polardb_clear_staged_wait_state_for_reset() rely on that. Capacity-wait
	 * accounting (exit count, elapsed sum and maximum, and the latency buckets) is
	 * recorded only for a wait that was active.
	 *
	 * On return the reservation server snapshot is released, the reservation fields
	 * are cleared, the wait is inactive, and @p status is stored as the last reader
	 * status with the result marked valid.
	 *
	 * @param status  Reader status to record as the outcome of this wait.
	 */
	void polardb_leave_reader_capacity_wait(PolarDB_ReaderStatus status);
#endif // POLARDB_PROXY

private:
#if POLARDB_PROXY
	/** @brief Clear notice metadata after flush transfers packet ownership. */
	void forget_transferred_notices();

	struct PolarDB_ResultProcessContext {
		PgSQL_Data_Stream* myds = nullptr;
		const char* query_digest_text = nullptr;
		PGSQL_QUERY_command query_cmd = PGSQL_QUERY___NONE;
		uint64_t lsn = 0;
		bool is_write = false;
		PgSQL_SrvC* backend_srv = nullptr;
		int backend_hg = -1;
		bool backend_is_polar_hg = false;
		int backend_writer_hg = -1;
		uint64_t backend_writer_epoch = 0;
		bool lsn_consistency_mode = false;
		bool split_observation_enabled = false;
	};

	/**
	 * @brief Tear down all PolarDB state belonging to the frontend session.
	 *
	 * The full teardown variant, used when the frontend session itself is being
	 * recycled rather than when a client sends RESET. On top of what
	 * polardb_clear_staged_wait_state_for_reset() does — leaving any reader capacity
	 * wait, resetting per-query state, and freeing pending notices — it also clears
	 * the no-write-XIDs transaction flag, clears the session route identity
	 * (dropping the negotiated client RFQ-LSN capability and the cached
	 * startup-identity hash, both of which the staged variant deliberately keeps),
	 * and destroys rather than pools any temporary transaction-split backend.
	 *
	 * Use polardb_clear_staged_wait_state_for_reset() for RESET / DISCARD ALL from a
	 * client that keeps its connection.
	 */
	void polardb_clear_session_state_for_recycle();
	void polardb_clear_request_state_for_query_end(
		PgSQL_Data_Stream* myds, bool called_on_failure);
	/**
	 * @brief Fold a backend RFQ LSN into the per-server cache and report acceptance.
	 *
	 * An RFQ LSN is only meaningful for the writer group and writer epoch the
	 * request ran under. A failover or a route into another PolarDB group makes an
	 * otherwise well-formed LSN name a position on a different timeline, so the
	 * per-server update is what determines acceptance and this return carries that
	 * result to the caller.
	 *
	 * @param ctx  Result-processing context for the finished query, holding the
	 *             backend server, hostgroup, and the RFQ LSN.
	 * @return true when the LSN was accepted for the current writer group and epoch
	 *         and folded into the per-server cache. false when the backend server or
	 *         hostgroup is unknown, or the update rejected the LSN as belonging to
	 *         another writer group or an older writer epoch — the caller must then
	 *         skip every session-LSN update for this result, or it imports a
	 *         stale-timeline position into the session.
	 */
	bool polardb_process_positioned_rfq_lsn(
		PolarDB_ResultProcessContext& ctx);
	void polardb_process_zero_rfq_lsn(
		PolarDB_ResultProcessContext& ctx);
	void polardb_process_missing_rfq_lsn(
		PolarDB_ResultProcessContext& ctx);
	/**
	 * @brief Backend used by the request currently being processed.
	 *
	 * Split execution temporarily swaps mybe to the temporary replica backend, so
	 * the active backend is still mybe. Keeping this accessor centralizes later
	 * full-retry/failure work.
	 */
	PgSQL_Backend* active_backend() const { return mybe; }
	/** @brief Prepare one transaction split read on a replica backend. */
	bool polardb_prepare_txn_split_read(const PolarDB_Query_RoutePlan& plan,
		const PolarDB_WriterScope& writer_scope, PtrSize_t& pkt);
	/** @brief Prepare one pre-write transaction consistency read on a replica backend. */
	bool polardb_prepare_txn_wait_read(const PolarDB_Query_RoutePlan& plan,
		const PolarDB_Query_RouteCtx& route_ctx, PtrSize_t& pkt);
	/** @brief Start using a reader backend for one transaction reader request. */
	void polardb_begin_txn_reader_read(PgSQL_Backend* reader_backend,
		PgSQL_Data_Stream* reader_myds,
		const PolarDB_WriterScope& writer_scope);
	/** @brief Install the packet for one pre-write transaction wait read. */
	void polardb_begin_txn_wait_read(PgSQL_Backend* reader_backend,
		PgSQL_Data_Stream* reader_myds, PtrSize_t& pkt,
		const PolarDB_WriterScope& writer_scope);
	/** @brief Install the wrapper for one transaction split read. */
	void polardb_begin_txn_split_read(PgSQL_Backend* reader_backend,
		PgSQL_Data_Stream* reader_myds, PtrSize_t& pkt,
		std::string&& wrapped_query, const PolarDB_WaitSpec& wait_spec,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WriterScope& writer_scope,
		uint32_t wrapper_stmts, bool wait_bypassed);
	/** @brief Complete a successful split read and restore the primary backend. */
	void polardb_complete_txn_split_read();
	/** @brief Complete a successful pre-write transaction wait read. */
	void polardb_complete_txn_wait_read();
	/** @brief Abort a failed split read and keep later reads on the primary. */
	void polardb_abort_txn_split_read(const char* reason);
	/** @brief Finish a transaction reader request and restore normal session state. */
	void polardb_finish_txn_reader_read(bool split_success,
		bool split_error, const char* reason,
		bool record_split_error_counter, bool mark_reader_not_reusable);
	/** @brief Release a failed pre-write transaction wait read and restore primary. */
	void polardb_release_txn_wait_read(bool want_reuse);
	/** @brief Reconcile a terminal txn-wait reader path and restore primary. */
	void polardb_reconcile_txn_wait_read_end(
		const char* reason, bool want_reuse);
	/** @brief Restore primary if a previous request still owns a txn-wait reader. */
	void polardb_reconcile_txn_wait_read_request_entry(const char* reason);
	/** @brief Clear temporary split-read buffers and restore mybe. */
	void polardb_reset_txn_reader_request();
	/** @brief Return/destroy the temporary split replica connection. */
	void polardb_release_txn_reader_backend(bool want_reuse);
	/** @brief Add the current split read's elapsed time to split latency counters. */
	void polardb_record_txn_split_latency();
	/** @brief Add the current split wait wrapper's elapsed time to wait buckets. */
	void polardb_record_txn_split_wait_latency();
	/** @brief Copy failed backend state before normal rc==-1 error handling changes it. */
	PolarDB_RequestOutcome polardb_capture_request_outcome(PgSQL_Backend* backend);
	/** @brief Classify a PolarDB rc==-1 replica-reader failure and apply its
	 *         retry/forward/terminate policy before generic retries. */
	PolarDB_FailureAction polardb_handle_reader_failure(const PolarDB_RequestOutcome& outcome);

	enum class PolarDB_ReaderRequestKind : uint8_t {
		NONE = 0,
		ORDINARY,
		WAIT,
		SPLIT
	};

	/**
	 * @brief Captured state for one failed PolarDB replica read.
	 *
	 * Built before the generic error path can release/destroy the backend or
	 * reset request-local state. Transaction split moves the original wire packet
	 * into retry_pkt; wait-read keeps retry_query instead because the installed
	 * packet contains internal wrapper SET statements and must be discarded.
	 */
	struct PolarDB_ReaderFailure {
		PolarDB_ReaderRequestKind request_kind =
			PolarDB_ReaderRequestKind::NONE;
		bool extended_query = false;
		bool wait_was_bypassed = false;
		bool timeout = false;
		bool reusable = false;
		bool result_started = false;
		bool connected = false;
		bool has_backend_error = false;
		bool timeout_already_accounted = false;
		bool wrapper_set_failure = false;
		bool wrapper_is_consistency_wait = false;
		bool can_return_to_pool = false;
		int reader_hg = -1;
		std::string reader_address;
		int reader_port = 0;
		int fallback_writer_hg = -1;
		PgSQL_Backend* reader_backend = nullptr;
		PgSQL_Data_Stream* failed_myds = nullptr;
		PtrSize_t retry_pkt{0, nullptr};
		PolarDB_Query_ReaderPlan reader_plan;
		PolarDB_WaitSpec wait_spec;
		PolarDB_WriterScope writer_scope;
		std::string txn_xids;
		std::string retry_query;
		PGSQL_ERROR_CODES backend_error_code =
			PGSQL_ERROR_CODES::ERRCODE_CONNECTION_FAILURE;
		std::string backend_error_message;

		bool is_ordinary() const {
			return request_kind == PolarDB_ReaderRequestKind::ORDINARY;
		}
		bool is_wait() const {
			return request_kind == PolarDB_ReaderRequestKind::WAIT;
		}
		bool is_split() const {
			return request_kind == PolarDB_ReaderRequestKind::SPLIT;
		}
	};

	/** @brief Policy decision for a captured replica-reader failure. */
	struct PolarDB_ReaderFailureDecision {
		PolarDB_ReaderFailureKind kind =
			PolarDB_ReaderFailureKind::REUSABLE_ERROR;
		PolarDB_ReaderAction action =
			PolarDB_ReaderAction::RETURN_ERROR;
		PolarDB_RetryTarget retry_target = PolarDB_RetryTarget::WRITER;
		PolarDB_ReaderFailureRoute reader_failure_route = PolarDB_ReaderFailureRoute::NONE;
		bool allow_writer_retry = true;
	};

	/** @brief Copy captured backend fields into the common failure record. */
	static PolarDB_ReaderFailure polardb_failure_from_outcome(
		const PolarDB_RequestOutcome& outcome);
	/** @brief Build the failure record and take its retry packet when needed. */
	PolarDB_ReaderFailure polardb_take_reader_failure(
		const PolarDB_RequestOutcome& outcome);
	/** @brief Resolve writer state and, for LIVE, return its backend. */
	PolarDB_WriterState polardb_resolve_writer_state(
		const PolarDB_ReaderFailure& failure, int& writer_hg,
		PgSQL_Backend*& writer_backend);
	/** @brief Find the expected backend when it has an open transaction. */
	PgSQL_Backend* polardb_find_open_transaction_backend(
		int expected_writer_hg, PgSQL_Backend* exclude_reader);
	/** @brief Writer hostgroup used when the writer backend has not started yet. */
	int polardb_writer_hostgroup_or_current();
	/** @brief Classify a reader failure before applying operator policy knobs. */
	PolarDB_ReaderFailureKind polardb_reader_failure_kind_for(
		const PolarDB_ReaderFailure& failure);
	/** @brief Pick retry/forward/terminate policy for this failure class. */
	PolarDB_ReaderFailureDecision polardb_reader_failure_decision(
		const PolarDB_ReaderFailure& failure);
	/** @brief Apply transaction-scoped routing selected by failure policy. */
	void polardb_apply_reader_failure_route_state(
		PolarDB_ReaderFailureRoute route, int writer_hg,
		const PolarDB_ReaderFailure* failure = nullptr);
	/**
	 * @brief Honor a session hostgroup lock by skipping automatic PolarDB routing.
	 *
	 * @param stage  Trace-only name of the call site.
	 * @param require_current_match When true, the lock is applied only if
	 *        current_hostgroup already equals locked_on_hostgroup. On a mismatch it
	 *        stops automatic routing but changes nothing, leaving the core
	 *        locked-hostgroup check to report the mismatch. The extended-protocol
	 *        path uses this.
	 * @return true when automatic PolarDB routing must be skipped. That does NOT
	 *         imply the route was applied: with @p require_current_match set and the
	 *         hostgroups differing, current_hostgroup is left alone, no writer scope
	 *         is captured, and no manual route is counted. In every other true case
	 *         current_hostgroup is set to the locked hostgroup, its writer scope is
	 *         captured, and the manual route is counted. false means no hostgroup
	 *         lock is in effect and the caller routes normally.
	 */
	bool polardb_handle_locked_hostgroup_route(
		const char* stage, bool require_current_match = false);
	/** @brief Redispatch the original client query on an existing live writer. */
	bool polardb_try_redispatch_to_writer(PolarDB_ReaderFailure& failure,
		int writer_hg, PgSQL_Backend* writer_backend);
	/** @brief Redispatch the original client query on another split reader. */
	bool polardb_try_redispatch_to_other_reader(PolarDB_ReaderFailure& failure);
	/** @brief Redispatch an ordinary or wait-wrapped query on another reader. */
	bool polardb_try_redispatch_reader_read_to_other_reader(
		PolarDB_ReaderFailure& failure);
	/** @brief Restart query dispatch from an owned simple-query packet. */
	void polardb_restart_query_dispatch_from_packet(const PtrSize_t& pkt);
	/** @brief Keep extended-query metadata but resolve its statement on the new backend. */
	void polardb_prepare_extended_retry();
	/** @brief Move request retry state from an abandoned backend to its retry target. */
	void polardb_prepare_retry_backend(
		PgSQL_Data_Stream* source_myds, PgSQL_Data_Stream* target_myds);
	/** @brief Move a retry packet to the writer stream and make that writer active. */
	bool polardb_move_retry_packet_to_writer(PgSQL_Data_Stream* source_myds,
		PgSQL_Backend* writer_backend, int writer_hg, PtrSize_t& retry_pkt,
		bool extended_query = false);
	/** @brief Return or destroy a backend stream after caller-specific cleanup. */
	void polardb_return_or_destroy_backend_stream(PgSQL_Data_Stream* myds,
		bool return_to_pool);
	/** @brief Release a failed ordinary or wait-wrapped reader stream. */
	void polardb_release_reader_stream(PgSQL_Data_Stream* failed_myds,
		bool can_return_to_pool);
	/** @brief Build the XID + strict LSN wait wrapper for transaction split. */
	uint32_t polardb_build_txn_split_wrapped_query(const PtrSize_t& pkt,
		const PolarDB_WaitSpec& wait_spec, std::string_view txn_xids,
		bool bypass_wait, std::string& wrapped_query);
	/** @brief Forward captured reader error and keep the transaction on writer. */
	PolarDB_FailureAction polardb_forward_error_and_keep_writer(
		PolarDB_ReaderFailure& failure);
	/** @brief Emit ErrorResponse plus ReadyForQuery with explicit txn status. */
	void polardb_forward_reader_error(const PolarDB_ReaderFailure& failure,
		char rfq);
	/** @brief Close the session after releasing the failed reader. */
	PolarDB_FailureAction polardb_terminate_session_after_reader_failure(
		PolarDB_ReaderFailure& failure);
	/** @brief Tear down split-read state after a reader failure is resolved. */
	void polardb_end_split_after_reader_failure();
	/** @brief Error accounting and dead-reader marking for resolved failures. */
	void polardb_record_reader_failure(
		const PolarDB_ReaderFailure& failure);
	/** @brief Release or destroy a failed reader backend after results are dropped. */
	void polardb_release_reader_backend(PgSQL_Backend* reader_backend,
		bool want_reuse);
	/** @brief Return an acquired reader that was rejected before query dispatch. */
	void polardb_release_unused_reader_backend(PgSQL_Backend* reader_backend);
	/** @brief Free failure.retry_pkt if it was not installed into a stream. */
	void polardb_free_retry_pkt_if_owned(PolarDB_ReaderFailure& failure);

	/**
	 * @brief Stop safely when the wait wrapper cannot be built or installed.
	 *
	 * Counts the abort, logs @p reason, records that later reads in this session
	 * must use the writer until RESET, and clears the half-built wrapper and wait
	 * state. Always returns PolarDB_WrapFinalizeResult::FAILED; the caller must
	 * stop before running the query and return a clean error.
	 */
	PolarDB_WrapFinalizeResult polardb_fail_wrap_and_disable_session_waits(
		const char* reason);
	/** @brief Add wait-request fields to a reader failure. */
	PolarDB_ReaderFailure polardb_capture_wait_read_failure(
		PolarDB_ReaderFailure failure);
	/** @brief Capture one automatic replica read that has no active wrapper. */
	PolarDB_ReaderFailure polardb_capture_ordinary_reader_failure(
		PolarDB_ReaderFailure failure);
	/**
	 * @brief Retry or finish a failed ordinary or wait-wrapped replica read.
	 *
	 * The common reader-failure policy selects retry/error/disconnect. This
	 * adapter owns packet cleanup and redispatch without creating another
	 * routing policy.
	 */
	PolarDB_FailureAction polardb_handle_failed_reader_read(
		PolarDB_ReaderFailure& failure,
		const PolarDB_ReaderFailureDecision& decision);
	/** @brief Build a PostgreSQL simple-query packet owned by the caller. */
	void polardb_build_simple_query_packet(const std::string& sql, PtrSize_t& out);
	/** @brief Take or rebuild the client packet needed for a reader retry. */
	bool polardb_prepare_reader_retry_packet(PolarDB_ReaderFailure& failure);
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
	friend struct PolarDB_SessionUnitAccess;
};



#endif /* PROXYSQL_PGSQL_SESSION_H */
#endif // CLASS_BASE_SESSION_H
