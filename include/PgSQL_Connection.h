#ifndef PROXYSQL_PGSQL_CONNECTION_H
#define PROXYSQL_PGSQL_CONNECTION_H
#include "libpq-fe.h"
#include "proxysql.h"
#include "cpp.h"
#include "PgSQL_Error_Helper.h"
#if POLARDB_PROXY
#include "PgSQL_PolarDB.h"
#include <atomic>
#include <sstream>  // std::ostringstream for PolarDB startup params
#include <string>
#endif

#ifndef PROXYJSON
#define PROXYJSON
#include "../deps/json/json_fwd.hpp"
#endif // PROXYJSON

class PgSQL_SrvC;
class PgSQL_Query_Result;
class PgSQL_STMT_Local;
//class PgSQL_Describe_Prepared_Info;
class PgSQL_Bind_Info;
//#define STATUS_PGSQL_CONNECTION_SEQUENCE			 0x00000001
#define STATUS_PGSQL_CONNECTION_COMPRESSION          0x00000002
#define STATUS_PGSQL_CONNECTION_USER_VARIABLE        0x00000004
#define STATUS_PGSQL_CONNECTION_PREPARED_STATEMENT   0x00000008
#define STATUS_PGSQL_CONNECTION_LOCK_TABLES          0x00000010
#define STATUS_PGSQL_CONNECTION_TEMPORARY_TABLE      0x00000020
#define STATUS_PGSQL_CONNECTION_ADVISORY_LOCK        0x00000040
#define STATUS_PGSQL_CONNECTION_NO_MULTIPLEX         0x00000080
#define STATUS_PGSQL_CONNECTION_HAS_SEQUENCES		 0x00000100
#define STATUS_PGSQL_CONNECTION_ADVISORY_XACT_LOCK   0x00000200
#define STATUS_PGSQL_CONNECTION_NO_MULTIPLEX_HG      0x00000400
#define STATUS_PGSQL_CONNECTION_HAS_SAVEPOINT        0x00000800
//#define STATUS_PGSQL_CONNECTION_HAS_WARNINGS         0x00001000


#if POLARDB_PROXY
enum class PolarDB_ParentCounterFlushReason : uint8_t {
	THRESHOLD_RECV,
	THRESHOLD_SENT,
	THRESHOLD_QUERIES,
	DESTRUCTOR,
};
#endif // POLARDB_PROXY

enum PgSQL_Param_Name {
	PG_HOST = 0,  // Name of host to connect to
	PG_HOSTADDR,  // Numeric IP address of host to connect to
	PG_PORT,  // Port number to connect to at the server host
	PG_DATABASE,  // The database name
	PG_USER,  // PgSQL user name to connect as
	PG_PASSWORD,  // Password to be used if the server demands password authentication
	PG_PASSFILE,  // Specifies the name of the file used to store passwords
	PG_REQUIRE_AUTH,  // Specifies the authentication method that the client requires from the server
	PG_CHANNEL_BINDING,  // Controls the client's use of channel binding
	PG_CONNECT_TIMEOUT,  // Maximum time to wait while connecting, in seconds
	PG_OPTIONS,  // Specifies command-line options to send to the server at connection start
	PG_APPLICATION_NAME,  // Specifies a value for the application_name configuration parameter
	PG_FALLBACK_APPLICATION_NAME,  // Specifies a fallback value for the application_name configuration parameter
	PG_KEEPALIVES,  // Controls whether client-side TCP keepalives are used
	PG_KEEPALIVES_IDLE,  // Controls the number of seconds of inactivity after which TCP should send a keepalive message to the server
	PG_KEEPALIVES_INTERVAL,  // Controls the number of seconds after which a TCP keepalive message that is not acknowledged by the server should be retransmitted
	PG_KEEPALIVES_COUNT,  // Controls the number of TCP keepalives that can be lost before the client's connection to the server is considered dead
	PG_TCP_USER_TIMEOUT,  // Controls the number of milliseconds that transmitted data may remain unacknowledged before a connection is forcibly closed
	PG_REPLICATION,  // Determines whether the connection should use the replication protocol instead of the normal protocol
	PG_GSSENCMODE,  // Determines whether a secure GSS TCP/IP connection will be negotiated with the server
	PG_SSLMODE,  // Determines whether a secure SSL TCP/IP connection will be negotiated with the server
	PG_REQUIRESSL,  // Requires an SSL connection to the server
	PG_SSLCOMPRESSION,  // If set, data sent over SSL connections will be compressed
	PG_SSLCERT,  // Specifies the file name of the client SSL certificate
	PG_SSLKEY,  // Specifies the location for the secret key used for the client certificate
	PG_SSLPASSWORD,  // Specifies the password for the secret key specified in sslkey
	PG_SSLCERTMODE,  // Determines whether a client certificate may be sent to the server
	PG_SSLROOTCERT,  // Specifies the name of a file containing SSL certificate authority (CA) certificate(s)
	PG_SSLCRL,  // Specifies the file name of the SSL server certificate revocation list (CRL)
	PG_SSLCRLDIR,  // Specifies the directory name of the SSL server certificate revocation list (CRL)
	PG_SSLSNI,  // Sets the TLS extension Server Name Indication (SNI) on SSL-enabled connections
	PG_REQUIREPEER,  // Specifies the operating-system user name of the server
	PG_SSL_MIN_PROTOCOL_VERSION,  // Specifies the minimum SSL/TLS protocol version to allow for the connection
	PG_SSL_MAX_PROTOCOL_VERSION,  // Specifies the maximum SSL/TLS protocol version to allow for the connection
	PG_KRBSRVNAME,  // Kerberos service name to use when authenticating with GSSAPI
	PG_GSSLIB,  // GSS library to use for GSSAPI authentication
	PG_GSSDELEGATION,  // Forward (delegate) GSS credentials to the server
	PG_SERVICE,  // Service name to use for additional parameters
	PG_TARGET_SESSION_ATTRS,  // Determines whether the session must have certain properties to be acceptable
	PG_LOAD_BALANCE_HOSTS,  // Controls the order in which the client tries to connect to the available hosts and addresses
	// Environment Options
	//PG_DATESTYLE,  // Sets the value of the DateStyle parameter
	//PG_TIMEZONE,  // Sets the value of the TimeZone parameter
	//PG_GEQO,  // Enables or disables the use of the GEQO query optimizer

	PG_PARAM_SIZE
};

struct Param_Name_Validation {
	const char** accepted_values;
	int default_value_idx;
};

static const Param_Name_Validation require_auth{ (const char* []) { "password","md5","gss","sspi","scram-sha-256","none",nullptr },-1 };
static const Param_Name_Validation replication{ (const char* []) { "true","on","yes","1","database","false","off","no","0",nullptr },-1 };
static const Param_Name_Validation gsseencmode{ (const char* []) { "disable","prefer","require",nullptr },1 };
static const Param_Name_Validation sslmode{ (const char* []) { "disable","allow","prefer","require","verify-ca","verify-full",nullptr },2 };
static const Param_Name_Validation sslcertmode{ (const char* []) { "disable","allow","require",nullptr },1 };
static const Param_Name_Validation target_session_attrs{ (const char* []) { "any","read-write","read-only","primary","standby","prefer-standby",nullptr },0 };
static const Param_Name_Validation load_balance_hosts{ (const char* []) { "disable","random",nullptr },-1 };

// Excluding client_encoding since it is managed as part of the session variables
#define PARAMETER_LIST \
    PARAM("host", nullptr) \
    PARAM("hostaddr", nullptr) \
    PARAM("port", nullptr) \
    PARAM("database", nullptr) \
    PARAM("user", nullptr) \
    PARAM("password", nullptr) \
    PARAM("passfile", nullptr) \
    PARAM("require_auth", &require_auth) \
    PARAM("channel_binding", nullptr) \
    PARAM("connect_timeout", nullptr) \
    PARAM("options", nullptr) \
    PARAM("application_name", nullptr) \
    PARAM("fallback_application_name", nullptr) \
    PARAM("keepalives", nullptr) \
    PARAM("keepalives_idle", nullptr) \
    PARAM("keepalives_interval", nullptr) \
    PARAM("keepalives_count", nullptr) \
    PARAM("tcp_user_timeout", nullptr) \
    PARAM("replication", &replication) \
    PARAM("gsseencmode", &gsseencmode) \
    PARAM("sslmode", &sslmode) \
    PARAM("requiressl", nullptr) \
    PARAM("sslcompression", nullptr) \
    PARAM("sslcert", nullptr) \
    PARAM("sslkey", nullptr) \
    PARAM("sslpassword", nullptr) \
    PARAM("sslcertmode", &sslcertmode) \
    PARAM("sslrootcert", nullptr) \
    PARAM("sslcrl", nullptr) \
    PARAM("sslcrldir", nullptr) \
    PARAM("sslsni", nullptr) \
    PARAM("requirepeer", nullptr) \
    PARAM("ssl_min_protocol_version", nullptr) \
    PARAM("ssl_max_protocol_version", nullptr) \
    PARAM("krbsrvname", nullptr) \
    PARAM("gsslib", nullptr) \
    PARAM("gssdelegation", nullptr) \
    PARAM("service", nullptr) \
    PARAM("target_session_attrs", &target_session_attrs) \
    PARAM("load_balance_hosts", &load_balance_hosts)

// Generate parameter array
constexpr const char* param_name[] = {
#define PARAM(name, val) name,
	PARAMETER_LIST
#undef PARAM
};

// make sure all the keys are in lower case
static const std::unordered_map<std::string_view, const Param_Name_Validation*> param_name_map = {
#define PARAM(name, val) {name, val},
	PARAMETER_LIST
#undef PARAM
};

#define PG_EVENT_NONE	 0x00
#define PG_EVENT_READ	 0x01
#define PG_EVENT_WRITE	 0x02
#define PG_EVENT_EXCEPT  0x04
#define PG_EVENT_TIMEOUT 0x08


/**
 * @class PgSQL_Conn_Param
 * @brief Stores PostgreSQL connection parameters sent by client.
 *
 * This class stores key-value pairs representing connection parameters 
 * for PostgreSQL connection. 
 *
 */
class PgSQL_Conn_Param {
public:
    PgSQL_Conn_Param() {}
    ~PgSQL_Conn_Param() {}

    bool set_value(const char* key, const char* val) {
        if (key == nullptr || val == nullptr) return false;
        connection_parameters[key] = val;
        return true;
    }

    inline
    const char* get_value(PgSQL_Param_Name key) const {
        return get_value(param_name[key]);
    }

    const char* get_value(const char* key) const {
        auto it = connection_parameters.find(key);
        if (it != connection_parameters.end()) {
            return it->second.c_str();
        }
        return nullptr;
    }

    bool remove_value(const char* key) {
        auto it = connection_parameters.find(key);
        if (it != connection_parameters.end()) {
            connection_parameters.erase(it);
            return true;
        }
        return false;
    }

    inline
    bool is_empty() const {
        return connection_parameters.empty();
    }

    inline
    void clear() {
        connection_parameters.clear();
    }

private:
    /**
     * @brief Stores the connection parameters as key-value pairs.
     */
    std::map<std::string, std::string> connection_parameters;

    friend class PgSQL_Session; 
    friend class PgSQL_Protocol;
};

class PgSQL_Variable {
public:
	char *value = nullptr;
	void fill_server_internal_session(nlohmann::json &j, int conn_num, int idx);
	void fill_client_internal_session(nlohmann::json &j, int idx);
};

class PgSQL_Connection_userinfo {
	private:
	uint64_t compute_hash();
  public:
	uint64_t hash;
	char *username;
	char *password;
	union {
		char* dbname;
		char* schemaname; // temporary fix. To avoid changes in Base_Session and Query_Processor
	};
	char *sha1_pass;
	char *fe_username;
	// TODO POSGRESQL: add client and server scram keys
	PgSQL_Connection_userinfo();
	~PgSQL_Connection_userinfo();
	void set(char *, char *, char *, char *);
	void set(PgSQL_Connection_userinfo *);
	bool set_dbname(const char *);
};

class PgSQL_Connection {
public:
	explicit PgSQL_Connection(bool is_client_conn);
	~PgSQL_Connection();

	PG_ASYNC_ST handler(short event);
	void connect_start();
	void connect_cont(short event);
	// Consults PgSQL_Monitor::dns_lookup; returns the cached IP on a hit,
	// empty std::string on a miss.  Used by connect_start() to set
	// `hostaddr=<ip>` in the libpq conninfo so PQconnectStart skips
	// getaddrinfo and doesn't block the worker thread on DNS.
	std::string connect_start_DNS_lookup();
	void query_start();
	void query_cont(short event);
	void fetch_result_start();
	void fetch_result_cont(short event);

    /**
     * @brief Initiates the asynchronous preparation of a SQL statement.
     *
     * This method starts the process of preparing a SQL statement on the PostgreSQL backend.
     *
     * The actual continuation and completion of the statement preparation is handled
     * by stmt_prepare_cont(short event).
     */
    void stmt_prepare_start();

    /**
     * @brief Continues the asynchronous preparation of a SQL statement.
     *
     * This method is called after stmt_prepare_start() to handle the next step in the
     * asynchronous state machine for preparing a SQL statement on the PostgreSQL backend.
     *
     * @param event The event flag indicating the current I/O event.
     */
    void stmt_prepare_cont(short event);

    /**
     * @brief Initiates the asynchronous description of a prepared SQL statement.
     *
     * This method starts the process of describing a previously prepared SQL statement
     * on the PostgreSQL backend.
	 * 
     */
    void stmt_describe_start();

    /**
     * @brief Continues the asynchronous description of a prepared SQL statement.
     *
     * This method is called after stmt_describe_start() to handle the next step in the
     * asynchronous state machine for describing a prepared SQL statement on the PostgreSQL backend.
     *
     * @param event The event flag indicating the current I/O event.
     */
    void stmt_describe_cont(short event);

    /**
     * @brief Initiates the asynchronous execution of a prepared SQL statement.
     *
     * This method starts the process of executing a previously prepared SQL statement
     * on the PostgreSQL backend. It sends Bind and Execute messages to the server
     * and transitions the connection's state machine to handle the subsequent response.
	 * 
     */
    void stmt_execute_start();

    /**
     * @brief Continues the asynchronous execution of a prepared SQL statement.
     *
     * This method is called after stmt_execute_start() to handle the next step in the
     * asynchronous state machine for executing a prepared SQL statement on the PostgreSQL backend.
     *
     * @param returned The event flag indicating the current I/O event.
     */
    void stmt_execute_cont(short event);

    /**
     * @brief Initiates the asynchronous reset of the PostgreSQL connection.
     *
     * Starts the internal state machine that resets connection to a clean,
     * reusable state so it can safely re-enter the multiplexing pool.
     *
     */
    void reset_session_start();

    /**
     * @brief Continues the asynchronous reset of the PostgreSQL session.
     *
     * This method advances the state machine initiated by reset_session_start()
     * to asynchronously reset the backend connection to a clean, reusable state.
     *
     * @param event The event flag indicating the current I/O event.
     */
    void reset_session_cont(short event);

    /**
     * @brief Start a resynchronization attempt for the current backend connection.
     *
     * Send protocol-level Sync (or otherwise trigger the backend to reach
     * ReadyForQuery) and transition the connection into the resynchronizing state.
     *
     */
    void resync_start();

    /**
	 * @brief Continue a previously started resynchronization in response to an event.
	 *
	 * @param event The event flag indicating the current I/O event.
	 */
    void resync_cont(short event);
	
	int async_connect(short event);
	int async_query(short event, const char* stmt, unsigned long length, const char* backend_stmt_name = nullptr, 
		PgSQL_Extended_Query_Type type = PGSQL_EXTENDED_QUERY_TYPE_NOT_SET, const PgSQL_Extended_Query_Info* extended_query_info = nullptr);
	int async_ping(short event);
	int async_reset_session(short event);
	int async_send_simple_command(short event, char* stmt, unsigned long length); // no result set expected
	int async_perform_resync(short event);

	void next_event(PG_ASYNC_ST new_st);
	bool is_connected() const;
	void compute_unknown_transaction_status();
	void async_free_result();
	void flush(bool is_resync = false);
	bool IsActiveTransaction();
	bool IsKnownActiveTransaction();
	bool IsServerOffline();
	void set_is_client(); // used for local_stmts
	bool is_connection_in_reusable_state() const;

	bool requires_RESETTING_CONNECTION(const PgSQL_Connection* client_conn);
	
	bool has_same_connection_options(const PgSQL_Connection* c);

	/**
	 * @brief Sets the error information for this connection from the current libpq error message.
	 *
	 * This method retrieves the latest error message from the underlying PostgreSQL connection (via PQerrorMessage),
	 * parses it into its component fields (such as severity, SQLSTATE, and message), and fills the internal error_info
	 * structure accordingly. If the error message is not available, it sets a generic "Unknown error" with fatal severity.
	 *
	 * The function distinguishes between server errors (with fields like "S", "C", "M") and library-generated errors
	 * (stored under the "LE" key). If a server error is present, it is preferred; otherwise, the library error message
	 * is used. The error fields are extracted using parse_pq_error_message().
	 *
	 * Example error string: "S:5:ERRORC:5:12345M:12:Some message"
	 *   - S: Severity
	 *   - C: SQLSTATE code
	 *   - M: Primary message
	 *   - LE: Library error (if present)
	 */
	void set_error_from_PQerrorMessage();

	int get_server_version() {
		return PQserverVersion(pgsql_conn);
	}

	int get_protocol_version() {
		return PQprotocolVersion(pgsql_conn);
	}

	inline
	bool is_error_present() const {
		if (error_info.severity == PGSQL_ERROR_SEVERITY::ERRSEVERITY_FATAL ||
			error_info.severity == PGSQL_ERROR_SEVERITY::ERRSEVERITY_ERROR ||
			error_info.severity == PGSQL_ERROR_SEVERITY::ERRSEVERITY_PANIC) {
			return true;
		}
		return false;
	}

	inline
	PGSQL_ERROR_SEVERITY get_error_severity() const {
		return error_info.severity;
	}

	inline
	PGSQL_ERROR_CATEGORY get_error_category() const {
		return error_info.category;
	}

	inline
	const std::string& get_error_message() const {
		return error_info.message;
	}

	inline
	const char* get_error_code_str() const {
		return error_info.sqlstate;
	}

	inline
	PGSQL_ERROR_CODES get_error_code() const {
		return error_info.code;
	}

	inline
	std::string get_error_code_with_message() const {
		return ("[" + std::string(error_info.sqlstate) + "] " + error_info.message);
	}

	void set_error(const char* code, const char* message, bool is_fatal) {
		PgSQL_Error_Helper::fill_error_info(error_info, code, message, is_fatal ? "FATAL" : "ERROR");
	}

	void set_error(PGSQL_ERROR_CODES code, const char* message, bool is_fatal) {
		PgSQL_Error_Helper::fill_error_info(error_info, code, message, is_fatal ? 
			PGSQL_ERROR_SEVERITY::ERRSEVERITY_FATAL : PGSQL_ERROR_SEVERITY::ERRSEVERITY_ERROR);
	}

	// safety check. Sometimes libpq return garbage result when connection is lost with the backend
	bool is_error_result_valid(const PGresult* result) const {
		if (result == nullptr)
			return false;
		return (PQresultErrorField(result, PG_DIAG_SQLSTATE) != nullptr);
	}

	void set_error_from_result(const PGresult* result, uint16_t ext_fields = 0) {
		if (is_error_result_valid(result)) { 
			PgSQL_Error_Helper::fill_error_info(error_info, result, ext_fields);
		} else {
			set_error_from_PQerrorMessage();
		}
	}

	void reset_error() { reset_error_info(error_info, false); }

	bool reset_session_in_txn = false;
	bool reset_session_in_pipeline = false;

	PGresult* get_result();
	void next_multi_statement_result(PGresult* result);
	bool set_single_row_mode();
#if POLARDB_PROXY
	/**
	 * @brief Count one query against this connection's deferred query counter.
	 *
	 * The parent PgSQL_SrvC counter is not touched per query. The count is kept in
	 * connection-local pending state and merged into the parent only once every
	 * POLARDB_PARENT_QUERIES_FLUSH_THRESHOLD queries, or by an explicit flush, so
	 * server statistics report a parent queries_sent that lags by up to that many
	 * queries on each open connection.
	 */
	void polardb_note_query_sent();
	/**
	 * @brief Merge only the deferred queries_sent count into the parent PgSQL_SrvC.
	 *
	 * The deferred byte counters are deliberately left alone — use
	 * polardb_flush_parent_counters() for those. The two are the pair used when a
	 * connection is detached from its data stream.
	 *
	 * Call this while `parent` is still valid: with a null parent the pending query
	 * count is cleared anyway, so those queries are lost rather than deferred.
	 */
	void polardb_flush_parent_queries();
	/**
	 * @brief Merge every deferred byte and query counter into the parent PgSQL_SrvC.
	 *
	 * Adds the pending bytes_recv, bytes_sent, and queries_sent to the parent with
	 * atomic adds, zeroes the pending values, and restarts the query batch count.
	 * Runs implicitly from update_bytes_recv() / update_bytes_sent() once the
	 * deferred byte total crosses its threshold, from polardb_note_query_sent() on its
	 * query threshold, and from the destructor — so no caller may assume the parent
	 * counters are current at any other moment.
	 *
	 * With a null parent all pending counts are zeroed and their contribution is
	 * lost.
	 *
	 * @param reason  Selects which flush-attribution counter is bumped and appears in
	 *                trace output; it does not change what is flushed.
	 */
	void polardb_flush_parent_counters(
		PolarDB_ParentCounterFlushReason reason);
#endif
	void update_bytes_recv(uint64_t bytes_recv);
	void update_bytes_sent(uint64_t bytes_sent);
	void ProcessQueryAndSetStatusFlags(const char* query_digest_text, int savepoint_count);

	inline const PGconn* get_pg_connection() const { return pgsql_conn; }
	inline int get_pg_server_version() { return PQserverVersion(pgsql_conn); }
	inline int get_pg_protocol_version() { return PQprotocolVersion(pgsql_conn); }
	inline const char* get_pg_host() { return PQhost(pgsql_conn); }
	inline const char* get_pg_hostaddr() { return PQhostaddr(pgsql_conn); }
	inline const char* get_pg_port() { return PQport(pgsql_conn); }
	inline const char* get_pg_dbname() { return PQdb(pgsql_conn); }
	inline const char* get_pg_user() { return PQuser(pgsql_conn); }
	inline const char* get_pg_password() { return PQpass(pgsql_conn); }
	inline const char* get_pg_options() { return PQoptions(pgsql_conn); }
	inline int get_pg_socket_fd() { return PQsocket(pgsql_conn); }
	inline int get_pg_backend_pid() { return PQbackendPID(pgsql_conn); }
	inline int get_pg_connection_needs_password() { return PQconnectionNeedsPassword(pgsql_conn); }
	inline int get_pg_connection_used_password() { return PQconnectionUsedPassword(pgsql_conn); }
	inline int get_pg_connection_used_gssapi() { return PQconnectionUsedGSSAPI(pgsql_conn); }
	inline int get_pg_client_encoding() { return PQclientEncoding(pgsql_conn); }
	inline int get_pg_ssl_in_use() { return PQsslInUse(pgsql_conn); }
	inline ConnStatusType get_pg_connection_status() { return PQstatus(pgsql_conn); }
	inline PGTransactionStatusType get_pg_transaction_status() { return PQtransactionStatus(pgsql_conn); }
	inline int get_pg_is_nonblocking() { return PQisnonblocking(pgsql_conn); }
	inline int get_pg_is_threadsafe() { return PQisthreadsafe(); }
	inline const char* get_pg_error_message() { return PQerrorMessage(pgsql_conn); }
	inline SSL* get_pg_ssl_object() { return (SSL*)PQsslStruct(pgsql_conn, "OpenSSL"); }
	inline const char* get_pg_parameter_status(const char* param) { return PQparameterStatus(pgsql_conn, param); }
	const char* get_pg_server_version_str(char* buff, int buff_size);
	const char* get_pg_connection_status_str();
	const char* get_pg_transaction_status_str();
	unsigned int get_memory_usage() const;
	char get_transaction_status_char();
	inline int get_backend_pid() { return (pgsql_conn) ? get_pg_backend_pid() : -1; }
	bool is_pipeline_active() { return (PQpipelineStatus(pgsql_conn) != PQ_PIPELINE_OFF); }
	const char* get_pg_backend_state() const;

	static int char_to_encoding(const char* name) {
		return pg_char_to_encoding(name);
	}

	static const char* encoding_to_char(int encoding) {
		return pg_encoding_to_char(encoding);
	}

	static int valid_server_encoding_id(int encoding) {
		return pg_valid_server_encoding_id(encoding);
	}

	inline
	void reduce_auto_increment_delay_token() { if (auto_increment_delay_token) auto_increment_delay_token--; };

	void set_status(bool set, uint32_t status_flag);
	bool get_status(uint32_t status_flag);
	bool MultiplexDisabled(bool check_delay_token = true);

	unsigned int reorder_dynamic_variables_idx();
	unsigned int number_of_matching_session_variables(const PgSQL_Connection* client_conn, unsigned int& not_matching);
	void set_query(const char* stmt, unsigned long length, const char* _backend_stmt_name = nullptr, const PgSQL_Extended_Query_Info* extended_query_info = nullptr);
	void reset();

	bool IsKeepMultiplexEnabledVariables(const char* query_digest_text);

	/**
	 * @brief Retrieves startup parameter and it's hash
	 *
	 * This function tries to retrieve value and hash of startup paramters if present (provided in connection parameters).
	 * If value is not found, it falls back to the thread-specific default variables.
	 *
	 * @param idx The index of startup parameter to retrieve.
	 * @return The value and hash of startup parameter.
	 *
	 */
	std::pair<const char*, uint32_t> get_startup_parameter_and_hash(enum pgsql_variable_name idx);
	
	/**
	 * @brief Copies tracked PgSQL session variables to startup parameters
	 *
	 * This function synchronizes the current tracked session variables (in `variables` and `var_hash`)
	 * to the startup parameters arrays (`startup_parameters` and `startup_parameters_hash`). If `copy_only_critical_param` 
	 * is true, only the critical parameters (indices 0 to PGSQL_NAME_LAST_LOW_WM-1) are copied. 
	 * Otherwise, all tracked variables up to PGSQL_NAME_LAST_HIGH_WM are copied.
	 *
	 * @param copy_only_critical_param If true, only critical parameters are copied; otherwise, all tracked variables.
	 */
	void copy_pgsql_variables_to_startup_parameters(bool copy_only_critical_param);

	/**
	 * @brief Copies startup parameters to tracked PgSQL session variables.
	 *
	 * This function synchronizes the startup parameters arrays (`startup_parameters` and `startup_parameters_hash`)
	 * to the tracked session variables (`variables` and `var_hash`). If `copy_only_critical_param` is true,
	 * only the critical parameters (indices 0 to PGSQL_NAME_LAST_LOW_WM-1) are copied. Otherwise, all tracked
	 * variables up to PGSQL_NAME_LAST_HIGH_WM are copied.
	 *
	 * @param copy_only_critical_param If true, only critical parameters are copied; otherwise, all tracked variables.
	 */
	void copy_startup_parameters_to_pgsql_variables(bool copy_only_critical_param);

	/**
	 * @brief Initialize critical startup parameters from backend ParameterStatus.
	 *
	 * Warmup connections are opened without a client session, so they do not run
	 * the client-variable copy path in connect_start(). Before entering the pool,
	 * they still need the same critical baseline that reset/reuse code requires.
	 */
	void init_startup_parameters_from_server();

	struct {
		unsigned long length;
		const char* ptr;
		const char* backend_stmt_name;
		const PgSQL_Extended_Query_Info* extended_query_info;
	} query;

	struct {
		char* init_connect;
		bool init_connect_sent;
	} options;

	struct {
		char* hostname;
		char* ip;
	} connected_host_details;

	bytes_stats_t bytes_info; // bytes statistics
	struct {
		unsigned long long questions;
		unsigned long long pgconnpoll_get;
		unsigned long long pgconnpoll_put;
	} statuses;

	std::array<PgSQL_Variable, PGSQL_NAME_LAST_HIGH_WM> variables = {};
	std::array<uint32_t, PGSQL_NAME_LAST_HIGH_WM> var_hash = {};
	// for now we store possibly missing variables in the lower range
	// we may need to fix that, but this will cost performance
	std::array<bool, PGSQL_NAME_LAST_HIGH_WM> var_absent = {};
	std::vector<uint32_t> dynamic_variables_idx;

	std::array<uint32_t, PGSQL_NAME_LAST_HIGH_WM> startup_parameters_hash = {};
	std::array<char*, PGSQL_NAME_LAST_HIGH_WM> startup_parameters = {};

	/**
	 * @brief Keeps tracks of the 'server_status'. Do not confuse with the 'server_status' from the
	 *  'MYSQL' connection itself. This flag keeps track of the configured server status from the
	 *  parent 'MySrvC'.
	 */
	enum MySerStatus server_status; // this to solve a side effect of #774

	PgSQL_Conn_Param conn_params;
	PgSQL_ErrorInfo error_info;
	PGconn* pgsql_conn;
	uint8_t result_type;
	PGresult* pgsql_result;
	PSresult  ps_result;
	PgSQL_Query_Result* query_result;
	PgSQL_Query_Result* query_result_reuse;
	unsigned long long creation_time;
	unsigned long long last_time_used;
	unsigned long long timeout;
	int auto_increment_delay_token;
	PG_ASYNC_ST async_state_machine;	// Async state machine
	short wait_events;
	bool new_result;
	bool is_copy_out;

	bool send_quit;
	bool reusable;
	bool processing_multi_statement;

#if POLARDB_PROXY
	uint64_t polardb_parent_bytes_recv_pending;
	uint64_t polardb_parent_bytes_sent_pending;
	uint64_t polardb_parent_queries_sent_pending;
	uint32_t polardb_parent_query_batch_count;
	uint32_t polardb_row_run_last_bytes = 0;
	// ---- PolarDB wrapped-wait result filtering ----
	// A wrapped LSN-wait read prepends N SET statements ahead of the user query
	// (see PgSQL_PolarDB_Wrap.cpp). The connection layer is the sole owner of SET
	// consumption: it silently skips the first N results and forwards only the
	// user's result to the session/client.

	/**
	 * @brief Per-dispatch query state passed from session to connection layer.
	 *
	 * Snapshotted at submit time (async_query ASYNC_IDLE) and consumed by
	 * query_start(). This decouples the connection layer from session state — it
	 * only needs the number of prepended SET results to skip.
	 *
	 * The connection layer may add wrapper state of its own on top of the session
	 * snapshot. When polardb_txn_split_xids_dirty is set on a simple query that is
	 * not already a split wait, async_query() builds
	 * "SET polar_xact_split_xids = ''; " followed by the user query into
	 * polardb_txn_split_xids_reset_query_buf, raises wrapper_stmts, sets
	 * txn_split_xids_reset, and promotes wrapper_kind to TXN_SPLIT_XIDS_RESET when
	 * the session asked for no wrapper. The dispatched query text then aliases that
	 * connection-owned buffer, which must stay alive for the whole dispatch; the
	 * non-dirty simple-query path clears it.
	 */
	struct PolarDB_Query_DispatchState {
		uint32_t wrapper_stmts{0};  // wrapper statements whose result sets to consume (0 = none)
		PolarDB_Query_WrapperKind wrapper_kind{PolarDB_Query_WrapperKind::NONE};
		bool txn_split_xids_reset{false};  // first wrapper SET clears stale split XIDs
		void reset() {
			wrapper_stmts = 0;
			wrapper_kind = PolarDB_Query_WrapperKind::NONE;
			txn_split_xids_reset = false;
		}
	};
	PolarDB_Query_DispatchState dispatch_state;

	/**
	 * @brief Per-query, connection-local state for consuming a wrapped query's
	 *        wrapper result sets.
	 *
	 * A consistency read is sent as a wrapped query: the user query preceded by a
	 * few wrapper statements (the consistency-mode / timeout / wait-LSN SETs). The
	 * backend returns one result set per statement, so this counts the wrapper
	 * result sets down and drops them, forwarding only the user query's result.
	 * For a straight, non-wrapped query it carries no state (was_wrapped == false,
	 * stmt_pending == 0) and the result path skips it entirely.
	 *
	 * wrapper_kind names which of three wrapper shapes was sent, and the
	 * is_consistency_wait() / is_txn_split_wait() / is_txn_split_xids_reset()
	 * helpers test it.
	 *
	 * CONSISTENCY_WAIT — an autocommit read routed to a replica with an LSN wait:
	 *
	 *     SET polar_consistency_mode = 'best_effort'|'strict';
	 *     SET polar_proxy_wait_timeout_ms = <resolved_ms>;
	 *     SET polar_xact_split_wait_lsn = '<target>';
	 *     <user query>
	 *
	 * TXN_SPLIT_WAIT — a read split out of an open transaction onto a replica. It
	 * prepends a fourth statement naming the transaction's XIDs, so the replica can
	 * read the uncommitted transaction's rows, and its wait always runs in strict
	 * mode because continuing past a timeout would read without those XIDs:
	 *
	 *     SET polar_xact_split_xids = '<xids>';
	 *     SET polar_consistency_mode = 'strict';
	 *     SET polar_proxy_wait_timeout_ms = <resolved_ms>;
	 *     SET polar_xact_split_wait_lsn = '<target>';
	 *     <user query>
	 *
	 * When the selected reader already reached the target, the three wait SETs are
	 * dropped and only the XID SET remains ahead of the user query.
	 *
	 * TXN_SPLIT_XIDS_RESET — generated by the connection layer, not the session. A
	 * pooled reader that ran a split read carries backend-local split-XID context,
	 * tracked by polardb_txn_split_xids_dirty. The next simple query on that
	 * connection, which is usually an ordinary query unrelated to any split, is
	 * prefixed to clear it:
	 *
	 *     SET polar_xact_split_xids = '';
	 *     <user query>
	 *
	 * txn_split_xids_reset_pending marks that leading SET, and
	 * consuming_txn_split_xids_reset() is true only while its result is the one
	 * being consumed.
	 *
	 * ProxySQL owns all three consistency values for a wrapped read. The first two SETs are
	 * always emitted, so a pooled backend connection cannot reuse an old wait mode
	 * or timeout from a previous client session. <resolved_ms> can be 0; in
	 * PolarDB that means the wait has no PolarDB wait-timeout limit. It is not an
	 * absolute "query can never stop" setting: PostgreSQL statement_timeout,
	 * client cancel, administrator termination, or connection loss can still
	 * interrupt the backend wait through the normal CHECK_FOR_INTERRUPTS() path.
	 * If none of those happens and the replica never reaches the target LSN, the
	 * query waits until replay catches up.
	 *
	 * The backend returns one result for each statement in order. This connection
	 * state stores the number of prepended SET results and PgSQL_Connection.cpp
	 * consumes exactly those leading results before forwarding the user query
	 * result. A SET appended after the user query is not supported by this state:
	 * without a separate trailing-skip counter, that result would be forwarded to
	 * the client as an extra result.
	 *
	 * PolarDB backend facts used here:
	 *  - procarray.c:GetSnapshotData() calls polar_split_wait_for_lsn() on a
	 *    replica only when polar_xact_split_wait_lsn is not InvalidXLogRecPtr
	 *    (procarray.c:2803-2820).
	 *  - procarray.c:polar_split_wait_for_lsn() loops while current_lsn < target.
	 *    It reads polar_proxy_wait_timeout_ms in that loop and checks
	 *    polar_consistency_mode only when a finite timeout is reached. strict
	 *    raises ERROR; best_effort raises WARNING and returns stale data
	 *    (procarray.c:2687-2742).
	 *  - With polar_proxy_wait_timeout_ms=0, the finite-timeout branch is not
	 *    reached. For the wait itself, best_effort and strict both mean "wait
	 *    until the target LSN is replayed or the statement is interrupted by
	 *    PostgreSQL statement_timeout, a client cancel, or administrator
	 *    termination."
	 *  - guc.c defines polar_xact_split_wait_lsn as a transactional PGC_USERSET
	 *    GUC; its assign hook treats an empty string as InvalidXLogRecPtr
	 *    (guc.c:6274-6281, guc.c:15837-15848).
	 *  - ReadyForQuery LSN capture is separate from polar_xact_split_wait_lsn.
	 *    The backend sends LSN when startup parameter _polar_send_lsn or
	 *    _polar_proxy_send_lsn sets Port.polar_proxy_send_lsn
	 *    (postmaster.c:2440-2442, dest.c:314-325).
	 *
	 * Result by case (PolarDB15 with proxy support):
	 *  1. Whether the wrapped read reaches the target, times out (best_effort
	 *     WARNING), or errors (strict), the backend consumes polar_xact_split_wait_lsn
	 *     after the first wait, so the wait target never survives on the pooled
	 *     backend connection. On a later straight read the target is Invalid and the
	 *     backend never waits, so ProxySQL does not need a second wrapper kind just
	 *     to clear the wait target.
	 *  2. A strict finite-timeout ERROR or user-query ERROR additionally aborts the
	 *     multi-statement batch; PostgreSQL transactional GUC behavior rolls back the
	 *     wrapper SETs.
	 * Postponed optimization:
	 *  - Repeated mode/timeout SETs can be skipped per backend connection later.
	 *    That state must live on PgSQL_Connection, not on the client session,
	 *    because backend connections are pooled and reused.
	 *  - Applied mode/timeout tracking must update only after the whole wrapped
	 *    query completes successfully. A strict timeout or user-query ERROR rolls
	 *    back the wrapper SETs, so updating at wrapper-build time would desync the
	 *    tracked state from the backend connection.
	 */
	struct PolarDB_Query_WrapState {
		bool was_wrapped{false};   // sticky: this query was sent wrapped (survives consumption)
		bool stmt_failed{false};   // sticky: a wrapper statement failed before the user result
		bool stmt_succeeded{false}; // sticky: every wrapper statement completed successfully
		uint32_t stmt_total{0};    // wrapper statements in the wrapped query (set once by begin)
		uint32_t stmt_pending{0};  // wrapper statements whose result set is still to consume
		PolarDB_Query_WrapperKind wrapper_kind{PolarDB_Query_WrapperKind::NONE};
		bool txn_split_xids_reset_pending{false}; // true only for the first wrapper SET
		PolarDB_ExtendedWaitNoticeOwner extended_wait_notice_owner{
			PolarDB_ExtendedWaitNoticeOwner::QUERY_RESULT};

		bool consuming_wrapper_set() const { return stmt_pending > 0; }
		bool consuming_txn_split_xids_reset() const {
			return txn_split_xids_reset_pending && stmt_pending == stmt_total;
		}
		bool wrapper_set_failed() const { return stmt_failed; }
		bool wrapper_set_succeeded() const { return stmt_succeeded; }
		bool is_consistency_wait() const {
			return wrapper_kind == PolarDB_Query_WrapperKind::CONSISTENCY_WAIT ||
				wrapper_kind == PolarDB_Query_WrapperKind::EXTENDED_WAIT;
		}
		bool is_extended_wait() const {
			return wrapper_kind == PolarDB_Query_WrapperKind::EXTENDED_WAIT;
		}
		bool is_txn_split_wait() const {
			return wrapper_kind == PolarDB_Query_WrapperKind::TXN_SPLIT_WAIT;
		}
		bool is_txn_split_xids_reset() const {
			return wrapper_kind == PolarDB_Query_WrapperKind::TXN_SPLIT_XIDS_RESET;
		}
		bool is_polar_wait_wrapper() const {
			return is_consistency_wait() || is_txn_split_wait();
		}
		bool wait_notice_uses_session_queue() const {
			return consuming_wrapper_set() ||
				(is_extended_wait() && extended_wait_notice_owner ==
					PolarDB_ExtendedWaitNoticeOwner::SESSION_QUEUE);
		}

		void begin(uint32_t n, PolarDB_Query_WrapperKind kind,
				bool txn_xids_reset = false) {
			was_wrapped = (n > 0);
			stmt_failed = false;
			stmt_succeeded = false;
			stmt_total = n;
			stmt_pending = n;
			wrapper_kind = (n > 0) ? kind : PolarDB_Query_WrapperKind::NONE;
			txn_split_xids_reset_pending = (n > 0) && txn_xids_reset;
			extended_wait_notice_owner =
				PolarDB_ExtendedWaitNoticeOwner::QUERY_RESULT;
		}
		void begin_extended_wait(PolarDB_ExtendedWaitNoticeOwner notice_owner) {
			was_wrapped = true;
			stmt_failed = false;
			stmt_succeeded = false;
			stmt_total = 0;
			stmt_pending = 0;
			wrapper_kind = PolarDB_Query_WrapperKind::EXTENDED_WAIT;
			txn_split_xids_reset_pending = false;
			extended_wait_notice_owner = notice_owner;
		}
		void mark_extended_wait_result_received() {
			if (is_extended_wait()) {
				extended_wait_notice_owner =
					PolarDB_ExtendedWaitNoticeOwner::QUERY_RESULT;
			}
		}
		void mark_extended_wait_succeeded() {
			if (is_extended_wait()) {
				stmt_succeeded = true;
			}
		}
		void mark_wrapper_set_failed() {
			stmt_failed = true;
			stmt_succeeded = false;
			stmt_pending = 0;
			txn_split_xids_reset_pending = false;
		}
		void consume_successful_wrapper_set() {
			assert(stmt_pending > 0);
			stmt_pending--;
			if (stmt_pending == 0) {
				stmt_succeeded = true;
			}
		}
		void clear() {
			was_wrapped = false;
			stmt_failed = false;
			stmt_succeeded = false;
			stmt_total = 0;
			stmt_pending = 0;
			wrapper_kind = PolarDB_Query_WrapperKind::NONE;
			txn_split_xids_reset_pending = false;
			extended_wait_notice_owner =
				PolarDB_ExtendedWaitNoticeOwner::QUERY_RESULT;
		}
	};
	PolarDB_Query_WrapState polardb_query_wrap_state;

	/**
	 * @brief A split-XID wrapper has not yet reached a safe query boundary.
	 *
	 * PolarDB clears its mock split transaction on commit or abort. Keep this
	 * marker until the complete wrapped query succeeds; incomplete or uncertain
	 * paths must reset the backend before reuse.
	 */
	bool polardb_txn_split_xids_dirty{false};
	bool polardb_txn_split_xids_reset_consumed{false};
	std::string polardb_txn_split_xids_reset_query_buf;

	/**
	 * @brief PolarDB startup profile requested on this backend connection.
	 *
	 * It records only what ProxySQL asked for in the startup packet; it does not show
	 * whether the backend returned RFQ payloads. When picking a pooled reader for an
	 * LSN-targeted read, the connection pool checks requests_rfq_lsn() on this profile to
	 * confirm the backend was asked to append the RFQ LSN.
	 */
	PolarDB_StartupProfile polardb_startup_profile;
	uint32_t polardb_startup_profile_generation;
	uint64_t polardb_startup_config_generation;
	int polardb_startup_identity_mode;
	bool polardb_startup_settings_set;

	/**
	 * @brief Explicit endpoint to advertise in PolarDB startup params.
	 *
	 * Used when the caller has already resolved the startup identity but the
	 * connection is opened outside that client dispatch path.
	 */
	PolarDB_StartupIdentity polardb_forced_startup_identity;
	bool polardb_forced_startup_parameters;

	/**
	 * @brief Startup-client metadata actually used by this backend.
	 *
	 * The identity is populated when PolarDB startup keys are appended. SSL and
	 * proxy session/cancel fields are reserved placeholders until those backend
	 * startup keys are wired. Pool reuse compares this whole object, so fields added
	 * later are included in the comparison as soon as they become active.
	 */
	PolarDB_StartupClientContext polardb_startup_client;

	PolarDB_PoolKey polardb_pool_key;
	uint32_t polardb_core_pool_position{UINT32_MAX};
	uint64_t polardb_worker_cache_epoch{0};
	/**
	 * True after this backend has entered the exact-key pool.
	 *
	 * Session-state changes can clear polardb_pool_key while the connection is
	 * in USED. This bit keeps its exact return path without making every
	 * classic keyless return lock the server merely to inspect the USED list.
	 */
	bool polardb_exact_pool_connection{false};
	std::atomic<bool> polardb_idle_ping_inflight{false};
	std::atomic<bool> polardb_reader_pool_connect_pending{false};
	bool polardb_reader_pool_created{false};
	bool polardb_idle_trim_pending{false};

	/**
	 * Keeps the selected server alive while this backend is in use. Server-list
	 * snapshots own retired server objects until every selected connection has
	 * been returned or destroyed.
	 */
	std::shared_ptr<const void> polardb_selected_server_snapshot;
#endif // POLARDB_PROXY

	bool multiplex_delayed;
	bool is_client_connection; // true if this is a client connection, false if it is a server connection
	bool exit_pipeline_mode; // true if it is safe to exit pipeline mode
	bool resync_failed; // true if the last resync attempt failed

	PgSQL_STMT_Local* local_stmts;
	PgSQL_SrvC *parent;
	PgSQL_Connection_userinfo* userinfo;
	PgSQL_Data_Stream* myds;
	//unsigned int warning_count;
	int fd;
	/**
	 * @brief This represents the internal knowledge of ProxySQL about the connection. It keeps track of those
	 *  states which *are not reflected* into 'server_status', but are relevant for connection handling.
	 */
	uint32_t status_flags;
	unsigned long largest_query_length;
	int async_exit_status; // exit status of Non blocking API
	bool unknown_transaction_status;

#if POLARDB_PROXY
	/**
	 * @brief Enable requested PolarDB RFQ parsing on a freshly connected backend.
	 *
	 * Called once per connection right after a successful connect (cold path).
	 * Turns on libpq parsing only for payloads this connection requested in its
	 * startup profile. No-op when there is no live connection.
	 */
	void polardb_enable_requested_rfq_parsing();

	/**
	 * @brief Install the startup settings chosen by ReaderPool creation.
	 *
	 * Demand creation resolves this state before asynchronous connect begins.
	 * connect_start() must then emit these exact settings rather than rereading
	 * thread-local configuration that may have changed after server selection.
	 */
	void set_polardb_startup_settings(
		const PolarDB_StartupProfile& profile,
		int identity_mode,
		uint64_t startup_config_generation,
		const PolarDB_StartupClientContext& startup_client);

	/**
	 * @brief Read the PolarDB WAL LSN carried by the last ReadyForQuery (RFQ-only).
	 *
	 * Reads the LSN libpq cached from the most recent extended ReadyForQuery via
	 * PQgetLSN()/PQhasLSN(). This is a pure accessor: it never issues a SQL query,
	 * so it is safe to call on the hot request/result-processing path.
	 * SQL-based LSN probing lives only in the monitor.
	 *
	 * @return LSN value (64-bit), or 0 if no non-zero LSN is cached / no connection.
	 */
	uint64_t get_polardb_lsn();

	/**
	 * @brief Whether libpq parsed an RFQ LSN payload on the last ReadyForQuery.
	 *
	 * A present payload may still carry value 0 on a session that has no usable
	 * WAL position yet. Callers that need a wait target must check both this flag
	 * and get_polardb_lsn() > 0; callers that diagnose RFQ protocol presence must
	 * not collapse "present zero" into "missing payload".
	 */
	bool has_polardb_lsn_payload();

	/**
	 * @brief Read the transaction XID list carried by the last ReadyForQuery.
	 *
	 * Pure RFQ accessor over libpq's cached payload. It never issues SQL and is
	 * meaningful only for connections whose startup profile requested
	 * REQUEST_RFQ_XID.
	 *
	 * @return comma-separated XID list, or nullptr when unavailable.
	 */
	const char* get_polardb_txn_xids();

	/**
	 * @brief True when the last RFQ says the transaction is safe to split.
	 *
	 * Pure RFQ accessor over libpq's cached `x` marker; no SQL is issued.
	 */
	bool is_polardb_txn_splittable();

	/**
	 * @brief True when the last RFQ says WAL is still pending for split reads.
	 *
	 * Pure RFQ accessor over libpq's cached `w` marker; no SQL is issued.
	 */
	bool is_polardb_txn_wal_pending();
#endif // POLARDB_PROXY

private:
	// Set end state for the fetch result to indicate that it originates from a simple query or statement execution.
	ASYNC_ST fetch_result_end_st = ASYNC_QUERY_END;
	inline void set_fetch_result_end_state(ASYNC_ST st) {
		assert(st == ASYNC_QUERY_END || st == ASYNC_STMT_EXECUTE_END || 
			st == ASYNC_STMT_DESCRIBE_END || st == ASYNC_STMT_PREPARE_END ||
			st == ASYNC_RESYNC_END);
		fetch_result_end_st = st;
	}
	// Handles the COPY OUT response from the server.
	// Returns true if it consumes all buffer data, or false if the threshold for result size is reached
	bool handle_copy_out(const PGresult* result, uint64_t* processed_bytes);
	static void notice_handler_cb(void* arg, const PGresult* result);
	static void unhandled_notice_cb(void* arg, const PGresult* result);
	void init_query_result();

#if POLARDB_PROXY
	/**
	 * @brief Resolve the PolarDB startup profile for this backend hostgroup.
	 *
	 * @param hid hostgroup id used to resolve the per-hostgroup proxy protocol.
	 * @return A profile built from PolarDB_ProxyProtocol::OFF when this connection
	 *         has no parent or parent hostgroup container, or when @p hid is not a
	 *         PolarDB hostgroup — such a connection gets no PolarDB startup
	 *         parameters. Otherwise a profile for the hostgroup's own
	 *         policy.proxy_protocol when that is set (>= 0), falling back to the
	 *         global pgsql-polardb_proxy_protocol default.
	 */
	PolarDB_StartupProfile polardb_build_startup_profile(unsigned int hid) const;

	/**
	 * @brief Append profile-driven PolarDB startup params to a conninfo.
	 *
	 * Emits no parameters when the profile requests no RFQ payloads (which includes
	 * proxy_protocol=off). For RFQ-requesting profiles, this resolves the startup
	 * identity first and fails the connection before PQconnectStart when no valid
	 * identity is available.
	 *
	 * This rewrites the connection's own startup state, which pool reuse compares:
	 * when no startup settings is installed it resets polardb_startup_client and
	 * re-reads polardb_startup_identity_mode from the thread variable, and on
	 * success it stores the resolved endpoint in polardb_startup_client.identity.
	 * Before returning false it also fills in the connection's error_info via
	 * set_error() and logs the reason, so the caller can surface it to the client.
	 *
	 * @param conninfo       conninfo string being built for the connection.
	 * @param profile        resolved startup profile.
	 * @param hid            hostgroup id for diagnostics.
	 * @return true on success, false when the connection should fail early.
	 */
	bool polardb_append_startup_params(std::ostringstream& conninfo,
		const PolarDB_StartupProfile& profile, unsigned int hid);

	/**
	 * @brief Choose the endpoint to advertise in the PolarDB startup params.
	 *
	 * Two short-circuits come first. When startup settings are set,
	 * polardb_startup_client.identity is returned as-is. Otherwise, when
	 * polardb_forced_startup_identity carries a source, that is returned — unless its
	 * source contradicts the identity mode or it fails its validity check, in which
	 * case the result is source NONE and no further source is tried.
	 *
	 * Only then does polardb_startup_identity_mode select between two disjoint
	 * chains; neither falls through to the other:
	 *  - client mode: the client endpoint resolved on the session, then the raw
	 *    client socket address. If both fail the result is NONE. The listener/proxy
	 *    address and pgsql-polardb_proxy_identity_host are never consulted, so a
	 *    configured fallback cannot rescue a client-mode connection that has no
	 *    usable client address.
	 *  - proxy mode: the ProxySQL listener/proxy address, rejecting a wildcard
	 *    listener because it names no real endpoint, then the operator-configured
	 *    fallback identity. The client sources are never consulted. This is also the
	 *    only chain available to connections with no client session, such as
	 *    monitor and other internal connections.
	 *
	 * @param profile Accepted for symmetry with the caller; identity selection does
	 *                not depend on it.
	 * @return The chosen identity, or an identity with source NONE when nothing
	 *         usable was found. polardb_append_startup_params() turns NONE into a
	 *         hard failure for any profile that requests an RFQ LSN, so the
	 *         connection is never opened without the parameters.
	 */
	PolarDB_StartupIdentity polardb_resolve_startup_identity(
		const PolarDB_StartupProfile& profile) const;
#endif // POLARDB_PROXY

	/**
	 * @brief Checks if a substring at a given position in a string matches the format of a formatted PostgreSQL error header.
	 *
	 * The expected format is: <UPPERCASE_PREFIX>:<SIZE>:
	 * - <UPPERCASE_PREFIX>: One or more uppercase letters (A-Z).
	 * - <SIZE>: One or more digits representing the length of the following value.
	 * - The header must be followed by a colon ':'.
	 *
	 * Example of a valid header: "S:5:Error"
	 *
	 * @param s The string to check.
	 * @param pos The position in the string to start checking.
	 * @return true if a valid formatted error header is found at the given position, false otherwise.
	 */
	static bool is_valid_formatted_pq_error_header(const std::string& s, size_t pos);

	/**
	 * @brief Parses a PostgreSQL error message string into its component fields.
	 *
	 * This function scans the input error string, extracts all such formatted fields, and stores them
	 * in a map from prefix to a vector of values (to support repeated fields). Any unformatted text
	 * is stored under the "LE" (Library Error) key.
	 *
	 * Example input: "S:5:ERRORC:5:12345M:12:Some message"
	 * Output: { "S": ["ERROR"], "C": ["12345"], "M": ["Some message"] }
	 *
	 * @param error_str The error message string to parse.
	 * @return std::map<std::string, std::vector<std::string>> Map of error field prefixes to their values.
	 */
	static std::map<std::string, std::vector<std::string>> parse_pq_error_message(const std::string& error_str);
};

class PgSQL_Backend_Kill_Args {
public:
	enum class TYPE {
		CANCEL_QUERY = 0,
		TERMINATE_CONNECTION
	};
	PGcancel* cancel_conn;
	PgSQL_Thread* pgsql_thd;

	char* username;
	char* password;
	char* hostname;
	char* dbname;
	unsigned int port;

	int backend_pid;
	unsigned int hostgroup_id;
	TYPE type;

	// SSL options
	struct SSLConfig {
		bool use_ssl = false;
		char* sslkey;
		char* sslcert;
		char* sslrootcert;
		char* sslcrl;
		char* sslcrldir;
		char* ssl_min_protocol_version;
		char* ssl_max_protocol_version;
	} ssl_config;

	PgSQL_Backend_Kill_Args(PGconn* conn, const char* user, const char* pass, const char* db, const char* host,
		unsigned int port, unsigned int hid, bool ssl, TYPE typ, PgSQL_Thread* thd);
	~PgSQL_Backend_Kill_Args();
};

void* PgSQL_backend_kill_thread(void* arg);

#endif /* PROXYSQL_PGSQL_CONNECTION_H */
