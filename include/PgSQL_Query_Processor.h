#ifndef PROXYSQL_PGSQL_QUERY_PROCESSOR_H
#define PROXYSQL_PGSQL_QUERY_PROCESSOR_H
#include "proxysql.h"
#include "cpp.h"
#include "QP_rule_text.h"
#include "query_processor.h"

class Command_Counter;
#if POLARDB_PROXY
struct PgSQL_Query_Processor_Rule_t : public QP_rule_t {
	int replica_eligible;
};

class PgSQL_Query_Processor_Output : public Query_Processor_Output {
public:
	PgSQL_Query_Processor_Output() = default;
	~PgSQL_Query_Processor_Output() = default;

	void init() {
		Query_Processor_Output::init();
		replica_eligible = -1;
		force_primary_hint = false;
	}

	int replica_eligible;
	bool force_primary_hint;  // /* route=primary */ hint from the query's first comment
};
#else
struct PgSQL_Query_Processor_Rule_t : public QP_rule_t {};
class PgSQL_Query_Processor_Output : public Query_Processor_Output {};
#endif // POLARDB_PROXY

class PgSQL_Rule_Text : public QP_rule_text {
public:
	PgSQL_Rule_Text(const PgSQL_Query_Processor_Rule_t* pqr);
	~PgSQL_Rule_Text() = default;
};

class PgSQL_Query_Processor : public Query_Processor<PgSQL_Query_Processor> {
public:
	PgSQL_Query_Processor();
	~PgSQL_Query_Processor();

	void init_thread();
	void end_thread();
	void update_query_processor_stats();
	SQLite3_result* get_stats_commands_counters();
	SQLite3_result* get_current_query_rules();
	PgSQL_Query_Processor_Output* process_query(PgSQL_Session* sess, void* ptr, unsigned int size, PgSQL_Query_Info* qi);
	unsigned long long query_parser_update_counters(PgSQL_Session* sess, enum PGSQL_QUERY_command c, SQP_par_t* qp, unsigned long long t);
	static enum PGSQL_QUERY_command query_parser_command_type(SQP_par_t* qp);
	static PgSQL_Query_Processor_Rule_t* new_query_rule(int rule_id, bool active, const char* username, const char* schemaname, int flagIN, const char* client_addr,
		const char* proxy_addr, int proxy_port, const char* digest, const char* match_digest, const char* match_pattern, bool negate_match_pattern,
		const char* re_modifiers, int flagOUT, const char* replace_pattern, int destination_hostgroup, int cache_ttl, int cache_empty_result,
		int cache_timeout, int reconnect, int timeout, int retries, int delay, int next_query_flagIN, int mirror_flagOUT, 
		int mirror_hostgroup, const char* error_msg, const char* OK_msg, int sticky_conn, int multiplex,
#if POLARDB_PROXY
		int replica_eligible,
#endif // POLARDB_PROXY
		int log,
		bool apply, const char* attributes, const char* comment);

private:
	Command_Counter* commands_counters[PGSQL_QUERY___NONE];
	static PgSQL_Query_Processor_Rule_t* new_query_rule(const PgSQL_Query_Processor_Rule_t* mqr);

#if POLARDB_PROXY
	// Copy a matched rule's replica_eligible onto the query output so the
	// PolarDB LSN routing pipeline can opt a read into replica routing. Default
	// (-1, unset) leaves routing unchanged from upstream.
	inline
	void process_query_extended(PgSQL_Query_Processor_Output* ret, const PgSQL_Query_Processor_Rule_t* pqr) {
		if (pqr->replica_eligible >= 0) {
			ret->replica_eligible = pqr->replica_eligible;
		}
	}

	// Handle PolarDB first-comment hints. The base Query_Processor detects this
	// method via SFINAE (query_processor.h) and calls it for each first-comment
	// key=value pair. `/* route=primary */` pins this query to the writer (plan L0
	// in PgSQL_PolarDB_Flow.cpp). Parsed from the query's first SQL comment, not a
	// query-rule column.
	inline
	void query_parser_first_comment_extended(const char* key, const char* value,
		PgSQL_Query_Processor_Output* qpo) {
		if (!strcasecmp(key, "route") && !strcasecmp(value, "primary")) {
			qpo->force_primary_hint = true;
		}
	}
#endif // POLARDB_PROXY

	friend class Query_Processor;
};

#endif /* PROXYSQL_PGSQL_QUERY_PROCESSOR_H */
