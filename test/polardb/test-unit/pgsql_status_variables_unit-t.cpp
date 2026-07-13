/**
 * @file pgsql_status_variables_unit-t.cpp
 * @brief Unit tests for PgSQL generic thread status-variable storage.
 *
 * PgSQL reuses the shared st_var_* counter indexes declared with the MySQL
 * thread status enum. Keep the PgSQL storage sized to that shared index range;
 * otherwise high-index generic counters can write past PgSQL_Thread::stvar[].
 */

#include "tap.h"
#include "cpp.h"

int main() {
	plan(4);
	ok(static_cast<int>(PG_st_var_END) == static_cast<int>(MY_st_var_END),
		"PgSQL generic status storage matches shared st_var index range");
	ok(static_cast<int>(PG_st_var_END) >
			static_cast<int>(st_var_client_host_error_killed_connections),
		"PgSQL generic status storage covers client-host-error killed counter");
	ok(static_cast<int>(PG_st_var_END) >
			static_cast<int>(st_var_set_wait_timeout_commands),
		"PgSQL generic status storage covers set-wait-timeout counter");
	ok(static_cast<int>(PG_st_var_END) >
			static_cast<int>(st_var_timeout_terminated_connections),
		"PgSQL generic status storage covers timeout-terminated counter");
	return exit_status();
}
