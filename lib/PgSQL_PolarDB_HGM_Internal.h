#ifndef PROXYSQL_PGSQL_POLARDB_HGM_INTERNAL_H
#define PROXYSQL_PGSQL_POLARDB_HGM_INTERNAL_H

#if POLARDB_PROXY
#include <atomic>
#include <cstdint>
#endif // POLARDB_PROXY

class PgSQL_Connection;
class PgSQL_SrvC;
class PgSQL_Thread;

bool pgsql_connection_creation_throttled_locked(PgSQL_SrvC* mysrvc);
PgSQL_Connection* pgsql_create_backend_connection_locked(PgSQL_SrvC* mysrvc);
void pgsql_pool_status_count(unsigned long* counter, unsigned long value = 1);
void pgsql_pool_status_count_get(
	PgSQL_Thread* thread, unsigned long* fallback_counter,
	unsigned long value = 1);
void pgsql_pool_status_count_get_ok(
	PgSQL_Thread* thread, unsigned long* fallback_counter,
	unsigned long value = 1);
unsigned long pgsql_pool_status_read(unsigned long* counter);
unsigned long pgsql_pool_status_read_get(unsigned long* fallback_counter);
unsigned long pgsql_pool_status_read_get_ok(unsigned long* fallback_counter);

unsigned int pgsql_srv_latency_limit_us(const PgSQL_SrvC* mysrvc);
bool pgsql_srv_latency_allowed(const PgSQL_SrvC* mysrvc);
#if POLARDB_PROXY
int current_global_polardb_proxy_protocol();
#endif // POLARDB_PROXY

#endif // PROXYSQL_PGSQL_POLARDB_HGM_INTERNAL_H
