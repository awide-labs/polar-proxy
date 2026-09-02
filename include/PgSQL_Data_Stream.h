#ifndef PROXYSQL_PGSQL_DATA_STREAM_H
#define PROXYSQL_PGSQL_DATA_STREAM_H

#include "proxysql.h"
#include "cpp.h"

#include "PgSQL_Protocol.h"
#include "scram.h"

#ifndef uchar
typedef unsigned char uchar;
#endif

#include "ma_pvio.h"

#define QUEUE_T_DEFAULT_SIZE	32768
#define MY_SSL_BUFFER	8192

typedef struct _pgsql_queue_t {
	void* buffer;
	unsigned int size;
	unsigned int head;
	unsigned int tail;
	unsigned int partial;
	PtrSize_t pkt;
	mysql_hdr hdr;
} pgsql_queue_t;

// this class avoid copying data
class PgSQL_MyDS_real_query {
public:
	PtrSize_t pkt{}; // packet coming from the client
	char* QueryPtr = nullptr;	// pointer to beginning of the query
	unsigned int QuerySize = 0;	// size of the query
	// Transfer packet ownership from source without copying its buffer.
	// This clears source. end() frees the packet unless release_packet() transfers
	// it. Borrowed descriptors are not accepted because end() owns the buffer.
	void take_packet(PtrSize_t& source) {
		assert(pkt.ptr == nullptr);
		assert(QueryPtr == nullptr);
		assert(QuerySize == 0);
		assert(source.ptr != nullptr);
		assert(source.size >= PGSQL_V3_MESSAGE_HEADER_SIZE);
		assert(!ptrsize_is_borrowed_owner(&source));
		pkt = source;
		source = {};
		QuerySize = pkt.size - PGSQL_V3_MESSAGE_HEADER_SIZE;
		if (QuerySize == 0) {
			QueryPtr = const_cast<char*>("");
		}
		else {
			QueryPtr = (char*)pkt.ptr + PGSQL_V3_MESSAGE_HEADER_SIZE;
		}
	}
	// Transfer packet ownership out and clear the stored packet.
	PtrSize_t release_packet() {
		PtrSize_t result = pkt;
		reset();
		return result;
	}
	void end() {
		l_free(pkt.size, pkt.ptr);
		reset();
	}
	void reset() {
		pkt = {};
		QuerySize = 0;
		QueryPtr = nullptr;
	}
	void move_from(PgSQL_MyDS_real_query& other) {
		// Transfer ownership of the client packet without copying bytes. This is
		// needed when routing changes after the packet was attached to another
		// backend data stream.
		assert(QueryPtr == NULL);
		assert(QuerySize == 0);
		assert(pkt.ptr == NULL);
		assert(pkt.size == 0);
		pkt = other.pkt;
		QueryPtr = other.QueryPtr;
		QuerySize = other.QuerySize;
		other.reset();
	}
};

enum pgsql_sslstatus { PGSQL_SSLSTATUS_OK, PGSQL_SSLSTATUS_WANT_IO, PGSQL_SSLSTATUS_FAIL };

class PgSQL_Data_Stream
{
private:
	int array2buffer();
	int buffer2array();
	enum pgsql_sslstatus do_ssl_handshake();
	void queue_encrypted_bytes(const char* buf, size_t len);
#if POLARDB_PROXY
	/**
	 * @brief Report whether the pending output is worth sending over the direct
	 *        write path instead of the buffered writer.
	 *
	 * Always returns true while polardb_write_head_partial is non-zero: a direct
	 * write has already handed the front of PSarrayOUT[0] to the kernel, and the
	 * buffered path does not pick a partially sent packet up, so that transfer
	 * must be resumed by the direct path. Otherwise this is purely a batching
	 * heuristic and returns true only when at least two packets are pending, or
	 * when a single pending packet would not fit in QUEUE_T_DEFAULT_SIZE.
	 *
	 * This answers "is it worthwhile", not "is it safe" — combine it with
	 * polardb_can_writev_direct() before taking the direct path.
	 *
	 * @return true when the direct path should be used, false to leave the data
	 *         to the buffered writer.
	 */
	bool polardb_direct_write_batch_ready() const;

	/**
	 * @brief Send pending PSarrayOUT packets straight to the socket with one
	 *        sendmsg(), skipping every safety and batching check.
	 *
	 * The caller must have already established that direct sending is safe for
	 * this stream through polardb_can_writev_direct(). Nothing is re-verified
	 * here, so calling this on an encrypted stream, a backend stream, a stream
	 * that still holds buffered queueOUT bytes, or another unsafe state emits
	 * bytes out of order and corrupts the client stream.
	 *
	 * On success the fully sent packets are retired from PSarrayOUT, pkts_sent
	 * and bytes_info are advanced, and polardb_write_head_partial records how far
	 * into the first surviving packet a short write reached. pollout is enabled
	 * on every outcome that leaves the socket usable.
	 *
	 * @param byte_budget  Maximum number of bytes to offer the kernel in this
	 *                     call; 0 means QUEUE_T_DEFAULT_SIZE.
	 * @return  > 0 the number of bytes the kernel accepted; -1 the write would
	 *          block (EINTR/EAGAIN/EWOULDBLOCK) and pollout is enabled again;
	 *          0 either nothing could be sent or the write failed fatally, in
	 *          which case shut_soft() has already run — check net_failure rather
	 *          than retrying.
	 */
	int polardb_writev_to_net_poll_unchecked(size_t byte_budget);
#endif // POLARDB_PROXY
public:
	void* operator new(size_t);
	void operator delete(void*);

	pgsql_queue_t queueIN;
	uint64_t pkts_recv; // counter of received packets
	pgsql_queue_t queueOUT;
	uint64_t pkts_sent; // counter of sent packets

	struct {
		PtrSize_t pkt;
		unsigned int partial;
	} CompPktIN;
	struct {
		PtrSize_t pkt;
		unsigned int partial;
	} CompPktOUT;

	PgSQL_Protocol myprot;
	PgSQL_MyDS_real_query pgsql_real_query;
	bytes_stats_t bytes_info; // bytes statistics

	//PtrSize_t multi_pkt;

	unsigned long long pause_until;
	unsigned long long wait_until;
	unsigned long long killed_at;
	unsigned long long max_connect_time;

	struct {
		unsigned long long questions;
		unsigned long long pgconnpoll_get;
		unsigned long long pgconnpoll_put;
	} statuses;

	PtrSizeArray* PSarrayIN;
	PtrSizeArray* PSarrayOUT;
	FixedSizeQueue data_packets_history_IN;
	FixedSizeQueue data_packets_history_OUT;
	//PtrSizeArray *PSarrayOUTpending;
	//PtrSizeArray* resultset;
	//unsigned int resultset_length;

	ProxySQL_Poll<PgSQL_Data_Stream>* mypolls;
	//int listener;
	PgSQL_Connection* myconn;
	PgSQL_Session* sess;  // pointer to the session using this data stream
	PgSQL_Backend* mybe;  // if this is a connection to a mysql server, this points to a backend structure
	char* x509_subject_alt_name;
	SSL* ssl;
	BIO* rbio_ssl;
	BIO* wbio_ssl;
	char* ssl_write_buf;
	size_t ssl_write_len;
	struct sockaddr* client_addr;

	struct {
		char* addr;
		int port;
	} addr;
	struct {
		char* addr;
		int port;
	} proxy_addr;

	AUTHENTICATION_METHOD auth_method = AUTHENTICATION_METHOD::NO_PASSWORD;
	uint32_t auth_next_pkt_type = 0;
	bool auth_received_startup = false;
	unsigned char tmp_login_salt[4];
	ScramState* scram_state;

	unsigned int connect_tries;
	int query_retries_on_failure;
	int connect_retries_on_failure;
	enum mysql_data_stream_status DSS;
	PgSQL_DS_type myds_type;

	socklen_t client_addrlen;

	int fd; // file descriptor
	int poll_fds_idx;


	int active_transaction; // 1 if there is an active transaction
	int active; // data stream is active. If not, shutdown+close needs to be called

	int switching_auth_stage;
	int switching_auth_type;
	unsigned int tmp_charset;

	short revents;

	char kill_type;

	bool encrypted;
	bool net_failure;
	bool cancel_query;

	bool com_field_list;
	char* com_field_wild;

	PgSQL_Data_Stream();
	virtual ~PgSQL_Data_Stream();
	int array2buffer_full();
	void init();	// initialize the data stream
	void init(PgSQL_DS_type, PgSQL_Session*, int); // initialize with arguments
	void shut_soft();
	void shut_hard();
	int read_from_net();
	int write_to_net();
	int write_to_net_poll();
#if POLARDB_PROXY
	/**
	 * @brief Report whether the writev direct path may be used on this stream.
	 *
	 * This is the complete synchronous direct-send safety gate, including the
	 * pgsql-polardb_writev_direct admin variable. While
	 * polardb_write_head_partial is non-zero the variable is ignored and only the
	 * safety gate applies. A direct write that already put part of a packet on
	 * the wire has to be finished on the direct path even if an operator turns
	 * the knob off mid-stream.
	 *
	 * @return true when a writev direct send is permitted, false otherwise.
	 */
	bool polardb_can_writev_direct() const;

	/**
	 * @brief Report whether write_to_net_poll() should take the writev direct
	 *        path for the currently pending output.
	 *
	 * Combines the two separate questions: worthwhile
	 * (polardb_direct_write_batch_ready()) and safe (polardb_can_writev_direct()).
	 * Both must hold.
	 *
	 * @return true when the direct path should be taken, false to fall through to
	 *         the buffered writer.
	 */
	bool polardb_should_writev_direct() const;

	/**
	 * @brief Send pending output over the writev direct path, checking first that
	 *        it is both worthwhile and safe.
	 *
	 * Re-checks polardb_direct_write_batch_ready() and polardb_can_writev_direct()
	 * and declines if either fails, then forwards to
	 * polardb_writev_to_net_poll_unchecked(). On success completed packets are
	 * retired from PSarrayOUT, pkts_sent and bytes_info advance, and a short write
	 * leaves polardb_write_head_partial pointing into the first surviving packet.
	 *
	 * @param byte_budget  Maximum number of bytes to offer the kernel; 0 selects
	 *                     polardb_direct_write_budget_bytes().
	 * @return  > 0 the number of bytes the kernel accepted; -1 the write would
	 *          block and pollout is enabled again; 0 either declined (fall back
	 *          to the buffered path) or a fatal write error, in which case
	 *          shut_soft() has already run. Callers seeing 0 must re-check
	 *          net_failure instead of assuming the buffered path is still viable.
	 */
	int polardb_writev_to_net_poll(size_t byte_budget);
#endif // POLARDB_PROXY
	bool available_data_out();
	void remove_pollout();
	void set_pollout();

	void set_net_failure();
	void setDSS_STATE_QUERY_SENT_NET();

	void setDSS(enum mysql_data_stream_status dss) {
		DSS = dss;
	}

	int read_pkts();

	void unplug_backend();

	void check_data_flow();
	int assign_fd_from_pgsql_conn();

#if POLARDB_PROXY
	size_t polardb_write_head_partial;
#endif // POLARDB_PROXY

	static unsigned char* copy_array_to_buffer(PtrSizeArray* resultset, size_t resultset_length, bool del);
	static void copy_buffer_to_resultset(PtrSizeArray* resultset, unsigned char* ptr, uint64_t size, 
		char current_transaction_state);

	// safe way to attach a PgSQL Connection
	void attach_connection(PgSQL_Connection* mc) {
		statuses.pgconnpoll_get++;
		myconn = mc;
		myconn->statuses.pgconnpoll_get++;
		mc->myds = this;
		encrypted = false; // this is the default
		// PMC-10005
		// we handle encryption for backend
		//
		// we have a similar code in MySQL_Connection
		// in case of ASYNC_CONNECT_SUCCESSFUL
		if (sess != NULL && sess->session_fast_forward) {
			// if frontend and backend connection use SSL we will set
			// encrypted = true and we will start using the SSL structure
			// directly from PGconn SSL structure.
			//
			// For futher details:
			// - without ssl: we use the file descriptor from pgsql connection
			// - with ssl: we use the SSL structure from pgsql connection
			if (myconn->is_connected() && myconn->get_pg_ssl_in_use()) {
				if (ssl == NULL) {
					encrypted = true;
					SSL* ssl_obj = myconn->get_pg_ssl_object();
					if (ssl_obj == NULL) assert(0); // Should not be null
					ssl = ssl_obj;
					rbio_ssl = BIO_new(BIO_s_mem());
					wbio_ssl = BIO_new(BIO_s_mem());
					SSL_set_bio(ssl, rbio_ssl, wbio_ssl);
				}
			}
		}
	}

	// safe way to detach a PgSQL Connection
	void detach_connection() {
		assert(myconn);
#if POLARDB_PROXY
		myconn->polardb_flush_parent_queries();
#endif
		myconn->statuses.pgconnpoll_put++;
		statuses.pgconnpoll_put++;
		myconn->myds = NULL;
		myconn = NULL;
		if (encrypted == true) {
			if (sess != NULL && sess->session_fast_forward) {
				// it seems we are a connection with SSL on a fast_forward session.
				// See attach_connection() for more details .
				// We now disable SSL metadata from the Data Stream
				encrypted = false;
				ssl = NULL;
			}
		}
	}

	void return_MySQL_Connection_To_Pool();

	void destroy_MySQL_Connection_From_Pool(bool sq);
	void free_pgsql_real_query();
	void reinit_queues();
	void destroy_queues();

	bool data_in_rbio();

	void reset_connection();
};
#endif /* PROXYSQL_PGSQL_DATA_STREAM_H */
