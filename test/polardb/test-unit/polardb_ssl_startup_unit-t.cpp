/**
 * @file polardb_ssl_startup_unit-t.cpp
 * @brief Unit tests for PolarDB frontend SSL descriptor propagation.
 *
 * The TLS fixture completes a real OpenSSL handshake over memory BIOs, so the
 * captured version and cipher are genuinely negotiated values — the same
 * values the production frontend capture reads from client_myds->ssl.
 */

#include "tap.h"
#include "test_globals.h"
#include "test_init.h"

#include "proxysql.h"

#include "PgSQL_Connection.h"
#include "PgSQL_Data_Stream.h"
#include "PgSQL_PolarDB.h"
#include "PgSQL_Session.h"

#include "polardb_unit_common.h"

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#include <sstream>
#include <string>

#if !POLARDB_PROXY
int main() {
	plan(1);
	ok(1, "SKIP: PolarDB SSL unit test requires POLARDB_PROXY");
	return exit_status();
}
#else

namespace {

class InMemoryTlsPair {
public:
	InMemoryTlsPair() {
		server_ctx_ = SSL_CTX_new(TLS_server_method());
		client_ctx_ = SSL_CTX_new(TLS_client_method());
		make_self_signed_certificate(server_ctx_);
		SSL_CTX_set_verify(client_ctx_, SSL_VERIFY_NONE, nullptr);
		server_ssl_ = SSL_new(server_ctx_);
		client_ssl_ = SSL_new(client_ctx_);
		server_rb_ = BIO_new(BIO_s_mem());
		server_wb_ = BIO_new(BIO_s_mem());
		client_rb_ = BIO_new(BIO_s_mem());
		client_wb_ = BIO_new(BIO_s_mem());

		/* SSL_set_bio() transfers ownership of the BIOs to the SSL objects. */
		SSL_set_bio(server_ssl_, server_rb_, server_wb_);
		SSL_set_bio(client_ssl_, client_rb_, client_wb_);
		SSL_set_accept_state(server_ssl_);
		SSL_set_connect_state(client_ssl_);

		run_handshake();
	}

	~InMemoryTlsPair() {
		SSL_free(server_ssl_);
		SSL_free(client_ssl_);
		SSL_CTX_free(server_ctx_);
		SSL_CTX_free(client_ctx_);
	}

	InMemoryTlsPair(const InMemoryTlsPair &) = delete;
	InMemoryTlsPair &operator=(const InMemoryTlsPair &) = delete;

	SSL *server_ssl() const { return server_ssl_; }

	const char *negotiated_version() const {
		return SSL_get_version(server_ssl_);
	}

	const char *negotiated_cipher_name() const {
		return SSL_CIPHER_get_name(SSL_get_current_cipher(server_ssl_));
	}

private:
	static void make_self_signed_certificate(SSL_CTX *ctx) {
		EVP_PKEY *pkey = EVP_RSA_gen(2048);
		X509 *cert = X509_new();
		ASN1_INTEGER_set(X509_get_serialNumber(cert), 1);
		X509_set_version(cert, 2);
		X509_gmtime_adj(X509_getm_notBefore(cert), -3600);
		X509_gmtime_adj(X509_getm_notAfter(cert), 24 * 3600);
		X509_set_pubkey(cert, pkey);
		X509_NAME *name = X509_NAME_new();
		X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
			reinterpret_cast<const unsigned char *>("polardb-ssl-unit"), -1, -1, 0);
		X509_set_subject_name(cert, name);
		X509_set_issuer_name(cert, name);
		X509_NAME_free(name);
		X509_sign(cert, pkey, EVP_sha256());

		SSL_CTX_use_certificate(ctx, cert);
		SSL_CTX_use_PrivateKey(ctx, pkey);
		X509_free(cert);
		EVP_PKEY_free(pkey);
	}

	static void move_mem_bio_bytes(BIO *from, BIO *to) {
		char buf[4096];
		int n = BIO_read(from, buf, sizeof(buf));
		while (n > 0) {
			int off = 0;
			while (off < n) {
				off += BIO_write(to, buf + off, n - off);
			}
			n = BIO_read(from, buf, sizeof(buf));
		}
	}

	void run_handshake() {
		while (!(SSL_is_init_finished(server_ssl_) &&
				SSL_is_init_finished(client_ssl_))) {
			SSL_do_handshake(client_ssl_);
			SSL_do_handshake(server_ssl_);

			/* The client's output feeds the server's input and vice versa. */
			move_mem_bio_bytes(client_wb_, server_rb_);
			move_mem_bio_bytes(server_wb_, client_rb_);
		}
	}

	SSL_CTX *server_ctx_ = nullptr;
	SSL_CTX *client_ctx_ = nullptr;
	SSL *server_ssl_ = nullptr;
	SSL *client_ssl_ = nullptr;
	BIO *server_rb_ = nullptr;
	BIO *server_wb_ = nullptr;
	BIO *client_rb_ = nullptr;
	BIO *client_wb_ = nullptr;
};

}

struct PolarDB_ConnectionUnitAccess {
	static bool append_startup_params(PgSQL_Connection *conn,
		std::ostringstream &conninfo,
		const PolarDB_StartupProfile &profile,
		unsigned int hid) {
		return conn->polardb_append_startup_params(conninfo, profile, hid);
	}
};

namespace {

class StartupEmissionFixture {
public:
	PgSQL_Session *session = nullptr;
	PgSQL_Data_Stream *frontend = nullptr;
	PgSQL_Data_Stream *backend = nullptr;
	PgSQL_Connection *backend_conn = nullptr;

	StartupEmissionFixture() {
		session = new PgSQL_Session();

		session->connections_handler = true;
		frontend = new PgSQL_Data_Stream();
		frontend->init(MYDS_FRONTEND, session, 0);
		frontend->myconn = new PgSQL_Connection(true);
		frontend->addr.addr = strdup("127.0.0.1");
		frontend->addr.port = 5432;
		frontend->proxy_addr.addr = strdup("127.0.0.10");
		frontend->proxy_addr.port = 6033;
		session->client_myds = frontend;

		backend = new PgSQL_Data_Stream();
		backend->init(MYDS_BACKEND, session, 0);
		backend_conn = new PgSQL_Connection(false);
		backend->myconn = backend_conn;
		backend_conn->myds = backend;
	}

	~StartupEmissionFixture() {
		frontend->ssl = nullptr;
		frontend->encrypted = false;
		delete backend_conn;

		backend->myconn = nullptr;
		delete backend;

		delete session;
	}

	StartupEmissionFixture(const StartupEmissionFixture &) = delete;
	StartupEmissionFixture &operator=(const StartupEmissionFixture &) = delete;
};

bool conninfo_contains(const std::string &conninfo, const char *needle) {
	return conninfo.find(needle) != std::string::npos;
}

bool conninfo_lacks_all_ssl_keys(const std::string &conninfo) {
	return !conninfo_contains(conninfo, "_polar_proxy_use_ssl") &&
		!conninfo_contains(conninfo, "_polar_proxy_ssl_version") &&
		!conninfo_contains(conninfo, "_polar_proxy_ssl_cipher_name");
}

bool emit_startup_params(PolarDB_ProxyProtocol protocol, bool encrypted,
		SSL *frontend_ssl, std::string *conninfo_out,
		PolarDB_ProxyIdentityMode identity_mode =
			PolarDB_ProxyIdentityMode::CLIENT) {
	pgsql_thread___polardb_proxy_identity_mode =
		static_cast<int>(identity_mode);
	StartupEmissionFixture fixture;
	fixture.frontend->encrypted = encrypted;
	fixture.frontend->ssl = frontend_ssl;
	std::ostringstream conninfo;
	const PolarDB_StartupProfile profile =
		PolarDB_StartupProfile::from_protocol(protocol);
	if (!PolarDB_ConnectionUnitAccess::append_startup_params(
			fixture.backend_conn, conninfo, profile, 10)) {
		return false;
	}
	if (conninfo_out) {
		*conninfo_out = conninfo.str();
	}
	return true;
}

void test_v15_tls_client_emits_all_three_keys(const InMemoryTlsPair &tls) {
	std::string conninfo;
	ok(emit_startup_params(PolarDB_ProxyProtocol::V15, true, tls.server_ssl(),
		&conninfo),
		"V15 + TLS frontend: startup params appended");
	ok(conninfo_contains(conninfo, " _polar_proxy_use_ssl=true "),
		"V15 + TLS frontend: _polar_proxy_use_ssl=true emitted");
	const std::string expected_version =
		std::string("_polar_proxy_ssl_version='") + tls.negotiated_version() +
		"'";
	ok(conninfo_contains(conninfo, expected_version.c_str()),
		"V15 + TLS frontend: _polar_proxy_ssl_version carries the negotiated "
		"version");
	const std::string expected_cipher =
		std::string("_polar_proxy_ssl_cipher_name='") +
		tls.negotiated_cipher_name() + "'";
	ok(conninfo_contains(conninfo, expected_cipher.c_str()),
		"V15 + TLS frontend: _polar_proxy_ssl_cipher_name carries the negotiated "
		"cipher");
	ok(conninfo_contains(conninfo, "_polar_proxy_client_host=127.0.0.1") &&
		conninfo_contains(conninfo, "_polar_proxy_client_port=5432"),
		"V15 + TLS frontend: identity keys are still emitted alongside the SSL "
		"keys");
}

void test_v15_plaintext_client_emits_no_ssl_keys() {
	std::string conninfo;
	ok(emit_startup_params(PolarDB_ProxyProtocol::V15, false, nullptr, &conninfo),
		"V15 + plaintext frontend: startup params appended");
	ok(conninfo_lacks_all_ssl_keys(conninfo),
		"V15 + plaintext frontend: no SSL keys emitted");
	ok(conninfo_contains(conninfo, "_polar_proxy_client_host=127.0.0.1"),
		"V15 + plaintext frontend: identity keys still emitted");
}

void test_v15_proxy_identity_suppresses_ssl_keys(const InMemoryTlsPair &tls) {
	std::string conninfo;
	ok(emit_startup_params(PolarDB_ProxyProtocol::V15, true, tls.server_ssl(),
		&conninfo, PolarDB_ProxyIdentityMode::PROXY),
		"V15 + TLS frontend with proxy identity: startup params appended");
	ok(conninfo_lacks_all_ssl_keys(conninfo),
		"V15 + TLS frontend with proxy identity: no SSL keys emitted");
	ok(conninfo_contains(conninfo, "_polar_proxy_client_host=127.0.0.10") &&
		conninfo_contains(conninfo, "_polar_proxy_client_port=6033"),
		"V15 + TLS frontend with proxy identity: listener identity emitted");
}

void test_legacy_tls_client_emits_no_ssl_keys(const InMemoryTlsPair &tls) {
	std::string conninfo;
	ok(emit_startup_params(PolarDB_ProxyProtocol::LEGACY, true, tls.server_ssl(),
		&conninfo),
		"LEGACY + TLS frontend: startup params appended");
	ok(conninfo_lacks_all_ssl_keys(conninfo),
		"LEGACY + TLS frontend: no SSL keys emitted");
	ok(conninfo_contains(conninfo, "_polar_origin_client_ip=127.0.0.1"),
		"LEGACY + TLS frontend: legacy identity keys still emitted");
}

void test_v15_wait_tls_client_emits_all_three_keys(const InMemoryTlsPair &tls) {
	std::string conninfo;
	ok(emit_startup_params(PolarDB_ProxyProtocol::V15_WAIT, true,
		tls.server_ssl(), &conninfo),
		"V15_WAIT + TLS frontend: startup params appended");
	ok(conninfo_contains(conninfo, " _polar_proxy_use_ssl=true ") &&
		conninfo_contains(conninfo,
			(std::string("_polar_proxy_ssl_version='") +
				tls.negotiated_version() + "'").c_str()) &&
		conninfo_contains(conninfo,
			(std::string("_polar_proxy_ssl_cipher_name='") +
				tls.negotiated_cipher_name() + "'").c_str()),
		"V15_WAIT + TLS frontend: all three SSL keys emitted");
	ok(conninfo_contains(conninfo, "_pq_.polar_proxy_wait_v1=1"),
		"V15_WAIT + TLS frontend: extended wait key still emitted");
}

void test_off_profile_emits_no_startup_keys(const InMemoryTlsPair &tls) {
	std::string conninfo;
	ok(emit_startup_params(PolarDB_ProxyProtocol::OFF, true, tls.server_ssl(),
		&conninfo),
		"OFF + TLS frontend: startup params accepted");
	ok(!conninfo_contains(conninfo, "_polar_"),
		"OFF + TLS frontend: no PolarDB startup keys emitted");
}

void test_capture_requires_usable_cipher() {
	PgSQL_Data_Stream *stream = new PgSQL_Data_Stream();
	stream->init(MYDS_FRONTEND, nullptr, 0);
	stream->encrypted = true;
	stream->ssl = nullptr;
	PolarDB_StartupClientContext captured;
	polardb_capture_frontend_ssl_descriptor(stream, &captured);
	ok(!captured.frontend_ssl && captured.ssl_version.empty() &&
		captured.ssl_cipher.empty(),
		"capture: encrypted flag without a usable SSL object stays plaintext");
	delete stream;
}

void test_v15_encrypted_without_cipher_emits_no_ssl_keys() {
	std::string conninfo;
	ok(emit_startup_params(PolarDB_ProxyProtocol::V15, true, nullptr, &conninfo),
		"V15 + encrypted frontend without an SSL object: startup params appended");
	ok(conninfo_lacks_all_ssl_keys(conninfo),
		"V15 + encrypted frontend without an SSL object: no use_ssl=true with "
		"empty metadata");
}

}

int main() {
	plan(25);

	int rc = test_init_minimal();
	ok(rc == 0, "test_init_minimal() succeeds");
	rc = test_init_query_processor();
	ok(rc == 0, "test_init_query_processor() succeeds");
	rc = test_init_hostgroups();
	ok(rc == 0, "test_init_hostgroups() succeeds");

	InMemoryTlsPair tls;

	test_v15_tls_client_emits_all_three_keys(tls);
	test_v15_plaintext_client_emits_no_ssl_keys();
	test_v15_proxy_identity_suppresses_ssl_keys(tls);
	test_legacy_tls_client_emits_no_ssl_keys(tls);
	test_v15_wait_tls_client_emits_all_three_keys(tls);
	test_off_profile_emits_no_startup_keys(tls);
	test_capture_requires_usable_cipher();
	test_v15_encrypted_without_cipher_emits_no_ssl_keys();

	return exit_status();
}

#endif /* POLARDB_PROXY */
