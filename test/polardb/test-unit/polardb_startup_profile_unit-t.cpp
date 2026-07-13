/**
 * @file polardb_startup_profile_unit-t.cpp
 * @brief Unit tests for PolarDB startup-profile request semantics.
 *
 * Domain: startup-profile request bits and protocol mapping, fallback-identity
 * validation, sockaddr identity resolution, and the profile string converters.
 */

#include "tap.h"
#include "PgSQL_PolarDB.h"
#include "polardb_unit_common.h"

static void test_protocol_request_bits() {
	PolarDB_StartupProfile off =
		PolarDB_StartupProfile::from_protocol(PolarDB_ProxyProtocol::OFF);
	ok(!off.has_rfq_lsn(), "off profile does not request RFQ LSN");
	ok(!off.emits_startup_params(), "off profile emits no startup params");
	ok(off.protocol == PolarDB_ProxyProtocol::OFF, "off profile records OFF protocol");
	off.request_rfq_xid();
	ok(!off.has_rfq_xid(), "off profile does not accept RFQ XID requests");

	PolarDB_StartupProfile legacy =
		PolarDB_StartupProfile::from_protocol(PolarDB_ProxyProtocol::LEGACY);
	ok(legacy.has_rfq_lsn(), "legacy profile requests RFQ LSN");
	ok(legacy.has_rfq_xid(), "legacy profile requests RFQ XID");
	ok(legacy.emits_startup_params(), "legacy profile emits startup params");
	ok(!legacy.requests(REQUEST_RFQ_CSN), "legacy profile does not request RFQ CSN");

	PolarDB_StartupProfile v15 = make_v15_profile();
	ok(v15.has_rfq_lsn(), "v15 profile requests RFQ LSN");
	ok(v15.has_rfq_xid(), "v15 profile requests RFQ XID");
	ok(v15.emits_startup_params(), "v15 profile emits startup params");
	ok(!v15.requests(REQUEST_RFQ_CSN), "v15 profile does not request RFQ CSN");
	v15.request_rfq_xid();
	ok(v15.has_rfq_lsn(), "v15 profile keeps RFQ LSN when RFQ XID is requested again");
	ok(v15.has_rfq_xid(), "v15 profile keeps RFQ XID when requested again");

	PolarDB_StartupProfile csn_only = make_v15_profile();
	csn_only.request_bits = REQUEST_RFQ_CSN;
	ok(!csn_only.has_rfq_lsn(),
		"CSN-only reserved profile does not request RFQ LSN");
}

static void test_profile_components_distinguish_protocol_and_bits() {
	PolarDB_StartupProfile off =
		PolarDB_StartupProfile::from_protocol(PolarDB_ProxyProtocol::OFF);
	PolarDB_StartupProfile legacy =
		PolarDB_StartupProfile::from_protocol(PolarDB_ProxyProtocol::LEGACY);
	PolarDB_StartupProfile v15 =
		PolarDB_StartupProfile::from_protocol(PolarDB_ProxyProtocol::V15);
	ok(legacy.protocol != v15.protocol, "profile protocol distinguishes legacy and v15");

	PolarDB_StartupProfile lsn_only = make_v15_profile();
	PolarDB_StartupProfile lsn_csn = lsn_only;
	lsn_csn.request_bits |= REQUEST_RFQ_CSN;
	ok(lsn_only.request_bits != lsn_csn.request_bits,
		"profile request bits distinguish RFQ payload requests");
	ok(lsn_csn.requests(REQUEST_RFQ_CSN), "profile preserves RFQ CSN bit");
	ok(v15.generation(static_cast<int>(PolarDB_ProxyIdentityMode::CLIENT)) !=
			v15.generation(static_cast<int>(PolarDB_ProxyIdentityMode::PROXY)),
		"profile generation distinguishes CLIENT and PROXY identity modes");
	ok(off.generation(static_cast<int>(PolarDB_ProxyIdentityMode::CLIENT)) ==
			off.generation(static_cast<int>(PolarDB_ProxyIdentityMode::PROXY)),
		"off profile generation is identity-neutral");
	ok(polardb_startup_profile_matches_request_generation(
			v15,
			v15.generation(static_cast<int>(PolarDB_ProxyIdentityMode::CLIENT)),
			v15,
			static_cast<int>(PolarDB_ProxyIdentityMode::CLIENT)),
		"profile generation match uses the requested identity mode");
}

static void test_startup_parameters_consumed_by_proxy() {
	ok(polardb_startup_parameter_consumed_by_proxy("_polar_send_lsn"),
		"startup parameters: legacy LSN request is consumed by ProxySQL");
	ok(polardb_startup_parameter_consumed_by_proxy("_polar_proxy_send_lsn"),
		"startup parameters: v15 LSN request is consumed by ProxySQL");
	ok(polardb_startup_parameter_consumed_by_proxy("_polar_send_xact"),
		"startup parameters: legacy XID request is consumed by ProxySQL");
	ok(polardb_startup_parameter_consumed_by_proxy("_polar_proxy_send_xact"),
		"startup parameters: v15 XID request is consumed by ProxySQL");
	ok(polardb_startup_parameter_consumed_by_proxy("_polar_origin_client_ip"),
		"startup parameters: legacy client host is consumed by ProxySQL");
	ok(polardb_startup_parameter_consumed_by_proxy("_polar_origin_client_port"),
		"startup parameters: legacy client port is consumed by ProxySQL");
	ok(polardb_startup_parameter_consumed_by_proxy("_polar_proxy_client_host"),
		"startup parameters: v15 client host is consumed by ProxySQL");
	ok(polardb_startup_parameter_consumed_by_proxy("_polar_proxy_client_port"),
		"startup parameters: v15 client port is consumed by ProxySQL");
	ok(!polardb_startup_parameter_consumed_by_proxy("application_name"),
		"startup parameters: ordinary PostgreSQL parameter is not consumed by ProxySQL");
}

static void test_fallback_identity_validation() {
	const char* null_host = nullptr;
	ok(!PolarDB_StartupIdentity{}.valid(true),
		"fallback identity rejects missing host");
	PolarDB_StartupIdentity null_identity{
		null_host,
		5432,
		PolarDB_StartupIdentitySource::CONFIGURED_FALLBACK
	};
	ok(null_identity.host.empty(),
		"startup identity converts null host to empty string");
	ok(null_identity.source == PolarDB_StartupIdentitySource::CONFIGURED_FALLBACK,
		"startup identity constructor preserves source for null host");
	ok(!null_identity.valid(true),
		"startup identity with null host is invalid without crashing");
	ok(!PolarDB_StartupIdentity{"", 5432}.valid(true),
		"fallback identity rejects empty host");
	ok(!PolarDB_StartupIdentity{"127.0.0.1", 0}.valid(true),
		"fallback identity rejects zero port");
	ok(!PolarDB_StartupIdentity{"127.0.0.1", 65536}.valid(true),
		"fallback identity rejects out-of-range port");
	ok(!PolarDB_StartupIdentity{"db.example.com", 5432}.valid(true),
		"fallback identity rejects hostname");
	ok(!PolarDB_StartupIdentity{"db.example.com", 5432}.valid(false),
		"startup identity rejects hostname without wildcard filtering");
	ok(PolarDB_StartupIdentity{"127.0.0.1", 5432}.valid(true),
		"fallback identity accepts IPv4 loopback");
	ok(PolarDB_StartupIdentity{"192.0.2.10", 65535}.valid(true),
		"fallback identity accepts non-wildcard IPv4");
	ok(PolarDB_StartupIdentity{"::1", 5432}.valid(true),
		"fallback identity accepts IPv6 loopback");
	ok(PolarDB_StartupIdentity{"2001:db8::1", 5432}.valid(true),
		"fallback identity accepts non-wildcard IPv6");
	ok(!PolarDB_StartupIdentity{"0.0.0.0", 5432}.valid(true),
		"fallback identity rejects IPv4 wildcard");
	ok(!PolarDB_StartupIdentity{"::", 5432}.valid(true),
		"fallback identity rejects IPv6 wildcard");
	ok(!PolarDB_StartupIdentity{"::0", 5432}.valid(true),
		"fallback identity rejects IPv6 wildcard alias");
	ok(!PolarDB_StartupIdentity{"0:0:0:0:0:0:0:0", 5432}.valid(true),
		"fallback identity rejects expanded IPv6 wildcard");
	ok(!PolarDB_StartupIdentity{"::ffff:0.0.0.0", 5432}.valid(true),
		"fallback identity rejects IPv4-mapped wildcard");
	ok(PolarDB_StartupIdentity{"::ffff:192.0.2.10", 5432}.valid(true),
		"fallback identity accepts IPv4-mapped non-wildcard host");
}

static void test_sockaddr_identity_resolution() {
	PolarDB_StartupIdentity identity = make_loopback_identity();
	ok(!polardb_startup_identity_from_sockaddr(nullptr, &identity),
		"sockaddr identity rejects null address");
	ok(identity.host.empty() && identity.port == 0 &&
			identity.source == PolarDB_StartupIdentitySource::NONE,
		"sockaddr identity clears output for null address");
	ok(!polardb_startup_identity_from_sockaddr(nullptr, nullptr),
		"sockaddr identity rejects null output");

	struct sockaddr_in addr4 = {};
	addr4.sin_family = AF_INET;
	addr4.sin_port = htons(15432);
	inet_pton(AF_INET, "192.0.2.44", &addr4.sin_addr);
	ok(polardb_startup_identity_from_sockaddr(
			reinterpret_cast<const struct sockaddr*>(&addr4),
			&identity),
		"sockaddr identity parses IPv4");
	ok(identity.host == "192.0.2.44", "sockaddr identity keeps IPv4 host");
	ok(identity.port == 15432, "sockaddr identity keeps IPv4 port");
	ok(identity.source == PolarDB_StartupIdentitySource::CLIENT,
		"sockaddr identity defaults to client source");

	struct sockaddr_in6 addr6 = {};
	addr6.sin6_family = AF_INET6;
	addr6.sin6_port = htons(25432);
	inet_pton(AF_INET6, "2001:db8::5", &addr6.sin6_addr);
	ok(polardb_startup_identity_from_sockaddr(
			reinterpret_cast<const struct sockaddr*>(&addr6),
			&identity,
			PolarDB_StartupIdentitySource::LISTENER_PROXY),
		"sockaddr identity parses IPv6");
	ok(identity.host == "2001:db8::5", "sockaddr identity keeps IPv6 host");
	ok(identity.port == 25432, "sockaddr identity keeps IPv6 port");
	ok(identity.source == PolarDB_StartupIdentitySource::LISTENER_PROXY,
		"sockaddr identity accepts caller source");

	struct sockaddr_in wildcard4 = {};
	wildcard4.sin_family = AF_INET;
	wildcard4.sin_port = htons(16432);
	wildcard4.sin_addr.s_addr = htonl(INADDR_ANY);
	ok(polardb_startup_identity_from_sockaddr(
			reinterpret_cast<const struct sockaddr*>(&wildcard4),
			&identity),
		"sockaddr identity accepts observed IPv4 wildcard");
	ok(identity.host == "0.0.0.0", "sockaddr identity records wildcard host");
	ok(identity.port == 16432, "sockaddr identity records wildcard port");

	struct sockaddr_in zero_port = addr4;
	zero_port.sin_port = 0;
	identity = make_loopback_identity();
	ok(!polardb_startup_identity_from_sockaddr(
			reinterpret_cast<const struct sockaddr*>(&zero_port),
			&identity),
		"sockaddr identity rejects zero port");
	ok(identity.host.empty() && identity.port == 0 &&
			identity.source == PolarDB_StartupIdentitySource::NONE,
		"sockaddr identity clears output for zero port");

	struct sockaddr unsupported = {};
	unsupported.sa_family = AF_UNSPEC;
	identity = make_loopback_identity();
	ok(!polardb_startup_identity_from_sockaddr(&unsupported, &identity),
		"sockaddr identity rejects unsupported address family");
	ok(identity.host.empty() && identity.port == 0 &&
			identity.source == PolarDB_StartupIdentitySource::NONE,
		"sockaddr identity clears output for unsupported address family");
}

static void test_startup_client_context_reuse_key() {
	PolarDB_StartupClientContext base;
	base.identity = PolarDB_StartupIdentity{
		"192.0.2.10", 15432, PolarDB_StartupIdentitySource::CLIENT};
	ok(base.identity_valid_for_startup(false),
		"startup client context accepts real client identity");

	PolarDB_StartupClientContext same = base;
	ok(base.compatible_for_reuse(same),
		"startup client context matches identical identity and placeholders");

	PolarDB_StartupClientContext other_port = base;
	other_port.identity.port++;
	ok(!base.compatible_for_reuse(other_port),
		"startup client context rejects a different client port");

	PolarDB_StartupClientContext other_source = base;
	other_source.identity.source = PolarDB_StartupIdentitySource::LISTENER_PROXY;
	ok(!base.compatible_for_reuse(other_source),
		"startup client context rejects a different identity source");

	PolarDB_StartupClientContext ssl = base;
	ssl.frontend_ssl = true;
	ssl.ssl_version = "TLSv1.3";
	ssl.ssl_cipher = "TLS_AES_256_GCM_SHA384";
	ok(!base.compatible_for_reuse(ssl),
		"startup client context reserves SSL fields for future reuse matching");

	PolarDB_StartupClientContext sid = base;
	sid.has_proxy_session = true;
	sid.proxy_session_id = 10000001;
	sid.proxy_cancel_key = 42;
	ok(!base.compatible_for_reuse(sid),
		"startup client context reserves proxy session fields for future reuse matching");
}

static void test_configured_fallback_identity_set_validation() {
	ok(polardb_identity_config_valid("", 0, true),
		"configured fallback accepts empty host and unset port");
	ok(polardb_identity_config_valid("", 5432, true),
		"configured fallback accepts clearing host while port remains set");
	ok(polardb_identity_config_valid("127.0.0.1", 0, true),
		"configured fallback can stage non-wildcard IPv4 host before port");
	ok(!polardb_identity_config_valid("127.0.0.1", 0, false),
		"completed configured fallback rejects zero port");
	ok(!polardb_identity_config_valid("0.0.0.0", 0, true),
		"configured fallback rejects staged IPv4 wildcard host");
	ok(!polardb_identity_config_valid("::", 0, true),
		"configured fallback rejects staged IPv6 wildcard host");
	ok(!polardb_identity_config_valid("::ffff:0.0.0.0", 0, true),
		"configured fallback rejects staged IPv4-mapped wildcard host");
	ok(!polardb_identity_config_valid("db.example.com", 5432, true),
		"configured fallback rejects hostname with port");
	ok(!polardb_identity_config_valid("db.example.com", 0, true),
		"configured fallback rejects staged hostname");
	ok(PolarDB_StartupIdentity{"127.0.0.1", 5432}.valid(true),
		"completed configured fallback accepts IPv4 host and port");
	ok(PolarDB_StartupIdentity{"::1", 5432}.valid(true),
		"completed configured fallback accepts IPv6 host and port");
	ok(!PolarDB_StartupIdentity{"", 5432}.valid(true),
		"completed configured fallback rejects empty host");
	ok(!PolarDB_StartupIdentity{"127.0.0.1", 0}.valid(true),
		"completed configured fallback rejects zero port");
}

static void test_string_converters() {
	const int fallback = -7;

	ok(polardb_consistency_mode_from_string(nullptr, fallback) == fallback,
		"consistency converter maps null to caller default");
	ok(polardb_consistency_mode_from_string("", fallback) == fallback,
		"consistency converter maps empty string to caller default");
	ok(polardb_consistency_mode_from_string("default", fallback) == fallback,
		"consistency converter maps default sentinel to caller default");
	ok(polardb_consistency_mode_from_string("off", fallback) ==
			static_cast<int>(PolarDB_ConsistencyMode::OFF),
		"consistency converter maps off");
	ok(polardb_consistency_mode_from_string("lsn", fallback) ==
			static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN),
		"consistency converter maps lsn");
	ok(polardb_consistency_mode_from_string("global_lsn", fallback) ==
			static_cast<int>(PolarDB_ConsistencyMode::GLOBAL_LSN),
		"consistency converter maps global_lsn");
	ok(polardb_consistency_mode_from_string("lsn_global", fallback) ==
			static_cast<int>(PolarDB_ConsistencyMode::GLOBAL_LSN),
		"consistency converter maps lsn_global alias");
	ok(polardb_consistency_mode_from_string("global", fallback) ==
			static_cast<int>(PolarDB_ConsistencyMode::GLOBAL_LSN),
		"consistency converter maps global alias");
	ok(polardb_consistency_mode_from_string("primary", fallback) ==
			static_cast<int>(PolarDB_ConsistencyMode::PRIMARY_ONLY),
		"consistency converter maps primary");
	ok(polardb_consistency_mode_from_string("bad", fallback) == fallback,
		"consistency converter maps unknown value to caller default");

	ok(polardb_proxy_protocol_from_string(nullptr, fallback) == fallback,
		"proxy protocol converter maps null to caller default");
	ok(polardb_proxy_protocol_from_string("default", fallback) == fallback,
		"proxy protocol converter maps default sentinel to caller default");
	ok(polardb_proxy_protocol_from_string("off", fallback) ==
			static_cast<int>(PolarDB_ProxyProtocol::OFF),
		"proxy protocol converter maps off");
	ok(polardb_proxy_protocol_from_string("legacy", fallback) ==
			static_cast<int>(PolarDB_ProxyProtocol::LEGACY),
		"proxy protocol converter maps legacy");
	ok(polardb_proxy_protocol_from_string("v15", fallback) ==
			static_cast<int>(PolarDB_ProxyProtocol::V15),
		"proxy protocol converter maps v15");
	ok(polardb_proxy_protocol_from_string("bad", fallback) == fallback,
		"proxy protocol converter maps unknown value to caller default");

	ok(polardb_wait_mode_from_string(nullptr, fallback) == fallback,
		"wait mode converter maps null to caller default");
	ok(polardb_wait_mode_from_string("best_effort", fallback) ==
			static_cast<int>(PolarDB_WaitMode::BEST_EFFORT),
		"wait mode converter maps best_effort");
	ok(polardb_wait_mode_from_string("strict", fallback) ==
			static_cast<int>(PolarDB_WaitMode::STRICT),
		"wait mode converter maps strict");
	ok(polardb_wait_mode_from_string("bad", fallback) == fallback,
		"wait mode converter maps unknown value to caller default");

	ok(polardb_route_rfq_policy_from_string(nullptr, fallback) == fallback,
		"RFQ policy converter maps null to caller default");
	ok(polardb_route_rfq_policy_from_string("best_effort", fallback) ==
			static_cast<int>(PolarDB_RfqRoutePolicy::BEST_EFFORT),
		"RFQ policy converter maps best_effort");
	ok(polardb_route_rfq_policy_from_string("strict", fallback) ==
			static_cast<int>(PolarDB_RfqRoutePolicy::STRICT),
		"RFQ policy converter maps strict");
	ok(polardb_route_rfq_policy_from_string("bad", fallback) == fallback,
		"RFQ policy converter maps unknown value to caller default");
	ok(polardb_route_rfq_policy_from_string("bad") ==
			static_cast<int>(PolarDB_RfqRoutePolicy::STRICT),
		"RFQ policy converter default fallback is strict");

	ok(polardb_session_lsn_baseline_from_string(nullptr, fallback) == fallback,
		"session LSN baseline converter maps null to caller default");
	ok(polardb_session_lsn_baseline_from_string("observed", fallback) ==
			static_cast<int>(PolarDB_SessionLsnBaseline::OBSERVED),
		"session LSN baseline converter maps observed");
	ok(polardb_session_lsn_baseline_from_string("primary", fallback) ==
			static_cast<int>(PolarDB_SessionLsnBaseline::PRIMARY),
		"session LSN baseline converter maps primary");
	ok(polardb_session_lsn_baseline_from_string("bad", fallback) == fallback,
		"session LSN baseline converter maps unknown value to caller default");
	ok(polardb_session_lsn_baseline_from_string("bad") ==
			static_cast<int>(PolarDB_SessionLsnBaseline::OBSERVED),
		"session LSN baseline converter default fallback is observed");
}

int main() {
	plan(116);
	test_protocol_request_bits();
	test_profile_components_distinguish_protocol_and_bits();
	test_startup_parameters_consumed_by_proxy();
	test_fallback_identity_validation();
	test_sockaddr_identity_resolution();
	test_startup_client_context_reuse_key();
	test_configured_fallback_identity_set_validation();
	test_string_converters();
	return exit_status();
}
