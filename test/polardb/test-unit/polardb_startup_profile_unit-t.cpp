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

struct PolarDB_StringConversionCase {
	const char* input;
	int expected;
};

template <size_t Count>
static void check_string_conversions(
		const PolarDB_StringConversionCase (&cases)[Count],
		int (*converter)(const char*, int),
		int fallback,
		const char* category) {
	for (const PolarDB_StringConversionCase& test_case : cases) {
		ok(converter(test_case.input, fallback) == test_case.expected,
			"%s converter maps %s", category,
			test_case.input ? test_case.input : "null");
	}
}

static void test_protocol_request_bits() {
	PolarDB_StartupProfile off =
		PolarDB_StartupProfile::from_protocol(PolarDB_ProxyProtocol::OFF);
	ok(!off.requests_rfq_lsn(), "off profile does not request RFQ LSN");
	ok(!off.emits_startup_params(), "off profile emits no startup params");
	ok(off.protocol == PolarDB_ProxyProtocol::OFF, "off profile records OFF protocol");
	off.request_rfq_xid();
	ok(!off.requests_rfq_xid(), "off profile does not request RFQ XID data");

	PolarDB_StartupProfile legacy =
		PolarDB_StartupProfile::from_protocol(PolarDB_ProxyProtocol::LEGACY);
	ok(legacy.requests_rfq_lsn(), "legacy profile requests RFQ LSN");
	ok(legacy.requests_rfq_xid(), "legacy profile requests RFQ XID");
	ok(legacy.emits_startup_params(), "legacy profile emits startup params");
	ok(!legacy.requests(REQUEST_RFQ_CSN), "legacy profile does not request RFQ CSN");

	PolarDB_StartupProfile v15 = make_v15_profile();
	ok(v15.requests_rfq_lsn(), "v15 profile requests RFQ LSN");
	ok(v15.requests_rfq_xid(), "v15 profile requests RFQ XID");
	ok(v15.emits_startup_params(), "v15 profile emits startup params");
	ok(!v15.requests(REQUEST_RFQ_CSN), "v15 profile does not request RFQ CSN");
	v15.request_rfq_xid();
	ok(v15.requests_rfq_lsn(), "v15 profile keeps RFQ LSN when RFQ XID is requested again");
	ok(v15.requests_rfq_xid(), "v15 profile keeps RFQ XID when requested again");

	PolarDB_StartupProfile csn_only = make_v15_profile();
	csn_only.request_bits = REQUEST_RFQ_CSN;
	ok(!csn_only.requests_rfq_lsn(),
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

	const PolarDB_StringConversionCase consistency_cases[] = {
		{nullptr, fallback},
		{"", fallback},
		{"default", fallback},
		{"off", static_cast<int>(PolarDB_ConsistencyMode::OFF)},
		{"eventual", static_cast<int>(PolarDB_ConsistencyMode::EVENTUAL)},
		{"session_lsn", static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN)},
		{"global_lsn", static_cast<int>(PolarDB_ConsistencyMode::GLOBAL_LSN)},
		{"bad", fallback},
	};
	check_string_conversions(
		consistency_cases, polardb_consistency_mode_from_string, fallback,
		"consistency");

	const PolarDB_StringConversionCase protocol_cases[] = {
		{nullptr, fallback},
		{"default", fallback},
		{"off", static_cast<int>(PolarDB_ProxyProtocol::OFF)},
		{"legacy", static_cast<int>(PolarDB_ProxyProtocol::LEGACY)},
		{"v15", static_cast<int>(PolarDB_ProxyProtocol::V15)},
		{"bad", fallback},
	};
	check_string_conversions(
		protocol_cases, polardb_proxy_protocol_from_string, fallback,
		"proxy protocol");

	const PolarDB_StringConversionCase missing_lsn_cases[] = {
		{nullptr, fallback},
		{"primary", static_cast<int>(PolarDB_MissingLsnAction::PRIMARY)},
		{"warning",
			static_cast<int>(PolarDB_MissingLsnAction::WARNING)},
		{"error",
			static_cast<int>(PolarDB_MissingLsnAction::ERROR)},
		{"bad", fallback},
	};
	check_string_conversions(
		missing_lsn_cases, polardb_missing_lsn_action_from_string, fallback,
		"missing LSN action");

	const PolarDB_StringConversionCase wait_timeout_cases[] = {
		{nullptr, fallback},
		{"warning",
			static_cast<int>(
				PolarDB_LsnWaitTimeoutAction::WARNING)},
		{"primary",
			static_cast<int>(PolarDB_LsnWaitTimeoutAction::PRIMARY)},
		{"error",
			static_cast<int>(PolarDB_LsnWaitTimeoutAction::ERROR)},
		{"disconnect",
			static_cast<int>(
				PolarDB_LsnWaitTimeoutAction::DISCONNECT)},
		{"bad", fallback},
	};
	check_string_conversions(
		wait_timeout_cases, polardb_lsn_wait_timeout_action_from_string,
		fallback, "LSN wait timeout action");
	ok(strcmp(polardb_lsn_wait_timeout_action_name(
			PolarDB_LsnWaitTimeoutAction::WARNING), "warning") == 0,
		"warning timeout action has a stable name");
	ok(strcmp(polardb_lsn_wait_timeout_action_name(
			PolarDB_LsnWaitTimeoutAction::PRIMARY), "primary") == 0,
		"primary timeout action has a stable name");
	ok(strcmp(polardb_lsn_wait_timeout_action_name(
			PolarDB_LsnWaitTimeoutAction::ERROR), "error") == 0,
		"error timeout action has a stable name");
	ok(strcmp(polardb_lsn_wait_timeout_action_name(
			PolarDB_LsnWaitTimeoutAction::DISCONNECT), "disconnect") == 0,
		"disconnect timeout action has a stable name");

	const PolarDB_StringConversionCase connection_loss_cases[] = {
		{nullptr, fallback},
		{"replica_then_primary",
			static_cast<int>(
				PolarDB_ReplicaLossAction::
					REPLICA_THEN_PRIMARY)},
		{"replica_then_error",
			static_cast<int>(
				PolarDB_ReplicaLossAction::
					REPLICA_THEN_ERROR)},
		{"primary",
			static_cast<int>(
				PolarDB_ReplicaLossAction::PRIMARY)},
		{"error",
			static_cast<int>(
				PolarDB_ReplicaLossAction::ERROR)},
		{"disconnect",
			static_cast<int>(
				PolarDB_ReplicaLossAction::DISCONNECT)},
		{"bad", fallback},
	};
	check_string_conversions(
		connection_loss_cases,
		polardb_replica_loss_action_from_string,
		fallback, "replica connection loss action");

	const PolarDB_StringConversionCase reader_error_cases[] = {
		{nullptr, fallback},
		{"primary",
			static_cast<int>(PolarDB_ReplicaErrorAction::PRIMARY)},
		{"error",
			static_cast<int>(PolarDB_ReplicaErrorAction::ERROR)},
		{"disconnect",
			static_cast<int>(PolarDB_ReplicaErrorAction::DISCONNECT)},
		{"bad", fallback},
	};
	check_string_conversions(
		reader_error_cases, polardb_replica_error_action_from_string,
		fallback, "replica error action");

	ok(!polardb_timeout_action_allows_global_lsn(
			PolarDB_LsnWaitTimeoutAction::WARNING),
		"global_lsn rejects warning");
	ok(polardb_timeout_action_allows_global_lsn(
			PolarDB_LsnWaitTimeoutAction::PRIMARY),
		"global_lsn accepts primary");
	ok(polardb_timeout_action_allows_global_lsn(
			PolarDB_LsnWaitTimeoutAction::ERROR),
		"global_lsn accepts error");
	ok(polardb_timeout_action_allows_global_lsn(
			PolarDB_LsnWaitTimeoutAction::DISCONNECT),
		"global_lsn accepts disconnect");
	ok(polardb_consistency_policy_error(
			PolarDB_ConsistencyMode::GLOBAL_LSN,
			PolarDB_MissingLsnAction::WARNING,
			PolarDB_LsnWaitTimeoutAction::PRIMARY) != nullptr,
		"global_lsn rejects a missing-LSN warning");
	ok(polardb_hostgroup_lsn_source_error(
			PolarDB_ConsistencyMode::SESSION_LSN,
			PolarDB_ProxyProtocol::OFF) != nullptr,
		"session_lsn rejects a hostgroup without RFQ LSN replies");
	ok(polardb_hostgroup_lsn_source_error(
			PolarDB_ConsistencyMode::SESSION_LSN,
			PolarDB_ProxyProtocol::V15) == nullptr &&
			polardb_hostgroup_lsn_source_error(
				PolarDB_ConsistencyMode::SESSION_LSN,
				PolarDB_ProxyProtocol::LEGACY) == nullptr,
		"session_lsn accepts both RFQ-capable startup protocols");
	ok(polardb_hostgroup_lsn_source_error(
			PolarDB_ConsistencyMode::GLOBAL_LSN,
			PolarDB_ProxyProtocol::OFF) == nullptr,
		"global_lsn may use monitor group LSN without RFQ replies");
	ok(polardb_wait_mode_for_timeout_action(
			PolarDB_LsnWaitTimeoutAction::WARNING) ==
			PolarDB_WaitMode::BEST_EFFORT,
		"warning selects the backend best_effort wait mode");
	ok(polardb_wait_mode_for_timeout_action(
			PolarDB_LsnWaitTimeoutAction::PRIMARY) ==
			PolarDB_WaitMode::STRICT,
		"primary selects the backend strict wait mode");
	ok(polardb_wait_mode_for_timeout_action(
			PolarDB_LsnWaitTimeoutAction::ERROR) ==
			PolarDB_WaitMode::STRICT,
		"error selects the backend strict wait mode");
	ok(polardb_wait_mode_for_timeout_action(
			PolarDB_LsnWaitTimeoutAction::DISCONNECT) ==
			PolarDB_WaitMode::STRICT,
		"disconnect selects the backend strict wait mode");
	ok(polardb_transaction_split_timeout_action(
			PolarDB_LsnWaitTimeoutAction::WARNING, true) ==
			PolarDB_LsnWaitTimeoutAction::PRIMARY,
		"transaction split promotes warning to primary");
	ok(polardb_transaction_split_timeout_action(
			PolarDB_LsnWaitTimeoutAction::WARNING, false) ==
			PolarDB_LsnWaitTimeoutAction::ERROR,
		"transaction split without primary fallback promotes warning to error");
	ok(polardb_transaction_split_timeout_action(
			PolarDB_LsnWaitTimeoutAction::PRIMARY, true) ==
			PolarDB_LsnWaitTimeoutAction::PRIMARY,
		"transaction split keeps primary");
	ok(polardb_transaction_split_timeout_action(
			PolarDB_LsnWaitTimeoutAction::ERROR, true) ==
			PolarDB_LsnWaitTimeoutAction::ERROR,
		"transaction split keeps error");
	ok(polardb_transaction_split_timeout_action(
			PolarDB_LsnWaitTimeoutAction::DISCONNECT, true) ==
			PolarDB_LsnWaitTimeoutAction::DISCONNECT,
		"transaction split keeps disconnect");
}

static void test_read_target_and_fallback() {
	const int fallback = -7;
	const PolarDB_StringConversionCase target_cases[] = {
		{nullptr, fallback}, {"default", fallback},
		{"primary", static_cast<int>(PolarDB_ReadTarget::PRIMARY)},
		{"replica", static_cast<int>(PolarDB_ReadTarget::REPLICA)},
		{"bad", fallback},
	};
	check_string_conversions(
		target_cases, polardb_read_target_from_string, fallback,
		"read target");

	ok(polardb_read_target_from_int(0) ==
			PolarDB_ReadTarget::PRIMARY,
		"read target value 0 is primary");
	ok(polardb_read_target_from_int(1) ==
			PolarDB_ReadTarget::REPLICA,
		"read target value 1 is replica");
	ok(polardb_read_target_from_int(99) ==
			PolarDB_ReadTarget::PRIMARY,
		"unknown read target value fails closed to primary");

	const PolarDB_StringConversionCase fallback_cases[] = {
		{nullptr, fallback}, {"default", fallback},
		{"primary", static_cast<int>(PolarDB_ReadFallbackAction::PRIMARY)},
		{"error", static_cast<int>(PolarDB_ReadFallbackAction::ERROR)},
		{"bad", fallback},
	};
	check_string_conversions(
		fallback_cases, polardb_read_fallback_action_from_string, fallback,
		"read fallback action");

	struct AcquireCase {
		PolarDB_ReadFallbackAction fallback;
		PolarDB_ReaderStatus status;
		PolarDB_ReaderAcquireAction action;
		const char* description;
	};
	const AcquireCase acquire_cases[] = {
		{PolarDB_ReadFallbackAction::PRIMARY,
			PolarDB_ReaderStatus::READER_BUSY,
			PolarDB_ReaderAcquireAction::RETRY_READER,
			"capacity waits before applying fallback"},
		{PolarDB_ReadFallbackAction::ERROR,
			PolarDB_ReaderStatus::READER_GROUP_BUSY,
			PolarDB_ReaderAcquireAction::RETRY_READER,
			"group capacity waits before applying fallback"},
		{PolarDB_ReadFallbackAction::ERROR,
			PolarDB_ReaderStatus::READER_UNAVAILABLE,
			PolarDB_ReaderAcquireAction::RETURN_ERROR,
			"error fallback rejects primary routing"},
		{PolarDB_ReadFallbackAction::PRIMARY,
			PolarDB_ReaderStatus::READER_UNAVAILABLE,
			PolarDB_ReaderAcquireAction::USE_PRIMARY,
			"primary fallback routes an unavailable replica read to primary"},
	};
	for (const auto& test_case : acquire_cases) {
		ok(polardb_reader_acquire_action(
				test_case.fallback, test_case.status) == test_case.action,
			"%s", test_case.description);
	}
}

static void test_policy_profiles() {
	ok(polardb_profile_definitions().size() == 7,
		"the public profile table contains seven named profiles");
	ok(POLARDB_DEFAULT_PROFILE_DEFINITION.profile ==
			PolarDB_Profile::SESSION_FALLBACK,
		"the factory profile is session_fallback");

	const PolarDB_ParsedGlobalConfigValue factory;
	ok(factory.profile == static_cast<int>(
				POLARDB_DEFAULT_PROFILE_DEFINITION.profile) &&
			polardb_config_matches_profile(
				factory, POLARDB_DEFAULT_PROFILE_DEFINITION.profile),
		"the default configuration exactly matches the factory profile");

	for (const PolarDB_ProfileDefinition& definition :
			polardb_profile_definitions()) {
		PolarDB_ParsedGlobalConfigValue config;
		config.profile = static_cast<int>(PolarDB_Profile::CUSTOM);
		config.lsn_wait_timeout_ms = 4321;
		config.split_warmup_max_connections_per_request = 7;
		config.startup.generation = 42;
		config.startup.identity_mode = PolarDB_ProxyIdentityMode::CLIENT;
		config.startup.configured_identity = {
			"192.0.2.90", 15432, PolarDB_StartupIdentitySource::CONFIGURED_FALLBACK};

		ok(polardb_apply_profile(definition.profile, &config),
			"profile %s expands", definition.name);
		ok(config.profile == static_cast<int>(definition.profile) &&
				config.consistency_mode ==
					static_cast<int>(definition.consistency) &&
				config.read_target ==
					static_cast<int>(definition.read_target) &&
				config.read_fallback_action ==
					static_cast<int>(definition.read_fallback),
			"profile %s expands consistency and read placement", definition.name);
		ok(config.missing_lsn_action ==
					static_cast<int>(definition.missing_lsn) &&
				config.lsn_wait_timeout_action ==
					static_cast<int>(definition.lsn_timeout) &&
				config.replica_error_action ==
					static_cast<int>(definition.replica_error) &&
				config.replica_loss_action ==
					static_cast<int>(definition.replica_loss),
			"profile %s expands all failure actions", definition.name);
		ok(config.startup.proxy_protocol == definition.rfq_protocol &&
				config.monitor_lsn_updates == definition.monitor_lsn_updates &&
				config.split_warmup == definition.split_warmup,
			"profile %s expands RFQ monitoring and warmup", definition.name);
		ok(config.lsn_wait_timeout_ms == 4321 &&
				config.split_warmup_max_connections_per_request == 7,
			"profile %s preserves numeric tuning", definition.name);
		ok(config.startup.generation == 42 &&
				config.startup.identity_mode == PolarDB_ProxyIdentityMode::CLIENT &&
				config.startup.configured_identity.host == "192.0.2.90" &&
				config.startup.configured_identity.port == 15432,
			"profile %s preserves startup identity", definition.name);
		ok(polardb_config_matches_profile(config, definition.profile),
			"profile %s matches its expanded bundle", definition.name);
		ok(polardb_profile_from_string(definition.name, -1) ==
				static_cast<int>(definition.profile) &&
				strcmp(polardb_profile_name(definition.profile),
					definition.name) == 0,
			"profile %s has one parse/display spelling", definition.name);
	}

	ok(polardb_profile_from_string("custom", -1) ==
			static_cast<int>(PolarDB_Profile::CUSTOM) &&
			strcmp(polardb_profile_name(PolarDB_Profile::CUSTOM), "custom") == 0,
		"custom is the derived profile name");

	PolarDB_ParsedGlobalConfigValue custom;
	ok(polardb_apply_profile(PolarDB_Profile::SESSION_FALLBACK, &custom),
		"session_fallback expands before an override");
	PolarDB_ParsedGlobalConfigValue changed = custom;
	ok(polardb_update_profile_setting(
			&changed, "polardb_action_lsn_timeout", "error") ==
			PolarDB_ProfileSettingResult::UPDATED &&
			!polardb_same_profile_owned_settings(custom, changed),
		"an individual action changes the profile-owned bundle");
	changed.profile = static_cast<int>(PolarDB_Profile::CUSTOM);
	ok(polardb_global_policy_error(changed) == nullptr,
		"a valid individual override is represented as custom");

	PolarDB_ParsedGlobalConfigValue named_mismatch = changed;
	named_mismatch.profile =
		static_cast<int>(PolarDB_Profile::SESSION_FALLBACK);
	ok(polardb_global_policy_error(named_mismatch) != nullptr,
		"a named profile cannot describe a mismatched bundle");

	PolarDB_ParsedGlobalConfigValue global_warning;
	polardb_apply_profile(PolarDB_Profile::GLOBAL_FALLBACK, &global_warning);
	global_warning.profile = static_cast<int>(PolarDB_Profile::CUSTOM);
	global_warning.lsn_wait_timeout_action =
		static_cast<int>(PolarDB_LsnWaitTimeoutAction::WARNING);
	ok(polardb_global_policy_error(global_warning) != nullptr,
		"global_lsn cannot return stale data after a timeout warning");
	global_warning.lsn_wait_timeout_action =
		static_cast<int>(PolarDB_LsnWaitTimeoutAction::PRIMARY);
	global_warning.missing_lsn_action =
		static_cast<int>(PolarDB_MissingLsnAction::WARNING);
	ok(polardb_global_policy_error(global_warning) != nullptr,
		"global_lsn cannot use a reader without an LSN target");

	for (const PolarDB_ProfileSettingDefinition& setting :
			polardb_profile_settings()) {
		ok(polardb_profile_owns_setting(setting.name),
			"profile owns %s", setting.name);
	}
	ok(!polardb_profile_owns_setting("polardb_lsn_wait_timeout_ms") &&
			!polardb_profile_owns_setting(
				"polardb_split_warmup_max_connections_per_request") &&
			!polardb_profile_owns_setting("polardb_proxy_identity_host"),
		"profiles preserve numeric tuning and startup identity");
}

int main() {
	plan(NO_PLAN);
	test_protocol_request_bits();
	test_profile_components_distinguish_protocol_and_bits();
	test_startup_parameters_consumed_by_proxy();
	test_fallback_identity_validation();
	test_sockaddr_identity_resolution();
	test_startup_client_context_reuse_key();
	test_configured_fallback_identity_set_validation();
	test_string_converters();
	test_read_target_and_fallback();
	test_policy_profiles();
	return exit_status();
}
