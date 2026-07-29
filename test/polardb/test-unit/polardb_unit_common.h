/**
 * @file polardb_unit_common.h
 * @brief Shared helpers for the PolarDB unit tests.
 *
 * Choose support based on what the test constructs:
 *
 *   - Focused tests include this header for small helpers that depend only on
 *     <tap.h> and <PgSQL_PolarDB.h>.
 *
 *   - Component tests include polardb_unit_support.h and link
 *     polardb_unit_support.cpp when they need real ProxySQL objects.
 *
 * Keep this header lightweight so focused binaries do not acquire dependencies
 * on HostGroups Manager, sessions, or connections.
 */

#ifndef POLARDB_UNIT_COMMON_H
#define POLARDB_UNIT_COMMON_H

#include "tap.h"
#include "PgSQL_PolarDB.h"

#include <cstring>

/**
 * @brief Walk a PostgreSQL NoticeResponse ('N') packet looking for a field.
 *
 * Returns true when a field with the given single-byte @p code carries exactly
 * @p expected as its NUL-terminated value. Bounds-checked against @p size so a
 * malformed packet can never read past the buffer.
 */
static inline bool notice_packet_has_field(
	const unsigned char* pkt,
	unsigned int size,
	unsigned char code,
	const char* expected) {
	if (!pkt || size < 6 || !expected) {
		return false;
	}
	const unsigned char* field_ptr = pkt + 5;
	const unsigned char* packet_end = pkt + size;
	while (field_ptr < packet_end && *field_ptr != '\0') {
		const unsigned char field_code = *field_ptr++;
		const char* value = reinterpret_cast<const char*>(field_ptr);
		const size_t value_len = strlen(value);
		if (field_ptr + value_len >= packet_end) {
			return false;
		}
		if (field_code == code && strcmp(value, expected) == 0) {
			return true;
		}
		field_ptr += value_len + 1;
	}
	return false;
}

/// @brief Default V15 startup profile (requests RFQ LSN, emits startup params).
static inline PolarDB_StartupProfile make_v15_profile() {
	return PolarDB_StartupProfile::from_protocol(PolarDB_ProxyProtocol::V15);
}

/// @brief A valid non-wildcard IPv4 loopback startup identity on the default port.
static inline PolarDB_StartupIdentity make_loopback_identity() {
	return PolarDB_StartupIdentity{
		"127.0.0.1",
		5432,
		PolarDB_StartupIdentitySource::CONFIGURED_FALLBACK
	};
}

/**
 * @brief Assert the PolarDB_WriterScope::matches() invariant matrix.
 *
 * Both the RFQ-result-update check and the session-LSN scope check accept new
 * state only when the request scope matches the current writer scope. This
 * helper runs the shared invariant matrix; @p label names the consuming domain
 * (e.g. "RFQ result update", "session LSN scope") so failures stay diagnosable.
 */
static inline void check_writer_scope_match_matrix(const char* label) {
	constexpr int writer_hg = 10;
	constexpr int other_writer_hg = 11;
	constexpr int missing_hg = -1;
	constexpr uint64_t writer_epoch = 7;
	constexpr uint64_t changed_epoch = 8;

	ok(PolarDB_WriterScope{writer_hg, writer_epoch}.matches(
			PolarDB_WriterScope{writer_hg, writer_epoch}),
		"matching writer hostgroup and epoch accepts %s", label);
	ok(!PolarDB_WriterScope{missing_hg, writer_epoch}.matches(
			PolarDB_WriterScope{writer_hg, writer_epoch}),
		"missing request writer hostgroup rejects %s", label);
	ok(!PolarDB_WriterScope{writer_hg, writer_epoch}.matches(
			PolarDB_WriterScope{missing_hg, writer_epoch}),
		"missing current writer hostgroup rejects %s", label);
	ok(!PolarDB_WriterScope{writer_hg, writer_epoch}.matches(
			PolarDB_WriterScope{other_writer_hg, writer_epoch}),
		"same scalar epoch from another writer hostgroup rejects %s", label);
	ok(!PolarDB_WriterScope{writer_hg, writer_epoch}.matches(
			PolarDB_WriterScope{writer_hg, changed_epoch}),
		"same writer hostgroup with changed epoch rejects %s", label);
	const PolarDB_WriterScope initial_scope{writer_hg, 0};
	ok(initial_scope.valid(),
		"writer hostgroup makes epoch zero valid for %s", label);
	ok(initial_scope.matches(PolarDB_WriterScope{writer_hg, 0}),
		"matching epoch zero accepts %s", label);
	ok(!initial_scope.matches(PolarDB_WriterScope{writer_hg, 1}),
		"epoch zero rejects epoch one for %s", label);
}

#endif // POLARDB_UNIT_COMMON_H
