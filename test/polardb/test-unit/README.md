# PolarDB Unit Tests

## Test Types

Choose the smallest test form that exercises the behavior:

- **Focused tests** own `main()` and `plan()`, include
  `polardb_unit_common.h`, and avoid constructing ProxySQL components.
- **Component tests** link `libproxysql.a` and use real ProxySQL objects.
  Single-source component tests own their initialization and cleanup.
- **HGM domain tests** are linked into `polardb_hgm_lsn_unit-t`. A domain file
  defines a `run_polardb_*_tests()` function but does not call `plan()`,
  initialize global state, or clean it up. The HGM main performs those steps
  once and keeps `run_polardb_topology_shutdown_tests()` last.

## Shared Helpers

Use `polardb_unit_common.h` for value, packet, profile, and scope helpers.
Use `polardb_unit_support.h` only when a test needs HGM, session, worker, or
connection objects.

For an ordinary one-reader or two-reader topology, use
`stage_polardb_one_reader_test_topology()` or
`stage_polardb_two_reader_test_topology()`. Their returned pointers are borrowed
from HGM and remain valid only until topology replacement or HGM cleanup. Keep
explicit hostgroup IDs, addresses, and ports in each test so its topology is
visible and independent of execution order.

Use the lower-level `stage_polardb_topology*()` and row builders when the test
itself verifies reload, status, identity, or unusual topology construction.
Do not hide those inputs behind a generic setup helper.

`attach_test_frontend()` creates the standard frontend stream and connection
for a session and optionally associates a worker. Let normal session reset or
destruction release those objects unless the test specifically covers teardown.

`make_cached_reader_connection()` returns a caller-owned connection. After a
test inserts it with `unit_reader_pool_add_matching()` or
`unit_reader_pool_add_shared()`, remove it through the corresponding pool helper
before deleting it unless pool lifetime is the behavior under test.

Use `PolarDB_ThreadCounterSnapshot` for one counter at a time:

```cpp
PolarDB_ThreadCounterSnapshot before(
	worker, polardb_st_var_reader_pool_server_considered);

// Exercise one behavior.

ok(before.delta() == 1, "matching attempt counted once");
```

Helper names communicate their effect:

- `stage_` changes shared HGM configuration.
- `make_` constructs and returns an object.
- `find_` returns a borrowed object or metric.
- `unit_` exposes a narrow test-only production operation.

## Adding Tests

1. Add the test to the existing domain that owns the production behavior.
2. Reuse ordinary topology, frontend, connection, and counter helpers.
3. Keep unusual setup explicit when the setup itself is under test.
4. Use one TAP assertion for each invariant and give it a diagnostic name.
5. Do not add comments that duplicate assertion totals. For focused and
   single-source component tests, `plan()` is the only assertion total.
6. Add a new `run_polardb_*_tests()` declaration to
   `polardb_unit_domains.h` only when introducing a new domain.

Avoid a test DSL, automatic hostgroup/port allocation, or whole-counter-array
snapshots. They hide important test inputs and make failures harder to read.

## Build And Run

Build and run all C++ units plus the Python coverage-summary unit:

```bash
make -C test/polardb/test-unit -j128 check
```

Optional build modes use the same target:

```bash
make -C test/polardb/test-unit -j128 POLARDB_PROFILE=1 check
```
