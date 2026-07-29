#!/bin/bash
# shellcheck disable=SC2034 # Scenario globals are consumed by scenario_harness.sh.
#
# PolarDB short scenario: test-case2
#
# Purpose:
#   Validate session-LSN consistency. A write advances the session LSN and the
#   following read is protected by the LSN wait path unless policy or topology
#   forces the writer. Success variants run without induced lag; failure variants
#   hold replica replay behind the target. warning returns a WARNING
#   and may return stale reader data. primary retries the read on the primary
#   when no user result has reached the client.
#
# Configuration:
#   - consistency mode: session LSN
#   - transaction split: disabled
#   - LSN wait timeout action and expected outcome: selected by arguments
#
# Usage:
#   ./test-case2.sh -a -success   # warning, no lag
#   ./test-case2.sh -a -failure   # warning, lag -> WARNING
#   ./test-case2.sh -b -success   # primary, no lag
#   ./test-case2.sh -b -failure   # primary, lag -> primary retry
#   ./test-case2.sh -a -s         # warning, no lag (shortcut)
#   ./test-case2.sh -b -f         # primary, lag -> primary retry (shortcut)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/scenario_harness.sh"
parse_common_args "$@"

CASE_NUM=2
CASE_NAME="Session LSN"
CONSISTENCY_MODE=session_lsn
SPLIT_ENABLED=0
XACT_SPLIT=0
TEST_ID=2

run_test "Case 2: Session LSN ($LSN_WAIT_TIMEOUT_ACTION, $EXPECT_OUTCOME)" run_consistency_test
