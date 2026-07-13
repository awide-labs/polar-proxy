#!/usr/bin/env bash
set -u

if [ "$#" -lt 3 ]; then
	echo "usage: $0 <polardb-test-dir> <target-name> <command> [args...]" >&2
	exit 2
fi

POLARDB_TEST_DIR="$1"
TARGET_NAME="$2"
shift 2

OUTPUT_BASE="${POLARDB_MAKE_OUTPUT_DIR:-${POLARDB_OUTPUT_DIR:-$POLARDB_TEST_DIR/test_output}}"
RUNID_FILE="${POLARDB_RUNID_FILE:-$POLARDB_TEST_DIR/.runid}"

safe_target=$(printf '%s' "$TARGET_NAME" | tr -c 'A-Za-z0-9_.-' '_')
mkdir -p "$OUTPUT_BASE"

next_runid() {
	local runid=1
	if command -v flock >/dev/null 2>&1; then
		touch "$RUNID_FILE"
		exec 9<>"$RUNID_FILE"
		flock 9
		if [ -s "$RUNID_FILE" ]; then
			runid=$(cat "$RUNID_FILE")
			runid=$((runid + 1))
		fi
		printf '%s\n' "$runid" >"$RUNID_FILE"
		printf '%s\n' "$runid"
		return 0
	fi

	if [ -f "$RUNID_FILE" ]; then
		runid=$(cat "$RUNID_FILE")
		runid=$((runid + 1))
	fi
	printf '%s\n' "$runid" >"$RUNID_FILE"
	printf '%s\n' "$runid"
}

RUN_ID=$(next_runid)
RUN_ID_PADDED=$(printf '%04d' "$RUN_ID")
RUN_DIR="$OUTPUT_BASE/run_${RUN_ID_PADDED}_make_${safe_target}"
LOG_FILE="$RUN_DIR/make.log"
STATUS_FILE="$RUN_DIR/status"
COMMAND_FILE="$RUN_DIR/command"

mkdir -p "$RUN_DIR"
printf '%q ' "$@" >"$COMMAND_FILE"
printf '\n' >>"$COMMAND_FILE"

export POLARDB_MAKE_RUN_DIR="$RUN_DIR"
export POLARDB_OUTPUT_DIR="$RUN_DIR"

{
	echo "================================================================================"
	echo "PolarDB make target: $TARGET_NAME"
	echo "Run ID: $RUN_ID"
	echo "Run dir: $RUN_DIR"
	echo "Started: $(date '+%Y-%m-%d %H:%M:%S')"
	echo "Command: $(cat "$COMMAND_FILE")"
	echo "================================================================================"
} | tee "$LOG_FILE"

set +e
"$@" 2>&1 | tee -a "$LOG_FILE"
status=${PIPESTATUS[0]}
set -e

{
	echo "================================================================================"
	echo "Finished: $(date '+%Y-%m-%d %H:%M:%S')"
	echo "Exit status: $status"
	echo "================================================================================"
} | tee -a "$LOG_FILE"

printf '%s\n' "$status" >"$STATUS_FILE"
exit "$status"
