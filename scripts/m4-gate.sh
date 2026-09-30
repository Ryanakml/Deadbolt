#!/usr/bin/env bash
# M4 gate harness for Issue #36 (Blueprint §28, §30).
#
# One repeatable entrypoint for the local portion of the M4 gate. It runs
# the new combined M4 tests plus the existing Issue #32-35 suites they
# orchestrate, emits readable PASS/FAIL/NOT_VERIFIED per case, and returns
# non-zero on any failure or unverified group.
#
# Status semantics (Issue #36 contract):
#   PASS         - go test exited 0 AND at least one required test actually
#                  executed and passed (a skip is never a pass).
#   FAIL         - test or command exited non-zero.
#   NOT_VERIFIED - test exited 0 but every required test SKIPPED because a
#                  required dependency/boundary was unavailable. An unavailable
#                  dependency is NOT VERIFIED, never PASS.
#
# Usage:
#   ./scripts/m4-gate.sh [--new-only]
#
# Requires real PostgreSQL (TEST_DATABASE_URL or default loopback) and uses
# real embedded NATS JetStream where the suites need it.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

NEW_ONLY=0
if [[ "${1:-}" == "--new-only" ]]; then
  NEW_ONLY=1
fi

PASS=0
FAIL=0
NOT_VERIFIED=0
FAILED_GROUPS=()
UNVERIFIED_GROUPS=()

# Count executed vs skipped tests from deterministic `go test -json` output.
count_json_results() {
  node "$REPO_ROOT/scripts/lib/m3-count-results.mjs" "$1"
}

run_group() {
  local label="$1"
  shift
  echo "=== M4-GATE [$label] ==="
  echo "+ go test -json $*"
  local json_out
  json_out="$(mktemp)"
  local exit_code=0
  set +e
  go test -json "$@" >"$json_out" 2>&1
  exit_code=$?
  set -e
  local counts
  counts="$(count_json_results "$json_out")"
  local passed=0 skipped=0 failed=0
  read -r passed skipped failed <<<"$counts"
  rm -f "$json_out"
  if [[ "$exit_code" -ne 0 ]]; then
    echo "FAIL [$label] (go test exit=$exit_code passed=$passed skipped=$skipped failed=$failed)"
    FAIL=$((FAIL + 1))
    FAILED_GROUPS+=("$label")
  elif [[ "$passed" -eq 0 ]]; then
    echo "NOT_VERIFIED [$label] (go test exit=0 but passed=$passed skipped=$skipped; required boundary unavailable)"
    NOT_VERIFIED=$((NOT_VERIFIED + 1))
    UNVERIFIED_GROUPS+=("$label")
  else
    echo "PASS [$label] (passed=$passed skipped=$skipped failed=$failed)"
    PASS=$((PASS + 1))
  fi
}

run_cmd() {
  local label="$1"
  shift
  echo "=== M4-GATE [$label] ==="
  echo "+ $*"
  local exit_code=0
  set +e
  "$@"
  exit_code=$?
  set -e
  if [[ "$exit_code" -ne 0 ]]; then
    echo "FAIL [$label] (command exit=$exit_code)"
    FAIL=$((FAIL + 1))
    FAILED_GROUPS+=("$label")
  else
    echo "PASS [$label] (command exit=0)"
    PASS=$((PASS + 1))
  fi
}

print_summary() {
  echo "---"
  echo "LOCAL AUTOMATED GATE SUMMARY: PASS=$PASS FAIL=$FAIL NOT_VERIFIED=$NOT_VERIFIED"
}

fail_if_not_green() {
  if [[ "$FAIL" -ne 0 ]]; then
    echo "FAILED GROUPS: ${FAILED_GROUPS[*]}"
    echo "LOCAL_AUTOMATED_GATE=FAIL"
    echo "OVERALL_M4_GATE=FAIL"
    exit 1
  fi
  if [[ "$NOT_VERIFIED" -ne 0 ]]; then
    echo "NOT_VERIFIED GROUPS: ${UNVERIFIED_GROUPS[*]}"
    echo "LOCAL_AUTOMATED_GATE=NOT_VERIFIED"
    echo "OVERALL_M4_GATE=PARTIAL"
    echo "M4-GATE: required local evidence missing; unavailable dependency is NOT VERIFIED, not PASS."
    exit 1
  fi
}

# 1. Combined M4 gate suite: approval -> delay in one workflow across engine
#    restarts with exactly-once human and timer actions, stale-revision and
#    expiry refusal inside the combined graph, and schedule restart-once.
run_group "m4-gate-new" -race -count=1 -v ./tests/integration/ -run '^TestM4_'

# 2. Human approvals (Issue #32): waits without lease/attempt, idempotent and
#    conflicting decisions, database-time expiry, sweeper settlement, cancel
#    closure, restart survival, concurrency single-winner, cross-tenant.
run_group "m4-approvals" -race -count=1 ./tests/integration/ -run '^TestApproval_'

if [[ "$NEW_ONLY" == "1" ]]; then
  print_summary
  fail_if_not_green
  echo "LOCAL_AUTOMATED_GATE=PASS (new-only)"
  echo "OVERALL_M4_GATE=PARTIAL"
  exit 0
fi

# 3. Delay nodes and waiting deadlines (Issue #33).
run_group "m4-delays" -race -count=1 ./tests/integration/ -run '^TestDelay'

# 4. Recurring schedules and unique occurrences (Issue #34): service, HTTP,
#    occurrence engine, two-evaluator races, DST/coalesce/overlap/quota.
run_group "m4-schedules" -race -count=1 ./tests/integration/ -run 'TestSchedule|TestOccurrence'

# 5. Dashboard schedule UI + full dashboard suite (Issue #35).
run_cmd "dashboard-suite" pnpm --filter @runtime/dashboard test

# 6. Cumulative M3 regressions orchestrated by the gate (choice/merge
#    determinism, control-race single-winner, inspector parity).
#    Real-agent execution and the M2 two-worker recovery stay owned by the
#    M2/M3 gates (bundle- and host-dependent); this gate does not re-own
#    their flakes.
run_group "cumulative-regressions" -race -count=1 ./tests/integration/ -run 'TestChoiceMerge_ReconcilerReplayInvariance|TestChoiceMerge_MergeIgnoresUnselectedInternalDep|TestConcurrentPauseCancelSingleWinner|TestRunInspectorConsistentSnapshotAndStepAttempts'

print_summary
fail_if_not_green
echo "LOCAL_AUTOMATED_GATE=PASS"
echo "HOSTED_CI=PENDING"
echo "DEPLOYED=NO"
echo "HOSTED_ACCEPTANCE=NOT_VERIFIED"
echo "OVERALL_M4_GATE=PARTIAL"
echo "M4-GATE: all local automated groups passed. Staging hosted deployment and acceptance remain pending."
