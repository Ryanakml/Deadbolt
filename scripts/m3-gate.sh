#!/usr/bin/env bash
# M3 gate harness for Issue #31 (Blueprint §29.3).
#
# One repeatable entrypoint for the local/CI portion of the M3 gate. It runs
# the new integrated M3 tests plus the existing Issue #26-30 suites they
# orchestrate, emits readable PASS/FAIL/NOT_VERIFIED per case, and returns
# non-zero on any failure or unverified group.
#
# Status semantics (Issue #31 contract):
#   PASS         - go test exited 0 AND at least one required test actually
#                  executed and passed (a skip is never a pass).
#   FAIL         - test or command exited non-zero.
#   NOT_VERIFIED - test exited 0 but every required test SKIPPED because a
#                  required dependency/boundary was unavailable. An unavailable
#                  dependency is NOT VERIFIED, never PASS.
#
# Usage:
#   ./scripts/m3-gate.sh [--new-only]
#
# Requires real PostgreSQL (TEST_DATABASE_URL or default loopback) and uses
# real embedded NATS JetStream and live worker agents with Node child processes.
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
  echo "=== M3-GATE [$label] ==="
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
  echo "=== M3-GATE [$label] ==="
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
    echo "OVERALL_M3_GATE=FAIL"
    exit 1
  fi
  if [[ "$NOT_VERIFIED" -ne 0 ]]; then
    echo "NOT_VERIFIED GROUPS: ${UNVERIFIED_GROUPS[*]}"
    echo "LOCAL_AUTOMATED_GATE=NOT_VERIFIED"
    echo "OVERALL_M3_GATE=PARTIAL"
    echo "M3-GATE: required local evidence missing; unavailable dependency is NOT VERIFIED, not PASS."
    exit 1
  fi
}

# 1. Integrated M3 gate suite: linear, parallel diamond, fail-fast, choice/merge, nested merge,
#    mapping/schema validation, pause vs claim/completion/resume, stale revision, duplicate safety,
#    real 2-worker execution, and inspector parity.
run_group "m3-gate-new" -race -count=1 -v ./tests/integration/ -run '^TestM3_'

# 2. Property-based bounded DAG invariant tests and schema non-retryable invariants
run_group "m3-property" -race -count=1 -v ./internal/execution/ -run '^TestM3_Property'

if [[ "$NEW_ONLY" == "1" ]]; then
  print_summary
  fail_if_not_green
  echo "LOCAL_AUTOMATED_GATE=PASS (new-only)"
  echo "OVERALL_M3_GATE=PARTIAL"
  exit 0
fi

# 3. Parallel DAG execution (Issue #26)
run_group "parallel-dag" -race -count=1 ./tests/integration/ -run 'TestParallelDiamond|TestParallelFailFast|TestParallelStaleRecovery|TestReconcile_SkippedPropagation|TestReconcile_MixedSucceededSkippedTerminalizes|TestReconcile_MappingFailureAfterRestart|TestReconcile_InputSchemaMismatchAfterRestart|TestParallelRetry_RunStatePriority'

# 4. Structured Choice & Merge (Issue #27)
run_group "choice-merge" -race -count=1 ./tests/integration/ -run 'TestChoiceMerge_'

# 5. Control Races & Blocker sweeping (Issue #29)
run_group "control-races" -race -count=1 ./tests/integration/ -run 'TestPauseAndResumeHappyPath|TestPauseInFlightDrainsToPaused|TestPauseStatePrioritySuccessWins|TestPauseStatePriorityFailureWins|TestCancelAuthoritativeOverPaused|TestPauseAndResumeRevisionConflictAndDuplicates|TestPausePermissions|TestPauseSubsequentClaimBlocked|TestPausingLeaseExpiryRecoversAndDrains|TestPausingClaimStartDeadlineRecoversAndDrains|TestPausingAttemptTimeoutRecoversAndDrains|TestPausingRunDeadlineFailsRun|TestConcurrentPauseCancelSingleWinner|TestConcurrentPauseResumeNoCorruption|TestPauseClaimTransactionOrdering'

# 6. Run Inspector parity & Real retention (Issue #30)
run_group "inspector-parity" -race -count=1 ./tests/integration/ -run 'TestRunInspectorConsistentSnapshotAndStepAttempts|TestRunInspectorPayloadReadBoundaryAndRedaction|TestRunInspectorScopedListing|TestRunInspectorCrossTenantIsolation|TestRunInspectorSSEReconnectAndCatchUp|TestRunInspectorBoundedTaskLogs|TestRunInspectorHonestNoWorkerState|TestRunInspector_StepGraphMetadataAndRedaction'

# 7. Dashboard Browser & DOM suite (WCAG 2.2, Logical Graph, Control Dialogs, SSE convergence)
run_cmd "dashboard-suite" pnpm --filter @runtime/dashboard test

# 8. Cumulative M1/M2 regressions orchestrated by the gate
run_group "cumulative-regressions" -race -count=1 ./tests/integration/ -run 'TestM2_TwoWorkerABCRecoveryKillDuringB|TestM2_UnknownExternalEffectLostResponseHoldsForReconciliation|TestLinearRunThroughActualAgentAndNodeChild|TestCreateRunIdempotencyAnd202|TestWorkerDrainForceStopRecoversViaPolicy|TestCrossTenantDenial'

print_summary
fail_if_not_green
echo "LOCAL_AUTOMATED_GATE=PASS"
echo "HOSTED_CI=PENDING"
echo "DEPLOYED=NO"
echo "HOSTED_ACCEPTANCE=NOT_VERIFIED"
echo "OVERALL_M3_GATE=PARTIAL"
echo "M3-GATE: all local automated groups passed. Staging hosted deployment and acceptance remain pending."
