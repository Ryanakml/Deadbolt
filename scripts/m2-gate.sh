#!/usr/bin/env bash
# M2 gate harness for Issue #25 (Blueprint §29.2).
#
# One repeatable entrypoint for the local/CI portion of the M2 gate. It runs
# the new integrated M2 tests plus the existing Issue #17-24 suites they
# orchestrate (no duplicated tests), emits readable PASS/FAIL/NOT_VERIFIED
# per case, and returns non-zero on any failure or unverified group.
#
# Status semantics (Issue #25 contract):
#   PASS         - go test exited 0 AND at least one required test actually
#                  executed and passed (a skip is never a pass).
#   FAIL         - go test exited non-zero.
#   NOT_VERIFIED - go test exited 0 but every required test SKIPPED because a
#                  required dependency/boundary was unavailable. An unavailable
#                  dependency is NOT VERIFIED, never PASS.
#
# The full gate exits non-zero when FAIL > 0 or NOT_VERIFIED > 0, so it can
# never claim "all local groups passed" while a required group only skipped.
#
# Usage:
#   ./scripts/m2-gate.sh [--new-only]
#
# Requires real PostgreSQL (TEST_DATABASE_URL or default loopback) and uses
# the real embedded NATS JetStream boundary where the test needs it.
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
# Prints "<passed> <skipped> <failed>" using only the structured Action/Test
# fields, never human-readable log text.
count_json_results() {
  node "$REPO_ROOT/scripts/lib/m2-count-results.mjs" "$1"
}

run_group() {
  local label="$1"
  shift
  echo "=== M2-GATE [$label] ==="
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

print_summary() {
  echo "---"
  echo "M2-GATE SUMMARY: PASS=$PASS FAIL=$FAIL NOT_VERIFIED=$NOT_VERIFIED"
}

fail_if_not_green() {
  if [[ "$FAIL" -ne 0 ]]; then
    echo "FAILED GROUPS: ${FAILED_GROUPS[*]}"
    exit 1
  fi
  if [[ "$NOT_VERIFIED" -ne 0 ]]; then
    echo "NOT_VERIFIED GROUPS: ${UNVERIFIED_GROUPS[*]}"
    echo "M2-GATE: required evidence missing; unavailable dependency is NOT VERIFIED, not PASS."
    exit 1
  fi
}

# New integrated M2 gate tests (Issue #25): central A→B→C kill-worker,
# unknown-external-effect loss/hold/resolve, local HTTP fixture contract.
# The hosted HTTP case skips locally as PENDING_HOSTED_STAGING, but the group
# still PASSes only because the other required tests actually execute.
run_group "m2-gate-new" -race -count=1 -v ./tests/integration/ -run 'TestM2_TwoWorkerABCRecoveryKillDuringB|TestM2_UnknownExternalEffectLostResponseHoldsForReconciliation|TestM2_HTTPStagingLocalFixture|TestM2_HTTPStagingHosted'

if [[ "$NEW_ONLY" == "1" ]]; then
  print_summary
  fail_if_not_green
  exit 0
fi

# Existing suites orchestrated by the gate (grouped by M2 case). A group that
# only skips is reported NOT_VERIFIED and fails the gate; the evidence report
# records unavailable dependencies as NOT VERIFIED, never PASS.
run_group "stale-worker" -race -count=1 ./tests/integration/ -run 'TestWorkerReconnectRecoveryEndToEndSchedulable|TestWorkerClaimExpiryAndDurableRecovery|TestWorkerRunningLeaseExpiryAndDurableRecovery|TestWorkerSessionReauthenticationAndFencing|TestWorkerDBTimeBoundariesAndHeartbeatRequiresStart'
run_group "duplicate-safety" -race -count=1 ./tests/integration/ -run 'TestCreateRunIdempotencyAnd202|TestCreateRunPreCommitFailureRollsBack|TestCreateRunPostCommitFailureReplaysSameID|TestConcurrentAuthenticatedClaimsHaveOneAuthority|TestCompletionPreCommitFailureRollsBack|TestCompletionPostCommitFailureReplaysWithoutDuplicateEffects|TestCompletionDigestConflictAfterCommit|TestDuplicateWakeupConsumer_SingularClaimEvidence|TestOutboxPublishCrashAndRedeliverySafety'
run_group "retry-timer" -race -count=1 ./tests/integration/ -run 'TestRetryBackoffPersistsTimerAndFires|TestRetryBudgetExhaustsToFailed|TestLeaseExpirySchedulesBackoffTimer|TestRetryAfterIncreasesDelay|TestIdempotencyWindowInsufficientHolds|TestAbsentWorkersSpendNoAttempt'
run_group "timeout-cancel" -race -count=1 ./tests/integration/ -run 'TestAttemptTimeoutFollowsPolicy|TestClaimBlockedPastDeadline|TestOverdueRunSweeperFailsRun|TestCancelHappyPathStopAckAndSettle|TestCancelCompletionFirstPreserved|TestCancelFirstRejectsLateResult|TestCancelDuplicateAndRevision'
run_group "broker-down" -race -count=1 ./tests/integration/ -run 'TestReconcileReadyWorkSurvivesBrokerDataLossAndIsIdempotent|TestSchedulerRestartRepairsCommittedCompletionWithoutBrokerHistory|TestReconcileReadyWorkFailsClosedWhenDatabaseUnavailable|TestNATSGracefulDegradationDoesNotBlockReadyz'
run_group "version-pinning" -race -count=1 ./tests/integration/ -run 'TestM1RunPinningAcrossDeploymentActivation|TestDeploymentRegistrationIsImmutableAndPersistsCompleteDefinitions|TestProductionActivationRequiresDistinctCompatibleWorkers|TestDeploymentAvailabilityLossWarningAndOutbox'
run_group "tenant-negatives" -race -count=1 ./tests/integration/ -run 'TestCrossTenantDenial|TestRLSFailsClosed|TestConnectionPoolReuseF27|TestRunInspectorCrossTenantIsolation|TestWorkerGatewayAuthAndScopeBoundaries|TestArtifactOwnershipAndIsolation'
run_group "artifacts" -race -count=1 ./tests/integration/ -run 'TestArtifactReservePutFinalizeAssociateDownload|TestArtifactFinalizeVerifiesSizeAndChecksum|TestArtifactOrphanGCCollectsSafely|TestArtifactCorruptBlocksConsumer|TestPublishFailureReportsFailedUnknownAndHoldsReconcile'
run_group "quota-fairness-logs-drain" -race -count=1 ./tests/integration/ -run 'TestClaimConcurrencyCapsAndQuotaWait|TestSchedulerFairnessRoundRobinAcrossEnvironments|TestCreateRunTokenBucketRefillAndAdmission429|TestWorkerDrainRefusesNewClaims|TestWorkerDrainGraceAndRunnerStop|TestWorkerDrainForceStopRecoversViaPolicy|TestLogPressurePreservesCorrectnessEvents|TestRunInspectorBoundedTaskLogs'
run_group "reconciliation-resolve" -race -count=1 ./tests/integration/ -run 'TestResolveConfirmSucceeded|TestResolveConfirmNotExecutedRetry|TestResolveFailRun|TestResolveRevisionConflictAndDoubleResolve|TestResolveConcurrentSingleWinner'
run_group "disaster-recovery" -race -count=1 ./tests/integration/ -run 'TestDisasterRecoveryPointRestoresAndEntersHold|TestDisasterRecoveryExternalEffectSurvivesRestore|TestDisasterRecoveryGradualResumptionAndRPOGap|TestDisasterRecoveryIntegrityAndDeletionLedgerHooks|TestDisasterRecoveryPolicyResolutionPerTask|TestDisasterRecoveryArtifactIntegrityValid|TestDisasterRecoveryArtifactIntegrityMissing|TestDisasterRecoveryArtifactIntegrityCorrupt'
run_group "linear-baseline" -race -count=1 ./tests/integration/ -run 'TestLinearRunProgressionThroughWorkerAgent|TestLinearRunThroughActualAgentAndNodeChild'

print_summary
fail_if_not_green
echo "M2-GATE: all local groups passed. Hosted staging + manual acceptance remain PENDING (see docs/reports/M2-mvp-evidence.md)."
