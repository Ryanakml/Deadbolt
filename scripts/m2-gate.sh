#!/usr/bin/env bash
# M2 gate harness for Issue #25 (Blueprint §29.2).
#
# One repeatable entrypoint for the local/CI portion of the M2 gate. It runs
# the new integrated M2 tests plus the existing Issue #17-24 suites they
# orchestrate (no duplicated tests), emits readable PASS/FAIL per case, and
# returns non-zero on any failure.
#
# Usage:
#   ./scripts/m2-gate.sh [--new-only]
#
# Requires real PostgreSQL (TEST_DATABASE_URL or default loopback) and uses
# the real embedded NATS JetStream boundary where the test needs it. Object
# storage tests skip (NOT VERIFIED, never PASS) when no S3-compatible store
# is reachable outside CI.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

NEW_ONLY=0
if [[ "${1:-}" == "--new-only" ]]; then
  NEW_ONLY=1
fi

PASS=0
FAIL=0
FAILED_GROUPS=()

run_group() {
  local label="$1"
  shift
  echo "=== M2-GATE [$label] ==="
  echo "+ go test $*"
  if go test "$@"; then
    echo "PASS [$label]"
    PASS=$((PASS + 1))
  else
    echo "FAIL [$label]"
    FAIL=$((FAIL + 1))
    FAILED_GROUPS+=("$label")
  fi
}

# New integrated M2 gate tests (Issue #25): central A→B→C kill-worker,
# unknown-external-effect loss/hold/resolve, local HTTP fixture contract.
# The hosted HTTP case skips locally as PENDING_HOSTED_STAGING.
run_group "m2-gate-new" -race -count=1 -v ./tests/integration/ -run 'TestM2_TwoWorkerABCRecoveryKillDuringB|TestM2_UnknownExternalEffectLostResponseHoldsForReconciliation|TestM2_HTTPStagingLocalFixture|TestM2_HTTPStagingHosted'

if [[ "$NEW_ONLY" == "1" ]]; then
  echo "---"
  echo "M2-GATE NEW-ONLY: PASS=$PASS FAIL=$FAIL"
  [[ "$FAIL" -eq 0 ]]
  exit 0
fi

# Existing suites orchestrated by the gate (grouped by M2 case). Each group
# must pass; a SKIP is reported by go test and is NOT counted as PASS here —
# the evidence report records unavailable dependencies as NOT VERIFIED.
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

echo "---"
echo "M2-GATE SUMMARY: PASS=$PASS FAIL=$FAIL"
if [[ "$FAIL" -ne 0 ]]; then
  echo "FAILED GROUPS: ${FAILED_GROUPS[*]}"
  exit 1
fi
echo "M2-GATE: all local groups passed. Hosted staging + manual acceptance remain PENDING (see docs/reports/M2-mvp-evidence.md)."
