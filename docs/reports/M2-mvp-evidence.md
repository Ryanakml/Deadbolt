# M2 MVP Evidence Report — Issue #25

This report is the Issue #25 documentation deliverable: MVP evidence report
and failure matrix. It separates what is proven locally from what remains
pending on hosted staging and manual acceptance. A skipped or unrun test is
never reported as PASS; an unavailable dependency is NOT VERIFIED, not PASS.

Status vocabulary (per-issue §31.3, per-test execution only):

- IMPLEMENTED means the code path exists on this branch.
- AUTOMATED_LOCAL_VERIFIED means the named test passed locally against real
  PostgreSQL (and real embedded NATS JetStream / real worker sessions / real
  HTTP fixture where stated) on the HEAD recorded below.
- HOSTED_CI_VERIFIED means Foundation contracts succeeded on the named HEAD
  in GitHub Actions.
- HOSTED_STAGING_PENDING means the hosted staging step has not been run; it
  will be triggered manually after PR review.
- MANUAL_ACCEPTANCE_PENDING means the manual acceptance step has not been run.
- NOT VERIFIED means the evidence is missing (e.g. dependency unavailable).

Source-of-truth order for this PR: Frozen Blueprint, locked implementation
plan, Issue #25 contract, existing M2 implementations from Issues #17–#24,
existing tests/harnesses. New code exists only where integrated proof was
missing.

## Change impact

This PR does not redesign Deadbolt and adds no unrelated product features.
Delivery impact versus main (`cf0e1d9`):

- Tests (new): `tests/integration/m2_gate_test.go`
  - `TestM2_TwoWorkerABCRecoveryKillDuringB` — central Blueprint §29.2
    A→B→C kill-worker recovery through two actual `worker.Agent` processes
    with real Node child execution: Agent 1 runs A, starts B, is killed
    while B is observably in-flight (SIGTERM abort observed via marker),
    and Agent 2 recovers B and completes C without rerunning A.
  - `TestM2_UnknownExternalEffectLostResponseHoldsForReconciliation` —
    unknown-outcome hold using the disposable external-effect fixture with a
    separately-stored dedup ledger and an intentionally lost response.
  - `TestM2_HTTPStagingLocalFixture` / `TestM2_HTTPStagingHosted` — safe HTTP
    fixture contract; hosted half skips as PENDING_HOSTED_STAGING.
- Fixture (new): `tests/fixtures/httpstaging/fixture.go` — controlled,
  dependency-free safe HTTP target (`/health`, `/echo`, bounded `/slow`) plus
  `DEADBOLT_HTTP_STAGING_URL` staging configuration. No financial
  transaction, no third-party dependency.
- Harness (new): `scripts/m2-gate.sh` plus `scripts/lib/m2-count-results.mjs`
  — one repeatable entrypoint. Runs the new M2 tests plus orchestrates (not
  copies) the existing Issue #17–#24 suites grouped by M2 case. Every group
  runs via `go test -json` and is classified from the structured
  Action/Test fields (never human-readable text) as PASS (exit 0 with at
  least one test actually executed and passed), FAIL (non-zero exit), or
  NOT_VERIFIED (exit 0 but every required test skipped because a required
  boundary is unavailable). The gate prints
  `PASS=<n> FAIL=<n> NOT_VERIFIED=<n>` and exits non-zero when FAIL or
  NOT_VERIFIED is greater than zero, so it can never claim success while a
  required group only skipped. `--new-only` follows the same rule.
- Docs (new): this report (`docs/reports/M2-mvp-evidence.md`).

No database migration is added. No Issues #17–#24 invariants are weakened.
No M3 functionality is added. Existing run/step/attempt, lease/fencing,
recovery-policy, reconciliation, timeout/cancel, broker-sweep, artifact,
quota/fairness/log, and disaster-recovery semantics are unchanged.

## M2 gate harness

Repeatable local entrypoint (repository convention: Go integration suite plus
a script orchestrator; no new test framework):

```bash
# New integrated M2 tests only (fast):
./scripts/m2-gate.sh --new-only

# Full local M2 gate (new + orchestrated Issue #17–#24 suites):
./scripts/m2-gate.sh
```

Direct Go equivalents:

```bash
go test -race -count=1 -v ./tests/integration/ -run 'TestM2_TwoWorkerABCRecoveryKillDuringB|TestM2_UnknownExternalEffectLostResponseHoldsForReconciliation|TestM2_HTTPStagingLocalFixture|TestM2_HTTPStagingHosted'
```

Worker-process failure proven. Host-failure resilience not claimed: the gate
uses two actual worker Agents (authenticated enrollment, real bundle, real
Node child processes) on one host, which proves worker death / ownership
recovery semantics only. A host-resilience claim requires workers in two
separate failure domains (Issue #25 contract) and is explicitly out of scope
for this PR.

## Local gate results (this PR)

HEAD at local gate run: see `Head:` in the final PR report (branch
`feat/issue-25-m2-gate`). All groups below ran locally with
`go test -race` against real PostgreSQL on loopback and the real embedded
NATS JetStream boundary where stated.

| #   | M2 integrated case                                                                                                                                                                                                                                                                                                                                                                                                                      | Source                                      | Test / harness command                                                                                                                                                                                                                                                                                                                                                                                                                                            | Result                   |
| --- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------ |
| 1   | Two-worker A→B→C recovery: Agent 1 runs A (real child), starts B and is killed while B is observably in-flight (DB RUNNING + child marker), abort observed, ownership expires, B follows safe-retry policy, Agent 2 takes over with epoch+1 and stable operation ID, A not rerun at runtime (A x1, B-start x2, B-done x1, C x1, children 2/2) or in DB (A 1/1, B LOST+SUCCEEDED, C 1/1), final SUCCEEDED, API/DB/events/Inspector agree | #25 new (Blueprint §29.2; REQ-DUR-01; F-05) | `TestM2_TwoWorkerABCRecoveryKillDuringB` via `./scripts/m2-gate.sh` group `m2-gate-new`                                                                                                                                                                                                                                                                                                                                                                           | AUTOMATED_LOCAL_VERIFIED |
| 2   | Stale-worker rejection: after reassignment, old worker/session Start/heartbeat/Complete with stale ownership rejected                                                                                                                                                                                                                                                                                                                   | #17                                         | `TestWorkerReconnectRecoveryEndToEndSchedulable`, `TestWorkerClaimExpiryAndDurableRecovery`, `TestWorkerRunningLeaseExpiryAndDurableRecovery`, `TestWorkerSessionReauthenticationAndFencing`, `TestWorkerDBTimeBoundariesAndHeartbeatRequiresStart`; group `stale-worker` (the dead Agent in case 1 is not resurrected; protocol mutations stay covered here)                                                                                                     | AUTOMATED_LOCAL_VERIFIED |
| 3   | Unknown external effects: provider effect commits to separate ledger, definitive completion lost, no blind re-execution, reconciliation hold, operator resolution completes workflow, ledger stays singular                                                                                                                                                                                                                             | #19 + #24 fixture, #25 new                  | `TestM2_UnknownExternalEffectLostResponseHoldsForReconciliation` (uses `tests/fixtures/externaleffect` file-backed ledger) + `TestResolveConfirmSucceeded`, `TestPublishFailureReportsFailedUnknownAndHoldsReconcile`, `TestDisasterRecoveryExternalEffectSurvivesRestore`; groups `m2-gate-new`, `reconciliation-resolve`                                                                                                                                        | AUTOMATED_LOCAL_VERIFIED |
| 4   | Duplicate safety: duplicate create / broker delivery / completion-ACK stay logically singular; no external exactly-once claimed                                                                                                                                                                                                                                                                                                         | #12, #13, #17, #22                          | Group `duplicate-safety`: `TestCreateRunIdempotencyAnd202`, `TestCreateRunPreCommitFailureRollsBack`, `TestCreateRunPostCommitFailureReplaysSameID`, `TestConcurrentAuthenticatedClaimsHaveOneAuthority`, `TestCompletionPreCommitFailureRollsBack`, `TestCompletionPostCommitFailureReplaysWithoutDuplicateEffects`, `TestCompletionDigestConflictAfterCommit`, `TestDuplicateWakeupConsumer_SingularClaimEvidence`, `TestOutboxPublishCrashAndRedeliverySafety` | AUTOMATED_LOCAL_VERIFIED |
| 5   | Retry/timer durability: retry/backoff survives scheduler/process restart, due action unique, attempt budget correct                                                                                                                                                                                                                                                                                                                     | #18 (+ #21 restart)                         | Group `retry-timer`: `TestRetryBackoffPersistsTimerAndFires`, `TestRetryBudgetExhaustsToFailed`, `TestLeaseExpirySchedulesBackoffTimer`, `TestRetryAfterIncreasesDelay`, `TestIdempotencyWindowInsufficientHolds`, `TestAbsentWorkersSpendNoAttempt`                                                                                                                                                                                                              | AUTOMATED_LOCAL_VERIFIED |
| 6   | Timeout: configured timeout/deadline enforced through the execution boundary                                                                                                                                                                                                                                                                                                                                                            | #20                                         | Group `timeout-cancel` (timeout subset): `TestAttemptTimeoutFollowsPolicy`, `TestClaimBlockedPastDeadline`, `TestOverdueRunSweeperFailsRun`                                                                                                                                                                                                                                                                                                                       | AUTOMATED_LOCAL_VERIFIED |
| 7   | Cancel race: first valid terminal commit wins, stale completion cannot revive the run                                                                                                                                                                                                                                                                                                                                                   | #20                                         | Group `timeout-cancel` (cancel subset): `TestCancelHappyPathStopAckAndSettle`, `TestCancelCompletionFirstPreserved`, `TestCancelFirstRejectsLateResult`, `TestCancelDuplicateAndRevision`                                                                                                                                                                                                                                                                         | AUTOMATED_LOCAL_VERIFIED |
| 8   | Broker-down progress: NATS hints unavailable, committed PostgreSQL authoritative, DB sweep progresses eligible work                                                                                                                                                                                                                                                                                                                     | #21                                         | Group `broker-down`: `TestReconcileReadyWorkSurvivesBrokerDataLossAndIsIdempotent` (real JetStream stream created then erased), `TestSchedulerRestartRepairsCommittedCompletionWithoutBrokerHistory`, `TestReconcileReadyWorkFailsClosedWhenDatabaseUnavailable`, `TestNATSGracefulDegradationDoesNotBlockReadyz`                                                                                                                                                 | AUTOMATED_LOCAL_VERIFIED |
| 9   | DB outage conservative stop: no new authoritative ownership, no false lease renewal, worker/control plane stops conservatively                                                                                                                                                                                                                                                                                                          | #21                                         | Covered in group `broker-down` by `TestReconcileReadyWorkFailsClosedWhenDatabaseUnavailable` plus `TestReadyzDatabaseFailure` (deploy suite, run in full `go test -race ./...` validation)                                                                                                                                                                                                                                                                        | AUTOMATED_LOCAL_VERIFIED |
| 10  | Version pinning / missing worker: old run pinned to original deployment/bundle, new activation does not rewrite old semantics, missing compatible worker surfaced                                                                                                                                                                                                                                                                       | #10, #16, #18                               | Group `version-pinning`: `TestM1RunPinningAcrossDeploymentActivation`, `TestDeploymentRegistrationIsImmutableAndPersistsCompleteDefinitions`, `TestProductionActivationRequiresDistinctCompatibleWorkers`, `TestDeploymentAvailabilityLossWarningAndOutbox`; missing-worker behavior via `TestAbsentWorkersSpendNoAttempt` (group `retry-timer`)                                                                                                                  | AUTOMATED_LOCAL_VERIFIED |
| 11  | Tenant-negative cases: cross-tenant run/worker-session/artifact/SSE/API/DB-RLS scope denied without leakage                                                                                                                                                                                                                                                                                                                             | #8, #14                                     | Group `tenant-negatives`: `TestCrossTenantDenial`, `TestRLSFailsClosed`, `TestConnectionPoolReuseF27`, `TestRunInspectorCrossTenantIsolation`, `TestWorkerGatewayAuthAndScopeBoundaries`, `TestArtifactOwnershipAndIsolation`                                                                                                                                                                                                                                     | AUTOMATED_LOCAL_VERIFIED |
| 12  | Artifact behavior: READY integrity, missing/corrupt rejection, association/orphan/GC safety                                                                                                                                                                                                                                                                                                                                             | #22                                         | Group `artifacts`: `TestArtifactReservePutFinalizeAssociateDownload`, `TestArtifactFinalizeVerifiesSizeAndChecksum`, `TestArtifactOrphanGCCollectsSafely`, `TestArtifactCorruptBlocksConsumer`, `TestPublishFailureReportsFailedUnknownAndHoldsReconcile`                                                                                                                                                                                                         | AUTOMATED_LOCAL_VERIFIED |
| 13  | Quota/fairness/logs: env concurrency cap, worker slot cap, QUOTA_WAIT, create-run rate limiting, scheduler fairness, bounded log flood/drop counters, correctness events never dropped, worker drain/reconnect                                                                                                                                                                                                                          | #23                                         | Group `quota-fairness-logs-drain`: `TestClaimConcurrencyCapsAndQuotaWait`, `TestSchedulerFairnessRoundRobinAcrossEnvironments`, `TestCreateRunTokenBucketRefillAndAdmission429`, `TestWorkerDrainRefusesNewClaims`, `TestWorkerDrainGraceAndRunnerStop`, `TestWorkerDrainForceStopRecoversViaPolicy`, `TestLogPressurePreservesCorrectnessEvents`, `TestRunInspectorBoundedTaskLogs`                                                                              | AUTOMATED_LOCAL_VERIFIED |
| 14  | Disaster recovery: older DB restore, surviving external-effect ledger, READ_ONLY first, nonterminal holds, uncertainty window, session revocation, artifact integrity, rollback compatibility                                                                                                                                                                                                                                           | #24                                         | Group `disaster-recovery`: `TestDisasterRecoveryPointRestoresAndEntersHold`, `TestDisasterRecoveryExternalEffectSurvivesRestore`, `TestDisasterRecoveryGradualResumptionAndRPOGap`, `TestDisasterRecoveryIntegrityAndDeletionLedgerHooks`, `TestDisasterRecoveryPolicyResolutionPerTask`, `TestDisasterRecoveryArtifactIntegrityValid/Missing/Corrupt`                                                                                                            | AUTOMATED_LOCAL_VERIFIED |
| —   | Linear baseline (real Agent + Node child bundle execution)                                                                                                                                                                                                                                                                                                                                                                              | #11, #12                                    | Group `linear-baseline`: `TestLinearRunProgressionThroughWorkerAgent`, `TestLinearRunThroughActualAgentAndNodeChild` (real Agent poll → Start → verified tar bundle → real Node child → Complete, incl. lost-ACK identical replay)                                                                                                                                                                                                                                | AUTOMATED_LOCAL_VERIFIED |

Evidence identifiers: the new tests emit run ID, attempt IDs,
worker/session IDs, ownership epochs, event sequences, and final results via
`go test -v` log lines prefixed `M2-GATE-REAL`, `M2-UNKNOWN`, `M2-HTTP-LOCAL`
(e.g. central-test lines record B in-flight ownership, the agent-1 stop, the
`B-aborted` kill marker, runtime `calls=[A … B-start … B-aborted … B-done …
C]` with one operation ID, per-agent child counts, durable `B: LOST +
SUCCEEDED`, and the full event sequence `RUN_CREATED … RUN_COMPLETED`). IDs
are per-run UUIDs; see CI log for the exact values on the recorded HEAD. No
run IDs are invented in this report.

Gate accounting: every group result is one of PASS, FAIL, or NOT_VERIFIED as
defined in the harness section above; the gate exits non-zero unless every
required group PASSes.

## Failure matrix (Blueprint §28 + §29.2/§30 cumulative)

`AUTOMATED` below means AUTOMATED_LOCAL_VERIFIED on this branch unless noted.
Staging/manual columns stay pending until the operator runs them after merge.

| ID   | Failure / race                               | Required behavior                                                                     | Evidence (this PR)                                                                                                                                                                                                        | Automated                              | Hosted CI                                               | Hosted staging / manual |
| ---- | -------------------------------------------- | ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------- | ------------------------------------------------------- | ----------------------- |
| F-01 | API dies before/after create commit          | No run, or exactly one logical run via idempotency                                    | `TestCreateRunIdempotencyAnd202`, `TestCreateRunPreCommitFailureRollsBack`, `TestCreateRunPostCommitFailureReplaysSameID` (group `duplicate-safety`)                                                                      | AUTOMATED                              | HOSTED_CI_VERIFIED after push (pending at report write) | PENDING                 |
| F-02 | Outbox publish ok but marking fails          | Duplicate hint safe; claim stays singular                                             | `TestOutboxAtomicIntentAndPublishAckPrecedesMark`, `TestOutboxPublishCrashAndRedeliverySafety`, `TestDuplicateWakeupConsumer_SingularClaimEvidence`                                                                       | AUTOMATED                              | pending                                                 | PENDING                 |
| F-03 | All broker messages lost                     | DB sweep still runs ready work                                                        | `TestReconcileReadyWorkSurvivesBrokerDataLossAndIsIdempotent` (real JetStream boundary erased)                                                                                                                            | AUTOMATED                              | pending                                                 | PENDING                 |
| F-04 | Two workers claim concurrently               | One current lease; other gets no work                                                 | `TestConcurrentAuthenticatedClaimsHaveOneAuthority`                                                                                                                                                                       | AUTOMATED                              | pending                                                 | PENDING                 |
| F-05 | Worker dies mid-task                         | Lease expiry → retry/reconcile per policy                                             | `TestM2_TwoWorkerABCRecoveryKillDuringB` (two real Agents, Agent 1 killed in-flight), `TestWorkerClaimExpiryAndDurableRecovery`, `TestWorkerRunningLeaseExpiryAndDurableRecovery`, `TestLeaseExpirySchedulesBackoffTimer` | AUTOMATED                              | pending                                                 | PENDING                 |
| F-06 | Old worker alive after reassignment          | Stale Start/heartbeat/result rejected                                                 | `TestWorkerReconnectRecoveryEndToEndSchedulable` + group `stale-worker` (dead Agent in F-05 is not resurrected; mutations covered here)                                                                                   | AUTOMATED                              | pending                                                 | PENDING                 |
| F-07 | Provider succeeded, completion not committed | Idempotent replay or reconciliation hold; no blind retry                              | `TestM2_UnknownExternalEffectLostResponseHoldsForReconciliation` + `TestResolve*` suite + `TestPublishFailureReportsFailedUnknownAndHoldsReconcile`                                                                       | AUTOMATED                              | pending                                                 | PENDING                 |
| F-08 | Completion committed, ACK lost               | Identical result replays same ACK; digest conflict on difference                      | `TestCompletionPostCommitFailureReplaysWithoutDuplicateEffects`, `TestCompletionDigestConflictAfterCommit`, `TestLinearRunThroughActualAgentAndNodeChild` (lost-ACK identical replay, one Node child)                     | AUTOMATED                              | pending                                                 | PENDING                 |
| F-09 | Scheduler restart during retry/delay         | Due timestamp persists; action unique                                                 | `TestRetryBackoffPersistsTimerAndFires`, `TestSchedulerRestartRepairsCommittedCompletionWithoutBrokerHistory`                                                                                                             | AUTOMATED                              | pending                                                 | PENDING                 |
| F-10 | DB outage while worker active                | No renew/admit; agent stops conservatively                                            | `TestReconcileReadyWorkFailsClosedWhenDatabaseUnavailable`, `TestReadyzDatabaseFailure`                                                                                                                                   | AUTOMATED                              | pending                                                 | PENDING                 |
| F-11 | New deployment with old run active           | Old run uses old manifest/digest                                                      | `TestM1RunPinningAcrossDeploymentActivation` + deployment lifecycle tests                                                                                                                                                 | AUTOMATED                              | pending                                                 | PENDING                 |
| F-12 | Cancel vs completion                         | First valid commit wins; stale result cannot revive run                               | `TestCancelCompletionFirstPreserved`, `TestCancelFirstRejectsLateResult`                                                                                                                                                  | AUTOMATED                              | pending                                                 | PENDING                 |
| F-20 | Artifact upload ok, DB association fails     | Orphan GC; referenced READY never deleted                                             | `TestArtifactOrphanGCCollectsSafely`, `TestPublishFailureReportsFailedUnknownAndHoldsReconcile`                                                                                                                           | AUTOMATED                              | pending                                                 | PENDING                 |
| F-21 | Cross-tenant IDs/key/worker/artifact/SSE     | Deny without leakage                                                                  | Group `tenant-negatives`                                                                                                                                                                                                  | AUTOMATED                              | pending                                                 | PENDING                 |
| F-22 | Run/worker quota full                        | Reject admission or queue eligible work, bounded memory                               | `TestClaimConcurrencyCapsAndQuotaWait`, `TestCreateRunTokenBucketRefillAndAdmission429`                                                                                                                                   | AUTOMATED                              | pending                                                 | PENDING                 |
| F-23 | Agent upgrade/reconnect                      | Old session fenced; old bundle kept as needed                                         | `TestWorkerReconnectRecoveryEndToEndSchedulable`, drain/reconnect group                                                                                                                                                   | AUTOMATED                              | pending                                                 | PENDING                 |
| F-24 | DB restore older than side effects           | Disaster hold + reconciliation before resume                                          | Group `disaster-recovery`                                                                                                                                                                                                 | AUTOMATED                              | pending                                                 | PENDING                 |
| F-25 | Telemetry down / log flood                   | Correctness preserved; bounded drop + metric                                          | `TestLogPressurePreservesCorrectnessEvents`, `TestRunInspectorBoundedTaskLogs`                                                                                                                                            | AUTOMATED                              | pending                                                 | PENDING                 |
| F-13 | Pause vs retry/claim                         | Covered by M3 (out of M2 scope)                                                       | —                                                                                                                                                                                                                         | NOT APPLICABLE (M3)                    | —                                                       | —                       |
| F-14 | Dual approval / expiry race                  | Covered by M4 (out of M2 scope)                                                       | —                                                                                                                                                                                                                         | NOT APPLICABLE (M4)                    | —                                                       | —                       |
| F-15 | Parallel branch failure                      | Covered by M3 (out of M2 scope)                                                       | —                                                                                                                                                                                                                         | NOT APPLICABLE (M3)                    | —                                                       | —                       |
| F-16 | Skipped branch join                          | Covered by M3 (out of M2 scope)                                                       | —                                                                                                                                                                                                                         | NOT APPLICABLE (M3)                    | —                                                       | —                       |
| F-17 | Cron duplicate / downtime / DST              | Covered by M4 (out of M2 scope)                                                       | —                                                                                                                                                                                                                         | NOT APPLICABLE (M4)                    | —                                                       | —                       |
| F-18 | SSE disconnect/gap                           | Catch-up or resync; auth enforced                                                     | `TestRunInspectorSSEReconnectAndCatchUp`, `TestRunInspectorSSERetentionGapResync` (run in full validation)                                                                                                                | AUTOMATED (full `go test -race ./...`) | pending                                                 | PENDING                 |
| F-19 | Webhook receiver timeout                     | Covered by M5 (out of M2 scope)                                                       | —                                                                                                                                                                                                                         | NOT APPLICABLE (M5)                    | —                                                       | —                       |
| F-26 | Migration/rollout failure                    | `TestMigration22To23`, `TestMigrationFreshAnd21To23`, deploy guards (full validation) | AUTOMATED (full validation)                                                                                                                                                                                               | pending                                | PENDING                                                 |
| F-27 | Tenant context reuse in pool                 | `TestConnectionPoolReuseF27`, `TestRLSFailsClosed`                                    | AUTOMATED                                                                                                                                                                                                                 | pending                                | PENDING                                                 |
| F-28 | Output/schema/mapping invalid                | Non-retryable failure with safe detail                                                | Covered via pinning/invalidation tests (full validation)                                                                                                                                                                  | AUTOMATED (full validation)            | pending                                                 | PENDING                 |

No failures were manufactured for matrix rows. Rows marked NOT APPLICABLE are
Later-milestone capabilities (M3/M4/M5) outside the Issue #25 MVP contract.

## Real safe HTTP staging integration

- IMPLEMENTED: `tests/fixtures/httpstaging` fixture + `TestM2_HTTPStagingHosted`
  configuration via `DEADBOLT_HTTP_STAGING_URL`.
- AUTOMATED_LOCAL_VERIFIED: `TestM2_HTTPStagingLocalFixture` (loopback
  fixture: health ok, 50ms ok under 5s timeout, 2000ms delay observed as
  client timeout at 200ms through the real HTTP boundary).
- HOSTED_STAGING_PENDING: the real safe HTTP staging integration has NOT been
  run. After merge/review, the operator sets `DEADBOLT_HTTP_STAGING_URL` to
  the controlled staging fixture URL and runs:
  `go test ./tests/integration/ -run TestM2_HTTPStagingHosted -count=1 -v`.
  The HTTP target must be safe (no financial transaction, no third-party
  dependency): the controlled fixture or the staging control plane itself.

## Explicitly pending (do not treat as complete)

- DEPLOYED: pending (hosted staging deployment triggered separately after PR
  review; not part of this PR).
- STAGING_VERIFIED: pending.
- ACCEPTANCE_PROVEN: pending.
- M2 CLOSED: NO (Issue #25 stays open until hosted staging + manual
  acceptance complete; this PR uses `Refs #25`, not `Closes #25`).
- MANUAL_ACCEPTANCE: PENDING (Blueprint §29.2 repeat demonstration on hosted
  staging with two workers in two failure domains for any host-resilience
  claim; local two-process evidence does not cover it).
