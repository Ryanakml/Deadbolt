# M1 Execution Gate Acceptance — Issue #16

This report is the evidence index for the first real execution gate. It separates code/test evidence from staging acceptance; a passing unit test or clean image build is not treated as staging proof. Skipped or unrun tests are never reported as PASS.

## Change impact

This PR gates M1 execution through staging. Delivery impact versus main:

- Dashboard: cross-platform public-file copy (`copy-public.mjs`) and portability fix.
- CLI: doctor platform interpretation accepts separate OS and architecture fields; credential file hardened to mode `0600`.
- Storage test helper: database URL role and password preservation for isolated test databases.
- Deploy: Linux M1 acceptance image (`deploy/Dockerfile.m1-e2e`).
- Worker: session deployment advertisement is now a replacement set, not append-only history (F-11 fix). Empty advertisement clears compatibility and availability is always reconciled.
- Execution: test-only create-commit hooks (`SetBeforeCreateCommitHookForTest`, `SetAfterCreateCommitHookForTest`), nil in production, to prove F-01 deterministically.
- Tests: expanded M1 integration coverage for F-01, F-08 conflict, F-11 pinning, F-21 wrong-pool, plus existing F-02, F-04, F-18, F-27 evidence.

No database migration is added in this phase. No M2 durability semantics are changed. No Later-marked capabilities are implemented.

## Gate contract

The gate must use the public CLI/API to create one run, execute a real Node.js child process, commit each result before acknowledging completion, advance a linear A to B to C workflow, and expose the same committed state through the API, Inspector, and logs.

## Automated evidence

| Evidence | Status | Source |
| --- | --- | --- |
| SDK bundle and workflow contracts | AUTOMATED TEST PASS | pnpm filter sdk test, 208 tests passed |
| Node runner execution | AUTOMATED TEST PASS | pnpm filter runner test, 8 tests passed |
| CLI package tests | AUTOMATED TEST PASS | go test ./internal/cli in Linux acceptance image |
| CLI E2E assertions | IMPLEMENTED | tests/integration/cli_e2e_test.go checks deployment pinning, three successful steps, committed attempts, final output, committed logs |
| Local PostgreSQL-backed CLI E2E | AUTOMATED TEST PASS | TestCLIEndToEndDeveloperJourney completed init, build, deploy, two workers, activation, A to B to C, inspect, logs |
| Hosted staging execution | STAGING PENDING | Must run build-once staging workflow and record version, image digest, run ID, event sequence, redacted logs |
| Browser Inspector and SSE | BROWSER PENDING | Manual browser reconnect and auth evidence intentionally deferred to final staging |

## Requirement and failure-case matrix

Status vocabulary is explicit. AUTOMATED TEST PASS means the named test passed locally against real PostgreSQL (and real NATS where stated). HOSTED CI PASS means Foundation contracts succeeded on the named HEAD. STAGING PENDING and BROWSER PENDING mean manual acceptance has not been performed. ACCEPTANCE VERIFIED means automated boundary only unless staging is also recorded.

| Case | Requirement | Tests | Command | Boundary | Automated | CI | Staging and Acceptance |
| --- | --- | --- | --- | --- | --- | --- | --- |
| F-01 | API dies before and after create commit, exactly one logical run via idempotency | TestCreateRunIdempotencyAnd202, TestIdempotencyFailedMutationRollbackAndFailClosed, TestCreateRunPreCommitFailureRollsBack, TestCreateRunPostCommitFailureReplaysSameID | See F-01 commands block | Real execution CreateRun plus PostgreSQL idempotency record and run tables | AUTOMATED TEST PASS | HOSTED CI PASS on 4acd2d2 run 35429046982, final HEAD rerun required | STAGING PENDING, ACCEPTANCE AUTOMATED VERIFIED and STAGING PENDING |
| F-02 | Outbox publish succeeds but marking fails, duplicate delivery stays singular | TestOutboxAtomicIntentAndPublishAckPrecedesMark, TestOutboxPublishCrashAndRedeliverySafety, TestDuplicateWakeupConsumer_SingularClaimEvidence | See F-02 commands block | Real PostgreSQL plus real NATS JetStream publish, ACK before mark, redelivery to singular claim | AUTOMATED TEST PASS | HOSTED CI PASS on 4acd2d2 run 35429046982, final HEAD rerun required | STAGING PENDING, ACCEPTANCE AUTOMATED VERIFIED and STAGING PENDING |
| F-04 | Two workers claim concurrently, exactly one owns the step | TestConcurrentAuthenticatedClaimsHaveOneAuthority | go test ./tests/integration -run TestConcurrentAuthenticatedClaimsHaveOneAuthority -count=1 -v | Two authenticated worker sessions plus PostgreSQL authoritative claim path | AUTOMATED TEST PASS | HOSTED CI PASS on 4acd2d2 run 35429046982, final HEAD rerun required | STAGING PENDING, ACCEPTANCE AUTOMATED VERIFIED and STAGING PENDING |
| F-08 | Completion commits but ACK is lost, identical replay ACKed, different digest conflicts | TestCompletionPreCommitFailureRollsBack, TestCompletionPostCommitFailureReplaysWithoutDuplicateEffects, TestCompletionDigestConflictAfterCommit | See F-08 commands block | Real worker completion API plus PostgreSQL commit and replay path | AUTOMATED TEST PASS | HOSTED CI PASS on 4acd2d2 run 35429046982, final HEAD rerun required | STAGING PENDING, ACCEPTANCE AUTOMATED VERIFIED and STAGING PENDING |
| F-11 | New deployment while old run is active, old run stays pinned | TestM1RunPinningAcrossDeploymentActivation, TestWorkerAdvertisementReplacesCurrentSet | go test ./tests/integration -run TestM1RunPinningAcrossDeploymentActivation -count=1 -v | Public deployment, activation, run APIs plus real worker poll, claim, start, complete | AUTOMATED TEST PASS | HOSTED CI PASS on 4acd2d2 run 35429046982, final HEAD rerun required | STAGING PENDING, ACCEPTANCE AUTOMATED VERIFIED and STAGING PENDING |
| F-18 | SSE disconnect and gap, catch-up or resync with auth enforced | TestRunInspectorSSEReconnectAndCatchUp, TestRunInspectorSSERetentionGapResync, TestRunInspectorDeterministicSnapshotToSubscribeRace, TestRunInspectorCrossTenantStreamRejection | See F-18 commands block | Real SSE HTTP stream plus PostgreSQL events plus auth boundary | AUTOMATED TEST PASS | HOSTED CI PASS on 4acd2d2 run 35429046982, final HEAD rerun required | STAGING PENDING and BROWSER PENDING, ACCEPTANCE AUTOMATED VERIFIED and STAGING PENDING |
| F-21 | Cross-tenant and pool negatives fail closed without leakage | TestCrossTenantDenial, TestAuthoritativeResourceScopingForeignIDs, TestRunInspectorCrossTenantIsolation, TestRunInspectorCrossTenantStreamRejection, TestEnrollmentTokenRejectsForeignEnvironment, TestEnvironmentMismatchRejection, TestWrongPoolPollRejectsWithoutClaim | See F-21 commands block | Authenticated API, worker poll and claim, and SSE stream boundaries | AUTOMATED TEST PASS | HOSTED CI PASS on 4acd2d2 run 35429046982, final HEAD rerun required | STAGING PENDING, ACCEPTANCE AUTOMATED VERIFIED and STAGING PENDING |
| F-27 | Pooled connection reuse does not leak tenant context | TestConnectionPoolReuseF27, TestRLSFailsClosed | See F-27 commands block | PostgreSQL SET LOCAL plus RLS on reused physical connection | AUTOMATED TEST PASS | HOSTED CI PASS on 4acd2d2 run 35429046982, final HEAD rerun required | STAGING PENDING, ACCEPTANCE AUTOMATED VERIFIED and STAGING PENDING |
| F-28 | Invalid output is terminal non-retryable with safe detail | TestM1RunPinningAcrossDeploymentActivation | go test ./tests/integration -run TestM1RunPinningAcrossDeploymentActivation -count=1 -v | Public worker completion boundary plus PostgreSQL run snapshot | AUTOMATED TEST PASS | HOSTED CI PASS on 4acd2d2 run 35429046982, final HEAD rerun required | STAGING PENDING, ACCEPTANCE AUTOMATED VERIFIED and STAGING PENDING |

## Runnable commands per case

Commands below are exact. A SKIP is not acceptance. Long M1 pinning test takes about 80 seconds locally because zero-assignment polls wait for the poll timeout.

```bash
# F-01
go test ./tests/integration -run 'TestCreateRunIdempotencyAnd202$' -count=1 -v
go test ./tests/integration -run 'TestIdempotencyFailedMutationRollbackAndFailClosed$' -count=1 -v
go test ./tests/integration -run 'TestCreateRunPreCommitFailureRollsBack$' -count=1 -v
go test ./tests/integration -run 'TestCreateRunPostCommitFailureReplaysSameID$' -count=1 -v
```

```bash
# F-02 (real PostgreSQL plus real NATS JetStream)
go test ./tests/integration -run 'TestOutboxAtomicIntentAndPublishAckPrecedesMark$' -count=1 -v
go test ./tests/integration -run 'TestOutboxPublishCrashAndRedeliverySafety$' -count=1 -v
go test ./tests/integration -run 'TestDuplicateWakeupConsumer_SingularClaimEvidence$' -count=1 -v
```

```bash
# F-04
go test ./tests/integration -run TestConcurrentAuthenticatedClaimsHaveOneAuthority -count=1 -v
```

```bash
# F-08
go test ./tests/integration -run 'TestCompletionPreCommitFailureRollsBack$' -count=1 -v
go test ./tests/integration -run 'TestCompletionPostCommitFailureReplaysWithoutDuplicateEffects$' -count=1 -v
go test ./tests/integration -run 'TestCompletionDigestConflictAfterCommit$' -count=1 -v
```

```bash
# F-11 and F-28 regression
go test -race ./tests/integration -run TestM1RunPinningAcrossDeploymentActivation -count=1 -v
go test ./tests/integration -run TestWorkerAdvertisementReplacesCurrentSet -count=1 -v
```

```bash
# F-18
go test ./tests/integration -run 'TestRunInspectorSSEReconnectAndCatchUp$' -count=1 -v
go test ./tests/integration -run 'TestRunInspectorSSERetentionGapResync$' -count=1 -v
go test ./tests/integration -run 'TestRunInspectorDeterministicSnapshotToSubscribeRace$' -count=1 -v
go test ./tests/integration -run 'TestRunInspectorCrossTenantStreamRejection$' -count=1 -v
```

```bash
# F-21 including wrong-pool
go test ./tests/integration -run 'TestCrossTenantDenial$' -count=1 -v
go test ./tests/integration -run 'TestAuthoritativeResourceScopingForeignIDs$' -count=1 -v
go test ./tests/integration -run 'TestRunInspectorCrossTenantIsolation$' -count=1 -v
go test ./tests/integration -run 'TestRunInspectorCrossTenantStreamRejection$' -count=1 -v
go test ./tests/integration -run 'TestEnrollmentTokenRejectsForeignEnvironment$' -count=1 -v
go test ./tests/integration -run 'TestEnvironmentMismatchRejection$' -count=1 -v
go test ./tests/integration -run 'TestWrongPoolPollRejectsWithoutClaim$' -count=1 -v
```

```bash
# F-27
go test ./tests/integration -run 'TestConnectionPoolReuseF27$' -count=1 -v
go test ./tests/integration -run 'TestRLSFailsClosed$' -count=1 -v
```

```bash
# Full required validation
go test -race ./...
go vet ./...
pnpm lint
pnpm typecheck
pnpm test
pnpm check:contracts
pnpm check:parity
git diff --check
```

## Reproducible local acceptance

```bash
docker build -f deploy/Dockerfile.m1-e2e -t deadbolt-m1-e2e deploy
docker run --rm --add-host=host.docker.internal:host-gateway \
  -v "$PWD:/workspace" -w /workspace \
  -e TEST_DATABASE_URL=postgres://deadbolt_system:<password>@host.docker.internal:5432/deadbolt?sslmode=disable \
  deadbolt-m1-e2e test ./tests/integration -run TestCLIEndToEndDeveloperJourney -count=1 -v
```

The test must show two worker agents, a `SUCCEEDED` run, three successful steps, committed task logs, and final output containing `accountId` and `deliveryId`. A `SKIP` is not acceptance.

## Staging evidence required

Record the exact deployed commit SHA, version response, control-plane image digest, bundle and lock digests, deployment ID, worker IDs and advertised digest, run ID, attempt IDs, event sequence, and redacted inspect and log output. Also record negative tests for incompatible runtime, wrong tenant and pool, duplicate completion, and invalid output.

The hosted evidence must use the packaged `runtime` executable or the documented shell journey against the deployed full stack (PostgreSQL, NATS, worker child process, public API and CLI boundaries). It must also include browser Inspector, auth, and SSE evidence. The hosted workflow URL, deployed commit SHA, image digest, and redacted identifiers are intentionally left blank until that run is performed. Artifact isolation is not an active M1 artifact path and is documented as a limitation rather than invented coverage.

Do not record API keys, cookies, enrollment tokens, task secrets, authorization headers, or unredacted stack traces.

## Final Issue acceptance status

Automated M1 failure-matrix coverage is complete for F-01, F-02, F-04, F-08, F-11, F-18, F-21, F-27, and F-28 at the stated test boundaries. Hosted CI must be green on the exact final HEAD. Final hosted staging and manual browser acceptance remain pending and Issue #16 remains open.
