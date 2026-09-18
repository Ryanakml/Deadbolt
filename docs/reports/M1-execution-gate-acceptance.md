# M1 Execution Gate Acceptance — Issue #16

This report is the evidence index for the first real execution gate. It separates code/test evidence from staging acceptance; a passing unit test or clean image build is not treated as staging proof.

## Change impact

This PR adds the M1 acceptance coverage and small portability fixes needed to run it consistently. It does not add a database migration or change the deployment/run schema. The CLI credential file is tightened to mode `0600`, the doctor check accepts the manifest's separate OS and architecture fields, the dashboard public-file copy is cross-platform, and the acceptance image provides a reproducible Linux/Node test environment.

## Gate contract

The gate must use the public CLI/API to create one run, execute a real Node.js child process, commit each result before acknowledging completion, advance a linear `A → B → C` workflow, and expose the same committed state through the API, Inspector, and logs.

## Automated evidence

| Evidence                          | Status           | Source                                                                                                                                       |
| --------------------------------- | ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| SDK bundle and workflow contracts | PASS             | `pnpm --filter @runtime/sdk run test` — 208 tests passed.                                                                                    |
| Node runner execution             | PASS             | `pnpm --filter @runtime/runner run test` — 8 tests passed.                                                                                   |
| CLI package tests                 | PASS             | `go test ./internal/cli` in the Linux acceptance image.                                                                                      |
| CLI E2E assertions                | IMPLEMENTED      | `tests/integration/cli_e2e_test.go` checks deployment pinning, three successful steps, committed attempts, final output, and committed logs. |
| Local PostgreSQL-backed CLI E2E   | PASS             | `TestCLIEndToEndDeveloperJourney` completed init/build/deploy, two workers, activation, `A → B → C`, inspect, and logs.                      |
| Hosted staging execution          | NOT YET VERIFIED | Must run this PR through the build-once staging workflow and record `/version`, image digest, run ID, event sequence, and redacted logs.     |

## Requirement and failure-case matrix

The status below is deliberately conservative. `PASS (local)` means the named automated test passed against the local PostgreSQL-backed control plane; it is not a hosted-staging sign-off.

| Requirement / failure case | Automated evidence | Boundary covered | Status |
| --- | --- | --- | --- |
| F-01 create-run durability and idempotency | `TestCreateRunIdempotencyAnd202`, `TestIdempotencyFailedMutationRollbackAndFailClosed`; `go test ./tests/integration -run 'Test(CreateRunIdempotencyAnd202|IdempotencyFailedMutationRollbackAndFailClosed)$'` | Real API handler plus PostgreSQL transaction and idempotency record | PASS (local); hosted replay evidence pending |
| F-11 deployment pinning after activation | `TestM1RunPinningAcrossDeploymentActivation`; `go test ./tests/integration -run TestM1RunPinningAcrossDeploymentActivation -count=1 -v` | Public deployment/activation/run APIs plus real worker poll, claim, start, and completion handlers | PASS (local); hosted multi-version evidence pending |
| F-28 invalid task output is terminal and non-retryable | `TestM1RunPinningAcrossDeploymentActivation` completes a real claimed attempt with an invalid payload and asserts `OUTPUT_SCHEMA_VIOLATION` plus `retryable=false` in the committed attempt | Public worker completion boundary and PostgreSQL-backed run snapshot | PASS (local); hosted negative evidence pending |
| Wrong tenant / environment / pool | `TestCrossTenantDenial`, `TestEnvironmentMismatchRejection`, `TestWorkerClaimEnforcesEnvironmentConcurrencyQuota` | Authenticated API and worker claim boundaries | PASS (local); hosted negative evidence pending |
| Incompatible runtime or bundle | `TestM1RunPinningAcrossDeploymentActivation` rejects an undeployed digest; CLI E2E runs doctor/runtime checks | Worker advertisement and claim boundary; packaged CLI still needs staging proof | PASS (local); hosted negative evidence pending |
| Duplicate completion / retry safety | `TestCompletionPostCommitFailureReplaysWithoutDuplicateEffects`, `TestCompletionPreCommitFailureRollsBack`, `TestWorkerOperationIDStableAcrossRetriesAndUniqueAcrossRuns` | Real worker completion API and PostgreSQL commit/replay path | PASS (local); hosted replay evidence pending |
| Inspector, event history, logs, and SSE reconnect | `TestRunInspectorConsistentSnapshotAndStepAttempts`, `TestRunInspectorSSEReconnectAndCatchUp`, `TestRunInspectorBoundedTaskLogs`, `TestRunInspectorCrossTenantStreamRejection` | API snapshot/stream/log boundaries; browser UI/auth evidence still absent | PASS (local API); browser/staging evidence pending |

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

Record the exact deployed commit SHA, `/version` response, control-plane image digest, bundle and lock digests, deployment ID, worker IDs and advertised digest, run ID, attempt IDs, event sequence, and redacted inspect/log output. Also record negative tests for incompatible runtime, wrong tenant/pool, duplicate completion, and invalid output.

The hosted evidence must use the packaged `runtime` executable or the documented shell journey against the deployed full stack (PostgreSQL, NATS, worker child process, public API/CLI boundaries). It must also include browser Inspector/auth/SSE evidence. The hosted workflow URL, deployed commit SHA, image digest, and redacted identifiers are intentionally left blank until that run is performed.

Do not record API keys, cookies, enrollment tokens, task secrets, authorization headers, or unredacted stack traces.
