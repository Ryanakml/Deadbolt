# M1 Execution Gate Acceptance — Issue #16

This report is the evidence index for the first real execution gate. It separates code/test evidence from staging acceptance; a passing unit test or clean image build is not treated as staging proof. Skipped or unrun tests are never reported as PASS.

Status vocabulary is explicit:

- IMPLEMENTED means the code path exists.
- AUTOMATED TEST PASS means the named test passed locally against real PostgreSQL (and real NATS where stated).
- HOSTED CI PASS means Foundation contracts succeeded on the named HEAD.
- DEPLOYED means the immutable staging workflow published and deployed the named commit.
- ACCEPTANCE VERIFIED is qualified per row as automated boundary only or hosted staging/manual as recorded.

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

The literal gate wording is satisfied as one task, then A to B to C. This report records a separate real one-node workflow followed immediately by a real three-step workflow on hosted staging.

## Exact-head validation

- Exact implementation HEAD: `f232f93b7db228861cb69f032b8a872392cd3061`
- Foundation contracts: run `35431949953`, workflow run `#203`, status `SUCCESS`
- Staging Immutable Deploy: `#37`, run `35432643158`, result `SUCCESS`
- Workflow URL: `https://github.com/Ryanakml/Deadbolt/actions/runs/35432643158`
- Deployed commit: `f232f93b7db228861cb69f032b8a872392cd3061`

## Automated evidence

| Evidence                          | Status              | Source                                                                                                                                |
| --------------------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| SDK bundle and workflow contracts | AUTOMATED TEST PASS | pnpm filter sdk test, 208 tests passed                                                                                                |
| Node runner execution             | AUTOMATED TEST PASS | pnpm filter runner test, 8 tests passed                                                                                               |
| CLI package tests                 | AUTOMATED TEST PASS | go test ./internal/cli in Linux acceptance image                                                                                      |
| CLI E2E assertions                | IMPLEMENTED         | tests/integration/cli_e2e_test.go checks deployment pinning, three successful steps, committed attempts, final output, committed logs |
| Local PostgreSQL-backed CLI E2E   | AUTOMATED TEST PASS | TestCLIEndToEndDeveloperJourney completed init, build, deploy, two workers, activation, A to B to C, inspect, logs                    |
| Hosted staging execution          | STAGING PASS        | Staging Immutable Deploy run 35432643158 SUCCESS on f232f93, /version commit and digest verified, runs below                          |
| Browser Inspector and SSE         | MANUAL PASS         | Hosted run 92d6ea0e-93f8-4c50-922c-1b5af8e76d79 Inspector SUCCEEDED, LIVE to RECONNECTING to LIVE, Last-Event-Id 13                   |

## Requirement and failure-case matrix

Status vocabulary is explicit. AUTOMATED TEST PASS means the named test passed locally against real PostgreSQL (and real NATS where stated). HOSTED CI PASS means Foundation contracts succeeded on the named HEAD. STAGING PASS means the hosted staging evidence below was observed. MANUAL PASS means witnessed browser/manual evidence below was observed. ACCEPTANCE VERIFIED is qualified per row as automated boundary only unless hosted staging/manual is also recorded.

| Case | Requirement                                                                            | Tests                                                                                                                                                                                                                                                                     | Command                                                                                        | Boundary                                                                                        | Automated           | CI                                        | Staging and Acceptance                                                                          |
| ---- | -------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- | ------------------- | ----------------------------------------- | ----------------------------------------------------------------------------------------------- |
| F-01 | API dies before and after create commit, exactly one logical run via idempotency       | TestCreateRunIdempotencyAnd202, TestIdempotencyFailedMutationRollbackAndFailClosed, TestCreateRunPreCommitFailureRollsBack, TestCreateRunPostCommitFailureReplaysSameID                                                                                                   | See F-01 commands block                                                                        | Real execution CreateRun plus PostgreSQL idempotency record and run tables                      | AUTOMATED TEST PASS | HOSTED CI PASS on f232f93 run 35431949953 | ACCEPTANCE AUTOMATED VERIFIED only                                                              |
| F-02 | Outbox publish succeeds but marking fails, duplicate delivery stays singular           | TestOutboxAtomicIntentAndPublishAckPrecedesMark, TestOutboxPublishCrashAndRedeliverySafety, TestDuplicateWakeupConsumer_SingularClaimEvidence                                                                                                                             | See F-02 commands block                                                                        | Real PostgreSQL plus real NATS JetStream publish, ACK before mark, redelivery to singular claim | AUTOMATED TEST PASS | HOSTED CI PASS on f232f93 run 35431949953 | ACCEPTANCE AUTOMATED VERIFIED only                                                              |
| F-04 | Two workers claim concurrently, exactly one owns the step                              | TestConcurrentAuthenticatedClaimsHaveOneAuthority                                                                                                                                                                                                                         | go test ./tests/integration -run TestConcurrentAuthenticatedClaimsHaveOneAuthority -count=1 -v | Two authenticated worker sessions plus PostgreSQL authoritative claim path                      | AUTOMATED TEST PASS | HOSTED CI PASS on f232f93 run 35431949953 | ACCEPTANCE AUTOMATED VERIFIED only                                                              |
| F-08 | Completion commits but ACK is lost, identical replay ACKed, different digest conflicts | TestCompletionPreCommitFailureRollsBack, TestCompletionPostCommitFailureReplaysWithoutDuplicateEffects, TestCompletionDigestConflictAfterCommit                                                                                                                           | See F-08 commands block                                                                        | Real worker completion API plus PostgreSQL commit and replay path                               | AUTOMATED TEST PASS | HOSTED CI PASS on f232f93 run 35431949953 | ACCEPTANCE AUTOMATED VERIFIED only, hosted duplicate completion not claimed                     |
| F-11 | New deployment while old run is active, old run stays pinned                           | TestM1RunPinningAcrossDeploymentActivation, TestWorkerAdvertisementReplacesCurrentSet                                                                                                                                                                                     | go test ./tests/integration -run TestM1RunPinningAcrossDeploymentActivation -count=1 -v        | Public deployment, activation, run APIs plus real worker poll, claim, start, complete           | AUTOMATED TEST PASS | HOSTED CI PASS on f232f93 run 35431949953 | ACCEPTANCE AUTOMATED VERIFIED only, plus hosted compatibility preflight noted below             |
| F-18 | SSE disconnect and gap, catch-up or resync with auth enforced                          | TestRunInspectorSSEReconnectAndCatchUp, TestRunInspectorSSERetentionGapResync, TestRunInspectorDeterministicSnapshotToSubscribeRace, TestRunInspectorCrossTenantStreamRejection                                                                                           | See F-18 commands block                                                                        | Real SSE HTTP stream plus PostgreSQL events plus auth boundary                                  | AUTOMATED TEST PASS | HOSTED CI PASS on f232f93 run 35431949953 | STAGING PASS plus MANUAL PASS, hosted cross-tenant SSE rejected and browser reconnect witnessed |
| F-21 | Cross-tenant and pool negatives fail closed without leakage                            | TestCrossTenantDenial, TestAuthoritativeResourceScopingForeignIDs, TestRunInspectorCrossTenantIsolation, TestRunInspectorCrossTenantStreamRejection, TestEnrollmentTokenRejectsForeignEnvironment, TestEnvironmentMismatchRejection, TestWrongPoolPollRejectsWithoutClaim | See F-21 commands block                                                                        | Authenticated API, worker poll and claim, and SSE stream boundaries                             | AUTOMATED TEST PASS | HOSTED CI PASS on f232f93 run 35431949953 | STAGING PASS, hosted wrong-pool 401 and cross-tenant 403 observed                               |
| F-27 | Pooled connection reuse does not leak tenant context                                   | TestConnectionPoolReuseF27, TestRLSFailsClosed                                                                                                                                                                                                                            | See F-27 commands block                                                                        | PostgreSQL SET LOCAL plus RLS on reused physical connection                                     | AUTOMATED TEST PASS | HOSTED CI PASS on f232f93 run 35431949953 | ACCEPTANCE AUTOMATED VERIFIED only                                                              |
| F-28 | Invalid output is terminal non-retryable with safe detail                              | TestM1RunPinningAcrossDeploymentActivation                                                                                                                                                                                                                                | go test ./tests/integration -run TestM1RunPinningAcrossDeploymentActivation -count=1 -v        | Public worker completion boundary plus PostgreSQL run snapshot                                  | AUTOMATED TEST PASS | HOSTED CI PASS on f232f93 run 35431949953 | STAGING PASS, hosted invalid bundle terminal FAILED OUTPUT_SCHEMA_VIOLATION                     |

F-08 is automated real-PostgreSQL evidence only. No manual hosted duplicate-completion probe is claimed. The final automated test proves completion is committed exactly once, identical completion replay is accepted idempotently, a different result digest returns `RESULT_CONFLICT`, and committed terminal, event, and outbox effects remain singular.

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

## Staging evidence

Build-once immutable staging deployment was performed on the exact HEAD. No staging identifiers below are invented.

### Immutable deployment

- Workflow: Staging Immutable Deploy `#37`
- Workflow URL: `https://github.com/Ryanakml/Deadbolt/actions/runs/35432643158`
- Run ID: `35432643158`
- Result: `SUCCESS`
- Deployed commit: `f232f93b7db228861cb69f032b8a872392cd3061`
- Public staging: `https://deadbolt.43.218.246.246.nip.io`
- Control-plane image: `ghcr.io/ryanakml/deadbolt/control-plane@sha256:09d75e29673ea46ffed034fc1f7c444eeaa5ce1f284c5afb80cd767987969044`
- PostgreSQL image: `ghcr.io/ryanakml/deadbolt/postgres@sha256:22941091ad8d8526518531d384de7698f4791c5d6bc5d956487a9664f5c9f2ad`
- `/version` returned: `version 0.1.0`, `commit f232f93b7db228861cb69f032b8a872392cd3061`, `build_time 9faa23f7895a2a201ec85a8269dd94c9c23c7932`, `image_digest ghcr.io/ryanakml/deadbolt/control-plane@sha256:09d75e29673ea46ffed034fc1f7c444eeaa5ce1f284c5afb80cd767987969044`, `runtime_mode hosted`
- Deployment checks verified: backup and WAL readiness passed, S3 AES256 configured, co-tenant protection passed, NATS running, PostgreSQL healthy and provenance verified, migrations succeeded, candidate `/readyz` passed, exact candidate commit and image digest matched, running repository digest independently matched, edge switched to candidate, public edge smoke passed, unauthenticated `GET /api/v1/organizations` returned `401` as expected.

### Tenant, project, and environment

- Organization: `674b13a3-2cb8-4ea1-9ac8-94c5504c5a27`
- Project: `927709e8-831d-46af-bd6b-a3a2b19bfd1c`
- Environment: `10811fd6-82d6-42a2-b7f4-24226b812479`
- Environment name: `staging`

No secret material is recorded. No API keys, cookies, enrollment tokens, session tokens, private keys, secrets, or authorization headers are included.

### Real hosted workers

Two real hosted workers were run with:

- Runtime base: `node:24.21.0-bookworm`
- Exact pulled Node image digest: `sha256:22553920add6fb1fd909104346924cd30b4b3ac76ca2980f3b8dba8ede3cf945`
- Worker IDs: `82d3b1ff-6fb4-490e-af84-ff0d8e5a52f4`, `eb982dd6-b195-42f7-9564-9a66fe61331f`
- Both `ACTIVE` in pool `default`
- Both used real Linux amd64 runtime executable, real Node runner, real child process execution, public staging HTTPS control plane, mounted immutable bundle tar files, and 2 slots.

Worker private keys, enrollment tokens, and session tokens are not recorded.

### Customer-onboarding bundle

- Workflow: `customer-onboarding`
- Bundle digest: `94aa90468a24ac2d32a54c4a17ce51aa0e257e250a8d8371b9ca363053321ace`
- Dependency lock digest: `c15e2cd042d82b3505b045ff609961dbfaf358f4827028fdc0582fbcc85e66b6`
- Target: `linux/amd64`
- Deployment: `9ec22662-5211-4f1a-a80b-7766a355117d`
- Manifest hash: `c8302f27e6e894f8efe79f718360aaf3a574c34664b4b7f2bc45fd999da223b2`
- A preflight activation attempt before compatible workers were present correctly failed with `Insufficient compatible workers online advertising this deployment’s bundle digest.` This is recorded only as hosted compatibility preflight evidence. It is not claimed as a broader architecture mismatch test.

### Literal one task, then A to B to C

A separate real one-node workflow was created specifically to satisfy the literal M1 gate wording `one task, then A -> B -> C`.

One-task workflow:

- Workflow: `single-task-check`
- Bundle digest: `03e1144c986214d8d514a4a5ca2210837b532e0af0cb8f545b827e40a91a0095`
- Dependency lock digest: `c15e2cd042d82b3505b045ff609961dbfaf358f4827028fdc0582fbcc85e66b6`
- Deployment: `9a16d5ad-2102-4a75-9b5e-6b8387e9de3e`
- Manifest hash: `40f065fb6850da5e69724f3a465b6d548de2d49c9e6c7fcb4c7405cba8fb28a8`
- Run: `0c31f13a-035a-4ec1-8b2a-61767c963c44`
- Final status: `SUCCEEDED`
- Last Event Sequence: `5`
- Step: `validate`
- Step ID: `e897282c-dc39-412d-b68d-20813f0dc087`
- Attempt: `07c146d0-f0fc-434e-8d0a-60523bee2031`
- Attempts: exactly `1`
- Output: `{"email":"single@example.com","userId":"usr_e897282c-dc39-412d-b68d-20813f0dc087"}`

This is the literal `one task` half of the gate.

Immediately after the single-task acceptance, a new real `customer-onboarding` run was created:

- Run: `08c86672-19d6-4ff9-9a2c-65e4b36275d2`
- Workflow: `customer-onboarding`
- Deployment: `9ec22662-5211-4f1a-a80b-7766a355117d`
- Final status: `SUCCEEDED`
- Last Event Sequence: `13`
- Validate: step `2d18cd25-83e5-4df6-8969-4279ddf95184`, attempt `86cd4651-57af-43cd-9e5b-43707d7b9271`, status `SUCCEEDED`, one attempt
- Provision: step `dcc092cc-61d4-45c7-92d4-70bcfa803aba`, attempt `5779da04-75e9-4dbb-9bb1-1221c45bca98`, status `SUCCEEDED`, one attempt
- Notify: step `19961f5d-6e2f-4526-a44c-75559598cbff`, attempt `6eeabe4c-ee84-4447-9cda-8e99f4a29448`, status `SUCCEEDED`, one attempt
- Final output: `{"accountId":"acc_usr_2d18cd25-83e5-4df6-8969-4279ddf95184","deliveryId":"del_op_bf2a80aa01953b9fa53dca08c877d7c514986f12a4ee454c0ee342648346c2ee"}`

This proves the literal sequence `one task`, then `A -> B -> C`. API and CLI inspect showed the persisted committed result and step history.

### Browser, auth, and SSE acceptance

A prior real hosted `customer-onboarding` run was used for browser evidence:

- Run: `92d6ea0e-93f8-4c50-922c-1b5af8e76d79`
- Final status: `SUCCEEDED`
- Last Event Sequence: `13`
- Unauthenticated `/dashboard` deep-link displayed `401` plus `Sign in with Deadbolt`
- Authenticated dashboard loaded successfully
- Run Inspector loaded the exact run
- Inspector showed `SUCCEEDED`
- Validate, provision, and notify all showed `SUCCEEDED`
- Stream status initially `LIVE`
- Browser DevTools network was switched Offline for more than 10 seconds
- Inspector changed to `RECONNECTING`
- Persisted run state remained intact
- Last Event Sequence remained `13`
- Request used `Last-Event-Id: 13`
- After restoring network, stream returned `LIVE`
- SSE request returned HTTP `200`
- `Last-Event-Id` remained `13`
- Persisted state and history remained consistent

This is recorded as manual browser, auth, and SSE acceptance. No screenshots are stored in the repository.

### Hosted wrong-pool negative

- Disposable worker: `af843bb6-60e3-46b9-9adb-80ce3d595c3b`
- Enrolled into pool `default`
- A fresh authenticated worker session deliberately polled with pool `gpu`
- Observed: `WRONG_POOL_HTTP=401`, `CODE=UNAUTHORIZED`, `PASS: WRONG POOL REJECTED`
- This was against public hosted staging.
- AvailableSlots was `0`, so the probe could not create a claim even if the boundary were incorrect.

This is recorded under F-21 hosted acceptance.

### Hosted cross-tenant and SSE negatives

Using the legitimate authenticated CLI session, organization context was deliberately overridden with a foreign UUID while targeting a real run ID:

- Real run: `08c86672-19d6-4ff9-9a2c-65e4b36275d2`
- Foreign organization: `11111111-2222-4333-8444-555555555555`
- Observed run read: HTTP `403`, code `FORBIDDEN_ORGANIZATION_MEMBERSHIP`
- Observed SSE stream: HTTP `403`, code `FORBIDDEN_ORGANIZATION_MEMBERSHIP`
- Conclusions: `PASS: CROSS-TENANT RUN READ REJECTED`, `PASS: CROSS-TENANT SSE REJECTED`

This is recorded under F-21 and F-18 hosted and manual boundary evidence.

### Hosted invalid-output F-28

A real intentionally invalid task bundle was executed through the real hosted stack:

- Workflow: `invalid-output-check`
- Bundle digest: `ba770a80c5224b8b9c4f97fcfe7dff36324d7799b74e7bb65d562caf07a2edee`
- Dependency lock digest: `c15e2cd042d82b3505b045ff609961dbfaf358f4827028fdc0582fbcc85e66b6`
- Deployment: `a14df8c6-1788-408e-a73d-7b5fbc0bb347`
- Run: `9b4a9e3a-e9f7-4582-a096-aab758a59a7b`
- Final status: `FAILED`
- Reason: `OUTPUT_SCHEMA_VIOLATION`
- Last Event Sequence: `8`
- Step: `validate`
- Step ID: `9acd2b30-d5f9-4782-bd29-4a5a863eaad6`
- Attempt 1: `00a5cc62-9689-42f6-8721-15effcb075cb`, status `LOST`, reason `START_DEADLINE_EXCEEDED`, retryable `true`, effect status `NOT_APPLIED`
- Attempt 2: `afd0de09-d92a-41c0-802e-3405d8e418a0`, status `FAILED`, reason `OUTPUT_SCHEMA_VIOLATION`, retryable `false`, effect status `NOT_APPLIED`
- Active compatible workers: `2`

The first `LOST` attempt is not hidden. It was a transient start-deadline loss followed by a second authoritative attempt that executed and produced the intended non-retryable `OUTPUT_SCHEMA_VIOLATION`. The final run state was correctly terminal `FAILED`. This proves the hosted F-28 boundary.

Artifact isolation is not an active M1 artifact path and is documented as a limitation rather than invented coverage. Do not record API keys, cookies, enrollment tokens, task secrets, authorization headers, or unredacted stack traces.

## Final Issue acceptance status

Automated M1 failure-matrix coverage is complete for F-01, F-02, F-04, F-08, F-11, F-18, F-21, F-27, and F-28 at the stated test boundaries. Hosted CI PASS on exact HEAD `f232f93b7db228861cb69f032b8a872392cd3061` (Foundation contracts run `35431949953`).

Hosted staging is complete: immutable build-once deployment run `35432643158` deployed commit `f232f93b7db228861cb69f032b8a872392cd3061` with verified `/version` commit and image digest. Literal `one task` run `0c31f13a-035a-4ec1-8b2a-61767c963c44` succeeded, followed by literal `A -> B -> C` run `08c86672-19d6-4ff9-9a2c-65e4b36275d2` with final sequence `13` and committed output. Manual browser, auth, and SSE acceptance was witnessed on run `92d6ea0e-93f8-4c50-922c-1b5af8e76d79`. Hosted negatives for wrong pool, cross-tenant run read and SSE, and invalid output were observed as recorded above. F-08 remains automated real-PostgreSQL evidence and is not claimed as manual hosted duplicate completion.

M1 does not claim full M2 recovery or durability semantics. No MVP recovery is claimed.
