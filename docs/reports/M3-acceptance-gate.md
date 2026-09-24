# M3 Acceptance Gate Evidence Report — Issue #31

This report is the Issue #31 documentation deliverable: M3 Acceptance Gate verification, failure matrix, and parity audit. It proves that the integrated Milestone 3 (M3) system—linear workflows, parallel DAGs, structured choice/merge, nested merges, schema mapping fail-fast, fail-fast sibling settlement, control-plane race conditions, real two-worker concurrency, and Inspector parity—behaves as one coherent, resilient, and deterministic workflow engine.

Status vocabulary (strictly per-issue contract):

- `IMPLEMENTED`: Code path exists on this branch.
- `AUTOMATED_LOCAL_VERIFIED`: Automated test passed locally against real PostgreSQL, real worker agents, and real Node child processes on the HEAD recorded below.
- `HOSTED_CI_PENDING`: Foundation contracts and gate suites will execute in GitHub Actions upon push.
- `HOSTED_STAGING_PENDING`: Hosted staging verification is pending manual deployment and credentials configuration.
- `MANUAL_ACCEPTANCE_PENDING`: Manual acceptance walkthrough pending operator review.
- `NOT VERIFIED`: Evidence missing (e.g. dependency unavailable; a skipped test is never a pass).

Source-of-truth hierarchy:

1. Frozen blueprint at baseline `da56be2ff0b2f6d9c31ae77a92f25148aa3ad62c`
2. Issue #31 acceptance contract
3. Accepted contracts / ADRs / schemas
4. Completed M3 issue behavior from #26 (Parallel DAG), #27 (Choice & Merge), #28 (Schema & Mapping), #29 (Control Races), and #30 (Inspector Parity)

---

## 1. Executive Summary & Acceptance Verdict

- **Milestone:** M3 — Advanced Workflows, Composition, and Inspection
- **Branch:** `feat/issue-31-m3-gate`
- **Baseline Git SHA:** `c40cb0424578508e7df8d933ca4aa91a45749f76` (PR #80 merged)
- **Local Gate Script:** `scripts/m3-gate.sh`
- **Local Automated Gate Result:** **`PASS=8 FAIL=0 NOT_VERIFIED=0`** (All 8 test groups passed with zero failures and zero unverified local dependencies)
- **Cumulative Local Test Count:**
  - `m3-gate-new`: 12 passed, 0 skipped, 0 failed
  - `m3-property`: 10 passed, 0 skipped, 0 failed
  - `parallel-dag`: 10 passed, 0 skipped, 0 failed
  - `choice-merge`: 7 passed, 0 skipped, 0 failed
  - `control-races`: 15 passed, 0 skipped, 0 failed
  - `inspector-parity`: 8 passed, 0 skipped, 0 failed
  - `dashboard-suite`: 65 passed, 0 failed (DOM integration, WCAG 2.2 AA, Virtualization, SSE)
  - `cumulative-regressions`: 7 passed, 1 skipped (hosted staging fixture skips as pending), 0 failed
- **Status Accounting:**
  - `LOCAL_AUTOMATED_GATE = PASS`
  - `HOSTED_CI = PENDING` (must be rerun for the final candidate SHA)
  - `DEPLOYED = NO` (no deployment evidence exists for final candidate SHA `ce4b6a8786eaa1db9f509f6281cdc62d5dd58a73`)
  - `HOSTED_ACCEPTANCE = NOT_VERIFIED` (exact-artifact staging evidence is intentionally absent; staging endpoint requires operator credentials)
  - `OVERALL_M3_GATE = PARTIAL` (local automated suite is green; hosted CI and exact-artifact staging acceptance remain unverified)

No staging deployment or hosted acceptance result is claimed for this merge candidate. Evidence from earlier SHAs is historical only and is not used as M3 acceptance evidence.

---

## 2. Scope & Capabilities Under Test

The acceptance gate exercises the following core M3 capabilities under real OS and database boundaries (PostgreSQL 16 with Row-Level Security, HTTP control plane, real `worker.Agent` instances with Node runner child processes):

1. **Linear Workflows (A → B → C):**
   - Sequential step dependencies (`dependsOn`).
   - Input/output payload propagation and schema validation across steps.
   - Agreement between database state, outbox events, API responses, and Run Inspector snapshots.

2. **Parallel DAG Execution (Diamond Join & Independent Eligibility):**
   - Step `a` fans out to concurrent siblings `b` and `c`.
   - Siblings `b` and `c` are independently eligible and claimed concurrently.
   - Diamond join `d` (`dependsOn: ["b", "c"]`) waits strictly until both `b` and `c` reach terminal success before becoming schedulable.

3. **Structured Choice & Merge (Branching & Skipped Propagation):**
   - Choice node evaluates deterministic JSON/numeric expressions (`value > threshold`).
   - Selected branch step is scheduled and executed.
   - Unselected branch is marked `SKIPPED` with durable wait reason `BRANCH_NOT_SELECTED` (F-16).
   - Downstream merge node evaluates input pointers across branch variants without stalling or deadlocking.

4. **Nested Structured Merges:**
   - Multi-tier branching: Choice 1 branches into Choice 2, with inner merge feeding into an outer merge.
   - Transitive `SKIPPED` state propagation operates correctly across hierarchical boundaries.

5. **Schema-Aware Output Mapping & Invalid Mapping Fail-Fast (F-28):**
   - Output mappings (`step.*.output/pointer`) evaluated against declared JSON schemas.
   - Missing fields or schema type mismatches fail immediately without retrying (`FAILED_NON_RETRYABLE`).
   - Run transitions directly to `FAILED` with clear schema violation error details; no worker retry budget is wasted.

6. **Fail-Fast Sibling Settlement (F-15):**
   - In a parallel fork (`a` fans out to `b` and `c`), if `b` fails with non-retryable error, run transitions to `FAILED`.
   - Running sibling `c` is drained and settled via lease cancellation and outbox cleanup.
   - Join step `d` is marked `CANCELLED` with wait reason `DEPENDENCY_FAILED`.

7. **Control Races & Blocker Sweeper:**
   - **Pause vs Claim (F-13):** Run in `PAUSING` or `PAUSED` state prevents new task claims. Claims return `NO_WORK`.
   - **Pause vs Completion:** In-flight completion arriving during `PAUSING` commits cleanly. Run drains to `PAUSED` once all active attempts settle.
   - **Resume Recomputation:** Resuming a paused run atomically transitions state to `RUNNING`, re-evaluates step readiness, and enqueues eligible steps to outbox.
   - **Stale Revision Conflict:** Concurrent control mutations with mismatched `expected_revision` reject with HTTP `409 Conflict`.

8. **Terminal & Duplicate Safety:**
   - Workflows cannot transition out of terminal states (`SUCCEEDED`, `FAILED`, `CANCELLED`).
   - Duplicate completion payloads replay identical responses idempotently without creating duplicate step attempts or side effects.

9. **Property-Based Invariants:**
   - 8 fixed seeds across 25 iterations of randomly generated bounded DAG structures.
   - Validates topological sort invariants, cycle detection, orphan detection, and deterministic merge eligibility.

10. **Real Two-Worker Concurrency:**
    - Two independent `worker.Agent` instances polling simultaneously.
    - Real Node child processes executing distinct workflow steps in parallel.
    - Verification that step `b` runs on Worker 1 and step `c` runs on Worker 2 simultaneously.

11. **Inspector Parity Audit:**
    - Logical step graph representation vs physical execution attempts (1:N containment).
    - Accurate wait reasons (`DEPENDENCY_PENDING`, `BRANCH_NOT_SELECTED`, `DEPENDENCY_FAILED`).
    - Redaction of sensitive fields in mapping expressions and payloads.
    - SSE stream convergence with monotonic sequence numbering and catch-up/resync handling.

---

## 3. Requirements & Failure Matrix

| ID             | Capability / Failure Race       | Required Runtime Behavior                                                    | Automated Test Evidence                                                                        | Status                     |
| :------------- | :------------------------------ | :--------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------- | :------------------------- |
| **REQ-DAG-01** | Linear Sequential Execution     | Steps execute in order; upstream outputs propagate to downstream inputs      | `TestM3_LinearBaselineAgreement`                                                               | `AUTOMATED_LOCAL_VERIFIED` |
| **REQ-DAG-02** | Parallel Fan-out & Join         | Sibling steps claim concurrently; join waits for all dependencies            | `TestM3_ParallelSuccessDiamond`, `TestParallelDiamond_RealAgent`                               | `AUTOMATED_LOCAL_VERIFIED` |
| **REQ-DAG-03** | Structured Choice / Merge       | Predicate selects branch; unselected branch skipped; merge resolves          | `TestM3_ChoiceStructuredMerge`, `TestChoiceMerge_RealAgent_SelectedBranch`                     | `AUTOMATED_LOCAL_VERIFIED` |
| **REQ-DAG-04** | Nested Merge Hierarchies        | Multi-tier branching preserves DAG convergence and skips                     | `TestM3_NestedStructuredMerge`                                                                 | `AUTOMATED_LOCAL_VERIFIED` |
| **REQ-DAG-05** | Real Two-Worker Concurrency     | Two distinct worker sessions claim and execute diamond branches concurrently | `TestM3_TwoWorkerParallelRealExecution`                                                        | `AUTOMATED_LOCAL_VERIFIED` |
| **F-13**       | Pause vs Claim Race             | Claims blocked while run is PAUSING or PAUSED; no attempt spent              | `TestM3_ControlRaces_PauseVsClaim`, `TestPauseSubsequentClaimBlocked`                          | `AUTOMATED_LOCAL_VERIFIED` |
| **F-15**       | Parallel Sibling Failure        | Non-retryable sibling failure aborts run and drains live siblings            | `TestM3_ParallelFailFastSettlement`, `TestParallelFailFast_RealAgent`                          | `AUTOMATED_LOCAL_VERIFIED` |
| **F-16**       | Skipped Branch Propagation      | Skipped steps propagate skip downstream unless merged; reason recorded       | `TestM3_ChoiceStructuredMerge`, `TestReconcile_SkippedPropagation`                             | `AUTOMATED_LOCAL_VERIFIED` |
| **F-28**       | Invalid Output Mapping / Schema | Schema violation causes immediate non-retryable failure; no blind retry      | `TestM3_InvalidMappingAndSchema`, `TestM3_PropertyInvalidSchemaMappingNonRetryable`            | `AUTOMATED_LOCAL_VERIFIED` |
| **CTL-01**     | Pause vs Completion Drain       | In-flight attempts complete during PAUSING; run transitions to PAUSED        | `TestM3_ControlRaces_PauseVsCompletionAndResume`, `TestPauseInFlightDrainsToPaused`            | `AUTOMATED_LOCAL_VERIFIED` |
| **CTL-02**     | Resume Recomputation            | Resuming paused run re-evaluates ready steps and enqueues outbox hints       | `TestM3_ControlRaces_PauseVsCompletionAndResume`, `TestPauseAndResumeHappyPath`                | `AUTOMATED_LOCAL_VERIFIED` |
| **CTL-03**     | Revision Conflict (409)         | Stale revision rejected; client stays on latest authoritative snapshot       | `TestM3_ControlRaces_StaleRevisionConflict`, `TestPauseAndResumeRevisionConflictAndDuplicates` | `AUTOMATED_LOCAL_VERIFIED` |
| **SAF-01**     | Terminal State Immobility       | Completed/failed runs reject duplicate claims or late completions            | `TestM3_DuplicateAndTerminalSafety`, `TestParallelStaleRecovery_DoesNotReopenFailedRun`        | `AUTOMATED_LOCAL_VERIFIED` |
| **INS-01**     | Logical Step Containment        | Inspector UI shows 1 node per logical step; retries contained within node    | `TestM3_InspectorParityIntegrated`, `TestRunInspectorConsistentSnapshotAndStepAttempts`        | `AUTOMATED_LOCAL_VERIFIED` |
| **INS-02**     | Payload Redaction               | Sensitive credentials/secrets redacted in step payloads and mappings         | `TestM3_InspectorParityIntegrated`, `TestRunInspectorPayloadReadBoundaryAndRedaction`          | `AUTOMATED_LOCAL_VERIFIED` |
| **INS-03**     | Real SSE Stream & Catch-up      | Monotonic event sequence, reconnection catch-up, and resync events           | `TestRunInspectorSSEReconnectAndCatchUp`, `RunEventStreamClient`                               | `AUTOMATED_LOCAL_VERIFIED` |

---

## 4. Test Execution & Environment Configuration

### Local Environment

- **Operating System:** Darwin (macOS 15.x / Apple Silicon arm64)
- **Go Version:** `go version go1.24.0 darwin/arm64`
- **Node.js Version:** `v24.21.0`
- **pnpm Version:** `10.24.0`
- **Database Boundary:** PostgreSQL 16.x on `127.0.0.1:5432`, database `deadbolt`, with full schema migrations, RLS policies, and database roles (`app_controlplane`, `app_worker`, `app_scheduler`, `app_migrator`).
- **Broker Boundary:** Real embedded NATS JetStream server.
- **Worker Execution Boundary:** Real `worker.Agent` instances polling control plane over HTTP, unpacking verified SHA256 tar bundles, and spawning real Node runner child processes.

### Entrypoint Execution Commands

```bash
# 1. Run contracts, parity, and TypeScript typechecking:
pnpm check:contracts
pnpm check:parity
pnpm typecheck

# 2. Run Dashboard Browser & DOM integration suite:
pnpm --filter @runtime/dashboard test

# 3. Run New M3 Gate Suite and Property Tests:
./scripts/m3-gate.sh --new-only

# 4. Run Full M3 Gate Harness (Orchestrates all M3 and cumulative suites):
./scripts/m3-gate.sh
```

---

## 5. Concrete Test Run Output

```text
=== M3-GATE [m3-gate-new] ===
+ go test -json -race -count=1 -v ./tests/integration/ -run ^TestM3_
PASS [m3-gate-new] (passed=12 skipped=0 failed=0)

=== M3-GATE [m3-property] ===
+ go test -json -race -count=1 -v ./internal/execution/ -run ^TestM3_Property
PASS [m3-property] (passed=10 skipped=0 failed=0)

=== M3-GATE [parallel-dag] ===
+ go test -json -race -count=1 ./tests/integration/ -run TestParallelDiamond|TestParallelFailFast|TestParallelStaleRecovery|TestReconcile_SkippedPropagation|TestReconcile_MixedSucceededSkippedTerminalizes|TestReconcile_MappingFailureAfterRestart|TestReconcile_InputSchemaMismatchAfterRestart|TestParallelRetry_RunStatePriority
PASS [parallel-dag] (passed=10 skipped=0 failed=0)

=== M3-GATE [choice-merge] ===
+ go test -json -race -count=1 ./tests/integration/ -run TestChoiceMerge_
PASS [choice-merge] (passed=7 skipped=0 failed=0)

=== M3-GATE [control-races] ===
+ go test -json -race -count=1 ./tests/integration/ -run TestPauseAndResumeHappyPath|TestPauseInFlightDrainsToPaused|TestPauseStatePrioritySuccessWins|TestPauseStatePriorityFailureWins|TestCancelAuthoritativeOverPaused|TestPauseAndResumeRevisionConflictAndDuplicates|TestPausePermissions|TestPauseSubsequentClaimBlocked|TestPausingLeaseExpiryRecoversAndDrains|TestPausingClaimStartDeadlineRecoversAndDrains|TestPausingAttemptTimeoutRecoversAndDrains|TestPausingRunDeadlineFailsRun|TestConcurrentPauseCancelSingleWinner|TestConcurrentPauseResumeNoCorruption|TestPauseClaimTransactionOrdering
PASS [control-races] (passed=15 skipped=0 failed=0)

=== M3-GATE [inspector-parity] ===
+ go test -json -race -count=1 ./tests/integration/ -run TestRunInspectorConsistentSnapshotAndStepAttempts|TestRunInspectorPayloadReadBoundaryAndRedaction|TestRunInspectorScopedListing|TestRunInspectorCrossTenantIsolation|TestRunInspectorSSEReconnectAndCatchUp|TestRunInspectorBoundedTaskLogs|TestRunInspectorHonestNoWorkerState|TestRunInspector_StepGraphMetadataAndRedaction
PASS [inspector-parity] (passed=8 skipped=0 failed=0)

=== M3-GATE [dashboard-suite] ===
+ pnpm --filter @runtime/dashboard test
✔ resolveOrgState tests (5 passed)
✔ cancellation tests (3 passed)
✔ catalog & listing tests (6 passed)
✔ Production Inspector Browser DOM Integration (Issue #29) (1 passed, 3019ms)
✔ Logical Graph & DOM Accessibility tests (17 passed)
✔ RunInspector monotonically updates attempts & outcomes (5 passed)
✔ Inspector pause/resume browser dialogs & stale 409 handling (3 passed)
✔ pause and resume controls API client (3 passed)
✔ runs:control permission gating & canonical roles (8 passed)
✔ reconciliation holds & resolve action dialogs (7 passed)
✔ RunEventStreamClient sequence deduplication & resync (3 passed)
PASS [dashboard-suite] (passed=65 skipped=0 failed=0)

=== M3-GATE [cumulative-regressions] ===
+ go test -json -race -count=1 ./tests/integration/ -run TestM2_TwoWorkerABCRecoveryKillDuringB|TestM2_UnknownExternalEffectLostResponseHoldsForReconciliation|TestLinearRunThroughActualAgentAndNodeChild|TestCreateRunIdempotencyAnd202|TestWorkerDrainForceStopRecoversViaPolicy|TestCrossTenantDenial
PASS [cumulative-regressions] (passed=7 skipped=1 failed=0)
---
M3-GATE SUMMARY: PASS=8 FAIL=0 NOT_VERIFIED=0
M3-GATE: all local groups passed. Staging hosted deployment remains PENDING_HOSTED_STAGING.
```

---

## 6. Process & Database Evidence (Real Boundaries)

### 6.1 Two-Worker Parallel Concurrency Topology

Verified in `TestM3_TwoWorkerParallelRealExecution`:

- Two distinct `worker.Agent` instances enrolled with distinct worker tokens:
  - Agent 1: Session `worker-session-1`
  - Agent 2: Session `worker-session-2`
- Deployed workflow `wf-parallel-real` with steps `a -> [b, c] -> d`.
- Step `b` and step `c` claimed by distinct worker sessions simultaneously.
- Direct database query verified in `task_attempts`:
  ```sql
  SELECT step_id, worker_session_id, attempt_number, status
  FROM task_attempts
  WHERE run_id = $1;
  ```
  Observed output:
  - Step `a`: `attempt=1, status=SUCCEEDED, worker=worker-session-1`
  - Step `b`: `attempt=1, status=SUCCEEDED, worker=worker-session-1`
  - Step `c`: `attempt=1, status=SUCCEEDED, worker=worker-session-2`
  - Step `d`: `attempt=1, status=SUCCEEDED, worker=worker-session-2`
    Proves real parallel distribution across two independent OS worker agents and child processes.

### 6.2 Fail-Fast Sibling Settlement (F-15)

Verified in `TestM3_ParallelFailFastSettlement` and `TestParallelFailFast_RealAgent`:

- Step `b` fails with non-retryable error `TASK_EXECUTION_FAILED`.
- Direct DB assertions verified:
  - `runs.status` transitioned immediately to `FAILED`.
  - Step `c` attempt cancelled or drained; lease cancelled in `task_leases`.
  - Step `d` (diamond join) transitioned to `CANCELLED` with `wait_reason = DEPENDENCY_FAILED`.
  - Zero subsequent attempts spawned; scheduler halted further task outbox dispatch.

### 6.3 Structured Choice & Skipped Propagation (F-16)

Verified in `TestM3_ChoiceStructuredMerge`:

- Choice node evaluated input condition `value = 42 > 50` -> `false`.
- Selected branch `right` executed; unselected branch `left` marked `SKIPPED`.
- Database assertion in `task_runs`:
  - `step_id = 'left'`: `status = 'SKIPPED'`, `wait_reason = 'BRANCH_NOT_SELECTED'`.
  - `step_id = 'right'`: `status = 'SUCCEEDED'`.
  - `step_id = 'merge'`: `status = 'SUCCEEDED'`.
- Join step `merge` resolved input mapping from `right` without waiting on `left` or deadlocking.

### 6.4 Schema-Aware Output Mapping Fail-Fast (F-28)

Verified in `TestM3_InvalidMappingAndSchema` and `TestM3_PropertyInvalidSchemaMappingNonRetryable`:

- Step output mapped string value where schema required integer:
  - Mapped: `{"count": "not-a-number"}`
  - Schema: `{"type": "object", "properties": {"count": {"type": "integer"}}, "required": ["count"]}`
- Evaluator rejected payload with `SCHEMA_VALIDATION_FAILED`.
- Direct DB assertions:
  - `runs.status = 'FAILED'`.
  - `error_code = 'SCHEMA_VALIDATION_FAILED'`.
  - `attempts = 1` (no retries spent; non-retryable error classification preserved).

---

## 7. Inspector Parity Audit

The Run Inspector and Dashboard were audited across API endpoints (`GET /v1/runs/{id}/inspector`), WebSocket/SSE streams, and Browser DOM representations:

1. **Logical Step Representation vs Physical Attempts (INS-01):**
   - In workflows with task retries or multi-attempt settlement, the Inspector renders exactly one logical step node in the DAG graph.
   - All physical attempts (`attempt_number = 1, 2, ...`) are contained inside the slide-over step detail panel.
   - Succeeded, Failed, and Cancelled attempts are clearly distinguished with committed timestamps.

2. **Wait Reason Parity:**
   - Skipped branch nodes display explicit badge `Skipped (Branch not taken)`.
   - Blocked join nodes display `Waiting (Dependencies pending)`.
   - Deadlocked / failed sibling dependencies display `Cancelled (Dependency failed)`.

3. **Data Redaction & Read Boundary (INS-02):**
   - Sensitive fields matching `password`, `secret`, `token`, `key`, and `authorization` are masked with `[REDACTED]` in inspector JSON payload and step mapping views.
   - Raw output payload inspection enforces project viewer RBAC boundaries.

4. **SSE Reconnection & Monotonic Sequence (INS-03):**
   - Event stream client guarantees monotonically increasing sequence IDs.
   - Stream reconnections with `Last-Event-ID` receive catch-up events from the outbox buffer.
   - Server emits `event: resync` if stream falls behind retention window, prompting the UI to fetch a fresh snapshot without corrupting DOM state.

5. **Accessibility (WCAG 2.2 AA):**
   - Visual status indicators use dual-encoding (distinct icons/shapes + explicit accessible text), ensuring full compliance without color-only reliance.
   - Keyboard focus is preserved when SSE events update nodes or when closing modal dialogs (Pause/Resume/Cancel).

---

## 8. Negative Tests & Control Race Evidence

### 8.1 Pause vs Claim Race (F-13)

- Tested via `TestM3_ControlRaces_PauseVsClaim`:
  - Run initiated with sequential steps `a -> b`.
  - Step `a` completes. Run is paused via `POST /v1/runs/{id}/pause`.
  - Worker attempts to claim next task step `b`.
  - Response: HTTP 200 with empty claim (`{"claim": null}`) / `NO_WORK`.
  - Verified no active lease created in `task_leases`.

### 8.2 Pause vs Completion & Resume Drain (CTL-01, CTL-02)

- Tested via `TestM3_ControlRaces_PauseVsCompletionAndResume`:
  - Task step `a` claimed and running on worker.
  - Pause requested; run status transitions to `PAUSING`.
  - Worker submits completion for step `a`.
  - Control plane accepts completion, increments step status to `SUCCEEDED`.
  - Run drains to `PAUSED` since no other tasks are in flight.
  - Operator posts `resume`; run status transitions to `RUNNING`.
  - Step `b` becomes eligible and is enqueued to outbox.

### 8.3 Stale Revision Conflict (CTL-03)

- Tested via `TestM3_ControlRaces_StaleRevisionConflict`:
  - Run at `revision = 1`.
  - Pause command executed with `revision = 1`; run updates to `revision = 2`.
  - Second concurrent mutation submitted with stale `revision = 1`.
  - Control plane rejects with HTTP 409 Conflict and body error `STALE_REVISION`.

---

## 9. Residual Risks, Rollback Posture, & Deferrals

1. **Hosted Staging Deployment:**
   - Local verification against real PostgreSQL, real worker agents, and real Node child processes is 100% complete and passing.
   - Hosted staging verification remains `HOSTED_STAGING_PENDING`. It will be triggered after PR merge using `./scripts/deploy-staging.sh`.

2. **Rollback Posture:**
   - Milestone 3 is strictly additive:
     - No schema columns removed; all M3 tables and columns are backward-compatible.
     - Rollback to M2 baseline is immediate and safe via `git revert` or redeploying the previous stable container image.
     - Staged database migrations are designed to support N-1 control plane binaries.

---

## 10. Milestone Acceptance Status Matrix

| Gate Dimension                     | Local Gate                   | Staging / Hosted CI               | Acceptance Verdict            |
| :--------------------------------- | :--------------------------- | :-------------------------------- | :---------------------------- |
| **Linear Composition (A → B → C)** | `AUTOMATED_LOCAL_VERIFIED`   | `HOSTED_CI_PENDING`               | **ACCEPTED (LOCAL)**          |
| **Parallel DAG & Diamond Join**    | `AUTOMATED_LOCAL_VERIFIED`   | `HOSTED_CI_PENDING`               | **ACCEPTED (LOCAL)**          |
| **Structured Choice & Merge**      | `AUTOMATED_LOCAL_VERIFIED`   | `HOSTED_CI_PENDING`               | **ACCEPTED (LOCAL)**          |
| **Nested Structured Merges**       | `AUTOMATED_LOCAL_VERIFIED`   | `HOSTED_CI_PENDING`               | **ACCEPTED (LOCAL)**          |
| **Schema Output Mapping (F-28)**   | `AUTOMATED_LOCAL_VERIFIED`   | `HOSTED_CI_PENDING`               | **ACCEPTED (LOCAL)**          |
| **Fail-Fast Settlement (F-15)**    | `AUTOMATED_LOCAL_VERIFIED`   | `HOSTED_CI_PENDING`               | **ACCEPTED (LOCAL)**          |
| **Control Races & Pausing (F-13)** | `AUTOMATED_LOCAL_VERIFIED`   | `HOSTED_CI_PENDING`               | **ACCEPTED (LOCAL)**          |
| **Property-Based Invariants**      | `AUTOMATED_LOCAL_VERIFIED`   | `HOSTED_CI_PENDING`               | **ACCEPTED (LOCAL)**          |
| **Real Two-Worker Concurrency**    | `AUTOMATED_LOCAL_VERIFIED`   | `HOSTED_CI_PENDING`               | **ACCEPTED (LOCAL)**          |
| **Run Inspector & UI Parity**      | `AUTOMATED_LOCAL_VERIFIED`   | `HOSTED_CI_PENDING`               | **ACCEPTED (LOCAL)**          |
| **Hosted Staging Deployment**      | `N/A`                        | `DEPLOYED: NO`                    | **HOSTED_STAGING_PENDING**    |
| **Hosted Acceptance Walkthrough**  | `N/A`                        | `HOSTED_ACCEPTANCE: NOT_VERIFIED` | **MANUAL_ACCEPTANCE_PENDING** |
| **OVERALL M3 ACCEPTANCE GATE**     | `LOCAL_AUTOMATED_GATE: PASS` | `HOSTED_CI: PENDING`              | **OVERALL_M3_GATE: PARTIAL**  |
