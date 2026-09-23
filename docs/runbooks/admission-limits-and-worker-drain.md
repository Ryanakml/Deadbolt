# Admission limits and worker drain (MVP / Issue #23)

Operator reference for the MVP §27.1 caps, admission behavior, and graceful
worker drain. Source of truth is the Blueprint; this runbook records the
as-built behavior with pointers to code and regression tests.

## 1. MVP limits

| Dimension | Limit | Enforced at |
|---|---|---|
| Nodes per workflow | 50 | Contract validation at register (`NODE_COUNT_EXCEEDED`); service defense in depth at run creation (`WORKFLOW_TOO_LARGE`, HTTP 422) |
| Running/claimed attempts (live leases) per env | 10 | Claim boundary under the environment admission lock; server ceiling 10 even if `max_concurrency` is configured higher |
| Nonterminal runs per env | 100 | Run creation (`RUN_QUOTA_EXCEEDED`, HTTP 429 + `Retry-After: 60`); terminal runs do not count |
| Create-run rate per env | 5/sec, burst 10 | Token bucket under the environment admission lock (`CREATE_RUN_RATE_LIMITED`, HTTP 429 + `Retry-After: 1`) |
| Worker sessions per env | 10 | Enrollment and session creation under the admission lock (`SESSION_QUOTA_EXCEEDED`, HTTP 429 + `Retry-After: 60`) |
| Worker slots per session | 2 (`DefaultSlots`) | Claim clamps advertised slots down to 2; a worker can never raise the cap |
| Inline JSON payload | 256 KiB | Request validation (`PAYLOAD_TOO_LARGE`) |
| Task logs | 16 KiB/line, 1 MiB/attempt | Ingest drops over-budget lines with `DroppedCount` + `BudgetExhausted`; `deadbolt_task_logs_dropped_total` metric; drop receipts persisted |
| Log retention | 7 days | `PruneExpiredTaskLogs` batch (limit 1000) on the scheduler slow sweep; expired reads return an explicit expired marker, never silent absence |
| Attempts per task | default 3, max 10 | Retry policy |
| Events per run | 10,000, final slot reserved | Non-terminal append at 9,999 rejected `HISTORY_LIMIT_EXCEEDED`; terminal `RUN_FAILED` commits as event 10,000 exactly once |

## 2. Admission behavior (429 + Retry-After)

- `RUN_QUOTA_EXCEEDED` → 429 + `Retry-After: 60`. Accepted work is never
  dropped: ready tasks wait as `WAITING` with `reason_code = QUOTA_WAIT`
  and resume to RUNNING when capacity frees (`TestClaimConcurrencyCapsAndQuotaWait`).
- `CREATE_RUN_RATE_LIMITED` → 429 + `Retry-After: 1`.
- `SESSION_QUOTA_EXCEEDED` → 429 + `Retry-After: 60`.
- Diagnosis: list runs/workers with `?environment=`; `QUOTA_WAIT` means
  "admitted, waiting for lease capacity", not an error. Do not confuse with
  `NO_COMPATIBLE_WORKERS` (no matching bundle) or reconciliation holds.

## 3. Fairness

- FIFO within an environment: `eligible_at`, then ID
  (`TestClaimFifoOrderingWithinEnvironment`).
- Round-robin across environments is a control-plane scheduler property:
  `ReconcileReadyWork` interleaves repair candidates per environment so no
  environment starves behind another's backlog in the bounded batch of 50
  (`TestSchedulerFairnessRoundRobinAcrossEnvironments`). Workers stay
  environment-bound and only claim within their own environment.

## 4. Worker drain

- Command: `runtime worker drain` or `POST /v1/workers/{id}/drain`
  (capability `workers:drain`). Worker state becomes `DRAINING`.
- A `DRAINING` worker is refused new assignments even with free slots
  (`TestWorkerDrainRefusesNewClaims`); its poll loop stops.
- Active attempts finish until the deployment grace limit, default 60 s
  (`worker.DrainGracePeriod`). Active runners that finish in time are not
  disturbed (`TestAgentDrainFinishesActiveAttemptBeforeGrace`).
- After grace, the agent stops remaining runner process groups
  (`TestAgentDrainForceStopsRunnerAfterGrace`); the control plane then
  applies the task's existing recovery policy (`safe` retries with durable
  backoff, `reconcile` holds an `OPEN` case for the operator, never a blind
  retry; `TestWorkerDrainForceStopRecoversViaPolicy`). A force-stopped task
  never remains silently healthy.
- Reconnect fences the old session (`revoked_at` set; stale token rejected)
  and establishes a fresh session (`TestWorkerReconnectFencesPreviousSessions`).
  Fencing runs before the session-quota check, so reconnect at the cap keeps
  the live count at 10 (`TestWorkerSessionQuotaBoundary`).
- Old bundle files on disk are retained across drain/reconnect
  (`TestWorkerPreservesBundlesAcrossDrainAndReconnect`); updating a worker
  never moves a run to another deployment.

## 5. Logs and correctness under pressure

- Diagnostic flood is absorbed by bounded drops + counters; execution
  events (`run_events`, outbox) are never dropped
  (`TestLogPressurePreservesCorrectnessEvents`, SP-04 report
  `docs/reports/SP-04-telemetry-storage-budget.md`).

## 6. Rollback / compatibility

- Migration `00023_admission_rate_and_limits` is additive; schema version 23.
  Fresh databases migrate to 23; 22 → 23 verified (`TestMigration22To23`,
  existing `TestMigrationFreshAnd21To23`). Never assume binary rollback
  rolls the database back.
