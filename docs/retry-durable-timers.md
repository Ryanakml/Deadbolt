# Retry, Error Classification, and Durable Timers (M2 — Issue #18)

Source of truth: `docs/blueprint.md` §13–§15, §17. This document is the
delivery artifact for Issue #18 and does not change blueprint defaults.

## 1. Retry defaults (authoritative)

- `maxAttempts` default **3**, maximum **10**, including the first attempt.
  Manifest values are clamped to `[1,10]`; `task.schema.json` already caps at 10.
- Backoff cap for failed attempt `n` (1-indexed):
  `min(maxDelayMs, initialDelayMs * 2^(n-1))` with defaults
  `initialDelayMs=1000`, `maxDelayMs=30000`.
- Full jitter: uniform integer in `[0, cap]`. The due timestamp is sampled
  **once** and persisted in `timers.due_at` (and mirrored to
  `run_steps.eligible_at`). Restart never redraws jitter or resets the timer.
- `Retry-After` (seconds or HTTP-date, RFC 9110 §13.1.1) may only **increase**
  the delay, capped at `maxDelayMs`. Invalid values are ignored. Parser and cap
  are covered by `TestParseRetryAfterSecondsAndDate` and
  `TestComputeRetryDelayRetryAfterIncreaseAndCap`.
- Claim-to-start budget is **5s** (`ClaimStartBudgetMs`, kept in sync with
  `worker.ClaimStartDeadline`). Attempt timeout default **5m**; run deadline
  guards apply before scheduling.

## 2. Error classification

| Class                         | Codes / conditions                                                                                                                                                                                                                                                              | Behavior                                                                                              |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| Non-retryable (deterministic) | `INPUT_MAPPING_ERROR`, `OUTPUT_MAPPING_ERROR`, `OUTPUT_SCHEMA_VIOLATION`, `SCHEMA_VIOLATION`, `INVALID_INPUT/OUTPUT`, `MISSING_TASK[_REF]`, `TASK_NOT_FOUND`, `INCOMPATIBLE_BUNDLE/DEPLOYMENT`, `UNSUPPORTED_CAPABILITY`, `UNAUTHORIZED/AUTH_ERROR/PERMISSION_DENIED/FORBIDDEN` | Fail fast to `FAILED`; no timer, no budget spent beyond the failed attempt. See `IsNonRetryableCode`. |
| Retryable definitive          | `retryable=true` + `effectStatus=NOT_APPLIED` (or explicit retryable provider error) with `safe`/`idempotent` recovery, budget left, deadline/window sufficient, run eligible                                                                                                   | `WAITING/RETRY_BACKOFF` + `PENDING` timer.                                                            |
| Ambiguous hold                | `recovery=reconcile` + `effectStatus=UNKNOWN` (unclassified), or `idempotent` with insufficient window                                                                                                                                                                          | `WAITING/RECONCILIATION` + `OPEN` `reconciliation_cases` row; never blind-retry.                      |
| Worker-declared non-retryable | `retryable=false` without hold condition                                                                                                                                                                                                                                        | `FAILED` with the worker code (or `MAX_ATTEMPTS_EXCEEDED` when budget exhausted).                     |
| Cancelled                     | `outcome=CANCELLED`                                                                                                                                                                                                                                                             | Terminal; never retries.                                                                              |

Absent workers create no attempt and spend no budget: `Claim` only transitions
`READY` steps; `WAITING` steps require timer firing first.

## 3. Idempotency-window responsibilities (recovery=idempotent)

- The developer declares `idempotencyWindowMs`, a conservative lower bound on
  provider dedup guarantees. Validator already rejects
  `window < 5000 + timeoutMs`.
- The engine stores `run_steps.idempotency_valid_until = first_claim + window`
  on first claim (`ensureIdempotencyWindowTx`). It is **never extended** by
  retry, deployment, or restore.
- Before scheduling or claiming a new attempt, the engine checks
  `now + 5s + timeoutMs > valid_until`. If insufficient, it routes to
  reconciliation (`IDEMPOTENCY_WINDOW_INSUFFICIENT`) with evidence
  `{operationId, attemptId, idempotencyValidUntil}`. No timer is left pending.
- Operation identity is stable: `op_<sha256(env:run:node)>` (attempt excluded),
  so retries reuse the same provider dedup key. Reruns create new runs/keys.
- A separate provider dedup ledger (test double in
  `TestRetryBackoffPersistsTimerAndFires`) must key on `operationId`, not on
  runtime DB rows.

## 4. Durable timer contract

- Table `timers` (`RETRY_BACKOFF` kind): `reference_id = step_id`,
  `run_id/step_id/attempt_number/operation_id/reason/due_at/state`.
- Partial unique `idx_timers_pending_identity` on
  `(organization_id, kind, reference_id) WHERE state='PENDING'` guarantees one
  pending timer per logical action. `INSERT ... ON CONFLICT DO NOTHING` makes
  duplicate scheduling idempotent without redrawing due.
- Lifecycle: `PENDING --due+guard--> FIRED --step READY--> CLAIM creates attempt`.
  Firing is conditional (`state='PENDING'` + step `WAITING/RETRY_BACKOFF`);
  only one scheduler wins (duplicate firing safe, INV-10).
- Restart safety (F-09): the scheduler reads the same row; `due_at` is never
  recomputed. `FireDueRetryTimers` + `Claim` (which fires due timers in the
  same admission transaction) both preserve `due_at`.
- Guards: scheduling requires run `QUEUED/RUNNING` (or `WAITING` without
  `RECONCILIATION`); firing requires `CanFireRetry` (paused/pausing, hold,
  cancelling, terminal keep `PENDING` with original `due_at`). Budget
  (`next_attempt_number <= maxAttempts`), run deadline
  (`due + 5s + timeout <= deadline`), and window guards apply in
  `scheduleRetryOrHoldTx`.
- Lock order (Blueprint §11.2): candidate scan without locks, then
  `run -> step -> timer` (`FOR UPDATE`), revalidate `PENDING + due <= now` under
  locks. `Claim` holds `environment_admissions -> run -> step` before timer work.
- Observability: `wait_reason/reason_code` canonical values `RETRY_BACKOFF`,
  `RECONCILIATION`; events `STEP_WAITING {reason, dueAt, operationId}`,
  `STEP_READY {reason: RETRY_DUE}`, `TASK_LOST/COMPLETED`; `reconciliation_cases`
  rows carry `reason` (`AMBIGUOUS_OUTCOME`, `IDEMPOTENCY_WINDOW_INSUFFICIENT`)
  and redacted evidence. No secrets in events.

## 5. Failure-matrix traceability

- F-05 (worker dies mid-task): lease expiry schedules a backoff timer
  (`TestLeaseExpirySchedulesBackoffTimer`).
- F-07 (provider success, completion uncommitted): `idempotent` replays same
  `operationId` within window; otherwise hold; `reconcile` holds on `UNKNOWN`.
- F-09 (scheduler restart during retry/delay): due persisted; restart test
  asserts identical `due_at`; duplicate firing test asserts single `READY`.

## 6. Migration and rollout

- Additive migration `00017_retry_durable_timers.sql`: nullable
  `run_steps.idempotency_valid_until`, nullable timer columns
  (`run_id/step_id/attempt_number/operation_id/reason/fired_at/updated_at`),
  partial unique pending identity + step index. No backfill; pre-M2 rows get
  `valid_until` lazily on next schedule without extending later retries.
- Binary rollback does not roll back the DB (forward fix only). No config/image
  changes beyond the control-plane binary; no UI changes in this issue.
