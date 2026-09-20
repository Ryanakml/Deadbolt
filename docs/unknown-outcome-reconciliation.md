# Unknown-Outcome Reconciliation Runbook (M2 — Issue #19)

Source of truth: `docs/blueprint.md` §10, §14, §15, §20, §23, §24.
This document is the delivery artifact for Issue #19. API contract:
`POST /v1/reconciliation-cases/{id}/resolve`.

## 1. What an unknown outcome is

A task attempt ends without proof of what the external provider did:
lease expiry, attempt timeout, or an `UNKNOWN` effect status under
`recovery: "reconcile"`. The platform must not repeat the work blindly
(INV-13). Instead the step parks in `WAITING / RECONCILIATION` with an
`OPEN` row in `reconciliation_cases`, and the run stops admitting new
claims until every hold resolves.

What the Inspector shows:

```text
Run is waiting for reconciliation
The provider may already have received the operation.
Check the external reference, then confirm succeeded / confirm not executed / fail.
```

## 2. Hold lifecycle

1. Loss, timeout, or an ambiguous error under `reconcile` (or an
   idempotency window that cannot cover another attempt) creates the hold:
   step `WAITING / RECONCILIATION`, run `WAITING / RECONCILIATION` once
   sibling attempts drain, `STEP_WAITING` event, and an `OPEN` case carrying
   the hold reason, operation ID, and attempt reference.
2. While any `OPEN` case exists for a run, the gateway admits no new claims
   for that run — even if its status still reads `RUNNING` during drain.
   Already-running siblings may still start, heartbeat, and complete; the
   run parks on the hold only after the last sibling settles.
3. The run deadline stays active while held. An overdue held run with no
   live work is terminalized as `FAILED / RUN_DEADLINE_EXCEEDED` and its
   holds close as system decisions.
4. A terminal run never reopens (INV-09). Resolving against one returns
   `409 RUN_TERMINAL`.

## 3. The three audited resolutions

Every decision binds the case `expectedRevision` (stale readers get `409
REVISION_CONFLICT` and must refresh from the dialog), an evidence
reference, and the deciding human identity. Machine API keys are denied
(`403`); `runs:reconcile` requires the Operator, Admin, or Owner role.

| Action                       | Requires                                                | Effect                                                                                                                                                                                                                     |
| ---------------------------- | ------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `confirm_succeeded`          | schema-valid `result` + `evidence` reference            | Held attempt keeps `LOST`/`TIMED_OUT`; step becomes `SUCCEEDED` with `completion_source=RECONCILIATION`. Dependents unblock; a finished workflow completes with the confirmed result.                                      |
| `confirm_not_executed_retry` | `evidence` reference declaring the effect did not occur | One retry intent within the remaining budget, same operation ID, fresh persisted backoff timer. Exhausted budget, insufficient idempotency window, or insufficient run deadline refuse with `409` and leave the hold open. |
| `fail_run`                   | `evidence` reference                                    | Run becomes `FAILED / RECONCILIATION_FAILED`; remaining holds close as decisions of the same actor.                                                                                                                        |

Repeat decisions against an already-resolved case return `409
CASE_RESOLVED`. Two concurrent resolvers produce exactly one decision;
the loser reads the committed outcome from the `409` response.

## 4. Side-effect evidence guidance

`evidence` must be an external reference the resolver actually checked —
never secret material:

- Payment/ledger providers: the provider-side payment or idempotency-key
  lookup result (e.g. `pi_3O...`, key status `processed`).
- Messaging providers: the provider message ID or queue receipt.
- Retries (`confirm_not_executed_retry`): the provider lookup proving
  absence (e.g. `GET /charges?op=...` → empty), quoted as the reference.
- `fail_run`: the observation behind stopping (e.g. duplicate confirmed
  under a different operation, downstream incident link).

The reference is stored on the resolution audit record
(`audit_events` action `reconciliation.resolve`) together with actor,
role, hold reason, and result digest. The hold's original evidence stays
untouched on the case row.

## 5. Operator checklist

1. Open the held run in the Inspector; read the hold reason and the
   operation ID from the hold banner.
2. Query the provider with the operation ID (or a deterministic subkey).
3. If the effect happened: `confirm_succeeded` with the schema-valid
   result and the provider reference.
4. If it provably did not: `confirm_not_executed_retry` with the lookup
   reference; the next attempt reuses the same operation ID.
5. If neither can be established, or the effect must not repeat:
   `fail_run`, then start an explicit new run if business needs it.
6. On `409 REVISION_CONFLICT`/`CASE_RESOLVED`: reload the snapshot — the
   dialog never retries blindly.
7. On `409 BUDGET_EXHAUSTED`/`INSUFFICIENT_WINDOW`/`RUN_DEADLINE_EXCEEDED`:
   the hold stays open; fail the run or wait for the deadline sweeper.

## 6. Observability

- Execution events: `STEP_WAITING` (`RECONCILIATION` + hold reason),
  `STEP_SUCCEEDED` (`completionSource: RECONCILIATION`), `STEP_WAITING`
  (`RETRY_BACKOFF` for resolve-created intents), `CASE_RESOLVED`,
  `RUN_WAITING`, `RUN_RESUMED`, `RUN_FAILED`.
- `GET /v1/runs/{id}` exposes `reconciliationCases` (id, step, reason,
  evidence reference, status, resolution, actor, revision) and per-step
  `completionSource` for the Inspector hold banner and resolve dialog.
- The live snapshot converges through SSE (`step.waiting`,
  `step.succeeded`, `run.resumed`, `run.failed`); a `409` in the dialog
  always triggers a snapshot refetch.

## 7. Migration and rollout

- Additive migration `00018_reconciliation_resolution.sql`: nullable
  `run_steps.completion_source` (`WORKER`/`RECONCILIATION`; `NULL` means
  the pre-existing worker/normal path). No backfill; binary rollback does
  not roll the database back — forward fix only.
- No config, image, or topology changes beyond the control-plane binary.
