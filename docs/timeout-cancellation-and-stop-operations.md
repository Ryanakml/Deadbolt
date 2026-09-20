# Timeout, Cancellation, and Stop Operations (M2 — Issue #20)

Source of truth: `docs/blueprint.md` §10, §12, §15, §17, §23.
This document is the delivery artifact for Issue #20.

## 1. Deadline contract

| Deadline           | Default / limit                                     | Enforcement                                                                                                                                                                                                                    |
| ------------------ | --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Attempt execution  | Default 5 minutes, maximum 1 hour                   | Clamped at claim; checked against DB time on Start, heartbeat, and completion — never only by the sweeper. Expiry marks the attempt `TIMED_OUT` and follows retry/reconcile policy.                                            |
| Run lifetime (MVP) | Default/maximum 24 hours from create-run acceptance | Stored as `runs.deadline_at`; checked on claim, Start, heartbeat, and before scheduling or firing retries. A lapsed deadline with no live work terminalizes the run as `FAILED / RUN_DEADLINE_EXCEEDED`, closing mooted holds. |
| Claim-to-start     | 5 seconds                                           | Unchanged (Issue #17).                                                                                                                                                                                                         |
| Lease TTL          | 30 seconds, 5-second heartbeat                      | Unchanged (Issue #17).                                                                                                                                                                                                         |
| Cancellation grace | 10 seconds                                          | `CANCELLING` settles to `CANCELLED` after every stop ACK or when the grace expires.                                                                                                                                            |

Timeouts are never silently extended: pause (M3) does not freeze the run
deadline, and `Retry-After` never raises a delay past the policy cap.

## 2. Cancellation contract

1. `POST /v1/runs/{id}/cancel` with `expectedRevision` commits the cancel:
   leases revoked, nonterminal steps/attempts marked `CANCELLED`,
   pending timers cancelled, open reconciliation holds closed as `CANCEL`
   with the deciding actor, and one stop command per live attempt with a
   10-second deadline. Already-`SUCCEEDED` steps are preserved.
2. With live attempts the run waits in `CANCELLING`; with none it settles
   to `CANCELLED` immediately as confirmed.
3. Each stop ACK re-checks settlement; the sweep settles overdue grace
   even across control-plane restarts.
4. `termination_confirmed` is `true` only when every stop was acknowledged
   (or none was needed), `false` when the grace expired with stops
   unacknowledged, and unset while not settled.

State priority (§10.2) applies centrally: a committed terminal state never
reopens, and a committed cancel outranks any later failure or deadline.
A completion that committed before cancel is preserved; cancel returns
`409 RUN_TERMINAL` on terminal runs, and results arriving after the cancel
commit are rejected. Duplicate cancels while `CANCELLING` are idempotent;
stale revisions return `409 REVISION_CONFLICT`.

## 3. Stop-risk wording (UI, API, and runbooks)

Use exactly this meaning everywhere cancellation is offered:

> Cancelling stops platform execution and revokes worker ownership.
> External side effects already performed are **not** rolled back.
> `termination_confirmed: false` means worker processes may still be
> running — verify provider state; never assume money moved, messages
> unsent, or records deleted themselves.

The Inspector shows `Cancelling — waiting for workers…` while settling
and a distinct unconfirmed warning afterwards. Mutation errors stay
inside the originating dialog with a refresh path on `409`.

## 4. Operations guide

- **Stuck `CANCELLING`:** reads `stop_commands` for unacked rows past
  `deadline_at`; the reconciler settles them automatically. If workers
  are gone for good, settlement still completes as unconfirmed — no
  manual DB write is ever required.
- **Deadline incidents:** overdue runs fail closed with
  `RUN_DEADLINE_EXCEEDED`; open holds close as system decisions with a
  `NULL` actor, distinguishable from human resolutions in audit.
- **Disputed cancel vs completion:** the committed order decides.
  `TASK_COMPLETED` before the cancel event means success stands;
  `STALE_OWNERSHIP` after it means the cancel won. Both are visible in
  the event timeline with the run's last event sequence as cursor.
- **Restart during grace:** safe by construction — settlement reads only
  committed `stop_commands` and run state, so a fresh scheduler
  continues where the old one stopped.

## 5. Migration and rollout

- Additive migration `00019_cancel_termination.sql`:
  `runs.termination_confirmed` plus overdue-deadline and unacked-stop
  sweep indexes. No backfill; binary rollback does not roll the database
  back — forward fix only.
- No config, image, or topology changes beyond the control-plane binary
  and rebuilt dashboard assets.
