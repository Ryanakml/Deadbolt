# Recurring schedules: API and operational semantics

Blueprint §17, §20, §24.2, §27.1. Implements Issue #34.

A recurring schedule turns a 5-field cron expression into runs, with the
platform holding no process alive for the duration of the wait. This
document covers the API surface and the behaviour an operator has to reason
about: DST, misfires, overlap, quota, revisions, and pinning.

## API

All routes require the `schedules:write` capability and an `environment`
query parameter. Mutating routes require an `Idempotency-Key`, and are
idempotent per the platform's durable command record (§20.1).

| Method   | Path                                         | Purpose                                |
| -------- | -------------------------------------------- | -------------------------------------- |
| `GET`    | `/v1/schedules?environment=<id>`             | List schedules in an environment       |
| `POST`   | `/v1/schedules?environment=<id>`             | Create a schedule                      |
| `PATCH`  | `/v1/schedules/{id}?environment=<id>`        | Edit the definition                    |
| `DELETE` | `/v1/schedules/{id}?environment=<id>`        | Delete the definition                  |
| `POST`   | `/v1/schedules/{id}/pause?environment=<id>`  | Stop future occurrences                |
| `POST`   | `/v1/schedules/{id}/resume?environment=<id>` | Re-arm and recompute the next due time |

Create body:

```json
{
  "workflow": "nightly-recon",
  "cron": "0 12 * * *",
  "timezone": "UTC",
  "deploymentId": null
}
```

`cron` is 5-field and `timezone` is an IANA zone name defaulting to `UTC`.
`deploymentId` is optional; see **Deployment pinning**.

The overlap and misfire policies are **not** request fields. §17 fixes both
for V1 (`skip-overlap` and `coalesce-one`) and the schema constrains each to
that single value, so a body carrying either is rejected as an unknown
field rather than silently ignored. Accepting them would imply a choice that
does not exist.

`PATCH` takes a required `expectedRevision` and nests the desired state:

```json
{
  "expectedRevision": 3,
  "configuration": {
    "workflow": "nightly-recon",
    "cron": "30 6 * * *",
    "timezone": "UTC"
  }
}
```

A stale `expectedRevision` is `409 REVISION_CONFLICT`. A second pause is
`409`, not a silent success, so a caller can tell a real transition from a
no-op.

### Authorization

`schedules:write` is held by **operator, admin, and owner**. It is
deliberately not granted to developer: a schedule is a standing grant to
create runs with no operator present, which is the same risk class as
`runs:reconcile` and `approvals:decide`, both operator and above. A
developer can still register a deployment and create runs explicitly, just
not unattended on a timer. Blueprint §24.2 has no schedule column, so this
mapping is recorded as a bounded change proposal in PR #85 rather than
treated as pre-existing.

Every mutation is audited as a `schedule` resource with the role and the
effective capabilities at the time the action was accepted.

## Limits

| Limit                            | V1 value |
| -------------------------------- | -------- |
| Active schedules per environment | 100      |

A paused schedule does not count. The cap is checked while holding the
environment admission advisory lock, together with the insert, so
concurrent creates cannot both observe a free slot and both insert. Reaching
the cap is `409 SCHEDULE_LIMIT_REACHED`; pausing a schedule frees a slot.

A tenant may hold a lower limit. The hard cap cannot be raised through an
API payload.

## What a schedule is not

§17 gives a schedule no input payload, so a scheduled run starts from an
empty object. A workflow whose input schema requires caller-supplied fields
**cannot be scheduled** and fails validation at run creation with
`SCHEMA_VIOLATION`. Model the scheduled workflow to accept an empty input.

## Deployment pinning

By default an occurrence pins the deployment that is **active in the
workflow's channel at the moment the occurrence fires**, not the deployment
active when the schedule was defined. A schedule created while version A is
active and fired while version B is active runs B.

Set `deploymentId` to pin a specific deployment. The pin must belong to the
same organization and environment; a cross-tenant or cross-environment pin
is refused at the database boundary by the composite foreign key from
migration 00025.

A schedule pointing at a workflow with no active deployment is marked error
and **paused**, with an audit record. It stays paused until an operator
activates a deployment and resumes it. It does not retry silently.

## Occurrence identity and idempotency

An occurrence is uniquely identified by `(schedule_id, revision, due_at_utc)`.
Two database constraints enforce it — `(schedule_id, occurrence_key)` from
00004 and `(schedule_id, revision, due_at)` from 00025 — and the insert
relies on both, so two evaluators racing the same schedule produce exactly
one occurrence and one run.

The run's idempotency key is derived from the occurrence rather than from
wall-clock time, so the same slot always presents the same key. A crash
between inserting the run and advancing the schedule rolls the whole
transaction back, and the retry produces exactly one run.

## Misfire: coalesce-one

After downtime a schedule does **not** replay its backlog. It creates at
most one run for the latest missed occurrence, records how many occurrences
were coalesced on that occurrence's `skipped_count`, and then continues at
the next future slot.

A ten-minute outage on a per-minute schedule produces one run and a
`skipped_count` of about nine, not ten runs.

`next_due_at` advances from the occurrence that was handled, not from the
moment of evaluation, so a long outage is walked forward one coalesced slot
at a time and converges rather than jumping. Coalescing never re-issues a
slot that already has an occurrence, so a slot is never run twice.

## Overlap: skip if the previous run is nonterminal

If the run this schedule most recently created is still nonterminal
(`QUEUED`, `RUNNING`, `WAITING`, `PAUSING`, `PAUSED`, or `CANCELLING`), the
next slot is recorded as `SKIPPED` with `skipped_reason = SKIPPED_OVERLAP`
and no run is created. The schedule still advances, so a skipped slot is not
retried indefinitely and the schedule cannot fall permanently behind.

A rejected approval inside a scheduled run does not trigger this: the run
still reaches a terminal state, so the next slot is unaffected.

## Quota

If admission refuses the run — the environment nonterminal run cap, the
create-run rate bucket, or disaster-recovery read-only mode — the slot is
recorded as `SKIPPED` with `skipped_reason = SKIPPED_QUOTA`, no run is
created, and a `schedule.occurrence_skipped` audit record is written.

The platform does not build a hidden backlog of runs waiting for capacity.
A schedule silently losing slots to quota is a visible, alertable condition
rather than a growing queue.

## DST

Wall times that never occur on a given date are **skipped**; wall times
that occur twice use the **first** UTC occurrence and the second is not
replayed.

The SP-05 evaluation in `tests/spikes/sp05` rejected both
`robfig/cron/v3` and `gorhill/cronexpr` because neither satisfies both
invariants, and neither implements coalesce-one or skip-overlap. The
in-repo evaluator in `internal/scheduling` is therefore the implementation
of record; the decision record is in `tests/spikes/sp05/candidate_test.go`.

Verified cases include the New York and London gaps and folds, and the
Lord Howe half-hour gap.

## Revisions

Editing a schedule advances its revision. By §17 an edit affects only
**future** occurrences: occurrences already recorded keep the revision they
were created under, and the runs they produced are untouched. Changing a
cron expression does not retroactively reschedule work that already exists.

## Observability

| Record                                                                          | Meaning                                                  |
| ------------------------------------------------------------------------------- | -------------------------------------------------------- |
| `schedule_occurrences.status`                                                   | `PENDING`, `STARTED`, or `SKIPPED`                       |
| `schedule_occurrences.skipped_reason`                                           | `SKIPPED_OVERLAP`, `SKIPPED_QUOTA`, or `SKIPPED_MISFIRE` |
| `schedule_occurrences.skipped_count`                                            | Occurrences coalesced into this one                      |
| `schedule_occurrences.run_id`                                                   | The run this slot produced, when it produced one         |
| `schedule.next_due_at`                                                          | Next pending slot; `NULL` while paused                   |
| `schedule.last_occurrence_at`                                                   | Most recent slot handled                                 |
| `audit_events` `schedule.create` / `.update` / `.pause` / `.resume` / `.delete` | Operator actions                                         |
| `audit_events` `schedule.error_pause`                                           | Schedule paused because it is broken                     |
| `audit_events` `schedule.occurrence_skipped`                                    | A slot lost to quota, with the reason                    |

## Recovery and operations

A restart is not an event for a schedule. `next_due_at` and the occurrence
rows are in the database, so a fresh process resumes from the stored slot
and the misfire policy decides what a gap means. The scheduler runs on the
existing bounded reconciler pass, so a schedule that was not due is a no-op
and a duplicated pass creates nothing extra.

To investigate a schedule that is not producing runs, in order:

1. Is it `paused`? A paused schedule has `next_due_at = NULL`.
2. Did it hit an error pause? Look for `schedule.error_pause` in the audit
   log; the reason is `NO_ACTIVE_DEPLOYMENT` or `INVALID_SCHEDULE`.
3. Are occurrences being recorded but skipped? Read `skipped_reason` and
   `skipped_count`.
4. Is the environment over quota or in disaster-recovery read-only?
5. Is `next_due_at` behind `clock_timestamp()`? If so the pass is behind and
   the run history will show the delay.

## Schema and rollout

No new migration. `schedules` and `schedule_occurrences` already carry the
policy columns, `revision`, `skipped_reason`, `skipped_count`,
`next_due_at`, `last_occurrence_at`, the pinned-deployment composite
foreign key, and the canonical unique index, from migrations 00004 and 00025.

The change is additive: new service and engine code, new routes behind an
existing capability, and a new sweep on the existing reconciler pass.
Rolling back the binary does not require rolling back the database, and no
existing row is reinterpreted — a schedule that existed before this change
is evaluated by the same policies afterwards.
