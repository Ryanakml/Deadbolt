# Schedules and Waiting — User Guide (Issue #35)

How to drive recurring schedules, human approvals, and durable waits from
the dashboard and the API. Operator semantics (DST, misfire, overlap,
quota, revisions, pinning) live in
[recurring-schedules.md](./recurring-schedules.md); approval semantics in
[human-approvals.md](./human-approvals.md); delay/deadline semantics in
[delay-and-waiting-deadline-contract.md](./delay-and-waiting-deadline-contract.md).
This guide is the doing: which view, which call, what you see back.

---

## 1. Schedules view

Open **Schedules** in the nav with an environment selected. The list shows
one row per schedule in that environment:

- workflow name, cron expression, timezone, revision,
- deployment pin (`active-at-fire` when unpinned — the occurrence pins
  whatever deployment is active when it fires, not when you created the
  schedule),
- `skip-overlap` / `coalesce-one` policy labels (V1 fixes both; they are
  shown, not chosen),
- `ACTIVE` / `PAUSED` badge,
- **next due**, rendered from the persisted `nextDueAt` at render time.
  A paused schedule shows `paused` because the database holds
  `next_due_at = NULL` — the UI never recomputes cron client-side, so the
  countdown cannot drift from or reset the stored slot.

History expands per schedule: every decided slot, newest first, with due
time, revision, status (`STARTED`, `SKIPPED`, `PENDING`), the skipped
reason verbatim (`SKIPPED_OVERLAP`, `SKIPPED_QUOTA`, `SKIPPED_MISFIRE`),
the coalesced count, and a link to the run a started slot produced.
What you see is what the engine wrote — there is no synthesized history.

## 2. Create a schedule

**New schedule** asks for workflow, cron (5-field), timezone (IANA, `UTC`
default), and an optional deployment pin. Both policies are fixed, so the
form has no policy fields: a request carrying them is rejected rather
than silently ignored.

The call is `POST /v1/schedules?environment=<uuid>` with `Idempotency-Key`.
The response echoes the stored definition including revision 1 and the
first computed due time. An invalid cron or timezone fails here (422
`INVALID_CRON` / `INVALID_TIMEZONE`), not at 3am when the slot fires.

A schedule needs no input payload, so the workflow it points at must
accept an empty object. A workflow requiring caller-supplied input fails
run creation with `SCHEMA_VIOLATION` and the schedule is marked error and
paused (`WORKFLOW_REQUIRES_INPUT`) instead of retrying forever.

## 3. Edit a schedule (two-editor rule)

**Edit** loads the current definition and its revision. Saving sends
`PATCH /v1/schedules/{id}?environment=<uuid>` with `expectedRevision`
plus the new configuration. Every accepted edit advances the revision,
and the new revision affects **future** occurrences only — recorded
occurrences keep theirs, and their runs are untouched.

If someone else edited first, the save returns `409 REVISION_CONFLICT`.
The dialog says so, refreshes the definition in place, and updates the
revision it holds. It never retries blindly and never overwrites an edit
it never saw. Read the refreshed values, then save again.

## 4. Pause and resume

Pause and Resume are separate buttons, not an edit field, because they
have different effects: pausing clears the pending due time (a long pause
neither replays missed slots nor banks time) and frees one of the 100
active-schedule slots; resuming recomputes the next due from the moment
of resume. A second pause is `409 INVALID_SCHEDULE_STATE`, not a silent
success, so a caller can tell a real transition from a no-op.

A schedule the platform paused itself (`NO_ACTIVE_DEPLOYMENT`,
`WORKFLOW_REQUIRES_INPUT`, `INVALID_SCHEDULE`) stays paused until an
operator fixes the cause and resumes it. The audit log names the reason
(`schedule.error_pause`).

## 5. Waiting: approvals and delays in the same UI

Schedules create runs; runs wait. The Inspector shows the two wait kinds
next to the schedule that may have started them:

- **Approvals** (`PENDING` → decide approve/reject with comment; expired
  and cancelled decisions are final and render no payload beyond the
  decision record). See [human-approvals.md](./human-approvals.md).
- **Delays and deadlines** (durable timers the reconciler fires; pausing a
  run parks its timers). See
  [delay-and-waiting-deadline-contract.md](./delay-and-waiting-deadline-contract.md).

A schedule row links to the runs its slots produced (`?runId=`), so from
a missed slot you can walk to the run that is still holding the next slot
in `SKIPPED_OVERLAP`, or to the quota alert that explains a
`SKIPPED_QUOTA` gap.

## 6. Who can do what

Schedules require `schedules:write`, held by **operator, admin, and
owner** — deliberately not developer. A schedule is a standing grant to
create runs with nobody present, the same risk class as `runs:reconcile`
and `approvals:decide`. Without the capability the Schedules view renders
a notice and no mutation buttons; the backend answers 403 and the UI
exposes nothing beyond the denial. A developer can still register a
deployment and create runs explicitly.

## 7. What this does not buy you

- The UI cannot schedule a workflow that needs caller input (§17 gives a
  schedule no input payload).
- The UI cannot raise the 100-active-schedules cap, pick policies, or
  rewrite occurrence history.
- A countdown is a rendering of the stored slot, not a promise the
  sweeper fires on the displayed second.

---

## UI acceptance notes (Issue #35)

Acceptance contract from the issue, mapped to proof:

- **UI/API agree on next occurrence and skipped reasons.** The panel
  renders `nextDueAt`, `revision`, `skippedReason`, `skippedCount` from
  `GET /v1/schedules` and `GET /v1/schedules/{id}/occurrences`; the Go
  suite asserts the same rows the engine wrote
  (`tests/integration/schedule_occurrences_http_test.go`), and the node
  suite asserts the history renders skipped reasons verbatim
  (`apps/dashboard/tests/schedules-dialogs.test.js`).
- **Revision conflict stays in the edit dialog.** `409 REVISION_CONFLICT`
  refreshes the dialog in place with the new revision; covered in
  `schedules-dialogs.test.js` and
  `tests/integration/schedule_http_test.go`.
- **Denied/expired/cancelled states expose no unauthorized payload.**
  Without `schedules:write` no mutation CTA renders and the 403 path
  renders only the capability notice; approval payloads follow the
  existing redaction path. Covered in `schedules-dialogs.test.js`
  (no-CTA case) and `pause-dialogs.test.js` (redaction precedent).
- **Keyboard/tablet/mobile, light/dark, SSE reconnect.** Dialogs take
  focus, close on Escape and overlay click, and return focus to the
  invoker; layout reuses the responsive hold-panel/dialog styles;
  light/dark follows the shared theme toggle; schedule views refresh on
  nav, env change, and after mutations, and the existing run-stream SSE
  behaviour is untouched (stream tests still green).
- **No fake history or countdown resetting persisted due_at.**
  `formatScheduleNextDue` renders the persisted slot at render time and
  nothing writes it back; history renders exactly the API page.

Staging browser journeys (two-editor conflict, downtime history,
permission states) are recorded in
[reports/M4-schedule-ui-acceptance.md](./reports/M4-schedule-ui-acceptance.md).
