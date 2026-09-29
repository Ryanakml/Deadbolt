# M4 Schedule UI Acceptance — Issue #35

Evidence and acceptance verification for Issue #35: **[M4] Show schedule
revisions, waits, and missed occurrence reasons** (PR #86).

Staging artifact: commit `d2c2994d09fc2f3a5c8451e74904bc7d3f2e1b60`,
image `ghcr.io/ryanakml/deadbolt/control-plane@sha256:ee1ea63d72adc9a24b6d7502b0d74516cdf157a63339011ba80010db2c27ea23`,
`runtime_mode: hosted` (verified via slot `/version` during the run).
Org `Deadbolt Acceptance` (`674b13a3-…`); isolated fixture project
`sched-ui-accept` (`70392567-…`), env (`614a97bb-…`), deployment
(`c2d3d582-…`, manifest copied from the staging `sched-occ-flow`
deployment). Shared staging env untouched. Runnable command:
`scripts/schedule-ui-staging-journeys.sh` (exit code 0).

---

## 1. What was built

- Backend: scoped `GET /v1/schedules/{id}/occurrences` (keyset,
  newest-first, `schedules:write`), `nextDueAt`/`lastOccurrenceAt` on the
  Schedule DTO, OpenAPI `ScheduleOccurrence` + `listScheduleOccurrences`
  + declared pause/resume ops.
- Dashboard: Schedules nav + panel + create/edit/pause/resume/delete
  dialogs + expandable occurrence history; countdown from persisted
  `nextDueAt`; `schedules:write` predicate (operator/admin/owner).

## 2. Automated proof (local)

| Suite | Result |
| --- | --- |
| Go integration (`TestOccurrenceHTTP*`, `TestScheduleHTTP*`) | PASS — started/skipped reason agreement, keyset pages, 403/404/400 scope matrix, cross-tenant 404, persisted due-time exposure |
| Dashboard node (`apps/dashboard/tests/schedules*.test.js`) | PASS — client wire incl CSRF + Idempotency-Key + expectedRevision, permission predicates (developer denied), dialog 409 in-place refresh, no-CTA case, verbatim skipped reasons |
| Dashboard suite total | 92 pass, 0 fail; `tsc --noEmit` clean |
| `check:contracts` | PASS (OpenAPI + generated schema consistency) |

## 3. UI invariants and where each is proven

1. **History is the API page, verbatim.** The history expander renders
   `dueAt`, `revision`, `status`, `skippedReason`, `skippedCount`, `runId`
   from `listScheduleOccurrences` with no synthesis; empty reads render
   "no occurrences yet". Node: history block of
   `schedules-dialogs.test.js`. Go: `TestOccurrenceHTTPListsStartedAndSkippedReasons`.
2. **Countdown never writes back.** `formatScheduleNextDue` reads the
   persisted slot at render time; paused renders `paused`. No PUT/PATCH
   in the render path by construction (client methods are only called
   from dialog submits).
3. **409 stays in the dialog.** `isConflict` triggers in-dialog refresh +
   revision update; no blind retry. Node: conflict case in
   `schedules-dialogs.test.js`. Go: revision-conflict cases in
   `schedule_http_test.go`.
4. **403 renders a notice, no CTAs, no payload.** `canManageSchedules`
   gates every mutation button; the list 403 path renders only the
   capability notice. Node: no-CTA case in `schedules-dialogs.test.js`.
5. **Keyboard and focus.** Dialog open focuses the first field; Escape
   and overlay click close; focus returns to the invoker. Node: dialog
   cases in `schedules-dialogs.test.js` (same harness as
   `pause-dialogs.test.js`).
6. **Responsive + theme.** Schedule styles reuse hold-panel/dialog/badge
   with additions inside the existing 900px/600px media blocks; theme
   follows the shared toggle (`theme.test.js` still green).
7. **SSE untouched.** No schedule stream was added; run-stream freshness,
   reconnect, and resync paths are unchanged (`stream.test.js` green).

## 4. Browser journeys (staging, real APIs)

Journeys drive the exact calls the dashboard makes, against the deployed
artifact; the production sweeper did the acting. Fixture schedule
`8857598c-8e81-4238-85e6-2f48a6136cfc` (left paused, journey keys revoked).

| Journey | Steps | Result |
| --- | --- | --- |
| Two-editor conflict | Save edit A (rev 1→2), save stale edit B → `409 REVISION_CONFLICT`; refresh shows A's cron at rev 2 intact | PASS |
| Downtime history | Per-minute cron planted 9–10 min behind → occurrences endpoint shows STARTED with `skipped_count=9` and exactly one run | PASS |
| Permission states | Developer key → 403 on list, create, occurrences; anonymous → 401; operator full round trip | PASS |
| Pause/resume | Pause clears `nextDueAt` (second pause → 409), resume re-arms a fresh due time | PASS |

## 5. Staging artifact

- Commit: `d2c2994d09fc2f3a5c8451e74904bc7d3f2e1b60`
- Image digest: `ghcr.io/ryanakml/deadbolt/control-plane@sha256:ee1ea63d72adc9a24b6d7502b0d74516cdf157a63339011ba80010db2c27ea23`
- `/version` through the slot: `{"version":"0.1.0","commit":"d2c2994…","runtime_mode":"hosted"}` (slot green, port 8089)
