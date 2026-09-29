# M4 Schedule UI Acceptance — Issue #35

Evidence and acceptance verification for Issue #35: **[M4] Show schedule
revisions, waits, and missed occurrence reasons** (PR #86).

Staging artifact for the journeys below: recorded in §5 when the run
lands. Until then every staging row is NOT YET.

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

| Journey | Steps | Result |
| --- | --- | --- |
| Two-editor conflict | Open one schedule in two edit dialogs (two sessions); save A, save B with stale revision → B sees in-dialog conflict, refreshes, saves cleanly | NOT YET |
| Downtime history | Force a schedule behind, let the sweeper coalesce, open History → one STARTED with `skipped_count`, slot advanced | NOT YET |
| Permission states | Operator sees New/Edit/Pause/Delete; developer key sees notice + no CTAs; direct API with developer key → 403 | NOT YET |

## 5. Staging artifact

- Commit: TBD
- Image digest: TBD
- `/version` through the edge: TBD
