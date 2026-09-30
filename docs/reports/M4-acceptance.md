# M4 Acceptance — Gate #36

Evidence and acceptance verification for **[M4] Gate M4: recover durable
waits and unique human/time actions** (Issue #36). M4 is human waiting
and time: approvals (#32), delays (#33), recurring schedules (#34), and
the schedule/waiting UI (#35). This gate proves they compose — one
workflow waiting on a person, then on a timer, then running — while
processes die and evaluators race.

Run the local gate: `./scripts/m4-gate.sh [--new-only]`.

---

## 1. Combined waits (new in this gate)

`tests/integration/m4_gate_test.go` wires `build → gate(approval) →
wait(delay 60s) → publish` in one workflow (`wf-m4-combined`):

| Test                                             | What it proves                                                                                                                                                                                                                                                                                                       |
| ------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `TestM4_CombinedApprovalDelayRestartDecidesOnce` | Build completes → PENDING approval; a recreated engine decides APPROVED; an opposing decision conflicts (`ErrApprovalConflict`); the released delay timer fires exactly once across a second engine recreation (1 then 0); publish consumes the committed decision; run SUCCEEDED with exactly one recorded decision |
| `TestM4_CombinedStaleRevisionCannotDecide`       | A stale `expectedRevision` conflicts and leaves the approval PENDING inside the combined graph                                                                                                                                                                                                                       |
| `TestM4_CombinedExpiredApprovalFailsRun`         | Deciding past the deadline is refused (`ErrApprovalExpired`), the run fails with `APPROVAL_EXPIRED`, and the downstream delay timer is never scheduled                                                                                                                                                               |
| `TestM4_ScheduleRestartEvaluatesOnce`            | A recreated occurrence engine evaluates a due schedule once (`STARTED`), then `NOT_DUE`; exactly one occurrence and one run — restart converges instead of replaying                                                                                                                                                 |

Restart means a brand new `WorkerEngine` / `Engine` against the same
PostgreSQL: no wait depends on surviving process memory, and no runner
is ever held for an approval or a delay (both settle without attempts).

## 2. Per-issue suites orchestrated by the gate

| Group                    | Suite                                                                                                                                                                                                            | Result                                                                                                                                       |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `m4-gate-new`            | 4 combined/restart tests                                                                                                                                                                                         | PASS locally (4 passed, 0 skipped, full-gate run)                                                                                            |
| `m4-approvals`           | `TestApproval_*` (12 tests: waits without lease/attempt, idempotent + conflicting decisions, database-time expiry, sweeper settlement, cancel closure, restart survival, concurrent single-winner, cross-tenant) | PASS locally                                                                                                                                 |
| `m4-delays`              | `TestDelay*` (persist + fire, restart-settles-once, pause preserves deadline, deadline cannot reopen)                                                                                                            | PASS locally (full-gate run)                                                                                                                 |
| `m4-schedules`           | `TestSchedule*` + `TestOccurrence*` (service, HTTP, engine, two-evaluator races, DST/coalesce/overlap/quota, occurrences endpoint)                                                                               | PASS locally (full-gate run)                                                                                                                 |
| `dashboard-suite`        | `pnpm --filter @runtime/dashboard test` (92 tests incl schedule views)                                                                                                                                           | PASS locally                                                                                                                                 |
| `cumulative-regressions` | Choice/merge determinism, pause/cancel single-winner, inspector parity                                                                                                                                           | PASS locally (real-agent execution and M2 two-worker recovery stay owned by the M2/M3 gates — bundle- and host-dependent, not re-owned here) |

## 3. Staging evidence

Artifact: commit `d040e05bfb9786a24992e34d5420143d43f650af` (PR #87
head; tests/harness/docs only — zero production-code delta vs
`cda91c1`, so the restart/API evidence below carries over
behaviorally), image
`ghcr.io/ryanakml/deadbolt/control-plane@sha256:619b334ab710535233bbd6bb14f6ec2ef9622edd12862d62fdb2bc13dd5f75ad`,
`runtime_mode: hosted` (slot green). Isolated fixtures in project
`m4-gate-accept` (`94fdbb74-…`), environment `5742f463-…`, deployment
`d11747d4-…`; shared staging env untouched. Runnable:
`scripts/m4-gate-staging-journeys.sh` (exit code 0 on `cda91c1`;
re-runnable on any head with no production delta). The restart below is
a real `docker restart` of the active control-plane container mid-run —
same image back, all waits settled by the new process.

| Check                                   | Result                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Combined restart on staging             | PASS — two PENDING approvals + one due schedule planted pre-restart; new process decides approval A once (opposing 409, machine key 403 `APPROVAL_HUMAN_ONLY`), fires the released delay exactly once, converges the schedule (2 occurrences, 1 run, `skipped_count=9`)                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| Two evaluators against staging          | YES (prior: #34 acceptance raced two evaluators on the deployed artifact)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| DST/coalesce/overlap fixtures           | YES (prior: #34 staging + integration fixtures)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| Browser approve/reject/expired journeys | PASS — decided through the real dashboard on `d040e05` (session `brian.sbg69@…`, env `m4-gate-accept / staging`): APPROVED run `6af57696-…` (approval `031fbf39-…`, actor `059270b1-…`, comment recorded, rev 1→2, decided 2026-09-30 12:42:08 UTC, gate SUCCEEDED, 0 attempts); REJECTED run `924a7161-…` (approval `3262553e-…`, same actor, comment recorded, decided 12:43:20 UTC, rev 2, gate still SUCCEEDED — business branch, not a failure); EXPIRED run `fef941d2-…` (approval `65884b8d-…`, gate FAILED `APPROVAL_EXPIRED`, run FAILED, no Decide CTA). Decided approvals render no Decide CTA (terminal, honest); opposing-decision 409 + machine-key 403 `APPROVAL_HUMAN_ONLY` proven at API level by the journeys script |
| Schedule journeys                       | YES (prior: #35 staging journeys, 5/5 exit 0) + browser History on `d040e05`: schedule `6312ddec-…` (`m4j-tick`, `* * * * *`, UTC, rev 1, PAUSED, `next due paused`) expands to `STARTED skipped 9` run `5084829c-…` plus `SKIPPED SKIPPED_OVERLAP`; left paused with journey keys/session revoked                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| Deployed artifact for this gate         | YES — `d040e05`, image `619b334a…`, hosted (slot green)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |

## 4. Requirement / failure traceability

- REQ-CONTROL-01: conflicting decisions keep one truth (`ErrApprovalConflict`), stale revisions conflict, cancel closes pending approvals.
- REQ-TIME-01: downtime coalesces to one run with `skipped_count`; restart converges schedules; delays fire once.
- REQ-SEC-01: cross-tenant approvals/schedules/occurrences invisible (404); developer denied schedule writes (403).
- F-09 (cancel/terminal), F-14 (lease/attempt discipline), F-17 (unique occurrence), F-21 (tenant isolation), F-22 (quota without backlog): each has a named test above or in the orchestrated suites.

## 5. What this gate does not claim

- Host-failure resilience (two processes on one host prove worker-process
  failure only — same scope note as the M2 gate).
- End-to-end run SUCCEEDED on the staging fixture: after both browser
  decisions the released delay fired and `hold` SUCCEEDED, but the
  staging `m4j-wait` fixture's downstream `done` task maps the gate
  decision output into `t-done` (which requires `{"n"}`), so both runs
  end `FAILED INPUT_MAPPING_ERROR` — a pre-existing staging-manifest
  wart (the script's own run `a546737b-…` shows the same), not a product
  regression. Full combined SUCCEEDED is proven by the Go gate test
  above, which uses a schema-correct fixture.
- Keyboard/tablet/mobile, light/dark, and SSE reconnect stay covered by
  the node DOM suite and #35 acceptance; this gate re-proves them only
  where the journeys above click through them.
