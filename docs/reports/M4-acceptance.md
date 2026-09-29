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

| Check                                   | Result                                                                    |
| --------------------------------------- | ------------------------------------------------------------------------- |
| Combined restart on staging             | NOT YET                                                                   |
| Two evaluators against staging          | YES (prior: #34 acceptance raced two evaluators on the deployed artifact) |
| DST/coalesce/overlap fixtures           | YES (prior: #34 staging + integration fixtures)                           |
| Browser approve/reject/expired journeys | NOT YET                                                                   |
| Schedule journeys                       | YES (prior: #35 staging journeys, 5/5 exit 0)                             |
| Deployed artifact for this gate         | NOT YET                                                                   |

## 4. Requirement / failure traceability

- REQ-CONTROL-01: conflicting decisions keep one truth (`ErrApprovalConflict`), stale revisions conflict, cancel closes pending approvals.
- REQ-TIME-01: downtime coalesces to one run with `skipped_count`; restart converges schedules; delays fire once.
- REQ-SEC-01: cross-tenant approvals/schedules/occurrences invisible (404); developer denied schedule writes (403).
- F-09 (cancel/terminal), F-14 (lease/attempt discipline), F-17 (unique occurrence), F-21 (tenant isolation), F-22 (quota without backlog): each has a named test above or in the orchestrated suites.

## 5. What this gate does not claim

- Host-failure resilience (two processes on one host prove worker-process
  failure only — same scope note as the M2 gate).
- Visual browser proof beyond the node DOM suite (recorded risk accepted
  in #35; functional journeys are API-driven against staging).
