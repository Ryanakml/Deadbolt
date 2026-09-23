# SP-05 — 5-Field Cron and Timezone Conformance Report

**Scope:** Issue #30.  
**Dependencies:** Gate #25 (M2 acceptance merged in `8661529`).  
**Blueprint references:** §17 (Durable timer dan recurring schedule), §18.1 (Entitas dan constraints), §28 (`F-17`: Cron duplicate / downtime / DST), §31 (Definition of Done), §32 (Fixed ADR baseline), §33 (Spike SP-05: Cron/timezone fixtures).  
**Requirements & Invariants:** `REQ-TIME-01`, `INV-10`, `SP-05`, `F-17`.

---

## 1. Outcome and Legitimate Uncertainty

Spike SP-05 evaluates 5-field cron parsing, IANA timezone resolution, and schedule advancement policies against the strict durability and correctness invariants established in Blueprint §17:

1. **DST Missing Wall Time (Spring Forward Gap):** Clocks jumping forward (e.g. 02:00 $\rightarrow$ 03:00) create wall clock moments that never occur. Blueprint §17 mandates: _“wall time yang tidak pernah terjadi dilewati”_. Schedulers must skip the gap entirely rather than executing at an arbitrary shifted time.
2. **DST Repeated Wall Time (Fall Back Fold):** Clocks falling back (e.g. 02:00 $\rightarrow$ 01:00) cause wall clock moments to occur twice in UTC. Blueprint §17 mandates: _“wall time yang muncul dua kali hanya diambil occurrence UTC pertama”_. Schedulers must emit strictly the first UTC occurrence and skip the duplicate UTC occurrence.
3. **Downtime Misfire Policy (`coalesce-one`):** Following control plane downtime or extended pauses where multiple scheduled occurrences elapse, Blueprint §17 mandates: _“setelah downtime buat maksimal satu run untuk occurrence terbaru yang terlewat, simpan jumlah occurrence yang dilewati, lalu hitung next future occurrence”_. Unchecked backlog floods are prohibited.
4. **Overlap Policy (`skip-overlap`):** When a scheduled occurrence becomes due while the previous run remains nonterminal (`QUEUED`, `RUNNING`, `WAITING`, `PAUSING`, `PAUSED`, `CANCELLING`), Blueprint §17 mandates: _“skip if previous scheduled run still nonterminal; tidak menumpuk backlog otomatis”_. Schedulers must record a `SKIPPED` occurrence with reason `SKIPPED_OVERLAP`.
5. **Schedule Revision Isolation:** Editing a schedule creates a new revision (`revision = revision + 1`) and only affects future occurrences. In-flight runs and past occurrences remain pinned to their original revision. Occurrence identity is uniquely scoped to `(schedule_id, revision, scheduled_at_utc)` preventing duplicate executions (`INV-10`).

---

## 2. Pinned Candidates Actually Executed and Evidence-Based Decision

All three candidates below were executed against the shared fixtures in `contracts/fixtures/schedules.json` via `tests/spikes/sp05/candidate_test.go` using location-aware input (`ref.In(loc)`). No claim below is made about a library that was not executed.

| Candidate                               | Exact version (go.mod)                                                                                                                                                 | Observed results (location-aware `Next`)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | Decision                                                                                                                                            |
| :-------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------- |
| **robfig/cron/v3**                      | `github.com/robfig/cron/v3 v3.0.1` (parser `Minute\|Hour\|Dom\|Month\|Dow`)                                                                                            | NY gap `30 2 * * *` ref `2026-03-07T07:30:00Z` => `2026-03-09T06:30:00Z` PASS. London gap `30 1 * * *` ref `2026-03-28T01:30:00Z` => `2026-03-30T00:30:00Z` PASS. Lord Howe `15 2 * * *` ref `2026-10-03T15:00:00Z` => `2026-10-04T15:15:00Z` PASS. NY fold `30 1 * * *` ref `2026-11-01T04:00:00Z` => first `2026-11-01T05:30:00Z` PASS, second `2026-11-01T06:30:00Z` FAIL (emits duplicate UTC occurrence; expected `2026-11-02T06:30:00Z`). London fold ref `2026-10-25T00:00:00Z` => first `2026-10-25T00:30:00Z` PASS, second `2026-10-25T01:30:00Z` FAIL (duplicate same day; expected `2026-10-26T01:30:00Z`). Hourly downtime (last `10:00Z`, now `14:30Z`) naive `Next` iteration queues 4 runs (`11:00,12:00,13:00,14:00`) — backlog flood without `coalesce-one`. | **REJECTED unguarded.** Passes gaps but violates §17 fold rule (emits both UTC occurrences) and has no `coalesce-one`/`skip-overlap` policy.        |
| **gorhill/cronexpr**                    | `github.com/gorhill/cronexpr v0.0.0-20180427100037-88b0669f7d75` (`cronexpr.Parse` / `Next`)                                                                           | NY gap => `2026-03-08T06:30:00Z` (`01:30 EST` gap day) FAIL (expected `2026-03-09T06:30:00Z`). London gap => `2026-03-29T01:30:00Z` FAIL (expected `2026-03-30T00:30:00Z`). Lord Howe => `2026-10-03T15:45:00Z` FAIL (expected `2026-10-04T15:15:00Z`). NY fold => first `2026-11-01T05:30:00Z` PASS, second `2026-11-02T06:30:00Z` PASS. London fold => first `2026-10-25T01:30:00Z` FAIL (selects second UTC occurrence; expected `2026-10-25T00:30:00Z`). Hourly downtime naive iteration queues 4 runs — same backlog flood.                                                                                                                                                                                                                                              | **REJECTED unguarded.** Passes NY fold but violates §17 gap rule and London first-occurrence rule, and has no `coalesce-one`/`skip-overlap` policy. |
| **Deadbolt Strict §17 Schedule Engine** | `internal/scheduling` (this repo: `cron.go` parser + `evaluator.go` with `isValidWallTime` / `isRepeatedUTCOccurrence` / bounded `CoalesceMissed` / `EvaluateOverlap`) | All gap fixtures PASS (NY `2026-03-09T06:30:00Z`, London `2026-03-30T00:30:00Z`, Lord Howe `2026-10-04T15:15:00Z`). All fold fixtures PASS (NY first `05:30Z` then `2026-11-02T06:30:00Z`; London first `00:30Z` then `2026-10-26T01:30:00Z`). Downtime `10:00Z→14:30Z` => `MissedCount=4`, latest `14:00Z`, `SkippedCount=3`, next `15:00Z` with `MissedOccurrences=nil` (O(1) memory). Overlap and revision fixtures PASS.                                                                                                                                                                                                                                                                                                                                                  | **SELECTED.** Only candidate satisfying both DST gap and fold invariants plus `coalesce-one` / `skip-overlap` with bounded memory.                  |

Neither pinned library satisfies §17 alone: `robfig/cron/v3 v3.0.1` passes gaps but fails folds; `gorhill/cronexpr v0.0.0-20180427100037-88b0669f7d75` passes NY fold but fails gaps and London first-occurrence. The Deadbolt evaluator is therefore selected, with third-party cron use (if ever) requiring explicit §17 policy guards.

---

## 3. Toolchain and Environment Setup

- **Host Architecture:** darwin/arm64 (macOS 15 / Darwin 25.1.0 locally); CI: ubuntu-24.04 (amd64/arm64)
- **Go Toolchain:** `go version go1.27.1 darwin/arm64` (CI: `1.27.1` via `actions/setup-go@v6`)
- **Node / Runtime:** Node `v24.21.0`, pnpm `10.24.0`
- **Zoneinfo Database:** System IANA tzdata at `/usr/share/zoneinfo` → `/var/db/timezone/zoneinfo`, active `+VERSION 2026c` (default snapshot `2025b`); fixtures cover `America/New_York`, `Europe/London`, `Australia/Lord_Howe`, `UTC`. CI uses the ubuntu-24.04 system tzdata via `time.LoadLocation`.
- **Pinned cron modules:** `github.com/robfig/cron/v3 v3.0.1` (2020-01-04), `github.com/gorhill/cronexpr v0.0.0-20180427100037-88b0669f7d75` (2018-04-27) per `go.mod` / `go.sum` (`go list -m` verified)
- **Database Engine:** PostgreSQL 14.19 locally (Homebrew); CI `postgres:16-alpine` service with Goose migration runner (`LatestSchemaVersion = 25`, migration `00025_recurring_schedules_contract.sql`)

---

## 4. Controlled-Clock Golden Fixtures (`contracts/fixtures/schedules.json`)

The shared fixture corpus defines 14 deterministic scenarios across multiple timezones:

1. `cron_parsing_standard_5_field`: Verifies minute, hour, DOM, month, DOW boundary matching.
2. `cron_step_expression`: Verifies step interval evaluation (`*/15`).
3. `cron_range_and_list`: Verifies combinations of lists (`9,12,18`) and ranges (`1-5`).
4. `dst_spring_forward_missing_wall_time_skipped_ny`: `America/New_York` springs forward 02:00 $\rightarrow$ 03:00 on 2026-03-08. Schedule `30 2 * * *` skips the missing wall time completely and advances to 2026-03-09 02:30 EDT (06:30 UTC).
5. `dst_spring_forward_missing_wall_time_skipped_london`: `Europe/London` springs forward 01:00 $\rightarrow$ 02:00 on 2026-03-29. Schedule `30 1 * * *` skips 01:30 and advances to 2026-03-30 01:30 BST (00:30 UTC).
6. `dst_fall_back_repeated_wall_time_first_utc_only_ny`: `America/New_York` falls back 02:00 $\rightarrow$ 01:00 on 2026-11-01. Schedule `30 1 * * *` fires at 05:30 UTC (EDT). The second occurrence at 06:30 UTC (EST) is skipped; next execution advances to 2026-11-02 01:30 EST (06:30 UTC).
7. `dst_fall_back_repeated_wall_time_first_utc_only_london`: `Europe/London` falls back 02:00 $\rightarrow$ 01:00 on 2026-10-25. Schedule `30 1 * * *` fires only at 00:30 UTC (BST) and skips 01:30 UTC (GMT).
8. `dst_half_hour_transition_lord_howe`: `Australia/Lord_Howe` shifts by 30 minutes (02:00 $\rightarrow$ 02:30) on 2026-10-04. Schedule `15 2 * * *` skips non-existent wall time 02:15 and advances to 2026-10-05 02:15 +11:00 (2026-10-04T15:15:00Z).
9. `misfire_coalesce_one_multiple_missed`: 4-hour downtime over hourly schedule yields latest missed run (14:00 UTC) with `skipped_count = 3` and next future at 15:00 UTC.
10. `misfire_coalesce_one_single_missed`: 1 missed run yields that occurrence with `skipped_count = 0`.
11. `misfire_coalesce_zero_missed_on_schedule`: Evaluation before due time yields 0 missed occurrences.
12. `overlap_policy_skip_when_previous_nonterminal`: Active `RUNNING` previous run skips next occurrence with `SKIPPED_OVERLAP`.
13. `overlap_policy_start_when_previous_terminal`: `SUCCEEDED` previous run starts next occurrence normally.
14. `schedule_revision_changes_future_only`: Schedule update increments revision from 1 to 2; pre-edit occurrence derives from `InitialCron` (`0 10 * * *` => `2026-05-01T10:00:00Z` under revision 1) and post-boundary occurrence derives from `UpdatedCron` (`0 15 * * *` after `2026-05-01T12:00:00Z` => `2026-05-01T15:00:00Z` under revision 2); past key remains pinned.

---

## 5. Executable Validation Commands and Measured Results

### 5.1 Unit and Fixture Verification

```sh
go test -v ./internal/scheduling
```

**Result (Go 1.27.1, tzdata 2026c):**

- 14/14 fixture contract test cases passed (`TestScheduleFixturesContract`, including strengthened revision transition that evaluates `InitialCron` pre-edit, `UpdatedCron` post-boundary, stale-cron negative, and pinned-key stability).
- 10 invalid cron/timezone error cases passed (`TestCronParseErrors`).
- `TestCoalesceMissed_BoundedMemory_HighFrequencyLongDowntime` passed: per-minute schedule over 30-day downtime yields `MissedCount=43200`, `SkippedCount=43199`, latest `2026-01-31T00:00:00Z`, next `2026-01-31T00:01:00Z` with `MissedOccurrences=nil`; bounded helper rejects the backlog with `max=1000`.

### 5.2 Spike Candidate Comparison (`INV-10` unit helper only)

```sh
go test -v ./tests/spikes/sp05
```

**Result (pinned modules executed, tzdata 2026c):**

- `TestPinnedCandidateVersions_Record`: logs `github.com/robfig/cron/v3 v3.0.1` and `github.com/gorhill/cronexpr v0.0.0-20180427100037-88b0669f7d75`.
- `TestCandidateComparison_DSTGap_NY`: Deadbolt PASS (`2026-03-09T06:30:00Z`); robfig PASS (`2026-03-09T06:30:00Z`); cronexpr FAIL (`2026-03-08T06:30:00Z`, fires gap-day `01:30 EST`).
- `TestCandidateComparison_DSTGap_London`: Deadbolt PASS (`2026-03-30T00:30:00Z`); robfig PASS; cronexpr FAIL (`2026-03-29T01:30:00Z`).
- `TestCandidateComparison_DSTFold_NY`: Deadbolt PASS (first `05:30Z`, subsequent `2026-11-02T06:30:00Z`); robfig FAIL (second `2026-11-01T06:30:00Z` duplicate); cronexpr PASS.
- `TestCandidateComparison_DSTFold_London`: Deadbolt PASS (first `00:30Z`, subsequent `2026-10-26T01:30:00Z`); robfig FAIL (second `2026-10-25T01:30:00Z` duplicate); cronexpr FAIL (first `2026-10-25T01:30:00Z`, wrong UTC occurrence).
- `TestCandidateComparison_LordHowe`: Deadbolt PASS (`2026-10-04T15:15:00Z`); robfig PASS; cronexpr FAIL (`2026-10-03T15:45:00Z`).
- `TestCandidateComparison_MisfireDowntime`: robfig naive queues 4 runs, cronexpr naive queues 4 runs (backlog flood); Deadbolt `CoalesceMissed` yields `MissedCount=4`, latest `14:00Z`, `SkippedCount=3`, next `15:00Z` with `MissedOccurrences=nil`.
- `TestOccurrenceKeyCanonicalIdentity_FormatHelper`: deterministic formatting verified; notes authoritative concurrency proof lives in §5.3 (not a mutex/map claim).

### 5.3 PostgreSQL Schema, Concurrency, and RLS Integration

```sh
go test -v ./tests/integration -run 'TestSchedule'
```

**Result (PostgreSQL 14.19 local; CI 16-alpine; `LatestSchemaVersion = 25`):**

- Migration `00025_recurring_schedules_contract.sql` applied: `uq_deployments_org_env_id`, `fk_schedules_pinned_deployment`, and `uq_schedule_occurrences_canonical_identity ON schedule_occurrences (schedule_id, revision, due_at)` present.
- `TestScheduleSchemaAndPolicies`, `TestScheduleOverlapAndMisfireAccounting`, `TestScheduleTenantIsolation` passed (policies, `SKIPPED_OVERLAP`/`SKIPPED_MISFIRE` with `skipped_count`, RLS 0-row isolation).
- `TestScheduleCanonicalIdentity_UniqueConstraint` passed: same `(schedule_id, revision, due_at)` with distinct `occurrence_key` strings rejected with `duplicate key value violates unique constraint "uq_schedule_occurrences_canonical_identity"`.
- `TestScheduleConcurrentDuplicateEvaluatorsINV10_Postgres` passed: 10 duplicate evaluators raced on separate transactions/connections with distinct `occurrence_key` values for the same `(schedule_id, 1, 2026-05-01T12:00:00Z)`; exactly 1 committed and 9 rejected with `duplicate key value`; final `count(*)=1`.
- `TestScheduleDeploymentIntegrity_Negative` passed: valid same-org/same-env pin accepted; nonexistent UUID, cross-tenant deployment, and cross-environment deployment each rejected with `fk_schedules_pinned_deployment` foreign-key violation.

---

## 6. Delivery Boundary Status

- **M4 Blocker Decision:** **UNBLOCKED for selection only.** Recurring schedule engine candidate selected and verified against §17 fixtures. No M4 recurring-schedule runtime execution is started in this spike.
- **Implemented:** 5-field cron parser, timezone validator, DST gap/fold handlers, bounded `coalesce-one` misfire evaluator (`MissedCount`/`SkippedCount`/`NextFuture`, `MissedOccurrences=nil`) with `EnumerateMissedBounded` test helper, `skip-overlap` evaluator, occurrence key formatting, migration `00025` (canonical unique index + pinned-deployment FK + composite uniqueness), and integration tests (PG race, canonical uniqueness, deployment negatives).
- **Automated Tests:** All unit, spike (real pinned candidates), and integration (real PostgreSQL concurrency) tests passed locally; exact-head CI validation required (see PR checks).
- **Hosted CI:** Pending PR submission (exact-head run).
- **Deployed:** No (documentation and spike contract verification).
