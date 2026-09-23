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

## 2. Candidates and Evaluation Decision

| Candidate                                             | Strategy & Evaluation                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | Decision                                                                                                                                                    |
| :---------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Candidate A: Naive Cron / Go `time.Date` Defaults** | Iterates calendar dates and constructs `time.Date(year, month, day, hour, min, ..., loc)`. Standard libraries and default cron runners (e.g. uninspected `robfig/cron/v3` or `gorhill/cronexpr`) rely directly on runtime date normalization. In `tests/spikes/sp05/candidate_test.go`: on 2026-03-08 in `America/New_York`, missing wall time 02:30 is normalized to 01:30 (or shifted to 03:30) rather than skipped, firing an invalid execution. On fall-back, uninspected iterators fire both UTC occurrences or violate monotonicity. On downtime, standard runners queue every missed execution, causing thundering herd backlogs. | **REJECTED.** Violates Blueprint §17 DST rules, misfire policy, and overlap limits. No uninspected library defaults may be inherited without policy guards. |
| **Candidate B: Deadbolt Strict §17 Schedule Engine**  | Strict 5-field parser (`internal/scheduling/cron.go`) + DST-safe monotonic evaluator (`internal/scheduling/evaluator.go`). Specifically validates wall time existence (`isValidWallTime`) to skip spring-forward gaps, inspects historical transition windows (`isRepeatedUTCOccurrence`) to strictly select the first UTC occurrence during fall back, enforces `coalesce-one` downtime calculation with exact `skipped_count`, and checks nonterminal run states for `skip-overlap`.                                                                                                                                                   | **SELECTED.** Fully satisfies Blueprint §17 invariants and passes all controlled-clock golden fixtures.                                                     |

---

## 3. Toolchain and Environment Setup

- **Host Architecture:** darwin/arm64 (macOS 15 / Darwin 25.1.0)
- **Go Toolchain:** `go version go1.27.1 darwin/arm64`
- **Node / Runtime:** Node `v24.21.0`, pnpm `10.24.0`
- **Zoneinfo Database:** System IANA tzdata at `/usr/share/zoneinfo` (`America/New_York`, `Europe/London`, `Australia/Lord_Howe`, `UTC`)
- **Database Engine:** PostgreSQL 14/16 with Goose migration runner (`LatestSchemaVersion = 25`)

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
14. `schedule_revision_changes_future_only`: Schedule update increments revision from 1 to 2; past occurrences retain revision 1 key while future occurrences evaluate under revision 2.

---

## 5. Executable Validation Commands and Measured Results

### 5.1 Unit and Fixture Verification

```sh
go test -v ./internal/scheduling
```

**Result:**

- 14/14 fixture contract test cases passed.
- 10 invalid cron/timezone error cases passed.

### 5.2 Spike Candidate Comparison and Concurrency (`INV-10`)

```sh
go test -v ./tests/spikes/sp05
```

**Result:**

- `TestCandidateComparison_DSTGap`: Candidate A (naive library default) failed §17 by firing on gap day with shifted hour (01:30). Candidate B (Deadbolt §17 evaluator) passed by skipping 2026-03-08 and scheduling 2026-03-09 02:30 EDT.
- `TestCandidateComparison_DSTFold`: Candidate B passed by selecting first UTC occurrence (05:30 UTC) and skipping duplicate UTC occurrence (06:30 UTC).
- `TestCandidateComparison_MisfireDowntime`: Candidate B passed by coalescing 4 missed runs into latest occurrence with `skipped_count = 3`.
- `TestConcurrentDuplicateEvaluators_INV10`: 10 concurrent racing evaluators resulted in exactly 1 committed logical action on unique occurrence key `(schedule_id, revision, scheduled_at_utc)`.

### 5.3 PostgreSQL Schema and RLS Integration

```sh
go test -v ./tests/integration -run 'TestSchedule'
```

**Result:**

- Migration `00025_recurring_schedules_contract.sql` successfully applied (`LatestSchemaVersion = 25`).
- Unique constraint `(schedule_id, occurrence_key)` prevented duplicate insertions.
- `SKIPPED_OVERLAP` and `SKIPPED_MISFIRE` with `skipped_count` persisted and queryable.
- RLS verified: foreign tenant queries return 0 rows.

---

## 6. Delivery Boundary Status

- **M4 Blocker Decision:** **UNBLOCKED.** Recurring schedule engine candidate selected and verified against §17 fixtures.
- **Implemented:** 5-field cron parser, timezone validator, DST gap/fold handlers, coalesce-one misfire evaluator, skip-overlap evaluator, occurrence key formatting, migration `00025`, and integration tests.
- **Automated Tests:** All unit, spike, and integration tests passed locally.
- **Hosted CI:** Pending PR submission.
- **Deployed:** No (documentation and spike contract verification).
