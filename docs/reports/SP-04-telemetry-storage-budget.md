# SP-04 — Telemetry and storage budget (Issue #23, M2)

**Scope:** Issue #23. Blueprint §33: cost/log volume/query latency at
retention limits. Decision rule: tune/batch first; replace backend only if
the measured budget fails without reducing correctness.

**What this report proves:** the bounded diagnostic path
(1 MiB/attempt, 16 KiB/line, drop accounting, 7-day retention, Inspector
pagination, prune batching) holds at measured scale, and diagnostic pressure
never touches correctness events. Monetary cloud cost is **not measured**
(no priced environment exists); this report gives measured storage and query
volume only.

## 1. Setup

- Code: PR #73 (migration `00023`, schema version 23; bounded logging in
  `internal/worker/service.go`, retention prune in
  `internal/execution/service.go:PruneExpiredTaskLogs`, Inspector pagination
  in `internal/execution/service.go:GetRunLogs`).
- Fixtures: `TestRunInspectorLogBoundsAndDroppedMetric`,
  `TestRunInspectorBoundedTaskLogs`,
  `TestLogPressurePreservesCorrectnessEvents`
  (`tests/integration/`), plus direct `psql` inspection of the integration
  database after those runs.
- Machine (local measurement): darwin/arm64, go1.27.1, PostgreSQL 14.19
  (Homebrew). Hosted CI uses PostgreSQL 16 on ubuntu-24.04.

## 2. Commands

```sh
go test ./tests/integration/ -run 'TestRunInspectorLogBoundsAndDroppedMetric|TestLogPressurePreservesCorrectnessEvents|TestRunInspectorBoundedTaskLogs' -count=1
psql "$TEST_DATABASE_URL-or-local-deadbolt_integration_test" \
  -c "SELECT count(*), pg_size_pretty(pg_total_relation_size('task_logs')) FROM task_logs;" \
  -c "SELECT indexrelname, pg_size_pretty(pg_relation_size(indexrelid)) FROM pg_stat_user_indexes WHERE relname='task_logs';" \
  -c "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) SELECT * FROM task_logs ORDER BY created_at, id LIMIT 51;"
```

## 3. Measured results (local, 2026-09-23)

### 3.1 Caps and drop accounting (test assertions, all passing)

- 16 KiB message accepted with `Dropped=0`; 16 KiB+1 byte message dropped
  with `Dropped=1` (`TestRunInspectorLogBoundsAndDroppedMetric`).
- 64 x 16 KiB fills the 1 MiB attempt budget; the next line returns
  `BudgetExhausted` with drops; persisted `DroppedLogsCount` and
  `deadbolt_task_logs_dropped_total` counter verified; retry-safe
  (duplicate ingest does not double-count).
- Flood coupling (`TestLogPressurePreservesCorrectnessEvents`): after
  budget exhaustion the same attempt still starts, completes, and the run
  reaches SUCCEEDED with `TASK_STARTED`/`TASK_COMPLETED` events present and
  `TASK_LOST = 0`. Diagnostic drops never become correctness drops.

### 3.2 Storage (measured in `deadbolt_integration_test` after the runs above)

- `task_logs`: 259 rows, 368 kB total relation (96 kB heap; remainder TOAST
  + indexes). Raw message bytes 50 kB; average stored row 198 bytes; max
  message length 16,384 (cap enforced at the application layer).
- Honesty note: test filler (`'F' x 16 KiB`) is highly compressible, so
  TOAST shrinks it far below 16 KiB on disk. Incompressible (e.g. encrypted
  or high-entropy) payloads would store near full size; see projections.
- Indexes (6): primary + `attempt_id_sequence` + `lookup/step/attempt/
  retention`, 16–56 kB each at this volume.
- `task_log_drop_receipts`: 6 rows, 32 kB.

### 3.3 Query latency (measured, same database)

- Inspector keyset pagination (`ORDER BY created_at, id LIMIT 51`):
  Index Scan on `idx_task_logs_retention`, 13 shared buffers hit,
  execution 0.130 ms, planning 0.754 ms.
- Retention prune path (`PruneExpiredTaskLogs`, 7-day TTL, batch limit
  1000, wired into the scheduler slow sweep) is covered by the 8-day-age
  prune test (expired flag + message) and runs bounded per sweep.

### 3.4 Calculated projections (clearly not measurements)

- Worst-case per attempt (incompressible): ~1 MiB log heap + TOAST + index
  entries (~6 indexes), i.e. low single-digit MiB per saturated attempt.
- Per-environment storage is therefore bounded by
  (attempt volume within 7 days) x (per-attempt worst case); the 7-day
  prune bounds retention regardless of volume.
- `run_events` (correctness) are permanent and unaffected by log pruning;
  their 10,000/run cap with terminal reserve is proven by
  `TestHistoryLimitBoundaryAndAtomicTerminalization`.

## 4. Decision

Tune/batch first holds: pagination is index-backed at sub-millisecond
latency, prune is batch-bounded, drops are accounted, and no correctness
event was lost under flood. No backend replacement. Re-measure at higher
log volumes on CI hardware if flood rates grow; monetary cost to be added
only from a real priced environment.
