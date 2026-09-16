# Run Inspector and Diagnostics Architecture

## 1. Overview & Core Invariants

The Deadbolt Run Inspector provides operators and SDK clients with an authoritative, real-time diagnostic window into workflow execution runs, step attempts, live execution events, and worker diagnostics.

Deadbolt adheres to strict foundational invariants across all inspection subsystems:

- **PostgreSQL as Sole Authority:** In-memory hubs and buffers are ephemeral delivery accelerators. All run states, step progression, attempt histories, and event logs are committed to and read from PostgreSQL.
- **Tenant Isolation via Row-Level Security (RLS):** Every database query and transaction sets `app.current_organization_id` to enforce strict multi-tenant data isolation. Cross-tenant access is rejected with `404 Not Found`.
- **Authorization & Redaction Boundaries:** Payloads, step outputs, and internal error stack traces are strictly protected by the `payload:read` permission. Callers without `payload:read` receive redacted payloads (`[REDACTED]`) and sanitized error messages.
- **Honest System Diagnostics:** If a run cannot proceed due to worker topology, Deadbolt surfaces concrete waiting reasons (e.g., `NO_COMPATIBLE_WORKERS`) rather than masking stall states.

---

## 2. Snapshot Consistency & Point-in-Time Reconstruction

### Read-Only Repeatable Read Transactions

The endpoint `GET /v1/runs/{id}` returns a unified snapshot of the run, including:

- Run metadata, status, reason code, revision, and optional execution deadline (`deadlineAt`).
- All steps and their associated attempts, ordered deterministically by attempt number.
- Authoritative worker count compatibility (`activeCompatibleWorkers`).
- Explicit diagnostic waiting reasons (`waitingReason`).

To eliminate race conditions between step executions and attempt insertions, the control plane executes snapshot assembly inside a PostgreSQL transaction configured as:

```sql
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
```

This guarantees that all steps, attempts, and metadata reflect a single, consistent snapshot in time without intermediate dirty reads or phantom steps.

### Honest Worker Compatibility & Waiting Reasons

When a run is queued or running, operators need to know if any worker can actually execute the workflow bundle:

1. `activeCompatibleWorkers`: The service queries active worker sessions (`w.status = 'ACTIVE'`, `ws.revoked_at IS NULL`, `ws.expires_at > clock_timestamp()`) advertising a matching `bundle_digest` for the run's deployment.
2. `waitingReason`:
   - If `activeCompatibleWorkers == 0`, the run status is non-terminal (`QUEUED` or `RUNNING`), and there are zero running attempts (`RUNNING`), the run snapshot reports `waitingReason: "NO_COMPATIBLE_WORKERS"`.
   - If compatible workers are active or attempts are currently running, `waitingReason` is `nil`.

### Redaction Boundaries

The inspection endpoints enforce granular permission filtering:

- With `payload:read`: Run inputs, step outputs, and attempt errors are returned in full.
- Without `payload:read`: Run input and step outputs are masked with `"[REDACTED]"`. Attempt errors (`attempts[].error`) are completely redacted (`null`), preventing leak of proprietary runtime details, environment variables, or sensitive exceptions.

---

## 3. Real-Time Event Streaming & Resilient Reconnection

### Streaming Endpoints & Ordering

- **REST Events:** `GET /v1/runs/{id}/events?cursor=N&limit=M`
- **Server-Sent Events (SSE):** `GET /v1/runs/{id}/stream`

Events are committed to the `run_events` table and assigned a monotonically increasing, gapless `sequence` (1, 2, 3, ...). The stream orders all events strictly by `sequence ASC`.

### EventHub & Authoritative DB Fallback

Live stream delivery is powered by an in-process `EventHub` coupled with PostgreSQL storage:

1. When a run event commits to PostgreSQL, `EventHub.Broadcast(event)` delivers the event to all active SSE subscriber channels.
2. If an SSE client connects or reconnects with `Last-Event-ID: N` (or `?cursor=N`):
   - The server queries `run_events` for all events where `sequence > N` ordered by `sequence ASC`.
   - Replayed historical events are transmitted first.
   - The stream smoothly transitions to live broadcast events without duplicates or dropped frames.

### Deterministic Snapshot-to-Subscribe Pattern

To avoid race conditions between fetching initial state and opening an SSE stream:

1. Client requests `GET /v1/runs/{id}` and notes `lastEventSequence` (the maximum committed event sequence for the run).
2. Client opens `GET /v1/runs/{id}/events/stream` passing `Last-Event-ID: <lastEventSequence>`.
3. Any events committed while the client was processing the snapshot are replayed immediately, guaranteeing zero missed state transitions.

### SSE Retention Gap Detection

While `run_events` are permanently retained for auditability, client reconnects may occasionally specify an invalid or pruned sequence. If the server determines that requested events are missing or discontinuous:

- The server emits an SSE event:
  ```
  event: error
  data: {"code":"RETENTION_GAP","message":"Event sequence gap detected; full snapshot required"}
  ```
- The server terminates the SSE connection with code 200/EOF.
- The client/dashboard detects `RETENTION_GAP` and automatically triggers a fresh snapshot fetch from `GET /v1/runs/{id}` before reconnecting.

---

## 4. Task Logs Architecture & Retention Policies

### Permanent Audit Events vs. 7-Day Ephemeral Logs

Deadbolt strictly distinguishes between workflow audit events and task log output:

- **`run_events`:** Permanent audit history. Records lifecycle state changes, step transitions, and execution outcomes. Never pruned.
- **`task_logs`:** High-volume execution standard out / standard error. Subject to a strict **7-day retention TTL**.

### Bounded Ingestion Limits

To protect control plane performance and prevent storage exhaustion:

1. **Per-Line Bound (`MaxLogLineSizeBytes`):** 16 KiB (16,384 bytes). Lines exceeding 16 KiB are dropped.
2. **Per-Attempt Cumulative Bound (`MaxAttemptLogSizeBytes`):** 1 MiB (1,048,576 bytes). Once an attempt exceeds 1 MiB of logs, subsequent lines are dropped.
3. **Worker Protocol Feedback:** The log ingestion endpoint `POST /worker/v1/logs` responds with:
   - `accepted: true`
   - `droppedCount: N` (count of lines dropped due to per-line or cumulative limit)
   - `budgetExhausted: true/false` (indicates whether the attempt budget was hit)
4. **Dropped Metric Counter:** Dropped logs increment an internal atomic counter (`DroppedLogsCount()`) exposed in control plane telemetry.
5. **Persistent incomplete-diagnostics signal:** `task_attempts` retains the dropped count and budget-exhausted flag, so a later `GET /v1/runs/{id}/logs` response remains honest after the worker ACK is gone. Rejected `(attempt_id, sequence)` records have durable receipts; an idempotent worker retry returns the original drop result but does not add another metric increment or another dropped record.

### Stable Keyset Pagination

Task logs are queried via `GET /v1/runs/{id}/logs?cursor=...&limit=...`.

Because multiple steps in a workflow can emit log lines concurrently with overlapping sequence numbers, Deadbolt uses a compound keyset cursor:

- **Keyset Columns:** `(created_at, id)`
- **SQL Predicate:** `WHERE ... AND (created_at, id) > ($cursor_time, $cursor_id) ORDER BY created_at ASC, id ASC LIMIT $limit`
- **Cursor Format:** `base64(RFC3339Nano_timestamp + ":" + uuid)`

This guarantees stable, reproducible forward pagination across page boundaries without duplicate or missing log entries.

### Authoritative 7-Day Expiration Distinction

When `GET /v1/runs/{id}/logs` returns 0 items on an initial query (cursor empty):

- The control plane checks `task_attempts.logs_recorded` for the run:
  - If `logs_recorded = TRUE`: Logs were previously ingested and subsequently removed by retention pruning. The response returns:
    ```json
    {
      "items": [],
      "expired": true,
      "message": "Logs have expired due to the 7-day retention policy"
    }
    ```
  - If `logs_recorded = FALSE`: No logs were ever emitted for this task attempt. The response returns:
    ```json
    {
      "items": [],
      "expired": false,
      "message": null
    }
    ```

### Tenant-Scoped Batch Retention Pruning

Pruning is performed by the control-plane's existing scheduler reconciliation sweep via `PruneExpiredTaskLogs(ctx, orgID, limit)`:

- Executes within the tenant's transaction context via `pool.WithTenantTx(ctx, orgID, ...)` to satisfy PostgreSQL Row-Level Security.
- Each tenant sweep deletes at most one bounded batch, then commits. The next scheduler pass continues safely if more rows remain:
  ```sql
  DELETE FROM task_logs
  WHERE organization_id = $1::uuid
    AND id IN (
      SELECT id FROM task_logs
      WHERE organization_id = $1::uuid
        AND created_at < clock_timestamp() - INTERVAL '7 days'
      ORDER BY created_at ASC
      LIMIT $2
    )
  ```

The sweep is safe to repeat: once a batch has deleted an expired row, subsequent passes simply find fewer rows. Retention deletion is irreversible; restore requires the normal PostgreSQL backup/restore procedure, not an application rollback. Migration `00016_task_log_drop_receipts.sql` adds only diagnostic accounting columns/table and can be rolled back only after the application version no longer reads them.

---

## 5. Dashboard UI & Accessibility (WCAG 2.2 AA)

The operator dashboard (`apps/dashboard/`) is a standalone single-page application served directly by the control plane at `/dashboard/`:

- **Accessibility & Contrast:** Conforms to WCAG 2.2 AA standards with high-contrast status colors, visible focus rings, and full screen-reader announcements via `aria-live="polite"`.
- **Reduced Motion:** Respects user motion preferences via `@media (prefers-reduced-motion: reduce)`, suppressing pulsing animations and smooth scroll effects.
- **Event Timeline Deduplication:** Deduplicates streamed events using event `sequence` and `id`, ensuring the timeline remains pristine during rapid reconnects.
- **Decoupled Freshness:** A dedicated SSE badge indicates stream transport health independently from the initial snapshot fetch timestamp.
- **Diagnostics Display:** Directly surfaces run timeouts (`deadlineAt`), waiting diagnostics (`Waiting Reason: NO_COMPATIBLE_WORKERS`), and log expiration notices.

---

## 6. Runtime CLI Commands

The `runtime` CLI (`cmd/runtime`) provides administrative and operational access to runs, workers, and logs:

```bash
# List runs in an environment with keyset pagination
runtime runs list --env staging --limit 20

# Inspect run snapshot details, steps, attempts, and waiting reasons
runtime runs get <run-id>

# Stream or paginate run execution logs
runtime runs logs <run-id> [--step-id <id>] [--attempt-id <id>] [--follow]

# List registered and active workers in an environment
runtime workers list --env staging
```
