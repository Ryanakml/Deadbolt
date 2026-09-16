# Outbox Contract, Broker Configuration, and Diagnostic Runbook

## 1. Overview & Source of Truth

This document defines the outbox contract, NATS JetStream broker configuration, and operations runbook for Deadbolt's control plane (Blueprint §11, §19, §25, `REQ-EVENT-01`, `INV-06`, `INV-11`, `INV-12`, `F-02`).

The primary purpose of the transactional outbox and NATS JetStream is to deliver low-latency **wake-up hints** to the control plane and workers without granting execution rights or compromising state durability:

1. **Transactional Atomicity (`INV-06`):** Every meaningful state transition commits state mutation, `run_events` history, and `outbox_events` wake-up intents in the exact same database transaction.
2. **Notification-Only Broker Transport (`ADR-05`, `INV-11`):** NATS JetStream messages carry IDs and routing hints only. A queue message does NOT grant task execution ownership. Only a PostgreSQL `Claim()` transaction under row locks grants ownership.
3. **Harmless Redelivery (`F-02`):** Stable event IDs and database-enforced singular leases guarantee that broker redelivery or crash after publish/before mark cannot duplicate task ownership or execute a step twice.

---

## 2. Outbox Schema & Lifecycle Contract

### 2.1 Table Schema (`outbox_events`)

```sql
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    event_id UUID NOT NULL DEFAULT gen_random_uuid(),
    subject TEXT NOT NULL,
    payload JSONB NOT NULL,
    payload_version INT NOT NULL DEFAULT 1,
    attempts INT NOT NULL DEFAULT 0,
    next_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    published_at TIMESTAMPTZ,
    last_error TEXT,
    dead_lettered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (event_id)
);

CREATE INDEX idx_outbox_events_pending ON outbox_events (next_at)
    WHERE published_at IS NULL AND dead_lettered_at IS NULL;
```

### 2.2 Security & Multi-Tenancy

- Row-Level Security (RLS) is enabled and forced on `outbox_events`.
- Tenant transactions access their own outbox rows via `tenant_isolation_outbox_events`.
- The background outbox dispatcher uses restricted `SECURITY DEFINER` functions in the `app` schema:
  - `app.claim_outbox_batch(p_batch_size INT)`
  - `app.mark_outbox_published(p_id UUID)`
  - `app.retry_outbox_event(p_id UUID, p_retry_delay INTERVAL)`
  - `app.get_outbox_metrics()`
- Permissions are strictly granted to `deadbolt_runtime` and `deadbolt_system`, revoked from `PUBLIC`.

### 2.3 Dispatch Lifecycle & State Invariants

1. **Write (Atomic with State):** Application logic commits `outbox_events` row with `published_at = NULL`, `next_at = clock_timestamp()`.
2. **Claim (`FOR UPDATE SKIP LOCKED`):** Dispatcher claims batches of pending events (`published_at IS NULL AND next_at <= clock_timestamp()`).
3. **Sanitize Payload:** Payload is stripped of all customer secrets, environment variables, task inputs, and task outputs.
4. **Publish to JetStream:** Message is published with `Nats-Msg-Id: <event_id>` header for broker-side deduplication.
5. **Wait for Broker ACK:** Dispatcher synchronously waits for JetStream publish ACK.
6. **Mark Published:** Upon ACK, `published_at` is set to `clock_timestamp()`.
7. **Classify failures:** malformed/unsupported payloads are permanent and are recorded in `last_error` before being marked with `dead_lettered_at`. NATS/network failures are transient: they remain eligible for retry with bounded exponential backoff (`500ms * 2^attempts`, capped at 60s) and are never dead-lettered solely because an outage lasted longer than the retry count.

---

## 3. NATS JetStream Broker Configuration

### 3.1 Stream Topology

| Parameter             | Value                        | Rationale                                                        |
| :-------------------- | :--------------------------- | :--------------------------------------------------------------- |
| **Stream Name**       | `RUNTIME_WAKEUP`             | Internal control plane stream.                                   |
| **Subjects**          | `runtime.v1.wakeup.>`        | Versioned subject hierarchy (Blueprint §19.1).                   |
| **Default Subject**   | `runtime.v1.wakeup.default`  | Default shard subject.                                           |
| **Storage Type**      | File storage (`FileStorage`) | Durable on-disk spooling across broker restarts.                 |
| **Retention Policy**  | `WorkQueue`                  | Messages are removed from the stream once consumed.              |
| **Duplicates Window** | 2 minutes (`120s`)           | Deduplicates retried publishes with the same `Nats-Msg-Id`.      |
| **Discard Policy**    | `DiscardOld`                 | Drops oldest messages if bounded stream limits are reached.      |
| **Max Message Age**   | 24 hours                     | Stale hints expire; PostgreSQL reconciler remains authoritative. |

### 3.2 Consumer Configuration

| Parameter         | Value                         | Rationale                                                                    |
| :---------------- | :---------------------------- | :--------------------------------------------------------------------------- |
| **Consumer Name** | `runtime-controlplane-wakeup` | Durable push/queue consumer on `runtime.v1.wakeup.>`.                        |
| **Ack Policy**    | `AckExplicit`                 | Consumer explicitly calls `msg.Ack()` only after DB scan finishes.           |
| **Ack Wait**      | 10 seconds                    | Unacknowledged messages are redelivered after 10s.                           |
| **Max Deliver**   | 5                             | Prevents poisonous retry loops; dead work is re-discovered by DB reconciler. |

### 3.3 Message Payload Contract (`WakeupHintDTO`)

```json
{
  "eventId": "22222222-3333-4444-5555-666677778888",
  "organizationId": "11111111-2222-3333-4444-555566667777",
  "runId": "98765432-1111-2222-3333-444455556666",
  "subject": "execution.state_changed",
  "eventType": "STEP_READY",
  "sequence": 4,
  "timestamp": "2026-09-15T12:00:00.123456789Z"
}
```

> [!IMPORTANT]
> Wake-up hints MUST NOT contain step inputs, step outputs, runner tokens, or customer secrets. Handlers use `runId` and `organizationId` to trigger database candidate queries.

---

## 4. Observability & Metrics

Prometheus metrics are exposed on the control-plane `/metrics` endpoint:

- `deadbolt_outbox_published_total` (counter): Total count of successfully published outbox events.
- `deadbolt_outbox_publish_failures_total` (counter): Total count of failed JetStream publish attempts.
- `deadbolt_outbox_age_seconds` (gauge): Current age in seconds of the oldest unpublished outbox event.
- `deadbolt_outbox_pending_count` (gauge): Number of outbox events currently awaiting dispatch.
- `deadbolt_outbox_dispatcher_loop_lag_seconds` (gauge): Time in seconds since the last outbox dispatcher sweep.
- `deadbolt_scheduler_loop_lag_seconds` (gauge): Time in seconds since the last authoritative scheduler/reconciler sweep.

### Alerting Thresholds

- **Warning:** `deadbolt_outbox_age_seconds > 30` (Outbox backlog accumulating).
- **Critical:** `deadbolt_outbox_age_seconds > 60` (Notification pipeline stalled).
- **Graceful Degradation:** When NATS is down, `/readyz` continues to return `200 OK` with `nats=degraded`. PostgreSQL reconciler polls and drives work without broker availability (Blueprint §25.2).

---

## 5. Diagnostic Runbook

### 5.1 Symptom: High Outbox Age (`outbox_age_seconds > 60s`)

**Causes:**

1. NATS JetStream broker is unreachable or restarted.
2. Control-plane dispatcher loop crashed or hung.
3. Database lock contention on `outbox_events`.

**Verification:**

1. Check `/readyz` endpoint:
   ```bash
   curl -s http://127.0.0.1:8080/readyz | jq .
   ```
   If `nats: "degraded"`, NATS is unreachable.
2. Inspect pending outbox events directly in PostgreSQL:
   ```sql
   SELECT count(*), min(created_at), max(attempts)
   FROM outbox_events
   WHERE published_at IS NULL AND dead_lettered_at IS NULL;
   ```
3. Inspect top failing outbox events:
   ```sql
   SELECT id, event_id, subject, attempts, next_at, last_error, dead_lettered_at, created_at
   FROM outbox_events
   WHERE published_at IS NULL OR dead_lettered_at IS NOT NULL
   ORDER BY attempts DESC
   LIMIT 10;
   ```

**Remediation:**

1. If NATS is degraded, check broker logs and restart NATS service:
   ```bash
   docker compose logs nats
   # or check systemd/local nats process
   ```
2. Once NATS is restored, the dispatcher will automatically resume claiming batches.
3. If backoff delays (`next_at`) are high for stalled events, reset `next_at` to trigger immediate dispatch:

   ```sql
   UPDATE outbox_events
   SET next_at = clock_timestamp()
   WHERE published_at IS NULL AND dead_lettered_at IS NULL;
   ```

4. If an invalid payload is dead-lettered, fix the producer/serializer first. Replaying it without correction will dead-letter it again; use the recorded `last_error` as the diagnosis.

### 5.2 Symptom: JetStream Broker Redelivery Spikes

**Causes:**

1. Consumer DB scan takes longer than `AckWait` (10s), causing JetStream to redeliver the message.
2. Network timeout between consumer and broker.

**Verification:**

1. Check slow queries on `run_steps` and `runs`.
2. Verify database connection pool saturation.

**Remediation:**

1. Check query plans for candidate scans (`idx_runs_env_status_created`, `idx_outbox_events_pending`).
2. Singular claim invariants guarantee that redeliveries do NOT cause duplicate worker execution (`INV-03`).

### 5.3 Dispatcher and Scheduler Lag

`deadbolt_outbox_dispatcher_loop_lag_seconds` diagnoses the outbox publisher loop. `deadbolt_scheduler_loop_lag_seconds` diagnoses the authoritative database reconciler; these are separate signals. A healthy scheduler can continue progressing work while NATS is degraded, and a healthy dispatcher does not prove the scheduler is running.

### 5.4 Broker Total Loss Recovery (`INV-11`)

If NATS JetStream storage volume is completely corrupted or destroyed:

1. Re-create the stream:
   The control plane automatically recreates `RUNTIME_WAKEUP` on boot via `outbox.EnsureStream()`.
2. Outstanding un-published outbox entries in PostgreSQL are preserved and will be published.
   Transiently failed events resume automatically after the NATS connection and JetStream stream are restored.
3. Ready steps in PostgreSQL will be discovered by the periodic scheduler sweep (every 5 seconds) and by worker long-polling regardless of broker message loss.
