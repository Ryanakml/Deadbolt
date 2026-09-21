# Broker outage and stuck-work runbook

PostgreSQL is the execution authority. NATS carries wake-up hints only, so a
broker outage must not be treated as data loss and must not be repaired by
replaying the entire published outbox history.

## Broker outage or stream loss

1. Check `/readyz` and `/metrics`. `deadbolt_scheduler_loop_lag_seconds` and
   `deadbolt_outbox_age_seconds` identify whether the database fallback is
   healthy and whether notifications are delayed.
2. Restore the NATS stream `RUNTIME_WAKEUP` and the versioned consumer/subject
   configuration (`runtime.v1.wakeup.default`). Do not edit run or step state.
3. Confirm the outbox dispatcher can connect. Pending, unpublished rows are
   retried with bounded backoff; a publish ACK is required before
   `published_at` is written.
4. Confirm workers continue polling. PostgreSQL scans discover eligible work
   even when no hint was delivered. Previously published outbox rows do not
   need to be replayed.

If the database is unavailable, admission and lease renewal must fail closed.
Follow the database outage runbook and let workers stop at their local lease
budget; never mark work successful from memory.

## Stuck READY/BLOCKED work

1. Inspect the run, step, attempt, lease, and event history. Distinguish
   `READY` waiting for a compatible worker from a stale dependency.
2. Check scheduler lag and compatible worker/deployment availability.
3. The fast reconciliation pass (1 second) handles due deadlines and expired
   leases. The slow pass (5 seconds) reevaluates persisted dependencies and
   repairs a missed wake-up through the execution engine, writing the matching
   event and outbox intent atomically.
4. Recheck the run after one slow pass. Reconciliation is idempotent; do not
   manually insert events, claims, leases, or outbox rows.

If a task has an ambiguous external side effect, do not force a retry. Use the
task's declared recovery policy and the reconciliation/hold workflow.
