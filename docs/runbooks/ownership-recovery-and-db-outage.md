# Ownership recovery and DB-outage diagnosis

This runbook covers the M2 ownership boundary: a worker lease is a temporary
right to report an attempt result, not proof that the worker process stopped.
PostgreSQL is authoritative for expiry and ownership.

## Expected recovery contract

- A claim has a five-second claim-to-start deadline.
- A started attempt has a 30-second lease, renewed every five seconds.
- The server compares `clock_timestamp()` with the stored lease and attempt/run
  deadlines while holding the attempt ownership lock. At `DB_now >= boundary`,
  the worker no longer has mutation rights.
- Lease expiry closes the attempt as `LOST`, deletes the current lease, records
  `TASK_LOST`, and applies the task recovery policy. A retry creates a new
  attempt and increments the step ownership epoch.
- The reconciler processes bounded batches. Re-running the sweep after commit
  is a no-op: it must not create another attempt, event, outbox intent, or
  ownership transition.
- `Start` and `Complete` from the old epoch return `409 STALE_OWNERSHIP`.
  Heartbeat returns a stop command (`LEASE_NOT_FOUND` or
  `STALE_OWNERSHIP`) and must not renew the old lease.

The old worker may still be executing customer code after the control plane
revokes its lease. Fencing protects committed Runtime Cloud state; it cannot
undo an external side effect already accepted by a provider. Tasks with an
unknown effect must use the `reconcile` recovery policy.

## DB outage diagnosis

During a PostgreSQL outage:

1. The API must stop admitting new runs.
2. The worker gateway must not grant or renew ownership.
3. Agents stop runners before their locally calculated conservative lease
   safety boundary; they must not continue from in-memory state.
4. After PostgreSQL returns, the reconciler reads committed state and processes
   expired ownership. It does not infer success from a missing response.

Check, in order:

- control-plane health and database connectivity;
- scheduler/reconciler heartbeat and expired-lease sweep errors;
- active leases and their `expires_at` against database time;
- attempt status, `epoch`, session ID, and `TASK_LOST` history;
- recovery policy (`safe`, `idempotent`, or `reconcile`) before releasing a
  held attempt.

Do not manually update attempt status or delete leases. All corrective changes
must use the transition engine so state, history, and outbox intents remain
atomic. If a result is ambiguous, leave the attempt held for reconciliation
instead of retrying an external side effect automatically.

## Evidence for this contract

The integration coverage in
`tests/integration/worker_execution_adversarial_test.go` exercises running-lease
expiry, replacement ownership, stale worker mutations, and a repeated sweep.
The test requires the real PostgreSQL-backed integration fixture; a unit test
or an unavailable database is not evidence of this behavior.
