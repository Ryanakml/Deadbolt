# Pause and Resume Safe Actions (M3 — Issue #28)

Source of truth: `docs/blueprint.md` §10.2, §11.2, §15.3, §20.1, §23.
This document is the canonical delivery artifact for Issue #28 / PR #79.

---

## 1. Overview & Core Invariants

The pause and resume control actions provide operator and automation controls to gracefully suspend and resume workflow run execution without risking lost updates, split-brain executions, or corrupting durable state invariants.

Control operations conform strictly to the state priority rules defined in Blueprint §10.2:

- **Terminal Priority:** `SUCCEEDED`, `FAILED`, and `CANCELLED` terminal states are immutable and outrank `PAUSING` and `PAUSED`.
- **Cancellation Authority:** A durable cancel commit always outranks pause; pause never blocks cancellation.
- **Deadline Dominance:** The overall run deadline never freezes while paused. If the deadline lapses, the run terminalizes to `FAILED / RUN_DEADLINE_EXCEEDED` regardless of pause state.

---

## 2. Distinction Between `PAUSING` and `PAUSED`

1. **`PAUSING` (Draining State):**
   - When a pause request is committed on a run with active in-flight task attempts (`CLAIMED` or `RUNNING`), the run enters the `PAUSING` state.
   - New task claims are atomically blocked.
   - The run remains in `PAUSING` while active attempts execute and drain.
   - If an active attempt encounters a lease timeout or worker failure while `PAUSING`, standard recovery mechanics apply (timers and holds record durably, but new worker claims do not launch).
   - Once all active attempts complete or settle, the run automatically drains to `PAUSED` (unless terminal completion or unrecoverable failure terminalizes the run first per §10.2 state priority).

2. **`PAUSED` (Resting State):**
   - The run enters `PAUSED` immediately if no active attempts are in flight at the moment of the pause commit, or automatically drains from `PAUSING` once all in-flight attempts finish.
   - Zero task attempts are in `CLAIMED` or `RUNNING` status.
   - The scheduler will not issue claims for ready tasks or dispatch due retry timers into runnable attempts.
   - The run remains parked in `PAUSED` until explicitly resumed, cancelled, or until the overall run deadline expires.

---

## 3. Claim Ordering & In-Flight Execution Semantics

- **Claims Committed Before Pause:**
  Any task attempt claimed prior to the commit of `pause_requested=true` remains authoritative. The worker holding the lease may execute `Start` and complete normally. Its results (success, failure, or retryable backoff) commit according to standard transactional protocol.
- **Claims Attempted After Pause:**
  Subsequent worker claims (`POST /worker/v1/poll`) are blocked in both candidate query and revalidation transactions (`FOR UPDATE` under PostgreSQL serialization). Workers receive 0 assignments for steps belonging to `PAUSING` or `PAUSED` runs.
- **In-Flight Completion:**
  When an in-flight attempt completes:
  - If the attempt succeeds and unlocks dependent DAG steps, those dependent steps advance durably to `READY` in the database, but will **not** be dispatched or claimed while paused.
  - If the attempt fails retryably, a retry backoff timer is scheduled (see Section 4).
  - If the attempt is the last in-flight attempt for the run, the completion transaction triggers the transition from `PAUSING` to `PAUSED`.

---

## 4. Timers and Reconciliation Holds While Paused

- **Retry Timers While Paused:**
  If an attempt fails with a retryable error while the run is `PAUSING` or `PAUSED`, the scheduler records a durable timer row in `timers` with `kind='RETRY_BACKOFF'` and status `PENDING`.
  - While paused, the retry timer **will not fire** and will not transition the step to `READY`.
  - The calculated `due_at` timestamp is **never reset or pushed forward** by pause or resume. When the run resumes, if the `due_at` timestamp is already in the past, the timer becomes immediately eligible to fire.
- **Reconciliation Holds While Paused:**
  Ambiguous attempt outcomes, unknown effect statuses, or step failures with hold policies create durable cases in `reconciliation_cases` with status `OPEN`.
  - The run records `WAITING` with reason `RECONCILIATION` when no live work remains.
  - Operators can submit human case resolutions (`RESOLVE` or `MOOT`) while paused; these update the case record durably without launching new attempts until the run is resumed.

---

## 5. Deadline Never Freezes

- Stored in `runs.deadline_at`, the run deadline is authoritative and absolute from the moment of run creation.
- Pausing a run **never freezes, extends, or resets** `deadline_at`.
- If `deadline_at` is reached while a run is `PAUSED` or `PAUSING`:
  - The bounded lease/deadline sweep (`reconcileExpiredLeasesTx` / `failOverdueRunsTx`) or any incoming control action inline settles the run to `FAILED` with reason `RUN_DEADLINE_EXCEEDED`.
  - Pending timers are cancelled (`timers.state = 'CANCELLED'`).
  - Open reconciliation cases are mooted.
  - Exactly one `RUN_FAILED` event is emitted.
  - Subsequent resume requests are rejected with `409 RUN_TERMINAL` (or `409 RUN_DEADLINE_EXCEEDED` if settling inline).

---

## 6. Resume State Recomputation

When `POST /v1/runs/{id}/resume` is committed:

1. `runs.pause_requested` is reset to `false`.
2. Step locks are acquired in canonical ID order under the run row lock (`ORDER BY id FOR UPDATE`), preventing deadlock with concurrent completions or lease recovery.
3. The engine evaluates the durable state of all workflow steps and determines the authoritative target status:
   - **Terminal (`SUCCEEDED` / `FAILED`):** If all workflow terminal nodes have finished, the run settles directly to terminal status.
   - **`RUNNING`:** If any active attempt is in flight (`CLAIMED` or `RUNNING`), or if ready work exists and the run previously started.
   - **`QUEUED`:** If ready work exists but the run never previously started an attempt.
   - **`WAITING`:** If no ready work or live attempts exist, but open reconciliation holds exist (reason `RECONCILIATION`), pending retry timers exist (reason `RETRY_BACKOFF`), or human approval is needed.
4. An authoritative `RUN_RESUMED` event is appended to the event timeline and outbox.

---

## 7. Revision Safety & Command Idempotency

- **Optimistic Concurrency Control (`expectedRevision`):**
  - Both `/pause` and `/resume` require `expectedRevision` in the request body.
  - If `runs.revision != req.expectedRevision`, the request is rejected with `409 Conflict` and error code `REVISION_CONFLICT`.
  - In the Inspector UI, 409 responses keep the control dialog open and prompt the user to refresh their view without losing context.
- **Durable Command Idempotency (`Idempotency-Key`):**
  - All control mutations are scoped through `tenant_commands` via `WithCommandTx`.
  - Once a command is `COMPLETED`, retrying with the identical `Idempotency-Key` and request body replays the exact recorded outcome and HTTP response code.
  - Replay occurs without re-evaluating the run's current state or deadline. Even if the deadline later passes and the run terminalizes, a retried completed idempotency key safely returns the recorded outcome.
  - Stale or conflicting payloads with the same idempotency key return `409 IDEMPOTENCY_CONFLICT`.

---

## 8. Cancellation Priority & Stop Operations

- A request to cancel (`POST /v1/runs/{id}/cancel`) outranks pause at all times.
- If a run is `PAUSED`, cancellation transitions it immediately to `CANCELLED` (or `CANCELLING` if any live attempts exist).
- If a run is `PAUSING`, cancellation revokes worker leases, issues stop commands with a 10-second grace deadline, and waits for worker acknowledgments.
- A pause request issued on a run in `CANCELLING` or terminal state is rejected with `409 RUN_CANCELLING` or `409 RUN_TERMINAL`.

---

## 9. Permissions & Authorization

Control plane endpoints enforce strict role-based access control (RBAC) and scoped API key capabilities:

- **`runs:control` Capability Required:**
  - `POST /v1/runs/{id}/pause` and `POST /v1/runs/{id}/resume` require the `runs:control` capability.
  - Organization roles `Owner`, `Admin`, and `Developer` possess `runs:control` and are admitted with `200 OK`.
  - The `Viewer` role lacks `runs:control` and is rejected with `403 Forbidden`.
  - Machine API keys must explicitly include the `runs:control` capability string in their token capabilities.
- **UI Gating:**
  - The Deadbolt Run Inspector evaluates the authenticated user's permissions and conditionally displays Pause and Resume action buttons only to authorized operators.

---

## 10. External Effects & Non-Rollback Warning

Per Deadbolt's core reliability contract:

> Pausing or cancelling platform execution prevents subsequent task dispatch and halts scheduling. **External side effects already performed by workers (such as external HTTP requests, database mutations, or third-party API calls) are NOT rolled back by the platform.**
> Operators must account for partial execution when reviewing paused workflows or initiating manual intervention.
