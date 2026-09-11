# SP-02 — DB Claim Contention and Lock Order Prototype

**Scope:** Issue #3.  
**Dependencies:** #1 (Workspace baseline, ADRs), #2 (Executable contracts).  
**Blueprint references:** §11.2 (Concurrency strategy), §18.1 (Entities and constraints), §24.3 (Database enforcement), §26.3 (Migration policy), §33 (Spike SP-02: DB claim contention).  
**Requirements & Invariants:** `REQ-SEC-01`, `REQ-OPS-01`, `REQ-DUR-01`, `INV-01`, `INV-03`, `INV-08`, `F-27`.

---

## 1. Objectives and Legitimate Uncertainty

Spike SP-02 proves the PostgreSQL concurrency and locking model for Deadbolt:
1. **Prescribed Lock Hierarchy:** Verifies that acquiring locks in the order specified in Blueprint §11.2 (`environment_admissions` $\rightarrow$ `runs` $\rightarrow$ `run_steps` in ascending ID order $\rightarrow$ `task_attempts`/`task_leases`) eliminates lock inversions and deadlocks (`SQLSTATE 40P01`).
2. **Candidate Discovery Strategy:** Validates that `FOR UPDATE SKIP LOCKED` on indexed eligible steps (`idx_run_steps_eligible`) enables concurrent workers to claim ready tasks without waiting on or blocking each other.
3. **Multi-Tenant Isolation & Failsafe (INV-01, F-27):** Proves that non-superuser runtime connections without `SET LOCAL app.current_organization_id` fail closed (0 rows), and that pooled connection reuse across tenants never leaks scope.
4. **Single Active Ownership (INV-03):** Proves that at most one current lease exists per step under concurrent contention.

---

## 2. Locking Strategies Evaluated

| Strategy | Contention Behavior | Deadlock Risk | Invariant Compliance | Decision |
| :--- | :--- | :--- | :--- | :--- |
| **Naive `SELECT ... FOR UPDATE`** | Workers queue sequentially behind the oldest ready task; high lock contention | High if steps or runs are locked out of order | Violates latency targets | **Rejected** |
| **Inverted Lock Order** (e.g. `run_step` before `run`) | Reconciler and claim workers interleave in opposite directions | Guaranteed `40P01` deadlock under load | Violates INV-08 | **Rejected** |
| **Prescribed Lock Order with `FOR UPDATE SKIP LOCKED`** | Non-blocking queue polling; candidates claimed independently; strict hierarchy: `environment_admissions` $\rightarrow$ `candidate SKIP LOCKED` $\rightarrow$ `run` $\rightarrow$ `step` $\rightarrow$ `lease` | **Zero** deadlocks observed | Enforces INV-01, INV-03, INV-08 | **Selected** |

---

## 3. Query Plan Evidence (`EXPLAIN ANALYZE`)

For candidate step selection with index condition `(environment_id, state = 'READY')`:
```text
Limit  (cost=0.14..8.17 rows=1 width=46) (actual time=0.034..0.035 rows=1 loops=1)
  Buffers: shared hit=3
  ->  LockRows  (cost=0.14..8.17 rows=1 width=46) (actual time=0.033..0.033 rows=1 loops=1)
        Buffers: shared hit=3
        ->  Index Scan using idx_run_steps_eligible on run_steps  (cost=0.14..8.16 rows=1 width=46) (actual time=0.018..0.018 rows=1 loops=1)
              Index Cond: ((environment_id = '30000000-0000-0000-0000-000000000001'::uuid) AND (state = 'READY'::text))
              Buffers: shared hit=2
Planning Time: 2.615 ms
Execution Time: 0.221 ms
```
The index `idx_run_steps_eligible ON run_steps (environment_id, state, eligible_at, id) WHERE state = 'READY'` delivers an index scan execution time of **0.221 ms** with only 3 shared buffer hits.

---

## 4. Concurrent Contention Benchmark Results

Executed via `tests/spikes/sp02/claim_test.go` on macOS arm64 against PostgreSQL 18:

* **Workload profile:**
  * 10 concurrent active runs.
  * 50 total eligible ready steps across runs.
  * 20 concurrent worker goroutines competing to claim tasks simultaneously.
  * Environment admission concurrency cap: 50.
* **Results:**
  * Total steps to claim: **50**
  * Total successfully claimed: **50**
  * Deadlock errors (`40P01`): **0**
  * Other database errors: **0**
  * Active leases created: **50 / 50**
  * Duplicate ownership occurrences: **0** (strictly enforced by `task_leases` primary key `step_id`).
* **Claim Latencies (under 20 concurrent contending workers):**
  * **p50:** 1.05 s
  * **p95:** 2.28 s
  * **p99:** 2.45 s

---

## 5. Security & Isolation Validation (F-27, INV-01)

Executed via `tests/integration/rls_test.go`:

1. **Runtime Privilege Revocation:** Role `deadbolt_runtime` is denied `CREATE TABLE` and `ALTER TABLE ... DISABLE ROW LEVEL SECURITY` with `SQLSTATE 42501 (permission denied)`.
2. **Fail-Closed Verification:** Queries executed without setting `app.current_organization_id` return **0 rows**, even when tables contain data.
3. **Connection Pool Reuse (F-27):** A connection used in Transaction 1 with `SELECT set_config('app.current_organization_id', OrgA, true)` returns 0 rows when used in Transaction 2 without setting tenant context, proving `set_config` with `is_local = true` cleanly resets upon commit.
4. **Composite Foreign Keys:** Inserting an environment with `(organization_id = OrgB, project_id = ProjectA)` fails with foreign key violation because `projects` is constrained by composite unique `(organization_id, id)`.
5. **Advisory Lock Migrator:** Migrations run under `pg_advisory_lock(7142893)`, serializing schema changes across multiple instances.

---

## 6. Runnable Validation Commands

```sh
# 1. Run RLS, multi-tenant isolation, and privilege tests
go test -v -race ./tests/integration/...

# 2. Run SP-02 claim contention, lock order, and benchmark test
go test -v -race ./tests/spikes/sp02/...

# 3. Run full contract parity and unit test suite
go test -race ./...
pnpm check:contracts
pnpm check:parity
```

---

## 7. Delivery Boundaries & Status

* **Implemented:** Ordered SQL migrations 00001–00004, `migrator.Runner` with advisory lock, `storage.Pool` with `WithTenantTx`, tenant discovery functions, RLS integration tests, and SP-02 contention benchmark.
* **Automated tests passed:** All 5 integration tests and SP-02 benchmark passed with `-race` enabled on PostgreSQL 18.
* **Hosted CI passed:** Pending PR creation and CI execution.
* **Deployed:** Not applicable to M0 issue #3 (local database verification only).
* **Live acceptance verified:** SP-02 contention and RLS boundary proven against real PostgreSQL instance.
