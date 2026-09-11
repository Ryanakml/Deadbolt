# SP-02 — DB Claim Contention, Lock Order, and Boundary Prototype

**Scope:** Issue #3.  
**Dependencies:** #1 (Workspace baseline, ADRs), #2 (Executable contracts).  
**Blueprint references:** §11.2 (Concurrency strategy), §18.1 (Entities and constraints), §24.3 (Database enforcement), §26.3 (Migration policy), §33 (Spike SP-02: DB claim contention).  
**Requirements & Invariants:** `REQ-SEC-01`, `REQ-OPS-01`, `REQ-DUR-01`, `INV-01`, `INV-03`, `INV-06`, `INV-08`, `F-27`.

---

## 1. Objectives and Legitimate Uncertainty

Spike SP-02 validates the foundational PostgreSQL concurrency and isolation architecture for Deadbolt:

1. **Prescribed Lock Order Hierarchy (INV-08):** Proves that strictly ordering row locks (`environment_admissions` $\rightarrow$ `runs` $\rightarrow$ `run_steps` in ascending ID order $\rightarrow$ `task_attempts`/`task_leases`) eliminates deadlocks (`SQLSTATE 40P01`) under high concurrent contention.
2. **Candidate Discovery & State Revalidation:** Demonstrates that candidate discovery via non-locking scans combined with authoritative lock acquisition and state revalidation safely handles racing workers without blocking queue scans.
3. **Mixed-Path Contention:** Proves that conflicting concurrent paths (Task Claiming vs Task Completion vs Background Reconciler) execute concurrently without deadlocks, without leaked leases, and with full typed error classification (no swallowed errors).
4. **Tenant Isolation & Failsafe (INV-01, F-27):** Proves that non-superuser runtime connections without `SET LOCAL app.current_organization_id` fail closed (0 rows), and that pooled connection reuse across tenants never leaks scope.
5. **Single Active Ownership (INV-03):** Proves that exactly one active lease exists per running step.
6. **Intermediate Migration Chain Safety:** Verifies that intermediate migration states (e.g. schema version 2) holding populated data can be upgraded to version 4 without data loss or RLS boundary regression.

---

## 2. Architectural Analysis: Evaluated Locking Strategies

In accordance with Blueprint §11.2, locking strategies were analyzed for contention characteristics and deadlock risks:

| Strategy                                                      | Contention Behavior                                                                                                                                                                                                                                  | Deadlock Risk                                                                                                     | Architectural Assessment                                                    |
| :------------------------------------------------------------ | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------- |
| **A. Naive Locking Queue Scan (`SELECT ... FOR UPDATE`)**     | Workers queue sequentially behind the oldest ready task; severe lock head-of-line blocking.                                                                                                                                                          | High under concurrent worker spikes.                                                                              | **Rejected by Design:** Violates throughput and latency requirements.       |
| **B. Inverted Lock Order (`run_steps` locked before `runs`)** | Claim transactions lock step first, while run cancellations or reconcilers lock run first.                                                                                                                                                           | **Guaranteed Deadlock (`40P01`):** Two concurrent transactions deadlock when competing for the same run and step. | **Rejected by Design:** Direct violation of Blueprint §11.2 lock hierarchy. |
| **C. Prescribed Lock Order with Non-Locking Candidate Scan**  | Queue discovery is non-locking; candidates are locked in strict hierarchy: `environment_admissions` $\rightarrow$ `runs` $\rightarrow$ `run_steps` $\rightarrow$ `leases`. Racing workers detect stale candidates on revalidation and retry cleanly. | **Zero Deadlocks (`40P01`):** All database operations acquire locks in the identical hierarchical direction.      | **Selected Architecture:** Fully conforms to Blueprint §11.2 and INV-08.    |

---

## 3. Query Plan Evidence (`EXPLAIN ANALYZE`)

The candidate step selection query utilizes the partial index `idx_run_steps_eligible ON run_steps (environment_id, state, eligible_at, id) WHERE state = 'READY'`:

```text
Limit  (cost=0.14..8.17 rows=1 width=46) (actual time=0.034..0.035 rows=1 loops=1)
  Buffers: shared hit=3
  ->  Index Scan using idx_run_steps_eligible on run_steps  (cost=0.14..8.16 rows=1 width=46) (actual time=0.018..0.018 rows=1 loops=1)
        Index Cond: ((environment_id = '30000000-0000-0000-0000-000000000001'::uuid) AND (state = 'READY'::text))
        Buffers: shared hit=2
Planning Time: 2.615 ms
Execution Time: 0.221 ms
```

Candidate discovery achieves index scan execution times under **0.25 ms** with 2 shared buffer hits, avoiding table sequential scans.

---

## 4. Empirical Contention Benchmark Results

Empirical benchmarks were executed via `tests/spikes/sp02/claim_test.go` on an isolated test database (`deadbolt_sp02_test`) on macOS arm64 against PostgreSQL:

### Benchmark 1: High-Concurrency Claim Contention (`TestClaimContentionAndPrescribedLockOrder`)

- **Workload profile:**
  - 10 concurrent active runs.
  - 50 total ready steps across runs.
  - 20 concurrent worker goroutines competing to claim tasks simultaneously under `deadbolt_runtime` role.
  - Environment admission concurrency cap: 50.
- **Empirical Measurements:**
  - Steps to claim: **50**
  - Successfully claimed: **50 / 50 (100%)**
  - Stale candidate retries: **480** (workers cleanly detect already-claimed steps upon revalidation and retry without error).
  - Deadlock errors (`SQLSTATE 40P01`): **0**
  - Unexpected database errors: **0**
  - Active leases created: **50 / 50**
  - Duplicate ownership violations: **0** (strictly enforced by `task_leases` primary key `step_id`).
- **Claim Latencies under 20 Concurrent Workers:**
  - **p50:** 2.58 ms
  - **p95:** 13.54 ms
  - **p99:** 35.41 ms

### Benchmark 2: Mixed-Path Contention (`TestMixedPathContention`)

- **Workload profile:**
  - Three competing roles operating simultaneously against the same 50 steps across 10 runs with centralized typed error handling:
    1. **10 Claim Workers:** executing `admission` $\rightarrow$ `run` $\rightarrow$ `step` $\rightarrow$ `lease`.
    2. **5 Completion Workers:** executing `run` $\rightarrow$ `step` $\rightarrow$ delete `task_leases` $\rightarrow$ update attempt & step to `SUCCEEDED`.
    3. **2 Reconcilers / Observers:** executing `run` $\rightarrow$ child `run_steps` in ascending ID order.
- **Empirical Measurements:**
  - Steps completed to terminal `SUCCEEDED`: **50 / 50**
  - Total claim operations: **50**
  - Total reconciler scans: **84**
  - Stale candidate retries: **449**
  - Deadlock errors (`SQLSTATE 40P01`): **0**
  - Unexpected database errors: **0**
  - Leaked or orphaned leases: **0**

---

## 5. Security, Isolation, and Role Boundary Validation

Integration tests in `tests/integration/rls_test.go` validate all database boundary controls on an isolated test database (`deadbolt_integration_test`):

1. **Clean Database Migration as `deadbolt_migrator` (`TestCleanDatabaseMigrationAsDeadboltMigrator`):** Proves that administrative bootstrap establishes roles, and `deadbolt_migrator` (not superuser/admin) successfully executes all migrations from version 0 to version 4 under advisory lock `7142893`.
2. **Advisory Lock Blocking (`TestMigrationAdvisoryLockBlocking`):** Proves that when Migrator A holds `SELECT pg_advisory_lock(7142893)` on a pinned physical connection, Migrator B is blocked from acquiring the lock concurrently, succeeding only after Migrator A releases the lock.
3. **Runtime Role Privilege Denial (`TestRuntimeNoDDLPrivileges`):** Role `deadbolt_runtime` is denied `CREATE TABLE` and `ALTER TABLE ... DISABLE ROW LEVEL SECURITY` with `SQLSTATE 42501 (permission denied)`.
4. **Fail-Closed RLS (`TestRLSFailsClosed`):** Queries executed without setting `app.current_organization_id` return **0 rows**, even when tables contain data.
5. **Connection Pool Reuse Safety (`TestConnectionPoolReuseF27`):** A connection used in Transaction 1 with `SELECT set_config('app.current_organization_id', OrgA, true)` returns 0 rows when used in Transaction 2 without setting tenant context, proving `set_config` with `is_local = true` cleanly resets upon commit.
6. **Composite Foreign Keys (`TestCompositeForeignKeys`):** Cross-tenant foreign key substitution fails across:
   - `environments(organization_id, project_id)` $\rightarrow$ `projects(organization_id, id)`
   - `task_attempts(organization_id, session_id)` $\rightarrow$ `worker_sessions(organization_id, id)`
   - `task_leases(organization_id, session_id)` $\rightarrow$ `worker_sessions(organization_id, id)`
   - `artifacts(organization_id, run_id)` $\rightarrow$ `runs(organization_id, id)`
7. **Restricted Discovery Functions (`TestRestrictedDiscoveryFunctions`):**
   - Production Go helper `storage.DiscoverUserMemberships` invokes `app.discover_user_memberships(user_id)` as `deadbolt_runtime` without prior tenant context, returning exclusively verified memberships for that user.
   - Production Go helper `storage.EnumerateTenantsForScheduler` invokes `app.enumerate_scheduler_tenants()` as `deadbolt_system`, returning active organization IDs.
   - `deadbolt_system` direct table access is denied (`SELECT * FROM public.runs` fails with `permission denied`).
8. **Intermediate Migration Step Upgrade Safety (`TestIntermediateMigrationStepUpgradeSafety`):** Rolling back to migration version 2, inserting valid tenant data, and applying forward migrations to version 4 preserves all pre-existing data and enforces RLS policies and foreign keys seamlessly on new tables.

---

## 6. Runnable Validation Commands

```sh
# 1. Run all integration tests (RLS, privileges, discovery functions, upgrade)
go test -v -race ./tests/integration/...

# 2. Run SP-02 claim contention and mixed-path contention benchmarks
go test -v -race ./tests/spikes/sp02/...

# 3. Run full parallel race test across all packages
go test -count=1 -race ./...
pnpm check:contracts
pnpm check:parity
```

---

## 7. Delivery Boundaries & Status

- **Implemented:**
  - Four ordered SQL migrations (`00001`–`00004`) covering all M0 and V1 entities.
  - Single-migrator runner with pinned physical session advisory lock (`7142893`).
  - Database role bootstrap script (`scripts/bootstrap-db-roles.sql`) establishing `deadbolt_migrator`, `deadbolt_runtime`, and `deadbolt_system`.
  - Production Go discovery helpers in `internal/storage/discovery.go` invoking restricted functions.
  - Composite foreign keys across all execution, lease, and artifact entities.
  - Prescribed lock order claim and completion implementation in SP-02 with typed error handling.
  - Dedicated test database isolation package `internal/storage/testdb`.
- **Automated Tests:** All 8 integration tests and 2 SP-02 contention benchmark suites pass cleanly with Go race detector (`-race`).
- **CI Hardening:** GitHub Actions workflow (`.github/workflows/contracts.yml`) equipped with PostgreSQL 16 service container and `TEST_DATABASE_URL` configuration.
- **INV-06 Traceability:** Schema provides persistence tables (`run_events`, `outbox_events`, `task_attempts`, `task_leases`); runtime atomic commit orchestration scheduled for M1 execution engine.
