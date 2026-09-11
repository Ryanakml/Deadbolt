# Database Schema, RLS Boundaries, and Migration Contract

## 1. Architectural Scope and Blueprint Traceability

- **Blueprint sections**: §11 (Persistence, transactions, and checkpoints), §18 (Data model and data lifecycle), §24 (Authentication, authorization, and tenant isolation), §26 (Deployment, delivery, and migrations).
- **Invariants enforced**:
  - `INV-01`: Tenant, project, and environment isolation via `FORCE ROW LEVEL SECURITY` and composite foreign keys.
  - `INV-03`: At most one current ownership lease exists per step (`task_leases` partial unique/primary key).
  - `INV-06`: State transitions, execution events, and outbox intents are committed atomically in the same transaction.
  - `INV-08`: Explicit lock hierarchy (`environment_admission` $\rightarrow$ `run` $\rightarrow$ `run_steps` sorted $\rightarrow$ `attempts/leases`).
- **Failure Mode coverage**:
  - `F-27`: Tenant context reused in connection pool. Prevented by `SET LOCAL app.current_organization_id` which automatically clears upon transaction completion, ensuring clean connection reuse.

---

## 2. PostgreSQL Role and Privilege Model

In accordance with Blueprint §24.3 and §26.3, runtime operations and database migrations use strictly separated database roles:

| Role | DDL Privileges | DML Privileges | RLS Policy Status | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| `deadbolt_migrator` | Yes (schema owner) | Yes | Bypassed during DDL migration execution | Executes ordered migrations with `pg_advisory_lock` |
| `deadbolt_runtime` | **No** (revoked) | `SELECT, INSERT, UPDATE, DELETE` | **FORCE ROW LEVEL SECURITY** | Used by Go control-plane application |
| `deadbolt_system` | **No** | Read-only discovery functions | Restricted security definer | Used by scheduler for tenant ID enumeration |

### Security Invariant: Missing Context Fails Closed
The tenant context function:
```sql
CREATE OR REPLACE FUNCTION app.current_organization_id() RETURNS uuid AS $$
BEGIN
    RETURN NULLIF(current_setting('app.current_organization_id', true), '')::uuid;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER;
```
If `app.current_organization_id` has not been set via `SET LOCAL`, the function evaluates to `NULL`. Since `organization_id = NULL` evaluates to `FALSE` in SQL boolean logic, all tenant-isolated queries fail closed, returning 0 rows.

---

## 3. Ordered Migrations

Migrations are stored in `migrations/` and executed sequentially using `goose` under an advisory lock:

1. `00001_roles_and_tenants.sql`:
   - `organizations`, `organization_members`
   - `projects`, `environments` (composite keys `(organization_id, project_id)`)
   - `environment_admissions` (concurrency quota lock rows)
   - `api_keys`
2. `00002_workflows_and_deployments.sql`:
   - `deployments` (immutable manifests, bundle digest, runtime versions)
   - `workflow_definitions`, `task_definitions`
   - `workflow_channels` (active deployment pointers with revisions)
3. `00003_execution_and_claims.sql`:
   - `workers`, `worker_sessions`, `worker_deployments`
   - `runs`, `run_steps`, `task_attempts`, `task_leases`
   - `run_events` (append-only history)
   - `timers`, `idempotency_records`, `outbox_events`
4. `00004_v1_entities.sql`:
   - `approvals`, `reconciliation_cases`, `stop_commands`
   - `schedules`, `schedule_occurrences`
   - `webhook_endpoints`, `webhook_deliveries`
   - `artifacts`, `audit_events`, `usage_records`

### Single-Migrator Advisory Lock
All migrations acquire `SELECT pg_advisory_lock(7142893)` before applying changes, guaranteeing that concurrent control-plane instances do not run migrations simultaneously.

---

## 4. Prescribed Lock Order

To eliminate lock inversions and deadlocks (Blueprint §11.2):
```text
1. environment_admissions (row lock FOR UPDATE when checking environment concurrency quota)
2. schedule (if scheduling occurrence)
3. runs (row lock FOR UPDATE)
4. run_steps (row locks FOR UPDATE in ascending UUID/ID order)
5. task_attempts / task_leases (row insert or lock in ascending ID order)
```

No path acquires `environment_admissions` after acquiring a `runs` or `run_steps` lock.
Candidate task discovery uses `FOR UPDATE SKIP LOCKED` to allow non-blocking concurrent worker polling.
