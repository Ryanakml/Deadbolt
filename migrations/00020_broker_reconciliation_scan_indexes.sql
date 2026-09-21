-- +goose Up
-- Bounded PostgreSQL fallback scans for M2 reconciliation. These indexes do not
-- change execution semantics; they keep broker-independent recovery predictable
-- as tenant data grows.
CREATE INDEX IF NOT EXISTS idx_runs_reconciliation_active
    ON runs (organization_id, updated_at, id)
    WHERE status IN ('QUEUED', 'RUNNING');

CREATE INDEX IF NOT EXISTS idx_run_steps_reconciliation_blocked
    ON run_steps (organization_id, run_id, updated_at, id)
    WHERE state = 'BLOCKED';

CREATE INDEX IF NOT EXISTS idx_task_attempts_reconciliation_active
    ON task_attempts (organization_id, step_id, status, id)
    WHERE status IN ('CLAIMED', 'RUNNING');

-- +goose Down
DROP INDEX IF EXISTS idx_task_attempts_reconciliation_active;
DROP INDEX IF EXISTS idx_run_steps_reconciliation_blocked;
DROP INDEX IF EXISTS idx_runs_reconciliation_active;
