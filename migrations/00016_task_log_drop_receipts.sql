-- +goose Up
-- Durable, retry-safe diagnostic-log drop accounting for Inspector observability.

ALTER TABLE task_attempts ADD COLUMN IF NOT EXISTS dropped_log_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_attempts ADD COLUMN IF NOT EXISTS log_budget_exhausted BOOLEAN NOT NULL DEFAULT FALSE;

-- Rejected records need a durable receipt too. Otherwise a worker retry of a
-- full/oversized batch would be counted as a new drop every time even though no
-- additional diagnostic data was lost.
CREATE TABLE task_log_drop_receipts (
    organization_id UUID NOT NULL,
    attempt_id UUID NOT NULL,
    sequence BIGINT NOT NULL,
    reason TEXT NOT NULL CHECK (reason IN ('LINE_TOO_LARGE', 'BUDGET_EXHAUSTED')),
    PRIMARY KEY (attempt_id, sequence),
    FOREIGN KEY (organization_id, attempt_id) REFERENCES task_attempts(organization_id, id) ON DELETE CASCADE
);

ALTER TABLE task_log_drop_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE task_log_drop_receipts FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_task_log_drop_receipts ON task_log_drop_receipts
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- +goose Down
DROP TABLE IF EXISTS task_log_drop_receipts CASCADE;
ALTER TABLE task_attempts DROP COLUMN IF EXISTS log_budget_exhausted;
ALTER TABLE task_attempts DROP COLUMN IF EXISTS dropped_log_count;
