-- +goose Up
-- Task Logs: Bounded diagnostic console logs per Blueprint §25.1 and ADR-15

CREATE TABLE task_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    environment_id UUID NOT NULL,
    run_id UUID NOT NULL,
    step_id UUID NOT NULL,
    attempt_id UUID NOT NULL,
    sequence BIGINT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    level TEXT NOT NULL CHECK (level IN ('debug', 'info', 'warn', 'error')),
    message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, run_id) REFERENCES runs(organization_id, id) ON DELETE CASCADE,
    FOREIGN KEY (organization_id, step_id) REFERENCES run_steps(organization_id, id) ON DELETE CASCADE,
    FOREIGN KEY (organization_id, attempt_id) REFERENCES task_attempts(organization_id, id) ON DELETE CASCADE,
    FOREIGN KEY (organization_id, environment_id) REFERENCES environments(organization_id, id) ON DELETE CASCADE,
    UNIQUE (attempt_id, sequence)
);

ALTER TABLE task_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE task_logs FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_task_logs ON task_logs
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

CREATE INDEX idx_task_logs_lookup ON task_logs (organization_id, run_id, created_at, id);
CREATE INDEX idx_task_logs_step ON task_logs (organization_id, step_id, created_at, id);
CREATE INDEX idx_task_logs_attempt ON task_logs (organization_id, attempt_id, created_at, id);
CREATE INDEX idx_task_logs_retention ON task_logs (created_at);

ALTER TABLE task_attempts ADD COLUMN IF NOT EXISTS logs_recorded BOOLEAN NOT NULL DEFAULT FALSE;

-- +goose Down
DROP TABLE IF EXISTS task_logs CASCADE;
ALTER TABLE task_attempts DROP COLUMN IF EXISTS logs_recorded;
