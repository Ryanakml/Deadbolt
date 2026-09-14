-- +goose Up
-- 1. Worker Enrollments
CREATE TABLE worker_enrollments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    environment_id UUID NOT NULL,
    token_hash TEXT NOT NULL,
    pool_name TEXT NOT NULL DEFAULT 'default',
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ,
    created_by UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, environment_id) REFERENCES environments(organization_id, id) ON DELETE CASCADE,
    UNIQUE (organization_id, id)
);

ALTER TABLE worker_enrollments ENABLE ROW LEVEL SECURITY;
ALTER TABLE worker_enrollments FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_worker_enrollments ON worker_enrollments
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

CREATE INDEX idx_worker_enrollments_lookup ON worker_enrollments (token_hash) WHERE used_at IS NULL;

-- 2. Worker Challenges (single-use challenge nonces)
CREATE TABLE worker_challenges (
    nonce TEXT PRIMARY KEY,
    worker_id TEXT,
    public_key TEXT,
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX idx_worker_challenges_expires ON worker_challenges (expires_at);

-- 3. Task Attempts Start Deadline
ALTER TABLE task_attempts ADD COLUMN IF NOT EXISTS claim_start_deadline_at TIMESTAMPTZ;

-- +goose Down
ALTER TABLE task_attempts DROP COLUMN IF EXISTS claim_start_deadline_at;
DROP TABLE IF EXISTS worker_challenges;
DROP TABLE IF EXISTS worker_enrollments CASCADE;
