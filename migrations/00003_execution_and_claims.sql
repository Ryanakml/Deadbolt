-- +goose Up
-- 1. Workers
CREATE TABLE workers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    environment_id UUID NOT NULL,
    public_key TEXT NOT NULL,
    pool_name TEXT NOT NULL DEFAULT 'default',
    status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'DRAINING', 'REVOKED')),
    last_seen_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, environment_id) REFERENCES environments(organization_id, id) ON DELETE CASCADE,
    UNIQUE (organization_id, id)
);

ALTER TABLE workers ENABLE ROW LEVEL SECURITY;
ALTER TABLE workers FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_workers ON workers
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- 2. Worker Sessions
CREATE TABLE worker_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    worker_id UUID NOT NULL,
    environment_id UUID NOT NULL,
    session_token_hash TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, worker_id) REFERENCES workers(organization_id, id) ON DELETE CASCADE,
    FOREIGN KEY (organization_id, environment_id) REFERENCES environments(organization_id, id) ON DELETE CASCADE,
    UNIQUE (organization_id, id)
);

ALTER TABLE worker_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE worker_sessions FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_worker_sessions ON worker_sessions
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- 3. Worker Deployments (available bundle digests per session)
CREATE TABLE worker_deployments (
    session_id UUID NOT NULL,
    organization_id UUID NOT NULL,
    bundle_digest TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (session_id, bundle_digest),
    FOREIGN KEY (organization_id, session_id) REFERENCES worker_sessions(organization_id, id) ON DELETE CASCADE
);

ALTER TABLE worker_deployments ENABLE ROW LEVEL SECURITY;
ALTER TABLE worker_deployments FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_worker_deployments ON worker_deployments
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- 4. Runs (pinned deployment, revision, event sequence)
CREATE TABLE runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    environment_id UUID NOT NULL,
    deployment_id UUID NOT NULL,
    workflow_name TEXT NOT NULL,
    idempotency_key TEXT,
    status TEXT NOT NULL DEFAULT 'QUEUED' CHECK (status IN ('QUEUED', 'RUNNING', 'WAITING', 'PAUSING', 'PAUSED', 'CANCELLING', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
    reason_code TEXT,
    revision BIGINT NOT NULL DEFAULT 1,
    input JSONB NOT NULL DEFAULT '{}',
    output JSONB,
    deadline_at TIMESTAMPTZ,
    last_event_sequence BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, environment_id) REFERENCES environments(organization_id, id) ON DELETE CASCADE,
    FOREIGN KEY (organization_id, deployment_id) REFERENCES deployments(organization_id, id) ON DELETE RESTRICT,
    UNIQUE (organization_id, id)
);

ALTER TABLE runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE runs FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_runs ON runs
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

CREATE INDEX idx_runs_env_status_created ON runs (environment_id, status, created_at);

-- 5. Run Steps (logical nodes with stable identities)
CREATE TABLE run_steps (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    environment_id UUID NOT NULL,
    run_id UUID NOT NULL,
    node_id TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'task' CHECK (kind IN ('task', 'approval', 'delay', 'choice', 'merge')),
    state TEXT NOT NULL DEFAULT 'BLOCKED' CHECK (state IN ('BLOCKED', 'READY', 'RUNNING', 'WAITING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'SKIPPED')),
    wait_reason TEXT,
    current_epoch BIGINT NOT NULL DEFAULT 0,
    next_attempt_number INT NOT NULL DEFAULT 1,
    eligible_at TIMESTAMPTZ,
    output JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, run_id) REFERENCES runs(organization_id, id) ON DELETE CASCADE,
    UNIQUE (organization_id, id),
    UNIQUE (run_id, node_id)
);

ALTER TABLE run_steps ENABLE ROW LEVEL SECURITY;
ALTER TABLE run_steps FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_run_steps ON run_steps
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

CREATE INDEX idx_run_steps_eligible ON run_steps (environment_id, state, eligible_at, id) WHERE state = 'READY';

-- 6. Task Attempts (immutable ownership effort)
CREATE TABLE task_attempts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    step_id UUID NOT NULL,
    attempt_number INT NOT NULL,
    session_id UUID,
    epoch BIGINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'CLAIMED' CHECK (status IN ('CLAIMED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'TIMED_OUT', 'LOST', 'CANCELLED')),
    outcome_digest TEXT,
    error JSONB,
    started_at TIMESTAMPTZ,
    deadline_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, step_id) REFERENCES run_steps(organization_id, id) ON DELETE CASCADE,
    FOREIGN KEY (organization_id, session_id) REFERENCES worker_sessions(organization_id, id) ON DELETE SET NULL,
    UNIQUE (organization_id, id),
    UNIQUE (step_id, attempt_number)
);

ALTER TABLE task_attempts ENABLE ROW LEVEL SECURITY;
ALTER TABLE task_attempts FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_task_attempts ON task_attempts
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- 7. Task Leases (exactly one current lease row per active step)
CREATE TABLE task_leases (
    step_id UUID PRIMARY KEY,
    organization_id UUID NOT NULL,
    attempt_id UUID NOT NULL,
    session_id UUID NOT NULL,
    epoch BIGINT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, step_id) REFERENCES run_steps(organization_id, id) ON DELETE CASCADE,
    FOREIGN KEY (organization_id, attempt_id) REFERENCES task_attempts(organization_id, id) ON DELETE CASCADE,
    FOREIGN KEY (organization_id, session_id) REFERENCES worker_sessions(organization_id, id) ON DELETE CASCADE
);

ALTER TABLE task_leases ENABLE ROW LEVEL SECURITY;
ALTER TABLE task_leases FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_task_leases ON task_leases
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

CREATE INDEX idx_task_leases_expires ON task_leases (expires_at);

-- 8. Run Events (append-only authoritative execution history)
CREATE TABLE run_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    run_id UUID NOT NULL,
    sequence BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    version INT NOT NULL DEFAULT 1,
    payload JSONB NOT NULL DEFAULT '{}',
    committed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, run_id) REFERENCES runs(organization_id, id) ON DELETE CASCADE,
    UNIQUE (run_id, sequence)
);

ALTER TABLE run_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE run_events FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_run_events ON run_events
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

CREATE INDEX idx_run_events_lookup ON run_events (run_id, sequence);

-- 9. Timers (durable timers with unique logical action identity)
CREATE TABLE timers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    environment_id UUID NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('RETRY_BACKOFF', 'DELAY', 'SCHEDULE', 'LEASE_TIMEOUT')),
    reference_id UUID NOT NULL,
    due_at TIMESTAMPTZ NOT NULL,
    state TEXT NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING', 'FIRED', 'CANCELLED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, environment_id) REFERENCES environments(organization_id, id) ON DELETE CASCADE
);

ALTER TABLE timers ENABLE ROW LEVEL SECURITY;
ALTER TABLE timers FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_timers ON timers
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

CREATE INDEX idx_timers_due ON timers (due_at) WHERE state = 'PENDING';

-- 10. Idempotency Records
CREATE TABLE idempotency_records (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    environment_id UUID NOT NULL,
    key_hash TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    response_identity UUID,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, environment_id) REFERENCES environments(organization_id, id) ON DELETE CASCADE,
    UNIQUE (environment_id, key_hash)
);

ALTER TABLE idempotency_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE idempotency_records FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_idempotency_records ON idempotency_records
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- 11. Outbox Events (transactional outbox for wake-up hints)
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    event_id UUID NOT NULL DEFAULT gen_random_uuid(),
    subject TEXT NOT NULL,
    payload JSONB NOT NULL,
    payload_version INT NOT NULL DEFAULT 1,
    attempts INT NOT NULL DEFAULT 0,
    next_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (event_id)
);

ALTER TABLE outbox_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE outbox_events FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_outbox_events ON outbox_events
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

CREATE INDEX idx_outbox_events_pending ON outbox_events (next_at) WHERE published_at IS NULL;

-- +goose Down
DROP TABLE IF EXISTS outbox_events CASCADE;
DROP TABLE IF EXISTS idempotency_records CASCADE;
DROP TABLE IF EXISTS timers CASCADE;
DROP TABLE IF EXISTS run_events CASCADE;
DROP TABLE IF EXISTS task_leases CASCADE;
DROP TABLE IF EXISTS task_attempts CASCADE;
DROP TABLE IF EXISTS run_steps CASCADE;
DROP TABLE IF EXISTS runs CASCADE;
DROP TABLE IF EXISTS worker_deployments CASCADE;
DROP TABLE IF EXISTS worker_sessions CASCADE;
DROP TABLE IF EXISTS workers CASCADE;
