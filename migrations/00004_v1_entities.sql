-- +goose Up
-- 1. Approvals (human waiting control node)
CREATE TABLE approvals (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    environment_id UUID NOT NULL,
    step_id UUID NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}',
    required_permission TEXT NOT NULL DEFAULT 'approvals:write',
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED', 'EXPIRED', 'CANCELLED')),
    decision_reason TEXT,
    actor_id UUID,
    expires_at TIMESTAMPTZ,
    decided_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, step_id) REFERENCES run_steps(organization_id, id) ON DELETE CASCADE,
    FOREIGN KEY (organization_id, environment_id) REFERENCES environments(organization_id, id) ON DELETE CASCADE,
    UNIQUE (step_id)
);

ALTER TABLE approvals ENABLE ROW LEVEL SECURITY;
ALTER TABLE approvals FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_approvals ON approvals
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- 2. Reconciliation Cases
CREATE TABLE reconciliation_cases (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    environment_id UUID NOT NULL,
    step_id UUID NOT NULL,
    attempt_id UUID,
    reason TEXT NOT NULL,
    evidence JSONB NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'RESOLVED')),
    resolution TEXT CHECK (resolution IN ('RETRY', 'SUCCEED', 'FAIL', 'CANCEL')),
    actor_id UUID,
    revision BIGINT NOT NULL DEFAULT 1,
    resolved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, step_id) REFERENCES run_steps(organization_id, id) ON DELETE CASCADE,
    FOREIGN KEY (organization_id, environment_id) REFERENCES environments(organization_id, id) ON DELETE CASCADE
);

ALTER TABLE reconciliation_cases ENABLE ROW LEVEL SECURITY;
ALTER TABLE reconciliation_cases FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_reconciliation_cases ON reconciliation_cases
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- 3. Stop Commands (process termination tracking)
CREATE TABLE stop_commands (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    attempt_id UUID NOT NULL,
    reason TEXT NOT NULL,
    deadline_at TIMESTAMPTZ NOT NULL,
    acked_at TIMESTAMPTZ,
    termination_confirmed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, attempt_id) REFERENCES task_attempts(organization_id, id) ON DELETE CASCADE
);

ALTER TABLE stop_commands ENABLE ROW LEVEL SECURITY;
ALTER TABLE stop_commands FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_stop_commands ON stop_commands
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- 4. Schedules and Schedule Occurrences
CREATE TABLE schedules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    environment_id UUID NOT NULL,
    workflow_name TEXT NOT NULL,
    cron_expression TEXT NOT NULL,
    timezone TEXT NOT NULL DEFAULT 'UTC',
    paused BOOLEAN NOT NULL DEFAULT false,
    revision BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, environment_id) REFERENCES environments(organization_id, id) ON DELETE CASCADE,
    UNIQUE (organization_id, id)
);

ALTER TABLE schedules ENABLE ROW LEVEL SECURITY;
ALTER TABLE schedules FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_schedules ON schedules
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

CREATE TABLE schedule_occurrences (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    schedule_id UUID NOT NULL,
    occurrence_key TEXT NOT NULL,
    due_at TIMESTAMPTZ NOT NULL,
    run_id UUID,
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'STARTED', 'SKIPPED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, schedule_id) REFERENCES schedules(organization_id, id) ON DELETE CASCADE,
    UNIQUE (schedule_id, occurrence_key)
);

ALTER TABLE schedule_occurrences ENABLE ROW LEVEL SECURITY;
ALTER TABLE schedule_occurrences FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_schedule_occurrences ON schedule_occurrences
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- 5. Webhook Endpoints and Deliveries
CREATE TABLE webhook_endpoints (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    environment_id UUID NOT NULL,
    url TEXT NOT NULL,
    encrypted_secret TEXT NOT NULL,
    subscribed_events TEXT[] NOT NULL DEFAULT '{"run.succeeded", "run.failed"}',
    active BOOLEAN NOT NULL DEFAULT true,
    revision BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, environment_id) REFERENCES environments(organization_id, id) ON DELETE CASCADE,
    UNIQUE (organization_id, id)
);

ALTER TABLE webhook_endpoints ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_endpoints FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_webhook_endpoints ON webhook_endpoints
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

CREATE TABLE webhook_deliveries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    endpoint_id UUID NOT NULL,
    event_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    attempts INT NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'DELIVERED', 'FAILED')),
    next_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    delivered_at TIMESTAMPTZ,
    last_status_code INT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, endpoint_id) REFERENCES webhook_endpoints(organization_id, id) ON DELETE CASCADE
);

ALTER TABLE webhook_deliveries ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_deliveries FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_webhook_deliveries ON webhook_deliveries
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- 6. Artifacts (typed object references)
CREATE TABLE artifacts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    environment_id UUID NOT NULL,
    run_id UUID,
    step_id UUID,
    storage_key TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    sha256_hash TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING_UPLOAD' CHECK (status IN ('PENDING_UPLOAD', 'READY', 'DELETING', 'DELETED', 'EXPIRED')),
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, environment_id) REFERENCES environments(organization_id, id) ON DELETE CASCADE,
    FOREIGN KEY (organization_id, run_id) REFERENCES runs(organization_id, id) ON DELETE CASCADE,
    FOREIGN KEY (organization_id, step_id) REFERENCES run_steps(organization_id, id) ON DELETE CASCADE
);

ALTER TABLE artifacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE artifacts FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_artifacts ON artifacts
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- 7. Audit Events (immutable append-only audit trail)
CREATE TABLE audit_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    actor_id UUID,
    action TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id UUID NOT NULL,
    reason TEXT,
    correlation_id TEXT,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

ALTER TABLE audit_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_events FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_audit_events ON audit_events
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- 8. Usage Records
CREATE TABLE usage_records (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    environment_id UUID NOT NULL,
    source_event_id UUID NOT NULL,
    meter_type TEXT NOT NULL,
    quantity BIGINT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, environment_id) REFERENCES environments(organization_id, id) ON DELETE CASCADE,
    UNIQUE (source_event_id, meter_type)
);

ALTER TABLE usage_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE usage_records FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_usage_records ON usage_records
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- +goose Down
DROP TABLE IF EXISTS usage_records CASCADE;
DROP TABLE IF EXISTS audit_events CASCADE;
DROP TABLE IF EXISTS artifacts CASCADE;
DROP TABLE IF EXISTS webhook_deliveries CASCADE;
DROP TABLE IF EXISTS webhook_endpoints CASCADE;
DROP TABLE IF EXISTS schedule_occurrences CASCADE;
DROP TABLE IF EXISTS schedules CASCADE;
DROP TABLE IF EXISTS stop_commands CASCADE;
DROP TABLE IF EXISTS reconciliation_cases CASCADE;
DROP TABLE IF EXISTS approvals CASCADE;
