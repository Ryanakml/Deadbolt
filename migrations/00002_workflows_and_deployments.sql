-- +goose Up
-- 1. Deployments (immutable per environment)
CREATE TABLE deployments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    environment_id UUID NOT NULL,
    manifest_hash TEXT NOT NULL,
    bundle_digest TEXT NOT NULL,
    manifest JSONB NOT NULL,
    protocol_version INT NOT NULL DEFAULT 1,
    runtime_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, environment_id) REFERENCES environments(organization_id, id) ON DELETE CASCADE,
    UNIQUE (organization_id, id),
    UNIQUE (environment_id, manifest_hash)
);

ALTER TABLE deployments ENABLE ROW LEVEL SECURITY;
ALTER TABLE deployments FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_deployments ON deployments
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- 2. Workflow Definitions
CREATE TABLE workflow_definitions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    deployment_id UUID NOT NULL,
    name TEXT NOT NULL,
    input_schema JSONB NOT NULL,
    output_schema JSONB NOT NULL,
    nodes JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, deployment_id) REFERENCES deployments(organization_id, id) ON DELETE CASCADE,
    UNIQUE (deployment_id, name),
    UNIQUE (organization_id, id)
);

ALTER TABLE workflow_definitions ENABLE ROW LEVEL SECURITY;
ALTER TABLE workflow_definitions FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_workflow_definitions ON workflow_definitions
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- 3. Task Definitions
CREATE TABLE task_definitions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    deployment_id UUID NOT NULL,
    name TEXT NOT NULL,
    entrypoint TEXT NOT NULL,
    input_schema JSONB NOT NULL,
    output_schema JSONB NOT NULL,
    recovery_policy TEXT NOT NULL CHECK (recovery_policy IN ('safe', 'idempotent', 'reconcile')),
    timeout_ms INT NOT NULL CHECK (timeout_ms > 0),
    max_attempts INT NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
    initial_delay_ms INT NOT NULL DEFAULT 1000,
    max_delay_ms INT NOT NULL DEFAULT 30000,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, deployment_id) REFERENCES deployments(organization_id, id) ON DELETE CASCADE,
    UNIQUE (deployment_id, name),
    UNIQUE (organization_id, id)
);

ALTER TABLE task_definitions ENABLE ROW LEVEL SECURITY;
ALTER TABLE task_definitions FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_task_definitions ON task_definitions
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- 4. Workflow Channels (Active deployment pointer per workflow and environment)
CREATE TABLE workflow_channels (
    environment_id UUID NOT NULL,
    organization_id UUID NOT NULL,
    workflow_name TEXT NOT NULL,
    active_deployment_id UUID NOT NULL,
    revision BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (environment_id, workflow_name),
    FOREIGN KEY (organization_id, environment_id) REFERENCES environments(organization_id, id) ON DELETE CASCADE,
    FOREIGN KEY (organization_id, active_deployment_id) REFERENCES deployments(organization_id, id) ON DELETE RESTRICT
);

ALTER TABLE workflow_channels ENABLE ROW LEVEL SECURITY;
ALTER TABLE workflow_channels FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_workflow_channels ON workflow_channels
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- +goose Down
DROP TABLE IF EXISTS workflow_channels CASCADE;
DROP TABLE IF EXISTS task_definitions CASCADE;
DROP TABLE IF EXISTS workflow_definitions CASCADE;
DROP TABLE IF EXISTS deployments CASCADE;
