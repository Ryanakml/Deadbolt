-- +goose Up
-- Tenant mutation commands are the idempotency authority.  Audit events remain
-- append-only observability records and must never be used as a replay store.
CREATE TABLE tenant_commands (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    command_scope TEXT NOT NULL,
    -- Deliberately not a foreign key: a successful organization deletion must
    -- retain its command outcome for deterministic retries.
    organization_id UUID NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    operation TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PROCESSING',
    resource_id TEXT,
    response_code INT NOT NULL DEFAULT 200,
    outcome JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT tenant_commands_scope_key_unique UNIQUE (command_scope, idempotency_key)
);

ALTER TABLE tenant_commands ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_commands FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_tenant_commands ON tenant_commands
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- +goose Down
DROP TABLE IF EXISTS tenant_commands;
