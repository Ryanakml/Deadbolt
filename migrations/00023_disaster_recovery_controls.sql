-- +goose Up
-- M2 Disaster recovery controls, incidents, and deletion ledger review hooks (Blueprint §18.3, §27.3 & Issue #24).

-- 1. System Recovery Controls (System-level admission, dispatch, and recovery mode state)
CREATE TABLE IF NOT EXISTS system_recovery_controls (
    id INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    mode TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (mode IN ('ACTIVE', 'READ_ONLY', 'DRAINING', 'DISASTER_RECOVERY', 'RESUMING')),
    admission_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    dispatch_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    schedules_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO system_recovery_controls (id, mode, admission_enabled, dispatch_enabled, schedules_enabled)
VALUES (1, 'ACTIVE', TRUE, TRUE, TRUE)
ON CONFLICT (id) DO NOTHING;

-- 2. Disaster Recovery Incidents (Audit of recovery points, uncertainty windows, RPO gaps, and hold counts)
CREATE TABLE IF NOT EXISTS disaster_recovery_incidents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    recovery_point TIMESTAMPTZ NOT NULL,
    incident_at TIMESTAMPTZ NOT NULL,
    uncertainty_window_start TIMESTAMPTZ NOT NULL,
    uncertainty_window_end TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'READ_ONLY' CHECK (status IN ('READ_ONLY', 'RESUMING', 'COMPLETED', 'ACTIVE')),
    restored_runs_count INT NOT NULL DEFAULT 0,
    revoked_worker_sessions_count INT NOT NULL DEFAULT 0,
    revoked_auth_sessions_count INT NOT NULL DEFAULT 0,
    rpo_gap_requests JSONB NOT NULL DEFAULT '[]',
    operator_notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- 3. Deletion Ledger (Blueprint §18.3: Restored backups must reapply deletion ledger before customer access opens)
CREATE TABLE IF NOT EXISTS deletion_ledger (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    resource_type TEXT NOT NULL CHECK (resource_type IN ('ORGANIZATION', 'PROJECT', 'ENVIRONMENT', 'ARTIFACT')),
    resource_id UUID NOT NULL,
    requested_by UUID,
    purge_status TEXT NOT NULL DEFAULT 'PENDING' CHECK (purge_status IN ('PENDING', 'PURGED')),
    purge_due_at TIMESTAMPTZ NOT NULL,
    applied_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

ALTER TABLE deletion_ledger ENABLE ROW LEVEL SECURITY;
ALTER TABLE deletion_ledger FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_deletion_ledger ON deletion_ledger
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

CREATE INDEX IF NOT EXISTS idx_deletion_ledger_status ON deletion_ledger (purge_status, purge_due_at);

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION app.count_pending_deletions()
RETURNS INT
SECURITY DEFINER
SET search_path = app, public, pg_temp
LANGUAGE sql
AS $$
    SELECT count(*)::int FROM deletion_ledger WHERE purge_status = 'PENDING';
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION app.count_pending_deletions() FROM PUBLIC;

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION app.enumerate_recovery_tenants()
RETURNS TABLE (
    organization_id UUID
)
SECURITY DEFINER
SET search_path = app, public, pg_temp
LANGUAGE sql
AS $$
    SELECT DISTINCT organization_id FROM (
        SELECT organization_id FROM runs
        WHERE status IN ('QUEUED', 'RUNNING', 'WAITING', 'PAUSING', 'PAUSED', 'CANCELLING')
        UNION
        SELECT organization_id FROM worker_sessions
        WHERE revoked_at IS NULL
    ) active_tenants;
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION app.enumerate_recovery_tenants() FROM PUBLIC;

-- 4. Permissions
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'deadbolt_runtime') THEN
        GRANT SELECT, INSERT, UPDATE ON system_recovery_controls TO deadbolt_runtime;
        GRANT SELECT, INSERT, UPDATE ON disaster_recovery_incidents TO deadbolt_runtime;
        GRANT SELECT, INSERT, UPDATE, DELETE ON deletion_ledger TO deadbolt_runtime;
        GRANT EXECUTE ON FUNCTION app.enumerate_scheduler_tenants() TO deadbolt_runtime;
        GRANT EXECUTE ON FUNCTION app.count_pending_deletions() TO deadbolt_runtime;
        GRANT EXECUTE ON FUNCTION app.enumerate_recovery_tenants() TO deadbolt_runtime;
    END IF;
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'deadbolt_system') THEN
        GRANT SELECT ON system_recovery_controls TO deadbolt_system;
        GRANT SELECT ON disaster_recovery_incidents TO deadbolt_system;
        GRANT SELECT ON deletion_ledger TO deadbolt_system;
        GRANT EXECUTE ON FUNCTION app.count_pending_deletions() TO deadbolt_system;
        GRANT EXECUTE ON FUNCTION app.enumerate_recovery_tenants() TO deadbolt_system;
    END IF;
END $$;
-- +goose StatementEnd

-- +goose Down
DROP TABLE IF EXISTS deletion_ledger CASCADE;
DROP TABLE IF EXISTS disaster_recovery_incidents CASCADE;
DROP TABLE IF EXISTS system_recovery_controls CASCADE;
