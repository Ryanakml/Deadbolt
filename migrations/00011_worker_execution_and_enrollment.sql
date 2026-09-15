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

-- 3. Worker Security Definer Discovery Functions
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION app.consume_enrollment_token(p_token_hash TEXT)
RETURNS TABLE (
    id UUID,
    organization_id UUID,
    environment_id UUID,
    pool_name TEXT
)
SECURITY DEFINER
SET search_path = app, public, pg_temp
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    UPDATE worker_enrollments
    SET used_at = clock_timestamp()
    WHERE token_hash = p_token_hash AND used_at IS NULL AND expires_at > clock_timestamp()
    RETURNING worker_enrollments.id, worker_enrollments.organization_id, worker_enrollments.environment_id, worker_enrollments.pool_name;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION app.authenticate_worker_session(p_token_hash TEXT)
RETURNS TABLE (
    session_id UUID,
    worker_id UUID,
    organization_id UUID,
    environment_id UUID,
    pool_name TEXT,
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    worker_status TEXT
)
SECURITY DEFINER
SET search_path = app, public, pg_temp
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        ws.id AS session_id,
        ws.worker_id,
        ws.organization_id,
        ws.environment_id,
        w.pool_name,
        ws.expires_at,
        ws.revoked_at,
        w.status AS worker_status
    FROM worker_sessions ws
    JOIN workers w ON w.id = ws.worker_id AND w.organization_id = ws.organization_id
    WHERE ws.session_token_hash = p_token_hash;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION app.lookup_worker_for_session(p_worker_id TEXT)
RETURNS TABLE (
    worker_id UUID,
    organization_id UUID,
    environment_id UUID,
    public_key TEXT,
    status TEXT
)
SECURITY DEFINER
SET search_path = app, public, pg_temp
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        w.id AS worker_id,
        w.organization_id,
        w.environment_id,
        w.public_key,
        w.status
    FROM workers w
    WHERE w.id = p_worker_id::uuid;
END;
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION app.consume_enrollment_token(TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION app.authenticate_worker_session(TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION app.lookup_worker_for_session(TEXT) FROM PUBLIC;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'deadbolt_runtime') THEN
        GRANT EXECUTE ON FUNCTION app.consume_enrollment_token(TEXT) TO deadbolt_runtime;
        GRANT EXECUTE ON FUNCTION app.authenticate_worker_session(TEXT) TO deadbolt_runtime;
        GRANT EXECUTE ON FUNCTION app.lookup_worker_for_session(TEXT) TO deadbolt_runtime;
    END IF;
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'deadbolt_system') THEN
        REVOKE EXECUTE ON FUNCTION app.consume_enrollment_token(TEXT) FROM deadbolt_system;
        REVOKE EXECUTE ON FUNCTION app.authenticate_worker_session(TEXT) FROM deadbolt_system;
        REVOKE EXECUTE ON FUNCTION app.lookup_worker_for_session(TEXT) FROM deadbolt_system;
    END IF;
END $$;
-- +goose StatementEnd

-- +goose Down
DROP FUNCTION IF EXISTS app.lookup_worker_for_session(TEXT) CASCADE;
DROP FUNCTION IF EXISTS app.authenticate_worker_session(TEXT) CASCADE;
DROP FUNCTION IF EXISTS app.consume_enrollment_token(TEXT) CASCADE;
DROP TABLE IF EXISTS worker_challenges;
DROP TABLE IF EXISTS worker_enrollments CASCADE;
