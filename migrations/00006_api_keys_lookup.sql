-- +goose Up
-- Restricted API Key Lookup Function (Blueprint §24.3, §24.4)
-- Narrowly scoped function with fixed search_path = app, public, pg_temp, SECURITY DEFINER.
-- +goose StatementBegin
DROP FUNCTION IF EXISTS app.authenticate_api_key(TEXT);
CREATE OR REPLACE FUNCTION app.authenticate_api_key(p_prefix TEXT)
RETURNS TABLE (
    id UUID,
    organization_id UUID,
    environment_id UUID,
    environment_name TEXT,
    prefix TEXT,
    hashed_secret TEXT,
    capabilities TEXT[],
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ
)
SECURITY DEFINER
SET search_path = app, public, pg_temp
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        ak.id,
        ak.organization_id,
        ak.environment_id,
        e.name AS environment_name,
        ak.prefix,
        ak.hashed_secret,
        ak.capabilities,
        ak.expires_at,
        ak.revoked_at,
        ak.created_at,
        ak.last_used_at
    FROM api_keys ak
    JOIN environments e ON e.id = ak.environment_id AND e.organization_id = ak.organization_id
    WHERE ak.prefix = p_prefix;
END;
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION app.authenticate_api_key(TEXT) FROM PUBLIC;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'deadbolt_runtime') THEN
        GRANT EXECUTE ON FUNCTION app.authenticate_api_key(TEXT) TO deadbolt_runtime;
    END IF;
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'deadbolt_system') THEN
        REVOKE EXECUTE ON FUNCTION app.authenticate_api_key(TEXT) FROM deadbolt_system;
    END IF;
END $$;
-- +goose StatementEnd

-- +goose Down
DROP FUNCTION IF EXISTS app.authenticate_api_key(TEXT) CASCADE;
