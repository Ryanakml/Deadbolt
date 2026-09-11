-- +goose Up
-- 1. Users (human identities across the platform)
-- Blueprint §18.1: "Human identities; unique issuer + subject; email is not treated as a permanent identifier"
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT,
    name TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- 2. OIDC Identities (managed OIDC identity provider link)
-- Blueprint §24.1: "Hosted deployment uses one managed OIDC provider with authorization code + PKCE"
CREATE TABLE oidc_identities (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    issuer TEXT NOT NULL,
    subject TEXT NOT NULL,
    email TEXT,
    raw_claims JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (issuer, subject)
);

CREATE INDEX idx_oidc_identities_user_id ON oidc_identities(user_id);

-- 3. Auth Sessions (Go BFF session state, DB-backed revocation and rotation)
-- Blueprint §24.1: "12-hour idle expiry and 7-day absolute expiry... session ID rotates after login/privilege change; logout/revocation is enforced in the DB"
CREATE TABLE auth_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_token_hash TEXT NOT NULL UNIQUE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    active_organization_id UUID REFERENCES organizations(id) ON DELETE SET NULL,
    csrf_token_hash TEXT NOT NULL,
    idle_expires_at TIMESTAMPTZ NOT NULL,
    absolute_expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    revocation_reason TEXT,
    ip_address TEXT,
    user_agent TEXT,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX idx_auth_sessions_user_id ON auth_sessions(user_id);
CREATE INDEX idx_auth_sessions_active ON auth_sessions(session_token_hash) WHERE revoked_at IS NULL;

-- 4. Role Permissions
-- deadbolt_runtime needs DML on users, oidc_identities, and auth_sessions to authenticate and manage sessions.
-- deadbolt_system has NO access to these tables.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'deadbolt_runtime') THEN
        GRANT SELECT, INSERT, UPDATE, DELETE ON users TO deadbolt_runtime;
        GRANT SELECT, INSERT, UPDATE, DELETE ON oidc_identities TO deadbolt_runtime;
        GRANT SELECT, INSERT, UPDATE, DELETE ON auth_sessions TO deadbolt_runtime;
    END IF;
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'deadbolt_system') THEN
        REVOKE ALL ON users FROM deadbolt_system;
        REVOKE ALL ON oidc_identities FROM deadbolt_system;
        REVOKE ALL ON auth_sessions FROM deadbolt_system;
    END IF;
END $$;
-- +goose StatementEnd

-- +goose Down
DROP TABLE IF EXISTS auth_sessions CASCADE;
DROP TABLE IF EXISTS oidc_identities CASCADE;
DROP TABLE IF EXISTS users CASCADE;
