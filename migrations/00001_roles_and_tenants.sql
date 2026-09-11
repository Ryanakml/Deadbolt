-- +goose Up
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE SCHEMA IF NOT EXISTS app;

-- Tenant context function reading transaction-local configuration.
-- Missing context returns NULL, which causes RLS policies to evaluate to false (fail closed).
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION app.current_organization_id() RETURNS uuid AS $$
BEGIN
    RETURN NULLIF(current_setting('app.current_organization_id', true), '')::uuid;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER;
-- +goose StatementEnd

-- Restricted Discovery Functions (Blueprint §24.3)
-- Narrowly scoped functions with fixed search_path = app, public, pg_temp, SECURITY DEFINER.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION app.discover_user_memberships(p_user_id UUID)
RETURNS TABLE (
    organization_id UUID,
    organization_name TEXT,
    role TEXT,
    status TEXT
)
SECURITY DEFINER
SET search_path = app, public, pg_temp
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        om.organization_id,
        o.name AS organization_name,
        om.role,
        om.status
    FROM organization_members om
    JOIN organizations o ON o.id = om.organization_id
    WHERE om.user_id = p_user_id
      AND om.status = 'ACTIVE';
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION app.enumerate_scheduler_tenants()
RETURNS TABLE (
    organization_id UUID
)
SECURITY DEFINER
SET search_path = app, public, pg_temp
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT o.id AS organization_id
    FROM organizations o;
END;
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION app.discover_user_memberships(UUID) FROM PUBLIC;
REVOKE ALL ON FUNCTION app.enumerate_scheduler_tenants() FROM PUBLIC;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'deadbolt_runtime') THEN
        GRANT EXECUTE ON FUNCTION app.current_organization_id() TO deadbolt_runtime;
        GRANT EXECUTE ON FUNCTION app.discover_user_memberships(UUID) TO deadbolt_runtime;
    END IF;
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'deadbolt_system') THEN
        GRANT EXECUTE ON FUNCTION app.enumerate_scheduler_tenants() TO deadbolt_system;
    END IF;
END $$;
-- +goose StatementEnd

-- 1. Organizations
CREATE TABLE organizations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

ALTER TABLE organizations ENABLE ROW LEVEL SECURITY;
ALTER TABLE organizations FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_organizations ON organizations
    FOR ALL
    USING (id = app.current_organization_id())
    WITH CHECK (id = app.current_organization_id());

-- 2. Organization Members
CREATE TABLE organization_members (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    user_id UUID NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('Viewer', 'Developer', 'Operator', 'Admin', 'Owner')),
    status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'SUSPENDED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, user_id)
);

ALTER TABLE organization_members ENABLE ROW LEVEL SECURITY;
ALTER TABLE organization_members FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_organization_members ON organization_members
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- 3. Projects (scoped by organization_id)
CREATE TABLE projects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, id),
    UNIQUE (organization_id, name)
);

ALTER TABLE projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE projects FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_projects ON projects
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- 4. Environments (composite scope with project and org)
CREATE TABLE environments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    project_id UUID NOT NULL,
    name TEXT NOT NULL CHECK (name IN ('development', 'staging', 'production')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, project_id) REFERENCES projects(organization_id, id) ON DELETE CASCADE,
    UNIQUE (organization_id, id),
    UNIQUE (organization_id, project_id, id),
    UNIQUE (project_id, name)
);

ALTER TABLE environments ENABLE ROW LEVEL SECURITY;
ALTER TABLE environments FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_environments ON environments
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- 5. Environment Admission (Concurrency quota lock row per environment)
CREATE TABLE environment_admissions (
    environment_id UUID PRIMARY KEY,
    organization_id UUID NOT NULL,
    max_concurrency INT NOT NULL DEFAULT 10 CHECK (max_concurrency > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, environment_id) REFERENCES environments(organization_id, id) ON DELETE CASCADE
);

ALTER TABLE environment_admissions ENABLE ROW LEVEL SECURITY;
ALTER TABLE environment_admissions FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_environment_admissions ON environment_admissions
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- 6. API Keys
CREATE TABLE api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    environment_id UUID NOT NULL,
    prefix TEXT NOT NULL,
    hashed_secret TEXT NOT NULL,
    capabilities TEXT[] NOT NULL DEFAULT '{}',
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_used_at TIMESTAMPTZ,
    FOREIGN KEY (organization_id, environment_id) REFERENCES environments(organization_id, id) ON DELETE CASCADE,
    UNIQUE (prefix)
);

ALTER TABLE api_keys ENABLE ROW LEVEL SECURITY;
ALTER TABLE api_keys FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_api_keys ON api_keys
    FOR ALL
    USING (organization_id = app.current_organization_id())
    WITH CHECK (organization_id = app.current_organization_id());

-- +goose Down
DROP TABLE IF EXISTS api_keys CASCADE;
DROP TABLE IF EXISTS environment_admissions CASCADE;
DROP TABLE IF EXISTS environments CASCADE;
DROP TABLE IF EXISTS projects CASCADE;
DROP TABLE IF EXISTS organization_members CASCADE;
DROP TABLE IF EXISTS organizations CASCADE;
DROP FUNCTION IF EXISTS app.enumerate_scheduler_tenants() CASCADE;
DROP FUNCTION IF EXISTS app.discover_user_memberships(UUID) CASCADE;
DROP FUNCTION IF EXISTS app.current_organization_id() CASCADE;
