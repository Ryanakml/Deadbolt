-- Database Identity Bootstrap Script for Deadbolt
-- Blueprint references: §24.3 (Database enforcement), §26.3 (Migration policy)
-- This script establishes the three core database roles with strict privilege separation:
--   1. deadbolt_migrator: DDL migration runner, advisory lock holder.
--   2. deadbolt_runtime: DML-only application role, subject to RLS (NO BYPASSRLS).
--   3. deadbolt_system: Minimal system identity for scheduler tenant enumeration only.

-- 1. Create Roles if they do not exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'deadbolt_migrator') THEN
        CREATE ROLE deadbolt_migrator WITH LOGIN;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'deadbolt_runtime') THEN
        CREATE ROLE deadbolt_runtime WITH LOGIN;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'deadbolt_system') THEN
        CREATE ROLE deadbolt_system WITH LOGIN;
    END IF;
END $$;

-- 2. Schema USAGE Permissions
CREATE SCHEMA IF NOT EXISTS app;
GRANT USAGE ON SCHEMA public, app TO deadbolt_migrator, deadbolt_runtime;
GRANT USAGE ON SCHEMA app TO deadbolt_system;

-- 3. Runtime Role (DML-Only, Subject to FORCE ROW LEVEL SECURITY)
ALTER ROLE deadbolt_runtime NOBYPASSRLS NOCREATEDB NOCREATEROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO deadbolt_runtime;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO deadbolt_runtime;

-- 4. System Role (Scheduler Tenant Enumeration Only)
ALTER ROLE deadbolt_system NOBYPASSRLS NOCREATEDB NOCREATEROLE;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM deadbolt_system;
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON TABLES FROM deadbolt_system;

-- 5. Grant function permissions if functions are already defined
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE n.nspname = 'app' AND p.proname = 'current_organization_id') THEN
        GRANT EXECUTE ON FUNCTION app.current_organization_id() TO deadbolt_runtime;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE n.nspname = 'app' AND p.proname = 'discover_user_memberships') THEN
        GRANT EXECUTE ON FUNCTION app.discover_user_memberships(UUID) TO deadbolt_runtime;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE n.nspname = 'app' AND p.proname = 'enumerate_scheduler_tenants') THEN
        GRANT EXECUTE ON FUNCTION app.enumerate_scheduler_tenants() TO deadbolt_system;
    END IF;
END $$;
