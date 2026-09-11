-- Database Identity Bootstrap Script for Deadbolt
-- Blueprint references: §24.3 (Database enforcement), §26.3 (Migration policy)
-- This script must be executed by an administrative/superuser connection to establish
-- the three core database roles and default privilege grants before migrations:
--   1. deadbolt_migrator: DDL migration runner, advisory lock holder, schema creator.
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

-- deadbolt_migrator possesses BYPASSRLS so that its SECURITY DEFINER discovery functions
-- can query user memberships across tenants before a session tenant context is established.
ALTER ROLE deadbolt_migrator BYPASSRLS;

-- 2. Database Connection and Schema Creation Grants
DO $$
BEGIN
    EXECUTE format('GRANT CREATE, CONNECT ON DATABASE %I TO deadbolt_migrator', current_database());
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO deadbolt_runtime, deadbolt_system', current_database());
END $$;

-- 3. Ensure Extensions and Schemas exist
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE SCHEMA IF NOT EXISTS app;

-- 4. Schema Ownership and DDL Permissions
-- Revoke default public schema CREATE privilege from PUBLIC to enforce DDL boundary
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

-- deadbolt_migrator owns or has CREATE on schemas so it can run goose migrations
GRANT ALL ON SCHEMA app TO deadbolt_migrator;
ALTER SCHEMA app OWNER TO deadbolt_migrator;
GRANT CREATE, USAGE ON SCHEMA public TO deadbolt_migrator;
GRANT USAGE ON SCHEMA public, app TO deadbolt_runtime;
GRANT USAGE ON SCHEMA app TO deadbolt_system;

-- 5. Default Privileges for Objects Created by deadbolt_migrator
-- Tables created by deadbolt_migrator automatically get DML grants for deadbolt_runtime
ALTER DEFAULT PRIVILEGES FOR ROLE deadbolt_migrator IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO deadbolt_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE deadbolt_migrator IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO deadbolt_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE deadbolt_migrator IN SCHEMA public
    REVOKE ALL ON TABLES FROM deadbolt_system;
ALTER DEFAULT PRIVILEGES FOR ROLE deadbolt_migrator IN SCHEMA app
    REVOKE ALL ON ROUTINES FROM PUBLIC;

-- Also set default privileges for admin role if tables are created directly
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO deadbolt_runtime;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    REVOKE ALL ON TABLES FROM deadbolt_system;

-- 6. Runtime Role Hardening (DML-Only, Subject to FORCE ROW LEVEL SECURITY)
ALTER ROLE deadbolt_runtime NOBYPASSRLS NOCREATEDB NOCREATEROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO deadbolt_runtime;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO deadbolt_runtime;

-- 7. System Role Hardening (Scheduler Tenant Enumeration Only)
ALTER ROLE deadbolt_system NOBYPASSRLS NOCREATEDB NOCREATEROLE;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM deadbolt_system;

-- 8. Grant function permissions if functions are already defined
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE n.nspname = 'app' AND p.proname = 'current_organization_id') THEN
        REVOKE ALL ON FUNCTION app.current_organization_id() FROM PUBLIC;
        GRANT EXECUTE ON FUNCTION app.current_organization_id() TO deadbolt_runtime;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE n.nspname = 'app' AND p.proname = 'discover_user_memberships') THEN
        REVOKE ALL ON FUNCTION app.discover_user_memberships(UUID) FROM PUBLIC, deadbolt_system;
        GRANT EXECUTE ON FUNCTION app.discover_user_memberships(UUID) TO deadbolt_runtime;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE n.nspname = 'app' AND p.proname = 'enumerate_scheduler_tenants') THEN
        REVOKE ALL ON FUNCTION app.enumerate_scheduler_tenants() FROM PUBLIC, deadbolt_runtime;
        GRANT EXECUTE ON FUNCTION app.enumerate_scheduler_tenants() TO deadbolt_system;
    END IF;
END $$;
