#!/usr/bin/env bash
# scripts/init-db-roles.sh
# Deterministically creates dedicated database roles with configured passwords on a fresh cluster.
# Blueprint §24.3 (Database enforcement) & §26.3 (Migration policy)

set -euo pipefail

: "${DEADBOLT_MIGRATOR_PASSWORD:?Required DEADBOLT_MIGRATOR_PASSWORD}"
: "${DEADBOLT_RUNTIME_PASSWORD:?Required DEADBOLT_RUNTIME_PASSWORD}"
: "${DEADBOLT_SYSTEM_PASSWORD:?Required DEADBOLT_SYSTEM_PASSWORD}"

psql -v ON_ERROR_STOP=1 \
     -v migrator_pass="$DEADBOLT_MIGRATOR_PASSWORD" \
     -v runtime_pass="$DEADBOLT_RUNTIME_PASSWORD" \
     -v system_pass="$DEADBOLT_SYSTEM_PASSWORD" \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'EOSQL'
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'deadbolt_migrator') THEN
        CREATE ROLE deadbolt_migrator WITH LOGIN;
    END IF;

    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'deadbolt_runtime') THEN
        CREATE ROLE deadbolt_runtime WITH LOGIN NOBYPASSRLS NOSUPERUSER;
    END IF;

    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'deadbolt_system') THEN
        CREATE ROLE deadbolt_system WITH LOGIN NOBYPASSRLS NOSUPERUSER;
    END IF;
END $$;

-- Set role passwords using psql's native string literal quoting (:'variable') to prevent SQL injection or escaping flaws
ALTER ROLE deadbolt_migrator WITH PASSWORD :'migrator_pass';
ALTER ROLE deadbolt_runtime WITH PASSWORD :'runtime_pass';
ALTER ROLE deadbolt_system WITH PASSWORD :'system_pass';
EOSQL

echo "[DEADBOLT_INIT] Dedicated roles (deadbolt_migrator, deadbolt_runtime, deadbolt_system) initialized successfully."
