#!/usr/bin/env bash
# scripts/init-db-roles.sh
# Deterministically creates dedicated database roles with configured passwords on a fresh cluster.
# Blueprint §24.3 (Database enforcement) & §26.3 (Migration policy)

set -euo pipefail

MIGRATOR_PASS="${DEADBOLT_MIGRATOR_PASSWORD:-migrator_secure_pass}"
RUNTIME_PASS="${DEADBOLT_RUNTIME_PASSWORD:-runtime_secure_pass}"
SYSTEM_PASS="${DEADBOLT_SYSTEM_PASSWORD:-system_secure_pass}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<EOSQL
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'deadbolt_migrator') THEN
        CREATE ROLE deadbolt_migrator WITH LOGIN PASSWORD '$MIGRATOR_PASS';
    ELSE
        ALTER ROLE deadbolt_migrator WITH PASSWORD '$MIGRATOR_PASS';
    END IF;

    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'deadbolt_runtime') THEN
        CREATE ROLE deadbolt_runtime WITH LOGIN NOBYPASSRLS NOSUPERUSER PASSWORD '$RUNTIME_PASS';
    ELSE
        ALTER ROLE deadbolt_runtime WITH PASSWORD '$RUNTIME_PASS';
    END IF;

    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'deadbolt_system') THEN
        CREATE ROLE deadbolt_system WITH LOGIN NOBYPASSRLS NOSUPERUSER PASSWORD '$SYSTEM_PASS';
    ELSE
        ALTER ROLE deadbolt_system WITH PASSWORD '$SYSTEM_PASS';
    END IF;
END \$\$;
EOSQL

echo "[DEADBOLT_INIT] Dedicated roles (deadbolt_migrator, deadbolt_runtime, deadbolt_system) initialized successfully."
