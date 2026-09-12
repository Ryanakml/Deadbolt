#!/usr/bin/env bash
# scripts/init-db-roles.sh
# Deterministically creates dedicated database roles with configured passwords on a fresh cluster.
# Blueprint §24.3 (Database enforcement) & §26.3 (Migration policy)

set -euo pipefail

: "${DEADBOLT_MIGRATOR_PASSWORD:?Required DEADBOLT_MIGRATOR_PASSWORD}"
: "${DEADBOLT_RUNTIME_PASSWORD:?Required DEADBOLT_RUNTIME_PASSWORD}"
: "${DEADBOLT_SYSTEM_PASSWORD:?Required DEADBOLT_SYSTEM_PASSWORD}"

MIGRATOR_PASS="$DEADBOLT_MIGRATOR_PASSWORD"
RUNTIME_PASS="$DEADBOLT_RUNTIME_PASSWORD"
SYSTEM_PASS="$DEADBOLT_SYSTEM_PASSWORD"

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
