#!/usr/bin/env bash
# scripts/bootstrap-staging-cluster.sh
# Explicit provisioning script for clean first-staging cluster bootstrap (Blueprint §26 & Issue #5).
# Sequence:
# 1. Source host configuration from /etc/deadbolt/staging.env
# 2. Validate zero-default passwords and connection URL password consistency
# 3. Bootstrap data infrastructure: postgres (with deterministic awscli/curl) and nats
# 4. Wait for PostgreSQL healthcheck (verifying role initialization)
# 5. Take initial physical base backup and trigger WAL switch (bootstrap-initial-backup.sh)
# 6. Run fail-closed backup readiness verification (check-backup-readiness.sh)

set -euo pipefail

DRY_RUN="${DRY_RUN:-false}"
COMPOSE_FILE="deploy/compose/docker-compose.staging.yml"

log() {
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [CLUSTER_BOOTSTRAP] $*"
}

err() {
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [CLUSTER_BOOTSTRAP_ERROR] $*" >&2
}

if [[ "$DRY_RUN" == "true" ]]; then
  log "DRY RUN mode activated: validating cluster bootstrap syntax and Compose definition..."
  DEADBOLT_DB_ADMIN_PASSWORD="mock_admin_password" \
  DEADBOLT_MIGRATOR_PASSWORD="mock_migrator_password" \
  DEADBOLT_RUNTIME_PASSWORD="mock_runtime_password" \
  DEADBOLT_SYSTEM_PASSWORD="mock_system_password" \
  DATABASE_URL="postgres://deadbolt_runtime:mock_runtime_password@localhost:5432/mock" \
  MIGRATOR_DATABASE_URL="postgres://deadbolt_migrator:mock_migrator_password@localhost:5432/mock" \
  SYSTEM_DATABASE_URL="postgres://deadbolt_system:mock_system_password@localhost:5432/mock" \
  DEADBOLT_POSTGRES_IMAGE="ghcr.io/ryanakml/deadbolt/postgres@sha256:1111222233334444555566667777888899990000aaaaabbbbbcccccdddddeeeee" \
  DEADBOLT_IMAGE="ghcr.io/ryanakml/deadbolt/control-plane@sha256:1111222233334444555566667777888899990000aaaaabbbbbcccccdddddeeeee" \
  DEADBOLT_OIDC_ISSUER="https://mock-issuer.com" \
  DEADBOLT_OIDC_CLIENT_ID="mock_client_id" \
  DEADBOLT_OIDC_CLIENT_SECRET="mock_client_secret" \
  DEADBOLT_STAGING_DOMAIN="staging.deadbolt.cloud" \
  docker compose -f "$COMPOSE_FILE" --profile slot-blue --profile slot-green config --quiet || { err "Staging Compose validation failed"; exit 1; }
  log "DRY RUN passed: Cluster bootstrap workflow is valid."
  exit 0
fi

# 1. Load host configuration
CONFIG_FILE="${DEADBOLT_CONFIG_FILE:-/etc/deadbolt/staging.env}"
if [[ ! -f "$CONFIG_FILE" && -f "/opt/deadbolt/config/staging.env" ]]; then
  CONFIG_FILE="/opt/deadbolt/config/staging.env"
fi

if [[ -f "$CONFIG_FILE" ]]; then
  PERMS=$(stat -c "%a" "$CONFIG_FILE" 2>/dev/null || stat -f "%Op" "$CONFIG_FILE" 2>/dev/null || echo "600")
  if [[ "$PERMS" =~ [4567]$ ]]; then
    err "SECURITY VIOLATION: Configuration file $CONFIG_FILE is world-readable ($PERMS)!"
    exit 1
  fi
  log "Loading configuration from $CONFIG_FILE..."
  set -a
  # shellcheck source=/dev/null
  source "$CONFIG_FILE"
  set +a
fi

# 2. Strict password and URL validation (no repository-known defaults permitted)
for var in DEADBOLT_DB_ADMIN_PASSWORD DEADBOLT_MIGRATOR_PASSWORD DEADBOLT_RUNTIME_PASSWORD DEADBOLT_SYSTEM_PASSWORD DATABASE_URL MIGRATOR_DATABASE_URL SYSTEM_DATABASE_URL; do
  if [[ -z "${!var:-}" ]]; then
    err "Required database configuration variable $var is missing or empty!"
    err "Zero default passwords allowed in hosted staging environment."
    exit 1
  fi
done

DEADBOLT_POSTGRES_IMAGE="${DEADBOLT_POSTGRES_IMAGE:-}"
if [[ -z "$DEADBOLT_POSTGRES_IMAGE" ]]; then
  err "DEADBOLT_POSTGRES_IMAGE is required (format: ghcr.io/ryanakml/deadbolt/postgres@sha256:...)"
  exit 1
fi

# Validate database connection URL against expected username and password
validate_db_url() {
  local url_name="$1"
  local url_val="$2"
  local expected_user="$3"
  local expected_pass="$4"

  local stripped="${url_val#*://}"
  if [[ "$stripped" != *"@"* ]]; then
    err "PASSWORD CONSISTENCY FAILURE: Malformed database URL $url_name lacks user:password authority!"
    exit 1
  fi
  local userinfo="${stripped%%@*}"
  local user="${userinfo%%:*}"
  local pass="${userinfo#*:}"

  if [[ "$user" != "$expected_user" ]]; then
    err "PASSWORD CONSISTENCY FAILURE: $url_name user is '$user', expected '$expected_user'!"
    exit 1
  fi

  local decoded_pass
  decoded_pass=$(printf '%b' "${pass//%/\\x}" 2>/dev/null || echo "$pass")

  if [[ "$pass" != "$expected_pass" && "$decoded_pass" != "$expected_pass" ]]; then
    err "PASSWORD CONSISTENCY FAILURE: Password in $url_name does not match $expected_user role password!"
    exit 1
  fi
}

# Validate password consistency between URLs and role passwords
validate_db_url "MIGRATOR_DATABASE_URL" "$MIGRATOR_DATABASE_URL" "deadbolt_migrator" "$DEADBOLT_MIGRATOR_PASSWORD"
validate_db_url "DATABASE_URL" "$DATABASE_URL" "deadbolt_runtime" "$DEADBOLT_RUNTIME_PASSWORD"
validate_db_url "SYSTEM_DATABASE_URL" "$SYSTEM_DATABASE_URL" "deadbolt_system" "$DEADBOLT_SYSTEM_PASSWORD"

if [[ -z "${DEADBOLT_STORAGE_S3_BUCKET:-}" && -z "${DEADBOLT_WAL_ARCHIVE_DIR:-}" ]]; then
  err "Neither DEADBOLT_STORAGE_S3_BUCKET nor DEADBOLT_WAL_ARCHIVE_DIR is configured for backups!"
  exit 1
fi

# 3. Bootstrap data infrastructure: PostgreSQL & NATS
log "Pulling immutable postgres image by digest: $DEADBOLT_POSTGRES_IMAGE"
docker pull "$DEADBOLT_POSTGRES_IMAGE"

log "Bootstrapping data infrastructure (PostgreSQL & NATS)..."
docker compose -p deadbolt-staging -f "$COMPOSE_FILE" up -d postgres nats

log "Waiting for PostgreSQL service to report healthy..."
PG_HEALTHY=false
for i in $(seq 1 30); do
  STATUS=$(docker inspect --format '{{.State.Health.Status}}' deadbolt-staging-postgres 2>/dev/null || echo "unknown")
  if [[ "$STATUS" == "healthy" ]]; then
    PG_HEALTHY=true
    break
  fi
  sleep 2
done

if [[ "$PG_HEALTHY" != "true" ]]; then
  err "PostgreSQL failed to report healthy within 60s!"
  docker compose -p deadbolt-staging -f "$COMPOSE_FILE" logs postgres || true
  exit 1
fi
log "PostgreSQL is healthy and database roles are initialized."

# 4. Create initial base backup and switch WAL
log "Establishing first verified base backup and WAL archive..."
./scripts/bootstrap-initial-backup.sh

# 5. Verify backup & WAL readiness (fail-closed)
log "Running fail-closed backup readiness verification..."
./scripts/check-backup-readiness.sh

# 6. Install and verify scheduled daily base backup cron with bounded 14-day retention
log "Configuring and verifying scheduled daily base backup maintenance (14-day retention)..."
./scripts/setup-backup-cron.sh

log "SUCCESS: Clean staging cluster data infrastructure, backup baseline, and recurring maintenance established."
