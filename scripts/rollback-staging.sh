#!/usr/bin/env bash
# scripts/rollback-staging.sh
# Performs an explicit, on-demand rollback to the previously recorded Deadbolt image release.
#
# Invariants:
# 1. Scope strictly to project `deadbolt-staging`. NEVER touch FlowDesk containers/networks/volumes.
# 2. Database state is preserved; down migrations are NEVER executed.
# 3. Uses blue/green slotting: gates restored instance before switching edge route.
# 4. Self-contained: loads and validates approved host configuration; zero domain fallbacks.

set -euo pipefail

RELEASE_DIR="${RELEASE_DIR:-/opt/deadbolt/releases}"
CURRENT_RELEASE_FILE="${RELEASE_DIR}/current"
PREVIOUS_RELEASE_FILE="${RELEASE_DIR}/previous"
ACTIVE_SLOT_FILE="${RELEASE_DIR}/active_slot"
COMPOSE_FILE="${COMPOSE_FILE:-deploy/compose/docker-compose.staging.yml}"
DRY_RUN="${DRY_RUN:-false}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
if [[ ! -f "$COMPOSE_FILE" && -f "${REPO_ROOT}/${COMPOSE_FILE}" ]]; then
  COMPOSE_FILE="${REPO_ROOT}/${COMPOSE_FILE}"
fi

log() {
  echo "[$(date '+%Y-%m-%dT%H:%M:%SZ')] [ROLLBACK] $*"
}

err() {
  echo "[$(date '+%Y-%m-%dT%H:%M:%SZ')] [ROLLBACK ERROR] $*" >&2
}

if [[ "$DRY_RUN" == "true" ]]; then
  log "DRY RUN mode: verifying rollback prerequisites and Compose configuration..."
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
  log "DRY RUN passed: Rollback configuration and Compose syntax are valid."
  exit 0
fi

log "Starting on-demand staging rollback..."

# 1. Deterministic host configuration loading
CONFIG_FILE="${DEADBOLT_CONFIG_FILE:-/etc/deadbolt/staging.env}"
if [[ ! -f "$CONFIG_FILE" && -f "/opt/deadbolt/config/staging.env" ]]; then
  CONFIG_FILE="/opt/deadbolt/config/staging.env"
fi

if [[ -f "$CONFIG_FILE" ]]; then
  PERMS=$(stat -c "%a" "$CONFIG_FILE" 2>/dev/null || stat -f "%Op" "$CONFIG_FILE" 2>/dev/null || echo "600")
  if [[ "$PERMS" =~ [4567]$ ]]; then
    err "SECURITY VIOLATION: Configuration file $CONFIG_FILE is world-readable ($PERMS)!"
    err "Remediation: chmod 600 $CONFIG_FILE"
    exit 1
  fi
  log "Loading host configuration from $CONFIG_FILE..."
  set -a
  # shellcheck source=/dev/null
  source "$CONFIG_FILE"
  set +a
fi

# 2. Strict validation of required staging variables (no invented domain or default passwords)
for var in DEADBOLT_STAGING_DOMAIN DATABASE_URL SYSTEM_DATABASE_URL DEADBOLT_DB_ADMIN_PASSWORD DEADBOLT_MIGRATOR_PASSWORD DEADBOLT_RUNTIME_PASSWORD DEADBOLT_SYSTEM_PASSWORD DEADBOLT_OIDC_ISSUER DEADBOLT_OIDC_CLIENT_ID DEADBOLT_OIDC_CLIENT_SECRET DEADBOLT_POSTGRES_IMAGE; do
  if [[ -z "${!var:-}" ]]; then
    err "Required configuration variable $var is missing or empty!"
    err "Configure in $CONFIG_FILE or via environment. Rollback cannot proceed without valid staging configuration."
    exit 1
  fi
done

# 3. Check recorded previous release
if [[ ! -f "$PREVIOUS_RELEASE_FILE" ]]; then
  err "No previous release recorded at $PREVIOUS_RELEASE_FILE. Cannot perform rollback."
  exit 1
fi

PREV_IMAGE=$(cat "$PREVIOUS_RELEASE_FILE" | tr -d '[:space:]')
if [[ -z "$PREV_IMAGE" ]]; then
  err "Previous release record is empty. Cannot perform rollback."
  exit 1
fi

log "Previous image to restore: $PREV_IMAGE"

CURRENT_SLOT="blue"
if [[ -f "$ACTIVE_SLOT_FILE" ]]; then
  CURRENT_SLOT=$(cat "$ACTIVE_SLOT_FILE" | tr -d '[:space:]')
fi

# Determine target rollback slot
if [[ "$CURRENT_SLOT" == "blue" ]]; then
  RESTORE_SLOT="green"
  RESTORE_PORT="8089"
  OLD_SLOT="blue"
else
  RESTORE_SLOT="blue"
  RESTORE_PORT="8088"
  OLD_SLOT="green"
fi

log "Launching rollback instance into slot: $RESTORE_SLOT (port $RESTORE_PORT)..."
if [[ "$RESTORE_SLOT" == "green" ]]; then
  DEADBOLT_IMAGE_GREEN="$PREV_IMAGE" \
  docker compose -p deadbolt-staging -f "$COMPOSE_FILE" --profile slot-green up -d control-plane-green
else
  DEADBOLT_IMAGE_BLUE="$PREV_IMAGE" \
  docker compose -p deadbolt-staging -f "$COMPOSE_FILE" --profile slot-blue up -d control-plane-blue
fi

# Gate readiness on restored instance
log "Waiting for /readyz on restored container (http://127.0.0.1:${RESTORE_PORT}/readyz)..."
READY=false
for i in $(seq 1 30); do
  sleep 2
  STATUS_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:${RESTORE_PORT}/readyz" 2>/dev/null || echo "000")
  if [[ "$STATUS_CODE" == "200" ]]; then
    READY=true
    break
  fi
  log "Waiting for /readyz (attempt $i/30, got HTTP $STATUS_CODE)..."
done

if [[ "$READY" != "true" ]]; then
  err "Restored container failed readiness probe!"
  exit 1
fi

log "Readiness verified on restored container."

# Reload Caddy edge to point to restored port
if [[ -f "scripts/reload-caddy.sh" ]]; then
  log "Switching Caddy edge route to port $RESTORE_PORT..."
  export DEADBOLT_UPSTREAM_PORT="$RESTORE_PORT"
  export DEADBOLT_STAGING_DOMAIN="$DEADBOLT_STAGING_DOMAIN"
  ./scripts/reload-caddy.sh "$RESTORE_PORT"
fi

# Stop the faulty container in old slot
log "Stopping faulty container in slot: $OLD_SLOT..."
docker compose -p deadbolt-staging -f "$COMPOSE_FILE" stop "control-plane-$OLD_SLOT" || true

# Update release pointers
echo "$RESTORE_SLOT" > "$ACTIVE_SLOT_FILE"
echo "$PREV_IMAGE" > "$CURRENT_RELEASE_FILE"

log "SUCCESS: Rollback complete. Restored image: $PREV_IMAGE on slot $RESTORE_SLOT."
log "INVARIANT PRESERVED: Database state intact. Persistent volumes and migrations preserved."
