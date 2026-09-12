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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/config-permissions.sh
source "${SCRIPT_DIR}/lib/config-permissions.sh"
# shellcheck source=lib/edge-smoke.sh
source "${SCRIPT_DIR}/lib/edge-smoke.sh"
# shellcheck source=lib/edge-smoke-rollback.sh
source "${SCRIPT_DIR}/lib/edge-smoke-rollback.sh"

RELEASE_DIR="${RELEASE_DIR:-/opt/deadbolt/releases}"
CURRENT_RELEASE_FILE="${RELEASE_DIR}/current"
PREVIOUS_RELEASE_FILE="${RELEASE_DIR}/previous"
ACTIVE_SLOT_FILE="${RELEASE_DIR}/active_slot"
POSTGRES_IMAGE_FILE="${RELEASE_DIR}/postgres_image"
CALLER_POSTGRES_IMAGE="${DEADBOLT_POSTGRES_IMAGE:-}"
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
  if ! validate_staging_config_permissions "$CONFIG_FILE"; then
    exit 1
  fi
  log "Loading host configuration from $CONFIG_FILE..."
  set -a
  # shellcheck source=/dev/null
  source "$CONFIG_FILE"
  set +a
fi

# Workflow-supplied or environment-supplied PostgreSQL image takes precedence over static host config
if [[ -n "$CALLER_POSTGRES_IMAGE" ]]; then
  DEADBOLT_POSTGRES_IMAGE="$CALLER_POSTGRES_IMAGE"
elif [[ -f "$POSTGRES_IMAGE_FILE" ]]; then
  RECORDED_PG_IMAGE=$(cat "$POSTGRES_IMAGE_FILE" | tr -d '[:space:]')
  if [[ -n "$RECORDED_PG_IMAGE" ]]; then
    DEADBOLT_POSTGRES_IMAGE="$RECORDED_PG_IMAGE"
  fi
elif [[ -f "${RELEASE_DIR}/postgres_image.previous" ]]; then
  RECORDED_PG_IMAGE=$(cat "${RELEASE_DIR}/postgres_image.previous" | tr -d '[:space:]')
  if [[ -n "$RECORDED_PG_IMAGE" ]]; then
    DEADBOLT_POSTGRES_IMAGE="$RECORDED_PG_IMAGE"
  fi
fi

# Fall back to inspecting running postgres container if image is still unset
if [[ -z "${DEADBOLT_POSTGRES_IMAGE:-}" ]]; then
  RUNNING_PG_IMAGE=$(docker inspect --format '{{.Config.Image}}' deadbolt-staging-postgres 2>/dev/null || true)
  if [[ -n "$RUNNING_PG_IMAGE" ]]; then
    DEADBOLT_POSTGRES_IMAGE="$RUNNING_PG_IMAGE"
  fi
fi
export DEADBOLT_POSTGRES_IMAGE

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
if [[ ! "$PREV_IMAGE" =~ ^.+@sha256:[a-f0-9]{64}$ ]]; then
  err "Previous release must be an immutable control-plane reference (@sha256:...)."
  exit 1
fi

# Compose interpolates both slots even when only the restore slot is started.
DEADBOLT_IMAGE="$PREV_IMAGE"
export DEADBOLT_IMAGE

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
  OLD_PORT="8088"
else
  RESTORE_SLOT="blue"
  RESTORE_PORT="8088"
  OLD_SLOT="green"
  OLD_PORT="8089"
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

# Prove local version and independently inspected container identity before traffic moves.
RESTORED_VERSION=$(curl -fsSL "http://127.0.0.1:${RESTORE_PORT}/version" 2>/dev/null || echo "{}")
if [[ "$RESTORED_VERSION" != *"$PREV_IMAGE"* ]]; then
  err "RESTORE VERSION MISMATCH: /version does not contain $PREV_IMAGE."
  exit 1
fi
RESTORE_CONTAINER="deadbolt-staging-control-plane-${RESTORE_SLOT}"
PREV_SHA=$(echo "$PREV_IMAGE" | grep -o 'sha256:[a-f0-9]\{64\}' || true)
RESTORE_IMAGE_ID=$(docker inspect --format '{{.Image}}' "$RESTORE_CONTAINER" 2>/dev/null || true)
RESTORE_IMAGE_DECL=$(docker inspect --format '{{index .Config.Image}}' "$RESTORE_CONTAINER" 2>/dev/null || true)
RESTORE_REPO_DIGESTS=$(docker inspect --format '{{json .RepoDigests}}' "$RESTORE_IMAGE_ID" 2>/dev/null || echo "[]")
if [[ -z "$PREV_SHA" || "$RESTORE_IMAGE_DECL $RESTORE_REPO_DIGESTS $RESTORE_IMAGE_ID" != *"$PREV_SHA"* ]]; then
  err "RESTORE CONTAINER IDENTITY MISMATCH: restored container does not match $PREV_IMAGE."
  exit 1
fi
log "Restored local version and container image identity verified."

# Reload Caddy edge to point to restored port
if [[ -f "scripts/reload-caddy.sh" ]]; then
  log "Switching Caddy edge route to port $RESTORE_PORT..."
  export DEADBOLT_UPSTREAM_PORT="$RESTORE_PORT"
  export DEADBOLT_STAGING_DOMAIN="$DEADBOLT_STAGING_DOMAIN"
  if ! ./scripts/reload-caddy.sh "$RESTORE_PORT"; then
    err "Caddy switch to restored release failed; preserving current slot $OLD_SLOT."
    exit 1
  fi
fi

# Keep the current release alive until HTTPS/TLS edge verification proves the restore.
rollback() {
  err "Rollback edge gate failed; stopping unproven restored slot $RESTORE_SLOT."
  docker compose -p deadbolt-staging -f "$COMPOSE_FILE" stop "control-plane-$RESTORE_SLOT" || true
}
CANDIDATE_SLOT="$RESTORE_SLOT"
CANDIDATE_PORT="$RESTORE_PORT"
if ! wait_for_candidate_edge_smoke "$DEADBOLT_STAGING_DOMAIN" "$PREV_IMAGE"; then
  err "Rollback edge smoke failed; restoring current route before touching current slot."
  if ! restore_edge_route_before_stopping_candidate "$OLD_PORT" true; then
    exit 1
  fi
  exit 1
fi

# Stop the former current container only after public edge proof.
log "Stopping former current container in slot: $OLD_SLOT..."
docker compose -p deadbolt-staging -f "$COMPOSE_FILE" stop "control-plane-$OLD_SLOT" || true

# Update release pointers
if [[ -f "$CURRENT_RELEASE_FILE" ]]; then
  cp "$CURRENT_RELEASE_FILE" "$PREVIOUS_RELEASE_FILE"
fi
echo "$RESTORE_SLOT" > "$ACTIVE_SLOT_FILE"
echo "$PREV_IMAGE" > "$CURRENT_RELEASE_FILE"

log "SUCCESS: Rollback complete. Restored image: $PREV_IMAGE on slot $RESTORE_SLOT."
log "INVARIANT PRESERVED: Database state intact. Persistent volumes and migrations preserved."
