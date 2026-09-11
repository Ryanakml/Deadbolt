#!/usr/bin/env bash
# scripts/rollback-staging.sh
# Performs an explicit, on-demand rollback to the previously recorded Deadbolt image release.
#
# Invariants:
# 1. Scope strictly to project `deadbolt-staging`. NEVER touch FlowDesk containers/networks/volumes.
# 2. Database state is preserved; down migrations are NEVER executed.
# 3. Uses blue/green slotting: gates restored instance before switching edge route.

set -euo pipefail

RELEASE_DIR="${RELEASE_DIR:-/opt/deadbolt/releases}"
CURRENT_RELEASE_FILE="${RELEASE_DIR}/current"
PREVIOUS_RELEASE_FILE="${RELEASE_DIR}/previous"
ACTIVE_SLOT_FILE="${RELEASE_DIR}/active_slot"
COMPOSE_FILE="${COMPOSE_FILE:-deploy/compose/docker-compose.staging.yml}"

log() {
  echo "[$(date '+%Y-%m-%dT%H:%M:%SZ')] [ROLLBACK] $*"
}

err() {
  echo "[$(date '+%Y-%m-%dT%H:%M:%SZ')] [ROLLBACK ERROR] $*" >&2
}

log "Starting on-demand staging rollback..."

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
  ./scripts/reload-caddy.sh
fi

# Stop the faulty container in old slot
log "Stopping faulty container in slot: $OLD_SLOT..."
docker compose -p deadbolt-staging -f "$COMPOSE_FILE" stop "control-plane-$OLD_SLOT" || true

# Update release pointers
echo "$RESTORE_SLOT" > "$ACTIVE_SLOT_FILE"
echo "$PREV_IMAGE" > "$CURRENT_RELEASE_FILE"

log "SUCCESS: Rollback complete. Restored image: $PREV_IMAGE on slot $RESTORE_SLOT."
log "INVARIANT PRESERVED: Database state intact. Persistent volumes and migrations preserved."
