#!/usr/bin/env bash
# scripts/rollback-staging.sh
# Performs an explicit, on-demand rollback to the previously recorded Deadbolt image release.
#
# Invariants:
# 1. Scope strictly to project `deadbolt-staging`. NEVER touch FlowDesk containers/networks/volumes.
# 2. Database state is preserved; down migrations are NEVER executed.
# 3. Restores previous Caddy edge configuration if necessary.

set -euo pipefail

RELEASE_DIR="${RELEASE_DIR:-/opt/deadbolt/releases}"
CURRENT_RELEASE_FILE="${RELEASE_DIR}/current"
PREVIOUS_RELEASE_FILE="${RELEASE_DIR}/previous"
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

export DEADBOLT_IMAGE="$PREV_IMAGE"

# Restart candidate container with previous image
log "Relaunching control-plane with previous image..."
docker compose -p deadbolt-staging -f "$COMPOSE_FILE" up -d control-plane

# Gate readiness
log "Waiting for /readyz on restored container..."
READY=false
for i in $(seq 1 30); do
  sleep 2
  STATUS_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8088/readyz 2>/dev/null || echo "000")
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

# Reload Caddy edge
if [[ -f "scripts/reload-caddy.sh" ]]; then
  log "Reloading Caddy edge..."
  ./scripts/reload-caddy.sh
fi

# Update release pointers: swap current to restored image
echo "$PREV_IMAGE" > "$CURRENT_RELEASE_FILE"

log "SUCCESS: Rollback complete. Restored image: $PREV_IMAGE"
log "INVARIANT PRESERVED: Database state intact. Persistent volumes and migrations preserved."
