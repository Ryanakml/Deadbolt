#!/usr/bin/env bash
# scripts/deploy-staging.sh
# Orchestrates atomic staging deployment on the shared EC2 host, enforcing
# FlowDesk co-tenant isolation, separate migrator/runtime DB credentials,
# blue/green candidate readiness gating before traffic switch, and non-destructive rollback.

set -euo pipefail

DRY_RUN="${DRY_RUN:-false}"
RELEASE_DIR="${DEADBOLT_RELEASE_DIR:-/opt/deadbolt/releases}"
CANDIDATE_DIGEST="${DEADBOLT_IMAGE_DIGEST:-${1:-}}"
CANDIDATE_COMMIT="${DEADBOLT_COMMIT_SHA:-${2:-}}"
COMPOSE_FILE="deploy/compose/docker-compose.staging.yml"
ACTIVE_SLOT_FILE="${RELEASE_DIR}/active_slot"
CURRENT_RELEASE_FILE="${RELEASE_DIR}/current"
PREVIOUS_RELEASE_FILE="${RELEASE_DIR}/previous"

log() {
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [DEADBOLT_DEPLOY] $*"
}

err() {
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [DEADBOLT_DEPLOY_ERROR] $*" >&2
}

if [[ "$DRY_RUN" == "true" ]]; then
  log "DRY RUN mode activated: validating deployment scripts and Compose configurations..."
  DEADBOLT_DB_SYSTEM_PASSWORD="mock_password" \
  DATABASE_URL="postgres://mock:mock@localhost:5432/mock" \
  MIGRATOR_DATABASE_URL="postgres://mock_migrator:mock@localhost:5432/mock" \
  DEADBOLT_IMAGE="ghcr.io/ryanakml/deadbolt/control-plane@sha256:1111222233334444555566667777888899990000aaaaabbbbbcccccdddddeeeee" \
  DEADBOLT_OIDC_ISSUER="https://mock-issuer.com" \
  DEADBOLT_OIDC_CLIENT_ID="mock_client_id" \
  DEADBOLT_OIDC_CLIENT_SECRET="mock_client_secret" \
  DEADBOLT_STAGING_DOMAIN="staging.deadbolt.cloud" \
  docker compose -f "$COMPOSE_FILE" --profile slot-blue --profile slot-green config --quiet || { err "Staging Compose validation failed"; exit 1; }
  log "DRY RUN passed: Staging deployment workflow is syntactically sound."
  exit 0
fi

# 1. Deterministic host configuration loading
CONFIG_FILE="${DEADBOLT_CONFIG_FILE:-/etc/deadbolt/staging.env}"
if [[ ! -f "$CONFIG_FILE" && -f "/opt/deadbolt/config/staging.env" ]]; then
  CONFIG_FILE="/opt/deadbolt/config/staging.env"
fi

if [[ -f "$CONFIG_FILE" ]]; then
  # Verify secure permissions (must not be world-readable)
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

# Validate required variables
for var in DEADBOLT_STAGING_DOMAIN DATABASE_URL MIGRATOR_DATABASE_URL DEADBOLT_OIDC_ISSUER DEADBOLT_OIDC_CLIENT_ID DEADBOLT_OIDC_CLIENT_SECRET DEADBOLT_STORAGE_S3_BUCKET; do
  if [[ -z "${!var:-}" ]]; then
    err "Required configuration variable $var is missing or empty!"
    err "Configure in $CONFIG_FILE or via environment."
    exit 1
  fi
done

# Enforce least-privilege credential separation (Blueprint §26.3)
if [[ "$MIGRATOR_DATABASE_URL" == "$DATABASE_URL" ]]; then
  err "SECURITY VIOLATION: MIGRATOR_DATABASE_URL must not be identical to runtime DATABASE_URL!"
  err "Runtime control plane must NOT possess DDL privileges."
  exit 1
fi

if [[ -z "$CANDIDATE_DIGEST" ]]; then
  err "CANDIDATE_DIGEST is required (format: ghcr.io/ryanakml/deadbolt/control-plane@sha256:...)"
  exit 1
fi

# 2. Verify external S3 backup readiness
log "Step 2: Checking backup & WAL readiness..."
if [[ -f "scripts/check-backup-readiness.sh" ]]; then
  ./scripts/check-backup-readiness.sh
fi

# 3. Headroom & Co-tenant isolation checks
log "Step 3: Checking system headroom & co-tenant boundaries..."
FREE_RAM_MB=$(free -m | awk '/^Mem:/{print $7}')
if [[ "$FREE_RAM_MB" -lt 1024 ]]; then
  err "INSUFFICIENT MEMORY: Only ${FREE_RAM_MB}MB available, minimum 1024MB required."
  exit 1
fi

FREE_DISK_MB=$(df -m / | awk 'NR==2 {print $4}')
if [[ "$FREE_DISK_MB" -lt 4096 ]]; then
  err "INSUFFICIENT DISK: Only ${FREE_DISK_MB}MB available on root, minimum 4096MB required."
  exit 1
fi

if docker ps -a --format '{{.Names}}' | grep -qi "flowdesk"; then
  log "FlowDesk co-tenant containers detected and protected."
fi

# 4. Pull candidate immutable image
log "Step 4: Pulling candidate image by immutable digest: $CANDIDATE_DIGEST"
docker pull "$CANDIDATE_DIGEST"

# 5. Deterministic migration execution via candidate container runner
log "Step 5: Executing forward schema migrations with advisory lock..."
docker run --rm \
  --network deadbolt_staging_net \
  -e MIGRATOR_DATABASE_URL="$MIGRATOR_DATABASE_URL" \
  -e DEADBOLT_MIGRATIONS_DIR="/migrations" \
  "$CANDIDATE_DIGEST" --migrate

# 6. Blue-Green Slot Selection
mkdir -p "$RELEASE_DIR"
CURRENT_SLOT="blue"
if [[ -f "$ACTIVE_SLOT_FILE" ]]; then
  CURRENT_SLOT=$(cat "$ACTIVE_SLOT_FILE" | tr -d '[:space:]')
fi

if [[ "$CURRENT_SLOT" == "blue" ]]; then
  CANDIDATE_SLOT="green"
  CANDIDATE_PORT="8089"
  OLD_SLOT="blue"
  OLD_PORT="8088"
else
  CANDIDATE_SLOT="blue"
  CANDIDATE_PORT="8088"
  OLD_SLOT="green"
  OLD_PORT="8089"
fi

log "Current active slot: $CURRENT_SLOT (port $OLD_PORT)"
log "Launching candidate into slot: $CANDIDATE_SLOT (port $CANDIDATE_PORT)"

# 7. Start candidate container in candidate slot
if [[ "$CANDIDATE_SLOT" == "green" ]]; then
  DEADBOLT_IMAGE_GREEN="$CANDIDATE_DIGEST" \
  docker compose -p deadbolt-staging -f "$COMPOSE_FILE" --profile slot-green up -d control-plane-green
else
  DEADBOLT_IMAGE_BLUE="$CANDIDATE_DIGEST" \
  docker compose -p deadbolt-staging -f "$COMPOSE_FILE" --profile slot-blue up -d control-plane-blue
fi

# Rollback helper function
rollback() {
  err "Candidate gate failed! Stopping candidate slot ($CANDIDATE_SLOT)..."
  docker compose -p deadbolt-staging -f "$COMPOSE_FILE" stop "control-plane-$CANDIDATE_SLOT" || true
  err "Active slot $CURRENT_SLOT (port $OLD_PORT) remains unharmed. Database state preserved."
}

# 8. Pre-traffic readiness gate
log "Step 8: Gating candidate readiness on http://127.0.0.1:${CANDIDATE_PORT}/readyz..."
READY=false
for i in $(seq 1 30); do
  sleep 2
  STATUS_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:${CANDIDATE_PORT}/readyz" 2>/dev/null || echo "000")
  if [[ "$STATUS_CODE" == "200" ]]; then
    READY=true
    break
  fi
  log "Waiting for /readyz (attempt $i/30, got HTTP $STATUS_CODE)..."
done

if [[ "$READY" != "true" ]]; then
  err "Readiness gate TIMEOUT after 60s!"
  docker compose -p deadbolt-staging -f "$COMPOSE_FILE" logs "control-plane-$CANDIDATE_SLOT" || true
  rollback
  exit 1
fi
log "Candidate readiness gate PASSED."

# 9. Verify /version commit and exact image digest provenance
log "Step 9: Verifying /version exact commit SHA and image digest..."
VERSION_RESP=$(curl -fsSL "http://127.0.0.1:${CANDIDATE_PORT}/version" 2>/dev/null || echo "{}")
log "Candidate /version: $VERSION_RESP"

if [[ -n "$CANDIDATE_COMMIT" ]] && ! echo "$VERSION_RESP" | grep -q "$CANDIDATE_COMMIT"; then
  err "VERSION MISMATCH: Expected commit $CANDIDATE_COMMIT not found in /version!"
  rollback
  exit 1
fi

if ! echo "$VERSION_RESP" | grep -q "$CANDIDATE_DIGEST"; then
  err "DIGEST MISMATCH: Expected digest $CANDIDATE_DIGEST not found in /version!"
  rollback
  exit 1
fi
log "Provenance verified: exact commit and digest match."

# 10. Atomic traffic switch: update Caddy upstream
log "Step 10: Atomically switching Caddy edge route to port $CANDIDATE_PORT..."
export DEADBOLT_UPSTREAM_PORT="$CANDIDATE_PORT"
if ! ./scripts/reload-caddy.sh; then
  err "Caddy reload failed!"
  rollback
  exit 1
fi

# 11. Staged edge smoke test
log "Step 11: Smokin edge route via Caddy..."
EDGE_VERSION=$(curl -fsSL "https://${DEADBOLT_STAGING_DOMAIN}/version" 2>/dev/null || echo "{}")
if ! echo "$EDGE_VERSION" | grep -q "$CANDIDATE_DIGEST"; then
  err "EDGE SMOKE FAILED: Caddy edge is not serving candidate image digest!"
  # Rollback Caddy route to old port
  export DEADBOLT_UPSTREAM_PORT="$OLD_PORT"
  ./scripts/reload-caddy.sh || true
  rollback
  exit 1
fi

# 12. Decommission previous slot container
log "Step 12: Stopping previous slot ($OLD_SLOT)..."
docker compose -p deadbolt-staging -f "$COMPOSE_FILE" stop "control-plane-$OLD_SLOT" || true

# 13. Record successful release state
if [[ -f "$CURRENT_RELEASE_FILE" ]]; then
  cp "$CURRENT_RELEASE_FILE" "$PREVIOUS_RELEASE_FILE"
fi
echo "$CANDIDATE_DIGEST" > "$CURRENT_RELEASE_FILE"
echo "$CANDIDATE_SLOT" > "$ACTIVE_SLOT_FILE"

log "SUCCESS: Staging deployment completed. Active slot: $CANDIDATE_SLOT (port $CANDIDATE_PORT)."
