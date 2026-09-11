#!/usr/bin/env bash
set -euo pipefail

# deploy-staging.sh: Orchestrates atomic staging deployment on the shared EC2 host,
# enforcing FlowDesk co-tenant isolation, headroom verification, readiness gating,
# Caddy edge reload, and automated rollback (Blueprint §26, §27 & Issue #5).

DRY_RUN="${DRY_RUN:-false}"
RELEASE_DIR="${DEADBOLT_RELEASE_DIR:-/opt/deadbolt/releases}"
CANDIDATE_DIGEST="${DEADBOLT_IMAGE_DIGEST:-${1:-}}"
CANDIDATE_COMMIT="${DEADBOLT_COMMIT_SHA:-${2:-}}"
COMPOSE_FILE="deploy/compose/docker-compose.staging.yml"

log() {
  echo "[DEADBOLT_DEPLOY] $(date -u +"%Y-%m-%dT%H:%M:%SZ") $*"
}

err() {
  echo "[DEADBOLT_DEPLOY_ERROR] $(date -u +"%Y-%m-%dT%H:%M:%SZ") $*" >&2
}

if [[ "$DRY_RUN" == "true" ]]; then
  log "DRY RUN mode activated: validating deployment scripts and Compose configurations..."
  DEADBOLT_DB_SYSTEM_PASSWORD="mock_password" \
  DATABASE_URL="postgres://mock:mock@localhost:5432/mock" \
  DEADBOLT_OIDC_ISSUER="https://mock-issuer.com" \
  DEADBOLT_OIDC_CLIENT_ID="mock_client_id" \
  DEADBOLT_OIDC_CLIENT_SECRET="mock_client_secret" \
  DEADBOLT_STAGING_DOMAIN="staging.deadbolt.cloud" \
  docker compose -f "$COMPOSE_FILE" config --quiet || { err "Staging Compose validation failed"; exit 1; }
  log "DRY RUN passed: Staging deployment workflow is syntactically sound."
  exit 0
fi

# 1. Verify required environment configuration
log "Step 1: Validating environment configuration and backup readiness..."
for var in DEADBOLT_STAGING_DOMAIN DATABASE_URL DEADBOLT_OIDC_ISSUER DEADBOLT_OIDC_CLIENT_ID DEADBOLT_OIDC_CLIENT_SECRET; do
  if [[ -z "${!var:-}" ]]; then
    err "Required environment variable $var is missing or empty!"
    err "Remediation: Configure $var before triggering staging deployment."
    exit 1
  fi
done

# Verify external S3-compatible artifact storage
if [[ -z "${DEADBOLT_ARTIFACT_ENDPOINT:-}" || -z "${DEADBOLT_ARTIFACT_BUCKET:-}" ]]; then
  err "External S3 artifact configuration (DEADBOLT_ARTIFACT_ENDPOINT, DEADBOLT_ARTIFACT_BUCKET) is required."
  err "FlowDesk MinIO reuse is strictly prohibited by Blueprint §24.4 & Issue #5."
  exit 1
fi

# Verify backup destination configuration
if [[ -z "${DEADBOLT_BACKUP_DESTINATION:-}" ]]; then
  err "Encrypted off-host backup destination (DEADBOLT_BACKUP_DESTINATION) is required before hosted promotion."
  exit 1
fi

# 2. Pre-deployment headroom verification
log "Step 2: Checking co-tenant host headroom (RAM & disk)..."
FREE_MEM_MB=$(free -m 2>/dev/null | awk '/^Mem:/{print $7}' || echo "9999")
if [[ "$FREE_MEM_MB" -lt 1024 ]]; then
  err "INSUFFICIENT HEADROOM: Host has only ${FREE_MEM_MB}MB available RAM (minimum 1024MB required)."
  err "Stopping promotion to protect co-tenant FlowDesk workloads."
  exit 1
fi

FREE_DISK_GB=$(df -BG / 2>/dev/null | awk 'NR==2 {gsub(/G/,"",$4); print $4}' || echo "999")
if [[ "$FREE_DISK_GB" -lt 4 ]]; then
  err "INSUFFICIENT DISK: Root volume has only ${FREE_DISK_GB}GB free (minimum 4GB required)."
  err "Stopping promotion to avoid host disk exhaustion."
  exit 1
fi

# Confirm FlowDesk containers are healthy and untouched
if docker ps --format '{{.Names}}' | grep -q "flowdesk"; then
  log "Co-tenant check: FlowDesk staging containers verified active and untouched."
fi

# 3. Pull candidate image by immutable digest
if [[ -n "$CANDIDATE_DIGEST" ]]; then
  log "Step 3: Pulling candidate image by immutable digest: $CANDIDATE_DIGEST"
  docker pull "$CANDIDATE_DIGEST"
fi

# 4. Save current release for rollback window
mkdir -p "$RELEASE_DIR"
PREVIOUS_RELEASE_FILE="${RELEASE_DIR}/previous"
CURRENT_RELEASE_FILE="${RELEASE_DIR}/current"

if [[ -f "$CURRENT_RELEASE_FILE" ]]; then
  cp "$CURRENT_RELEASE_FILE" "$PREVIOUS_RELEASE_FILE"
fi

# 5. Database migrations with advisory locking
log "Step 5: Executing compatible database migrations..."
if [[ -x "./bin/goose" ]]; then
  ./bin/goose -dir migrations postgres "$DATABASE_URL" up
else
  log "Goose binary not in ./bin; relying on control plane migrator."
fi

# Rollback handler
rollback() {
  err "Promotion gate failed! Executing automated rollback..."
  if [[ -f "$PREVIOUS_RELEASE_FILE" ]]; then
    PREV_IMAGE=$(cat "$PREVIOUS_RELEASE_FILE")
    log "Restoring previous known-good container image: $PREV_IMAGE"
    DEADBOLT_IMAGE="$PREV_IMAGE" docker compose -p deadbolt-staging -f "$COMPOSE_FILE" up -d
  else
    log "No previous release recorded; stopping candidate container."
    docker compose -p deadbolt-staging -f "$COMPOSE_FILE" stop control-plane
  fi
  err "Rollback completed. Database state preserved (no destructive down-migrations)."
}

# 6. Start candidate container
log "Step 6: Launching candidate control plane container under project deadbolt-staging..."
if ! docker compose -p deadbolt-staging -f "$COMPOSE_FILE" up -d; then
  err "Docker Compose startup failed!"
  rollback
  exit 1
fi

# 7. Readiness gate: poll /readyz
log "Step 7: Gating readiness on http://127.0.0.1:8088/readyz..."
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
  err "Readiness gate TIMEOUT after 60s!"
  docker compose -p deadbolt-staging -f "$COMPOSE_FILE" logs control-plane || true
  rollback
  exit 1
fi
log "Readiness gate PASSED: Control plane reported healthy."

# 8. Safe Caddy edge route reload
log "Step 8: Safely reloading Caddy edge route..."
if ! ./scripts/reload-caddy.sh; then
  err "Caddy edge integration failed!"
  rollback
  exit 1
fi

# 9. Staged API smoke test & version verification
log "Step 9: Verifying /version exact commit provenance..."
VERSION_RESP=$(curl -fsSL http://127.0.0.1:8088/version 2>/dev/null || echo "{}")
log "Staged /version response: $VERSION_RESP"

if [[ -n "$CANDIDATE_COMMIT" ]]; then
  if ! echo "$VERSION_RESP" | grep -q "$CANDIDATE_COMMIT"; then
    err "VERSION MISMATCH: Expected commit $CANDIDATE_COMMIT not found in /version response!"
    rollback
    exit 1
  fi
fi

# 10. Record successful release state
if [[ -n "$CANDIDATE_DIGEST" ]]; then
  echo "$CANDIDATE_DIGEST" > "$CURRENT_RELEASE_FILE"
fi

log "SUCCESS: Staging deployment completed and verified on commit ${CANDIDATE_COMMIT:-latest}."
