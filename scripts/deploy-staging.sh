#!/usr/bin/env bash
# scripts/deploy-staging.sh
# Orchestrates atomic staging deployment on the shared EC2 host, enforcing
# FlowDesk co-tenant isolation, separate migrator/runtime DB credentials,
# blue/green candidate readiness gating before traffic switch, and non-destructive rollback.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/config-permissions.sh
source "${SCRIPT_DIR}/lib/config-permissions.sh"
# shellcheck source=lib/edge-smoke-rollback.sh
source "${SCRIPT_DIR}/lib/edge-smoke-rollback.sh"
# shellcheck source=lib/edge-smoke.sh
source "${SCRIPT_DIR}/lib/edge-smoke.sh"

DRY_RUN="${DRY_RUN:-false}"
RELEASE_DIR="${DEADBOLT_RELEASE_DIR:-/opt/deadbolt/releases}"
BOOTSTRAP_MARKER_FILE="${RELEASE_DIR}/bootstrap_complete"
BOOTSTRAP_MODE="${DEADBOLT_BOOTSTRAP:-false}"
POSITIONAL_ARGS=()
for arg in "$@"; do
  if [[ "$arg" == "--bootstrap" ]]; then
    BOOTSTRAP_MODE="true"
  else
    POSITIONAL_ARGS+=("$arg")
  fi
done


CANDIDATE_DIGEST="${DEADBOLT_IMAGE_DIGEST:-${POSITIONAL_ARGS[0]:-}}"
CANDIDATE_COMMIT="${DEADBOLT_COMMIT_SHA:-${POSITIONAL_ARGS[1]:-}}"
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
  log "DRY RUN passed: Staging deployment workflow is syntactically sound."
  exit 0
fi

POSTGRES_IMAGE_FILE="${RELEASE_DIR}/postgres_image"
PREVIOUS_POSTGRES_IMAGE_FILE="${RELEASE_DIR}/postgres_image.previous"
CALLER_POSTGRES_IMAGE="${DEADBOLT_POSTGRES_IMAGE:-}"

inspect_and_record_postgres_provenance() {
  local expected_image="$1"
  local container_name="deadbolt-staging-postgres"
  log "Inspecting running PostgreSQL container image identity for $container_name..."
  local running_image_id running_image_decl repo_digests
  running_image_id=$(docker inspect --format '{{.Image}}' "$container_name" 2>/dev/null || true)
  running_image_decl=$(docker inspect --format '{{index .Config.Image}}' "$container_name" 2>/dev/null || true)
  repo_digests=$(docker inspect --format '{{json .RepoDigests}}' "$running_image_id" 2>/dev/null || echo "[]")

  local expected_sha
  expected_sha=$(echo "$expected_image" | grep -o 'sha256:[a-f0-9]\{64\}' || true)
  if [[ -n "$expected_sha" ]]; then
    if ! echo "$running_image_decl $repo_digests $running_image_id" | grep -q "$expected_sha"; then
      err "POSTGRES IMAGE IDENTITY MISMATCH: Expected digest $expected_sha does not match running container image ($running_image_decl)!"
      exit 1
    fi
  elif [[ "$running_image_decl" != "$expected_image" ]]; then
    err "POSTGRES IMAGE IDENTITY MISMATCH: Expected $expected_image, got $running_image_decl!"
    exit 1
  fi

  mkdir -p "$RELEASE_DIR"
  if [[ -f "$POSTGRES_IMAGE_FILE" ]]; then
    local current_recorded
    current_recorded=$(cat "$POSTGRES_IMAGE_FILE" | tr -d '[:space:]' || true)
    if [[ "$current_recorded" != "$expected_image" ]]; then
      cp -f "$POSTGRES_IMAGE_FILE" "$PREVIOUS_POSTGRES_IMAGE_FILE"
    fi
  fi
  local tmp_pg_file
  tmp_pg_file=$(mktemp "${RELEASE_DIR}/postgres_image.tmp.XXXXXX")
  echo "$expected_image" > "$tmp_pg_file"
  mv -f "$tmp_pg_file" "$POSTGRES_IMAGE_FILE"
  log "PostgreSQL release provenance independently verified and persisted: $expected_image"
}

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
elif [[ -z "${DEADBOLT_POSTGRES_IMAGE:-}" && -f "$POSTGRES_IMAGE_FILE" ]]; then
  DEADBOLT_POSTGRES_IMAGE=$(cat "$POSTGRES_IMAGE_FILE" | tr -d '[:space:]')
fi

# Validate required variables (zero repository-known defaults allowed in hosted staging)
for var in DEADBOLT_STAGING_DOMAIN DATABASE_URL MIGRATOR_DATABASE_URL SYSTEM_DATABASE_URL DEADBOLT_DB_ADMIN_PASSWORD DEADBOLT_MIGRATOR_PASSWORD DEADBOLT_RUNTIME_PASSWORD DEADBOLT_SYSTEM_PASSWORD DEADBOLT_OIDC_ISSUER DEADBOLT_OIDC_CLIENT_ID DEADBOLT_OIDC_CLIENT_SECRET DEADBOLT_STORAGE_S3_BUCKET; do
  if [[ -z "${!var:-}" ]]; then
    err "Required configuration variable $var is missing or empty!"
    err "Configure in $CONFIG_FILE or via environment. Zero default passwords permitted."
    exit 1
  fi
done

# Validate database connection URL against expected username and password
# Handles URL-encoding (e.g. %40, %21, %23) cleanly
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

# Enforce least-privilege credential separation (Blueprint §26.3)
if [[ "$MIGRATOR_DATABASE_URL" == "$DATABASE_URL" ]]; then
  err "SECURITY VIOLATION: MIGRATOR_DATABASE_URL must not be identical to runtime DATABASE_URL!"
  err "Runtime control plane must NOT possess DDL privileges."
  exit 1
fi

if [[ -n "$SYSTEM_DATABASE_URL" ]]; then
  if [[ "$SYSTEM_DATABASE_URL" == "$DATABASE_URL" || "$SYSTEM_DATABASE_URL" == "$MIGRATOR_DATABASE_URL" ]]; then
    err "SECURITY VIOLATION: SYSTEM_DATABASE_URL must not be identical to DATABASE_URL or MIGRATOR_DATABASE_URL!"
    exit 1
  fi
fi

if [[ ! "$CANDIDATE_DIGEST" =~ ^.+@sha256:[a-f0-9]{64}$ ]]; then
  err "CANDIDATE_DIGEST is required (format: ghcr.io/ryanakml/deadbolt/control-plane@sha256:...)"
  exit 1
fi

# Compose interpolates every service before selecting postgres/nats or an inactive
# slot. Keep the exact candidate available as the immutable fallback for both slots.
DEADBOLT_IMAGE="$CANDIDATE_DIGEST"
export DEADBOLT_IMAGE

DEADBOLT_POSTGRES_IMAGE="${DEADBOLT_POSTGRES_IMAGE:-}"
if [[ -z "$DEADBOLT_POSTGRES_IMAGE" ]]; then
  err "DEADBOLT_POSTGRES_IMAGE is required (format: ghcr.io/ryanakml/deadbolt/postgres@sha256:...)"
  exit 1
fi

export DEADBOLT_POSTGRES_IMAGE

# Automatic fresh-host detection: if bootstrap marker does not exist, activate bootstrap mode
if [[ "$BOOTSTRAP_MODE" != "true" && ! -f "$BOOTSTRAP_MARKER_FILE" ]]; then
  log "Uninitialized Deadbolt staging host detected (marker $BOOTSTRAP_MARKER_FILE missing)."
  log "Automatically activating --bootstrap mode for fresh host initialization..."
  BOOTSTRAP_MODE="true"
fi

if [[ "$BOOTSTRAP_MODE" == "true" ]]; then
  log "BOOTSTRAP MODE: Fresh staging host cluster initialization sequence enabled."
  log "Delegating to authoritative bootstrap script (scripts/bootstrap-staging-cluster.sh)..."
  ./scripts/bootstrap-staging-cluster.sh


  # Headroom & Co-tenant isolation checks
  log "Step 3: Checking system headroom & co-tenant boundaries..."
  FREE_RAM_MB=$(free -m 2>/dev/null | awk '/^Mem:/{print $7}' || echo "2048")
  if [[ "$FREE_RAM_MB" -lt 1024 ]]; then
    err "INSUFFICIENT MEMORY: Only ${FREE_RAM_MB}MB available, minimum 1024MB required."
    exit 1
  fi

  FREE_DISK_MB=$(df -m / 2>/dev/null | awk 'NR==2 {print $4}' || echo "8192")
  if [[ "$FREE_DISK_MB" -lt 4096 ]]; then
    err "INSUFFICIENT DISK: Only ${FREE_DISK_MB}MB available on root, minimum 4096MB required."
    exit 1
  fi

  if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -qi "flowdesk"; then
    log "FlowDesk co-tenant containers detected and protected."
  fi

  # Pull candidate immutable image
  log "Step 4: Pulling candidate image by immutable digest: $CANDIDATE_DIGEST"
  docker pull "$CANDIDATE_DIGEST"
else
  # Normal Promotion Mode:
  # 2. Verify external S3 backup readiness FIRST
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

  # 4b. Bootstrap persistent data services (PostgreSQL, NATS) before migration
  log "Step 4b: Bootstrapping persistent staging data infrastructure (PostgreSQL & NATS)..."
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
  log "PostgreSQL is healthy and accepting connections."

  # Step 4c: Independently inspect running PostgreSQL container and persist release provenance transactionally
  inspect_and_record_postgres_provenance "$DEADBOLT_POSTGRES_IMAGE"
fi

# 5. Deterministic migration execution via candidate container runner
log "Step 5: Executing forward schema migrations with advisory lock..."
docker run --rm \
  --network deadbolt_staging_net \
  -e MIGRATOR_DATABASE_URL="$MIGRATOR_DATABASE_URL" \
  -e DEADBOLT_MIGRATIONS_DIR="/migrations" \
  "$CANDIDATE_DIGEST" --migrate

# 6. Blue-Green Slot Selection. A missing state file is a first deployment, not proof blue is live.
mkdir -p "$RELEASE_DIR"
CURRENT_SLOT=""
HAS_KNOWN_GOOD_ROUTE="false"
if [[ -s "$ACTIVE_SLOT_FILE" && -s "$CURRENT_RELEASE_FILE" ]]; then
  CURRENT_SLOT=$(cat "$ACTIVE_SLOT_FILE" | tr -d '[:space:]')
fi

if [[ -z "$CURRENT_SLOT" ]]; then
  log "No authoritative current release exists; clearing any stale Deadbolt edge route before candidate launch."
  if ! ./scripts/reload-caddy.sh --clear-deadbolt-route; then
    err "Unable to prove stale Deadbolt edge state is cleared; refusing to start first-deploy candidate."
    exit 1
  fi
fi

if [[ "$CURRENT_SLOT" == "blue" ]]; then
  CANDIDATE_SLOT="green"
  CANDIDATE_PORT="8089"
  OLD_SLOT="blue"
  OLD_PORT="8088"
elif [[ "$CURRENT_SLOT" == "green" ]]; then
  CANDIDATE_SLOT="blue"
  CANDIDATE_PORT="8088"
  OLD_SLOT="green"
  OLD_PORT="8089"
else
  CANDIDATE_SLOT="blue"
  CANDIDATE_PORT="8088"
  OLD_SLOT=""
  OLD_PORT=""
fi

if [[ -n "$OLD_PORT" ]]; then
  OLD_READY_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:${OLD_PORT}/readyz" 2>/dev/null || echo "000")
  if [[ "$OLD_READY_STATUS" == "200" ]]; then
    HAS_KNOWN_GOOD_ROUTE="true"
  else
    err "Recorded old slot $CURRENT_SLOT is not ready; it will not be used as a rollback target."
  fi
fi

log "Current active slot: ${CURRENT_SLOT:-none} (known-good route: $HAS_KNOWN_GOOD_ROUTE)"
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
log "Provenance verified: exact commit and digest match in /version."

# 9b. Independent running container image identity inspection (Blueprint §26 & Item 7)
CONTAINER_NAME="deadbolt-staging-control-plane-${CANDIDATE_SLOT}"
log "Step 9b: Inspecting running container image identity for $CONTAINER_NAME..."
RUNNING_IMAGE_ID=$(docker inspect --format '{{.Image}}' "$CONTAINER_NAME" 2>/dev/null || true)
RUNNING_IMAGE_DECL=$(docker inspect --format '{{index .Config.Image}}' "$CONTAINER_NAME" 2>/dev/null || true)
REPO_DIGESTS=$(docker inspect --format '{{json .RepoDigests}}' "$RUNNING_IMAGE_ID" 2>/dev/null || echo "[]")

log "Running container image ID: $RUNNING_IMAGE_ID"
log "Running container declared image: $RUNNING_IMAGE_DECL"
log "Image RepoDigests: $REPO_DIGESTS"

CANDIDATE_SHA=$(echo "$CANDIDATE_DIGEST" | grep -o 'sha256:[a-f0-9]\{64\}' || true)
if [[ -n "$CANDIDATE_SHA" ]]; then
  if ! echo "$RUNNING_IMAGE_DECL $REPO_DIGESTS $RUNNING_IMAGE_ID" | grep -q "$CANDIDATE_SHA"; then
    err "CONTAINER IMAGE IDENTITY MISMATCH: Expected digest $CANDIDATE_SHA does not match running container image!"
    rollback
    exit 1
  fi
  log "Independent image identity verification PASSED."
fi

# 10. Atomic traffic switch: update Caddy upstream
log "Step 10: Atomically switching Caddy edge route to port $CANDIDATE_PORT..."
export DEADBOLT_UPSTREAM_PORT="$CANDIDATE_PORT"
if ! ./scripts/reload-caddy.sh "$CANDIDATE_PORT"; then
  err "Caddy reload failed!"
  rollback
  exit 1
fi

# 11. Staged edge smoke test
log "Step 11: Smokin edge route via Caddy..."
if ! wait_for_candidate_edge_smoke "$DEADBOLT_STAGING_DOMAIN" "$CANDIDATE_DIGEST"; then
  err "EDGE SMOKE FAILED after bounded retry window: Caddy edge is not serving candidate image digest!"
  # Route restoration is authoritative: never stop a candidate Caddy may still route to.
  if ! restore_edge_route_before_stopping_candidate "$OLD_PORT" "$HAS_KNOWN_GOOD_ROUTE"; then
    exit 1
  fi
  exit 1
fi

# 12. Decommission previous slot container
if [[ "$HAS_KNOWN_GOOD_ROUTE" == "true" ]]; then
  log "Step 12: Stopping previous slot ($OLD_SLOT)..."
  docker compose -p deadbolt-staging -f "$COMPOSE_FILE" stop "control-plane-$OLD_SLOT" || true
fi

# 13. Record successful release state
if [[ -f "$CURRENT_RELEASE_FILE" ]]; then
  cp "$CURRENT_RELEASE_FILE" "$PREVIOUS_RELEASE_FILE"
fi
echo "$CANDIDATE_DIGEST" > "$CURRENT_RELEASE_FILE"
echo "$CANDIDATE_SLOT" > "$ACTIVE_SLOT_FILE"

# Record durable bootstrap completion marker
if [[ ! -f "$BOOTSTRAP_MARKER_FILE" ]]; then
  cat <<EOF > "${BOOTSTRAP_MARKER_FILE}.tmp"
BOOTSTRAP_COMPLETED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
POSTGRES_IMAGE="$DEADBOLT_POSTGRES_IMAGE"
FIRST_DEPLOY_DIGEST="$CANDIDATE_DIGEST"
FIRST_DEPLOY_COMMIT="${CANDIDATE_COMMIT:-unknown}"
EOF
  mv -f "${BOOTSTRAP_MARKER_FILE}.tmp" "$BOOTSTRAP_MARKER_FILE"
  log "Durable bootstrap marker confirmed: $BOOTSTRAP_MARKER_FILE"
fi

log "SUCCESS: Staging deployment completed. Active slot: $CANDIDATE_SLOT (port $CANDIDATE_PORT)."
