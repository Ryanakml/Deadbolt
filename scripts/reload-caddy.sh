#!/usr/bin/env bash
set -euo pipefail

# reload-caddy.sh: Safely integrates Deadbolt staging routes into existing containerized Caddy edge
# using Docker network DNS aliases (deadbolt-edge) and zero host caddy binary dependency (Issue #5).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=lib/config-permissions.sh
source "${SCRIPT_DIR}/lib/config-permissions.sh"

RELEASE_DIR="${DEADBOLT_RELEASE_DIR:-/opt/deadbolt/releases}"
ACTIVE_UPSTREAM_FILE="${RELEASE_DIR}/active_upstream_port"
ACTIVE_SLOT_FILE="${RELEASE_DIR}/active_slot"
PREVIOUS_ROUTE_DIR="${RELEASE_DIR}/previous_edge_route"
DRY_RUN="${DRY_RUN:-false}"

log() {
  echo "[CADDY_EDGE] $*"
}

err() {
  echo "[CADDY_EDGE_ERROR] $*" >&2
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
  set -a
  # shellcheck source=/dev/null
  source "$CONFIG_FILE"
  set +a
fi

# Managed host snippet path (default /opt/deadbolt/caddy/Deadbolt.caddyfile)
if [[ -n "${DEADBOLT_CADDYFILE_SNIPPET:-}" ]]; then
  HOST_SNIPPET_FILE="$DEADBOLT_CADDYFILE_SNIPPET"
elif [[ -d "/opt/deadbolt/caddy" || -w "/opt/deadbolt" ]]; then
  HOST_SNIPPET_FILE="/opt/deadbolt/caddy/Deadbolt.caddyfile"
else
  HOST_SNIPPET_FILE="${REPO_ROOT}/deploy/caddy/Deadbolt.caddyfile"
fi

# Existing containerized Caddy edge (FlowDesk Caddy owning 80/443)
CADDY_CONTAINER="${DEADBOLT_CADDY_CONTAINER:-flowdesk-staging-caddy-1}"
CONTAINER_CADDYFILE="${DEADBOLT_CONTAINER_CADDYFILE:-/etc/caddy/Caddyfile}"
CONTAINER_SNIPPET_FILE="${DEADBOLT_CONTAINER_SNIPPET:-/etc/caddy/deadbolt/Deadbolt.caddyfile}"

snapshot_route_file() {
  local source_file="$1" snapshot_file="$2"
  if [[ -f "$source_file" ]]; then
    cp "$source_file" "$snapshot_file"
    touch "${snapshot_file}.present"
  fi
}

restore_route_file() {
  local snapshot_file="$1" destination_file="$2"
  if [[ -f "${snapshot_file}.present" ]]; then
    cp -f "$snapshot_file" "$destination_file"
  else
    rm -f "$destination_file"
  fi
}

restore_previous_route() {
  if [[ ! -d "$PREVIOUS_ROUTE_DIR" ]]; then
    err "No authoritative pre-switch Deadbolt route snapshot exists; refusing to stop candidate."
    return 1
  fi
  verify_caddy_container_prerequisites
  local current_dir
  current_dir=$(mktemp -d "${RELEASE_DIR}/.current-edge-route.XXXXXX")
  snapshot_route_file "$HOST_SNIPPET_FILE" "${current_dir}/snippet"
  snapshot_route_file "$ACTIVE_UPSTREAM_FILE" "${current_dir}/upstream"
  snapshot_route_file "$ACTIVE_SLOT_FILE" "${current_dir}/slot"

  restore_route_file "${PREVIOUS_ROUTE_DIR}/snippet" "$HOST_SNIPPET_FILE"
  restore_route_file "${PREVIOUS_ROUTE_DIR}/upstream" "$ACTIVE_UPSTREAM_FILE"
  restore_route_file "${PREVIOUS_ROUTE_DIR}/slot" "$ACTIVE_SLOT_FILE"
  if ! docker exec "$CADDY_CONTAINER" caddy validate --config "$CONTAINER_CADDYFILE" --adapter caddyfile || ! docker exec "$CADDY_CONTAINER" caddy reload --config "$CONTAINER_CADDYFILE" --adapter caddyfile; then
    err "Prior-route restoration failed; returning host files to candidate state and preserving candidate container."
    restore_route_file "${current_dir}/snippet" "$HOST_SNIPPET_FILE"
    restore_route_file "${current_dir}/upstream" "$ACTIVE_UPSTREAM_FILE"
    restore_route_file "${current_dir}/slot" "$ACTIVE_SLOT_FILE"
    docker exec "$CADDY_CONTAINER" caddy reload --config "$CONTAINER_CADDYFILE" --adapter caddyfile 2>/dev/null || true
    rm -rf "$current_dir"
    return 1
  fi
  rm -rf "$current_dir"
  log "SUCCESS: Restored exact pre-switch Deadbolt route state."
}

clear_deadbolt_route() {
  verify_caddy_container_prerequisites
  local current_dir
  current_dir=$(mktemp -d "${RELEASE_DIR}/.current-edge-route.XXXXXX")
  snapshot_route_file "$HOST_SNIPPET_FILE" "${current_dir}/snippet"
  snapshot_route_file "$ACTIVE_UPSTREAM_FILE" "${current_dir}/upstream"
  snapshot_route_file "$ACTIVE_SLOT_FILE" "${current_dir}/slot"
  rm -f "$HOST_SNIPPET_FILE" "$ACTIVE_UPSTREAM_FILE" "$ACTIVE_SLOT_FILE"
  if ! docker exec "$CADDY_CONTAINER" caddy validate --config "$CONTAINER_CADDYFILE" --adapter caddyfile || ! docker exec "$CADDY_CONTAINER" caddy reload --config "$CONTAINER_CADDYFILE" --adapter caddyfile; then
    err "Unable to clear stale Deadbolt route; restoring prior host state and failing closed."
    restore_route_file "${current_dir}/snippet" "$HOST_SNIPPET_FILE"
    restore_route_file "${current_dir}/upstream" "$ACTIVE_UPSTREAM_FILE"
    restore_route_file "${current_dir}/slot" "$ACTIVE_SLOT_FILE"
    docker exec "$CADDY_CONTAINER" caddy reload --config "$CONTAINER_CADDYFILE" --adapter caddyfile 2>/dev/null || true
    rm -rf "$current_dir"
    return 1
  fi
  rm -rf "$current_dir"
  log "SUCCESS: Cleared stale Deadbolt edge route and state."
}

# Resolve target upstream slot and Docker alias. Restore mode bypasses target use
# after prerequisite functions are defined, but retains a valid placeholder here.
RESTORE_PREVIOUS_ROUTE="false"
CLEAR_DEADBOLT_ROUTE="false"
if [[ "${1:-}" == "--restore-previous-route" ]]; then
  RESTORE_PREVIOUS_ROUTE="true"
  TARGET_ARG="8088"
elif [[ "${1:-}" == "--clear-deadbolt-route" ]]; then
  CLEAR_DEADBOLT_ROUTE="true"
  TARGET_ARG="8088"
else
  TARGET_ARG="${1:-${DEADBOLT_UPSTREAM_TARGET:-${DEADBOLT_UPSTREAM_PORT:-}}}"
fi
if [[ -z "$TARGET_ARG" && -f "$ACTIVE_UPSTREAM_FILE" ]]; then
  TARGET_ARG=$(cat "$ACTIVE_UPSTREAM_FILE" | tr -d '[:space:]')
fi
if [[ -z "$TARGET_ARG" && -f "$ACTIVE_SLOT_FILE" ]]; then
  TARGET_ARG=$(cat "$ACTIVE_SLOT_FILE" | tr -d '[:space:]')
fi
if [[ -z "$TARGET_ARG" ]]; then
  TARGET_ARG="8088"
fi

if [[ "$TARGET_ARG" == "8088" || "$TARGET_ARG" == "blue" || "$TARGET_ARG" == "deadbolt-control-plane-blue:8080" ]]; then
  TARGET_SLOT="blue"
  TARGET_PORT="8088"
  TARGET_UPSTREAM="deadbolt-control-plane-blue:8080"
elif [[ "$TARGET_ARG" == "8089" || "$TARGET_ARG" == "green" || "$TARGET_ARG" == "deadbolt-control-plane-green:8080" ]]; then
  TARGET_SLOT="green"
  TARGET_PORT="8089"
  TARGET_UPSTREAM="deadbolt-control-plane-green:8080"
else
  err "Invalid upstream target: $TARGET_ARG (must resolve to blue/8088 or green/8089)"
  exit 1
fi

if [[ "$DRY_RUN" != "true" && -z "${DEADBOLT_STAGING_DOMAIN:-}" ]]; then
  err "Required DEADBOLT_STAGING_DOMAIN is missing or empty! Zero invented fallback domains permitted."
  exit 1
fi

render_snippet() {
  local target_file="$1"
  local domain="${DEADBOLT_STAGING_DOMAIN:-}"
  if [[ -z "$domain" ]]; then
    if [[ "$DRY_RUN" == "true" ]]; then
      domain="staging-dryrun.internal"
    else
      err "DEADBOLT_STAGING_DOMAIN is required to render Caddy snippet!"
      exit 1
    fi
  fi
  cat <<EOF > "$target_file"
# Deadbolt Control Plane Staging Route Snippet (Issue #5)
# Managed active upstream: ${TARGET_UPSTREAM} (slot: ${TARGET_SLOT})
${domain} {
    reverse_proxy ${TARGET_UPSTREAM} {
        header_up Host {host}
        header_up X-Real-IP {remote_host}
        header_up X-Forwarded-For {remote_host}
        header_up X-Forwarded-Proto {scheme}
    }
}
EOF
}

verify_caddy_container_prerequisites() {
  # 1. Verify Caddy container exists and is running
  local caddy_status
  caddy_status=$(docker inspect --format '{{.State.Status}}' "$CADDY_CONTAINER" 2>/dev/null || true)
  if [[ "$caddy_status" != "running" ]]; then
    err "PREREQUISITE FAILURE: Caddy container '$CADDY_CONTAINER' is not running (status: ${caddy_status:-not found})!"
    err "Remediation: Ensure FlowDesk Caddy container is running and joined to external network 'deadbolt-edge'."
    exit 1
  fi

  # 2. Verify managed host snippet directory is mounted into container
  local mounts_json
  mounts_json=$(docker inspect --format '{{json .Mounts}}' "$CADDY_CONTAINER" 2>/dev/null || echo "[]")
  if ! echo "$mounts_json" | grep -Eq '/etc/caddy/deadbolt|/etc/caddy/deadbolt/Deadbolt\.caddyfile'; then
    err "PREREQUISITE FAILURE: Managed snippet directory '/etc/caddy/deadbolt' is not mounted in container '$CADDY_CONTAINER'!"
    err "Remediation: Add mount '/opt/deadbolt/caddy:/etc/caddy/deadbolt:ro' to Caddy container."
    exit 1
  fi

  # 3. Verify active container Caddyfile imports the managed Deadbolt snippet path
  local container_caddy_content
  container_caddy_content=$(docker exec "$CADDY_CONTAINER" cat "$CONTAINER_CADDYFILE" 2>/dev/null || true)
  if ! echo "$container_caddy_content" | grep -Eq 'import +/etc/caddy/deadbolt/.*|import +/etc/caddy/deadbolt/Deadbolt\.caddyfile|import +/etc/caddy/deadbolt'; then
    err "PREREQUISITE FAILURE: Container Caddyfile ($CONTAINER_CADDYFILE) does not import Deadbolt snippet!"
    err "Expected import directive: 'import /etc/caddy/deadbolt/*.caddyfile'"
    err "Deadbolt does not own or modify the FlowDesk Caddyfile. The host import is a provisioning prerequisite."
    exit 1
  fi
}

if [[ "$RESTORE_PREVIOUS_ROUTE" == "true" ]]; then
  restore_previous_route
  exit $?
fi
if [[ "$CLEAR_DEADBOLT_ROUTE" == "true" ]]; then
  clear_deadbolt_route
  exit $?
fi

if [[ "$DRY_RUN" == "true" ]]; then
  log "DRY RUN: Verifying Caddy snippet rendering and container contracts (target: $TARGET_UPSTREAM)..."
  TMP_SNIPPET=$(mktemp)
  render_snippet "$TMP_SNIPPET"

  # Enforce strict contract: rendered snippet must use Docker DNS alias, NEVER loopback
  if grep -q "127.0.0.1" "$TMP_SNIPPET"; then
    err "DRY RUN: Snippet contains forbidden host loopback address (127.0.0.1)!"
    rm -f "$TMP_SNIPPET"
    exit 1
  fi
  if ! grep -q "$TARGET_UPSTREAM" "$TMP_SNIPPET"; then
    err "DRY RUN: Snippet does not contain expected upstream '$TARGET_UPSTREAM'!"
    rm -f "$TMP_SNIPPET"
    exit 1
  fi

  if [[ "${DEADBOLT_SYNTAX_ONLY:-${DEADBOLT_SNIPPET_ONLY:-false}}" == "true" ]]; then
    rm -f "$TMP_SNIPPET"
    log "DRY RUN passed: Caddy edge snippet syntax and Docker DNS alias verified for $TARGET_UPSTREAM (syntax only)."
    exit 0
  fi


  # Dry run strictly verifies container prerequisites and executes validation inside container
  verify_caddy_container_prerequisites

  log "DRY RUN: Validating merged Caddy configuration inside container '$CADDY_CONTAINER'..."
  if ! docker exec "$CADDY_CONTAINER" caddy validate --config "$CONTAINER_CADDYFILE" --adapter caddyfile; then
    err "DRY RUN: Caddy container validation failed inside '$CADDY_CONTAINER'!"
    rm -f "$TMP_SNIPPET"
    exit 1
  fi

  rm -f "$TMP_SNIPPET"
  log "DRY RUN passed: Caddy edge configuration verified for $TARGET_UPSTREAM."
  exit 0
fi



# 1. Verify containerized Caddy prerequisites on live host
verify_caddy_container_prerequisites

# 2. Back up current snippet and upstream route state
TIMESTAMP="$(date +%Y%m%d%H%M%S)"
SNIPPET_BACKUP="${HOST_SNIPPET_FILE}.bak.${TIMESTAMP}"
UPSTREAM_BACKUP="${ACTIVE_UPSTREAM_FILE}.bak.${TIMESTAMP}"
SLOT_BACKUP="${ACTIVE_SLOT_FILE}.bak.${TIMESTAMP}"

mkdir -p "$(dirname "$HOST_SNIPPET_FILE")"
if [[ -f "$HOST_SNIPPET_FILE" ]]; then
  cp "$HOST_SNIPPET_FILE" "$SNIPPET_BACKUP"
fi
if [[ -f "$ACTIVE_UPSTREAM_FILE" ]]; then
  cp "$ACTIVE_UPSTREAM_FILE" "$UPSTREAM_BACKUP"
fi
if [[ -f "$ACTIVE_SLOT_FILE" ]]; then
  cp "$ACTIVE_SLOT_FILE" "$SLOT_BACKUP"
fi

# Persist the exact pre-switch route, including an intentionally absent first route.
mkdir -p "$RELEASE_DIR"
NEW_PREVIOUS_ROUTE_DIR=$(mktemp -d "${RELEASE_DIR}/.previous-edge-route.XXXXXX")
snapshot_route_file "$HOST_SNIPPET_FILE" "${NEW_PREVIOUS_ROUTE_DIR}/snippet"
snapshot_route_file "$ACTIVE_UPSTREAM_FILE" "${NEW_PREVIOUS_ROUTE_DIR}/upstream"
snapshot_route_file "$ACTIVE_SLOT_FILE" "${NEW_PREVIOUS_ROUTE_DIR}/slot"

rollback() {
  err "Caddy reload or validation failed! Rolling back Deadbolt snippet and route state..."
  if [[ -f "$SNIPPET_BACKUP" ]]; then
    cp -f "$SNIPPET_BACKUP" "$HOST_SNIPPET_FILE"
    rm -f "$SNIPPET_BACKUP"
  else
    rm -f "$HOST_SNIPPET_FILE"
  fi
  if [[ -f "$UPSTREAM_BACKUP" ]]; then
    cp -f "$UPSTREAM_BACKUP" "$ACTIVE_UPSTREAM_FILE"
    rm -f "$UPSTREAM_BACKUP"
  else
    rm -f "$ACTIVE_UPSTREAM_FILE"
  fi
  if [[ -f "$SLOT_BACKUP" ]]; then
    cp -f "$SLOT_BACKUP" "$ACTIVE_SLOT_FILE"
    rm -f "$SLOT_BACKUP"
  else
    rm -f "$ACTIVE_SLOT_FILE"
  fi
  docker exec "$CADDY_CONTAINER" caddy reload --config "$CONTAINER_CADDYFILE" --adapter caddyfile 2>/dev/null || true
  err "Rollback completed. Prior snippet and Caddy in-memory state restored."
}

# 3. Atomically render and place candidate snippet
TMP_SNIPPET="$(mktemp "$(dirname "$HOST_SNIPPET_FILE")/Deadbolt.caddyfile.tmp.XXXXXX")"
render_snippet "$TMP_SNIPPET"
mv -f "$TMP_SNIPPET" "$HOST_SNIPPET_FILE"

# 4. Validate merged configuration inside existing Caddy container
log "Validating Caddy configuration inside container '$CADDY_CONTAINER'..."
if ! docker exec "$CADDY_CONTAINER" caddy validate --config "$CONTAINER_CADDYFILE" --adapter caddyfile; then
  err "Caddy validation failed inside container '$CADDY_CONTAINER'!"
  rollback
  exit 1
fi
log "Caddy configuration validated successfully."

# 5. Execute zero-downtime reload inside existing Caddy container
log "Executing Caddy reload inside container '$CADDY_CONTAINER'..."
if ! docker exec "$CADDY_CONTAINER" caddy reload --config "$CONTAINER_CADDYFILE" --adapter caddyfile; then
  err "Caddy reload failed inside container '$CADDY_CONTAINER'!"
  rollback
  exit 1
fi
log "Caddy reload completed successfully."

# 6. Persist active upstream state ONLY after successful reload
mkdir -p "$RELEASE_DIR"
echo "$TARGET_PORT" > "$ACTIVE_UPSTREAM_FILE"
echo "$TARGET_SLOT" > "$ACTIVE_SLOT_FILE"
rm -rf "$PREVIOUS_ROUTE_DIR"
mv "$NEW_PREVIOUS_ROUTE_DIR" "$PREVIOUS_ROUTE_DIR"
rm -f "$SNIPPET_BACKUP" "$UPSTREAM_BACKUP" "$SLOT_BACKUP" 2>/dev/null || true

log "SUCCESS: Caddy edge route switched to $TARGET_UPSTREAM (slot: $TARGET_SLOT, port: $TARGET_PORT)."
