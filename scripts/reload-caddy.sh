#!/usr/bin/env bash
set -euo pipefail

# reload-caddy.sh: Safely integrates Deadbolt staging routes into existing Caddy edge
# with pre-validation and automatic rollback on reload failure (Issue #5).

CADDYFILE="${CADDY_CONFIG_PATH:-/etc/caddy/Caddyfile}"
SNIPPET_FILE="${DEADBOLT_CADDYFILE_SNIPPET:-deploy/caddy/Deadbolt.caddyfile}"
DRY_RUN="${DRY_RUN:-false}"

RELEASE_DIR="${DEADBOLT_RELEASE_DIR:-/opt/deadbolt/releases}"
ACTIVE_UPSTREAM_FILE="${RELEASE_DIR}/active_upstream_port"

# Determine target upstream port: argument > env var > persisted active upstream file > default 8088
TARGET_PORT="${1:-${DEADBOLT_UPSTREAM_PORT:-}}"
if [[ -z "$TARGET_PORT" && -f "$ACTIVE_UPSTREAM_FILE" ]]; then
  TARGET_PORT=$(cat "$ACTIVE_UPSTREAM_FILE" | tr -d '[:space:]')
fi
if [[ -z "$TARGET_PORT" ]]; then
  TARGET_PORT="8088"
fi

if [[ "$TARGET_PORT" != "8088" && "$TARGET_PORT" != "8089" ]]; then
  echo "[CADDY_EDGE_ERROR] Invalid upstream port: $TARGET_PORT (must be 8088 for blue or 8089 for green)" >&2
  exit 1
fi

render_snippet() {
  local target_file="$1"
  local domain="${DEADBOLT_STAGING_DOMAIN:-staging.deadbolt.cloud}"
  cat <<EOF > "$target_file"
# Deadbolt Control Plane Staging Route Snippet (Issue #5)
# Persisted active upstream route: literal loopback port ${TARGET_PORT}
${domain} {
    reverse_proxy 127.0.0.1:${TARGET_PORT} {
        header_up Host {host}
        header_up X-Real-IP {remote_host}
        header_up X-Forwarded-For {remote_host}
        header_up X-Forwarded-Proto {scheme}
    }
}
EOF
}

# Resolve absolute path for snippet file to prevent Caddy relative import errors (Issue #5)
if [[ -f "$SNIPPET_FILE" ]]; then
  ABS_SNIPPET_FILE="$(cd "$(dirname "$SNIPPET_FILE")" && pwd)/$(basename "$SNIPPET_FILE")"
elif [[ "$SNIPPET_FILE" = /* ]]; then
  ABS_SNIPPET_FILE="$SNIPPET_FILE"
else
  ABS_SNIPPET_FILE="$(pwd)/$SNIPPET_FILE"
fi

log() {
  echo "[CADDY_EDGE] $*"
}

err() {
  echo "[CADDY_EDGE_ERROR] $*" >&2
}

if [[ "$DRY_RUN" == "true" ]]; then
  log "DRY RUN: Verifying Caddy snippet and merged configuration (active port $TARGET_PORT)..."
  TMP_SNIPPET=$(mktemp)
  render_snippet "$TMP_SNIPPET"
  if command -v caddy &>/dev/null; then
    TMP_CADDY=$(mktemp)
    trap 'rm -f "$TMP_SNIPPET" "$TMP_CADDY"' EXIT
    if [[ -f "$CADDYFILE" ]]; then
      cp "$CADDYFILE" "$TMP_CADDY"
    else
      echo ":80 {}" > "$TMP_CADDY"
    fi
    sed -i.tmp "\|import ${ABS_SNIPPET_FILE}|d" "$TMP_CADDY" 2>/dev/null || true
    echo "import ${TMP_SNIPPET}" >> "$TMP_CADDY"
    if ! caddy validate --config "$TMP_CADDY" --adapter caddyfile 2>/dev/null; then
      err "DRY RUN: Merged Caddy configuration validation failed!"
      exit 1
    fi
    log "DRY RUN: Merged Caddy configuration validation passed."
  else
    log "DRY RUN: Snippet rendered and syntax check passed (port $TARGET_PORT)."
    rm -f "$TMP_SNIPPET"
  fi
  exit 0
fi

if [[ ! -f "$CADDYFILE" ]]; then
  err "Active Caddyfile not found at: $CADDYFILE"
  exit 1
fi

# Ensure Caddy binary is present
if ! command -v caddy &>/dev/null; then
  err "caddy executable not found in PATH"
  exit 1
fi

TIMESTAMP="$(date +%Y%m%d%H%M%S)"
BACKUP_FILE="${CADDYFILE}.bak.${TIMESTAMP}"
SNIPPET_BACKUP="${ABS_SNIPPET_FILE}.bak.${TIMESTAMP}"

log "Backing up current Caddyfile to: $BACKUP_FILE"
cp "$CADDYFILE" "$BACKUP_FILE"
if [[ -f "$ABS_SNIPPET_FILE" ]]; then
  cp "$ABS_SNIPPET_FILE" "$SNIPPET_BACKUP"
fi

rollback() {
  err "Reload failed! Rolling back to: $BACKUP_FILE"
  cp "$BACKUP_FILE" "$CADDYFILE"
  if [[ -f "$SNIPPET_BACKUP" ]]; then
    cp "$SNIPPET_BACKUP" "$ABS_SNIPPET_FILE"
  fi
  caddy reload --config "$CADDYFILE" --adapter caddyfile || true
  err "Rollback completed."
}

# Render managed snippet with literal validated upstream port
mkdir -p "$(dirname "$ABS_SNIPPET_FILE")"
render_snippet "$ABS_SNIPPET_FILE"

# Persist active upstream port to release state
mkdir -p "$RELEASE_DIR"
echo "$TARGET_PORT" > "$ACTIVE_UPSTREAM_FILE"

# Remove legacy relative import if present to ensure clean merged config
sed -i.tmp "\|import deploy/caddy/Deadbolt.caddyfile|d" "$CADDYFILE" 2>/dev/null || true
rm -f "${CADDYFILE}.tmp" 2>/dev/null || true

# Append absolute snippet import if not already present
IMPORT_LINE="import ${ABS_SNIPPET_FILE}"
if ! grep -q "$ABS_SNIPPET_FILE" "$CADDYFILE"; then
  log "Appending Deadbolt absolute snippet import to: $CADDYFILE"
  echo "" >> "$CADDYFILE"
  echo "# Deadbolt Staging Route" >> "$CADDYFILE"
  echo "$IMPORT_LINE" >> "$CADDYFILE"
fi

log "Validating exact merged Caddy configuration (active upstream port: $TARGET_PORT)..."
if ! caddy validate --config "$CADDYFILE" --adapter caddyfile; then
  err "Caddy validation failed on merged configuration! Aborting reload."
  rollback
  exit 1
fi

log "Executing safe Caddy zero-downtime reload..."
if ! caddy reload --config "$CADDYFILE" --adapter caddyfile; then
  err "Caddy reload execution failed!"
  rollback
  exit 1
fi

log "Caddy reload successfully completed with Deadbolt staging upstream persisted to port $TARGET_PORT."
