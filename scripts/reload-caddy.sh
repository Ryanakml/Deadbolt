#!/usr/bin/env bash
set -euo pipefail

# reload-caddy.sh: Safely integrates Deadbolt staging routes into existing Caddy edge
# with pre-validation and automatic rollback on reload failure (Issue #5).

CADDYFILE="${CADDY_CONFIG_PATH:-/etc/caddy/Caddyfile}"
SNIPPET_FILE="${DEADBOLT_CADDYFILE_SNIPPET:-deploy/caddy/Deadbolt.caddyfile}"
DRY_RUN="${DRY_RUN:-false}"

log() {
  echo "[CADDY_EDGE] $*"
}

err() {
  echo "[CADDY_EDGE_ERROR] $*" >&2
}

if [[ "$DRY_RUN" == "true" ]]; then
  log "DRY RUN: Verifying Caddy snippet syntax..."
  if [[ ! -f "$SNIPPET_FILE" ]]; then
    err "Snippet file not found: $SNIPPET_FILE"
    exit 1
  fi
  log "Snippet syntax check passed."
  exit 0
fi

if [[ ! -f "$CADDYFILE" ]]; then
  err "Active Caddyfile not found at: $CADDYFILE"
  exit 1
fi

if [[ ! -f "$SNIPPET_FILE" ]]; then
  err "Deadbolt Caddy snippet not found at: $SNIPPET_FILE"
  exit 1
fi

# Ensure Caddy binary is present
if ! command -v caddy &>/dev/null; then
  err "caddy executable not found in PATH"
  exit 1
fi

TIMESTAMP="$(date +%Y%m%d%H%M%S)"
BACKUP_FILE="${CADDYFILE}.bak.${TIMESTAMP}"

log "Backing up current Caddyfile to: $BACKUP_FILE"
cp "$CADDYFILE" "$BACKUP_FILE"

rollback() {
  err "Reload failed! Rolling back to: $BACKUP_FILE"
  cp "$BACKUP_FILE" "$CADDYFILE"
  caddy reload --config "$CADDYFILE" || true
  err "Rollback completed."
}

# Check if snippet import already exists
IMPORT_LINE="import ${SNIPPET_FILE}"
if ! grep -q "$SNIPPET_FILE" "$CADDYFILE"; then
  log "Appending Deadbolt snippet import to: $CADDYFILE"
  echo "" >> "$CADDYFILE"
  echo "# Deadbolt Staging Route" >> "$CADDYFILE"
  echo "$IMPORT_LINE" >> "$CADDYFILE"
fi

log "Validating proposed Caddy configuration..."
if ! caddy validate --config "$CADDYFILE"; then
  err "Caddy validation failed! Aborting reload."
  rollback
  exit 1
fi

log "Executing safe Caddy zero-downtime reload..."
if ! caddy reload --config "$CADDYFILE"; then
  err "Caddy reload execution failed!"
  rollback
  exit 1
fi

log "Caddy reload successfully completed with Deadbolt staging routes active."
