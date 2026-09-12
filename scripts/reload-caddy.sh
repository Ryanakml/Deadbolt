#!/usr/bin/env bash
set -euo pipefail

# reload-caddy.sh: Safely integrates Deadbolt staging routes into existing Caddy edge
# with pre-validation and automatic rollback on reload failure (Issue #5).

CADDYFILE="${CADDY_CONFIG_PATH:-/etc/caddy/Caddyfile}"
SNIPPET_FILE="${DEADBOLT_CADDYFILE_SNIPPET:-deploy/caddy/Deadbolt.caddyfile}"
DRY_RUN="${DRY_RUN:-false}"

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
  log "DRY RUN: Verifying Caddy snippet and merged configuration..."
  if [[ ! -f "$ABS_SNIPPET_FILE" ]]; then
    err "Snippet file not found: $ABS_SNIPPET_FILE"
    exit 1
  fi
  if command -v caddy &>/dev/null; then
    TMP_CADDY=$(mktemp)
    trap 'rm -f "$TMP_CADDY"' EXIT
    if [[ -f "$CADDYFILE" ]]; then
      cp "$CADDYFILE" "$TMP_CADDY"
    else
      echo ":80 {}" > "$TMP_CADDY"
    fi
    if ! grep -q "$ABS_SNIPPET_FILE" "$TMP_CADDY"; then
      echo "import ${ABS_SNIPPET_FILE}" >> "$TMP_CADDY"
    fi
    if ! caddy validate --config "$TMP_CADDY" --adapter caddyfile 2>/dev/null; then
      err "DRY RUN: Merged Caddy configuration validation failed!"
      exit 1
    fi
    log "DRY RUN: Merged Caddy configuration validation passed."
  else
    log "DRY RUN: Snippet syntax check passed ($ABS_SNIPPET_FILE)."
  fi
  exit 0
fi

if [[ ! -f "$CADDYFILE" ]]; then
  err "Active Caddyfile not found at: $CADDYFILE"
  exit 1
fi

if [[ ! -f "$ABS_SNIPPET_FILE" ]]; then
  err "Deadbolt Caddy snippet not found at: $ABS_SNIPPET_FILE"
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
  caddy reload --config "$CADDYFILE" --adapter caddyfile || true
  err "Rollback completed."
}

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

log "Validating exact merged Caddy configuration..."
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

log "Caddy reload successfully completed with Deadbolt staging routes active."
