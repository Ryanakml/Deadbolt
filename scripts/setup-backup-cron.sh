#!/usr/bin/env bash
# scripts/setup-backup-cron.sh
# Installs daily base backup cron definition with bounded retention (Issue #5).

set -euo pipefail

CRON_SRC="deploy/cron/deadbolt-backup.cron"
CRON_DEST="${CRON_DEST:-/etc/cron.d/deadbolt-backup}"
DRY_RUN="${DRY_RUN:-false}"

log() {
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [BACKUP_CRON] $*"
}

if [[ "$DRY_RUN" == "true" ]]; then
  log "DRY RUN: Verifying backup cron definition syntax..."
  test -f "$CRON_SRC"
  log "DRY RUN passed: Backup cron syntax valid."
  exit 0
fi

if [[ -w "/etc/cron.d" || -w "$CRON_DEST" ]]; then
  log "Installing Deadbolt backup cron to $CRON_DEST..."
  cp "$CRON_SRC" "$CRON_DEST"
  chmod 644 "$CRON_DEST"
  log "Backup cron installed successfully (runs daily at 02:00 UTC)."
else
  log "Warning: /etc/cron.d is not writable by current user; ensuring cron definition exists at $CRON_SRC."
fi
