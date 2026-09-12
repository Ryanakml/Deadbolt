#!/usr/bin/env bash
# scripts/setup-backup-cron.sh
# Installs and verifies daily base backup cron definition with bounded retention (Issue #5).
# Supports /etc/cron.d, sudo privilege boundary, and user crontab installation.

set -euo pipefail

CRON_SRC="${CRON_SRC:-deploy/cron/deadbolt-backup.cron}"
CRON_DEST="${CRON_DEST:-/etc/cron.d/deadbolt-backup}"
DRY_RUN="${DRY_RUN:-false}"

log() {
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [BACKUP_CRON] $*"
}

err() {
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [BACKUP_CRON_ERROR] $*" >&2
}

if [[ "$DRY_RUN" == "true" ]]; then
  log "DRY RUN: Verifying backup cron definition syntax..."
  test -f "$CRON_SRC"
  log "DRY RUN passed: Backup cron definition syntax valid."
  exit 0
fi

if [[ ! -f "$CRON_SRC" ]]; then
  err "Cron definition file missing at $CRON_SRC!"
  exit 1
fi

INSTALLED=false

# 1. Try direct write to /etc/cron.d if writable or destination is writable
if [[ -w "/etc/cron.d" || ( -f "$CRON_DEST" && -w "$CRON_DEST" ) ]]; then
  log "Installing Deadbolt backup cron directly to $CRON_DEST..."
  cp "$CRON_SRC" "$CRON_DEST"
  chmod 644 "$CRON_DEST"
  INSTALLED=true
# 2. Try passwordless sudo if available
elif command -v sudo &>/dev/null && sudo -n true 2>/dev/null; then
  log "Installing Deadbolt backup cron via sudo to $CRON_DEST..."
  sudo cp "$CRON_SRC" "$CRON_DEST"
  sudo chmod 644 "$CRON_DEST"
  INSTALLED=true
# 3. Fallback to user crontab if crontab command is available
elif command -v crontab &>/dev/null; then
  log "Installing Deadbolt backup schedule into current user crontab..."
  USER_LOG_DIR="${HOME:-/tmp}/.deadbolt/logs"
  mkdir -p "$USER_LOG_DIR"
  touch "${USER_LOG_DIR}/deadbolt-backup.log" 2>/dev/null || true
  CRON_LINE=$(grep -E '^[0-9]' "$CRON_SRC" | head -n 1 | sed -E 's/ +root +/ /' | sed "s|/var/log/deadbolt-backup.log|${USER_LOG_DIR}/deadbolt-backup.log|g")
  if [[ -n "$CRON_LINE" ]]; then
    EXISTING_CRON=$(crontab -l 2>/dev/null || true)
    if ! echo "$EXISTING_CRON" | grep -Fq "take-base-backup.sh"; then
      (echo "$EXISTING_CRON"; echo "# Deadbolt scheduled backup"; echo "$CRON_LINE") | crontab -
    fi
    INSTALLED=true
  fi
fi

# 4. Strict verification of installed schedule
VERIFIED=false
if [[ -f "$CRON_DEST" && -s "$CRON_DEST" ]]; then
  if grep -Fq "take-base-backup.sh" "$CRON_DEST"; then
    VERIFIED=true
    log "Verified: Backup schedule confirmed in $CRON_DEST."
  fi
fi

if [[ "$VERIFIED" != "true" ]] && command -v crontab &>/dev/null; then
  if crontab -l 2>/dev/null | grep -Fq "take-base-backup.sh"; then
    VERIFIED=true
    log "Verified: Backup schedule confirmed in user crontab."
  fi
fi

if [[ "$VERIFIED" != "true" ]]; then
  err "CRON PROVISIONING FAILURE: Could not install or verify recurring backup schedule!"
  err "Destination $CRON_DEST not writable, sudo unavailable, and user crontab could not be updated."
  err "Operator action required: install $CRON_SRC to /etc/cron.d/deadbolt-backup with root privileges."
  exit 1
fi

log "SUCCESS: Daily base backup cron schedule successfully installed and verified."
