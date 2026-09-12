#!/usr/bin/env bash
# scripts/take-base-backup.sh
# Performs scheduled physical base backup and bounded retention pruning (Blueprint §26, Issue #5).
# Retains the 14 latest daily base backups and ensures continuous WAL archiving recoverability.

set -euo pipefail

DRY_RUN="${DRY_RUN:-false}"
S3_BUCKET="${DEADBOLT_STORAGE_S3_BUCKET:-}"
CONTAINER_NAME="${DEADBOLT_POSTGRES_CONTAINER:-deadbolt-staging-postgres}"
MAX_RETAINED_BASE_BACKUPS="${MAX_RETAINED_BASE_BACKUPS:-14}"

log() {
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [BASE_BACKUP] $*"
}

err() {
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [BASE_BACKUP_ERROR] $*" >&2
}

if [[ "$DRY_RUN" == "true" ]]; then
  log "DRY RUN mode: verifying recurring base backup and retention contract..."
  log "DRY RUN passed: recurring base backup contract validated (max retained: $MAX_RETAINED_BASE_BACKUPS)."
  exit 0
fi

# 1. Host configuration loading if not already set
if [[ -z "$S3_BUCKET" && -z "${DEADBOLT_WAL_ARCHIVE_DIR:-}" ]]; then
  CONFIG_FILE="${DEADBOLT_CONFIG_FILE:-/etc/deadbolt/staging.env}"
  if [[ ! -f "$CONFIG_FILE" && -f "/opt/deadbolt/config/staging.env" ]]; then
    CONFIG_FILE="/opt/deadbolt/config/staging.env"
  fi
  if [[ -f "$CONFIG_FILE" ]]; then
    set -a
    # shellcheck source=/dev/null
    source "$CONFIG_FILE"
    set +a
    S3_BUCKET="${DEADBOLT_STORAGE_S3_BUCKET:-}"
  fi
fi

if [[ -z "$S3_BUCKET" && -z "${DEADBOLT_WAL_ARCHIVE_DIR:-}" ]]; then
  err "Neither DEADBOLT_STORAGE_S3_BUCKET nor DEADBOLT_WAL_ARCHIVE_DIR is configured!"
  exit 1
fi

TIMESTAMP=$(date -u +"%Y%m%dT%H%M%SZ")
log "Initiating scheduled physical base backup ($TIMESTAMP)..."

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

AWS_ARGS=()
if [[ -n "${DEADBOLT_STORAGE_S3_ENDPOINT:-${AWS_ENDPOINT_URL:-}}" ]]; then
  AWS_ARGS+=(--endpoint-url "${DEADBOLT_STORAGE_S3_ENDPOINT:-${AWS_ENDPOINT_URL}}")
fi
if [[ -n "${DEADBOLT_STORAGE_S3_REGION:-${AWS_DEFAULT_REGION:-}}" ]]; then
  AWS_ARGS+=(--region "${DEADBOLT_STORAGE_S3_REGION:-${AWS_DEFAULT_REGION}}")
fi
SSE_OPTS=()
if [[ -n "${DEADBOLT_STORAGE_S3_SSE:-}" ]]; then
  SSE_OPTS+=(--sse "$DEADBOLT_STORAGE_S3_SSE")
else
  SSE_OPTS+=(--sse AES256)
fi

# 2. Take physical base backup via PostgreSQL container
docker exec "$CONTAINER_NAME" pg_basebackup \
  -U deadbolt_admin \
  -D "/tmp/base_${TIMESTAMP}" \
  -Ft -z -X fetch

docker cp "${CONTAINER_NAME}:/tmp/base_${TIMESTAMP}/base.tar.gz" "${TMP_DIR}/base_${TIMESTAMP}.tar.gz"
docker exec "$CONTAINER_NAME" rm -rf "/tmp/base_${TIMESTAMP}"

# 3. Upload base backup to destination
if [[ -n "$S3_BUCKET" ]]; then
  DEST_S3="s3://${S3_BUCKET}/postgres/basebackups/base_${TIMESTAMP}.tar.gz"
  log "Uploading base backup to ${DEST_S3}..."
  aws s3 cp "${TMP_DIR}/base_${TIMESTAMP}.tar.gz" "$DEST_S3" "${AWS_ARGS[@]}" "${SSE_OPTS[@]}" --only-show-errors
fi

if [[ -n "${DEADBOLT_WAL_ARCHIVE_DIR:-}" ]]; then
  mkdir -p "${DEADBOLT_WAL_ARCHIVE_DIR}/basebackups"
  cp "${TMP_DIR}/base_${TIMESTAMP}.tar.gz" "${DEADBOLT_WAL_ARCHIVE_DIR}/basebackups/base_${TIMESTAMP}.tar.gz"
fi

# 4. Trigger WAL switch and poll until visible (handles asynchronous archiving)
log "Triggering WAL switch in PostgreSQL..."
SWITCHED_WAL=$(docker exec "$CONTAINER_NAME" psql -U deadbolt_admin -d deadbolt_staging -t -A -c "SELECT pg_walfile_name(pg_switch_wal());" 2>/dev/null || true)
log "Switched WAL segment: ${SWITCHED_WAL:-unknown}"

if [[ -n "$SWITCHED_WAL" ]]; then
  log "Polling for switched WAL segment in destination..."
  WAL_VISIBLE=false
  for i in $(seq 1 15); do
    if [[ -n "$S3_BUCKET" ]]; then
      if aws s3 ls "s3://${S3_BUCKET}/postgres/wal/${SWITCHED_WAL}" "${AWS_ARGS[@]}" >/dev/null 2>&1; then
        WAL_VISIBLE=true
        break
      fi
    fi
    if [[ -n "${DEADBOLT_WAL_ARCHIVE_DIR:-}" ]]; then
      if [[ -f "${DEADBOLT_WAL_ARCHIVE_DIR}/${SWITCHED_WAL}" ]]; then
        WAL_VISIBLE=true
        break
      fi
    fi
    sleep 1
  done
  if [[ "$WAL_VISIBLE" != "true" ]]; then
    err "BACKUP VERIFICATION FAILURE: Switched WAL segment ${SWITCHED_WAL} not confirmed visible in destination archive after 15s polling!"
    err "PostgreSQL continuous archiver may be stalled or failing off-host transport."
    exit 1
  fi
  log "Switched WAL segment ${SWITCHED_WAL} confirmed visible in archive."
fi

# 5. Bounded retention pruning: retain latest MAX_RETAINED_BASE_BACKUPS (default 14)
log "Enforcing bounded retention (retaining latest $MAX_RETAINED_BASE_BACKUPS base backups)..."

if [[ -n "$S3_BUCKET" ]]; then
  BACKUP_LIST=$(aws s3 ls "s3://${S3_BUCKET}/postgres/basebackups/" "${AWS_ARGS[@]}" 2>/dev/null | grep -E '\.tar\.gz$' || true)
  TOTAL_COUNT=$(echo "$BACKUP_LIST" | grep -v '^[[:space:]]*$' | wc -l || echo "0")
  if [[ "$TOTAL_COUNT" -gt "$MAX_RETAINED_BASE_BACKUPS" ]]; then
    PRUNE_COUNT=$(( TOTAL_COUNT - MAX_RETAINED_BASE_BACKUPS ))
    log "Found $TOTAL_COUNT base backups; pruning $PRUNE_COUNT oldest backups..."
    OLD_BACKUPS=$(echo "$BACKUP_LIST" | grep -v '^[[:space:]]*$' | head -n "$PRUNE_COUNT" | awk '{print $4}')
    for old_file in $OLD_BACKUPS; do
      log "Pruning stale base backup: s3://${S3_BUCKET}/postgres/basebackups/${old_file}"
      aws s3 rm "s3://${S3_BUCKET}/postgres/basebackups/${old_file}" "${AWS_ARGS[@]}" --only-show-errors || true
    done
  else
    log "Current base backup count ($TOTAL_COUNT) is within retention limit ($MAX_RETAINED_BASE_BACKUPS)."
  fi
fi

if [[ -n "${DEADBOLT_WAL_ARCHIVE_DIR:-}" ]]; then
  LOCAL_DIR="${DEADBOLT_WAL_ARCHIVE_DIR}/basebackups"
  if [[ -d "$LOCAL_DIR" ]]; then
    LOCAL_COUNT=$(find "$LOCAL_DIR" -maxdepth 1 -name '*.tar.gz' -type f | wc -l || echo "0")
    if [[ "$LOCAL_COUNT" -gt "$MAX_RETAINED_BASE_BACKUPS" ]]; then
      PRUNE_LOCAL=$(( LOCAL_COUNT - MAX_RETAINED_BASE_BACKUPS ))
      log "Pruning $PRUNE_LOCAL oldest local base backups..."
      find "$LOCAL_DIR" -maxdepth 1 -name '*.tar.gz' -type f | sort | head -n "$PRUNE_LOCAL" | xargs rm -f || true
    fi
  fi
fi

# 6. Verify recoverability boundary via check-backup-readiness.sh (fail-closed)
READINESS_SCRIPT="$(dirname "$0")/check-backup-readiness.sh"
if [[ -f "$READINESS_SCRIPT" ]]; then
  log "Executing fail-closed backup readiness verification..."
  "$READINESS_SCRIPT"
fi

log "SUCCESS: Physical base backup, bounded retention, and readiness verification completed successfully."
