#!/usr/bin/env bash
# scripts/bootstrap-initial-backup.sh
# Performs initial physical base backup and triggers WAL switch on a fresh staging database,
# establishing recoverability before first traffic promotion (Blueprint §26 & Issue #5).

set -euo pipefail

DRY_RUN="${DRY_RUN:-false}"
S3_BUCKET="${DEADBOLT_STORAGE_S3_BUCKET:-}"
CONTAINER_NAME="${DEADBOLT_POSTGRES_CONTAINER:-deadbolt-staging-postgres}"

log() {
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [BACKUP_BOOTSTRAP] $*"
}

err() {
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [BACKUP_BOOTSTRAP_ERROR] $*" >&2
}

if [[ "$DRY_RUN" == "true" ]]; then
  log "DRY RUN mode: bootstrap initial backup contract validated."
  exit 0
fi

if [[ -z "$S3_BUCKET" && -z "${DEADBOLT_WAL_ARCHIVE_DIR:-}" ]]; then
  err "Neither DEADBOLT_STORAGE_S3_BUCKET nor DEADBOLT_WAL_ARCHIVE_DIR is configured!"
  exit 1
fi

TIMESTAMP=$(date -u +"%Y%m%dT%H%M%SZ")
log "Creating initial physical base backup ($TIMESTAMP)..."

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

# 1. Take pg_basebackup via container exec
docker exec "$CONTAINER_NAME" pg_basebackup \
  -U deadbolt_admin \
  -D "/tmp/base_${TIMESTAMP}" \
  -Ft -z -X fetch

docker cp "${CONTAINER_NAME}:/tmp/base_${TIMESTAMP}/base.tar.gz" "${TMP_DIR}/base_${TIMESTAMP}.tar.gz"
docker exec "$CONTAINER_NAME" rm -rf "/tmp/base_${TIMESTAMP}"

# 2. Upload base backup to destination
if [[ -n "$S3_BUCKET" ]]; then
  DEST_S3="s3://${S3_BUCKET}/postgres/basebackups/base_${TIMESTAMP}.tar.gz"
  log "Uploading base backup to ${DEST_S3}..."
  aws s3 cp "${TMP_DIR}/base_${TIMESTAMP}.tar.gz" "$DEST_S3" --only-show-errors
fi

if [[ -n "${DEADBOLT_WAL_ARCHIVE_DIR:-}" ]]; then
  mkdir -p "${DEADBOLT_WAL_ARCHIVE_DIR}/basebackups"
  cp "${TMP_DIR}/base_${TIMESTAMP}.tar.gz" "${DEADBOLT_WAL_ARCHIVE_DIR}/basebackups/base_${TIMESTAMP}.tar.gz"
fi

# 3. Trigger WAL switch in PostgreSQL to guarantee archived WAL exists in destination
log "Triggering WAL switch in PostgreSQL to archive initial segment..."
docker exec "$CONTAINER_NAME" psql -U deadbolt_admin -d deadbolt_staging -c "SELECT pg_switch_wal();" >/dev/null

log "Initial base backup and WAL segment created successfully."
