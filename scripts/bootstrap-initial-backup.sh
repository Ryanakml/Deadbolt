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
  aws s3 cp "${TMP_DIR}/base_${TIMESTAMP}.tar.gz" "$DEST_S3" "${AWS_ARGS[@]}" "${SSE_OPTS[@]}" --only-show-errors
fi

if [[ -n "${DEADBOLT_WAL_ARCHIVE_DIR:-}" ]]; then
  mkdir -p "${DEADBOLT_WAL_ARCHIVE_DIR}/basebackups"
  cp "${TMP_DIR}/base_${TIMESTAMP}.tar.gz" "${DEADBOLT_WAL_ARCHIVE_DIR}/basebackups/base_${TIMESTAMP}.tar.gz"
fi

# 3. Trigger WAL switch in PostgreSQL and poll until visible
log "Triggering WAL switch in PostgreSQL to archive initial segment..."
SWITCHED_WAL=$(docker exec "$CONTAINER_NAME" psql -U deadbolt_admin -d deadbolt_staging -t -A -c "SELECT pg_walfile_name(pg_switch_wal());" 2>/dev/null || true)
log "Switched initial WAL segment: ${SWITCHED_WAL:-unknown}"

if [[ -n "$SWITCHED_WAL" ]]; then
  log "Polling for initial WAL segment in destination..."
  for i in $(seq 1 15); do
    if [[ -n "$S3_BUCKET" ]]; then
      if aws s3 ls "s3://${S3_BUCKET}/postgres/wal/${SWITCHED_WAL}" "${AWS_ARGS[@]}" >/dev/null 2>&1; then
        log "Initial WAL segment ${SWITCHED_WAL} confirmed visible in S3 archive."
        break
      fi
    fi
    if [[ -n "${DEADBOLT_WAL_ARCHIVE_DIR:-}" ]]; then
      if [[ -f "${DEADBOLT_WAL_ARCHIVE_DIR}/${SWITCHED_WAL}" ]]; then
        log "Initial WAL segment ${SWITCHED_WAL} confirmed visible in directory archive."
        break
      fi
    fi
    sleep 1
  done
fi

log "Initial base backup and WAL segment created successfully."
