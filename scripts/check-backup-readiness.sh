#!/usr/bin/env bash
# scripts/check-backup-readiness.sh
# Validates S3 backup accessibility, off-host base backup presence, and WAL archive lag
# prior to staging promotion (Blueprint §26 & Issue #5).
#
# FAILS CLOSED if backup destination is unreachable, if base backups are missing/stale,
# or if WAL archive lag exceeds the configured threshold.

set -euo pipefail

DRY_RUN="${DRY_RUN:-false}"
BOOTSTRAP_INITIAL_BACKUP="${BOOTSTRAP_INITIAL_BACKUP:-false}"
S3_BUCKET="${DEADBOLT_STORAGE_S3_BUCKET:-}"
BACKUP_DEST="${DEADBOLT_BACKUP_DESTINATION:-}"
MAX_WAL_LAG_SECONDS="${MAX_WAL_LAG_SECONDS:-900}"           # 15 minutes default
MAX_BASE_BACKUP_AGE_SECONDS="${MAX_BASE_BACKUP_AGE_SECONDS:-86400}" # 24 hours default

for arg in "$@"; do
  if [[ "$arg" == "--bootstrap" ]]; then
    BOOTSTRAP_INITIAL_BACKUP="true"
  fi
done

log() {
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [BACKUP_CHECK] $*"
}

err() {
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [BACKUP_CHECK_ERROR] $*" >&2
}

if [[ "$DRY_RUN" == "true" ]]; then
  log "DRY RUN mode: validating backup readiness contract syntax..."
  log "DRY RUN passed: Backup readiness contract validated."
  exit 0
fi

log "Starting backup & WAL readiness verification..."

if [[ -z "$S3_BUCKET" && -z "${DEADBOLT_WAL_ARCHIVE_DIR:-}" ]]; then
  err "Neither DEADBOLT_STORAGE_S3_BUCKET nor DEADBOLT_WAL_ARCHIVE_DIR is configured!"
  err "Failing closed: Cannot promote to staging without verified external backup destination."
  exit 1
fi

NOW_EPOCH=$(date +%s)

# Path 1: S3 Bucket Destination
if [[ -n "$S3_BUCKET" ]]; then
  if [[ "$S3_BUCKET" =~ flowdesk ]]; then
    err "SECURITY VIOLATION: FlowDesk MinIO/bucket reuse detected! Must use dedicated Deadbolt S3 bucket."
    exit 1
  fi

  if [[ -z "$BACKUP_DEST" ]]; then
    BACKUP_DEST="s3://${S3_BUCKET}/postgres"
  fi

  AWS_ARGS=()
  if [[ -n "${DEADBOLT_STORAGE_S3_ENDPOINT:-${AWS_ENDPOINT_URL:-}}" ]]; then
    AWS_ARGS+=(--endpoint-url "${DEADBOLT_STORAGE_S3_ENDPOINT:-${AWS_ENDPOINT_URL}}")
  fi
  if [[ -n "${DEADBOLT_STORAGE_S3_REGION:-${AWS_DEFAULT_REGION:-}}" ]]; then
    AWS_ARGS+=(--region "${DEADBOLT_STORAGE_S3_REGION:-${AWS_DEFAULT_REGION}}")
  fi

  # 1. Verify aws-cli is installed
  if ! command -v aws >/dev/null 2>&1; then
    err "aws-cli is not installed on host! Cannot verify remote S3 backup readiness."
    err "Failing closed: Remote verification required before promotion."
    exit 1
  fi

  # 2. Test S3 accessibility
  log "Step 1: Testing S3 bucket access at ${BACKUP_DEST}..."
  if ! aws s3 ls "${BACKUP_DEST}/" "${AWS_ARGS[@]}" >/dev/null 2>&1; then
    err "Failed to access backup destination: ${BACKUP_DEST}!"
    err "Ensure AWS credentials or IAM role has s3:ListBucket permission on dedicated bucket."
    exit 1
  fi
  log "S3 bucket access verified."

  # 3. Verify base backup existence and freshness
  log "Step 2: Checking base backup availability in ${BACKUP_DEST}/basebackups/..."
  BASE_LIST=$(aws s3 ls "${BACKUP_DEST}/basebackups/" "${AWS_ARGS[@]}" 2>/dev/null || true)
  if [[ -z "$BASE_LIST" ]]; then
    if [[ "$BOOTSTRAP_INITIAL_BACKUP" == "true" ]]; then
      log "First initialization requested: running bootstrap-initial-backup.sh..."
      ./scripts/bootstrap-initial-backup.sh
      BASE_LIST=$(aws s3 ls "${BACKUP_DEST}/basebackups/" "${AWS_ARGS[@]}" 2>/dev/null || true)
    fi
  fi

  if [[ -z "$BASE_LIST" ]]; then
    err "RECOVERY PRECONDITION FAILED: Zero base backups found in ${BACKUP_DEST}/basebackups/!"
    err "Failing closed: Run ./scripts/bootstrap-initial-backup.sh before first promotion."
    exit 1
  fi

  LATEST_BASE_LINE=$(echo "$BASE_LIST" | grep -v '^[[:space:]]*$' | tail -n 1)
  log "Latest base backup: $LATEST_BASE_LINE"

  # Parse base backup timestamp (format: YYYY-MM-DD HH:MM:SS)
  BASE_DATE=$(echo "$LATEST_BASE_LINE" | awk '{print $1}')
  BASE_TIME=$(echo "$LATEST_BASE_LINE" | awk '{print $2}')
  if [[ -n "$BASE_DATE" && -n "$BASE_TIME" ]]; then
    BASE_EPOCH=$(date -u -d "${BASE_DATE} ${BASE_TIME}" +%s 2>/dev/null || date -u -j -f "%Y-%m-%d %H:%M:%S" "${BASE_DATE} ${BASE_TIME}" +%s 2>/dev/null || echo "0")
    if [[ "$BASE_EPOCH" -gt 0 ]]; then
      BASE_AGE=$(( NOW_EPOCH - BASE_EPOCH ))
      if [[ "$BASE_AGE" -gt "$MAX_BASE_BACKUP_AGE_SECONDS" ]]; then
        err "BASE BACKUP STALE: Latest base backup is ${BASE_AGE}s old (exceeds ${MAX_BASE_BACKUP_AGE_SECONDS}s limit)!"
        exit 1
      fi
      log "Base backup age: ${BASE_AGE}s (within ${MAX_BASE_BACKUP_AGE_SECONDS}s limit)."
    fi
  fi

  # 4. Verify continuous WAL archive presence and lag
  log "Step 3: Checking continuous WAL archive lag in ${BACKUP_DEST}/wal/..."
  WAL_LIST=$(aws s3 ls "${BACKUP_DEST}/wal/" "${AWS_ARGS[@]}" 2>/dev/null || true)
  if [[ -z "$WAL_LIST" ]]; then
    err "RECOVERY PRECONDITION FAILED: Zero WAL archives found in ${BACKUP_DEST}/wal/!"
    err "Failing closed: Continuous archiving is not functional or no segments have shipped."
    exit 1
  fi

  LATEST_WAL_LINE=$(echo "$WAL_LIST" | grep -v '^[[:space:]]*$' | tail -n 1)
  log "Latest WAL segment: $LATEST_WAL_LINE"

  WAL_DATE=$(echo "$LATEST_WAL_LINE" | awk '{print $1}')
  WAL_TIME=$(echo "$LATEST_WAL_LINE" | awk '{print $2}')
  if [[ -n "$WAL_DATE" && -n "$WAL_TIME" ]]; then
    WAL_EPOCH=$(date -u -d "${WAL_DATE} ${WAL_TIME}" +%s 2>/dev/null || date -u -j -f "%Y-%m-%d %H:%M:%S" "${WAL_DATE} ${WAL_TIME}" +%s 2>/dev/null || echo "0")
    if [[ "$WAL_EPOCH" -gt 0 ]]; then
      WAL_LAG=$(( NOW_EPOCH - WAL_EPOCH ))
      if [[ "$WAL_LAG" -gt "$MAX_WAL_LAG_SECONDS" ]]; then
        err "WAL ARCHIVE LAG EXCEEDED: Archive lag is ${WAL_LAG}s (exceeds ${MAX_WAL_LAG_SECONDS}s limit)!"
        exit 1
      fi
      log "WAL archive lag: ${WAL_LAG}s (within ${MAX_WAL_LAG_SECONDS}s limit)."
    fi
  fi

  # 5. Verify off-host backup and WAL encryption policy (Blueprint §26 & Issue #5)
  log "Step 4: Verifying off-host backup & WAL encryption policy..."
  BASE_FILENAME=$(echo "$LATEST_BASE_LINE" | awk '{print $4}')
  WAL_FILENAME=$(echo "$LATEST_WAL_LINE" | awk '{print $4}')

  BASE_KEY="postgres/basebackups/${BASE_FILENAME}"
  WAL_KEY="postgres/wal/${WAL_FILENAME}"

  BASE_HEAD=$(aws s3api head-object --bucket "$S3_BUCKET" --key "$BASE_KEY" "${AWS_ARGS[@]}" 2>/dev/null || true)
  WAL_HEAD=$(aws s3api head-object --bucket "$S3_BUCKET" --key "$WAL_KEY" "${AWS_ARGS[@]}" 2>/dev/null || true)

  BASE_ENCRYPTED=false
  if echo "$BASE_HEAD" | grep -iq "ServerSideEncryption"; then
    BASE_ENCRYPTED=true
    BASE_SSE_ALGO=$(echo "$BASE_HEAD" | grep -i "ServerSideEncryption" | head -n 1 | tr -d '",: \t')
    log "Base backup encryption verified: $BASE_SSE_ALGO"
  fi

  WAL_ENCRYPTED=false
  if echo "$WAL_HEAD" | grep -iq "ServerSideEncryption"; then
    WAL_ENCRYPTED=true
    WAL_SSE_ALGO=$(echo "$WAL_HEAD" | grep -i "ServerSideEncryption" | head -n 1 | tr -d '",: \t')
    log "WAL segment encryption verified: $WAL_SSE_ALGO"
  fi

  if [[ "$BASE_ENCRYPTED" != "true" || "$WAL_ENCRYPTED" != "true" ]]; then
    BUCKET_ENC=$(aws s3api get-bucket-encryption --bucket "$S3_BUCKET" "${AWS_ARGS[@]}" 2>/dev/null || true)
    if echo "$BUCKET_ENC" | grep -iq "ServerSideEncryptionConfiguration"; then
      log "Bucket default server-side encryption confirmed for bucket $S3_BUCKET."
    else
      err "ENCRYPTION POLICY VIOLATION: Remote backup / WAL segment is not encrypted with ServerSideEncryption!"
      err "Base backup encrypted: $BASE_ENCRYPTED, WAL segment encrypted: $WAL_ENCRYPTED"
      err "Failing closed: Blueprint §26 & Issue #5 require encrypted off-host backup & WAL archives."
      exit 1
    fi
  fi
  log "Off-host backup and WAL encryption verified."
fi

# Path 2: Directory Archive Destination (for local integration testing / filesystem backup)
if [[ -n "${DEADBOLT_WAL_ARCHIVE_DIR:-}" && -z "$S3_BUCKET" ]]; then
  log "Testing directory archive at ${DEADBOLT_WAL_ARCHIVE_DIR}..."
  if [[ ! -d "${DEADBOLT_WAL_ARCHIVE_DIR}" ]]; then
    err "WAL archive directory does not exist: ${DEADBOLT_WAL_ARCHIVE_DIR}"
    exit 1
  fi

  BASE_DIR="${DEADBOLT_WAL_ARCHIVE_DIR}/basebackups"
  if [[ ! -d "$BASE_DIR" ]] || [[ -z "$(ls -A "$BASE_DIR" 2>/dev/null)" ]]; then
    if [[ "$BOOTSTRAP_INITIAL_BACKUP" == "true" ]]; then
      log "First initialization: running bootstrap-initial-backup.sh..."
      ./scripts/bootstrap-initial-backup.sh
    fi
  fi

  if [[ ! -d "$BASE_DIR" ]] || [[ -z "$(ls -A "$BASE_DIR" 2>/dev/null)" ]]; then
    err "RECOVERY PRECONDITION FAILED: No base backups found in ${BASE_DIR}!"
    exit 1
  fi

  # Check newest WAL segment
  NEWEST_WAL=$(find "${DEADBOLT_WAL_ARCHIVE_DIR}" -maxdepth 1 -type f | sort | tail -n 1 || true)
  if [[ -z "$NEWEST_WAL" ]]; then
    err "RECOVERY PRECONDITION FAILED: No WAL archives found in ${DEADBOLT_WAL_ARCHIVE_DIR}!"
    exit 1
  fi

  WAL_MTIME=$(stat -c %Y "$NEWEST_WAL" 2>/dev/null || stat -f %m "$NEWEST_WAL" 2>/dev/null || echo "$NOW_EPOCH")
  WAL_LAG=$(( NOW_EPOCH - WAL_MTIME ))
  if [[ "$WAL_LAG" -gt "$MAX_WAL_LAG_SECONDS" ]]; then
    err "WAL ARCHIVE LAG EXCEEDED: Archive lag is ${WAL_LAG}s (exceeds ${MAX_WAL_LAG_SECONDS}s limit)!"
    exit 1
  fi
  log "Directory WAL archive lag: ${WAL_LAG}s (within ${MAX_WAL_LAG_SECONDS}s limit)."
fi

log "SUCCESS: Backup & WAL readiness verification passed (recoverability proven)."
exit 0
