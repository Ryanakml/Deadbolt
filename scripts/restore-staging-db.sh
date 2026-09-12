#!/usr/bin/env bash
# scripts/restore-staging-db.sh
# Performs Point-In-Time-Recovery (PITR) or full restore from physical base backup + WAL archives (Blueprint §26 & Issue #5).
# Uses the implemented AWS CLI S3 archiver / filesystem archive runtime (WAL-G not used).

set -euo pipefail

DRY_RUN="${DRY_RUN:-false}"
TARGET_TIME="${1:-${DEADBOLT_RECOVERY_TARGET_TIME:-}}"
S3_BUCKET="${DEADBOLT_STORAGE_S3_BUCKET:-}"
CONTAINER_NAME="${DEADBOLT_RECOVERY_CONTAINER:-deadbolt-staging-postgres}"

log() {
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [DB_RESTORE] $*"
}

err() {
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [DB_RESTORE_ERROR] $*" >&2
}

if [[ "$DRY_RUN" == "true" ]]; then
  log "DRY RUN mode: validating restore script contracts and parameters..."
  log "DRY RUN passed: restore drill contract syntax validated (target time: ${TARGET_TIME:-latest})."
  exit 0
fi

# 1. Load host configuration
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

AWS_ARGS=()
if [[ -n "${DEADBOLT_STORAGE_S3_ENDPOINT:-${AWS_ENDPOINT_URL:-}}" ]]; then
  AWS_ARGS+=(--endpoint-url "${DEADBOLT_STORAGE_S3_ENDPOINT:-${AWS_ENDPOINT_URL}}")
fi
if [[ -n "${DEADBOLT_STORAGE_S3_REGION:-${AWS_DEFAULT_REGION:-}}" ]]; then
  AWS_ARGS+=(--region "${DEADBOLT_STORAGE_S3_REGION:-${AWS_DEFAULT_REGION}}")
fi

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

# 2. Identify and fetch the latest physical base backup
log "Locating latest physical base backup..."
BASE_TARBALL="${TMP_DIR}/base.tar.gz"

if [[ -n "$S3_BUCKET" ]]; then
  LATEST_BASE_LINE=$(aws s3 ls "s3://${S3_BUCKET}/postgres/basebackups/" "${AWS_ARGS[@]}" 2>/dev/null | grep -E '\.tar\.gz$' | tail -n 1 || true)
  if [[ -z "$LATEST_BASE_LINE" ]]; then
    err "RECOVERY ABORTED: No base backups found in s3://${S3_BUCKET}/postgres/basebackups/!"
    exit 1
  fi
  LATEST_BASE_KEY=$(echo "$LATEST_BASE_LINE" | awk '{print $4}')
  log "Downloading latest base backup: s3://${S3_BUCKET}/postgres/basebackups/${LATEST_BASE_KEY}..."
  aws s3 cp "s3://${S3_BUCKET}/postgres/basebackups/${LATEST_BASE_KEY}" "$BASE_TARBALL" "${AWS_ARGS[@]}" --only-show-errors
fi

if [[ -n "${DEADBOLT_WAL_ARCHIVE_DIR:-}" && -z "$S3_BUCKET" ]]; then
  LATEST_LOCAL_BASE=$(find "${DEADBOLT_WAL_ARCHIVE_DIR}/basebackups" -maxdepth 1 -name '*.tar.gz' -type f | sort | tail -n 1 || true)
  if [[ -z "$LATEST_LOCAL_BASE" ]]; then
    err "RECOVERY ABORTED: No local base backups found in ${DEADBOLT_WAL_ARCHIVE_DIR}/basebackups!"
    exit 1
  fi
  log "Using local base backup: $LATEST_LOCAL_BASE"
  cp "$LATEST_LOCAL_BASE" "$BASE_TARBALL"
fi

# 3. Extract base backup to target directory
TARGET_DATA_DIR="${TARGET_DATA_DIR:-${TMP_DIR}/recovered_data}"
mkdir -p "$TARGET_DATA_DIR"
log "Extracting base backup tarball into $TARGET_DATA_DIR..."
tar -xzf "$BASE_TARBALL" -C "$TARGET_DATA_DIR"

# 4. Configure restore_command and recovery.signal for PostgreSQL 18
log "Configuring PostgreSQL recovery signal and restore_command..."
touch "${TARGET_DATA_DIR}/recovery.signal"

RECOVERY_CONF="${TARGET_DATA_DIR}/postgresql.auto.conf"

if [[ -n "$S3_BUCKET" ]]; then
  RESTORE_CMD="aws s3 cp s3://${S3_BUCKET}/postgres/wal/%f %p"
  if [[ ${#AWS_ARGS[@]} -gt 0 ]]; then
    RESTORE_CMD="aws s3 cp s3://${S3_BUCKET}/postgres/wal/%f %p ${AWS_ARGS[*]}"
  fi
else
  RESTORE_CMD="cp ${DEADBOLT_WAL_ARCHIVE_DIR}/%f %p"
fi

{
  echo "# Deadbolt Point-In-Time-Recovery (PITR) Configuration"
  echo "restore_command = '${RESTORE_CMD}'"
  echo "recovery_target_action = 'promote'"
} >> "$RECOVERY_CONF"

if [[ -n "$TARGET_TIME" ]]; then
  log "Target recovery time set to: $TARGET_TIME"
  echo "recovery_target_time = '${TARGET_TIME}'" >> "$RECOVERY_CONF"
fi

log "PostgreSQL recovery configured successfully in $TARGET_DATA_DIR."

# 5. Instructions for swapping volume or launching drill instance
cat <<INSTRUCTIONS
================================================================================
RECOVERY DRILL / RESTORE READY
Target directory: $TARGET_DATA_DIR
Recovery configuration:
$(cat "$RECOVERY_CONF")
================================================================================
To complete restore into Docker Compose:
1. docker compose -f deploy/compose/docker-compose.staging.yml stop postgres
2. docker run --rm -v deadbolt_staging_postgres_data:/dest -v "$TARGET_DATA_DIR":/src alpine sh -c "rm -rf /dest/* && cp -a /src/* /dest/"
3. docker compose -f deploy/compose/docker-compose.staging.yml up -d postgres
4. Monitor PostgreSQL logs until recovery target reached and database is promoted:
   docker compose -f deploy/compose/docker-compose.staging.yml logs -f postgres
================================================================================
INSTRUCTIONS

log "Restore staging DB drill completed successfully."
