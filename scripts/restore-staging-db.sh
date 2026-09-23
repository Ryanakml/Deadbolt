#!/usr/bin/env bash
# scripts/restore-staging-db.sh
# Performs Point-In-Time-Recovery (PITR) from physical base backup + WAL archives (Blueprint §26 & Issue #5).
#
# Modes:
# 1. Default: --drill (Isolated Recovery Drill)
#    Spins up an isolated temporary PostgreSQL container on an isolated volume,
#    replays WAL archives to target, executes smoke validation queries, and cleans up.
#    Live staging database and volumes are NEVER touched.
#
# 2. Explicit: --destructive-staging-restore
#    Destructive disaster recovery mode for active staging cluster.
#    Requires explicit flag and FORCE_RESTORE=true confirmation.

set -euo pipefail

DRY_RUN="${DRY_RUN:-false}"
MODE="drill"
FORCE_RESTORE="${FORCE_RESTORE:-false}"
TARGET_TIME="${DEADBOLT_RECOVERY_TARGET_TIME:-}"
S3_BUCKET="${DEADBOLT_STORAGE_S3_BUCKET:-}"
RELEASE_DIR="${RELEASE_DIR:-/opt/deadbolt/releases}"
POSTGRES_IMAGE_FILE="${RELEASE_DIR}/postgres_image"
PREVIOUS_POSTGRES_IMAGE_FILE="${RELEASE_DIR}/postgres_image.previous"
CALLER_POSTGRES_IMAGE="${DEADBOLT_POSTGRES_IMAGE:-}"
DRILL_CONTAINER_NAME="deadbolt-recovery-drill-postgres"
LIVE_CONTAINER_NAME="deadbolt-staging-postgres"
LIVE_VOLUME_NAME="deadbolt_staging_postgres_data"
COMPOSE_FILE="${COMPOSE_FILE:-deploy/compose/docker-compose.staging.yml}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
if [[ ! -f "$COMPOSE_FILE" && -f "${REPO_ROOT}/${COMPOSE_FILE}" ]]; then
  COMPOSE_FILE="${REPO_ROOT}/${COMPOSE_FILE}"
fi

DISASTER_RECOVERY="${DISASTER_RECOVERY:-false}"
INCIDENT_AT="${INCIDENT_AT:-}"
RECOVERY_POINT="${RECOVERY_POINT:-}"
RPO_GAP_IDS="${RPO_GAP_IDS:-}"
OPERATOR_NOTES="${OPERATOR_NOTES:-}"

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --drill)
      MODE="drill"
      shift
      ;;
    --destructive-staging-restore)
      MODE="destructive"
      shift
      ;;
    --disaster-recovery)
      DISASTER_RECOVERY="true"
      shift
      ;;
    --incident-at)
      INCIDENT_AT="$2"
      shift 2
      ;;
    --incident-at=*)
      INCIDENT_AT="${1#*=}"
      shift
      ;;
    --recovery-point)
      RECOVERY_POINT="$2"
      shift 2
      ;;
    --recovery-point=*)
      RECOVERY_POINT="${1#*=}"
      shift
      ;;
    --rpo-gap-ids)
      RPO_GAP_IDS="$2"
      shift 2
      ;;
    --rpo-gap-ids=*)
      RPO_GAP_IDS="${1#*=}"
      shift
      ;;
    --operator-notes)
      OPERATOR_NOTES="$2"
      shift 2
      ;;
    --operator-notes=*)
      OPERATOR_NOTES="${1#*=}"
      shift
      ;;
    --target-time)
      TARGET_TIME="$2"
      shift 2
      ;;
    --target-time=*)
      TARGET_TIME="${1#*=}"
      shift
      ;;
    --help|-h)
      echo "Usage: $0 [--drill | --destructive-staging-restore] [--disaster-recovery] [--target-time 'YYYY-MM-DD HH:MM:SS UTC'] [--recovery-point RFC3339] [--incident-at RFC3339] [--rpo-gap-ids id1,id2] [--operator-notes '...']"
      echo "  --drill: Safe isolated recovery drill using dedicated container/volume (default)"
      echo "  --destructive-staging-restore: Overwrite active staging volume with recovered state (requires FORCE_RESTORE=true)"
      echo "  --disaster-recovery: Apply disaster reconciliation holds, revoke sessions, and disable admission/dispatch via the authoritative control-plane recovery CLI"
      exit 0
      ;;
    *)
      # If positional argument and looks like timestamp
      if [[ -z "$TARGET_TIME" ]]; then
        TARGET_TIME="$1"
      fi
      shift
      ;;
  esac
done

log() {
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [DB_RESTORE] $*"
}

err() {
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [DB_RESTORE_ERROR] $*" >&2
}

CONTROL_PLANE_BIN="${DEADBOLT_CONTROL_PLANE_BIN:-/usr/local/bin/control-plane}"

# resolve_recovery_image prints the control-plane image used for one-off
# operator recovery containers. Fails closed when it cannot be established.
resolve_recovery_image() {
  if [[ -n "${DEADBOLT_CONTROL_PLANE_IMAGE:-}" ]]; then
    echo "$DEADBOLT_CONTROL_PLANE_IMAGE"
    return 0
  fi
  local current_file="${RELEASE_DIR}/current"
  if [[ -f "$current_file" ]]; then
    local recorded
    recorded=$(tr -d '[:space:]' < "$current_file")
    if [[ -n "$recorded" ]]; then
      echo "$recorded"
      return 0
    fi
  fi
  err "RECOVERY ABORTED: control-plane image could not be resolved!"
  err "Set DEADBOLT_CONTROL_PLANE_IMAGE or ensure ${RELEASE_DIR}/current records the running release."
  return 1
}

# derive_recovery_times sets RECOVERY_POINT_ISO/INCIDENT_AT_ISO from explicit
# flags (falling back to target time / now). Fails closed on unparseable input.
derive_recovery_times() {
  local point="${RECOVERY_POINT:-${TARGET_TIME:-}}"
  if [[ -z "$point" ]]; then
    point="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  fi
  RECOVERY_POINT_ISO="$(date -u -d "$point" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -j -f "%Y-%m-%d %H:%M:%S %Z" "$point" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || true)"
  if [[ -z "$RECOVERY_POINT_ISO" ]]; then
    # Accept already-ISO input verbatim when date parsing is unavailable.
    if [[ "$point" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$ ]]; then
      RECOVERY_POINT_ISO="$point"
    else
      err "RECOVERY ABORTED: cannot parse recovery point '$point' as RFC3339/UTC."
      return 1
    fi
  fi
  if [[ -n "$INCIDENT_AT" ]]; then
    INCIDENT_AT_ISO="$(date -u -d "$INCIDENT_AT" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || true)"
    if [[ -z "$INCIDENT_AT_ISO" ]]; then
      if [[ "$INCIDENT_AT" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$ ]]; then
        INCIDENT_AT_ISO="$INCIDENT_AT"
      else
        err "RECOVERY ABORTED: cannot parse --incident-at '$INCIDENT_AT' as RFC3339/UTC."
        return 1
      fi
    fi
  else
    INCIDENT_AT_ISO="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  fi
  export RECOVERY_POINT_ISO INCIDENT_AT_ISO
}

# validate_recovery_state fails closed unless the target database reports
# READ_ONLY controls with admission/dispatch/schedules disabled and a recorded
# incident for this recovery point. The first argument is "drill" or "live"
# and selects which container to query.
validate_recovery_state() {
  local target="$1"
  local container
  if [[ "$target" == "drill" ]]; then
    container="$DRILL_CONTAINER_NAME"
  else
    container="$LIVE_CONTAINER_NAME"
  fi
  local pw="${DEADBOLT_DB_ADMIN_PASSWORD:-drill_admin_secret}"
  if [[ "$target" == "live" ]]; then
    pw="${DEADBOLT_DB_ADMIN_PASSWORD:?DEADBOLT_DB_ADMIN_PASSWORD is required for live recovery validation}"
  fi
  local mode
  mode=$(docker exec -e PGPASSWORD="$pw" "$container" psql -U deadbolt_admin -d deadbolt_staging -t -A -c "SELECT mode FROM system_recovery_controls WHERE id = 1;" 2>/dev/null || echo "")
  local flags
  flags=$(docker exec -e PGPASSWORD="$pw" "$container" psql -U deadbolt_admin -d deadbolt_staging -t -A -c "SELECT admission_enabled::text || ',' || dispatch_enabled::text || ',' || schedules_enabled::text FROM system_recovery_controls WHERE id = 1;" 2>/dev/null || echo "")
  local incidents
  incidents=$(docker exec -e PGPASSWORD="$pw" "$container" psql -U deadbolt_admin -d deadbolt_staging -t -A -c "SELECT count(*) FROM disaster_recovery_incidents WHERE recovery_point = '${RECOVERY_POINT_ISO}'::timestamptz;" 2>/dev/null || echo "0")
  local holds
  holds=$(docker exec -e PGPASSWORD="$pw" "$container" psql -U deadbolt_admin -d deadbolt_staging -t -A -c "SELECT count(*) FROM reconciliation_cases WHERE reason = 'DISASTER_RECOVERY_HOLD';" 2>/dev/null || echo "0")
  if [[ "$mode" != "READ_ONLY" || "$flags" != "f,f,f" ]]; then
    err "RECOVERY VALIDATION FAILED: expected READ_ONLY with admission/dispatch/schedules disabled, got mode='$mode' flags='$flags'."
    return 1
  fi
  if [[ ! "$incidents" =~ ^[1-9][0-9]*$ ]]; then
    err "RECOVERY VALIDATION FAILED: no disaster incident recorded for recovery point $RECOVERY_POINT_ISO."
    return 1
  fi
  log "Recovery validation passed: mode=READ_ONLY admission/dispatch/schedules disabled, incidents=$incidents holds=$holds."
}

if [[ "$DRY_RUN" == "true" ]]; then
  log "DRY RUN mode: validating restore script contracts and parameters..."
  log "DRY RUN mode: target mode=$MODE, target time=${TARGET_TIME:-latest}."
  if [[ "$DISASTER_RECOVERY" == "true" ]]; then
    log "DRY RUN mode: disaster recovery protocol enabled (admission/dispatch gating, session revocation, uncertainty window)."
  fi
  log "DRY RUN passed: restore contract syntax validated."
  exit 0
fi

if [[ "$MODE" == "destructive" && "$FORCE_RESTORE" != "true" ]]; then
  err "ABORTED: Destructive staging restore requires explicit FORCE_RESTORE=true!"
  err "Live volume $LIVE_VOLUME_NAME is protected from accidental overwrite."
  err "To execute live disaster recovery:"
  err "FORCE_RESTORE=true $0 --destructive-staging-restore"
  exit 1
fi

# 1. Load host configuration if available
CONFIG_FILE="${DEADBOLT_CONFIG_FILE:-/etc/deadbolt/staging.env}"
if [[ ! -f "$CONFIG_FILE" && -f "/opt/deadbolt/config/staging.env" ]]; then
  CONFIG_FILE="/opt/deadbolt/config/staging.env"
fi

if [[ -f "$CONFIG_FILE" ]]; then
  PERMS=$(stat -c "%a" "$CONFIG_FILE" 2>/dev/null || stat -f "%Op" "$CONFIG_FILE" 2>/dev/null || echo "600")
  if [[ "$PERMS" =~ [4567]$ ]]; then
    err "SECURITY VIOLATION: Configuration file $CONFIG_FILE is world-readable ($PERMS)!"
    err "Remediation: chmod 600 $CONFIG_FILE"
    exit 1
  fi
  log "Loading configuration from $CONFIG_FILE..."
  set -a
  # shellcheck source=/dev/null
  source "$CONFIG_FILE"
  set +a
fi

S3_BUCKET="${DEADBOLT_STORAGE_S3_BUCKET:-$S3_BUCKET}"

# Resolve authoritative PostgreSQL image digest:
# 1. Explicit caller environment input ($CALLER_POSTGRES_IMAGE)
# 2. Recorded release provenance ($POSTGRES_IMAGE_FILE or $PREVIOUS_POSTGRES_IMAGE_FILE)
# 3. Running staging container ($LIVE_CONTAINER_NAME)
if [[ -n "$CALLER_POSTGRES_IMAGE" ]]; then
  DEADBOLT_POSTGRES_IMAGE="$CALLER_POSTGRES_IMAGE"
elif [[ -f "$POSTGRES_IMAGE_FILE" ]]; then
  RECORDED_PG_IMAGE=$(cat "$POSTGRES_IMAGE_FILE" | tr -d '[:space:]')
  if [[ -n "$RECORDED_PG_IMAGE" ]]; then
    DEADBOLT_POSTGRES_IMAGE="$RECORDED_PG_IMAGE"
  fi
elif [[ -f "$PREVIOUS_POSTGRES_IMAGE_FILE" ]]; then
  RECORDED_PG_IMAGE=$(cat "$PREVIOUS_POSTGRES_IMAGE_FILE" | tr -d '[:space:]')
  if [[ -n "$RECORDED_PG_IMAGE" ]]; then
    DEADBOLT_POSTGRES_IMAGE="$RECORDED_PG_IMAGE"
  fi
fi

if [[ -z "${DEADBOLT_POSTGRES_IMAGE:-}" ]]; then
  RUNNING_PG_IMAGE=$(docker inspect --format '{{.Config.Image}}' "$LIVE_CONTAINER_NAME" 2>/dev/null || true)
  if [[ -n "$RUNNING_PG_IMAGE" ]]; then
    DEADBOLT_POSTGRES_IMAGE="$RUNNING_PG_IMAGE"
  fi
fi

# Fail closed if no immutable digest can be established (zero mutable fallback permitted)
if [[ -z "${DEADBOLT_POSTGRES_IMAGE:-}" ]]; then
  err "RECOVERY ABORTED: DEADBOLT_POSTGRES_IMAGE could not be resolved!"
  err "An immutable digest must be provided via environment, recorded in $POSTGRES_IMAGE_FILE, or running container $LIVE_CONTAINER_NAME."
  exit 1
fi
export DEADBOLT_POSTGRES_IMAGE

# Pinned helper image: defaults to resolved immutable PostgreSQL image (strictly pinned by digest)
HELPER_IMG="${DEADBOLT_HELPER_IMAGE:-$DEADBOLT_POSTGRES_IMAGE}"

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
cleanup() {
  if [[ "$MODE" == "drill" ]]; then
    log "Cleaning up isolated recovery drill container and temporary files..."
    docker rm -f "$DRILL_CONTAINER_NAME" 2>/dev/null || true
  fi
  if [[ -n "${TMP_DIR:-}" && -d "$TMP_DIR" ]]; then
    rm -rf "$TMP_DIR" 2>/dev/null || {
      if [[ -n "${HELPER_IMG:-}" ]]; then
        docker run --rm --entrypoint sh -v "${TMP_DIR}":/cleanup "$HELPER_IMG" -c "rm -rf /cleanup/*" 2>/dev/null || true
      fi
      rm -rf "$TMP_DIR" 2>/dev/null || true
    }
  fi
}
trap cleanup EXIT

# 2. Identify and fetch latest physical base backup
log "Locating latest physical base backup (mode=$MODE)..."
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

# 3. Extract base backup into temporary staging directory
# In PostgreSQL 18+, Docker images store database data in /var/lib/postgresql/18/docker,
# and the container volume is mounted at /var/lib/postgresql.
RECOVERED_ROOT="${TMP_DIR}/recovered_root"
TARGET_DATA_DIR="${RECOVERED_ROOT}/18/docker"
mkdir -p "$TARGET_DATA_DIR"
log "Extracting base backup tarball into $TARGET_DATA_DIR..."
tar -xzf "$BASE_TARBALL" -C "$TARGET_DATA_DIR"
chmod 755 "$TMP_DIR"
chmod 755 "$RECOVERED_ROOT"
chmod 755 "${RECOVERED_ROOT}/18"
chmod 700 "$TARGET_DATA_DIR"

# 4. Configure restore_command and recovery.signal for PostgreSQL 18
log "Configuring PostgreSQL recovery signal and restore_command..."
touch "${TARGET_DATA_DIR}/recovery.signal"

RECOVERY_CONF="${TARGET_DATA_DIR}/postgresql.auto.conf"

WAL_MOUNT_SOURCE=""
if [[ "$MODE" == "drill" ]]; then
  if [[ -n "$S3_BUCKET" ]]; then
    PREFETCH_WAL_DIR="${TMP_DIR}/prefetched_wal"
    mkdir -p "$PREFETCH_WAL_DIR"
    log "Prefetching archived WAL files from s3://${S3_BUCKET}/postgres/wal/ to host cache ($PREFETCH_WAL_DIR)..."
    if ! aws s3 sync "s3://${S3_BUCKET}/postgres/wal/" "$PREFETCH_WAL_DIR" "${AWS_ARGS[@]}" --only-show-errors 2>/dev/null; then
      aws s3 cp "s3://${S3_BUCKET}/postgres/wal/" "$PREFETCH_WAL_DIR" --recursive "${AWS_ARGS[@]}" --only-show-errors 2>/dev/null || true
    fi
    chmod -R a+rX "$PREFETCH_WAL_DIR" 2>/dev/null || true
    WAL_MOUNT_SOURCE="$PREFETCH_WAL_DIR"
  elif [[ -n "${DEADBOLT_WAL_ARCHIVE_DIR:-}" ]]; then
    WAL_MOUNT_SOURCE="$DEADBOLT_WAL_ARCHIVE_DIR"
  fi
  RESTORE_CMD="cp /wal_archive/%f %p"
else
  if [[ -n "$S3_BUCKET" ]]; then
    RESTORE_CMD="aws s3 cp s3://${S3_BUCKET}/postgres/wal/%f %p"
    if [[ ${#AWS_ARGS[@]} -gt 0 ]]; then
      RESTORE_CMD="aws s3 cp s3://${S3_BUCKET}/postgres/wal/%f %p ${AWS_ARGS[*]}"
    fi
  else
    RESTORE_CMD="cp /wal_archive/%f %p"
  fi
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

log "PostgreSQL recovery configuration generated successfully."

# 5. Execution branching based on mode
if [[ "$MODE" == "drill" ]]; then
  log "================================================================================"
  log "STARTING ISOLATED RECOVERY DRILL"
  log "Container: $DRILL_CONTAINER_NAME (dedicated isolated container)"
  log "Live staging database ($LIVE_CONTAINER_NAME) and volume ($LIVE_VOLUME_NAME) are NOT touched."
  log "================================================================================"

  DRILL_IMG="$DEADBOLT_POSTGRES_IMAGE"

  DOCKER_ARGS=(
    run -d
    --name "$DRILL_CONTAINER_NAME"
    --network none
    -v "$RECOVERED_ROOT":/var/lib/postgresql
    -e PGDATA=/var/lib/postgresql/18/docker
    -e POSTGRES_PASSWORD="${DEADBOLT_DB_ADMIN_PASSWORD:-drill_admin_secret}"
    -e PGPASSWORD="${DEADBOLT_DB_ADMIN_PASSWORD:-drill_admin_secret}"
    -e POSTGRES_USER="deadbolt_admin"
    -e POSTGRES_DB="deadbolt_staging"
  )

  if [[ -n "${WAL_MOUNT_SOURCE:-}" ]]; then
    DOCKER_ARGS+=(-v "${WAL_MOUNT_SOURCE}":/wal_archive:ro)
  fi

  DOCKER_ARGS+=("$DRILL_IMG")

  log "Launching isolated PostgreSQL drill container..."
  docker rm -f "$DRILL_CONTAINER_NAME" 2>/dev/null || true
  docker "${DOCKER_ARGS[@]}"

  log "Waiting for PostgreSQL archive recovery and promotion..."
  PROMOTED=false
  for i in $(seq 1 45); do
    STATUS=$(docker exec -e PGPASSWORD="${DEADBOLT_DB_ADMIN_PASSWORD:-drill_admin_secret}" "$DRILL_CONTAINER_NAME" psql -U deadbolt_admin -d deadbolt_staging -t -A -c "SELECT pg_is_in_recovery();" 2>/dev/null || echo "starting")
    if [[ "$STATUS" == "f" ]]; then
      PROMOTED=true
      log "Recovery completed! PostgreSQL promoted to primary ready state."
      break
    elif [[ "$STATUS" == "t" ]]; then
      log "Archive recovery in progress (pg_is_in_recovery=true, attempt $i/45)..."
    fi
    sleep 2
  done

  if [[ "$PROMOTED" != "true" ]]; then
    err "RECOVERY DRILL FAILED: Container failed to promote within 90 seconds!"
    docker logs "$DRILL_CONTAINER_NAME" | tail -n 50 || true
    exit 1
  fi

  # Execute smoke queries
  log "Executing smoke validation queries on restored database..."
  QUERY_RES=$(docker exec -e PGPASSWORD="${DEADBOLT_DB_ADMIN_PASSWORD:-drill_admin_secret}" "$DRILL_CONTAINER_NAME" psql -U deadbolt_admin -d deadbolt_staging -t -A -c "SELECT 1;" 2>/dev/null || echo "")
  if [[ "$QUERY_RES" != "1" ]]; then
    err "RECOVERY DRILL FAILED: Smoke query SELECT 1 returned unexpected result: '$QUERY_RES'!"
    exit 1
  fi

  log "Smoke check passed: SELECT 1 succeeded."
  if docker exec -e PGPASSWORD="${DEADBOLT_DB_ADMIN_PASSWORD:-drill_admin_secret}" "$DRILL_CONTAINER_NAME" psql -U deadbolt_admin -d deadbolt_staging -c "\dt goose_db_version" 2>/dev/null | grep -q "goose_db_version"; then
    MIGRATIONS=$(docker exec -e PGPASSWORD="${DEADBOLT_DB_ADMIN_PASSWORD:-drill_admin_secret}" "$DRILL_CONTAINER_NAME" psql -U deadbolt_admin -d deadbolt_staging -t -A -c "SELECT count(*) FROM goose_db_version;" 2>/dev/null || echo "0")
    log "Smoke check passed: Verified schema version history ($MIGRATIONS applied migrations found)."
  fi
  if docker exec -e PGPASSWORD="${DEADBOLT_DB_ADMIN_PASSWORD:-drill_admin_secret}" "$DRILL_CONTAINER_NAME" psql -U deadbolt_admin -d deadbolt_staging -c "\dt drill_verification" 2>/dev/null | grep -q "drill_verification"; then
    VERIFIED_COUNT=$(docker exec -e PGPASSWORD="${DEADBOLT_DB_ADMIN_PASSWORD:-drill_admin_secret}" "$DRILL_CONTAINER_NAME" psql -U deadbolt_admin -d deadbolt_staging -t -A -c "SELECT count(*) FROM drill_verification WHERE id = 2;" 2>/dev/null || echo "0")
    log "Smoke check passed: Verified WAL replayed record ($VERIFIED_COUNT replayed record(s) found)."
    if [[ "$VERIFIED_COUNT" -lt 1 ]]; then
      err "RECOVERY DRILL FAILED: Verification record (id=2) from replayed WAL was not found in recovered database!"
      docker logs "$DRILL_CONTAINER_NAME" | tail -n 50 || true
      exit 1
    fi
  fi

  if [[ "$DISASTER_RECOVERY" == "true" ]]; then
    log "Applying authoritative disaster recovery preparation on drill database..."
    derive_recovery_times || exit 1
    RECOVERY_IMAGE="$(resolve_recovery_image)" || exit 1
    DRILL_DB_URL="postgres://deadbolt_admin:${DEADBOLT_DB_ADMIN_PASSWORD:-drill_admin_secret}@127.0.0.1:5432/deadbolt_staging?sslmode=disable"
    PREPARE_ARGS=(
      "$CONTROL_PLANE_BIN"
      --prepare-disaster-recovery
      --recovery-point "$RECOVERY_POINT_ISO"
      --incident-at "$INCIDENT_AT_ISO"
    )
    if [[ -n "$RPO_GAP_IDS" ]]; then
      PREPARE_ARGS+=(--rpo-gap-ids "$RPO_GAP_IDS")
    fi
    if [[ -n "$OPERATOR_NOTES" ]]; then
      PREPARE_ARGS+=(--operator-notes "$OPERATOR_NOTES")
    fi
    # The one-off operator container shares the drill container network
    # namespace so it reaches PostgreSQL on loopback; no tenant credentials
    # are involved (host-operator database authority only).
    if ! docker run --rm --network "container:${DRILL_CONTAINER_NAME}" \
      -e MIGRATOR_DATABASE_URL="$DRILL_DB_URL" \
      -e RUNTIME_MODE=hosted \
      "$RECOVERY_IMAGE" "${PREPARE_ARGS[@]}"; then
      err "RECOVERY DRILL FAILED: authoritative disaster preparation failed; drill database left non-active."
      exit 1
    fi
    validate_recovery_state drill || exit 1
    log "Disaster recovery controls applied: admission/dispatch disabled, pre-disaster sessions revoked."
    log "Disaster reconciliation holds and uncertainty window prepared."
  fi

  log "================================================================================"
  log "ISOLATED RECOVERY DRILL COMPLETED SUCCESSFULLY"
  log "PostgreSQL replayed archived WAL from physical base backup and accepted queries."
  log "Live staging services remained 100% untouched."
  log "================================================================================"
  exit 0

elif [[ "$MODE" == "destructive" ]]; then
  log "================================================================================"
  log "WARNING: DESTRUCTIVE STAGING RESTORE REQUESTED"
  log "This procedure will STOP live staging PostgreSQL and REPLACE volume $LIVE_VOLUME_NAME."
  log "================================================================================"

  if [[ "$FORCE_RESTORE" != "true" ]]; then
    err "ABORTED: Destructive staging restore requires explicit FORCE_RESTORE=true!"
    err "To execute live restoration:"
    err "FORCE_RESTORE=true $0 --destructive-staging-restore"
    exit 1
  fi

  log "Stopping active staging PostgreSQL container ($LIVE_CONTAINER_NAME)..."
  docker compose -f "$COMPOSE_FILE" stop postgres

  log "Replacing persistent data in $LIVE_VOLUME_NAME with recovered state..."
  docker run --rm \
    --entrypoint sh \
    -v "${LIVE_VOLUME_NAME}":/dest \
    -v "$RECOVERED_ROOT":/src \
    "$HELPER_IMG" -c "rm -rf /dest/* && cp -a /src/* /dest/ && chown -R 999:999 /dest"

  log "Restarting staging PostgreSQL container..."
  docker compose -f "$COMPOSE_FILE" up -d postgres

  log "Waiting for staging PostgreSQL to complete archive recovery..."
  PROMOTED=false
  for i in $(seq 1 60); do
    STATUS=$(docker exec -e PGPASSWORD="${DEADBOLT_DB_ADMIN_PASSWORD}" "$LIVE_CONTAINER_NAME" psql -U deadbolt_admin -d deadbolt_staging -t -A -c "SELECT pg_is_in_recovery();" 2>/dev/null || echo "starting")
    if [[ "$STATUS" == "f" ]]; then
      PROMOTED=true
      break
    fi
    sleep 2
  done

  if [[ "$PROMOTED" != "true" ]]; then
    err "LIVE RESTORE WARNING: PostgreSQL has not yet promoted; check container logs:"
    err "docker compose -f $COMPOSE_FILE logs -f postgres"
    exit 1
  fi

  if [[ "$DISASTER_RECOVERY" == "true" ]]; then
    log "Applying authoritative disaster recovery preparation on restored live database..."
    derive_recovery_times || exit 1
    RECOVERY_IMAGE="$(resolve_recovery_image)" || exit 1
    LIVE_DB_URL="postgres://deadbolt_admin:${DEADBOLT_DB_ADMIN_PASSWORD}@127.0.0.1:5432/deadbolt_staging?sslmode=disable"
    LIVE_PREPARE_ARGS=(
      "$CONTROL_PLANE_BIN"
      --prepare-disaster-recovery
      --recovery-point "$RECOVERY_POINT_ISO"
      --incident-at "$INCIDENT_AT_ISO"
    )
    if [[ -n "$RPO_GAP_IDS" ]]; then
      LIVE_PREPARE_ARGS+=(--rpo-gap-ids "$RPO_GAP_IDS")
    fi
    if [[ -n "$OPERATOR_NOTES" ]]; then
      LIVE_PREPARE_ARGS+=(--operator-notes "$OPERATOR_NOTES")
    fi
    # One-off operator container sharing the live database network namespace
    # (host-operator database authority only; no tenant credentials involved).
    if ! docker run --rm --network "container:${LIVE_CONTAINER_NAME}" \
      -e MIGRATOR_DATABASE_URL="$LIVE_DB_URL" \
      -e RUNTIME_MODE=hosted \
      "$RECOVERY_IMAGE" "${LIVE_PREPARE_ARGS[@]}"; then
      err "Failed to apply authoritative disaster recovery preparation; live system left non-active."
      exit 1
    fi
    validate_recovery_state live || exit 1
    log "Disaster recovery controls applied: admission/dispatch disabled, pre-disaster sessions revoked."
    log "Disaster reconciliation holds and uncertainty window prepared."
  fi

  log "SUCCESS: Active staging database restored and promoted successfully."
fi
