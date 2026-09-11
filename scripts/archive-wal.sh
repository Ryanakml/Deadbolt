#!/usr/bin/env bash
# scripts/archive-wal.sh
# Invoked by PostgreSQL archive_command: /usr/local/bin/archive-wal.sh %p %f
# Archives closed WAL segments off-host to dedicated S3 bucket (Blueprint §26, ADR-14, Issue #5)
# Fails closed (exit 1) if archive destination is unreachable.

set -euo pipefail

WAL_PATH="${1:-}"
WAL_FILE="${2:-}"

if [[ -z "$WAL_PATH" || -z "$WAL_FILE" ]]; then
  echo "[WAL_ARCHIVE_ERROR] Usage: archive-wal.sh <wal-path> <wal-file>" >&2
  exit 1
fi

# 1. Local development or explicit test bypass
if [[ "${DEADBOLT_WAL_ARCHIVE_DISABLED:-false}" == "true" ]]; then
  exit 0
fi

# 2. Archive to external dedicated S3 bucket via aws-cli if configured
if [[ -n "${DEADBOLT_STORAGE_S3_BUCKET:-}" ]] && command -v aws >/dev/null 2>&1; then
  S3_DEST="s3://${DEADBOLT_STORAGE_S3_BUCKET}/postgres/wal/${WAL_FILE}"
  if aws s3 cp "$WAL_PATH" "$S3_DEST" --only-show-errors; then
    exit 0
  else
    echo "[WAL_ARCHIVE_ERROR] Failed to upload WAL segment $WAL_FILE to $S3_DEST" >&2
    exit 1
  fi
fi

# 3. Archive to local/mounted directory (used in local integration tests and volume archives)
if [[ -n "${DEADBOLT_WAL_ARCHIVE_DIR:-}" ]]; then
  mkdir -p "${DEADBOLT_WAL_ARCHIVE_DIR}"
  if cp "$WAL_PATH" "${DEADBOLT_WAL_ARCHIVE_DIR}/${WAL_FILE}"; then
    exit 0
  else
    echo "[WAL_ARCHIVE_ERROR] Failed to copy WAL segment $WAL_FILE to ${DEADBOLT_WAL_ARCHIVE_DIR}" >&2
    exit 1
  fi
fi

# 4. If wal-g is configured
if [[ -n "${WALG_S3_PREFIX:-}" ]] && command -v wal-g >/dev/null 2>&1; then
  exec wal-g wal-push "$WAL_PATH"
fi

# 5. Fail closed if destination is unconfigured or tool is missing
echo "[WAL_ARCHIVE_ERROR] No valid WAL archive destination or upload tool available for $WAL_FILE!" >&2
exit 1
