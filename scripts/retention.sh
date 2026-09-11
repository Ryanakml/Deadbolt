#!/usr/bin/env bash
set -euo pipefail

# retention.sh: Scoped Docker image retention policy for Deadbolt staging (Issue #5).
# Strictly scoped to Deadbolt images. FlowDesk images, containers, volumes, and networks
# are explicitly protected and excluded. Rollback window artifacts are preserved.
# Blind docker system prune or docker image prune -a is strictly barred.

DRY_RUN="${DRY_RUN:-false}"
PROJECT_NAME="deadbolt-staging"
RELEASE_DIR="${DEADBOLT_RELEASE_DIR:-/opt/deadbolt/releases}"

log() {
  echo "[RETENTION] $*"
}

err() {
  echo "[RETENTION_ERROR] $*" >&2
}

# Collect protected image IDs (current and previous releases)
PROTECTED_IMAGES=()

if [[ -f "${RELEASE_DIR}/current" ]]; then
  CURRENT_IMAGE=$(cat "${RELEASE_DIR}/current" | tr -d '\n')
  if [[ -n "$CURRENT_IMAGE" ]]; then
    PROTECTED_IMAGES+=("$CURRENT_IMAGE")
    log "Protecting active release image: $CURRENT_IMAGE"
  fi
fi

if [[ -f "${RELEASE_DIR}/previous" ]]; then
  PREVIOUS_IMAGE=$(cat "${RELEASE_DIR}/previous" | tr -d '\n')
  if [[ -n "$PREVIOUS_IMAGE" ]]; then
    PROTECTED_IMAGES+=("$PREVIOUS_IMAGE")
    log "Protecting rollback release image: $PREVIOUS_IMAGE"
  fi
fi

# Protect currently running Deadbolt containers' images
RUNNING_IMAGES=$(docker ps --filter "label=com.docker.compose.project=${PROJECT_NAME}" --format '{{.Image}}' 2>/dev/null || true)
while IFS= read -r img; do
  if [[ -n "$img" ]]; then
    PROTECTED_IMAGES+=("$img")
    log "Protecting running container image: $img"
  fi
done <<< "$RUNNING_IMAGES"

# Inventory candidate Deadbolt images (only images tagged with deadbolt/control-plane or project label)
CANDIDATE_IMAGES=$(docker images --filter "reference=ghcr.io/ryanakml/deadbolt/control-plane" --format '{{.Repository}}:{{.Tag}}' 2>/dev/null || true)

REMOVABLE_IMAGES=()
while IFS= read -r img; do
  if [[ -z "$img" || "$img" == "<none>:<none>" ]]; then
    continue
  fi

  # Explicit safety check: NEVER touch any image matching FlowDesk
  if [[ "$img" =~ flowdesk ]]; then
    log "PROTECTION GUARANTEE: Skipping FlowDesk co-tenant image: $img"
    continue
  fi

  # Check if image is protected
  IS_PROTECTED=false
  if [[ ${#PROTECTED_IMAGES[@]} -gt 0 ]]; then
    for p in "${PROTECTED_IMAGES[@]}"; do
      if [[ "$p" == "$img" || "$p" == *"$img"* || "$img" == *"$p"* ]]; then
        IS_PROTECTED=true
        break
      fi
    done
  fi

  if [[ "$IS_PROTECTED" == "false" ]]; then
    REMOVABLE_IMAGES+=("$img")
  fi
done <<< "$CANDIDATE_IMAGES"

TOTAL_CANDIDATES=0
if [[ -n "$CANDIDATE_IMAGES" ]]; then
  TOTAL_CANDIDATES=$(echo "$CANDIDATE_IMAGES" | grep -c . || echo 0)
fi

log "Retention preview:"
log "  Total candidate Deadbolt images found: $TOTAL_CANDIDATES"
log "  Protected images retained for rollback: ${#PROTECTED_IMAGES[@]}"
log "  Images eligible for deletion: ${#REMOVABLE_IMAGES[@]}"

if [[ ${#REMOVABLE_IMAGES[@]} -gt 0 ]]; then
  for rem in "${REMOVABLE_IMAGES[@]}"; do
    log "  -> [DELETE CANDIDATE] $rem"
  done
fi

if [[ "$DRY_RUN" == "true" ]]; then
  log "DRY RUN complete: Zero images were modified or removed."
  exit 0
fi

if [[ ${#REMOVABLE_IMAGES[@]} -gt 0 ]]; then
  # Execute explicit scoped removal
  for rem in "${REMOVABLE_IMAGES[@]}"; do
    log "Removing scoped Deadbolt image: $rem"
    docker rmi "$rem" || err "Failed to remove image $rem (skipping)"
  done
fi

log "Retention sweep completed successfully."
