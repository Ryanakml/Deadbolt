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

# Docker reports digest-pulled images as repo:<none>; IDs plus RepoDigests are
# the only reliable local identity. Never infer safety from mutable tags.
PROTECTED_IMAGE_IDS=()
add_protected_id() {
  local image_id="$1" reason="$2"
  for protected in "${PROTECTED_IMAGE_IDS[@]:-}"; do
    [[ "$protected" == "$image_id" ]] && return 0
  done
  PROTECTED_IMAGE_IDS+=("$image_id")
  log "Protecting image ID $image_id ($reason)"
}

resolve_recorded_release() {
  local release_name="$1" release_ref="$2" image_id
  if [[ ! "$release_ref" =~ ^ghcr\.io/ryanakml/deadbolt/control-plane@sha256:[a-f0-9]{64}$ ]]; then
    err "$release_name release is not an immutable Deadbolt control-plane digest: $release_ref"
    exit 1
  fi
  image_id=$(docker image inspect --format '{{.Id}}' "$release_ref" 2>/dev/null || true)
  if [[ -z "$image_id" ]]; then
    err "$release_name release digest cannot be resolved locally: $release_ref. Refusing retention."
    exit 1
  fi
  add_protected_id "$image_id" "$release_name release $release_ref"
}

for release_name in current previous; do
  release_file="${RELEASE_DIR}/${release_name}"
  if [[ -s "$release_file" ]]; then
    release_ref=$(cat "$release_file" | tr -d '[:space:]')
    resolve_recorded_release "$release_name" "$release_ref"
  fi
done

# Protect image IDs used by every running Deadbolt Compose container.
RUNNING_CONTAINER_IDS=$(docker ps --filter "label=com.docker.compose.project=${PROJECT_NAME}" --format '{{.ID}}' 2>/dev/null || true)
while IFS= read -r container_id; do
  [[ -z "$container_id" ]] && continue
  image_id=$(docker inspect --format '{{.Image}}' "$container_id" 2>/dev/null || true)
  if [[ -z "$image_id" ]]; then
    err "Could not resolve image ID for running Deadbolt container $container_id. Refusing retention."
    exit 1
  fi
  add_protected_id "$image_id" "running Deadbolt container $container_id"
done <<< "$RUNNING_CONTAINER_IDS"

CANDIDATE_IMAGES=()
REMOVABLE_IMAGES=()
ALL_IMAGE_IDS=$(docker image ls --all --no-trunc --format '{{.ID}}' 2>/dev/null || true)
while IFS= read -r image_id; do
  [[ -z "$image_id" ]] && continue
  repo_digests=$(docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$image_id" 2>/dev/null || true)
  candidate_digest=$(echo "$repo_digests" | grep '^ghcr.io/ryanakml/deadbolt/control-plane@sha256:' | head -n 1 || true)
  [[ -z "$candidate_digest" ]] && continue
  CANDIDATE_IMAGES+=("${image_id}|${candidate_digest}")
  is_protected=false
  for protected in "${PROTECTED_IMAGE_IDS[@]:-}"; do
    if [[ "$protected" == "$image_id" ]]; then
      is_protected=true
      break
    fi
  done
  [[ "$is_protected" == "false" ]] && REMOVABLE_IMAGES+=("${image_id}|${candidate_digest}")
done <<< "$ALL_IMAGE_IDS"

TOTAL_CANDIDATES=${#CANDIDATE_IMAGES[@]}

log "Retention preview:"
log "  Total candidate Deadbolt images found: $TOTAL_CANDIDATES"
log "  Protected image IDs retained for rollback: ${#PROTECTED_IMAGE_IDS[@]}"
log "  Images eligible for deletion: ${#REMOVABLE_IMAGES[@]}"

if [[ ${#REMOVABLE_IMAGES[@]} -gt 0 ]]; then
  for rem in "${REMOVABLE_IMAGES[@]}"; do
    log "  -> [DELETE CANDIDATE] image_id=${rem%%|*} digest=${rem#*|}"
  done
fi

if [[ "$DRY_RUN" == "true" ]]; then
  log "DRY RUN complete: Zero images were modified or removed."
  exit 0
fi

if [[ ${#REMOVABLE_IMAGES[@]} -gt 0 ]]; then
  # Execute explicit scoped removal
  for rem in "${REMOVABLE_IMAGES[@]}"; do
    image_id="${rem%%|*}"
    log "Removing scoped Deadbolt image ID: $image_id"
    docker rmi "$image_id" || err "Failed to remove image $image_id (skipping)"
  done
fi

log "Retention sweep completed successfully."
