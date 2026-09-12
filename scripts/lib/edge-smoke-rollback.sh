#!/usr/bin/env bash
# Keep a candidate alive unless Caddy has authoritatively returned to the old slot.

restore_edge_route_before_stopping_candidate() {
  local old_port="$1" prior_route_known="${2:-false}"
  local reload_script="${DEADBOLT_CADDY_RELOAD_SCRIPT:-./scripts/reload-caddy.sh}"

  if [[ "$prior_route_known" == "true" ]]; then
    export DEADBOLT_UPSTREAM_PORT="$old_port"
    "$reload_script" "$old_port" || {
      err "ROUTE RESTORATION FAILED: preserving both $OLD_SLOT and $CANDIDATE_SLOT for operator recovery."
      err "Release state was not advanced; candidate remains running because Caddy may still route to it."
      return 1
    }
  elif ! "$reload_script" --restore-previous-route; then
    err "ROUTE RESTORATION FAILED: preserving both $OLD_SLOT and $CANDIDATE_SLOT for operator recovery."
    err "Release state was not advanced; candidate remains running because Caddy may still route to it."
    return 1
  fi

  rollback
}
