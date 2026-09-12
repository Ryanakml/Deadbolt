#!/usr/bin/env bash
# Keep a candidate alive unless Caddy has authoritatively returned to the old slot.

restore_edge_route_before_stopping_candidate() {
  local old_port="$1"
  local reload_script="${DEADBOLT_CADDY_RELOAD_SCRIPT:-./scripts/reload-caddy.sh}"

  export DEADBOLT_UPSTREAM_PORT="$old_port"
  if ! "$reload_script" "$old_port"; then
    err "ROUTE RESTORATION FAILED: preserving both $OLD_SLOT and $CANDIDATE_SLOT for operator recovery."
    err "Release state was not advanced; candidate remains running because Caddy may still route to it."
    return 1
  fi

  rollback
}
