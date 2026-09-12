#!/usr/bin/env bash
# Bounded HTTPS edge verification. TLS is intentionally verified normally.

wait_for_candidate_edge_smoke() {
  local domain="$1" candidate_digest="$2"
  local attempts="${DEADBOLT_EDGE_SMOKE_ATTEMPTS:-30}"
  local interval="${DEADBOLT_EDGE_SMOKE_INTERVAL_SECONDS:-2}"
  local response

  for attempt in $(seq 1 "$attempts"); do
    response=$(curl -fsSL "https://${domain}/version" 2>/dev/null || curl -fsSL --resolve "${domain}:443:127.0.0.1" "https://${domain}/version" 2>/dev/null || true)
    if [[ "$response" == *"$candidate_digest"* ]]; then
      log "Edge smoke passed on attempt ${attempt}/${attempts}."
      return 0
    fi
    err "Edge smoke attempt ${attempt}/${attempts} did not return the candidate digest; waiting ${interval}s for TLS/edge readiness."
    sleep "$interval"
  done
  return 1
}
