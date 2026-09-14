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

# Proves that the deployed M1 tenant route is mounted through the HTTPS edge.
# No credentials are sent: the expected 401 JSON envelope demonstrates that the
# request reached Deadbolt's authentication boundary rather than a Caddy/CDN
# fallback. TLS verification remains enabled on both bounded attempts.
wait_for_tenant_route_edge_smoke() {
  local domain="$1"
  local attempts="${DEADBOLT_EDGE_SMOKE_ATTEMPTS:-30}"
  local interval="${DEADBOLT_EDGE_SMOKE_INTERVAL_SECONDS:-2}"
  local response_file status

  response_file=$(mktemp)
  for attempt in $(seq 1 "$attempts"); do
    : > "$response_file"
    status=$(curl -sS --max-time 10 -o "$response_file" -w "%{http_code}" "https://${domain}/api/v1/organizations" 2>/dev/null || true)
    if [[ "$status" != "401" ]]; then
      : > "$response_file"
      status=$(curl -sS --max-time 10 --resolve "${domain}:443:127.0.0.1" -o "$response_file" -w "%{http_code}" "https://${domain}/api/v1/organizations" 2>/dev/null || true)
    fi

    if [[ "$status" == "401" ]] && grep -Eq '"code"[[:space:]]*:[[:space:]]*"UNAUTHENTICATED"' "$response_file"; then
      rm -f "$response_file"
      log "Tenant route edge smoke passed: GET /api/v1/organizations -> HTTP 401 (attempt ${attempt}/${attempts})."
      return 0
    fi

    err "Tenant route edge smoke attempt ${attempt}/${attempts} did not return Deadbolt HTTP 401 UNAUTHENTICATED; waiting ${interval}s for TLS/edge readiness."
    sleep "$interval"
  done
  rm -f "$response_file"
  return 1
}
