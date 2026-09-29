#!/usr/bin/env bash
# scripts/m4-gate-staging-journeys.sh
# Staging evidence for PR #87 (Issue #36, M4 gate).
#
# Runs ON the staging host (direct SSH). Proves the combined waits survive
# a real control-plane process restart: fixtures are planted, the ACTIVE
# control-plane container is restarted (same image), and every wait is
# settled by the NEW process. Isolated project `m4-gate-accept` in the
# `Deadbolt Acceptance` org; shared staging env untouched.
# Exit non-zero on any failed assertion; evidence prints to stdout.

set -euo pipefail

PROJECT_NAME="m4-gate-accept"

CONFIG_FILE="${DEADBOLT_CONFIG_FILE:-/etc/deadbolt/staging.env}"
if [[ -f "$CONFIG_FILE" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "$CONFIG_FILE"
  set +a
fi

pass() { echo "[GATE-PASS] $*"; }
info() { echo "[GATE-INFO] $*"; }
fail() {
  echo "[GATE-FAIL] $*" >&2
  exit 1
}

PSQL="docker exec -i deadbolt-staging-postgres psql -U deadbolt_admin -d deadbolt_staging -v ON_ERROR_STOP=1 -At"
q() { PGPASSWORD="$DEADBOLT_DB_ADMIN_PASSWORD" $PSQL -c "$1"; }
q1() { q "$1" | head -n 1 | tr -d '[:space:]'; }

ACTIVE_SLOT="$(cat /opt/deadbolt/releases/active_slot 2>/dev/null | tr -d '[:space:]' || echo green)"
if [[ "$ACTIVE_SLOT" == "blue" ]]; then PORT="8088"; else PORT="8089"; fi
CONTAINER="deadbolt-staging-control-plane-${ACTIVE_SLOT}"
BASE="http://127.0.0.1:${PORT}"

VERSION_JSON="$(curl -fsSL --max-time 20 "$BASE/version")"
info "slot=$ACTIVE_SLOT version=$VERSION_JSON"
COMMIT="$(echo "$VERSION_JSON" | python3 -c 'import json,sys; print(json.load(sys.stdin)["commit"])')"
DOMAIN="${DEADBOLT_STAGING_DOMAIN:-staging.deadbolt.cloud}"
ORIGIN="https://$DOMAIN"

ORG_ID="$(q1 "SELECT id::text FROM organizations WHERE name='Deadbolt Acceptance' LIMIT 1")"
[[ -n "$ORG_ID" ]] || fail "org 'Deadbolt Acceptance' not found"

# Fixture project + env + admission row.
q "DELETE FROM projects WHERE organization_id='$ORG_ID'::uuid AND name='$PROJECT_NAME'" >/dev/null
PROJ_ID="$(q1 "INSERT INTO projects (organization_id, name) VALUES ('$ORG_ID'::uuid, '$PROJECT_NAME') RETURNING id::text")"
ENV_ID="$(q1 "INSERT INTO environments (organization_id, project_id, name) VALUES ('$ORG_ID'::uuid, '$PROJ_ID'::uuid, 'staging') RETURNING id::text")"
q "INSERT INTO environment_admissions (environment_id, organization_id, max_concurrency) VALUES ('$ENV_ID'::uuid, '$ORG_ID'::uuid, 10)" >/dev/null
info "proj=$PROJ_ID env=$ENV_ID commit=$COMMIT"

# Machine key: everything except human-only decisions.
mkkey() {
  local caps="$1"
  local prefix="db_staging_$(openssl rand -hex 4)"
  local secret
  secret="$(openssl rand -base64 32 | tr -d '\n')"
  local plain="${prefix}_${secret}"
  local hashed
  hashed="$(printf '%s' "$plain" | sha256sum | cut -d' ' -f1)"
  q "INSERT INTO api_keys (organization_id, environment_id, prefix, hashed_secret, capabilities, expires_at) VALUES ('$ORG_ID'::uuid, '$ENV_ID'::uuid, '$prefix', '$hashed', '{$caps}', clock_timestamp() + INTERVAL '1 day')" >/dev/null
  printf '%s' "$plain"
}
MKEY="$(mkkey 'deployments:register,runs:create,runs:read,approvals:decide,schedules:write')"

# Human operator: user + membership + cookie session minted the way the
# service does (sha256 hex token hashes), valid 12h idle / 7d absolute.
USER_ID="$(q1 "INSERT INTO users (email, name) VALUES ('m4-gate-journey@example.com', 'M4 Gate Journey') RETURNING id::text")"
q "INSERT INTO organization_members (organization_id, user_id, role, status) VALUES ('$ORG_ID'::uuid, '$USER_ID'::uuid, 'Operator', 'ACTIVE')" >/dev/null
RAW_SESSION="$(openssl rand -hex 32)"
RAW_CSRF="$(openssl rand -hex 32)"
HASH_SESSION="$(printf '%s' "$RAW_SESSION" | sha256sum | cut -d' ' -f1)"
HASH_CSRF="$(printf '%s' "$RAW_CSRF" | sha256sum | cut -d' ' -f1)"
q "INSERT INTO auth_sessions (session_token_hash, user_id, active_organization_id, csrf_token_hash, idle_expires_at, absolute_expires_at, ip_address, user_agent, last_seen_at, created_at) VALUES ('$HASH_SESSION', '$USER_ID'::uuid, '$ORG_ID'::uuid, '$HASH_CSRF', clock_timestamp() + INTERVAL '12 hours', clock_timestamp() + INTERVAL '7 days', '127.0.0.1', 'm4-gate-journey', clock_timestamp(), clock_timestamp())" >/dev/null
H_COOKIES="__Host-runtime_session=$RAW_SESSION"
info "human operator session minted for user=$USER_ID"

# Deployment manifest: gate(approval, root) -> hold(delay 60s) -> done(task),
# plus a schedule-friendly single-task workflow accepting empty input.
python3 - > /tmp/m4j-manifest.json <<'EOF'
import json
num = {"type": "object", "properties": {"n": {"type": "integer"}},
       "required": ["n"], "additionalProperties": False}
anyObj = {"type": "object"}
decision = {"type": "object", "properties": {"decision": {"type": "string"}}, "required": ["decision"], "additionalProperties": True}
manifest = {
    "manifestVersion": 1, "sdkVersion": "1.0.0", "protocolMajor": 1,
    "nodeRuntimeMajor": 24,
    "targetOS": "linux", "targetArchitecture": "amd64",
    "dependencyLockDigest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "bundleDigest": "b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4",
    "secretNames": [],
    "tasks": [
        {"name": "t-done", "entrypoint": "tasks/done.js", "timeoutMs": 30000,
         "recovery": "safe", "inputSchema": num, "outputSchema": num},
    ],
    "workflows": [
        {"manifestVersion": 1, "name": "m4j-wait", "inputSchema": num, "outputSchema": num,
         "nodes": [
             {"id": "gate", "type": "approval",
              "approval": {"payload": {"question": "Continue after restart?"},
                           "requiredPermission": "approvals:decide",
                           "outputSchema": decision}},
             {"id": "hold", "type": "delay", "after": ["gate"], "delayMs": 60000},
             {"id": "done", "type": "task", "task": "t-done", "after": ["hold"],
              "input": {"decision": {"$ref": "step.output", "stepId": "gate", "pointer": "/decision"}}},
         ],
         "output": {"n": {"$ref": "step.output", "stepId": "done", "pointer": "/n"}}},
        {"manifestVersion": 1, "name": "m4j-tick", "inputSchema": anyObj, "outputSchema": anyObj,
         "nodes": [{"id": "only", "type": "task", "task": "t-done", "after": []}],
         "output": {"n": {"$ref": "step.output", "stepId": "only", "pointer": "/n"}}},
    ],
}
print(json.dumps(manifest))
EOF
CODE="$(curl -sS --max-time 20 -o /tmp/m4j-reg.txt -w '%{http_code}' -X POST "$BASE/v1/deployments?environment=$ENV_ID" \
  -H "Authorization: Bearer $MKEY" -H "X-Organization-ID: $ORG_ID" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(python3 -c 'import uuid; print(uuid.uuid4())')" -d @/tmp/m4j-manifest.json)"
[[ "$CODE" == "200" || "$CODE" == "201" ]] || fail "register deployment: HTTP $CODE $(head -c 400 /tmp/m4j-reg.txt)"
DEPLOY_ID="$(python3 -c 'import json; print(json.load(open("/tmp/m4j-reg.txt"))["id"])')"
q "INSERT INTO workflow_channels (organization_id, environment_id, workflow_name, active_deployment_id, revision, updated_at) VALUES ('$ORG_ID'::uuid, '$ENV_ID'::uuid, 'm4j-wait', '$DEPLOY_ID'::uuid, 1, clock_timestamp()), ('$ORG_ID'::uuid, '$ENV_ID'::uuid, 'm4j-tick', '$DEPLOY_ID'::uuid, 1, clock_timestamp())" >/dev/null
info "deployment=$DEPLOY_ID channels pinned"

mkrun() { # $1=workflow $2=inputjson $3=idem -> prints run id
  # Environment travels as the canonical UUID: the org holds several envs
  # named staging across fixture projects, so a bare name is ambiguous.
  python3 -c 'import json,sys; print(json.dumps({"environment": sys.argv[2], "input": json.loads(sys.argv[1])}))' "$2" "$ENV_ID" > /tmp/m4j-input.json
  local code
  code="$(curl -sS --max-time 20 -o /tmp/m4j-run.txt -w '%{http_code}' -X POST "$BASE/v1/workflows/$1/runs" \
    -H "Authorization: Bearer $MKEY" -H "X-Organization-ID: $ORG_ID" -H 'Content-Type: application/json' \
    -H "Idempotency-Key: $3" -d @/tmp/m4j-input.json)"
  [[ "$code" == "202" ]] || { head -c 400 /tmp/m4j-run.txt; return 1; }
  python3 -c 'import json; print(json.load(open("/tmp/m4j-run.txt"))["id"])'
}
RUN_A="$(mkrun m4j-wait '{"n":7}' "m4j-run-a-$COMMIT")" || fail "create run A"
RUN_B="$(mkrun m4j-wait '{"n":7}' "m4j-run-b-$COMMIT")" || fail "create run B"
info "runA=$RUN_A runB=$RUN_B"

# Root approval must be PENDING with no worker involved.
sleep 5
APPROVAL_A="$(q1 "SELECT a.id::text FROM approvals a JOIN run_steps s ON s.id = a.step_id WHERE a.organization_id='$ORG_ID'::uuid AND s.run_id='$RUN_A'::uuid AND a.status='PENDING' LIMIT 1")"
APPROVAL_B="$(q1 "SELECT a.id::text FROM approvals a JOIN run_steps s ON s.id = a.step_id WHERE a.organization_id='$ORG_ID'::uuid AND s.run_id='$RUN_B'::uuid AND a.status='PENDING' LIMIT 1")"
[[ -n "$APPROVAL_A" && -n "$APPROVAL_B" ]] || fail "root approvals not PENDING (A=$APPROVAL_A B=$APPROVAL_B)"
info "approvalA=$APPROVAL_A approvalB=$APPROVAL_B (both PENDING, no runner held)"

# Due schedule planted pre-restart: the NEW process must evaluate it.
cat > /tmp/m4j-sched.json <<'EOF'
{"workflow":"m4j-tick","cron":"* * * * *","timezone":"UTC"}
EOF
CODE="$(curl -sS --max-time 20 -o /tmp/m4j-sched.txt -w '%{http_code}' -X POST "$BASE/v1/schedules?environment=$ENV_ID" \
  -H "Authorization: Bearer $MKEY" -H "X-Organization-ID: $ORG_ID" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(python3 -c 'import uuid; print(uuid.uuid4())')" -d @/tmp/m4j-sched.json)"
[[ "$CODE" == "200" ]] || fail "create schedule: HTTP $CODE $(head -c 400 /tmp/m4j-sched.txt)"
SCHED_ID="$(python3 -c 'import json; print(json.load(open("/tmp/m4j-sched.txt"))["id"])')"
q "UPDATE schedules SET last_occurrence_at = clock_timestamp() - INTERVAL '10 minutes', next_due_at = clock_timestamp() - INTERVAL '9 minutes' WHERE id='$SCHED_ID'::uuid" >/dev/null
info "schedule=$SCHED_ID planted 9-10min behind (pre-restart)"

# RESTART: kill the active control-plane process; same image comes back.
info "restarting $CONTAINER ..."
docker restart "$CONTAINER" >/dev/null
READY=false
for _ in $(seq 1 30); do
  if [[ "$(curl -sS --max-time 10 -o /dev/null -w '%{http_code}' "$BASE/readyz" 2>/dev/null)" == "200" ]]; then READY=true; break; fi
  sleep 2
done
[[ "$READY" == "true" ]] || fail "control plane did not return after restart"
POST_COMMIT="$(curl -fsSL --max-time 20 "$BASE/version" | python3 -c 'import json,sys; print(json.load(sys.stdin)["commit"])')"
[[ "$POST_COMMIT" == "$COMMIT" ]] || fail "post-restart commit $POST_COMMIT != $COMMIT"
pass "restart: new process serves the same artifact $COMMIT"

# JOURNEY 1 — the pre-restart approval is decidable by the new process.
REV_A="$(q1 "SELECT revision FROM approvals WHERE id='$APPROVAL_A'::uuid")"
cat > /tmp/m4j-decide-a.json <<EOF
{"decision":"approved","expectedRevision":$REV_A}
EOF
CODE="$(curl -sS --max-time 20 -o /tmp/m4j-decide-a.txt -w '%{http_code}' -X POST "$BASE/v1/approvals/$APPROVAL_A/decision" \
  -H "Cookie: $H_COOKIES" -H "X-CSRF-Token: $RAW_CSRF" -H "Origin: $ORIGIN" -H "X-Organization-ID: $ORG_ID" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: $(python3 -c 'import uuid; print(uuid.uuid4())')" -d @/tmp/m4j-decide-a.json)"
[[ "$CODE" == "200" ]] || fail "decide A: HTTP $CODE $(head -c 400 /tmp/m4j-decide-a.txt)"
grep -q '"status":"APPROVED"' /tmp/m4j-decide-a.txt || fail "decide A not APPROVED $(head -c 400 /tmp/m4j-decide-a.txt)"
# Opposing decision conflicts; machine key is refused as non-human.
cat > /tmp/m4j-decide-opp.json <<EOF
{"decision":"rejected","expectedRevision":$((REV_A + 1))}
EOF
CODE="$(curl -sS --max-time 10 -o /dev/null -w '%{http_code}' -X POST "$BASE/v1/approvals/$APPROVAL_A/decision" \
  -H "Cookie: $H_COOKIES" -H "X-CSRF-Token: $RAW_CSRF" -H "Origin: $ORIGIN" -H "X-Organization-ID: $ORG_ID" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: $(python3 -c 'import uuid; print(uuid.uuid4())')" -d @/tmp/m4j-decide-opp.json)"
[[ "$CODE" == "409" ]] || fail "opposing decision: HTTP $CODE, want 409"
CODE="$(curl -sS --max-time 20 -o /tmp/m4j-machine.txt -w '%{http_code}' -X POST "$BASE/v1/approvals/$APPROVAL_A/decision" \
  -H "Authorization: Bearer $MKEY" -H "X-Organization-ID: $ORG_ID" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(python3 -c 'import uuid; print(uuid.uuid4())')" -d @/tmp/m4j-decide-opp.json)"
[[ "$CODE" == "403" ]] || fail "machine decide: HTTP $CODE, want 403"
grep -q "APPROVAL_HUMAN_ONLY" /tmp/m4j-machine.txt || fail "machine decide missing APPROVAL_HUMAN_ONLY"
pass "journey 1: pre-restart approval decided once by new process; conflict + human-only hold"

# JOURNEY 2 — the released delay fires under the new process, exactly once.
for _ in $(seq 1 60); do
  NTIMER="$(q1 "SELECT count(*) FROM timers WHERE organization_id='$ORG_ID'::uuid AND run_id='$RUN_A'::uuid AND kind='DELAY' AND state='PENDING'")"
  [[ "$NTIMER" == "1" ]] && break
  sleep 2
done
[[ "$NTIMER" == "1" ]] || fail "no PENDING delay timer for run A after decide"
q "UPDATE timers SET due_at=clock_timestamp()-INTERVAL '1 second' WHERE organization_id='$ORG_ID'::uuid AND run_id='$RUN_A'::uuid AND kind='DELAY' AND state='PENDING'" >/dev/null
sleep 20
FIRED="$(q1 "SELECT count(*) FROM timers WHERE organization_id='$ORG_ID'::uuid AND run_id='$RUN_A'::uuid AND kind='DELAY' AND state='FIRED'")"
PENDING_LEFT="$(q1 "SELECT count(*) FROM timers WHERE organization_id='$ORG_ID'::uuid AND run_id='$RUN_A'::uuid AND kind='DELAY' AND state='PENDING'")"
[[ "$FIRED" == "1" && "$PENDING_LEFT" == "0" ]] || fail "delay not fired exactly once (fired=$FIRED pending=$PENDING_LEFT)"
sleep 10
FIRED_AGAIN="$(q1 "SELECT count(*) FROM timers WHERE organization_id='$ORG_ID'::uuid AND run_id='$RUN_A'::uuid AND kind='DELAY' AND state='FIRED'")"
[[ "$FIRED_AGAIN" == "1" ]] || fail "duplicate timer firing (fired=$FIRED_AGAIN)"
pass "journey 2: delay released post-restart fired exactly once by the sweeper"

# JOURNEY 3 — expiry: force run B past its deadline, decide refused, swept.
q "UPDATE approvals SET expires_at = clock_timestamp() - INTERVAL '1 minute' WHERE id='$APPROVAL_B'::uuid" >/dev/null
REV_B="$(q1 "SELECT revision FROM approvals WHERE id='$APPROVAL_B'::uuid")"
cat > /tmp/m4j-decide-b.json <<EOF
{"decision":"approved","expectedRevision":$REV_B}
EOF
CODE="$(curl -sS --max-time 20 -o /tmp/m4j-decide-b.txt -w '%{http_code}' -X POST "$BASE/v1/approvals/$APPROVAL_B/decision" \
  -H "Cookie: $H_COOKIES" -H "X-CSRF-Token: $RAW_CSRF" -H "Origin: $ORIGIN" -H "X-Organization-ID: $ORG_ID" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: $(python3 -c 'import uuid; print(uuid.uuid4())')" -d @/tmp/m4j-decide-b.json)"
[[ "$CODE" == "409" ]] || fail "expired decide: HTTP $CODE, want 409"
grep -q "APPROVAL_EXPIRED" /tmp/m4j-decide-b.txt || fail "expired decide missing APPROVAL_EXPIRED"
sleep 20
STATUS_B="$(q1 "SELECT status FROM approvals WHERE id='$APPROVAL_B'::uuid")"
RUN_B_STATUS="$(q1 "SELECT status FROM runs WHERE id='$RUN_B'::uuid")"
[[ "$STATUS_B" == "EXPIRED" ]] || fail "approval B = $STATUS_B, want EXPIRED"
[[ "$RUN_B_STATUS" == "FAILED" ]] || fail "run B = $RUN_B_STATUS, want FAILED"
pass "journey 3: expired approval refused then swept; run FAILED"

# JOURNEY 4 — the pre-restart schedule was evaluated by the new process.
OCC="$(q1 "SELECT count(*) FROM schedule_occurrences WHERE schedule_id='$SCHED_ID'::uuid")"
ORUNS="$(q1 "SELECT count(*) FROM schedule_occurrences WHERE schedule_id='$SCHED_ID'::uuid AND run_id IS NOT NULL")"
FIRST_SKIPPED="$(q "SELECT skipped_count FROM schedule_occurrences WHERE schedule_id='$SCHED_ID'::uuid ORDER BY due_at LIMIT 1")"
[[ "$ORUNS" == "1" ]] || fail "schedule runs = $ORUNS, want exactly 1 (occurrences=$OCC)"
[[ "$FIRST_SKIPPED" -ge 5 ]] || fail "first coalesced skipped_count = $FIRST_SKIPPED"
pass "journey 4: pre-restart schedule converged post-restart (occurrences=$OCC, 1 run, skipped_count=$FIRST_SKIPPED)"

# Cleanup: pause schedule, revoke keys and session.
q "UPDATE schedules SET paused=true, next_due_at=NULL WHERE id='$SCHED_ID'::uuid" >/dev/null
q "UPDATE api_keys SET revoked_at=clock_timestamp() WHERE organization_id='$ORG_ID'::uuid AND environment_id='$ENV_ID'::uuid" >/dev/null
q "UPDATE auth_sessions SET revoked_at=clock_timestamp(), revocation_reason='m4-gate-journey-complete' WHERE user_id='$USER_ID'::uuid" >/dev/null
info "schedule=$SCHED_ID paused; keys and session revoked"
pass "M4 GATE STAGING JOURNEYS COMPLETE on $COMMIT"
