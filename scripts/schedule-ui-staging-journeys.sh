#!/usr/bin/env bash
# scripts/schedule-ui-staging-journeys.sh
# Staging API journeys for PR #86 (Issue #35): the exact behaviours the
# dashboard relies on, exercised through the real deployed API.
#
# Runs ON the staging host (direct SSH, like schedule-staging-acceptance.sh).
# Fixtures live in an isolated project `sched-ui-accept` inside the existing
# `Deadbolt Acceptance` org; the shared staging env is never touched.
# Exit non-zero on any failed assertion; all evidence prints to stdout.

set -euo pipefail

PROJECT_NAME="sched-ui-accept"
WORKFLOW="sched-occ-flow"

CONFIG_FILE="${DEADBOLT_CONFIG_FILE:-/etc/deadbolt/staging.env}"
if [[ -f "$CONFIG_FILE" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "$CONFIG_FILE"
  set +a
fi

pass() { echo "[JOURNEY-PASS] $*"; }
info() { echo "[JOURNEY-INFO] $*"; }
fail() {
  echo "[JOURNEY-FAIL] $*" >&2
  exit 1
}

PSQL="docker exec -i deadbolt-staging-postgres psql -U deadbolt_admin -d deadbolt_staging -v ON_ERROR_STOP=1 -At"
q() { PGPASSWORD="$DEADBOLT_DB_ADMIN_PASSWORD" $PSQL -c "$1"; }
q1() { q "$1" | head -n 1 | tr -d '[:space:]'; }

ACTIVE_SLOT="$(cat /opt/deadbolt/releases/active_slot 2>/dev/null | tr -d '[:space:]' || echo green)"
if [[ "$ACTIVE_SLOT" == "blue" ]]; then PORT="8088"; else PORT="8089"; fi
BASE="http://127.0.0.1:${PORT}"

VERSION_JSON="$(curl -fsSL "$BASE/version")"
info "slot=$ACTIVE_SLOT version=$VERSION_JSON"
COMMIT="$(echo "$VERSION_JSON" | python3 -c 'import json,sys; print(json.load(sys.stdin)["commit"])')"

ORG_ID="$(q1 "SELECT id::text FROM organizations WHERE name='Deadbolt Acceptance' LIMIT 1")"
[[ -n "$ORG_ID" ]] || fail "org 'Deadbolt Acceptance' not found"
SRC_ROW="$(q1 "SELECT c.environment_id::text || '|' || c.active_deployment_id::text FROM workflow_channels c WHERE c.organization_id='$ORG_ID'::uuid AND c.workflow_name='$WORKFLOW' LIMIT 1")"
[[ -n "$SRC_ROW" ]] || fail "no active channel for $WORKFLOW"
MANIFEST="$(PGPASSWORD="$DEADBOLT_DB_ADMIN_PASSWORD" docker exec -i deadbolt-staging-postgres psql -U deadbolt_admin -d deadbolt_staging -v ON_ERROR_STOP=1 -At -c "SELECT manifest FROM deployments WHERE id='${SRC_ROW##*|}'::uuid")"
[[ -n "$MANIFEST" ]] || fail "source manifest empty"

# Fresh fixture project + env + deployment + channel.
q "DELETE FROM projects WHERE organization_id='$ORG_ID'::uuid AND name='$PROJECT_NAME'" >/dev/null
PROJ_ID="$(q1 "INSERT INTO projects (organization_id, name) VALUES ('$ORG_ID'::uuid, '$PROJECT_NAME') RETURNING id::text")"
ENV_ID="$(q1 "INSERT INTO environments (organization_id, project_id, name) VALUES ('$ORG_ID'::uuid, '$PROJ_ID'::uuid, 'staging') RETURNING id::text")"
q "INSERT INTO environment_admissions (environment_id, organization_id, max_concurrency) VALUES ('$ENV_ID'::uuid, '$ORG_ID'::uuid, 10)" >/dev/null
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ESCAPED_MANIFEST="${MANIFEST//\'/\'\'}"
NEW_DEPLOY_ID="$(PGPASSWORD="$DEADBOLT_DB_ADMIN_PASSWORD" docker exec -i deadbolt-staging-postgres psql -U deadbolt_admin -d deadbolt_staging -v ON_ERROR_STOP=1 -At -c "INSERT INTO deployments (organization_id, environment_id, manifest_hash, bundle_digest, manifest, protocol_version, runtime_version, status) VALUES ('$ORG_ID'::uuid, '$ENV_ID'::uuid, 'sched-ui-manifest-$STAMP', 'scheduibundle$STAMP', '$ESCAPED_MANIFEST'::jsonb, 1, '1.0', 'ACTIVE') RETURNING id::text" | head -n 1 | tr -d '[:space:]')"
q "INSERT INTO workflow_channels (organization_id, environment_id, workflow_name, active_deployment_id, revision) VALUES ('$ORG_ID'::uuid, '$ENV_ID'::uuid, '$WORKFLOW', '$NEW_DEPLOY_ID'::uuid, 1)" >/dev/null
info "proj=$PROJ_ID env=$ENV_ID deploy=$NEW_DEPLOY_ID commit=$COMMIT"

# API keys minted the same way the service does: prefix db_<env>_<8hex>,
# plaintext prefix + '_' + base64url(32B), SHA-256 hex stored.
mkkey() { # $1=caps CSV -> prints plaintext
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
OP_KEY="$(mkkey 'schedules:write,runs:read')"
DEV_KEY="$(mkkey 'runs:create,runs:read')"
info "keys minted (operator + developer, 1-day expiry)"

api() { # $1=method $2=path $3=key $4=bodyfile(optional)
  local args=(-sS -o /tmp/ui-journey-body.txt -w '%{http_code}')
  [[ -n "${4:-}" ]] && args+=(-H 'Content-Type: application/json' -d @"$4")
  curl "${args[@]}" -X "$1" "$BASE$2" -H "Authorization: Bearer $3" -H "X-Organization-ID: $ORG_ID" -H "Idempotency-Key: $(python3 -c 'import uuid; print(uuid.uuid4())')"
}
show() { head -c 600 /tmp/ui-journey-body.txt; echo; }

# JOURNEY 1 — create through the real API.
cat > /tmp/ui-journey-create.json <<'EOF'
{"workflow":"sched-occ-flow","cron":"0 12 * * *","timezone":"UTC"}
EOF
CODE="$(api POST "/v1/schedules?environment=$ENV_ID" "$OP_KEY" /tmp/ui-journey-create.json)"
[[ "$CODE" == "200" ]] || fail "create: HTTP $CODE $(show)"
SCHED_ID="$(python3 -c 'import json; print(json.load(open("/tmp/ui-journey-body.txt"))["id"])')"
REV="$(python3 -c 'import json; print(json.load(open("/tmp/ui-journey-body.txt"))["revision"])')"
DUE="$(python3 -c 'import json; print(json.load(open("/tmp/ui-journey-body.txt"))["nextDueAt"])')"
[[ -n "$SCHED_ID" && -n "$DUE" ]] || fail "create response missing id/nextDueAt $(show)"
info "created schedule=$SCHED_ID rev=$REV nextDueAt=$DUE"
pass "journey 1: create returns persisted definition with nextDueAt"

# JOURNEY 2 — two-editor conflict: A saves, B (stale) gets 409, refreshes, saves.
cat > /tmp/ui-journey-edit-a.json <<EOF
{"expectedRevision":$REV,"configuration":{"workflow":"sched-occ-flow","cron":"30 6 * * *","timezone":"UTC"}}
EOF
CODE="$(api PATCH "/v1/schedules/$SCHED_ID?environment=$ENV_ID" "$OP_KEY" /tmp/ui-journey-edit-a.json)"
[[ "$CODE" == "200" ]] || fail "edit A: HTTP $CODE $(show)"
REV2="$(python3 -c 'import json; print(json.load(open("/tmp/ui-journey-body.txt"))["revision"])')"
[[ "$REV2" == "$((REV + 1))" ]] || fail "edit A revision = $REV2, want $((REV + 1))"
CODE="$(api PATCH "/v1/schedules/$SCHED_ID?environment=$ENV_ID" "$OP_KEY" /tmp/ui-journey-edit-a.json)"
[[ "$CODE" == "409" ]] || fail "stale edit B: HTTP $CODE, want 409 $(show)"
grep -q "REVISION_CONFLICT" /tmp/ui-journey-body.txt || fail "stale edit B missing REVISION_CONFLICT $(show)"
CODE="$(api GET "/v1/schedules?environment=$ENV_ID" "$OP_KEY")"
[[ "$CODE" == "200" ]] || fail "refresh list: HTTP $CODE"
python3 - "$SCHED_ID" <<'EOF'
import json, sys
items = json.load(open("/tmp/ui-journey-body.txt"))["items"]
row = [s for s in items if s["id"] == sys.argv[1]][0]
assert row["cron"] == "30 6 * * *", row
assert row["revision"] == 2, row
EOF
info "editor B refreshed to rev 2 with A's cron intact"
pass "journey 2: stale edit 409 REVISION_CONFLICT, refresh keeps winner"

# JOURNEY 3 — downtime history through the occurrences endpoint. Switch to a
# per-minute cron first: a daily cron has no missed slots inside a 10-minute
# window, so there would be nothing to coalesce.
REV_NOW="$(q1 "SELECT revision FROM schedules WHERE id='$SCHED_ID'::uuid")"
cat > /tmp/ui-journey-edit-cron.json <<EOF
{"expectedRevision":$REV_NOW,"configuration":{"workflow":"sched-occ-flow","cron":"* * * * *","timezone":"UTC"}}
EOF
CODE="$(api PATCH "/v1/schedules/$SCHED_ID?environment=$ENV_ID" "$OP_KEY" /tmp/ui-journey-edit-cron.json)"
[[ "$CODE" == "200" ]] || fail "cron switch: HTTP $CODE $(show)"
q "UPDATE schedules SET last_occurrence_at = clock_timestamp() - INTERVAL '10 minutes', next_due_at = clock_timestamp() - INTERVAL '9 minutes' WHERE id='$SCHED_ID'::uuid" >/dev/null
info "planted 9-10min behind on a per-minute cron; waiting for the production sweeper..."
sleep 20
CODE="$(api GET "/v1/schedules/$SCHED_ID/occurrences?environment=$ENV_ID&limit=25" "$OP_KEY")"
[[ "$CODE" == "200" ]] || fail "occurrences: HTTP $CODE $(show)"
python3 - <<'EOF'
import json
body = json.load(open("/tmp/ui-journey-body.txt"))
items = body["items"]
assert len(items) >= 1, body
started = [o for o in items if o["status"] == "STARTED"]
assert started, f"no STARTED slot: {body}"
first = sorted(started, key=lambda o: o["dueAt"])[0]
assert first["skippedCount"] >= 5, first
assert first["runId"], first
runs = [o for o in items if o["runId"]]
assert len(runs) == 1, f"coalesce-one must yield exactly one run: {body}"
print(json.dumps({"occurrences": len(items), "skippedCount": first["skippedCount"], "run": first["runId"]}))
EOF
pass "journey 3: downtime history shows STARTED + skipped_count with exactly one run"

# JOURNEY 4 — permission states: developer denied everywhere, anonymous 401.
CODE="$(api GET "/v1/schedules?environment=$ENV_ID" "$DEV_KEY")"
[[ "$CODE" == "403" ]] || fail "developer list: HTTP $CODE, want 403"
CODE="$(api POST "/v1/schedules?environment=$ENV_ID" "$DEV_KEY" /tmp/ui-journey-create.json)"
[[ "$CODE" == "403" ]] || fail "developer create: HTTP $CODE, want 403"
CODE="$(api GET "/v1/schedules/$SCHED_ID/occurrences?environment=$ENV_ID" "$DEV_KEY")"
[[ "$CODE" == "403" ]] || fail "developer occurrences: HTTP $CODE, want 403"
CODE="$(curl -sS -o /tmp/ui-journey-body.txt -w '%{http_code}' "$BASE/v1/schedules?environment=$ENV_ID" -H "X-Organization-ID: $ORG_ID")"
[[ "$CODE" == "401" ]] || fail "anonymous list: HTTP $CODE, want 401"
pass "journey 4: developer 403 on list/create/occurrences, anonymous 401"

# JOURNEY 5 — pause/resume round trip with persisted due-time semantics.
REV_PRE_PAUSE="$(q1 "SELECT revision FROM schedules WHERE id='$SCHED_ID'::uuid")"
cat > /tmp/ui-journey-pause.json <<EOF
{"expectedRevision":$REV_PRE_PAUSE}
EOF
CODE="$(api POST "/v1/schedules/$SCHED_ID/pause?environment=$ENV_ID" "$OP_KEY" /tmp/ui-journey-pause.json)"
[[ "$CODE" == "200" ]] || fail "pause: HTTP $CODE $(show)"
python3 - <<'EOF'
import json
s = json.load(open("/tmp/ui-journey-body.txt"))
assert s["paused"] is True and s["nextDueAt"] is None, s
EOF
CODE="$(api POST "/v1/schedules/$SCHED_ID/pause?environment=$ENV_ID" "$OP_KEY" /tmp/ui-journey-pause.json)"
[[ "$CODE" == "409" ]] || fail "double pause: HTTP $CODE, want 409"
# Pause bumped the revision; resume with the fresh one.
REV_PAUSED="$(q1 "SELECT revision FROM schedules WHERE id='$SCHED_ID'::uuid")"
cat > /tmp/ui-journey-resume.json <<EOF
{"expectedRevision":$REV_PAUSED}
EOF
CODE="$(api POST "/v1/schedules/$SCHED_ID/resume?environment=$ENV_ID" "$OP_KEY" /tmp/ui-journey-resume.json)"
[[ "$CODE" == "200" ]] || fail "resume: HTTP $CODE $(show)"
python3 - <<'EOF'
import json
s = json.load(open("/tmp/ui-journey-body.txt"))
assert s["paused"] is False and s["nextDueAt"], s
EOF
pass "journey 5: pause clears nextDueAt (409 on double-pause), resume re-arms"

# Cleanup: leave the schedule paused, revoke the minted keys.
q "UPDATE schedules SET paused=true, next_due_at=NULL WHERE id='$SCHED_ID'::uuid" >/dev/null
q "UPDATE api_keys SET revoked_at=clock_timestamp() WHERE organization_id='$ORG_ID'::uuid AND environment_id='$ENV_ID'::uuid" >/dev/null
info "schedule=$SCHED_ID paused; journey keys revoked"
pass "UI STAGING JOURNEYS COMPLETE on $COMMIT"
