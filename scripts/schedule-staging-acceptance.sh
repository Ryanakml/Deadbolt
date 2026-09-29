#!/usr/bin/env bash
# scripts/schedule-staging-acceptance.sh
# Staging acceptance for PR #85 (Issue #34) — the three behaviours proven only
# by the integration suite so far: coalesce-one misfire, SKIPPED_QUOTA, and
# revision-change-under-evaluation.
#
# Runs ON the staging host (via SSH from the schedule-staging-acceptance
# workflow). The deployed control-plane sweeper does all the acting; this
# script only plants fixtures as the postgres superuser (bypassing RLS, which
# the runtime role remains subject to) and asserts on the rows the sweeper
# produces.
#
# Exit non-zero on any failed assertion. All evidence (IDs, counts, statuses)
# is printed to stdout for the PR record.
#
# Fixture isolation: everything lives in a dedicated project
# `sched-accept-pr85` inside the existing `Deadbolt Acceptance` org, reusing
# the `sched-occ-flow` workflow shape (accepts an empty input object, which
# §17 requires of any schedulable workflow). The shared staging env is never
# touched. A re-run starts by deleting the project (FK cascades clean it).

set -euo pipefail

EXPECTED_COMMIT="12f2af35696d06733434a4b606881ae919853488"
PROJECT_NAME="sched-accept-pr85"
WORKFLOW="sched-occ-flow"

CONFIG_FILE="${DEADBOLT_CONFIG_FILE:-/etc/deadbolt/staging.env}"
if [[ -f "$CONFIG_FILE" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "$CONFIG_FILE"
  set +a
fi

pass() { echo "[ACCEPT-PASS] $*"; }
info() { echo "[ACCEPT-INFO] $*"; }
fail() {
  echo "[ACCEPT-FAIL] $*" >&2
  exit 1
}

PSQL="docker exec -i deadbolt-staging-postgres psql -U deadbolt_admin -d deadbolt_staging -v ON_ERROR_STOP=1 -At"
q() {
  PGPASSWORD="$DEADBOLT_DB_ADMIN_PASSWORD" $PSQL -c "$1"
}

# 0. Deployed artifact identity: the staging slot must serve this PR head.
ACTIVE_SLOT="$(cat /opt/deadbolt/releases/active_slot 2>/dev/null | tr -d '[:space:]' || echo green)"
if [[ "$ACTIVE_SLOT" == "blue" ]]; then PORT="8088"; else PORT="8089"; fi
VERSION_JSON="$(curl -fsSL "http://127.0.0.1:${PORT}/version")"
info "slot=$ACTIVE_SLOT port=$PORT version=$VERSION_JSON"
echo "$VERSION_JSON" | grep -q "$EXPECTED_COMMIT" \
  || fail "staging slot $ACTIVE_SLOT does not serve $EXPECTED_COMMIT"
echo "$VERSION_JSON" | grep -q '"runtime_mode":"hosted"' \
  || fail "staging runtime_mode is not hosted"

# 1. Resolve org and a source deployment for the schedulable workflow shape.
ORG_ID="$(q "SELECT id::text FROM organizations WHERE name='Deadbolt Acceptance' LIMIT 1")"
[[ -n "$ORG_ID" ]] || fail "org 'Deadbolt Acceptance' not found on staging"
info "org_id=$ORG_ID"

SRC_ROW="$(q "SELECT c.environment_id::text || '|' || c.active_deployment_id::text FROM workflow_channels c WHERE c.organization_id='$ORG_ID'::uuid AND c.workflow_name='$WORKFLOW' LIMIT 1")"
[[ -n "$SRC_ROW" ]] || fail "no active channel for workflow '$WORKFLOW' on staging (previous acceptance planted it)"
SRC_ENV_ID="${SRC_ROW%%|*}"
SRC_DEPLOY_ID="${SRC_ROW##*|}"
info "src_env_id=$SRC_ENV_ID src_deploy_id=$SRC_DEPLOY_ID"

MANIFEST="$(PGPASSWORD="$DEADBOLT_DB_ADMIN_PASSWORD" docker exec -i deadbolt-staging-postgres psql -U deadbolt_admin -d deadbolt_staging -v ON_ERROR_STOP=1 -At -c "SELECT manifest FROM deployments WHERE id='$SRC_DEPLOY_ID'::uuid")"
[[ -n "$MANIFEST" ]] || fail "source deployment manifest is empty"

# 2. Fresh fixture project + staging env + admissions + deployment + channel.
q "DELETE FROM projects WHERE organization_id='$ORG_ID'::uuid AND name='$PROJECT_NAME'" >/dev/null
PROJ_ID="$(q "INSERT INTO projects (organization_id, name) VALUES ('$ORG_ID'::uuid, '$PROJECT_NAME') RETURNING id::text")"
ENV_ID="$(q "INSERT INTO environments (organization_id, project_id, name) VALUES ('$ORG_ID'::uuid, '$PROJ_ID'::uuid, 'staging') RETURNING id::text")"
q "INSERT INTO environment_admissions (environment_id, organization_id, max_concurrency) VALUES ('$ENV_ID'::uuid, '$ORG_ID'::uuid, 10)" >/dev/null
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ESCAPED_MANIFEST="${MANIFEST//\'/\'\'}"
NEW_DEPLOY_ID="$(PGPASSWORD="$DEADBOLT_DB_ADMIN_PASSWORD" docker exec -i deadbolt-staging-postgres psql -U deadbolt_admin -d deadbolt_staging -v ON_ERROR_STOP=1 -At -c "INSERT INTO deployments (organization_id, environment_id, manifest_hash, bundle_digest, manifest, protocol_version, runtime_version, status) VALUES ('$ORG_ID'::uuid, '$ENV_ID'::uuid, 'sched-accept-manifest-$STAMP', 'schedacceptbundle$STAMP', '$ESCAPED_MANIFEST'::jsonb, 1, '1.0', 'ACTIVE') RETURNING id::text")"
q "INSERT INTO workflow_channels (organization_id, environment_id, workflow_name, active_deployment_id, revision) VALUES ('$ORG_ID'::uuid, '$ENV_ID'::uuid, '$WORKFLOW', '$NEW_DEPLOY_ID'::uuid, 1)" >/dev/null
info "proj_id=$PROJ_ID env_id=$ENV_ID deploy_id=$NEW_DEPLOY_ID"

mk_schedule() { # $1=cron
  q "INSERT INTO schedules (organization_id, environment_id, workflow_name, cron_expression, timezone) VALUES ('$ORG_ID'::uuid, '$ENV_ID'::uuid, '$WORKFLOW', '$1', 'UTC') RETURNING id::text"
}

# 3. TEST 1 — coalesce-one: every-minute schedule "missed" ten slots.
S1="$(mk_schedule '* * * * *')"
q "UPDATE schedules SET last_occurrence_at = clock_timestamp() - INTERVAL '10 minutes', next_due_at = clock_timestamp() - INTERVAL '9 minutes' WHERE id='$S1'::uuid" >/dev/null
info "coalesce schedule_id=$S1 planted 9-10min in the past; waiting for the production sweeper..."
sleep 20
OCC_COUNT="$(q "SELECT count(*) FROM schedule_occurrences WHERE schedule_id='$S1'::uuid")"
RUN_COUNT="$(q "SELECT count(*) FROM schedule_occurrences WHERE schedule_id='$S1'::uuid AND run_id IS NOT NULL")"
SKIPPED="$(q "SELECT skipped_count FROM schedule_occurrences WHERE schedule_id='$S1'::uuid LIMIT 1")"
OCC_STATUS="$(q "SELECT status FROM schedule_occurrences WHERE schedule_id='$S1'::uuid LIMIT 1")"
NEXT_DUE="$(q "SELECT next_due_at > clock_timestamp() FROM schedules WHERE id='$S1'::uuid")"
info "coalesce occurrences=$OCC_COUNT runs=$RUN_COUNT skipped_count=$SKIPPED status=$OCC_STATUS next_due_future=$NEXT_DUE"
[[ "$OCC_COUNT" == "1" ]] || fail "coalesce-one must create exactly one occurrence, got $OCC_COUNT"
[[ "$RUN_COUNT" == "1" ]] || fail "coalesce-one must create exactly one run, got $RUN_COUNT"
[[ "$OCC_STATUS" == "STARTED" ]] || fail "coalesce occurrence status = $OCC_STATUS, want STARTED"
[[ "$SKIPPED" -ge 5 ]] || fail "skipped_count = $SKIPPED, want the coalesced remainder recorded (>=5)"
[[ "$NEXT_DUE" == "t" ]] || fail "next_due_at did not advance to a future slot"
pass "coalesce-one misfire proven on staging (schedule $S1, 1 occurrence, 1 run, skipped_count=$SKIPPED)"
q "UPDATE schedules SET paused=true, next_due_at=NULL WHERE id='$S1'::uuid" >/dev/null

# 4. TEST 2 — SKIPPED_QUOTA: saturate the env cap with PAUSED filler runs
# (PAUSED counts as nonterminal for admission but is never claimed by the
# dispatcher, so the filler is inert).
S2="$(mk_schedule '0 12 * * *')"
q "INSERT INTO runs (organization_id, environment_id, deployment_id, workflow_name, idempotency_key, status, revision, input, last_event_sequence, created_at, updated_at) SELECT '$ORG_ID'::uuid, '$ENV_ID'::uuid, '$NEW_DEPLOY_ID'::uuid, 'filler', 'sched-accept-filler-'||g, 'PAUSED', 1, '{}'::jsonb, 1, clock_timestamp(), clock_timestamp() FROM generate_series(1, 120) g" >/dev/null
NONTERM="$(q "SELECT count(*) FROM runs WHERE environment_id='$ENV_ID'::uuid AND status IN ('QUEUED','RUNNING','WAITING','PAUSING','PAUSED','CANCELLING')")"
info "quota schedule_id=$S2 planted; nonterminal runs in fixture env=$NONTERM"
[[ "$NONTERM" -ge 100 ]] || fail "filler did not saturate the 100-run cap (got $NONTERM)"
q "UPDATE schedules SET next_due_at = clock_timestamp() - INTERVAL '1 minute' WHERE id='$S2'::uuid" >/dev/null
sleep 20
Q_STATUS="$(q "SELECT status FROM schedule_occurrences WHERE schedule_id='$S2'::uuid LIMIT 1")"
Q_REASON="$(q "SELECT skipped_reason FROM schedule_occurrences WHERE schedule_id='$S2'::uuid LIMIT 1")"
Q_RUNS="$(q "SELECT count(*) FROM schedule_occurrences WHERE schedule_id='$S2'::uuid AND run_id IS NOT NULL")"
Q_ALERTS="$(q "SELECT count(*) FROM audit_events WHERE organization_id='$ORG_ID'::uuid AND action='schedule.occurrence_skipped' AND target_id='$S2'::uuid")"
Q_NEXT="$(q "SELECT next_due_at > clock_timestamp() FROM schedules WHERE id='$S2'::uuid")"
info "quota status=$Q_STATUS reason=$Q_REASON runs=$Q_RUNS alerts=$Q_ALERTS next_due_future=$Q_NEXT"
[[ "$Q_STATUS" == "SKIPPED" ]] || fail "quota occurrence status = $Q_STATUS, want SKIPPED"
[[ "$Q_REASON" == "SKIPPED_QUOTA" ]] || fail "skipped_reason = $Q_REASON, want SKIPPED_QUOTA"
[[ "$Q_RUNS" == "0" ]] || fail "a quota-skipped slot must not create a run"
[[ "$Q_ALERTS" -ge 1 ]] || fail "a quota skip must leave an operator-visible alert"
[[ "$Q_NEXT" == "t" ]] || fail "quota skip must advance next_due_at without a hidden backlog"
pass "SKIPPED_QUOTA proven on staging (schedule $S2, SKIPPED/SKIPPED_QUOTA, no run, $Q_ALERTS alert)"
q "UPDATE schedules SET paused=true, next_due_at=NULL WHERE id='$S2'::uuid" >/dev/null
q "DELETE FROM runs WHERE environment_id='$ENV_ID'::uuid AND workflow_name='filler'" >/dev/null
NONTERM_AFTER="$(q "SELECT count(*) FROM runs WHERE environment_id='$ENV_ID'::uuid AND status IN ('QUEUED','RUNNING','WAITING','PAUSING','PAUSED','CANCELLING')")"
info "filler removed; nonterminal runs in fixture env now=$NONTERM_AFTER"

# 5. TEST 3 — revision change affects only future occurrences.
S3="$(mk_schedule '0 12 * * *')"
q "UPDATE schedules SET next_due_at = clock_timestamp() - INTERVAL '1 minute' WHERE id='$S3'::uuid" >/dev/null
sleep 20
REV1="$(q "SELECT revision FROM schedule_occurrences WHERE schedule_id='$S3'::uuid ORDER BY due_at LIMIT 1")"
[[ -n "$REV1" ]] || fail "revision test produced no first occurrence"
info "revision schedule_id=$S3 first occurrence revision=$REV1"
q "UPDATE schedules SET cron_expression='30 6 * * *', revision=revision+1, next_due_at = clock_timestamp() - INTERVAL '1 minute', updated_at=clock_timestamp() WHERE id='$S3'::uuid" >/dev/null
sleep 20
REVS="$(q "SELECT revision FROM schedule_occurrences WHERE schedule_id='$S3'::uuid ORDER BY due_at")"
REV_COUNT="$(q "SELECT count(*) FROM schedule_occurrences WHERE schedule_id='$S3'::uuid")"
FIRST_REV="$(echo "$REVS" | head -n 1)"
LAST_REV="$(echo "$REVS" | tail -n 1)"
info "revision occurrences=$REV_COUNT revisions: first=$FIRST_REV last=$LAST_REV"
[[ "$REV_COUNT" == "2" ]] || fail "expected 2 occurrences across the edit, got $REV_COUNT"
[[ "$FIRST_REV" == "$REV1" ]] || fail "an edit must not rewrite history: $REV1 -> $FIRST_REV"
[[ "$LAST_REV" == "$((REV1 + 1))" ]] || fail "new occurrence revision = $LAST_REV, want $((REV1 + 1))"
pass "revision-change-under-evaluation proven on staging (schedule $S3, rev $FIRST_REV untouched, new rev $LAST_REV)"
q "UPDATE schedules SET paused=true, next_due_at=NULL WHERE id='$S3'::uuid" >/dev/null

# 6. Sweeper log evidence (best effort, non-fatal).
docker logs "deadbolt-staging-control-plane-${ACTIVE_SLOT}" --since 10m 2>/dev/null \
  | grep -c "Schedule .* failed evaluation" | xargs -I{} echo "[ACCEPT-INFO] per-schedule evaluation failures in last 10m: {}" || true

info "fixture project=$PROJECT_NAME env_id=$ENV_ID schedules=$S1,$S2,$S3 (all paused)"
pass "STAGING ACCEPTANCE COMPLETE: coalesce-one, SKIPPED_QUOTA, revision-change all proven on $EXPECTED_COMMIT"
