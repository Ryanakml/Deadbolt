package integration_test

// M4 gate (Issue #36): the combined approval → delay wait in one workflow,
// across engine restarts, with exactly-once human and timer actions.
//
// Approvals (#32), delays (#33), and schedules (#34) are each proven alone.
// This file proves they compose: one workflow parks on a person, then on a
// timer, then runs — while every process involved is killed and recreated
// in between, and no runner is ever held for the waits.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"net/http/httptest"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/scheduling"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
)

// m4CombinedManifest declares: build -> gate(approval) -> wait(delay 60s)
// -> publish. Two waits back to back, then the only claimable task.
func m4CombinedManifest() []byte {
	decisionSchema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"decision": map[string]any{"type": "string"},
			"actorId":  map[string]any{"type": "string"},
		},
		"required":             []any{"decision"},
		"additionalProperties": true,
	}
	numSchema := map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"n": map[string]any{"type": "integer"}},
		"required":             []any{"n"},
		"additionalProperties": false,
	}
	publishInputSchema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"n":        map[string]any{"type": "integer"},
			"decision": map[string]any{"type": "string"},
		},
		"required":             []any{"n", "decision"},
		"additionalProperties": false,
	}
	tasks := []map[string]any{
		{"name": "t-build", "inputSchema": numSchema, "outputSchema": numSchema, "recovery": "safe", "entrypoint": "tasks/build.js"},
		{"name": "t-publish", "inputSchema": publishInputSchema, "outputSchema": numSchema, "recovery": "safe", "entrypoint": "tasks/publish.js"},
	}
	workflows := []map[string]any{{
		"manifestVersion": 1, "name": "wf-m4-combined",
		"inputSchema": numSchema, "outputSchema": numSchema,
		"nodes": []map[string]any{
			{"id": "build", "type": "task", "task": "t-build",
				"input": map[string]any{"n": map[string]any{"literal": float64(7)}}},
			{
				"id": "gate", "type": "approval", "after": []any{"build"},
				"approval": map[string]any{
					"payload":            map[string]any{"question": "Publish build 7 after a wait?"},
					"requiredPermission": contracts.ApprovalDecisionCapability,
					"outputSchema":       decisionSchema,
				},
			},
			{"id": "wait", "type": "delay", "after": []any{"gate"}, "delayMs": float64(60000)},
			{"id": "publish", "type": "task", "task": "t-publish", "after": []any{"wait"},
				"input": map[string]any{
					"n":        map[string]any{"literal": float64(11)},
					"decision": map[string]any{"$ref": "step.output", "stepId": "gate", "pointer": "/decision"},
				}},
		},
		"output": map[string]any{"n": map[string]any{"$ref": "step.output", "stepId": "publish", "pointer": "/n"}},
	}}
	return createLifecycleManifest(approvalBundleDigest, tasks, workflows)
}

type m4CombinedFixture struct {
	tc       *tenantTestContext
	server   *httptest.Server
	engine   *execution.WorkerEngine
	orgID    string
	envID    string
	apiKey   *tenant.GeneratedKey
	operator string
}

func setupM4Combined(t *testing.T) *m4CombinedFixture {
	t.Helper()
	tc, server, orgID, envID, apiKey := setupRunLifecycleTest(t)
	t.Cleanup(func() {
		server.Close()
		tc.cleanup()
	})
	engine := execution.NewWorkerEngine(tc.pool)
	engine.SetCommands(tc.service)
	operator, _ := tenant.NewUUID()
	if _, err := tc.service.AddMember(context.Background(), orgID, operator, tenant.RoleOperator); err != nil {
		t.Fatalf("add operator member: %v", err)
	}
	registerAndActivateTestWorkflow(t, tc, server, apiKey, orgID, envID, "wf-m4-combined", m4CombinedManifest())
	return &m4CombinedFixture{
		tc: tc, server: server, engine: engine,
		orgID: orgID, envID: envID, apiKey: apiKey, operator: operator,
	}
}

// restartedEngine stands up a brand new engine against the same pool, as a
// restarted control-plane process would. Nothing about the waits may depend
// on surviving process memory.
func (f *m4CombinedFixture) restartedEngine() *execution.WorkerEngine {
	eng := execution.NewWorkerEngine(f.tc.pool)
	eng.SetCommands(f.tc.service)
	return eng
}

func (f *m4CombinedFixture) newRun(t *testing.T, key string) execution.RunDTO {
	t.Helper()
	return m3CreateRun(t, f.server.URL, "wf-m4-combined", f.apiKey.PlaintextKey, f.orgID, map[string]any{"n": float64(7)}, key)
}

func (f *m4CombinedFixture) claimBuild(t *testing.T, s *testWorkerSession) {
	t.Helper()
	asgns := parallelClaim(t, f.engine, s, f.orgID, f.envID, 1, "m4-combined-claim-"+t.Name())
	if len(asgns) != 1 {
		t.Fatalf("expected the build step to be the only claimable work, got %d", len(asgns))
	}
	if node := m3NodeForStep(t, f.tc, f.orgID, asgns[0].StepID); node != "build" {
		t.Fatalf("expected to claim node build, got %q", node)
	}
	parallelStart(t, f.engine, s, f.orgID, f.envID, asgns[0])
	parallelCompleteSuccess(t, f.engine, s, f.orgID, f.envID, asgns[0], map[string]any{"n": float64(7)})
}

func (f *m4CombinedFixture) pendingApproval(t *testing.T, runID string) execution.ApprovalDTO {
	t.Helper()
	snap := m2GetSnapshot(t, f.server.URL, runID, f.apiKey.PlaintextKey, f.orgID)
	for _, a := range snap.Approvals {
		if a.NodeID == "gate" {
			return a
		}
	}
	t.Fatalf("no approval exposed for run %s", runID)
	return execution.ApprovalDTO{}
}

func (f *m4CombinedFixture) gateStepID(t *testing.T, runID string) string {
	t.Helper()
	snap := m2GetSnapshot(t, f.server.URL, runID, f.apiKey.PlaintextKey, f.orgID)
	for _, s := range snap.Steps {
		if s.NodeID == "gate" {
			return s.ID
		}
	}
	t.Fatalf("gate step not found in snapshot for run %s", runID)
	return ""
}

// waitForDelayTimer polls for the wait node's persisted timer. The timer is
// created by DAG progression after the gate is decided; the test must not
// assume which call performed that progression.
func (f *m4CombinedFixture) waitForDelayTimer(t *testing.T, runID string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var n int
		err := f.tc.pool.WithTenantTx(context.Background(), f.orgID, func(ctx context.Context, tx pgx.Tx) error {
			return tx.QueryRow(ctx, `SELECT count(*) FROM timers
				WHERE organization_id=$1::uuid AND run_id=$2::uuid AND kind='DELAY' AND state='PENDING'`,
				f.orgID, runID).Scan(&n)
		})
		if err != nil {
			t.Fatalf("count delay timers: %v", err)
		}
		if n == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no PENDING delay timer appeared for run %s", runID)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestM4_CombinedApprovalDelayRestartDecidesOnce is the gate's core: one
// run waits on a person, then on a timer, then completes — with the engine
// recreated from scratch between every step, and each wait settling
// exactly once.
func TestM4_CombinedApprovalDelayRestartDecidesOnce(t *testing.T) {
	f := setupM4Combined(t)
	sess, _ := enrollExecutionWorker(t, f.tc, f.server, f.orgID, f.envID, "m4c-w1")
	m3AssociateWorkerDeployment(t, f.tc, f.orgID, sess.SessionID, approvalBundleDigest)
	ctx := context.Background()

	run := f.newRun(t, "m4-combined-restart-001")
	f.claimBuild(t, sess)
	approval := f.pendingApproval(t, run.ID)
	if approval.Status != "PENDING" {
		t.Fatalf("expected PENDING approval, got %q", approval.Status)
	}

	// Restart 1: a fresh engine decides the pending approval.
	decided, err := f.restartedEngine().DecideApproval(ctx, f.orgID, approval.ID,
		execution.DecideApprovalRequest{Decision: contracts.ApprovalDecisionApproved, ExpectedRevision: approval.Revision},
		approvalHumanAudit(f.operator))
	if err != nil {
		t.Fatalf("restarted engine must decide: %v", err)
	}
	if decided.Status != "APPROVED" {
		t.Fatalf("expected APPROVED, got %q", decided.Status)
	}

	// The opposing decision is a conflict, not a second truth: one logical
	// action comes out of the gate no matter how many times it is poked.
	if _, err := f.restartedEngine().DecideApproval(ctx, f.orgID, approval.ID,
		execution.DecideApprovalRequest{Decision: contracts.ApprovalDecisionRejected, ExpectedRevision: decided.Revision},
		approvalHumanAudit(f.operator)); !errors.Is(err, execution.ErrApprovalConflict) {
		t.Fatalf("opposing decision must conflict, got %v", err)
	}

	// The decided gate releases the delay wait; force it due without
	// running any clock, exactly as downtime would leave it.
	f.waitForDelayTimer(t, run.ID)
	var forced int
	if err := f.tc.pool.WithTenantTx(ctx, f.orgID, func(ctx context.Context, tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE timers SET due_at=clock_timestamp()-INTERVAL '1 second'
			WHERE organization_id=$1::uuid AND run_id=$2::uuid AND kind='DELAY' AND state='PENDING'`,
			f.orgID, run.ID)
		if err != nil {
			return err
		}
		forced = int(tag.RowsAffected())
		return nil
	}); err != nil || forced != 1 {
		t.Fatalf("force delay due: updated=%d err=%v", forced, err)
	}

	// Restart 2: a fresh engine fires the due timer exactly once.
	fired, err := f.restartedEngine().FireDueDelayTimers(ctx, f.orgID)
	if err != nil || fired != 1 {
		t.Fatalf("restarted engine must fire the delay once, fired=%d err=%v", fired, err)
	}
	if fired, err := f.restartedEngine().FireDueDelayTimers(ctx, f.orgID); err != nil || fired != 0 {
		t.Fatalf("duplicate timer firing must be a no-op, fired=%d err=%v", fired, err)
	}

	// Only publish is claimable now; it consumes the committed decision.
	asgns := parallelClaim(t, f.engine, sess, f.orgID, f.envID, 1, "m4-combined-claim-publish")
	if len(asgns) != 1 {
		t.Fatalf("expected publish to be the only claimable work, got %d", len(asgns))
	}
	if node := m3NodeForStep(t, f.tc, f.orgID, asgns[0].StepID); node != "publish" {
		t.Fatalf("expected to claim node publish, got %q", node)
	}
	parallelStart(t, f.engine, sess, f.orgID, f.envID, asgns[0])
	parallelCompleteSuccess(t, f.engine, sess, f.orgID, f.envID, asgns[0], map[string]any{"n": float64(11)})

	snap := m2GetSnapshot(t, f.server.URL, run.ID, f.apiKey.PlaintextKey, f.orgID)
	if string(snap.Status) != "SUCCEEDED" {
		t.Fatalf("combined run must succeed, got %s", snap.Status)
	}
	var decisionCount int
	if err := f.tc.pool.WithTenantTx(ctx, f.orgID, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM approvals WHERE step_id=$1::uuid AND status IN ('APPROVED','REJECTED')`,
			f.gateStepID(t, run.ID)).Scan(&decisionCount)
	}); err != nil || decisionCount != 1 {
		t.Fatalf("exactly one decision must be recorded, count=%d err=%v", decisionCount, err)
	}
}

// TestM4_CombinedStaleRevisionCannotDecide proves the two-editor rule
// survives inside the combined graph: a stale screen cannot decide.
func TestM4_CombinedStaleRevisionCannotDecide(t *testing.T) {
	f := setupM4Combined(t)
	sess, _ := enrollExecutionWorker(t, f.tc, f.server, f.orgID, f.envID, "m4c-w2")
	m3AssociateWorkerDeployment(t, f.tc, f.orgID, sess.SessionID, approvalBundleDigest)
	ctx := context.Background()

	run := f.newRun(t, "m4-combined-stale-001")
	f.claimBuild(t, sess)
	approval := f.pendingApproval(t, run.ID)

	if _, err := f.restartedEngine().DecideApproval(ctx, f.orgID, approval.ID,
		execution.DecideApprovalRequest{Decision: contracts.ApprovalDecisionApproved, ExpectedRevision: approval.Revision + 7},
		approvalHumanAudit(f.operator)); !errors.Is(err, execution.ErrRevisionConflict) {
		t.Fatalf("stale expectedRevision must conflict, got %v", err)
	}
	still := f.pendingApproval(t, run.ID)
	if still.Status != "PENDING" {
		t.Fatalf("refused decision must leave the approval pending, got %q", still.Status)
	}
}

// TestM4_CombinedExpiredApprovalFailsRun proves expiry still fails the run
// when a delay waits downstream: the timer is never scheduled.
func TestM4_CombinedExpiredApprovalFailsRun(t *testing.T) {
	f := setupM4Combined(t)
	sess, _ := enrollExecutionWorker(t, f.tc, f.server, f.orgID, f.envID, "m4c-w3")
	m3AssociateWorkerDeployment(t, f.tc, f.orgID, sess.SessionID, approvalBundleDigest)
	ctx := context.Background()

	run := f.newRun(t, "m4-combined-expired-001")
	f.claimBuild(t, sess)
	approval := f.pendingApproval(t, run.ID)

	var expired int
	if err := f.tc.pool.WithTenantTx(ctx, f.orgID, func(ctx context.Context, tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE approvals SET expires_at = clock_timestamp() - INTERVAL '1 minute' WHERE id=$1::uuid`, approval.ID)
		if err != nil {
			return err
		}
		expired = int(tag.RowsAffected())
		return nil
	}); err != nil || expired != 1 {
		t.Fatalf("expire approval: updated=%d err=%v", expired, err)
	}
	if _, err := f.restartedEngine().DecideApproval(ctx, f.orgID, approval.ID,
		execution.DecideApprovalRequest{Decision: contracts.ApprovalDecisionApproved, ExpectedRevision: approval.Revision},
		approvalHumanAudit(f.operator)); !errors.Is(err, execution.ErrApprovalExpired) {
		t.Fatalf("decision after the deadline must be refused, got %v", err)
	}
	snap := m2GetSnapshot(t, f.server.URL, run.ID, f.apiKey.PlaintextKey, f.orgID)
	if string(snap.Status) != "FAILED" {
		t.Fatalf("expired approval must fail the run, got %s", snap.Status)
	}
	if snap.ReasonCode == nil || *snap.ReasonCode != "APPROVAL_EXPIRED" {
		t.Fatalf("expected reason APPROVAL_EXPIRED, got %v", snap.ReasonCode)
	}
	var timers int
	if err := f.tc.pool.WithTenantTx(ctx, f.orgID, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM timers
			WHERE organization_id=$1::uuid AND run_id=$2::uuid AND kind='DELAY'`, f.orgID, run.ID).Scan(&timers)
	}); err != nil || timers != 0 {
		t.Fatalf("a failed run must never schedule the downstream delay, timers=%d err=%v", timers, err)
	}
}

// TestM4_ScheduleRestartEvaluatesOnce proves the schedule side of the gate:
// a recreated occurrence engine over the same database evaluates a due
// schedule exactly once and converges instead of replaying.
func TestM4_ScheduleRestartEvaluatesOnce(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()
	ctx := context.Background()

	org, err := tc.service.CreateOrganization(ctx, newUUID(t), "M4 Gate Org")
	if err != nil {
		t.Fatalf("create organization: %v", err)
	}
	proj, err := tc.service.CreateProject(ctx, org.ID, "M4 Gate Proj")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	env, err := tc.service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvStaging, 10)
	if err != nil {
		t.Fatalf("create environment: %v", err)
	}
	svc := scheduling.NewService(tc.pool)
	runs := execution.NewService(tc.pool, tc.service)
	first := scheduling.NewEngine(tc.pool, runs, tc.service)

	s, err := svc.Create(ctx, org.ID, env.ID, scheduling.CreateScheduleRequest{
		WorkflowName: "m4-gate-flow", CronExpression: "* * * * *", Timezone: "UTC",
	}, &tenant.AuditContext{ActorType: tenant.IdentityTypeMachine})
	if err != nil {
		t.Fatalf("create schedule: %v", err)
	}
	// A schedulable deployment the restarted engine can pin.
	if err := tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx pgx.Tx) error {
		var deployID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO deployments
				(organization_id,environment_id,manifest_hash,bundle_digest,manifest,protocol_version,runtime_version,status)
			VALUES ($1::uuid,$2::uuid,$3,$4,$5::jsonb,1,'1.0','ACTIVE')
			RETURNING id::text`,
			org.ID, env.ID, "manifest-m4-gate",
			"m4gatebundle00000000000000000000000000",
			`{"workflows":[{"manifestVersion":1,"name":"m4-gate-flow","inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"nodes":[],"output":{}}]}`).Scan(&deployID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO workflow_channels
				(organization_id, environment_id, workflow_name, active_deployment_id, revision, updated_at)
			VALUES ($1::uuid,$2::uuid,'m4-gate-flow',$3::uuid,1,clock_timestamp())`,
			org.ID, env.ID, deployID)
		return err
	}); err != nil {
		t.Fatalf("seed deployment: %v", err)
	}
	if err := tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE schedules SET next_due_at=clock_timestamp()-INTERVAL '1 second'
			WHERE id=$1::uuid AND organization_id=$2::uuid`, s.ID, org.ID)
		return err
	}); err != nil {
		t.Fatalf("make due: %v", err)
	}

	outcome, err := first.EvaluateSchedule(ctx, org.ID, env.ID, s.ID)
	if err != nil {
		t.Fatalf("first evaluate: %v", err)
	}
	if outcome != scheduling.OutcomeStarted {
		t.Fatalf("outcome = %s, want STARTED", outcome)
	}

	// Restart: a brand new engine against the same database.
	restarted := scheduling.NewEngine(tc.pool, runs, tc.service)
	outcome, err = restarted.EvaluateSchedule(ctx, org.ID, env.ID, s.ID)
	if err != nil {
		t.Fatalf("restarted evaluate: %v", err)
	}
	if outcome != scheduling.OutcomeNotDue {
		t.Fatalf("restarted outcome = %s, want NOT_DUE (schedule already advanced)", outcome)
	}
	var occurrences, linkedRuns int
	if err := tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM schedule_occurrences WHERE schedule_id=$1::uuid`, s.ID).Scan(&occurrences); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT count(*) FROM schedule_occurrences WHERE schedule_id=$1::uuid AND run_id IS NOT NULL`, s.ID).Scan(&linkedRuns)
	}); err != nil {
		t.Fatalf("count occurrences: %v", err)
	}
	if occurrences != 1 || linkedRuns != 1 {
		t.Fatalf("restart must not duplicate: occurrences=%d runs=%d", occurrences, linkedRuns)
	}
}
