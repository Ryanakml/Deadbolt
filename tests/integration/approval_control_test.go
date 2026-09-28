package integration_test

// M4 / Issue #32: human approval control nodes (Blueprint §16.3).
//
// These tests run against real PostgreSQL with the real engine. They are the
// evidence for the acceptance items that matter: a pending approval holds no
// lease, attempt, or runner; approve and reject are both successful step
// outcomes; a decision requires an identifiable human; expiry is decided by
// database time; and exactly one decision can ever commit (INV-10, F-14).

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/jackc/pgx/v5"
)

const approvalBundleDigest = "5151515151515151515151515151515151515151515151515151515151515151"

func approvalHumanAudit(userID string) *tenant.AuditContext {
	return &tenant.AuditContext{
		ActorID:       &userID,
		ActorType:     tenant.IdentityTypeHuman,
		Role:          tenant.RoleOperator,
		Capabilities:  tenant.RoleCapabilities(tenant.RoleOperator),
		CorrelationID: "m4-approval-fixture",
	}
}

// approvalWorkflowManifest declares: build -> approval -> publish.
//
// The approval node is a control node, so the graph still has three steps but
// only two of them are ever claimable by a worker.
func approvalWorkflowManifest() []byte {
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
	// The publish node consumes the committed decision, which is the only
	// reason to place an approval in a graph: the human answer has to be
	// readable by the rest of the workflow.
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
		"manifestVersion": 1, "name": "wf-approval",
		"inputSchema": numSchema, "outputSchema": numSchema,
		"nodes": []map[string]any{
			{"id": "build", "type": "task", "task": "t-build",
				"input": map[string]any{"n": map[string]any{"literal": float64(7)}}},
			{
				"id": "gate", "type": "approval", "after": []any{"build"},
				"approval": map[string]any{
					"payload":            map[string]any{"question": "Publish build 7?", "risk": "high"},
					"requiredPermission": contracts.ApprovalDecisionCapability,
					"outputSchema":       decisionSchema,
				},
			},
			{"id": "publish", "type": "task", "task": "t-publish", "after": []any{"gate"},
				"input": map[string]any{
					"n":        map[string]any{"literal": float64(11)},
					"decision": map[string]any{"$ref": "step.output", "stepId": "gate", "pointer": "/decision"},
				}},
		},
		"output": map[string]any{"n": map[string]any{"$ref": "step.output", "stepId": "publish", "pointer": "/n"}},
	}}
	return createLifecycleManifest(approvalBundleDigest, tasks, workflows)
}

// approvalGateFixture wires a registered + activated approval workflow and
// returns the pieces every test below needs.
type approvalGateFixture struct {
	tc       *tenantTestContext
	server   *httptest.Server
	engine   *execution.WorkerEngine
	orgID    string
	envID    string
	apiKey   *tenant.GeneratedKey
	operator string
}

func setupApprovalGate(t *testing.T) *approvalGateFixture {
	t.Helper()
	tc, server, orgID, envID, apiKey := setupRunLifecycleTest(t)
	t.Cleanup(func() {
		server.Close()
		tc.cleanup()
	})

	engine := execution.NewWorkerEngine(tc.pool)
	engine.SetCommands(tc.service)

	// The decision endpoint is human-only, so the fixture needs a real member
	// with a role that carries approvals:decide (§24.2).
	operator, _ := tenant.NewUUID()
	if _, err := tc.service.AddMember(context.Background(), orgID, operator, tenant.RoleOperator); err != nil {
		t.Fatalf("add operator member: %v", err)
	}

	registerAndActivateTestWorkflow(t, tc, server, apiKey, orgID, envID, "wf-approval", approvalWorkflowManifest())
	return &approvalGateFixture{
		tc: tc, server: server, engine: engine,
		orgID: orgID, envID: envID, apiKey: apiKey, operator: operator,
	}
}

func (f *approvalGateFixture) newRun(t *testing.T, key string) execution.RunDTO {
	t.Helper()
	run := m3CreateRun(t, f.server.URL, "wf-approval", f.apiKey.PlaintextKey, f.orgID, map[string]any{"n": float64(7)}, key)
	return run
}

// claimBuild claims and completes the single task node preceding the gate.
func (f *approvalGateFixture) claimBuild(t *testing.T, s *testWorkerSession) {
	t.Helper()
	asgns := parallelClaim(t, f.engine, s, f.orgID, f.envID, 1, "m4-claim-"+t.Name())
	if len(asgns) != 1 {
		t.Fatalf("expected the build step to be the only claimable work, got %d", len(asgns))
	}
	if node := m3NodeForStep(t, f.tc, f.orgID, asgns[0].StepID); node != "build" {
		t.Fatalf("expected to claim node build, got %q", node)
	}
	parallelStart(t, f.engine, s, f.orgID, f.envID, asgns[0])
	parallelCompleteSuccess(t, f.engine, s, f.orgID, f.envID, asgns[0], map[string]any{"n": float64(7)})
}

func (f *approvalGateFixture) gateStepID(t *testing.T, runID string) string {
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

func (f *approvalGateFixture) pendingApproval(t *testing.T, runID string) execution.ApprovalDTO {
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

// 1. An eligible approval parks the step and never becomes claimable work.
func TestApproval_EligibleApprovalWaitsWithoutLeaseOrAttempt(t *testing.T) {
	f := setupApprovalGate(t)
	sess, _ := enrollExecutionWorker(t, f.tc, f.server, f.orgID, f.envID, "m4-w1")
	m3AssociateWorkerDeployment(t, f.tc, f.orgID, sess.SessionID, approvalBundleDigest)

	run := f.newRun(t, "m4-approval-eligible-001")
	f.claimBuild(t, sess)

	approval := f.pendingApproval(t, run.ID)
	if approval.Status != "PENDING" {
		t.Fatalf("expected PENDING approval, got %q", approval.Status)
	}
	if approval.RequiredPermission != contracts.ApprovalDecisionCapability {
		t.Fatalf("approval must record the canonical decision permission, got %q", approval.RequiredPermission)
	}
	if approval.ExpiresAt == nil {
		t.Fatal("approval must persist an expiry so waiting survives process death")
	}

	snap := m2GetSnapshot(t, f.server.URL, run.ID, f.apiKey.PlaintextKey, f.orgID)
	if string(snap.Status) != "WAITING" {
		t.Fatalf("a run whose only remaining work is human waiting must be WAITING, got %s", snap.Status)
	}
	// The run reason names the durable wait; the snapshot's waitingReason is the
	// separate no-compatible-worker diagnostic and must stay nil here because
	// workers are online and simply have nothing claimable.
	if snap.ReasonCode == nil || *snap.ReasonCode != "APPROVAL" {
		t.Fatalf("run reason must name the durable wait APPROVAL, got %v", snap.ReasonCode)
	}
	if snap.WaitingReason != nil {
		t.Fatalf("waitingReason is the no-worker diagnostic and must stay nil, got %q", *snap.WaitingReason)
	}
	var gateWaitReason *string
	for _, s := range snap.Steps {
		if s.NodeID == "gate" {
			gateWaitReason = s.WaitReason
			if string(s.Status) != "WAITING" {
				t.Fatalf("the gate step must be WAITING, got %s", s.Status)
			}
		}
	}
	if gateWaitReason == nil || *gateWaitReason != "APPROVAL" {
		t.Fatalf("gate step wait reason must be APPROVAL, got %v", gateWaitReason)
	}

	gateStep := f.gateStepID(t, run.ID)
	var gateAttempts, gateLeases int
	countRow(t, f, `SELECT count(*) FROM task_attempts WHERE step_id=$1::uuid`, gateStep, &gateAttempts)
	countRow(t, f, `SELECT count(*) FROM task_leases WHERE step_id=$1::uuid`, gateStep, &gateLeases)
	if gateAttempts != 0 {
		t.Fatalf("an approval control node must never create an attempt, got %d", gateAttempts)
	}
	if gateLeases != 0 {
		t.Fatalf("an approval control node must never hold a lease, got %d", gateLeases)
	}

	// A worker polling now must be told there is no work: the gate is waiting
	// on a person, not on capacity.
	if got := parallelClaim(t, f.engine, sess, f.orgID, f.envID, 1, "m4-poll-after-gate"); len(got) != 0 {
		t.Fatalf("expected no claimable work while approval is pending, got %d assignments", len(got))
	}
}

// 2. Approve is a successful step outcome and unblocks downstream work.
func TestApproval_ApproveSucceedsStepAndContinuesWorkflow(t *testing.T) {
	f := setupApprovalGate(t)
	sess, _ := enrollExecutionWorker(t, f.tc, f.server, f.orgID, f.envID, "m4-w2")
	m3AssociateWorkerDeployment(t, f.tc, f.orgID, sess.SessionID, approvalBundleDigest)

	run := f.newRun(t, "m4-approval-approve-001")
	f.claimBuild(t, sess)
	approval := f.pendingApproval(t, run.ID)

	decided, err := f.engine.DecideApproval(context.Background(), f.orgID, approval.ID,
		execution.DecideApprovalRequest{Decision: contracts.ApprovalDecisionApproved, ExpectedRevision: approval.Revision, Comment: "reviewed"},
		approvalHumanAudit(f.operator))
	if err != nil {
		t.Fatalf("approve failed: %v", err)
	}
	if decided.Status != "APPROVED" {
		t.Fatalf("expected APPROVED, got %q", decided.Status)
	}
	if decided.Revision != approval.Revision+1 {
		t.Fatalf("a decision must advance the durable revision, got %d", decided.Revision)
	}

	snap := m2GetSnapshot(t, f.server.URL, run.ID, f.apiKey.PlaintextKey, f.orgID)
	var gateOutput map[string]any
	for _, s := range snap.Steps {
		if s.NodeID == "gate" {
			if string(s.Status) != "SUCCEEDED" {
				t.Fatalf("approve must make the step SUCCEEDED, got %s", s.Status)
			}
			b, _ := json.Marshal(s.Output)
			_ = json.Unmarshal(b, &gateOutput)
		}
	}
	if gateOutput["decision"] != contracts.ApprovalDecisionApproved {
		t.Fatalf("gate output must carry the business decision, got %v", gateOutput["decision"])
	}
	if gateOutput["actorId"] != f.operator {
		t.Fatalf("gate output must record the deciding actor, got %v", gateOutput["actorId"])
	}
	if gateOutput["comment"] != "reviewed" {
		t.Fatalf("gate output must carry the comment, got %v", gateOutput["comment"])
	}

	// A decided run must stop claiming to be waiting on a person. §10.2 rule 6
	// says claimable work means RUNNING, and a RUNNING run carries no wait
	// reason, so a live run must not still be labelled APPROVAL.
	mid := m2GetSnapshot(t, f.server.URL, run.ID, f.apiKey.PlaintextKey, f.orgID)
	if string(mid.Status) != "RUNNING" {
		t.Fatalf("a run with claimable work must be RUNNING, got %s", mid.Status)
	}
	if mid.ReasonCode != nil {
		t.Fatalf("a RUNNING run must carry no wait reason, got %q", *mid.ReasonCode)
	}

	// Downstream work is now eligible and the run finishes normally.
	asgns := parallelClaim(t, f.engine, sess, f.orgID, f.envID, 1, "m4-claim-publish")
	if len(asgns) != 1 || m3NodeForStep(t, f.tc, f.orgID, asgns[0].StepID) != "publish" {
		t.Fatalf("expected publish to become claimable after approval, got %v", asgns)
	}
	parallelStart(t, f.engine, sess, f.orgID, f.envID, asgns[0])
	parallelCompleteSuccess(t, f.engine, sess, f.orgID, f.envID, asgns[0], map[string]any{"n": float64(11)})

	final := m2GetSnapshot(t, f.server.URL, run.ID, f.apiKey.PlaintextKey, f.orgID)
	if string(final.Status) != "SUCCEEDED" {
		t.Fatalf("expected the run to succeed after approval, got %s", final.Status)
	}
}

// 3. Reject is a business outcome, not a technical error: the step still
// succeeds and the run continues so a following choice can act on it.
func TestApproval_RejectSucceedsStepAndRunContinues(t *testing.T) {
	f := setupApprovalGate(t)
	sess, _ := enrollExecutionWorker(t, f.tc, f.server, f.orgID, f.envID, "m4-w3")
	m3AssociateWorkerDeployment(t, f.tc, f.orgID, sess.SessionID, approvalBundleDigest)

	run := f.newRun(t, "m4-approval-reject-001")
	f.claimBuild(t, sess)
	approval := f.pendingApproval(t, run.ID)

	if _, err := f.engine.DecideApproval(context.Background(), f.orgID, approval.ID,
		execution.DecideApprovalRequest{Decision: contracts.ApprovalDecisionRejected, ExpectedRevision: approval.Revision, Comment: "not yet"},
		approvalHumanAudit(f.operator)); err != nil {
		t.Fatalf("reject failed: %v", err)
	}

	snap := m2GetSnapshot(t, f.server.URL, run.ID, f.apiKey.PlaintextKey, f.orgID)
	if string(snap.Status) == "FAILED" {
		t.Fatal("rejecting an approval must not fail the run; it is a business result (§16.3)")
	}
	for _, s := range snap.Steps {
		if s.NodeID == "gate" && string(s.Status) != "SUCCEEDED" {
			t.Fatalf("reject must still make the step SUCCEEDED, got %s", s.Status)
		}
	}
	if asgns := parallelClaim(t, f.engine, sess, f.orgID, f.envID, 1, "m4-claim-publish-reject"); len(asgns) != 1 {
		t.Fatalf("downstream work must remain eligible after reject, got %d", len(asgns))
	}
}

// 4. Only an identifiable human may decide (§16.3, §24.2, F-21).
func TestApproval_MachineAndWorkerActorsAreRefused(t *testing.T) {
	f := setupApprovalGate(t)
	sess, _ := enrollExecutionWorker(t, f.tc, f.server, f.orgID, f.envID, "m4-w4")
	m3AssociateWorkerDeployment(t, f.tc, f.orgID, sess.SessionID, approvalBundleDigest)

	run := f.newRun(t, "m4-approval-human-only-001")
	f.claimBuild(t, sess)
	approval := f.pendingApproval(t, run.ID)

	keyID, _ := tenant.NewUUID()
	machineAudit := &tenant.AuditContext{
		ActorID: &keyID, ActorType: tenant.IdentityTypeMachine,
		Capabilities: []string{tenant.CapApprovalsDecide}, CorrelationID: "m4-machine",
	}
	_, err := f.engine.DecideApproval(context.Background(), f.orgID, approval.ID,
		execution.DecideApprovalRequest{Decision: contracts.ApprovalDecisionApproved, ExpectedRevision: approval.Revision},
		machineAudit)
	if !errors.Is(err, execution.ErrApprovalHumanOnly) {
		t.Fatalf("a machine key must not decide an approval, got %v", err)
	}

	// A worker session identity is not a human either.
	_, err = f.engine.DecideApproval(context.Background(), f.orgID, approval.ID,
		execution.DecideApprovalRequest{Decision: contracts.ApprovalDecisionApproved, ExpectedRevision: approval.Revision},
		&tenant.AuditContext{ActorID: &sess.WorkerID, ActorType: tenant.IdentityTypeMachine, CorrelationID: "m4-worker"})
	if !errors.Is(err, execution.ErrApprovalHumanOnly) {
		t.Fatalf("a worker session must not decide an approval, got %v", err)
	}

	// The record is untouched after both refusals.
	after := f.pendingApproval(t, run.ID)
	if after.Status != "PENDING" || after.Revision != approval.Revision {
		t.Fatalf("a refused decision must not mutate the approval, got %s rev=%d", after.Status, after.Revision)
	}

	// The HTTP surface refuses a machine caller even when the key carries the
	// capability, so a future route change cannot silently widen this.
	handler := execution.NewHTTPHandler(execution.NewService(f.tc.pool, f.tc.service), f.tc.service)
	handler.SetWorkerEngine(f.engine)
	machineCtx := tenant.ContextWithCaller(context.Background(), &tenant.CallerIdentity{
		Type: tenant.IdentityTypeMachine, KeyID: keyID,
		OrganizationID: f.orgID, EnvironmentID: f.envID,
		Capabilities: []string{tenant.CapApprovalsDecide},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/approvals/"+approval.ID+"/decision",
		jsonBody(map[string]any{"decision": "approved", "expectedRevision": approval.Revision}))
	rec := httptest.NewRecorder()
	handler.HandleDecideApproval(rec, req.WithContext(machineCtx))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("machine caller must be refused with 403, got %d (%s)", rec.Code, rec.Body.String())
	}
}

// 5. F-14: exactly one decision commits. Identical replay is idempotent, an
// opposing decision and a stale revision both conflict.
func TestApproval_SingleDecisionIdempotentAndConflicting(t *testing.T) {
	f := setupApprovalGate(t)
	sess, _ := enrollExecutionWorker(t, f.tc, f.server, f.orgID, f.envID, "m4-w5")
	m3AssociateWorkerDeployment(t, f.tc, f.orgID, sess.SessionID, approvalBundleDigest)

	run := f.newRun(t, "m4-approval-f14-001")
	f.claimBuild(t, sess)
	approval := f.pendingApproval(t, run.ID)

	audit := approvalHumanAudit(f.operator)
	first, err := f.engine.DecideApproval(context.Background(), f.orgID, approval.ID,
		execution.DecideApprovalRequest{Decision: contracts.ApprovalDecisionApproved, ExpectedRevision: approval.Revision},
		audit)
	if err != nil {
		t.Fatalf("first decision failed: %v", err)
	}

	// Identical decision replayed by a double-clicking browser is idempotent.
	replay, err := f.engine.DecideApproval(context.Background(), f.orgID, approval.ID,
		execution.DecideApprovalRequest{Decision: contracts.ApprovalDecisionApproved, ExpectedRevision: approval.Revision},
		approvalHumanAudit(f.operator))
	if err != nil {
		t.Fatalf("identical decision replay must be idempotent, got %v", err)
	}
	if replay.Status != first.Status || replay.Revision != first.Revision {
		t.Fatalf("idempotent replay must not advance state: %+v vs %+v", replay, first)
	}

	// The opposing decision is a conflict, not a second truth (INV-10).
	if _, err := f.engine.DecideApproval(context.Background(), f.orgID, approval.ID,
		execution.DecideApprovalRequest{Decision: contracts.ApprovalDecisionRejected, ExpectedRevision: first.Revision},
		approvalHumanAudit(f.operator)); !errors.Is(err, execution.ErrApprovalConflict) {
		t.Fatalf("an opposing decision must conflict, got %v", err)
	}

	// A stale screen on a still-pending approval cannot decide at all. This is
	// the same rejection an operator sees when someone else already acted.
	// Run 1's publish step is drained first so the next claim unambiguously
	// belongs to the second run.
	if asgns := parallelClaim(t, f.engine, sess, f.orgID, f.envID, 1, "m4-claim-drain-publish"); len(asgns) == 1 {
		parallelStart(t, f.engine, sess, f.orgID, f.envID, asgns[0])
		parallelCompleteSuccess(t, f.engine, sess, f.orgID, f.envID, asgns[0], map[string]any{"n": float64(11)})
	}
	second := f.newRun(t, "m4-approval-f14-002")
	f.claimBuild(t, sess)
	fresh := f.pendingApproval(t, second.ID)
	if _, err := f.engine.DecideApproval(context.Background(), f.orgID, fresh.ID,
		execution.DecideApprovalRequest{Decision: contracts.ApprovalDecisionApproved, ExpectedRevision: fresh.Revision + 7},
		approvalHumanAudit(f.operator)); !errors.Is(err, execution.ErrRevisionConflict) {
		t.Fatalf("a stale expectedRevision on a pending approval must conflict, got %v", err)
	}

	// Exactly one decision is durably recorded, with its actor.
	after := f.pendingApproval(t, run.ID)
	if after.Status != "APPROVED" || after.ActorID == nil || *after.ActorID != f.operator {
		t.Fatalf("expected a single APPROVED decision by the operator, got %+v", after)
	}
	var decisionCount int
	countRow(t, f, `SELECT count(*) FROM approvals WHERE step_id=$1::uuid AND status IN ('APPROVED','REJECTED')`,
		f.gateStepID(t, run.ID), &decisionCount)
	if decisionCount != 1 {
		t.Fatalf("expected exactly one committed decision, got %d", decisionCount)
	}
}

// 6. Two operators deciding at once produce one winner, never a blend.
func TestApproval_ConcurrentDecisionsSingleWinner(t *testing.T) {
	f := setupApprovalGate(t)
	sess, _ := enrollExecutionWorker(t, f.tc, f.server, f.orgID, f.envID, "m4-w6")
	m3AssociateWorkerDeployment(t, f.tc, f.orgID, sess.SessionID, approvalBundleDigest)

	run := f.newRun(t, "m4-approval-concurrent-001")
	f.claimBuild(t, sess)
	approval := f.pendingApproval(t, run.ID)

	other, _ := tenant.NewUUID()
	if _, err := f.tc.service.AddMember(context.Background(), f.orgID, other, tenant.RoleOperator); err != nil {
		t.Fatalf("add racing operator: %v", err)
	}

	type outcome struct {
		decision string
		err      error
	}
	results := make([]outcome, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err := f.engine.DecideApproval(context.Background(), f.orgID, approval.ID,
			execution.DecideApprovalRequest{Decision: contracts.ApprovalDecisionApproved, ExpectedRevision: approval.Revision},
			approvalHumanAudit(f.operator))
		results[0] = outcome{decision: contracts.ApprovalDecisionApproved, err: err}
	}()
	go func() {
		defer wg.Done()
		_, err := f.engine.DecideApproval(context.Background(), f.orgID, approval.ID,
			execution.DecideApprovalRequest{Decision: contracts.ApprovalDecisionRejected, ExpectedRevision: approval.Revision},
			approvalHumanAudit(other))
		results[1] = outcome{decision: contracts.ApprovalDecisionRejected, err: err}
	}()
	wg.Wait()

	wins, conflicts := 0, 0
	for _, r := range results {
		switch {
		case r.err == nil:
			wins++
		case errors.Is(r.err, execution.ErrApprovalConflict), errors.Is(r.err, execution.ErrRevisionConflict):
			conflicts++
		default:
			t.Fatalf("unexpected concurrent decision error: %v", r.err)
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatalf("expected exactly one winner and one conflict, got wins=%d conflicts=%d", wins, conflicts)
	}

	after := f.pendingApproval(t, run.ID)
	if after.Status != "APPROVED" && after.Status != "REJECTED" {
		t.Fatalf("a race must still leave one committed decision, got %q", after.Status)
	}
	var decisionCount int
	countRow(t, f, `SELECT count(*) FROM approvals WHERE step_id=$1::uuid AND decided_at IS NOT NULL`,
		f.gateStepID(t, run.ID), &decisionCount)
	if decisionCount != 1 {
		t.Fatalf("exactly one decision must be durable after a race, got %d", decisionCount)
	}
}

// 7. Expiry is decided by database time inside the decision transaction, so a
// delayed sweeper can never admit a decision on an expired request (§16.3).
func TestApproval_ExpiryUsesDatabaseTimeBeforeSweeper(t *testing.T) {
	f := setupApprovalGate(t)
	sess, _ := enrollExecutionWorker(t, f.tc, f.server, f.orgID, f.envID, "m4-w7")
	m3AssociateWorkerDeployment(t, f.tc, f.orgID, sess.SessionID, approvalBundleDigest)

	run := f.newRun(t, "m4-approval-expiry-001")
	f.claimBuild(t, sess)
	approval := f.pendingApproval(t, run.ID)

	// Force the stored deadline into the past without running the sweeper, which
	// is exactly the situation a delayed background pass produces.
	var expired int
	execRow(t, f, `UPDATE approvals SET expires_at = clock_timestamp() - INTERVAL '1 minute' WHERE id=$1::uuid`, approval.ID, &expired)

	// The decision is refused and the run fails with the explicit reason.
	_, err := f.engine.DecideApproval(context.Background(), f.orgID, approval.ID,
		execution.DecideApprovalRequest{Decision: contracts.ApprovalDecisionApproved, ExpectedRevision: approval.Revision},
		approvalHumanAudit(f.operator))
	if !errors.Is(err, execution.ErrApprovalExpired) {
		t.Fatalf("a decision after the deadline must be refused, got %v", err)
	}

	after := f.pendingApproval(t, run.ID)
	if after.Status != "EXPIRED" {
		t.Fatalf("expected EXPIRED approval, got %q", after.Status)
	}
	snap := m2GetSnapshot(t, f.server.URL, run.ID, f.apiKey.PlaintextKey, f.orgID)
	if string(snap.Status) != "FAILED" {
		t.Fatalf("an expired approval must fail the run, got %s", snap.Status)
	}
	if snap.ReasonCode == nil || *snap.ReasonCode != "APPROVAL_EXPIRED" {
		t.Fatalf("expected reason APPROVAL_EXPIRED, got %v", snap.ReasonCode)
	}
}

// 8. The sweeper is progression, not authority: it settles approvals whose
// deadline passed while no decision was attempted.
func TestApproval_SweeperSettlesOverdueApprovals(t *testing.T) {
	f := setupApprovalGate(t)
	sess, _ := enrollExecutionWorker(t, f.tc, f.server, f.orgID, f.envID, "m4-w8")
	m3AssociateWorkerDeployment(t, f.tc, f.orgID, sess.SessionID, approvalBundleDigest)

	run := f.newRun(t, "m4-approval-sweeper-001")
	f.claimBuild(t, sess)
	approval := f.pendingApproval(t, run.ID)

	var affected int
	execRow(t, f, `UPDATE approvals SET expires_at = clock_timestamp() - INTERVAL '1 second' WHERE id=$1::uuid`, approval.ID, &affected)

	swept, err := f.engine.SweepExpiredApprovals(context.Background(), f.orgID)
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if swept < 1 {
		t.Fatalf("expected the overdue approval to be swept, swept=%d", swept)
	}
	after := f.pendingApproval(t, run.ID)
	if after.Status != "EXPIRED" {
		t.Fatalf("expected EXPIRED after sweep, got %q", after.Status)
	}
}

// 9. Cancelling a run cancels its pending approval, and a late decision on the
// terminal record is refused rather than reopening the run (§9 INV-09).
func TestApproval_CancelClosesPendingApprovalAndRefusesLateDecision(t *testing.T) {
	f := setupApprovalGate(t)
	sess, _ := enrollExecutionWorker(t, f.tc, f.server, f.orgID, f.envID, "m4-w9")
	m3AssociateWorkerDeployment(t, f.tc, f.orgID, sess.SessionID, approvalBundleDigest)

	run := f.newRun(t, "m4-approval-cancel-001")
	f.claimBuild(t, sess)
	approval := f.pendingApproval(t, run.ID)

	if _, err := f.engine.CancelRun(context.Background(), f.orgID, run.ID,
		execution.CancelRunRequest{ExpectedRevision: 1}, approvalHumanAudit(f.operator)); err != nil {
		t.Fatalf("cancel failed: %v", err)
	}

	after := f.pendingApproval(t, run.ID)
	if after.Status != "CANCELLED" {
		t.Fatalf("expected CANCELLED approval after run cancel, got %q", after.Status)
	}
	_, err := f.engine.DecideApproval(context.Background(), f.orgID, approval.ID,
		execution.DecideApprovalRequest{Decision: contracts.ApprovalDecisionApproved, ExpectedRevision: after.Revision},
		approvalHumanAudit(f.operator))
	if !errors.Is(err, execution.ErrRunTerminal) && !errors.Is(err, execution.ErrApprovalTerminal) {
		t.Fatalf("a late decision on a cancelled run must be refused, got %v", err)
	}
	snap := m2GetSnapshot(t, f.server.URL, run.ID, f.apiKey.PlaintextKey, f.orgID)
	if string(snap.Status) != "CANCELLED" {
		t.Fatalf("run must stay CANCELLED, got %s", snap.Status)
	}
}

// 10. Waiting is durable state, not a live process. A fresh engine reading the
// same database sees the same pending approval and can still decide it.
func TestApproval_SurvivesEngineRestart(t *testing.T) {
	f := setupApprovalGate(t)
	sess, _ := enrollExecutionWorker(t, f.tc, f.server, f.orgID, f.envID, "m4-w10")
	m3AssociateWorkerDeployment(t, f.tc, f.orgID, sess.SessionID, approvalBundleDigest)

	run := f.newRun(t, "m4-approval-restart-001")
	f.claimBuild(t, sess)
	approval := f.pendingApproval(t, run.ID)

	// Stand up a brand new engine against the same pool, as a restarted
	// control-plane process would.
	restarted := execution.NewWorkerEngine(f.tc.pool)
	restarted.SetCommands(f.tc.service)
	readBack, err := restarted.GetApproval(context.Background(), f.orgID, approval.ID)
	if err != nil {
		t.Fatalf("a restarted engine must still read the pending approval: %v", err)
	}
	if readBack.Status != "PENDING" || readBack.Revision != approval.Revision {
		t.Fatalf("restart changed the approval identity: %+v", readBack)
	}
	if readBack.ExpiresAt == nil {
		t.Fatal("the expiry must survive restart; otherwise waiting could live forever")
	}

	decided, err := restarted.DecideApproval(context.Background(), f.orgID, approval.ID,
		execution.DecideApprovalRequest{Decision: contracts.ApprovalDecisionApproved, ExpectedRevision: readBack.Revision},
		approvalHumanAudit(f.operator))
	if err != nil {
		t.Fatalf("a restarted engine must be able to decide: %v", err)
	}
	if decided.Status != "APPROVED" {
		t.Fatalf("expected APPROVED after restart-decide, got %q", decided.Status)
	}
}

// 11. F-21: a foreign organization cannot see or decide an approval.
func TestApproval_CrossTenantApprovalIsInvisible(t *testing.T) {
	f := setupApprovalGate(t)
	sess, _ := enrollExecutionWorker(t, f.tc, f.server, f.orgID, f.envID, "m4-w11")
	m3AssociateWorkerDeployment(t, f.tc, f.orgID, sess.SessionID, approvalBundleDigest)

	run := f.newRun(t, "m4-approval-cross-tenant-001")
	f.claimBuild(t, sess)
	approval := f.pendingApproval(t, run.ID)

	// A second organization with its own operator.
	ctx := context.Background()
	otherOwner, _ := tenant.NewUUID()
	otherOrg, err := f.tc.service.CreateOrganization(ctx, otherOwner, "Other Tenant")
	if err != nil {
		t.Fatalf("create other org: %v", err)
	}
	otherProject, err := f.tc.service.CreateProject(ctx, otherOrg.ID, "Other Project")
	if err != nil {
		t.Fatalf("create other project: %v", err)
	}
	if _, err := f.tc.service.CreateEnvironment(ctx, otherOrg.ID, otherProject.ID, tenant.EnvStaging, 5); err != nil {
		t.Fatalf("create other env: %v", err)
	}
	otherOperator, _ := tenant.NewUUID()
	if _, err := f.tc.service.AddMember(ctx, otherOrg.ID, otherOperator, tenant.RoleOwner); err != nil {
		t.Fatalf("add other operator: %v", err)
	}

	if _, err := f.engine.GetApproval(ctx, otherOrg.ID, approval.ID); !errors.Is(err, execution.ErrApprovalNotFound) {
		t.Fatalf("a foreign approval must be invisible, got %v", err)
	}
	if _, err := f.engine.DecideApproval(ctx, otherOrg.ID, approval.ID,
		execution.DecideApprovalRequest{Decision: contracts.ApprovalDecisionApproved, ExpectedRevision: approval.Revision},
		approvalHumanAudit(otherOperator)); !errors.Is(err, execution.ErrApprovalNotFound) {
		t.Fatalf("a foreign tenant must not decide another tenant's approval, got %v", err)
	}

	// The owner's approval is untouched by the attack.
	after := f.pendingApproval(t, run.ID)
	if after.Status != "PENDING" {
		t.Fatalf("cross-tenant attempt must not change the approval, got %q", after.Status)
	}
}

// countRow reads one integer under the fixture's tenant context so assertions
// observe committed authority rather than a value the test wrote earlier.
func countRow(t *testing.T, f *approvalGateFixture, query, arg string, out *int) {
	t.Helper()
	err := f.tc.pool.WithTenantTx(context.Background(), f.orgID, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, query, arg).Scan(out)
	})
	if err != nil {
		t.Fatalf("count query %q failed: %v", query, err)
	}
}

func execRow(t *testing.T, f *approvalGateFixture, query, arg string, out *int) {
	t.Helper()
	err := f.tc.pool.WithTenantTx(context.Background(), f.orgID, func(ctx context.Context, tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, query, arg)
		if err != nil {
			return err
		}
		*out = int(tag.RowsAffected())
		return nil
	})
	if err != nil {
		t.Fatalf("exec query %q failed: %v", query, err)
	}
}

func jsonBody(v any) *bytes.Reader {
	b, _ := json.Marshal(v)
	return bytes.NewReader(b)
}

// 12. A downstream node must be able to read the approval decision. The
// validator resolves a step.output reference to an approval through the node's
// own outputSchema, because an approval has no task definition to look up.
func TestApproval_DownstreamCanConsumeDecisionOutput(t *testing.T) {
	f := setupApprovalGate(t)
	sess, _ := enrollExecutionWorker(t, f.tc, f.server, f.orgID, f.envID, "m4-w12")
	m3AssociateWorkerDeployment(t, f.tc, f.orgID, sess.SessionID, approvalBundleDigest)

	run := f.newRun(t, "m4-approval-consume-001")
	f.claimBuild(t, sess)
	approval := f.pendingApproval(t, run.ID)
	if _, err := f.engine.DecideApproval(context.Background(), f.orgID, approval.ID,
		execution.DecideApprovalRequest{Decision: contracts.ApprovalDecisionRejected, ExpectedRevision: approval.Revision},
		approvalHumanAudit(f.operator)); err != nil {
		t.Fatalf("reject failed: %v", err)
	}

	// The publish node consumes gate's decision, proving the reference resolved
	// and the committed output matched the declared decision schema. Input is
	// resolved at claim time, so the assertion reads what the worker receives.
	asgns := parallelClaim(t, f.engine, sess, f.orgID, f.envID, 1, "m4-claim-consume")
	if len(asgns) != 1 {
		t.Fatalf("expected the downstream node to be claimable, got %d", len(asgns))
	}
	if node := m3NodeForStep(t, f.tc, f.orgID, asgns[0].StepID); node != "publish" {
		t.Fatalf("expected the downstream publish node, got %q", node)
	}
	resolved, ok := asgns[0].Input.(map[string]any)
	if !ok {
		t.Fatalf("assignment input must be an object, got %T", asgns[0].Input)
	}
	if resolved["decision"] != contracts.ApprovalDecisionRejected {
		t.Fatalf("downstream input must carry the committed decision %q, got %v",
			contracts.ApprovalDecisionRejected, resolved["decision"])
	}
	// The node only mapped /decision, so nothing else may leak into its input.
	if _, present := resolved["actorId"]; present {
		t.Fatalf("downstream input must carry only what the node mapped, got %v", resolved)
	}
}
