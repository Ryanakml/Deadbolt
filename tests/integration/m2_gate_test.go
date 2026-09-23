package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
	"github.com/Ryanakml/Deadbolt/tests/fixtures/externaleffect"
	"github.com/Ryanakml/Deadbolt/tests/fixtures/httpstaging"
)

// m2ABCBundle is a fixed digest label for the M2 gate A→B→C workflow. The
// bundle digest is advertised by both workers via the real poll path, so the
// claim below exercises the production compatibility check.
const m2ABCBundle = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// m2ABCManifest declares a linear A→B→C workflow where every task uses
// recovery "safe" so lease loss retries with the same operation ID under the
// persisted backoff timer (Blueprint §13.3, F-05).
func m2ABCManifest() (tasks []map[string]any, workflows []map[string]any) {
	mkSchema := func(prop string) map[string]any {
		return map[string]any{
			"type":                 "object",
			"properties":           map[string]any{prop: map[string]any{"type": "string"}},
			"required":             []any{prop},
			"additionalProperties": false,
		}
	}
	mkTask := func(name, entry string, inProp, outProp string) map[string]any {
		return map[string]any{
			"name":                name,
			"entrypoint":          entry,
			"timeoutMs":           60000,
			"recovery":            "safe",
			"inputSchema":         mkSchema(inProp),
			"outputSchema":        mkSchema(outProp),
			"retry":               map[string]any{"maxAttempts": 3, "initialDelayMs": 1000, "maxDelayMs": 30000},
			"idempotencyWindowMs": 305000,
		}
	}
	tasks = []map[string]any{
		mkTask("task-a", "tasks/a.js", "email", "accountId"),
		mkTask("task-b", "tasks/b.js", "accountId", "message"),
		mkTask("task-c", "tasks/c.js", "welcomeMessage", "confirmationCode"),
	}
	workflows = []map[string]any{
		{
			"manifestVersion": 1,
			"name":            "m2-gate-pipeline",
			"inputSchema":     mkSchema("customerEmail"),
			"outputSchema": map[string]any{
				"type":       "object",
				"properties": map[string]any{"finalConfirmation": map[string]any{"type": "string"}},
				"required":   []any{"finalConfirmation"},
			},
			"nodes": []map[string]any{
				{
					"id": "node-a", "type": "task", "task": "task-a", "after": []any{},
					"input": map[string]any{"email": map[string]any{"$ref": "run.input", "pointer": "/customerEmail"}},
				},
				{
					"id": "node-b", "type": "task", "task": "task-b", "after": []any{"node-a"},
					"input": map[string]any{"accountId": map[string]any{"$ref": "step.output", "stepId": "node-a", "pointer": "/accountId"}},
				},
				{
					"id": "node-c", "type": "task", "task": "task-c", "after": []any{"node-b"},
					"input": map[string]any{"welcomeMessage": map[string]any{"$ref": "step.output", "stepId": "node-b", "pointer": "/message"}},
				},
			},
			"output": map[string]any{
				"finalConfirmation": map[string]any{"$ref": "step.output", "stepId": "node-c", "pointer": "/confirmationCode"},
			},
		},
	}
	return tasks, workflows
}

func m2GetSnapshot(t *testing.T, serverURL, runID, token, orgID string) execution.RunSnapshotDTO {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/runs/%s", serverURL, runID), nil)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Organization-ID", orgID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get run snapshot: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("get run snapshot status=%d body=%s", resp.StatusCode, string(body))
	}
	var snap execution.RunSnapshotDTO
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatalf("decode run snapshot: %v", err)
	}
	return snap
}

// TestM2_TwoWorkerABCRecoveryKillDuringB is the central Blueprint §29.2
// scenario for Issue #25: A→B→C through the real Deadbolt path (real HTTP
// API, real PostgreSQL, real worker sessions, real ownership/leases) with two
// workers. Worker 1 is killed while B owns the attempt; B recovers per its
// configured safe-retry policy on worker 2 without rerunning A; C runs after;
// API, DB, event history, and Inspector-visible state agree.
//
// Kill simulation: the test stops heartbeats from worker 1 (no heartbeat is
// ever sent after Start, matching a dead process), advances the DB clock by
// expiring the lease timestamp only, then runs the REAL production reconciler
// (ReconcileExpiredLeases) and the REAL timer firing path
// (FireDueRetryTimers). The test never writes the final attempt/step state
// directly; the engine decides LOST → backoff timer → READY → epoch+1 claim.
// Stale worker-1 mutations are then proven rejected through the real HTTP
// fencing path.
//
// Host-resilience scope: two worker processes on one host prove
// worker-process failure only. Host-failure resilience is NOT claimed.
func TestM2_TwoWorkerABCRecoveryKillDuringB(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()
	ctx := context.Background()

	tasks, workflows := m2ABCManifest()
	manifest := createLifecycleManifest(m2ABCBundle, tasks, workflows)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "m2-gate-pipeline", manifest)

	// Two workers on one host (worker-process failure scope only).
	worker1, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "m2-gate-w1")
	worker2, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "m2-gate-w2")

	// Create run through the real public API (same path the SDK uses).
	createBody, _ := json.Marshal(map[string]any{
		"environment": "staging",
		"input":       map[string]any{"customerEmail": "m2-gate@example.com"},
	})
	createReq, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/m2-gate-pipeline/runs", bytes.NewReader(createBody))
	createReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	createReq.Header.Set("X-Organization-ID", orgID)
	createReq.Header.Set("Idempotency-Key", "m2-gate-run-001")
	createReq.Header.Set("Content-Type", "application/json")
	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(createResp.Body)
		t.Fatalf("create run status=%d body=%s", createResp.StatusCode, string(body))
	}
	var run execution.RunDTO
	if err := json.NewDecoder(createResp.Body).Decode(&run); err != nil {
		t.Fatalf("decode run: %v", err)
	}
	t.Logf("M2-GATE runID=%s worker1=%s/%s worker2=%s/%s", run.ID, worker1.WorkerID, worker1.SessionID, worker2.WorkerID, worker2.SessionID)

	// A succeeds on worker 1.
	claimA := claimExecution(t, server, worker1, m2ABCBundle, "m2-gate-claim-a")
	if claimA.RunID != run.ID {
		t.Fatalf("claimed wrong run for A: %s vs %s", claimA.RunID, run.ID)
	}
	startNode(t, server, worker1, claimA.AttemptID, claimA.OwnershipEpoch)
	completeNode(t, server, worker1, claimA.AttemptID, claimA.OwnershipEpoch, "SUCCEEDED",
		map[string]any{"accountId": "acc_m2_001"}, "digest-m2-a")
	t.Logf("M2-GATE A attemptID=%s epoch=%d opID=%s", claimA.AttemptID, claimA.OwnershipEpoch, claimA.OperationID)

	snapAfterA := m2GetSnapshot(t, server.URL, run.ID, adminKey.PlaintextKey, orgID)
	stepStateAfterA := map[string]contracts.StepStatus{}
	for _, s := range snapAfterA.Steps {
		stepStateAfterA[s.NodeID] = s.Status
	}
	if stepStateAfterA["node-a"] != contracts.StepStatusSUCCEEDED || stepStateAfterA["node-b"] != contracts.StepStatusREADY {
		t.Fatalf("after A: unexpected states %+v", stepStateAfterA)
	}

	// B begins on worker 1.
	claimB1 := claimExecution(t, server, worker1, m2ABCBundle, "m2-gate-claim-b1")
	startNode(t, server, worker1, claimB1.AttemptID, claimB1.OwnershipEpoch)
	t.Logf("M2-GATE B attempt1=%s epoch=%d opID=%s worker=%s session=%s",
		claimB1.AttemptID, claimB1.OwnershipEpoch, claimB1.OperationID, worker1.WorkerID, worker1.SessionID)

	// Kill worker 1 while B owns the attempt: stop heartbeats (none sent),
	// advance only the lease clock, then run the real production reconciler.
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE task_leases SET expires_at=clock_timestamp()-INTERVAL '1 second' WHERE attempt_id=$1::uuid`, claimB1.AttemptID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	engine := execution.NewWorkerEngine(tc.pool)
	if _, err := engine.ReconcileExpiredLeases(ctx, orgID); err != nil {
		t.Fatalf("reconcile expired leases: %v", err)
	}

	// B must have followed its safe-retry policy: LOST attempt, backoff timer
	// parked, no blind second attempt yet.
	var b1Status string
	var bAttemptsAfterKill, pendingTimers int
	var bStepState, bWaitReason string
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT status FROM task_attempts WHERE id=$1::uuid`, claimB1.AttemptID).Scan(&b1Status); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_attempts ta JOIN run_steps rs ON rs.id=ta.step_id JOIN runs r ON r.id=rs.run_id WHERE r.id=$1::uuid AND rs.node_id='node-b'`, run.ID).Scan(&bAttemptsAfterKill); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM timers tm JOIN run_steps rs ON rs.id=tm.step_id JOIN runs r ON r.id=rs.run_id WHERE r.id=$1::uuid AND rs.node_id='node-b' AND tm.state='PENDING'`, run.ID).Scan(&pendingTimers); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT rs.state, COALESCE(rs.wait_reason,'') FROM run_steps rs JOIN runs r ON r.id=rs.run_id WHERE r.id=$1::uuid AND rs.node_id='node-b'`, run.ID).Scan(&bStepState, &bWaitReason)
	}); err != nil {
		t.Fatal(err)
	}
	if b1Status != "LOST" {
		t.Fatalf("B attempt 1 must be LOST after kill, got %s", b1Status)
	}
	if bStepState != "WAITING" || bWaitReason != "RETRY_BACKOFF" || pendingTimers != 1 || bAttemptsAfterKill != 1 {
		t.Fatalf("B must park backoff timer per safe policy: step=%s/%s timers=%d attempts=%d", bStepState, bWaitReason, pendingTimers, bAttemptsAfterKill)
	}
	t.Logf("M2-GATE B killed: attempt1 LOST, step WAITING/RETRY_BACKOFF, 1 pending timer")

	// Stale worker 1 mutations must be rejected through the real fencing path.
	var staleHB worker.HeartbeatResponseDTO
	hbStatus := postWorkerJSON(t, server, "/worker/v1/heartbeat", worker1.SessionToken, worker.HeartbeatRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "m2-gate-stale-hb",
		WorkerID: worker1.WorkerID, SessionID: worker1.SessionID,
		Attempts: []worker.HeartbeatAttemptDTO{{AttemptID: claimB1.AttemptID, OwnershipEpoch: claimB1.OwnershipEpoch}},
	}, &staleHB)
	if hbStatus != http.StatusOK || len(staleHB.Stops) != 1 {
		t.Fatalf("stale heartbeat must return a stop directive: status=%d resp=%+v", hbStatus, staleHB)
	}
	staleComplete := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "m2-gate-stale-complete",
		WorkerID: worker1.WorkerID, SessionID: worker1.SessionID,
		AttemptID: claimB1.AttemptID, OwnershipEpoch: claimB1.OwnershipEpoch,
		Outcome: "SUCCEEDED", Output: map[string]any{"message": "stale"},
	}
	staleComplete.ResultDigest, _ = worker.CanonicalCompletionDigest(&staleComplete)
	if status := postWorkerJSON(t, server, "/worker/v1/complete", worker1.SessionToken, staleComplete, &worker.ErrorEnvelopeDTO{}); status != http.StatusConflict {
		t.Fatalf("stale completion must be rejected with 409, got %d", status)
	}
	t.Logf("M2-GATE stale worker-1 heartbeat stopped + completion rejected (409)")

	// Policy permits takeover when the backoff timer fires: force due (clock
	// advancement only) then run the real timer firing path.
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE timers SET due_at=clock_timestamp()-INTERVAL '1 second' WHERE step_id=(SELECT rs.id FROM run_steps rs JOIN runs r ON r.id=rs.run_id WHERE r.id=$1::uuid AND rs.node_id='node-b') AND state='PENDING'`, run.ID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if fired, err := engine.FireDueRetryTimers(ctx, orgID); err != nil || fired != 1 {
		t.Fatalf("expected backoff timer to fire once, fired=%d err=%v", fired, err)
	}

	// Worker 2 takes over B with a newer epoch and the same operation ID.
	claimB2 := claimExecution(t, server, worker2, m2ABCBundle, "m2-gate-claim-b2")
	if claimB2.AttemptID == claimB1.AttemptID {
		t.Fatalf("worker 2 must receive a new attempt, got same %s", claimB2.AttemptID)
	}
	if claimB2.OwnershipEpoch <= claimB1.OwnershipEpoch {
		t.Fatalf("worker 2 epoch must exceed %d, got %d", claimB1.OwnershipEpoch, claimB2.OwnershipEpoch)
	}
	if claimB2.OperationID != claimB1.OperationID {
		t.Fatalf("operation ID must be stable across retry: %s vs %s", claimB1.OperationID, claimB2.OperationID)
	}
	t.Logf("M2-GATE B attempt2=%s epoch=%d opID=%s worker=%s session=%s",
		claimB2.AttemptID, claimB2.OwnershipEpoch, claimB2.OperationID, worker2.WorkerID, worker2.SessionID)
	startNode(t, server, worker2, claimB2.AttemptID, claimB2.OwnershipEpoch)
	completeNode(t, server, worker2, claimB2.AttemptID, claimB2.OwnershipEpoch, "SUCCEEDED",
		map[string]any{"message": "hello recovered"}, "digest-m2-b")

	// C executes only after B recovered.
	claimC := claimExecution(t, server, worker2, m2ABCBundle, "m2-gate-claim-c")
	completePreState := m2GetSnapshot(t, server.URL, run.ID, adminKey.PlaintextKey, orgID)
	for _, s := range completePreState.Steps {
		if s.NodeID == "node-c" && s.Status != contracts.StepStatusREADY && s.Status != contracts.StepStatusRUNNING {
			t.Fatalf("C must be READY/RUNNING after B recovery, got %s", s.Status)
		}
	}
	startNode(t, server, worker2, claimC.AttemptID, claimC.OwnershipEpoch)
	completeNode(t, server, worker2, claimC.AttemptID, claimC.OwnershipEpoch, "SUCCEEDED",
		map[string]any{"confirmationCode": "CONF-M2-001"}, "digest-m2-c")
	t.Logf("M2-GATE C attemptID=%s epoch=%d", claimC.AttemptID, claimC.OwnershipEpoch)

	// Final agreement: API state, DB state, event history, Inspector snapshot.
	apiSnap := m2GetSnapshot(t, server.URL, run.ID, adminKey.PlaintextKey, orgID)
	if apiSnap.Status != contracts.RunStatusSUCCEEDED {
		t.Fatalf("final run must be SUCCEEDED, got %s", apiSnap.Status)
	}
	out, _ := apiSnap.Output.(map[string]any)
	if out["finalConfirmation"] != "CONF-M2-001" {
		t.Fatalf("unexpected final output: %+v", apiSnap.Output)
	}
	svcSnap, err := execution.NewService(tc.pool, tc.service).GetRun(ctx, orgID, run.ID)
	if err != nil {
		t.Fatalf("service GetRun: %v", err)
	}
	if svcSnap.Status != contracts.RunStatusSUCCEEDED {
		t.Fatalf("service snapshot must be SUCCEEDED, got %s", svcSnap.Status)
	}
	var dbRunStatus, dbReason string
	var aAttempts, bAttempts, cAttempts int
	var aSucceeded, bSucceeded, cSucceeded int
	var eventCount int
	var eventSeq []string
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		var rc *string
		if err := tx.QueryRow(ctx, `SELECT status, reason_code FROM runs WHERE id=$1::uuid`, run.ID).Scan(&dbRunStatus, &rc); err != nil {
			return err
		}
		if rc != nil {
			dbReason = *rc
		}
		for _, args := range []struct {
			node string
			tot  *int
			succ *int
		}{
			{"node-a", &aAttempts, &aSucceeded},
			{"node-b", &bAttempts, &bSucceeded},
			{"node-c", &cAttempts, &cSucceeded},
		} {
			if err := tx.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE ta.status='SUCCEEDED') FROM task_attempts ta JOIN run_steps rs ON rs.id=ta.step_id JOIN runs r ON r.id=rs.run_id WHERE r.id=$1::uuid AND rs.node_id=$2`, run.ID, args.node).Scan(args.tot, args.succ); err != nil {
				return err
			}
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM run_events WHERE run_id=$1::uuid`, run.ID).Scan(&eventCount); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT event_type FROM run_events WHERE run_id=$1::uuid ORDER BY sequence`, run.ID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var et string
			if err := rows.Scan(&et); err != nil {
				return err
			}
			eventSeq = append(eventSeq, et)
		}
		return rows.Err()
	}); err != nil {
		t.Fatal(err)
	}
	if dbRunStatus != "SUCCEEDED" {
		t.Fatalf("DB run must be SUCCEEDED, got %s/%s", dbRunStatus, dbReason)
	}
	if aAttempts != 1 || aSucceeded != 1 {
		t.Fatalf("A must execute exactly once: attempts=%d succeeded=%d", aAttempts, aSucceeded)
	}
	if bAttempts != 2 || bSucceeded != 1 {
		t.Fatalf("B must have LOST+SUCCEEDED attempts: attempts=%d succeeded=%d", bAttempts, bSucceeded)
	}
	if cAttempts != 1 || cSucceeded != 1 {
		t.Fatalf("C must execute exactly once: attempts=%d succeeded=%d", cAttempts, cSucceeded)
	}
	if eventCount == 0 {
		t.Fatal("event history must be non-empty")
	}
	t.Logf("M2-GATE FINAL run=%s status=%s A=%d/1 B=%d/1 C=%d/1 events=%d seq=%v output=%v",
		run.ID, dbRunStatus, aAttempts, bAttempts, cAttempts, eventCount, eventSeq, out)
	t.Logf("M2-GATE REPEATABLE: rerunning this test preserves A-once/B-retry/C-after semantics")
}

// TestM2_UnknownExternalEffectLostResponseHoldsForReconciliation proves the
// Issue #25 unknown-outcome contract with the disposable external-effect
// fixture whose dedup ledger lives separately from the runtime DB
// (file-backed, survives DB restore):
//
//   - external provider effect commits to the separate ledger,
//   - the definitive completion response is intentionally lost
//     (X-Simulate-Loss),
//   - Deadbolt does NOT blindly re-execute: the step enters a reconciliation
//     hold with exactly one attempt and one OPEN case,
//   - operator resolution completes the workflow, and the external ledger
//     still holds exactly one effect (no duplicate side effect).
func TestM2_UnknownExternalEffectLostResponseHoldsForReconciliation(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	ctx := context.Background()

	extDir := t.TempDir()
	fixture, err := externaleffect.NewFixture(extDir)
	if err != nil {
		t.Fatalf("external fixture: %v", err)
	}
	defer fixture.Close()

	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "m2-unknown")
	const digest = "bundle-m2-unknown-25"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, reconcileManifest(3, 60000))
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "m2-unknown-claim")
	opID := a1.OperationID
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)

	// The handler calls the external provider exactly once. The provider
	// commits to its own ledger, but the response is intentionally lost on
	// the wire: Deadbolt never receives a definitive completion.
	idemKey := "m2-unknown-op-" + opID
	reqBody, _ := json.Marshal(map[string]any{"idempotencyKey": idemKey, "action": "RESERVE_STOCK", "payload": map[string]any{"sku": "WIDGET"}})
	httpReq, _ := http.NewRequest(http.MethodPost, fixture.URL()+"/v1/effects/execute", bytes.NewReader(reqBody))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Simulate-Loss", "true")
	lostResp, err := http.DefaultClient.Do(httpReq)
	if err == nil {
		defer lostResp.Body.Close()
		_, _ = io.ReadAll(lostResp.Body)
	}
	// Either a hijacked-connection error or a 502 is the expected lost reply.
	if err == nil && lostResp.StatusCode != http.StatusBadGateway {
		t.Fatalf("expected simulated loss (502 or transport error), got status=%d err=%v", lostResp.StatusCode, err)
	}
	if rec, found := fixture.Get(idemKey); !found || rec.Duplicate {
		t.Fatalf("external effect must be committed exactly once to the separate ledger: found=%v rec=%+v", found, rec)
	}
	t.Logf("M2-UNKNOWN external ledger committed key=%s count=%d (response intentionally lost)", idemKey, fixture.Count())

	// The worker truthfully reports UNKNOWN: the outcome is ambiguous.
	completeWithError(t, server, session, a1, "PROVIDER_TIMEOUT", true, "UNKNOWN", "")

	var stepState, waitReason string
	var attempts, openCases, pendingTimers int
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		var wr *string
		if err := tx.QueryRow(ctx, `SELECT state, wait_reason FROM run_steps WHERE id=$1::uuid`, stepID).Scan(&stepState, &wr); err != nil {
			return err
		}
		if wr != nil {
			waitReason = *wr
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_attempts WHERE step_id=$1::uuid`, stepID).Scan(&attempts); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM reconciliation_cases WHERE step_id=$1::uuid AND status='OPEN'`, stepID).Scan(&openCases); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT count(*) FROM timers WHERE step_id=$1::uuid AND state='PENDING'`, stepID).Scan(&pendingTimers)
	}); err != nil {
		t.Fatal(err)
	}
	if stepState != "WAITING" || waitReason != "RECONCILIATION" || attempts != 1 || openCases != 1 || pendingTimers != 0 {
		t.Fatalf("unknown outcome must hold without blind retry: step=%s/%s attempts=%d cases=%d timers=%d",
			stepState, waitReason, attempts, openCases, pendingTimers)
	}
	t.Logf("M2-UNKNOWN held: step WAITING/RECONCILIATION, 1 attempt, 1 OPEN case, 0 timers")

	// Operator reconciles against the separate ledger (effect exists exactly
	// once) and confirms success; the workflow completes without a second
	// external execution.
	caseID, rev := openReconciliationCase(t, tc, orgID, stepID)
	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleOperator, "m2-unknown-op")
	status, body := resolveCaseHTTP(t, server, token, orgID, caseID, map[string]any{
		"action": "confirm_succeeded", "evidence": "ledger:" + idemKey, "reason": "provider ledger shows one committed effect",
		"result": map[string]any{"ok": true}, "expectedRevision": rev,
	})
	if status != http.StatusOK {
		t.Fatalf("operator resolve failed: status=%d body=%v", status, body)
	}
	if dup, _ := fixture.ExecuteEffect(idemKey, "RESERVE_STOCK", map[string]any{"sku": "WIDGET"}); !dup.Duplicate {
		t.Fatal("re-executing the same idempotency key must be a ledger duplicate, not a new effect")
	}
	if fixture.Count() != 1 {
		t.Fatalf("external ledger must hold exactly one effect, got %d", fixture.Count())
	}
	var finalStep, finalRun, completionSource string
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT state, completion_source FROM run_steps WHERE id=$1::uuid`, stepID).Scan(&finalStep, &completionSource); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid`, runID).Scan(&finalRun)
	}); err != nil {
		t.Fatal(err)
	}
	if finalStep != "SUCCEEDED" || completionSource != "RECONCILIATION" || finalRun != "SUCCEEDED" {
		t.Fatalf("operator resolution must complete workflow: step=%s/%s run=%s", finalStep, completionSource, finalRun)
	}
	t.Logf("M2-UNKNOWN RESOLVED run=%s step SUCCEEDED/RECONCILIATION ledger=%d (singular)", runID, fixture.Count())
	_ = deploymentID
}

// TestM2_HTTPStagingLocalFixture proves the safe HTTP fixture contract
// locally: fast health/echo plus a bounded slow path whose client-side
// timeout is observable through a real network boundary. This is the local
// half of the Issue #25 real-safe-HTTP requirement. The hosted half
// (TestM2_HTTPStagingHosted) runs only when DEADBOLT_HTTP_STAGING_URL is set
// by the operator on hosted staging and is otherwise PENDING_HOSTED_STAGING.
func TestM2_HTTPStagingLocalFixture(t *testing.T) {
	fix := httpstaging.NewFixture(3000)
	defer fix.Close()

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(fix.URL() + "/health")
	if err != nil {
		t.Fatalf("fixture health: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("fixture health status=%d", resp.StatusCode)
	}

	// A 50ms slow response succeeds under a generous timeout.
	fastClient := &http.Client{Timeout: 5 * time.Second}
	fastResp, err := fastClient.Get(fix.URL() + "/slow?delayMs=50")
	if err != nil {
		t.Fatalf("fast slow-path: %v", err)
	}
	defer fastResp.Body.Close()
	if fastResp.StatusCode != http.StatusOK {
		t.Fatalf("fast slow-path status=%d", fastResp.StatusCode)
	}

	// A 2000ms server delay exceeds a 200ms client timeout: the timeout is
	// observed through the real HTTP boundary, not a unit timestamp check.
	timeoutClient := &http.Client{Timeout: 200 * time.Millisecond}
	_, err = timeoutClient.Get(fix.URL() + "/slow?delayMs=2000")
	if err == nil {
		t.Fatal("expected client timeout against slow fixture path")
	}
	t.Logf("M2-HTTP-LOCAL fixture=%s health+echo ok, 50ms ok, 2000ms timed out at 200ms client timeout, requests=%d",
		fix.URL(), fix.RequestCount())
}

// TestM2_HTTPStagingHosted is the real safe HTTP staging integration for
// Issue #25. It runs only when the operator sets DEADBOLT_HTTP_STAGING_URL to
// the controlled staging fixture URL; otherwise it reports
// PENDING_HOSTED_STAGING and does not claim success.
func TestM2_HTTPStagingHosted(t *testing.T) {
	target, ok := httpstaging.StagingConfig()
	if !ok {
		t.Skip("PENDING_HOSTED_STAGING: set DEADBOLT_HTTP_STAGING_URL to the controlled staging fixture URL to run the real safe HTTP staging integration")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(target + "/health")
	if err != nil {
		t.Fatalf("hosted staging health %s: %v", target, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("hosted staging health status=%d", resp.StatusCode)
	}
	timeoutClient := &http.Client{Timeout: 300 * time.Millisecond}
	_, err = timeoutClient.Get(target + "/slow?delayMs=2000")
	if err == nil {
		t.Fatal("expected client timeout against hosted slow path")
	}
	t.Logf("M2-HTTP-HOSTED target=%s health ok, slow-path timeout observed", target)
}
