package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

var cancelKeySeq int64

func cancelRunHTTP(t *testing.T, server *httptest.Server, token, orgID, runID string, revision int64, key string) (int, map[string]any) {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"expectedRevision": revision})
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/runs/"+runID+"/cancel", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Organization-ID", orgID)
	req.Header.Set("Content-Type", "application/json")
	if key == "" {
		key = fmt.Sprintf("cancel-%s-%d", runID, atomic.AddInt64(&cancelKeySeq, 1))
	}
	req.Header.Set("Idempotency-Key", key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("cancel request failed: %v", err)
	}
	defer resp.Body.Close()
	var parsed map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&parsed)
	return resp.StatusCode, parsed
}

func runRevision(t *testing.T, tc *tenantTestContext, orgID, runID string) int64 {
	t.Helper()
	var rev int64
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT revision FROM runs WHERE id=$1::uuid`, runID).Scan(&rev)
	}); err != nil {
		t.Fatal(err)
	}
	return rev
}

// TestCancelHappyPathStopAckAndSettle proves the durable cancellation core:
// leases revoked, stop command recorded with a 10s grace, worker heartbeat
// delivers the stop, and the ACK settles the run as confirmed CANCELLED.
func TestCancelHappyPathStopAckAndSettle(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "cancel-happy")
	const digest = "bundle-cancel-happy-20"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "cancel-happy-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)
	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "cancel-happy")

	status, body := cancelRunHTTP(t, server, token, orgID, runID, runRevision(t, tc, orgID, runID), "")
	if status != http.StatusOK {
		t.Fatalf("cancel expected 200, got %d (%v)", status, body)
	}
	if body["status"] != "CANCELLING" {
		t.Fatalf("expected CANCELLING, got %v", body["status"])
	}

	var attemptStatus, stepState string
	var leases, stops int
	var stopReason string
	var stopDeadline time.Time
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT status FROM task_attempts WHERE id=$1::uuid`, a1.AttemptID).Scan(&attemptStatus); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT state FROM run_steps WHERE id=$1::uuid`, stepID).Scan(&stepState); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_leases WHERE attempt_id=$1::uuid`, a1.AttemptID).Scan(&leases); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*), MAX(reason) FROM stop_commands WHERE attempt_id=$1::uuid AND acked_at IS NULL`, a1.AttemptID).Scan(&stops, &stopReason); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT deadline_at FROM stop_commands WHERE attempt_id=$1::uuid`, a1.AttemptID).Scan(&stopDeadline)
	}); err != nil {
		t.Fatal(err)
	}
	if attemptStatus != "CANCELLED" || stepState != "CANCELLED" || leases != 0 || stops != 1 || stopReason != "CANCEL_REQUESTED" {
		t.Fatalf("cancel commit wrong: attempt=%s step=%s leases=%d stops=%d reason=%s",
			attemptStatus, stepState, leases, stops, stopReason)
	}
	if dt := time.Until(stopDeadline); dt < 9*time.Second || dt > 11*time.Second {
		t.Fatalf("stop grace must be ~10s, got %s", dt)
	}

	// The worker heartbeat delivers the stop for the cancelled attempt.
	var hb worker.HeartbeatResponseDTO
	postWorkerJSON(t, server, "/worker/v1/heartbeat", session.SessionToken, worker.HeartbeatRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "cancel-hb",
		WorkerID: session.WorkerID, SessionID: session.SessionID,
		Attempts: []worker.HeartbeatAttemptDTO{{AttemptID: a1.AttemptID, OwnershipEpoch: a1.OwnershipEpoch}},
	}, &hb)
	if len(hb.Stops) == 0 {
		t.Fatalf("heartbeat must deliver the stop command: %+v", hb)
	}

	// The stop ACK settles the run as confirmed.
	var ack worker.AckResponseDTO
	postWorkerJSON(t, server, "/worker/v1/stop-ack", session.SessionToken, worker.StopAckRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "cancel-ack",
		WorkerID: session.WorkerID, SessionID: session.SessionID,
		AttemptID: a1.AttemptID, OwnershipEpoch: a1.OwnershipEpoch, ProcessStopped: true,
	}, &ack)
	var runStatus string
	var confirmed *bool
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, termination_confirmed FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus, &confirmed)
	}); err != nil {
		t.Fatal(err)
	}
	if runStatus != "CANCELLED" || confirmed == nil || !*confirmed {
		t.Fatalf("expected confirmed CANCELLED, got %s/%v", runStatus, confirmed)
	}
}

// TestCancelGraceExpirySettlesUnconfirmed proves restart-safe grace
// settlement: with no ACK and a lapsed grace, a fresh engine settles the run
// as CANCELLED with termination_confirmed=false.
func TestCancelGraceExpirySettlesUnconfirmed(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "cancel-grace")
	const digest = "bundle-cancel-grace-20"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "cancel-grace-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)
	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleOperator, "cancel-grace")
	if status, _ := cancelRunHTTP(t, server, token, orgID, runID, runRevision(t, tc, orgID, runID), ""); status != http.StatusOK {
		t.Fatalf("cancel expected 200, got %d", status)
	}
	// No ACK arrives; force the grace into the past, then settle with a
	// fresh engine instance as after a control-plane restart.
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE stop_commands SET deadline_at=clock_timestamp()-INTERVAL '1 second' WHERE attempt_id=$1::uuid`, a1.AttemptID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	engine := execution.NewWorkerEngine(tc.pool)
	if _, err := engine.ReconcileExpiredLeases(context.Background(), orgID); err != nil {
		t.Fatal(err)
	}
	var runStatus string
	var confirmed *bool
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, termination_confirmed FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus, &confirmed)
	}); err != nil {
		t.Fatal(err)
	}
	if runStatus != "CANCELLED" || confirmed == nil || *confirmed {
		t.Fatalf("expected unconfirmed CANCELLED, got %s/%v", runStatus, confirmed)
	}
}

// TestCancelCompletionFirstPreserved proves a result committed before cancel
// stands, and cancelling a terminal run returns 409 RUN_TERMINAL.
func TestCancelCompletionFirstPreserved(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "cancel-first-win")
	const digest = "bundle-cancel-first-win-20"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "cancel-first-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)
	completion := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "cancel-first-complete",
		WorkerID: session.WorkerID, SessionID: session.SessionID,
		AttemptID: a1.AttemptID, OwnershipEpoch: a1.OwnershipEpoch,
		Outcome: "SUCCEEDED", Output: map[string]any{"ok": true},
	}
	completion.ResultDigest, _ = worker.CanonicalCompletionDigest(&completion)
	var compResp worker.CompleteResponseDTO
	if status := postWorkerJSON(t, server, "/worker/v1/complete", session.SessionToken, completion, &compResp); status != http.StatusOK {
		t.Fatalf("complete expected 200, got %d", status)
	}
	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "cancel-first-dev")
	status, body := cancelRunHTTP(t, server, token, orgID, runID, runRevision(t, tc, orgID, runID), "")
	if status != http.StatusConflict || body["code"] != "RUN_TERMINAL" {
		t.Fatalf("cancel on terminal run must be 409 RUN_TERMINAL, got %d (%v)", status, body)
	}
	var runStatus string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus)
	}); err != nil {
		t.Fatal(err)
	}
	if runStatus != "SUCCEEDED" {
		t.Fatalf("committed success must stand, got %s", runStatus)
	}
}

// TestCancelFirstRejectsLateResult proves results arriving after the cancel
// commit are rejected, identical or not.
func TestCancelFirstRejectsLateResult(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "cancel-first-reject")
	const digest = "bundle-cancel-first-reject-20"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "cancel-reject-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)
	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "cancel-reject")
	if status, _ := cancelRunHTTP(t, server, token, orgID, runID, runRevision(t, tc, orgID, runID), ""); status != http.StatusOK {
		t.Fatalf("cancel expected 200, got %d", status)
	}
	completion := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "cancel-reject-complete",
		WorkerID: session.WorkerID, SessionID: session.SessionID,
		AttemptID: a1.AttemptID, OwnershipEpoch: a1.OwnershipEpoch,
		Outcome: "SUCCEEDED", Output: map[string]any{"ok": true},
	}
	completion.ResultDigest, _ = worker.CanonicalCompletionDigest(&completion)
	var compResp worker.CompleteResponseDTO
	if status := postWorkerJSON(t, server, "/worker/v1/complete", session.SessionToken, completion, &compResp); status != http.StatusConflict {
		t.Fatalf("post-cancel result must be rejected, got %d", status)
	}
	// Identical retry is rejected too: cancel revoked the identity.
	if status := postWorkerJSON(t, server, "/worker/v1/complete", session.SessionToken, completion, &compResp); status != http.StatusConflict {
		t.Fatalf("identical post-cancel result must be rejected, got %d", status)
	}
}

// TestCancelDuplicateAndRevision proves idempotent duplicate cancels while
// CANCELLING and 409 on stale revisions.
func TestCancelDuplicateAndRevision(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "cancel-dupe")
	const digest = "bundle-cancel-dupe-20"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "cancel-dupe-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)
	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "cancel-dupe")
	rev := runRevision(t, tc, orgID, runID)

	if status, body := cancelRunHTTP(t, server, token, orgID, runID, rev+99, ""); status != http.StatusConflict || body["code"] != "REVISION_CONFLICT" {
		t.Fatalf("stale revision must be 409, got %d (%v)", status, body)
	}
	if status, _ := cancelRunHTTP(t, server, token, orgID, runID, rev, "dupe-key-1"); status != http.StatusOK {
		t.Fatalf("first cancel expected 200, got %d", status)
	}
	// Same command identity replays the recorded outcome.
	if status, body := cancelRunHTTP(t, server, token, orgID, runID, rev, "dupe-key-1"); status != http.StatusOK || body["status"] != "CANCELLING" {
		t.Fatalf("duplicate cancel must replay 200 CANCELLING, got %d (%v)", status, body)
	}
	// New key while settling is idempotent success, not a conflict.
	if status, body := cancelRunHTTP(t, server, token, orgID, runID, rev+1, "dupe-key-2"); status != http.StatusOK || body["status"] != "CANCELLING" {
		t.Fatalf("cancel while CANCELLING must stay 200, got %d (%v)", status, body)
	}
}

// TestCancelPermissions proves runs:control gating: viewers denied,
// developers and control-scoped machine keys admitted.
func TestCancelPermissions(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	const digest = "bundle-cancel-perms-20"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))

	newRun := func(node string) string {
		runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, node)
		return runID
	}
	viewerToken, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleViewer, "cancel-viewer")
	devToken, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "cancel-perms-dev")
	machineKey := bootstrapTestKey(t, tc.service, orgID, envID, []string{tenant.CapRunsControl})

	if status, body := cancelRunHTTP(t, server, viewerToken, orgID, newRun("node-a"), 1, ""); status != http.StatusForbidden {
		t.Fatalf("viewer cancel must be 403, got %d (%v)", status, body)
	}
	if status, _ := cancelRunHTTP(t, server, devToken, orgID, newRun("node-a"), 1, ""); status != http.StatusOK {
		t.Fatalf("developer cancel must be 200, got %d", status)
	}
	if status, _ := cancelRunHTTP(t, server, machineKey.PlaintextKey, orgID, newRun("node-a"), 1, ""); status != http.StatusOK {
		t.Fatalf("control-scoped machine cancel must be 200, got %d", status)
	}
}

// TestCancelPreservesSucceededSteps proves fail-fast-style cancellation
// keeps committed success while stopping the rest, settling confirmed.
func TestCancelPreservesSucceededSteps(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "cancel-preserve")
	const digest = "bundle-cancel-preserve-20"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, siblingManifest())
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "a")
	var stepB string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `INSERT INTO run_steps
			(organization_id,environment_id,run_id,node_id,state,eligible_at)
			VALUES ($1,$2,$3,'b','READY',clock_timestamp()) RETURNING id::text`, orgID, envID, runID).Scan(&stepB)
	}); err != nil {
		t.Fatal(err)
	}
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	first := claimExecution(t, server, session, digest, "preserve-claim-1")
	second := claimExecution(t, server, session, digest, "preserve-claim-2")
	claims := map[string]worker.AssignmentDTO{
		attemptNode(t, tc, orgID, first.AttemptID):  first,
		attemptNode(t, tc, orgID, second.AttemptID): second,
	}
	claimA, claimB := claims["a"], claims["b"]
	startNode(t, server, session, claimA.AttemptID, claimA.OwnershipEpoch)
	startNode(t, server, session, claimB.AttemptID, claimB.OwnershipEpoch)
	succeed := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "preserve-succeed-a",
		WorkerID: session.WorkerID, SessionID: session.SessionID,
		AttemptID: claimA.AttemptID, OwnershipEpoch: claimA.OwnershipEpoch,
		Outcome: "SUCCEEDED", Output: map[string]any{"ok": true},
	}
	succeed.ResultDigest, _ = worker.CanonicalCompletionDigest(&succeed)
	var succeedResp worker.CompleteResponseDTO
	if status := postWorkerJSON(t, server, "/worker/v1/complete", session.SessionToken, succeed, &succeedResp); status != http.StatusOK {
		t.Fatalf("sibling success expected 200, got %d", status)
	}

	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleOperator, "cancel-preserve")
	if status, _ := cancelRunHTTP(t, server, token, orgID, runID, runRevision(t, tc, orgID, runID), ""); status != http.StatusOK {
		t.Fatalf("cancel expected 200, got %d", status)
	}
	var stateA, stateB, runStatus string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT state FROM run_steps WHERE node_id='a' AND run_id=$1::uuid`, runID).Scan(&stateA); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT state FROM run_steps WHERE id=$1::uuid`, stepB).Scan(&stateB); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus)
	}); err != nil {
		t.Fatal(err)
	}
	if stateA != "SUCCEEDED" || stateB != "CANCELLED" || runStatus != "CANCELLING" {
		t.Fatalf("cancel must preserve success: a=%s b=%s run=%s", stateA, stateB, runStatus)
	}
	// ACK the stop and settle confirmed.
	var ack worker.AckResponseDTO
	postWorkerJSON(t, server, "/worker/v1/stop-ack", session.SessionToken, worker.StopAckRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "preserve-ack",
		WorkerID: session.WorkerID, SessionID: session.SessionID,
		AttemptID: claimB.AttemptID, OwnershipEpoch: claimB.OwnershipEpoch, ProcessStopped: true,
	}, &ack)
	var confirmed *bool
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, termination_confirmed FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus, &confirmed)
	}); err != nil {
		t.Fatal(err)
	}
	if runStatus != "CANCELLED" || confirmed == nil || !*confirmed {
		t.Fatalf("expected confirmed CANCELLED, got %s/%v", runStatus, confirmed)
	}
}

// TestCancelWithoutLiveWorkSettlesImmediately proves cancel on a run with no
// active attempts settles straight to confirmed CANCELLED with no stops.
func TestCancelWithoutLiveWorkSettlesImmediately(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	const digest = "bundle-cancel-idle-20"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "cancel-idle")

	status, body := cancelRunHTTP(t, server, token, orgID, runID, runRevision(t, tc, orgID, runID), "")
	if status != http.StatusOK || body["status"] != "CANCELLED" {
		t.Fatalf("idle cancel must settle immediately, got %d (%v)", status, body)
	}
	if body["terminationConfirmed"] != true {
		t.Fatalf("idle cancel is trivially confirmed, got %v", body)
	}
	var stops int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM stop_commands sc
			JOIN task_attempts a ON a.id=sc.attempt_id
			JOIN run_steps rs ON rs.id=a.step_id WHERE rs.run_id=$1::uuid`, runID).Scan(&stops)
	}); err != nil {
		t.Fatal(err)
	}
	if stops != 0 {
		t.Fatalf("no live work means no stop commands, got %d", stops)
	}
}

// TestCancelKillsPendingRetryTimer proves cancel revokes scheduled retries:
// the orphaned timer is marked CANCELLED and never fires.
func TestCancelKillsPendingRetryTimer(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "cancel-timer")
	const digest = "bundle-cancel-timer-20"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 10000, 30000))
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "cancel-timer-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)
	completeWithError(t, server, session, a1, "PROVIDER_500", true, "NOT_APPLIED", "")
	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "cancel-timer")
	if status, _ := cancelRunHTTP(t, server, token, orgID, runID, runRevision(t, tc, orgID, runID), ""); status != http.StatusOK {
		t.Fatalf("cancel expected 200, got %d", status)
	}
	var timerState string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT state FROM timers WHERE step_id=$1::uuid`, stepID).Scan(&timerState)
	}); err != nil {
		t.Fatal(err)
	}
	if timerState != "CANCELLED" {
		t.Fatalf("pending timer must die with the run, got %s", timerState)
	}
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE timers SET due_at=clock_timestamp()-INTERVAL '1 second' WHERE step_id=$1::uuid`, stepID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	engine := execution.NewWorkerEngine(tc.pool)
	fired, err := engine.FireDueRetryTimers(context.Background(), orgID)
	if err != nil || fired != 0 {
		t.Fatalf("cancelled timer must never fire, got %d err %v", fired, err)
	}
}

// TestCancelClosesHoldsAsCancel proves cancelling a held run closes its
// OPEN cases with CANCEL resolution under the deciding actor.
func TestCancelClosesHoldsAsCancel(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "cancel-hold")
	const digest = "bundle-cancel-hold-20"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, reconcileManifest(3, 60000))
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "cancel-hold-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)
	completeWithError(t, server, session, a1, "PROVIDER_500", true, "UNKNOWN", "")
	caseID, _ := openReconciliationCase(t, tc, orgID, stepID)
	token, userID := reconcileHumanToken(t, tc, orgID, tenant.RoleOperator, "cancel-hold")
	if status, _ := cancelRunHTTP(t, server, token, orgID, runID, runRevision(t, tc, orgID, runID), ""); status != http.StatusOK {
		t.Fatalf("cancel expected 200, got %d", status)
	}
	var caseStatus, resolution, actorID string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, resolution, actor_id::text FROM reconciliation_cases WHERE id=$1::uuid`, caseID).Scan(&caseStatus, &resolution, &actorID)
	}); err != nil {
		t.Fatal(err)
	}
	if caseStatus != "RESOLVED" || resolution != "CANCEL" || actorID != userID {
		t.Fatalf("hold must close as CANCEL by the actor, got %s/%s/%s", caseStatus, resolution, actorID)
	}
}

// TestOverdueRunSweeperFailsRun proves the run deadline stays active without
// live work: a lapsed QUEUED run terminalizes on the next sweep.
func TestOverdueRunSweeperFailsRun(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	const digest = "bundle-overdue-20"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE runs SET deadline_at=clock_timestamp()-INTERVAL '1 second' WHERE id=$1::uuid`, runID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	engine := execution.NewWorkerEngine(tc.pool)
	if _, err := engine.ReconcileExpiredLeases(context.Background(), orgID); err != nil {
		t.Fatal(err)
	}
	var runStatus string
	var reasonCode *string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, reason_code FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus, &reasonCode)
	}); err != nil {
		t.Fatal(err)
	}
	if runStatus != "FAILED" || reasonCode == nil || *reasonCode != "RUN_DEADLINE_EXCEEDED" {
		t.Fatalf("overdue run must fail closed, got %s/%v", runStatus, reasonCode)
	}
}

// TestClaimBlockedPastDeadline proves DB-time deadline checks apply before
// any sweep: a READY step on a lapsed run admits no claim.
func TestClaimBlockedPastDeadline(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "deadline-claim")
	const digest = "bundle-deadline-claim-20"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE runs SET deadline_at=clock_timestamp()-INTERVAL '1 second' WHERE id=$1::uuid`, runID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var polled worker.PollResponseDTO
	if status := postWorkerJSON(t, server, "/worker/v1/poll", session.SessionToken, worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "deadline-poll", WorkerID: session.WorkerID,
		SessionID: session.SessionID, AvailableSlots: 1, DeploymentDigests: []string{digest}, Pool: "default",
	}, &polled); status != http.StatusOK || len(polled.Assignments) != 0 {
		t.Fatalf("lapsed run must admit no claim, got status %d n=%d", status, len(polled.Assignments))
	}
}

// TestAttemptTimeoutFollowsPolicy proves TIMED_OUT attempts route through
// the same policy as failures: safe retries with a timer, reconcile holds.
func TestAttemptTimeoutFollowsPolicy(t *testing.T) {
	for _, tc2 := range []struct {
		name     string
		manifest func() string
		holds    bool
	}{
		{"safe-retries", func() string { return safeManifest(3, 10, 100) }, false},
		{"reconcile-holds", func() string { return reconcileManifest(3, 60000) }, true},
	} {
		t.Run(tc2.name, func(t *testing.T) {
			tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
			defer tc.cleanup()
			defer server.Close()
			session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "timeout-policy")
			const digest = "bundle-timeout-policy-20"
			deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, tc2.manifest())
			runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
			advertiseDigest(t, tc, orgID, session.SessionID, digest)

			a1 := claimExecution(t, server, session, digest, "timeout-claim")
			startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)
			if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
				_, err := tx.Exec(ctx, `UPDATE task_attempts SET deadline_at=clock_timestamp()-INTERVAL '1 second' WHERE id=$1::uuid`, a1.AttemptID)
				if err != nil {
					return err
				}
				_, err = tx.Exec(ctx, `UPDATE task_leases SET expires_at=clock_timestamp()-INTERVAL '1 second' WHERE attempt_id=$1::uuid`, a1.AttemptID)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			engine := execution.NewWorkerEngine(tc.pool)
			if _, err := engine.ReconcileExpiredLeases(context.Background(), orgID); err != nil {
				t.Fatal(err)
			}
			stepState, waitReason, _, _, timerState, _, _, _, _, _ := queryRetryState(t, tc, orgID, stepID, runID)
			if tc2.holds {
				if stepState != "WAITING" || waitReason != "RECONCILIATION" {
					t.Fatalf("reconcile timeout must hold, got %s/%s", stepState, waitReason)
				}
			} else {
				if stepState != "WAITING" || waitReason != "RETRY_BACKOFF" || timerState != "PENDING" {
					t.Fatalf("safe timeout must schedule backoff, got %s/%s/%s", stepState, waitReason, timerState)
				}
			}
		})
	}
}

// TestCancelCommandIdempotency proves same-key cancel replays the recorded
// outcome while a different body under the same key conflicts.
func TestCancelCommandIdempotency(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	const digest = "bundle-cancel-idem-20"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "cancel-idem")
	rev := runRevision(t, tc, orgID, runID)

	firstStatus, firstBody := cancelRunHTTP(t, server, token, orgID, runID, rev, "idem-key-1")
	if firstStatus != http.StatusOK {
		t.Fatalf("first cancel expected 200, got %d", firstStatus)
	}
	secondStatus, secondBody := cancelRunHTTP(t, server, token, orgID, runID, rev, "idem-key-1")
	if secondStatus != http.StatusOK || fmt.Sprint(secondBody["status"]) != fmt.Sprint(firstBody["status"]) {
		t.Fatalf("same-key replay must return the same outcome, got %d (%v)", secondStatus, secondBody)
	}
	// Same key, different semantic body (stale revision) conflicts.
	raw, _ := json.Marshal(map[string]any{"expectedRevision": rev + 99})
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/runs/"+runID+"/cancel", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Organization-ID", orgID)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "idem-key-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var parsed map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&parsed)
	if resp.StatusCode != http.StatusConflict || parsed["code"] != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("conflicting reuse must be 409, got %d (%v)", resp.StatusCode, parsed)
	}
}

// TestClaimTimeoutClampedToOneHour proves the engine backstop: a manifest
// timeout above the 1h maximum claims attempts capped at exactly 1h.
func TestClaimTimeoutClampedToOneHour(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "timeout-clamp")
	const digest = "bundle-timeout-clamp-20"
	manifest := `{"targetOS":"linux","targetArchitecture":"amd64","tasks":[{"name":"task-a","entrypoint":"tasks/a.js","recovery":"safe","timeoutMs":7200000,"inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"retry":{"maxAttempts":3,"initialDelayMs":1000,"maxDelayMs":30000}}],"workflows":[{"name":"workflow-a","inputSchema":{"type":"object"},"nodes":[{"id":"node-a","type":"task","task":"task-a"}]}]}`
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, manifest)
	_, _ = seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "timeout-clamp-claim")
	if a1.AttemptTimeoutMs != 3600000 {
		t.Fatalf("attempt timeout must clamp to 1h, got %d", a1.AttemptTimeoutMs)
	}
}

// TestCreateRunDefaultsDeadline24h proves the MVP lifetime default is set at
// acceptance: every run carries a ~24h deadline for all later enforcement.
func TestCreateRunDefaultsDeadline24h(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()
	const workflowName = "deadline-default-flow"
	bundle := "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	schema := map[string]any{
		"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}},
		"required": []any{"value"}, "additionalProperties": false,
	}
	tasks := []map[string]any{
		{"name": "task-a", "entrypoint": "tasks/a.js", "timeoutMs": 30000, "recovery": "safe", "inputSchema": schema, "outputSchema": schema},
	}
	workflow := map[string]any{
		"manifestVersion": 1, "name": workflowName, "inputSchema": schema, "outputSchema": schema,
		"nodes": []map[string]any{
			{"id": "node-a", "type": "task", "task": "task-a", "after": []any{}, "input": map[string]any{"value": map[string]any{"$ref": "run.input", "pointer": "/value"}}},
		},
		"output": map[string]any{"value": map[string]any{"$ref": "step.output", "stepId": "node-a", "pointer": "/value"}},
	}
	dep := registerM1Deployment(t, server, adminKey, orgID, envID, createLifecycleManifest(bundle, tasks, []map[string]any{workflow}), "deadline-default")
	workerSession, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "deadline-default-worker")
	pollM1Worker(t, server, workerSession, []string{bundle}, "advertise-deadline-default")
	activateM1Deployment(t, server, adminKey, orgID, envID, workflowName, dep, 0, "activate-deadline-default")

	before := time.Now()
	run := createM1Run(t, server, adminKey, orgID, workflowName, envID, "deadline-default-run", "v")
	if run.DeadlineAt == nil {
		t.Fatalf("created run must carry the 24h deadline default")
	}
	deadline, err := time.Parse(time.RFC3339, *run.DeadlineAt)
	if err != nil {
		t.Fatalf("unparseable deadline %q: %v", *run.DeadlineAt, err)
	}
	if dt := deadline.Sub(before); dt < 23*time.Hour || dt > 25*time.Hour {
		t.Fatalf("deadline must be ~24h after acceptance, got %s", dt)
	}
}
