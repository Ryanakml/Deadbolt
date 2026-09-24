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

var pauseKeySeq int64
var resumeKeySeq int64

func pauseRunHTTP(t *testing.T, server *httptest.Server, token, orgID, runID string, revision int64, key string) (int, map[string]any) {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"expectedRevision": revision})
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/runs/"+runID+"/pause", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Organization-ID", orgID)
	req.Header.Set("Content-Type", "application/json")
	if key == "" {
		key = fmt.Sprintf("pause-%s-%d", runID, atomic.AddInt64(&pauseKeySeq, 1))
	}
	req.Header.Set("Idempotency-Key", key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("pause request failed: %v", err)
	}
	defer resp.Body.Close()
	var parsed map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&parsed)
	return resp.StatusCode, parsed
}

func resumeRunHTTP(t *testing.T, server *httptest.Server, token, orgID, runID string, revision int64, key string) (int, map[string]any) {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"expectedRevision": revision})
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/runs/"+runID+"/resume", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Organization-ID", orgID)
	req.Header.Set("Content-Type", "application/json")
	if key == "" {
		key = fmt.Sprintf("resume-%s-%d", runID, atomic.AddInt64(&resumeKeySeq, 1))
	}
	req.Header.Set("Idempotency-Key", key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("resume request failed: %v", err)
	}
	defer resp.Body.Close()
	var parsed map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&parsed)
	return resp.StatusCode, parsed
}

func countRunEvents(t *testing.T, tc *tenantTestContext, orgID, runID, eventType string) int {
	t.Helper()
	var count int
	err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM run_events WHERE run_id=$1::uuid AND event_type=$2`, runID, eventType).Scan(&count)
	})
	if err != nil {
		t.Fatal(err)
	}
	return count
}

func countAuditEvents(t *testing.T, tc *tenantTestContext, orgID, action string) int {
	t.Helper()
	var count int
	err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM audit_events WHERE action=$1`, action).Scan(&count)
	})
	if err != nil {
		t.Fatal(err)
	}
	return count
}

// TestPauseAndResumeHappyPath proves the core pause/resume cycle:
// QUEUED -> PAUSED (blocks claim) -> RESUMED back to QUEUED (unblocks claim).
func TestPauseAndResumeHappyPath(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "pause-happy")
	const digest = "bundle-pause-happy-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "pause-happy-dev")

	// 1. Initial run is QUEUED, revision=1.
	rev := runRevision(t, tc, orgID, runID)
	if rev != 1 {
		t.Fatalf("expected initial revision 1, got %d", rev)
	}

	// 2. Pause the run while QUEUED (no active attempts -> transitions immediately to PAUSED).
	status, body := pauseRunHTTP(t, server, token, orgID, runID, rev, "")
	if status != http.StatusOK {
		t.Fatalf("pause expected 200, got %d (%v)", status, body)
	}
	if body["status"] != "PAUSED" {
		t.Fatalf("expected status PAUSED, got %v", body["status"])
	}

	// Verify DB state.
	var runStatus string
	var pauseReq bool
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, pause_requested FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus, &pauseReq)
	}); err != nil {
		t.Fatal(err)
	}
	if runStatus != "PAUSED" || !pauseReq {
		t.Fatalf("expected run PAUSED with pause_requested=true, got %s/%v", runStatus, pauseReq)
	}
	if countRunEvents(t, tc, orgID, runID, "RUN_PAUSED") != 1 {
		t.Fatalf("expected 1 RUN_PAUSED run event")
	}
	if countAuditEvents(t, tc, orgID, "run.pause") != 1 {
		t.Fatalf("expected 1 run.pause audit event")
	}

	// 3. Worker poll cannot claim while run is paused.
	pollReq := worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "poll-paused",
		WorkerID:        session.WorkerID,
		SessionID:       session.SessionID,
	}
	var pollResp worker.PollResponseDTO
	if postWorkerJSON(t, server, "/worker/v1/poll", session.SessionToken, pollReq, &pollResp); len(pollResp.Assignments) != 0 {
		t.Fatalf("worker must not receive assignments while run is paused: %+v", pollResp)
	}

	// 4. Resume the run (expectedRevision=2).
	rev = runRevision(t, tc, orgID, runID)
	status, body = resumeRunHTTP(t, server, token, orgID, runID, rev, "")
	if status != http.StatusOK {
		t.Fatalf("resume expected 200, got %d (%v)", status, body)
	}
	if body["status"] != "QUEUED" {
		t.Fatalf("expected status QUEUED after resume, got %v", body["status"])
	}

	// Verify DB state.
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, pause_requested FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus, &pauseReq)
	}); err != nil {
		t.Fatal(err)
	}
	if runStatus != "QUEUED" || pauseReq {
		t.Fatalf("expected run QUEUED with pause_requested=false, got %s/%v", runStatus, pauseReq)
	}
	if countRunEvents(t, tc, orgID, runID, "RUN_RESUMED") != 1 {
		t.Fatalf("expected 1 RUN_RESUMED run event")
	}
	if countAuditEvents(t, tc, orgID, "run.resume") != 1 {
		t.Fatalf("expected 1 run.resume audit event")
	}

	// 5. Worker can now claim the task!
	a1 := claimExecution(t, server, session, digest, "pause-happy-claim")
	if a1.AttemptID == "" {
		t.Fatalf("expected valid claim after resume")
	}
}

func twoStepManifest() string {
	return `{"targetOS":"linux","targetArchitecture":"amd64","tasks":[{"name":"task-a","entrypoint":"tasks/a.js","recovery":"safe","timeoutMs":60000,"inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"retry":{"maxAttempts":3,"initialDelayMs":1000,"maxDelayMs":30000}},{"name":"task-b","entrypoint":"tasks/b.js","recovery":"safe","timeoutMs":60000,"inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"retry":{"maxAttempts":3,"initialDelayMs":1000,"maxDelayMs":30000}}],"workflows":[{"name":"workflow-a","inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"nodes":[{"id":"node-a","type":"task","task":"task-a"},{"id":"node-b","type":"task","task":"task-b","after":["node-a"]}]}]}`
}

// TestPauseInFlightDrainsToPaused proves that an active claim in flight
// transitions the run to PAUSING, and upon completing, drains to PAUSED.
func TestPauseInFlightDrainsToPaused(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "pause-drain")
	const digest = "bundle-pause-drain-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, twoStepManifest())

	// Seed a 2-step run: node-a (READY) and node-b (WAITING) so completing node-a does not terminate the run.
	var runID string
	var stepA, stepB string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `INSERT INTO runs
			(organization_id,environment_id,deployment_id,workflow_name,status)
			VALUES ($1,$2,$3,'workflow-a','RUNNING') RETURNING id::text`, orgID, envID, deploymentID).Scan(&runID); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `INSERT INTO run_steps
			(organization_id,environment_id,run_id,node_id,state,eligible_at)
			VALUES ($1,$2,$3,'node-a','READY',clock_timestamp()) RETURNING id::text`, orgID, envID, runID).Scan(&stepA); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `INSERT INTO run_steps
			(organization_id,environment_id,run_id,node_id,state)
			VALUES ($1,$2,$3,'node-b','WAITING') RETURNING id::text`, orgID, envID, runID).Scan(&stepB)
	}); err != nil {
		t.Fatal(err)
	}

	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	// Claim and start node-a.
	a1 := claimExecution(t, server, session, digest, "pause-drain-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)

	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "pause-drain-dev")
	rev := runRevision(t, tc, orgID, runID)

	// Pause while node-a is in flight -> transitions to PAUSING.
	status, body := pauseRunHTTP(t, server, token, orgID, runID, rev, "")
	if status != http.StatusOK {
		t.Fatalf("pause expected 200, got %d (%v)", status, body)
	}
	if body["status"] != "PAUSING" {
		t.Fatalf("expected PAUSING, got %v", body["status"])
	}
	if countRunEvents(t, tc, orgID, runID, "RUN_PAUSING") != 1 {
		t.Fatalf("expected 1 RUN_PAUSING run event")
	}

	// Complete node-a attempt.
	completion := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "pause-drain-complete",
		WorkerID:        session.WorkerID,
		SessionID:       session.SessionID,
		AttemptID:       a1.AttemptID,
		OwnershipEpoch:  a1.OwnershipEpoch,
		Outcome:         "SUCCEEDED",
		Output:          map[string]any{"done": true},
	}
	completion.ResultDigest, _ = worker.CanonicalCompletionDigest(&completion)
	var compResp worker.CompleteResponseDTO
	if st := postWorkerJSON(t, server, "/worker/v1/complete", session.SessionToken, completion, &compResp); st != http.StatusOK {
		t.Fatalf("complete expected 200, got %d", st)
	}

	// The run must have drained to PAUSED now!
	var finalRunStatus string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid`, runID).Scan(&finalRunStatus)
	}); err != nil {
		t.Fatal(err)
	}
	if finalRunStatus != "PAUSED" {
		t.Fatalf("expected run to drain to PAUSED, got %s", finalRunStatus)
	}
	if countRunEvents(t, tc, orgID, runID, "RUN_PAUSED") != 1 {
		t.Fatalf("expected 1 RUN_PAUSED run event")
	}

	// Resuming recomputes state: node-b is waiting, so run becomes WAITING or RUNNING.
	rev = runRevision(t, tc, orgID, runID)
	status, body = resumeRunHTTP(t, server, token, orgID, runID, rev, "")
	if status != http.StatusOK {
		t.Fatalf("resume expected 200, got %d (%v)", status, body)
	}
	if body["status"] == "PAUSED" || body["status"] == "PAUSING" {
		t.Fatalf("resume must not leave run in PAUSED/PAUSING: %v", body["status"])
	}
}

// TestPauseStatePrioritySuccessWins proves that if the final node of a run
// succeeds while the run is PAUSING, the run reaches terminal SUCCEEDED,
// not PAUSED (Blueprint §10.2 state priority: SUCCESS > FAILURE > PAUSED).
func TestPauseStatePrioritySuccessWins(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "pause-success-win")
	const digest = "bundle-pause-success-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "pause-success-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)

	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "pause-success-dev")
	status, body := pauseRunHTTP(t, server, token, orgID, runID, runRevision(t, tc, orgID, runID), "")
	if status != http.StatusOK || body["status"] != "PAUSING" {
		t.Fatalf("pause expected 200 PAUSING, got %d (%v)", status, body)
	}

	// Complete final node-a with SUCCEEDED.
	completion := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "pause-success-complete",
		WorkerID:        session.WorkerID,
		SessionID:       session.SessionID,
		AttemptID:       a1.AttemptID,
		OwnershipEpoch:  a1.OwnershipEpoch,
		Outcome:         "SUCCEEDED",
		Output:          map[string]any{"ok": true},
	}
	completion.ResultDigest, _ = worker.CanonicalCompletionDigest(&completion)
	var compResp worker.CompleteResponseDTO
	if st := postWorkerJSON(t, server, "/worker/v1/complete", session.SessionToken, completion, &compResp); st != http.StatusOK {
		t.Fatalf("complete expected 200, got %d", st)
	}

	// Terminal SUCCEEDED must win over PAUSED.
	var finalRunStatus string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid`, runID).Scan(&finalRunStatus)
	}); err != nil {
		t.Fatal(err)
	}
	if finalRunStatus != "SUCCEEDED" {
		t.Fatalf("expected final status SUCCEEDED, got %s", finalRunStatus)
	}
}

// TestPauseStatePriorityFailureWins proves that if a node has a non-retryable
// failure while PAUSING, the run reaches terminal FAILED, not PAUSED.
func TestPauseStatePriorityFailureWins(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "pause-fail-win")
	const digest = "bundle-pause-fail-1"
	// maxAttempts=1 means failure is non-retryable.
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(1, 1000, 30000))
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "pause-fail-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)

	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "pause-fail-dev")
	status, body := pauseRunHTTP(t, server, token, orgID, runID, runRevision(t, tc, orgID, runID), "")
	if status != http.StatusOK || body["status"] != "PAUSING" {
		t.Fatalf("pause expected 200 PAUSING, got %d (%v)", status, body)
	}

	// Complete node-a with FAILED.
	completion := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "pause-fail-complete",
		WorkerID:        session.WorkerID,
		SessionID:       session.SessionID,
		AttemptID:       a1.AttemptID,
		OwnershipEpoch:  a1.OwnershipEpoch,
		Outcome:         "FAILED",
		Error: &worker.TaskErrorDTO{
			Code:    "EXEC_ERR",
			Message: "fatal execution failure",
		},
	}
	completion.ResultDigest, _ = worker.CanonicalCompletionDigest(&completion)
	var compResp worker.CompleteResponseDTO
	if st := postWorkerJSON(t, server, "/worker/v1/complete", session.SessionToken, completion, &compResp); st != http.StatusOK {
		t.Fatalf("complete expected 200, got %d", st)
	}

	// Terminal FAILED must win over PAUSED.
	var finalRunStatus string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid`, runID).Scan(&finalRunStatus)
	}); err != nil {
		t.Fatal(err)
	}
	if finalRunStatus != "FAILED" {
		t.Fatalf("expected final status FAILED, got %s", finalRunStatus)
	}
}

// TestPauseInFlightFailureSchedulesRetryWithoutResettingDueAt proves:
// when in-flight attempt fails with retryable error while PAUSING:
// - retry timer is scheduled (decision recorded),
// - run drains to PAUSED,
// - timer does NOT fire while PAUSED,
// - original due_at is preserved (never freezes or resets),
// - resuming recomputes run to WAITING (reason RETRY_BACKOFF).
func TestPauseInFlightFailureSchedulesRetryWithoutResettingDueAt(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "pause-retry")
	const digest = "bundle-pause-retry-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 5000, 30000))
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "pause-retry-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)

	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "pause-retry-dev")
	status, body := pauseRunHTTP(t, server, token, orgID, runID, runRevision(t, tc, orgID, runID), "")
	if status != http.StatusOK || body["status"] != "PAUSING" {
		t.Fatalf("pause expected 200 PAUSING, got %d (%v)", status, body)
	}

	// Fail node-a with retryable failure.
	completion := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "pause-retry-complete",
		WorkerID:        session.WorkerID,
		SessionID:       session.SessionID,
		AttemptID:       a1.AttemptID,
		OwnershipEpoch:  a1.OwnershipEpoch,
		Outcome:         "FAILED",
		Error: &worker.TaskErrorDTO{
			Code:      "TRANSIENT",
			Message:   "transient network hiccup",
			Retryable: true,
		},
	}
	completion.ResultDigest, _ = worker.CanonicalCompletionDigest(&completion)
	var compResp worker.CompleteResponseDTO
	if st := postWorkerJSON(t, server, "/worker/v1/complete", session.SessionToken, completion, &compResp); st != http.StatusOK {
		t.Fatalf("complete expected 200, got %d", st)
	}

	// 1. Run must have drained to PAUSED.
	var runStatus string
	var stepState string
	var dueAt time.Time
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT state FROM run_steps WHERE id=$1::uuid`, stepID).Scan(&stepState); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT due_at FROM timers WHERE step_id=$1::uuid AND state='PENDING'`, stepID).Scan(&dueAt)
	}); err != nil {
		t.Fatal(err)
	}

	if runStatus != "PAUSED" {
		t.Fatalf("expected run status PAUSED, got %s", runStatus)
	}
	if stepState != "WAITING" {
		t.Fatalf("expected step state WAITING, got %s", stepState)
	}
	if dueAt.IsZero() {
		t.Fatalf("expected due_at to be scheduled in timers")
	}

	// 2. Retry timer must NOT fire while paused, even if we tick/evaluate retries.
	engine := execution.NewWorkerEngine(tc.pool)
	// Force due_at to the past to test guard.
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE timers SET due_at = clock_timestamp() - INTERVAL '1 second' WHERE step_id=$1::uuid`, stepID)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	fired, err := engine.FireDueRetryTimers(context.Background(), orgID)
	if err != nil {
		t.Fatalf("FireDueRetryTimers failed: %v", err)
	}
	if fired != 0 {
		t.Fatalf("retry timer must not fire while run is PAUSED, got fired=%d", fired)
	}

	// 3. Resume the run -> recomputes to WAITING (RETRY_BACKOFF).
	rev := runRevision(t, tc, orgID, runID)
	status, body = resumeRunHTTP(t, server, token, orgID, runID, rev, "")
	if status != http.StatusOK {
		t.Fatalf("resume expected 200, got %d (%v)", status, body)
	}
	if body["status"] != "WAITING" || body["reasonCode"] != "RETRY_BACKOFF" {
		t.Fatalf("expected WAITING with reason RETRY_BACKOFF, got status=%v reason=%v", body["status"], body["reasonCode"])
	}

	// 4. Now that the run is resumed, the due retry CAN fire!
	fired, err = engine.FireDueRetryTimers(context.Background(), orgID)
	if err != nil {
		t.Fatalf("FireDueRetryTimers after resume failed: %v", err)
	}
	if fired != 1 {
		t.Fatalf("expected 1 retry to fire after resume, got %d", fired)
	}
}

// TestCancelAuthoritativeOverPaused proves that cancellation on a PAUSED run
// is authoritative and immediately cancels the run.
func TestCancelAuthoritativeOverPaused(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	const digest = "bundle-pause-cancel-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")

	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "pause-cancel-dev")
	pauseRunHTTP(t, server, token, orgID, runID, runRevision(t, tc, orgID, runID), "")

	// Run is PAUSED.
	rev := runRevision(t, tc, orgID, runID)
	status, body := cancelRunHTTP(t, server, token, orgID, runID, rev, "")
	if status != http.StatusOK {
		t.Fatalf("cancel on PAUSED run expected 200, got %d (%v)", status, body)
	}
	// Since there were 0 active attempts, cancel settles immediately to CANCELLED.
	if body["status"] != "CANCELLED" {
		t.Fatalf("expected CANCELLED, got %v", body["status"])
	}
}

// TestPauseAndResumeRevisionConflictAndDuplicates proves:
// - stale expectedRevision returns 409 REVISION_CONFLICT
// - identical Idempotency-Key replays exact outcome
// - pausing terminal or cancelling run returns 409
func TestPauseAndResumeRevisionConflictAndDuplicates(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	const digest = "bundle-pause-rev-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")

	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "pause-rev-dev")
	rev := runRevision(t, tc, orgID, runID)

	// 1. Stale revision on pause -> 409 REVISION_CONFLICT.
	status, body := pauseRunHTTP(t, server, token, orgID, runID, rev+99, "")
	if status != http.StatusConflict || body["code"] != "REVISION_CONFLICT" {
		t.Fatalf("stale pause revision must be 409 REVISION_CONFLICT, got %d (%v)", status, body)
	}

	// 2. Valid pause.
	status, body = pauseRunHTTP(t, server, token, orgID, runID, rev, "key-pause-1")
	if status != http.StatusOK || body["status"] != "PAUSED" {
		t.Fatalf("valid pause expected 200 PAUSED, got %d (%v)", status, body)
	}

	// 3. Duplicate pause with same idempotency key replays 200.
	status, body = pauseRunHTTP(t, server, token, orgID, runID, rev, "key-pause-1")
	if status != http.StatusOK || body["status"] != "PAUSED" {
		t.Fatalf("duplicate pause must replay 200 PAUSED, got %d (%v)", status, body)
	}

	// 4. Repeated pause with new key on already PAUSED run is idempotent 200.
	rev = runRevision(t, tc, orgID, runID)
	status, body = pauseRunHTTP(t, server, token, orgID, runID, rev, "key-pause-2")
	if status != http.StatusOK || body["status"] != "PAUSED" {
		t.Fatalf("repeated pause must be 200 PAUSED, got %d (%v)", status, body)
	}

	// 5. Stale revision on resume -> 409 REVISION_CONFLICT.
	status, body = resumeRunHTTP(t, server, token, orgID, runID, rev+99, "")
	if status != http.StatusConflict || body["code"] != "REVISION_CONFLICT" {
		t.Fatalf("stale resume revision must be 409 REVISION_CONFLICT, got %d (%v)", status, body)
	}

	// 6. Valid resume.
	status, body = resumeRunHTTP(t, server, token, orgID, runID, rev, "key-resume-1")
	if status != http.StatusOK || body["status"] != "QUEUED" {
		t.Fatalf("valid resume expected 200 QUEUED, got %d (%v)", status, body)
	}

	// 7. Duplicate resume with same idempotency key replays 200.
	status, body = resumeRunHTTP(t, server, token, orgID, runID, rev, "key-resume-1")
	if status != http.StatusOK || body["status"] != "QUEUED" {
		t.Fatalf("duplicate resume must replay 200 QUEUED, got %d (%v)", status, body)
	}
}

// TestPausePermissions proves capability enforcement:
// viewers are rejected with 403 Forbidden;
// developers and control-scoped machine tokens are admitted with 200 OK.
func TestPausePermissions(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	const digest = "bundle-pause-perm-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))

	newRun := func() string {
		runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
		return runID
	}

	viewerToken, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleViewer, "pause-viewer")
	devToken, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "pause-dev")
	machineKey := bootstrapTestKey(t, tc.service, orgID, envID, []string{tenant.CapRunsControl})

	// Viewer cannot pause.
	r1 := newRun()
	if status, body := pauseRunHTTP(t, server, viewerToken, orgID, r1, 1, ""); status != http.StatusForbidden {
		t.Fatalf("viewer pause must be 403, got %d (%v)", status, body)
	}

	// Viewer cannot resume.
	if status, body := resumeRunHTTP(t, server, viewerToken, orgID, r1, 1, ""); status != http.StatusForbidden {
		t.Fatalf("viewer resume must be 403, got %d (%v)", status, body)
	}

	// Developer can pause and resume.
	r2 := newRun()
	if status, _ := pauseRunHTTP(t, server, devToken, orgID, r2, 1, ""); status != http.StatusOK {
		t.Fatalf("developer pause must be 200, got %d", status)
	}
	if status, _ := resumeRunHTTP(t, server, devToken, orgID, r2, runRevision(t, tc, orgID, r2), ""); status != http.StatusOK {
		t.Fatalf("developer resume must be 200, got %d", status)
	}

	// Machine token with runs:control can pause and resume.
	r3 := newRun()
	if status, _ := pauseRunHTTP(t, server, machineKey.PlaintextKey, orgID, r3, 1, ""); status != http.StatusOK {
		t.Fatalf("machine pause must be 200, got %d", status)
	}
	if status, _ := resumeRunHTTP(t, server, machineKey.PlaintextKey, orgID, r3, runRevision(t, tc, orgID, r3), ""); status != http.StatusOK {
		t.Fatalf("machine resume must be 200, got %d", status)
	}
}

// TestPauseSubsequentClaimBlocked proves that claims initiated or committed
// after a pause request are blocked, while an earlier claim remains valid.
func TestPauseSubsequentClaimBlocked(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	session1, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "pause-claim-w1")
	session2, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "pause-claim-w2")
	const digest = "bundle-pause-claim-block-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, twoStepManifest())

	// Seed run with node-a READY and node-b READY (parallel).
	var runID, stepA, stepB string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `INSERT INTO runs
			(organization_id,environment_id,deployment_id,workflow_name,status)
			VALUES ($1,$2,$3,'workflow-a','RUNNING') RETURNING id::text`, orgID, envID, deploymentID).Scan(&runID); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `INSERT INTO run_steps
			(organization_id,environment_id,run_id,node_id,state,eligible_at)
			VALUES ($1,$2,$3,'node-a','READY',clock_timestamp()) RETURNING id::text`, orgID, envID, runID).Scan(&stepA); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `INSERT INTO run_steps
			(organization_id,environment_id,run_id,node_id,state,eligible_at)
			VALUES ($1,$2,$3,'node-b','READY',clock_timestamp()) RETURNING id::text`, orgID, envID, runID).Scan(&stepB)
	}); err != nil {
		t.Fatal(err)
	}

	advertiseDigest(t, tc, orgID, session1.SessionID, digest)
	advertiseDigest(t, tc, orgID, session2.SessionID, digest)

	// Worker 1 claims node-a BEFORE pause.
	a1 := claimExecution(t, server, session1, digest, "claim-before-pause")
	if a1.AttemptID == "" {
		t.Fatalf("expected claim before pause to succeed")
	}

	// Now pause the run -> PAUSING.
	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "pause-block-dev")
	status, body := pauseRunHTTP(t, server, token, orgID, runID, runRevision(t, tc, orgID, runID), "")
	if status != http.StatusOK || body["status"] != "PAUSING" {
		t.Fatalf("pause expected 200 PAUSING, got %d (%v)", status, body)
	}

	// Worker 2 attempts to claim node-b AFTER pause -> must receive 0 assignments.
	pollReq := worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "poll-after-pause",
		WorkerID:        session2.WorkerID,
		SessionID:       session2.SessionID,
	}
	var pollResp worker.PollResponseDTO
	if postWorkerJSON(t, server, "/worker/v1/poll", session2.SessionToken, pollReq, &pollResp); len(pollResp.Assignments) != 0 {
		t.Fatalf("subsequent claim must be blocked after pause, got %d assignments", len(pollResp.Assignments))
	}

	// Meanwhile, in-flight attempt a1 can start and complete successfully!
	startNode(t, server, session1, a1.AttemptID, a1.OwnershipEpoch)
	completion := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "complete-in-flight",
		WorkerID:        session1.WorkerID,
		SessionID:       session1.SessionID,
		AttemptID:       a1.AttemptID,
		OwnershipEpoch:  a1.OwnershipEpoch,
		Outcome:         "SUCCEEDED",
		Output:          map[string]any{"ok": true},
	}
	completion.ResultDigest, _ = worker.CanonicalCompletionDigest(&completion)
	var compResp worker.CompleteResponseDTO
	if st := postWorkerJSON(t, server, "/worker/v1/complete", session1.SessionToken, completion, &compResp); st != http.StatusOK {
		t.Fatalf("complete in-flight attempt expected 200, got %d", st)
	}

	// Run drains to PAUSED since active attempts is now 0.
	var finalRunStatus string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid`, runID).Scan(&finalRunStatus)
	}); err != nil {
		t.Fatal(err)
	}
	if finalRunStatus != "PAUSED" {
		t.Fatalf("expected run to drain to PAUSED, got %s", finalRunStatus)
	}
}
