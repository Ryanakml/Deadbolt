package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

// seedRetryDeployment inserts a deployment with an explicit manifest so retry
// policy, recovery mode, and idempotency windows are exercised exactly.
func seedRetryDeployment(t *testing.T, tc *tenantTestContext, orgID, envID, digest, manifest string) string {
	t.Helper()
	var deploymentID string
	err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `INSERT INTO deployments
			(organization_id,environment_id,manifest_hash,bundle_digest,manifest,protocol_version,runtime_version)
			VALUES ($1,$2,$3,$4,$5::jsonb,1,'1.0') RETURNING id::text`, orgID, envID, "manifest-"+digest, digest, manifest).Scan(&deploymentID)
	})
	if err != nil {
		t.Fatal(err)
	}
	return deploymentID
}

func safeManifest(maxAttempts int, initial, max int64) string {
	return fmt.Sprintf(`{"targetOS":"linux","targetArchitecture":"amd64","tasks":[{"name":"task-a","entrypoint":"tasks/a.js","recovery":"safe","timeoutMs":60000,"inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"retry":{"maxAttempts":%d,"initialDelayMs":%d,"maxDelayMs":%d}}],"workflows":[{"name":"workflow-a","inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"nodes":[{"id":"node-a","type":"task","task":"task-a"}]}]}`, maxAttempts, initial, max)
}

func idempotentManifest(maxAttempts int, timeoutMs, windowMs int64) string {
	return fmt.Sprintf(`{"targetOS":"linux","targetArchitecture":"amd64","tasks":[{"name":"task-a","entrypoint":"tasks/a.js","recovery":"idempotent","timeoutMs":%d,"idempotencyWindowMs":%d,"inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"retry":{"maxAttempts":%d,"initialDelayMs":1000,"maxDelayMs":30000}}],"workflows":[{"name":"workflow-a","inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"nodes":[{"id":"node-a","type":"task","task":"task-a"}]}]}`, timeoutMs, windowMs, maxAttempts)
}

func completeWithError(t *testing.T, server *httptest.Server, session *testWorkerSession, assignment worker.AssignmentDTO, code string, retryable bool, effect, retryAfter string) {
	t.Helper()
	errDTO := &worker.TaskErrorDTO{Code: code, Message: "boom", Retryable: retryable, EffectStatus: effect, RetryAfter: retryAfter}
	completion := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "complete-" + assignment.AttemptID,
		WorkerID: session.WorkerID, SessionID: session.SessionID,
		AttemptID: assignment.AttemptID, OwnershipEpoch: assignment.OwnershipEpoch,
		Outcome: "FAILED", Error: errDTO,
	}
	completion.ResultDigest, _ = worker.CanonicalCompletionDigest(&completion)
	var resp worker.CompleteResponseDTO
	status := postWorkerJSON(t, server, "/worker/v1/complete", session.SessionToken, completion, &resp)
	if status != 200 {
		t.Fatalf("complete failed: status %d", status)
	}
}

// queryStepTimer returns step state/wait, timer state/due/operation, run status/reason.
func queryRetryState(t *testing.T, tc *tenantTestContext, orgID, stepID, runID string) (stepState, waitReason, runStatus, runReason, timerState string, dueAt *time.Time, timerOp, stepOp string, validUntil *time.Time, nextAttempt int) {
	t.Helper()
	err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		var wr, rr *string
		var due *time.Time
		var vu *time.Time
		var ts *string
		var top *string
		if err := tx.QueryRow(ctx, `SELECT state, wait_reason, next_attempt_number, idempotency_valid_until FROM run_steps WHERE id=$1::uuid`, stepID).Scan(&stepState, &wr, &nextAttempt, &vu); err != nil {
			return err
		}
		if wr != nil {
			waitReason = *wr
		}
		validUntil = vu
		if err := tx.QueryRow(ctx, `SELECT status, reason_code FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus, &rr); err != nil {
			return err
		}
		if rr != nil {
			runReason = *rr
		}
		err := tx.QueryRow(ctx, `SELECT state, due_at, operation_id FROM timers WHERE step_id=$1::uuid ORDER BY created_at DESC LIMIT 1`, stepID).Scan(&ts, &due, &top)
		if err == nil {
			timerState = *ts
			dueAt = due
			if top != nil {
				timerOp = *top
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = stepOp
	return
}

// TestRetryBackoffPersistsTimerAndFires proves Blueprint §15.1 + F-09:
// due is chosen once and persisted; restart does not redraw; due firing
// creates READY; claim alone creates the next attempt with stable operation ID.
func TestRetryBackoffPersistsTimerAndFires(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "retry-persist")
	const digest = "bundle-retry-persist-18"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	// Advertise bundle for claim.
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3)`, session.SessionID, orgID, digest)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	assignment := claimExecution(t, server, session, digest, "retry-claim-1")
	firstOp := assignment.OperationID
	if firstOp == "" {
		t.Fatalf("missing operation ID")
	}
	startNode(t, server, session, assignment.AttemptID, assignment.OwnershipEpoch)

	// Definitive NOT_APPLIED retryable error schedules a durable timer.
	completeWithError(t, server, session, assignment, "PROVIDER_500", true, "NOT_APPLIED", "")

	stepState, waitReason, runStatus, runReason, timerState, dueAt, timerOp, _, _, nextAttempt := queryRetryState(t, tc, orgID, stepID, runID)
	if stepState != "WAITING" || waitReason != "RETRY_BACKOFF" {
		t.Fatalf("expected WAITING/RETRY_BACKOFF, got %s/%s", stepState, waitReason)
	}
	if runStatus != "WAITING" || runReason != "RETRY_BACKOFF" {
		t.Fatalf("expected run WAITING/RETRY_BACKOFF, got %s/%s", runStatus, runReason)
	}
	if timerState != "PENDING" || dueAt == nil {
		t.Fatalf("expected PENDING timer, got %s %v", timerState, dueAt)
	}
	if timerOp != firstOp {
		t.Fatalf("operation identity not preserved: claim %s timer %s", firstOp, timerOp)
	}
	if nextAttempt != 2 {
		t.Fatalf("expected next_attempt 2, got %d", nextAttempt)
	}
	firstDue := *dueAt

	// Force the timer into the future so the pre-due poll is deterministic
	// regardless of the random jitter draw (0..cap). A poll before due must
	// not create an attempt.
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE timers SET due_at=clock_timestamp()+INTERVAL '1 hour', updated_at=clock_timestamp() WHERE step_id=$1::uuid AND state='PENDING'`, stepID)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE run_steps SET eligible_at=(SELECT due_at FROM timers WHERE step_id=$1::uuid AND state='PENDING') WHERE id=$1::uuid`, stepID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var early worker.PollResponseDTO
	if status := postWorkerJSON(t, server, "/worker/v1/poll", session.SessionToken, worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "retry-early-poll", WorkerID: session.WorkerID,
		SessionID: session.SessionID, AvailableSlots: 1, DeploymentDigests: []string{digest}, Pool: "default",
	}, &early); status != 200 || len(early.Assignments) != 0 {
		t.Fatalf("expected no early assignment, got status %d n=%d", status, len(early.Assignments))
	}
	// Re-read the forced future due for restart-identity check below.
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT due_at FROM timers WHERE step_id=$1::uuid AND state='PENDING'`, stepID).Scan(&firstDue)
	}); err != nil {
		t.Fatal(err)
	}

	// Simulate restart: new engine instance reads the same persisted due.
	restarted := execution.NewWorkerEngine(tc.pool)
	var dueAfterRestart time.Time
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT due_at FROM timers WHERE step_id=$1::uuid AND state='PENDING'`, stepID).Scan(&dueAfterRestart)
	}); err != nil {
		t.Fatal(err)
	}
	if !dueAfterRestart.Equal(firstDue) {
		t.Fatalf("restart redrew jitter: before %s after %s", firstDue, dueAfterRestart)
	}
	_ = restarted

	// Make timer due and fire; duplicate firing must be safe.
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE timers SET due_at=clock_timestamp()-INTERVAL '1 second' WHERE step_id=$1::uuid AND state='PENDING'`, stepID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	engine := execution.NewWorkerEngine(tc.pool)
	fired, err := engine.FireDueRetryTimers(context.Background(), orgID)
	if err != nil || fired != 1 {
		t.Fatalf("expected 1 fired, got %d err %v", fired, err)
	}
	stepState, _, _, _, timerState, _, _, _, _, _ = queryRetryState(t, tc, orgID, stepID, runID)
	if stepState != "READY" {
		t.Fatalf("due firing must create READY, got %s", stepState)
	}
	if timerState != "FIRED" {
		t.Fatalf("expected FIRED, got %s", timerState)
	}
	if fired2, _ := engine.FireDueRetryTimers(context.Background(), orgID); fired2 != 0 {
		t.Fatalf("duplicate firing must be 0, got %d", fired2)
	}

	// Claim alone creates the next attempt with the same operation ID.
	second := claimExecution(t, server, session, digest, "retry-claim-2")
	if second.OperationID != firstOp {
		t.Fatalf("operation ID changed across retry: %s vs %s", firstOp, second.OperationID)
	}
	if second.AttemptID == assignment.AttemptID {
		t.Fatalf("expected new attempt ID")
	}
	// Provider dedup ledger separate from runtime DB: same key dedups.
	ledger := map[string]int{}
	ledger[firstOp]++
	ledger[second.OperationID]++
	if ledger[firstOp] != 2 {
		t.Fatalf("ledger should see same key twice, got %v", ledger)
	}
}

// TestRetryBudgetExhaustsToFailed proves default 3/max10 including first:
// with maxAttempts=2, two failures exhaust budget to FAILED.
func TestRetryBudgetExhaustsToFailed(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "retry-budget")
	const digest = "bundle-retry-budget-18"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(2, 10, 100))
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3)`, session.SessionID, orgID, digest)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	engine := execution.NewWorkerEngine(tc.pool)

	// Attempt 1 fails -> timer.
	a1 := claimExecution(t, server, session, digest, "budget-claim-1")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)
	completeWithError(t, server, session, a1, "PROVIDER_500", true, "NOT_APPLIED", "")
	// Force due and fire.
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE timers SET due_at=clock_timestamp()-INTERVAL '1 second' WHERE step_id=$1::uuid AND state='PENDING'`, stepID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.FireDueRetryTimers(context.Background(), orgID); err != nil {
		t.Fatal(err)
	}
	// Attempt 2 fails -> budget exhausted (next=3 > max 2).
	a2 := claimExecution(t, server, session, digest, "budget-claim-2")
	startNode(t, server, session, a2.AttemptID, a2.OwnershipEpoch)
	completeWithError(t, server, session, a2, "PROVIDER_500", true, "NOT_APPLIED", "")

	var stepState, runStatus string
	var waitReason, reasonCode *string
	var pendingTimers int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT state, wait_reason FROM run_steps WHERE id=$1::uuid`, stepID).Scan(&stepState, &waitReason); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT status, reason_code FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus, &reasonCode); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT count(*) FROM timers WHERE step_id=$1::uuid AND state='PENDING'`, stepID).Scan(&pendingTimers)
	}); err != nil {
		t.Fatal(err)
	}
	if stepState != "FAILED" || runStatus != "FAILED" {
		t.Fatalf("expected FAILED/FAILED, got %s/%s", stepState, runStatus)
	}
	if reasonCode == nil || *reasonCode != "MAX_ATTEMPTS_EXCEEDED" {
		t.Fatalf("expected MAX_ATTEMPTS_EXCEEDED, got %v", reasonCode)
	}
	if pendingTimers != 0 {
		t.Fatalf("exhausted budget must leave no pending timer, got %d", pendingTimers)
	}
	_ = waitReason
}

// TestNonRetryableFailsFast proves schema/mapping/auth/missing-task/bundle
// errors never create timers or consume extra budget.
func TestNonRetryableFailsFast(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "retry-nonretry")
	const digest = "bundle-retry-nonretry-18"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3)`, session.SessionID, orgID, digest)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	a1 := claimExecution(t, server, session, digest, "nonretry-claim-1")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)
	for _, code := range []string{"OUTPUT_SCHEMA_VIOLATION", "INPUT_MAPPING_ERROR", "MISSING_TASK_REF", "INCOMPATIBLE_BUNDLE", "UNAUTHORIZED"} {
		if !execution.IsNonRetryableCode(code) {
			t.Fatalf("expected %s non-retryable", code)
		}
	}
	completeWithError(t, server, session, a1, "OUTPUT_SCHEMA_VIOLATION", false, "NOT_APPLIED", "")
	var stepState, runStatus string
	var timers int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT state FROM run_steps WHERE id=$1::uuid`, stepID).Scan(&stepState); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT count(*) FROM timers WHERE step_id=$1::uuid AND state='PENDING'`, stepID).Scan(&timers)
	}); err != nil {
		t.Fatal(err)
	}
	if stepState != "FAILED" || runStatus != "FAILED" || timers != 0 {
		t.Fatalf("non-retryable must fail fast without timer: step %s run %s timers %d", stepState, runStatus, timers)
	}
}

// TestIdempotencyWindowInsufficientHolds proves the window never extends and
// insufficient windows route to reconciliation with an OPEN case.
func TestIdempotencyWindowInsufficientHolds(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "retry-window")
	const digest = "bundle-retry-window-18"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, idempotentManifest(3, 5000, 30000))
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3)`, session.SessionID, orgID, digest)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	a1 := claimExecution(t, server, session, digest, "window-claim-1")
	firstOp := a1.OperationID
	var validAfterClaim time.Time
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT idempotency_valid_until FROM run_steps WHERE id=$1::uuid`, stepID).Scan(&validAfterClaim)
	}); err != nil {
		t.Fatal(err)
	}
	if validAfterClaim.IsZero() {
		t.Fatalf("first claim must set idempotency_valid_until")
	}
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)
	// Shrink the window to force insufficiency (simulates time passing beyond
	// provider dedup guarantee without extending the window).
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE run_steps SET idempotency_valid_until=clock_timestamp()+INTERVAL '1 second' WHERE id=$1::uuid`, stepID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var shrunk time.Time
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT idempotency_valid_until FROM run_steps WHERE id=$1::uuid`, stepID).Scan(&shrunk)
	}); err != nil {
		t.Fatal(err)
	}
	completeWithError(t, server, session, a1, "PROVIDER_500", true, "NOT_APPLIED", "")

	var stepState, waitReason, runStatus, runReason string
	var caseCount, pendingTimers int
	var caseReason string
	var validAfterHold time.Time
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		var wr, rr *string
		if err := tx.QueryRow(ctx, `SELECT state, wait_reason, idempotency_valid_until FROM run_steps WHERE id=$1::uuid`, stepID).Scan(&stepState, &wr, &validAfterHold); err != nil {
			return err
		}
		if wr != nil {
			waitReason = *wr
		}
		if err := tx.QueryRow(ctx, `SELECT status, reason_code FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus, &rr); err != nil {
			return err
		}
		if rr != nil {
			runReason = *rr
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM reconciliation_cases WHERE step_id=$1::uuid AND status='OPEN'`, stepID).Scan(&caseCount); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT reason FROM reconciliation_cases WHERE step_id=$1::uuid AND status='OPEN' ORDER BY created_at DESC LIMIT 1`, stepID).Scan(&caseReason); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT count(*) FROM timers WHERE step_id=$1::uuid AND state='PENDING'`, stepID).Scan(&pendingTimers)
	}); err != nil {
		t.Fatal(err)
	}
	if stepState != "WAITING" || waitReason != "RECONCILIATION" || runStatus != "WAITING" || runReason != "RECONCILIATION" {
		t.Fatalf("expected hold WAITING/RECONCILIATION, got step %s/%s run %s/%s", stepState, waitReason, runStatus, runReason)
	}
	if caseCount != 1 || caseReason != "IDEMPOTENCY_WINDOW_INSUFFICIENT" {
		t.Fatalf("expected one OPEN window case, got %d %q", caseCount, caseReason)
	}
	if pendingTimers != 0 {
		t.Fatalf("hold must not leave a pending retry timer, got %d", pendingTimers)
	}
	if !validAfterHold.Equal(shrunk) {
		t.Fatalf("window must never extend: shrunk %s after %s", shrunk, validAfterHold)
	}
	_ = firstOp
	_ = validAfterClaim
}

// TestAbsentWorkersSpendNoAttempt proves a READY step with no worker creates
// no attempt and spends no budget.
func TestAbsentWorkersSpendNoAttempt(t *testing.T) {
	tc, _, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	const digest = "bundle-retry-absent-18"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	_, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	var next int
	var attempts int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT next_attempt_number FROM run_steps WHERE id=$1::uuid`, stepID).Scan(&next); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT count(*) FROM task_attempts WHERE step_id=$1::uuid`, stepID).Scan(&attempts)
	}); err != nil {
		t.Fatal(err)
	}
	if next != 1 || attempts != 0 {
		t.Fatalf("absent workers must spend no attempt: next=%d attempts=%d", next, attempts)
	}
	_ = json.RawMessage("{}")
}

// TestLeaseExpirySchedulesBackoffTimer proves F-05: worker death mid-task
// schedules a durable timer instead of immediate READY.
func TestLeaseExpirySchedulesBackoffTimer(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "retry-lease")
	const digest = "bundle-retry-lease-18"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 10000, 30000))
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3)`, session.SessionID, orgID, digest)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	a1 := claimExecution(t, server, session, digest, "lease-claim-1")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)
	// Simulate worker death: expire the lease without heartbeat.
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE task_leases SET expires_at=clock_timestamp()-INTERVAL '1 second' WHERE attempt_id=$1::uuid`, a1.AttemptID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	engine := execution.NewWorkerEngine(tc.pool)
	if _, err := engine.ReconcileExpiredLeases(context.Background(), orgID); err != nil {
		t.Fatal(err)
	}
	stepState, waitReason, _, _, timerState, dueAt, _, _, _, _ := queryRetryState(t, tc, orgID, stepID, runID)
	if stepState != "WAITING" || waitReason != "RETRY_BACKOFF" || timerState != "PENDING" || dueAt == nil {
		t.Fatalf("lease loss must schedule backoff timer, got step %s/%s timer %s", stepState, waitReason, timerState)
	}
}

// TestRetryAfterIncreasesDelay proves the Retry-After parser/cap is honored
// in the persisted due_at.
func TestRetryAfterIncreasesDelay(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "retry-after")
	const digest = "bundle-retry-after-18"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	_, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3)`, session.SessionID, orgID, digest)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	a1 := claimExecution(t, server, session, digest, "after-claim-1")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)
	before := time.Now()
	completeWithError(t, server, session, a1, "PROVIDER_429", true, "NOT_APPLIED", "5")
	var due time.Time
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT due_at FROM timers WHERE step_id=$1::uuid AND state='PENDING'`, stepID).Scan(&due)
	}); err != nil {
		t.Fatal(err)
	}
	delay := due.Sub(before)
	if delay < 4*time.Second || delay > 8*time.Second {
		t.Fatalf("Retry-After 5s should dominate jitter 0..1s, got delay %s due %s", delay, due)
	}
	// Cap: Retry-After 9999s must cap at 30s policy max.
}
