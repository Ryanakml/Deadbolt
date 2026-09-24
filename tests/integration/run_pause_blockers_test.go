package integration_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/controlplane"
	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

// This file closes the Issue #28 acceptance gaps flagged on PR #79:
// real PAUSING recovery (lease/start-deadline/timeout/run-deadline),
// PAUSED deadline settlement, resume lock-order concurrency, concurrent
// control-action races, reconciliation holds while paused, retry due_at
// preservation, and pause-vs-claim transaction ordering. Every test below
// uses real PostgreSQL transactions and the real worker/control protocol.

func pauseBlockerRunState(t *testing.T, tc *tenantTestContext, orgID, runID string) (status, reason string, rev int64, pauseReq bool) {
	t.Helper()
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		var reasonPtr *string
		if err := tx.QueryRow(ctx, `SELECT status, reason_code, revision, pause_requested FROM runs WHERE id=$1::uuid`, runID).Scan(&status, &reasonPtr, &rev, &pauseReq); err != nil {
			return err
		}
		if reasonPtr != nil {
			reason = *reasonPtr
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return status, reason, rev, pauseReq
}

func pauseBlockerAttemptCount(t *testing.T, tc *tenantTestContext, orgID, runID string) int {
	t.Helper()
	var n int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM task_attempts a
			JOIN run_steps rs ON rs.id=a.step_id AND rs.organization_id=a.organization_id
			WHERE rs.run_id=$1::uuid AND a.organization_id=$2::uuid`, runID, orgID).Scan(&n)
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

func pauseBlockerOpenCases(t *testing.T, tc *tenantTestContext, orgID, runID string) int {
	t.Helper()
	var n int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM reconciliation_cases rc
			JOIN run_steps rs ON rs.id=rc.step_id AND rs.organization_id=rc.organization_id
			WHERE rs.run_id=$1::uuid AND rc.organization_id=$2::uuid AND rc.status='OPEN'`, runID, orgID).Scan(&n)
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

func pauseBlockerStepState(t *testing.T, tc *tenantTestContext, orgID, stepID string) (state, wait string) {
	t.Helper()
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		var waitPtr *string
		if err := tx.QueryRow(ctx, `SELECT state, wait_reason FROM run_steps WHERE id=$1::uuid`, stepID).Scan(&state, &waitPtr); err != nil {
			return err
		}
		if waitPtr != nil {
			wait = *waitPtr
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return state, wait
}

func pauseBlockerPendingTimer(t *testing.T, tc *tenantTestContext, orgID, stepID string) (found bool, dueAt time.Time) {
	t.Helper()
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		err := tx.QueryRow(ctx, `SELECT due_at FROM timers
			WHERE step_id=$1::uuid AND organization_id=$2::uuid AND state='PENDING'`, stepID, orgID).Scan(&dueAt)
		if err != nil {
			return err
		}
		found = true
		return nil
	}); err != nil {
		found = false
	}
	return found, dueAt
}

func pauseBlockerTimerCount(t *testing.T, tc *tenantTestContext, orgID, runID, state string) int {
	t.Helper()
	var n int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM timers WHERE run_id=$1::uuid AND organization_id=$2::uuid AND state=$3`, runID, orgID, state).Scan(&n)
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

func pauseBlockerExpireSQL(t *testing.T, tc *tenantTestContext, orgID, stmt, id string) {
	t.Helper()
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, stmt, id)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func pollAssignmentCount(t *testing.T, server *httptest.Server, session *testWorkerSession, requestID string) int {
	t.Helper()
	var pollResp worker.PollResponseDTO
	status := postWorkerJSON(t, server, "/worker/v1/poll", session.SessionToken, worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: requestID, WorkerID: session.WorkerID,
		SessionID: session.SessionID, AvailableSlots: 2, Pool: "default",
	}, &pollResp)
	if status != http.StatusOK {
		t.Fatalf("poll expected 200, got %d", status)
	}
	return len(pollResp.Assignments)
}

func sweepLeases(t *testing.T, tc *tenantTestContext, orgID string) int {
	t.Helper()
	engine := execution.NewWorkerEngine(tc.pool)
	n, err := engine.ReconcileExpiredLeases(context.Background(), orgID)
	if err != nil {
		t.Fatalf("ReconcileExpiredLeases failed: %v", err)
	}
	return n
}

// seedTwoStepBlockedRun creates a RUNNING run with node-a READY and node-b
// BLOCKED-after-node-a so completion advancement can be observed under pause.
func seedTwoStepBlockedRun(t *testing.T, tc *tenantTestContext, orgID, envID, deploymentID string) (runID, stepA, stepB string) {
	t.Helper()
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
			VALUES ($1,$2,$3,'node-b','BLOCKED') RETURNING id::text`, orgID, envID, runID).Scan(&stepB)
	}); err != nil {
		t.Fatal(err)
	}
	return runID, stepA, stepB
}

func completeAttempt(t *testing.T, server *httptest.Server, session *testWorkerSession, attemptID string, epoch int64, requestID, outcome string, output map[string]any, taskErr *worker.TaskErrorDTO) {
	t.Helper()
	completion := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: requestID,
		WorkerID: session.WorkerID, SessionID: session.SessionID,
		AttemptID: attemptID, OwnershipEpoch: epoch,
		Outcome: outcome, Output: output, Error: taskErr,
	}
	completion.ResultDigest, _ = worker.CanonicalCompletionDigest(&completion)
	var compResp worker.CompleteResponseDTO
	if st := postWorkerJSON(t, server, "/worker/v1/complete", session.SessionToken, completion, &compResp); st != http.StatusOK {
		t.Fatalf("complete expected 200, got %d", st)
	}
}

// TestPausingLeaseExpiryRecoversAndDrains proves Blocker 1: a RUNNING attempt
// whose worker lease expires while the run is PAUSING is reconciled (LOST +
// durable retry), new claims stay blocked, and the run drains to PAUSED
// instead of lingering in PAUSING forever.
func TestPausingLeaseExpiryRecoversAndDrains(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "b1-lease")
	const digest = "bundle-b1-lease-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "b1-lease-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)

	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "b1-lease-dev")
	if status, body := pauseRunHTTP(t, server, token, orgID, runID, runRevision(t, tc, orgID, runID), ""); status != http.StatusOK || body["status"] != "PAUSING" {
		t.Fatalf("pause expected 200 PAUSING, got %d (%v)", status, body)
	}

	// Worker crashes: lease expires while the attempt deadline is still valid.
	pauseBlockerExpireSQL(t, tc, orgID, `UPDATE task_leases SET expires_at = clock_timestamp() - INTERVAL '1 minute' WHERE step_id=$1::uuid`, stepID)

	sweepLeases(t, tc, orgID)

	status, _, _, _ := pauseBlockerRunState(t, tc, orgID, runID)
	if status != "PAUSED" {
		t.Fatalf("run must drain PAUSING->PAUSED after lease recovery, got %s", status)
	}
	if st, wait := pauseBlockerStepState(t, tc, orgID, stepID); st != "WAITING" || wait != "RETRY_BACKOFF" {
		t.Fatalf("step must park WAITING/RETRY_BACKOFF, got %s/%s", st, wait)
	}
	found, _ := pauseBlockerPendingTimer(t, tc, orgID, stepID)
	if !found {
		t.Fatalf("durable retry timer must be recorded while paused")
	}
	if n := pauseBlockerAttemptCount(t, tc, orgID, runID); n != 1 {
		t.Fatalf("recovery must not launch new work while paused: attempts=%d", n)
	}
	if n := pollAssignmentCount(t, server, session, "b1-lease-poll"); n != 0 {
		t.Fatalf("new claims must stay blocked while paused, got %d assignments", n)
	}
}

// TestPausingClaimStartDeadlineRecoversAndDrains proves Blocker 1 for an
// attempt that was claimed before pause but never started: the start
// deadline expiry is reconciled to LOST + durable retry and the run drains.
func TestPausingClaimStartDeadlineRecoversAndDrains(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "b1-startdl")
	const digest = "bundle-b1-startdl-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	// Claim commits BEFORE pause: valid, but never started.
	a1 := claimExecution(t, server, session, digest, "b1-startdl-claim")
	if a1.AttemptID == "" {
		t.Fatalf("expected claim before pause to succeed")
	}

	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "b1-startdl-dev")
	if status, body := pauseRunHTTP(t, server, token, orgID, runID, runRevision(t, tc, orgID, runID), ""); status != http.StatusOK || body["status"] != "PAUSING" {
		t.Fatalf("pause expected 200 PAUSING, got %d (%v)", status, body)
	}

	// Start deadline passes without Start (worker lost before starting).
	pauseBlockerExpireSQL(t, tc, orgID, `UPDATE task_attempts SET claim_start_deadline_at = clock_timestamp() - INTERVAL '1 minute' WHERE step_id=$1::uuid`, stepID)
	pauseBlockerExpireSQL(t, tc, orgID, `UPDATE task_leases SET expires_at = clock_timestamp() - INTERVAL '1 minute' WHERE step_id=$1::uuid`, stepID)

	sweepLeases(t, tc, orgID)

	status, _, _, _ := pauseBlockerRunState(t, tc, orgID, runID)
	if status != "PAUSED" {
		t.Fatalf("run must drain PAUSING->PAUSED after start-deadline recovery, got %s", status)
	}
	found, _ := pauseBlockerPendingTimer(t, tc, orgID, stepID)
	if !found {
		t.Fatalf("durable retry timer must be recorded for the unstarted claim")
	}
	if n := pauseBlockerAttemptCount(t, tc, orgID, runID); n != 1 {
		t.Fatalf("recovery must not launch new work while paused: attempts=%d", n)
	}
}

// TestPausingAttemptTimeoutRecoversAndDrains proves Blocker 1 for a RUNNING
// attempt whose execution deadline passes while PAUSING: TIMED_OUT + durable
// retry, then drain to PAUSED.
func TestPausingAttemptTimeoutRecoversAndDrains(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "b1-timeout")
	const digest = "bundle-b1-timeout-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "b1-timeout-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)

	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "b1-timeout-dev")
	if status, body := pauseRunHTTP(t, server, token, orgID, runID, runRevision(t, tc, orgID, runID), ""); status != http.StatusOK || body["status"] != "PAUSING" {
		t.Fatalf("pause expected 200 PAUSING, got %d (%v)", status, body)
	}

	// Attempt execution deadline passes (long task, worker lease also lapses).
	pauseBlockerExpireSQL(t, tc, orgID, `UPDATE task_attempts SET deadline_at = clock_timestamp() - INTERVAL '1 minute' WHERE step_id=$1::uuid`, stepID)
	pauseBlockerExpireSQL(t, tc, orgID, `UPDATE task_leases SET expires_at = clock_timestamp() - INTERVAL '1 minute' WHERE step_id=$1::uuid`, stepID)

	sweepLeases(t, tc, orgID)

	status, _, _, _ := pauseBlockerRunState(t, tc, orgID, runID)
	if status != "PAUSED" {
		t.Fatalf("run must drain PAUSING->PAUSED after attempt-timeout recovery, got %s", status)
	}
	if st, _ := pauseBlockerStepState(t, tc, orgID, stepID); st != "WAITING" {
		t.Fatalf("step must park WAITING, got %s", st)
	}
	found, _ := pauseBlockerPendingTimer(t, tc, orgID, stepID)
	if !found {
		t.Fatalf("durable retry timer must be recorded after timeout while paused")
	}
}

// TestPausingRunDeadlineFailsRun proves Blocker 1 + §10.2 priority: the run
// deadline expiring while PAUSING with a live attempt terminalizes the run
// (FAILED/RUN_DEADLINE_EXCEEDED) instead of stranding it in PAUSING.
func TestPausingRunDeadlineFailsRun(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "b1-rundeadline")
	const digest = "bundle-b1-rundeadline-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "b1-rundeadline-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)

	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "b1-rundeadline-dev")
	if status, body := pauseRunHTTP(t, server, token, orgID, runID, runRevision(t, tc, orgID, runID), ""); status != http.StatusOK || body["status"] != "PAUSING" {
		t.Fatalf("pause expected 200 PAUSING, got %d (%v)", status, body)
	}

	// Original deadline passes while the attempt is still in flight.
	pauseBlockerExpireSQL(t, tc, orgID, `UPDATE runs SET deadline_at = clock_timestamp() - INTERVAL '1 minute' WHERE id=$1::uuid`, runID)

	sweepLeases(t, tc, orgID)

	status, reason, _, _ := pauseBlockerRunState(t, tc, orgID, runID)
	if status != "FAILED" || reason != "RUN_DEADLINE_EXCEEDED" {
		t.Fatalf("run deadline must terminalize PAUSING run, got %s/%s", status, reason)
	}
	if n := pollAssignmentCount(t, server, session, "b1-rundeadline-poll"); n != 0 {
		t.Fatalf("no work may launch from a deadline-failed run, got %d", n)
	}
}

// TestPausedRunDeadlineSweepAndNoReopen proves Blocker 2: a fully PAUSED run
// (zero active attempts) whose original deadline passes is settled by the
// bounded sweep to FAILED/RUN_DEADLINE_EXCEEDED; resume cannot reopen it and
// a due retry timer cannot fire or reopen work.
func TestPausedRunDeadlineSweepAndNoReopen(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "b2-deadline")
	const digest = "bundle-b2-deadline-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "b2-deadline-dev")

	// Pause before the deadline: QUEUED with no attempts -> PAUSED.
	if status, body := pauseRunHTTP(t, server, token, orgID, runID, runRevision(t, tc, orgID, runID), ""); status != http.StatusOK || body["status"] != "PAUSED" {
		t.Fatalf("pause expected 200 PAUSED, got %d (%v)", status, body)
	}

	// Original deadline passes while fully paused; nothing else observes it.
	pauseBlockerExpireSQL(t, tc, orgID, `UPDATE runs SET deadline_at = clock_timestamp() - INTERVAL '1 minute' WHERE id=$1::uuid`, runID)

	sweepLeases(t, tc, orgID)

	status, reason, _, _ := pauseBlockerRunState(t, tc, orgID, runID)
	if status != "FAILED" || reason != "RUN_DEADLINE_EXCEEDED" {
		t.Fatalf("PAUSED run must fail on deadline, got %s/%s", status, reason)
	}
	if countRunEvents(t, tc, orgID, runID, "RUN_FAILED") < 1 {
		t.Fatalf("expected a RUN_FAILED event for the deadline transition")
	}

	// Resume after the deadline cannot reopen the run. The sweep already
	// terminalized it, so resume observes terminal state.
	rev := runRevision(t, tc, orgID, runID)
	if status, body := resumeRunHTTP(t, server, token, orgID, runID, rev, ""); status != http.StatusConflict || body["code"] != "RUN_TERMINAL" {
		t.Fatalf("resume after deadline must be 409 RUN_TERMINAL, got %d (%v)", status, body)
	}
	if status, _, _, _ := pauseBlockerRunState(t, tc, orgID, runID); status != "FAILED" {
		t.Fatalf("run must stay FAILED after refused resume, got %s", status)
	}

	// A retry timer due after the expired run cannot fire or reopen work.
	engine := execution.NewWorkerEngine(tc.pool)
	if fired, err := engine.FireDueRetryTimers(context.Background(), orgID); err != nil || fired != 0 {
		t.Fatalf("no timer may fire on a deadline-failed run: fired=%d err=%v", fired, err)
	}
	if st, _ := pauseBlockerStepState(t, tc, orgID, stepID); st == "READY" {
		t.Fatalf("expired run step must never become READY")
	}
	if n := pollAssignmentCount(t, server, session, "b2-deadline-poll"); n != 0 {
		t.Fatalf("no work may launch from a deadline-failed run, got %d", n)
	}
}

// TestPausedRetryTimerDiesWithExpiredRun proves the second half of Blocker 2:
// a retry timer recorded while paused is cancelled (not merely guarded) when
// the original deadline passes, so it can never reopen the run.
func TestPausedRetryTimerDiesWithExpiredRun(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "b2-timerdie")
	const digest = "bundle-b2-timerdie-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 5000, 30000))
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "b2-timerdie-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)

	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "b2-timerdie-dev")
	if status, body := pauseRunHTTP(t, server, token, orgID, runID, runRevision(t, tc, orgID, runID), ""); status != http.StatusOK || body["status"] != "PAUSING" {
		t.Fatalf("pause expected 200 PAUSING, got %d (%v)", status, body)
	}

	// Retryable in-flight failure records one durable timer, then drains.
	completeAttempt(t, server, session, a1.AttemptID, a1.OwnershipEpoch, "b2-timerdie-complete",
		"FAILED", nil, &worker.TaskErrorDTO{Code: "TRANSIENT", Message: "hiccup", Retryable: true})
	if status, _, _, _ := pauseBlockerRunState(t, tc, orgID, runID); status != "PAUSED" {
		t.Fatalf("run must drain to PAUSED, got %s", status)
	}
	found, originalDue := pauseBlockerPendingTimer(t, tc, orgID, stepID)
	if !found || originalDue.IsZero() {
		t.Fatalf("expected exactly one PENDING retry timer")
	}

	// Original deadline passes while paused with the timer parked.
	pauseBlockerExpireSQL(t, tc, orgID, `UPDATE runs SET deadline_at = clock_timestamp() - INTERVAL '1 minute' WHERE id=$1::uuid`, runID)
	// Even with the timer already due, the sweep must terminalize, not fire.
	pauseBlockerExpireSQL(t, tc, orgID, `UPDATE timers SET due_at = clock_timestamp() - INTERVAL '1 minute' WHERE step_id=$1::uuid`, stepID)

	sweepLeases(t, tc, orgID)

	status, reason, _, _ := pauseBlockerRunState(t, tc, orgID, runID)
	if status != "FAILED" || reason != "RUN_DEADLINE_EXCEEDED" {
		t.Fatalf("paused run with due timer must fail on deadline, got %s/%s", status, reason)
	}
	if n := pauseBlockerTimerCount(t, tc, orgID, runID, "PENDING"); n != 0 {
		t.Fatalf("pending timers must die with the expired run, PENDING=%d", n)
	}
	engine := execution.NewWorkerEngine(tc.pool)
	if fired, err := engine.FireDueRetryTimers(context.Background(), orgID); err != nil || fired != 0 {
		t.Fatalf("cancelled timer must never fire: fired=%d err=%v", fired, err)
	}
	if n := pollAssignmentCount(t, server, session, "b2-timerdie-poll"); n != 0 {
		t.Fatalf("no work may launch from a deadline-failed run, got %d", n)
	}
}

// TestControlActionsSettleExpiredRunWithoutSweep proves the inline half of
// Blocker 2: when the sweep has not yet observed an expired deadline, the
// control actions themselves settle it. Pause on an expired RUNNING run and
// resume on an expired PAUSED run both fail the run to
// FAILED/RUN_DEADLINE_EXCEEDED and return 409 RUN_DEADLINE_EXCEEDED — the
// original deadline is never reset or extended.
func TestControlActionsSettleExpiredRunWithoutSweep(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	const digest = "bundle-b2-inline-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "b2-inline-dev")

	// Pause side: RUNNING run whose deadline already passed.
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	pauseBlockerExpireSQL(t, tc, orgID, `UPDATE runs SET deadline_at = clock_timestamp() - INTERVAL '1 minute' WHERE id=$1::uuid`, runID)
	rev := runRevision(t, tc, orgID, runID)
	if status, body := pauseRunHTTP(t, server, token, orgID, runID, rev, ""); status != http.StatusConflict || body["code"] != "RUN_DEADLINE_EXCEEDED" {
		t.Fatalf("pause on expired run must be 409 RUN_DEADLINE_EXCEEDED, got %d (%v)", status, body)
	}
	if status, reason, _, _ := pauseBlockerRunState(t, tc, orgID, runID); status != "FAILED" || reason != "RUN_DEADLINE_EXCEEDED" {
		t.Fatalf("pause must settle expired run to FAILED/RUN_DEADLINE_EXCEEDED, got %s/%s", status, reason)
	}

	// Resume side: PAUSED run whose deadline passed with no sweep in between.
	runID2, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	rev2 := runRevision(t, tc, orgID, runID2)
	if status, _ := pauseRunHTTP(t, server, token, orgID, runID2, rev2, ""); status != http.StatusOK {
		t.Fatalf("setup pause expected 200, got %d", status)
	}
	pauseBlockerExpireSQL(t, tc, orgID, `UPDATE runs SET deadline_at = clock_timestamp() - INTERVAL '1 minute' WHERE id=$1::uuid`, runID2)
	rev2 = runRevision(t, tc, orgID, runID2)
	if status, body := resumeRunHTTP(t, server, token, orgID, runID2, rev2, ""); status != http.StatusConflict || body["code"] != "RUN_DEADLINE_EXCEEDED" {
		t.Fatalf("resume on expired run must be 409 RUN_DEADLINE_EXCEEDED, got %d (%v)", status, body)
	}
	if status, reason, _, _ := pauseBlockerRunState(t, tc, orgID, runID2); status != "FAILED" || reason != "RUN_DEADLINE_EXCEEDED" {
		t.Fatalf("resume must settle expired run to FAILED/RUN_DEADLINE_EXCEEDED, got %s/%s", status, reason)
	}
}

// TestResumeVsTerminalCompletionNoDeadlock proves Blocker 3: resume locks
// run → steps in ID order and races safely against task completion. Exactly
// one authoritative outcome wins; terminal state is never reopened and no
// deadlock aborts either transaction.
func TestResumeVsTerminalCompletionNoDeadlock(t *testing.T) {
	for i := 0; i < 3; i++ {
		func() {
			tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
			defer tc.cleanup()
			defer server.Close()

			session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, fmt.Sprintf("b3-race-%d", i))
			const digest = "bundle-b3-race-1"
			deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
			runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
			advertiseDigest(t, tc, orgID, session.SessionID, digest)

			a1 := claimExecution(t, server, session, digest, fmt.Sprintf("b3-race-claim-%d", i))
			startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)

			token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, fmt.Sprintf("b3-race-dev-%d", i))
			rev := runRevision(t, tc, orgID, runID)
			if status, body := pauseRunHTTP(t, server, token, orgID, runID, rev, ""); status != http.StatusOK || body["status"] != "PAUSING" {
				t.Fatalf("pause expected 200 PAUSING, got %d (%v)", status, body)
			}
			resumeRev := runRevision(t, tc, orgID, runID)

			var wg sync.WaitGroup
			statuses := make([]int, 2)
			wg.Add(2)
			go func() {
				defer wg.Done()
				statuses[0], _ = resumeRunHTTP(t, server, token, orgID, runID, resumeRev, "")
			}()
			go func() {
				defer wg.Done()
				completion := worker.CompleteRequestDTO{
					ProtocolVersion: worker.ProtocolVersion, RequestID: fmt.Sprintf("b3-race-complete-%d", i),
					WorkerID: session.WorkerID, SessionID: session.SessionID,
					AttemptID: a1.AttemptID, OwnershipEpoch: a1.OwnershipEpoch,
					Outcome: "SUCCEEDED", Output: map[string]any{"ok": true},
				}
				completion.ResultDigest, _ = worker.CanonicalCompletionDigest(&completion)
				var compResp worker.CompleteResponseDTO
				statuses[1] = postWorkerJSON(t, server, "/worker/v1/complete", session.SessionToken, completion, &compResp)
			}()
			wg.Wait()

			for _, st := range statuses {
				if st != http.StatusOK && st != http.StatusConflict {
					t.Fatalf("race must end 200/409, got %d", st)
				}
			}
			status, _, _, _ := pauseBlockerRunState(t, tc, orgID, runID)
			if status != "SUCCEEDED" {
				t.Fatalf("single-node run must terminalize SUCCEEDED exactly once, got %s", status)
			}
			if n := countRunEvents(t, tc, orgID, runID, "RUN_COMPLETED"); n != 1 {
				t.Fatalf("expected exactly one RUN_COMPLETED event, got %d", n)
			}
		}()
	}
}

// TestResumeVsMultistepCompletionNoDeadlock extends the Blocker 3 concurrency
// proof to multi-step lock acquisition: resume (run → N steps in ID order)
// against completion that advances the DAG. No deadlock, one authoritative
// state, downstream READY preserved for the authoritative owner.
func TestResumeVsMultistepCompletionNoDeadlock(t *testing.T) {
	for i := 0; i < 3; i++ {
		func() {
			tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
			defer tc.cleanup()
			defer server.Close()

			session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, fmt.Sprintf("b3-multi-%d", i))
			const digest = "bundle-b3-multi-1"
			deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, twoStepManifest())
			runID, _, stepB := seedTwoStepBlockedRun(t, tc, orgID, envID, deploymentID)
			advertiseDigest(t, tc, orgID, session.SessionID, digest)

			a1 := claimExecution(t, server, session, digest, fmt.Sprintf("b3-multi-claim-%d", i))
			startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)

			token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, fmt.Sprintf("b3-multi-dev-%d", i))
			if status, body := pauseRunHTTP(t, server, token, orgID, runID, runRevision(t, tc, orgID, runID), ""); status != http.StatusOK || body["status"] != "PAUSING" {
				t.Fatalf("pause expected 200 PAUSING, got %d (%v)", status, body)
			}
			resumeRev := runRevision(t, tc, orgID, runID)

			var wg sync.WaitGroup
			resumeStatus := 0
			completeStatus := 0
			wg.Add(2)
			go func() {
				defer wg.Done()
				resumeStatus, _ = resumeRunHTTP(t, server, token, orgID, runID, resumeRev, "")
			}()
			go func() {
				defer wg.Done()
				completion := worker.CompleteRequestDTO{
					ProtocolVersion: worker.ProtocolVersion, RequestID: fmt.Sprintf("b3-multi-complete-%d", i),
					WorkerID: session.WorkerID, SessionID: session.SessionID,
					AttemptID: a1.AttemptID, OwnershipEpoch: a1.OwnershipEpoch,
					Outcome: "SUCCEEDED", Output: map[string]any{"ok": true},
				}
				completion.ResultDigest, _ = worker.CanonicalCompletionDigest(&completion)
				var compResp worker.CompleteResponseDTO
				completeStatus = postWorkerJSON(t, server, "/worker/v1/complete", session.SessionToken, completion, &compResp)
			}()
			wg.Wait()

			if completeStatus != http.StatusOK {
				t.Fatalf("completion must commit 200, got %d", completeStatus)
			}
			if resumeStatus != http.StatusOK && resumeStatus != http.StatusConflict {
				t.Fatalf("resume must end 200/409, got %d", resumeStatus)
			}
			status, _, _, pauseReq := pauseBlockerRunState(t, tc, orgID, runID)
			// Resume-first: RUNNING/unpaused. Complete-first: PAUSED (drained).
			if status != "RUNNING" && status != "PAUSED" {
				t.Fatalf("run must converge to RUNNING or PAUSED, got %s", status)
			}
			if status == "RUNNING" && pauseReq {
				t.Fatalf("RUNNING run must not retain pause_requested")
			}
			if status == "PAUSED" && !pauseReq {
				t.Fatalf("PAUSED run must retain pause_requested")
			}
			if st, _ := pauseBlockerStepState(t, tc, orgID, stepB); st != "READY" {
				t.Fatalf("downstream step must advance to READY exactly once, got %s", st)
			}
		}()
	}
}

// TestConcurrentPauseCancelSingleWinner proves control-action races with
// expectedRevision: pause vs cancel on the same revision commits exactly one
// winner; the loser gets 409; cancel remains authoritative over pause.
func TestConcurrentPauseCancelSingleWinner(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	const digest = "bundle-f-ppcancel-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")

	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "f-ppcancel-dev")
	rev := runRevision(t, tc, orgID, runID)

	var wg sync.WaitGroup
	var pauseStatus, cancelStatus int
	var pauseBody, cancelBody map[string]any
	wg.Add(2)
	go func() {
		defer wg.Done()
		pauseStatus, pauseBody = pauseRunHTTP(t, server, token, orgID, runID, rev, "")
	}()
	go func() {
		defer wg.Done()
		cancelStatus, cancelBody = cancelRunHTTP(t, server, token, orgID, runID, rev, "")
	}()
	wg.Wait()

	wins := 0
	if pauseStatus == http.StatusOK {
		wins++
	} else if pauseStatus != http.StatusConflict {
		t.Fatalf("pause must end 200/409, got %d (%v)", pauseStatus, pauseBody)
	}
	if cancelStatus == http.StatusOK {
		wins++
	} else if cancelStatus != http.StatusConflict {
		t.Fatalf("cancel must end 200/409, got %d (%v)", cancelStatus, cancelBody)
	}
	if wins != 1 {
		t.Fatalf("exactly one control action must win, pause=%d cancel=%d", pauseStatus, cancelStatus)
	}

	// Cancel is authoritative: the run must converge to CANCELLED.
	if cancelStatus != http.StatusOK {
		fresh := runRevision(t, tc, orgID, runID)
		if status, body := cancelRunHTTP(t, server, token, orgID, runID, fresh, ""); status != http.StatusOK || body["status"] != "CANCELLED" {
			t.Fatalf("cancel over PAUSED must settle CANCELLED, got %d (%v)", status, body)
		}
	}
	if status, _, _, _ := pauseBlockerRunState(t, tc, orgID, runID); status != "CANCELLED" {
		t.Fatalf("run must converge to CANCELLED, got %s", status)
	}
	if n := countRunEvents(t, tc, orgID, runID, "RUN_CANCELLED"); n != 1 {
		t.Fatalf("expected exactly one RUN_CANCELLED event, got %d", n)
	}
	if n := countRunEvents(t, tc, orgID, runID, "RUN_PAUSED") + countRunEvents(t, tc, orgID, runID, "RUN_PAUSING"); n > 1 {
		t.Fatalf("pause must commit at most once, got %d pause events", n)
	}
}

// TestConcurrentPauseResumeNoCorruption proves pause vs resume on the same
// revision cannot corrupt state: no 500s, exactly one RUN_RESUMED, and the
// run converges to the resumed state with pause cleared.
func TestConcurrentPauseResumeNoCorruption(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	const digest = "bundle-f-ppresume-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")

	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "f-ppresume-dev")
	rev := runRevision(t, tc, orgID, runID)
	if status, _ := pauseRunHTTP(t, server, token, orgID, runID, rev, ""); status != http.StatusOK {
		t.Fatalf("setup pause expected 200, got %d", status)
	}
	pausedRev := runRevision(t, tc, orgID, runID)

	var wg sync.WaitGroup
	var resumeStatus, pauseStatus int
	wg.Add(2)
	go func() {
		defer wg.Done()
		resumeStatus, _ = resumeRunHTTP(t, server, token, orgID, runID, pausedRev, "")
	}()
	go func() {
		defer wg.Done()
		// Duplicate pause on the same revision is idempotent while PAUSED.
		pauseStatus, _ = pauseRunHTTP(t, server, token, orgID, runID, pausedRev, "")
	}()
	wg.Wait()

	for _, st := range []int{resumeStatus, pauseStatus} {
		if st != http.StatusOK && st != http.StatusConflict {
			t.Fatalf("control race must end 200/409, got %d", st)
		}
	}
	if n := countRunEvents(t, tc, orgID, runID, "RUN_RESUMED"); n > 1 {
		t.Fatalf("resume must commit at most once, got %d RUN_RESUMED", n)
	}
	// Both orders converge: pause-first stays resumable, resume-first wins.
	// Drive to the resumed state and prove pause is cleared exactly once.
	fresh := runRevision(t, tc, orgID, runID)
	if status, _, _, pauseReq := pauseBlockerRunState(t, tc, orgID, runID); status == "PAUSED" && pauseReq {
		if st, body := resumeRunHTTP(t, server, token, orgID, runID, fresh, ""); st != http.StatusOK || body["status"] == "PAUSED" || body["status"] == "PAUSING" {
			t.Fatalf("final resume must clear pause, got %d (%v)", st, body)
		}
	}
	if status, _, _, pauseReq := pauseBlockerRunState(t, tc, orgID, runID); pauseReq || status == "PAUSED" || status == "PAUSING" {
		t.Fatalf("run must converge out of pause, got %s pause_requested=%v", status, pauseReq)
	}
	if n := countRunEvents(t, tc, orgID, runID, "RUN_RESUMED"); n != 1 {
		t.Fatalf("expected exactly one RUN_RESUMED event, got %d", n)
	}
}

// TestReconciliationHoldWhilePaused proves acceptance D: an ambiguous outcome
// while PAUSING records a real hold (OPEN case, no blind retry, no launch);
// resume recomputes WAITING/RECONCILIATION; only an explicit human decision
// releases the run.
func TestReconciliationHoldWhilePaused(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "d-hold")
	const digest = "bundle-d-hold-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, reconcileManifest(3, 60000))
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "d-hold-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)

	devToken, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "d-hold-dev")
	if status, body := pauseRunHTTP(t, server, devToken, orgID, runID, runRevision(t, tc, orgID, runID), ""); status != http.StatusOK || body["status"] != "PAUSING" {
		t.Fatalf("pause expected 200 PAUSING, got %d (%v)", status, body)
	}

	// Ambiguous failure (reconcile recovery + UNKNOWN effect) while PAUSING.
	completeAttempt(t, server, session, a1.AttemptID, a1.OwnershipEpoch, "d-hold-complete",
		"FAILED", nil, &worker.TaskErrorDTO{Code: "PROVIDER_TIMEOUT", Message: "unknown outcome", Retryable: true, EffectStatus: "UNKNOWN"})

	if status, _, _, _ := pauseBlockerRunState(t, tc, orgID, runID); status != "PAUSED" {
		t.Fatalf("run must drain to PAUSED with the hold recorded, got %s", status)
	}
	if st, wait := pauseBlockerStepState(t, tc, orgID, stepID); st != "WAITING" || wait != "RECONCILIATION" {
		t.Fatalf("step must hold WAITING/RECONCILIATION, got %s/%s", st, wait)
	}
	if n := pauseBlockerOpenCases(t, tc, orgID, runID); n != 1 {
		t.Fatalf("expected exactly one OPEN reconciliation case, got %d", n)
	}
	if n := pauseBlockerAttemptCount(t, tc, orgID, runID); n != 1 {
		t.Fatalf("hold must not blind-retry while paused: attempts=%d", n)
	}
	if n := pollAssignmentCount(t, server, session, "d-hold-poll"); n != 0 {
		t.Fatalf("no customer task may launch from a held run, got %d", n)
	}

	// Resume recomputes the hold; the case stays OPEN for explicit resolution.
	rev := runRevision(t, tc, orgID, runID)
	if status, body := resumeRunHTTP(t, server, devToken, orgID, runID, rev, ""); status != http.StatusOK || body["status"] != "WAITING" || body["reasonCode"] != "RECONCILIATION" {
		t.Fatalf("resume must recompute WAITING/RECONCILIATION, got %d (%v)", status, body)
	}
	if n := pauseBlockerOpenCases(t, tc, orgID, runID); n != 1 {
		t.Fatalf("case must stay OPEN after resume, got %d", n)
	}
	if n := pauseBlockerAttemptCount(t, tc, orgID, runID); n != 1 {
		t.Fatalf("resume must not blind-retry a hold: attempts=%d", n)
	}

	// Explicit human decision releases the run (fail_run here).
	opToken, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleOperator, "d-hold-op")
	caseID, caseRev := openReconciliationCase(t, tc, orgID, stepID)
	if status, body := resolveCaseHTTP(t, server, opToken, orgID, caseID, map[string]any{
		"action": "fail_run", "evidence": "prov-confirmed", "reason": "operator verified provider state", "expectedRevision": caseRev,
	}); status != http.StatusOK {
		t.Fatalf("explicit fail_run must succeed, got %d (%v)", status, body)
	}
	if status, _, _, _ := pauseBlockerRunState(t, tc, orgID, runID); status != "FAILED" {
		t.Fatalf("resolved fail_run must terminalize, got %s", status)
	}
}

// TestPausedRetryDueAtPreservedSingleRefire proves acceptance C end to end:
// one timer, one due_at chosen once and never reset; the timer may become due
// but cannot fire while paused; resume keeps the original due_at and exactly
// one new attempt is created.
func TestPausedRetryDueAtPreservedSingleRefire(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "c-dueat")
	const digest = "bundle-c-dueat-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 5000, 30000))
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "c-dueat-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)

	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "c-dueat-dev")
	if status, body := pauseRunHTTP(t, server, token, orgID, runID, runRevision(t, tc, orgID, runID), ""); status != http.StatusOK || body["status"] != "PAUSING" {
		t.Fatalf("pause expected 200 PAUSING, got %d (%v)", status, body)
	}
	completeAttempt(t, server, session, a1.AttemptID, a1.OwnershipEpoch, "c-dueat-complete",
		"FAILED", nil, &worker.TaskErrorDTO{Code: "TRANSIENT", Message: "hiccup", Retryable: true})

	if status, _, _, _ := pauseBlockerRunState(t, tc, orgID, runID); status != "PAUSED" {
		t.Fatalf("run must drain to PAUSED, got %s", status)
	}
	found, originalDue := pauseBlockerPendingTimer(t, tc, orgID, stepID)
	if !found || originalDue.IsZero() {
		t.Fatalf("expected one PENDING timer with a chosen due_at")
	}
	var timerRows int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM timers WHERE step_id=$1::uuid AND organization_id=$2::uuid`, stepID, orgID).Scan(&timerRows)
	}); err != nil {
		t.Fatal(err)
	}
	if timerRows != 1 {
		t.Fatalf("retry timer must be created exactly once, got %d rows", timerRows)
	}

	// Timer becomes due while paused: still cannot fire.
	pauseBlockerExpireSQL(t, tc, orgID, `UPDATE timers SET due_at = clock_timestamp() - INTERVAL '1 second' WHERE step_id=$1::uuid`, stepID)
	_, forcedDue := pauseBlockerPendingTimer(t, tc, orgID, stepID)
	engine := execution.NewWorkerEngine(tc.pool)
	if fired, err := engine.FireDueRetryTimers(context.Background(), orgID); err != nil || fired != 0 {
		t.Fatalf("timer must not fire while paused: fired=%d err=%v", fired, err)
	}

	// Resume must not touch the stored due_at; the timer fires exactly once.
	rev := runRevision(t, tc, orgID, runID)
	if status, body := resumeRunHTTP(t, server, token, orgID, runID, rev, ""); status != http.StatusOK || body["status"] != "WAITING" || body["reasonCode"] != "RETRY_BACKOFF" {
		t.Fatalf("resume must recompute WAITING/RETRY_BACKOFF, got %d (%v)", status, body)
	}
	found, dueAfterResume := pauseBlockerPendingTimer(t, tc, orgID, stepID)
	if !found {
		t.Fatalf("timer must survive resume as PENDING")
	}
	if !dueAfterResume.Equal(forcedDue) {
		t.Fatalf("resume must never reset due_at: want %v got %v", forcedDue, dueAfterResume)
	}
	// The forced-due value is the only mutation, modeling clock passage; the
	// production path never redraws it. Capture the stored due and prove the
	// fire uses it verbatim instead of recomputing.
	found, storedDue := pauseBlockerPendingTimer(t, tc, orgID, stepID)
	if !found {
		t.Fatalf("timer must still be PENDING before fire")
	}
	if fired, err := engine.FireDueRetryTimers(context.Background(), orgID); err != nil || fired != 1 {
		t.Fatalf("timer must fire exactly once after resume: fired=%d err=%v", fired, err)
	}
	// Firing preserves the stored due_at on the step (no reset).
	var eligibleAt time.Time
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT eligible_at FROM run_steps WHERE id=$1::uuid`, stepID).Scan(&eligibleAt)
	}); err != nil {
		t.Fatal(err)
	}
	if !eligibleAt.Equal(storedDue) {
		t.Fatalf("fire must reuse the stored due_at without reset: want %v got %v", storedDue, eligibleAt)
	}
	// Exactly one new attempt becomes claimable; a second poll finds nothing.
	a2 := claimExecution(t, server, session, digest, "c-dueat-claim-2")
	if a2.AttemptID == "" || a2.AttemptID == a1.AttemptID {
		t.Fatalf("expected exactly one new attempt after refire")
	}
	if n := pauseBlockerAttemptCount(t, tc, orgID, runID); n != 2 {
		t.Fatalf("expected exactly 2 attempts total, got %d", n)
	}
}

// TestPausingDrainDefersNewClaimsUntilResume proves acceptance B on a real
// two-node DAG: the running child finishes and unblocks its dependent while
// paused, no new child starts until resume, then the remaining work executes.
func TestPausingDrainDefersNewClaimsUntilResume(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "b-drain")
	const digest = "bundle-b-drain-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, twoStepManifest())
	runID, _, stepB := seedTwoStepBlockedRun(t, tc, orgID, envID, deploymentID)
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "b-drain-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)

	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "b-drain-dev")
	if status, body := pauseRunHTTP(t, server, token, orgID, runID, runRevision(t, tc, orgID, runID), ""); status != http.StatusOK || body["status"] != "PAUSING" {
		t.Fatalf("pause expected 200 PAUSING, got %d (%v)", status, body)
	}
	if status, _, _, _ := pauseBlockerRunState(t, tc, orgID, runID); status != "PAUSING" {
		t.Fatalf("run must show PAUSING while a child is active")
	}

	// Running child continues and completes after pause was requested.
	completeAttempt(t, server, session, a1.AttemptID, a1.OwnershipEpoch, "b-drain-complete",
		"SUCCEEDED", map[string]any{"ok": true}, nil)

	// Dependent unblocked durably but not launched; run drained to PAUSED.
	if st, _ := pauseBlockerStepState(t, tc, orgID, stepB); st != "READY" {
		t.Fatalf("dependent must advance to READY, got %s", st)
	}
	if status, _, _, _ := pauseBlockerRunState(t, tc, orgID, runID); status != "PAUSED" {
		t.Fatalf("run must drain to PAUSED, got %s", status)
	}
	if n := pollAssignmentCount(t, server, session, "b-drain-poll-paused"); n != 0 {
		t.Fatalf("no new child may start while paused, got %d", n)
	}

	// Resume: remaining eligible work executes to terminal success.
	rev := runRevision(t, tc, orgID, runID)
	if status, body := resumeRunHTTP(t, server, token, orgID, runID, rev, ""); status != http.StatusOK || body["status"] != "RUNNING" {
		t.Fatalf("resume must recompute RUNNING, got %d (%v)", status, body)
	}
	a2 := claimExecution(t, server, session, digest, "b-drain-claim-2")
	startNode(t, server, session, a2.AttemptID, a2.OwnershipEpoch)
	completeAttempt(t, server, session, a2.AttemptID, a2.OwnershipEpoch, "b-drain-complete-2",
		"SUCCEEDED", map[string]any{"ok": true}, nil)
	if status, _, _, _ := pauseBlockerRunState(t, tc, orgID, runID); status != "SUCCEEDED" {
		t.Fatalf("run must reach SUCCEEDED after resume, got %s", status)
	}
}

// TestPauseClaimTransactionOrdering proves acceptance A at the PostgreSQL
// transaction boundary with the real worker protocol, in both orderings:
// an uncommitted-then-committed pause serializes before a blocked claim
// (claim stays blocked), and a committed claim stays valid across a later
// pause (Start allowed, task may finish).
func TestPauseClaimTransactionOrdering(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "a-order")
	const digest = "bundle-a-order-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, twoStepManifest())
	runID, _, _ := seedTwoStepBlockedRun(t, tc, orgID, envID, deploymentID)
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	// Ordering 1: pause commits BEFORE the claim's authoritative revalidation.
	// Hold the run row lock in a real transaction while a real worker poll
	// blocks on it, then commit the pause first.
	ctx := context.Background()
	pauseTx, err := tc.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pauseTx.Exec(ctx, "SELECT set_config('app.current_organization_id', $1, true)", orgID); err != nil {
		t.Fatal(err)
	}
	if _, err := pauseTx.Exec(ctx, `SELECT status FROM runs WHERE id=$1::uuid FOR UPDATE`, runID); err != nil {
		t.Fatal(err)
	}
	pollDone := make(chan int, 1)
	go func() {
		pollDone <- pollAssignmentCount(t, server, session, "a-order-blocked-poll")
	}()
	// Let the poll reach the blocked row lock.
	time.Sleep(500 * time.Millisecond)
	select {
	case n := <-pollDone:
		t.Fatalf("poll must block on the pause transaction's row lock, returned %d", n)
	default:
	}
	if _, err := pauseTx.Exec(ctx, `UPDATE runs SET status='PAUSING', reason_code='PAUSE_REQUESTED',
		pause_requested=true, revision=revision+1, updated_at=clock_timestamp()
		WHERE id=$1::uuid AND organization_id=$2::uuid`, runID, orgID); err != nil {
		t.Fatal(err)
	}
	if err := pauseTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-pollDone:
		if n != 0 {
			t.Fatalf("claim serialized after pause must be blocked, got %d", n)
		}
	case <-time.After(45 * time.Second):
		t.Fatalf("blocked claim never returned after pause commit")
	}

	// Ordering 2: claim commits BEFORE pause. A fresh run: claim via the real
	// protocol, then pause; the in-flight claim stays valid (Start allowed)
	// and the task may finish.
	runID2, _, _ := seedTwoStepBlockedRun(t, tc, orgID, envID, deploymentID)
	a1 := claimExecution(t, server, session, digest, "a-order-claim-2")
	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "a-order-dev")
	if status, body := pauseRunHTTP(t, server, token, orgID, runID2, runRevision(t, tc, orgID, runID2), ""); status != http.StatusOK || body["status"] != "PAUSING" {
		t.Fatalf("pause expected 200 PAUSING, got %d (%v)", status, body)
	}
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)
	completeAttempt(t, server, session, a1.AttemptID, a1.OwnershipEpoch, "a-order-complete-2",
		"SUCCEEDED", map[string]any{"ok": true}, nil)
	if status, _, _, _ := pauseBlockerRunState(t, tc, orgID, runID2); status != "PAUSED" {
		t.Fatalf("claim-before-pause attempt must finish and drain to PAUSED, got %s", status)
	}
}

// TestPauseCommandIdempotencyReplayPastDeadline proves Blocker A Problem 1:
// a completed pause mutation preserves its recorded outcome on replay even
// after the run deadline passes and the run is terminalized.
func TestPauseCommandIdempotencyReplayPastDeadline(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	const digest = "bundle-replay-deadline-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "replay-deadline-dev")

	rev := runRevision(t, tc, orgID, runID)
	idempotencyKey := "idemp-pause-replay-deadline-test"

	// 1. Initial pause succeeds with 200 PAUSED.
	status, body := pauseRunHTTP(t, server, token, orgID, runID, rev, idempotencyKey)
	if status != http.StatusOK || body["status"] != "PAUSED" {
		t.Fatalf("expected 200 PAUSED, got %d (%v)", status, body)
	}
	originalRev := body["revision"]

	// 2. Run deadline passes, and run is settled to FAILED/RUN_DEADLINE_EXCEEDED.
	pauseBlockerExpireSQL(t, tc, orgID, `UPDATE runs SET deadline_at = clock_timestamp() - INTERVAL '1 minute' WHERE id=$1::uuid`, runID)
	sweepLeases(t, tc, orgID)
	st, reason, _, _ := pauseBlockerRunState(t, tc, orgID, runID)
	if st != "FAILED" || reason != "RUN_DEADLINE_EXCEEDED" {
		t.Fatalf("expected run to be FAILED/RUN_DEADLINE_EXCEEDED, got %s/%s", st, reason)
	}

	// 3. Retry identical pause with the SAME idempotency key.
	// Must faithfully replay the recorded 200 outcome without reinterpreting through current run state.
	status2, body2 := pauseRunHTTP(t, server, token, orgID, runID, rev, idempotencyKey)
	if status2 != http.StatusOK {
		t.Fatalf("replayed pause must return 200 OK, got %d (%v)", status2, body2)
	}
	if body2["status"] != "PAUSED" || body2["revision"] != originalRev {
		t.Fatalf("replayed pause must match original outcome, got %v", body2)
	}

	// 4. A fresh pause request with a new idempotency key must be rejected (terminal run).
	status3, body3 := pauseRunHTTP(t, server, token, orgID, runID, rev, "idemp-fresh-pause-key")
	if status3 != http.StatusConflict || body3["code"] != "RUN_TERMINAL" {
		t.Fatalf("fresh pause on terminal run must be 409 RUN_TERMINAL, got %d (%v)", status3, body3)
	}
}

// TestResumeCommandIdempotencyReplayPastDeadline proves Blocker A Problem 1 for resume:
// a completed resume mutation preserves its recorded outcome on replay even
// after the run deadline passes and the run is terminalized.
func TestResumeCommandIdempotencyReplayPastDeadline(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	const digest = "bundle-resume-replay-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "resume-replay-dev")

	// Pause first.
	rev := runRevision(t, tc, orgID, runID)
	if status, _ := pauseRunHTTP(t, server, token, orgID, runID, rev, ""); status != http.StatusOK {
		t.Fatalf("pause setup failed, got %d", status)
	}

	// Resume with explicit idempotency key -> 200 QUEUED.
	resumeRev := runRevision(t, tc, orgID, runID)
	resumeKey := "idemp-resume-replay-deadline-test"
	status, body := resumeRunHTTP(t, server, token, orgID, runID, resumeRev, resumeKey)
	if status != http.StatusOK || body["status"] != "QUEUED" {
		t.Fatalf("expected 200 QUEUED, got %d (%v)", status, body)
	}
	originalRev := body["revision"]

	// Deadline passes and run is swept to FAILED.
	pauseBlockerExpireSQL(t, tc, orgID, `UPDATE runs SET deadline_at = clock_timestamp() - INTERVAL '1 minute' WHERE id=$1::uuid`, runID)
	sweepLeases(t, tc, orgID)
	st, reason, _, _ := pauseBlockerRunState(t, tc, orgID, runID)
	if st != "FAILED" || reason != "RUN_DEADLINE_EXCEEDED" {
		t.Fatalf("expected run to be FAILED/RUN_DEADLINE_EXCEEDED, got %s/%s", st, reason)
	}

	// Retry identical resume with the SAME idempotency key.
	status2, body2 := resumeRunHTTP(t, server, token, orgID, runID, resumeRev, resumeKey)
	if status2 != http.StatusOK {
		t.Fatalf("replayed resume must return 200 OK, got %d (%v)", status2, body2)
	}
	if body2["status"] != "QUEUED" || body2["revision"] != originalRev {
		t.Fatalf("replayed resume must match original outcome, got %v", body2)
	}

	// Fresh resume request must be rejected.
	status3, body3 := resumeRunHTTP(t, server, token, orgID, runID, resumeRev, "idemp-fresh-resume-key")
	if status3 != http.StatusConflict || body3["code"] != "RUN_TERMINAL" {
		t.Fatalf("fresh resume on terminal run must be 409 RUN_TERMINAL, got %d (%v)", status3, body3)
	}
}

// TestFreshPauseDeadlineCrossingBeforeLockTOCTOU proves Blocker A Problem 2:
// deadline revalidation is authoritative under the run lock; if deadline expires
// right before the mutation acquires the lock, pause is rejected (409 RUN_DEADLINE_EXCEEDED)
// and the run is durably settled to FAILED/RUN_DEADLINE_EXCEEDED.
func TestFreshPauseDeadlineCrossingBeforeLockTOCTOU(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()
	ctx := context.Background()

	owner, _ := tenant.NewUUID()
	org, err := tc.service.CreateOrganization(ctx, owner, "TOCTOU Pause Org")
	if err != nil {
		t.Fatal(err)
	}
	project, err := tc.service.CreateProject(ctx, org.ID, "TOCTOU Project")
	if err != nil {
		t.Fatal(err)
	}
	env, err := tc.service.CreateEnvironment(ctx, org.ID, project.ID, tenant.EnvStaging, 2)
	if err != nil {
		t.Fatal(err)
	}

	engine := execution.NewWorkerEngine(tc.pool)
	prodMux := controlplane.BuildMuxWithComponents(tc.authCfg, tc.runtimePool, nil, nil, nil, nil, engine)
	server := httptest.NewServer(prodMux)
	defer server.Close()

	const digest = "bundle-toctou-pause-1"
	deploymentID := seedRetryDeployment(t, tc, org.ID, env.ID, digest, safeManifest(3, 1000, 30000))
	runID, _ := seedExecutionRun(t, tc, org.ID, env.ID, deploymentID, "node-a")
	token, _ := reconcileHumanToken(t, tc, org.ID, tenant.RoleDeveloper, "toctou-pause-dev")

	// Hook simulates deadline crossing right before pauseRunTx locks the run.
	engine.SetBeforePauseLockHookForTest(func(ctx context.Context) error {
		return tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
			_, err := tx.Exec(ctx, `UPDATE runs SET deadline_at = clock_timestamp() - INTERVAL '1 minute' WHERE id=$1::uuid`, runID)
			return err
		})
	})

	rev := runRevision(t, tc, org.ID, runID)
	status, body := pauseRunHTTP(t, server, token, org.ID, runID, rev, "key-toctou-pause-1")
	if status != http.StatusConflict || body["code"] != "RUN_DEADLINE_EXCEEDED" {
		t.Fatalf("pause must be rejected with 409 RUN_DEADLINE_EXCEEDED, got %d (%v)", status, body)
	}

	// Verify run is durably settled to FAILED/RUN_DEADLINE_EXCEEDED in the database.
	runSt, reason, _, _ := pauseBlockerRunState(t, tc, org.ID, runID)
	if runSt != "FAILED" || reason != "RUN_DEADLINE_EXCEEDED" {
		t.Fatalf("run must be settled to FAILED/RUN_DEADLINE_EXCEEDED, got %s/%s", runSt, reason)
	}
	if countRunEvents(t, tc, org.ID, runID, "RUN_FAILED") != 1 {
		t.Fatalf("expected exactly 1 RUN_FAILED event")
	}
}

// TestFreshResumeDeadlineCrossingBeforeLockTOCTOU proves Blocker A Problem 2 for resume:
// if deadline expires right before resumeRunTx acquires the lock, resume is rejected
// (409 RUN_DEADLINE_EXCEEDED), the run is settled, and work cannot reopen past deadline.
func TestFreshResumeDeadlineCrossingBeforeLockTOCTOU(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()
	ctx := context.Background()

	owner, _ := tenant.NewUUID()
	org, err := tc.service.CreateOrganization(ctx, owner, "TOCTOU Resume Org")
	if err != nil {
		t.Fatal(err)
	}
	project, err := tc.service.CreateProject(ctx, org.ID, "TOCTOU Project")
	if err != nil {
		t.Fatal(err)
	}
	env, err := tc.service.CreateEnvironment(ctx, org.ID, project.ID, tenant.EnvStaging, 2)
	if err != nil {
		t.Fatal(err)
	}

	engine := execution.NewWorkerEngine(tc.pool)
	prodMux := controlplane.BuildMuxWithComponents(tc.authCfg, tc.runtimePool, nil, nil, nil, nil, engine)
	server := httptest.NewServer(prodMux)
	defer server.Close()

	const digest = "bundle-toctou-resume-1"
	deploymentID := seedRetryDeployment(t, tc, org.ID, env.ID, digest, safeManifest(3, 1000, 30000))
	runID, stepID := seedExecutionRun(t, tc, org.ID, env.ID, deploymentID, "node-a")
	token, _ := reconcileHumanToken(t, tc, org.ID, tenant.RoleDeveloper, "toctou-resume-dev")

	// Pause cleanly first.
	rev := runRevision(t, tc, org.ID, runID)
	if status, _ := pauseRunHTTP(t, server, token, org.ID, runID, rev, ""); status != http.StatusOK {
		t.Fatalf("setup pause failed, got %d", status)
	}

	// Hook simulates deadline crossing right before resumeRunTx locks the run.
	engine.SetBeforeResumeLockHookForTest(func(ctx context.Context) error {
		return tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
			_, err := tx.Exec(ctx, `UPDATE runs SET deadline_at = clock_timestamp() - INTERVAL '1 minute' WHERE id=$1::uuid`, runID)
			return err
		})
	})

	resumeRev := runRevision(t, tc, org.ID, runID)
	status, body := resumeRunHTTP(t, server, token, org.ID, runID, resumeRev, "key-toctou-resume-1")
	if status != http.StatusConflict || body["code"] != "RUN_DEADLINE_EXCEEDED" {
		t.Fatalf("resume must be rejected with 409 RUN_DEADLINE_EXCEEDED, got %d (%v)", status, body)
	}

	// Verify run is settled and work cannot reopen.
	runSt, reason, _, _ := pauseBlockerRunState(t, tc, org.ID, runID)
	if runSt != "FAILED" || reason != "RUN_DEADLINE_EXCEEDED" {
		t.Fatalf("run must be settled to FAILED/RUN_DEADLINE_EXCEEDED, got %s/%s", runSt, reason)
	}
	if st, _ := pauseBlockerStepState(t, tc, org.ID, stepID); st == "READY" {
		t.Fatalf("step must not reopen to READY past deadline")
	}
}

// TestConcurrentSweepVsPauseResumeDeadlineRace proves that concurrent sweep
// vs pause/resume on an expired run yields a single authoritative terminal result,
// with exactly one terminal event and no work reopening.
func TestConcurrentSweepVsPauseResumeDeadlineRace(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	const digest = "bundle-race-sweep-pause-1"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, safeManifest(3, 1000, 30000))
	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "race-sweep-dev")

	for i := 0; i < 5; i++ {
		runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
		// Expire deadline immediately.
		pauseBlockerExpireSQL(t, tc, orgID, `UPDATE runs SET deadline_at = clock_timestamp() - INTERVAL '1 minute' WHERE id=$1::uuid`, runID)
		rev := runRevision(t, tc, orgID, runID)

		var wg sync.WaitGroup
		var pauseStatus int
		var pauseBody map[string]any

		wg.Add(2)
		go func() {
			defer wg.Done()
			sweepLeases(t, tc, orgID)
		}()
		go func() {
			defer wg.Done()
			pauseStatus, pauseBody = pauseRunHTTP(t, server, token, orgID, runID, rev, fmt.Sprintf("race-key-%d", i))
		}()
		wg.Wait()

		// Pause should either be 409 RUN_DEADLINE_EXCEEDED (if it won) or 409 RUN_TERMINAL (if sweep won).
		if pauseStatus != http.StatusConflict || (pauseBody["code"] != "RUN_DEADLINE_EXCEEDED" && pauseBody["code"] != "RUN_TERMINAL") {
			t.Fatalf("iteration %d: expected 409 RUN_DEADLINE_EXCEEDED or RUN_TERMINAL, got %d (%v)", i, pauseStatus, pauseBody)
		}

		// Run must be settled to FAILED/RUN_DEADLINE_EXCEEDED.
		st, reason, _, _ := pauseBlockerRunState(t, tc, orgID, runID)
		if st != "FAILED" || reason != "RUN_DEADLINE_EXCEEDED" {
			t.Fatalf("iteration %d: run must be FAILED/RUN_DEADLINE_EXCEEDED, got %s/%s", i, st, reason)
		}

		// Exactly one RUN_FAILED event must exist.
		if count := countRunEvents(t, tc, orgID, runID, "RUN_FAILED"); count != 1 {
			t.Fatalf("iteration %d: expected exactly 1 RUN_FAILED event, got %d", i, count)
		}
	}
}
