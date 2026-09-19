package integration_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

// TestCompletionDigestConflictAfterCommit proves F-08's conflict branch:
// after a completion is durably committed, an identical replay is ACKed
// idempotently, but a different result digest for the same attempt is
// deterministically rejected and leaves the committed result authoritative.
func TestCompletionDigestConflictAfterCommit(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "completion-conflict")
	const digest = "bundle-conflict"
	deploymentID := seedExecutionDeployment(t, tc, orgID, envID, digest)
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	assignment := claimExecution(t, server, session, digest, "conflict-claim")
	startNode(t, server, session, assignment.AttemptID, assignment.OwnershipEpoch)

	workerCtx := &worker.WorkerSessionContext{SessionID: session.SessionID, WorkerID: session.WorkerID, OrganizationID: orgID, EnvironmentID: envID, PoolName: "default"}
	engine := execution.NewWorkerEngine(tc.pool)
	completion := worker.CompleteRequestDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: "conflict-complete", WorkerID: session.WorkerID, SessionID: session.SessionID, AttemptID: assignment.AttemptID, OwnershipEpoch: assignment.OwnershipEpoch, Outcome: "SUCCEEDED", Output: map[string]any{"ok": true}}
	completion.ResultDigest, _ = worker.CanonicalCompletionDigest(&completion)
	// Commit once via post-commit-loss pattern without hook: use direct commit.
	engine.SetAfterCompleteCommitHookForTest(func() error { return errors.New("injected postcommit ACK loss") })
	if _, err := engine.Complete(context.Background(), workerCtx, &completion); err == nil {
		t.Fatal("expected injected postcommit failure")
	}
	engine.SetAfterCompleteCommitHookForTest(nil)

	countEffects := func() (terminal, completed, outbox int) {
		t.Helper()
		if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
			return tx.QueryRow(ctx, `SELECT
				(SELECT count(*) FROM task_attempts a JOIN run_steps rs ON rs.id=a.step_id WHERE rs.run_id=$1::uuid AND a.status IN ('SUCCEEDED','FAILED','CANCELLED')),
				(SELECT count(*) FROM run_events WHERE run_id=$1::uuid AND event_type='TASK_COMPLETED'),
				(SELECT count(*) FROM outbox_events WHERE payload->>'runId'=$1::text AND payload->>'eventType'='TASK_COMPLETED')`, runID).Scan(&terminal, &completed, &outbox)
		}); err != nil {
			t.Fatal(err)
		}
		return terminal, completed, outbox
	}
	termBefore, completedBefore, outboxBefore := countEffects()
	if termBefore != 1 || completedBefore != 1 || outboxBefore != 1 {
		t.Fatalf("setup did not commit exactly once: terminal=%d completed=%d outbox=%d", termBefore, completedBefore, outboxBefore)
	}

	// Identical replay is accepted.
	replay, err := engine.Complete(context.Background(), workerCtx, &completion)
	if err != nil || !replay.Accepted {
		t.Fatalf("identical replay rejected: %+v err=%v", replay, err)
	}

	// Different digest must conflict.
	conflict := worker.CompleteRequestDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: "conflict-different", WorkerID: session.WorkerID, SessionID: session.SessionID, AttemptID: assignment.AttemptID, OwnershipEpoch: assignment.OwnershipEpoch, Outcome: "SUCCEEDED", Output: map[string]any{"ok": false}}
	conflict.ResultDigest, _ = worker.CanonicalCompletionDigest(&conflict)
	if _, err := engine.Complete(context.Background(), workerCtx, &conflict); !errors.Is(err, worker.ErrResultConflict) {
		t.Fatalf("different digest was not rejected with RESULT_CONFLICT: err=%v", err)
	}

	termAfter, completedAfter, outboxAfter := countEffects()
	if termAfter != termBefore || completedAfter != completedBefore || outboxAfter != outboxBefore {
		t.Fatalf("conflict changed committed effects: before=%d/%d/%d after=%d/%d/%d", termBefore, completedBefore, outboxBefore, termAfter, completedAfter, outboxAfter)
	}
}
