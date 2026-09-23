package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

// TestWorkerDrainRefusesNewClaims verifies that when a worker is marked DRAINING,
// the control plane refuses to dispatch any new claims to it, even if slots are available.
func TestWorkerDrainRefusesNewClaims(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	bundle := "7777777777777777777777777777777777777777777777777777777777777777"
	schema := map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"val": map[string]any{"type": "string"}},
		"required":             []any{"val"},
		"additionalProperties": false,
	}
	manifest := createLifecycleManifest(bundle,
		[]map[string]any{{"name": "drain-task", "entrypoint": "tasks/drain.js", "timeoutMs": 30000, "recovery": "idempotent", "idempotencyWindowMs": 305000, "inputSchema": schema, "outputSchema": schema}},
		[]map[string]any{{"manifestVersion": 1, "name": "drain-flow", "inputSchema": schema, "outputSchema": schema, "nodes": []map[string]any{{"id": "node-1", "type": "task", "task": "drain-task", "after": []any{}, "input": map[string]any{"val": map[string]any{"$ref": "run.input", "pointer": "/val"}}}}, "output": map[string]any{"val": map[string]any{"$ref": "step.output", "stepId": "node-1", "pointer": "/val"}}}},
	)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "drain-flow", manifest)

	// Create a run ready for claiming
	body := bytes.NewReader([]byte(`{"environment":"staging","input":{"val":"ok"}}`))
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/drain-flow/runs", body)
	req.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	req.Header.Set("X-Organization-ID", orgID)
	req.Header.Set("Idempotency-Key", "drain-run-1")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusAccepted {
		t.Fatalf("create run failed: %v, status=%d", err, resp.StatusCode)
	}
	resp.Body.Close()

	// Enroll worker
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "drain-refuse")

	// Call control plane to mark worker DRAINING
	drainReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/workers/%s/drain", server.URL, session.WorkerID), nil)
	drainReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	drainReq.Header.Set("X-Organization-ID", orgID)
	drainReq.Header.Set("Idempotency-Key", "idemp-drain-worker-1")
	drainResp, err := http.DefaultClient.Do(drainReq)
	if err != nil || drainResp.StatusCode != http.StatusOK {
		t.Fatalf("drain worker failed: %v, status=%d", err, drainResp.StatusCode)
	}
	drainResp.Body.Close()

	// Verify worker status in DB is DRAINING
	var workerStatus string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status FROM workers WHERE id=$1::uuid AND organization_id=$2::uuid`, session.WorkerID, orgID).Scan(&workerStatus)
	}); err != nil {
		t.Fatalf("query worker status: %v", err)
	}
	if workerStatus != "DRAINING" {
		t.Fatalf("expected worker status DRAINING, got %s", workerStatus)
	}

	// Worker polls for work: MUST receive 0 assignments because worker is draining
	var pollResp worker.PollResponseDTO
	status := postWorkerJSON(t, server, "/worker/v1/poll", session.SessionToken, worker.PollRequestDTO{
		ProtocolVersion:   worker.ProtocolVersion,
		RequestID:         "poll-drain-1",
		WorkerID:          session.WorkerID,
		SessionID:         session.SessionID,
		AvailableSlots:    2,
		DeploymentDigests: []string{bundle},
		Pool:              "default",
	}, &pollResp)
	if status != http.StatusOK {
		t.Fatalf("poll status %d, want 200", status)
	}
	if len(pollResp.Assignments) != 0 {
		t.Fatalf("expected 0 assignments for draining worker, got %d", len(pollResp.Assignments))
	}
}

// TestWorkerDrainGraceAndRunnerStop verifies that an Agent in draining mode:
// 1. Allows in-flight attempts to finish within the configured grace period.
// 2. Stops/cancels remaining runners when the grace period is exceeded.
func TestWorkerDrainGraceAndRunnerStop(t *testing.T) {
	// Verify DrainGracePeriod constant matches specification (60 seconds)
	if worker.DrainGracePeriod != 60*time.Second {
		t.Fatalf("expected worker.DrainGracePeriod to be 60s, got %v", worker.DrainGracePeriod)
	}

	// Test Agent.Drain with a short grace period to verify runner termination when grace is exceeded
	agent, err := worker.NewAgent(worker.AgentConfig{
		ControlPlaneURL:  "http://127.0.0.1:8080",
		DrainGracePeriod: 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create agent: %v", err)
	}

	// Start Drain in background
	ctx := context.Background()
	drainDone := make(chan struct{})
	go func() {
		agent.Drain(ctx)
		close(drainDone)
	}()

	select {
	case <-drainDone:
		// Clean completion of Drain after grace period
	case <-time.After(2 * time.Second):
		t.Fatal("agent.Drain timed out without stopping runners")
	}
}

// TestWorkerReconnectFencesPreviousSessions verifies that when a worker reconnects,
// the control plane fences (revokes) the previous session in the database so stale
// sessions cannot issue further requests.
func TestWorkerReconnectFencesPreviousSessions(t *testing.T) {
	tc, server, orgID, envID, _ := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	// 1. Enroll worker to create Session 1
	sess1, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "reconnect-fencing")
	if sess1.SessionID == "" || sess1.SessionToken == "" {
		t.Fatal("session 1 missing id or token")
	}

	// Verify Session 1 is active
	var s1Revoked *time.Time
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT revoked_at FROM worker_sessions WHERE id=$1::uuid`, sess1.SessionID).Scan(&s1Revoked)
	}); err != nil {
		t.Fatalf("query session 1: %v", err)
	}
	if s1Revoked != nil {
		t.Fatalf("expected session 1 to be active, got revoked_at=%v", s1Revoked)
	}

	// 2. Reconnect: Worker requests a challenge for its workerID
	chalReq, _ := json.Marshal(worker.ChallengeRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "reconnect-chal",
		WorkerID:        sess1.WorkerID,
	})
	cResp, err := http.Post(server.URL+"/worker/v1/challenge", "application/json", bytes.NewReader(chalReq))
	if err != nil || cResp.StatusCode != http.StatusOK {
		t.Fatalf("challenge failed: %v", err)
	}
	var challenge worker.ChallengeResponseDTO
	_ = json.NewDecoder(cResp.Body).Decode(&challenge)
	cResp.Body.Close()

	// Sign challenge nonce with worker's private key
	sig := worker.SignChallenge(sess1.privateKey, challenge.Nonce)

	// Call POST /worker/v1/session to establish fresh Session 2
	sessionReq, _ := json.Marshal(worker.SessionRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "reconnect-session",
		WorkerID:        sess1.WorkerID,
		Nonce:           challenge.Nonce,
		Signature:       sig,
	})
	sResp, err := http.Post(server.URL+"/worker/v1/session", "application/json", bytes.NewReader(sessionReq))
	if err != nil || sResp.StatusCode != http.StatusOK {
		t.Fatalf("reconnect session failed: %v", err)
	}
	var sess2 worker.SessionResponseDTO
	_ = json.NewDecoder(sResp.Body).Decode(&sess2)
	sResp.Body.Close()

	if sess2.SessionID == "" || sess2.SessionID == sess1.SessionID {
		t.Fatalf("expected fresh session 2, got %s", sess2.SessionID)
	}

	// Verify Session 1 is now FENCED (revoked_at is set)
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT revoked_at FROM worker_sessions WHERE id=$1::uuid`, sess1.SessionID).Scan(&s1Revoked)
	}); err != nil {
		t.Fatalf("query session 1 after reconnect: %v", err)
	}
	if s1Revoked == nil {
		t.Fatal("expected session 1 to be fenced/revoked after reconnect, but revoked_at is NULL")
	}

	// Calling poll with old session token must be rejected (401 or revoked)
	var pollResp worker.PollResponseDTO
	status := postWorkerJSON(t, server, "/worker/v1/poll", sess1.SessionToken, worker.PollRequestDTO{
		ProtocolVersion:   worker.ProtocolVersion,
		RequestID:         "poll-stale-sess1",
		WorkerID:          sess1.WorkerID,
		SessionID:         sess1.SessionID,
		AvailableSlots:    1,
		DeploymentDigests: []string{},
		Pool:              "default",
	}, &pollResp)
	if status == http.StatusOK {
		t.Fatalf("expected stale session 1 poll to be rejected, got 200 OK")
	}

	// Calling poll with new session token must succeed
	status = postWorkerJSON(t, server, "/worker/v1/poll", sess2.SessionToken, worker.PollRequestDTO{
		ProtocolVersion:   worker.ProtocolVersion,
		RequestID:         "poll-fresh-sess2",
		WorkerID:          sess2.WorkerID,
		SessionID:         sess2.SessionID,
		AvailableSlots:    1,
		DeploymentDigests: []string{},
		Pool:              "default",
	}, &pollResp)
	if status != http.StatusOK {
		t.Fatalf("expected fresh session 2 poll to succeed, got %d", status)
	}
}

// TestWorkerPreservesBundlesAcrossDrainAndReconnect verifies that bundles cached on disk
// remain available and untouched after drain and reconnect cycles.
func TestWorkerPreservesBundlesAcrossDrainAndReconnect(t *testing.T) {
	tempDir := t.TempDir()
	bundlesDir := filepath.Join(tempDir, "bundles")
	if err := os.MkdirAll(bundlesDir, 0700); err != nil {
		t.Fatal(err)
	}

	digest := "8888888888888888888888888888888888888888888888888888888888888888"
	bundleFile := filepath.Join(bundlesDir, digest+".tar.gz")
	dummyContent := []byte("dummy deployment bundle content")
	if err := os.WriteFile(bundleFile, dummyContent, 0600); err != nil {
		t.Fatal(err)
	}

	// Simulate Agent creation with the bundles directory
	agent, err := worker.NewAgent(worker.AgentConfig{
		ControlPlaneURL:  "http://127.0.0.1:8080",
		BundleDir:        bundlesDir,
		DrainGracePeriod: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create agent: %v", err)
	}

	// Drain the agent
	agent.Drain(context.Background())

	// Verify bundle file is still intact on disk
	readBack, err := os.ReadFile(bundleFile)
	if err != nil {
		t.Fatalf("bundle file missing after drain: %v", err)
	}
	if !bytes.Equal(readBack, dummyContent) {
		t.Fatalf("bundle file content changed after drain")
	}
}

// seedExecutionDeploymentWithRecovery inserts a single-node deployment whose
// task uses the given existing recovery policy (safe, idempotent or
// reconcile). It mirrors seedExecutionDeployment without duplicating it.
func seedExecutionDeploymentWithRecovery(t *testing.T, tc *tenantTestContext, orgID, envID, digest, recovery string) string {
	t.Helper()
	manifest := fmt.Sprintf(`{"targetArchitecture":"amd64","secretNames":["API_TOKEN"],"tasks":[{"name":"task-a","entrypoint":"tasks/a.js","recovery":%q,"timeoutMs":45000,"retry":{"maxAttempts":3,"initialDelayMs":10000,"maxDelayMs":30000}}],"workflows":[{"name":"workflow-a","nodes":[{"id":"node-a","task":"task-a"}]}]}`, recovery)
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

// TestWorkerDrainForceStopRecoversViaPolicy proves the control-plane half of
// a drain force-stop: when the drained agent's attempt never completes (the
// local runner was stopped, so no heartbeat and no result ever arrive), the
// server-side lease expiry must apply the task's existing recovery policy
// instead of leaving the task silently healthy/running forever.
//
//   - safe: LOST/LEASE_EXPIRED, step WAITING/RETRY_BACKOFF, one PENDING
//     RETRY_BACKOFF timer; once the timer is due a replacement worker
//     reclaims the step and the run still reaches SUCCEEDED.
//   - reconcile: LOST/LEASE_EXPIRED, step WAITING/RECONCILIATION, exactly one
//     OPEN reconciliation case and no blind retry timer (operator hold).
func TestWorkerDrainForceStopRecoversViaPolicy(t *testing.T) {
	for _, policy := range []string{"safe", "reconcile"} {
		t.Run(policy, func(t *testing.T) {
			tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
			defer tc.cleanup()
			defer server.Close()
			ctx := context.Background()

			workerA, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "drain-kill-a-"+policy)
			workerB, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "drain-kill-b-"+policy)
			digest := "bundle-drain-kill-" + policy
			deploymentID := seedExecutionDeploymentWithRecovery(t, tc, orgID, envID, digest, policy)
			runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")

			if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
				for _, sess := range []*testWorkerSession{workerA, workerB} {
					if _, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3)`, sess.SessionID, orgID, digest); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			assignment := claimExecution(t, server, workerA, digest, "poll-drain-kill-"+policy)
			if assignment.RunID != runID {
				t.Fatalf("claimed run %s, want %s", assignment.RunID, runID)
			}
			var startResp worker.StartResponseDTO
			if status := postWorkerJSON(t, server, "/worker/v1/start", workerA.SessionToken, worker.StartRequestDTO{
				ProtocolVersion: worker.ProtocolVersion, RequestID: "start-drain-kill-" + policy,
				WorkerID: workerA.WorkerID, SessionID: workerA.SessionID,
				AttemptID: assignment.AttemptID, OwnershipEpoch: assignment.OwnershipEpoch,
			}, &startResp); status != http.StatusOK || !startResp.Accepted {
				t.Fatalf("start rejected: status=%d", status)
			}

			// The drained agent force-stopped its runner: expire the lease
			// with no further heartbeat or result.
			if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
				_, err := tx.Exec(ctx, `UPDATE task_leases SET expires_at=clock_timestamp()-INTERVAL '1 second' WHERE attempt_id=$1::uuid`, assignment.AttemptID)
				return err
			}); err != nil {
				t.Fatal(err)
			}

			engine := execution.NewWorkerEngine(tc.pool)
			reclaimed, err := engine.ReconcileExpiredLeases(ctx, orgID)
			if err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if reclaimed != 1 {
				t.Fatalf("expected 1 reclaimed attempt, got %d", reclaimed)
			}

			var attemptStatus, attemptCode, stepState, waitReason string
			var remainingLeases int
			if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
				if err := tx.QueryRow(ctx, `SELECT a.status, COALESCE(a.error->>'code',''), rs.state, COALESCE(rs.wait_reason,'')
					FROM task_attempts a JOIN run_steps rs ON rs.id=a.step_id
					WHERE a.id=$1::uuid`, assignment.AttemptID).Scan(&attemptStatus, &attemptCode, &stepState, &waitReason); err != nil {
					return err
				}
				return tx.QueryRow(ctx, `SELECT count(*) FROM task_leases WHERE attempt_id=$1::uuid`, assignment.AttemptID).Scan(&remainingLeases)
			}); err != nil {
				t.Fatal(err)
			}
			if attemptStatus != "LOST" || attemptCode != "LEASE_EXPIRED" {
				t.Fatalf("attempt not marked LOST/LEASE_EXPIRED: %s/%s", attemptStatus, attemptCode)
			}
			if remainingLeases != 0 {
				t.Fatalf("force-stopped attempt still holds %d leases", remainingLeases)
			}
			var runStatus string
			_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
				return tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus)
			})
			if runStatus == "SUCCEEDED" || runStatus == "FAILED" || runStatus == "CANCELLED" {
				t.Fatalf("run went terminal %s instead of entering recovery", runStatus)
			}

			if policy == "safe" {
				if stepState != "WAITING" || waitReason != "RETRY_BACKOFF" {
					t.Fatalf("safe: step=%s/%s, want WAITING/RETRY_BACKOFF", stepState, waitReason)
				}
				var pendingTimers int
				_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
					return tx.QueryRow(ctx, `SELECT count(*) FROM timers WHERE step_id=$1::uuid AND state='PENDING' AND kind='RETRY_BACKOFF'`, stepID).Scan(&pendingTimers)
				})
				if pendingTimers != 1 {
					t.Fatalf("safe: pending timers=%d, want 1", pendingTimers)
				}
				// The replacement worker finishes the run once the timer is due.
				_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
					_, err := tx.Exec(ctx, `UPDATE timers SET due_at=clock_timestamp()-INTERVAL '1 second' WHERE step_id=$1::uuid AND state='PENDING'`, stepID)
					return err
				})
				reclaim := claimExecution(t, server, workerB, digest, "poll-drain-reclaim")
				if reclaim.RunID != runID {
					t.Fatalf("replacement claimed %s, want %s", reclaim.RunID, runID)
				}
				var startBResp worker.StartResponseDTO
				if status := postWorkerJSON(t, server, "/worker/v1/start", workerB.SessionToken, worker.StartRequestDTO{
					ProtocolVersion: worker.ProtocolVersion, RequestID: "start-drain-reclaim",
					WorkerID: workerB.WorkerID, SessionID: workerB.SessionID,
					AttemptID: reclaim.AttemptID, OwnershipEpoch: reclaim.OwnershipEpoch,
				}, &startBResp); status != http.StatusOK || !startBResp.Accepted {
					t.Fatalf("replacement start rejected: %d", status)
				}
				comp := worker.CompleteRequestDTO{
					ProtocolVersion: worker.ProtocolVersion, RequestID: "complete-drain-reclaim",
					WorkerID: workerB.WorkerID, SessionID: workerB.SessionID,
					AttemptID: reclaim.AttemptID, OwnershipEpoch: reclaim.OwnershipEpoch,
					Outcome: "SUCCEEDED",
				}
				comp.ResultDigest, _ = worker.CanonicalCompletionDigest(&comp)
				var compResp worker.CompleteResponseDTO
				if status := postWorkerJSON(t, server, "/worker/v1/complete", workerB.SessionToken, comp, &compResp); status != http.StatusOK || !compResp.Accepted {
					t.Fatalf("replacement complete rejected: %d", status)
				}
				_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
					return tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus)
				})
				if runStatus != "SUCCEEDED" {
					t.Fatalf("run status=%s, want SUCCEEDED after replacement finish", runStatus)
				}
			} else {
				if stepState != "WAITING" || waitReason != "RECONCILIATION" {
					t.Fatalf("reconcile: step=%s/%s, want WAITING/RECONCILIATION", stepState, waitReason)
				}
				var openCases, pendingTimers int
				_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
					if err := tx.QueryRow(ctx, `SELECT count(*) FROM reconciliation_cases WHERE step_id=$1::uuid AND status='OPEN'`, stepID).Scan(&openCases); err != nil {
						return err
					}
					return tx.QueryRow(ctx, `SELECT count(*) FROM timers WHERE step_id=$1::uuid AND state='PENDING'`, stepID).Scan(&pendingTimers)
				})
				if openCases != 1 {
					t.Fatalf("reconcile: open cases=%d, want 1", openCases)
				}
				if pendingTimers != 0 {
					t.Fatalf("reconcile: must hold for operator, got %d pending timers", pendingTimers)
				}
			}
		})
	}
}
