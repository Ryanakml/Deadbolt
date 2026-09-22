package integration_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

type testWorkerSession struct {
	worker.SessionResponseDTO
	privateKey ed25519.PrivateKey
}

// TestConcurrentAuthenticatedClaimsHaveOneAuthority races two real persisted
// worker sessions against a single READY step through the PostgreSQL engine.
func TestConcurrentAuthenticatedClaimsHaveOneAuthority(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	first, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "concurrent-claim-a")
	second, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "concurrent-claim-b")
	const digest = "bundle-concurrent-claim"
	deploymentID := seedExecutionDeployment(t, tc, orgID, envID, digest)
	_, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		for _, session := range []*testWorkerSession{first, second} {
			if _, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3)`, session.SessionID, orgID, digest); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	engine := execution.NewWorkerEngine(tc.pool)
	type result struct {
		assignments int
		err         error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for index, session := range []*testWorkerSession{first, second} {
		wg.Add(1)
		go func(index int, session *testWorkerSession) {
			defer wg.Done()
			<-start
			response, err := engine.Claim(context.Background(), &worker.WorkerSessionContext{SessionID: session.SessionID, WorkerID: session.WorkerID, OrganizationID: orgID, EnvironmentID: envID, PoolName: "default"}, &worker.PollRequestDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: fmt.Sprintf("concurrent-%d", index), WorkerID: session.WorkerID, SessionID: session.SessionID, AvailableSlots: 1, DeploymentDigests: []string{digest}, Pool: "default"})
			if err != nil {
				results <- result{err: err}
				return
			}
			results <- result{assignments: len(response.Assignments)}
		}(index, session)
	}
	close(start)
	wg.Wait()
	close(results)
	claimed := 0
	for outcome := range results {
		if outcome.err != nil {
			t.Fatalf("concurrent claim failed: %v", outcome.err)
		}
		claimed += outcome.assignments
	}
	if claimed != 1 {
		t.Fatalf("expected exactly one concurrent claim, got %d", claimed)
	}
	var attempts, leases, active int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM task_attempts WHERE step_id=$1::uuid),
			(SELECT count(*) FROM task_leases WHERE step_id=$1::uuid),
			(SELECT count(*) FROM task_attempts WHERE step_id=$1::uuid AND status IN ('CLAIMED','RUNNING'))`, stepID).Scan(&attempts, &leases, &active)
	}); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || leases != 1 || active != 1 {
		t.Fatalf("duplicate claim authority: attempts=%d leases=%d active=%d", attempts, leases, active)
	}
}

// TestCompletionPreCommitFailureRollsBack proves a database failure after the
// authoritative transition starts cannot leak a partial completion; the exact
// result is safe to retry once the dependency recovers.
func TestCompletionPreCommitFailureRollsBack(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "completion-precommit")
	const digest = "bundle-precommit"
	deploymentID := seedExecutionDeployment(t, tc, orgID, envID, digest)
	_, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	assignment := claimExecution(t, server, session, digest, "precommit-claim")
	startNode(t, server, session, assignment.AttemptID, assignment.OwnershipEpoch)
	completion := worker.CompleteRequestDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: "precommit-complete", WorkerID: session.WorkerID, SessionID: session.SessionID, AttemptID: assignment.AttemptID, OwnershipEpoch: assignment.OwnershipEpoch, Outcome: "SUCCEEDED", Output: map[string]any{"ok": true}}
	completion.ResultDigest, _ = worker.CanonicalCompletionDigest(&completion)
	engine := execution.NewWorkerEngine(tc.pool)
	engine.SetBeforeCompleteCommitHookForTest(func() error { return errors.New("injected precommit failure") })
	workerContext := &worker.WorkerSessionContext{SessionID: session.SessionID, WorkerID: session.WorkerID, OrganizationID: orgID, EnvironmentID: envID, PoolName: "default"}
	if _, err := engine.Complete(context.Background(), workerContext, &completion); err == nil {
		t.Fatal("expected injected precommit failure")
	}
	var attemptStatus, stepState string
	var leases, completions int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT a.status,rs.state,(SELECT count(*) FROM task_leases WHERE attempt_id=a.id),(SELECT count(*) FROM run_events WHERE run_id=rs.run_id AND event_type='TASK_COMPLETED') FROM task_attempts a JOIN run_steps rs ON rs.id=a.step_id WHERE a.id=$1::uuid AND rs.id=$2::uuid`, assignment.AttemptID, stepID).Scan(&attemptStatus, &stepState, &leases, &completions)
	}); err != nil {
		t.Fatal(err)
	}
	if attemptStatus != "RUNNING" || stepState != "RUNNING" || leases != 1 || completions != 0 {
		t.Fatalf("precommit failure leaked state: attempt=%s step=%s leases=%d completions=%d", attemptStatus, stepState, leases, completions)
	}
	engine.SetBeforeCompleteCommitHookForTest(nil)
	accepted, err := engine.Complete(context.Background(), workerContext, &completion)
	if err != nil || !accepted.Accepted {
		t.Fatalf("retry after precommit failure rejected: response=%+v err=%v", accepted, err)
	}
}

// TestCompletionPostCommitFailureReplaysWithoutDuplicateEffects proves the
// caller can lose an ACK after the database commits and safely resend the exact
// completion without creating another terminal transition or outbox emission.
func TestCompletionPostCommitFailureReplaysWithoutDuplicateEffects(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "completion-postcommit")
	const digest = "bundle-postcommit"
	deploymentID := seedExecutionDeployment(t, tc, orgID, envID, digest)
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	assignment := claimExecution(t, server, session, digest, "postcommit-claim")
	startNode(t, server, session, assignment.AttemptID, assignment.OwnershipEpoch)
	completion := worker.CompleteRequestDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: "postcommit-complete", WorkerID: session.WorkerID, SessionID: session.SessionID, AttemptID: assignment.AttemptID, OwnershipEpoch: assignment.OwnershipEpoch, Outcome: "SUCCEEDED", Output: map[string]any{"ok": true}}
	completion.ResultDigest, _ = worker.CanonicalCompletionDigest(&completion)
	engine := execution.NewWorkerEngine(tc.pool)
	engine.SetAfterCompleteCommitHookForTest(func() error { return errors.New("injected postcommit ACK loss") })
	workerContext := &worker.WorkerSessionContext{SessionID: session.SessionID, WorkerID: session.WorkerID, OrganizationID: orgID, EnvironmentID: envID, PoolName: "default"}
	if _, err := engine.Complete(context.Background(), workerContext, &completion); err == nil {
		t.Fatal("expected injected postcommit failure")
	}

	assertPostCommitEffects := func() {
		t.Helper()
		var terminalAttempts, releasedLeases, taskCompleted, runCompleted, taskOutbox, runOutbox int
		if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
			return tx.QueryRow(ctx, `SELECT
				(SELECT count(*) FROM task_attempts a JOIN run_steps rs ON rs.id=a.step_id WHERE rs.run_id=$1::uuid AND a.status IN ('SUCCEEDED','FAILED','CANCELLED')),
				(SELECT count(*) FROM task_leases l JOIN task_attempts a ON a.id=l.attempt_id JOIN run_steps rs ON rs.id=a.step_id WHERE rs.run_id=$1::uuid),
				(SELECT count(*) FROM run_events WHERE run_id=$1::uuid AND event_type='TASK_COMPLETED'),
				(SELECT count(*) FROM run_events WHERE run_id=$1::uuid AND event_type='RUN_COMPLETED'),
				(SELECT count(*) FROM outbox_events WHERE payload->>'runId'=$1::text AND payload->>'eventType'='TASK_COMPLETED'),
				(SELECT count(*) FROM outbox_events WHERE payload->>'runId'=$1::text AND payload->>'eventType'='RUN_COMPLETED')`, runID).Scan(&terminalAttempts, &releasedLeases, &taskCompleted, &runCompleted, &taskOutbox, &runOutbox)
		}); err != nil {
			t.Fatal(err)
		}
		if terminalAttempts != 1 || releasedLeases != 0 || taskCompleted != 1 || runCompleted != 1 || taskOutbox != 1 || runOutbox != 1 {
			t.Fatalf("postcommit replay duplicated durable effects: terminalAttempts=%d leases=%d taskCompleted=%d runCompleted=%d taskOutbox=%d runOutbox=%d", terminalAttempts, releasedLeases, taskCompleted, runCompleted, taskOutbox, runOutbox)
		}
	}
	assertPostCommitEffects()

	engine.SetAfterCompleteCommitHookForTest(nil)
	accepted, err := engine.Complete(context.Background(), workerContext, &completion)
	if err != nil || !accepted.Accepted {
		t.Fatalf("replay after postcommit failure rejected: response=%+v err=%v", accepted, err)
	}
	assertPostCommitEffects()
}

func enrollExecutionWorker(t *testing.T, tc *tenantTestContext, server *httptest.Server, orgID, envID, suffix string) (*testWorkerSession, *tenant.GeneratedKey) {
	t.Helper()
	adminKey := bootstrapTestKey(t, tc.service, orgID, envID, []string{tenant.CapDeploymentsWrite, tenant.CapWorkersDrain, tenant.CapAdminKey})
	body, _ := json.Marshal(map[string]any{"poolName": "default"})
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/environments/%s/worker-enrollments", server.URL, envID), bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	req.Header.Set("X-Organization-ID", orgID)
	req.Header.Set("Idempotency-Key", "worker-adversarial-enroll-"+suffix)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create enrollment: status %d", resp.StatusCode)
	}
	var token worker.EnrollmentTokenInfo
	if err := json.NewDecoder(resp.Body).Decode(&token); err != nil {
		t.Fatal(err)
	}

	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	challengeBody, _ := json.Marshal(worker.ChallengeRequestDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: "challenge-" + suffix, PublicKey: hex.EncodeToString(publicKey)})
	challengeResp, err := http.Post(server.URL+"/worker/v1/challenge", "application/json", bytes.NewReader(challengeBody))
	if err != nil {
		t.Fatal(err)
	}
	defer challengeResp.Body.Close()
	var challenge worker.ChallengeResponseDTO
	if err := json.NewDecoder(challengeResp.Body).Decode(&challenge); err != nil {
		t.Fatal(err)
	}

	enrollBody, _ := json.Marshal(worker.EnrollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "enroll-" + suffix,
		EnrollmentToken: token.Token, Nonce: challenge.Nonce,
		Signature: worker.SignChallenge(privateKey, challenge.Nonce), PublicKey: hex.EncodeToString(publicKey),
	})
	enrollResp, err := http.Post(server.URL+"/worker/v1/enroll", "application/json", bytes.NewReader(enrollBody))
	if err != nil {
		t.Fatal(err)
	}
	defer enrollResp.Body.Close()
	if enrollResp.StatusCode != http.StatusOK {
		t.Fatalf("enroll: status %d", enrollResp.StatusCode)
	}
	var session worker.SessionResponseDTO
	if err := json.NewDecoder(enrollResp.Body).Decode(&session); err != nil {
		t.Fatal(err)
	}
	return &testWorkerSession{SessionResponseDTO: session, privateKey: privateKey}, adminKey
}

func seedExecutionDeployment(t *testing.T, tc *tenantTestContext, orgID, envID, digest string) string {
	t.Helper()
	manifest := `{"targetArchitecture":"amd64","secretNames":["API_TOKEN"],"tasks":[{"name":"task-a","entrypoint":"tasks/a.js","recovery":"safe","timeoutMs":45000,"retry":{"maxAttempts":3,"initialDelayMs":10000,"maxDelayMs":30000}}],"workflows":[{"name":"workflow-a","nodes":[{"id":"node-a","task":"task-a"}]}]}`
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

func seedExecutionRun(t *testing.T, tc *tenantTestContext, orgID, envID, deploymentID, nodeID string) (string, string) {
	t.Helper()
	var runID, stepID string
	err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `INSERT INTO runs
			(organization_id,environment_id,deployment_id,workflow_name,status)
			VALUES ($1,$2,$3,'workflow-a','QUEUED') RETURNING id::text`, orgID, envID, deploymentID).Scan(&runID); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `INSERT INTO run_steps
			(organization_id,environment_id,run_id,node_id,state,eligible_at)
			VALUES ($1,$2,$3,$4,'READY',clock_timestamp()) RETURNING id::text`, orgID, envID, runID, nodeID).Scan(&stepID)
	})
	if err != nil {
		t.Fatal(err)
	}
	return runID, stepID
}

func postWorkerJSON(t *testing.T, server *httptest.Server, path, token string, requestBody, responseBody any) int {
	t.Helper()
	body, err := json.Marshal(requestBody)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodPost, server.URL+path, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if responseBody != nil {
		if err := json.NewDecoder(resp.Body).Decode(responseBody); err != nil {
			t.Fatalf("decode %s response: %v", path, err)
		}
	}
	return resp.StatusCode
}

func claimExecution(t *testing.T, server *httptest.Server, session *testWorkerSession, digest, requestID string) worker.AssignmentDTO {
	t.Helper()
	var response worker.PollResponseDTO
	status := postWorkerJSON(t, server, "/worker/v1/poll", session.SessionToken, worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: requestID, WorkerID: session.WorkerID,
		SessionID: session.SessionID, AvailableSlots: 1, DeploymentDigests: []string{digest}, Pool: "default",
	}, &response)
	if status != http.StatusOK || len(response.Assignments) != 1 {
		t.Fatalf("claim status=%d assignments=%d", status, len(response.Assignments))
	}
	return response.Assignments[0]
}

func TestWorkerDBTimeBoundariesAndHeartbeatRequiresStart(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "db-time")
	deploymentID := seedExecutionDeployment(t, tc, orgID, envID, "bundle-db-time")
	_, _ = seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	assignment := claimExecution(t, server, session, "bundle-db-time", "claim-db-time")
	if assignment.TaskEntrypoint != "tasks/a.js" || assignment.AttemptTimeoutMs != 45000 || len(assignment.SecretNames) != 1 {
		t.Fatalf("assignment did not use persisted manifest policy: %+v", assignment)
	}

	var originalLease time.Time
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT expires_at FROM task_leases WHERE attempt_id=$1::uuid`, assignment.AttemptID).Scan(&originalLease)
	}); err != nil {
		t.Fatal(err)
	}
	var heartbeat worker.HeartbeatResponseDTO
	status := postWorkerJSON(t, server, "/worker/v1/heartbeat", session.SessionToken, worker.HeartbeatRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "heartbeat-before-start", WorkerID: session.WorkerID, SessionID: session.SessionID,
		Attempts: []worker.HeartbeatAttemptDTO{{AttemptID: assignment.AttemptID, OwnershipEpoch: assignment.OwnershipEpoch}},
	}, &heartbeat)
	if status != http.StatusOK || len(heartbeat.Renewals) != 0 || len(heartbeat.Stops) != 1 || heartbeat.Stops[0].Reason != "ATTEMPT_NOT_RUNNING" {
		t.Fatalf("heartbeat-before-Start was not fenced: status=%d response=%+v", status, heartbeat)
	}
	var unchangedLease time.Time
	_ = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT expires_at FROM task_leases WHERE attempt_id=$1::uuid`, assignment.AttemptID).Scan(&unchangedLease)
	})
	if !unchangedLease.Equal(originalLease) {
		t.Fatalf("heartbeat-before-Start renewed lease: before=%s after=%s", originalLease, unchangedLease)
	}

	startRequest := worker.StartRequestDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: "start-boundary", WorkerID: session.WorkerID,
		SessionID: session.SessionID, AttemptID: assignment.AttemptID, OwnershipEpoch: assignment.OwnershipEpoch}
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE task_attempts SET claim_start_deadline_at=clock_timestamp() WHERE id=$1::uuid`, assignment.AttemptID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE task_leases SET expires_at=clock_timestamp()+INTERVAL '30 seconds' WHERE attempt_id=$1::uuid`, assignment.AttemptID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if status := postWorkerJSON(t, server, "/worker/v1/start", session.SessionToken, startRequest, &worker.ErrorEnvelopeDTO{}); status != http.StatusConflict {
		t.Fatalf("Start accepted at DB deadline boundary: status=%d", status)
	}

	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE task_attempts SET claim_start_deadline_at=clock_timestamp()+INTERVAL '5 seconds' WHERE id=$1::uuid`, assignment.AttemptID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE task_leases SET expires_at=clock_timestamp()+INTERVAL '30 seconds' WHERE attempt_id=$1::uuid`, assignment.AttemptID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var start worker.StartResponseDTO
	startRequest.RequestID = "start-accepted"
	if status := postWorkerJSON(t, server, "/worker/v1/start", session.SessionToken, startRequest, &start); status != http.StatusOK || !start.Accepted {
		t.Fatalf("valid Start rejected: status=%d response=%+v", status, start)
	}

	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE task_leases SET expires_at=clock_timestamp() WHERE attempt_id=$1::uuid`, assignment.AttemptID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	heartbeat = worker.HeartbeatResponseDTO{}
	if status := postWorkerJSON(t, server, "/worker/v1/heartbeat", session.SessionToken, worker.HeartbeatRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "heartbeat-boundary", WorkerID: session.WorkerID, SessionID: session.SessionID,
		Attempts: []worker.HeartbeatAttemptDTO{{AttemptID: assignment.AttemptID, OwnershipEpoch: assignment.OwnershipEpoch}},
	}, &heartbeat); status != http.StatusOK || len(heartbeat.Renewals) != 0 || len(heartbeat.Stops) != 1 {
		t.Fatalf("heartbeat accepted expired DB lease: status=%d response=%+v", status, heartbeat)
	}

	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE task_leases SET expires_at=clock_timestamp()+INTERVAL '30 seconds' WHERE attempt_id=$1::uuid`, assignment.AttemptID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE task_attempts SET deadline_at=clock_timestamp() WHERE id=$1::uuid`, assignment.AttemptID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	completion := worker.CompleteRequestDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: "complete-boundary", WorkerID: session.WorkerID,
		SessionID: session.SessionID, AttemptID: assignment.AttemptID, OwnershipEpoch: assignment.OwnershipEpoch,
		Outcome: "SUCCEEDED", ResultDigest: "digest-boundary"}
	if status := postWorkerJSON(t, server, "/worker/v1/complete", session.SessionToken, completion, &worker.ErrorEnvelopeDTO{}); status != http.StatusConflict {
		t.Fatalf("Complete accepted at DB attempt deadline boundary: status=%d", status)
	}
}

func TestWorkerDrainRejectsNewAssignments(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, adminKey := enrollExecutionWorker(t, tc, server, orgID, envID, "drain-claim")
	deploymentID := seedExecutionDeployment(t, tc, orgID, envID, "bundle-drain")
	_, _ = seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/workers/%s/drain", server.URL, session.WorkerID), nil)
	req.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	req.Header.Set("X-Organization-ID", orgID)
	req.Header.Set("Idempotency-Key", "drain-before-claim")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("drain status=%d", resp.StatusCode)
	}
	var poll worker.PollResponseDTO
	status := postWorkerJSON(t, server, "/worker/v1/poll", session.SessionToken, worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "poll-draining", WorkerID: session.WorkerID, SessionID: session.SessionID,
		AvailableSlots: 1, DeploymentDigests: []string{"bundle-drain"}, Pool: "default",
	}, &poll)
	if status != http.StatusOK || len(poll.Assignments) != 0 {
		t.Fatalf("draining worker received assignment: status=%d response=%+v", status, poll)
	}
	var attempts int
	_ = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM task_attempts`).Scan(&attempts)
	})
	if attempts != 0 {
		t.Fatalf("draining poll created %d attempts", attempts)
	}
}

func TestWorkerClaimEnforcesEnvironmentConcurrencyQuota(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "claim-quota")
	deploymentID := seedExecutionDeployment(t, tc, orgID, envID, "bundle-quota")
	for index := 0; index < 3; index++ {
		_, _ = seedExecutionRun(t, tc, orgID, envID, deploymentID, fmt.Sprintf("node-%d", index))
	}
	var poll worker.PollResponseDTO
	status := postWorkerJSON(t, server, "/worker/v1/poll", session.SessionToken, worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "poll-over-quota", WorkerID: session.WorkerID, SessionID: session.SessionID,
		AvailableSlots: 3, DeploymentDigests: []string{"bundle-quota"}, Pool: "default",
	}, &poll)
	if status != http.StatusOK || len(poll.Assignments) != 2 {
		t.Fatalf("environment quota was not enforced: status=%d assignments=%d", status, len(poll.Assignments))
	}
	var attempts, ready int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM task_attempts),
			(SELECT count(*) FROM run_steps WHERE state='READY')`).Scan(&attempts, &ready)
	}); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || ready != 1 {
		t.Fatalf("quota claim persisted wrong state: attempts=%d ready=%d", attempts, ready)
	}
}

func TestWorkerReconnectCreatesDurableRecoveryHandoff(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "reconnect-recovery")
	deploymentID := seedExecutionDeployment(t, tc, orgID, envID, "bundle-recovery")
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	assignment := claimExecution(t, server, session, "bundle-recovery", "claim-before-reconnect")

	challengeBody, _ := json.Marshal(worker.ChallengeRequestDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: "reconnect-challenge", WorkerID: session.WorkerID})
	challengeResp, err := http.Post(server.URL+"/worker/v1/challenge", "application/json", bytes.NewReader(challengeBody))
	if err != nil {
		t.Fatal(err)
	}
	var challenge worker.ChallengeResponseDTO
	_ = json.NewDecoder(challengeResp.Body).Decode(&challenge)
	challengeResp.Body.Close()
	sessionBody, _ := json.Marshal(worker.SessionRequestDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: "reconnect-session",
		WorkerID: session.WorkerID, Nonce: challenge.Nonce, Signature: worker.SignChallenge(session.privateKey, challenge.Nonce)})
	newSessionResp, err := http.Post(server.URL+"/worker/v1/session", "application/json", bytes.NewReader(sessionBody))
	if err != nil {
		t.Fatal(err)
	}
	if newSessionResp.StatusCode != http.StatusOK {
		var responseError worker.ErrorEnvelopeDTO
		_ = json.NewDecoder(newSessionResp.Body).Decode(&responseError)
		newSessionResp.Body.Close()
		t.Fatalf("reconnect status=%d error=%+v", newSessionResp.StatusCode, responseError)
	}
	newSessionResp.Body.Close()

	var attemptStatus, stepState, waitReason, runStatus, reasonCode string
	var leases, events, outbox int
	err = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT a.status,rs.state,rs.wait_reason,r.status,r.reason_code,
			(SELECT count(*) FROM task_leases l WHERE l.attempt_id=a.id),
			(SELECT count(*) FROM run_events re WHERE re.run_id=r.id AND re.event_type='TASK_LOST'),
			(SELECT count(*) FROM outbox_events oe WHERE oe.subject='execution.state_changed')
			FROM task_attempts a JOIN run_steps rs ON rs.id=a.step_id JOIN runs r ON r.id=rs.run_id
			WHERE a.id=$1::uuid AND rs.id=$2::uuid AND r.id=$3::uuid`, assignment.AttemptID, stepID, runID).Scan(
			&attemptStatus, &stepState, &waitReason, &runStatus, &reasonCode, &leases, &events, &outbox)
	})
	if err != nil {
		t.Fatal(err)
	}
	if attemptStatus != "LOST" || stepState != "WAITING" || waitReason != "RECOVERY_HANDOFF" || runStatus != "WAITING" || reasonCode != "RECOVERY_HANDOFF" || leases != 0 || events != 1 || outbox < 2 {
		t.Fatalf("incomplete recovery handoff: attempt=%s step=%s/%s run=%s/%s leases=%d events=%d outbox=%d",
			attemptStatus, stepState, waitReason, runStatus, reasonCode, leases, events, outbox)
	}
}

// TestWorkerReconnectRecoveryEndToEndSchedulable is the regression test for the
// REAL persisted state that deadlocked in manual acceptance after commit d206b29.
// It exercises the FenceWorkerSessions code path (worker reconnect), NOT the
// reconcileExpiredLeasesTx path (reconciler sweep), because the deadlock was in
// FenceWorkerSessions setting both step and run to WAITING/RECOVERY_HANDOFF — a
// state excluded from the Claim candidate query (rs.state='READY' AND r.status
// IN ('QUEUED','RUNNING')).
//
// The test proves that after reconnect:
//  1. The unstarted claim is durably invalidated (LOST)
//  2. The step is requeued to READY (not WAITING/RECOVERY_HANDOFF)
//  3. The run remains schedulable (QUEUED, not WAITING)
//  4. A second worker can claim replacement Attempt 2 with Epoch 2
//  5. Stale epoch-1 Start/Heartbeat/Complete from Worker A are rejected
//  6. The replacement attempt can execute to completion (SUCCEEDED)
//  7. No RECOVERY_HANDOFF reason_code exists anywhere in the final state
func TestWorkerReconnectRecoveryEndToEndSchedulable(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	workerA, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "reconnect-e2e-a")
	workerB, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "reconnect-e2e-b")

	const digest = "bundle-reconnect-e2e"
	deploymentID := seedExecutionDeployment(t, tc, orgID, envID, digest)
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")

	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		for _, s := range []*testWorkerSession{workerA, workerB} {
			if _, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3)`, s.SessionID, orgID, digest); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// 1. Worker A claims Attempt 1, Epoch 1 — does NOT send Start
	assignmentA := claimExecution(t, server, workerA, digest, "poll-reconnect-a")
	if assignmentA.AttemptID == "" || assignmentA.OwnershipEpoch != 1 {
		t.Fatalf("expected attempt 1 epoch 1, got attempt=%s epoch=%d", assignmentA.AttemptID, assignmentA.OwnershipEpoch)
	}

	// Verify: CLAIMED, started_at IS NULL
	var a1Status string
	var a1StartedAt *time.Time
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, started_at FROM task_attempts WHERE id=$1::uuid`, assignmentA.AttemptID).Scan(&a1Status, &a1StartedAt)
	}); err != nil {
		t.Fatal(err)
	}
	if a1Status != "CLAIMED" || a1StartedAt != nil {
		t.Fatalf("expected CLAIMED/NULL, got status=%s started_at=%v", a1Status, a1StartedAt)
	}

	// 2. Worker A reconnects (triggers FenceWorkerSessions, the code path that deadlocked)
	challengeBody, _ := json.Marshal(worker.ChallengeRequestDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: "e2e-challenge", WorkerID: workerA.WorkerID})
	challengeResp, err := http.Post(server.URL+"/worker/v1/challenge", "application/json", bytes.NewReader(challengeBody))
	if err != nil {
		t.Fatal(err)
	}
	var challenge worker.ChallengeResponseDTO
	_ = json.NewDecoder(challengeResp.Body).Decode(&challenge)
	challengeResp.Body.Close()
	sessionBody, _ := json.Marshal(worker.SessionRequestDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: "e2e-session",
		WorkerID: workerA.WorkerID, Nonce: challenge.Nonce, Signature: worker.SignChallenge(workerA.privateKey, challenge.Nonce)})
	newSessionResp, err := http.Post(server.URL+"/worker/v1/session", "application/json", bytes.NewReader(sessionBody))
	if err != nil {
		t.Fatal(err)
	}
	if newSessionResp.StatusCode != http.StatusOK {
		var responseError worker.ErrorEnvelopeDTO
		_ = json.NewDecoder(newSessionResp.Body).Decode(&responseError)
		newSessionResp.Body.Close()
		t.Fatalf("reconnect status=%d error=%+v", newSessionResp.StatusCode, responseError)
	}
	var newSession worker.SessionResponseDTO
	_ = json.NewDecoder(newSessionResp.Body).Decode(&newSession)
	newSessionResp.Body.Close()

	// 3 & 4. Verify the REAL persisted recovery handoff state:
	// - attempt 1 is marked LOST
	// - step is WAITING with wait_reason RECOVERY_HANDOFF
	// - run is WAITING with reason_code RECOVERY_HANDOFF
	// - lease is deleted
	var a1FinalStatus, stepState, runStatus string
	var stepWaitReason, runReasonCode *string
	var leasesCount int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT a.status, rs.state, rs.wait_reason, r.status, r.reason_code,
			(SELECT count(*) FROM task_leases l WHERE l.attempt_id=a.id)
			FROM task_attempts a JOIN run_steps rs ON rs.id=a.step_id JOIN runs r ON r.id=rs.run_id
			WHERE a.id=$1::uuid AND rs.id=$2::uuid AND r.id=$3::uuid`, assignmentA.AttemptID, stepID, runID).Scan(
			&a1FinalStatus, &stepState, &stepWaitReason, &runStatus, &runReasonCode, &leasesCount)
	}); err != nil {
		t.Fatal(err)
	}
	if a1FinalStatus != "LOST" {
		t.Fatalf("attempt 1 not marked LOST: %s", a1FinalStatus)
	}
	if stepState != "WAITING" || stepWaitReason == nil || *stepWaitReason != "RECOVERY_HANDOFF" {
		t.Fatalf("expected step in WAITING/RECOVERY_HANDOFF, got state=%s reason=%v", stepState, stepWaitReason)
	}
	if runStatus != "WAITING" || runReasonCode == nil || *runReasonCode != "RECOVERY_HANDOFF" {
		t.Fatalf("expected run in WAITING/RECOVERY_HANDOFF, got status=%s reason=%v", runStatus, runReasonCode)
	}
	if leasesCount != 0 {
		t.Fatalf("stale lease not deleted: %d", leasesCount)
	}

	// 5 & 6. Worker B polls: in the poll transaction, the reconciler automatically
	// resolves RECOVERY_HANDOFF (step -> READY, run -> QUEUED), and Worker B
	// claims replacement Attempt 2 with Epoch 2.
	assignmentB := claimExecution(t, server, workerB, digest, "poll-reconnect-b")
	if assignmentB.AttemptID == "" || assignmentB.AttemptID == assignmentA.AttemptID {
		t.Fatalf("expected new attempt for worker B, got %s", assignmentB.AttemptID)
	}
	if assignmentB.OwnershipEpoch <= assignmentA.OwnershipEpoch {
		t.Fatalf("expected epoch > %d for worker B, got %d", assignmentA.OwnershipEpoch, assignmentB.OwnershipEpoch)
	}

	// 7. Verify step became RUNNING, attempt 2 is CLAIMED, run is QUEUED (or RUNNING)
	var a2Status, step2State, run2Status string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT a.status, rs.state, r.status
			FROM task_attempts a JOIN run_steps rs ON rs.id=a.step_id JOIN runs r ON r.id=rs.run_id
			WHERE a.id=$1::uuid AND rs.id=$2::uuid AND r.id=$3::uuid`, assignmentB.AttemptID, stepID, runID).Scan(
			&a2Status, &step2State, &run2Status)
	}); err != nil {
		t.Fatal(err)
	}
	if a2Status != "CLAIMED" || step2State != "RUNNING" {
		t.Fatalf("unexpected replacement assignment state: attempt=%s step=%s", a2Status, step2State)
	}

	// 8. Stale epoch-1 requests must ALL be rejected:
	// a) Stale Start from Worker A's revoked session token -> 401 Unauthorized (SESSION_REVOKED)
	staleStartOldSession := worker.StartRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "stale-start-old",
		WorkerID: workerA.WorkerID, SessionID: workerA.SessionID,
		AttemptID: assignmentA.AttemptID, OwnershipEpoch: assignmentA.OwnershipEpoch,
	}
	if status := postWorkerJSON(t, server, "/worker/v1/start", workerA.SessionToken, staleStartOldSession, &worker.ErrorEnvelopeDTO{}); status != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized for revoked session Start, got %d", status)
	}

	// b) Stale Start with Worker A's new session token or Worker B's session token -> 409 Conflict
	staleStartNewSession := worker.StartRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "stale-start-new",
		WorkerID: workerA.WorkerID, SessionID: newSession.SessionID,
		AttemptID: assignmentA.AttemptID, OwnershipEpoch: assignmentA.OwnershipEpoch,
	}
	if status := postWorkerJSON(t, server, "/worker/v1/start", newSession.SessionToken, staleStartNewSession, &worker.ErrorEnvelopeDTO{}); status != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for stale Start with new session, got %d", status)
	}

	staleStartWorkerB := worker.StartRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "stale-start-b",
		WorkerID: workerB.WorkerID, SessionID: workerB.SessionID,
		AttemptID: assignmentA.AttemptID, OwnershipEpoch: assignmentA.OwnershipEpoch,
	}
	if status := postWorkerJSON(t, server, "/worker/v1/start", workerB.SessionToken, staleStartWorkerB, &worker.ErrorEnvelopeDTO{}); status != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for stale Start from worker B, got %d", status)
	}

	// c) Stale Heartbeat with active session -> stop command with LEASE_NOT_FOUND
	var hbResp worker.HeartbeatResponseDTO
	statusHB := postWorkerJSON(t, server, "/worker/v1/heartbeat", newSession.SessionToken, worker.HeartbeatRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "stale-heartbeat-new",
		WorkerID: workerA.WorkerID, SessionID: newSession.SessionID,
		Attempts: []worker.HeartbeatAttemptDTO{{AttemptID: assignmentA.AttemptID, OwnershipEpoch: assignmentA.OwnershipEpoch}},
	}, &hbResp)
	if statusHB != http.StatusOK || len(hbResp.Stops) != 1 || hbResp.Stops[0].Reason != "LEASE_NOT_FOUND" {
		t.Fatalf("expected LEASE_NOT_FOUND stop for stale Heartbeat, got status=%d stops=%+v", statusHB, hbResp.Stops)
	}

	// d) Stale Complete with active session -> 409 Conflict
	staleComp := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "stale-complete-new",
		WorkerID: workerA.WorkerID, SessionID: newSession.SessionID,
		AttemptID: assignmentA.AttemptID, OwnershipEpoch: assignmentA.OwnershipEpoch,
		Outcome: "SUCCEEDED",
	}
	staleComp.ResultDigest, _ = worker.CanonicalCompletionDigest(&staleComp)
	if status := postWorkerJSON(t, server, "/worker/v1/complete", newSession.SessionToken, staleComp, &worker.ErrorEnvelopeDTO{}); status != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for stale Complete, got %d", status)
	}

	// 9. Replacement Worker B starts and completes Attempt 2
	startB := worker.StartRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "start-reconnect-b",
		WorkerID: workerB.WorkerID, SessionID: workerB.SessionID,
		AttemptID: assignmentB.AttemptID, OwnershipEpoch: assignmentB.OwnershipEpoch,
	}
	var startBResp worker.StartResponseDTO
	if status := postWorkerJSON(t, server, "/worker/v1/start", workerB.SessionToken, startB, &startBResp); status != http.StatusOK || !startBResp.Accepted {
		t.Fatalf("Start for worker B rejected: status=%d resp=%+v", status, startBResp)
	}

	compB := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "complete-reconnect-b",
		WorkerID: workerB.WorkerID, SessionID: workerB.SessionID,
		AttemptID: assignmentB.AttemptID, OwnershipEpoch: assignmentB.OwnershipEpoch,
		Outcome: "SUCCEEDED",
	}
	compB.ResultDigest, _ = worker.CanonicalCompletionDigest(&compB)
	var compBResp worker.CompleteResponseDTO
	if status := postWorkerJSON(t, server, "/worker/v1/complete", workerB.SessionToken, compB, &compBResp); status != http.StatusOK || !compBResp.Accepted {
		t.Fatalf("Complete for worker B rejected: status=%d resp=%+v", status, compBResp)
	}

	// Verify run completed SUCCEEDED
	var runFinalStatus, stepFinalState string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT r.status, rs.state FROM runs r
			JOIN run_steps rs ON rs.run_id=r.id WHERE r.id=$1::uuid AND rs.id=$2::uuid`, runID, stepID).Scan(&runFinalStatus, &stepFinalState)
	}); err != nil {
		t.Fatal(err)
	}
	if runFinalStatus != "SUCCEEDED" || stepFinalState != "SUCCEEDED" {
		t.Fatalf("run failed to complete: run=%s step=%s", runFinalStatus, stepFinalState)
	}

	// Verify no RECOVERY_HANDOFF exists anywhere in final state
	var recoveryHandoffCount int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM run_steps WHERE wait_reason='RECOVERY_HANDOFF') +
			(SELECT count(*) FROM runs WHERE reason_code='RECOVERY_HANDOFF')`).Scan(&recoveryHandoffCount)
	}); err != nil {
		t.Fatal(err)
	}
	if recoveryHandoffCount != 0 {
		t.Fatalf("RECOVERY_HANDOFF still present in DB: count=%d", recoveryHandoffCount)
	}
}

func TestWorkerOperationIDStableAcrossRetriesAndUniqueAcrossRuns(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "operation-id")
	deploymentID := seedExecutionDeployment(t, tc, orgID, envID, "bundle-operation")
	_, firstStepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	first := claimExecution(t, server, session, "bundle-operation", "operation-first")
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE task_attempts SET status='LOST',completed_at=clock_timestamp() WHERE id=$1::uuid`, first.AttemptID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM task_leases WHERE attempt_id=$1::uuid`, first.AttemptID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE run_steps SET state='READY',eligible_at=clock_timestamp() WHERE id=$1::uuid`, firstStepID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	second := claimExecution(t, server, session, "bundle-operation", "operation-retry")
	if first.OperationID == "" || first.OperationID != second.OperationID || first.AttemptID == second.AttemptID {
		t.Fatalf("operation identity changed across retry: first=%+v second=%+v", first, second)
	}
	_, _ = seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	third := claimExecution(t, server, session, "bundle-operation", "operation-other-run")
	if third.OperationID == first.OperationID {
		t.Fatalf("operation identity reused across runs: %s", third.OperationID)
	}
}

// TestWorkerClaimExpiryAndDurableRecovery proves that when a worker claims an assignment
// but fails to send Start, the system automatically invalidates stale ownership, reclaims
// the step, assigns a replacement attempt with a newer epoch, rejects stale worker actions,
// and completes the run normally.
func TestWorkerClaimExpiryAndDurableRecovery(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	workerA, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "recovery-worker-a")
	workerB, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "recovery-worker-b")

	const digest = "bundle-claim-recovery"
	deploymentID := seedExecutionDeployment(t, tc, orgID, envID, digest)
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")

	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		for _, s := range []*testWorkerSession{workerA, workerB} {
			if _, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3)`, s.SessionID, orgID, digest); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// 1. Worker A polls and claims the assignment (Attempt 1, Epoch 1)
	assignmentA := claimExecution(t, server, workerA, digest, "poll-recovery-a")
	if assignmentA.AttemptID == "" || assignmentA.OwnershipEpoch != 1 {
		t.Fatalf("expected attempt 1 with epoch 1, got attempt=%s epoch=%d", assignmentA.AttemptID, assignmentA.OwnershipEpoch)
	}

	// Verify Attempt 1 state in DB: CLAIMED, started_at IS NULL, 1 active lease
	var attempt1Status string
	var attempt1StartedAt *time.Time
	var leasesCount int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT a.status, a.started_at,
			(SELECT count(*) FROM task_leases l WHERE l.attempt_id=a.id)
			FROM task_attempts a WHERE a.id=$1::uuid`, assignmentA.AttemptID).Scan(&attempt1Status, &attempt1StartedAt, &leasesCount)
	}); err != nil {
		t.Fatal(err)
	}
	if attempt1Status != "CLAIMED" || attempt1StartedAt != nil || leasesCount != 1 {
		t.Fatalf("unexpected attempt 1 initial state: status=%s started_at=%v leases=%d", attempt1Status, attempt1StartedAt, leasesCount)
	}

	// 2. Worker A intentionally NEVER sends Start. Claim start deadline expires.
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE task_attempts SET claim_start_deadline_at=clock_timestamp()-INTERVAL '1 second' WHERE id=$1::uuid`, assignmentA.AttemptID)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	// 3. Worker B polls for work.
	// In the same poll transaction, the engine reconciles Worker A's expired claim,
	// marks Attempt 1 as LOST, deletes the lease, resets the step to READY,
	// and assigns Attempt 2 (with newer Epoch 2) to Worker B.
	assignmentB := claimExecution(t, server, workerB, digest, "poll-recovery-b")
	if assignmentB.AttemptID == "" || assignmentB.AttemptID == assignmentA.AttemptID {
		t.Fatalf("expected new attempt ID for worker B, got %s (same as %s)", assignmentB.AttemptID, assignmentA.AttemptID)
	}
	if assignmentB.OwnershipEpoch <= assignmentA.OwnershipEpoch {
		t.Fatalf("expected newer ownership epoch for worker B, got epoch=%d (worker A epoch=%d)", assignmentB.OwnershipEpoch, assignmentA.OwnershipEpoch)
	}

	// 4. Verify Attempt 1 was marked LOST with START_DEADLINE_EXCEEDED, lease deleted
	var attempt1FinalStatus, attempt1ErrorCode string
	var attempt1Leases int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT a.status, COALESCE(a.error->>'code', ''),
			(SELECT count(*) FROM task_leases l WHERE l.attempt_id=a.id)
			FROM task_attempts a WHERE a.id=$1::uuid`, assignmentA.AttemptID).Scan(&attempt1FinalStatus, &attempt1ErrorCode, &attempt1Leases)
	}); err != nil {
		t.Fatal(err)
	}
	if attempt1FinalStatus != "LOST" || attempt1ErrorCode != "START_DEADLINE_EXCEEDED" || attempt1Leases != 0 {
		t.Fatalf("attempt 1 not durably invalidated: status=%s error=%s leases=%d", attempt1FinalStatus, attempt1ErrorCode, attempt1Leases)
	}

	// Verify events in run_events: TASK_LOST and STEP_READY
	var taskLostCount, stepReadyCount int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM run_events WHERE run_id=$1::uuid AND event_type='TASK_LOST'),
			(SELECT count(*) FROM run_events WHERE run_id=$1::uuid AND event_type='STEP_READY')`, runID).Scan(&taskLostCount, &stepReadyCount)
	}); err != nil {
		t.Fatal(err)
	}
	if taskLostCount < 1 || stepReadyCount < 1 {
		t.Fatalf("missing recovery events: taskLost=%d stepReady=%d", taskLostCount, stepReadyCount)
	}

	// 5. Stale Worker A requests must ALL be rejected:
	// a) Worker A calls Start(Attempt 1, Epoch 1) -> 409 Conflict
	startA := worker.StartRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "stale-start-a",
		WorkerID: workerA.WorkerID, SessionID: workerA.SessionID,
		AttemptID: assignmentA.AttemptID, OwnershipEpoch: assignmentA.OwnershipEpoch,
	}
	if status := postWorkerJSON(t, server, "/worker/v1/start", workerA.SessionToken, startA, &worker.ErrorEnvelopeDTO{}); status != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for stale Start, got status %d", status)
	}

	// b) Worker A calls Heartbeat(Attempt 1, Epoch 1) -> stop command with LEASE_NOT_FOUND
	var heartbeatA worker.HeartbeatResponseDTO
	statusHB := postWorkerJSON(t, server, "/worker/v1/heartbeat", workerA.SessionToken, worker.HeartbeatRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "stale-heartbeat-a",
		WorkerID: workerA.WorkerID, SessionID: workerA.SessionID,
		Attempts: []worker.HeartbeatAttemptDTO{{AttemptID: assignmentA.AttemptID, OwnershipEpoch: assignmentA.OwnershipEpoch}},
	}, &heartbeatA)
	if statusHB != http.StatusOK || len(heartbeatA.Stops) != 1 || heartbeatA.Stops[0].Reason != "LEASE_NOT_FOUND" {
		t.Fatalf("expected LEASE_NOT_FOUND stop command for stale Heartbeat, got status=%d stops=%+v", statusHB, heartbeatA.Stops)
	}

	// c) Worker A calls Complete(Attempt 1, Epoch 1) -> 409 Conflict
	compA := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "stale-complete-a",
		WorkerID: workerA.WorkerID, SessionID: workerA.SessionID,
		AttemptID: assignmentA.AttemptID, OwnershipEpoch: assignmentA.OwnershipEpoch,
		Outcome: "SUCCEEDED",
	}
	compA.ResultDigest, _ = worker.CanonicalCompletionDigest(&compA)
	if status := postWorkerJSON(t, server, "/worker/v1/complete", workerA.SessionToken, compA, &worker.ErrorEnvelopeDTO{}); status != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for stale Complete, got status %d", status)
	}

	// 6. Replacement Worker B proceeds normally:
	// a) Worker B calls Start(Attempt 2, Epoch 2) -> 200 OK
	startB := worker.StartRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "start-b",
		WorkerID: workerB.WorkerID, SessionID: workerB.SessionID,
		AttemptID: assignmentB.AttemptID, OwnershipEpoch: assignmentB.OwnershipEpoch,
	}
	var startBResp worker.StartResponseDTO
	if status := postWorkerJSON(t, server, "/worker/v1/start", workerB.SessionToken, startB, &startBResp); status != http.StatusOK || !startBResp.Accepted {
		t.Fatalf("valid Start for worker B rejected: status=%d resp=%+v", status, startBResp)
	}

	// b) Worker B calls Complete(Attempt 2, Epoch 2) -> 200 OK
	compB := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "complete-b",
		WorkerID: workerB.WorkerID, SessionID: workerB.SessionID,
		AttemptID: assignmentB.AttemptID, OwnershipEpoch: assignmentB.OwnershipEpoch,
		Outcome: "SUCCEEDED",
	}
	compB.ResultDigest, _ = worker.CanonicalCompletionDigest(&compB)
	var compBResp worker.CompleteResponseDTO
	if status := postWorkerJSON(t, server, "/worker/v1/complete", workerB.SessionToken, compB, &compBResp); status != http.StatusOK || !compBResp.Accepted {
		t.Fatalf("valid Complete for worker B rejected: status=%d resp=%+v", status, compBResp)
	}

	// 7. Verify Run completed SUCCEEDED and step completed SUCCEEDED
	var runFinalStatus, stepFinalState string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT r.status, rs.state FROM runs r
			JOIN run_steps rs ON rs.run_id=r.id WHERE r.id=$1::uuid AND rs.id=$2::uuid`, runID, stepID).Scan(&runFinalStatus, &stepFinalState)
	}); err != nil {
		t.Fatal(err)
	}
	if runFinalStatus != "SUCCEEDED" || stepFinalState != "SUCCEEDED" {
		t.Fatalf("run failed to complete normally: run=%s step=%s", runFinalStatus, stepFinalState)
	}
}

// TestWorkerRunningLeaseExpiryAndDurableRecovery proves that when a worker starts an attempt
// but subsequently goes silent and its lease expires without renewal, the reconciler durably
// recovers the step and allows a replacement worker to finish the run.
func TestWorkerRunningLeaseExpiryAndDurableRecovery(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	workerA, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "running-recovery-a")
	workerB, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "running-recovery-b")

	const digest = "bundle-running-lease-recovery"
	deploymentID := seedExecutionDeployment(t, tc, orgID, envID, digest)
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")

	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		for _, s := range []*testWorkerSession{workerA, workerB} {
			if _, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3)`, s.SessionID, orgID, digest); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// 1. Worker A claims and starts Attempt 1
	assignmentA := claimExecution(t, server, workerA, digest, "poll-running-a")
	startA := worker.StartRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "start-running-a",
		WorkerID: workerA.WorkerID, SessionID: workerA.SessionID,
		AttemptID: assignmentA.AttemptID, OwnershipEpoch: assignmentA.OwnershipEpoch,
	}
	var startAResp worker.StartResponseDTO
	if status := postWorkerJSON(t, server, "/worker/v1/start", workerA.SessionToken, startA, &startAResp); status != http.StatusOK || !startAResp.Accepted {
		t.Fatalf("Start for worker A rejected: status=%d resp=%+v", status, startAResp)
	}

	// 2. Worker A stops heartbeating and lease expires
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE task_leases SET expires_at=clock_timestamp()-INTERVAL '1 second' WHERE attempt_id=$1::uuid`, assignmentA.AttemptID)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	// 3. Worker Engine ReconcileExpiredLeases runs (as reconciler would)
	var preTransitionLostEvents, preTransitionWaitingEvents, preTransitionOutbox int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM run_events WHERE run_id=$1::uuid AND event_type='TASK_LOST'),
			(SELECT count(*) FROM run_events WHERE run_id=$1::uuid AND event_type='STEP_WAITING'),
			(SELECT count(*) FROM outbox_events WHERE subject='execution.state_changed' AND payload->>'runId'=$1::text)`, runID).
			Scan(&preTransitionLostEvents, &preTransitionWaitingEvents, &preTransitionOutbox)
	}); err != nil {
		t.Fatal(err)
	}
	engine := execution.NewWorkerEngine(tc.pool)
	reclaimed, err := engine.ReconcileExpiredLeases(context.Background(), orgID)
	if err != nil {
		t.Fatalf("ReconcileExpiredLeases failed: %v", err)
	}
	if reclaimed != 1 {
		t.Fatalf("expected 1 reclaimed attempt, got %d", reclaimed)
	}

	// 4. Verify Attempt 1 marked LOST with LEASE_EXPIRED and step parked in
	// WAITING/RETRY_BACKOFF with a persisted PENDING timer (M2 #18: durable
	// backoff replaces immediate READY; firing creates READY).
	var attempt1Status, attempt1Error, stepState, waitReason string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT a.status, COALESCE(a.error->>'code', ''), rs.state, COALESCE(rs.wait_reason,'')
			FROM task_attempts a JOIN run_steps rs ON rs.id=a.step_id
			WHERE a.id=$1::uuid`, assignmentA.AttemptID).Scan(&attempt1Status, &attempt1Error, &stepState, &waitReason)
	}); err != nil {
		t.Fatal(err)
	}
	if attempt1Status != "LOST" || attempt1Error != "LEASE_EXPIRED" || stepState != "WAITING" || waitReason != "RETRY_BACKOFF" {
		t.Fatalf("unexpected state after lease expiry: attemptStatus=%s error=%s stepState=%s wait=%s", attempt1Status, attempt1Error, stepState, waitReason)
	}
	var pendingTimers int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM timers WHERE step_id=$1::uuid AND state='PENDING' AND kind='RETRY_BACKOFF'`, stepID).Scan(&pendingTimers)
	}); err != nil {
		t.Fatal(err)
	}
	if pendingTimers != 1 {
		t.Fatalf("expected one pending retry timer, got %d", pendingTimers)
	}

	// A second sweep must be a no-op. Lease expiry is an authoritative state
	// transition, not a notification that can be emitted again on every sweep.
	var lostEvents, waitingEvents, stateChangedOutbox int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM run_events WHERE run_id=$1::uuid AND event_type='TASK_LOST'),
			(SELECT count(*) FROM run_events WHERE run_id=$1::uuid AND event_type='STEP_WAITING'),
			(SELECT count(*) FROM outbox_events WHERE subject='execution.state_changed' AND payload->>'runId'=$1::text)`, runID).
			Scan(&lostEvents, &waitingEvents, &stateChangedOutbox)
	}); err != nil {
		t.Fatal(err)
	}
	if lostEvents != preTransitionLostEvents+1 || waitingEvents != preTransitionWaitingEvents+1 || stateChangedOutbox != preTransitionOutbox+2 {
		t.Fatalf("lease expiry did not record the required durable effects: before=%d/%d/%d after=%d/%d/%d",
			preTransitionLostEvents, preTransitionWaitingEvents, preTransitionOutbox, lostEvents, waitingEvents, stateChangedOutbox)
	}

	preSweepLostEvents, preSweepWaitingEvents, preSweepOutbox := lostEvents, waitingEvents, stateChangedOutbox
	reclaimed, err = engine.ReconcileExpiredLeases(context.Background(), orgID)
	if err != nil {
		t.Fatalf("second ReconcileExpiredLeases failed: %v", err)
	}
	if reclaimed != 0 {
		t.Fatalf("expected idempotent second sweep to reclaim nothing, got %d", reclaimed)
	}
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM run_events WHERE run_id=$1::uuid AND event_type='TASK_LOST'),
			(SELECT count(*) FROM run_events WHERE run_id=$1::uuid AND event_type='STEP_WAITING'),
			(SELECT count(*) FROM outbox_events WHERE subject='execution.state_changed' AND payload->>'runId'=$1::text)`, runID).
			Scan(&lostEvents, &waitingEvents, &stateChangedOutbox)
	}); err != nil {
		t.Fatal(err)
	}
	if lostEvents != preSweepLostEvents || waitingEvents != preSweepWaitingEvents || stateChangedOutbox != preSweepOutbox {
		t.Fatalf("lease expiry was not idempotent: before=%d/%d/%d after=%d/%d/%d",
			preSweepLostEvents, preSweepWaitingEvents, preSweepOutbox, lostEvents, waitingEvents, stateChangedOutbox)
	}

	// 5. Fire the due retry timer (M2 #18): due firing creates READY, then
	// Worker B claims the recovered step. Make the timer due deterministically.
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE timers SET due_at=clock_timestamp()-INTERVAL '1 second' WHERE step_id=$1::uuid AND state='PENDING'`, stepID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if fired, err := engine.FireDueRetryTimers(context.Background(), orgID); err != nil || fired != 1 {
		t.Fatalf("expected retry timer to fire once, got %d err %v", fired, err)
	}
	assignmentB := claimExecution(t, server, workerB, digest, "poll-running-b")
	if assignmentB.AttemptID == assignmentA.AttemptID || assignmentB.OwnershipEpoch <= assignmentA.OwnershipEpoch {
		t.Fatalf("expected replacement attempt with higher epoch, got attempt=%s epoch=%d", assignmentB.AttemptID, assignmentB.OwnershipEpoch)
	}

	// 6. Worker B starts and completes Attempt 2
	startB := worker.StartRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "start-running-b",
		WorkerID: workerB.WorkerID, SessionID: workerB.SessionID,
		AttemptID: assignmentB.AttemptID, OwnershipEpoch: assignmentB.OwnershipEpoch,
	}
	var startBResp worker.StartResponseDTO
	if status := postWorkerJSON(t, server, "/worker/v1/start", workerB.SessionToken, startB, &startBResp); status != http.StatusOK || !startBResp.Accepted {
		t.Fatalf("Start for worker B rejected: status=%d resp=%+v", status, startBResp)
	}

	compB := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "complete-running-b",
		WorkerID: workerB.WorkerID, SessionID: workerB.SessionID,
		AttemptID: assignmentB.AttemptID, OwnershipEpoch: assignmentB.OwnershipEpoch,
		Outcome: "SUCCEEDED",
	}
	compB.ResultDigest, _ = worker.CanonicalCompletionDigest(&compB)
	var compBResp worker.CompleteResponseDTO
	if status := postWorkerJSON(t, server, "/worker/v1/complete", workerB.SessionToken, compB, &compBResp); status != http.StatusOK || !compBResp.Accepted {
		t.Fatalf("Complete for worker B rejected: status=%d resp=%+v", status, compBResp)
	}

	// 7. Verify final success
	var runStatus, finalStepState string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT r.status, rs.state FROM runs r
			JOIN run_steps rs ON rs.run_id=r.id WHERE r.id=$1::uuid AND rs.id=$2::uuid`, runID, stepID).Scan(&runStatus, &finalStepState)
	}); err != nil {
		t.Fatal(err)
	}
	if runStatus != "SUCCEEDED" || finalStepState != "SUCCEEDED" {
		t.Fatalf("expected run and step SUCCEEDED, got run=%s step=%s", runStatus, finalStepState)
	}
}
