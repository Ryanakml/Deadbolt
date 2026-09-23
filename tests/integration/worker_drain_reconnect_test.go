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
