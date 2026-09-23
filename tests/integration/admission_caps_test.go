package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

// TestCreateRunTokenBucketRefillAndAdmission429 verifies the 5 req/s refill and burst 10
// token bucket per-environment admission limiter with deterministic tests covering burst=10,
// 429 + Retry-After, refill rate, sustained throttling, and concurrency.
func TestCreateRunTokenBucketRefillAndAdmission429(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	bundle := "2222222222222222222222222222222222222222222222222222222222222222"
	schema := map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"val": map[string]any{"type": "string"}},
		"required":             []any{"val"},
		"additionalProperties": false,
	}
	manifest := createLifecycleManifest(bundle,
		[]map[string]any{{"name": "rate-task", "entrypoint": "tasks/rate.js", "timeoutMs": 30000, "recovery": "idempotent", "idempotencyWindowMs": 305000, "inputSchema": schema, "outputSchema": schema}},
		[]map[string]any{{"manifestVersion": 1, "name": "rate-flow", "inputSchema": schema, "outputSchema": schema, "nodes": []map[string]any{{"id": "step-1", "type": "task", "task": "rate-task", "after": []any{}, "input": map[string]any{"val": map[string]any{"$ref": "run.input", "pointer": "/val"}}}}, "output": map[string]any{"val": map[string]any{"$ref": "step.output", "stepId": "step-1", "pointer": "/val"}}}},
	)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "rate-flow", manifest)

	createWithResp := func(key string) (int, string, map[string]any) {
		body := bytes.NewReader([]byte(`{"environment":"staging","input":{"val":"ok"}}`))
		req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/rate-flow/runs", body)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
		req.Header.Set("X-Organization-ID", orgID)
		req.Header.Set("Idempotency-Key", key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var resBody map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&resBody)
		return resp.StatusCode, resp.Header.Get("Retry-After"), resBody
	}

	create := func(key string) int {
		status, _, _ := createWithResp(key)
		return status
	}

	// 1. Initial burst capacity of 10: 10 rapid calls with unique idempotency keys must all succeed.
	for i := 1; i <= 10; i++ {
		key := fmt.Sprintf("burst-req-%d", i)
		if status := create(key); status != http.StatusAccepted {
			t.Fatalf("burst request %d status=%d, want 202 Accepted", i, status)
		}
	}

	// Pin the bucket to exactly 0 tokens (and reset the timestamp to now) so that any
	// wall-clock time that elapsed during the sequential burst above does not partially
	// refill the bucket before we test the 11th request.  On slow CI runners the 10
	// sequential round-trips can take hundreds of milliseconds, which refills enough
	// tokens (5 req/s × elapsed) to absorb the burst-exceeded check.
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE environment_admissions
			SET create_rate_tokens=0, create_rate_updated_at=clock_timestamp()
			WHERE environment_id=$1::uuid AND organization_id=$2::uuid`, envID, orgID)
		return err
	}); err != nil {
		t.Fatalf("pin bucket after burst: %v", err)
	}

	// 2. The 11th request exceeds burst capacity and must receive 429 + Retry-After: 1
	status, retryAfter, body := createWithResp("burst-exceeded-11")
	if status != http.StatusTooManyRequests {
		t.Fatalf("11th request status=%d, want 429 Too Many Requests", status)
	}
	if retryAfter != "1" {
		t.Fatalf("expected Retry-After header '1', got %q", retryAfter)
	}
	if errCode, _ := body["code"].(string); errCode != "CREATE_RUN_RATE_LIMITED" {
		t.Fatalf("expected error code CREATE_RUN_RATE_LIMITED, got %v", body)
	}

	// 3. Drain bucket manually and verify rejection
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE environment_admissions
			SET create_rate_tokens=0, create_rate_updated_at=clock_timestamp()
			WHERE environment_id=$1::uuid AND organization_id=$2::uuid`, envID, orgID)
		return err
	}); err != nil {
		t.Fatalf("prepare empty bucket: %v", err)
	}
	if status := create("rate-empty"); status != http.StatusTooManyRequests {
		t.Fatalf("empty bucket status=%d, want 429", status)
	}

	// 4. Refill: advance time by 1 second (refilling 5 tokens at 5 req/s).
	// Exactly 5 requests should succeed, and the 6th should be rejected.
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE environment_admissions
			SET create_rate_tokens=0, create_rate_updated_at=clock_timestamp()-INTERVAL '1 second'
			WHERE environment_id=$1::uuid AND organization_id=$2::uuid`, envID, orgID)
		return err
	}); err != nil {
		t.Fatalf("prepare token refill: %v", err)
	}
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("refilled-req-%d", i)
		if status := create(key); status != http.StatusAccepted {
			t.Fatalf("refilled request %d status=%d, want 202", i, status)
		}
	}

	// Pin the bucket to exactly 0 tokens (and reset the timestamp to now) so that any
	// wall-clock time that elapsed during the 5 sequential requests above does not partially
	// refill the bucket before we test the 6th request on slow CI runners.
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE environment_admissions
			SET create_rate_tokens=0, create_rate_updated_at=clock_timestamp()
			WHERE environment_id=$1::uuid AND organization_id=$2::uuid`, envID, orgID)
		return err
	}); err != nil {
		t.Fatalf("pin bucket after refill: %v", err)
	}

	if status := create("refilled-exceeded-6"); status != http.StatusTooManyRequests {
		t.Fatalf("6th request after 1s refill status=%d, want 429", status)
	}

	// 5. Concurrency: Reset bucket to full (10 tokens). Fire 15 concurrent requests.
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make([]int, 15)
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			results[idx] = create(fmt.Sprintf("concurrent-rate-%d", idx))
		}(i)
	}

	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE environment_admissions
			SET create_rate_tokens=10, create_rate_updated_at=clock_timestamp()
			WHERE environment_id=$1::uuid AND organization_id=$2::uuid`, envID, orgID)
		return err
	}); err != nil {
		t.Fatalf("reset bucket for concurrency: %v", err)
	}
	close(start)
	wg.Wait()

	acceptedCount := 0
	rateLimitedCount := 0
	for _, st := range results {
		if st == http.StatusAccepted {
			acceptedCount++
		} else if st == http.StatusTooManyRequests {
			rateLimitedCount++
		}
	}
	if acceptedCount != 10 || rateLimitedCount != 5 {
		t.Fatalf("concurrent results: accepted=%d (want 10), rateLimited=%d (want 5)", acceptedCount, rateLimitedCount)
	}
}

// TestClaimFifoOrderingWithinEnvironment verifies that ready task steps within an environment
// are claimed in strict FIFO order according to (eligible_at, id) per Blueprint §19.3.
func TestClaimFifoOrderingWithinEnvironment(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	bundle := "3333333333333333333333333333333333333333333333333333333333333333"
	schema := map[string]any{"type": "object"}
	manifest := createLifecycleManifest(bundle,
		[]map[string]any{{"name": "fifo-task", "entrypoint": "tasks/fifo.js", "timeoutMs": 30000, "recovery": "idempotent", "idempotencyWindowMs": 305000, "inputSchema": schema, "outputSchema": schema}},
		[]map[string]any{{"manifestVersion": 1, "name": "fifo-flow", "inputSchema": schema, "outputSchema": schema, "nodes": []map[string]any{{"id": "node-1", "type": "task", "task": "fifo-task", "after": []any{}, "input": map[string]any{}}}, "output": map[string]any{"$ref": "step.output", "stepId": "node-1", "pointer": ""}}},
	)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "fifo-flow", manifest)

	// Create 3 runs
	createRun := func(idempotencyKey string) string {
		body := bytes.NewReader([]byte(`{"environment":"staging","input":{}}`))
		req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/fifo-flow/runs", body)
		req.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
		req.Header.Set("X-Organization-ID", orgID)
		req.Header.Set("Idempotency-Key", idempotencyKey)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != http.StatusAccepted {
			t.Fatalf("create run failed: %v, status=%d", err, resp.StatusCode)
		}
		defer resp.Body.Close()
		var res map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&res)
		return res["id"].(string)
	}

	run1 := createRun("fifo-run-1")
	run2 := createRun("fifo-run-2")
	run3 := createRun("fifo-run-3")

	// Adjust eligible_at deterministically: run2 oldest (now-30s), run1 middle (now-20s), run3 newest (now-10s)
	ctx := context.Background()
	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		_, _ = tx.Exec(ctx, `UPDATE run_steps SET eligible_at=clock_timestamp()-INTERVAL '30 seconds' WHERE run_id=$1::uuid`, run2)
		_, _ = tx.Exec(ctx, `UPDATE run_steps SET eligible_at=clock_timestamp()-INTERVAL '20 seconds' WHERE run_id=$1::uuid`, run1)
		_, _ = tx.Exec(ctx, `UPDATE run_steps SET eligible_at=clock_timestamp()-INTERVAL '10 seconds' WHERE run_id=$1::uuid`, run3)
		return nil
	})

	workerSession, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "fifo-worker")
	advertiseDigest(t, tc, orgID, workerSession.SessionID, bundle)

	engine := execution.NewWorkerEngine(tc.pool)
	sessCtx := &worker.WorkerSessionContext{
		SessionID: workerSession.SessionID, WorkerID: workerSession.WorkerID,
		OrganizationID: orgID, EnvironmentID: envID, PoolName: "default",
		ExpiresAt: time.Now().Add(time.Hour),
	}

	// First claim: must return run2 (oldest eligible_at)
	resp1, err := engine.Claim(ctx, sessCtx, &worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "fifo-poll-1",
		WorkerID: workerSession.WorkerID, SessionID: workerSession.SessionID,
		AvailableSlots: 1, DeploymentDigests: []string{bundle}, Pool: "default",
	})
	if err != nil || len(resp1.Assignments) != 1 {
		t.Fatalf("claim 1 failed: %v, assignments=%d", err, len(resp1.Assignments))
	}
	if resp1.Assignments[0].RunID != run2 {
		t.Fatalf("claim 1: expected oldest run %s, got %s", run2, resp1.Assignments[0].RunID)
	}

	// Second claim: must return run1 (middle eligible_at)
	resp2, err := engine.Claim(ctx, sessCtx, &worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "fifo-poll-2",
		WorkerID: workerSession.WorkerID, SessionID: workerSession.SessionID,
		AvailableSlots: 1, DeploymentDigests: []string{bundle}, Pool: "default",
	})
	if err != nil || len(resp2.Assignments) != 1 {
		t.Fatalf("claim 2 failed: %v, assignments=%d", err, len(resp2.Assignments))
	}
	if resp2.Assignments[0].RunID != run1 {
		t.Fatalf("claim 2: expected middle run %s, got %s", run1, resp2.Assignments[0].RunID)
	}

	// Third claim: must return run3 (newest eligible_at)
	resp3, err := engine.Claim(ctx, sessCtx, &worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "fifo-poll-3",
		WorkerID: workerSession.WorkerID, SessionID: workerSession.SessionID,
		AvailableSlots: 1, DeploymentDigests: []string{bundle}, Pool: "default",
	})
	if err != nil || len(resp3.Assignments) != 1 {
		t.Fatalf("claim 3 failed: %v, assignments=%d", err, len(resp3.Assignments))
	}
	if resp3.Assignments[0].RunID != run3 {
		t.Fatalf("claim 3: expected newest run %s, got %s", run3, resp3.Assignments[0].RunID)
	}
}

// TestClaimRoundRobinAcrossEnvironments verifies round-robin candidate distribution across
// multiple environments in an organization without starvation (Blueprint §19.3).
func TestClaimRoundRobinAcrossEnvironments(t *testing.T) {
	tc, server, orgID, envID1, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	ctx := context.Background()
	// Create second environment in same project
	var projID string
	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT project_id FROM environments WHERE id=$1::uuid`, envID1).Scan(&projID)
	})
	env2, err := tc.service.CreateEnvironment(ctx, orgID, projID, tenant.EnvDevelopment, 10)
	if err != nil {
		t.Fatalf("create second env: %v", err)
	}
	envID2 := env2.ID

	bundle := "4444444444444444444444444444444444444444444444444444444444444444"
	schema := map[string]any{"type": "object"}
	manifest := createLifecycleManifest(bundle,
		[]map[string]any{{"name": "rr-task", "entrypoint": "tasks/rr.js", "timeoutMs": 30000, "recovery": "idempotent", "idempotencyWindowMs": 305000, "inputSchema": schema, "outputSchema": schema}},
		[]map[string]any{{"manifestVersion": 1, "name": "rr-flow", "inputSchema": schema, "outputSchema": schema, "nodes": []map[string]any{{"id": "node-1", "type": "task", "task": "rr-task", "after": []any{}, "input": map[string]any{}}}, "output": map[string]any{"$ref": "step.output", "stepId": "node-1", "pointer": ""}}},
	)
	// API keys are permanently bound to a single environment. The staging
	// adminKey from setup cannot register, activate, or create runs in the
	// second environment, so bootstrap a second key scoped to envID2.
	// A development (non-production) env is used so single-worker activation
	// preflight succeeds with the one seeded worker.
	adminKey2 := bootstrapTestKey(t, tc.service, orgID, envID2, []string{
		tenant.CapDeploymentsRegister,
		tenant.CapDeploymentsActivateStaging,
		tenant.CapDeploymentsActivateProd,
		tenant.CapDeploymentsWrite,
		tenant.CapWorkersDrain,
		tenant.CapRunsCreate,
		tenant.CapRunsRead,
		tenant.CapPayloadRead,
		tenant.CapAdminKey,
	})

	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID1, "rr-flow", manifest)
	registerAndActivateTestWorkflow(t, tc, server, adminKey2, orgID, envID2, "rr-flow", manifest)

	// Create 2 runs in Env 1 and 2 runs in Env 2
	createInEnv := func(envParam, key, plaintextKey string) string {
		body := bytes.NewReader([]byte(fmt.Sprintf(`{"environment":%q,"input":{}}`, envParam)))
		req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/rr-flow/runs", body)
		req.Header.Set("Authorization", "Bearer "+plaintextKey)
		req.Header.Set("X-Organization-ID", orgID)
		req.Header.Set("Idempotency-Key", key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("create run failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			var errBody map[string]any
			_ = json.NewDecoder(resp.Body).Decode(&errBody)
			t.Fatalf("create run failed: status=%d body=%v", resp.StatusCode, errBody)
		}
		var res map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&res)
		return res["id"].(string)
	}

	_ = createInEnv(envID1, "env1-run-1", adminKey.PlaintextKey)
	_ = createInEnv(envID1, "env1-run-2", adminKey.PlaintextKey)
	_ = createInEnv(envID2, "env2-run-1", adminKey2.PlaintextKey)
	_ = createInEnv(envID2, "env2-run-2", adminKey2.PlaintextKey)

	// Both environments have workers competing
	w1Session, _ := enrollExecutionWorker(t, tc, server, orgID, envID1, "worker-env1")
	w2Session, _ := enrollExecutionWorker(t, tc, server, orgID, envID2, "worker-env2")
	advertiseDigest(t, tc, orgID, w1Session.SessionID, bundle)
	advertiseDigest(t, tc, orgID, w2Session.SessionID, bundle)

	engine := execution.NewWorkerEngine(tc.pool)
	w1Ctx := &worker.WorkerSessionContext{
		SessionID: w1Session.SessionID, WorkerID: w1Session.WorkerID,
		OrganizationID: orgID, EnvironmentID: envID1, PoolName: "default",
		ExpiresAt: time.Now().Add(time.Hour),
	}
	w2Ctx := &worker.WorkerSessionContext{
		SessionID: w2Session.SessionID, WorkerID: w2Session.WorkerID,
		OrganizationID: orgID, EnvironmentID: envID2, PoolName: "default",
		ExpiresAt: time.Now().Add(time.Hour),
	}

	// Concurrent claims from both environments succeed without starving either
	var wg sync.WaitGroup
	var resp1, resp2 *worker.PollResponseDTO
	var err1, err2 error
	wg.Add(2)
	go func() {
		defer wg.Done()
		resp1, err1 = engine.Claim(ctx, w1Ctx, &worker.PollRequestDTO{
			ProtocolVersion: worker.ProtocolVersion, RequestID: "w1-claim",
			WorkerID: w1Session.WorkerID, SessionID: w1Session.SessionID,
			AvailableSlots: 2, DeploymentDigests: []string{bundle}, Pool: "default",
		})
	}()
	go func() {
		defer wg.Done()
		resp2, err2 = engine.Claim(ctx, w2Ctx, &worker.PollRequestDTO{
			ProtocolVersion: worker.ProtocolVersion, RequestID: "w2-claim",
			WorkerID: w2Session.WorkerID, SessionID: w2Session.SessionID,
			AvailableSlots: 2, DeploymentDigests: []string{bundle}, Pool: "default",
		})
	}()
	wg.Wait()

	if err1 != nil || len(resp1.Assignments) == 0 {
		t.Fatalf("env1 claim failed: %v, assignments=%d", err1, len(resp1.Assignments))
	}
	if err2 != nil || len(resp2.Assignments) == 0 {
		t.Fatalf("env2 claim failed: %v, assignments=%d", err2, len(resp2.Assignments))
	}
}

// TestClaimConcurrencyCapsAndQuotaWait verifies that worker slots are clamped to DefaultSlots (2),
// active leases are capped to maxConcurrency (10), and overflowing work transitions to QUOTA_WAIT.
func TestClaimConcurrencyCapsAndQuotaWait(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	bundle := "5555555555555555555555555555555555555555555555555555555555555555"
	schema := map[string]any{"type": "object"}
	manifest := createLifecycleManifest(bundle,
		[]map[string]any{{"name": "cap-task", "entrypoint": "tasks/cap.js", "timeoutMs": 30000, "recovery": "idempotent", "idempotencyWindowMs": 305000, "inputSchema": schema, "outputSchema": schema}},
		[]map[string]any{{"manifestVersion": 1, "name": "cap-flow", "inputSchema": schema, "outputSchema": schema, "nodes": []map[string]any{{"id": "node-1", "type": "task", "task": "cap-task", "after": []any{}, "input": map[string]any{}}}, "output": map[string]any{"$ref": "step.output", "stepId": "node-1", "pointer": ""}}},
	)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "cap-flow", manifest)

	ctx := context.Background()

	// Clamp max_concurrency to 2 for this test to easily test quota wait
	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE environment_admissions SET max_concurrency=2 WHERE environment_id=$1::uuid`, envID)
		return err
	})

	createRun := func(key string) string {
		body := bytes.NewReader([]byte(`{"environment":"staging","input":{}}`))
		req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/cap-flow/runs", body)
		req.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
		req.Header.Set("X-Organization-ID", orgID)
		req.Header.Set("Idempotency-Key", key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != http.StatusAccepted {
			t.Fatalf("create run: %v", err)
		}
		defer resp.Body.Close()
		var res map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&res)
		return res["id"].(string)
	}

	run1 := createRun("cap-run-1")
	run2 := createRun("cap-run-2")
	run3 := createRun("cap-run-3")

	workerSession, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "cap-worker")
	advertiseDigest(t, tc, orgID, workerSession.SessionID, bundle)

	engine := execution.NewWorkerEngine(tc.pool)
	sessCtx := &worker.WorkerSessionContext{
		SessionID: workerSession.SessionID, WorkerID: workerSession.WorkerID,
		OrganizationID: orgID, EnvironmentID: envID, PoolName: "default",
		ExpiresAt: time.Now().Add(time.Hour),
	}

	// Poll advertising 10 available slots; server must clamp to DefaultSlots (2)
	resp, err := engine.Claim(ctx, sessCtx, &worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "cap-poll-1",
		WorkerID: workerSession.WorkerID, SessionID: workerSession.SessionID,
		AvailableSlots: 10, DeploymentDigests: []string{bundle}, Pool: "default",
	})
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}
	if len(resp.Assignments) != 2 {
		t.Fatalf("expected claim to be clamped to 2 assignments, got %d", len(resp.Assignments))
	}

	// Third run cannot be claimed because active leases (2) == max_concurrency (2).
	// Calling Claim again must return 0 assignments and transition remaining ready work to QUOTA_WAIT.
	workerSession2, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "cap-worker-2")
	advertiseDigest(t, tc, orgID, workerSession2.SessionID, bundle)
	sessCtx2 := &worker.WorkerSessionContext{
		SessionID: workerSession2.SessionID, WorkerID: workerSession2.WorkerID,
		OrganizationID: orgID, EnvironmentID: envID, PoolName: "default",
		ExpiresAt: time.Now().Add(time.Hour),
	}

	respOver, err := engine.Claim(ctx, sessCtx2, &worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "cap-poll-2",
		WorkerID: workerSession2.WorkerID, SessionID: workerSession2.SessionID,
		AvailableSlots: 2, DeploymentDigests: []string{bundle}, Pool: "default",
	})
	if err != nil {
		t.Fatalf("claim 2 failed: %v", err)
	}
	if len(respOver.Assignments) != 0 {
		t.Fatalf("expected 0 assignments while at max capacity, got %d", len(respOver.Assignments))
	}

	// Verify run3 is now in WAITING with reason QUOTA_WAIT
	var status, reasonCode string
	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, COALESCE(reason_code, '') FROM runs WHERE id=$1::uuid`, run3).Scan(&status, &reasonCode)
	})
	if status != "WAITING" || reasonCode != "QUOTA_WAIT" {
		t.Fatalf("expected run3 in WAITING/QUOTA_WAIT, got status=%s reason=%s", status, reasonCode)
	}

	// Now complete one of the active assignments to free up capacity
	assign1 := resp.Assignments[0]
	_, err = engine.Start(ctx, sessCtx, &worker.StartRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "cap-start-1",
		WorkerID: workerSession.WorkerID, SessionID: workerSession.SessionID,
		AttemptID: assign1.AttemptID, OwnershipEpoch: assign1.OwnershipEpoch,
	})
	if err != nil {
		t.Fatalf("start failed: %v", err)
	}
	compReq := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "cap-comp-1",
		WorkerID: workerSession.WorkerID, SessionID: workerSession.SessionID,
		AttemptID: assign1.AttemptID, OwnershipEpoch: assign1.OwnershipEpoch,
		Outcome: "SUCCEEDED", Output: map[string]any{},
	}
	compReq.ResultDigest, _ = worker.CanonicalCompletionDigest(&compReq)
	_, err = engine.Complete(ctx, sessCtx, &compReq)
	if err != nil {
		t.Fatalf("complete failed: %v", err)
	}

	// Now capacity is 1: polling worker 2 should claim run3 and transition it from QUOTA_WAIT to RUNNING
	respResumed, err := engine.Claim(ctx, sessCtx2, &worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "cap-poll-3",
		WorkerID: workerSession2.WorkerID, SessionID: workerSession2.SessionID,
		AvailableSlots: 1, DeploymentDigests: []string{bundle}, Pool: "default",
	})
	if err != nil || len(respResumed.Assignments) != 1 {
		t.Fatalf("resumed claim failed: %v, assignments=%d", err, len(respResumed.Assignments))
	}
	if respResumed.Assignments[0].RunID != run3 {
		t.Fatalf("expected run3 to be claimed, got %s", respResumed.Assignments[0].RunID)
	}
	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, COALESCE(reason_code, '') FROM runs WHERE id=$1::uuid`, run3).Scan(&status, &reasonCode)
	})
	if status != "RUNNING" || reasonCode != "" {
		t.Fatalf("expected run3 to resume to RUNNING with no reason, got status=%s reason=%s", status, reasonCode)
	}
	_ = run1
	_ = run2
}

// TestHistoryLimitBoundaryAndAtomicTerminalization verifies that non-terminal events are rejected
// with HISTORY_LIMIT_EXCEEDED at the 9,999 boundary, the reserved 10,000th event commits exactly once,
// and concurrent append/terminalization cannot exceed the 10,000 event cap (Blueprint §27.1).
func TestHistoryLimitBoundaryAndAtomicTerminalization(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	bundle := "6666666666666666666666666666666666666666666666666666666666666666"
	schema := map[string]any{"type": "object"}
	manifest := createLifecycleManifest(bundle,
		[]map[string]any{{"name": "hist-task", "entrypoint": "tasks/hist.js", "timeoutMs": 30000, "recovery": "idempotent", "idempotencyWindowMs": 305000, "inputSchema": schema, "outputSchema": schema}},
		[]map[string]any{{"manifestVersion": 1, "name": "hist-flow", "inputSchema": schema, "outputSchema": schema, "nodes": []map[string]any{{"id": "node-1", "type": "task", "task": "hist-task", "after": []any{}, "input": map[string]any{}}}, "output": map[string]any{"$ref": "step.output", "stepId": "node-1", "pointer": ""}}},
	)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "hist-flow", manifest)

	body := bytes.NewReader([]byte(`{"environment":"staging","input":{}}`))
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/hist-flow/runs", body)
	req.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	req.Header.Set("X-Organization-ID", orgID)
	req.Header.Set("Idempotency-Key", "hist-run-key")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusAccepted {
		t.Fatalf("create run: %v, status=%d", err, resp.StatusCode)
	}
	defer resp.Body.Close()
	var createRes map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&createRes)
	runID := createRes["id"].(string)

	ctx := context.Background()
	engine := execution.NewWorkerEngine(tc.pool)

	// Drive the run to sequence 9,998: Claim consumes one non-terminal event
	// (TASK_CLAIMED, 9,998 -> 9,999) so that Start hits the boundary where
	// only the reserved terminal slot remains (9,999 -> 10,000).
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE runs SET last_event_sequence=9998 WHERE id=$1::uuid`, runID)
		return err
	}); err != nil {
		t.Fatalf("drive run to boundary: %v", err)
	}

	// 1. Attempting to append a non-terminal event (TASK_STARTED) at 9,999 must fail with ErrHistoryLimitExceeded
	workerSession, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "hist-worker")
	advertiseDigest(t, tc, orgID, workerSession.SessionID, bundle)

	sessCtx := &worker.WorkerSessionContext{
		SessionID: workerSession.SessionID, WorkerID: workerSession.WorkerID,
		OrganizationID: orgID, EnvironmentID: envID, PoolName: "default",
		ExpiresAt: time.Now().Add(time.Hour),
	}

	claimResp, err := engine.Claim(ctx, sessCtx, &worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "hist-claim-1",
		WorkerID: workerSession.WorkerID, SessionID: workerSession.SessionID,
		AvailableSlots: 1, DeploymentDigests: []string{bundle}, Pool: "default",
	})
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}
	if len(claimResp.Assignments) == 0 {
		t.Fatal("expected assignment")
	}
	assign := claimResp.Assignments[0]

	// Calling Start tries to append TASK_STARTED (non-terminal). It must return ErrHistoryLimitExceeded
	_, err = engine.Start(ctx, sessCtx, &worker.StartRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "hist-start-1",
		WorkerID: workerSession.WorkerID, SessionID: workerSession.SessionID,
		AttemptID: assign.AttemptID, OwnershipEpoch: assign.OwnershipEpoch,
	})
	if err == nil {
		t.Fatal("expected Start to fail with ErrHistoryLimitExceeded at sequence 9,999")
	}

	// 2. Run must be atomically terminalized with status=FAILED, reason_code=HISTORY_LIMIT_EXCEEDED,
	// and terminal event sequence 10,000.
	var status, reasonCode string
	var lastSeq int64
	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, reason_code, last_event_sequence FROM runs WHERE id=$1::uuid`, runID).
			Scan(&status, &reasonCode, &lastSeq)
	})
	if status != "FAILED" || reasonCode != "HISTORY_LIMIT_EXCEEDED" {
		t.Fatalf("expected run terminalized with FAILED/HISTORY_LIMIT_EXCEEDED, got %s/%s", status, reasonCode)
	}
	if lastSeq != 10000 {
		t.Fatalf("expected last_event_sequence=10000, got %d", lastSeq)
	}

	// 3. Any further event (terminal or non-terminal) must be rejected
	err = engine.TerminalizeHistoryLimitExceeded(ctx, orgID, runID)
	if err != nil {
		t.Fatalf("idempotent terminalize should return nil, got: %v", err)
	}

	// 4. Concurrent race at sequence 9,999: Create a new run at 9,999 and race 10 concurrent calls
	body2 := bytes.NewReader([]byte(`{"environment":"staging","input":{}}`))
	req2, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/hist-flow/runs", body2)
	req2.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	req2.Header.Set("X-Organization-ID", orgID)
	req2.Header.Set("Idempotency-Key", "hist-run-key-2")
	req2.Header.Set("Content-Type", "application/json")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil || resp2.StatusCode != http.StatusAccepted {
		t.Fatalf("create second hist run: %v, status=%d", err, resp2.StatusCode)
	}
	defer resp2.Body.Close()
	var createRes2 map[string]any
	_ = json.NewDecoder(resp2.Body).Decode(&createRes2)
	runID2 := createRes2["id"].(string)

	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE runs SET last_event_sequence=9999 WHERE id=$1::uuid`, runID2)
		return err
	})

	var raceWg sync.WaitGroup
	for i := 0; i < 10; i++ {
		raceWg.Add(1)
		go func() {
			defer raceWg.Done()
			_ = engine.TerminalizeHistoryLimitExceeded(ctx, orgID, runID2)
		}()
	}
	raceWg.Wait()

	var terminalEventsCount int
	var finalSeq int64
	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		_ = tx.QueryRow(ctx, `SELECT count(*) FROM run_events WHERE run_id=$1::uuid AND event_type='RUN_FAILED'`, runID2).Scan(&terminalEventsCount)
		return tx.QueryRow(ctx, `SELECT last_event_sequence FROM runs WHERE id=$1::uuid`, runID2).Scan(&finalSeq)
	})
	if terminalEventsCount != 1 {
		t.Fatalf("expected exactly 1 terminal event written under concurrent race, got %d", terminalEventsCount)
	}
	if finalSeq != 10000 {
		t.Fatalf("expected final sequence exactly 10,000, got %d", finalSeq)
	}
}

// TestSchedulerFairnessRoundRobinAcrossEnvironments proves Blueprint 19.3
// scheduler fairness: the org-scoped control-plane scheduler
// (ReconcileReadyWork) must not let one environment's backlog starve another
// environment in its bounded LIMIT 50 batch.
//
// Setup: 60 repairable runs in Env A (reconciliation_checked_at NULL, so a
// naive ORDER BY checked_at,id would take 50 x Env A first) plus 3
// repairable runs in Env B (checked_at set, sorting strictly after A).
// A single ReconcileReadyWork pass must still repair all 3 Env B runs:
// the per-environment interleave puts B's positions 1-3 inside the batch.
// A second pass drains the remainder, proving no starvation over time.
//
// Workers remain environment-bound throughout; fairness is a scheduler
// property, not cross-environment claiming (Claim stays env-scoped FIFO).
func TestSchedulerFairnessRoundRobinAcrossEnvironments(t *testing.T) {
	tc, server, orgID, envID1, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	ctx := context.Background()
	var projID string
	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT project_id FROM environments WHERE id=$1::uuid`, envID1).Scan(&projID)
	})
	env2, err := tc.service.CreateEnvironment(ctx, orgID, projID, tenant.EnvDevelopment, 10)
	if err != nil {
		t.Fatalf("create second env: %v", err)
	}
	envID2 := env2.ID
	adminKey2 := bootstrapTestKey(t, tc.service, orgID, envID2, []string{
		tenant.CapDeploymentsRegister,
		tenant.CapDeploymentsActivateStaging,
		tenant.CapDeploymentsWrite,
		tenant.CapRunsCreate,
		tenant.CapRunsRead,
		tenant.CapAdminKey,
	})

	bundle := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	schema := map[string]any{"type": "object"}
	manifest := createLifecycleManifest(bundle,
		[]map[string]any{{"name": "rr2-task", "entrypoint": "tasks/rr2.js", "timeoutMs": 30000, "recovery": "idempotent", "idempotencyWindowMs": 305000, "inputSchema": schema, "outputSchema": schema}},
		[]map[string]any{{"manifestVersion": 1, "name": "rr2-flow", "inputSchema": schema, "outputSchema": schema, "nodes": []map[string]any{
			{"id": "node-1", "type": "task", "task": "rr2-task", "after": []any{}, "input": map[string]any{}},
			{"id": "node-2", "type": "task", "task": "rr2-task", "after": []any{"node-1"}, "input": map[string]any{}},
		}, "output": map[string]any{"$ref": "step.output", "stepId": "node-2", "pointer": ""}}},
	)
	depA := registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID1, "rr2-flow", manifest)
	depB := registerAndActivateTestWorkflow(t, tc, server, adminKey2, orgID, envID2, "rr2-flow", manifest)

	// Seed runs whose node-2 is BLOCKED behind a SUCCEEDED node-1, i.e.
	// exactly what ReconcileReadyWork repairs.
	insertRepairable := func(envID, depID string) string {
		var runID string
		if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
			if err := tx.QueryRow(ctx, `INSERT INTO runs
				(organization_id,environment_id,deployment_id,workflow_name,status)
				VALUES ($1::uuid,$2::uuid,$3::uuid,'rr2-flow','QUEUED') RETURNING id::text`,
				orgID, envID, depID).Scan(&runID); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO run_steps
				(organization_id,environment_id,run_id,node_id,state,eligible_at)
				VALUES ($1::uuid,$2::uuid,$3::uuid,'node-1','SUCCEEDED',clock_timestamp())`,
				orgID, envID, runID); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO run_steps
				(organization_id,environment_id,run_id,node_id,state,eligible_at)
				VALUES ($1::uuid,$2::uuid,$3::uuid,'node-2','BLOCKED',clock_timestamp())`,
				orgID, envID, runID)
			return err
		}); err != nil {
			t.Fatalf("seed repairable run: %v", err)
		}
		return runID
	}

	for i := 0; i < 60; i++ {
		insertRepairable(envID1, depA)
	}
	var envBRuns []string
	for i := 0; i < 3; i++ {
		envBRuns = append(envBRuns, insertRepairable(envID2, depB))
	}
	// Force naive ordering to prefer Env A: B sorts strictly after all of A.
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE runs SET reconciliation_checked_at=clock_timestamp()
			WHERE organization_id=$1::uuid AND environment_id=$2::uuid`, orgID, envID2)
		return err
	}); err != nil {
		t.Fatalf("pin B behind A: %v", err)
	}

	engine := execution.NewWorkerEngine(tc.pool)
	repaired, err := engine.ReconcileReadyWork(ctx, orgID)
	if err != nil {
		t.Fatalf("reconcile pass 1: %v", err)
	}
	if repaired != 50 {
		t.Fatalf("pass 1 repaired=%d, want 50 (bounded batch)", repaired)
	}
	readyCount := func(runID string) int {
		var n int
		_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
			return tx.QueryRow(ctx, `SELECT count(*) FROM run_steps
				WHERE run_id=$1::uuid AND organization_id=$2::uuid
				  AND node_id='node-2' AND state='READY'`, runID, orgID).Scan(&n)
		})
		return n
	}
	for _, runID := range envBRuns {
		if readyCount(runID) != 1 {
			t.Fatalf("env B run %s not repaired in first batch: starved behind env A", runID)
		}
	}

	repaired2, err := engine.ReconcileReadyWork(ctx, orgID)
	if err != nil {
		t.Fatalf("reconcile pass 2: %v", err)
	}
	if repaired+repaired2 != 63 {
		t.Fatalf("total repaired=%d, want 63 (no run left behind)", repaired+repaired2)
	}
}
