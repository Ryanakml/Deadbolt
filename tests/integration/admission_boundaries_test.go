package integration_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/storage/migrator"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

// TestWorkerSessionQuotaBoundary proves the 10 live worker-session cap per
// environment: 10 enrollments succeed, the 11th is rejected with 429
// SESSION_QUOTA_EXCEEDED + Retry-After, and a reconnect that fences its own
// old session does not consume an extra slot.
func TestWorkerSessionQuotaBoundary(t *testing.T) {
	tc, server, orgID, envID, _ := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()
	ctx := context.Background()

	enrollKey := bootstrapTestKey(t, tc.service, orgID, envID, []string{tenant.CapDeploymentsWrite, tenant.CapWorkersDrain, tenant.CapAdminKey})

	// enrollOne performs the full token/challenge/enroll flow, returning the
	// worker identity on success so the caller can reconnect later.
	enrollOne := func(suffix string) (status int, code, retryAfter, workerID, sessionID string, priv ed25519.PrivateKey) {
		body, _ := json.Marshal(map[string]any{"poolName": "default"})
		req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/environments/%s/worker-enrollments", server.URL, envID), bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+enrollKey.PlaintextKey)
		req.Header.Set("X-Organization-ID", orgID)
		req.Header.Set("Idempotency-Key", "boundary-enroll-"+suffix)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("enrollment token request failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			return resp.StatusCode, "", resp.Header.Get("Retry-After"), "", "", nil
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
		raw, _ := io.ReadAll(enrollResp.Body)
		var decoded map[string]any
		_ = json.Unmarshal(raw, &decoded)
		qcode, _ := decoded["code"].(string)
		if enrollResp.StatusCode != http.StatusOK {
			return enrollResp.StatusCode, qcode, enrollResp.Header.Get("Retry-After"), "", "", nil
		}
		var sess worker.SessionResponseDTO
		if err := json.Unmarshal(raw, &sess); err != nil {
			t.Fatal(err)
		}
		return enrollResp.StatusCode, "", "", sess.WorkerID, sess.SessionID, privateKey
	}

	for i := 1; i <= 9; i++ {
		status, code, _, _, _, _ := enrollOne(fmt.Sprintf("ok-%d", i))
		if status != http.StatusOK {
			t.Fatalf("enrollment %d: status=%d code=%s, want 200", i, status, code)
		}
	}
	status, code, _, tenthWorker, tenthSession, tenthPriv := enrollOne("ok-10")
	if status != http.StatusOK {
		t.Fatalf("enrollment 10: status=%d code=%s, want 200", status, code)
	}
	status, code, retryAfter, _, _, _ := enrollOne("over-quota")
	if status != http.StatusTooManyRequests {
		t.Fatalf("11th enrollment: status=%d, want 429", status)
	}
	if code != "SESSION_QUOTA_EXCEEDED" {
		t.Fatalf("11th enrollment: code=%q, want SESSION_QUOTA_EXCEEDED", code)
	}
	if retryAfter == "" {
		t.Fatal("11th enrollment: missing Retry-After header")
	}

	// Reconnect fences the worker's own old session before the quota check,
	// so it succeeds at the cap without growing the live count.
	chalReq, _ := json.Marshal(worker.ChallengeRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "reconnect-chal",
		WorkerID: tenthWorker,
	})
	cResp, err := http.Post(server.URL+"/worker/v1/challenge", "application/json", bytes.NewReader(chalReq))
	if err != nil || cResp.StatusCode != http.StatusOK {
		t.Fatalf("reconnect challenge failed: %v", err)
	}
	var challenge worker.ChallengeResponseDTO
	_ = json.NewDecoder(cResp.Body).Decode(&challenge)
	cResp.Body.Close()
	sessionReq, _ := json.Marshal(worker.SessionRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "reconnect-session",
		WorkerID: tenthWorker, Nonce: challenge.Nonce,
		Signature: worker.SignChallenge(tenthPriv, challenge.Nonce),
	})
	sResp, err := http.Post(server.URL+"/worker/v1/session", "application/json", bytes.NewReader(sessionReq))
	if err != nil || sResp.StatusCode != http.StatusOK {
		t.Fatalf("reconnect session failed: %v status=%d", err, sResp.StatusCode)
	}
	var sess2 worker.SessionResponseDTO
	_ = json.NewDecoder(sResp.Body).Decode(&sess2)
	sResp.Body.Close()
	if sess2.SessionID == "" || sess2.SessionID == tenthSession {
		t.Fatal("expected fresh session on reconnect")
	}
	var liveCount int
	var oldRevoked *time.Time
	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM worker_sessions
			WHERE organization_id=$1::uuid AND environment_id=$2::uuid
			  AND revoked_at IS NULL AND expires_at > clock_timestamp()`, orgID, envID).Scan(&liveCount); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT revoked_at FROM worker_sessions WHERE id=$1::uuid`, tenthSession).Scan(&oldRevoked)
	})
	if oldRevoked == nil {
		t.Fatal("reconnect did not fence the old session")
	}
	if liveCount != 10 {
		t.Fatalf("live sessions=%d, want 10 after fenced reconnect", liveCount)
	}
	status, _, _, _, _, _ = enrollOne("still-over-quota")
	if status != http.StatusTooManyRequests {
		t.Fatalf("post-reconnect enrollment: status=%d, want 429", status)
	}
}

// TestWorkflowNodeCountBoundary proves the 50-node MVP cap: a 50-node
// workflow admits runs, while a 51-node workflow is rejected with
// WORKFLOW_TOO_LARGE at run creation.
func TestWorkflowNodeCountBoundary(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	// Linear chain: the manifest contract allows one root and at most one
	// predecessor/successor per node.
	buildNodes := func(n int) []map[string]any {
		nodes := make([]map[string]any, 0, n)
		for i := 0; i < n; i++ {
			after := []any{}
			if i > 0 {
				after = []any{fmt.Sprintf("n-%d", i-1)}
			}
			nodes = append(nodes, map[string]any{"id": fmt.Sprintf("n-%d", i), "type": "task", "task": "bn-task", "after": after, "input": map[string]any{}})
		}
		return nodes
	}
	schema := map[string]any{"type": "object"}
	buildManifest := func(bundle string, nodes []map[string]any, flow string) []byte {
		last := fmt.Sprintf("n-%d", len(nodes)-1)
		return createLifecycleManifest(bundle,
			[]map[string]any{{"name": "bn-task", "entrypoint": "tasks/bn.js", "timeoutMs": 30000, "recovery": "idempotent", "idempotencyWindowMs": 305000, "inputSchema": schema, "outputSchema": schema}},
			[]map[string]any{{"manifestVersion": 1, "name": flow, "inputSchema": schema, "outputSchema": schema, "nodes": nodes, "output": map[string]any{"$ref": "step.output", "stepId": last, "pointer": ""}}},
		)
	}
	createRun := func(flow, key string) (int, map[string]any) {
		body := bytes.NewReader([]byte(`{"environment":"staging","input":{}}`))
		req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/"+flow+"/runs", body)
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
		return resp.StatusCode, resBody
	}

	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "bn-flow-50",
		buildManifest("b000000000000000000000000000000000000000000000000000000000000001", buildNodes(50), "bn-flow-50"))
	if status, _ := createRun("bn-flow-50", "bn-50"); status != http.StatusAccepted {
		t.Fatalf("50-node workflow run: status=%d, want 202", status)
	}

	// The public contract boundary rejects 51 nodes at registration.
	manifest51 := buildManifest("b000000000000000000000000000000000000000000000000000000000000002", buildNodes(51), "bn-flow-51")
	regReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/deployments?environment=%s", server.URL, envID), bytes.NewReader(manifest51))
	regReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	regReq.Header.Set("X-Organization-ID", orgID)
	regReq.Header.Set("Content-Type", "application/json")
	regReq.Header.Set("Idempotency-Key", "bn-reg-51")
	regResp, err := http.DefaultClient.Do(regReq)
	if err != nil {
		t.Fatal(err)
	}
	defer regResp.Body.Close()
	var regBody map[string]any
	_ = json.NewDecoder(regResp.Body).Decode(&regBody)
	if regResp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("51-node register: status=%d, want 422", regResp.StatusCode)
	}
	if code, _ := regBody["code"].(string); code != "NODE_COUNT_EXCEEDED" {
		t.Fatalf("51-node register: code=%v, want NODE_COUNT_EXCEEDED", regBody["code"])
	}

	// The execution service redundantly enforces the same 50-node cap as
	// defense in depth: a 51-node deployment smuggled past contract
	// validation (direct persistence) is still rejected at run creation
	// with WORKFLOW_TOO_LARGE.
	var smuggledDep string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		var manifestJSON struct {
			Tasks     []map[string]any `json:"tasks"`
			Workflows []map[string]any `json:"workflows"`
		}
		if err := json.Unmarshal(manifest51, &manifestJSON); err != nil {
			return err
		}
		nodesJSON, _ := json.Marshal(manifestJSON.Workflows[0]["nodes"])
		inputJSON, _ := json.Marshal(manifestJSON.Workflows[0]["inputSchema"])
		outputJSON, _ := json.Marshal(manifestJSON.Workflows[0]["outputSchema"])
		wfOutputJSON, _ := json.Marshal(manifestJSON.Workflows[0]["output"])
		taskInputJSON, _ := json.Marshal(manifestJSON.Tasks[0]["inputSchema"])
		taskOutputJSON, _ := json.Marshal(manifestJSON.Tasks[0]["outputSchema"])
		if err := tx.QueryRow(ctx, `INSERT INTO deployments
			(organization_id,environment_id,manifest_hash,bundle_digest,manifest,protocol_version,runtime_version)
			VALUES ($1::uuid,$2::uuid,'smuggled-51-nodes','b000000000000000000000000000000000000000000000000000000000000002',$3::jsonb,1,'1.0')
			RETURNING id::text`, orgID, envID, string(manifest51)).Scan(&smuggledDep); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO task_definitions
			(organization_id,deployment_id,name,entrypoint,input_schema,output_schema,recovery_policy,timeout_ms,max_attempts,initial_delay_ms,max_delay_ms,idempotency_window_ms)
			VALUES ($1::uuid,$2::uuid,'bn-task','tasks/bn.js',$3::jsonb,$4::jsonb,'idempotent',30000,3,1000,30000,305000)`,
			orgID, smuggledDep, string(taskInputJSON), string(taskOutputJSON)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO workflow_definitions
			(organization_id,deployment_id,name,input_schema,output_schema,nodes,output_mapping)
			VALUES ($1::uuid,$2::uuid,'bn-flow-51',$3::jsonb,$4::jsonb,$5::jsonb,$6::jsonb)`,
			orgID, smuggledDep, string(inputJSON), string(outputJSON), string(nodesJSON), string(wfOutputJSON)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO workflow_channels
			(environment_id,organization_id,workflow_name,active_deployment_id,revision)
			VALUES ($1::uuid,$2::uuid,'bn-flow-51',$3::uuid,1)`, envID, orgID, smuggledDep)
		return err
	}); err != nil {
		t.Fatalf("smuggle 51-node deployment: %v", err)
	}
	status, resBody := createRun("bn-flow-51", "bn-51-svc")
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("51-node service run: status=%d, want 422", status)
	}
	if code, _ := resBody["code"].(string); code != "WORKFLOW_TOO_LARGE" {
		t.Fatalf("51-node service run: code=%v, want WORKFLOW_TOO_LARGE", resBody["code"])
	}
}

// resetCreateRateBucket pins the per-env token bucket full so run-quota
// assertions are not distorted by the 5/sec burst-10 create limiter.
func resetCreateRateBucket(t *testing.T, tc *tenantTestContext, orgID, envID string) {
	t.Helper()
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE environment_admissions
			SET create_rate_tokens=10, create_rate_updated_at=clock_timestamp()
			WHERE environment_id=$1::uuid AND organization_id=$2::uuid`, envID, orgID)
		return err
	}); err != nil {
		t.Fatalf("reset rate bucket: %v", err)
	}
}

// TestNonterminalRunQuotaBoundary proves the 100 nonterminal-run cap: with
// 100 live runs the next admission gets 429 RUN_QUOTA_EXCEEDED +
// Retry-After, terminal runs do not consume quota, and the freed slot
// admits exactly one more run before throttling again.
func TestNonterminalRunQuotaBoundary(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()
	ctx := context.Background()

	bundle := "b222222222222222222222222222222222222222222222222222222222222222"
	schema := map[string]any{"type": "object"}
	manifest := createLifecycleManifest(bundle,
		[]map[string]any{{"name": "rq-task", "entrypoint": "tasks/rq.js", "timeoutMs": 30000, "recovery": "idempotent", "idempotencyWindowMs": 305000, "inputSchema": schema, "outputSchema": schema}},
		[]map[string]any{{"manifestVersion": 1, "name": "rq-flow", "inputSchema": schema, "outputSchema": schema, "nodes": []map[string]any{{"id": "node-1", "type": "task", "task": "rq-task", "after": []any{}, "input": map[string]any{}}}, "output": map[string]any{"$ref": "step.output", "stepId": "node-1", "pointer": ""}}},
	)
	depID := registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "rq-flow", manifest)

	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		for i := 0; i < 100; i++ {
			if _, err := tx.Exec(ctx, `INSERT INTO runs
				(organization_id,environment_id,deployment_id,workflow_name,status)
				VALUES ($1::uuid,$2::uuid,$3::uuid,'rq-flow','QUEUED')`, orgID, envID, depID); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed 100 runs: %v", err)
	}

	createRun := func(key string) (int, map[string]any, string) {
		body := bytes.NewReader([]byte(`{"environment":"staging","input":{}}`))
		req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/rq-flow/runs", body)
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
		return resp.StatusCode, resBody, resp.Header.Get("Retry-After")
	}

	resetCreateRateBucket(t, tc, orgID, envID)
	status, resBody, retryAfter := createRun("rq-over-quota")
	if status != http.StatusTooManyRequests {
		t.Fatalf("101st run: status=%d, want 429", status)
	}
	if code, _ := resBody["code"].(string); code != "RUN_QUOTA_EXCEEDED" {
		t.Fatalf("101st run: code=%v, want RUN_QUOTA_EXCEEDED", resBody["code"])
	}
	if retryAfter == "" {
		t.Fatal("101st run: missing Retry-After header")
	}

	// Terminal runs do not consume nonterminal quota: completing one frees a slot.
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE runs SET status='SUCCEEDED', updated_at=clock_timestamp()
			WHERE id=(SELECT id FROM runs WHERE organization_id=$1::uuid AND environment_id=$2::uuid AND status='QUEUED' LIMIT 1)`, orgID, envID)
		return err
	}); err != nil {
		t.Fatalf("terminalize one run: %v", err)
	}
	resetCreateRateBucket(t, tc, orgID, envID)
	if status, _, _ := createRun("rq-after-free"); status != http.StatusAccepted {
		t.Fatalf("run after freeing slot: status=%d, want 202", status)
	}
	resetCreateRateBucket(t, tc, orgID, envID)
	if status, _, _ := createRun("rq-full-again"); status != http.StatusTooManyRequests {
		t.Fatalf("run at restored cap: status=%d, want 429", status)
	}
}

// TestMigration22To23 verifies the additive migration step owned by this
// issue: an existing schema-22 database migrates cleanly to 23 with the
// admission rate/limit objects present.
func TestMigration22To23(t *testing.T) {
	db, runtimePool, _ := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()
	ctx := context.Background()
	runner := migrator.NewRunner(db, "../../migrations")
	if err := runner.DownTo(ctx, 22); err != nil {
		t.Fatalf("down to 22: %v", err)
	}
	if v, _ := runner.Version(ctx); v != 22 {
		t.Fatalf("down version=%d, want 22", v)
	}
	if err := runner.Up(ctx); err != nil {
		t.Fatalf("up to 23: %v", err)
	}
	if v, _ := runner.Version(ctx); v != 23 {
		t.Fatalf("up version=%d, want 23", v)
	}
	var tokens, updatedAt bool
	if err := db.QueryRow(`SELECT
		EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='environment_admissions' AND column_name='create_rate_tokens'),
		EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='environment_admissions' AND column_name='create_rate_updated_at')`).Scan(&tokens, &updatedAt); err != nil {
		t.Fatal(err)
	}
	if !tokens || !updatedAt {
		t.Fatalf("00023 columns missing after 22->23: tokens=%v updated_at=%v", tokens, updatedAt)
	}
}

// TestLogPressurePreservesCorrectnessEvents proves the Issue #23 log contract:
// diagnostic flood causes bounded drops with a counter, while execution
// correctness events remain durable. A run whose attempt first exhausts its
// 1 MiB log budget still starts, completes and reaches SUCCEEDED with its
// TASK_STARTED/COMPLETED events intact and no TASK_LOST.
func TestLogPressurePreservesCorrectnessEvents(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()
	ctx := context.Background()

	_, bundleDigest := setupInspectorWorkflow(t, tc, server, orgID, envID, adminKey)
	sessionCtx, sessionToken := enrollTestWorker(t, tc, server, orgID, envID, bundleDigest)

	createReq, _ := http.NewRequest("POST", server.URL+"/v1/workflows/inspector-workflow/runs", strings.NewReader(`{"environment":"staging","input":{"val":"pressure-test"}}`))
	createReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	createReq.Header.Set("Idempotency-Key", "log-pressure-run")
	createReq.Header.Set("Content-Type", "application/json")
	createRes, err := http.DefaultClient.Do(createReq)
	if err != nil || createRes.StatusCode != http.StatusAccepted {
		t.Fatalf("create run: %v status=%d", err, createRes.StatusCode)
	}
	var runResp execution.RunDTO
	_ = json.NewDecoder(createRes.Body).Decode(&runResp)
	createRes.Body.Close()

	pollBody, _ := json.Marshal(worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "p-pressure",
		WorkerID: sessionCtx.WorkerID, SessionID: sessionCtx.SessionID,
		AvailableSlots: 1, DeploymentDigests: []string{bundleDigest}, Pool: "default",
	})
	pReq, _ := http.NewRequest("POST", server.URL+"/worker/v1/poll", bytes.NewReader(pollBody))
	pReq.Header.Set("Authorization", "Bearer "+sessionToken)
	pReq.Header.Set("Content-Type", "application/json")
	pRes, _ := http.DefaultClient.Do(pReq)
	var pollResp worker.PollResponseDTO
	_ = json.NewDecoder(pRes.Body).Decode(&pollResp)
	pRes.Body.Close()
	if len(pollResp.Assignments) != 1 {
		t.Fatalf("expected 1 assignment, got %d", len(pollResp.Assignments))
	}
	as := pollResp.Assignments[0]

	// Flood diagnostics: 64 x 16 KiB fills the 1 MiB attempt budget, then one
	// more line forces BudgetExhausted with drop accounting.
	ingest := func(seq int64, msg string) worker.AckResponseDTO {
		batch := worker.LogBatchRequestDTO{
			ProtocolVersion: 1, RequestID: fmt.Sprintf("pressure-%d", seq),
			WorkerID: sessionCtx.WorkerID, SessionID: sessionCtx.SessionID,
			AttemptID: as.AttemptID,
			Records: []worker.LogRecordDTO{
				{Sequence: seq, Level: "info", Message: msg, Timestamp: time.Now().UTC().Format(time.RFC3339Nano)},
			},
		}
		raw, _ := json.Marshal(batch)
		lReq, _ := http.NewRequest("POST", server.URL+"/worker/v1/logs", bytes.NewReader(raw))
		lReq.Header.Set("Authorization", "Bearer "+sessionToken)
		lReq.Header.Set("Content-Type", "application/json")
		lRes, err := http.DefaultClient.Do(lReq)
		if err != nil || lRes.StatusCode != http.StatusOK {
			t.Fatalf("ingest %d failed: %v", seq, err)
		}
		var ack worker.AckResponseDTO
		_ = json.NewDecoder(lRes.Body).Decode(&ack)
		lRes.Body.Close()
		return ack
	}
	filler := strings.Repeat("F", 16*1024)
	for seq := int64(1); seq <= 64; seq++ {
		ingest(seq, filler)
	}
	finalAck := ingest(65, "over-budget")
	if !finalAck.BudgetExhausted || finalAck.DroppedCount == 0 {
		t.Fatalf("expected budget exhausted with drops, got %+v", finalAck)
	}

	// Correctness path must be unaffected: start, complete, terminalize.
	var startResp worker.StartResponseDTO
	if status := postWorkerJSON(t, server, "/worker/v1/start", sessionToken, worker.StartRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "start-pressure",
		WorkerID: sessionCtx.WorkerID, SessionID: sessionCtx.SessionID,
		AttemptID: as.AttemptID, OwnershipEpoch: as.OwnershipEpoch,
	}, &startResp); status != http.StatusOK || !startResp.Accepted {
		t.Fatalf("start after log flood rejected: %d", status)
	}
	comp := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "complete-pressure",
		WorkerID: sessionCtx.WorkerID, SessionID: sessionCtx.SessionID,
		AttemptID: as.AttemptID, OwnershipEpoch: as.OwnershipEpoch,
		Outcome: "SUCCEEDED", Output: map[string]any{"val": "pressure-test"},
	}
	comp.ResultDigest, _ = worker.CanonicalCompletionDigest(&comp)
	var compResp worker.CompleteResponseDTO
	if status := postWorkerJSON(t, server, "/worker/v1/complete", sessionToken, comp, &compResp); status != http.StatusOK || !compResp.Accepted {
		t.Fatalf("complete after log flood rejected: %d", status)
	}

	var runStatus string
	var taskStarted, taskCompleted, taskLost int
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid`, runResp.ID).Scan(&runStatus); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM run_events WHERE run_id=$1::uuid AND event_type='TASK_STARTED'`, runResp.ID).Scan(&taskStarted); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM run_events WHERE run_id=$1::uuid AND event_type IN ('TASK_COMPLETED','STEP_SUCCEEDED')`, runResp.ID).Scan(&taskCompleted); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT count(*) FROM run_events WHERE run_id=$1::uuid AND event_type='TASK_LOST'`, runResp.ID).Scan(&taskLost)
	}); err != nil {
		t.Fatal(err)
	}
	if runStatus != "SUCCEEDED" {
		t.Fatalf("run status=%s, want SUCCEEDED despite log flood", runStatus)
	}
	if taskStarted == 0 || taskCompleted == 0 {
		t.Fatalf("correctness events missing after flood: started=%d completed=%d", taskStarted, taskCompleted)
	}
	if taskLost != 0 {
		t.Fatalf("unexpected TASK_LOST=%d after log flood", taskLost)
	}
}
