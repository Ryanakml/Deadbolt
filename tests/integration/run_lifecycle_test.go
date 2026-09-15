package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"github.com/Ryanakml/Deadbolt/internal/controlplane"
	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

func setupRunLifecycleTest(t *testing.T) (*tenantTestContext, *httptest.Server, string, string, *tenant.GeneratedKey) {
	t.Helper()
	tc := setupTenantContext(t)
	ctx := context.Background()

	owner, _ := tenant.NewUUID()
	org, err := tc.service.CreateOrganization(ctx, owner, "Execution Lifecycle Org")
	if err != nil {
		t.Fatalf("create org failed: %v", err)
	}

	project, err := tc.service.CreateProject(ctx, org.ID, "Lifecycle Project")
	if err != nil {
		t.Fatalf("create project failed: %v", err)
	}

	env, err := tc.service.CreateEnvironment(ctx, org.ID, project.ID, tenant.EnvStaging, 5)
	if err != nil {
		t.Fatalf("create env failed: %v", err)
	}

	prodMux := controlplane.BuildMux(tc.authCfg, tc.runtimePool, nil, nil)
	server := httptest.NewServer(prodMux)

	adminKey := bootstrapTestKey(t, tc.service, org.ID, env.ID, []string{
		tenant.CapDeploymentsRegister,
		tenant.CapDeploymentsActivateStaging,
		tenant.CapDeploymentsWrite,
		tenant.CapWorkersDrain,
		tenant.CapRunsCreate,
		tenant.CapRunsRead,
		tenant.CapAdminKey,
	})

	return tc, server, org.ID, env.ID, adminKey
}

func createLifecycleManifest(bundle string, tasks []map[string]any, workflows []map[string]any) []byte {
	m := map[string]any{
		"manifestVersion":      1,
		"sdkVersion":           "1.0.0",
		"protocolMajor":        1,
		"nodeRuntimeMajor":     24,
		"targetOS":             "linux",
		"targetArchitecture":   "amd64",
		"dependencyLockDigest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bundleDigest":         bundle,
		"secretNames":          []string{},
		"tasks":                tasks,
		"workflows":            workflows,
	}
	b, _ := json.Marshal(m)
	return b
}

func registerAndActivateTestWorkflow(t *testing.T, tc *tenantTestContext, server *httptest.Server, apiKey *tenant.GeneratedKey, orgID, envID, workflowName string, manifestJSON []byte) string {
	t.Helper()
	// 1. Register deployment
	regReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/deployments?environment=%s", server.URL, envID), bytes.NewReader(manifestJSON))
	regReq.Header.Set("Authorization", "Bearer "+apiKey.PlaintextKey)
	regReq.Header.Set("X-Organization-ID", orgID)
	regReq.Header.Set("Content-Type", "application/json")
	regReq.Header.Set("Idempotency-Key", fmt.Sprintf("dep-reg-%s-%d", workflowName, time.Now().UnixNano()))
	regResp, err := http.DefaultClient.Do(regReq)
	if err != nil {
		t.Fatalf("register deployment: %v", err)
	}
	defer regResp.Body.Close()
	if regResp.StatusCode != http.StatusOK && regResp.StatusCode != http.StatusCreated {
		var errEnv map[string]any
		_ = json.NewDecoder(regResp.Body).Decode(&errEnv)
		t.Fatalf("register deployment failed: status %d body %+v", regResp.StatusCode, errEnv)
	}
	var regResult struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(regResp.Body).Decode(&regResult); err != nil {
		t.Fatalf("decode register result: %v", err)
	}

	// 1.5. Seed active worker with bundle deployment to satisfy preflight in staging
	workerID, _ := tenant.NewUUID()
	sessionID, _ := tenant.NewUUID()
	var m struct {
		BundleDigest string `json:"bundleDigest"`
	}
	_ = json.Unmarshal(manifestJSON, &m)
	_ = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO workers (id,organization_id,environment_id,public_key,status) VALUES ($1,$2,$3,'pk','ACTIVE')`, workerID, orgID, envID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO worker_sessions (id,organization_id,worker_id,environment_id,session_token_hash,expires_at) VALUES ($1,$2,$3,$4,'h',clock_timestamp()+interval '1 hour')`, sessionID, orgID, workerID, envID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1,$2,$3)`, sessionID, orgID, m.BundleDigest); err != nil {
			return err
		}
		return nil
	})

	// 2. Activate deployment
	actBody, _ := json.Marshal(map[string]any{
		"deploymentId":      regResult.ID,
		"expectedRevision":  0,
		"allowSingleWorker": true,
	})
	actReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/workflows/%s/activate?environment=%s", server.URL, workflowName, envID), bytes.NewReader(actBody))
	actReq.Header.Set("Authorization", "Bearer "+apiKey.PlaintextKey)
	actReq.Header.Set("X-Organization-ID", orgID)
	actReq.Header.Set("Content-Type", "application/json")
	actReq.Header.Set("Idempotency-Key", fmt.Sprintf("dep-act-%s-%d", workflowName, time.Now().UnixNano()))
	actResp, err := http.DefaultClient.Do(actReq)
	if err != nil {
		t.Fatalf("activate workflow: %v", err)
	}
	defer actResp.Body.Close()
	if actResp.StatusCode != http.StatusOK {
		var errEnv map[string]any
		_ = json.NewDecoder(actResp.Body).Decode(&errEnv)
		t.Fatalf("activate workflow failed: status %d body %+v", actResp.StatusCode, errEnv)
	}

	return regResult.ID
}

// TestCreateRunIdempotencyAnd202 proves:
// 1. 202 occurs after run, steps, input, idempotency, events, and outbox commit.
// 2. Replay with same key + same payload returns original run.
// 3. Replay with same key + changed payload returns 409 IDEMPOTENCY_CONFLICT.
func TestCreateRunIdempotencyAnd202(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	bundle := "1111111111111111111111111111111111111111111111111111111111111111"
	schema := map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"val": map[string]any{"type": "string"}},
		"required":             []any{"val"},
		"additionalProperties": false,
	}
	tasks := []map[string]any{
		{
			"name":                "task-simple",
			"entrypoint":          "tasks/simple.js",
			"timeoutMs":           30000,
			"recovery":            "idempotent",
			"idempotencyWindowMs": 305000,
			"inputSchema":         schema,
			"outputSchema":        schema,
		},
	}
	workflows := []map[string]any{
		{
			"manifestVersion": 1,
			"name":            "simple-flow",
			"inputSchema":     schema,
			"outputSchema":    schema,
			"nodes": []map[string]any{
				{
					"id":    "step-1",
					"type":  "task",
					"task":  "task-simple",
					"after": []any{},
					"input": map[string]any{
						"val": map[string]any{"$ref": "run.input", "pointer": "/val"},
					},
				},
			},
			"output": map[string]any{
				"val": map[string]any{"$ref": "step.output", "stepId": "step-1", "pointer": "/val"},
			},
		},
	}

	manifest := createLifecycleManifest(bundle, tasks, workflows)
	depID := registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "simple-flow", manifest)

	// Step 1: Create Run (First call)
	idempKey := "idemp-req-001"
	createBody, _ := json.Marshal(map[string]any{
		"environment": "staging",
		"input":       map[string]any{"val": "hello"},
	})
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/simple-flow/runs", bytes.NewReader(createBody))
	req.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	req.Header.Set("X-Organization-ID", orgID)
	req.Header.Set("Idempotency-Key", idempKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create run request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		var errEnv map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&errEnv)
		t.Fatalf("expected 202 Accepted on create run, got %d: %+v", resp.StatusCode, errEnv)
	}

	var run execution.RunDTO
	if err := json.NewDecoder(resp.Body).Decode(&run); err != nil {
		t.Fatalf("decode run: %v", err)
	}
	if run.ID == "" || run.WorkflowName != "simple-flow" || run.DeploymentID != depID || run.Status != contracts.RunStatusQUEUED {
		t.Fatalf("unexpected run state: %+v", run)
	}

	// Verify database rows committed atomically
	var stepsCount, eventCount, outboxCount, idempCount int
	err = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM run_steps WHERE run_id = $1::uuid),
			(SELECT count(*) FROM run_events WHERE run_id = $1::uuid),
			(SELECT count(*) FROM outbox_events WHERE organization_id = $2::uuid),
			(SELECT count(*) FROM idempotency_records WHERE response_identity = $1::uuid)`, run.ID, orgID).
			Scan(&stepsCount, &eventCount, &outboxCount, &idempCount)
	})
	if err != nil {
		t.Fatalf("query verification: %v", err)
	}
	if stepsCount != 1 || eventCount != 1 || outboxCount == 0 || idempCount != 1 {
		t.Fatalf("incomplete atomic commit: steps=%d events=%d outbox=%d idemp=%d", stepsCount, eventCount, outboxCount, idempCount)
	}

	// Step 2: Replay with identical key & payload returns original run (HTTP 202)
	reqReplay, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/simple-flow/runs", bytes.NewReader(createBody))
	reqReplay.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	reqReplay.Header.Set("X-Organization-ID", orgID)
	reqReplay.Header.Set("Idempotency-Key", idempKey)
	reqReplay.Header.Set("Content-Type", "application/json")

	respReplay, err := http.DefaultClient.Do(reqReplay)
	if err != nil {
		t.Fatalf("replay request: %v", err)
	}
	defer respReplay.Body.Close()

	if respReplay.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted on replay, got %d", respReplay.StatusCode)
	}
	var replayRun execution.RunDTO
	if err := json.NewDecoder(respReplay.Body).Decode(&replayRun); err != nil {
		t.Fatalf("decode replay run: %v", err)
	}
	if replayRun.ID != run.ID {
		t.Fatalf("replay returned different run ID: expected %s, got %s", run.ID, replayRun.ID)
	}

	// Step 3: Replay with SAME key but CHANGED payload returns 409 IDEMPOTENCY_CONFLICT
	conflictBody, _ := json.Marshal(map[string]any{
		"environment": "staging",
		"input":       map[string]any{"val": "DIFFERENT_PAYLOAD"},
	})
	reqConflict, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/simple-flow/runs", bytes.NewReader(conflictBody))
	reqConflict.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	reqConflict.Header.Set("X-Organization-ID", orgID)
	reqConflict.Header.Set("Idempotency-Key", idempKey)
	reqConflict.Header.Set("Content-Type", "application/json")

	respConflict, err := http.DefaultClient.Do(reqConflict)
	if err != nil {
		t.Fatalf("conflict request: %v", err)
	}
	defer respConflict.Body.Close()

	if respConflict.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for changed payload, got %d", respConflict.StatusCode)
	}
	var errEnv map[string]any
	_ = json.NewDecoder(respConflict.Body).Decode(&errEnv)
	if errEnv["code"] != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("expected IDEMPOTENCY_CONFLICT error code, got %v", errEnv["code"])
	}

	// Step 4: Missing Idempotency-Key returns 400 MISSING_IDEMPOTENCY_KEY
	reqMissing, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/simple-flow/runs", bytes.NewReader(createBody))
	reqMissing.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	reqMissing.Header.Set("X-Organization-ID", orgID)
	reqMissing.Header.Set("Content-Type", "application/json")
	respMissing, err := http.DefaultClient.Do(reqMissing)
	if err != nil {
		t.Fatalf("missing idemp key request: %v", err)
	}
	defer respMissing.Body.Close()
	if respMissing.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for missing Idempotency-Key, got %d", respMissing.StatusCode)
	}

	_ = envID
}

// TestLinearRunProgressionThroughWorkerAgent proves end-to-end:
// 1. 3-node linear workflow: Node A -> Node B -> Node C
// 2. Initial state: Node A is READY, Node B & C are BLOCKED
// 3. Worker claims & executes Node A -> Step A succeeds, Step B transitions to READY
// 4. Worker claims & executes Node B -> Step B succeeds, Step C transitions to READY
// 5. Worker claims & executes Node C -> Step C succeeds, Run becomes SUCCEEDED with committed output
// 6. Result API (GET /v1/runs/{id}) accurately reflects committed outputs, step states, and revision
// 7. SUCCEEDED run/step never executes again
func TestLinearRunProgressionThroughWorkerAgent(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	bundle := "2222222222222222222222222222222222222222222222222222222222222222"
	tasks := []map[string]any{
		{
			"name":                "task-a",
			"entrypoint":          "tasks/a.js",
			"timeoutMs":           30000,
			"recovery":            "idempotent",
			"idempotencyWindowMs": 305000,
			"inputSchema": map[string]any{
				"type":                 "object",
				"properties":           map[string]any{"email": map[string]any{"type": "string"}},
				"required":             []any{"email"},
				"additionalProperties": false,
			},
			"outputSchema": map[string]any{
				"type":                 "object",
				"properties":           map[string]any{"accountId": map[string]any{"type": "string"}},
				"required":             []any{"accountId"},
				"additionalProperties": false,
			},
		},
		{
			"name":                "task-b",
			"entrypoint":          "tasks/b.js",
			"timeoutMs":           30000,
			"recovery":            "idempotent",
			"idempotencyWindowMs": 305000,
			"inputSchema": map[string]any{
				"type":                 "object",
				"properties":           map[string]any{"accountId": map[string]any{"type": "string"}},
				"required":             []any{"accountId"},
				"additionalProperties": false,
			},
			"outputSchema": map[string]any{
				"type":                 "object",
				"properties":           map[string]any{"message": map[string]any{"type": "string"}},
				"required":             []any{"message"},
				"additionalProperties": false,
			},
		},
		{
			"name":                "task-c",
			"entrypoint":          "tasks/c.js",
			"timeoutMs":           30000,
			"recovery":            "idempotent",
			"idempotencyWindowMs": 305000,
			"inputSchema": map[string]any{
				"type":                 "object",
				"properties":           map[string]any{"welcomeMessage": map[string]any{"type": "string"}},
				"required":             []any{"welcomeMessage"},
				"additionalProperties": false,
			},
			"outputSchema": map[string]any{
				"type":                 "object",
				"properties":           map[string]any{"confirmationCode": map[string]any{"type": "string"}},
				"required":             []any{"confirmationCode"},
				"additionalProperties": false,
			},
		},
	}
	workflows := []map[string]any{
		{
			"manifestVersion": 1,
			"name":            "linear-pipeline",
			"inputSchema": map[string]any{
				"type":                 "object",
				"properties":           map[string]any{"customerEmail": map[string]any{"type": "string"}},
				"required":             []any{"customerEmail"},
				"additionalProperties": false,
			},
			"outputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"onboardingStatus":  map[string]any{"type": "string"},
					"finalConfirmation": map[string]any{"type": "string"},
				},
				"required":             []any{"onboardingStatus", "finalConfirmation"},
				"additionalProperties": false,
			},
			"nodes": []map[string]any{
				{
					"id":    "node-a",
					"type":  "task",
					"task":  "task-a",
					"after": []any{},
					"input": map[string]any{"email": map[string]any{"$ref": "run.input", "pointer": "/customerEmail"}},
				},
				{
					"id":    "node-b",
					"type":  "task",
					"task":  "task-b",
					"after": []any{"node-a"},
					"input": map[string]any{"accountId": map[string]any{"$ref": "step.output", "stepId": "node-a", "pointer": "/accountId"}},
				},
				{
					"id":    "node-c",
					"type":  "task",
					"task":  "task-c",
					"after": []any{"node-b"},
					"input": map[string]any{"welcomeMessage": map[string]any{"$ref": "step.output", "stepId": "node-b", "pointer": "/message"}},
				},
			},
			"output": map[string]any{
				"onboardingStatus":  map[string]any{"literal": "COMPLETE"},
				"finalConfirmation": map[string]any{"$ref": "step.output", "stepId": "node-c", "pointer": "/confirmationCode"},
			},
		},
	}

	manifest := createLifecycleManifest(bundle, tasks, workflows)
	depID := registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "linear-pipeline", manifest)

	// Enroll worker
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "worker-linear")

	// 1. Create run
	createBody, _ := json.Marshal(map[string]any{
		"environment": "staging",
		"input":       map[string]any{"customerEmail": "alice@example.com"},
	})
	createReq, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/linear-pipeline/runs", bytes.NewReader(createBody))
	createReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	createReq.Header.Set("X-Organization-ID", orgID)
	createReq.Header.Set("Idempotency-Key", "idemp-linear-pipeline-run")
	createReq.Header.Set("Content-Type", "application/json")

	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatalf("create run request: %v", err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", createResp.StatusCode)
	}
	var run execution.RunDTO
	_ = json.NewDecoder(createResp.Body).Decode(&run)

	// Verify initial step states: A is READY, B and C are BLOCKED
	getSnap := func() *execution.RunSnapshotDTO {
		getReq, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/runs/%s", server.URL, run.ID), nil)
		getReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
		getReq.Header.Set("X-Organization-ID", orgID)
		gResp, gErr := http.DefaultClient.Do(getReq)
		if gErr != nil || gResp.StatusCode != http.StatusOK {
			t.Fatalf("get run failed: status %v err %v", gResp.StatusCode, gErr)
		}
		defer gResp.Body.Close()
		var snap execution.RunSnapshotDTO
		_ = json.NewDecoder(gResp.Body).Decode(&snap)
		return &snap
	}

	initSnap := getSnap()
	if len(initSnap.Steps) != 3 {
		t.Fatalf("expected 3 steps, got %d", len(initSnap.Steps))
	}
	stepMap := make(map[string]contracts.StepStatus)
	for _, s := range initSnap.Steps {
		stepMap[s.NodeID] = s.Status
	}
	if stepMap["node-a"] != contracts.StepStatusREADY || stepMap["node-b"] != contracts.StepStatusBLOCKED || stepMap["node-c"] != contracts.StepStatusBLOCKED {
		t.Fatalf("unexpected initial step states: %+v", stepMap)
	}

	// 2. Worker claims Node A
	claimA := claimExecution(t, server, session, bundle, "claim-node-a")
	if claimA.RunID != run.ID {
		t.Fatalf("claimed wrong run: %s vs %s", claimA.RunID, run.ID)
	}
	inputA, _ := claimA.Input.(map[string]any)
	if inputA["email"] != "alice@example.com" {
		t.Fatalf("node-a resolved wrong input: %+v", claimA.Input)
	}

	// Start Node A
	startNode(t, server, session, claimA.AttemptID, claimA.OwnershipEpoch)

	// Complete Node A
	completeNode(t, server, session, claimA.AttemptID, claimA.OwnershipEpoch, "SUCCEEDED", map[string]any{
		"accountId": "acc_1001",
	}, "digest-node-a")

	// Verify Step A is SUCCEEDED and Step B became READY
	snapAfterA := getSnap()
	for _, s := range snapAfterA.Steps {
		stepMap[s.NodeID] = s.Status
	}
	if stepMap["node-a"] != contracts.StepStatusSUCCEEDED || stepMap["node-b"] != contracts.StepStatusREADY || stepMap["node-c"] != contracts.StepStatusBLOCKED {
		t.Fatalf("unexpected step states after Node A: %+v", stepMap)
	}

	// 3. Worker claims Node B
	claimB := claimExecution(t, server, session, bundle, "claim-node-b")
	inputB, _ := claimB.Input.(map[string]any)
	if inputB["accountId"] != "acc_1001" {
		t.Fatalf("node-b resolved wrong mapped input: %+v", claimB.Input)
	}

	// Start and complete Node B
	startNode(t, server, session, claimB.AttemptID, claimB.OwnershipEpoch)
	completeNode(t, server, session, claimB.AttemptID, claimB.OwnershipEpoch, "SUCCEEDED", map[string]any{
		"message": "Welcome Alice!",
	}, "digest-node-b")

	// Verify Step B is SUCCEEDED and Step C became READY
	snapAfterB := getSnap()
	for _, s := range snapAfterB.Steps {
		stepMap[s.NodeID] = s.Status
	}
	if stepMap["node-b"] != contracts.StepStatusSUCCEEDED || stepMap["node-c"] != contracts.StepStatusREADY {
		t.Fatalf("unexpected step states after Node B: %+v", stepMap)
	}

	// 4. Worker claims Node C
	claimC := claimExecution(t, server, session, bundle, "claim-node-c")
	inputC, _ := claimC.Input.(map[string]any)
	if inputC["welcomeMessage"] != "Welcome Alice!" {
		t.Fatalf("node-c resolved wrong mapped input: %+v", claimC.Input)
	}

	// Start and complete Node C
	startNode(t, server, session, claimC.AttemptID, claimC.OwnershipEpoch)
	completeNode(t, server, session, claimC.AttemptID, claimC.OwnershipEpoch, "SUCCEEDED", map[string]any{
		"confirmationCode": "CONF-9999",
	}, "digest-node-c")

	// 5. Verify terminal state of the Run
	finalSnap := getSnap()
	if finalSnap.Status != contracts.RunStatusSUCCEEDED {
		t.Fatalf("expected run SUCCEEDED, got %s", finalSnap.Status)
	}
	finalOut, _ := finalSnap.Output.(map[string]any)
	if finalOut["onboardingStatus"] != "COMPLETE" || finalOut["finalConfirmation"] != "CONF-9999" {
		t.Fatalf("unexpected final run output: %+v", finalSnap.Output)
	}

	// 6. SUCCEEDED steps never execute again
	// Verify subsequent poll claims 0 assignments
	ctxShort, cancelShort := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancelShort()
	pollBody, _ := json.Marshal(worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "poll-after-terminal", WorkerID: session.WorkerID,
		SessionID: session.SessionID, AvailableSlots: 1, DeploymentDigests: []string{bundle}, Pool: "default",
	})
	pollReq, _ := http.NewRequestWithContext(ctxShort, http.MethodPost, server.URL+"/worker/v1/poll", bytes.NewReader(pollBody))
	pollReq.Header.Set("Authorization", "Bearer "+session.SessionToken)
	pollReq.Header.Set("Content-Type", "application/json")
	pResp, pErr := http.DefaultClient.Do(pollReq)
	if pErr == nil {
		defer pResp.Body.Close()
		var emptyResp worker.PollResponseDTO
		_ = json.NewDecoder(pResp.Body).Decode(&emptyResp)
		if len(emptyResp.Assignments) != 0 {
			t.Fatalf("expected 0 assignments after workflow completion, got %d", len(emptyResp.Assignments))
		}
	}

	_ = depID
}

func startNode(t *testing.T, server *httptest.Server, session *testWorkerSession, attemptID string, epoch int64) {
	t.Helper()
	var resp worker.StartResponseDTO
	status := postWorkerJSON(t, server, "/worker/v1/start", session.SessionToken, worker.StartRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "start-" + attemptID,
		WorkerID: session.WorkerID, SessionID: session.SessionID, AttemptID: attemptID, OwnershipEpoch: epoch,
	}, &resp)
	if status != http.StatusOK || !resp.Accepted {
		t.Fatalf("start failed: status=%d resp=%+v", status, resp)
	}
}

func completeNode(t *testing.T, server *httptest.Server, session *testWorkerSession, attemptID string, epoch int64, outcome string, output any, digest string) {
	t.Helper()
	var resp worker.CompleteResponseDTO
	status := postWorkerJSON(t, server, "/worker/v1/complete", session.SessionToken, worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "complete-" + attemptID,
		WorkerID: session.WorkerID, SessionID: session.SessionID, AttemptID: attemptID, OwnershipEpoch: epoch,
		Outcome: outcome, Output: output, ResultDigest: digest,
	}, &resp)
	if status != http.StatusOK || !resp.Accepted {
		t.Fatalf("complete failed: status=%d resp=%+v", status, resp)
	}
}
