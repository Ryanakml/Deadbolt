package integration_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"github.com/Ryanakml/Deadbolt/internal/controlplane"
	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

type synchronizedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *synchronizedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func writeAgentBundle(t *testing.T, dir, targetOS, targetArch string) string {
	t.Helper()
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	code := []byte("export default async function task(input) { return { value: input.value + '-done' }; }\n")
	if err := tw.WriteHeader(&tar.Header{Name: "tasks/agent.mjs", Mode: 0o600, Size: int64(len(code))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(code); err != nil {
		t.Fatal(err)
	}
	// Platform identity is part of the immutable artifact: the fixture must
	// carry the same embedded target the manifest declares, mirroring
	// production `runtime build` output, or worker preflight rejects it.
	platformBytes, err := worker.CanonicalPlatformBytes(targetOS, targetArch)
	if err != nil {
		t.Fatalf("canonical platform bytes: %v", err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: worker.BundlePlatformPath, Mode: 0o644, Size: int64(len(platformBytes))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(platformBytes); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(archive.Bytes()))
	if err := os.WriteFile(filepath.Join(dir, digest+".tar"), archive.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return digest
}

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
		tenant.CapPayloadRead,
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
	var stepsCount, eventCount, outboxCount, idempCount, auditCount int
	err = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM run_steps WHERE run_id = $1::uuid),
			(SELECT count(*) FROM run_events WHERE run_id = $1::uuid),
			(SELECT count(*) FROM outbox_events WHERE organization_id = $2::uuid),
			(SELECT count(*) FROM idempotency_records WHERE response_identity = $1::uuid),
			(SELECT count(*) FROM audit_events WHERE organization_id = $2::uuid AND target_id = $1::uuid AND action = 'run.create')`, run.ID, orgID).
			Scan(&stepsCount, &eventCount, &outboxCount, &idempCount, &auditCount)
	})
	if err != nil {
		t.Fatalf("query verification: %v", err)
	}
	if stepsCount != 1 || eventCount != 1 || outboxCount == 0 || idempCount != 1 || auditCount != 1 {
		t.Fatalf("incomplete atomic commit: steps=%d events=%d outbox=%d idemp=%d audit=%d", stepsCount, eventCount, outboxCount, idempCount, auditCount)
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

	// Verify replay did not create another audit event
	err = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM audit_events WHERE organization_id = $1::uuid AND target_id = $2::uuid AND action = 'run.create'`, orgID, run.ID).
			Scan(&auditCount)
	})
	if err != nil {
		t.Fatalf("query replay audit count: %v", err)
	}
	if auditCount != 1 {
		t.Fatalf("expected exactly 1 audit event after replay, got %d", auditCount)
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

	// Step 3b: a previously unseen key is serialized too. Both requests must
	// observe the same committed logical run instead of leaking a unique error.
	concurrentBody, _ := json.Marshal(map[string]any{"environment": "staging", "input": map[string]any{"val": "CONCURRENT"}})
	type concurrentResult struct {
		status int
		runID  string
		err    error
	}
	results := make(chan concurrentResult, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/simple-flow/runs", bytes.NewReader(concurrentBody))
			req.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
			req.Header.Set("X-Organization-ID", orgID)
			req.Header.Set("Idempotency-Key", "run-concurrent-key")
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				results <- concurrentResult{err: err}
				return
			}
			defer resp.Body.Close()
			var out execution.RunDTO
			_ = json.NewDecoder(resp.Body).Decode(&out)
			results <- concurrentResult{status: resp.StatusCode, runID: out.ID}
		}()
	}
	wg.Wait()
	close(results)
	var concurrentRunID string
	for result := range results {
		if result.err != nil || result.status != http.StatusAccepted {
			t.Fatalf("concurrent create: status=%d err=%v", result.status, result.err)
		}
		if concurrentRunID == "" {
			concurrentRunID = result.runID
		} else if result.runID != concurrentRunID {
			t.Fatalf("concurrent replay created distinct runs: %s != %s", concurrentRunID, result.runID)
		}
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

	// Step 5: Verify fail-closed behavior when audit context is nil
	execSvc := execution.NewService(tc.pool, tc.service)
	_, _, err = execSvc.CreateRun(context.Background(), orgID, "staging", "simple-flow", "idemp-nil-audit", nil, map[string]any{"val": "foo"}, nil)
	if !errors.Is(err, tenant.ErrAuditRequired) {
		t.Fatalf("expected ErrAuditRequired when audit is nil, got %v", err)
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

// TestLinearRunThroughActualAgentAndNodeChild proves the production worker path:
// authenticated Agent poll -> Start -> verified tar bundle -> real Node child ->
// Complete -> durable run output. It intentionally does not call protocol helpers.
func TestLinearRunThroughActualAgentAndNodeChild(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("real Agent bundle fixture requires the Linux worker runtime used by CI")
	}
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()
	bundleDir := t.TempDir()
	targetOS, targetArch := "linux", runtime.GOARCH
	bundle := writeAgentBundle(t, bundleDir, targetOS, targetArch)
	tasks := []map[string]any{{"name": "agent-task", "entrypoint": "tasks/agent.mjs", "timeoutMs": 30000, "recovery": "idempotent", "idempotencyWindowMs": 305000,
		"inputSchema":  map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}, "required": []any{"value"}},
		"outputSchema": map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}, "required": []any{"value"}}}}
	workflows := []map[string]any{{"manifestVersion": 1, "name": "agent-linear", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}, "required": []any{"value"}}, "outputSchema": map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}, "required": []any{"value"}}, "nodes": []map[string]any{{"id": "agent-node", "type": "task", "task": "agent-task"}}, "output": map[string]any{"value": map[string]any{"$ref": "step.output", "stepId": "agent-node", "pointer": "/value"}}}}
	manifest := createLifecycleManifest(bundle, tasks, workflows)
	var manifestMap map[string]any
	_ = json.Unmarshal(manifest, &manifestMap)
	manifestMap["targetOS"], manifestMap["targetArchitecture"] = targetOS, targetArch
	manifest, _ = json.Marshal(manifestMap)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "agent-linear", manifest)
	var completeMu sync.Mutex
	completeRequests := make([]worker.CompleteRequestDTO, 0, 2)
	dropFirstCompleteAck := true
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	faultClient := &http.Client{Timeout: 30 * time.Second, Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/worker/v1/complete" {
			return transport.RoundTrip(req)
		}
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		if err := req.Body.Close(); err != nil {
			return nil, err
		}
		req.Body = io.NopCloser(bytes.NewReader(body))

		response, err := transport.RoundTrip(req)
		if err != nil {
			return nil, err
		}
		var completion worker.CompleteRequestDTO
		if err := json.Unmarshal(body, &completion); err != nil {
			response.Body.Close()
			return nil, err
		}
		completeMu.Lock()
		completeRequests = append(completeRequests, completion)
		drop := dropFirstCompleteAck
		if drop && response.StatusCode < http.StatusBadRequest {
			dropFirstCompleteAck = false
		} else {
			drop = false
		}
		completeMu.Unlock()
		if !drop {
			return response, nil
		}

		// The handler has returned a successful response, so the control-plane
		// transaction is committed. Only the caller's ACK is now lost.
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		return nil, errors.New("injected COMPLETE ACK loss after backend commit")
	})}

	// Issue a real enrollment token; Agent owns challenge/enroll/session itself.
	enrollBody, _ := json.Marshal(map[string]any{"poolName": "default"})
	enrollReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/environments/%s/worker-enrollments", server.URL, envID), bytes.NewReader(enrollBody))
	enrollReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	enrollReq.Header.Set("X-Organization-ID", orgID)
	enrollReq.Header.Set("Idempotency-Key", "agent-e2e-enrollment")
	enrollReq.Header.Set("Content-Type", "application/json")
	enrollResp, err := http.DefaultClient.Do(enrollReq)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollResp.Body.Close()
	if enrollResp.StatusCode != http.StatusCreated {
		t.Fatalf("agent enrollment status=%d", enrollResp.StatusCode)
	}
	var enrollment worker.EnrollmentTokenInfo
	if err := json.NewDecoder(enrollResp.Body).Decode(&enrollment); err != nil {
		t.Fatal(err)
	}
	runnerPath, err := filepath.Abs("../../runner/node/dist/index.js")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var agentLogs synchronizedBuffer
	var childLaunches atomic.Int32
	agent, err := worker.NewAgent(worker.AgentConfig{ControlPlaneURL: server.URL, KeyPath: filepath.Join(t.TempDir(), "worker.key"), EnrollmentToken: enrollment.Token, BundleDir: bundleDir, RunnerPath: runnerPath, PollTimeout: 100 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond, Logger: log.New(&agentLogs, "", 0), HTTPClient: faultClient, OnTaskProcessStart: func() { childLaunches.Add(1) }})
	if err != nil {
		t.Fatal(err)
	}
	agentDone := make(chan struct{})
	go func() { defer close(agentDone); _ = agent.Start(ctx) }()
	defer func() { cancel(); <-agentDone }()

	createBody, _ := json.Marshal(map[string]any{"environment": "staging", "input": map[string]any{"value": "agent"}})
	createReq, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/agent-linear/runs", bytes.NewReader(createBody))
	createReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	createReq.Header.Set("X-Organization-ID", orgID)
	createReq.Header.Set("Idempotency-Key", "agent-e2e-run")
	createReq.Header.Set("Content-Type", "application/json")
	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatal(err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusAccepted {
		t.Fatalf("agent create status=%d", createResp.StatusCode)
	}
	var run execution.RunDTO
	_ = json.NewDecoder(createResp.Body).Decode(&run)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := execution.NewService(tc.pool, tc.service).GetRun(context.Background(), orgID, run.ID)
		if err == nil && snapshot.Status == contracts.RunStatusSUCCEEDED {
			output, _ := snapshot.Output.(map[string]any)
			if output["value"] != "agent-done" {
				t.Fatalf("unexpected agent child output: %+v", snapshot.Output)
			}
			completeMu.Lock()
			requests := append([]worker.CompleteRequestDTO(nil), completeRequests...)
			completeMu.Unlock()
			if len(requests) != 2 {
				t.Fatalf("expected exactly two Complete requests after lost ACK, got %d; childLaunches=%d agentLogs=%s", len(requests), childLaunches.Load(), agentLogs.String())
			}
			first, second := requests[0], requests[1]
			if first.AttemptID != second.AttemptID || first.OwnershipEpoch != second.OwnershipEpoch || first.Outcome != second.Outcome || first.ResultDigest != second.ResultDigest || !reflect.DeepEqual(first.Output, second.Output) || !reflect.DeepEqual(first.Error, second.Error) {
				t.Fatalf("Agent ACK-loss retry changed result identity: first=%+v second=%+v", first, second)
			}
			if childLaunches.Load() != 1 {
				t.Fatalf("expected exactly one Node child execution, got %d; completes=%d agentLogs=%s", childLaunches.Load(), len(requests), agentLogs.String())
			}
			var terminalAttempts, releasedLeases, taskCompleted, runCompleted, taskOutbox, runOutbox int
			if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
				return tx.QueryRow(ctx, `SELECT
					(SELECT count(*) FROM task_attempts a JOIN run_steps rs ON rs.id=a.step_id WHERE rs.run_id=$1::uuid AND a.status IN ('SUCCEEDED','FAILED','CANCELLED')),
					(SELECT count(*) FROM task_leases l JOIN task_attempts a ON a.id=l.attempt_id JOIN run_steps rs ON rs.id=a.step_id WHERE rs.run_id=$1::uuid),
					(SELECT count(*) FROM run_events WHERE run_id=$1::uuid AND event_type='TASK_COMPLETED'),
					(SELECT count(*) FROM run_events WHERE run_id=$1::uuid AND event_type='RUN_COMPLETED'),
					(SELECT count(*) FROM outbox_events WHERE payload->>'runId'=$1::text AND payload->>'eventType'='TASK_COMPLETED'),
					(SELECT count(*) FROM outbox_events WHERE payload->>'runId'=$1::text AND payload->>'eventType'='RUN_COMPLETED')`, run.ID).Scan(&terminalAttempts, &releasedLeases, &taskCompleted, &runCompleted, &taskOutbox, &runOutbox)
			}); err != nil {
				t.Fatal(err)
			}
			if terminalAttempts != 1 || releasedLeases != 0 || taskCompleted != 1 || runCompleted != 1 || taskOutbox != 1 || runOutbox != 1 {
				t.Fatalf("ACK-loss retry duplicated durable effects: terminalAttempts=%d leases=%d taskCompleted=%d runCompleted=%d taskOutbox=%d runOutbox=%d", terminalAttempts, releasedLeases, taskCompleted, runCompleted, taskOutbox, runOutbox)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	snapshot, _ := execution.NewService(tc.pool, tc.service).GetRun(context.Background(), orgID, run.ID)
	completeMu.Lock()
	completeCount := len(completeRequests)
	completeMu.Unlock()
	t.Fatalf("actual agent/node child did not complete run before deadline: run=%s snapshot=%+v completes=%d childLaunches=%d agentLogs=%s", run.ID, snapshot, completeCount, childLaunches.Load(), agentLogs.String())
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
	req := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "complete-" + attemptID,
		WorkerID: session.WorkerID, SessionID: session.SessionID, AttemptID: attemptID, OwnershipEpoch: epoch,
		Outcome: outcome, Output: output, ResultDigest: digest,
	}
	req.ResultDigest, _ = worker.CanonicalCompletionDigest(&req)
	status := postWorkerJSON(t, server, "/worker/v1/complete", session.SessionToken, req, &resp)
	if status != http.StatusOK || !resp.Accepted {
		t.Fatalf("complete failed: status=%d resp=%+v", status, resp)
	}
}
