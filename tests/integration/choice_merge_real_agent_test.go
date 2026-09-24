package integration_test

// Issue #27 real worker acceptance (Blueprint §16.2): real PostgreSQL +
// worker.Agent + Node runner child processes + real worker HTTP protocol +
// actual bundle.
//
// Proves:
//  1. Condition selects exactly one branch; selected Node child launches;
//     unselected branch child NEVER launches; unselected steps are SKIPPED
//     with BRANCH_NOT_SELECTED.
//  2. Selected committed output reaches merge; merge emits canonical
//     {branch, value} passing the explicit merge schema; final run succeeds.
//  3. Selected-branch definitive failure fail-fasts: branch B stays skipped,
//     merge never executes, run fails.
//  4. Replay/reconciliation preserves the persisted selected branch without
//     re-evaluation, duplicate execution, or reopening terminal work.

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

func writeChoiceMergeAgentBundle(t *testing.T, dir, targetOS, targetArch string) string {
	t.Helper()
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	files := map[string]string{
		"tasks/high.mjs": "export default async function task(input) { return { x: 100 }; }\n",
		"tasks/low.mjs":  "export default async function task(input) { return { x: 200 }; }\n",
		"tasks/boom.mjs": "export default async function task(input) { throw new Error('selected branch boom'); }\n",
	}
	for name, code := range files {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(code))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(code)); err != nil {
			t.Fatal(err)
		}
	}
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

func enrollRealAgent(t *testing.T, serverURL, envID, adminKey, orgID string) worker.EnrollmentTokenInfo {
	t.Helper()
	enrollBody, _ := json.Marshal(map[string]any{"poolName": "default"})
	enrollReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/environments/%s/worker-enrollments", serverURL, envID), bytes.NewReader(enrollBody))
	enrollReq.Header.Set("Authorization", "Bearer "+adminKey)
	enrollReq.Header.Set("X-Organization-ID", orgID)
	enrollReq.Header.Set("Idempotency-Key", fmt.Sprintf("choice-merge-enroll-%d", time.Now().UnixNano()))
	enrollReq.Header.Set("Content-Type", "application/json")
	enrollResp, err := http.DefaultClient.Do(enrollReq)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollResp.Body.Close()
	if enrollResp.StatusCode != http.StatusCreated {
		t.Fatalf("enrollment status=%d", enrollResp.StatusCode)
	}
	var enrollment worker.EnrollmentTokenInfo
	if err := json.NewDecoder(enrollResp.Body).Decode(&enrollment); err != nil {
		t.Fatal(err)
	}
	return enrollment
}

func startChoiceMergeAgent(t *testing.T, serverURL, bundleDir, runnerPath, enrollToken string) (*worker.Agent, context.CancelFunc, *synchronizedBuffer, *atomic.Int32) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var agentLogs synchronizedBuffer
	var childLaunches atomic.Int32
	agent, err := worker.NewAgent(worker.AgentConfig{ControlPlaneURL: serverURL, KeyPath: filepath.Join(t.TempDir(), "worker.key"), EnrollmentToken: enrollToken, BundleDir: bundleDir, RunnerPath: runnerPath, PollTimeout: 100 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond, Logger: log.New(&agentLogs, "", 0), OnTaskProcessStart: func() { childLaunches.Add(1) }})
	if err != nil {
		t.Fatal(err)
	}
	agentDone := make(chan struct{})
	go func() { defer close(agentDone); _ = agent.Start(ctx) }()
	t.Cleanup(func() { cancel(); <-agentDone })
	return agent, cancel, &agentLogs, &childLaunches
}

func createChoiceMergeRun(t *testing.T, serverURL, workflowName, adminKey, orgID string, input map[string]any) execution.RunDTO {
	t.Helper()
	createBody, _ := json.Marshal(map[string]any{"environment": "staging", "input": input})
	createReq, _ := http.NewRequest(http.MethodPost, serverURL+"/v1/workflows/"+workflowName+"/runs", bytes.NewReader(createBody))
	createReq.Header.Set("Authorization", "Bearer "+adminKey)
	createReq.Header.Set("X-Organization-ID", orgID)
	createReq.Header.Set("Idempotency-Key", fmt.Sprintf("%s-run-%d", workflowName, time.Now().UnixNano()))
	createReq.Header.Set("Content-Type", "application/json")
	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatal(err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusAccepted {
		var body map[string]any
		_ = json.NewDecoder(createResp.Body).Decode(&body)
		t.Fatalf("create status=%d body=%v", createResp.StatusCode, body)
	}
	var run execution.RunDTO
	_ = json.NewDecoder(createResp.Body).Decode(&run)
	return run
}

// TestChoiceMerge_RealAgent_SelectedBranch proves the successful choice path
// through real Agent + Node children: condition selects high, high child
// launches and commits, low child never launches, low stays SKIPPED,
// merge consumes high output and emits schema-valid {branch,value}, run
// succeeds, and replay preserves the one persisted choice.
func TestChoiceMerge_RealAgent_SelectedBranch(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()
	bundleDir := t.TempDir()
	targetOS, targetArch := "linux", runtime.GOARCH
	bundle := writeChoiceMergeAgentBundle(t, bundleDir, targetOS, targetArch)
	intSchema := choiceMergeIntSchema()
	taggedSchema := choiceMergeTaggedSchema()
	tasks := []map[string]any{
		{"name": "high-task", "entrypoint": "tasks/high.mjs", "timeoutMs": 30000, "recovery": "safe",
			"inputSchema": intSchema, "outputSchema": intSchema},
		{"name": "low-task", "entrypoint": "tasks/low.mjs", "timeoutMs": 30000, "recovery": "safe",
			"inputSchema": intSchema, "outputSchema": intSchema},
	}
	workflows := []map[string]any{{
		"manifestVersion": 1, "name": "choice-agent",
		"inputSchema": intSchema, "outputSchema": taggedSchema,
		"nodes": []map[string]any{
			{"id": "decide", "type": "choice", "choice": map[string]any{
				"branches": []map[string]any{
					{"name": "high", "condition": map[string]any{
						"op": "gt", "args": []any{
							map[string]any{"$ref": "run.input", "pointer": "/x"},
							map[string]any{"literal": 5},
						}}},
					{"name": "low"},
				},
				"default": "low",
			}},
			{"id": "high_step", "type": "task", "task": "high-task", "after": []any{"decide"},
				"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}}},
			{"id": "low_step", "type": "task", "task": "low-task", "after": []any{"decide"},
				"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}}},
			{"id": "join", "type": "merge", "after": []any{"high_step", "low_step"}, "merge": map[string]any{
				"choice": "decide",
				"branches": []map[string]any{
					{"branch": "high", "terminal": "high_step",
						"value": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "high_step", "pointer": "/x"}}},
					{"branch": "low", "terminal": "low_step",
						"value": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "low_step", "pointer": "/x"}}},
				},
				"outputSchema": taggedSchema,
			}},
		},
		"output": map[string]any{
			"branch": map[string]any{"$ref": "step.output", "stepId": "join", "pointer": "/branch"},
			"value":  map[string]any{"$ref": "step.output", "stepId": "join", "pointer": "/value"},
		},
	}}
	manifest := createLifecycleManifest(bundle, tasks, workflows)
	var manifestMap map[string]any
	_ = json.Unmarshal(manifest, &manifestMap)
	manifestMap["targetOS"], manifestMap["targetArchitecture"] = targetOS, targetArch
	manifest, _ = json.Marshal(manifestMap)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "choice-agent", manifest)

	enrollment := enrollRealAgent(t, server.URL, envID, adminKey.PlaintextKey, orgID)
	runnerPath, err := filepath.Abs("../../runner/node/dist/index.js")
	if err != nil {
		t.Fatal(err)
	}
	_, cancel, agentLogs, childLaunches := startChoiceMergeAgent(t, server.URL, bundleDir, runnerPath, enrollment.Token)
	defer cancel()

	run := createChoiceMergeRun(t, server.URL, "choice-agent", adminKey.PlaintextKey, orgID, map[string]any{"x": 10})

	svc := execution.NewService(tc.pool, tc.service)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := svc.GetRun(context.Background(), orgID, run.ID)
		if err == nil && snapshot.Status == contracts.RunStatusSUCCEEDED {
			output, _ := snapshot.Output.(map[string]any)
			if output["branch"] != "high" {
				t.Fatalf("merge must emit selected branch high, got %+v", snapshot.Output)
			}
			val, _ := output["value"].(map[string]any)
			if val["x"] != float64(100) {
				t.Fatalf("merge must carry selected committed output x=100, got %+v", snapshot.Output)
			}
			// Explicit merge schema validation (frozen Blueprint §16.2).
			if err := contracts.ValidatePayload(taggedSchema, snapshot.Output); err != nil {
				t.Fatalf("merge output must pass explicit schema: %v", err)
			}
			if childLaunches.Load() != 1 {
				t.Fatalf("expected exactly 1 Node child (high), unselected low must never launch, got %d logs=%s", childLaunches.Load(), agentLogs.String())
			}
			// Durable skip/branch proof.
			var highState, lowState, lowReason, joinBranch string
			var joinOut []byte
			if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
				if err := tx.QueryRow(ctx, `SELECT state FROM run_steps WHERE run_id=$1::uuid AND node_id='high_step'`, run.ID).Scan(&highState); err != nil {
					return err
				}
				if err := tx.QueryRow(ctx, `SELECT state, COALESCE(wait_reason,'') FROM run_steps WHERE run_id=$1::uuid AND node_id='low_step'`, run.ID).Scan(&lowState, &lowReason); err != nil {
					return err
				}
				return tx.QueryRow(ctx, `SELECT output FROM run_steps WHERE run_id=$1::uuid AND node_id='join'`, run.ID).Scan(&joinOut)
			}); err != nil {
				t.Fatal(err)
			}
			if highState != "SUCCEEDED" {
				t.Fatalf("selected high_step must be SUCCEEDED, got %s", highState)
			}
			if lowState != "SKIPPED" || lowReason != "BRANCH_NOT_SELECTED" {
				t.Fatalf("unselected low_step must be SKIPPED/BRANCH_NOT_SELECTED, got %s/%s", lowState, lowReason)
			}
			var joinMap map[string]any
			_ = json.Unmarshal(joinOut, &joinMap)
			joinBranch, _ = joinMap["branch"].(string)
			if joinBranch != "high" {
				t.Fatalf("merge tagged output must name high, got %v", joinMap)
			}
			// Replay/reconciliation: persisted selected branch never changes,
			// unselected remains skipped, no duplicate execution.
			launchesBefore := childLaunches.Load()
			engine := execution.NewWorkerEngine(tc.pool)
			repaired, err := engine.ReconcileReadyWork(context.Background(), orgID)
			if err != nil {
				t.Fatalf("ReconcileReadyWork failed: %v", err)
			}
			if repaired != 0 {
				t.Fatalf("expected 0 repaired transitions on settled choice run, got %d", repaired)
			}
			var decideOut []byte
			var lowStateAfter string
			if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
				if err := tx.QueryRow(ctx, `SELECT output FROM run_steps WHERE run_id=$1::uuid AND node_id='decide'`, run.ID).Scan(&decideOut); err != nil {
					return err
				}
				return tx.QueryRow(ctx, `SELECT state FROM run_steps WHERE run_id=$1::uuid AND node_id='low_step'`, run.ID).Scan(&lowStateAfter)
			}); err != nil {
				t.Fatal(err)
			}
			var decideMap map[string]any
			_ = json.Unmarshal(decideOut, &decideMap)
			if decideMap["selected"] != "high" {
				t.Fatalf("replay must preserve selected branch high, got %v", decideMap)
			}
			if lowStateAfter != "SKIPPED" {
				t.Fatalf("unselected branch must remain skipped after replay, got %s", lowStateAfter)
			}
			if childLaunches.Load() != launchesBefore {
				t.Fatalf("no duplicate execution on replay: launches %d -> %d", launchesBefore, childLaunches.Load())
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	snapshot, _ := svc.GetRun(context.Background(), orgID, run.ID)
	t.Fatalf("real-agent choice did not succeed: %+v launches=%d logs=%s", snapshot, childLaunches.Load(), agentLogs.String())
}

// TestChoiceMerge_RealAgent_SelectedBranchFailure proves fail-fast through
// real children: choice selects high, high starts and fails definitively,
// low stays skipped, merge never executes, run fails per normal semantics.
func TestChoiceMerge_RealAgent_SelectedBranchFailure(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()
	bundleDir := t.TempDir()
	targetOS, targetArch := "linux", runtime.GOARCH
	bundle := writeChoiceMergeAgentBundle(t, bundleDir, targetOS, targetArch)
	intSchema := choiceMergeIntSchema()
	taggedSchema := choiceMergeTaggedSchema()
	tasks := []map[string]any{
		{"name": "boom-task", "entrypoint": "tasks/boom.mjs", "timeoutMs": 30000, "recovery": "safe",
			"inputSchema": intSchema, "outputSchema": intSchema,
			"retry": map[string]any{"maxAttempts": 1, "initialDelayMs": 100, "maxDelayMs": 1000}},
		{"name": "low-task", "entrypoint": "tasks/low.mjs", "timeoutMs": 30000, "recovery": "safe",
			"inputSchema": intSchema, "outputSchema": intSchema},
	}
	workflows := []map[string]any{{
		"manifestVersion": 1, "name": "choice-agent-fail",
		"inputSchema": intSchema, "outputSchema": taggedSchema,
		"nodes": []map[string]any{
			{"id": "decide", "type": "choice", "choice": map[string]any{
				"branches": []map[string]any{
					{"name": "high", "condition": map[string]any{
						"op": "gt", "args": []any{
							map[string]any{"$ref": "run.input", "pointer": "/x"},
							map[string]any{"literal": 5},
						}}},
					{"name": "low"},
				},
				"default": "low",
			}},
			{"id": "high_step", "type": "task", "task": "boom-task", "after": []any{"decide"},
				"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}}},
			{"id": "low_step", "type": "task", "task": "low-task", "after": []any{"decide"},
				"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}}, "sideEffect": true},
			{"id": "join", "type": "merge", "after": []any{"high_step", "low_step"}, "merge": map[string]any{
				"choice": "decide",
				"branches": []map[string]any{
					{"branch": "high", "terminal": "high_step",
						"value": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "high_step", "pointer": "/x"}}},
					{"branch": "low", "terminal": "low_step",
						"value": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "low_step", "pointer": "/x"}}},
				},
				"outputSchema": taggedSchema,
			}},
		},
		"output": map[string]any{
			"branch": map[string]any{"$ref": "step.output", "stepId": "join", "pointer": "/branch"},
			"value":  map[string]any{"$ref": "step.output", "stepId": "join", "pointer": "/value"},
		},
	}}
	manifest := createLifecycleManifest(bundle, tasks, workflows)
	var manifestMap map[string]any
	_ = json.Unmarshal(manifest, &manifestMap)
	manifestMap["targetOS"], manifestMap["targetArchitecture"] = targetOS, targetArch
	manifest, _ = json.Marshal(manifestMap)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "choice-agent-fail", manifest)

	enrollment := enrollRealAgent(t, server.URL, envID, adminKey.PlaintextKey, orgID)
	runnerPath, err := filepath.Abs("../../runner/node/dist/index.js")
	if err != nil {
		t.Fatal(err)
	}
	_, cancel, agentLogs, childLaunches := startChoiceMergeAgent(t, server.URL, bundleDir, runnerPath, enrollment.Token)
	defer cancel()

	run := createChoiceMergeRun(t, server.URL, "choice-agent-fail", adminKey.PlaintextKey, orgID, map[string]any{"x": 10})

	svc := execution.NewService(tc.pool, tc.service)
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := svc.GetRun(context.Background(), orgID, run.ID)
		if err == nil && snapshot.Status == contracts.RunStatusFAILED {
			var highState, lowState, joinState string
			if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
				if err := tx.QueryRow(ctx, `SELECT state FROM run_steps WHERE run_id=$1::uuid AND node_id='high_step'`, run.ID).Scan(&highState); err != nil {
					return err
				}
				if err := tx.QueryRow(ctx, `SELECT state FROM run_steps WHERE run_id=$1::uuid AND node_id='low_step'`, run.ID).Scan(&lowState); err != nil {
					return err
				}
				return tx.QueryRow(ctx, `SELECT state FROM run_steps WHERE run_id=$1::uuid AND node_id='join'`, run.ID).Scan(&joinState)
			}); err != nil {
				t.Fatal(err)
			}
			if highState != "FAILED" {
				t.Fatalf("selected high_step must be FAILED, got %s", highState)
			}
			if lowState != "SKIPPED" {
				t.Fatalf("unselected low_step must remain SKIPPED, got %s", lowState)
			}
			if joinState == "SUCCEEDED" {
				t.Fatalf("merge must never execute when selected branch fails, got SUCCEEDED")
			}
			if childLaunches.Load() != 1 {
				t.Fatalf("expected exactly 1 Node child (failed high), low must never launch, got %d logs=%s", childLaunches.Load(), agentLogs.String())
			}
			// No replay changes selected branch.
			var decideOut []byte
			if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
				return tx.QueryRow(ctx, `SELECT output FROM run_steps WHERE run_id=$1::uuid AND node_id='decide'`, run.ID).Scan(&decideOut)
			}); err != nil {
				t.Fatal(err)
			}
			var decideMap map[string]any
			_ = json.Unmarshal(decideOut, &decideMap)
			if decideMap["selected"] != "high" {
				t.Fatalf("failed run must preserve selected branch high, got %v", decideMap)
			}
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	snapshot, _ := svc.GetRun(context.Background(), orgID, run.ID)
	t.Fatalf("real-agent selected-branch failure did not fail fast: %+v launches=%d logs=%s", snapshot, childLaunches.Load(), agentLogs.String())
}
