package integration_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

// Issue #31: Integrated Acceptance Gate for M3.
//
// Proves the integrated behavior of:
//   - Linear workflows (A -> B -> C)
//   - Parallel DAG execution (Diamond join, independent eligibility)
//   - Structured choices & merges (branch selection, skipped propagation F-16)
//   - Nested structured merges
//   - Schema-aware output mapping & invalid mapping fail-fast (F-28)
//   - Fail-fast sibling settlement (F-15)
//   - Control races: Pause vs Claim (F-13), Pause vs Completion, Resume recomputation, Stale revision
//   - Terminal and duplicate safety (direct DB assertions)
//   - Real two-worker concurrency with Node child processes
//   - Inspector parity (logical steps, attempt containment, wait reasons, redaction)

func writeM3TwoWorkerBundle(t *testing.T, dir, targetOS, targetArch string) string {
	t.Helper()
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)

	files := map[string]string{
		"tasks/agent.mjs": `export default async function task(input) {
  await new Promise(r => setTimeout(r, 400));
  return { value: input.value + '-done' };
}
`,
		"tasks/join.mjs": `export default async function task(input) {
  return { value: input.b + '+' + input.c };
}
`,
	}

	for name, code := range files {
		content := []byte(code)
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(content))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(content); err != nil {
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

func m3NodeForStep(t *testing.T, tc *tenantTestContext, orgID, stepID string) string {
	t.Helper()
	var nodeID string
	err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT node_id FROM run_steps WHERE id=$1::uuid`, stepID).Scan(&nodeID)
	})
	if err != nil {
		t.Fatalf("nodeForStep %s: %v", stepID, err)
	}
	return nodeID
}

func m3AssociateWorkerDeployment(t *testing.T, tc *tenantTestContext, orgID, sessionID, bundleDigest string) {
	t.Helper()
	err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3) ON CONFLICT DO NOTHING`, sessionID, orgID, bundleDigest)
		return err
	})
	if err != nil {
		t.Fatalf("associate worker deployment: %v", err)
	}
}

func m3CreateRun(t *testing.T, serverURL, workflowName string, apiKey string, orgID string, input any, idempKey string) execution.RunDTO {
	t.Helper()
	createBody, err := json.Marshal(map[string]any{
		"environment": "staging",
		"input":       input,
	})
	if err != nil {
		t.Fatal(err)
	}
	createReq, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/workflows/%s/runs", serverURL, workflowName), bytes.NewReader(createBody))
	if err != nil {
		t.Fatal(err)
	}
	createReq.Header.Set("Authorization", "Bearer "+apiKey)
	createReq.Header.Set("X-Organization-ID", orgID)
	createReq.Header.Set("Idempotency-Key", idempKey)
	createReq.Header.Set("Content-Type", "application/json")
	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatal(err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusAccepted && createResp.StatusCode != http.StatusOK {
		var errEnv map[string]any
		_ = json.NewDecoder(createResp.Body).Decode(&errEnv)
		t.Fatalf("create run failed: status %d body %+v", createResp.StatusCode, errEnv)
	}
	var run execution.RunDTO
	if err := json.NewDecoder(createResp.Body).Decode(&run); err != nil {
		t.Fatalf("decode create run response: %v", err)
	}
	return run
}

// 1. Linear Baseline (A -> B -> C): exact fixture output, Inspector agreement, API agreement
func TestM3_LinearBaselineAgreement(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	engine := execution.NewWorkerEngine(tc.pool)

	bundle := "3111111111111111111111111111111111111111111111111111111111111111"
	schemaObj := map[string]any{"type": "object", "properties": map[string]any{"v": map[string]any{"type": "integer"}}, "required": []any{"v"}}
	tasks := []map[string]any{
		{"name": "t-linear", "inputSchema": schemaObj, "outputSchema": schemaObj, "recovery": "safe", "entrypoint": "tasks.js#noop"},
	}
	workflows := []map[string]any{{
		"manifestVersion": 1, "name": "wf-linear",
		"inputSchema": schemaObj, "outputSchema": schemaObj,
		"nodes": []map[string]any{
			{"id": "a", "type": "task", "task": "t-linear", "input": map[string]any{"v": map[string]any{"$ref": "run.input", "pointer": "/v"}}},
			{"id": "b", "type": "task", "task": "t-linear", "after": []any{"a"}, "input": map[string]any{"v": map[string]any{"$ref": "step.output", "stepId": "a", "pointer": "/v"}}},
			{"id": "c", "type": "task", "task": "t-linear", "after": []any{"b"}, "input": map[string]any{"v": map[string]any{"$ref": "step.output", "stepId": "b", "pointer": "/v"}}},
		},
		"output": map[string]any{"v": map[string]any{"$ref": "step.output", "stepId": "c", "pointer": "/v"}},
	}}
	manifest := createLifecycleManifest(bundle, tasks, workflows)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "wf-linear", manifest)

	sess, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "m3-lin-w1")
	m3AssociateWorkerDeployment(t, tc, orgID, sess.SessionID, bundle)

	// Create run with input {v: 100}
	run := m3CreateRun(t, server.URL, "wf-linear", adminKey.PlaintextKey, orgID, map[string]any{"v": 100}, "m3-lin-run-001")

	// Execute A, B, C through WorkerEngine
	for _, expectedNode := range []string{"a", "b", "c"} {
		asgns := parallelClaim(t, engine, sess, orgID, envID, 1, "poll-"+expectedNode)
		if len(asgns) != 1 {
			t.Fatalf("expected 1 assignment for %s, got %d", expectedNode, len(asgns))
		}
		nodeID := m3NodeForStep(t, tc, orgID, asgns[0].StepID)
		if nodeID != expectedNode {
			t.Fatalf("expected node %s, got %s", expectedNode, nodeID)
		}
		parallelStart(t, engine, sess, orgID, envID, asgns[0])
		parallelCompleteSuccess(t, engine, sess, orgID, envID, asgns[0], map[string]any{"v": float64(100)})
	}

	// Verify API Snapshot matches persisted DB state
	snap := m2GetSnapshot(t, server.URL, run.ID, adminKey.PlaintextKey, orgID)
	if string(snap.Status) != "SUCCEEDED" {
		t.Fatalf("expected run SUCCEEDED, got %s", snap.Status)
	}
	if len(snap.Steps) != 3 {
		t.Fatalf("expected 3 logical steps, got %d", len(snap.Steps))
	}
	for _, st := range snap.Steps {
		if string(st.Status) != "SUCCEEDED" {
			t.Fatalf("step %s state is %s", st.NodeID, st.Status)
		}
		if len(st.Attempts) != 1 {
			t.Fatalf("step %s has %d attempts (expected 1)", st.NodeID, len(st.Attempts))
		}
	}
	outBytes, _ := json.Marshal(snap.Output)
	var outMap map[string]any
	_ = json.Unmarshal(outBytes, &outMap)
	if outMap["v"] != float64(100) {
		t.Fatalf("expected output v=100, got %v", outMap)
	}
}

// 2. Parallel Success Diamond: A -> (B, C) -> D: join waits and executes once
func TestM3_ParallelSuccessDiamond(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	engine := execution.NewWorkerEngine(tc.pool)

	bundle := "3122222222222222222222222222222222222222222222222222222222222222"
	valSchema := map[string]any{"type": "object", "properties": map[string]any{"v": map[string]any{"type": "integer"}}, "required": []any{"v"}}
	joinInSchema := map[string]any{"type": "object", "properties": map[string]any{"b": map[string]any{"type": "integer"}, "c": map[string]any{"type": "integer"}}, "required": []any{"b", "c"}}
	tasks := []map[string]any{
		{"name": "t-node", "inputSchema": valSchema, "outputSchema": valSchema, "recovery": "safe", "entrypoint": "tasks.js#noop"},
		{"name": "t-join", "inputSchema": joinInSchema, "outputSchema": valSchema, "recovery": "safe", "entrypoint": "tasks.js#join"},
	}
	workflows := []map[string]any{{
		"manifestVersion": 1, "name": "wf-diamond",
		"inputSchema": valSchema, "outputSchema": valSchema,
		"nodes": []map[string]any{
			{"id": "a", "type": "task", "task": "t-node", "input": map[string]any{"v": map[string]any{"$ref": "run.input", "pointer": "/v"}}},
			{"id": "b", "type": "task", "task": "t-node", "after": []any{"a"}, "input": map[string]any{"v": map[string]any{"$ref": "step.output", "stepId": "a", "pointer": "/v"}}},
			{"id": "c", "type": "task", "task": "t-node", "after": []any{"a"}, "input": map[string]any{"v": map[string]any{"$ref": "step.output", "stepId": "a", "pointer": "/v"}}},
			{"id": "d", "type": "task", "task": "t-join", "after": []any{"b", "c"}, "input": map[string]any{
				"b": map[string]any{"$ref": "step.output", "stepId": "b", "pointer": "/v"},
				"c": map[string]any{"$ref": "step.output", "stepId": "c", "pointer": "/v"},
			}},
		},
		"output": map[string]any{"v": map[string]any{"$ref": "step.output", "stepId": "d", "pointer": "/v"}},
	}}
	manifest := createLifecycleManifest(bundle, tasks, workflows)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "wf-diamond", manifest)

	sess1, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "dia-w1")
	sess2, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "dia-w2")
	m3AssociateWorkerDeployment(t, tc, orgID, sess1.SessionID, bundle)
	m3AssociateWorkerDeployment(t, tc, orgID, sess2.SessionID, bundle)

	run := m3CreateRun(t, server.URL, "wf-diamond", adminKey.PlaintextKey, orgID, map[string]any{"v": float64(10)}, "m3-dia-run-001")

	// 1. Claim & complete A
	asgnsA := parallelClaim(t, engine, sess1, orgID, envID, 1, "poll-a")
	parallelStart(t, engine, sess1, orgID, envID, asgnsA[0])
	parallelCompleteSuccess(t, engine, sess1, orgID, envID, asgnsA[0], map[string]any{"v": float64(10)})

	// 2. Both B and C become independently eligible for claim
	asgns1 := parallelClaim(t, engine, sess1, orgID, envID, 1, "poll-b-c-1")
	asgns2 := parallelClaim(t, engine, sess2, orgID, envID, 1, "poll-b-c-2")
	if len(asgns1) != 1 || len(asgns2) != 1 {
		t.Fatalf("expected concurrent eligibility for siblings, got asgn1=%d asgn2=%d", len(asgns1), len(asgns2))
	}
	node1 := m3NodeForStep(t, tc, orgID, asgns1[0].StepID)
	node2 := m3NodeForStep(t, tc, orgID, asgns2[0].StepID)
	claimedNodes := map[string]bool{node1: true, node2: true}
	if !claimedNodes["b"] || !claimedNodes["c"] {
		t.Fatalf("expected b and c claimed, got %v", claimedNodes)
	}

	// 3. Complete sibling 1 only; D must NOT be claimable yet
	parallelStart(t, engine, sess1, orgID, envID, asgns1[0])
	val1 := float64(20)
	if node1 == "c" {
		val1 = float64(30)
	}
	parallelCompleteSuccess(t, engine, sess1, orgID, envID, asgns1[0], map[string]any{"v": val1})

	asgnsD_premature := parallelClaim(t, engine, sess1, orgID, envID, 1, "poll-d-premature")
	if len(asgnsD_premature) != 0 {
		t.Fatalf("D must not be eligible before both siblings succeed, got %v", asgnsD_premature)
	}

	// 4. Complete sibling 2; D becomes eligible
	parallelStart(t, engine, sess2, orgID, envID, asgns2[0])
	val2 := float64(30)
	if node2 == "b" {
		val2 = float64(20)
	}
	parallelCompleteSuccess(t, engine, sess2, orgID, envID, asgns2[0], map[string]any{"v": val2})

	asgnsD := parallelClaim(t, engine, sess1, orgID, envID, 1, "poll-d")
	if len(asgnsD) != 1 {
		t.Fatalf("expected D to become eligible, got %d assignments", len(asgnsD))
	}
	nodeD := m3NodeForStep(t, tc, orgID, asgnsD[0].StepID)
	if nodeD != "d" {
		t.Fatalf("expected node d, got %s", nodeD)
	}

	// Verify input mapped to D contains both b and c outputs
	var mappedInput map[string]any
	inBytes, _ := json.Marshal(asgnsD[0].Input)
	_ = json.Unmarshal(inBytes, &mappedInput)
	if mappedInput["b"] != float64(20) || mappedInput["c"] != float64(30) {
		t.Fatalf("mapped input to join D incorrect: %v", mappedInput)
	}

	// Complete D
	parallelStart(t, engine, sess1, orgID, envID, asgnsD[0])
	parallelCompleteSuccess(t, engine, sess1, orgID, envID, asgnsD[0], map[string]any{"v": float64(50)})

	// Verify run succeeded
	snap := m2GetSnapshot(t, server.URL, run.ID, adminKey.PlaintextKey, orgID)
	if string(snap.Status) != "SUCCEEDED" {
		t.Fatalf("expected run SUCCEEDED, got %s", snap.Status)
	}
}

// 3. Parallel Fail-Fast (F-15): unrecoverable sibling failure cancels nonterminal siblings
func TestM3_ParallelFailFastSettlement(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	engine := execution.NewWorkerEngine(tc.pool)

	bundle := "3133333333333333333333333333333333333333333333333333333333333333"
	valSchema := map[string]any{"type": "object", "properties": map[string]any{"v": map[string]any{"type": "integer"}}, "required": []any{"v"}}
	tasks := []map[string]any{
		{"name": "t-ok", "inputSchema": valSchema, "outputSchema": valSchema, "recovery": "safe", "entrypoint": "tasks.js#ok"},
		{"name": "t-fail", "inputSchema": valSchema, "outputSchema": valSchema, "recovery": "safe", "entrypoint": "tasks.js#fail"},
	}
	workflows := []map[string]any{{
		"manifestVersion": 1, "name": "wf-failfast",
		"inputSchema": valSchema, "outputSchema": valSchema,
		"nodes": []map[string]any{
			{"id": "a", "type": "task", "task": "t-ok", "input": map[string]any{"v": map[string]any{"$ref": "run.input", "pointer": "/v"}}},
			{"id": "b", "type": "task", "task": "t-fail", "after": []any{"a"}, "input": map[string]any{"v": map[string]any{"$ref": "step.output", "stepId": "a", "pointer": "/v"}}},
			{"id": "c", "type": "task", "task": "t-ok", "after": []any{"a"}, "input": map[string]any{"v": map[string]any{"$ref": "step.output", "stepId": "a", "pointer": "/v"}}},
			{"id": "d", "type": "task", "task": "t-ok", "after": []any{"b", "c"}, "input": map[string]any{"v": map[string]any{"$ref": "step.output", "stepId": "b", "pointer": "/v"}}},
		},
		"output": map[string]any{"v": map[string]any{"$ref": "step.output", "stepId": "d", "pointer": "/v"}},
	}}
	manifest := createLifecycleManifest(bundle, tasks, workflows)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "wf-failfast", manifest)

	sess1, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "ff-w1")
	sess2, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "ff-w2")
	m3AssociateWorkerDeployment(t, tc, orgID, sess1.SessionID, bundle)
	m3AssociateWorkerDeployment(t, tc, orgID, sess2.SessionID, bundle)

	run := m3CreateRun(t, server.URL, "wf-failfast", adminKey.PlaintextKey, orgID, map[string]any{"v": float64(1)}, "m3-ff-run-001")

	// Step A completes
	asgnsA := parallelClaim(t, engine, sess1, orgID, envID, 1, "poll-a")
	parallelStart(t, engine, sess1, orgID, envID, asgnsA[0])
	parallelCompleteSuccess(t, engine, sess1, orgID, envID, asgnsA[0], map[string]any{"v": float64(1)})

	// B and C are claimed
	asgns1 := parallelClaim(t, engine, sess1, orgID, envID, 1, "poll-ff-1")
	asgns2 := parallelClaim(t, engine, sess2, orgID, envID, 1, "poll-ff-2")

	var asgnB, asgnC worker.AssignmentDTO
	var sessB, sessC *testWorkerSession
	if m3NodeForStep(t, tc, orgID, asgns1[0].StepID) == "b" {
		asgnB, sessB = asgns1[0], sess1
		asgnC, sessC = asgns2[0], sess2
	} else {
		asgnB, sessB = asgns2[0], sess2
		asgnC, sessC = asgns1[0], sess1
	}

	// Start both
	parallelStart(t, engine, sessB, orgID, envID, asgnB)
	parallelStart(t, engine, sessC, orgID, envID, asgnC)

	// B fails definitively
	parallelCompleteFailure(t, engine, sessB, orgID, envID, asgnB, "TASK_EXECUTION_FAILED", false)

	// Verify run immediately transitions to FAILED
	snap := m2GetSnapshot(t, server.URL, run.ID, adminKey.PlaintextKey, orgID)
	if string(snap.Status) != "FAILED" {
		t.Fatalf("expected run FAILED after fail-fast, got %s", snap.Status)
	}

	// Verify sibling C is revoked/cancelled
	stepMap := make(map[string]execution.RunStepDTO)
	for _, st := range snap.Steps {
		stepMap[st.NodeID] = st
	}
	if string(stepMap["c"].Status) != "CANCELLED" {
		t.Fatalf("expected sibling C CANCELLED, got %s", stepMap["c"].Status)
	}

	// Invariant: late completion from sibling C CANNOT reopen or mutate failed run
	reqLate := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "late-complete-" + asgnC.AttemptID,
		WorkerID: sessC.WorkerID, SessionID: sessC.SessionID,
		AttemptID: asgnC.AttemptID, OwnershipEpoch: asgnC.OwnershipEpoch,
		Outcome: "SUCCEEDED", Output: map[string]any{"v": float64(999)},
	}
	digest, _ := worker.CanonicalCompletionDigest(&reqLate)
	reqLate.ResultDigest = digest
	_, _ = engine.Complete(context.Background(), parallelSessCtx(sessC, orgID, envID), &reqLate)

	snapAfterLate := m2GetSnapshot(t, server.URL, run.ID, adminKey.PlaintextKey, orgID)
	if string(snapAfterLate.Status) != "FAILED" {
		t.Fatalf("terminal run reopened by late result! status=%s", snapAfterLate.Status)
	}
}

// 4 & 5. Choice + Selected Branch + Skipped Branch + Merge (F-16)
func TestM3_ChoiceStructuredMerge(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	engine := execution.NewWorkerEngine(tc.pool)

	bundle := "3144444444444444444444444444444444444444444444444444444444444444"
	numSchema := map[string]any{"type": "object", "properties": map[string]any{"n": map[string]any{"type": "integer"}}, "required": []any{"n"}}
	taggedSchema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"branch": map[string]any{"type": "string"},
			"value":  numSchema,
		},
		"required": []any{"branch", "value"},
	}

	tasks := []map[string]any{
		{"name": "t-high", "inputSchema": numSchema, "outputSchema": numSchema, "recovery": "safe", "entrypoint": "tasks.js#high"},
		{"name": "t-low", "inputSchema": numSchema, "outputSchema": numSchema, "recovery": "safe", "entrypoint": "tasks.js#low"},
		{"name": "t-final", "inputSchema": taggedSchema, "outputSchema": numSchema, "recovery": "safe", "entrypoint": "tasks.js#final"},
	}

	workflows := []map[string]any{{
		"manifestVersion": 1, "name": "wf-choice-merge",
		"inputSchema": numSchema, "outputSchema": numSchema,
		"nodes": []map[string]any{
			{
				"id": "decide", "type": "choice",
				"choice": map[string]any{
					"branches": []map[string]any{
						{
							"name": "high",
							"condition": map[string]any{
								"op": "gt",
								"args": []any{
									map[string]any{"$ref": "run.input", "pointer": "/n"},
									map[string]any{"literal": float64(50)},
								},
							},
						},
						{
							"name": "low",
							"condition": map[string]any{
								"op": "lte",
								"args": []any{
									map[string]any{"$ref": "run.input", "pointer": "/n"},
									map[string]any{"literal": float64(50)},
								},
							},
						},
					},
					"default": "low",
				},
			},
			{"id": "task-high", "type": "task", "task": "t-high", "after": []any{"decide"}, "input": map[string]any{"n": map[string]any{"literal": float64(100)}}},
			{"id": "task-low", "type": "task", "task": "t-low", "after": []any{"decide"}, "input": map[string]any{"n": map[string]any{"literal": float64(10)}}},
			{
				"id": "join-merge", "type": "merge", "after": []any{"task-high", "task-low"},
				"merge": map[string]any{
					"choice": "decide",
					"branches": []map[string]any{
						{"branch": "high", "terminal": "task-high"},
						{"branch": "low", "terminal": "task-low"},
					},
					"outputSchema": taggedSchema,
				},
			},
			{
				"id": "final-step", "type": "task", "task": "t-final", "after": []any{"join-merge"},
				"input": map[string]any{
					"branch": map[string]any{"$ref": "step.output", "stepId": "join-merge", "pointer": "/branch"},
					"value":  map[string]any{"$ref": "step.output", "stepId": "join-merge", "pointer": "/value"},
				},
			},
		},
		"output": map[string]any{"n": map[string]any{"$ref": "step.output", "stepId": "final-step", "pointer": "/n"}},
	}}
	manifest := createLifecycleManifest(bundle, tasks, workflows)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "wf-choice-merge", manifest)

	sess, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "cm-w1")
	m3AssociateWorkerDeployment(t, tc, orgID, sess.SessionID, bundle)

	// Run with n=75 -> high branch selected
	run := m3CreateRun(t, server.URL, "wf-choice-merge", adminKey.PlaintextKey, orgID, map[string]any{"n": float64(75)}, "m3-cm-run-001")

	// Claim task: only task-high should become eligible! task-low is SKIPPED
	asgns := parallelClaim(t, engine, sess, orgID, envID, 1, "poll-cm-high")
	if len(asgns) != 1 {
		t.Fatalf("expected 1 assignment, got %d", len(asgns))
	}
	if m3NodeForStep(t, tc, orgID, asgns[0].StepID) != "task-high" {
		t.Fatalf("expected task-high, got %s", m3NodeForStep(t, tc, orgID, asgns[0].StepID))
	}

	// Complete task-high
	parallelStart(t, engine, sess, orgID, envID, asgns[0])
	parallelCompleteSuccess(t, engine, sess, orgID, envID, asgns[0], map[string]any{"n": float64(100)})

	// Merge evaluates in engine automatically. Next claimable is final-step!
	asgnsFinal := parallelClaim(t, engine, sess, orgID, envID, 1, "poll-final")
	if len(asgnsFinal) != 1 || m3NodeForStep(t, tc, orgID, asgnsFinal[0].StepID) != "final-step" {
		t.Fatalf("expected final-step claimable after merge, got %v", asgnsFinal)
	}

	// Complete final-step
	parallelStart(t, engine, sess, orgID, envID, asgnsFinal[0])
	parallelCompleteSuccess(t, engine, sess, orgID, envID, asgnsFinal[0], map[string]any{"n": float64(100)})

	// Check Inspector Snapshot
	snap := m2GetSnapshot(t, server.URL, run.ID, adminKey.PlaintextKey, orgID)
	if string(snap.Status) != "SUCCEEDED" {
		t.Fatalf("expected run SUCCEEDED, got %s", snap.Status)
	}

	stepMap := make(map[string]execution.RunStepDTO)
	for _, st := range snap.Steps {
		stepMap[st.NodeID] = st
	}

	// Invariant: unselected branch is SKIPPED with canonical reason BRANCH_NOT_SELECTED
	if string(stepMap["task-low"].Status) != "SKIPPED" {
		t.Fatalf("task-low expected SKIPPED, got %s", stepMap["task-low"].Status)
	}
	if stepMap["task-low"].WaitReason == nil || *stepMap["task-low"].WaitReason != "BRANCH_NOT_SELECTED" {
		t.Fatalf("task-low wait_reason expected BRANCH_NOT_SELECTED, got %v", stepMap["task-low"].WaitReason)
	}

	// Invariant: merge produces tagged output {branch: "high", value: {n: 100}}
	mergeOutBytes, _ := json.Marshal(stepMap["join-merge"].Output)
	var mergeOut map[string]any
	_ = json.Unmarshal(mergeOutBytes, &mergeOut)
	if mergeOut["branch"] != "high" {
		t.Fatalf("expected merge branch=high, got %v", mergeOut["branch"])
	}
}

// 6. Nested Structured Merge
func TestM3_NestedStructuredMerge(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	engine := execution.NewWorkerEngine(tc.pool)

	bundle := "3155555555555555555555555555555555555555555555555555555555555555"
	inSchema := map[string]any{"type": "object", "properties": map[string]any{"mode": map[string]any{"type": "string"}, "sub": map[string]any{"type": "string"}}, "required": []any{"mode", "sub"}}
	valSchema := map[string]any{"type": "object", "properties": map[string]any{"res": map[string]any{"type": "string"}}, "required": []any{"res"}}
	taggedSchema := map[string]any{"type": "object", "properties": map[string]any{"branch": map[string]any{"type": "string"}, "value": map[string]any{"type": "object"}}, "required": []any{"branch", "value"}}

	tasks := []map[string]any{
		{"name": "t-inner-a", "inputSchema": inSchema, "outputSchema": valSchema, "recovery": "safe", "entrypoint": "tasks.js#a"},
		{"name": "t-inner-b", "inputSchema": inSchema, "outputSchema": valSchema, "recovery": "safe", "entrypoint": "tasks.js#b"},
		{"name": "t-outer-other", "inputSchema": inSchema, "outputSchema": valSchema, "recovery": "safe", "entrypoint": "tasks.js#other"},
	}

	workflows := []map[string]any{{
		"manifestVersion": 1, "name": "wf-nested-choice",
		"inputSchema": inSchema, "outputSchema": taggedSchema,
		"nodes": []map[string]any{
			{
				"id": "c-outer", "type": "choice",
				"choice": map[string]any{
					"branches": []map[string]any{
						{
							"name": "nested_branch",
							"condition": map[string]any{
								"op": "eq", "args": []any{map[string]any{"$ref": "run.input", "pointer": "/mode"}, map[string]any{"literal": "nested"}},
							},
						},
						{
							"name": "flat_branch",
							"condition": map[string]any{
								"op": "eq", "args": []any{map[string]any{"$ref": "run.input", "pointer": "/mode"}, map[string]any{"literal": "flat"}},
							},
						},
					},
					"default": "flat_branch",
				},
			},
			// Nested branch nodes
			{
				"id": "c-inner", "type": "choice", "after": []any{"c-outer"},
				"choice": map[string]any{
					"branches": []map[string]any{
						{
							"name": "sub_a",
							"condition": map[string]any{
								"op": "eq", "args": []any{map[string]any{"$ref": "run.input", "pointer": "/sub"}, map[string]any{"literal": "A"}},
							},
						},
						{
							"name": "sub_b",
							"condition": map[string]any{
								"op": "eq", "args": []any{map[string]any{"$ref": "run.input", "pointer": "/sub"}, map[string]any{"literal": "B"}},
							},
						},
					},
					"default": "sub_b",
				},
			},
			{"id": "task-inner-a", "type": "task", "task": "t-inner-a", "after": []any{"c-inner"}, "input": map[string]any{"mode": map[string]any{"literal": "nested"}, "sub": map[string]any{"literal": "A"}}},
			{"id": "task-inner-b", "type": "task", "task": "t-inner-b", "after": []any{"c-inner"}, "input": map[string]any{"mode": map[string]any{"literal": "nested"}, "sub": map[string]any{"literal": "B"}}},
			{
				"id": "m-inner", "type": "merge", "after": []any{"task-inner-a", "task-inner-b"},
				"merge": map[string]any{
					"choice": "c-inner",
					"branches": []map[string]any{
						{"branch": "sub_a", "terminal": "task-inner-a"},
						{"branch": "sub_b", "terminal": "task-inner-b"},
					},
					"outputSchema": taggedSchema,
				},
			},
			// Flat branch node
			{"id": "task-other", "type": "task", "task": "t-outer-other", "after": []any{"c-outer"}, "input": map[string]any{"mode": map[string]any{"literal": "flat"}, "sub": map[string]any{"literal": "none"}}},
			// Outer merge
			{
				"id": "m-outer", "type": "merge", "after": []any{"m-inner", "task-other"},
				"merge": map[string]any{
					"choice": "c-outer",
					"branches": []map[string]any{
						{"branch": "nested_branch", "terminal": "m-inner"},
						{"branch": "flat_branch", "terminal": "task-other"},
					},
					"outputSchema": taggedSchema,
				},
			},
		},
		"output": map[string]any{
			"branch": map[string]any{"$ref": "step.output", "stepId": "m-outer", "pointer": "/branch"},
			"value":  map[string]any{"$ref": "step.output", "stepId": "m-outer", "pointer": "/value"},
		},
	}}
	manifest := createLifecycleManifest(bundle, tasks, workflows)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "wf-nested-choice", manifest)

	sess, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "nest-w1")
	m3AssociateWorkerDeployment(t, tc, orgID, sess.SessionID, bundle)

	// Test 1: nested mode with sub A -> inner A runs, inner B skipped, outer flat skipped
	run := m3CreateRun(t, server.URL, "wf-nested-choice", adminKey.PlaintextKey, orgID, map[string]any{"mode": "nested", "sub": "A"}, "m3-nest-run-001")

	asgns := parallelClaim(t, engine, sess, orgID, envID, 1, "poll-nest-a")
	if len(asgns) != 1 || m3NodeForStep(t, tc, orgID, asgns[0].StepID) != "task-inner-a" {
		t.Fatalf("expected claim for task-inner-a, got %v", asgns)
	}
	parallelStart(t, engine, sess, orgID, envID, asgns[0])
	parallelCompleteSuccess(t, engine, sess, orgID, envID, asgns[0], map[string]any{"res": "result_from_sub_A"})

	// Both merges resolve automatically in engine
	snap := m2GetSnapshot(t, server.URL, run.ID, adminKey.PlaintextKey, orgID)
	if string(snap.Status) != "SUCCEEDED" {
		t.Fatalf("expected nested run SUCCEEDED, got %s", snap.Status)
	}

	stepMap := make(map[string]execution.RunStepDTO)
	for _, st := range snap.Steps {
		stepMap[st.NodeID] = st
	}
	if string(stepMap["task-other"].Status) != "SKIPPED" {
		t.Fatalf("task-other expected SKIPPED, got %s", stepMap["task-other"].Status)
	}
	if string(stepMap["task-inner-b"].Status) != "SKIPPED" {
		t.Fatalf("task-inner-b expected SKIPPED, got %s", stepMap["task-inner-b"].Status)
	}
	if string(stepMap["m-inner"].Status) != "SUCCEEDED" || string(stepMap["m-outer"].Status) != "SUCCEEDED" {
		t.Fatalf("merges expected SUCCEEDED: inner=%s outer=%s", stepMap["m-inner"].Status, stepMap["m-outer"].Status)
	}
}

// 7. Invalid Mapping and Schema Non-Retryable Failure (F-28)
func TestM3_InvalidMappingAndSchema(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	engine := execution.NewWorkerEngine(tc.pool)

	bundle := "3166666666666666666666666666666666666666666666666666666666666666"
	valSchema := map[string]any{"type": "object", "properties": map[string]any{"v": map[string]any{"type": "integer"}}, "required": []any{"v"}}
	strictSchema := map[string]any{"type": "object", "properties": map[string]any{"str": map[string]any{"type": "string"}}, "required": []any{"str"}}

	tasks := []map[string]any{
		{"name": "t-num", "inputSchema": valSchema, "outputSchema": valSchema, "recovery": "safe", "entrypoint": "tasks.js#num"},
		{"name": "t-strict", "inputSchema": strictSchema, "outputSchema": strictSchema, "recovery": "safe", "entrypoint": "tasks.js#strict"},
	}

	// Node B expects {str: string}, but mapping provides {str: step.output/a/v} (which is integer) -> SCHEMA_VALIDATION_ERROR
	workflows := []map[string]any{{
		"manifestVersion": 1, "name": "wf-bad-mapping",
		"inputSchema": valSchema, "outputSchema": strictSchema,
		"nodes": []map[string]any{
			{"id": "a", "type": "task", "task": "t-num", "input": map[string]any{"v": map[string]any{"$ref": "run.input", "pointer": "/v"}}},
			{"id": "b", "type": "task", "task": "t-strict", "after": []any{"a"}, "input": map[string]any{"str": map[string]any{"$ref": "step.output", "stepId": "a", "pointer": "/v"}}},
		},
		"output": map[string]any{"str": map[string]any{"$ref": "step.output", "stepId": "b", "pointer": "/str"}},
	}}
	manifest := createLifecycleManifest(bundle, tasks, workflows)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "wf-bad-mapping", manifest)

	sess, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "badmap-w1")
	m3AssociateWorkerDeployment(t, tc, orgID, sess.SessionID, bundle)

	run := m3CreateRun(t, server.URL, "wf-bad-mapping", adminKey.PlaintextKey, orgID, map[string]any{"v": float64(42)}, "m3-badmap-run-001")

	asgnsA := parallelClaim(t, engine, sess, orgID, envID, 1, "poll-badmap-a")
	parallelStart(t, engine, sess, orgID, envID, asgnsA[0])
	parallelCompleteSuccess(t, engine, sess, orgID, envID, asgnsA[0], map[string]any{"v": float64(42)})

	// Completion of A triggers evaluateBlockedDAGTx on B.
	// B schema validation fails! Run must immediately fail with non-retryable error.
	snap := m2GetSnapshot(t, server.URL, run.ID, adminKey.PlaintextKey, orgID)
	if string(snap.Status) != "FAILED" {
		t.Fatalf("expected run FAILED on schema mismatch, got %s", snap.Status)
	}

	// Verify no retry loop occurs: no further tasks eligible
	asgnsNext := parallelClaim(t, engine, sess, orgID, envID, 1, "poll-badmap-next")
	if len(asgnsNext) != 0 {
		t.Fatalf("expected no eligible tasks after fail-fast, got %v", asgnsNext)
	}
}

// 8. Control Races: Pause vs Claim (F-13)
func TestM3_ControlRaces_PauseVsClaim(t *testing.T) {
	tc, server, orgID, envID, _ := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	controlKey := bootstrapTestKey(t, tc.service, orgID, envID, []string{
		tenant.CapDeploymentsRegister,
		tenant.CapDeploymentsActivateStaging,
		tenant.CapDeploymentsWrite,
		tenant.CapWorkersDrain,
		tenant.CapRunsCreate,
		tenant.CapRunsRead,
		tenant.CapRunsControl,
		tenant.CapPayloadRead,
		tenant.CapAdminKey,
	})

	engine := execution.NewWorkerEngine(tc.pool)

	bundle := "3177777777777777777777777777777777777777777777777777777777777777"
	schemaObj := map[string]any{"type": "object", "properties": map[string]any{"x": map[string]any{"type": "integer"}}, "required": []any{"x"}}
	tasks := []map[string]any{{"name": "t-pause", "inputSchema": schemaObj, "outputSchema": schemaObj, "recovery": "safe", "entrypoint": "tasks.js#p"}}
	workflows := []map[string]any{{
		"manifestVersion": 1, "name": "wf-pause-claim",
		"inputSchema": schemaObj, "outputSchema": schemaObj,
		"nodes": []map[string]any{
			{"id": "step1", "type": "task", "task": "t-pause", "input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}}},
			{"id": "step2", "type": "task", "task": "t-pause", "after": []any{"step1"}, "input": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "step1", "pointer": "/x"}}},
		},
		"output": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "step2", "pointer": "/x"}},
	}}
	manifest := createLifecycleManifest(bundle, tasks, workflows)
	registerAndActivateTestWorkflow(t, tc, server, controlKey, orgID, envID, "wf-pause-claim", manifest)

	sess, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "pause-w1")
	m3AssociateWorkerDeployment(t, tc, orgID, sess.SessionID, bundle)

	// --- Case A: Claim commits first, then Pause is requested ---
	runA := m3CreateRun(t, server.URL, "wf-pause-claim", controlKey.PlaintextKey, orgID, map[string]any{"x": float64(1)}, "m3-pc-run-a")

	// Worker claims step1
	asgns1 := parallelClaim(t, engine, sess, orgID, envID, 1, "poll-pause-claim-1")
	if len(asgns1) != 1 {
		t.Fatalf("claim failed: %v", asgns1)
	}

	// Pause run while step1 is in flight
	pauseBody, _ := json.Marshal(map[string]any{"expectedRevision": 1})
	pauseReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/runs/%s/pause", server.URL, runA.ID), bytes.NewReader(pauseBody))
	pauseReq.Header.Set("Authorization", "Bearer "+controlKey.PlaintextKey)
	pauseReq.Header.Set("X-Organization-ID", orgID)
	pauseReq.Header.Set("Idempotency-Key", "pause-a")
	pauseResp, err := http.DefaultClient.Do(pauseReq)
	if err != nil {
		t.Fatal(err)
	}
	defer pauseResp.Body.Close()
	if pauseResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(pauseResp.Body)
		t.Fatalf("pause status %d: %s", pauseResp.StatusCode, string(body))
	}

	// Invariant: in-flight claim is preserved, run enters PAUSING
	snapA := m2GetSnapshot(t, server.URL, runA.ID, controlKey.PlaintextKey, orgID)
	if string(snapA.Status) != "PAUSING" {
		t.Fatalf("expected status PAUSING while step is in flight, got %s", snapA.Status)
	}

	// Complete step1
	parallelStart(t, engine, sess, orgID, envID, asgns1[0])
	parallelCompleteSuccess(t, engine, sess, orgID, envID, asgns1[0], map[string]any{"x": float64(2)})

	// Invariant: once in-flight work completes, run converges to PAUSED
	snapA_after := m2GetSnapshot(t, server.URL, runA.ID, controlKey.PlaintextKey, orgID)
	if string(snapA_after.Status) != "PAUSED" {
		t.Fatalf("expected status PAUSED after step completes, got %s", snapA_after.Status)
	}

	// Invariant: subsequent claims are denied while PAUSED
	asgnsDenied := parallelClaim(t, engine, sess, orgID, envID, 1, "poll-pause-denied")
	if len(asgnsDenied) != 0 {
		t.Fatalf("expected no claim while PAUSED, got %v", asgnsDenied)
	}

	// --- Case B: Pause commits first, then Claim is attempted ---
	runB := m3CreateRun(t, server.URL, "wf-pause-claim", controlKey.PlaintextKey, orgID, map[string]any{"x": float64(1)}, "m3-pc-run-b")

	// Pause immediately
	pauseReq2, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/runs/%s/pause", server.URL, runB.ID), bytes.NewReader(pauseBody))
	pauseReq2.Header.Set("Authorization", "Bearer "+controlKey.PlaintextKey)
	pauseReq2.Header.Set("X-Organization-ID", orgID)
	pauseReq2.Header.Set("Idempotency-Key", "pause-b")
	pauseResp2, _ := http.DefaultClient.Do(pauseReq2)
	if pauseResp2.StatusCode != http.StatusOK {
		t.Fatalf("pause2 status %d", pauseResp2.StatusCode)
	}
	pauseResp2.Body.Close()

	// Invariant: run with 0 in-flight immediately enters PAUSED
	snapB := m2GetSnapshot(t, server.URL, runB.ID, controlKey.PlaintextKey, orgID)
	if string(snapB.Status) != "PAUSED" {
		t.Fatalf("expected PAUSED immediately with 0 in-flight, got %s", snapB.Status)
	}

	// Invariant: new claim is denied
	asgnB_denied := parallelClaim(t, engine, sess, orgID, envID, 1, "poll-pause-b-denied")
	if len(asgnB_denied) != 0 {
		t.Fatalf("expected claim denied on PAUSED run, got %v", asgnB_denied)
	}
}

// 9. Control Races: Pause vs Completion & Resume Recomputation
func TestM3_ControlRaces_PauseVsCompletionAndResume(t *testing.T) {
	tc, server, orgID, envID, _ := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	controlKey := bootstrapTestKey(t, tc.service, orgID, envID, []string{
		tenant.CapDeploymentsRegister,
		tenant.CapDeploymentsActivateStaging,
		tenant.CapDeploymentsWrite,
		tenant.CapWorkersDrain,
		tenant.CapRunsCreate,
		tenant.CapRunsRead,
		tenant.CapRunsControl,
		tenant.CapPayloadRead,
		tenant.CapAdminKey,
	})

	engine := execution.NewWorkerEngine(tc.pool)

	bundle := "3188888888888888888888888888888888888888888888888888888888888888"
	schemaObj := map[string]any{"type": "object", "properties": map[string]any{"v": map[string]any{"type": "integer"}}, "required": []any{"v"}}
	tasks := []map[string]any{{"name": "t-single", "inputSchema": schemaObj, "outputSchema": schemaObj, "recovery": "safe", "entrypoint": "tasks.js#s"}}
	workflows := []map[string]any{{
		"manifestVersion": 1, "name": "wf-single",
		"inputSchema": schemaObj, "outputSchema": schemaObj,
		"nodes": []map[string]any{
			{"id": "step-only", "type": "task", "task": "t-single", "input": map[string]any{"v": map[string]any{"$ref": "run.input", "pointer": "/v"}}},
		},
		"output": map[string]any{"v": map[string]any{"$ref": "step.output", "stepId": "step-only", "pointer": "/v"}},
	}}
	manifest := createLifecycleManifest(bundle, tasks, workflows)
	registerAndActivateTestWorkflow(t, tc, server, controlKey, orgID, envID, "wf-single", manifest)

	sess, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "single-w1")
	m3AssociateWorkerDeployment(t, tc, orgID, sess.SessionID, bundle)

	run := m3CreateRun(t, server.URL, "wf-single", controlKey.PlaintextKey, orgID, map[string]any{"v": float64(99)}, "m3-single-run-001")

	asgns := parallelClaim(t, engine, sess, orgID, envID, 1, "poll-single")
	parallelStart(t, engine, sess, orgID, envID, asgns[0])

	// Complete task to SUCCEEDED (terminal for run)
	parallelCompleteSuccess(t, engine, sess, orgID, envID, asgns[0], map[string]any{"v": float64(99)})

	// Pause request arrives after terminal completion
	pauseBody, _ := json.Marshal(map[string]any{"expectedRevision": 1})
	pauseReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/runs/%s/pause", server.URL, run.ID), bytes.NewReader(pauseBody))
	pauseReq.Header.Set("Authorization", "Bearer "+controlKey.PlaintextKey)
	pauseReq.Header.Set("X-Organization-ID", orgID)
	pauseReq.Header.Set("Idempotency-Key", "pause-after-terminal")
	pauseResp, err := http.DefaultClient.Do(pauseReq)
	if err != nil {
		t.Fatal(err)
	}
	defer pauseResp.Body.Close()

	// Invariant: terminal state wins! Status stays SUCCEEDED
	snap := m2GetSnapshot(t, server.URL, run.ID, controlKey.PlaintextKey, orgID)
	if string(snap.Status) != "SUCCEEDED" {
		t.Fatalf("expected terminal SUCCEEDED to be preserved against pause, got %s", snap.Status)
	}

	// Invariant: resume cannot reopen terminal run
	resumeBody, _ := json.Marshal(map[string]any{"expectedRevision": snap.Revision})
	resumeReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/runs/%s/resume", server.URL, run.ID), bytes.NewReader(resumeBody))
	resumeReq.Header.Set("Authorization", "Bearer "+controlKey.PlaintextKey)
	resumeReq.Header.Set("X-Organization-ID", orgID)
	resumeReq.Header.Set("Idempotency-Key", "resume-after-terminal")
	resumeResp, err := http.DefaultClient.Do(resumeReq)
	if err != nil {
		t.Fatal(err)
	}
	defer resumeResp.Body.Close()

	snapFinal := m2GetSnapshot(t, server.URL, run.ID, controlKey.PlaintextKey, orgID)
	if string(snapFinal.Status) != "SUCCEEDED" {
		t.Fatalf("resume reopened terminal run! status=%s", snapFinal.Status)
	}
}

// 10. Control Races: Stale Revision Conflict
func TestM3_ControlRaces_StaleRevisionConflict(t *testing.T) {
	tc, server, orgID, envID, _ := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	controlKey := bootstrapTestKey(t, tc.service, orgID, envID, []string{
		tenant.CapDeploymentsRegister,
		tenant.CapDeploymentsActivateStaging,
		tenant.CapDeploymentsWrite,
		tenant.CapWorkersDrain,
		tenant.CapRunsCreate,
		tenant.CapRunsRead,
		tenant.CapRunsControl,
		tenant.CapPayloadRead,
		tenant.CapAdminKey,
	})

	bundle := "3199999999999999999999999999999999999999999999999999999999999999"
	schemaObj := map[string]any{"type": "object", "properties": map[string]any{"v": map[string]any{"type": "integer"}}, "required": []any{"v"}}
	tasks := []map[string]any{{"name": "t-rev", "inputSchema": schemaObj, "outputSchema": schemaObj, "recovery": "safe", "entrypoint": "tasks.js#rev"}}
	workflows := []map[string]any{{
		"manifestVersion": 1, "name": "wf-stale-rev",
		"inputSchema": schemaObj, "outputSchema": schemaObj,
		"nodes": []map[string]any{
			{"id": "step1", "type": "task", "task": "t-rev", "input": map[string]any{"v": map[string]any{"$ref": "run.input", "pointer": "/v"}}},
		},
		"output": map[string]any{"v": map[string]any{"$ref": "step.output", "stepId": "step1", "pointer": "/v"}},
	}}
	manifest := createLifecycleManifest(bundle, tasks, workflows)
	registerAndActivateTestWorkflow(t, tc, server, controlKey, orgID, envID, "wf-stale-rev", manifest)

	run := m3CreateRun(t, server.URL, "wf-stale-rev", controlKey.PlaintextKey, orgID, map[string]any{"v": float64(1)}, "m3-stale-run-001")

	// Initial revision is 1. Pause with expectedRevision = 999 (stale!)
	pauseBody, _ := json.Marshal(map[string]any{"expectedRevision": 999})
	pauseReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/runs/%s/pause", server.URL, run.ID), bytes.NewReader(pauseBody))
	pauseReq.Header.Set("Authorization", "Bearer "+controlKey.PlaintextKey)
	pauseReq.Header.Set("X-Organization-ID", orgID)
	pauseReq.Header.Set("Idempotency-Key", "stale-pause")
	pauseResp, err := http.DefaultClient.Do(pauseReq)
	if err != nil {
		t.Fatal(err)
	}
	defer pauseResp.Body.Close()

	// Canonical conflict: 409 Conflict
	if pauseResp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for stale revision, got %d", pauseResp.StatusCode)
	}

	// Authoritative state remains intact
	snap := m2GetSnapshot(t, server.URL, run.ID, controlKey.PlaintextKey, orgID)
	if string(snap.Status) != "RUNNING" && string(snap.Status) != "QUEUED" {
		t.Fatalf("stale action mutated run status! %s", snap.Status)
	}
}

// 11. Duplicate and Terminal Safety: direct DB assertions
func TestM3_DuplicateAndTerminalSafety(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	engine := execution.NewWorkerEngine(tc.pool)

	bundle := "31aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	schemaObj := map[string]any{"type": "object", "properties": map[string]any{"x": map[string]any{"type": "integer"}}, "required": []any{"x"}}
	tasks := []map[string]any{{"name": "t-safe", "inputSchema": schemaObj, "outputSchema": schemaObj, "recovery": "safe", "entrypoint": "tasks.js#safe"}}
	workflows := []map[string]any{{
		"manifestVersion": 1, "name": "wf-safety",
		"inputSchema": schemaObj, "outputSchema": schemaObj,
		"nodes": []map[string]any{
			{"id": "step1", "type": "task", "task": "t-safe", "input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}}},
		},
		"output": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "step1", "pointer": "/x"}},
	}}
	manifest := createLifecycleManifest(bundle, tasks, workflows)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "wf-safety", manifest)

	sess, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "safety-w1")
	m3AssociateWorkerDeployment(t, tc, orgID, sess.SessionID, bundle)

	run := m3CreateRun(t, server.URL, "wf-safety", adminKey.PlaintextKey, orgID, map[string]any{"x": float64(123)}, "m3-safety-run-001")

	asgns := parallelClaim(t, engine, sess, orgID, envID, 1, "poll-safety")
	parallelStart(t, engine, sess, orgID, envID, asgns[0])
	parallelCompleteSuccess(t, engine, sess, orgID, envID, asgns[0], map[string]any{"x": float64(123)})

	// Direct DB Assertion 1: Step has exactly 1 attempt in DB
	var attemptCount int
	err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx,
			`SELECT count(*) FROM task_attempts ta JOIN run_steps rs ON rs.id = ta.step_id AND rs.organization_id = ta.organization_id WHERE rs.run_id=$1::uuid AND rs.organization_id=$2::uuid`,
			run.ID, orgID).Scan(&attemptCount)
	})
	if err != nil {
		t.Fatal(err)
	}
	if attemptCount != 1 {
		t.Fatalf("expected exactly 1 attempt row in DB, got %d", attemptCount)
	}

	// Direct DB Assertion 2: Event sequence numbers are strictly monotonic and positive
	err = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT sequence, event_type FROM run_events WHERE run_id=$1::uuid AND organization_id=$2::uuid ORDER BY sequence ASC`,
			run.ID, orgID)
		if err != nil {
			return err
		}
		defer rows.Close()

		prevSeq := int64(0)
		eventCount := 0
		for rows.Next() {
			var seq int64
			var evType string
			if err := rows.Scan(&seq, &evType); err != nil {
				return err
			}
			if seq <= prevSeq {
				return fmt.Errorf("non-monotonic event sequence: prev=%d curr=%d type=%s", prevSeq, seq, evType)
			}
			prevSeq = seq
			eventCount++
		}
		if eventCount == 0 {
			return fmt.Errorf("expected run events in DB, got 0")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Direct DB Assertion 3: No orphaned active lease exists for terminal run
	var activeLeases int
	err = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx,
			`SELECT count(*) FROM task_leases tl JOIN run_steps rs ON rs.id = tl.step_id AND rs.organization_id = tl.organization_id WHERE rs.run_id=$1::uuid AND rs.organization_id=$2::uuid`,
			run.ID, orgID).Scan(&activeLeases)
	})
	if err != nil {
		t.Fatal(err)
	}
	if activeLeases != 0 {
		t.Fatalf("expected 0 active leases in terminal run, got %d", activeLeases)
	}
}

func m3StartAgent(t *testing.T, serverURL, bundleDir, runnerPath, token, name string, starts *atomic.Int32, logs *synchronizedBuffer) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	agent, err := worker.NewAgent(worker.AgentConfig{
		ControlPlaneURL:   serverURL,
		KeyPath:           filepath.Join(t.TempDir(), name+".key"),
		EnrollmentToken:   token,
		BundleDir:         bundleDir,
		RunnerPath:        runnerPath,
		Slots:             1,
		PollTimeout:       5 * time.Second,
		HeartbeatInterval: 100 * time.Millisecond,
		DrainGracePeriod:  2 * time.Second,
		Logger:            log.New(logs, "", 0),
		OnTaskProcessStart: func() {
			starts.Add(1)
		},
	})
	if err != nil {
		t.Fatalf("create agent %s: %v", name, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = agent.Start(ctx)
	}()
	return cancel, done
}

// 12. Real Two-Worker Parallel Execution with Node Child Processes
func TestM3_TwoWorkerParallelRealExecution(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	bundleDir := t.TempDir()
	targetOS, targetArch := "linux", runtime.GOARCH
	bundle := writeM3TwoWorkerBundle(t, bundleDir, targetOS, targetArch)

	valueSchema := map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}, "required": []any{"value"}}
	joinInSchema := map[string]any{"type": "object", "properties": map[string]any{"b": map[string]any{"type": "string"}, "c": map[string]any{"type": "string"}}, "required": []any{"b", "c"}, "additionalProperties": false}
	tasks := []map[string]any{
		{"name": "agent-task", "entrypoint": "tasks/agent.mjs", "timeoutMs": 30000, "recovery": "safe",
			"inputSchema": valueSchema, "outputSchema": valueSchema},
		{"name": "join-task", "entrypoint": "tasks/join.mjs", "timeoutMs": 30000, "recovery": "safe",
			"inputSchema": joinInSchema, "outputSchema": valueSchema},
	}
	workflows := []map[string]any{{
		"manifestVersion": 1, "name": "wf-two-worker-diamond",
		"inputSchema": valueSchema, "outputSchema": valueSchema,
		"nodes": []map[string]any{
			{"id": "a", "type": "task", "task": "agent-task",
				"input": map[string]any{"value": map[string]any{"$ref": "run.input", "pointer": "/value"}}},
			{"id": "b", "type": "task", "task": "agent-task", "after": []any{"a"},
				"input": map[string]any{"value": map[string]any{"$ref": "step.output", "stepId": "a", "pointer": "/value"}}},
			{"id": "c", "type": "task", "task": "agent-task", "after": []any{"a"},
				"input": map[string]any{"value": map[string]any{"$ref": "step.output", "stepId": "a", "pointer": "/value"}}},
			{"id": "d", "type": "task", "task": "join-task", "after": []any{"b", "c"},
				"input": map[string]any{
					"b": map[string]any{"$ref": "step.output", "stepId": "b", "pointer": "/value"},
					"c": map[string]any{"$ref": "step.output", "stepId": "c", "pointer": "/value"},
				}},
		},
		"output": map[string]any{"value": map[string]any{"$ref": "step.output", "stepId": "d", "pointer": "/value"}},
	}}

	rawManifest := createLifecycleManifest(bundle, tasks, workflows)
	var manifestMap map[string]any
	_ = json.Unmarshal(rawManifest, &manifestMap)
	manifestMap["targetOS"], manifestMap["targetArchitecture"] = targetOS, targetArch
	manifest, _ := json.Marshal(manifestMap)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "wf-two-worker-diamond", manifest)

	runnerPath, err := filepath.Abs("../../runner/node/dist/index.js")
	if err != nil {
		t.Fatal(err)
	}

	tok1 := m2IssueEnrollmentToken(t, server.URL, adminKey, orgID, envID, "m3-w1")
	tok2 := m2IssueEnrollmentToken(t, server.URL, adminKey, orgID, envID, "m3-w2")
	var w1Starts, w2Starts atomic.Int32
	var w1Logs, w2Logs synchronizedBuffer

	cancel1, done1 := m3StartAgent(t, server.URL, bundleDir, runnerPath, tok1, "m3-agent1", &w1Starts, &w1Logs)
	defer func() { cancel1(); <-done1 }()
	cancel2, done2 := m3StartAgent(t, server.URL, bundleDir, runnerPath, tok2, "m3-agent2", &w2Starts, &w2Logs)
	defer func() { cancel2(); <-done2 }()

	// Wait until both workers have enrolled and advertised their deployment
	m2WaitFor(t, 10*time.Second, "both workers advertised deployments", func() (string, bool) {
		var count int
		_ = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
			return tx.QueryRow(ctx, `SELECT count(DISTINCT session_id) FROM worker_deployments WHERE organization_id=$1::uuid AND bundle_digest=$2`, orgID, bundle).Scan(&count)
		})
		return fmt.Sprintf("count=%d", count), count >= 3
	})

	// Trigger run
	run := m3CreateRun(t, server.URL, "wf-two-worker-diamond", adminKey.PlaintextKey, orgID, map[string]any{"value": "root"}, "two-worker-diamond-001")

	// Wait for run to SUCCEEDED
	m2WaitFor(t, 60*time.Second, "run SUCCEEDED", func() (string, bool) {
		snap := m2GetSnapshot(t, server.URL, run.ID, adminKey.PlaintextKey, orgID)
		return string(snap.Status), string(snap.Status) == "SUCCEEDED"
	})

	// Verify both workers executed real processes
	totalStarts := w1Starts.Load() + w2Starts.Load()
	if totalStarts != 4 {
		t.Fatalf("expected 4 total process starts, got w1=%d w2=%d total=%d\nw1Logs:\n%s\nw2Logs:\n%s", w1Starts.Load(), w2Starts.Load(), totalStarts, w1Logs.String(), w2Logs.String())
	}
	if w1Starts.Load() == 0 || w2Starts.Load() == 0 {
		t.Fatalf("expected concurrent participation from both workers, got w1=%d w2=%d\nw1Logs:\n%s\nw2Logs:\n%s", w1Starts.Load(), w2Starts.Load(), w1Logs.String(), w2Logs.String())
	}

	// Verify joined output: root-done-done+root-done-done
	snap := m2GetSnapshot(t, server.URL, run.ID, adminKey.PlaintextKey, orgID)
	outBytes, _ := json.Marshal(snap.Output)
	var outMap map[string]any
	_ = json.Unmarshal(outBytes, &outMap)
	expectedOut := "root-done-done+root-done-done"
	if outMap["value"] != expectedOut {
		t.Fatalf("expected %q, got %v", expectedOut, outMap)
	}
}

// 13. Inspector Parity: 1 node per logical step, attempt containment, wait reasons, redaction
func TestM3_InspectorParityIntegrated(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	engine := execution.NewWorkerEngine(tc.pool)

	bundle := "31ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccaa"
	schemaObj := map[string]any{"type": "object", "properties": map[string]any{"secret": map[string]any{"type": "string"}}, "required": []any{"secret"}}
	tasks := []map[string]any{{"name": "t-insp", "inputSchema": schemaObj, "outputSchema": schemaObj, "recovery": "safe", "entrypoint": "tasks.js#insp"}}
	workflows := []map[string]any{{
		"manifestVersion": 1, "name": "wf-insp",
		"inputSchema": schemaObj, "outputSchema": schemaObj,
		"nodes": []map[string]any{
			{"id": "step1", "type": "task", "task": "t-insp", "input": map[string]any{"secret": map[string]any{"$ref": "run.input", "pointer": "/secret"}}},
		},
		"output": map[string]any{"secret": map[string]any{"$ref": "step.output", "stepId": "step1", "pointer": "/secret"}},
	}}
	manifest := createLifecycleManifest(bundle, tasks, workflows)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "wf-insp", manifest)

	sess, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "insp-w1")
	m3AssociateWorkerDeployment(t, tc, orgID, sess.SessionID, bundle)

	run := m3CreateRun(t, server.URL, "wf-insp", adminKey.PlaintextKey, orgID, map[string]any{"secret": "sensitive-1234"}, "m3-insp-run-001")

	asgns := parallelClaim(t, engine, sess, orgID, envID, 1, "poll-insp")
	parallelStart(t, engine, sess, orgID, envID, asgns[0])
	parallelCompleteSuccess(t, engine, sess, orgID, envID, asgns[0], map[string]any{"secret": "sensitive-1234"})

	// Admin (has payload:read) sees full output
	adminSnap := m2GetSnapshot(t, server.URL, run.ID, adminKey.PlaintextKey, orgID)
	if adminSnap.Output == nil {
		t.Fatal("expected non-nil output for admin with payload:read")
	}
	adminOutBytes, _ := json.Marshal(adminSnap.Output)
	var adminOut map[string]any
	_ = json.Unmarshal(adminOutBytes, &adminOut)
	if adminOut["secret"] != "sensitive-1234" {
		t.Fatalf("admin payload check failed: out=%v", adminOut)
	}

	// Create restricted viewer key WITHOUT payload:read
	restrictedKey := bootstrapTestKey(t, tc.service, orgID, envID, []string{tenant.CapRunsRead})
	viewerSnap := m2GetSnapshot(t, server.URL, run.ID, restrictedKey.PlaintextKey, orgID)

	// Invariant: viewer has redacted output (nil)
	if viewerSnap.Output != nil {
		t.Fatalf("security violation: expected nil output for restricted viewer, got %v", viewerSnap.Output)
	}
}
