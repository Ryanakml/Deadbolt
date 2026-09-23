package integration_test

// Issue #26 parallel-DAG acceptance suite (Blueprint §6, §10, §16).
//
// Real PostgreSQL + real WorkerEngine state transitions throughout. Engine
// Claim/Start/Complete exercise the same ownership, digest, lease, and
// fencing checks as the HTTP worker protocol; the diamond test additionally
// runs real worker.Agent + Node child processes on Linux (CI). SQL is used
// only to arrange preconditions (expiry, fixtures) or assert durable
// outcomes — never to simulate the transitions under test.

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

// writeParallelAgentBundle writes a multi-entrypoint Agent bundle (echo, slow,
// boom, join) so real-Agent workflows can prove mapped committed outputs and
// definitive branch failure through real Node child processes.
func writeParallelAgentBundle(t *testing.T, dir, targetOS, targetArch string) string {
	t.Helper()
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	files := map[string]string{
		"tasks/agent.mjs": "export default async function task(input) { return { value: input.value + '-done' }; }\n",
		"tasks/slow.mjs":  "export default async function task(input) { await new Promise((r) => setTimeout(r, 40000)); return { value: input.value + '-slow' }; }\n",
		"tasks/boom.mjs":  "export default async function task(input) { await new Promise((r) => setTimeout(r, 8000)); throw new Error('branch boom'); }\n",
		"tasks/join.mjs":  "export default async function task(input) { return { value: input.b + '+' + input.c }; }\n",
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

func startRealAgent(t *testing.T, serverURL, bundleDir, runnerPath, enrollToken string) (*worker.Agent, context.CancelFunc, *synchronizedBuffer, *atomic.Int32) {
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

func parallelIntSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"x": map[string]any{"type": "integer"}},
		"required":             []any{"x"},
		"additionalProperties": false,
	}
}

func parallelTask(name, recovery string, maxAttempts int) map[string]any {
	return map[string]any{
		"name":         name,
		"entrypoint":   "tasks/" + name + ".js",
		"timeoutMs":    30000,
		"recovery":     recovery,
		"inputSchema":  parallelIntSchema(),
		"outputSchema": parallelIntSchema(),
		"retry":        map[string]any{"maxAttempts": maxAttempts, "initialDelayMs": 100, "maxDelayMs": 1000},
	}
}

func seedParallelDeployment(t *testing.T, tc *tenantTestContext, orgID, envID, digest, workflowName string, tasks []map[string]any, nodes []map[string]any, output any) string {
	t.Helper()
	manifest := map[string]any{
		"targetOS": "linux", "targetArchitecture": "amd64", "secretNames": []string{},
		"tasks": tasks,
		"workflows": []map[string]any{{
			"manifestVersion": 1, "name": workflowName,
			"inputSchema": parallelIntSchema(), "outputSchema": parallelIntSchema(),
			"nodes": nodes, "output": output,
		}},
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var deploymentID string
	err = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `INSERT INTO deployments
			(organization_id,environment_id,manifest_hash,bundle_digest,manifest,protocol_version,runtime_version)
			VALUES ($1,$2,$3,$4,$5::jsonb,1,'1.0') RETURNING id::text`,
			orgID, envID, "manifest-"+digest, digest, string(raw)).Scan(&deploymentID)
	})
	if err != nil {
		t.Fatalf("seed deployment: %v", err)
	}
	return deploymentID
}

func seedParallelRun(t *testing.T, tc *tenantTestContext, orgID, envID, deploymentID, workflowName string, nodeIDs []string, roots map[string]bool) (string, map[string]string) {
	t.Helper()
	var runID string
	steps := map[string]string{}
	err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `INSERT INTO runs
			(organization_id,environment_id,deployment_id,workflow_name,input,status)
			VALUES ($1,$2,$3,$4,'{"x":1}'::jsonb,'QUEUED') RETURNING id::text`,
			orgID, envID, deploymentID, workflowName).Scan(&runID); err != nil {
			return err
		}
		for _, nodeID := range nodeIDs {
			state := "BLOCKED"
			var eligible any
			if roots[nodeID] {
				state = "READY"
				eligible = time.Now()
			}
			var stepID string
			if err := tx.QueryRow(ctx, `INSERT INTO run_steps
				(organization_id,environment_id,run_id,node_id,kind,state,eligible_at)
				VALUES ($1,$2,$3,$4,'task',$5,$6) RETURNING id::text`,
				orgID, envID, runID, nodeID, state, eligible).Scan(&stepID); err != nil {
				return err
			}
			steps[nodeID] = stepID
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed run: %v", err)
	}
	return runID, steps
}

func parallelSessCtx(s *testWorkerSession, orgID, envID string) *worker.WorkerSessionContext {
	return &worker.WorkerSessionContext{
		SessionID: s.SessionID, WorkerID: s.WorkerID,
		OrganizationID: orgID, EnvironmentID: envID, PoolName: "default",
	}
}

func assignmentNode(t *testing.T, steps map[string]string, a worker.AssignmentDTO) string {
	t.Helper()
	for node, stepID := range steps {
		if stepID == a.StepID {
			return node
		}
	}
	t.Fatalf("assignment step %s not in seeded steps %v", a.StepID, steps)
	return ""
}

func parallelClaim(t *testing.T, engine *execution.WorkerEngine, s *testWorkerSession, orgID, envID string, slots int, reqID string) []worker.AssignmentDTO {
	t.Helper()
	resp, err := engine.Claim(context.Background(), parallelSessCtx(s, orgID, envID), &worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: reqID,
		WorkerID: s.WorkerID, SessionID: s.SessionID,
		AvailableSlots: slots, DeploymentDigests: []string{}, Pool: "default",
	})
	if err != nil {
		t.Fatalf("claim %s: %v", reqID, err)
	}
	return resp.Assignments
}

func parallelStart(t *testing.T, engine *execution.WorkerEngine, s *testWorkerSession, orgID, envID string, a worker.AssignmentDTO) {
	t.Helper()
	resp, err := engine.Start(context.Background(), parallelSessCtx(s, orgID, envID), &worker.StartRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "start-" + a.AttemptID,
		WorkerID: s.WorkerID, SessionID: s.SessionID,
		AttemptID: a.AttemptID, OwnershipEpoch: a.OwnershipEpoch,
	})
	if err != nil || !resp.Accepted {
		t.Fatalf("start %s: err=%v resp=%+v", a.AttemptID, err, resp)
	}
}

func parallelCompleteSuccess(t *testing.T, engine *execution.WorkerEngine, s *testWorkerSession, orgID, envID string, a worker.AssignmentDTO, output map[string]any) {
	t.Helper()
	req := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "complete-" + a.AttemptID,
		WorkerID: s.WorkerID, SessionID: s.SessionID,
		AttemptID: a.AttemptID, OwnershipEpoch: a.OwnershipEpoch,
		Outcome: "SUCCEEDED", Output: output,
	}
	digest, err := worker.CanonicalCompletionDigest(&req)
	if err != nil {
		t.Fatal(err)
	}
	req.ResultDigest = digest
	resp, err := engine.Complete(context.Background(), parallelSessCtx(s, orgID, envID), &req)
	if err != nil || !resp.Accepted {
		t.Fatalf("complete success %s: err=%v resp=%+v", a.AttemptID, err, resp)
	}
}

func parallelCompleteFailure(t *testing.T, engine *execution.WorkerEngine, s *testWorkerSession, orgID, envID string, a worker.AssignmentDTO, code string, retryable bool) {
	t.Helper()
	req := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "complete-" + a.AttemptID,
		WorkerID: s.WorkerID, SessionID: s.SessionID,
		AttemptID: a.AttemptID, OwnershipEpoch: a.OwnershipEpoch,
		Outcome: "FAILED",
		Error:   &worker.TaskErrorDTO{Code: code, Message: "test failure " + code, Retryable: retryable, EffectStatus: "NOT_APPLIED"},
	}
	digest, err := worker.CanonicalCompletionDigest(&req)
	if err != nil {
		t.Fatal(err)
	}
	req.ResultDigest = digest
	resp, err := engine.Complete(context.Background(), parallelSessCtx(s, orgID, envID), &req)
	if err != nil || !resp.Accepted {
		t.Fatalf("complete failure %s: err=%v resp=%+v", a.AttemptID, err, resp)
	}
}

func queryRunStepStates(t *testing.T, tc *tenantTestContext, orgID, runID string) (string, map[string]string) {
	t.Helper()
	var runStatus string
	states := map[string]string{}
	err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT node_id, state FROM run_steps WHERE run_id=$1::uuid`, runID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var node, state string
			if err := rows.Scan(&node, &state); err != nil {
				return err
			}
			states[node] = state
		}
		return rows.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	return runStatus, states
}

func diamondNodes() []map[string]any {
	return []map[string]any{
		{"id": "a", "type": "task", "task": "task-a",
			"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}}},
		{"id": "b", "type": "task", "task": "task-b", "after": []any{"a"},
			"input": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "a", "pointer": "/x"}}},
		{"id": "c", "type": "task", "task": "task-c", "after": []any{"a"},
			"input": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "a", "pointer": "/x"}}},
		{"id": "d", "type": "task", "task": "task-d", "after": []any{"b", "c"},
			"input": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "b", "pointer": "/x"}}},
	}
}

// TestParallelDiamond_JoinWaitsForAllSuccess proves the §6 all-success join:
// D stays BLOCKED after one parent succeeds and becomes READY only after both
// succeed, with committed outputs mapped into D.
func TestParallelDiamond_JoinWaitsForAllSuccess(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	sess, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "diamond-join")
	engine := execution.NewWorkerEngine(tc.pool)
	const digest = "bundle-parallel-diamond-join"
	tasks := []map[string]any{
		parallelTask("task-a", "safe", 1), parallelTask("task-b", "safe", 1),
		parallelTask("task-c", "safe", 1), parallelTask("task-d", "safe", 1),
	}
	depID := seedParallelDeployment(t, tc, orgID, envID, digest, "diamond",
		tasks, diamondNodes(),
		map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "d", "pointer": "/x"}})
	_ = depID
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3)`,
			sess.SessionID, orgID, digest)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	runID, diamondSteps := seedParallelRun(t, tc, orgID, envID, depID, "diamond",
		[]string{"a", "b", "c", "d"}, map[string]bool{"a": true})

	// Claim + start + succeed A; B and C become READY, D stays BLOCKED.
	got := parallelClaim(t, engine, sess, orgID, envID, 2, "diamond-claim-a")
	if len(got) != 1 || assignmentNode(t, diamondSteps, got[0]) != "a" {
		t.Fatalf("expected single A assignment, got %+v", got)
	}
	parallelStart(t, engine, sess, orgID, envID, got[0])
	parallelCompleteSuccess(t, engine, sess, orgID, envID, got[0], map[string]any{"x": float64(1)})

	if _, states := queryRunStepStates(t, tc, orgID, runID); states["d"] != "BLOCKED" {
		t.Fatalf("D must stay BLOCKED before parents finish, got %v", states)
	}

	// Both B and C claimable subject to capacity (2 slots).
	bc := parallelClaim(t, engine, sess, orgID, envID, 2, "diamond-claim-bc")
	if len(bc) != 2 {
		t.Fatalf("expected B+C parallel assignments with 2 slots, got %+v", bc)
	}
	byNode := map[string]worker.AssignmentDTO{}
	for _, a := range bc {
		byNode[assignmentNode(t, diamondSteps, a)] = a
	}
	if _, ok := byNode["b"]; !ok {
		t.Fatalf("missing B assignment: %+v", bc)
	}
	if _, ok := byNode["c"]; !ok {
		t.Fatalf("missing C assignment: %+v", bc)
	}
	parallelStart(t, engine, sess, orgID, envID, byNode["b"])
	parallelCompleteSuccess(t, engine, sess, orgID, envID, byNode["b"], map[string]any{"x": float64(2)})
	if _, states := queryRunStepStates(t, tc, orgID, runID); states["d"] != "BLOCKED" {
		t.Fatalf("D must not become READY after only one parent succeeds, got %v", states)
	}
	parallelStart(t, engine, sess, orgID, envID, byNode["c"])
	parallelCompleteSuccess(t, engine, sess, orgID, envID, byNode["c"], map[string]any{"x": float64(3)})

	runStatus, states := queryRunStepStates(t, tc, orgID, runID)
	if states["d"] != "READY" {
		t.Fatalf("D must become READY after all-success join, got %v (run=%s)", states, runStatus)
	}
	// D input is constructed from committed B output (x=2).
	d := parallelClaim(t, engine, sess, orgID, envID, 2, "diamond-claim-d")
	found := false
	for _, a := range d {
		if assignmentNode(t, diamondSteps, a) == "d" {
			found = true
			raw, _ := json.Marshal(a.Input)
			var decoded map[string]any
			_ = json.Unmarshal(raw, &decoded)
			if decoded["x"] != float64(2) {
				t.Fatalf("D input must carry committed B output x=2, got %v", decoded)
			}
			parallelStart(t, engine, sess, orgID, envID, a)
			parallelCompleteSuccess(t, engine, sess, orgID, envID, a, map[string]any{"x": float64(4)})
		}
	}
	if !found {
		t.Fatalf("expected D assignment, got %+v", d)
	}
	runStatus, states = queryRunStepStates(t, tc, orgID, runID)
	if runStatus != string(contracts.RunStatusSUCCEEDED) {
		t.Fatalf("diamond must SUCCEED, run=%s states=%v", runStatus, states)
	}
}

// TestParallelFailFast_SettlesLiveSiblings proves Blueprint §10.4: a definitive
// branch failure preserves SUCCEEDED siblings exactly, CANCELLEDs every other
// nonterminal step (including live RUNNING steps), CANCELLEDs live attempts,
// revokes leases, persists stop commands, terminalizes the run, and leaves no
// nonterminal step behind.
func TestParallelFailFast_SettlesLiveSiblings(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	sess, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "failfast-settle-a")
	sessB, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "failfast-settle-b")
	engine := execution.NewWorkerEngine(tc.pool)
	const digest = "bundle-failfast-settle"
	tasks := []map[string]any{
		parallelTask("task-a", "safe", 1), parallelTask("task-b", "safe", 1),
		parallelTask("task-c", "safe", 1), parallelTask("task-e", "safe", 1),
	}
	nodes := []map[string]any{
		{"id": "a", "type": "task", "task": "task-a",
			"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}}},
		{"id": "b", "type": "task", "task": "task-b", "after": []any{"a"},
			"input": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "a", "pointer": "/x"}}},
		{"id": "c", "type": "task", "task": "task-c", "after": []any{"a"},
			"input": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "a", "pointer": "/x"}}},
		{"id": "e", "type": "task", "task": "task-e", "after": []any{"a"},
			"input": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "a", "pointer": "/x"}}, "sideEffect": true},
	}
	depID := seedParallelDeployment(t, tc, orgID, envID, digest, "fanout",
		tasks, nodes,
		map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "b", "pointer": "/x"}})
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		for _, s := range []*testWorkerSession{sess, sessB} {
			if _, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3)`,
				s.SessionID, orgID, digest); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	runID, fanoutSteps := seedParallelRun(t, tc, orgID, envID, depID, "fanout",
		[]string{"a", "b", "c", "e"}, map[string]bool{"a": true})

	a := parallelClaim(t, engine, sess, orgID, envID, 2, "ff-claim-a")
	parallelStart(t, engine, sess, orgID, envID, a[0])
	parallelCompleteSuccess(t, engine, sess, orgID, envID, a[0], map[string]any{"x": float64(1)})

	// Env capacity is 2 concurrent leases: claim B+C first, then E after B
	// succeeds and releases its lease. E stays live RUNNING at fail time.
	bc := parallelClaim(t, engine, sess, orgID, envID, 2, "ff-claim-bc")
	if len(bc) != 2 {
		t.Fatalf("expected B+C assignments, got %+v", bc)
	}
	byNode := map[string]worker.AssignmentDTO{}
	for _, x := range bc {
		byNode[assignmentNode(t, fanoutSteps, x)] = x
	}
	if _, ok := byNode["b"]; !ok {
		t.Fatalf("missing B assignment: %+v", bc)
	}
	if _, ok := byNode["c"]; !ok {
		t.Fatalf("missing C assignment: %+v", bc)
	}
	for _, x := range bc {
		parallelStart(t, engine, sess, orgID, envID, x)
	}
	// B succeeds first; its output must remain durable after fail-fast.
	parallelCompleteSuccess(t, engine, sess, orgID, envID, byNode["b"], map[string]any{"x": float64(2)})
	// Capacity freed: a second concurrent worker claims E so it is live
	// RUNNING on another session when C fails on the first.
	eGot := parallelClaim(t, engine, sessB, orgID, envID, 2, "ff-claim-e")
	if len(eGot) != 1 || assignmentNode(t, fanoutSteps, eGot[0]) != "e" {
		t.Fatalf("expected E assignment, got %+v", eGot)
	}
	byNode["e"] = eGot[0]
	parallelStart(t, engine, sessB, orgID, envID, byNode["e"])
	// C fails definitively (non-retryable) while E is still live RUNNING.
	parallelCompleteFailure(t, engine, sess, orgID, envID, byNode["c"], "PERMISSION_DENIED", false)

	runStatus, states := queryRunStepStates(t, tc, orgID, runID)
	if runStatus != "FAILED" {
		t.Fatalf("run must be FAILED, got %s states=%v", runStatus, states)
	}
	if states["b"] != "SUCCEEDED" {
		t.Fatalf("successful sibling B must stay SUCCEEDED, got %v", states)
	}
	if states["c"] != "FAILED" {
		t.Fatalf("failing step C must be FAILED, got %v", states)
	}
	if states["e"] != "CANCELLED" {
		t.Fatalf("live sibling step E must be CANCELLED (not RUNNING), got %v", states)
	}
	for node, st := range states {
		switch st {
		case "SUCCEEDED", "FAILED", "CANCELLED", "SKIPPED":
		default:
			t.Fatalf("nonterminal step remains after fail-fast: %s=%s (all=%v)", node, st, states)
		}
	}
	var liveAttempts, leases, stops int
	var bOutput []byte
	err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_attempts a JOIN run_steps rs ON rs.id=a.step_id
			WHERE rs.run_id=$1::uuid AND a.status IN ('CLAIMED','RUNNING')`, runID).Scan(&liveAttempts); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_leases l JOIN task_attempts a ON a.id=l.attempt_id
			JOIN run_steps rs ON rs.id=a.step_id WHERE rs.run_id=$1::uuid`, runID).Scan(&leases); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM stop_commands sc JOIN task_attempts a ON a.id=sc.attempt_id
			JOIN run_steps rs ON rs.id=a.step_id WHERE rs.run_id=$1::uuid AND sc.reason='SIBLING_FAILED'`, runID).Scan(&stops); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT output FROM run_steps WHERE run_id=$1::uuid AND node_id='b'`, runID).Scan(&bOutput)
	})
	if err != nil {
		t.Fatal(err)
	}
	if liveAttempts != 0 || leases != 0 {
		t.Fatalf("live work must drain: attempts=%d leases=%d", liveAttempts, leases)
	}
	if stops == 0 {
		t.Fatalf("stop command must persist for live sibling process")
	}
	var decoded map[string]any
	_ = json.Unmarshal(bOutput, &decoded)
	if decoded["x"] != float64(2) {
		t.Fatalf("B output must remain stored, got %v", decoded)
	}
	// Late completion from cancelled E must be fenced.
	late := byNode["e"]
	lateReq := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "late-e",
		WorkerID: sess.WorkerID, SessionID: sess.SessionID,
		AttemptID: late.AttemptID, OwnershipEpoch: late.OwnershipEpoch,
		Outcome: "SUCCEEDED", Output: map[string]any{"x": float64(9)},
	}
	lateReq.ResultDigest, _ = worker.CanonicalCompletionDigest(&lateReq)
	if _, err := engine.Complete(context.Background(), parallelSessCtx(sess, orgID, envID), &lateReq); err == nil {
		t.Fatalf("late result from cancelled sibling must be rejected")
	}
	if _, states := queryRunStepStates(t, tc, orgID, runID); states["e"] != "CANCELLED" {
		t.Fatalf("cancelled sibling must not become SUCCEEDED, got %v", states)
	}
}

// TestParallelStaleRecovery_DoesNotReopenFailedRun proves two sibling attempts
// expiring in one sweep cannot reopen a FAILED run: the first exhausts policy
// and fail-fasts, the stale second must not create LOST/WAITING/READY, timers,
// holds, or new attempts.
func TestParallelStaleRecovery_DoesNotReopenFailedRun(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	sess, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "stale-recovery")
	engine := execution.NewWorkerEngine(tc.pool)
	const digest = "bundle-stale-recovery"
	// maxAttempts=1: any lease-expiry recovery exhausts budget and fail-fasts.
	tasks := []map[string]any{
		parallelTask("task-a", "safe", 1), parallelTask("task-b", "safe", 1),
		parallelTask("task-c", "safe", 1),
	}
	nodes := []map[string]any{
		{"id": "a", "type": "task", "task": "task-a",
			"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}}},
		{"id": "b", "type": "task", "task": "task-b", "after": []any{"a"},
			"input": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "a", "pointer": "/x"}}},
		{"id": "c", "type": "task", "task": "task-c", "after": []any{"a"},
			"input": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "a", "pointer": "/x"}}, "sideEffect": true},
	}
	depID := seedParallelDeployment(t, tc, orgID, envID, digest, "stale",
		tasks, nodes,
		map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "b", "pointer": "/x"}})
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3)`,
			sess.SessionID, orgID, digest)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	runID, _ := seedParallelRun(t, tc, orgID, envID, depID, "stale",
		[]string{"a", "b", "c"}, map[string]bool{"a": true})
	a := parallelClaim(t, engine, sess, orgID, envID, 2, "stale-claim-a")
	parallelStart(t, engine, sess, orgID, envID, a[0])
	parallelCompleteSuccess(t, engine, sess, orgID, envID, a[0], map[string]any{"x": float64(1)})
	bc := parallelClaim(t, engine, sess, orgID, envID, 2, "stale-claim-bc")
	if len(bc) != 2 {
		t.Fatalf("expected B+C assignments, got %+v", bc)
	}
	for _, x := range bc {
		parallelStart(t, engine, sess, orgID, envID, x)
	}
	// Expire both leases in the same sweep.
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE task_leases SET expires_at=clock_timestamp()-interval '1 minute'
			WHERE organization_id=$1::uuid AND step_id IN (SELECT id FROM run_steps WHERE run_id=$2::uuid)`, orgID, runID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ReconcileExpiredLeases(context.Background(), orgID); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	runStatus, states := queryRunStepStates(t, tc, orgID, runID)
	if runStatus != "FAILED" {
		t.Fatalf("run must be FAILED after exhausted recovery, got %s %v", runStatus, states)
	}
	// Exactly one branch FAILED (first processed), the other CANCELLED — never
	// WAITING/READY, and no new work exists.
	failed, cancelled := 0, 0
	for _, node := range []string{"b", "c"} {
		switch states[node] {
		case "FAILED":
			failed++
		case "CANCELLED":
			cancelled++
		default:
			t.Fatalf("sibling %s must be FAILED or CANCELLED, got %s (all=%v)", node, states[node], states)
		}
	}
	if failed != 1 || cancelled != 1 {
		t.Fatalf("expected 1 FAILED + 1 CANCELLED, got %v", states)
	}
	var timers, waiting, ready, attempts int
	err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM timers WHERE run_id=$1::uuid AND state='PENDING'`, runID).Scan(&timers); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM run_steps WHERE run_id=$1::uuid AND state='WAITING'`, runID).Scan(&waiting); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM run_steps WHERE run_id=$1::uuid AND state='READY'`, runID).Scan(&ready); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT count(*) FROM task_attempts a JOIN run_steps rs ON rs.id=a.step_id
			WHERE rs.run_id=$1::uuid AND a.status IN ('CLAIMED','RUNNING')`, runID).Scan(&attempts)
	})
	if err != nil {
		t.Fatal(err)
	}
	if timers != 0 || waiting != 0 || ready != 0 || attempts != 0 {
		t.Fatalf("stale candidate reopened work: timers=%d waiting=%d ready=%d liveAttempts=%d states=%v",
			timers, waiting, ready, attempts, states)
	}
}

// TestReconcile_SkippedPropagation proves SKIPPED cascades (single + transitive
// multi-level) through ReconcileReadyWork regardless of manifest order, and
// that no worker assignment is generated for skipped steps.
func TestReconcile_SkippedPropagation(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	sess, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "skip-propagate")
	engine := execution.NewWorkerEngine(tc.pool)
	const digest = "bundle-skip-propagate"
	tasks := []map[string]any{
		parallelTask("task-a", "safe", 1), parallelTask("task-b", "safe", 1),
		parallelTask("task-c", "safe", 1), parallelTask("task-d", "safe", 1),
	}
	// Deliberately non-topological order: d listed before its parents.
	nodes := []map[string]any{
		{"id": "d", "type": "task", "task": "task-d", "after": []any{"b", "c"},
			"input": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "b", "pointer": "/x"}}},
		{"id": "a", "type": "task", "task": "task-a",
			"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}}},
		{"id": "b", "type": "task", "task": "task-b", "after": []any{"a"},
			"input": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "a", "pointer": "/x"}}},
		{"id": "c", "type": "task", "task": "task-c", "after": []any{"b"},
			"input": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "b", "pointer": "/x"}}, "sideEffect": true},
	}
	depID := seedParallelDeployment(t, tc, orgID, envID, digest, "skipchain",
		tasks, nodes,
		map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}})
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3)`,
			sess.SessionID, orgID, digest)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	runID, _ := seedParallelRun(t, tc, orgID, envID, depID, "skipchain",
		[]string{"d", "a", "b", "c"}, map[string]bool{"a": true})
	// Fixture: upstream conditional skip of A (durable SKIPPED seed). The engine
	// must propagate B, C, then D to SKIPPED transitively via reconciliation.
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE run_steps SET state='SKIPPED', wait_reason='CONDITIONAL_SKIP'
			WHERE run_id=$1::uuid AND node_id='a'`, runID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ReconcileReadyWork(context.Background(), orgID); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	runStatus, states := queryRunStepStates(t, tc, orgID, runID)
	for _, node := range []string{"b", "c", "d"} {
		if states[node] != "SKIPPED" {
			t.Fatalf("node %s must propagate SKIPPED, got %v", node, states)
		}
	}
	// Canonical terminal settlement: an all-SKIPPED graph with a valid final
	// output (mapped from run input) must SUCCEED, not strand in RUNNING.
	if runStatus != "SUCCEEDED" {
		t.Fatalf("skip-propagated run must terminalize SUCCEEDED, got %s states=%v", runStatus, states)
	}
	var output []byte
	err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT output FROM runs WHERE id=$1::uuid`, runID).Scan(&output)
	})
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	_ = json.Unmarshal(output, &decoded)
	if decoded["x"] != float64(1) {
		t.Fatalf("terminal output must map run input x=1, got %v", decoded)
	}
	got := parallelClaim(t, engine, sess, orgID, envID, 4, "skip-noassign")
	if len(got) != 0 {
		t.Fatalf("no worker assignment may be generated for skipped steps, got %+v", got)
	}
}

// TestReconcile_MixedSucceededSkippedTerminalizes proves final-output handling
// on the reconciliation path with mixed terminal states: B SUCCEEDED with a
// committed output, C SKIPPED, D SKIPPED via propagation, workflow output
// mapped from B. The run must SUCCEED with B's committed value.
func TestReconcile_MixedSucceededSkippedTerminalizes(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	sess, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "mixed-terminal")
	engine := execution.NewWorkerEngine(tc.pool)
	const digest = "bundle-mixed-terminal"
	tasks := []map[string]any{
		parallelTask("task-a", "safe", 1), parallelTask("task-b", "safe", 1),
		parallelTask("task-c", "safe", 1), parallelTask("task-d", "safe", 1),
	}
	nodes := []map[string]any{
		{"id": "a", "type": "task", "task": "task-a",
			"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}}},
		{"id": "b", "type": "task", "task": "task-b", "after": []any{"a"},
			"input": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "a", "pointer": "/x"}}},
		{"id": "c", "type": "task", "task": "task-c", "after": []any{"a"},
			"input": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "a", "pointer": "/x"}}, "sideEffect": true},
		{"id": "d", "type": "task", "task": "task-d", "after": []any{"b", "c"},
			"input": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "b", "pointer": "/x"}}},
	}
	depID := seedParallelDeployment(t, tc, orgID, envID, digest, "mixed",
		tasks, nodes,
		map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "b", "pointer": "/x"}})
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3)`,
			sess.SessionID, orgID, digest)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	runID, mixedSteps := seedParallelRun(t, tc, orgID, envID, depID, "mixed",
		[]string{"a", "b", "c", "d"}, map[string]bool{"a": true})
	a := parallelClaim(t, engine, sess, orgID, envID, 2, "mixed-claim-a")
	parallelStart(t, engine, sess, orgID, envID, a[0])
	parallelCompleteSuccess(t, engine, sess, orgID, envID, a[0], map[string]any{"x": float64(1)})
	// B succeeds with committed output x=2; C is skipped upstream.
	bc := parallelClaim(t, engine, sess, orgID, envID, 2, "mixed-claim-bc")
	byNode := map[string]worker.AssignmentDTO{}
	for _, x := range bc {
		byNode[assignmentNode(t, mixedSteps, x)] = x
	}
	bAssign, found := byNode["b"]
	if !found {
		t.Fatalf("missing B assignment: %+v", bc)
	}
	parallelStart(t, engine, sess, orgID, envID, bAssign)
	parallelCompleteSuccess(t, engine, sess, orgID, envID, bAssign, map[string]any{"x": float64(2)})
	// Legitimate upstream skip of C; D must propagate SKIPPED via reconcile and
	// the mixed SUCCEEDED+SKIPPED graph must terminalize with B's output.
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE run_steps SET state='SKIPPED', wait_reason='CONDITIONAL_SKIP'
			WHERE run_id=$1::uuid AND node_id='c'`, runID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ReconcileReadyWork(context.Background(), orgID); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	runStatus, states := queryRunStepStates(t, tc, orgID, runID)
	if states["d"] != "SKIPPED" {
		t.Fatalf("D must propagate SKIPPED from C, got %v", states)
	}
	if runStatus != "SUCCEEDED" {
		t.Fatalf("mixed SUCCEEDED+SKIPPED run must terminalize SUCCEEDED, got %s %v", runStatus, states)
	}
	var output []byte
	err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT output FROM runs WHERE id=$1::uuid`, runID).Scan(&output)
	})
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	_ = json.Unmarshal(output, &decoded)
	if decoded["x"] != float64(2) {
		t.Fatalf("terminal output must carry committed B output x=2, got %v", decoded)
	}
}

// TestReconcile_MappingFailureAfterRestart proves a deterministic input-mapping
// failure surfaces as INPUT_MAPPING_ERROR fail-fast through reconciliation
// (simulated restart), not a transient Poll error.
func TestReconcile_MappingFailureAfterRestart(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	sess, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "mapping-restart")
	engine := execution.NewWorkerEngine(tc.pool)
	const digest = "bundle-mapping-restart"
	tasks := []map[string]any{parallelTask("task-a", "safe", 1), parallelTask("task-b", "safe", 1)}
	// B maps a nonexistent output pointer: deterministic mapping failure once A commits.
	nodes := []map[string]any{
		{"id": "a", "type": "task", "task": "task-a",
			"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}}},
		{"id": "b", "type": "task", "task": "task-b", "after": []any{"a"},
			"input": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "a", "pointer": "/missing"}}},
	}
	depID := seedParallelDeployment(t, tc, orgID, envID, digest, "badmap",
		tasks, nodes,
		map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "b", "pointer": "/x"}})
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3)`,
			sess.SessionID, orgID, digest)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	runID, _ := seedParallelRun(t, tc, orgID, envID, depID, "badmap",
		[]string{"a", "b"}, map[string]bool{"a": true})
	a := parallelClaim(t, engine, sess, orgID, envID, 2, "map-claim-a")
	parallelStart(t, engine, sess, orgID, envID, a[0])
	parallelCompleteSuccess(t, engine, sess, orgID, envID, a[0], map[string]any{"x": float64(1)})
	// Simulate restart: wipe any READY repair the completion path may have made
	// and force the reconciler to evaluate the mapping from committed outputs.
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE run_steps SET state='BLOCKED', wait_reason=NULL WHERE run_id=$1::uuid AND node_id='b'`, runID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ReconcileReadyWork(context.Background(), orgID); err != nil {
		t.Fatalf("reconcile must not return a transient error for mapping failure: %v", err)
	}
	var runStatus, reason string
	err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, COALESCE(reason_code,'') FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus, &reason)
	})
	if err != nil {
		t.Fatal(err)
	}
	if runStatus != "FAILED" || reason != "INPUT_MAPPING_ERROR" {
		t.Fatalf("mapping failure must fail-fast deterministically, run=%s reason=%s", runStatus, reason)
	}
}

// TestReconcile_InputSchemaMismatchAfterRestart proves a mapped input that
// violates the target task schema fails the run deterministically on the
// reconciliation path.
func TestReconcile_InputSchemaMismatchAfterRestart(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	sess, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "schema-restart")
	engine := execution.NewWorkerEngine(tc.pool)
	const digest = "bundle-schema-restart"
	strictIn := map[string]any{
		"type": "object", "properties": map[string]any{"x": map[string]any{"type": "string"}},
		"required": []any{"x"}, "additionalProperties": false,
	}
	tasks := []map[string]any{
		{
			"name": "task-a", "entrypoint": "tasks/a.js", "timeoutMs": 30000, "recovery": "safe",
			"inputSchema": parallelIntSchema(), "outputSchema": parallelIntSchema(),
			"retry": map[string]any{"maxAttempts": 1, "initialDelayMs": 100, "maxDelayMs": 1000},
		},
		{
			"name": "task-b", "entrypoint": "tasks/b.js", "timeoutMs": 30000, "recovery": "safe",
			"inputSchema":  strictIn,
			"outputSchema": parallelIntSchema(),
			"retry":        map[string]any{"maxAttempts": 1, "initialDelayMs": 100, "maxDelayMs": 1000},
		},
	}
	// A commits integer x=1; B maps it into a string-only schema => violation.
	nodes := []map[string]any{
		{"id": "a", "type": "task", "task": "task-a",
			"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}}},
		{"id": "b", "type": "task", "task": "task-b", "after": []any{"a"},
			"input": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "a", "pointer": "/x"}}},
	}
	depID := seedParallelDeployment(t, tc, orgID, envID, digest, "schemabad",
		tasks, nodes,
		map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "b", "pointer": "/x"}})
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3)`,
			sess.SessionID, orgID, digest)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	runID, _ := seedParallelRun(t, tc, orgID, envID, depID, "schemabad",
		[]string{"a", "b"}, map[string]bool{"a": true})
	a := parallelClaim(t, engine, sess, orgID, envID, 2, "schema-claim-a")
	parallelStart(t, engine, sess, orgID, envID, a[0])
	parallelCompleteSuccess(t, engine, sess, orgID, envID, a[0], map[string]any{"x": float64(1)})
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE run_steps SET state='BLOCKED', wait_reason=NULL WHERE run_id=$1::uuid AND node_id='b'`, runID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ReconcileReadyWork(context.Background(), orgID); err != nil {
		t.Fatalf("reconcile must not return transient error for schema mismatch: %v", err)
	}
	var runStatus, reason string
	err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, COALESCE(reason_code,'') FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus, &reason)
	})
	if err != nil {
		t.Fatal(err)
	}
	if runStatus != "FAILED" || reason != "INPUT_MAPPING_ERROR" {
		t.Fatalf("schema mismatch must fail-fast deterministically, run=%s reason=%s", runStatus, reason)
	}
}

// TestParallelRetry_RunStatePriority proves Blueprint §10.2: one branch in
// RETRY_BACKOFF must not stall READY/RUNNING siblings; WAITING applies only
// when all remaining work is durably waiting.
func TestParallelRetry_RunStatePriority(t *testing.T) {
	setup := func(t *testing.T, suffix string) (*tenantTestContext, string, string, *testWorkerSession, *execution.WorkerEngine, string, map[string]string) {
		tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
		t.Cleanup(tc.cleanup)
		t.Cleanup(server.Close)
		sess, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "retry-prio-"+suffix)
		engine := execution.NewWorkerEngine(tc.pool)
		const digest = "bundle-retry-prio"
		tasks := []map[string]any{
			parallelTask("task-a", "safe", 3), parallelTask("task-b", "safe", 3),
		}
		nodes := []map[string]any{
			{"id": "a", "type": "task", "task": "task-a",
				"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}}},
			{"id": "b", "type": "task", "task": "task-b",
				"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}}, "sideEffect": true},
		}
		depID := seedParallelDeployment(t, tc, orgID, envID, digest+"-"+suffix, "retry",
			tasks, nodes,
			map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "a", "pointer": "/x"}})
		_ = depID
		if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
			_, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3)`,
				sess.SessionID, orgID, digest+"-"+suffix)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		runID, steps := seedParallelRun(t, tc, orgID, envID, depID, "retry",
			[]string{"a", "b"}, map[string]bool{"a": true, "b": true})
		return tc, orgID, envID, sess, engine, runID, steps
	}

	t.Run("CaseA/readySiblingStaysClaimable", func(t *testing.T) {
		tc, orgID, envID, sess, engine, runID, steps := setup(t, "a")
		// Claim exactly one branch; the other stays READY (unclaimed).
		one := parallelClaim(t, engine, sess, orgID, envID, 1, "prio-a-claim")
		if len(one) != 1 {
			t.Fatalf("expected 1 assignment, got %+v", one)
		}
		failedNode := assignmentNode(t, steps, one[0])
		otherNode := "b"
		if failedNode == "b" {
			otherNode = "a"
		}
		parallelStart(t, engine, sess, orgID, envID, one[0])
		// The claimed branch fails retryably (safe + retryable + budget) while
		// its sibling is still READY and unclaimed.
		failedReq := worker.CompleteRequestDTO{
			ProtocolVersion: worker.ProtocolVersion, RequestID: "complete-" + one[0].AttemptID,
			WorkerID: sess.WorkerID, SessionID: sess.SessionID,
			AttemptID: one[0].AttemptID, OwnershipEpoch: one[0].OwnershipEpoch,
			Outcome: "FAILED",
			Error:   &worker.TaskErrorDTO{Code: "TASK_FAILED", Message: "retryable", Retryable: true, EffectStatus: "NOT_APPLIED"},
		}
		failedReq.ResultDigest, _ = worker.CanonicalCompletionDigest(&failedReq)
		resp, err := engine.Complete(context.Background(), parallelSessCtx(sess, orgID, envID), &failedReq)
		if err != nil || !resp.Accepted {
			t.Fatalf("complete failure: err=%v resp=%+v", err, resp)
		}
		runStatus, states := queryRunStepStates(t, tc, orgID, runID)
		if runStatus != "RUNNING" {
			t.Fatalf("run must stay RUNNING with READY sibling, got %s %v", runStatus, states)
		}
		if states[otherNode] != "READY" {
			t.Fatalf("sibling %s must remain READY claimable, got %v", otherNode, states)
		}
		again := parallelClaim(t, engine, sess, orgID, envID, 2, "prio-a-reclaim")
		found := false
		for _, x := range again {
			if assignmentNode(t, steps, x) == otherNode {
				found = true
			}
		}
		if !found {
			t.Fatalf("READY sibling %s must remain claimable after backoff, got %+v", otherNode, again)
		}
	})

	t.Run("CaseB/runningSiblingFinishes", func(t *testing.T) {
		tc, orgID, envID, sess, engine, runID, steps := setup(t, "b")
		ab := parallelClaim(t, engine, sess, orgID, envID, 2, "prio-b-claim")
		byNode := map[string]worker.AssignmentDTO{}
		for _, x := range ab {
			byNode[assignmentNode(t, steps, x)] = x
		}
		parallelStart(t, engine, sess, orgID, envID, byNode["a"])
		parallelStart(t, engine, sess, orgID, envID, byNode["b"])
		parallelCompleteFailure(t, engine, sess, orgID, envID, byNode["a"], "TASK_FAILED", true)
		runStatus, states := queryRunStepStates(t, tc, orgID, runID)
		if runStatus != "RUNNING" {
			t.Fatalf("run must stay RUNNING with RUNNING sibling, got %s %v", runStatus, states)
		}
		parallelCompleteSuccess(t, engine, sess, orgID, envID, byNode["b"], map[string]any{"x": float64(2)})
		if _, states := queryRunStepStates(t, tc, orgID, runID); states["b"] != "SUCCEEDED" {
			t.Fatalf("B must finish normally, got %v", states)
		}
	})

	t.Run("CaseC/onlyWaitsBecomeWaiting", func(t *testing.T) {
		tc, orgID, envID, sess, engine, runID, steps := setup(t, "c")
		ab := parallelClaim(t, engine, sess, orgID, envID, 2, "prio-c-claim")
		byNode := map[string]worker.AssignmentDTO{}
		for _, x := range ab {
			byNode[assignmentNode(t, steps, x)] = x
		}
		parallelStart(t, engine, sess, orgID, envID, byNode["a"])
		parallelStart(t, engine, sess, orgID, envID, byNode["b"])
		// B succeeds; only A will be in backoff afterwards.
		parallelCompleteSuccess(t, engine, sess, orgID, envID, byNode["b"], map[string]any{"x": float64(2)})
		parallelCompleteFailure(t, engine, sess, orgID, envID, byNode["a"], "TASK_FAILED", true)
		runStatus, states := queryRunStepStates(t, tc, orgID, runID)
		if runStatus != "WAITING" {
			t.Fatalf("run must become WAITING when only durable waits remain, got %s %v", runStatus, states)
		}
	})
}

// TestParallelDiamond_RealAgent proves parallel execution through real
// worker.Agent + Node child processes with two slots: eligible siblings lease
// concurrently, the all-success join waits for both parents, and every Node
// child receives control-plane-mapped committed ancestor outputs.
func TestParallelDiamond_RealAgent(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("real Agent bundle fixture requires the Linux worker runtime used by CI")
	}
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()
	bundleDir := t.TempDir()
	targetOS, targetArch := "linux", runtime.GOARCH
	bundle := writeParallelAgentBundle(t, bundleDir, targetOS, targetArch)
	valueSchema := map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}, "required": []any{"value"}}
	joinInSchema := map[string]any{"type": "object", "properties": map[string]any{"b": map[string]any{"type": "string"}, "c": map[string]any{"type": "string"}}, "required": []any{"b", "c"}, "additionalProperties": false}
	tasks := []map[string]any{
		{"name": "agent-task", "entrypoint": "tasks/agent.mjs", "timeoutMs": 30000, "recovery": "safe",
			"inputSchema": valueSchema, "outputSchema": valueSchema},
		{"name": "join-task", "entrypoint": "tasks/join.mjs", "timeoutMs": 30000, "recovery": "safe",
			"inputSchema": joinInSchema, "outputSchema": valueSchema},
	}
	workflows := []map[string]any{{
		"manifestVersion": 1, "name": "agent-diamond",
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
	manifest := createLifecycleManifest(bundle, tasks, workflows)
	var manifestMap map[string]any
	_ = json.Unmarshal(manifest, &manifestMap)
	manifestMap["targetOS"], manifestMap["targetArchitecture"] = targetOS, targetArch
	manifest, _ = json.Marshal(manifestMap)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "agent-diamond", manifest)

	enrollBody, _ := json.Marshal(map[string]any{"poolName": "default"})
	enrollReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/environments/%s/worker-enrollments", server.URL, envID), bytes.NewReader(enrollBody))
	enrollReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	enrollReq.Header.Set("X-Organization-ID", orgID)
	enrollReq.Header.Set("Idempotency-Key", "agent-diamond-enrollment")
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
	runnerPath, err := filepath.Abs("../../runner/node/dist/index.js")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var agentLogs synchronizedBuffer
	var childLaunches atomic.Int32
	agent, err := worker.NewAgent(worker.AgentConfig{ControlPlaneURL: server.URL, KeyPath: filepath.Join(t.TempDir(), "worker.key"), EnrollmentToken: enrollment.Token, BundleDir: bundleDir, RunnerPath: runnerPath, PollTimeout: 100 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond, Logger: log.New(&agentLogs, "", 0), OnTaskProcessStart: func() { childLaunches.Add(1) }})
	if err != nil {
		t.Fatal(err)
	}
	agentDone := make(chan struct{})
	go func() { defer close(agentDone); _ = agent.Start(ctx) }()
	defer func() { cancel(); <-agentDone }()

	createBody, _ := json.Marshal(map[string]any{"environment": "staging", "input": map[string]any{"value": "diamond"}})
	createReq, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/agent-diamond/runs", bytes.NewReader(createBody))
	createReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	createReq.Header.Set("X-Organization-ID", orgID)
	createReq.Header.Set("Idempotency-Key", "agent-diamond-run")
	createReq.Header.Set("Content-Type", "application/json")
	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatal(err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusAccepted {
		t.Fatalf("create status=%d", createResp.StatusCode)
	}
	var run execution.RunDTO
	_ = json.NewDecoder(createResp.Body).Decode(&run)

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := execution.NewService(tc.pool, tc.service).GetRun(context.Background(), orgID, run.ID)
		if err == nil && snapshot.Status == contracts.RunStatusSUCCEEDED {
			output, _ := snapshot.Output.(map[string]any)
			// Chain proof: run "diamond" -> A "diamond-done" -> B/C
			// "diamond-done-done" -> D "diamond-done-done+diamond-done-done".
			// Any unmapped hop would produce fewer segments.
			if output["value"] != "diamond-done-done+diamond-done-done" {
				t.Fatalf("D must receive mapped committed B+C outputs, got %+v", snapshot.Output)
			}
			if childLaunches.Load() != 4 {
				t.Fatalf("expected 4 Node child executions (a,b,c,d), got %d logs=%s", childLaunches.Load(), agentLogs.String())
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	snapshot, _ := execution.NewService(tc.pool, tc.service).GetRun(context.Background(), orgID, run.ID)
	t.Fatalf("real-agent diamond did not succeed: %+v launches=%d logs=%s", snapshot, childLaunches.Load(), agentLogs.String())
}

// TestParallelFailFast_RealAgent proves the definitive branch-failure path
// through real worker.Agent + Node child processes: root and one sibling
// commit output, another sibling throws definitively while a slow sibling's
// Node child is still live, the run fail-fasts, the live step/attempt is
// CANCELLED, its lease is revoked, the stop command reaches the Agent (child
// killed, stop acked), the committed output stays durable, and the late
// ABORTED completion cannot reopen the run.
func TestParallelFailFast_RealAgent(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("real Agent bundle fixture requires the Linux worker runtime used by CI")
	}
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()
	bundleDir := t.TempDir()
	targetOS, targetArch := "linux", runtime.GOARCH
	bundle := writeParallelAgentBundle(t, bundleDir, targetOS, targetArch)
	valueSchema := map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}, "required": []any{"value"}}
	tasks := []map[string]any{
		{"name": "agent-task", "entrypoint": "tasks/agent.mjs", "timeoutMs": 30000, "recovery": "safe",
			"inputSchema": valueSchema, "outputSchema": valueSchema},
		{"name": "boom-task", "entrypoint": "tasks/boom.mjs", "timeoutMs": 30000, "recovery": "safe",
			"inputSchema": valueSchema, "outputSchema": valueSchema},
		{"name": "slow-task", "entrypoint": "tasks/slow.mjs", "timeoutMs": 120000, "recovery": "safe",
			"inputSchema": valueSchema, "outputSchema": valueSchema},
	}
	// a -> b -> {c(boom), e(slow)}: b must commit before c can fail, so the
	// failure always lands while e's Node child is live.
	workflows := []map[string]any{{
		"manifestVersion": 1, "name": "agent-failfast",
		"inputSchema": valueSchema, "outputSchema": valueSchema,
		"nodes": []map[string]any{
			{"id": "a", "type": "task", "task": "agent-task",
				"input": map[string]any{"value": map[string]any{"$ref": "run.input", "pointer": "/value"}}},
			{"id": "b", "type": "task", "task": "agent-task", "after": []any{"a"},
				"input": map[string]any{"value": map[string]any{"$ref": "step.output", "stepId": "a", "pointer": "/value"}}},
			{"id": "c", "type": "task", "task": "boom-task", "after": []any{"b"},
				"input": map[string]any{"value": map[string]any{"$ref": "step.output", "stepId": "b", "pointer": "/value"}}, "sideEffect": true},
			{"id": "e", "type": "task", "task": "slow-task", "after": []any{"b"},
				"input": map[string]any{"value": map[string]any{"$ref": "step.output", "stepId": "b", "pointer": "/value"}}, "sideEffect": true},
		},
		"output": map[string]any{"value": map[string]any{"$ref": "step.output", "stepId": "b", "pointer": "/value"}},
	}}
	manifest := createLifecycleManifest(bundle, tasks, workflows)
	var manifestMap map[string]any
	_ = json.Unmarshal(manifest, &manifestMap)
	manifestMap["targetOS"], manifestMap["targetArchitecture"] = targetOS, targetArch
	manifest, _ = json.Marshal(manifestMap)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "agent-failfast", manifest)

	enrollBody, _ := json.Marshal(map[string]any{"poolName": "default"})
	enrollReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/environments/%s/worker-enrollments", server.URL, envID), bytes.NewReader(enrollBody))
	enrollReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	enrollReq.Header.Set("X-Organization-ID", orgID)
	enrollReq.Header.Set("Idempotency-Key", "agent-failfast-enrollment")
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
	runnerPath, err := filepath.Abs("../../runner/node/dist/index.js")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var agentLogs synchronizedBuffer
	var childLaunches atomic.Int32
	agent, err := worker.NewAgent(worker.AgentConfig{ControlPlaneURL: server.URL, KeyPath: filepath.Join(t.TempDir(), "worker.key"), EnrollmentToken: enrollment.Token, BundleDir: bundleDir, RunnerPath: runnerPath, PollTimeout: 100 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond, Logger: log.New(&agentLogs, "", 0), OnTaskProcessStart: func() { childLaunches.Add(1) }})
	if err != nil {
		t.Fatal(err)
	}
	agentDone := make(chan struct{})
	go func() { defer close(agentDone); _ = agent.Start(ctx) }()
	defer func() { cancel(); <-agentDone }()

	createBody, _ := json.Marshal(map[string]any{"environment": "staging", "input": map[string]any{"value": "ff"}})
	createReq, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/agent-failfast/runs", bytes.NewReader(createBody))
	createReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	createReq.Header.Set("X-Organization-ID", orgID)
	createReq.Header.Set("Idempotency-Key", "agent-failfast-run")
	createReq.Header.Set("Content-Type", "application/json")
	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatal(err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusAccepted {
		t.Fatalf("create status=%d", createResp.StatusCode)
	}
	var run execution.RunDTO
	_ = json.NewDecoder(createResp.Body).Decode(&run)

	// Phase 1: fail-fast settlement through the real worker HTTP protocol.
	deadline := time.Now().Add(90 * time.Second)
	for {
		snapshot, err := execution.NewService(tc.pool, tc.service).GetRun(context.Background(), orgID, run.ID)
		if err == nil && snapshot.Status == contracts.RunStatusFAILED {
			break
		}
		if time.Now().After(deadline) {
			snapshot, _ := execution.NewService(tc.pool, tc.service).GetRun(context.Background(), orgID, run.ID)
			t.Fatalf("real-agent fail-fast did not terminalize: %+v launches=%d logs=%s", snapshot, childLaunches.Load(), agentLogs.String())
		}
		time.Sleep(200 * time.Millisecond)
	}
	var stepC, stepE, runReason string
	var liveAttempts, leases, stops, stopAcked, eAttempts int
	var nonterminal int
	var bOutput []byte
	err = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT COALESCE(reason_code,'') FROM runs WHERE id=$1::uuid`, run.ID).Scan(&runReason); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT state FROM run_steps WHERE run_id=$1::uuid AND node_id='c'`, run.ID).Scan(&stepC); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT state FROM run_steps WHERE run_id=$1::uuid AND node_id='e'`, run.ID).Scan(&stepE); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM run_steps WHERE run_id=$1::uuid AND state IN ('BLOCKED','READY','RUNNING','WAITING')`, run.ID).Scan(&nonterminal); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_attempts a JOIN run_steps rs ON rs.id=a.step_id
			WHERE rs.run_id=$1::uuid AND a.status IN ('CLAIMED','RUNNING')`, run.ID).Scan(&liveAttempts); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_leases l JOIN task_attempts a ON a.id=l.attempt_id
			JOIN run_steps rs ON rs.id=a.step_id WHERE rs.run_id=$1::uuid`, run.ID).Scan(&leases); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM stop_commands sc JOIN task_attempts a ON a.id=sc.attempt_id
			JOIN run_steps rs ON rs.id=a.step_id WHERE rs.run_id=$1::uuid AND rs.node_id='e' AND sc.reason='SIBLING_FAILED'`, run.ID).Scan(&stops); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM stop_commands sc JOIN task_attempts a ON a.id=sc.attempt_id
			JOIN run_steps rs ON rs.id=a.step_id WHERE rs.run_id=$1::uuid AND rs.node_id='e' AND sc.acked_at IS NOT NULL`, run.ID).Scan(&stopAcked); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_attempts a JOIN run_steps rs ON rs.id=a.step_id
			WHERE rs.run_id=$1::uuid AND rs.node_id='e'`, run.ID).Scan(&eAttempts); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT output FROM run_steps WHERE run_id=$1::uuid AND node_id='b'`, run.ID).Scan(&bOutput)
	})
	if err != nil {
		t.Fatal(err)
	}
	if stepC != "FAILED" {
		t.Fatalf("boom branch must be FAILED, got %s", stepC)
	}
	if stepE != "CANCELLED" {
		t.Fatalf("live slow sibling must be CANCELLED, got %s", stepE)
	}
	if nonterminal != 0 || liveAttempts != 0 || leases != 0 {
		t.Fatalf("settlement leaked live work: nonterminal=%d live=%d leases=%d", nonterminal, liveAttempts, leases)
	}
	if stops == 0 {
		t.Fatalf("stop command must persist for the live sibling attempt")
	}
	// Stop delivery is asynchronous via Agent heartbeat: the SIBLING_FAILED
	// row commits atomically with fail-fast, but the Agent acks on its next
	// heartbeat after its lease join misses (synthetic LEASE_NOT_FOUND stop).
	ackDeadline := time.Now().Add(25 * time.Second)
	for stopAcked == 0 && time.Now().Before(ackDeadline) {
		time.Sleep(200 * time.Millisecond)
		_ = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
			return tx.QueryRow(ctx, `SELECT count(*) FROM stop_commands sc JOIN task_attempts a ON a.id=sc.attempt_id
				JOIN run_steps rs ON rs.id=a.step_id WHERE rs.run_id=$1::uuid AND rs.node_id='e' AND sc.acked_at IS NOT NULL`, run.ID).Scan(&stopAcked)
		})
	}
	if stopAcked == 0 {
		t.Fatalf("stop command must reach the Agent (acked), logs=%s", agentLogs.String())
	}
	var decoded map[string]any
	_ = json.Unmarshal(bOutput, &decoded)
	if decoded["value"] != "ff-done-done" {
		t.Fatalf("committed B output must remain durable, got %v", decoded)
	}
	if childLaunches.Load() != 4 {
		t.Fatalf("expected 4 Node children (a,b,c,e), got %d logs=%s", childLaunches.Load(), agentLogs.String())
	}
	_ = runReason

	// Phase 2: late-result fencing — the killed slow child (or its ABORTED
	// delivery) must not reopen anything.
	time.Sleep(5 * time.Second)
	err = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT state FROM run_steps WHERE run_id=$1::uuid AND node_id='e'`, run.ID).Scan(&stepE); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_attempts a JOIN run_steps rs ON rs.id=a.step_id
			WHERE rs.run_id=$1::uuid AND rs.node_id='e'`, run.ID).Scan(&eAttempts); err != nil {
			return err
		}
		var ready int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM run_steps WHERE run_id=$1::uuid AND state='READY'`, run.ID).Scan(&ready); err != nil {
			return err
		}
		if ready != 0 {
			t.Fatalf("no downstream step may become READY after fail-fast")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if stepE != "CANCELLED" {
		t.Fatalf("cancelled sibling must stay CANCELLED after late path, got %s", stepE)
	}
	if eAttempts != 1 {
		t.Fatalf("no new attempt may be created after fail-fast, e attempts=%d", eAttempts)
	}
	snapshot, _ := execution.NewService(tc.pool, tc.service).GetRun(context.Background(), orgID, run.ID)
	if snapshot.Status != contracts.RunStatusFAILED {
		t.Fatalf("run must stay FAILED, got %s", snapshot.Status)
	}
}
