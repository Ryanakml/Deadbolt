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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
	"github.com/Ryanakml/Deadbolt/tests/fixtures/externaleffect"
	"github.com/Ryanakml/Deadbolt/tests/fixtures/httpstaging"
)

// m2RealWorkflow is the A→B→C workflow executed by two actual worker Agents
// through real Node child processes in the central M2 gate test.
const m2RealWorkflow = "m2-gate-real-pipeline"

// Node handler sources for the real runnable A→B→C bundle. Task input
// carries markerDir (mapped from run input) so every child execution leaves
// observable runtime evidence outside the database. B blocks on a release
// file so the test can observe B in-flight on Agent 1 before killing it.
const m2RealTaskA = `import { appendFileSync } from "node:fs";
export default async function task(input, ctx) {
  appendFileSync(input.markerDir + "/calls.log", "A " + ctx.operationId + "\n");
  return { accountId: "acc-m2-real" };
}
`

const m2RealTaskB = `import { appendFileSync, existsSync } from "node:fs";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
export default async function task(input, ctx) {
  appendFileSync(input.markerDir + "/calls.log", "B-start " + ctx.operationId + "\n");
  // Abort (SIGTERM from the Agent kill path) strictly wins over release: it
  // is checked before the release file, so a killed child can never write
  // B-done afterwards. The release file is attempt-scoped, so only the
  // recovered attempt's child can ever complete.
  let aborted = false;
  if (ctx.signal) {
    ctx.signal.addEventListener("abort", () => { aborted = true; });
  }
  const release = input.markerDir + "/b.release." + ctx.attemptId;
  const deadline = Date.now() + 55000;
  for (;;) {
    if (aborted) {
      appendFileSync(input.markerDir + "/calls.log", "B-aborted " + ctx.attemptId + "\n");
      throw new Error("TASK_ABORTED");
    }
    if (existsSync(release)) break;
    if (Date.now() > deadline) throw new Error("B_RELEASE_TIMEOUT");
    await sleep(100);
  }
  appendFileSync(input.markerDir + "/calls.log", "B-done " + ctx.operationId + "\n");
  return { message: "hello-real" };
}
`

const m2RealTaskC = `import { appendFileSync } from "node:fs";
export default async function task(input, ctx) {
  appendFileSync(input.markerDir + "/calls.log", "C " + ctx.operationId + "\n");
  return { confirmationCode: "CONF-M2-REAL" };
}
`

// m2RealABCManifest declares a linear A→B→C workflow where every task uses
// recovery "safe" so lease loss retries with the same operation ID under the
// persisted backoff timer (Blueprint §13.3, F-05). markerDir flows from run
// input into every task so handlers can record runtime execution evidence.
func m2RealABCManifest() (tasks []map[string]any, workflows []map[string]any) {
	mkSchema := func(props map[string]any, required []any) map[string]any {
		return map[string]any{
			"type":                 "object",
			"properties":           props,
			"required":             required,
			"additionalProperties": false,
		}
	}
	strProp := map[string]any{"type": "string"}
	mkTask := func(name, entry string, inProps map[string]any, inReq []any, outProps map[string]any, outReq []any) map[string]any {
		return map[string]any{
			"name":                name,
			"entrypoint":          entry,
			"timeoutMs":           60000,
			"recovery":            "safe",
			"inputSchema":         mkSchema(inProps, inReq),
			"outputSchema":        mkSchema(outProps, outReq),
			"retry":               map[string]any{"maxAttempts": 3, "initialDelayMs": 1000, "maxDelayMs": 30000},
			"idempotencyWindowMs": 305000,
		}
	}
	tasks = []map[string]any{
		mkTask("task-a", "tasks/a.mjs",
			map[string]any{"email": strProp, "markerDir": strProp}, []any{"email", "markerDir"},
			map[string]any{"accountId": strProp}, []any{"accountId"}),
		mkTask("task-b", "tasks/b.mjs",
			map[string]any{"accountId": strProp, "markerDir": strProp}, []any{"accountId", "markerDir"},
			map[string]any{"message": strProp}, []any{"message"}),
		mkTask("task-c", "tasks/c.mjs",
			map[string]any{"welcomeMessage": strProp, "markerDir": strProp}, []any{"welcomeMessage", "markerDir"},
			map[string]any{"confirmationCode": strProp}, []any{"confirmationCode"}),
	}
	markerFromRun := map[string]any{"$ref": "run.input", "pointer": "/markerDir"}
	workflows = []map[string]any{
		{
			"manifestVersion": 1,
			"name":            m2RealWorkflow,
			"inputSchema": mkSchema(map[string]any{"customerEmail": strProp, "markerDir": strProp},
				[]any{"customerEmail", "markerDir"}),
			"outputSchema": map[string]any{
				"type":       "object",
				"properties": map[string]any{"finalConfirmation": map[string]any{"type": "string"}},
				"required":   []any{"finalConfirmation"},
			},
			"nodes": []map[string]any{
				{
					"id": "node-a", "type": "task", "task": "task-a", "after": []any{},
					"input": map[string]any{
						"email":     map[string]any{"$ref": "run.input", "pointer": "/customerEmail"},
						"markerDir": markerFromRun,
					},
				},
				{
					"id": "node-b", "type": "task", "task": "task-b", "after": []any{"node-a"},
					"input": map[string]any{
						"accountId": map[string]any{"$ref": "step.output", "stepId": "node-a", "pointer": "/accountId"},
						"markerDir": markerFromRun,
					},
				},
				{
					"id": "node-c", "type": "task", "task": "task-c", "after": []any{"node-b"},
					"input": map[string]any{
						"welcomeMessage": map[string]any{"$ref": "step.output", "stepId": "node-b", "pointer": "/message"},
						"markerDir":      markerFromRun,
					},
				},
			},
			"output": map[string]any{
				"finalConfirmation": map[string]any{"$ref": "step.output", "stepId": "node-c", "pointer": "/confirmationCode"},
			},
		},
	}
	return tasks, workflows
}

// writeM2RealBundle writes a real runnable tar bundle with the three M2 gate
// task handlers plus the immutable platform identity file, mirroring
// production `runtime build` output. It returns the SHA-256 bundle digest.
func writeM2RealBundle(t *testing.T, dir, targetOS, targetArch string) string {
	t.Helper()
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	for name, code := range map[string]string{
		"tasks/a.mjs": m2RealTaskA,
		"tasks/b.mjs": m2RealTaskB,
		"tasks/c.mjs": m2RealTaskC,
	} {
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

// m2IssueEnrollmentToken issues a single-use worker enrollment token through
// the real public API. The actual Agent consumes it during ensureIdentity.
func m2IssueEnrollmentToken(t *testing.T, serverURL string, adminKey *tenant.GeneratedKey, orgID, envID, suffix string) string {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"poolName": "default"})
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/environments/%s/worker-enrollments", serverURL, envID), bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	req.Header.Set("X-Organization-ID", orgID)
	req.Header.Set("Idempotency-Key", "m2-real-enroll-"+suffix)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("issue enrollment token: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("issue enrollment token status=%d body=%s", resp.StatusCode, string(raw))
	}
	var info worker.EnrollmentTokenInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		t.Fatalf("decode enrollment token: %v", err)
	}
	if info.Token == "" {
		t.Fatal("empty enrollment token")
	}
	return info.Token
}

// m2StartAgent boots an actual worker.Agent (authenticated enrollment,
// polling, Start/heartbeat/Complete, real Node child processes) on the
// calling test's goroutine pool. Cancellation of the returned context is the
// test's worker-process-equivalent kill boundary: the Agent loop stops,
// heartbeats stop because the Agent stopped, and its active Node child is
// terminated according to current Agent behavior.
func m2StartAgent(t *testing.T, serverURL, bundleDir, runnerPath, token, name string, starts *atomic.Int32, logs *synchronizedBuffer) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	agent, err := worker.NewAgent(worker.AgentConfig{
		ControlPlaneURL:   serverURL,
		KeyPath:           filepath.Join(t.TempDir(), name+".key"),
		EnrollmentToken:   token,
		BundleDir:         bundleDir,
		RunnerPath:        runnerPath,
		Slots:             1,
		PollTimeout:       200 * time.Millisecond,
		HeartbeatInterval: 300 * time.Millisecond,
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

// m2WaitFor polls cond until it reports ready or the timeout elapses.
func m2WaitFor(t *testing.T, timeout time.Duration, what string, cond func() (string, bool)) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		state, ok := cond()
		if ok {
			return state
		}
		last = state
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s: last=%s", what, last)
	return ""
}

// m2NodeAttempts returns this run's attempts for one node ordered by attempt
// number as "id=status=epoch=session" strings.
func m2NodeAttempts(t *testing.T, tc *tenantTestContext, orgID, runID, nodeID string) []string {
	t.Helper()
	var out []string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		rows, err := tx.Query(ctx, `SELECT a.id::text, a.status, a.epoch, COALESCE(a.session_id::text,'') FROM task_attempts a JOIN run_steps rs ON rs.id=a.step_id JOIN runs r ON r.id=rs.run_id WHERE r.id=$1::uuid AND r.organization_id=$2::uuid AND rs.node_id=$3 ORDER BY a.attempt_number`, runID, orgID, nodeID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id, status, session string
			var epoch int64
			if err := rows.Scan(&id, &status, &epoch, &session); err != nil {
				return err
			}
			out = append(out, fmt.Sprintf("%s=%s=epoch%d=session%s", id, status, epoch, session))
		}
		return rows.Err()
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

// m2ReadCalls returns the marker lines written by real Node child executions.
func m2ReadCalls(markerDir string) []string {
	raw, err := os.ReadFile(filepath.Join(markerDir, "calls.log"))
	if err != nil {
		return nil
	}
	var lines []string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func m2CountPrefix(lines []string, prefix string) int {
	count := 0
	for _, line := range lines {
		if strings.HasPrefix(line, prefix) {
			count++
		}
	}
	return count
}

func m2GetSnapshot(t *testing.T, serverURL, runID, token, orgID string) execution.RunSnapshotDTO {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/runs/%s", serverURL, runID), nil)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Organization-ID", orgID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get run snapshot: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("get run snapshot status=%d body=%s", resp.StatusCode, string(body))
	}
	var snap execution.RunSnapshotDTO
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatalf("decode run snapshot: %v", err)
	}
	return snap
}

// TestM2_TwoWorkerABCRecoveryKillDuringB is the central Blueprint §29.2
// scenario for Issue #25, executed through the real Deadbolt path with two
// ACTUAL worker.Agent processes and real Node child execution:
//
//   - Agent 1 is the only worker at first: it runs A (real child) to
//     SUCCEEDED, then claims and starts B.
//   - B blocks on an attempt-scoped release file, so the test observes B
//     observably RUNNING/in-flight on Agent 1 (DB attempt RUNNING + B-start
//     marker written by the real child + Agent 1 child count) before killing
//     it.
//   - Agent 1 is killed via context cancellation: its Agent loop actually
//     stops, heartbeats stop because the Agent stopped, and its active Node
//     child is terminated per current Agent behavior (SIGTERM abort observed
//     via a B-aborted marker, then SIGKILL after the supervisor grace
//     period). Abort strictly wins over release in the handler and the
//     release file is attempt-scoped, so Agent 1 provably never writes
//     B-done.
//   - Ownership expires (controlled clock advancement only; final step/attempt
//     state is never written directly) and the production recovery path
//     (ReconcileExpiredLeases + FireDueRetryTimers) marks B LOST, parks the
//     safe-retry backoff timer, and re-queues B.
//   - Agent 2 starts, claims B with a newer epoch and the same operation ID,
//     completes B, then completes C. The run ends SUCCEEDED.
//   - A is proven executed exactly once by BOTH runtime evidence (calls.log:
//     A x1, B-start x2 under one operation ID, B-done x1, C x1; per-agent
//     child counts 2 and 2) and durable evidence (A attempts 1/1, B LOST +
//     SUCCEEDED, C attempts 1/1). API, DB, event history, and
//     Inspector-visible state agree.
//
// Stale Start/heartbeat/Complete fencing mutations are proven by the dedicated
// stale-worker gate groups (Issue #17); the dead Agent is not resurrected to
// replay them here.
//
// Host-resilience scope: two worker processes on one host prove
// worker-process failure only. Host-failure resilience is NOT claimed.
func TestM2_TwoWorkerABCRecoveryKillDuringB(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()
	ctx := context.Background()

	// Real runnable bundle + manifest pinned to the host architecture so the
	// Agent's bundle identity and host-compatibility checks pass on both
	// darwin/arm64 developer machines and linux CI.
	targetOS, targetArch := "linux", runtime.GOARCH
	bundleDir := t.TempDir()
	bundle := writeM2RealBundle(t, bundleDir, targetOS, targetArch)
	tasks, workflows := m2RealABCManifest()
	rawManifest := createLifecycleManifest(bundle, tasks, workflows)
	var manifestMap map[string]any
	if err := json.Unmarshal(rawManifest, &manifestMap); err != nil {
		t.Fatal(err)
	}
	manifestMap["targetOS"], manifestMap["targetArchitecture"] = targetOS, targetArch
	manifest, _ := json.Marshal(manifestMap)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, m2RealWorkflow, manifest)

	markerDir := t.TempDir()
	runnerPath, err := filepath.Abs("../../runner/node/dist/index.js")
	if err != nil {
		t.Fatal(err)
	}

	// Two actual worker Agents (authenticated enrollment, real Node child
	// processes). Both run on this host: worker-process failure is proven,
	// host-failure resilience is NOT claimed.
	tok1 := m2IssueEnrollmentToken(t, server.URL, adminKey, orgID, envID, "real1")
	tok2 := m2IssueEnrollmentToken(t, server.URL, adminKey, orgID, envID, "real2")
	var w1Starts, w2Starts atomic.Int32
	var w1Logs, w2Logs synchronizedBuffer

	// Create run through the real public API (same path the SDK uses).
	createBody, _ := json.Marshal(map[string]any{
		"environment": "staging",
		"input":       map[string]any{"customerEmail": "m2-gate-real@example.com", "markerDir": markerDir},
	})
	createReq, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/"+m2RealWorkflow+"/runs", bytes.NewReader(createBody))
	createReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	createReq.Header.Set("X-Organization-ID", orgID)
	createReq.Header.Set("Idempotency-Key", "m2-gate-real-run-001")
	createReq.Header.Set("Content-Type", "application/json")
	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(createResp.Body)
		t.Fatalf("create run status=%d body=%s", createResp.StatusCode, string(body))
	}
	var run execution.RunDTO
	if err := json.NewDecoder(createResp.Body).Decode(&run); err != nil {
		t.Fatalf("decode run: %v", err)
	}
	t.Logf("M2-GATE-REAL runID=%s markerDir=%s bundle=%s", run.ID, markerDir, bundle)

	// Agent 1 is the only worker at first: it runs A, then starts B.
	cancel1, agent1Done := m2StartAgent(t, server.URL, bundleDir, runnerPath, tok1, "m2-real-w1", &w1Starts, &w1Logs)

	// A completes on Agent 1 as a real child execution.
	m2WaitFor(t, 60*time.Second, "A SUCCEEDED", func() (string, bool) {
		a := m2NodeAttempts(t, tc, orgID, run.ID, "node-a")
		state := fmt.Sprintf("attempts=%v", a)
		return state, len(a) == 1 && strings.Contains(a[0], "=SUCCEEDED=")
	})

	// B becomes observably in-flight on Agent 1: DB attempt RUNNING, B-start
	// marker written by the real child, and Agent 1's second child launched.
	// No blind race: Agent 1 is killed only after this is observed.
	b1desc := m2WaitFor(t, 60*time.Second, "B RUNNING on agent 1", func() (string, bool) {
		b := m2NodeAttempts(t, tc, orgID, run.ID, "node-b")
		calls := m2ReadCalls(markerDir)
		state := fmt.Sprintf("attempts=%v calls=%v agent1children=%d", b, calls, w1Starts.Load())
		if len(b) == 1 && strings.Contains(b[0], "=RUNNING=") &&
			m2CountPrefix(calls, "B-start ") == 1 && w1Starts.Load() == 2 {
			return state, true
		}
		return state, false
	})
	t.Logf("M2-GATE-REAL B in-flight on agent 1: %s", b1desc)
	// Kill Agent 1 while B is in-flight: the Agent loop actually stops,
	// heartbeats stop because the Agent stopped, and its active Node child is
	// terminated per current Agent behavior (SIGTERM abort, then SIGKILL after
	// the supervisor grace period). Termination is observable: the killed
	// child writes B-aborted and can never write B-done afterwards.
	b1 := m2NodeAttempts(t, tc, orgID, run.ID, "node-b")
	if len(b1) != 1 {
		t.Fatalf("expected one B attempt owned by agent 1, got %v", b1)
	}
	attempt1ID := strings.Split(b1[0], "=")[0]
	cancel1()
	select {
	case <-agent1Done:
		t.Logf("M2-GATE-REAL agent 1 loop stopped")
	case <-time.After(20 * time.Second):
		t.Fatal("agent 1 did not stop after kill")
	}
	m2WaitFor(t, 20*time.Second, "B-aborted by killed agent 1", func() (string, bool) {
		calls := m2ReadCalls(markerDir)
		state := fmt.Sprintf("calls=%v", calls)
		return state, m2CountPrefix(calls, "B-aborted "+attempt1ID) == 1
	})
	if got := m2CountPrefix(m2ReadCalls(markerDir), "B-done "); got != 0 {
		t.Fatalf("agent 1 must never complete B after the kill: B-done=%d", got)
	}

	// Ownership expires (controlled clock advancement only; final step/attempt
	// state is never written directly) and the production recovery path marks
	// B LOST with the safe-retry backoff timer parked.
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE task_leases SET expires_at=clock_timestamp()-INTERVAL '1 second' WHERE step_id IN (SELECT rs.id FROM run_steps rs JOIN runs r ON r.id=rs.run_id WHERE r.id=$1::uuid AND r.organization_id=$2::uuid AND rs.node_id='node-b')`, run.ID, orgID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	engine := execution.NewWorkerEngine(tc.pool)
	if _, err := engine.ReconcileExpiredLeases(ctx, orgID); err != nil {
		t.Fatalf("reconcile expired leases: %v", err)
	}
	bAfterKill := m2NodeAttempts(t, tc, orgID, run.ID, "node-b")
	if len(bAfterKill) != 1 || !strings.Contains(bAfterKill[0], "=LOST=") {
		t.Fatalf("B attempt 1 must be LOST after kill, got %v", bAfterKill)
	}
	var bStepState, bWaitReason string
	var pendingTimers int
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		var wr *string
		if err := tx.QueryRow(ctx, `SELECT rs.state, rs.wait_reason FROM run_steps rs JOIN runs r ON r.id=rs.run_id WHERE r.id=$1::uuid AND r.organization_id=$2::uuid AND rs.node_id='node-b'`, run.ID, orgID).Scan(&bStepState, &wr); err != nil {
			return err
		}
		if wr != nil {
			bWaitReason = *wr
		}
		return tx.QueryRow(ctx, `SELECT count(*) FROM timers tm JOIN run_steps rs ON rs.id=tm.step_id JOIN runs r ON r.id=rs.run_id WHERE r.id=$1::uuid AND r.organization_id=$2::uuid AND rs.node_id='node-b' AND tm.state='PENDING'`, run.ID, orgID).Scan(&pendingTimers)
	}); err != nil {
		t.Fatal(err)
	}
	if bStepState != "WAITING" || bWaitReason != "RETRY_BACKOFF" || pendingTimers != 1 {
		t.Fatalf("B must park backoff timer per safe policy: step=%s/%s timers=%d attempts=%v", bStepState, bWaitReason, pendingTimers, bAfterKill)
	}
	t.Logf("M2-GATE-REAL B killed: attempt LOST, step WAITING/RETRY_BACKOFF, 1 pending timer")

	// The backoff timer fires through the production timer path, then Agent 2
	// starts. Only after Agent 2 observably owns B (attempt 2 started) is
	// exactly that attempt released via its attempt-scoped release file, so
	// the recovered child — and no other — can complete.
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE timers SET due_at=clock_timestamp()-INTERVAL '1 second' WHERE step_id IN (SELECT rs.id FROM run_steps rs JOIN runs r ON r.id=rs.run_id WHERE r.id=$1::uuid AND r.organization_id=$2::uuid AND rs.node_id='node-b') AND state='PENDING'`, run.ID, orgID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if fired, err := engine.FireDueRetryTimers(ctx, orgID); err != nil || fired != 1 {
		t.Fatalf("expected backoff timer to fire once, fired=%d err=%v", fired, err)
	}
	cancel2, agent2Done := m2StartAgent(t, server.URL, bundleDir, runnerPath, tok2, "m2-real-w2", &w2Starts, &w2Logs)
	defer func() {
		cancel2()
		select {
		case <-agent2Done:
		case <-time.After(20 * time.Second):
			t.Error("agent 2 did not stop after cancel")
		}
	}()

	attempt2ID := ""
	m2WaitFor(t, 60*time.Second, "agent 2 starts B", func() (string, bool) {
		b := m2NodeAttempts(t, tc, orgID, run.ID, "node-b")
		calls := m2ReadCalls(markerDir)
		state := fmt.Sprintf("attempts=%v calls=%v agent2children=%d", b, calls, w2Starts.Load())
		if len(b) == 2 && m2CountPrefix(calls, "B-start ") == 2 {
			attempt2ID = strings.Split(b[1], "=")[0]
			return state, true
		}
		return state, false
	})
	t.Logf("M2-GATE-REAL agent 2 owns B attempt %s", attempt2ID)
	if err := os.WriteFile(filepath.Join(markerDir, "b.release."+attempt2ID), []byte("go"), 0o600); err != nil {
		t.Fatal(err)
	}

	m2WaitFor(t, 120*time.Second, "run SUCCEEDED", func() (string, bool) {
		var status string
		if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
			return tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid`, run.ID).Scan(&status)
		}); err != nil {
			return fmt.Sprintf("query: %v", err), false
		}
		return status, status == "SUCCEEDED"
	})

	// Runtime proof that A was not rerun: exactly one A child execution, two
	// B executions under one operation ID with a single B completion, one
	// C execution, and one observable kill of the first B child.
	calls := m2ReadCalls(markerDir)
	if m2CountPrefix(calls, "A ") != 1 {
		t.Fatalf("A must execute exactly once at runtime, calls=%v", calls)
	}
	if m2CountPrefix(calls, "B-start ") != 2 {
		t.Fatalf("B must start twice at runtime (killed + recovered), calls=%v", calls)
	}
	if m2CountPrefix(calls, "B-aborted "+attempt1ID) != 1 {
		t.Fatalf("killed B child must observably abort, calls=%v", calls)
	}
	if m2CountPrefix(calls, "B-done ") != 1 {
		t.Fatalf("B must complete exactly once at runtime, calls=%v", calls)
	}
	if m2CountPrefix(calls, "C ") != 1 {
		t.Fatalf("C must execute exactly once at runtime, calls=%v", calls)
	}
	var bOps []string
	for _, line := range calls {
		if strings.HasPrefix(line, "B-start ") {
			bOps = append(bOps, strings.TrimSpace(strings.TrimPrefix(line, "B-start ")))
		}
	}
	if len(bOps) != 2 || bOps[0] == "" || bOps[0] != bOps[1] {
		t.Fatalf("both B executions must share one operation ID, got %v", bOps)
	}
	if w1Starts.Load() != 2 {
		t.Fatalf("agent 1 must launch exactly A and B children, got %d", w1Starts.Load())
	}
	if w2Starts.Load() != 2 {
		t.Fatalf("agent 2 must launch exactly B and C children, got %d", w2Starts.Load())
	}
	t.Logf("M2-GATE-REAL runtime evidence calls=%v agent1children=%d agent2children=%d opID=%s",
		calls, w1Starts.Load(), w2Starts.Load(), bOps[0])

	// Durable proof: B is LOST + SUCCEEDED with a newer epoch.
	bFinal := m2NodeAttempts(t, tc, orgID, run.ID, "node-b")
	if len(bFinal) != 2 || !strings.Contains(bFinal[0], "=LOST=") || !strings.Contains(bFinal[1], "=SUCCEEDED=") {
		t.Fatalf("B must be LOST+SUCCEEDED, got %v", bFinal)
	}
	t.Logf("M2-GATE-REAL B durable: %v", bFinal)

	// Final agreement: API state, DB state, event history, Inspector snapshot.
	apiSnap := m2GetSnapshot(t, server.URL, run.ID, adminKey.PlaintextKey, orgID)
	if apiSnap.Status != contracts.RunStatusSUCCEEDED {
		t.Fatalf("final run must be SUCCEEDED, got %s", apiSnap.Status)
	}
	out, _ := apiSnap.Output.(map[string]any)
	if out["finalConfirmation"] != "CONF-M2-REAL" {
		t.Fatalf("unexpected final output: %+v", apiSnap.Output)
	}
	svcSnap, err := execution.NewService(tc.pool, tc.service).GetRun(ctx, orgID, run.ID)
	if err != nil {
		t.Fatalf("service GetRun: %v", err)
	}
	if svcSnap.Status != contracts.RunStatusSUCCEEDED {
		t.Fatalf("service snapshot must be SUCCEEDED, got %s", svcSnap.Status)
	}
	var dbRunStatus, dbReason string
	var aAttempts, bAttempts, cAttempts int
	var aSucceeded, bSucceeded, cSucceeded int
	var eventCount int
	var eventSeq []string
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		var rc *string
		if err := tx.QueryRow(ctx, `SELECT status, reason_code FROM runs WHERE id=$1::uuid`, run.ID).Scan(&dbRunStatus, &rc); err != nil {
			return err
		}
		if rc != nil {
			dbReason = *rc
		}
		for _, args := range []struct {
			node string
			tot  *int
			succ *int
		}{
			{"node-a", &aAttempts, &aSucceeded},
			{"node-b", &bAttempts, &bSucceeded},
			{"node-c", &cAttempts, &cSucceeded},
		} {
			if err := tx.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE ta.status='SUCCEEDED') FROM task_attempts ta JOIN run_steps rs ON rs.id=ta.step_id JOIN runs r ON r.id=rs.run_id WHERE r.id=$1::uuid AND rs.node_id=$2`, run.ID, args.node).Scan(args.tot, args.succ); err != nil {
				return err
			}
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM run_events WHERE run_id=$1::uuid`, run.ID).Scan(&eventCount); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT event_type FROM run_events WHERE run_id=$1::uuid ORDER BY sequence`, run.ID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var et string
			if err := rows.Scan(&et); err != nil {
				return err
			}
			eventSeq = append(eventSeq, et)
		}
		return rows.Err()
	}); err != nil {
		t.Fatal(err)
	}
	if dbRunStatus != "SUCCEEDED" {
		t.Fatalf("DB run must be SUCCEEDED, got %s/%s", dbRunStatus, dbReason)
	}
	if aAttempts != 1 || aSucceeded != 1 {
		t.Fatalf("A must execute exactly once: attempts=%d succeeded=%d", aAttempts, aSucceeded)
	}
	if bAttempts != 2 || bSucceeded != 1 {
		t.Fatalf("B must have LOST+SUCCEEDED attempts: attempts=%d succeeded=%d", bAttempts, bSucceeded)
	}
	if cAttempts != 1 || cSucceeded != 1 {
		t.Fatalf("C must execute exactly once: attempts=%d succeeded=%d", cAttempts, cSucceeded)
	}
	if eventCount == 0 {
		t.Fatal("event history must be non-empty")
	}
	t.Logf("M2-GATE FINAL run=%s status=%s A=%d/1 B=%d/1 C=%d/1 events=%d seq=%v output=%v",
		run.ID, dbRunStatus, aAttempts, bAttempts, cAttempts, eventCount, eventSeq, out)
	t.Logf("M2-GATE REPEATABLE: rerunning this test preserves A-once/B-retry/C-after semantics")
}

// TestM2_UnknownExternalEffectLostResponseHoldsForReconciliation proves the
// Issue #25 unknown-outcome contract with the disposable external-effect
// fixture whose dedup ledger lives separately from the runtime DB
// (file-backed, survives DB restore):
//
//   - external provider effect commits to the separate ledger,
//   - the definitive completion response is intentionally lost
//     (X-Simulate-Loss),
//   - Deadbolt does NOT blindly re-execute: the step enters a reconciliation
//     hold with exactly one attempt and one OPEN case,
//   - operator resolution completes the workflow, and the external ledger
//     still holds exactly one effect (no duplicate side effect).
func TestM2_UnknownExternalEffectLostResponseHoldsForReconciliation(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	ctx := context.Background()

	extDir := t.TempDir()
	fixture, err := externaleffect.NewFixture(extDir)
	if err != nil {
		t.Fatalf("external fixture: %v", err)
	}
	defer fixture.Close()

	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "m2-unknown")
	const digest = "bundle-m2-unknown-25"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, reconcileManifest(3, 60000))
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "m2-unknown-claim")
	opID := a1.OperationID
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)

	// The handler calls the external provider exactly once. The provider
	// commits to its own ledger, but the response is intentionally lost on
	// the wire: Deadbolt never receives a definitive completion.
	idemKey := "m2-unknown-op-" + opID
	reqBody, _ := json.Marshal(map[string]any{"idempotencyKey": idemKey, "action": "RESERVE_STOCK", "payload": map[string]any{"sku": "WIDGET"}})
	httpReq, _ := http.NewRequest(http.MethodPost, fixture.URL()+"/v1/effects/execute", bytes.NewReader(reqBody))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Simulate-Loss", "true")
	lostResp, err := http.DefaultClient.Do(httpReq)
	if err == nil {
		defer lostResp.Body.Close()
		_, _ = io.ReadAll(lostResp.Body)
	}
	// Either a hijacked-connection error or a 502 is the expected lost reply.
	if err == nil && lostResp.StatusCode != http.StatusBadGateway {
		t.Fatalf("expected simulated loss (502 or transport error), got status=%d err=%v", lostResp.StatusCode, err)
	}
	if rec, found := fixture.Get(idemKey); !found || rec.Duplicate {
		t.Fatalf("external effect must be committed exactly once to the separate ledger: found=%v rec=%+v", found, rec)
	}
	t.Logf("M2-UNKNOWN external ledger committed key=%s count=%d (response intentionally lost)", idemKey, fixture.Count())

	// The worker truthfully reports UNKNOWN: the outcome is ambiguous.
	completeWithError(t, server, session, a1, "PROVIDER_TIMEOUT", true, "UNKNOWN", "")

	var stepState, waitReason string
	var attempts, openCases, pendingTimers int
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		var wr *string
		if err := tx.QueryRow(ctx, `SELECT state, wait_reason FROM run_steps WHERE id=$1::uuid`, stepID).Scan(&stepState, &wr); err != nil {
			return err
		}
		if wr != nil {
			waitReason = *wr
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_attempts WHERE step_id=$1::uuid`, stepID).Scan(&attempts); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM reconciliation_cases WHERE step_id=$1::uuid AND status='OPEN'`, stepID).Scan(&openCases); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT count(*) FROM timers WHERE step_id=$1::uuid AND state='PENDING'`, stepID).Scan(&pendingTimers)
	}); err != nil {
		t.Fatal(err)
	}
	if stepState != "WAITING" || waitReason != "RECONCILIATION" || attempts != 1 || openCases != 1 || pendingTimers != 0 {
		t.Fatalf("unknown outcome must hold without blind retry: step=%s/%s attempts=%d cases=%d timers=%d",
			stepState, waitReason, attempts, openCases, pendingTimers)
	}
	t.Logf("M2-UNKNOWN held: step WAITING/RECONCILIATION, 1 attempt, 1 OPEN case, 0 timers")

	// Operator reconciles against the separate ledger (effect exists exactly
	// once) and confirms success; the workflow completes without a second
	// external execution.
	caseID, rev := openReconciliationCase(t, tc, orgID, stepID)
	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleOperator, "m2-unknown-op")
	status, body := resolveCaseHTTP(t, server, token, orgID, caseID, map[string]any{
		"action": "confirm_succeeded", "evidence": "ledger:" + idemKey, "reason": "provider ledger shows one committed effect",
		"result": map[string]any{"ok": true}, "expectedRevision": rev,
	})
	if status != http.StatusOK {
		t.Fatalf("operator resolve failed: status=%d body=%v", status, body)
	}
	if dup, _ := fixture.ExecuteEffect(idemKey, "RESERVE_STOCK", map[string]any{"sku": "WIDGET"}); !dup.Duplicate {
		t.Fatal("re-executing the same idempotency key must be a ledger duplicate, not a new effect")
	}
	if fixture.Count() != 1 {
		t.Fatalf("external ledger must hold exactly one effect, got %d", fixture.Count())
	}
	var finalStep, finalRun, completionSource string
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT state, completion_source FROM run_steps WHERE id=$1::uuid`, stepID).Scan(&finalStep, &completionSource); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid`, runID).Scan(&finalRun)
	}); err != nil {
		t.Fatal(err)
	}
	if finalStep != "SUCCEEDED" || completionSource != "RECONCILIATION" || finalRun != "SUCCEEDED" {
		t.Fatalf("operator resolution must complete workflow: step=%s/%s run=%s", finalStep, completionSource, finalRun)
	}
	t.Logf("M2-UNKNOWN RESOLVED run=%s step SUCCEEDED/RECONCILIATION ledger=%d (singular)", runID, fixture.Count())
	_ = deploymentID
}

// TestM2_HTTPStagingLocalFixture proves the safe HTTP fixture contract
// locally: fast health/echo plus a bounded slow path whose client-side
// timeout is observable through a real network boundary. This is the local
// half of the Issue #25 real-safe-HTTP requirement. The hosted half
// (TestM2_HTTPStagingHosted) runs only when DEADBOLT_HTTP_STAGING_URL is set
// by the operator on hosted staging and is otherwise PENDING_HOSTED_STAGING.
func TestM2_HTTPStagingLocalFixture(t *testing.T) {
	fix := httpstaging.NewFixture(3000)
	defer fix.Close()

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(fix.URL() + "/health")
	if err != nil {
		t.Fatalf("fixture health: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("fixture health status=%d", resp.StatusCode)
	}

	// A 50ms slow response succeeds under a generous timeout.
	fastClient := &http.Client{Timeout: 5 * time.Second}
	fastResp, err := fastClient.Get(fix.URL() + "/slow?delayMs=50")
	if err != nil {
		t.Fatalf("fast slow-path: %v", err)
	}
	defer fastResp.Body.Close()
	if fastResp.StatusCode != http.StatusOK {
		t.Fatalf("fast slow-path status=%d", fastResp.StatusCode)
	}

	// A 2000ms server delay exceeds a 200ms client timeout: the timeout is
	// observed through the real HTTP boundary, not a unit timestamp check.
	timeoutClient := &http.Client{Timeout: 200 * time.Millisecond}
	_, err = timeoutClient.Get(fix.URL() + "/slow?delayMs=2000")
	if err == nil {
		t.Fatal("expected client timeout against slow fixture path")
	}
	t.Logf("M2-HTTP-LOCAL fixture=%s health+echo ok, 50ms ok, 2000ms timed out at 200ms client timeout, requests=%d",
		fix.URL(), fix.RequestCount())
}

// TestM2_HTTPStagingHosted is the real safe HTTP staging integration for
// Issue #25. It runs only when the operator sets DEADBOLT_HTTP_STAGING_URL to
// the controlled staging fixture URL; otherwise it reports
// PENDING_HOSTED_STAGING and does not claim success.
func TestM2_HTTPStagingHosted(t *testing.T) {
	target, ok := httpstaging.StagingConfig()
	if !ok {
		t.Skip("PENDING_HOSTED_STAGING: set DEADBOLT_HTTP_STAGING_URL to the controlled staging fixture URL to run the real safe HTTP staging integration")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(target + "/health")
	if err != nil {
		t.Fatalf("hosted staging health %s: %v", target, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("hosted staging health status=%d", resp.StatusCode)
	}
	timeoutClient := &http.Client{Timeout: 300 * time.Millisecond}
	_, err = timeoutClient.Get(target + "/slow?delayMs=2000")
	if err == nil {
		t.Fatal("expected client timeout against hosted slow path")
	}
	t.Logf("M2-HTTP-HOSTED target=%s health ok, slow-path timeout observed", target)
}
