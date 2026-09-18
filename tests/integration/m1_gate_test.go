package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

// TestM1RunPinningAcrossDeploymentActivation proves F-11 at the real worker
// claim boundary. An in-flight run remains pinned to V1 after V2 is activated;
// only a worker advertising V1 can claim its remaining step, while a new run
// is claimed only with V2.
func TestM1RunPinningAcrossDeploymentActivation(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	const workflowName = "m1-pinned-flow"
	v1 := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	v2 := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	manifest := func(bundle string) []byte {
		schema := map[string]any{
			"type":                 "object",
			"properties":           map[string]any{"value": map[string]any{"type": "string"}},
			"required":             []any{"value"},
			"additionalProperties": false,
		}
		tasks := []map[string]any{
			{"name": "task-a", "entrypoint": "tasks/a.js", "timeoutMs": 30000, "recovery": "idempotent", "idempotencyWindowMs": 305000, "inputSchema": schema, "outputSchema": schema},
			{"name": "task-b", "entrypoint": "tasks/b.js", "timeoutMs": 30000, "recovery": "idempotent", "idempotencyWindowMs": 305000, "inputSchema": schema, "outputSchema": schema},
		}
		workflow := map[string]any{
			"manifestVersion": 1,
			"name":            workflowName,
			"inputSchema":     schema,
			"outputSchema":    schema,
			"nodes": []map[string]any{
				{"id": "node-a", "type": "task", "task": "task-a", "after": []any{}, "input": map[string]any{"value": map[string]any{"$ref": "run.input", "pointer": "/value"}}},
				{"id": "node-b", "type": "task", "task": "task-b", "after": []any{"node-a"}, "input": map[string]any{"value": map[string]any{"$ref": "step.output", "stepId": "node-a", "pointer": "/value"}}},
			},
			"output": map[string]any{"value": map[string]any{"$ref": "step.output", "stepId": "node-b", "pointer": "/value"}},
		}
		return createLifecycleManifest(bundle, tasks, []map[string]any{workflow})
	}

	workerSession, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "m1-pinning")
	depV1 := registerM1Deployment(t, server, adminKey, orgID, envID, manifest(v1), "v1")

	// Advertise V1 before activation so preflight sees a real enrolled worker.
	pollM1Worker(t, server, workerSession, []string{v1}, "advertise-v1")
	activateM1Deployment(t, server, adminKey, orgID, envID, workflowName, depV1, 0, "activate-v1")

	runV1 := createM1Run(t, server, adminKey, orgID, workflowName, envID, "run-v1", "first")
	assignmentA := claimExecution(t, server, workerSession, v1, "claim-v1-a")
	if assignmentA.RunID != runV1.ID || assignmentA.DeploymentDigest != v1 || assignmentA.BundleDigest != v1 {
		t.Fatalf("V1 assignment was not pinned to V1: %+v", assignmentA)
	}
	startNode(t, server, workerSession, assignmentA.AttemptID, assignmentA.OwnershipEpoch)

	depV2 := registerM1Deployment(t, server, adminKey, orgID, envID, manifest(v2), "v2")
	// Add V2 to the same worker's advertised capabilities, then activate it.
	pollM1Worker(t, server, workerSession, []string{v2}, "advertise-v2")
	activateM1Deployment(t, server, adminKey, orgID, envID, workflowName, depV2, 1, "activate-v2")

	// Finish A after the channel moved to V2. B is still part of the V1 run.
	completeNode(t, server, workerSession, assignmentA.AttemptID, assignmentA.OwnershipEpoch, "SUCCEEDED", map[string]any{"value": "from-v1"}, "complete-v1-a")

	// A V2-only poll must not claim the remaining V1 step.
	v2Poll := pollM1Worker(t, server, workerSession, []string{v2}, "claim-v2-old-run")
	if len(v2Poll.Assignments) != 0 {
		t.Fatalf("V2 worker claimed an old V1 run: %+v", v2Poll.Assignments)
	}

	v1B := claimExecution(t, server, workerSession, v1, "claim-v1-b")
	if v1B.RunID != runV1.ID || v1B.DeploymentDigest != v1 || v1B.BundleDigest != v1 {
		t.Fatalf("remaining V1 step was not pinned to V1: %+v", v1B)
	}
	startNode(t, server, workerSession, v1B.AttemptID, v1B.OwnershipEpoch)
	completeNode(t, server, workerSession, v1B.AttemptID, v1B.OwnershipEpoch, "SUCCEEDED", map[string]any{"value": "done-v1"}, "complete-v1-b")

	runV2 := createM1Run(t, server, adminKey, orgID, workflowName, envID, "run-v2", "second")
	v2Assignment := claimExecution(t, server, workerSession, v2, "claim-v2-new-run")
	if v2Assignment.RunID != runV2.ID || v2Assignment.DeploymentDigest != v2 || v2Assignment.BundleDigest != v2 {
		t.Fatalf("new run did not resolve to active V2: %+v", v2Assignment)
	}
	startNode(t, server, workerSession, v2Assignment.AttemptID, v2Assignment.OwnershipEpoch)
	completeM1Node(t, server, workerSession, v2Assignment.AttemptID, v2Assignment.OwnershipEpoch, map[string]any{"unexpected": true}, "complete-invalid-output")
	var failed execution.RunSnapshotDTO
	getRunM1(t, server, adminKey, orgID, runV2.ID, &failed)
	if failed.Status != "FAILED" || failed.ReasonCode == nil || *failed.ReasonCode != "OUTPUT_SCHEMA_VIOLATION" {
		t.Fatalf("invalid task output was not terminal: status=%s reason=%v", failed.Status, failed.ReasonCode)
	}
	steps := make(map[string]execution.RunStepDTO, len(failed.Steps))
	for _, step := range failed.Steps {
		steps[step.NodeID] = step
	}
	invalidStep, ok := steps["node-a"]
	if !ok || len(invalidStep.Attempts) != 1 {
		t.Fatalf("invalid output snapshot missing node-a attempt: %+v", failed.Steps)
	}
	if err, ok := invalidStep.Attempts[0].Error.(map[string]any); !ok || err["code"] != "OUTPUT_SCHEMA_VIOLATION" || err["retryable"] != false {
		t.Fatalf("invalid output was not recorded as non-retryable: %+v", invalidStep.Attempts[0].Error)
	}
	if blocked, ok := steps["node-b"]; !ok || blocked.Status != "CANCELLED" || len(blocked.Attempts) != 0 {
		t.Fatalf("remaining V2 node was not cancelled safely: %+v", blocked)
	}

	// A digest that was never deployed cannot claim the new V2 run.
	badPoll := pollM1Worker(t, server, workerSession, []string{"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}, "claim-incompatible")
	if len(badPoll.Assignments) != 0 {
		t.Fatalf("incompatible worker claimed an assignment: %+v", badPoll.Assignments)
	}
}

func completeM1Node(t *testing.T, server *httptest.Server, session *testWorkerSession, attemptID string, epoch int64, output any, digest string) {
	t.Helper()
	req := worker.CompleteRequestDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: digest, WorkerID: session.WorkerID, SessionID: session.SessionID, AttemptID: attemptID, OwnershipEpoch: epoch, Outcome: "SUCCEEDED", Output: output, ResultDigest: digest}
	req.ResultDigest, _ = worker.CanonicalCompletionDigest(&req)
	var resp worker.CompleteResponseDTO
	status := postWorkerJSON(t, server, "/worker/v1/complete", session.SessionToken, req, &resp)
	if status != http.StatusOK || !resp.Accepted {
		t.Fatalf("completion %s failed: status=%d resp=%+v", digest, status, resp)
	}
}

func getRunM1(t *testing.T, server *httptest.Server, key *tenant.GeneratedKey, orgID, runID string, out *execution.RunSnapshotDTO) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/runs/%s", server.URL, runID), nil)
	req.Header.Set("Authorization", "Bearer "+key.PlaintextKey)
	req.Header.Set("X-Organization-ID", orgID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get run %s: status %d", runID, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatal(err)
	}
}

func registerM1Deployment(t *testing.T, server *httptest.Server, key *tenant.GeneratedKey, orgID, envID string, manifest []byte, suffix string) string {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/deployments?environment=%s", server.URL, envID), bytes.NewReader(manifest))
	req.Header.Set("Authorization", "Bearer "+key.PlaintextKey)
	req.Header.Set("X-Organization-ID", orgID)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "m1-deployment-"+suffix)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		t.Fatalf("register %s: status %d", suffix, resp.StatusCode)
	}
	var out struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out.ID
}

func activateM1Deployment(t *testing.T, server *httptest.Server, key *tenant.GeneratedKey, orgID, envID, workflow, deploymentID string, revision int, suffix string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"deploymentId": deploymentID, "expectedRevision": revision, "allowSingleWorker": true})
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/workflows/%s/activate?environment=%s", server.URL, workflow, envID), bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+key.PlaintextKey)
	req.Header.Set("X-Organization-ID", orgID)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "m1-activation-"+suffix)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("activate %s: status %d", suffix, resp.StatusCode)
	}
}

func createM1Run(t *testing.T, server *httptest.Server, key *tenant.GeneratedKey, orgID, workflow, envID, idempotency, value string) execution.RunDTO {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"environment": "staging", "input": map[string]any{"value": value}})
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/workflows/%s/runs", server.URL, workflow), bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+key.PlaintextKey)
	req.Header.Set("X-Organization-ID", orgID)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "m1-"+idempotency)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("create %s: status %d", idempotency, resp.StatusCode)
	}
	var run execution.RunDTO
	if err := json.NewDecoder(resp.Body).Decode(&run); err != nil {
		t.Fatal(err)
	}
	return run
}

func pollM1Worker(t *testing.T, server *httptest.Server, session *testWorkerSession, digests []string, requestID string) worker.PollResponseDTO {
	t.Helper()
	var out worker.PollResponseDTO
	status := postWorkerJSON(t, server, "/worker/v1/poll", session.SessionToken, worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: requestID, WorkerID: session.WorkerID,
		SessionID: session.SessionID, AvailableSlots: 1, DeploymentDigests: digests, Pool: "default",
	}, &out)
	if status != http.StatusOK {
		t.Fatalf("poll %s: status %d", requestID, status)
	}
	return out
}
