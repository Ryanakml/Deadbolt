package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

func setupInspectorWorkflow(t *testing.T, tc *tenantTestContext, server *httptest.Server, orgID, envID string, adminKey *tenant.GeneratedKey) (string, string) {
	t.Helper()
	tempDir := t.TempDir()
	bundleDigest := writeAgentBundle(t, tempDir)

	schema := map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"val": map[string]any{"type": "string"}},
		"required":             []any{"val"},
		"additionalProperties": false,
	}
	tasks := []map[string]any{
		{
			"name":                "task-simple",
			"entrypoint":          "tasks/agent.mjs",
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
			"name":            "inspector-workflow",
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

	manifestBytes := createLifecycleManifest(bundleDigest, tasks, workflows)
	depID := registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "inspector-workflow", manifestBytes)
	return depID, bundleDigest
}

func enrollTestWorker(t *testing.T, tc *tenantTestContext, server *httptest.Server, orgID, envID, bundleDigest string) (*worker.WorkerSessionContext, string) {
	t.Helper()
	sess, _ := enrollExecutionWorker(t, tc, server, orgID, envID, fmt.Sprintf("insp-%d", time.Now().UnixNano()))

	if bundleDigest != "" {
		pollBody, _ := json.Marshal(worker.PollRequestDTO{
			ProtocolVersion:   worker.ProtocolVersion,
			RequestID:         fmt.Sprintf("init-poll-%d", time.Now().UnixNano()),
			WorkerID:          sess.WorkerID,
			SessionID:         sess.SessionID,
			AvailableSlots:    0,
			DeploymentDigests: []string{bundleDigest},
			Pool:              "default",
		})
		pReq, _ := http.NewRequest(http.MethodPost, server.URL+"/worker/v1/poll", bytes.NewReader(pollBody))
		pReq.Header.Set("Authorization", "Bearer "+sess.SessionToken)
		pReq.Header.Set("Content-Type", "application/json")
		pRes, err := http.DefaultClient.Do(pReq)
		if err == nil {
			pRes.Body.Close()
		}
	}

	return &worker.WorkerSessionContext{
		SessionID:      sess.SessionID,
		WorkerID:       sess.WorkerID,
		OrganizationID: orgID,
		EnvironmentID:  envID,
		PoolName:       "default",
	}, sess.SessionToken
}

func TestRunInspectorConsistentSnapshotAndStepAttempts(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer server.Close()

	depID, bundleDigest := setupInspectorWorkflow(t, tc, server, orgID, envID, adminKey)
	sessionCtx, sessionToken := enrollTestWorker(t, tc, server, orgID, envID, bundleDigest)

	// Create run
	createReq, _ := http.NewRequest("POST", server.URL+"/v1/workflows/inspector-workflow/runs", strings.NewReader(`{"environment":"staging","input":{"val":"test-data"}}`))
	createReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	createReq.Header.Set("Idempotency-Key", "inspect-run-001")
	createReq.Header.Set("Content-Type", "application/json")
	createRes, err := http.DefaultClient.Do(createReq)
	if err != nil || createRes.StatusCode != http.StatusAccepted {
		t.Fatalf("create run failed: %v (status %d)", err, createRes.StatusCode)
	}
	var runResp execution.RunDTO
	_ = json.NewDecoder(createRes.Body).Decode(&runResp)
	createRes.Body.Close()

	// Initial snapshot check
	snapReq, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+runResp.ID, nil)
	snapReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	snapRes, err := http.DefaultClient.Do(snapReq)
	if err != nil || snapRes.StatusCode != http.StatusOK {
		t.Fatalf("get snapshot failed: %v (status %d)", err, snapRes.StatusCode)
	}
	var snap execution.RunSnapshotDTO
	_ = json.NewDecoder(snapRes.Body).Decode(&snap)
	snapRes.Body.Close()

	if snap.Status != contracts.RunStatusQUEUED {
		t.Fatalf("expected QUEUED, got %s", snap.Status)
	}
	if snap.LastEventSequence != 1 {
		t.Fatalf("expected lastEventSequence 1, got %d", snap.LastEventSequence)
	}
	if len(snap.Steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(snap.Steps))
	}
	if len(snap.Steps[0].Attempts) != 0 {
		t.Fatalf("expected 0 attempts initially, got %d", len(snap.Steps[0].Attempts))
	}

	// Worker polls and claims task
	pollBody, _ := json.Marshal(worker.PollRequestDTO{
		ProtocolVersion:   worker.ProtocolVersion,
		RequestID:         "p-1",
		WorkerID:          sessionCtx.WorkerID,
		SessionID:         sessionCtx.SessionID,
		AvailableSlots:    1,
		DeploymentDigests: []string{bundleDigest},
		Pool:              "default",
	})
	pollReq, _ := http.NewRequest("POST", server.URL+"/worker/v1/poll", bytes.NewReader(pollBody))
	pollReq.Header.Set("Authorization", "Bearer "+sessionToken)
	pollReq.Header.Set("Content-Type", "application/json")
	pollRes, err := http.DefaultClient.Do(pollReq)
	if err != nil || pollRes.StatusCode != http.StatusOK {
		t.Fatalf("poll failed: %v (status %d)", err, pollRes.StatusCode)
	}
	var pollResp worker.PollResponseDTO
	_ = json.NewDecoder(pollRes.Body).Decode(&pollResp)
	pollRes.Body.Close()

	if len(pollResp.Assignments) != 1 {
		t.Fatalf("expected 1 assignment, got %d", len(pollResp.Assignments))
	}
	as := pollResp.Assignments[0]

	// Worker starts task
	startReq, _ := http.NewRequest("POST", server.URL+"/worker/v1/start", strings.NewReader(
		fmt.Sprintf(`{"protocolVersion":1,"requestId":"s-1","workerId":"%s","sessionId":"%s","attemptId":"%s","ownershipEpoch":%d}`, sessionCtx.WorkerID, sessionCtx.SessionID, as.AttemptID, as.OwnershipEpoch),
	))
	startReq.Header.Set("Authorization", "Bearer "+sessionToken)
	startReq.Header.Set("Content-Type", "application/json")
	startRes, err := http.DefaultClient.Do(startReq)
	if err != nil || startRes.StatusCode != http.StatusOK {
		t.Fatalf("start failed: %v (status %d)", err, startRes.StatusCode)
	}
	startRes.Body.Close()

	// Snapshot during RUNNING
	snapRes2, err := http.DefaultClient.Do(snapReq)
	if err != nil || snapRes2.StatusCode != http.StatusOK {
		t.Fatalf("get snapshot running failed: %v", err)
	}
	var snap2 execution.RunSnapshotDTO
	_ = json.NewDecoder(snapRes2.Body).Decode(&snap2)
	snapRes2.Body.Close()

	if snap2.Status != contracts.RunStatusRUNNING {
		t.Fatalf("expected RUNNING, got %s", snap2.Status)
	}
	if len(snap2.Steps[0].Attempts) != 1 {
		t.Fatalf("expected 1 attempt, got %d", len(snap2.Steps[0].Attempts))
	}
	att := snap2.Steps[0].Attempts[0]
	if att.Status != "RUNNING" {
		t.Fatalf("expected attempt status RUNNING, got %s", att.Status)
	}
	if att.WorkerSessionID == nil || *att.WorkerSessionID != sessionCtx.SessionID {
		t.Fatalf("expected session %s, got %v", sessionCtx.SessionID, att.WorkerSessionID)
	}
	if att.StartedAt == nil {
		t.Fatal("expected non-nil startedAt")
	}

	// Worker completes task
	completePayload := worker.CompleteRequestDTO{
		ProtocolVersion: 1,
		RequestID:       "c-1",
		WorkerID:        sessionCtx.WorkerID,
		SessionID:       sessionCtx.SessionID,
		AttemptID:       as.AttemptID,
		OwnershipEpoch:  as.OwnershipEpoch,
		Outcome:         "SUCCEEDED",
		Output:          map[string]any{"val": "test-data-done"},
	}
	digest, _ := worker.CanonicalCompletionDigest(&completePayload)
	completePayload.ResultDigest = digest
	cb, _ := json.Marshal(completePayload)

	compReq, _ := http.NewRequest("POST", server.URL+"/worker/v1/complete", bytes.NewReader(cb))
	compReq.Header.Set("Authorization", "Bearer "+sessionToken)
	compReq.Header.Set("Content-Type", "application/json")
	compRes, err := http.DefaultClient.Do(compReq)
	if err != nil || compRes.StatusCode != http.StatusOK {
		t.Fatalf("complete failed: %v (status %d)", err, compRes.StatusCode)
	}
	compRes.Body.Close()

	// Snapshot after SUCCEEDED
	snapRes3, err := http.DefaultClient.Do(snapReq)
	if err != nil || snapRes3.StatusCode != http.StatusOK {
		t.Fatalf("get snapshot complete failed: %v", err)
	}
	var snap3 execution.RunSnapshotDTO
	_ = json.NewDecoder(snapRes3.Body).Decode(&snap3)
	snapRes3.Body.Close()

	if snap3.Status != contracts.RunStatusSUCCEEDED {
		t.Fatalf("expected SUCCEEDED, got %s", snap3.Status)
	}
	if len(snap3.Steps[0].Attempts) != 1 {
		t.Fatalf("expected 1 attempt, got %d", len(snap3.Steps[0].Attempts))
	}
	if snap3.Steps[0].Attempts[0].Status != "SUCCEEDED" {
		t.Fatalf("expected attempt SUCCEEDED, got %s", snap3.Steps[0].Attempts[0].Status)
	}
	if snap3.Steps[0].Attempts[0].CompletedAt == nil {
		t.Fatal("expected non-nil completedAt")
	}
	if snap3.Output == nil {
		t.Fatal("expected non-nil output for caller with payload:read")
	}
	_ = depID
}

func TestRunInspectorPayloadReadBoundaryAndRedaction(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer server.Close()

	_, bundleDigest := setupInspectorWorkflow(t, tc, server, orgID, envID, adminKey)
	sessionCtx, sessionToken := enrollTestWorker(t, tc, server, orgID, envID, bundleDigest)

	// Create run with admin key
	createReq, _ := http.NewRequest("POST", server.URL+"/v1/workflows/inspector-workflow/runs", strings.NewReader(`{"environment":"staging","input":{"val":"sensitive-secret-input"}}`))
	createReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	createReq.Header.Set("Idempotency-Key", "payload-boundary-run")
	createReq.Header.Set("Content-Type", "application/json")
	createRes, err := http.DefaultClient.Do(createReq)
	if err != nil || createRes.StatusCode != http.StatusAccepted {
		t.Fatalf("create run failed: %v", err)
	}
	var runResp execution.RunDTO
	_ = json.NewDecoder(createRes.Body).Decode(&runResp)
	createRes.Body.Close()

	// Complete run
	pollBody, _ := json.Marshal(worker.PollRequestDTO{
		ProtocolVersion:   worker.ProtocolVersion,
		RequestID:         "p-2",
		WorkerID:          sessionCtx.WorkerID,
		SessionID:         sessionCtx.SessionID,
		AvailableSlots:    1,
		DeploymentDigests: []string{bundleDigest},
		Pool:              "default",
	})
	pollReq, _ := http.NewRequest("POST", server.URL+"/worker/v1/poll", bytes.NewReader(pollBody))
	pollReq.Header.Set("Authorization", "Bearer "+sessionToken)
	pollReq.Header.Set("Content-Type", "application/json")
	pollRes, _ := http.DefaultClient.Do(pollReq)
	var pollResp worker.PollResponseDTO
	_ = json.NewDecoder(pollRes.Body).Decode(&pollResp)
	pollRes.Body.Close()
	as := pollResp.Assignments[0]

	startReq, _ := http.NewRequest("POST", server.URL+"/worker/v1/start", strings.NewReader(
		fmt.Sprintf(`{"protocolVersion":1,"requestId":"s-2","workerId":"%s","sessionId":"%s","attemptId":"%s","ownershipEpoch":%d}`, sessionCtx.WorkerID, sessionCtx.SessionID, as.AttemptID, as.OwnershipEpoch),
	))
	startReq.Header.Set("Authorization", "Bearer "+sessionToken)
	startReq.Header.Set("Content-Type", "application/json")
	startRes, _ := http.DefaultClient.Do(startReq)
	startRes.Body.Close()

	compPayload := worker.CompleteRequestDTO{
		ProtocolVersion: 1,
		RequestID:       "c-2",
		WorkerID:        sessionCtx.WorkerID,
		SessionID:       sessionCtx.SessionID,
		AttemptID:       as.AttemptID,
		OwnershipEpoch:  as.OwnershipEpoch,
		Outcome:         "SUCCEEDED",
		Output:          map[string]any{"val": "sensitive-classified-output"},
	}
	digest, _ := worker.CanonicalCompletionDigest(&compPayload)
	compPayload.ResultDigest = digest
	cb, _ := json.Marshal(compPayload)
	compReq, _ := http.NewRequest("POST", server.URL+"/worker/v1/complete", bytes.NewReader(cb))
	compReq.Header.Set("Authorization", "Bearer "+sessionToken)
	compReq.Header.Set("Content-Type", "application/json")
	compRes, _ := http.DefaultClient.Do(compReq)
	compRes.Body.Close()

	// Create restricted key WITHOUT payload:read (only runs:read)
	restrictedKey := bootstrapTestKey(t, tc.service, orgID, envID, []string{
		tenant.CapRunsRead,
	})

	// 1. Snapshot: Output & Error must be redacted (nil)
	req1, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+runResp.ID, nil)
	req1.Header.Set("Authorization", "Bearer "+restrictedKey.PlaintextKey)
	res1, err := http.DefaultClient.Do(req1)
	if err != nil || res1.StatusCode != http.StatusOK {
		t.Fatalf("restricted snapshot failed: %v (status %d)", err, res1.StatusCode)
	}
	var snap execution.RunSnapshotDTO
	_ = json.NewDecoder(res1.Body).Decode(&snap)
	res1.Body.Close()

	if snap.Output != nil {
		t.Fatalf("security violation: expected nil output for restricted viewer, got %v", snap.Output)
	}

	// 2. Events: Payload must be sanitized
	req2, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+runResp.ID+"/events", nil)
	req2.Header.Set("Authorization", "Bearer "+restrictedKey.PlaintextKey)
	res2, err := http.DefaultClient.Do(req2)
	if err != nil || res2.StatusCode != http.StatusOK {
		t.Fatalf("restricted events failed: %v", err)
	}
	var eventsResp execution.RunEventsResponseDTO
	_ = json.NewDecoder(res2.Body).Decode(&eventsResp)
	res2.Body.Close()

	for _, ev := range eventsResp.Events {
		if ev.Payload != nil {
			if m, ok := ev.Payload.(map[string]any); ok {
				if _, hasOutput := m["output"]; hasOutput {
					t.Fatalf("security violation: event %s contains unredacted output", ev.ID)
				}
				if _, hasInput := m["input"]; hasInput {
					t.Fatalf("security violation: event %s contains unredacted input", ev.ID)
				}
			}
		}
	}

	// 3. Logs query: Restricted viewer must receive 403 Forbidden
	req3, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+runResp.ID+"/logs", nil)
	req3.Header.Set("Authorization", "Bearer "+restrictedKey.PlaintextKey)
	res3, err := http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatalf("logs request failed: %v", err)
	}
	defer res3.Body.Close()

	if res3.StatusCode != http.StatusForbidden {
		t.Fatalf("security violation: expected 403 FORBIDDEN for logs query without payload:read, got %d", res3.StatusCode)
	}
}

func TestRunInspectorScopedListing(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer server.Close()

	_, bundleDigest := setupInspectorWorkflow(t, tc, server, orgID, envID, adminKey)
	enrollTestWorker(t, tc, server, orgID, envID, bundleDigest)

	// Create 3 runs
	for i := 1; i <= 3; i++ {
		r, _ := http.NewRequest("POST", server.URL+"/v1/workflows/inspector-workflow/runs", strings.NewReader(`{"environment":"staging","input":{"val":"item"}}`))
		r.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
		r.Header.Set("Idempotency-Key", fmt.Sprintf("scoped-run-%d", i))
		r.Header.Set("Content-Type", "application/json")
		res, _ := http.DefaultClient.Do(r)
		res.Body.Close()
	}

	// List runs with limit 2
	listReq, _ := http.NewRequest("GET", server.URL+"/v1/runs?environment=staging&limit=2", nil)
	listReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	listRes, err := http.DefaultClient.Do(listReq)
	if err != nil || listRes.StatusCode != http.StatusOK {
		t.Fatalf("list runs failed: %v (status %d)", err, listRes.StatusCode)
	}
	var listResp execution.RunListResponseDTO
	_ = json.NewDecoder(listRes.Body).Decode(&listResp)
	listRes.Body.Close()

	if len(listResp.Items) != 2 {
		t.Fatalf("expected 2 runs, got %d", len(listResp.Items))
	}
	if listResp.NextCursor == nil || *listResp.NextCursor == "" {
		t.Fatal("expected nextCursor to be populated")
	}

	// Follow next cursor
	nextReq, _ := http.NewRequest("GET", server.URL+"/v1/runs?environment=staging&limit=2&cursor="+*listResp.NextCursor, nil)
	nextReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	nextRes, err := http.DefaultClient.Do(nextReq)
	if err != nil || nextRes.StatusCode != http.StatusOK {
		t.Fatalf("list runs next page failed: %v", err)
	}
	var nextResp execution.RunListResponseDTO
	_ = json.NewDecoder(nextRes.Body).Decode(&nextResp)
	nextRes.Body.Close()

	if len(nextResp.Items) != 1 {
		t.Fatalf("expected 1 run on second page, got %d", len(nextResp.Items))
	}

	// List workers (requires CapWorkersRead)
	workersKey := bootstrapTestKey(t, tc.service, orgID, envID, []string{
		tenant.CapWorkersRead,
	})
	wReq, _ := http.NewRequest("GET", server.URL+"/v1/workers?environment=staging", nil)
	wReq.Header.Set("Authorization", "Bearer "+workersKey.PlaintextKey)
	wRes, err := http.DefaultClient.Do(wReq)
	if err != nil || wRes.StatusCode != http.StatusOK {
		t.Fatalf("list workers failed: %v (status %d)", err, wRes.StatusCode)
	}
	var wResp execution.WorkerListResponseDTO
	_ = json.NewDecoder(wRes.Body).Decode(&wResp)
	wRes.Body.Close()

	if len(wResp.Items) < 1 {
		t.Fatalf("expected at least 1 worker, got %d", len(wResp.Items))
	}
	foundActiveWithDigest := false
	for _, w := range wResp.Items {
		if w.Status == "ACTIVE" {
			for _, d := range w.DeploymentDigests {
				if d == bundleDigest {
					foundActiveWithDigest = true
					break
				}
			}
		}
	}
	if !foundActiveWithDigest {
		t.Fatalf("expected active worker advertising bundle digest %s in list %+v", bundleDigest, wResp.Items)
	}
}

func TestRunInspectorCrossTenantIsolation(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer server.Close()

	setupInspectorWorkflow(t, tc, server, orgID, envID, adminKey)

	// Create run in Org A
	createReq, _ := http.NewRequest("POST", server.URL+"/v1/workflows/inspector-workflow/runs", strings.NewReader(`{"environment":"staging","input":{"val":"tenant-a"}}`))
	createReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	createReq.Header.Set("Idempotency-Key", "tenant-iso-run")
	createReq.Header.Set("Content-Type", "application/json")
	createRes, _ := http.DefaultClient.Do(createReq)
	var runResp execution.RunDTO
	_ = json.NewDecoder(createRes.Body).Decode(&runResp)
	createRes.Body.Close()

	// Create separate Org B and Key B
	ctx := context.Background()
	ownerB, _ := tenant.NewUUID()
	orgB, _ := tc.service.CreateOrganization(ctx, ownerB, "Tenant B Org")
	projB, _ := tc.service.CreateProject(ctx, orgB.ID, "Tenant B Proj")
	envB, _ := tc.service.CreateEnvironment(ctx, orgB.ID, projB.ID, tenant.EnvStaging, 5)
	keyB := bootstrapTestKey(t, tc.service, orgB.ID, envB.ID, []string{
		tenant.CapRunsRead,
		tenant.CapPayloadRead,
		tenant.CapWorkersRead,
	})

	// Tenant B tries to inspect Run A -> must 404
	req, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+runResp.ID, nil)
	req.Header.Set("Authorization", "Bearer "+keyB.PlaintextKey)
	res, _ := http.DefaultClient.Do(req)
	res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-tenant violation: expected 404, got %d", res.StatusCode)
	}

	// Tenant B tries to query events of Run A -> must 404
	reqEv, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+runResp.ID+"/events", nil)
	reqEv.Header.Set("Authorization", "Bearer "+keyB.PlaintextKey)
	resEv, _ := http.DefaultClient.Do(reqEv)
	resEv.Body.Close()
	if resEv.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-tenant violation: expected 404 on events, got %d", resEv.StatusCode)
	}

	// Tenant B tries to query logs of Run A -> must 404
	reqLogs, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+runResp.ID+"/logs", nil)
	reqLogs.Header.Set("Authorization", "Bearer "+keyB.PlaintextKey)
	resLogs, _ := http.DefaultClient.Do(reqLogs)
	resLogs.Body.Close()
	if resLogs.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-tenant violation: expected 404 on logs, got %d", resLogs.StatusCode)
	}
}

func TestRunInspectorSSEReconnectAndCatchUp(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer server.Close()

	_, bundleDigest := setupInspectorWorkflow(t, tc, server, orgID, envID, adminKey)
	sessionCtx, sessionToken := enrollTestWorker(t, tc, server, orgID, envID, bundleDigest)

	// Create run
	createReq, _ := http.NewRequest("POST", server.URL+"/v1/workflows/inspector-workflow/runs", strings.NewReader(`{"environment":"staging","input":{"val":"sse-stream-test"}}`))
	createReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	createReq.Header.Set("Idempotency-Key", "sse-run-001")
	createReq.Header.Set("Content-Type", "application/json")
	createRes, _ := http.DefaultClient.Do(createReq)
	var runResp execution.RunDTO
	_ = json.NewDecoder(createRes.Body).Decode(&runResp)
	createRes.Body.Close()

	// Advance state: claim and start task (creates sequences 2 and 3)
	pollBody, _ := json.Marshal(worker.PollRequestDTO{
		ProtocolVersion:   worker.ProtocolVersion,
		RequestID:         "p-sse",
		WorkerID:          sessionCtx.WorkerID,
		SessionID:         sessionCtx.SessionID,
		AvailableSlots:    1,
		DeploymentDigests: []string{bundleDigest},
		Pool:              "default",
	})
	pollReq, _ := http.NewRequest("POST", server.URL+"/worker/v1/poll", bytes.NewReader(pollBody))
	pollReq.Header.Set("Authorization", "Bearer "+sessionToken)
	pollReq.Header.Set("Content-Type", "application/json")
	pollRes, _ := http.DefaultClient.Do(pollReq)
	var pollResp worker.PollResponseDTO
	_ = json.NewDecoder(pollRes.Body).Decode(&pollResp)
	pollRes.Body.Close()
	as := pollResp.Assignments[0]

	startReq, _ := http.NewRequest("POST", server.URL+"/worker/v1/start", strings.NewReader(
		fmt.Sprintf(`{"protocolVersion":1,"requestId":"s-sse","workerId":"%s","sessionId":"%s","attemptId":"%s","ownershipEpoch":%d}`, sessionCtx.WorkerID, sessionCtx.SessionID, as.AttemptID, as.OwnershipEpoch),
	))
	startReq.Header.Set("Authorization", "Bearer "+sessionToken)
	startReq.Header.Set("Content-Type", "application/json")
	startRes, _ := http.DefaultClient.Do(startReq)
	startRes.Body.Close()

	// Connect SSE client with Last-Event-ID: 1 (should catch up sequences 2 and 3)
	sseReq, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+runResp.ID+"/stream", nil)
	sseReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	sseReq.Header.Set("Last-Event-ID", "1")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sseReq = sseReq.WithContext(ctx)

	sseRes, err := http.DefaultClient.Do(sseReq)
	if err != nil || sseRes.StatusCode != http.StatusOK {
		t.Fatalf("SSE connect failed: %v (status %d)", err, sseRes.StatusCode)
	}
	defer sseRes.Body.Close()

	if ct := sseRes.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("expected text/event-stream, got %s", ct)
	}

	reader := bufio.NewReader(sseRes.Body)

	// Read catch-up events
	receivedEvents := make([]string, 0)
	readChan := make(chan string, 10)
	go func() {
		for {
			line, rErr := reader.ReadString('\n')
			if rErr != nil {
				return
			}
			if strings.HasPrefix(line, "event:") {
				readChan <- strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			}
		}
	}()

	// Expect attempt.claimed and attempt.started within 3 seconds
	timeout := time.After(3 * time.Second)
	for len(receivedEvents) < 2 {
		select {
		case ev := <-readChan:
			receivedEvents = append(receivedEvents, ev)
		case <-timeout:
			t.Fatalf("timed out waiting for catch-up events; got %v", receivedEvents)
		}
	}

	if receivedEvents[0] != "attempt.claimed" || receivedEvents[1] != "attempt.started" {
		t.Fatalf("unexpected catch-up events: %v", receivedEvents)
	}

	// Now test real-time wakeup: Complete task while stream is open
	compPayload := worker.CompleteRequestDTO{
		ProtocolVersion: 1,
		RequestID:       "c-sse",
		WorkerID:        sessionCtx.WorkerID,
		SessionID:       sessionCtx.SessionID,
		AttemptID:       as.AttemptID,
		OwnershipEpoch:  as.OwnershipEpoch,
		Outcome:         "SUCCEEDED",
		Output:          map[string]any{"val": "live-done"},
	}
	digest, _ := worker.CanonicalCompletionDigest(&compPayload)
	compPayload.ResultDigest = digest
	cb, _ := json.Marshal(compPayload)
	compReq, _ := http.NewRequest("POST", server.URL+"/worker/v1/complete", bytes.NewReader(cb))
	compReq.Header.Set("Authorization", "Bearer "+sessionToken)
	compReq.Header.Set("Content-Type", "application/json")
	compRes, _ := http.DefaultClient.Do(compReq)
	compRes.Body.Close()

	// Expect attempt.completed and run.completed delivered live without reconnecting
	liveTimeout := time.After(3 * time.Second)
	for len(receivedEvents) < 4 {
		select {
		case ev := <-readChan:
			receivedEvents = append(receivedEvents, ev)
		case <-liveTimeout:
			t.Fatalf("timed out waiting for live broadcast events; got %v", receivedEvents)
		}
	}

	if receivedEvents[2] != "attempt.completed" || receivedEvents[3] != "run.completed" {
		t.Fatalf("unexpected live events: %v", receivedEvents)
	}
}

func TestRunInspectorSSERetentionGapResync(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer server.Close()

	setupInspectorWorkflow(t, tc, server, orgID, envID, adminKey)

	createReq, _ := http.NewRequest("POST", server.URL+"/v1/workflows/inspector-workflow/runs", strings.NewReader(`{"environment":"staging","input":{"val":"gap-test"}}`))
	createReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	createReq.Header.Set("Idempotency-Key", "retention-gap-run")
	createReq.Header.Set("Content-Type", "application/json")
	createRes, _ := http.DefaultClient.Do(createReq)
	var runResp execution.RunDTO
	_ = json.NewDecoder(createRes.Body).Decode(&runResp)
	createRes.Body.Close()

	// Simulate event retention pruning: bump min event sequence in run_events to 10
	_ = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE run_events SET sequence = 10 WHERE run_id = $1::uuid AND organization_id = $2::uuid`, runResp.ID, orgID)
		return err
	})

	// Client reconnects with Last-Event-ID: 2 (which was pruned by retention)
	sseReq, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+runResp.ID+"/stream", nil)
	sseReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	sseReq.Header.Set("Last-Event-ID", "2")

	sseRes, err := http.DefaultClient.Do(sseReq)
	if err != nil || sseRes.StatusCode != http.StatusOK {
		t.Fatalf("SSE request failed: %v", err)
	}
	defer sseRes.Body.Close()

	bodyBytes, _ := io.ReadAll(sseRes.Body)
	bodyStr := string(bodyBytes)

	if !strings.Contains(bodyStr, "event: resync") {
		t.Fatalf("expected 'event: resync' on retention gap, got:\n%s", bodyStr)
	}
	if !strings.Contains(bodyStr, "RETENTION_GAP") {
		t.Fatalf("expected RETENTION_GAP in resync payload, got:\n%s", bodyStr)
	}
}

func TestRunInspectorBoundedTaskLogs(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer server.Close()

	_, bundleDigest := setupInspectorWorkflow(t, tc, server, orgID, envID, adminKey)
	sessionCtx, sessionToken := enrollTestWorker(t, tc, server, orgID, envID, bundleDigest)

	// Create run
	createReq, _ := http.NewRequest("POST", server.URL+"/v1/workflows/inspector-workflow/runs", strings.NewReader(`{"environment":"staging","input":{"val":"logs-test"}}`))
	createReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	createReq.Header.Set("Idempotency-Key", "task-logs-run")
	createReq.Header.Set("Content-Type", "application/json")
	createRes, _ := http.DefaultClient.Do(createReq)
	var runResp execution.RunDTO
	_ = json.NewDecoder(createRes.Body).Decode(&runResp)
	createRes.Body.Close()

	// Worker polls and claims task
	pollBody, _ := json.Marshal(worker.PollRequestDTO{
		ProtocolVersion:   worker.ProtocolVersion,
		RequestID:         "p-log",
		WorkerID:          sessionCtx.WorkerID,
		SessionID:         sessionCtx.SessionID,
		AvailableSlots:    1,
		DeploymentDigests: []string{bundleDigest},
		Pool:              "default",
	})
	pollReq, _ := http.NewRequest("POST", server.URL+"/worker/v1/poll", bytes.NewReader(pollBody))
	pollReq.Header.Set("Authorization", "Bearer "+sessionToken)
	pollReq.Header.Set("Content-Type", "application/json")
	pollRes, _ := http.DefaultClient.Do(pollReq)
	var pollResp worker.PollResponseDTO
	_ = json.NewDecoder(pollRes.Body).Decode(&pollResp)
	pollRes.Body.Close()
	as := pollResp.Assignments[0]

	// Send valid log batch
	logBatch := worker.LogBatchRequestDTO{
		ProtocolVersion: 1,
		RequestID:       "log-req-1",
		WorkerID:        sessionCtx.WorkerID,
		SessionID:       sessionCtx.SessionID,
		AttemptID:       as.AttemptID,
		Records: []worker.LogRecordDTO{
			{Sequence: 1, Level: "info", Message: "Starting database migration check", Timestamp: time.Now().UTC().Format(time.RFC3339Nano)},
			{Sequence: 2, Level: "warn", Message: "Index building takes longer than usual", Timestamp: time.Now().UTC().Format(time.RFC3339Nano)},
			{Sequence: 3, Level: "info", Message: "Migration completed successfully", Timestamp: time.Now().UTC().Format(time.RFC3339Nano)},
		},
	}
	logBytes, _ := json.Marshal(logBatch)
	lReq, _ := http.NewRequest("POST", server.URL+"/worker/v1/logs", bytes.NewReader(logBytes))
	lReq.Header.Set("Authorization", "Bearer "+sessionToken)
	lReq.Header.Set("Content-Type", "application/json")
	lRes, err := http.DefaultClient.Do(lReq)
	if err != nil || lRes.StatusCode != http.StatusOK {
		t.Fatalf("log ingestion failed: %v (status %d)", err, lRes.StatusCode)
	}
	lRes.Body.Close()

	// Query logs via GET /v1/runs/{id}/logs
	qReq, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+runResp.ID+"/logs", nil)
	qReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	qRes, err := http.DefaultClient.Do(qReq)
	if err != nil || qRes.StatusCode != http.StatusOK {
		t.Fatalf("query logs failed: %v (status %d)", err, qRes.StatusCode)
	}
	var logsResp execution.RunLogsResponseDTO
	_ = json.NewDecoder(qRes.Body).Decode(&logsResp)
	qRes.Body.Close()

	if len(logsResp.Items) != 3 {
		t.Fatalf("expected 3 log items, got %d", len(logsResp.Items))
	}
	if logsResp.Items[0].Message != "Starting database migration check" {
		t.Fatalf("unexpected first message: %s", logsResp.Items[0].Message)
	}
	if logsResp.Items[1].Level != "warn" {
		t.Fatalf("expected level warn, got %s", logsResp.Items[1].Level)
	}

	// Test idempotency: send duplicate sequence 2 and 3
	lReqDup, _ := http.NewRequest("POST", server.URL+"/worker/v1/logs", bytes.NewReader(logBytes))
	lReqDup.Header.Set("Authorization", "Bearer "+sessionToken)
	lReqDup.Header.Set("Content-Type", "application/json")
	lResDup, err := http.DefaultClient.Do(lReqDup)
	if err != nil || lResDup.StatusCode != http.StatusOK {
		t.Fatalf("duplicate log ingestion failed: %v", err)
	}
	lResDup.Body.Close()

	// Verify count is still exactly 3 (no duplicates inserted)
	qRes2, _ := http.DefaultClient.Do(qReq)
	var logsResp2 execution.RunLogsResponseDTO
	_ = json.NewDecoder(qRes2.Body).Decode(&logsResp2)
	qRes2.Body.Close()
	if len(logsResp2.Items) != 3 {
		t.Fatalf("expected exactly 3 items after idempotent duplicate batch, got %d", len(logsResp2.Items))
	}

	// Test unauthorized log submission: different session ID trying to log to attempt
	unauthBatch := logBatch
	unauthBatch.SessionID = "00000000-0000-0000-0000-000000000000"
	uBytes, _ := json.Marshal(unauthBatch)
	uReq, _ := http.NewRequest("POST", server.URL+"/worker/v1/logs", bytes.NewReader(uBytes))
	uReq.Header.Set("Authorization", "Bearer "+sessionToken)
	uReq.Header.Set("Content-Type", "application/json")
	uRes, _ := http.DefaultClient.Do(uReq)
	uRes.Body.Close()
	if uRes.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 UNAUTHORIZED for mismatched worker session, got %d", uRes.StatusCode)
	}
}
