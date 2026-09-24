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
	"github.com/Ryanakml/Deadbolt/internal/deployment"
	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

func setupInspectorWorkflow(t *testing.T, tc *tenantTestContext, server *httptest.Server, orgID, envID string, adminKey *tenant.GeneratedKey) (string, string) {
	t.Helper()
	tempDir := t.TempDir()
	bundleDigest := writeAgentBundle(t, tempDir, "linux", "amd64")

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

	// 4. Failed run attempt: verify nested attempt error and top-level error redaction
	createFailReq, _ := http.NewRequest("POST", server.URL+"/v1/workflows/inspector-workflow/runs", strings.NewReader(`{"environment":"staging","input":{"val":"fail-input-secret"}}`))
	createFailReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	createFailReq.Header.Set("Idempotency-Key", "payload-boundary-fail-run")
	createFailReq.Header.Set("Content-Type", "application/json")
	createFailRes, err := http.DefaultClient.Do(createFailReq)
	if err != nil || createFailRes.StatusCode != http.StatusAccepted {
		t.Fatalf("create fail run failed: %v", err)
	}
	var failRunResp execution.RunDTO
	_ = json.NewDecoder(createFailRes.Body).Decode(&failRunResp)
	createFailRes.Body.Close()

	pollFailBody, _ := json.Marshal(worker.PollRequestDTO{
		ProtocolVersion:   worker.ProtocolVersion,
		RequestID:         "p-fail-poll",
		WorkerID:          sessionCtx.WorkerID,
		SessionID:         sessionCtx.SessionID,
		AvailableSlots:    1,
		DeploymentDigests: []string{bundleDigest},
		Pool:              "default",
	})
	pollFailReq, _ := http.NewRequest("POST", server.URL+"/worker/v1/poll", bytes.NewReader(pollFailBody))
	pollFailReq.Header.Set("Authorization", "Bearer "+sessionToken)
	pollFailReq.Header.Set("Content-Type", "application/json")
	pollFailRes, err := http.DefaultClient.Do(pollFailReq)
	if err != nil || pollFailRes.StatusCode != http.StatusOK {
		t.Fatalf("poll for failed run failed: %v", err)
	}
	var pollFailResp worker.PollResponseDTO
	_ = json.NewDecoder(pollFailRes.Body).Decode(&pollFailResp)
	pollFailRes.Body.Close()
	if len(pollFailResp.Assignments) == 0 {
		t.Fatalf("expected assignment for failed run")
	}
	asFail := pollFailResp.Assignments[0]

	startFailReq, _ := http.NewRequest("POST", server.URL+"/worker/v1/start", strings.NewReader(
		fmt.Sprintf(`{"protocolVersion":1,"requestId":"s-fail-1","workerId":"%s","sessionId":"%s","attemptId":"%s","ownershipEpoch":%d}`, sessionCtx.WorkerID, sessionCtx.SessionID, asFail.AttemptID, asFail.OwnershipEpoch),
	))
	startFailReq.Header.Set("Authorization", "Bearer "+sessionToken)
	startFailReq.Header.Set("Content-Type", "application/json")
	startFailRes, err := http.DefaultClient.Do(startFailReq)
	if err != nil || startFailRes.StatusCode != http.StatusOK {
		t.Fatalf("start for failed run failed: %v", err)
	}
	startFailRes.Body.Close()

	secretErrMsg := "secret_db_pass_9988: failed to connect to internal db"
	compFailPayload := worker.CompleteRequestDTO{
		ProtocolVersion: 1,
		RequestID:       "c-fail-1",
		WorkerID:        sessionCtx.WorkerID,
		SessionID:       sessionCtx.SessionID,
		AttemptID:       asFail.AttemptID,
		OwnershipEpoch:  asFail.OwnershipEpoch,
		Outcome:         "FAILED",
		Error: &worker.TaskErrorDTO{
			Code:    "ERR_INTERNAL_SECRET",
			Message: secretErrMsg,
		},
	}
	failDigest, _ := worker.CanonicalCompletionDigest(&compFailPayload)
	compFailPayload.ResultDigest = failDigest
	cfb, _ := json.Marshal(compFailPayload)
	compFailReq, _ := http.NewRequest("POST", server.URL+"/worker/v1/complete", bytes.NewReader(cfb))
	compFailReq.Header.Set("Authorization", "Bearer "+sessionToken)
	compFailReq.Header.Set("Content-Type", "application/json")
	compFailRes, err := http.DefaultClient.Do(compFailReq)
	if err != nil || compFailRes.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(compFailRes.Body)
		t.Fatalf("complete for failed run failed: %v (status %d): %s", err, compFailRes.StatusCode, string(b))
	}
	compFailRes.Body.Close()

	// 4a. Restricted viewer inspects failed run snapshot
	reqFailRestricted, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+failRunResp.ID, nil)
	reqFailRestricted.Header.Set("Authorization", "Bearer "+restrictedKey.PlaintextKey)
	resFailRestricted, err := http.DefaultClient.Do(reqFailRestricted)
	if err != nil || resFailRestricted.StatusCode != http.StatusOK {
		t.Fatalf("restricted snapshot failed: %v", err)
	}
	var failSnapRestricted execution.RunSnapshotDTO
	_ = json.NewDecoder(resFailRestricted.Body).Decode(&failSnapRestricted)
	resFailRestricted.Body.Close()

	if failSnapRestricted.Error != nil {
		t.Fatalf("security violation: expected nil top-level error for restricted viewer, got %v", failSnapRestricted.Error)
	}
	if len(failSnapRestricted.Steps) == 0 || len(failSnapRestricted.Steps[0].Attempts) == 0 {
		t.Fatalf("expected steps and attempts in snapshot")
	}
	for _, step := range failSnapRestricted.Steps {
		for _, attempt := range step.Attempts {
			if attempt.Error != nil {
				t.Fatalf("security violation: expected nil nested attempt.Error for restricted viewer, got %v", attempt.Error)
			}
		}
	}

	// 4b. Restricted viewer queries events of failed run: no error leak
	reqEventsRestricted, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+failRunResp.ID+"/events", nil)
	reqEventsRestricted.Header.Set("Authorization", "Bearer "+restrictedKey.PlaintextKey)
	resEventsRestricted, err := http.DefaultClient.Do(reqEventsRestricted)
	if err != nil || resEventsRestricted.StatusCode != http.StatusOK {
		t.Fatalf("restricted events failed: %v", err)
	}
	var failEventsRestricted execution.RunEventsResponseDTO
	_ = json.NewDecoder(resEventsRestricted.Body).Decode(&failEventsRestricted)
	resEventsRestricted.Body.Close()

	for _, ev := range failEventsRestricted.Events {
		if ev.Payload != nil {
			if m, ok := ev.Payload.(map[string]any); ok {
				if _, hasErr := m["error"]; hasErr {
					t.Fatalf("security violation: event %s contains unredacted error payload for restricted viewer", ev.ID)
				}
			}
		}
	}

	// 4c. Privileged caller with payload:read inspects failed run snapshot: error is present
	reqFailAdmin, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+failRunResp.ID, nil)
	reqFailAdmin.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	resFailAdmin, err := http.DefaultClient.Do(reqFailAdmin)
	if err != nil || resFailAdmin.StatusCode != http.StatusOK {
		t.Fatalf("admin snapshot failed: %v", err)
	}
	var failSnapAdmin execution.RunSnapshotDTO
	_ = json.NewDecoder(resFailAdmin.Body).Decode(&failSnapAdmin)
	resFailAdmin.Body.Close()

	if failSnapAdmin.Steps[0].Attempts[0].Error == nil {
		t.Fatal("expected non-nil attempt error for privileged caller with payload:read")
	}
	errStr, _ := json.Marshal(failSnapAdmin.Steps[0].Attempts[0].Error)
	if !strings.Contains(string(errStr), secretErrMsg) {
		t.Fatalf("expected admin attempt error to contain secret message, got: %s", string(errStr))
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
		b, _ := io.ReadAll(lRes.Body)
		t.Fatalf("log ingestion failed: %v (status %d): %s", err, lRes.StatusCode, string(b))
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

func setupTwoStepWorkflow(t *testing.T, tc *tenantTestContext, server *httptest.Server, orgID, envID string, adminKey *tenant.GeneratedKey) (string, string) {
	t.Helper()
	tempDir := t.TempDir()
	bundleDigest := writeAgentBundle(t, tempDir, "linux", "amd64")

	schema := map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"val": map[string]any{"type": "string"}},
		"required":             []any{"val"},
		"additionalProperties": false,
	}
	tasks := []map[string]any{
		{
			"name":                "task-step1",
			"entrypoint":          "tasks/agent.mjs",
			"timeoutMs":           30000,
			"recovery":            "idempotent",
			"idempotencyWindowMs": 305000,
			"inputSchema":         schema,
			"outputSchema":        schema,
		},
		{
			"name":                "task-step2",
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
			"name":            "two-step-workflow",
			"inputSchema":     schema,
			"outputSchema":    schema,
			"nodes": []map[string]any{
				{
					"id":    "step-1",
					"type":  "task",
					"task":  "task-step1",
					"after": []any{},
					"input": map[string]any{
						"val": map[string]any{"$ref": "run.input", "pointer": "/val"},
					},
				},
				{
					"id":    "step-2",
					"type":  "task",
					"task":  "task-step2",
					"after": []any{"step-1"},
					"input": map[string]any{
						"val": map[string]any{"$ref": "step.output", "stepId": "step-1", "pointer": "/val"},
					},
				},
			},
			"output": map[string]any{
				"val": map[string]any{"$ref": "step.output", "stepId": "step-2", "pointer": "/val"},
			},
		},
	}

	manifestBytes := createLifecycleManifest(bundleDigest, tasks, workflows)
	depID := registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "two-step-workflow", manifestBytes)
	return depID, bundleDigest
}

func TestRunInspectorStableLogKeysetPagination(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer server.Close()

	_, bundleDigest := setupTwoStepWorkflow(t, tc, server, orgID, envID, adminKey)
	sessionCtx, sessionToken := enrollTestWorker(t, tc, server, orgID, envID, bundleDigest)

	// Create run
	createReq, _ := http.NewRequest("POST", server.URL+"/v1/workflows/two-step-workflow/runs", strings.NewReader(`{"environment":"staging","input":{"val":"pagination-test"}}`))
	createReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	createReq.Header.Set("Idempotency-Key", "keyset-pagination-run")
	createReq.Header.Set("Content-Type", "application/json")
	createRes, err := http.DefaultClient.Do(createReq)
	if err != nil || createRes.StatusCode != http.StatusAccepted {
		t.Fatalf("create run failed: %v", err)
	}
	var runResp execution.RunDTO
	_ = json.NewDecoder(createRes.Body).Decode(&runResp)
	createRes.Body.Close()

	// 1. Worker polls and claims step-1
	pollBody, _ := json.Marshal(worker.PollRequestDTO{
		ProtocolVersion:   worker.ProtocolVersion,
		RequestID:         "poll-step1",
		WorkerID:          sessionCtx.WorkerID,
		SessionID:         sessionCtx.SessionID,
		AvailableSlots:    1,
		DeploymentDigests: []string{bundleDigest},
		Pool:              "default",
	})
	pReq, _ := http.NewRequest("POST", server.URL+"/worker/v1/poll", bytes.NewReader(pollBody))
	pReq.Header.Set("Authorization", "Bearer "+sessionToken)
	pReq.Header.Set("Content-Type", "application/json")
	pRes, _ := http.DefaultClient.Do(pReq)
	var pollResp worker.PollResponseDTO
	_ = json.NewDecoder(pRes.Body).Decode(&pollResp)
	pRes.Body.Close()
	if len(pollResp.Assignments) != 1 {
		t.Fatalf("expected assignment for step-1")
	}
	as1 := pollResp.Assignments[0]

	// Send 3 logs for step-1 (sequences 1, 2, 3)
	batch1 := worker.LogBatchRequestDTO{
		ProtocolVersion: 1,
		RequestID:       "log-batch-step1",
		WorkerID:        sessionCtx.WorkerID,
		SessionID:       sessionCtx.SessionID,
		AttemptID:       as1.AttemptID,
		Records: []worker.LogRecordDTO{
			{Sequence: 1, Level: "info", Message: "step1 log A", Timestamp: time.Now().UTC().Format(time.RFC3339Nano)},
			{Sequence: 2, Level: "info", Message: "step1 log B", Timestamp: time.Now().UTC().Format(time.RFC3339Nano)},
			{Sequence: 3, Level: "warn", Message: "step1 log C", Timestamp: time.Now().UTC().Format(time.RFC3339Nano)},
		},
	}
	b1Bytes, _ := json.Marshal(batch1)
	lReq1, _ := http.NewRequest("POST", server.URL+"/worker/v1/logs", bytes.NewReader(b1Bytes))
	lReq1.Header.Set("Authorization", "Bearer "+sessionToken)
	lReq1.Header.Set("Content-Type", "application/json")
	lRes1, err := http.DefaultClient.Do(lReq1)
	if err != nil || lRes1.StatusCode != http.StatusOK {
		t.Fatalf("ingest step1 logs failed: %v", err)
	}
	lRes1.Body.Close()

	// Complete step-1
	startReq1, _ := http.NewRequest("POST", server.URL+"/worker/v1/start", strings.NewReader(
		fmt.Sprintf(`{"protocolVersion":1,"requestId":"s-p1","workerId":"%s","sessionId":"%s","attemptId":"%s","ownershipEpoch":%d}`, sessionCtx.WorkerID, sessionCtx.SessionID, as1.AttemptID, as1.OwnershipEpoch),
	))
	startReq1.Header.Set("Authorization", "Bearer "+sessionToken)
	startReq1.Header.Set("Content-Type", "application/json")
	sRes1, _ := http.DefaultClient.Do(startReq1)
	sRes1.Body.Close()

	compPayload1 := worker.CompleteRequestDTO{
		ProtocolVersion: 1,
		RequestID:       "c-step1",
		WorkerID:        sessionCtx.WorkerID,
		SessionID:       sessionCtx.SessionID,
		AttemptID:       as1.AttemptID,
		OwnershipEpoch:  as1.OwnershipEpoch,
		Outcome:         "SUCCEEDED",
		Output:          map[string]any{"val": "data-from-step1"},
	}
	d1, _ := worker.CanonicalCompletionDigest(&compPayload1)
	compPayload1.ResultDigest = d1
	c1Bytes, _ := json.Marshal(compPayload1)
	cReq1, _ := http.NewRequest("POST", server.URL+"/worker/v1/complete", bytes.NewReader(c1Bytes))
	cReq1.Header.Set("Authorization", "Bearer "+sessionToken)
	cReq1.Header.Set("Content-Type", "application/json")
	cRes1, _ := http.DefaultClient.Do(cReq1)
	cRes1.Body.Close()

	// 2. Worker polls and claims step-2
	pollBody2, _ := json.Marshal(worker.PollRequestDTO{
		ProtocolVersion:   worker.ProtocolVersion,
		RequestID:         "poll-step2",
		WorkerID:          sessionCtx.WorkerID,
		SessionID:         sessionCtx.SessionID,
		AvailableSlots:    1,
		DeploymentDigests: []string{bundleDigest},
		Pool:              "default",
	})
	pReq2, _ := http.NewRequest("POST", server.URL+"/worker/v1/poll", bytes.NewReader(pollBody2))
	pReq2.Header.Set("Authorization", "Bearer "+sessionToken)
	pReq2.Header.Set("Content-Type", "application/json")
	pRes2, _ := http.DefaultClient.Do(pReq2)
	var pollResp2 worker.PollResponseDTO
	_ = json.NewDecoder(pRes2.Body).Decode(&pollResp2)
	pRes2.Body.Close()
	if len(pollResp2.Assignments) != 1 {
		t.Fatalf("expected assignment for step-2")
	}
	as2 := pollResp2.Assignments[0]

	// Send 3 logs for step-2 (sequences 1, 2, 3 - deliberate overlap with step-1!)
	batch2 := worker.LogBatchRequestDTO{
		ProtocolVersion: 1,
		RequestID:       "log-batch-step2",
		WorkerID:        sessionCtx.WorkerID,
		SessionID:       sessionCtx.SessionID,
		AttemptID:       as2.AttemptID,
		Records: []worker.LogRecordDTO{
			{Sequence: 1, Level: "info", Message: "step2 log A", Timestamp: time.Now().UTC().Format(time.RFC3339Nano)},
			{Sequence: 2, Level: "info", Message: "step2 log B", Timestamp: time.Now().UTC().Format(time.RFC3339Nano)},
			{Sequence: 3, Level: "error", Message: "step2 log C", Timestamp: time.Now().UTC().Format(time.RFC3339Nano)},
		},
	}
	b2Bytes, _ := json.Marshal(batch2)
	lReq2, _ := http.NewRequest("POST", server.URL+"/worker/v1/logs", bytes.NewReader(b2Bytes))
	lReq2.Header.Set("Authorization", "Bearer "+sessionToken)
	lReq2.Header.Set("Content-Type", "application/json")
	lRes2, err := http.DefaultClient.Do(lReq2)
	if err != nil || lRes2.StatusCode != http.StatusOK {
		t.Fatalf("ingest step2 logs failed: %v", err)
	}
	lRes2.Body.Close()

	// 3. Paginate logs with limit=2 using keyset cursor
	var allItems []execution.TaskLogRecordDTO
	var cursor *string
	pageCount := 0

	for {
		pageCount++
		if pageCount > 10 {
			t.Fatal("infinite loop detected in log pagination")
		}

		urlStr := fmt.Sprintf("%s/v1/runs/%s/logs?limit=2", server.URL, runResp.ID)
		if cursor != nil && *cursor != "" {
			urlStr += "&cursor=" + *cursor
		}

		qReq, _ := http.NewRequest("GET", urlStr, nil)
		qReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
		qRes, err := http.DefaultClient.Do(qReq)
		if err != nil || qRes.StatusCode != http.StatusOK {
			t.Fatalf("query page %d failed: %v", pageCount, err)
		}
		var pageResp execution.RunLogsResponseDTO
		_ = json.NewDecoder(qRes.Body).Decode(&pageResp)
		qRes.Body.Close()

		if len(pageResp.Items) == 0 {
			break
		}
		allItems = append(allItems, pageResp.Items...)
		if pageResp.NextCursor == nil || *pageResp.NextCursor == "" {
			break
		}
		cursor = pageResp.NextCursor
	}

	if len(allItems) != 6 {
		t.Fatalf("expected 6 total log items across overlapping sequences, got %d", len(allItems))
	}

	// Verify all IDs are unique
	seenIDs := make(map[string]bool)
	for _, it := range allItems {
		if seenIDs[it.ID] {
			t.Fatalf("duplicate log item returned across pagination: %s", it.ID)
		}
		seenIDs[it.ID] = true
	}

	// Verify messages order
	expectedMessages := []string{
		"step1 log A",
		"step1 log B",
		"step1 log C",
		"step2 log A",
		"step2 log B",
		"step2 log C",
	}
	for i, it := range allItems {
		if it.Message != expectedMessages[i] {
			t.Errorf("item %d: expected message %q, got %q", i, expectedMessages[i], it.Message)
		}
	}
}

func TestRunInspectorLogBoundsAndDroppedMetric(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer server.Close()

	_, bundleDigest := setupInspectorWorkflow(t, tc, server, orgID, envID, adminKey)
	sessionCtx, sessionToken := enrollTestWorker(t, tc, server, orgID, envID, bundleDigest)

	createReq, _ := http.NewRequest("POST", server.URL+"/v1/workflows/inspector-workflow/runs", strings.NewReader(`{"environment":"staging","input":{"val":"bounds-test"}}`))
	createReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	createReq.Header.Set("Idempotency-Key", "log-bounds-run")
	createReq.Header.Set("Content-Type", "application/json")
	createRes, _ := http.DefaultClient.Do(createReq)
	var runResp execution.RunDTO
	_ = json.NewDecoder(createRes.Body).Decode(&runResp)
	createRes.Body.Close()

	pollBody, _ := json.Marshal(worker.PollRequestDTO{
		ProtocolVersion:   worker.ProtocolVersion,
		RequestID:         "p-bounds",
		WorkerID:          sessionCtx.WorkerID,
		SessionID:         sessionCtx.SessionID,
		AvailableSlots:    1,
		DeploymentDigests: []string{bundleDigest},
		Pool:              "default",
	})
	pReq, _ := http.NewRequest("POST", server.URL+"/worker/v1/poll", bytes.NewReader(pollBody))
	pReq.Header.Set("Authorization", "Bearer "+sessionToken)
	pReq.Header.Set("Content-Type", "application/json")
	pRes, _ := http.DefaultClient.Do(pReq)
	var pollResp worker.PollResponseDTO
	_ = json.NewDecoder(pRes.Body).Decode(&pollResp)
	pRes.Body.Close()
	as := pollResp.Assignments[0]

	// 1. Exact 16 KiB message (16384 bytes) -> Accepted
	msg16k := strings.Repeat("A", 16*1024)
	batch1 := worker.LogBatchRequestDTO{
		ProtocolVersion: 1,
		RequestID:       "log-req-16k",
		WorkerID:        sessionCtx.WorkerID,
		SessionID:       sessionCtx.SessionID,
		AttemptID:       as.AttemptID,
		Records: []worker.LogRecordDTO{
			{Sequence: 1, Level: "info", Message: msg16k, Timestamp: time.Now().UTC().Format(time.RFC3339Nano)},
		},
	}
	b1Bytes, _ := json.Marshal(batch1)
	lReq1, _ := http.NewRequest("POST", server.URL+"/worker/v1/logs", bytes.NewReader(b1Bytes))
	lReq1.Header.Set("Authorization", "Bearer "+sessionToken)
	lReq1.Header.Set("Content-Type", "application/json")
	lRes1, err := http.DefaultClient.Do(lReq1)
	if err != nil || lRes1.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(lRes1.Body)
		lRes1.Body.Close()
		t.Fatalf("log 16k failed: %v status=%d body=%s", err, lRes1.StatusCode, body)
	}
	var ack1 worker.AckResponseDTO
	_ = json.NewDecoder(lRes1.Body).Decode(&ack1)
	lRes1.Body.Close()

	if !ack1.Accepted || ack1.DroppedCount != 0 || ack1.BudgetExhausted {
		t.Fatalf("expected accepted with 0 dropped, got %+v", ack1)
	}

	// 2. 16 KiB + 1 byte message (16385 bytes) -> Dropped
	msg16kPlus1 := strings.Repeat("B", 16*1024+1)
	batch2 := worker.LogBatchRequestDTO{
		ProtocolVersion: 1,
		RequestID:       "log-req-16k-plus-1",
		WorkerID:        sessionCtx.WorkerID,
		SessionID:       sessionCtx.SessionID,
		AttemptID:       as.AttemptID,
		Records: []worker.LogRecordDTO{
			{Sequence: 2, Level: "info", Message: msg16kPlus1, Timestamp: time.Now().UTC().Format(time.RFC3339Nano)},
		},
	}
	b2Bytes, _ := json.Marshal(batch2)
	lReq2, _ := http.NewRequest("POST", server.URL+"/worker/v1/logs", bytes.NewReader(b2Bytes))
	lReq2.Header.Set("Authorization", "Bearer "+sessionToken)
	lReq2.Header.Set("Content-Type", "application/json")
	lRes2, err := http.DefaultClient.Do(lReq2)
	if err != nil || lRes2.StatusCode != http.StatusOK {
		t.Fatalf("log 16k+1 request failed: %v", err)
	}
	var ack2 worker.AckResponseDTO
	_ = json.NewDecoder(lRes2.Body).Decode(&ack2)
	lRes2.Body.Close()

	if !ack2.Accepted || ack2.DroppedCount != 1 || ack2.BudgetExhausted {
		t.Fatalf("expected droppedCount=1 for oversized line, got %+v", ack2)
	}

	// 3. Fill up to 1 MiB cumulative attempt budget
	// Currently at 16 KiB (sequence 1). Need 63 more 16 KiB records to hit exactly 1 MiB (64 * 16384 = 1048576)
	var recordsToFill []worker.LogRecordDTO
	for seq := 3; seq <= 65; seq++ {
		recordsToFill = append(recordsToFill, worker.LogRecordDTO{
			Sequence:  int64(seq),
			Level:     "info",
			Message:   msg16k,
			Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		})
	}
	// Send in batches of 9 records
	for i := 0; i < len(recordsToFill); i += 9 {
		end := i + 9
		if end > len(recordsToFill) {
			end = len(recordsToFill)
		}
		fillBatch := worker.LogBatchRequestDTO{
			ProtocolVersion: 1,
			RequestID:       fmt.Sprintf("log-req-fill-%d", i),
			WorkerID:        sessionCtx.WorkerID,
			SessionID:       sessionCtx.SessionID,
			AttemptID:       as.AttemptID,
			Records:         recordsToFill[i:end],
		}
		fbBytes, _ := json.Marshal(fillBatch)
		fReq, _ := http.NewRequest("POST", server.URL+"/worker/v1/logs", bytes.NewReader(fbBytes))
		fReq.Header.Set("Authorization", "Bearer "+sessionToken)
		fReq.Header.Set("Content-Type", "application/json")
		fRes, _ := http.DefaultClient.Do(fReq)
		fRes.Body.Close()
	}

	// Now cumulative size is at 1 MiB. Sending sequence 66 must hit budgetExhausted.
	overflowBatch := worker.LogBatchRequestDTO{
		ProtocolVersion: 1,
		RequestID:       "log-req-overflow",
		WorkerID:        sessionCtx.WorkerID,
		SessionID:       sessionCtx.SessionID,
		AttemptID:       as.AttemptID,
		Records: []worker.LogRecordDTO{
			{Sequence: 66, Level: "info", Message: "one byte over budget", Timestamp: time.Now().UTC().Format(time.RFC3339Nano)},
		},
	}
	obBytes, _ := json.Marshal(overflowBatch)
	oReq, _ := http.NewRequest("POST", server.URL+"/worker/v1/logs", bytes.NewReader(obBytes))
	oReq.Header.Set("Authorization", "Bearer "+sessionToken)
	oReq.Header.Set("Content-Type", "application/json")
	oRes, _ := http.DefaultClient.Do(oReq)
	var ackOverflow worker.AckResponseDTO
	_ = json.NewDecoder(oRes.Body).Decode(&ackOverflow)
	oRes.Body.Close()

	if !ackOverflow.BudgetExhausted || ackOverflow.DroppedCount != 1 {
		t.Fatalf("expected budgetExhausted=true and droppedCount=1, got %+v", ackOverflow)
	}

	// 4. Retry overflow batch (idempotency check)
	oReqRetry, _ := http.NewRequest("POST", server.URL+"/worker/v1/logs", bytes.NewReader(obBytes))
	oReqRetry.Header.Set("Authorization", "Bearer "+sessionToken)
	oReqRetry.Header.Set("Content-Type", "application/json")
	oResRetry, _ := http.DefaultClient.Do(oReqRetry)
	var ackRetry worker.AckResponseDTO
	_ = json.NewDecoder(oResRetry.Body).Decode(&ackRetry)
	oResRetry.Body.Close()

	if !ackRetry.Accepted || !ackRetry.BudgetExhausted || ackRetry.DroppedCount != 1 {
		t.Fatalf("expected idempotent response on retry, got %+v", ackRetry)
	}

	// The inspector's persisted state must make partial diagnostics observable;
	// the worker ACK alone is not enough once an operator opens the run later.
	logsReq, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+runResp.ID+"/logs?attemptId="+as.AttemptID, nil)
	logsReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	logsRes, err := http.DefaultClient.Do(logsReq)
	if err != nil {
		t.Fatalf("get persisted log drop state failed: %v", err)
	}
	if logsRes.StatusCode != http.StatusOK {
		logsRes.Body.Close()
		t.Fatalf("get persisted log drop state returned %d", logsRes.StatusCode)
	}
	var logsSnapshot execution.RunLogsResponseDTO
	_ = json.NewDecoder(logsRes.Body).Decode(&logsSnapshot)
	logsRes.Body.Close()
	if logsSnapshot.DroppedCount != 2 || !logsSnapshot.BudgetExhausted {
		t.Fatalf("expected persisted two-drop incomplete state, got %+v", logsSnapshot)
	}

	metricsRes, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatalf("get production metrics failed: %v", err)
	}
	metricsBody, _ := io.ReadAll(metricsRes.Body)
	metricsRes.Body.Close()
	if metricsRes.StatusCode != http.StatusOK {
		t.Fatalf("expected metrics status 200, got %d", metricsRes.StatusCode)
	}
	if !strings.Contains(string(metricsBody), "deadbolt_task_logs_dropped_total 2") {
		t.Fatalf("expected exactly two newly dropped logs in production metrics, got:\n%s", metricsBody)
	}

	// 5. Verify worker.Service DroppedLogsCount metric
	ws := worker.NewService(tc.pool, deployment.NewService(tc.pool, tc.service), execution.NewWorkerEngine(tc.pool, execution.NewEventHub()))
	directOversized := &worker.LogBatchRequestDTO{
		ProtocolVersion: 1,
		RequestID:       "direct-svc-oversized",
		WorkerID:        sessionCtx.WorkerID,
		SessionID:       sessionCtx.SessionID,
		AttemptID:       as.AttemptID,
		Records: []worker.LogRecordDTO{
			{Sequence: 999, Level: "error", Message: strings.Repeat("Z", 20000), Timestamp: time.Now().UTC().Format(time.RFC3339Nano)},
		},
	}
	directAck1, err := ws.RecordLogs(context.Background(), sessionCtx, directOversized)
	if err != nil || directAck1.DroppedCount != 1 {
		t.Fatalf("first direct oversized log was not dropped: ack=%+v err=%v", directAck1, err)
	}
	directAck2, err := ws.RecordLogs(context.Background(), sessionCtx, directOversized)
	if err != nil || directAck2.DroppedCount != 1 {
		t.Fatalf("retried direct oversized log did not return prior drop: ack=%+v err=%v", directAck2, err)
	}
	if ws.DroppedLogsCount() != 1 {
		t.Fatalf("expected retry-safe dropped metric count of 1, got %d", ws.DroppedLogsCount())
	}
}

func TestRunInspectorRealRetentionCleanupAndStatusDistinction(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer server.Close()

	_, bundleDigest := setupInspectorWorkflow(t, tc, server, orgID, envID, adminKey)
	sessionCtx, sessionToken := enrollTestWorker(t, tc, server, orgID, envID, bundleDigest)

	// Run 1: Created, claimed, and completed with SUCCEEDED, but NO logs ever recorded (logs_recorded = false)
	createReq1, _ := http.NewRequest("POST", server.URL+"/v1/workflows/inspector-workflow/runs", strings.NewReader(`{"environment":"staging","input":{"val":"no-logs"}}`))
	createReq1.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	createReq1.Header.Set("Idempotency-Key", "retention-no-logs-run")
	createReq1.Header.Set("Content-Type", "application/json")
	createRes1, _ := http.DefaultClient.Do(createReq1)
	var run1 execution.RunDTO
	_ = json.NewDecoder(createRes1.Body).Decode(&run1)
	createRes1.Body.Close()

	// Worker claims, starts, and completes Run 1
	pollBody1, _ := json.Marshal(worker.PollRequestDTO{
		ProtocolVersion:   worker.ProtocolVersion,
		RequestID:         "p-retention-1",
		WorkerID:          sessionCtx.WorkerID,
		SessionID:         sessionCtx.SessionID,
		AvailableSlots:    1,
		DeploymentDigests: []string{bundleDigest},
		Pool:              "default",
	})
	pReq1, _ := http.NewRequest("POST", server.URL+"/worker/v1/poll", bytes.NewReader(pollBody1))
	pReq1.Header.Set("Authorization", "Bearer "+sessionToken)
	pReq1.Header.Set("Content-Type", "application/json")
	pRes1, err := http.DefaultClient.Do(pReq1)
	if err != nil || pRes1.StatusCode != http.StatusOK {
		t.Fatalf("poll 1 failed: %v", err)
	}
	var pollResp1 worker.PollResponseDTO
	_ = json.NewDecoder(pRes1.Body).Decode(&pollResp1)
	pRes1.Body.Close()
	if len(pollResp1.Assignments) == 0 {
		t.Fatalf("expected assignment for run 1")
	}
	as1 := pollResp1.Assignments[0]

	startReq1, _ := http.NewRequest("POST", server.URL+"/worker/v1/start", strings.NewReader(
		fmt.Sprintf(`{"protocolVersion":1,"requestId":"s-ret-1","workerId":"%s","sessionId":"%s","attemptId":"%s","ownershipEpoch":%d}`, sessionCtx.WorkerID, sessionCtx.SessionID, as1.AttemptID, as1.OwnershipEpoch),
	))
	startReq1.Header.Set("Authorization", "Bearer "+sessionToken)
	startReq1.Header.Set("Content-Type", "application/json")
	sRes1, err := http.DefaultClient.Do(startReq1)
	if err != nil || sRes1.StatusCode != http.StatusOK {
		t.Fatalf("start 1 failed: %v", err)
	}
	sRes1.Body.Close()

	compPayload1 := worker.CompleteRequestDTO{
		ProtocolVersion: 1,
		RequestID:       "c-ret-1",
		WorkerID:        sessionCtx.WorkerID,
		SessionID:       sessionCtx.SessionID,
		AttemptID:       as1.AttemptID,
		OwnershipEpoch:  as1.OwnershipEpoch,
		Outcome:         "SUCCEEDED",
		Output:          map[string]any{"val": "done-no-logs"},
	}
	digest1, _ := worker.CanonicalCompletionDigest(&compPayload1)
	compPayload1.ResultDigest = digest1
	cb1, _ := json.Marshal(compPayload1)
	compReq1, _ := http.NewRequest("POST", server.URL+"/worker/v1/complete", bytes.NewReader(cb1))
	compReq1.Header.Set("Authorization", "Bearer "+sessionToken)
	compReq1.Header.Set("Content-Type", "application/json")
	cRes1, err := http.DefaultClient.Do(compReq1)
	if err != nil || cRes1.StatusCode != http.StatusOK {
		t.Fatalf("complete 1 failed: %v", err)
	}
	cRes1.Body.Close()

	// Initial check for Run 1: expired should be FALSE, message nil
	qReq1, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+run1.ID+"/logs", nil)
	qReq1.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	qRes1, err := http.DefaultClient.Do(qReq1)
	if err != nil || qRes1.StatusCode != http.StatusOK {
		t.Fatalf("get logs for run1 failed: %v", err)
	}
	var logsResp1 execution.RunLogsResponseDTO
	_ = json.NewDecoder(qRes1.Body).Decode(&logsResp1)
	qRes1.Body.Close()

	if logsResp1.Expired {
		t.Fatal("expected expired=false for run with no logs recorded")
	}
	if logsResp1.Message != nil {
		t.Fatalf("expected nil message for run with no logs recorded, got: %s", *logsResp1.Message)
	}
	if len(logsResp1.Items) != 0 {
		t.Fatalf("expected 0 items, got %d", len(logsResp1.Items))
	}

	// Run 2: Created, logs recorded, and then aged & pruned
	createReq2, _ := http.NewRequest("POST", server.URL+"/v1/workflows/inspector-workflow/runs", strings.NewReader(`{"environment":"staging","input":{"val":"has-logs"}}`))
	createReq2.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	createReq2.Header.Set("Idempotency-Key", "retention-with-logs-run")
	createReq2.Header.Set("Content-Type", "application/json")
	createRes2, _ := http.DefaultClient.Do(createReq2)
	var run2 execution.RunDTO
	_ = json.NewDecoder(createRes2.Body).Decode(&run2)
	createRes2.Body.Close()

	pollBody2, _ := json.Marshal(worker.PollRequestDTO{
		ProtocolVersion:   worker.ProtocolVersion,
		RequestID:         "p-retention-2",
		WorkerID:          sessionCtx.WorkerID,
		SessionID:         sessionCtx.SessionID,
		AvailableSlots:    1,
		DeploymentDigests: []string{bundleDigest},
		Pool:              "default",
	})
	pReq2, _ := http.NewRequest("POST", server.URL+"/worker/v1/poll", bytes.NewReader(pollBody2))
	pReq2.Header.Set("Authorization", "Bearer "+sessionToken)
	pReq2.Header.Set("Content-Type", "application/json")
	pRes2, err := http.DefaultClient.Do(pReq2)
	if err != nil || pRes2.StatusCode != http.StatusOK {
		t.Fatalf("poll 2 failed: %v", err)
	}
	var pollResp2 worker.PollResponseDTO
	_ = json.NewDecoder(pRes2.Body).Decode(&pollResp2)
	pRes2.Body.Close()
	if len(pollResp2.Assignments) == 0 {
		t.Fatalf("expected assignment for run 2")
	}
	as2 := pollResp2.Assignments[0]

	// Ingest 2 log lines for Run 2
	batch2 := worker.LogBatchRequestDTO{
		ProtocolVersion: 1,
		RequestID:       "log-retention-batch-2",
		WorkerID:        sessionCtx.WorkerID,
		SessionID:       sessionCtx.SessionID,
		AttemptID:       as2.AttemptID,
		Records: []worker.LogRecordDTO{
			{Sequence: 1, Level: "info", Message: "Old log message 1", Timestamp: time.Now().UTC().Format(time.RFC3339Nano)},
			{Sequence: 2, Level: "info", Message: "Old log message 2", Timestamp: time.Now().UTC().Format(time.RFC3339Nano)},
		},
	}
	bBytes2, _ := json.Marshal(batch2)
	lReq2, _ := http.NewRequest("POST", server.URL+"/worker/v1/logs", bytes.NewReader(bBytes2))
	lReq2.Header.Set("Authorization", "Bearer "+sessionToken)
	lReq2.Header.Set("Content-Type", "application/json")
	lRes2, err := http.DefaultClient.Do(lReq2)
	if err != nil || lRes2.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(lRes2.Body)
		t.Fatalf("log ingestion failed: %v (status %d): %s", err, lRes2.StatusCode, string(b))
	}
	lRes2.Body.Close()

	// Verify Run 2 has 2 log records before pruning
	qReq2Pre, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+run2.ID+"/logs", nil)
	qReq2Pre.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	qRes2Pre, err := http.DefaultClient.Do(qReq2Pre)
	if err != nil || qRes2Pre.StatusCode != http.StatusOK {
		t.Fatalf("pre-prune logs query failed: %v", err)
	}
	var logsPre execution.RunLogsResponseDTO
	_ = json.NewDecoder(qRes2Pre.Body).Decode(&logsPre)
	qRes2Pre.Body.Close()
	if len(logsPre.Items) != 2 || logsPre.Expired {
		t.Fatalf("expected 2 unexpired items pre-prune, got %d (expired=%v)", len(logsPre.Items), logsPre.Expired)
	}

	// Age logs by 8 days directly in PostgreSQL
	ctx := context.Background()
	err = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE task_logs SET created_at = clock_timestamp() - INTERVAL '8 days' WHERE run_id = $1::uuid`, run2.ID)
		return err
	})
	if err != nil {
		t.Fatalf("failed to age task logs: %v", err)
	}

	// Run pruning
	execSvc := execution.NewService(tc.pool, tc.service, execution.NewEventHub())
	pruned, err := execSvc.PruneExpiredTaskLogs(ctx, orgID, 100)
	if err != nil {
		t.Fatalf("prune expired logs failed: %v", err)
	}
	if pruned < 2 {
		t.Fatalf("expected at least 2 pruned logs, got %d", pruned)
	}

	// Verify task_attempts.logs_recorded is still true in DB for Run 2
	var recorded2 bool
	err = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT logs_recorded FROM task_attempts WHERE id = $1::uuid`, as2.AttemptID).Scan(&recorded2)
	})
	if err != nil || !recorded2 {
		t.Fatalf("expected logs_recorded to remain true for run 2, got %v (err: %v)", recorded2, err)
	}

	// Query logs for Run 2: expired MUST be true, message must cite 7-day retention
	qReq2Post, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+run2.ID+"/logs", nil)
	qReq2Post.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	qRes2Post, err := http.DefaultClient.Do(qReq2Post)
	if err != nil || qRes2Post.StatusCode != http.StatusOK {
		t.Fatalf("post-prune logs query failed: %v", err)
	}
	var logsResp2 execution.RunLogsResponseDTO
	_ = json.NewDecoder(qRes2Post.Body).Decode(&logsResp2)
	qRes2Post.Body.Close()

	if !logsResp2.Expired {
		t.Fatal("expected expired=true for run with pruned logs")
	}
	if len(logsResp2.Items) != 0 {
		t.Fatalf("expected 0 items after pruning, got %d", len(logsResp2.Items))
	}
	if logsResp2.Message == nil {
		t.Fatal("expected message mentioning 7-day retention, got nil")
	} else if !strings.Contains(*logsResp2.Message, "7-day") {
		t.Fatalf("expected message mentioning 7-day retention, got: %q", *logsResp2.Message)
	}

	// Query logs for Run 1 after pruning: expired must STILL be false!
	qReq1Post, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+run1.ID+"/logs", nil)
	qReq1Post.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	qRes1Post, err := http.DefaultClient.Do(qReq1Post)
	if err != nil || qRes1Post.StatusCode != http.StatusOK {
		t.Fatalf("post-prune logs query for run 1 failed: %v", err)
	}
	var logsResp1Post execution.RunLogsResponseDTO
	_ = json.NewDecoder(qRes1Post.Body).Decode(&logsResp1Post)
	qRes1Post.Body.Close()

	if logsResp1Post.Expired {
		t.Fatal("expected expired=false for run 1 even after pruning")
	}
	if logsResp1Post.Message != nil {
		t.Fatalf("expected nil message for run 1, got: %v", *logsResp1Post.Message)
	}
	if len(logsResp1Post.Items) != 0 {
		t.Fatalf("expected 0 items for run 1, got %d", len(logsResp1Post.Items))
	}
}

func TestRunInspectorDeterministicSnapshotToSubscribeRace(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer server.Close()

	_, bundleDigest := setupInspectorWorkflow(t, tc, server, orgID, envID, adminKey)
	sessionCtx, sessionToken := enrollTestWorker(t, tc, server, orgID, envID, bundleDigest)

	// 1. Create run
	createReq, _ := http.NewRequest("POST", server.URL+"/v1/workflows/inspector-workflow/runs", strings.NewReader(`{"environment":"staging","input":{"val":"race-test"}}`))
	createReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	createReq.Header.Set("Idempotency-Key", "race-test-run")
	createReq.Header.Set("Content-Type", "application/json")
	createRes, _ := http.DefaultClient.Do(createReq)
	var runResp execution.RunDTO
	_ = json.NewDecoder(createRes.Body).Decode(&runResp)
	createRes.Body.Close()

	// 2. Snapshot: lastEventSequence is 1 (run.created)
	snapReq, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+runResp.ID, nil)
	snapReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	snapRes, _ := http.DefaultClient.Do(snapReq)
	var snap execution.RunSnapshotDTO
	_ = json.NewDecoder(snapRes.Body).Decode(&snap)
	snapRes.Body.Close()

	if snap.LastEventSequence != 1 {
		t.Fatalf("expected lastEventSequence=1, got %d", snap.LastEventSequence)
	}

	// 3. Before client subscribes to SSE, worker claims task (commits event sequence 2)
	pollBody, _ := json.Marshal(worker.PollRequestDTO{
		ProtocolVersion:   worker.ProtocolVersion,
		RequestID:         "p-race",
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

	// 4. Client connects SSE with Last-Event-ID = 1 (simulates snapshot at seq 1, then connecting)
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

	reader := bufio.NewReader(sseRes.Body)
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

	// 5. Expect sequence 2 (attempt.claimed) from replay catchup
	select {
	case ev := <-readChan:
		if ev != "attempt.claimed" {
			t.Fatalf("expected attempt.claimed catch-up event, got %s", ev)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for catch-up event sequence 2")
	}

	// 6. Worker starts task (live broadcast: sequence 3)
	startReq, _ := http.NewRequest("POST", server.URL+"/worker/v1/start", strings.NewReader(
		fmt.Sprintf(`{"protocolVersion":1,"requestId":"s-race","workerId":"%s","sessionId":"%s","attemptId":"%s","ownershipEpoch":%d}`, sessionCtx.WorkerID, sessionCtx.SessionID, as.AttemptID, as.OwnershipEpoch),
	))
	startReq.Header.Set("Authorization", "Bearer "+sessionToken)
	startReq.Header.Set("Content-Type", "application/json")
	startRes, _ := http.DefaultClient.Do(startReq)
	startRes.Body.Close()

	// 7. Expect sequence 3 (attempt.started) delivered live without reconnecting
	select {
	case ev := <-readChan:
		if ev != "attempt.started" {
			t.Fatalf("expected attempt.started live event, got %s", ev)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for live broadcast event sequence 3")
	}
}

func TestRunInspectorCrossTenantStreamRejection(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer server.Close()

	setupInspectorWorkflow(t, tc, server, orgID, envID, adminKey)

	// Create run in Org A
	createReq, _ := http.NewRequest("POST", server.URL+"/v1/workflows/inspector-workflow/runs", strings.NewReader(`{"environment":"staging","input":{"val":"tenant-a"}}`))
	createReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	createReq.Header.Set("Idempotency-Key", "stream-isolation-run")
	createReq.Header.Set("Content-Type", "application/json")
	createRes, _ := http.DefaultClient.Do(createReq)
	var runResp execution.RunDTO
	_ = json.NewDecoder(createRes.Body).Decode(&runResp)
	createRes.Body.Close()

	// Create Org B and Key B
	ctx := context.Background()
	ownerB, _ := tenant.NewUUID()
	orgB, _ := tc.service.CreateOrganization(ctx, ownerB, "Tenant B Org")
	projB, _ := tc.service.CreateProject(ctx, orgB.ID, "Tenant B Proj")
	envB, _ := tc.service.CreateEnvironment(ctx, orgB.ID, projB.ID, tenant.EnvStaging, 5)
	keyB := bootstrapTestKey(t, tc.service, orgB.ID, envB.ID, []string{
		tenant.CapRunsRead,
	})

	// Tenant B attempts to open SSE stream for Run A
	req, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+runResp.ID+"/stream", nil)
	req.Header.Set("Authorization", "Bearer "+keyB.PlaintextKey)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-tenant violation: expected 404 Not Found for stream, got %d", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("cross-tenant violation: stream headers flushed on unauthorized request")
	}
}

func TestRunInspectorHonestNoWorkerState(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer server.Close()

	_, bundleDigest := setupInspectorWorkflow(t, tc, server, orgID, envID, adminKey)

	// Expire the preflight-seeded worker session so that active compatible worker count starts at 0
	_ = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE worker_sessions SET expires_at = clock_timestamp() - interval '1 minute' WHERE organization_id = $1::uuid`, orgID)
		return err
	})

	// Create run with NO worker connected
	createReq, _ := http.NewRequest("POST", server.URL+"/v1/workflows/inspector-workflow/runs", strings.NewReader(`{"environment":"staging","input":{"val":"no-worker-test"}}`))
	createReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	createReq.Header.Set("Idempotency-Key", "honest-no-worker-run")
	createReq.Header.Set("Content-Type", "application/json")
	createRes, _ := http.DefaultClient.Do(createReq)
	var runResp execution.RunDTO
	_ = json.NewDecoder(createRes.Body).Decode(&runResp)
	createRes.Body.Close()

	// Snapshot before worker enrolls
	snapReq, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+runResp.ID, nil)
	snapReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	snapRes, _ := http.DefaultClient.Do(snapReq)
	var snap execution.RunSnapshotDTO
	_ = json.NewDecoder(snapRes.Body).Decode(&snap)
	snapRes.Body.Close()

	if snap.Status != contracts.RunStatusQUEUED {
		t.Fatalf("expected QUEUED, got %s", snap.Status)
	}
	if snap.ActiveCompatibleWorkers != 0 {
		t.Fatalf("expected 0 active compatible workers, got %d", snap.ActiveCompatibleWorkers)
	}
	if snap.WaitingReason == nil || *snap.WaitingReason != "NO_COMPATIBLE_WORKERS" {
		t.Fatalf("expected waitingReason=NO_COMPATIBLE_WORKERS, got %v", snap.WaitingReason)
	}

	// A worker in another environment may advertise the identical bundle but
	// cannot execute this staging run. It must not clear the waiting diagnosis.
	var projectID string
	err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT project_id::text FROM environments WHERE id = $1::uuid`, envID).Scan(&projectID)
	})
	if err != nil {
		t.Fatalf("resolve staging project for cross-environment fixture: %v", err)
	}
	envB, err := tc.service.CreateEnvironment(context.Background(), orgID, projectID, tenant.EnvProduction, 5)
	if err != nil {
		t.Fatalf("create second environment: %v", err)
	}
	enrollTestWorker(t, tc, server, orgID, envB.ID, bundleDigest)

	snapResOtherEnv, _ := http.DefaultClient.Do(snapReq)
	var snapOtherEnv execution.RunSnapshotDTO
	_ = json.NewDecoder(snapResOtherEnv.Body).Decode(&snapOtherEnv)
	snapResOtherEnv.Body.Close()
	if snapOtherEnv.ActiveCompatibleWorkers != 0 || snapOtherEnv.WaitingReason == nil {
		t.Fatalf("other-environment worker must not satisfy staging run: %+v", snapOtherEnv)
	}

	// Now enroll worker and send poll advertising bundle digest
	enrollTestWorker(t, tc, server, orgID, envID, bundleDigest)

	// Snapshot after worker enrolls: waitingReason must clear
	snapRes2, _ := http.DefaultClient.Do(snapReq)
	var snap2 execution.RunSnapshotDTO
	_ = json.NewDecoder(snapRes2.Body).Decode(&snap2)
	snapRes2.Body.Close()

	if snap2.ActiveCompatibleWorkers < 1 {
		t.Fatalf("expected >= 1 active compatible workers, got %d", snap2.ActiveCompatibleWorkers)
	}
	if snap2.WaitingReason != nil {
		t.Fatalf("expected waitingReason to be nil once worker is active, got %v", *snap2.WaitingReason)
	}
}

func TestDashboardStaticFileServingSmoke(t *testing.T) {
	_, server, _, _, _ := setupRunLifecycleTest(t)
	defer server.Close()

	// 1. GET /dashboard/ -> HTML index
	resIndex, err := http.Get(server.URL + "/dashboard/")
	if err != nil {
		t.Fatalf("GET /dashboard/ failed: %v", err)
	}
	bodyIndex, _ := io.ReadAll(resIndex.Body)
	resIndex.Body.Close()

	if resIndex.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for /dashboard/, got %d", resIndex.StatusCode)
	}
	if !strings.Contains(string(bodyIndex), "Deadbolt") {
		t.Fatalf("expected index.html to mention Deadbolt, got:\n%s", string(bodyIndex))
	}

	// 2. GET /dashboard (without trailing slash) -> 301 redirect to /dashboard/
	clientNoFollow := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resRedir, err := clientNoFollow.Get(server.URL + "/dashboard")
	if err != nil {
		t.Fatalf("GET /dashboard failed: %v", err)
	}
	resRedir.Body.Close()

	if resRedir.StatusCode != http.StatusMovedPermanently {
		t.Fatalf("expected 301 Moved Permanently for /dashboard, got %d", resRedir.StatusCode)
	}
	if loc := resRedir.Header.Get("Location"); loc != "/dashboard/" {
		t.Fatalf("expected Location: /dashboard/, got %s", loc)
	}

	// 3. GET /dashboard/styles.css
	resCSS, err := http.Get(server.URL + "/dashboard/styles.css")
	if err != nil {
		t.Fatalf("GET /dashboard/styles.css failed: %v", err)
	}
	bodyCSS, _ := io.ReadAll(resCSS.Body)
	resCSS.Body.Close()

	if resCSS.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for styles.css, got %d", resCSS.StatusCode)
	}
	if len(bodyCSS) == 0 {
		t.Fatal("expected non-empty styles.css")
	}

	// 4. GET /dashboard/index.js
	resJS, err := http.Get(server.URL + "/dashboard/index.js")
	if err != nil {
		t.Fatalf("GET /dashboard/index.js failed: %v", err)
	}
	bodyJS, _ := io.ReadAll(resJS.Body)
	resJS.Body.Close()

	if resJS.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for index.js, got %d", resJS.StatusCode)
	}
	if len(bodyJS) == 0 {
		t.Fatal("expected non-empty index.js")
	}
}

func setupInspectorMultiStepWorkflow(t *testing.T, tc *tenantTestContext, server *httptest.Server, orgID, envID string, adminKey *tenant.GeneratedKey) (string, string) {
	t.Helper()
	tempDir := t.TempDir()
	bundleDigest := writeAgentBundle(t, tempDir, "linux", "amd64")

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
			"name":            "inspector-multi-step",
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
				{
					"id":    "step-2",
					"type":  "task",
					"task":  "task-simple",
					"after": []any{"step-1"},
					"input": map[string]any{
						"val": map[string]any{"$ref": "step.output", "stepId": "step-1", "pointer": "/val"},
					},
				},
			},
			"output": map[string]any{
				"val": map[string]any{"$ref": "step.output", "stepId": "step-2", "pointer": "/val"},
			},
		},
	}

	manifestBytes := createLifecycleManifest(bundleDigest, tasks, workflows)
	depID := registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "inspector-multi-step", manifestBytes)
	return depID, bundleDigest
}

func TestRunInspector_StepGraphMetadataAndRedaction(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer server.Close()

	_, bundleDigest := setupInspectorMultiStepWorkflow(t, tc, server, orgID, envID, adminKey)
	sessionCtx, sessionToken := enrollTestWorker(t, tc, server, orgID, envID, bundleDigest)

	// Create run
	createReq, _ := http.NewRequest("POST", server.URL+"/v1/workflows/inspector-multi-step/runs", strings.NewReader(`{"environment":"staging","input":{"val":"inspector-graph-input"}}`))
	createReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	createReq.Header.Set("Idempotency-Key", "graph-metadata-run")
	createReq.Header.Set("Content-Type", "application/json")
	createRes, err := http.DefaultClient.Do(createReq)
	if err != nil || createRes.StatusCode != http.StatusAccepted {
		t.Fatalf("create run failed: %v", err)
	}
	var runResp execution.RunDTO
	_ = json.NewDecoder(createRes.Body).Decode(&runResp)
	createRes.Body.Close()

	// Worker polls and starts
	pollBody, _ := json.Marshal(worker.PollRequestDTO{
		ProtocolVersion:   worker.ProtocolVersion,
		RequestID:         "p-graph-1",
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
		fmt.Sprintf(`{"protocolVersion":1,"requestId":"s-graph-1","workerId":"%s","sessionId":"%s","attemptId":"%s","ownershipEpoch":%d}`, sessionCtx.WorkerID, sessionCtx.SessionID, as.AttemptID, as.OwnershipEpoch),
	))
	startReq.Header.Set("Authorization", "Bearer "+sessionToken)
	startReq.Header.Set("Content-Type", "application/json")
	startRes, _ := http.DefaultClient.Do(startReq)
	startRes.Body.Close()

	// Complete step-1 with output
	compPayload := worker.CompleteRequestDTO{
		ProtocolVersion: 1,
		RequestID:       "c-graph-1",
		WorkerID:        sessionCtx.WorkerID,
		SessionID:       sessionCtx.SessionID,
		AttemptID:       as.AttemptID,
		OwnershipEpoch:  as.OwnershipEpoch,
		Outcome:         "SUCCEEDED",
		Output:          map[string]any{"val": "confidential-step-result"},
	}
	digest, _ := worker.CanonicalCompletionDigest(&compPayload)
	compPayload.ResultDigest = digest
	cb, _ := json.Marshal(compPayload)
	compReq, _ := http.NewRequest("POST", server.URL+"/worker/v1/complete", bytes.NewReader(cb))
	compReq.Header.Set("Authorization", "Bearer "+sessionToken)
	compReq.Header.Set("Content-Type", "application/json")
	compRes, _ := http.DefaultClient.Do(compReq)
	compRes.Body.Close()

	// 1. Caller with payload:read capability receives step kind, after, and output
	adminReq, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+runResp.ID, nil)
	adminReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	adminRes, err := http.DefaultClient.Do(adminReq)
	if err != nil || adminRes.StatusCode != http.StatusOK {
		t.Fatalf("admin get run failed: %v", err)
	}
	var adminSnap execution.RunSnapshotDTO
	_ = json.NewDecoder(adminRes.Body).Decode(&adminSnap)
	adminRes.Body.Close()

	if len(adminSnap.Steps) < 2 {
		t.Fatalf("expected at least 2 steps, got %d", len(adminSnap.Steps))
	}

	// Locate step-1 and step-2
	var step1, step2 *execution.RunStepDTO
	for i := range adminSnap.Steps {
		if adminSnap.Steps[i].NodeID == "step-1" {
			step1 = &adminSnap.Steps[i]
		}
		if adminSnap.Steps[i].NodeID == "step-2" {
			step2 = &adminSnap.Steps[i]
		}
	}

	if step1 == nil || step2 == nil {
		t.Fatalf("expected step-1 and step-2 in snapshot, got %+v", adminSnap.Steps)
	}

	if step1.Kind == nil || *step1.Kind != "task" {
		t.Fatalf("expected step-1 kind 'task', got %v", step1.Kind)
	}
	if step1.Output == nil {
		t.Fatal("expected step-1 output to be visible for caller with payload:read")
	}

	if step2.Kind == nil || *step2.Kind != "task" {
		t.Fatalf("expected step-2 kind 'task', got %v", step2.Kind)
	}
	if len(step2.After) != 1 || step2.After[0] != "step-1" {
		t.Fatalf("expected step-2 after ['step-1'], got %v", step2.After)
	}

	// 2. Caller WITHOUT payload:read capability receives step kind & after, but Output is redacted (nil)
	restrictedKey := bootstrapTestKey(t, tc.service, orgID, envID, []string{
		tenant.CapRunsRead,
	})
	viewerReq, _ := http.NewRequest("GET", server.URL+"/v1/runs/"+runResp.ID, nil)
	viewerReq.Header.Set("Authorization", "Bearer "+restrictedKey.PlaintextKey)
	viewerRes, err := http.DefaultClient.Do(viewerReq)
	if err != nil || viewerRes.StatusCode != http.StatusOK {
		t.Fatalf("viewer get run failed: %v", err)
	}
	var viewerSnap execution.RunSnapshotDTO
	_ = json.NewDecoder(viewerRes.Body).Decode(&viewerSnap)
	viewerRes.Body.Close()

	var vStep1, vStep2 *execution.RunStepDTO
	for i := range viewerSnap.Steps {
		if viewerSnap.Steps[i].NodeID == "step-1" {
			vStep1 = &viewerSnap.Steps[i]
		}
		if viewerSnap.Steps[i].NodeID == "step-2" {
			vStep2 = &viewerSnap.Steps[i]
		}
	}

	if vStep1 == nil || vStep2 == nil {
		t.Fatalf("expected step-1 and step-2 in viewer snapshot")
	}

	if vStep1.Kind == nil || *vStep1.Kind != "task" {
		t.Fatalf("expected vStep1 kind 'task', got %v", vStep1.Kind)
	}
	if vStep1.Output != nil {
		t.Fatalf("security violation: expected step-1 output to be redacted (nil) for viewer, got %v", vStep1.Output)
	}

	if vStep2.Kind == nil || *vStep2.Kind != "task" {
		t.Fatalf("expected vStep2 kind 'task', got %v", vStep2.Kind)
	}
	if len(vStep2.After) != 1 || vStep2.After[0] != "step-1" {
		t.Fatalf("expected vStep2 after ['step-1'], got %v", vStep2.After)
	}
}

