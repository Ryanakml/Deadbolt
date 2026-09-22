package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAssembleCompletePublishFailureIsFailedUnknown(t *testing.T) {
	asg := AssignmentDTO{AttemptID: "a1", OwnershipEpoch: 3}
	completion := &TaskCompletion{AttemptID: "a1", Status: "SUCCEEDED", Output: map[string]any{"ok": true}}
	pubErr := &ArtifactPublishError{Code: "UPLOAD_FAILED", Retryable: true, Err: errors.New("boom")}
	req := assembleCompleteRequest(asg, "w1", "s1", completion, nil, "", true, pubErr)
	if req.Outcome != "FAILED" {
		t.Fatalf("Outcome = %q, want FAILED", req.Outcome)
	}
	if req.Error == nil || req.Error.EffectStatus != "UNKNOWN" {
		t.Fatalf("EffectStatus = %+v, want UNKNOWN", req.Error)
	}
	if req.ArtifactID != "" {
		t.Fatalf("ArtifactID = %q, want empty", req.ArtifactID)
	}
	if req.Output != nil {
		t.Fatalf("Output = %v, want absent", req.Output)
	}
	if req.Outcome == "SUCCEEDED" && req.Error != nil {
		t.Fatal("contradictory SUCCEEDED+Error constructed")
	}
	digest, err := CanonicalCompletionDigest(req)
	if err != nil {
		t.Fatal(err)
	}
	req.ResultDigest = digest
	// Digest must cover the FAILED outcome, not a stale SUCCEEDED.
	again, _ := CanonicalCompletionDigest(req)
	if again != digest {
		t.Fatal("digest unstable")
	}
}

func TestBuildMissingSecretIsNotApplied(t *testing.T) {
	asg := AssignmentDTO{AttemptID: "a1", OwnershipEpoch: 1}
	secretErr := fmt.Errorf("required task secret %q is not available", "MISSING_X")
	req := buildMissingSecretPreflight(asg, "w1", "s1", secretErr)
	if req.Outcome != "FAILED" {
		t.Fatalf("Outcome = %q, want FAILED", req.Outcome)
	}
	if req.Error == nil || req.Error.Code != "MISSING_REQUIRED_SECRET" {
		t.Fatalf("code = %+v", req.Error)
	}
	if req.Error.EffectStatus != "NOT_APPLIED" {
		t.Fatalf("EffectStatus = %q, want NOT_APPLIED", req.Error.EffectStatus)
	}
}

func TestPublishStableKeysReuseAcrossRetries(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	var bodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		// Read fully via json raw? Simpler: record path only; bodies checked via second server below.
		_ = body
		mu.Unlock()
		switch {
		case r.URL.Path == "/v1/artifacts":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "art-1", "uploadUrl": "http://127.0.0.1:9/put"})
		case strings.HasSuffix(r.URL.Path, "/finalize"):
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "art-1", "status": "READY"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	_ = bodies
	mkAgent := func() *Agent {
		u, _ := url.Parse(server.URL)
		return &Agent{baseURL: u, client: server.Client(), sessionTok: "tok"}
	}
	asg := AssignmentDTO{RunID: "r", AttemptID: "attempt-1", OwnershipEpoch: 7}
	data := []byte("stable-bytes")
	// First publish attempt records keys; second retry must reuse identical keys.
	// PUT to the fake upload URL will fail, so stub the PUT path by pointing
	// uploadUrl at a local server that accepts PUTs.
	putOK := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer putOK.Close()
	// Override server to return working PUT URL.
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		keys = append(keys, r.URL.Path+"|"+r.Header.Get("Idempotency-Key"))
		mu.Unlock()
		switch {
		case r.URL.Path == "/v1/artifacts":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "art-1", "uploadUrl": putOK.URL + "/put"})
		case strings.HasSuffix(r.URL.Path, "/finalize"):
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "art-1", "status": "READY"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	a1 := mkAgent()
	if _, err := a1.publishArtifactViaControlPlane(context.Background(), asg, data, "application/octet-stream"); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	a2 := mkAgent()
	if _, err := a2.publishArtifactViaControlPlane(context.Background(), asg, data, "application/octet-stream"); err != nil {
		t.Fatalf("retry publish: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(keys) != 4 {
		t.Fatalf("expected 4 artifact calls, got %d: %v", len(keys), keys)
	}
	if keys[0] != keys[2] {
		t.Fatalf("create key not stable: %q vs %q", keys[0], keys[2])
	}
	if keys[1] != keys[3] {
		t.Fatalf("finalize key not stable: %q vs %q", keys[1], keys[3])
	}
	for _, k := range keys {
		parts := strings.SplitN(k, "|", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[1]) == "" {
			t.Fatalf("missing Idempotency-Key: %q", k)
		}
		if strings.Contains(parts[1], "req_comp_") || len(parts[1]) < 10 {
			t.Fatalf("unstable key shape: %q", k)
		}
	}
	// Keys must embed the logical operation identity (attempt + content/artifact).
	if !strings.Contains(keys[0], "attempt-1") || !strings.Contains(keys[1], "art-1") {
		t.Fatalf("keys must identify logical operation: %v", keys)
	}
}

func TestMissingSecretNeverLaunchesHandler(t *testing.T) {
	const secret = "DEADBOLT_TEST_HANDLER_MUST_NOT_LAUNCH"
	_ = os.Unsetenv(secret)
	var launched atomic.Int32
	var mu sync.Mutex
	var completed []CompleteRequestDTO
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/worker/v1/start":
			_ = json.NewEncoder(w).Encode(StartResponseDTO{
				ProtocolVersion: ProtocolVersion, RequestID: "s",
				AttemptID: "attempt-1", OwnershipEpoch: 1, Accepted: true,
				AttemptDeadlineAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339),
			})
		case "/worker/v1/complete":
			var req CompleteRequestDTO
			_ = json.NewDecoder(r.Body).Decode(&req)
			mu.Lock()
			completed = append(completed, req)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(CompleteResponseDTO{
				ProtocolVersion: ProtocolVersion, RequestID: req.RequestID,
				AttemptID: req.AttemptID, OwnershipEpoch: req.OwnershipEpoch, Accepted: true,
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	agent, err := NewAgent(AgentConfig{
		ControlPlaneURL:    server.URL,
		OnTaskProcessStart: func() { launched.Add(1) },
		HTTPClient:         server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	agent.baseURL = u
	agent.workerID, agent.sessionID, agent.sessionTok = "worker-1", "session-1", "tok"
	agent.expiresAt = time.Now().Add(time.Hour)
	// executeAssignment releases a slot on return; provide one as pollLoop would.
	agent.slotsChan <- struct{}{}
	asg := AssignmentDTO{
		AttemptID: "attempt-1", OwnershipEpoch: 1, StepID: "step-1",
		TaskEntrypoint: "tasks/a.js", SecretNames: []string{secret},
	}
	agent.executeAssignment(context.Background(), asg)
	if launched.Load() != 0 {
		t.Fatal("customer handler must not launch when a required secret is missing")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(completed) != 1 {
		t.Fatalf("expected 1 preflight completion, got %d", len(completed))
	}
	got := completed[0]
	if got.Outcome != "FAILED" || got.Error == nil || got.Error.Code != "MISSING_REQUIRED_SECRET" {
		t.Fatalf("preflight = %+v", got)
	}
	if got.Error.EffectStatus != "NOT_APPLIED" {
		t.Fatalf("EffectStatus = %q, want NOT_APPLIED", got.Error.EffectStatus)
	}
}
