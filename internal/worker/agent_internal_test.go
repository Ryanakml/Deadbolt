package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestResolveTaskSecretsFailsClosedWhenRequiredSecretIsMissing(t *testing.T) {
	const name = "DEADBOLT_TEST_REQUIRED_SECRET_MUST_NOT_EXIST"
	t.Setenv(name, "temporary")
	// t.Setenv registers restoration; remove the temporary value to exercise
	// the actual missing-secret path used before process creation.
	if err := os.Unsetenv(name); err != nil {
		t.Fatal(err)
	}
	values, err := resolveTaskSecrets([]string{name})
	if err == nil || values != nil || !strings.Contains(err.Error(), name) {
		t.Fatalf("missing required secret did not fail closed: values=%v err=%v", values, err)
	}
}

func TestClassifyLogLevelPreservesRunnerLevel(t *testing.T) {
	runnerEntry := func(level string) string {
		return `{"timestamp":"2026-09-17T00:00:00.000Z","level":"` + level + `","attemptId":"a","message":"hello"}`
	}
	for _, tc := range []struct {
		line     string
		fallback string
		want     string
	}{
		{runnerEntry("INFO"), "warn", "info"},
		{runnerEntry("WARN"), "warn", "warn"},
		{runnerEntry("ERROR"), "warn", "error"},
		{runnerEntry("DEBUG"), "info", "debug"},
		{runnerEntry("info"), "warn", "info"},
		// Unstructured lines keep the stream default: stderr warn, stdout info.
		{"FATAL_RUNNER_ERROR: boom", "warn", "warn"},
		{"plain stdout line", "info", "info"},
		{"{not json", "warn", "warn"},
		{`{"level":"VERBOSE","message":"x"}`, "warn", "warn"},
		{`{"message":"no level"}`, "info", "info"},
	} {
		if got := classifyLogLevel(tc.line, tc.fallback); got != tc.want {
			t.Errorf("classifyLogLevel(%q, %q) = %q, want %q", tc.line, tc.fallback, got, tc.want)
		}
	}
}

func TestCapturedProcessOutputIsStrictlyBounded(t *testing.T) {
	var output cappedBuffer
	payload := bytes.Repeat([]byte("x"), maxCapturedOutput+4096)
	written, err := output.Write(payload)
	if err != nil || written != len(payload) {
		t.Fatalf("writer contract failed: written=%d err=%v", written, err)
	}
	if output.Len() != maxCapturedOutput || !output.truncated {
		t.Fatalf("output cap failed: len=%d truncated=%v", output.Len(), output.truncated)
	}
	if written, err := output.Write([]byte("ignored")); err != nil || written != len("ignored") || output.Len() != maxCapturedOutput {
		t.Fatalf("writes after cap changed buffer: written=%d len=%d err=%v", written, output.Len(), err)
	}
}

func TestOversizedRunnerInputNeverSpawnsChild(t *testing.T) {
	bundle, err := os.CreateTemp(t.TempDir(), "task-*.js")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bundle.WriteString("export default async () => ({ ok: true });\n"); err != nil {
		t.Fatal(err)
	}
	if err := bundle.Close(); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(bundle.Name())
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(contents)
	supervisor := NewProcessSupervisor("definitely-not-node", "ignored")
	supervisor.LeaseTracker = NewLeaseTracker(time.Now().Add(time.Minute), 0, 0)
	supervisor.StartAckFn = func(context.Context, string, int64) error { return nil }
	supervisor.MaxRunnerInputBytes = 256
	started := false
	supervisor.OnProcessStart = func(int) { started = true }

	_, _, err = supervisor.ExecuteAttempt(context.Background(), &TaskInput{
		AttemptID: "too-large", Entrypoint: bundle.Name(),
		Bundle: &BundleSpec{Path: bundle.Name(), SHA256: hex.EncodeToString(digest[:]), TargetArch: CurrentHostArchitecture()},
		Input:  strings.Repeat("x", 4096),
	}, 1)
	if !errors.Is(err, ErrRunnerInputTooLarge) {
		t.Fatalf("expected oversized runner input to fail before spawn, got %v", err)
	}
	if started {
		t.Fatal("runner child was spawned for oversized serialized input")
	}
}

func TestCustomerSecretInChildLogsIsRedactedBeforeControlPlaneTransport(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node binary not found in PATH")
	}
	const secret = "sentinel-secret-must-never-reach-control-plane"
	var received []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		received, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read log transport: %v", err)
		}
		_ = json.NewEncoder(w).Encode(AckResponseDTO{ProtocolVersion: ProtocolVersion, Accepted: true})
	}))
	defer server.Close()

	agent, err := NewAgent(AgentConfig{ControlPlaneURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	agent.workerID, agent.sessionID, agent.sessionTok = "worker", "session", "token"
	fixture, err := filepath.Abs("../../runner/node/tests/fixtures/print-secret-task.js")
	if err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(contents)
	supervisor := NewProcessSupervisor("node", mustWorkerRunnerPath(t))
	supervisor.LeaseTracker = NewLeaseTracker(time.Now().Add(time.Minute), 0, 0)
	supervisor.StartAckFn = func(context.Context, string, int64) error { return nil }
	supervisor.TaskEnvAllowlist = []string{"DEMO_TASK_SECRET"}
	_, logs, err := supervisor.ExecuteAttempt(context.Background(), &TaskInput{
		AttemptID: "secret-log", StepID: "step", TaskName: "printSecretTask", Entrypoint: fixture,
		Bundle: &BundleSpec{Path: fixture, SHA256: hex.EncodeToString(digest[:]), TargetArch: CurrentHostArchitecture()},
		Env:    map[string]string{"DEMO_TASK_SECRET": secret},
	}, 1)
	if err != nil {
		t.Fatalf("run child that prints secret: %v", err)
	}
	if logs == nil || !strings.Contains(logs.Stdout+logs.Stderr, secret) {
		t.Fatal("test fixture did not emit the task secret into captured logs")
	}
	agent.sendLogBatch(context.Background(), "secret-log", logs, map[string]string{"DEMO_TASK_SECRET": secret})
	if strings.Contains(string(received), secret) {
		t.Fatalf("control plane log transport received raw task secret: %s", received)
	}
	if !strings.Contains(string(received), "[REDACTED]") {
		t.Fatalf("expected transport to contain a redaction marker, got %s", received)
	}
}

func mustWorkerRunnerPath(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs("../../runner/node/dist/index.js")
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func TestHeartbeatRevocationTerminatesActiveRunnerAndAgentFailsClosed(t *testing.T) {
	var heartbeats atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		heartbeats.Add(1)
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(ErrorEnvelopeDTO{Code: "WORKER_REVOKED", Message: "revoked"})
	}))
	defer server.Close()
	agent, err := NewAgent(AgentConfig{ControlPlaneURL: server.URL, HeartbeatInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	agent.supervisor.GracePeriod = 100 * time.Millisecond
	agent.workerID, agent.sessionID, agent.sessionTok = "worker", "session", "token"
	agent.expiresAt = time.Now().Add(time.Hour)
	cmd := exec.Command("sh", "-c", "sleep 30")
	configureProcessGroup(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	cancelled := make(chan struct{})
	agent.runningAttempts["attempt"] = &activeAttempt{attemptID: "attempt", pid: cmd.Process.Pid, cancel: func() { close(cancelled) }}
	// Keep polling blocked on slots; revocation must wake it without waiting for the lease.
	for i := 0; i < agent.cfg.Slots; i++ {
		agent.slotsChan <- struct{}{}
	}
	pollDone := make(chan error, 1)
	go func() { pollDone <- agent.pollLoop(context.Background()) }()
	hbDone := make(chan struct{})
	go func() {
		agent.heartbeatLoop(context.Background(), "attempt", 1, NewLeaseTracker(time.Now().Add(time.Minute), 0, 0), hbDone)
		close(hbDone)
	}()
	select {
	case <-hbDone:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not stop on WORKER_REVOKED")
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("active attempt was not cancelled on revocation")
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("active child process was not terminated immediately")
	}
	select {
	case err := <-pollDone:
		if !errors.Is(err, ErrWorkerRevoked) {
			t.Fatalf("poll loop returned %v, want ErrWorkerRevoked", err)
		}
	case <-time.After(time.Second):
		t.Fatal("agent waited instead of exiting after revocation")
	}
	if got := heartbeats.Load(); got != 1 {
		t.Fatalf("heartbeats continued after revocation: %d", got)
	}
}

func TestHeartbeatTransientErrorsRemainLeaseSafetyErrors(t *testing.T) {
	var heartbeats atomic.Int32
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if heartbeats.Add(1) == 2 {
			close(done)
		}
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(ErrorEnvelopeDTO{Code: "INTERNAL_ERROR", Message: "temporary"})
	}))
	defer server.Close()
	agent, err := NewAgent(AgentConfig{ControlPlaneURL: server.URL, HeartbeatInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	agent.workerID, agent.sessionID, agent.sessionTok = "worker", "session", "token"
	loopDone := make(chan struct{})
	go func() {
		agent.heartbeatLoop(context.Background(), "attempt", 1, NewLeaseTracker(time.Now().Add(time.Minute), 0, 0), done)
		close(loopDone)
	}()
	select {
	case <-loopDone:
	case <-time.After(time.Second):
		t.Fatal("heartbeat loop did not observe test completion")
	}
	if agent.isRevoked() {
		t.Fatal("transient heartbeat failure incorrectly revoked worker")
	}
	if got := heartbeats.Load(); got < 2 {
		t.Fatalf("expected transient retry behavior, got %d calls", got)
	}
}

func TestStopAckReportsUnconfirmedWhenGroupSurvives(t *testing.T) {
	var mu sync.Mutex
	var stopAcks []StopAckRequestDTO
	acked := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/worker/v1/heartbeat":
			_ = json.NewEncoder(w).Encode(HeartbeatResponseDTO{
				ProtocolVersion: ProtocolVersion,
				RequestID:       "hb",
				Stops:           []StopCommandDTO{{AttemptID: "attempt", OwnershipEpoch: 1, Reason: "CANCEL_REQUESTED", GraceTimeoutMs: 10000}},
			})
		case "/worker/v1/stop-ack":
			var ack StopAckRequestDTO
			_ = json.NewDecoder(r.Body).Decode(&ack)
			mu.Lock()
			stopAcks = append(stopAcks, ack)
			mu.Unlock()
			select {
			case acked <- struct{}{}:
			default:
			}
			_ = json.NewEncoder(w).Encode(AckResponseDTO{ProtocolVersion: ProtocolVersion, RequestID: ack.RequestID, Accepted: true})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	agent, err := NewAgent(AgentConfig{ControlPlaneURL: server.URL, HeartbeatInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	agent.supervisor.GracePeriod = 100 * time.Millisecond
	agent.workerID, agent.sessionID, agent.sessionTok = "worker", "session", "token"
	agent.expiresAt = time.Now().Add(time.Hour)
	// Spawn without reaping: after SIGTERM+SIGKILL the child persists as an
	// unreaped zombie, so the group cannot be confirmed dead and the ACK
	// must report ProcessStopped=false rather than an assumed true.
	cmd := exec.Command("sleep", "30")
	configureProcessGroup(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	agent.runningAttempts["attempt"] = &activeAttempt{attemptID: "attempt", epoch: 1, pid: cmd.Process.Pid, cancel: func() {}}
	hbDone := make(chan struct{})
	go func() {
		agent.heartbeatLoop(context.Background(), "attempt", 1, NewLeaseTracker(time.Now().Add(time.Minute), 0, 0), hbDone)
		close(hbDone)
	}()
	select {
	case <-acked:
	case <-time.After(8 * time.Second):
		t.Fatal("stop ACK was never posted")
	}
	select {
	case <-hbDone:
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat loop did not exit after the stop")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(stopAcks) != 1 {
		t.Fatalf("expected exactly one stop ACK, got %d", len(stopAcks))
	}
	if stopAcks[0].ProcessStopped {
		t.Fatalf("unconfirmable termination must ACK ProcessStopped=false: %+v", stopAcks[0])
	}
	if stopAcks[0].AttemptID != "attempt" || stopAcks[0].OwnershipEpoch != 1 {
		t.Fatalf("ACK must carry the stopped attempt identity: %+v", stopAcks[0])
	}
}

func TestMaybePublishArtifactDecision(t *testing.T) {
	agent, err := NewAgent(AgentConfig{ControlPlaneURL: "http://127.0.0.1:9"})
	if err != nil {
		t.Fatal(err)
	}
	var published []byte
	agent.PublishArtifactFn = func(_ context.Context, _ AssignmentDTO, data []byte, _ string) (string, error) {
		published = data
		return "artifact-1", nil
	}
	asg := AssignmentDTO{RunID: "r", StepID: "s", AttemptID: "a", OwnershipEpoch: 1}

	// Small inline output passes through untouched.
	id, handled, err := agent.maybePublishArtifact(context.Background(), asg, map[string]any{"ok": true})
	if err != nil || handled || id != "" {
		t.Fatalf("small output must pass through: %q %v %v", id, handled, err)
	}
	// Explicit marker uploads even when small.
	marker := map[string]any{ArtifactUploadMarkerKey: map[string]any{
		"data": base64.StdEncoding.EncodeToString([]byte("bytes")), "contentType": "text/plain",
	}}
	id, handled, err = agent.maybePublishArtifact(context.Background(), asg, marker)
	if err != nil || !handled || id != "artifact-1" || string(published) != "bytes" {
		t.Fatalf("marker must publish: %q %v %v", id, handled, err)
	}
	// Oversize canonical output spills automatically.
	big := map[string]any{"blob": strings.Repeat("x", InlineResultLimitBytes+1)}
	id, handled, err = agent.maybePublishArtifact(context.Background(), asg, big)
	if err != nil || !handled || id != "artifact-1" {
		t.Fatalf("oversize output must spill: %q %v %v", id, handled, err)
	}
	// Over-limit bytes fail closed before any network use.
	huge := make([]byte, MaxArtifactUploadBytes+1)
	_, _, err = agent.maybePublishArtifact(context.Background(), asg,
		map[string]any{ArtifactUploadMarkerKey: map[string]any{
			"data": base64.StdEncoding.EncodeToString(huge), "contentType": "text/plain",
		}})
	if err == nil {
		t.Fatalf("over-limit upload must fail")
	}
	if pub, ok := err.(*ArtifactPublishError); !ok || pub.Code != "ARTIFACT_TOO_LARGE" || pub.Retryable {
		t.Fatalf("over-limit must be non-retryable ARTIFACT_TOO_LARGE, got %+v", err)
	}
}
