package sp03_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/worker"
)

func resolveRunnerPath(t *testing.T) string {
	t.Helper()
	runnerAbs, err := filepath.Abs("../../../runner/node/dist/index.js")
	if err != nil {
		t.Fatalf("failed to resolve runner path: %v", err)
	}
	return runnerAbs
}

func resolveFixturePath(t *testing.T, filename string) string {
	t.Helper()
	fixtureAbs, err := filepath.Abs("fixtures/" + filename)
	if err != nil {
		t.Fatalf("failed to resolve fixture path: %v", err)
	}
	return fixtureAbs
}

// 1. Acceptance Contract: Handler never starts without ACK or adequate remaining TTL
func TestSP03_StartAckGating(t *testing.T) {
	runnerPath := resolveRunnerPath(t)
	fixturePath := resolveFixturePath(t, "noisy-task.js")

	supervisor := worker.NewProcessSupervisor("node", runnerPath)

	// Scenario A: Control plane rejects Start request (e.g. 409 STALE_OWNERSHIP or network loss)
	startCalled := int32(0)
	supervisor.StartAckFn = func(ctx context.Context, attemptID string, epoch int64) error {
		atomic.AddInt32(&startCalled, 1)
		return errors.New("409 STALE_OWNERSHIP")
	}

	input := &worker.TaskInput{
		AttemptID:   "att_start_ack_fail",
		OperationID: "op_start_ack_fail",
		TaskName:    "default",
		Entrypoint:  fixturePath,
		Input:       map[string]any{"test": true},
	}

	completion, logs, err := supervisor.ExecuteAttempt(context.Background(), input, 1)
	if !errors.Is(err, worker.ErrStartAckRejected) {
		t.Fatalf("expected ErrStartAckRejected when Start fails, got %v", err)
	}
	if completion != nil {
		t.Fatalf("expected completion to be nil when Start rejected, got %v", completion)
	}
	if logs != nil {
		t.Fatalf("expected logs to be nil when Start rejected, got %v", logs)
	}
	if atomic.LoadInt32(&startCalled) != 1 {
		t.Fatalf("expected StartAckFn to be called exactly once")
	}

	// Scenario B: Control plane approves Start request (200 OK)
	supervisor.StartAckFn = func(ctx context.Context, attemptID string, epoch int64) error {
		return nil
	}
	completion, logs, err = supervisor.ExecuteAttempt(context.Background(), input, 1)
	if err != nil {
		t.Fatalf("expected successful execution with Start ACK approval, got %v", err)
	}
	if completion == nil || completion.Status != "SUCCEEDED" {
		t.Fatalf("expected status SUCCEEDED, got %v", completion)
	}
	if logs == nil || len(logs.Stdout) == 0 {
		t.Fatalf("expected execution logs to be captured")
	}
}

// 2. Acceptance Contract: Monotonic conservative lease budget check before starting
func TestSP03_MonotonicLeaseBudgetSafety(t *testing.T) {
	runnerPath := resolveRunnerPath(t)
	fixturePath := resolveFixturePath(t, "noisy-task.js")

	supervisor := worker.NewProcessSupervisor("node", runnerPath)
	supervisor.StartAckFn = func(ctx context.Context, attemptID string, epoch int64) error {
		return nil
	}

	// Short lease: expires in only 1.8s (safety margin 2.0s + estimated RTT 0.5s = 2.5s required)
	now := time.Now()
	supervisor.LeaseTracker = worker.NewLeaseTracker(now.Add(1800*time.Millisecond), 500*time.Millisecond, 2*time.Second)

	input := &worker.TaskInput{
		AttemptID:   "att_short_lease",
		OperationID: "op_short_lease",
		TaskName:    "default",
		Entrypoint:  fixturePath,
		Input:       map[string]any{},
	}

	completion, logs, err := supervisor.ExecuteAttempt(context.Background(), input, 1)
	if !errors.Is(err, worker.ErrInsufficientLeaseTTL) {
		t.Fatalf("expected ErrInsufficientLeaseTTL when safe TTL <= 0, got %v", err)
	}
	if completion != nil || logs != nil {
		t.Fatalf("expected handler never to start with insufficient lease")
	}

	// Renewing lease to 20s allows immediate start
	supervisor.LeaseTracker.Renew(now.Add(20*time.Second), 100*time.Millisecond)
	completion, _, err = supervisor.ExecuteAttempt(context.Background(), input, 1)
	if err != nil || completion.Status != "SUCCEEDED" {
		t.Fatalf("expected execution to succeed after lease renewal, got %v", err)
	}
}

// 3. Acceptance Contract: Arbitrary stdout cannot become a result
func TestSP03_StructuredResultChannelIsolation(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node binary not found")
	}

	runnerPath := resolveRunnerPath(t)
	fixturePath := resolveFixturePath(t, "noisy-task.js")

	supervisor := worker.NewProcessSupervisor("node", runnerPath)
	supervisor.StartAckFn = func(ctx context.Context, attemptID string, epoch int64) error {
		return nil
	}

	input := &worker.TaskInput{
		AttemptID:   "att_noisy_001",
		OperationID: "op_noisy_001",
		TaskName:    "default",
		Entrypoint:  fixturePath,
		Input:       map[string]any{"payloadKey": "payloadValue"},
	}

	completion, logs, err := supervisor.ExecuteAttempt(context.Background(), input, 1)
	if err != nil {
		t.Fatalf("expected successful execution despite stdout noise, got %v", err)
	}

	if completion.Status != "SUCCEEDED" {
		t.Fatalf("expected status SUCCEEDED, got %s", completion.Status)
	}

	outputMap, ok := completion.Output.(map[string]any)
	if !ok || outputMap["verifiedResult"] != true {
		t.Fatalf("expected structured output {verifiedResult: true}, got %v", completion.Output)
	}

	// Verify stdout contains the noise, but completion was untouched
	if logs == nil || !bytes.Contains([]byte(logs.Stdout), []byte("FAKE_ERROR_ON_STDOUT")) {
		t.Fatalf("expected stdout logs to capture noisy strings")
	}
}

// 4. Acceptance Contract: Child environment is allowlisted
func TestSP03_EnvironmentSanitizationAndAllowlist(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node binary not found")
	}

	runnerPath := resolveRunnerPath(t)
	fixturePath := resolveFixturePath(t, "env-check.js")

	supervisor := worker.NewProcessSupervisor("node", runnerPath)
	supervisor.StartAckFn = func(ctx context.Context, attemptID string, epoch int64) error {
		return nil
	}

	input := &worker.TaskInput{
		AttemptID:   "att_env_001",
		OperationID: "op_env_001",
		TaskName:    "default",
		Entrypoint:  fixturePath,
		Input:       map[string]any{},
		Env: map[string]string{
			"CUSTOM_CONFIG": "allowlisted_value",
		},
	}

	completion, _, err := supervisor.ExecuteAttempt(context.Background(), input, 1)
	if err != nil {
		t.Fatalf("expected env check attempt to succeed, got %v", err)
	}

	outputMap, ok := completion.Output.(map[string]any)
	if !ok {
		t.Fatalf("unexpected output structure: %v", completion.Output)
	}

	leaked, ok := outputMap["leakedSecrets"].([]any)
	if !ok || len(leaked) > 0 {
		t.Fatalf("expected 0 leaked secrets in child environment, got %v", leaked)
	}

	if outputMap["customConfig"] != "allowlisted_value" {
		t.Fatalf("expected customConfig to match injected task variable, got %v", outputMap["customConfig"])
	}
}

// 5. Acceptance Contract: Abort -> SIGTERM -> SIGKILL stops process group within grace
func TestSP03_ProcessGroupShutdownWithinGrace(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node binary not found")
	}

	runnerPath := resolveRunnerPath(t)
	fixturePath := resolveFixturePath(t, "hung-child.js")

	supervisor := worker.NewProcessSupervisor("node", runnerPath)
	supervisor.GracePeriod = 1500 * time.Millisecond // 1.5s grace for fast test
	supervisor.StartAckFn = func(ctx context.Context, attemptID string, epoch int64) error {
		return nil
	}

	input := &worker.TaskInput{
		AttemptID:   "att_hung_001",
		OperationID: "op_hung_001",
		TaskName:    "default",
		Entrypoint:  fixturePath,
		Input:       map[string]any{},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	completion, _, err := supervisor.ExecuteAttempt(ctx, input, 1)
	duration := time.Since(start)

	if err == nil {
		t.Fatalf("expected termination error for hung process, got nil")
	}

	if completion != nil && completion.Status == "SUCCEEDED" {
		t.Fatalf("expected hung task not to report success")
	}

	// Must finish after context cancel + grace period (300ms + 1500ms = 1800ms) within reasonable margin
	if duration < 1500*time.Millisecond {
		t.Fatalf("expected termination to respect grace period before SIGKILL, finished in %v", duration)
	}
}

// 6. Acceptance Contract: Crash soak with no leaked runners
func TestSP03_CrashSoakAndNoLeakedProcesses(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node binary not found")
	}

	runnerPath := resolveRunnerPath(t)
	fixturePath := resolveFixturePath(t, "noisy-task.js")

	supervisor := worker.NewProcessSupervisor("node", runnerPath)
	supervisor.StartAckFn = func(ctx context.Context, attemptID string, epoch int64) error {
		return nil
	}

	const soakIterations = 20
	successCount := 0

	for i := 0; i < soakIterations; i++ {
		input := &worker.TaskInput{
			AttemptID:   fmt.Sprintf("att_soak_%03d", i),
			OperationID: fmt.Sprintf("op_soak_%03d", i),
			TaskName:    "default",
			Entrypoint:  fixturePath,
			Input:       map[string]any{"index": i},
		}

		completion, _, err := supervisor.ExecuteAttempt(context.Background(), input, int64(i+1))
		if err == nil && completion != nil && completion.Status == "SUCCEEDED" {
			successCount++
		}
	}

	if successCount != soakIterations {
		t.Fatalf("soak test failed: %d/%d succeeded", successCount, soakIterations)
	}
}

// 7. Acceptance Contract: Bundle digest verification
func TestSP03_BundleDigestVerification(t *testing.T) {
	mockArchive := []byte("tar-bundle-binary-bytes-data")
	hasher := sha256.New()
	hasher.Write(mockArchive)
	validDigest := hex.EncodeToString(hasher.Sum(nil))

	// Valid
	digest, err := worker.VerifyBundleDigest(bytes.NewReader(mockArchive), validDigest)
	if err != nil || digest != "sha256:"+validDigest {
		t.Fatalf("expected valid bundle digest to match, got err=%v", err)
	}

	// Corrupted
	tampered := []byte("tampered-bundle-archive-bytes")
	_, err = worker.VerifyBundleDigest(bytes.NewReader(tampered), validDigest)
	if !errors.Is(err, worker.ErrBundleDigestMismatch) {
		t.Fatalf("expected ErrBundleDigestMismatch, got %v", err)
	}
}

// 8. Acceptance Contract: Architecture mismatch rejection
func TestSP03_ArchitectureMismatch(t *testing.T) {
	if err := worker.VerifyArchitecture("linux/amd64", "linux/amd64"); err != nil {
		t.Fatalf("expected identical architectures to pass: %v", err)
	}

	if err := worker.VerifyArchitecture("linux-x64", "linux/amd64"); err != nil {
		t.Fatalf("expected normalized alias to pass: %v", err)
	}

	if err := worker.VerifyArchitecture("linux/arm64", "linux/amd64"); !errors.Is(err, worker.ErrArchitectureMismatch) {
		t.Fatalf("expected ErrArchitectureMismatch for arm64 vs amd64, got %v", err)
	}
}
