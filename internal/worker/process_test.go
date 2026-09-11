package worker_test

import (
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/worker"
)

func resolveRunnerPath(t *testing.T) string {
	t.Helper()
	runnerAbs, err := filepath.Abs("../../runner/node/dist/index.js")
	if err != nil {
		t.Fatalf("failed to resolve runner path: %v", err)
	}
	return runnerAbs
}

func resolveFixturePath(t *testing.T, fixtureName string) string {
	t.Helper()
	fixtureAbs, err := filepath.Abs("../../runner/node/tests/fixtures/" + fixtureName)
	if err != nil {
		t.Fatalf("failed to resolve fixture path: %v", err)
	}
	return fixtureAbs
}

func TestStartAckGating(t *testing.T) {
	runnerPath := resolveRunnerPath(t)
	fixturePath := resolveFixturePath(t, "sample-task.js")

	supervisor := worker.NewProcessSupervisor("node", runnerPath)

	// Scenario 1: Start ACK Rejected by control plane (e.g. 409 STALE_OWNERSHIP)
	supervisor.StartAckFn = func(ctx context.Context, attemptID string, epoch int64) error {
		return errors.New("409 STALE_OWNERSHIP")
	}

	input := &worker.TaskInput{
		AttemptID:   "att_start_ack_rejected",
		OperationID: "op_001",
		TaskName:    "sampleTask",
		Entrypoint:  fixturePath,
		Input:       map[string]any{"x": 5, "y": 10},
	}

	completion, logs, err := supervisor.ExecuteAttempt(context.Background(), input, 1)
	if err == nil {
		t.Fatalf("expected Start ACK rejection error, got nil")
	}
	if !errors.Is(err, worker.ErrStartAckRejected) {
		t.Fatalf("expected ErrStartAckRejected, got %v", err)
	}
	if completion != nil {
		t.Fatalf("expected no completion payload when Start ACK rejected, got %v", completion)
	}
	if logs != nil {
		t.Fatalf("expected no execution logs when Start ACK rejected, got %v", logs)
	}
}

func TestLeaseGatingBeforeStart(t *testing.T) {
	runnerPath := resolveRunnerPath(t)
	fixturePath := resolveFixturePath(t, "sample-task.js")

	supervisor := worker.NewProcessSupervisor("node", runnerPath)

	// Lease expiring immediately (insufficient safe TTL < 2.5s)
	now := time.Now()
	supervisor.LeaseTracker = worker.NewLeaseTracker(now.Add(1*time.Second), 500*time.Millisecond, 2*time.Second)

	input := &worker.TaskInput{
		AttemptID:   "att_insufficient_lease",
		OperationID: "op_002",
		TaskName:    "sampleTask",
		Entrypoint:  fixturePath,
		Input:       map[string]any{"x": 1, "y": 2},
	}

	completion, logs, err := supervisor.ExecuteAttempt(context.Background(), input, 1)
	if !errors.Is(err, worker.ErrInsufficientLeaseTTL) {
		t.Fatalf("expected ErrInsufficientLeaseTTL, got %v", err)
	}
	if completion != nil || logs != nil {
		t.Fatalf("expected no execution when lease insufficient")
	}
}

func TestExecuteAttemptSuccessAndChannelIsolation(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node binary not found in PATH")
	}

	runnerPath := resolveRunnerPath(t)
	fixturePath := resolveFixturePath(t, "sample-task.js")

	supervisor := worker.NewProcessSupervisor("node", runnerPath)
	supervisor.StartAckFn = func(ctx context.Context, attemptID string, epoch int64) error {
		return nil // 200 OK ACK
	}

	input := &worker.TaskInput{
		AttemptID:   "att_success_001",
		OperationID: "op_003",
		TaskName:    "noisyTask", // task that writes arbitrary noise to stdout/stderr
		Entrypoint:  fixturePath,
		Input:       map[string]any{},
	}

	completion, logs, err := supervisor.ExecuteAttempt(context.Background(), input, 1)
	if err != nil {
		t.Fatalf("expected successful attempt execution, got %v", err)
	}

	if completion.Status != "SUCCEEDED" {
		t.Fatalf("expected status SUCCEEDED, got %s (err: %v)", completion.Status, completion.Error)
	}

	// Verify structured output was isolated from stdout noise
	outMap, ok := completion.Output.(map[string]any)
	if !ok || outMap["clean"] != true {
		t.Fatalf("expected output {clean: true}, got %v", completion.Output)
	}

	// Verify stdout and stderr logs captured the noisy logs without breaking the completion
	if logs == nil || len(logs.Stdout) == 0 {
		t.Fatalf("expected stdout logs to be captured in ExecutionLogs")
	}
}

func TestProcessTerminationOnContextCancel(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node binary not found in PATH")
	}

	runnerPath := resolveRunnerPath(t)
	fixturePath := resolveFixturePath(t, "sample-task.js")

	supervisor := worker.NewProcessSupervisor("node", runnerPath)
	supervisor.GracePeriod = 2 * time.Second
	supervisor.StartAckFn = func(ctx context.Context, attemptID string, epoch int64) error {
		return nil
	}

	input := &worker.TaskInput{
		AttemptID:   "att_timeout_001",
		OperationID: "op_004",
		TaskName:    "slowTask", // slow task that waits 5 seconds
		Entrypoint:  fixturePath,
		Input:       map[string]any{},
	}

	// Cancel context after 200ms to trigger graceful abort sequence
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	completion, _, err := supervisor.ExecuteAttempt(ctx, input, 1)
	if err == nil {
		t.Fatalf("expected context timeout / termination error, got nil")
	}

	if completion != nil && completion.Status == "SUCCEEDED" {
		t.Fatalf("expected aborted task not to succeed")
	}
}
