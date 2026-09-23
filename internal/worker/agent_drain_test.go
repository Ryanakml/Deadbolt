package worker

import (
	"context"
	"os/exec"
	"testing"
	"time"
)

// TestAgentDrainFinishesActiveAttemptBeforeGrace proves Drain lets an active
// attempt finish within grace: the runner completes (entry removed, as
// executeAssignment does on completion), so Drain returns on the polling tick
// without invoking the attempt's cancel, i.e. without force-stopping it.
// Short test-specific grace is injected; the 60s production default is
// asserted separately in integration (TestWorkerDrainGraceAndRunnerStop).
func TestAgentDrainFinishesActiveAttemptBeforeGrace(t *testing.T) {
	agent, err := NewAgent(AgentConfig{
		ControlPlaneURL:  "http://127.0.0.1:8080",
		DrainGracePeriod: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	cancelCalled := make(chan struct{}, 1)
	agent.mu.Lock()
	agent.runningAttempts["attempt-finish"] = &activeAttempt{
		attemptID: "attempt-finish",
		cancel:    func() { cancelCalled <- struct{}{} },
	}
	agent.mu.Unlock()
	// Simulate the runner finishing before grace: executeAssignment removes
	// the entry on completion.
	go func() {
		time.Sleep(100 * time.Millisecond)
		agent.mu.Lock()
		delete(agent.runningAttempts, "attempt-finish")
		agent.mu.Unlock()
	}()
	start := time.Now()
	agent.Drain(context.Background())
	elapsed := time.Since(start)
	if elapsed >= 2*time.Second {
		t.Fatalf("Drain blocked until grace expiry (%v) despite finished runner", elapsed)
	}
	select {
	case <-cancelCalled:
		t.Fatal("Drain cancelled an attempt that had already finished")
	default:
	}
	agent.mu.RLock()
	remaining := len(agent.runningAttempts)
	draining := agent.draining
	agent.mu.RUnlock()
	if remaining != 0 {
		t.Fatalf("expected no remaining attempts, got %d", remaining)
	}
	if !draining {
		t.Fatal("expected agent to remain in draining state")
	}
}

// TestAgentDrainForceStopsRunnerAfterGrace proves that when an active child
// process outlives the grace period, Drain stops the actual remaining
// runner process group and cancels the attempt instead of returning while
// work continues locally.
func TestAgentDrainForceStopsRunnerAfterGrace(t *testing.T) {
	agent, err := NewAgent(AgentConfig{
		ControlPlaneURL:  "http://127.0.0.1:8080",
		DrainGracePeriod: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	agent.supervisor.GracePeriod = 50 * time.Millisecond
	cmd := exec.Command("sh", "-c", "sleep 30")
	configureProcessGroup(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	cancelled := make(chan struct{})
	agent.mu.Lock()
	agent.runningAttempts["attempt-stuck"] = &activeAttempt{
		attemptID: "attempt-stuck", pid: cmd.Process.Pid,
		cancel: func() { close(cancelled) },
	}
	agent.mu.Unlock()
	start := time.Now()
	agent.Drain(context.Background())
	elapsed := time.Since(start)
	if elapsed < 300*time.Millisecond {
		t.Fatalf("Drain returned after %v, before grace expiry, with runner still active", elapsed)
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("stuck attempt was not cancelled after grace")
	}
	agent.mu.RLock()
	remaining := len(agent.runningAttempts)
	agent.mu.RUnlock()
	if remaining != 0 {
		t.Fatalf("expected runners stopped after grace, %d remaining", remaining)
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("child process group survived Drain force-stop")
	}
}
