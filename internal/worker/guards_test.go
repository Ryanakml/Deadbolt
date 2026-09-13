package worker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/worker"
)

func TestStartRechecksMonotonicLeaseBeforeLaunch(t *testing.T) {
	var elapsed time.Duration
	now := time.Now()
	// Initial safe budget is 500ms, then Start consumes 600ms.
	tracker := worker.NewLeaseTrackerWithElapsed(now.Add(2500*time.Millisecond), 0, 0, func() time.Duration { return elapsed })
	s := worker.NewProcessSupervisor("definitely-not-node", "ignored")
	s.LeaseTracker = tracker
	s.StartFn = func(context.Context, string, int64) (worker.StartDecision, error) {
		elapsed = 600 * time.Millisecond
		return worker.StartAccepted, nil
	}
	_, _, err := s.ExecuteAttempt(context.Background(), &worker.TaskInput{AttemptID: "a"}, 1)
	if !errors.Is(err, worker.ErrInsufficientLeaseTTL) {
		t.Fatalf("got %v", err)
	}
}

func TestAmbiguousStartRetriesSameIdentityAndNeverLaunchesWithoutDecision(t *testing.T) {
	s := worker.NewProcessSupervisor("definitely-not-node", "ignored")
	calls := 0
	s.StartFn = func(_ context.Context, attemptID string, epoch int64) (worker.StartDecision, error) {
		if attemptID != "attempt" || epoch != 7 {
			t.Fatalf("Start identity changed")
		}
		calls++
		if calls == 1 {
			return worker.StartAmbiguous, nil
		}
		return worker.StartAccepted, nil
	}
	_, _, err := s.ExecuteAttempt(context.Background(), &worker.TaskInput{AttemptID: "attempt"}, 7)
	if !errors.Is(err, worker.ErrBundleVerificationNeed) || calls != 2 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}

	s.StartFn = func(context.Context, string, int64) (worker.StartDecision, error) { return worker.StartAmbiguous, nil }
	_, _, err = s.ExecuteAttempt(context.Background(), &worker.TaskInput{AttemptID: "attempt"}, 7)
	if !errors.Is(err, worker.ErrStartAckAmbiguous) {
		t.Fatalf("got %v", err)
	}
}

func TestTaskEnvironmentRequiresManifestAllowlist(t *testing.T) {
	if err := worker.ValidateTaskEnvironment(map[string]string{"UNDECLARED": "x"}, nil); err == nil {
		t.Fatal("undeclared key accepted")
	}
	if err := worker.ValidateTaskEnvironment(map[string]string{"NODE_OPTIONS": "--require evil"}, []string{"NODE_OPTIONS"}); err == nil {
		t.Fatal("reserved key accepted")
	}
	if err := worker.ValidateTaskEnvironment(map[string]string{"CUSTOM": "ok"}, []string{"CUSTOM"}); err != nil {
		t.Fatal(err)
	}
}
