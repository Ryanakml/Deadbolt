package worker_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
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

func TestVerifiedBundleCannotExecuteDifferentEntrypoint(t *testing.T) {
	verified, err := os.CreateTemp(t.TempDir(), "verified-*.js")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verified.WriteString("export default () => 'verified'\n"); err != nil {
		t.Fatal(err)
	}
	if err := verified.Close(); err != nil {
		t.Fatal(err)
	}
	other, err := os.CreateTemp(t.TempDir(), "other-*.js")
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(verified.Name())
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(contents)
	s := worker.NewProcessSupervisor("definitely-not-node", "ignored")
	s.LeaseTracker = worker.NewLeaseTracker(time.Now().Add(time.Minute), 0, 0)
	s.StartAckFn = func(context.Context, string, int64) error { return nil }
	started := false
	s.OnProcessStart = func(int) { started = true }
	_, _, err = s.ExecuteAttempt(context.Background(), &worker.TaskInput{AttemptID: "a", Entrypoint: other.Name(), Bundle: &worker.BundleSpec{Path: verified.Name(), SHA256: hex.EncodeToString(digest[:]), TargetArch: worker.CurrentHostArchitecture()}}, 1)
	if !errors.Is(err, worker.ErrBundleEntrypoint) || started {
		t.Fatalf("err=%v started=%v", err, started)
	}
}

func TestDigestAndArchitectureRejectBeforeProcessStart(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "bundle-*.js")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("export default () => 'ok'\n"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	start := false
	newSupervisor := func() *worker.ProcessSupervisor {
		s := worker.NewProcessSupervisor("definitely-not-node", "ignored")
		s.LeaseTracker = worker.NewLeaseTracker(time.Now().Add(time.Minute), 0, 0)
		s.StartAckFn = func(context.Context, string, int64) error { return nil }
		s.OnProcessStart = func(int) { start = true }
		return s
	}
	input := &worker.TaskInput{AttemptID: "a", Entrypoint: file.Name(), Bundle: &worker.BundleSpec{Path: file.Name(), SHA256: "00", TargetArch: worker.CurrentHostArchitecture()}}
	_, _, err = newSupervisor().ExecuteAttempt(context.Background(), input, 1)
	if !errors.Is(err, worker.ErrBundleDigestMismatch) || start {
		t.Fatalf("digest err=%v started=%v", err, start)
	}
	input.Bundle.SHA256 = hex.EncodeToString(make([]byte, 32))
	input.Bundle.TargetArch = "linux/not-this-host"
	_, _, err = newSupervisor().ExecuteAttempt(context.Background(), input, 1)
	if !errors.Is(err, worker.ErrArchitectureMismatch) || start {
		t.Fatalf("arch err=%v started=%v", err, start)
	}
}

func TestAmbiguousStartRetriesSameIdentityAndNeverLaunchesWithoutDecision(t *testing.T) {
	s := worker.NewProcessSupervisor("definitely-not-node", "ignored")
	s.LeaseTracker = worker.NewLeaseTracker(time.Now().Add(time.Minute), 0, 0)
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

func TestExecutionFailsClosedWithoutStartOrLeaseAuthority(t *testing.T) {
	input := &worker.TaskInput{AttemptID: "never-starts"}
	s := worker.NewProcessSupervisor("definitely-not-node", "ignored")
	started := false
	s.OnProcessStart = func(int) { started = true }
	_, _, err := s.ExecuteAttempt(context.Background(), input, 1)
	if !errors.Is(err, worker.ErrLeaseAuthorityMissing) || started {
		t.Fatalf("err=%v started=%v", err, started)
	}
	s.LeaseTracker = worker.NewLeaseTracker(time.Now().Add(time.Minute), 0, 0)
	_, _, err = s.ExecuteAttempt(context.Background(), input, 1)
	if !errors.Is(err, worker.ErrStartAuthorityMissing) || started {
		t.Fatalf("err=%v started=%v", err, started)
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
