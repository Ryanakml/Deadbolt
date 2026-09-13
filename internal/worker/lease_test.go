package worker_test

import (
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/worker"
)

func TestLeaseTrackerMonotonicSafety(t *testing.T) {
	now := time.Now()
	// Expiry in 10s: With 2s margin + 0.5s RTT, safe TTL is 7.5s > 0
	tracker := worker.NewLeaseTracker(now.Add(10*time.Second), 500*time.Millisecond, 2*time.Second)

	safe, err := tracker.SafeRemainingTTL(now)
	if err != nil {
		t.Fatalf("expected safe remaining calculation to succeed: %v", err)
	}
	if safe < 7*time.Second || safe > 8*time.Second {
		t.Fatalf("expected safe TTL ~7.5s, got %v", safe)
	}

	canStart, err := tracker.CanStart(now)
	if err != nil || !canStart {
		t.Fatalf("expected CanStart to be true: %v", err)
	}

	// Insufficient TTL: expiry in only 2.1s (less than 2s margin + 0.5s RTT = 2.5s)
	shortTracker := worker.NewLeaseTracker(now.Add(2100*time.Millisecond), 500*time.Millisecond, 2*time.Second)
	canStart, err = shortTracker.CanStart(now)
	if canStart || err != worker.ErrInsufficientLeaseTTL {
		t.Fatalf("expected ErrInsufficientLeaseTTL when below safety margin, got %v", err)
	}

	// Renewing extends the safe boundary
	shortTracker.Renew(now.Add(15*time.Second), 200*time.Millisecond)
	canStart, err = shortTracker.CanStart(now)
	if err != nil || !canStart {
		t.Fatalf("expected renewed lease to permit start: %v", err)
	}
}

func TestLeaseTrackerConsumesInjectedMonotonicElapsed(t *testing.T) {
	var elapsed time.Duration
	tracker := worker.NewLeaseTrackerWithElapsed(time.Now().Add(10*time.Second), 500*time.Millisecond, 2*time.Second, func() time.Duration { return elapsed })
	before, err := tracker.SafeRemainingTTL(time.Now().Add(-24 * time.Hour)) // hostile wall-clock input is ignored
	if err != nil {
		t.Fatal(err)
	}
	elapsed = 8 * time.Second
	after, err := tracker.SafeRemainingTTL(time.Now().Add(24 * time.Hour))
	if err == nil || after != 0 {
		t.Fatalf("clock jump extended lease: before=%v after=%v err=%v", before, after, err)
	}
}
