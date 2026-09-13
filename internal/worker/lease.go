package worker

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrInsufficientLeaseTTL = errors.New("INSUFFICIENT_LEASE_TTL: Safe remaining lease duration is below safety boundary; cannot start handler")
	ErrLeaseExpired         = errors.New("LEASE_EXPIRED: Monotonic lease deadline expired without verified renewal")
)

const (
	DefaultSafetyMargin = 2 * time.Second
	DefaultEstimatedRTT = 500 * time.Millisecond
)

// LeaseTracker turns a server wall-clock expiry into a local duration at the
// Start/renewal ACK boundary. From then on it consumes only a monotonic elapsed
// clock, so a wall-clock correction cannot extend execution rights.
type LeaseTracker struct {
	mu           sync.RWMutex
	budget       time.Duration
	safetyMargin time.Duration
	estimatedRTT time.Duration
	ackElapsed   time.Duration
	elapsed      func() time.Duration
	now          func() time.Time
}

func NewLeaseTracker(expiresAt time.Time, rtt, margin time.Duration) *LeaseTracker {
	started := time.Now()
	return NewLeaseTrackerWithElapsed(expiresAt, rtt, margin, func() time.Duration { return time.Since(started) })
}

// NewLeaseTrackerWithElapsed exists for deterministic lifecycle tests. elapsed
// must be monotonic; time.Since is used by production code.
func NewLeaseTrackerWithElapsed(expiresAt time.Time, rtt, margin time.Duration, elapsed func() time.Duration) *LeaseTracker {
	if margin <= 0 {
		margin = DefaultSafetyMargin
	}
	if rtt <= 0 {
		rtt = DefaultEstimatedRTT
	}
	if elapsed == nil {
		started := time.Now()
		elapsed = func() time.Duration { return time.Since(started) }
	}
	wallNow := time.Now()
	return &LeaseTracker{
		budget: expiresAt.Sub(wallNow), safetyMargin: margin, estimatedRTT: rtt,
		ackElapsed: elapsed(), elapsed: elapsed, now: time.Now,
	}
}

func (lt *LeaseTracker) safeRemainingLocked() (time.Duration, error) {
	raw := lt.budget - (lt.elapsed() - lt.ackElapsed)
	safe := raw - lt.estimatedRTT - lt.safetyMargin
	if safe <= 0 {
		return 0, ErrInsufficientLeaseTTL
	}
	return safe, nil
}

// SafeRemainingTTL keeps its argument for source compatibility. It is ignored:
// caller-provided wall time is not an execution authority.
func (lt *LeaseTracker) SafeRemainingTTL(_ time.Time) (time.Duration, error) {
	lt.mu.RLock()
	defer lt.mu.RUnlock()
	return lt.safeRemainingLocked()
}

func (lt *LeaseTracker) CanStart(_ time.Time) (bool, error) {
	_, err := lt.SafeRemainingTTL(time.Time{})
	return err == nil, err
}

// Renew establishes a fresh monotonic budget at a verified renewal ACK.
func (lt *LeaseTracker) Renew(newExpiresAt time.Time, measuredRTT time.Duration) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.budget = newExpiresAt.Sub(lt.now())
	if measuredRTT > 0 {
		lt.estimatedRTT = measuredRTT
	}
	lt.ackElapsed = lt.elapsed()
}

func (lt *LeaseTracker) IsExpired(_ time.Time) bool {
	_, err := lt.SafeRemainingTTL(time.Time{})
	return err != nil
}
