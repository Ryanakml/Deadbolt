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
	// DefaultSafetyMargin is the conservative margin required by Blueprint §13.1.
	DefaultSafetyMargin = 2 * time.Second
	// DefaultEstimatedRTT is the baseline round-trip time allowance.
	DefaultEstimatedRTT = 500 * time.Millisecond
)

// LeaseTracker evaluates monotonic conservative lease duration per Blueprint §13.1.
// The agent calculates safe TTL = (expires_at - now) - RTT - 2s margin.
type LeaseTracker struct {
	mu           sync.RWMutex
	expiresAt    time.Time
	safetyMargin time.Duration
	estimatedRTT time.Duration
	lastRenewed  time.Time
}

// NewLeaseTracker creates a tracker with explicit or default safety margins.
func NewLeaseTracker(expiresAt time.Time, rtt time.Duration, margin time.Duration) *LeaseTracker {
	if margin <= 0 {
		margin = DefaultSafetyMargin
	}
	if rtt <= 0 {
		rtt = DefaultEstimatedRTT
	}

	return &LeaseTracker{
		expiresAt:    expiresAt,
		safetyMargin: margin,
		estimatedRTT: rtt,
		lastRenewed:  time.Now(),
	}
}

// SafeRemainingTTL computes the conservative remaining duration before expiration.
// If the safe remaining duration is <= 0, ErrInsufficientLeaseTTL is returned.
func (lt *LeaseTracker) SafeRemainingTTL(now time.Time) (time.Duration, error) {
	lt.mu.RLock()
	defer lt.mu.RUnlock()

	rawRemaining := lt.expiresAt.Sub(now)
	safeRemaining := rawRemaining - lt.estimatedRTT - lt.safetyMargin

	if safeRemaining <= 0 {
		return 0, ErrInsufficientLeaseTTL
	}
	return safeRemaining, nil
}

// CanStart asserts whether the attempt has adequate safe TTL to begin execution.
func (lt *LeaseTracker) CanStart(now time.Time) (bool, error) {
	safe, err := lt.SafeRemainingTTL(now)
	if err != nil {
		return false, err
	}
	return safe > 0, nil
}

// Renew updates the authoritative lease expiry and measured RTT after a successful heartbeat.
func (lt *LeaseTracker) Renew(newExpiresAt time.Time, measuredRTT time.Duration) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	lt.expiresAt = newExpiresAt
	if measuredRTT > 0 {
		lt.estimatedRTT = measuredRTT
	}
	lt.lastRenewed = time.Now()
}

// IsExpired checks if the lease has exceeded its conservative safe boundary.
func (lt *LeaseTracker) IsExpired(now time.Time) bool {
	_, err := lt.SafeRemainingTTL(now)
	return err != nil
}
