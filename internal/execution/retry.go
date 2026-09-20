package execution

import (
	"crypto/rand"
	"encoding/binary"
	"math"
	"strconv"
	"strings"
	"time"
)

// Durable retry/timer contract for Blueprint §15.1, §14.1 and §17.
//
// Defaults (authoritative, do not change without blueprint/ADR update):
//   - maxAttempts default 3, maximum 10, including the first attempt.
//   - exponential backoff cap = min(maxDelayMs, initialDelayMs * 2^(failedAttempt-1))
//     with full jitter uniform in [0, cap]. Due timestamp is chosen once and
//     persisted; restart must not redraw jitter or reset the timer.
//   - Retry-After may only increase the delay, capped at maxDelayMs.
//   - Claim-to-start budget is 5s; attempt budget and run deadline/hold/pause/
//     cancel guards apply before scheduling or firing a retry.
//   - Idempotency window (recovery=idempotent) is first-claim + window, never
//     extended; insufficient window routes to reconciliation, never to blind retry.

const (
	DefaultMaxAttempts      = 3
	MaxMaxAttempts          = 10
	MinMaxAttempts          = 1
	DefaultInitialDelayMs   = 1000
	DefaultMaxDelayMs       = 30000
	DefaultAttemptTimeoutMs = 300000
	// MaxAttemptTimeoutMs is the execution backstop for the attempt deadline
	// (Blueprint §15.2: default 5m, maximum 1h). Registration validation
	// enforces the same bound; the engine clamps persisted outliers.
	MaxAttemptTimeoutMs = 3600000
	// RunLifetimeMs is the MVP run-deadline default and maximum: 24 hours
	// from create-run acceptance (Blueprint §15.2).
	RunLifetimeMs = int64(24 * 60 * 60 * 1000)
	// CancelGraceMs bounds cancellation settlement: CANCELLING settles to
	// CANCELLED after all stop ACKs or when this grace expires (§15.2).
	CancelGraceMs = int64(10 * 1000)

	// ClaimStartBudgetMs mirrors worker.ClaimStartDeadline (5s) to avoid an
	// import cycle with internal/worker. Keep in sync; covered by unit test.
	ClaimStartBudgetMs = int64(5000)
)

// RetryPolicy is the normalized per-task retry contract.
type RetryPolicy struct {
	MaxAttempts         int
	InitialDelayMs      int64
	MaxDelayMs          int64
	TimeoutMs           int64
	Recovery            string
	IdempotencyWindowMs *int64
}

// NormalizeRetryPolicy clamps raw manifest values to the canonical defaults
// and limits. maxAttempts is clamped to [1,10] with default 3.
//
// Recovery policy is NEVER defaulted: Deadbolt requires explicit
// safe/idempotent/reconcile semantics, so an absent/invalid value is
// preserved as-is and the engine must fail closed (no automatic retry).
// Manifest/schema validation should reject such deployments first; this is
// the execution-domain backstop against malformed persisted manifests.
func NormalizeRetryPolicy(maxAttempts int, initialDelayMs, maxDelayMs, timeoutMs int64, recovery string, idempotencyWindowMs *int64) RetryPolicy {
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxAttempts
	}
	if maxAttempts < MinMaxAttempts {
		maxAttempts = MinMaxAttempts
	}
	if maxAttempts > MaxMaxAttempts {
		maxAttempts = MaxMaxAttempts
	}
	if initialDelayMs < 0 {
		initialDelayMs = DefaultInitialDelayMs
	}
	if initialDelayMs == 0 && maxDelayMs == 0 {
		// Both unset: apply blueprint defaults.
		initialDelayMs = DefaultInitialDelayMs
		maxDelayMs = DefaultMaxDelayMs
	} else if initialDelayMs == 0 {
		// Preserve explicit zero initial delay (immediate retry) when max is set.
		initialDelayMs = 0
	}
	if maxDelayMs <= 0 {
		maxDelayMs = DefaultMaxDelayMs
	}
	if timeoutMs <= 0 {
		timeoutMs = DefaultAttemptTimeoutMs
	}
	if timeoutMs > MaxAttemptTimeoutMs {
		timeoutMs = MaxAttemptTimeoutMs
	}
	return RetryPolicy{
		MaxAttempts:         maxAttempts,
		InitialDelayMs:      initialDelayMs,
		MaxDelayMs:          maxDelayMs,
		TimeoutMs:           timeoutMs,
		Recovery:            recovery,
		IdempotencyWindowMs: idempotencyWindowMs,
	}
}

// BackoffCapMs returns min(maxDelayMs, initialDelayMs * 2^(failedAttemptNumber-1)).
// failedAttemptNumber is 1-indexed (1 = first attempt just failed).
func BackoffCapMs(failedAttemptNumber int, initialDelayMs, maxDelayMs int64) int64 {
	if failedAttemptNumber < 1 {
		failedAttemptNumber = 1
	}
	if maxDelayMs <= 0 {
		maxDelayMs = DefaultMaxDelayMs
	}
	if initialDelayMs <= 0 {
		return 0
	}
	// Guard overflow: shift beyond 62 would overflow int64.
	shift := failedAttemptNumber - 1
	var base int64
	if shift >= 62 {
		base = math.MaxInt64
	} else {
		// initial * 2^shift with overflow saturation.
		if initialDelayMs > math.MaxInt64>>shift {
			base = math.MaxInt64
		} else {
			base = initialDelayMs << shift
		}
	}
	if base > maxDelayMs {
		base = maxDelayMs
	}
	if base < 0 {
		base = 0
	}
	return base
}

// JitteredDelayMs applies full jitter: uniform integer in [0, capMs].
// sample must be in [0,1); values outside are clamped.
func JitteredDelayMs(capMs int64, sample float64) int64 {
	if capMs <= 0 {
		return 0
	}
	if sample < 0 {
		sample = 0
	}
	if sample >= 1 {
		// sample==1 would give cap+1 with floor; clamp to cap.
		return capMs
	}
	return int64(sample * float64(capMs))
}

// SampleJitter returns a crypto-random float64 in [0,1) for production use.
// Tests inject deterministic samples instead.
func SampleJitter() float64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0.5
	}
	v := binary.BigEndian.Uint64(b[:])
	// Use 53 bits for float64 mantissa precision.
	return float64(v>>11) / float64(1<<53)
}

// ComputeRetryDelayMs returns the persisted retry delay: full-jitter backoff,
// raised by Retry-After when present, capped at maxDelayMs.
func ComputeRetryDelayMs(failedAttemptNumber int, initialDelayMs, maxDelayMs int64, jitterSample float64, retryAfterMs *int64) int64 {
	capMs := BackoffCapMs(failedAttemptNumber, initialDelayMs, maxDelayMs)
	delay := JitteredDelayMs(capMs, jitterSample)
	if retryAfterMs != nil {
		ra := *retryAfterMs
		if ra < 0 {
			ra = 0
		}
		if ra > maxDelayMs && maxDelayMs > 0 {
			ra = maxDelayMs
		}
		if ra > delay {
			delay = ra
		}
	}
	return delay
}

// ParseRetryAfter parses a Retry-After header value per RFC 9110 §13.1.1:
// either delay-seconds (non-negative integer) or an HTTP-date.
// Returns delay in milliseconds. ok=false means absent/invalid (ignore it).
func ParseRetryAfter(value string, now time.Time) (delayMs int64, ok bool) {
	s := strings.TrimSpace(value)
	if s == "" {
		return 0, false
	}
	if secs, err := strconv.ParseInt(s, 10, 64); err == nil {
		if secs < 0 {
			return 0, false
		}
		if secs > math.MaxInt64/1000 {
			// Saturate instead of overflowing: downstream capping turns this
			// into maxDelayMs, never into a negative/immediate retry.
			return math.MaxInt64, true
		}
		return secs * 1000, true
	}
	// HTTP-date: try common IMF/RFC850/asctime layouts.
	layouts := []string{
		time.RFC1123,
		time.RFC1123Z,
		time.RFC850,
		time.ANSIC,
		"Mon, 02 Jan 2006 15:04:05 GMT",
		"Monday, 02-Jan-06 15:04:05 MST",
		"Mon Jan _2 15:04:05 2006",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			d := t.Sub(now)
			if d < 0 {
				return 0, true
			}
			ms := d.Milliseconds()
			return ms, true
		}
	}
	return 0, false
}

// IsValidRecoveryPolicy reports whether a recovery mode is an explicit
// Deadbolt contract value. Absent/invalid policy must fail closed: it is
// never reinterpreted as permission to automatically repeat customer work.
func IsValidRecoveryPolicy(recovery string) bool {
	switch strings.ToLower(strings.TrimSpace(recovery)) {
	case "safe", "idempotent", "reconcile":
		return true
	default:
		return false
	}
}

// NonRetryableCodes are deterministic contract failures that must never
// consume retry budget, per Blueprint §15.1.
var nonRetryableCodes = map[string]struct{}{
	"INPUT_MAPPING_ERROR":     {},
	"OUTPUT_MAPPING_ERROR":    {},
	"OUTPUT_SCHEMA_VIOLATION": {},
	"SCHEMA_VIOLATION":        {},
	"SCHEMA_VALIDATION_ERROR": {},
	"INVALID_INPUT":           {},
	"INVALID_OUTPUT":          {},
	"MISSING_TASK":            {},
	"MISSING_TASK_REF":        {},
	"TASK_NOT_FOUND":          {},
	"INCOMPATIBLE_BUNDLE":     {},
	"INCOMPATIBLE_DEPLOYMENT": {},
	"UNSUPPORTED_CAPABILITY":  {},
	"UNAUTHORIZED":            {},
	"AUTH_ERROR":              {},
	"PERMISSION_DENIED":       {},
	"FORBIDDEN":               {},
}

// IsNonRetryableCode reports whether an error code is deterministically
// non-retryable and must fail fast without a timer.
func IsNonRetryableCode(code string) bool {
	if code == "" {
		return false
	}
	upper := strings.ToUpper(strings.TrimSpace(code))
	_, ok := nonRetryableCodes[upper]
	return ok
}

// HasRetryBudget reports whether another attempt may be scheduled.
// nextAttemptNumber is the 1-indexed number the next claim would use
// (run_steps.next_attempt_number). Attempts include the first.
func HasRetryBudget(nextAttemptNumber, maxAttempts int) bool {
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxAttempts
	}
	return nextAttemptNumber <= maxAttempts && nextAttemptNumber >= 1
}

// RequiresReconciliation reports whether an ambiguous outcome must enter a
// human/application hold instead of automatic retry (Blueprint §14.3).
// reconcile + UNKNOWN (unclassified) effects are never blindly retried.
func RequiresReconciliation(recovery, effectStatus string) bool {
	return strings.ToLower(strings.TrimSpace(recovery)) == "reconcile" &&
		strings.ToUpper(strings.TrimSpace(effectStatus)) == "UNKNOWN"
}

// InsufficientIdempotencyWindow reports whether dispatching a new attempt now
// could overrun the provider dedup guarantee (Blueprint §14.1):
// now + claimBudget + attemptTimeout > validUntil => hold, never retry.
func InsufficientIdempotencyWindow(now, validUntil time.Time, attemptTimeoutMs int64) bool {
	if attemptTimeoutMs <= 0 {
		attemptTimeoutMs = DefaultAttemptTimeoutMs
	}
	required := now.Add(time.Duration(ClaimStartBudgetMs+attemptTimeoutMs) * time.Millisecond)
	return required.After(validUntil)
}

// InsufficientRunDeadline reports whether a retry due at dueAt could not
// complete before the run deadline (including claim budget + execution).
func InsufficientRunDeadline(dueAt time.Time, attemptTimeoutMs int64, runDeadline *time.Time) bool {
	if runDeadline == nil {
		return false
	}
	if attemptTimeoutMs <= 0 {
		attemptTimeoutMs = DefaultAttemptTimeoutMs
	}
	required := dueAt.Add(time.Duration(ClaimStartBudgetMs+attemptTimeoutMs) * time.Millisecond)
	return required.After(*runDeadline)
}

// CanScheduleRetry guards scheduling a new retry intent from a failure.
// Terminal, cancelling, paused/pausing runs never gain new timers.
func CanScheduleRetry(runStatus, runReason string) bool {
	switch runStatus {
	case "QUEUED", "RUNNING":
		return true
	case "WAITING":
		// A run already held for reconciliation must not gain a retry timer.
		if strings.ToUpper(strings.TrimSpace(runReason)) == "RECONCILIATION" {
			return false
		}
		return true
	default:
		return false
	}
}

// CanFireRetry guards firing a due timer (Blueprint §15.1 guard arrow).
// Paused/pausing, held, cancelling, or terminal runs keep waiting;
// the timer stays PENDING with its original due_at.
func CanFireRetry(runStatus, runReason string) bool {
	switch runStatus {
	case "QUEUED", "RUNNING":
		return true
	case "WAITING":
		reason := strings.ToUpper(strings.TrimSpace(runReason))
		switch reason {
		case "RETRY_BACKOFF", "RETRY", "":
			return true
		default:
			return false
		}
	default:
		return false
	}
}
