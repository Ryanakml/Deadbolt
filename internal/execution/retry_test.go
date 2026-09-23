package execution

import (
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/worker"
)

func TestNormalizeRetryPolicyDefaultsAndClamp(t *testing.T) {
	p := NormalizeRetryPolicy(0, 0, 0, 0, "safe", nil)
	if p.MaxAttempts != DefaultMaxAttempts || p.InitialDelayMs != DefaultInitialDelayMs || p.MaxDelayMs != DefaultMaxDelayMs {
		t.Fatalf("defaults wrong: %+v", p)
	}
	// Absent/invalid recovery is preserved, never defaulted: the engine must
	// fail closed instead of treating it as permission to retry.
	for _, recovery := range []string{"", "bogus", "SAFE "} {
		got := NormalizeRetryPolicy(3, 1000, 30000, 60000, recovery, nil).Recovery
		if got != recovery {
			t.Fatalf("recovery %q was reinterpreted as %q", recovery, got)
		}
	}
	p = NormalizeRetryPolicy(99, 1000, 30000, 60000, "safe", nil)
	if p.MaxAttempts != MaxMaxAttempts {
		t.Fatalf("expected clamp to 10, got %d", p.MaxAttempts)
	}
	p = NormalizeRetryPolicy(-5, 1000, 30000, 60000, "safe", nil)
	if p.MaxAttempts != DefaultMaxAttempts {
		t.Fatalf("expected default for non-positive, got %d", p.MaxAttempts)
	}
	// Claim budget must stay in sync with worker protocol (5s).
	if ClaimStartBudgetMs != worker.ClaimStartDeadline.Milliseconds() {
		t.Fatalf("ClaimStartBudgetMs %d != worker ClaimStartDeadline %d", ClaimStartBudgetMs, worker.ClaimStartDeadline.Milliseconds())
	}
}

func TestBackoffCapExponentialAndCapped(t *testing.T) {
	cases := []struct {
		attempt int
		want    int64
	}{
		{1, 1000},
		{2, 2000},
		{3, 4000},
		{4, 8000},
		{5, 16000},
		{6, 30000},
		{7, 30000},
		{10, 30000},
	}
	for _, c := range cases {
		if got := BackoffCapMs(c.attempt, 1000, 30000); got != c.want {
			t.Fatalf("attempt %d: got %d want %d", c.attempt, got, c.want)
		}
	}
}

func TestJitteredDelayBounds(t *testing.T) {
	if got := JitteredDelayMs(1000, 0); got != 0 {
		t.Fatalf("sample 0 should give 0, got %d", got)
	}
	if got := JitteredDelayMs(1000, 0.999999); got < 0 || got > 1000 {
		t.Fatalf("jitter out of bounds: %d", got)
	}
	if got := JitteredDelayMs(1000, 1); got != 1000 {
		t.Fatalf("sample 1 clamped to cap, got %d", got)
	}
	if got := JitteredDelayMs(0, 0.5); got != 0 {
		t.Fatalf("zero cap should give 0, got %d", got)
	}
}

func TestComputeRetryDelayRetryAfterIncreaseAndCap(t *testing.T) {
	// Jittered 0.5 * 1000 = 500; Retry-After 2000 raises to 2000.
	ra := int64(2000)
	if got := ComputeRetryDelayMs(1, 1000, 30000, 0.5, &ra); got != 2000 {
		t.Fatalf("expected Retry-After increase to 2000, got %d", got)
	}
	// Retry-After beyond cap is capped at maxDelay.
	big := int64(999000)
	if got := ComputeRetryDelayMs(1, 1000, 30000, 0.5, &big); got != 30000 {
		t.Fatalf("expected cap 30000, got %d", got)
	}
	// No Retry-After: pure jitter.
	if got := ComputeRetryDelayMs(2, 1000, 30000, 0.5, nil); got != 1000 {
		t.Fatalf("expected 0.5*2000=1000, got %d", got)
	}
}

func TestIsValidRecoveryPolicyFailsClosed(t *testing.T) {
	for _, recovery := range []string{"safe", "idempotent", "reconcile", " Safe ", "IDEMPOTENT"} {
		if !IsValidRecoveryPolicy(recovery) {
			t.Fatalf("expected valid recovery for %q", recovery)
		}
	}
	for _, recovery := range []string{"", "bogus", "at-most-once", "safe-ish"} {
		if IsValidRecoveryPolicy(recovery) {
			t.Fatalf("expected invalid recovery for %q", recovery)
		}
	}
}

func TestParseRetryAfterOverflowSaturates(t *testing.T) {
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	// Normal seconds still behave exactly.
	if ms, ok := ParseRetryAfter("90", now); !ok || ms != 90000 {
		t.Fatalf("normal seconds failed: %d %v", ms, ok)
	}
	// Near-MaxInt64 seconds must saturate, never overflow negative.
	huge := "9223372036854775807"
	ms, ok := ParseRetryAfter(huge, now)
	if !ok {
		t.Fatalf("huge Retry-After should parse as saturating, got ok=false")
	}
	if ms < 0 {
		t.Fatalf("overflow turned huge Retry-After negative: %d", ms)
	}
	// Downstream capping must yield the policy max, not an immediate retry.
	if got := ComputeRetryDelayMs(1, 1000, 30000, 0, &ms); got != 30000 {
		t.Fatalf("saturated Retry-After should cap to maxDelay 30000, got %d", got)
	}
	// A large-but-representable value also caps rather than overflowing.
	big := int64(1 << 50)
	if got := ComputeRetryDelayMs(1, 1000, 30000, 0, &big); got != 30000 {
		t.Fatalf("large Retry-After should cap to maxDelay 30000, got %d", got)
	}
}

func TestParseRetryAfterSecondsAndDate(t *testing.T) {
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	if ms, ok := ParseRetryAfter("5", now); !ok || ms != 5000 {
		t.Fatalf("seconds parse failed: %d %v", ms, ok)
	}
	if ms, ok := ParseRetryAfter("  0  ", now); !ok || ms != 0 {
		t.Fatalf("zero parse failed: %d %v", ms, ok)
	}
	if _, ok := ParseRetryAfter("-3", now); ok {
		t.Fatalf("negative should be invalid")
	}
	if _, ok := ParseRetryAfter("not-a-date", now); ok {
		t.Fatalf("garbage should be invalid")
	}
	if _, ok := ParseRetryAfter("", now); ok {
		t.Fatalf("empty should be invalid")
	}
	// HTTP-date 10s in future.
	future := now.Add(10 * time.Second).UTC().Format(time.RFC1123)
	if ms, ok := ParseRetryAfter(future, now); !ok || ms < 9000 || ms > 11000 {
		t.Fatalf("date parse failed: %d %v (%s)", ms, ok, future)
	}
	// Past date => 0 with ok.
	past := now.Add(-5 * time.Second).UTC().Format(time.RFC1123)
	if ms, ok := ParseRetryAfter(past, now); !ok || ms != 0 {
		t.Fatalf("past date should give 0 ok, got %d %v", ms, ok)
	}
}

func TestIsNonRetryableCodes(t *testing.T) {
	for _, code := range []string{"INPUT_MAPPING_ERROR", "OUTPUT_SCHEMA_VIOLATION", "MISSING_TASK_REF", "INCOMPATIBLE_BUNDLE", "UNAUTHORIZED", "UNSUPPORTED_CAPABILITY", "SCHEMA_VIOLATION"} {
		if !IsNonRetryableCode(code) {
			t.Fatalf("expected non-retryable for %s", code)
		}
	}
	for _, code := range []string{"TASK_FAILED", "LEASE_EXPIRED", "ATTEMPT_TIMED_OUT", "PROVIDER_500", ""} {
		if IsNonRetryableCode(code) {
			t.Fatalf("expected retryable-eligible for %q", code)
		}
	}
}

func TestHasRetryBudgetIncludesFirst(t *testing.T) {
	// maxAttempts=3: next=1,2,3 allowed; 4 exhausted.
	if !HasRetryBudget(1, 3) || !HasRetryBudget(2, 3) || !HasRetryBudget(3, 3) {
		t.Fatalf("budget should allow 1..3 for max 3")
	}
	if HasRetryBudget(4, 3) {
		t.Fatalf("budget should exhaust at 4 for max 3")
	}
}

func TestRequiresReconciliationOnlyForReconcileUnknown(t *testing.T) {
	if !RequiresReconciliation("reconcile", "UNKNOWN") {
		t.Fatalf("reconcile+UNKNOWN must hold")
	}
	if RequiresReconciliation("reconcile", "NOT_APPLIED") {
		t.Fatalf("reconcile+NOT_APPLIED must not hold")
	}
	if RequiresReconciliation("safe", "UNKNOWN") {
		t.Fatalf("safe+UNKNOWN must not hold")
	}
}

func TestIdempotencyWindowAdmission(t *testing.T) {
	now := time.Now()
	valid := now.Add(60 * time.Second)
	// 5s claim + 5s timeout fits in 60s window.
	if InsufficientIdempotencyWindow(now, valid, 5000) {
		t.Fatalf("sufficient window reported insufficient")
	}
	// Tight window: now+5s+30s > valid in 10s.
	tight := now.Add(10 * time.Second)
	if !InsufficientIdempotencyWindow(now, tight, 30000) {
		t.Fatalf("insufficient window should hold")
	}
}

func TestRunDeadlineGuard(t *testing.T) {
	now := time.Now()
	due := now.Add(1 * time.Second)
	far := now.Add(1 * time.Hour)
	if InsufficientRunDeadline(due, 5000, &far) {
		t.Fatalf("far deadline should allow retry")
	}
	if InsufficientRunDeadline(due, 5000, nil) {
		t.Fatalf("nil deadline should allow retry")
	}
	near := now.Add(2 * time.Second)
	if !InsufficientRunDeadline(due, 30000, &near) {
		t.Fatalf("near deadline should fail retry")
	}
}

func TestScheduleAndFireGuards(t *testing.T) {
	if !CanScheduleRetry("RUNNING", "") || !CanScheduleRetry("QUEUED", "") || !CanScheduleRetry("PAUSING", "") || !CanScheduleRetry("PAUSED", "") {
		t.Fatalf("running/queued/paused/pausing should schedule")
	}
	if CanScheduleRetry("FAILED", "") || CanScheduleRetry("CANCELLED", "") || CanScheduleRetry("CANCELLING", "") {
		t.Fatalf("terminal/cancelling must not schedule")
	}
	if CanScheduleRetry("WAITING", "RECONCILIATION") {
		t.Fatalf("reconciliation hold must not schedule")
	}
	if !CanFireRetry("WAITING", "RETRY_BACKOFF") || !CanFireRetry("RUNNING", "") {
		t.Fatalf("retry wait/running should fire")
	}
	if CanFireRetry("WAITING", "RECONCILIATION") || CanFireRetry("PAUSED", "") || CanFireRetry("PAUSING", "") || CanFireRetry("FAILED", "") {
		t.Fatalf("hold/paused/pausing/terminal must keep waiting")
	}
}

func TestNormalizeAttemptTimeoutDefaultAndMax(t *testing.T) {
	if got := NormalizeRetryPolicy(3, 1000, 30000, 0, "safe", nil).TimeoutMs; got != DefaultAttemptTimeoutMs {
		t.Fatalf("unset timeout should default to 5m, got %d", got)
	}
	if got := NormalizeRetryPolicy(3, 1000, 30000, 2*MaxAttemptTimeoutMs, "safe", nil).TimeoutMs; got != MaxAttemptTimeoutMs {
		t.Fatalf("timeout above 1h must clamp to %d, got %d", MaxAttemptTimeoutMs, got)
	}
	if MaxAttemptTimeoutMs != 3600000 {
		t.Fatalf("attempt max must be exactly 1h in ms, got %d", MaxAttemptTimeoutMs)
	}
	if RunLifetimeMs != int64(24*60*60*1000) {
		t.Fatalf("MVP run lifetime must be exactly 24h in ms, got %d", RunLifetimeMs)
	}
	if CancelGraceMs != 10000 {
		t.Fatalf("cancel grace must be exactly 10s in ms, got %d", CancelGraceMs)
	}
}
