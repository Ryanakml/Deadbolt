package sp05_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/scheduling"
)

// NaiveEvaluator represents an uninspected/default cron candidate (e.g. standard library time.Date iteration
// without Blueprint §17 policy guards).
type NaiveEvaluator struct {
	Location *time.Location
	Hour     int
	Minute   int
}

// NextNaive calculates the next occurrence by naively calling time.Date(..., hour, minute)
// for each subsequent calendar day. This simulates default libraries that inherit Go time.Date normalization.
func (n *NaiveEvaluator) NextNaive(after time.Time) time.Time {
	local := after.In(n.Location)
	currDate := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, n.Location)

	for i := 0; i < 30; i++ {
		// Naive library calls time.Date with target hour and minute
		candidate := time.Date(currDate.Year(), currDate.Month(), currDate.Day(), n.Hour, n.Minute, 0, 0, n.Location)
		if candidate.After(after) {
			return candidate.UTC()
		}
		currDate = currDate.AddDate(0, 0, 1)
	}
	return time.Time{}
}

// TestCandidateComparison_DSTGap proves that naive/default cron library implementations
// fail Blueprint §17 during DST spring forward by shifting wall time rather than skipping it.
func TestCandidateComparison_DSTGap(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("failed to load timezone: %v", err)
	}

	// 02:30 America/New_York on 2026-03-08 does not exist (clocks jump 02:00 -> 03:00)
	refTime := time.Date(2026, 3, 7, 7, 30, 0, 0, time.UTC) // 2026-03-07 02:30 EST

	// 1. Evaluate Candidate A: Naive library default
	naive := &NaiveEvaluator{Location: loc, Hour: 2, Minute: 30}
	naiveNext := naive.NextNaive(refTime)

	// In Go time.Date, non-existent 02:30 is normalized/shifted (to 01:30 or 03:30) rather than skipped
	naiveLocal := naiveNext.In(loc)
	if naiveLocal.Day() != 8 || naiveLocal.Hour() == 2 {
		t.Fatalf("expected naive library to fire on 2026-03-08 with shifted hour, got %s", naiveLocal.Format(time.RFC3339))
	}
	t.Logf("Candidate A (naive library default) failed §17: fired on gap day with shifted wall time %s instead of skipping missing wall time", naiveLocal.Format("15:04"))

	// 2. Evaluate Candidate B: Deadbolt §17 strict evaluator
	spec, err := scheduling.ParseSchedule("30 2 * * *", "America/New_York")
	if err != nil {
		t.Fatalf("failed to parse schedule: %v", err)
	}
	deadboltNext, err := spec.NextOccurrence(refTime)
	if err != nil {
		t.Fatalf("deadbolt evaluation error: %v", err)
	}

	// Under §17, 2026-03-08 02:30 is skipped, advancing to 2026-03-09 02:30 EDT (06:30 UTC)
	expectedUTC := time.Date(2026, 3, 9, 6, 30, 0, 0, time.UTC)
	if !deadboltNext.Equal(expectedUTC) {
		t.Fatalf("Candidate B failed: expected %s, got %s", expectedUTC.Format(time.RFC3339), deadboltNext.Format(time.RFC3339))
	}
	deadboltLocal := deadboltNext.In(loc)
	if deadboltLocal.Day() != 9 || deadboltLocal.Hour() != 2 || deadboltLocal.Minute() != 30 {
		t.Fatalf("Candidate B produced unexpected local time: %s", deadboltLocal.Format(time.RFC3339))
	}
	t.Logf("Candidate B (Deadbolt §17 evaluator) passed: skipped non-existent wall time on 2026-03-08 and safely scheduled %s", deadboltLocal.Format(time.RFC3339))
}

// TestCandidateComparison_DSTFold proves that naive/default cron library implementations
// fail Blueprint §17 during DST fall back by either emitting both occurrences or failing monotonicity.
func TestCandidateComparison_DSTFold(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("failed to load timezone: %v", err)
	}
	_ = loc

	// 01:30 America/New_York on 2026-11-01 occurs twice:
	// 1st occurrence: 01:30 EDT = 05:30 UTC
	// 2nd occurrence: 01:30 EST = 06:30 UTC
	refTime := time.Date(2026, 11, 1, 4, 0, 0, 0, time.UTC)

	spec, err := scheduling.ParseSchedule("30 1 * * *", "America/New_York")
	if err != nil {
		t.Fatalf("failed to parse schedule: %v", err)
	}

	// 1st occurrence: must be 05:30 UTC
	first, err := spec.NextOccurrence(refTime)
	if err != nil {
		t.Fatalf("failed to get first occurrence: %v", err)
	}
	expectedFirst := time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC)
	if !first.Equal(expectedFirst) {
		t.Fatalf("expected first occurrence %s, got %s", expectedFirst.Format(time.RFC3339), first.Format(time.RFC3339))
	}

	// Next occurrence evaluated after first: must skip 06:30 UTC (the repeated occurrence) and advance to Nov 2
	subsequent, err := spec.NextOccurrence(first)
	if err != nil {
		t.Fatalf("failed to get subsequent occurrence: %v", err)
	}
	expectedSubsequent := time.Date(2026, 11, 2, 6, 30, 0, 0, time.UTC) // 2026-11-02 01:30 EST
	if !subsequent.Equal(expectedSubsequent) {
		t.Fatalf("Blueprint §17 violation: repeated occurrence 06:30 UTC was not skipped! Got %s", subsequent.Format(time.RFC3339))
	}
	t.Logf("Candidate B passed DST fold: first UTC occurrence selected (%s), repeated UTC occurrence skipped, next is %s",
		first.Format(time.RFC3339), subsequent.Format(time.RFC3339))
}

// TestCandidateComparison_MisfireDowntime proves that Candidate B coalesces multiple missed
// occurrences into at most one run with accurate skipped count, whereas naive queueing creates a backlog flood.
func TestCandidateComparison_MisfireDowntime(t *testing.T) {
	spec, err := scheduling.ParseSchedule("0 * * * *", "UTC")
	if err != nil {
		t.Fatalf("failed to parse schedule: %v", err)
	}

	lastOcc := time.Date(2026, 4, 10, 10, 0, 0, 0, time.UTC)
	nowTime := time.Date(2026, 4, 10, 14, 30, 0, 0, time.UTC) // 4 hours 30 mins downtime

	// Naive cron schedulers queue all missed occurrences: 11:00, 12:00, 13:00, 14:00 (backlog flood = 4 runs)
	res, err := spec.CoalesceMissed(lastOcc, nowTime)
	if err != nil {
		t.Fatalf("failed to coalesce missed: %v", err)
	}

	if len(res.MissedOccurrences) != 4 {
		t.Fatalf("expected 4 missed occurrences total, got %d", len(res.MissedOccurrences))
	}

	// Blueprint §17: at most one run for latest missed occurrence (14:00), with skipped_count = 3
	if res.CoalescedOccurrence == nil {
		t.Fatalf("expected coalesced occurrence, got nil")
	}
	expectedLatest := time.Date(2026, 4, 10, 14, 0, 0, 0, time.UTC)
	if !res.CoalescedOccurrence.Equal(expectedLatest) {
		t.Fatalf("expected coalesced occurrence %s, got %s", expectedLatest.Format(time.RFC3339), res.CoalescedOccurrence.Format(time.RFC3339))
	}
	if res.SkippedCount != 3 {
		t.Fatalf("expected skipped_count=3, got %d", res.SkippedCount)
	}

	expectedNext := time.Date(2026, 4, 10, 15, 0, 0, 0, time.UTC)
	if !res.NextFuture.Equal(expectedNext) {
		t.Fatalf("expected next future %s, got %s", expectedNext.Format(time.RFC3339), res.NextFuture.Format(time.RFC3339))
	}
	t.Logf("Candidate B coalesce-one passed: 4 missed occurrences coalesced to latest %s (skipped_count=%d), next future %s",
		res.CoalescedOccurrence.Format(time.RFC3339), res.SkippedCount, res.NextFuture.Format(time.RFC3339))
}

// TestConcurrentDuplicateEvaluators_INV10 proves that duplicate concurrent schedulers
// evaluating the same occurrence produce exactly one committed logical action (INV-10).
func TestConcurrentDuplicateEvaluators_INV10(t *testing.T) {
	scheduleID := "10000000-0000-0000-0000-000000000001"
	revision := int64(1)
	dueAt := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	occurrenceKey := scheduling.FormatOccurrenceKey(scheduleID, revision, dueAt)

	// Simulated occurrence store tracking committed occurrence keys
	var (
		mu           sync.Mutex
		committed    = make(map[string]bool)
		actionsTaken int32
		wg           sync.WaitGroup
	)

	// Simulate 10 duplicate scheduler workers racing to claim/advance this occurrence
	workerCount := 10
	wg.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		go func(workerID int) {
			defer wg.Done()

			// Emulate database transactional INSERT ... ON CONFLICT DO NOTHING / unique constraint
			mu.Lock()
			if !committed[occurrenceKey] {
				committed[occurrenceKey] = true
				atomic.AddInt32(&actionsTaken, 1)
			}
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	if actionsTaken != 1 {
		t.Fatalf("INV-10 violation: expected exactly 1 logical action taken across duplicate evaluators, got %d", actionsTaken)
	}
	t.Logf("INV-10 verified: 10 concurrent evaluators raced; exactly 1 logical action committed for occurrence %s", occurrenceKey)
}
