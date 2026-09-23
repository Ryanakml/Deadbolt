package scheduling

import (
	"fmt"
	"time"
)

// Policy constants prescribed by Blueprint §17 and §18.1.
const (
	OverlapPolicySkipOverlap = "skip-overlap"
	MisfirePolicyCoalesceOne = "coalesce-one"

	OccurrenceStatusPending = "PENDING"
	OccurrenceStatusStarted = "STARTED"
	OccurrenceStatusSkipped = "SKIPPED"

	SkippedReasonOverlap = "SKIPPED_OVERLAP"
	SkippedReasonQuota   = "SKIPPED_QUOTA"
	SkippedReasonMisfire = "SKIPPED_MISFIRE"
)

// NonterminalRunStatuses lists run statuses where previous run is considered still active (Blueprint §13 & §17).
var NonterminalRunStatuses = map[string]bool{
	"QUEUED":     true,
	"RUNNING":    true,
	"WAITING":    true,
	"PAUSING":    true,
	"PAUSED":     true,
	"CANCELLING": true,
}

// NextOccurrence computes the next valid UTC time strictly after `after`
// satisfying the schedule specification and Blueprint §17 DST invariants.
func (s *ScheduleSpec) NextOccurrence(after time.Time) (time.Time, error) {
	if s.Location == nil {
		s.Location = time.UTC
	}

	// Advance to next minute boundary
	curr := after.UTC().Truncate(time.Minute).Add(time.Minute)

	// Bounded search limit: 5 years (approx 2,628,000 minutes) to avoid infinite loops on impossible schedules
	limit := curr.AddDate(5, 0, 0)

	for !curr.After(limit) {
		localTime := curr.In(s.Location)

		if s.MatchesLocal(localTime) {
			// Invariant Check 1: Spring forward missing wall time
			// If wall time was constructed via time.Date, verify it is a valid wall time in this location
			if !isValidWallTime(localTime, s.Location) {
				curr = curr.Add(time.Minute)
				continue
			}

			// Invariant Check 2: Repeated wall time during fall back (fold)
			// Blueprint §17: "wall time yang muncul dua kali hanya diambil occurrence UTC pertama"
			if isRepeatedUTCOccurrence(curr, s.Location) {
				curr = curr.Add(time.Minute)
				continue
			}

			return curr, nil
		}

		curr = curr.Add(time.Minute)
	}

	return time.Time{}, fmt.Errorf("no matching schedule occurrence found within search limit")
}

// isValidWallTime checks if the local representation corresponds to a valid existing wall time in the location
// (i.e. not skipped during a spring-forward transition).
func isValidWallTime(localTime time.Time, loc *time.Location) bool {
	reconstructed := time.Date(
		localTime.Year(), localTime.Month(), localTime.Day(),
		localTime.Hour(), localTime.Minute(), localTime.Second(),
		localTime.Nanosecond(), loc,
	)
	return reconstructed.Hour() == localTime.Hour() && reconstructed.Minute() == localTime.Minute()
}

// isRepeatedUTCOccurrence checks if this UTC moment represents a second/repeated occurrence
// of an identical local wall time on the same local date caused by a DST fall-back transition.
func isRepeatedUTCOccurrence(utcTime time.Time, loc *time.Location) bool {
	localTime := utcTime.In(loc)

	// Check previous UTC offsets within a 2-hour window (covering standard 1h and fractional DST shifts)
	// If an earlier UTC time on the same local date had the identical wall clock (hour, minute),
	// this current utcTime is the second occurrence and must be skipped.
	for delta := 1 * time.Minute; delta <= 120*time.Minute; delta += 1 * time.Minute {
		earlierUTC := utcTime.Add(-delta)
		earlierLocal := earlierUTC.In(loc)

		if earlierLocal.Year() == localTime.Year() &&
			earlierLocal.Month() == localTime.Month() &&
			earlierLocal.Day() == localTime.Day() &&
			earlierLocal.Hour() == localTime.Hour() &&
			earlierLocal.Minute() == localTime.Minute() {
			return true
		}
	}

	return false
}

// CoalesceMissedResult holds the outcome of evaluating downtime misfire according to Blueprint §17 coalesce-one.
// Bounded-memory contract: CoalesceMissed never materializes the full missed backlog.
// It preserves only the latest missed occurrence, total missed count, skipped count,
// and next future occurrence. MissedOccurrences is always nil in the production path;
// tests requiring the full list must use EnumerateMissedBounded with an explicit bound.
type CoalesceMissedResult struct {
	CoalescedOccurrence *time.Time
	SkippedCount        int
	MissedCount         int
	MissedOccurrences   []time.Time
	NextFuture          time.Time
}

// CoalesceMissed evaluates missed occurrences between lastOccurrence (exclusive) and now (inclusive)
// according to the coalesce-one misfire policy (Blueprint §17):
// "setelah downtime buat maksimal satu run untuk occurrence terbaru yang terlewat,
// simpan jumlah occurrence yang dilewati, lalu hitung next future occurrence."
// It uses O(1) memory regardless of downtime length: only the latest missed occurrence
// and integer counters are retained.
func (s *ScheduleSpec) CoalesceMissed(lastOccurrence time.Time, now time.Time) (*CoalesceMissedResult, error) {
	if lastOccurrence.IsZero() {
		// First evaluation ever; evaluate next occurrence from now
		next, err := s.NextOccurrence(now)
		if err != nil {
			return nil, err
		}
		return &CoalesceMissedResult{
			CoalescedOccurrence: nil,
			SkippedCount:        0,
			MissedCount:         0,
			MissedOccurrences:   nil,
			NextFuture:          next,
		}, nil
	}

	var latest time.Time
	missedCount := 0
	cursor := lastOccurrence

	for {
		next, err := s.NextOccurrence(cursor)
		if err != nil {
			return nil, err
		}
		if next.After(now) {
			break
		}
		latest = next
		missedCount++
		cursor = next
	}

	nextFuture, err := s.NextOccurrence(now)
	if err != nil {
		return nil, err
	}

	res := &CoalesceMissedResult{
		MissedCount:       missedCount,
		MissedOccurrences: nil,
		NextFuture:        nextFuture,
	}

	if missedCount == 0 {
		res.CoalescedOccurrence = nil
		res.SkippedCount = 0
	} else {
		latestCopy := latest
		res.CoalescedOccurrence = &latestCopy
		res.SkippedCount = missedCount - 1
	}

	return res, nil
}

// EnumerateMissedBounded is a deliberately bounded test helper that materializes the full
// missed occurrence list only when the total does not exceed max. It returns an error when
// the backlog exceeds max, preventing accidental unbounded allocation in tests.
func (s *ScheduleSpec) EnumerateMissedBounded(lastOccurrence time.Time, now time.Time, max int) ([]time.Time, error) {
	if max <= 0 {
		return nil, fmt.Errorf("max must be positive, got %d", max)
	}
	var out []time.Time
	cursor := lastOccurrence
	for {
		next, err := s.NextOccurrence(cursor)
		if err != nil {
			return nil, err
		}
		if next.After(now) {
			break
		}
		if len(out)+1 > max {
			return nil, fmt.Errorf("missed backlog exceeds bound %d", max)
		}
		out = append(out, next)
		cursor = next
	}
	return out, nil
}

// EvaluateOverlap determines whether a scheduled occurrence should start or be skipped
// based on the previous scheduled run's status (Blueprint §17 skip-overlap policy).
func EvaluateOverlap(previousRunStatus string) (action string, skippedReason string) {
	if NonterminalRunStatuses[previousRunStatus] {
		return "SKIP", SkippedReasonOverlap
	}
	return "START", ""
}

// FormatOccurrenceKey produces the unique occurrence key enforcing single logical execution (INV-10):
// (schedule_id, revision, scheduled_at_utc).
func FormatOccurrenceKey(scheduleID string, revision int64, dueAtUTC time.Time) string {
	return fmt.Sprintf("%s:%d:%s", scheduleID, revision, dueAtUTC.UTC().Format(time.RFC3339))
}
