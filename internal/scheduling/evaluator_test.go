package scheduling_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/scheduling"
)

type scheduleFixtureCorpus struct {
	Version     string            `json:"version"`
	Description string            `json:"description"`
	Fixtures    []scheduleFixture `json:"fixtures"`
}

type scheduleFixture struct {
	ID                             string            `json:"id"`
	Category                       string            `json:"category"`
	Cron                           string            `json:"cron"`
	Timezone                       string            `json:"timezone"`
	ReferenceTime                  string            `json:"reference_time,omitempty"`
	ExpectedNextUTC                string            `json:"expected_next_utc,omitempty"`
	ExpectedSubsequentUTC          string            `json:"expected_subsequent_utc,omitempty"`
	LastOccurrenceTime             string            `json:"last_occurrence_time,omitempty"`
	NowAfterDowntime               string            `json:"now_after_downtime,omitempty"`
	ExpectedCoalescedOccurrenceUTC *string           `json:"expected_coalesced_occurrence_utc,omitempty"`
	ExpectedSkippedCount           int               `json:"expected_skipped_count,omitempty"`
	ExpectedMissedOccurrencesUTC   []string          `json:"expected_missed_occurrences_utc,omitempty"`
	ExpectedNextFutureUTC          string            `json:"expected_next_future_utc,omitempty"`
	PreviousRunStatus              string            `json:"previous_run_status,omitempty"`
	ExpectedAction                 string            `json:"expected_action,omitempty"`
	ExpectedSkippedReason          *string           `json:"expected_skipped_reason,omitempty"`
	ScheduleID                     string            `json:"schedule_id,omitempty"`
	InitialRevision                int64             `json:"initial_revision,omitempty"`
	InitialCron                    string            `json:"initial_cron,omitempty"`
	UpdatedRevision                int64             `json:"updated_revision,omitempty"`
	UpdatedCron                    string            `json:"updated_cron,omitempty"`
	PastOccurrenceUTC              string            `json:"past_occurrence_utc,omitempty"`
	RevisionEffectiveTime          string            `json:"revision_effective_time,omitempty"`
	ExpectedFutureOccurrenceUTC    string            `json:"expected_future_occurrence_utc,omitempty"`
	ExpectedOccurrenceKeys         map[string]string `json:"expected_occurrence_keys,omitempty"`
	Description                    string            `json:"description"`
}

func loadFixtures(t *testing.T) *scheduleFixtureCorpus {
	t.Helper()
	path := filepath.Join("..", "..", "contracts", "fixtures", "schedules.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read schedule fixtures from %s: %v", path, err)
	}
	var corpus scheduleFixtureCorpus
	if err := json.Unmarshal(data, &corpus); err != nil {
		t.Fatalf("failed to unmarshal schedule fixtures: %v", err)
	}
	return &corpus
}

func TestScheduleFixturesContract(t *testing.T) {
	corpus := loadFixtures(t)
	if len(corpus.Fixtures) == 0 {
		t.Fatalf("expected fixtures to be populated")
	}

	for _, f := range corpus.Fixtures {
		f := f
		t.Run(f.ID, func(t *testing.T) {
			switch f.Category {
			case "cron_syntax", "dst_gap":
				spec, err := scheduling.ParseSchedule(f.Cron, f.Timezone)
				if err != nil {
					t.Fatalf("unexpected parse error: %v", err)
				}
				refTime, err := time.Parse(time.RFC3339, f.ReferenceTime)
				if err != nil {
					t.Fatalf("invalid reference time: %v", err)
				}
				next, err := spec.NextOccurrence(refTime)
				if err != nil {
					t.Fatalf("unexpected next occurrence error: %v", err)
				}
				expectedNext, err := time.Parse(time.RFC3339, f.ExpectedNextUTC)
				if err != nil {
					t.Fatalf("invalid expected next: %v", err)
				}
				if !next.Equal(expectedNext) {
					t.Errorf("expected next %s, got %s", expectedNext.Format(time.RFC3339), next.Format(time.RFC3339))
				}

			case "dst_fold":
				spec, err := scheduling.ParseSchedule(f.Cron, f.Timezone)
				if err != nil {
					t.Fatalf("unexpected parse error: %v", err)
				}
				refTime, err := time.Parse(time.RFC3339, f.ReferenceTime)
				if err != nil {
					t.Fatalf("invalid reference time: %v", err)
				}
				first, err := spec.NextOccurrence(refTime)
				if err != nil {
					t.Fatalf("unexpected next occurrence error: %v", err)
				}
				expectedFirst, err := time.Parse(time.RFC3339, f.ExpectedNextUTC)
				if err != nil {
					t.Fatalf("invalid expected next: %v", err)
				}
				if !first.Equal(expectedFirst) {
					t.Errorf("expected first UTC occurrence %s, got %s", expectedFirst.Format(time.RFC3339), first.Format(time.RFC3339))
				}

				if f.ExpectedSubsequentUTC != "" {
					second, err := spec.NextOccurrence(first)
					if err != nil {
						t.Fatalf("unexpected subsequent occurrence error: %v", err)
					}
					expectedSecond, err := time.Parse(time.RFC3339, f.ExpectedSubsequentUTC)
					if err != nil {
						t.Fatalf("invalid expected subsequent: %v", err)
					}
					if !second.Equal(expectedSecond) {
						t.Errorf("expected subsequent occurrence %s, got %s (repeated wall time was not skipped!)", expectedSecond.Format(time.RFC3339), second.Format(time.RFC3339))
					}
				}

			case "misfire_downtime":
				spec, err := scheduling.ParseSchedule(f.Cron, f.Timezone)
				if err != nil {
					t.Fatalf("unexpected parse error: %v", err)
				}
				lastOcc, err := time.Parse(time.RFC3339, f.LastOccurrenceTime)
				if err != nil {
					t.Fatalf("invalid last occurrence time: %v", err)
				}
				nowTime, err := time.Parse(time.RFC3339, f.NowAfterDowntime)
				if err != nil {
					t.Fatalf("invalid now time: %v", err)
				}

				res, err := spec.CoalesceMissed(lastOcc, nowTime)
				if err != nil {
					t.Fatalf("coalesce missed error: %v", err)
				}

				if f.ExpectedCoalescedOccurrenceUTC == nil {
					if res.CoalescedOccurrence != nil {
						t.Errorf("expected nil coalesced occurrence, got %s", res.CoalescedOccurrence.Format(time.RFC3339))
					}
				} else {
					if res.CoalescedOccurrence == nil {
						t.Fatalf("expected coalesced occurrence %s, got nil", *f.ExpectedCoalescedOccurrenceUTC)
					}
					expectedCoal, _ := time.Parse(time.RFC3339, *f.ExpectedCoalescedOccurrenceUTC)
					if !res.CoalescedOccurrence.Equal(expectedCoal) {
						t.Errorf("expected coalesced occurrence %s, got %s", expectedCoal.Format(time.RFC3339), res.CoalescedOccurrence.Format(time.RFC3339))
					}
				}

				if res.SkippedCount != f.ExpectedSkippedCount {
					t.Errorf("expected skipped count %d, got %d", f.ExpectedSkippedCount, res.SkippedCount)
				}

				if res.MissedCount != len(f.ExpectedMissedOccurrencesUTC) {
					t.Errorf("expected missed count %d, got %d", len(f.ExpectedMissedOccurrencesUTC), res.MissedCount)
				}

				if res.MissedOccurrences != nil {
					t.Errorf("bounded-memory violation: MissedOccurrences must be nil, got %d entries", len(res.MissedOccurrences))
				}

				// For small controlled fixtures, verify the full backlog via the bounded helper.
				if len(f.ExpectedMissedOccurrencesUTC) > 0 {
					bounded, err := spec.EnumerateMissedBounded(lastOcc, nowTime, 1000)
					if err != nil {
						t.Fatalf("bounded helper failed: %v", err)
					}
					if len(bounded) != len(f.ExpectedMissedOccurrencesUTC) {
						t.Fatalf("bounded helper: expected %d missed, got %d", len(f.ExpectedMissedOccurrencesUTC), len(bounded))
					}
					for i, wantStr := range f.ExpectedMissedOccurrencesUTC {
						want, _ := time.Parse(time.RFC3339, wantStr)
						if !bounded[i].Equal(want) {
							t.Errorf("bounded helper missed[%d]: expected %s, got %s", i, want.Format(time.RFC3339), bounded[i].Format(time.RFC3339))
						}
					}
				}

				expectedNextFuture, _ := time.Parse(time.RFC3339, f.ExpectedNextFutureUTC)
				if !res.NextFuture.Equal(expectedNextFuture) {
					t.Errorf("expected next future %s, got %s", expectedNextFuture.Format(time.RFC3339), res.NextFuture.Format(time.RFC3339))
				}

			case "overlap":
				action, reason := scheduling.EvaluateOverlap(f.PreviousRunStatus)
				if action != f.ExpectedAction {
					t.Errorf("expected action %s, got %s", f.ExpectedAction, action)
				}
				if f.ExpectedSkippedReason == nil {
					if reason != "" {
						t.Errorf("expected empty reason, got %s", reason)
					}
				} else {
					if reason != *f.ExpectedSkippedReason {
						t.Errorf("expected reason %s, got %s", *f.ExpectedSkippedReason, reason)
					}
				}

			case "revision":
				pastDue, err := time.Parse(time.RFC3339, f.PastOccurrenceUTC)
				if err != nil {
					t.Fatalf("invalid past occurrence: %v", err)
				}
				futureDue, err := time.Parse(time.RFC3339, f.ExpectedFutureOccurrenceUTC)
				if err != nil {
					t.Fatalf("invalid expected future: %v", err)
				}
				effectiveTime, err := time.Parse(time.RFC3339, f.RevisionEffectiveTime)
				if err != nil {
					t.Fatalf("invalid revision effective time: %v", err)
				}

				initialSpec, err := scheduling.ParseSchedule(f.InitialCron, "UTC")
				if err != nil {
					t.Fatalf("invalid initial cron %q: %v", f.InitialCron, err)
				}
				updatedSpec, err := scheduling.ParseSchedule(f.UpdatedCron, "UTC")
				if err != nil {
					t.Fatalf("invalid updated cron %q: %v", f.UpdatedCron, err)
				}

				// 1. Pre-edit occurrence evaluates from the old cron under revision 1.
				dayStart, _ := time.Parse(time.RFC3339, "2026-05-01T00:00:00Z")
				preEdit, err := initialSpec.NextOccurrence(dayStart)
				if err != nil {
					t.Fatalf("pre-edit evaluation failed: %v", err)
				}
				if !preEdit.Equal(pastDue) {
					t.Fatalf("pre-edit occurrence must derive from InitialCron: expected %s, got %s", pastDue.Format(time.RFC3339), preEdit.Format(time.RFC3339))
				}
				if !initialSpec.MatchesLocal(pastDue) {
					t.Errorf("past due %s must match InitialCron %q", pastDue.Format(time.RFC3339), f.InitialCron)
				}
				if updatedSpec.MatchesLocal(pastDue) {
					t.Errorf("past due %s must NOT match UpdatedCron %q (edits affect future only)", pastDue.Format(time.RFC3339), f.UpdatedCron)
				}

				// 2. Apply the revision boundary: future work evaluates from the updated cron under revision 2.
				nextFuture, err := updatedSpec.NextOccurrence(effectiveTime)
				if err != nil {
					t.Fatalf("post-edit evaluation failed: %v", err)
				}
				if !nextFuture.Equal(futureDue) {
					t.Fatalf("future occurrence must derive from UpdatedCron after boundary: expected %s, got %s", futureDue.Format(time.RFC3339), nextFuture.Format(time.RFC3339))
				}
				if !updatedSpec.MatchesLocal(futureDue) {
					t.Errorf("future due %s must match UpdatedCron %q", futureDue.Format(time.RFC3339), f.UpdatedCron)
				}
				if initialSpec.MatchesLocal(futureDue) {
					t.Errorf("future due %s must NOT match InitialCron %q", futureDue.Format(time.RFC3339), f.InitialCron)
				}

				// 3. A broken implementation reusing the old cron after the boundary would miss the new time.
				staleFuture, err := initialSpec.NextOccurrence(effectiveTime)
				if err != nil {
					t.Fatalf("stale-cron evaluation failed: %v", err)
				}
				if staleFuture.Equal(futureDue) {
					t.Fatalf("stale old-cron evaluation must NOT produce the new future %s (got %s)", futureDue.Format(time.RFC3339), staleFuture.Format(time.RFC3339))
				}

				// 4. Past occurrence/run identity remains pinned to revision 1.
				pastKey := scheduling.FormatOccurrenceKey(f.ScheduleID, f.InitialRevision, pastDue)
				futureKey := scheduling.FormatOccurrenceKey(f.ScheduleID, f.UpdatedRevision, futureDue)
				if pastKey != f.ExpectedOccurrenceKeys["past"] {
					t.Errorf("expected past key %s, got %s", f.ExpectedOccurrenceKeys["past"], pastKey)
				}
				if futureKey != f.ExpectedOccurrenceKeys["future"] {
					t.Errorf("expected future key %s, got %s", f.ExpectedOccurrenceKeys["future"], futureKey)
				}
				if pastKey == futureKey {
					t.Errorf("past and future occurrence keys must differ across revisions")
				}
				// Re-formatting the pinned past occurrence must be stable after the edit.
				repinned := scheduling.FormatOccurrenceKey(f.ScheduleID, f.InitialRevision, pastDue)
				if repinned != pastKey {
					t.Errorf("past occurrence identity changed after revision: %s vs %s", pastKey, repinned)
				}
			}
		})
	}
}

func TestCronParseErrors(t *testing.T) {
	cases := []struct {
		cron string
		tz   string
	}{
		{"* * * *", "UTC"},                 // 4 fields
		{"* * * * * *", "UTC"},             // 6 fields
		{"60 * * * *", "UTC"},              // invalid minute
		{"* 24 * * *", "UTC"},              // invalid hour
		{"* * 32 * *", "UTC"},              // invalid day
		{"* * * 13 *", "UTC"},              // invalid month
		{"* * * * 8", "UTC"},               // invalid weekday
		{"* * * * *", "Invalid/Zone_Name"}, // invalid timezone
		{"10-5 * * * *", "UTC"},            // invalid range start > end
		{"*/0 * * * *", "UTC"},             // invalid step 0
	}

	for _, c := range cases {
		_, err := scheduling.ParseSchedule(c.cron, c.tz)
		if err == nil {
			t.Errorf("expected error for cron=%q tz=%q, got nil", c.cron, c.tz)
		}
	}
}

// TestCoalesceMissed_BoundedMemory_HighFrequencyLongDowntime is the §17 backlog regression:
// a per-minute schedule after 30 days of downtime (~43,200 missed occurrences) must preserve
// only the latest missed occurrence, skipped count, and next future occurrence with O(1)
// memory. The production path must not materialize the full slice.
func TestCoalesceMissed_BoundedMemory_HighFrequencyLongDowntime(t *testing.T) {
	spec, err := scheduling.ParseSchedule("* * * * *", "UTC")
	if err != nil {
		t.Fatalf("failed to parse schedule: %v", err)
	}

	lastOcc := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	nowTime := time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)

	res, err := spec.CoalesceMissed(lastOcc, nowTime)
	if err != nil {
		t.Fatalf("coalesce missed failed: %v", err)
	}

	const wantMissed = 30 * 24 * 60 // 43,200 per-minute occurrences in 30 days
	if res.MissedCount != wantMissed {
		t.Fatalf("expected MissedCount=%d, got %d", wantMissed, res.MissedCount)
	}
	if res.SkippedCount != wantMissed-1 {
		t.Fatalf("expected SkippedCount=%d, got %d", wantMissed-1, res.SkippedCount)
	}
	if res.MissedOccurrences != nil {
		t.Fatalf("bounded-memory violation: MissedOccurrences must be nil, got %d entries", len(res.MissedOccurrences))
	}
	expectedLatest := time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)
	if res.CoalescedOccurrence == nil || !res.CoalescedOccurrence.Equal(expectedLatest) {
		t.Fatalf("expected coalesced latest %s, got %v", expectedLatest.Format(time.RFC3339), res.CoalescedOccurrence)
	}
	expectedNext := time.Date(2026, 1, 31, 0, 1, 0, 0, time.UTC)
	if !res.NextFuture.Equal(expectedNext) {
		t.Fatalf("expected next future %s, got %s", expectedNext.Format(time.RFC3339), res.NextFuture.Format(time.RFC3339))
	}

	// The bounded helper must refuse to materialize this backlog without an explicit large bound.
	if _, err := spec.EnumerateMissedBounded(lastOcc, nowTime, 1000); err == nil {
		t.Fatalf("expected bounded helper to reject 43,200 backlog with max=1000")
	}
	t.Logf("bounded coalesce-one: MissedCount=%d SkippedCount=%d latest=%s next=%s (MissedOccurrences=nil)",
		res.MissedCount, res.SkippedCount, res.CoalescedOccurrence.Format(time.RFC3339), res.NextFuture.Format(time.RFC3339))
}
