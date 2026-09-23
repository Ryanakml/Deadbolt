package sp05_test

// SP-05 real candidate comparison (Blueprint §33, Issue #30).
//
// Pinned candidates actually executed here (see go.mod for exact versions):
//   - github.com/robfig/cron/v3 v3.0.1
//   - github.com/gorhill/cronexpr v0.0.0-20180427100037-88b0669f7d75
//   - Deadbolt strict §17 evaluator (internal/scheduling, this repo)
//
// Execution contract: every candidate is evaluated against the shared
// controlled-clock fixtures in contracts/fixtures/schedules.json using
// location-aware input (ref.In(loc)). Observed results below were produced by
// running `go test -v ./tests/spikes/sp05` with Go 1.27.1.
//
// Summary of observed behavior (NY = America/New_York):
//   - DST gap NY (30 2 * * *, ref 2026-03-07T07:30Z, expect 2026-03-09T06:30Z):
//     robfig => 2026-03-09T06:30:00Z (PASS), cronexpr => 2026-03-08T06:30:00Z (FAIL, fires gap day 01:30 EST).
//   - DST gap London (30 1 * * *, ref 2026-03-28T01:30Z, expect 2026-03-30T00:30Z):
//     robfig => 2026-03-30T00:30:00Z (PASS), cronexpr => 2026-03-29T01:30:00Z (FAIL).
//   - DST fold NY (30 1 * * *, ref 2026-11-01T04:00Z, expect first 05:30Z then 2026-11-02T06:30Z):
//     robfig => first 05:30Z (PASS) then 06:30Z same day (FAIL, emits duplicate UTC occurrence),
//     cronexpr => first 05:30Z then 2026-11-02T06:30Z (PASS).
//   - DST fold London (30 1 * * *, ref 2026-10-25T00:00Z, expect first 00:30Z then 2026-10-26T01:30Z):
//     robfig => first 00:30Z (PASS) then 01:30Z same day (FAIL, duplicate),
//     cronexpr => first 01:30Z (FAIL, selects second UTC occurrence instead of first).
//   - Lord Howe half-hour gap (15 2 * * *, ref 2026-10-03T15:00Z, expect 2026-10-04T15:15Z):
//     robfig => 2026-10-04T15:15:00Z (PASS), cronexpr => 2026-10-03T15:45:00Z (FAIL).
//   - Misfire/downtime (hourly, last 10:00Z, now 14:30Z): naive Next-iteration on either
//     library yields 4 queued runs (11:00,12:00,13:00,14:00 backlog); Deadbolt coalesce-one
//     yields latest 14:00Z with skipped_count=3 and next future 15:00Z.
// Decision: SELECT Deadbolt strict §17 evaluator; REJECT unguarded robfig/cron/v3 and
// gorhill/cronexpr defaults because neither satisfies both DST gap and fold invariants,
// and neither implements coalesce-one / skip-overlap.
//
// INV-10 concurrency proof does NOT live here. The authoritative proof is the real
// concurrent PostgreSQL race in tests/integration/schedule_contract_test.go
// (TestScheduleConcurrentDuplicateEvaluatorsINV10_Postgres) using separate
// transactions/connections against the canonical (schedule_id, revision, due_at)
// uniqueness constraint.

import (
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/scheduling"
	"github.com/gorhill/cronexpr"
	"github.com/robfig/cron/v3"
)

const (
	pinnedRobfigVersion   = "github.com/robfig/cron/v3 v3.0.1"
	pinnedCronexprVersion = "github.com/gorhill/cronexpr v0.0.0-20180427100037-88b0669f7d75"
)

func mustLoadLocation(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatalf("failed to load location %s: %v", name, err)
	}
	return loc
}

func mustParseTime(t *testing.T, v string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, v)
	if err != nil {
		t.Fatalf("invalid time %q: %v", v, err)
	}
	return ts
}

func robfigNext(t *testing.T, cronExpr string, ref time.Time) time.Time {
	t.Helper()
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := parser.Parse(cronExpr)
	if err != nil {
		t.Fatalf("robfig/cron/v3 failed to parse %q: %v", cronExpr, err)
	}
	return sched.Next(ref)
}

func cronexprNext(t *testing.T, cronExpr string, ref time.Time) time.Time {
	t.Helper()
	expr, err := cronexpr.Parse(cronExpr)
	if err != nil {
		t.Fatalf("gorhill/cronexpr failed to parse %q: %v", cronExpr, err)
	}
	return expr.Next(ref)
}

func deadboltNext(t *testing.T, cronExpr, tz string, ref time.Time) time.Time {
	t.Helper()
	spec, err := scheduling.ParseSchedule(cronExpr, tz)
	if err != nil {
		t.Fatalf("deadbolt failed to parse %q tz %q: %v", cronExpr, tz, err)
	}
	next, err := spec.NextOccurrence(ref)
	if err != nil {
		t.Fatalf("deadbolt NextOccurrence failed: %v", err)
	}
	return next
}

func TestPinnedCandidateVersions_Record(t *testing.T) {
	t.Logf("pinned candidates: %s ; %s ; Deadbolt internal/scheduling (this repo)", pinnedRobfigVersion, pinnedCronexprVersion)
	t.Logf("run: go test -v ./tests/spikes/sp05 (Go 1.27.1)")
}

// TestCandidateComparison_DSTGap_NY executes all three pinned candidates against the shared
// fixture dst_spring_forward_missing_wall_time_skipped_ny.
func TestCandidateComparison_DSTGap_NY(t *testing.T) {
	loc := mustLoadLocation(t, "America/New_York")
	ref := mustParseTime(t, "2026-03-07T07:30:00Z").In(loc)
	expected := mustParseTime(t, "2026-03-09T06:30:00Z")

	deadbolt := deadboltNext(t, "30 2 * * *", "America/New_York", mustParseTime(t, "2026-03-07T07:30:00Z"))
	if !deadbolt.Equal(expected) {
		t.Fatalf("Deadbolt §17 evaluator failed NY gap: expected %s, got %s", expected.Format(time.RFC3339), deadbolt.Format(time.RFC3339))
	}

	robfig := robfigNext(t, "30 2 * * *", ref)
	t.Logf("robfig/cron/v3 v3.0.1 NY gap Next => %s", robfig.UTC().Format(time.RFC3339))
	if !robfig.UTC().Equal(expected) {
		t.Fatalf("unexpected robfig NY gap result: expected %s, got %s", expected.Format(time.RFC3339), robfig.UTC().Format(time.RFC3339))
	}

	cronExpr := cronexprNext(t, "30 2 * * *", ref)
	t.Logf("gorhill/cronexpr v0.0.0-20180427100037 NY gap Next => %s (NY %s)", cronExpr.UTC().Format(time.RFC3339), cronExpr.In(loc).Format(time.RFC3339))
	if cronExpr.UTC().Equal(expected) {
		t.Fatalf("expected gorhill/cronexpr to fail NY gap §17 (fire gap day), but it matched expected %s", expected.Format(time.RFC3339))
	}
	if got := cronExpr.UTC().Format(time.RFC3339); got != "2026-03-08T06:30:00Z" {
		t.Fatalf("unexpected cronexpr NY gap observation: got %s, want 2026-03-08T06:30:00Z", got)
	}
	t.Logf("Decision evidence: robfig PASS gap NY; cronexpr FAIL gap NY (fires 2026-03-08 01:30 EST); Deadbolt PASS")
}

// TestCandidateComparison_DSTGap_London executes candidates against the London gap fixture.
func TestCandidateComparison_DSTGap_London(t *testing.T) {
	loc := mustLoadLocation(t, "Europe/London")
	ref := mustParseTime(t, "2026-03-28T01:30:00Z").In(loc)
	expected := mustParseTime(t, "2026-03-30T00:30:00Z")

	deadbolt := deadboltNext(t, "30 1 * * *", "Europe/London", mustParseTime(t, "2026-03-28T01:30:00Z"))
	if !deadbolt.Equal(expected) {
		t.Fatalf("Deadbolt failed London gap: expected %s, got %s", expected.Format(time.RFC3339), deadbolt.Format(time.RFC3339))
	}

	robfig := robfigNext(t, "30 1 * * *", ref)
	t.Logf("robfig London gap Next => %s", robfig.UTC().Format(time.RFC3339))
	if !robfig.UTC().Equal(expected) {
		t.Fatalf("unexpected robfig London gap result: expected %s, got %s", expected.Format(time.RFC3339), robfig.UTC().Format(time.RFC3339))
	}

	cronExpr := cronexprNext(t, "30 1 * * *", ref)
	t.Logf("cronexpr London gap Next => %s", cronExpr.UTC().Format(time.RFC3339))
	if cronExpr.UTC().Equal(expected) {
		t.Fatalf("expected cronexpr to fail London gap §17, but it matched expected")
	}
	t.Logf("Decision evidence: robfig PASS gap London; cronexpr FAIL gap London; Deadbolt PASS")
}

// TestCandidateComparison_DSTFold_NY executes candidates against the NY fold fixture.
func TestCandidateComparison_DSTFold_NY(t *testing.T) {
	loc := mustLoadLocation(t, "America/New_York")
	ref := mustParseTime(t, "2026-11-01T04:00:00Z").In(loc)
	expectedFirst := mustParseTime(t, "2026-11-01T05:30:00Z")
	expectedSubsequent := mustParseTime(t, "2026-11-02T06:30:00Z")

	deadboltFirst := deadboltNext(t, "30 1 * * *", "America/New_York", mustParseTime(t, "2026-11-01T04:00:00Z"))
	if !deadboltFirst.Equal(expectedFirst) {
		t.Fatalf("Deadbolt failed NY fold first: expected %s, got %s", expectedFirst.Format(time.RFC3339), deadboltFirst.Format(time.RFC3339))
	}
	deadboltSecond, err := func() (time.Time, error) {
		spec, _ := scheduling.ParseSchedule("30 1 * * *", "America/New_York")
		return spec.NextOccurrence(deadboltFirst)
	}()
	if err != nil {
		t.Fatalf("deadbolt subsequent failed: %v", err)
	}
	if !deadboltSecond.Equal(expectedSubsequent) {
		t.Fatalf("Deadbolt failed NY fold subsequent: expected %s, got %s", expectedSubsequent.Format(time.RFC3339), deadboltSecond.Format(time.RFC3339))
	}

	robfigFirst := robfigNext(t, "30 1 * * *", ref)
	if !robfigFirst.UTC().Equal(expectedFirst) {
		t.Fatalf("unexpected robfig NY fold first: got %s want %s", robfigFirst.UTC().Format(time.RFC3339), expectedFirst.Format(time.RFC3339))
	}
	robfigSecond := robfigNext(t, "30 1 * * *", robfigFirst)
	t.Logf("robfig NY fold: first=%s second=%s (expected subsequent %s)", robfigFirst.UTC().Format(time.RFC3339), robfigSecond.UTC().Format(time.RFC3339), expectedSubsequent.Format(time.RFC3339))
	if robfigSecond.UTC().Equal(expectedSubsequent) {
		t.Fatalf("expected robfig to emit duplicate UTC occurrence on NY fold, but it skipped to next day")
	}

	cronexprFirst := cronexprNext(t, "30 1 * * *", ref)
	if !cronexprFirst.UTC().Equal(expectedFirst) {
		t.Fatalf("unexpected cronexpr NY fold first: got %s want %s", cronexprFirst.UTC().Format(time.RFC3339), expectedFirst.Format(time.RFC3339))
	}
	cronexprSecond := cronexprNext(t, "30 1 * * *", cronexprFirst)
	t.Logf("cronexpr NY fold: first=%s second=%s", cronexprFirst.UTC().Format(time.RFC3339), cronexprSecond.UTC().Format(time.RFC3339))
	if !cronexprSecond.UTC().Equal(expectedSubsequent) {
		t.Fatalf("unexpected cronexpr NY fold subsequent: got %s want %s", cronexprSecond.UTC().Format(time.RFC3339), expectedSubsequent.Format(time.RFC3339))
	}
	_ = loc
	t.Logf("Decision evidence: robfig FAIL fold NY (duplicate 06:30Z same day); cronexpr PASS fold NY; Deadbolt PASS")
}

// TestCandidateComparison_DSTFold_London executes candidates against the London fold fixture.
func TestCandidateComparison_DSTFold_London(t *testing.T) {
	loc := mustLoadLocation(t, "Europe/London")
	ref := mustParseTime(t, "2026-10-25T00:00:00Z").In(loc)
	expectedFirst := mustParseTime(t, "2026-10-25T00:30:00Z")
	expectedSubsequent := mustParseTime(t, "2026-10-26T01:30:00Z")

	deadboltFirst := deadboltNext(t, "30 1 * * *", "Europe/London", mustParseTime(t, "2026-10-25T00:00:00Z"))
	if !deadboltFirst.Equal(expectedFirst) {
		t.Fatalf("Deadbolt failed London fold first: expected %s, got %s", expectedFirst.Format(time.RFC3339), deadboltFirst.Format(time.RFC3339))
	}

	robfigFirst := robfigNext(t, "30 1 * * *", ref)
	t.Logf("robfig London fold first => %s (expected %s)", robfigFirst.UTC().Format(time.RFC3339), expectedFirst.Format(time.RFC3339))
	if !robfigFirst.UTC().Equal(expectedFirst) {
		t.Fatalf("unexpected robfig London fold first: got %s", robfigFirst.UTC().Format(time.RFC3339))
	}
	robfigSecond := robfigNext(t, "30 1 * * *", robfigFirst)
	t.Logf("robfig London fold second => %s (expected %s)", robfigSecond.UTC().Format(time.RFC3339), expectedSubsequent.Format(time.RFC3339))
	if robfigSecond.UTC().Equal(expectedSubsequent) {
		t.Fatalf("expected robfig to emit duplicate on London fold, but it skipped")
	}

	cronexprFirst := cronexprNext(t, "30 1 * * *", ref)
	t.Logf("cronexpr London fold first => %s (expected %s)", cronexprFirst.UTC().Format(time.RFC3339), expectedFirst.Format(time.RFC3339))
	if cronexprFirst.UTC().Equal(expectedFirst) {
		t.Fatalf("expected cronexpr to fail London fold first (select second occurrence), but it matched")
	}
	t.Logf("Decision evidence: robfig FAIL fold London (duplicate same-day 01:30Z); cronexpr FAIL fold London (picks 01:30Z not 00:30Z); Deadbolt PASS")
}

// TestCandidateComparison_LordHowe executes candidates against the half-hour transition fixture.
func TestCandidateComparison_LordHowe(t *testing.T) {
	loc := mustLoadLocation(t, "Australia/Lord_Howe")
	ref := mustParseTime(t, "2026-10-03T15:00:00Z").In(loc)
	expected := mustParseTime(t, "2026-10-04T15:15:00Z")

	deadbolt := deadboltNext(t, "15 2 * * *", "Australia/Lord_Howe", mustParseTime(t, "2026-10-03T15:00:00Z"))
	if !deadbolt.Equal(expected) {
		t.Fatalf("Deadbolt failed Lord Howe: expected %s, got %s", expected.Format(time.RFC3339), deadbolt.Format(time.RFC3339))
	}

	robfig := robfigNext(t, "15 2 * * *", ref)
	t.Logf("robfig Lord Howe => %s", robfig.UTC().Format(time.RFC3339))
	if !robfig.UTC().Equal(expected) {
		t.Fatalf("unexpected robfig Lord Howe: got %s want %s", robfig.UTC().Format(time.RFC3339), expected.Format(time.RFC3339))
	}

	cronExpr := cronexprNext(t, "15 2 * * *", ref)
	t.Logf("cronexpr Lord Howe => %s", cronExpr.UTC().Format(time.RFC3339))
	if cronExpr.UTC().Equal(expected) {
		t.Fatalf("expected cronexpr to fail Lord Howe half-hour gap, but it matched")
	}
	t.Logf("Decision evidence: robfig PASS Lord Howe; cronexpr FAIL; Deadbolt PASS")
}

// TestCandidateComparison_MisfireDowntime proves that naive Next-iteration on either pinned
// library queues every missed run (backlog flood), while Deadbolt coalesce-one yields at most
// one run with exact skipped_count.
func TestCandidateComparison_MisfireDowntime(t *testing.T) {
	spec, err := scheduling.ParseSchedule("0 * * * *", "UTC")
	if err != nil {
		t.Fatalf("failed to parse schedule: %v", err)
	}

	lastOcc := time.Date(2026, 4, 10, 10, 0, 0, 0, time.UTC)
	nowTime := time.Date(2026, 4, 10, 14, 30, 0, 0, time.UTC)

	// Naive backlog via robfig: iterate Next in UTC (hourly UTC has no TZ ambiguity).
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	robfigSched, err := parser.Parse("0 * * * *")
	if err != nil {
		t.Fatalf("robfig parse failed: %v", err)
	}
	var robfigBacklog []time.Time
	cursor := lastOcc
	for {
		nxt := robfigSched.Next(cursor)
		if nxt.After(nowTime) {
			break
		}
		robfigBacklog = append(robfigBacklog, nxt.UTC())
		cursor = nxt
		if len(robfigBacklog) > 10 {
			t.Fatalf("robfig backlog unexpectedly large")
		}
	}
	if len(robfigBacklog) != 4 {
		t.Fatalf("expected robfig naive backlog of 4 queued runs, got %d (%v)", len(robfigBacklog), robfigBacklog)
	}
	t.Logf("robfig naive queues %d runs (backlog flood); §17 forbids this without coalesce-one", len(robfigBacklog))

	// Naive backlog via cronexpr.
	cronExpr, err := cronexpr.Parse("0 * * * *")
	if err != nil {
		t.Fatalf("cronexpr parse failed: %v", err)
	}
	var cronexprBacklog []time.Time
	cursor = lastOcc
	for {
		nxt := cronExpr.Next(cursor)
		if nxt.After(nowTime) {
			break
		}
		cronexprBacklog = append(cronexprBacklog, nxt.UTC())
		cursor = nxt
		if len(cronexprBacklog) > 10 {
			t.Fatalf("cronexpr backlog unexpectedly large")
		}
	}
	if len(cronexprBacklog) != 4 {
		t.Fatalf("expected cronexpr naive backlog of 4 queued runs, got %d", len(cronexprBacklog))
	}
	t.Logf("cronexpr naive queues %d runs (backlog flood)", len(cronexprBacklog))

	// Deadbolt coalesce-one with bounded memory.
	res, err := spec.CoalesceMissed(lastOcc, nowTime)
	if err != nil {
		t.Fatalf("failed to coalesce missed: %v", err)
	}
	if res.MissedCount != 4 {
		t.Fatalf("expected MissedCount=4, got %d", res.MissedCount)
	}
	if res.MissedOccurrences != nil {
		t.Fatalf("bounded-memory violation: MissedOccurrences must be nil, got %d entries", len(res.MissedOccurrences))
	}
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
	t.Logf("Deadbolt coalesce-one passed: 4 missed coalesced to latest %s (skipped_count=%d), next future %s",
		res.CoalescedOccurrence.Format(time.RFC3339), res.SkippedCount, res.NextFuture.Format(time.RFC3339))
}

// TestOccurrenceKeyCanonicalIdentity_FormatHelper is a pure unit helper for INV-10 key formatting.
// It does NOT prove concurrency safety. The authoritative duplicate-evaluator proof is
// TestScheduleConcurrentDuplicateEvaluatorsINV10_Postgres in tests/integration/schedule_contract_test.go,
// which races separate PostgreSQL transactions/connections against the canonical
// UNIQUE (schedule_id, revision, due_at) constraint with distinct occurrence_key strings.
func TestOccurrenceKeyCanonicalIdentity_FormatHelper(t *testing.T) {
	scheduleID := "10000000-0000-0000-0000-000000000001"
	revision := int64(1)
	dueAt := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	keyA := scheduling.FormatOccurrenceKey(scheduleID, revision, dueAt)
	keyB := scheduling.FormatOccurrenceKey(scheduleID, revision, dueAt)
	if keyA != keyB {
		t.Fatalf("occurrence key formatting is not deterministic: %q vs %q", keyA, keyB)
	}
	keyOtherRevision := scheduling.FormatOccurrenceKey(scheduleID, revision+1, dueAt)
	if keyA == keyOtherRevision {
		t.Fatalf("occurrence keys must differ across revisions")
	}
	t.Logf("format helper: canonical identity (schedule_id, revision, due_at) => %s; see integration PG race for concurrency proof", keyA)
}
