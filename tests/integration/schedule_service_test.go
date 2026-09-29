package integration_test

// Schedule definition service (Issue #34, Blueprint §17).
//
// The SP-05 work in #30 proved the cron/DST evaluator and the database
// contract. These tests cover the layer that was missing: the audited,
// scoped service that creates and governs a definition, and the V1 active
// schedule cap.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/scheduling"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
)

type scheduleFixture struct {
	orgID  string
	envID  string
	audit  *tenant.AuditContext
	svc    *scheduling.Service
	tenant *tenantTestContext
}

func setupScheduleFixture(t *testing.T) *scheduleFixture {
	t.Helper()
	tc := setupTenantContext(t)
	org, err := tc.service.CreateOrganization(context.Background(), newUUID(t), "Sched Org")
	if err != nil {
		t.Fatalf("create organization: %v", err)
	}
	proj, err := tc.service.CreateProject(context.Background(), org.ID, "Sched Proj")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	env, err := tc.service.CreateEnvironment(context.Background(), org.ID, proj.ID, tenant.EnvStaging, 10)
	if err != nil {
		t.Fatalf("create environment: %v", err)
	}
	return &scheduleFixture{
		orgID:  org.ID,
		envID:  env.ID,
		tenant: tc,
		svc:    scheduling.NewService(tc.pool),
		audit: &tenant.AuditContext{
			ActorType:     tenant.IdentityTypeMachine,
			CorrelationID: "sched-test",
		},
	}
}

func newUUID(t *testing.T) string {
	t.Helper()
	id, err := tenant.NewUUID()
	if err != nil {
		t.Fatalf("uuid: %v", err)
	}
	return id
}

func TestScheduleServiceCreateComputesFirstDueAndPolicy(t *testing.T) {
	f := setupScheduleFixture(t)
	defer f.tenant.cleanup()
	ctx := context.Background()

	s, err := f.svc.Create(ctx, f.orgID, f.envID, scheduling.CreateScheduleRequest{
		WorkflowName:   "nightly-recon",
		CronExpression: "0 12 * * *",
		Timezone:       "America/New_York",
	}, f.audit)
	if err != nil {
		t.Fatalf("create schedule: %v", err)
	}

	if s.Paused {
		t.Fatal("a new schedule must be active")
	}
	if s.Revision != 1 {
		t.Fatalf("new schedule must start at revision 1, got %d", s.Revision)
	}
	// §17 fixes both policies; the service must not let a caller choose.
	if s.OverlapPolicy != "skip-overlap" {
		t.Fatalf("overlap policy = %q, want skip-overlap", s.OverlapPolicy)
	}
	if s.MisfirePolicy != "coalesce-one" {
		t.Fatalf("misfire policy = %q, want coalesce-one", s.MisfirePolicy)
	}
	if s.NextDueAt == nil {
		t.Fatal("an active schedule must have a first due time")
	}
	if !s.NextDueAt.After(time.Now().Add(-time.Minute)) {
		t.Fatalf("first due time %s is not in the future", s.NextDueAt)
	}
	if s.DeploymentID != nil {
		t.Fatal("an unpinned schedule must not store a deployment id")
	}

	// The definition must be audited as a schedule resource.
	var action string
	if err := f.tenant.pool.WithTenantTx(ctx, f.orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `
			SELECT action FROM audit_events
			WHERE organization_id=$1::uuid AND target_type='schedule' AND target_id=$2::uuid
			ORDER BY created_at DESC LIMIT 1`, f.orgID, s.ID).Scan(&action)
	}); err != nil {
		t.Fatalf("read audit event: %v", err)
	}
	if action != "schedule.create" {
		t.Fatalf("audited action = %q, want schedule.create", action)
	}
}

func TestScheduleServiceRejectsInvalidDefinition(t *testing.T) {
	f := setupScheduleFixture(t)
	defer f.tenant.cleanup()
	ctx := context.Background()

	cases := []struct {
		name string
		req  scheduling.CreateScheduleRequest
		want error
	}{
		{"bad cron", scheduling.CreateScheduleRequest{WorkflowName: "w", CronExpression: "not a cron", Timezone: "UTC"}, scheduling.ErrInvalidCron},
		{"bad timezone", scheduling.CreateScheduleRequest{WorkflowName: "w", CronExpression: "0 12 * * *", Timezone: "Mars/Olympus"}, scheduling.ErrInvalidTimezone},
		{"missing workflow", scheduling.CreateScheduleRequest{CronExpression: "0 12 * * *"}, scheduling.ErrInvalidWorkflow},
		{"cross-environment deployment pin", scheduling.CreateScheduleRequest{WorkflowName: "w", CronExpression: "0 12 * * *", Timezone: "UTC", DeploymentID: newUUIDPtr(t)}, scheduling.ErrPinnedDeployment},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := f.svc.Create(ctx, f.orgID, f.envID, tc.req, f.audit)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestScheduleServiceEditAdvancesRevisionForFutureOnly(t *testing.T) {
	f := setupScheduleFixture(t)
	defer f.tenant.cleanup()
	ctx := context.Background()

	s, err := f.svc.Create(ctx, f.orgID, f.envID, scheduling.CreateScheduleRequest{
		WorkflowName: "nightly-recon", CronExpression: "0 12 * * *", Timezone: "UTC",
	}, f.audit)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	newCron := "30 6 * * *"
	updated, err := f.svc.Update(ctx, f.orgID, f.envID, s.ID, scheduling.UpdateScheduleRequest{
		ExpectedRevision: s.Revision,
		CronExpression:   &newCron,
	}, f.audit)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.Revision != s.Revision+1 {
		t.Fatalf("edit must advance revision: %d -> %d", s.Revision, updated.Revision)
	}
	if updated.CronExpression != newCron {
		t.Fatalf("cron = %q, want %q", updated.CronExpression, newCron)
	}

	// A stale writer must be rejected rather than silently overwriting an
	// edit it never observed.
	_, err = f.svc.Update(ctx, f.orgID, f.envID, s.ID, scheduling.UpdateScheduleRequest{
		ExpectedRevision: s.Revision,
		CronExpression:   &newCron,
	}, f.audit)
	if !errors.Is(err, scheduling.ErrRevisionConflict) {
		t.Fatalf("stale edit error = %v, want REVISION_CONFLICT", err)
	}

	// An occurrence created under the old revision keeps it: the edit must
	// not retroactively move work that already exists.
	oldDue := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	var occRev int64
	if err := f.tenant.pool.WithTenantTx(ctx, f.orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `
			INSERT INTO schedule_occurrences
				(organization_id, schedule_id, occurrence_key, due_at, revision, status)
			VALUES ($1::uuid,$2::uuid,'old-rev',$3,1,'PENDING')
			RETURNING revision`, f.orgID, s.ID, oldDue).Scan(&occRev)
	}); err != nil {
		t.Fatalf("seed pre-edit occurrence: %v", err)
	}
	if occRev != 1 {
		t.Fatalf("pre-edit occurrence revision = %d, want 1", occRev)
	}
	after, err := f.svc.Get(ctx, f.orgID, f.envID, s.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if after.Revision != 2 {
		t.Fatalf("schedule revision = %d, want 2", after.Revision)
	}
}

func TestScheduleServicePauseAndResume(t *testing.T) {
	f := setupScheduleFixture(t)
	defer f.tenant.cleanup()
	ctx := context.Background()

	s, err := f.svc.Create(ctx, f.orgID, f.envID, scheduling.CreateScheduleRequest{
		WorkflowName: "nightly-recon", CronExpression: "0 12 * * *", Timezone: "UTC",
	}, f.audit)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	paused, err := f.svc.Pause(ctx, f.orgID, f.envID, s.ID, scheduling.ControlScheduleRequest{ExpectedRevision: s.Revision}, f.audit)
	if err != nil {
		t.Fatalf("pause: %v", err)
	}
	if !paused.Paused {
		t.Fatal("schedule must be paused")
	}
	if paused.NextDueAt != nil {
		t.Fatal("a paused schedule must have no pending due time")
	}
	if _, err := f.svc.Pause(ctx, f.orgID, f.envID, s.ID, scheduling.ControlScheduleRequest{}, f.audit); !errors.Is(err, scheduling.ErrScheduleAlreadyPaused) {
		t.Fatalf("double pause error = %v, want SCHEDULE_ALREADY_PAUSED", err)
	}

	resumed, err := f.svc.Resume(ctx, f.orgID, f.envID, s.ID, scheduling.ControlScheduleRequest{ExpectedRevision: paused.Revision}, f.audit)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if resumed.Paused {
		t.Fatal("schedule must be active after resume")
	}
	if resumed.NextDueAt == nil {
		t.Fatal("resume must recompute a due time")
	}
	if !resumed.NextDueAt.After(time.Now().Add(-time.Minute)) {
		t.Fatal("resumed due time must be recomputed from now, not from the pause instant")
	}
}

func TestScheduleServiceCapsActiveSchedulesUnderConcurrentCreates(t *testing.T) {
	f := setupScheduleFixture(t)
	defer f.tenant.cleanup()
	ctx := context.Background()

	// Fill the environment to the hard cap through the service itself.
	for i := 0; i < scheduling.MaxActiveSchedulesPerEnvironment; i++ {
		if _, err := f.svc.Create(ctx, f.orgID, f.envID, scheduling.CreateScheduleRequest{
			WorkflowName:   fmt.Sprintf("wf-%03d", i),
			CronExpression: "0 12 * * *",
			Timezone:       "UTC",
		}, f.audit); err != nil {
			t.Fatalf("seed schedule %d: %v", i, err)
		}
	}

	// The 101st must be refused.
	_, err := f.svc.Create(ctx, f.orgID, f.envID, scheduling.CreateScheduleRequest{
		WorkflowName: "one-too-many", CronExpression: "0 12 * * *", Timezone: "UTC",
	}, f.audit)
	if !errors.Is(err, scheduling.ErrScheduleLimitReached) {
		t.Fatalf("error = %v, want SCHEDULE_LIMIT_REACHED", err)
	}

	// A paused schedule frees a slot, so a create must succeed again.
	listed, err := f.svc.List(ctx, f.orgID, f.envID)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != scheduling.MaxActiveSchedulesPerEnvironment {
		t.Fatalf("listed %d schedules, want %d", len(listed), scheduling.MaxActiveSchedulesPerEnvironment)
	}
	if _, err := f.svc.Pause(ctx, f.orgID, f.envID, listed[0].ID, scheduling.ControlScheduleRequest{}, f.audit); err != nil {
		t.Fatalf("pause to free a slot: %v", err)
	}
	if _, err := f.svc.Create(ctx, f.orgID, f.envID, scheduling.CreateScheduleRequest{
		WorkflowName: "now-fits", CronExpression: "0 12 * * *", Timezone: "UTC",
	}, f.audit); err != nil {
		t.Fatalf("create after freeing a slot: %v", err)
	}
}

// TestScheduleServiceCapSurvivesConcurrentCreates proves the cap is decided
// under the environment admission lock. Without that lock two evaluators
// would both count 99 and both insert, ending above the hard cap.
func TestScheduleServiceCapSurvivesConcurrentCreates(t *testing.T) {
	f := setupScheduleFixture(t)
	defer f.tenant.cleanup()
	ctx := context.Background()

	// Leave exactly one free slot.
	for i := 0; i < scheduling.MaxActiveSchedulesPerEnvironment-1; i++ {
		if _, err := f.svc.Create(ctx, f.orgID, f.envID, scheduling.CreateScheduleRequest{
			WorkflowName:   fmt.Sprintf("fill-%03d", i),
			CronExpression: "0 12 * * *",
			Timezone:       "UTC",
		}, f.audit); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	const racers = 6
	var wg sync.WaitGroup
	var mu sync.Mutex
	created, limited := 0, 0
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := f.svc.Create(ctx, f.orgID, f.envID, scheduling.CreateScheduleRequest{
				WorkflowName:   fmt.Sprintf("race-%d", i),
				CronExpression: "0 12 * * *",
				Timezone:       "UTC",
			}, f.audit)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				created++
			case errors.Is(err, scheduling.ErrScheduleLimitReached):
				limited++
			default:
				t.Errorf("unexpected create error: %v", err)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if created != 1 {
		t.Fatalf("%d concurrent creates succeeded, want exactly 1", created)
	}
	if limited != racers-1 {
		t.Fatalf("%d refused for the cap, want %d", limited, racers-1)
	}

	var active int
	if err := f.tenant.pool.WithTenantTx(ctx, f.orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `
			SELECT count(*) FROM schedules
			WHERE environment_id=$1::uuid AND paused=false`, f.envID).Scan(&active)
	}); err != nil {
		t.Fatalf("count active: %v", err)
	}
	if active != scheduling.MaxActiveSchedulesPerEnvironment {
		t.Fatalf("active schedules = %d, want exactly the cap %d", active, scheduling.MaxActiveSchedulesPerEnvironment)
	}
}

func TestScheduleServiceCrossTenantInvisible(t *testing.T) {
	f := setupScheduleFixture(t)
	defer f.tenant.cleanup()
	ctx := context.Background()

	s, err := f.svc.Create(ctx, f.orgID, f.envID, scheduling.CreateScheduleRequest{
		WorkflowName: "nightly-recon", CronExpression: "0 12 * * *", Timezone: "UTC",
	}, f.audit)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	otherOrg, err := f.tenant.service.CreateOrganization(ctx, newUUID(t), "Other Org")
	if err != nil {
		t.Fatalf("create other org: %v", err)
	}
	other := &scheduleFixture{orgID: otherOrg.ID, tenant: f.tenant, svc: scheduling.NewService(f.tenant.pool), audit: f.audit}

	if _, err := other.svc.Get(ctx, other.orgID, otherOrg.ID, s.ID); !errors.Is(err, scheduling.ErrScheduleNotFound) {
		t.Fatalf("cross-tenant read error = %v, want SCHEDULE_NOT_FOUND", err)
	}
	if err := other.svc.Delete(ctx, other.orgID, otherOrg.ID, s.ID, f.audit); !errors.Is(err, scheduling.ErrScheduleNotFound) {
		t.Fatalf("cross-tenant delete error = %v, want SCHEDULE_NOT_FOUND", err)
	}
	if err := f.svc.Delete(ctx, f.orgID, f.envID, s.ID, f.audit); err != nil {
		t.Fatalf("owner delete: %v", err)
	}
	if _, err := f.svc.Get(ctx, f.orgID, f.envID, s.ID); !errors.Is(err, scheduling.ErrScheduleNotFound) {
		t.Fatalf("post-delete read error = %v, want SCHEDULE_NOT_FOUND", err)
	}
}

func newUUIDPtr(t *testing.T) *string {
	t.Helper()
	id := newUUID(t)
	return &id
}
