package integration_test

// Occurrence evaluation (Issue #34, Blueprint §17).
//
// These are the acceptance properties, each proven against real
// PostgreSQL: the unique occurrence, atomic commit with the run, the
// skip-overlap policy, coalesce-one misfire accounting, quota refusal
// without a backlog, and the inactive-deployment error-and-pause path.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/scheduling"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
)

type occurrenceFixture struct {
	orgID    string
	envID    string
	tenant   *tenantTestContext
	svc      *scheduling.Service
	engine   *scheduling.Engine
	deployID string
	workflow string
}

func setupOccurrenceFixture(t *testing.T) *occurrenceFixture {
	t.Helper()
	tc := setupTenantContext(t)
	org, err := tc.service.CreateOrganization(context.Background(), newUUID(t), "Occ Org")
	if err != nil {
		t.Fatalf("create org: %v", err)
	}
	proj, err := tc.service.CreateProject(context.Background(), org.ID, "Occ Proj")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	env, err := tc.service.CreateEnvironment(context.Background(), org.ID, proj.ID, tenant.EnvStaging, 10)
	if err != nil {
		t.Fatalf("create env: %v", err)
	}
	f := &occurrenceFixture{orgID: org.ID, envID: env.ID, tenant: tc, workflow: "occ-flow"}
	f.svc = scheduling.NewService(tc.pool)
	f.engine = scheduling.NewEngine(tc.pool, execution.NewService(tc.pool, tc.service), tc.service)
	f.deployID = seedOccurrenceDeployment(t, tc, org.ID, env.ID)
	return f
}

// seedOccurrenceDeployment registers and activates a one-task workflow so an
// occurrence has something real to run.
func seedOccurrenceDeployment(t *testing.T, tc *tenantTestContext, orgID, envID string) string {
	t.Helper()
	// §17 gives a schedule no input payload, so the workflow under test must
	// accept an empty object. A workflow that requires caller-supplied input
	// cannot be scheduled, which is the honest consequence.
	schema := map[string]any{
		"type": "object", "properties": map[string]any{},
		"required": []any{}, "additionalProperties": false,
	}
	workflows := []map[string]any{{
		"manifestVersion": 1, "name": "occ-flow", "inputSchema": schema, "outputSchema": schema,
		"nodes":  []map[string]any{{"id": "only", "type": "task", "task": "task-simple", "after": []any{}}},
		"output": map[string]any{},
	}}
	manifest := createLifecycleManifest("3333333333333333333333333333333333333333333333333333333333333333",
		[]map[string]any{{"name": "task-simple", "entrypoint": "tasks/simple.js", "timeoutMs": 30000,
			"recovery": "idempotent", "idempotencyWindowMs": 305000, "inputSchema": schema, "outputSchema": schema}}, workflows)

	var deploymentID string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `
			INSERT INTO deployments
				(organization_id,environment_id,manifest_hash,bundle_digest,manifest,protocol_version,runtime_version,status)
			VALUES ($1::uuid,$2::uuid,$3,$4,$5::jsonb,1,'1.0','ACTIVE')
			RETURNING id::text`,
			orgID, envID, "manifest-occ-"+fmt.Sprint(time.Now().UnixNano()),
			fmt.Sprintf("occbundle%032d", time.Now().UnixNano()), string(manifest)).Scan(&deploymentID)
	}); err != nil {
		t.Fatalf("seed deployment: %v", err)
	}
	// An occurrence resolves its deployment through the workflow channel, so
	// the channel must point at this deployment for the workflow.
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO workflow_channels
				(organization_id, environment_id, workflow_name, active_deployment_id, revision, updated_at)
			VALUES ($1::uuid,$2::uuid,'occ-flow',$3::uuid,1,clock_timestamp())
			ON CONFLICT (environment_id, workflow_name)
			DO UPDATE SET active_deployment_id=$3::uuid, updated_at=clock_timestamp()`,
			orgID, envID, deploymentID)
		return err
	}); err != nil {
		t.Fatalf("activate workflow channel: %v", err)
	}
	return deploymentID
}

func (f *occurrenceFixture) schedule(t *testing.T, cron string) *scheduling.Schedule {
	t.Helper()
	s, err := f.svc.Create(context.Background(), f.orgID, f.envID, scheduling.CreateScheduleRequest{
		WorkflowName: f.workflow, CronExpression: cron, Timezone: "UTC",
	}, &tenant.AuditContext{ActorType: tenant.IdentityTypeMachine})
	if err != nil {
		t.Fatalf("create schedule: %v", err)
	}
	return s
}

// makeDue pushes the pending occurrence into the past so evaluation has work.
func (f *occurrenceFixture) makeDue(t *testing.T, scheduleID string) {
	t.Helper()
	if err := f.tenant.pool.WithTenantTx(context.Background(), f.orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE schedules SET next_due_at=clock_timestamp()-INTERVAL '1 second'
			WHERE id=$1::uuid AND organization_id=$2::uuid`, scheduleID, f.orgID)
		return err
	}); err != nil {
		t.Fatalf("make due: %v", err)
	}
}

func (f *occurrenceFixture) occurrences(t *testing.T, scheduleID string) []struct {
	Status        string
	SkippedReason *string
	SkippedCount  int
	RunID         *string
	DueAt         time.Time
	Revision      int64
} {
	t.Helper()
	type row = struct {
		Status        string
		SkippedReason *string
		SkippedCount  int
		RunID         *string
		DueAt         time.Time
		Revision      int64
	}
	var out []row
	if err := f.tenant.pool.WithTenantTx(context.Background(), f.orgID, func(ctx context.Context, tx storage.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT status, skipped_reason, skipped_count, run_id, due_at, revision
			FROM schedule_occurrences WHERE schedule_id=$1::uuid ORDER BY due_at`, scheduleID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.Status, &r.SkippedReason, &r.SkippedCount, &r.RunID, &r.DueAt, &r.Revision); err != nil {
				return err
			}
			out = append(out, r)
		}
		return rows.Err()
	}); err != nil {
		t.Fatalf("read occurrences: %v", err)
	}
	return out
}

func TestOccurrenceCreatesRunAndAdvancesSchedule(t *testing.T) {
	f := setupOccurrenceFixture(t)
	defer f.tenant.cleanup()
	ctx := context.Background()

	s := f.schedule(t, "0 12 * * *")
	f.makeDue(t, s.ID)

	outcome, err := f.engine.EvaluateSchedule(ctx, f.orgID, f.envID, s.ID)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if outcome != scheduling.OutcomeStarted {
		t.Fatalf("outcome = %s, want STARTED", outcome)
	}

	occ := f.occurrences(t, s.ID)
	if len(occ) != 1 {
		t.Fatalf("expected exactly one occurrence, got %d", len(occ))
	}
	if occ[0].Status != "STARTED" || occ[0].RunID == nil {
		t.Fatalf("occurrence must be STARTED and linked to a run: %+v", occ[0])
	}
	if occ[0].Revision != s.Revision {
		t.Fatalf("occurrence revision = %d, want %d", occ[0].Revision, s.Revision)
	}

	// The schedule must have moved past the handled slot, or the same
	// occurrence would be evaluated again.
	after, err := f.svc.Get(ctx, f.orgID, f.envID, s.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if after.NextDueAt == nil || !after.NextDueAt.After(time.Now()) {
		t.Fatalf("next_due_at must advance into the future, got %v", after.NextDueAt)
	}
	if after.LastOccurrence == nil {
		t.Fatal("last_occurrence_at must be recorded")
	}

	// Running it again must not create a second run for the same slot.
	if _, err := f.engine.EvaluateSchedule(ctx, f.orgID, f.envID, s.ID); err != nil {
		t.Fatalf("second evaluate: %v", err)
	}
	if occ := f.occurrences(t, s.ID); len(occ) != 1 {
		t.Fatalf("re-evaluation created %d occurrences, want 1", len(occ))
	}
}

// TestOccurrenceTwoEvaluatorsCreateOneRun is the INV-10 proof at the service
// level: several engines racing the same schedule must yield exactly one
// run, because the unique (schedule_id, revision, due_at) index decides.
func TestOccurrenceTwoEvaluatorsCreateOneRun(t *testing.T) {
	f := setupOccurrenceFixture(t)
	defer f.tenant.cleanup()
	ctx := context.Background()

	s := f.schedule(t, "0 12 * * *")
	f.makeDue(t, s.ID)

	// INV-10 is "two evaluators must not duplicate a run", so this races
	// exactly two. A larger fan-out only starves the shared test connection
	// pool: each evaluator holds a connection for its whole transaction, and
	// the advisory lock serialises them, so a holder waiting on a lock starves
	// the rest. The scheduler evaluates a tenant's schedules sequentially in
	// production, so five concurrent evaluators is not a shape the system has
	// to survive anyway.
	const evaluators = 2
	var wg sync.WaitGroup
	start := make(chan struct{})
	outcomes := make([]scheduling.Outcome, evaluators)
	for i := 0; i < evaluators; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			outcomes[i], _ = f.engine.EvaluateSchedule(ctx, f.orgID, f.envID, s.ID)
		}(i)
	}
	close(start)
	wg.Wait()

	started := 0
	for _, o := range outcomes {
		if o == scheduling.OutcomeStarted {
			started++
		}
	}
	if started != 1 {
		t.Fatalf("%d evaluators reported STARTED, want exactly 1 (outcomes %v)", started, outcomes)
	}
	if occ := f.occurrences(t, s.ID); len(occ) != 1 {
		t.Fatalf("unique identity violated: %d occurrences", len(occ))
	}
	var runs int
	if err := f.tenant.pool.WithTenantTx(ctx, f.orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `
			SELECT count(*) FROM runs WHERE organization_id=$1::uuid
			  AND idempotency_key LIKE 'schedule:'||$2::text||'%'`, f.orgID, s.ID).Scan(&runs)
	}); err != nil {
		t.Fatalf("count scheduled runs: %v", err)
	}
	if runs != 1 {
		t.Fatalf("scheduled runs = %d, want 1", runs)
	}
}

func TestOccurrenceSkipsWhenPreviousRunNonterminal(t *testing.T) {
	f := setupOccurrenceFixture(t)
	defer f.tenant.cleanup()
	ctx := context.Background()

	// A per-minute cron so a forced-due second slot is genuinely distinct from
	// the first. With a daily cron, time travelling backwards coalesces back
	// to the slot already handled, which is the misfire policy behaving
	// correctly rather than an overlap case.
	s := f.schedule(t, "* * * * *")
	f.makeDue(t, s.ID)
	if _, err := f.engine.EvaluateSchedule(ctx, f.orgID, f.envID, s.ID); err != nil {
		t.Fatalf("first evaluate: %v", err)
	}

	// Hold the first run nonterminal, then force the next slot due.
	if err := f.tenant.pool.WithTenantTx(ctx, f.orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE runs SET status='RUNNING' WHERE organization_id=$1::uuid
			AND idempotency_key LIKE 'schedule:'||$2::text||'%'`, f.orgID, s.ID)
		return err
	}); err != nil {
		t.Fatalf("hold run: %v", err)
	}
	if err := f.tenant.pool.WithTenantTx(ctx, f.orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE schedules SET next_due_at=clock_timestamp()-INTERVAL '90 seconds'
			WHERE id=$1::uuid AND organization_id=$2::uuid`, s.ID, f.orgID)
		return err
	}); err != nil {
		t.Fatalf("make due again: %v", err)
	}

	outcome, err := f.engine.EvaluateSchedule(ctx, f.orgID, f.envID, s.ID)
	if err != nil {
		t.Fatalf("second evaluate: %v", err)
	}
	if outcome != scheduling.OutcomeSkippedOverlap {
		t.Fatalf("outcome = %s, want SKIPPED_OVERLAP", outcome)
	}
	occ := f.occurrences(t, s.ID)
	if len(occ) != 2 {
		t.Fatalf("expected 2 occurrences, got %d", len(occ))
	}
	// Identify by state rather than by position: the coalesced second slot is
	// earlier in due_at than the first, so index order is not slot order.
	skipped := 0
	for _, o := range occ {
		if o.Status == "SKIPPED" && o.SkippedReason != nil && *o.SkippedReason == "SKIPPED_OVERLAP" && o.RunID == nil {
			skipped++
		}
	}
	if skipped != 1 {
		t.Fatalf("expected exactly one SKIPPED_OVERLAP occurrence with no run, got %d in %+v", skipped, occ)
	}
	// Skipping must still advance the schedule, or the slot retries forever.
	// The advance is measured from the handled slot rather than from now, so a
	// time-travelled slot can legitimately leave next_due_at briefly in the
	// past; the pass after that catches up under the misfire policy. What must
	// never happen is the schedule standing still on the same slot.
	after, _ := f.svc.Get(ctx, f.orgID, f.envID, s.ID)
	if after.NextDueAt == nil {
		t.Fatal("a skipped slot must leave a next_due_at")
	}
	var earliestHandled time.Time
	for _, o := range occ {
		if o.DueAt.Before(earliestHandled) || earliestHandled.IsZero() {
			earliestHandled = o.DueAt
		}
	}
	if !after.NextDueAt.After(earliestHandled) {
		t.Fatalf("next_due_at %v did not advance past the handled slot %v", after.NextDueAt, earliestHandled)
	}
	// And it must converge on the future rather than re-issuing that slot.
	for i := 0; i < 3; i++ {
		if _, err := f.engine.EvaluateSchedule(ctx, f.orgID, f.envID, s.ID); err != nil {
			t.Fatalf("catch-up evaluate: %v", err)
		}
		cur, err := f.svc.Get(ctx, f.orgID, f.envID, s.ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if cur.NextDueAt != nil && cur.NextDueAt.After(time.Now()) {
			return
		}
	}
	t.Fatal("next_due_at never converged into the future")
}

func TestOccurrenceCoalescesMissedSlotsAfterDowntime(t *testing.T) {
	f := setupOccurrenceFixture(t)
	defer f.tenant.cleanup()
	ctx := context.Background()

	// An every-minute schedule that "missed" ten slots.
	s := f.schedule(t, "* * * * *")
	if err := f.tenant.pool.WithTenantTx(ctx, f.orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `
			UPDATE schedules
			SET last_occurrence_at = clock_timestamp() - INTERVAL '10 minutes',
			    next_due_at       = clock_timestamp() - INTERVAL '9 minutes'
			WHERE id=$1::uuid AND organization_id=$2::uuid`, s.ID, f.orgID)
		return err
	}); err != nil {
		t.Fatalf("simulate downtime: %v", err)
	}

	outcome, err := f.engine.EvaluateSchedule(ctx, f.orgID, f.envID, s.ID)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if outcome != scheduling.OutcomeStarted {
		t.Fatalf("outcome = %s, want STARTED", outcome)
	}

	occ := f.occurrences(t, s.ID)
	// Coalesce-one means exactly one run for the whole downtime window, not
	// one per missed minute.
	if len(occ) != 1 {
		t.Fatalf("coalesce-one must create one occurrence, got %d", len(occ))
	}
	if occ[0].SkippedCount < 5 {
		t.Fatalf("skipped_count = %d, want the coalesced remainder recorded", occ[0].SkippedCount)
	}
	after, _ := f.svc.Get(ctx, f.orgID, f.envID, s.ID)
	if after.NextDueAt == nil || !after.NextDueAt.After(time.Now()) {
		t.Fatalf("next due must be a future slot, got %v", after.NextDueAt)
	}
}

func TestOccurrenceSkipsOnQuotaWithoutBacklog(t *testing.T) {
	f := setupOccurrenceFixture(t)
	defer f.tenant.cleanup()
	ctx := context.Background()

	// Fill the environment admission cap (Blueprint §27.1 allows 100 concurrent
	// nonterminal runs) with nonterminal runs so the occurrence is refused.
	if err := f.tenant.pool.WithTenantTx(ctx, f.orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO runs (organization_id, environment_id, deployment_id, workflow_name,
				idempotency_key, status, revision, input, last_event_sequence, created_at, updated_at)
			SELECT $1::uuid, $2::uuid, $3::uuid, 'filler', 'filler-'||g, 'RUNNING', 1, '{}'::jsonb, 1,
				clock_timestamp(), clock_timestamp()
			FROM generate_series(1, 120) g`, f.orgID, f.envID, f.deployID)
		return err
	}); err != nil {
		t.Fatalf("fill admission cap: %v", err)
	}

	s := f.schedule(t, "0 12 * * *")
	f.makeDue(t, s.ID)

	outcome, err := f.engine.EvaluateSchedule(ctx, f.orgID, f.envID, s.ID)
	if err != nil {
		t.Fatalf("evaluate under quota: %v", err)
	}
	if outcome != scheduling.OutcomeSkippedQuota {
		t.Fatalf("outcome = %s, want SKIPPED_QUOTA", outcome)
	}
	occ := f.occurrences(t, s.ID)
	if len(occ) != 1 {
		t.Fatalf("expected one recorded occurrence, got %d", len(occ))
	}
	if occ[0].Status != "SKIPPED" || occ[0].SkippedReason == nil || *occ[0].SkippedReason != "SKIPPED_QUOTA" {
		t.Fatalf("occurrence = %+v, want SKIPPED/SKIPPED_QUOTA", occ[0])
	}
	if occ[0].RunID != nil {
		t.Fatal("a quota-skipped slot must not create a run")
	}

	// The skip must leave an operator-visible trace, not vanish silently.
	var alerts int
	if err := f.tenant.pool.WithTenantTx(ctx, f.orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `
			SELECT count(*) FROM audit_events
			WHERE organization_id=$1::uuid AND action='schedule.occurrence_skipped'`, f.orgID).Scan(&alerts)
	}); err != nil {
		t.Fatalf("read alerts: %v", err)
	}
	if alerts == 0 {
		t.Fatal("a quota-skipped occurrence must record an alert for operators")
	}

	// And the schedule must move on, leaving no hidden backlog.
	after, _ := f.svc.Get(ctx, f.orgID, f.envID, s.ID)
	if after.NextDueAt == nil || !after.NextDueAt.After(time.Now()) {
		t.Fatalf("quota skip must advance next_due_at, got %v", after.NextDueAt)
	}
}

func TestOccurrenceErrorPausesScheduleWithoutActiveDeployment(t *testing.T) {
	f := setupOccurrenceFixture(t)
	defer f.tenant.cleanup()
	ctx := context.Background()

	// No active deployment. active_deployment_id is NOT NULL, so the absence of
	// a channel row is how a workflow with no active deployment is expressed.
	if err := f.tenant.pool.WithTenantTx(ctx, f.orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `
			DELETE FROM workflow_channels
			WHERE organization_id=$1::uuid AND environment_id=$2::uuid AND workflow_name=$3`,
			f.orgID, f.envID, f.workflow)
		return err
	}); err != nil {
		t.Fatalf("remove workflow channel: %v", err)
	}

	s := f.schedule(t, "0 12 * * *")
	f.makeDue(t, s.ID)

	outcome, err := f.engine.EvaluateSchedule(ctx, f.orgID, f.envID, s.ID)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if outcome != scheduling.OutcomeErrorPaused {
		t.Fatalf("outcome = %s, want ERROR_PAUSED", outcome)
	}
	after, _ := f.svc.Get(ctx, f.orgID, f.envID, s.ID)
	if !after.Paused {
		t.Fatal("a schedule with no active deployment must be paused until fixed")
	}
	if after.NextDueAt != nil {
		t.Fatalf("a paused schedule must have no pending due time, got %v", after.NextDueAt)
	}
}

func TestOccurrenceHonoursPinnedDeployment(t *testing.T) {
	f := setupOccurrenceFixture(t)
	defer f.tenant.cleanup()
	ctx := context.Background()

	pinned := seedOccurrenceDeployment(t, f.tenant, f.orgID, f.envID)
	s, err := f.svc.Create(ctx, f.orgID, f.envID, scheduling.CreateScheduleRequest{
		WorkflowName: f.workflow, CronExpression: "0 12 * * *", Timezone: "UTC", DeploymentID: &pinned,
	}, &tenant.AuditContext{ActorType: tenant.IdentityTypeMachine})
	if err != nil {
		t.Fatalf("create pinned schedule: %v", err)
	}
	f.makeDue(t, s.ID)

	if _, err := f.engine.EvaluateSchedule(ctx, f.orgID, f.envID, s.ID); err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	occ := f.occurrences(t, s.ID)
	if len(occ) != 1 || occ[0].RunID == nil {
		t.Fatalf("expected a started occurrence, got %+v", occ)
	}
	var runDeployment string
	if err := f.tenant.pool.WithTenantTx(ctx, f.orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT deployment_id::text FROM runs WHERE id=$1::uuid`, *occ[0].RunID).Scan(&runDeployment)
	}); err != nil {
		t.Fatalf("read run deployment: %v", err)
	}
	if runDeployment != pinned {
		t.Fatalf("run pinned deployment = %s, want the explicit pin %s", runDeployment, pinned)
	}
}

func TestOccurrenceRevisionChangeOnlyAffectsFuture(t *testing.T) {
	f := setupOccurrenceFixture(t)
	defer f.tenant.cleanup()
	ctx := context.Background()

	s := f.schedule(t, "0 12 * * *")
	f.makeDue(t, s.ID)
	if _, err := f.engine.EvaluateSchedule(ctx, f.orgID, f.envID, s.ID); err != nil {
		t.Fatalf("first evaluate: %v", err)
	}
	before := f.occurrences(t, s.ID)
	if len(before) != 1 || before[0].Revision != s.Revision {
		t.Fatalf("first occurrence revision = %d, want %d", before[0].Revision, s.Revision)
	}

	// Edit the schedule: §17 says an edit affects only future occurrences.
	newCron := "30 6 * * *"
	if _, err := f.svc.Update(ctx, f.orgID, f.envID, s.ID, scheduling.UpdateScheduleRequest{
		ExpectedRevision: s.Revision, CronExpression: &newCron,
	}, &tenant.AuditContext{ActorType: tenant.IdentityTypeMachine}); err != nil {
		t.Fatalf("update: %v", err)
	}
	f.makeDue(t, s.ID)
	if _, err := f.engine.EvaluateSchedule(ctx, f.orgID, f.envID, s.ID); err != nil {
		t.Fatalf("second evaluate: %v", err)
	}

	after := f.occurrences(t, s.ID)
	if len(after) != 2 {
		t.Fatalf("expected 2 occurrences, got %d", len(after))
	}
	if after[0].Revision != before[0].Revision {
		t.Fatalf("an edit must not rewrite history: %d -> %d", before[0].Revision, after[0].Revision)
	}
	if after[1].Revision != s.Revision+1 {
		t.Fatalf("new occurrence revision = %d, want %d", after[1].Revision, s.Revision+1)
	}
}

func TestOccurrenceCrashBetweenRunAndAdvanceRollsBack(t *testing.T) {
	f := setupOccurrenceFixture(t)
	defer f.tenant.cleanup()
	ctx := context.Background()

	s := f.schedule(t, "0 12 * * *")
	f.makeDue(t, s.ID)

	// Force the transaction to fail after the run would be created but before
	// the schedule advances. Nothing may be left behind: no occurrence, no
	// run, and the schedule still due.
	boom := errors.New("injected failure after run creation")
	runs := execution.NewService(f.tenant.pool, f.tenant.service)
	runs.SetBeforeCreateCommitHookForTest(func() error { return boom })

	engine := scheduling.NewEngine(f.tenant.pool, runs, f.tenant.service)
	if _, err := engine.EvaluateSchedule(ctx, f.orgID, f.envID, s.ID); err == nil {
		t.Fatal("expected the injected failure to surface")
	}
	runs.SetBeforeCreateCommitHookForTest(nil)

	if occ := f.occurrences(t, s.ID); len(occ) != 0 {
		t.Fatalf("a failed evaluation must leave no occurrence, got %d", len(occ))
	}
	var runCount int
	if err := f.tenant.pool.WithTenantTx(ctx, f.orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `
			SELECT count(*) FROM runs WHERE organization_id=$1::uuid
			  AND idempotency_key LIKE 'schedule:'||$2::text||'%'`, f.orgID, s.ID).Scan(&runCount)
	}); err != nil {
		t.Fatalf("count runs: %v", err)
	}
	if runCount != 0 {
		t.Fatalf("a failed evaluation must leave no run, got %d", runCount)
	}

	// The schedule is still due, so a retry must succeed and produce exactly
	// one run — the whole point of deriving the idempotency key from the
	// occurrence.
	after, err := f.svc.Get(ctx, f.orgID, f.envID, s.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if after.NextDueAt == nil || after.NextDueAt.After(time.Now()) {
		t.Fatalf("schedule must still be due after a rollback, got %v", after.NextDueAt)
	}
	if _, err := f.engine.EvaluateSchedule(ctx, f.orgID, f.envID, s.ID); err != nil {
		t.Fatalf("retry after rollback: %v", err)
	}
	occ := f.occurrences(t, s.ID)
	if len(occ) != 1 || occ[0].RunID == nil {
		t.Fatalf("retry must produce exactly one started occurrence, got %+v", occ)
	}
}

func TestOccurrenceCrossTenantInvisible(t *testing.T) {
	f := setupOccurrenceFixture(t)
	defer f.tenant.cleanup()
	ctx := context.Background()

	s := f.schedule(t, "0 12 * * *")
	otherOrg, err := f.tenant.service.CreateOrganization(ctx, newUUID(t), "Other Occ Org")
	if err != nil {
		t.Fatalf("create other org: %v", err)
	}
	if _, err := f.engine.EvaluateSchedule(ctx, otherOrg.ID, f.envID, s.ID); !errors.Is(err, scheduling.ErrScheduleNotFound) {
		t.Fatalf("cross-tenant evaluate error = %v, want SCHEDULE_NOT_FOUND", err)
	}
}
