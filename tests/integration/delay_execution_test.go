package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/scheduling"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/jackc/pgx/v5/pgxpool"
)

func seedDelayDeployment(t *testing.T, tc *tenantTestContext, orgID, envID, digest string) string {
	t.Helper()
	manifest := map[string]any{
		"targetOS": "linux", "targetArchitecture": "amd64", "secretNames": []string{},
		"tasks": []any{},
		"workflows": []any{map[string]any{
			"manifestVersion": 1, "name": "delay-flow",
			"inputSchema":  map[string]any{"type": "object"},
			"outputSchema": map[string]any{"type": "object", "additionalProperties": false},
			"nodes": []any{
				map[string]any{"id": "wait", "type": "delay", "delayMs": 60000},
			},
			"output": map[string]any{},
		}},
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var deploymentID string
	err = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `INSERT INTO deployments
			(organization_id,environment_id,manifest_hash,bundle_digest,manifest,protocol_version,runtime_version)
			VALUES ($1,$2,$3,$4,$5::jsonb,1,'1.0') RETURNING id::text`,
			orgID, envID, "manifest-"+digest, digest, string(raw)).Scan(&deploymentID)
	})
	if err != nil {
		t.Fatal(err)
	}
	return deploymentID
}

// TestDelayNodePersistsAndFires proves the durable user-wait boundary:
// first eligibility chooses one due_at, no worker attempt is created, a fresh
// engine can fire it after restart, and duplicate firing is a no-op.
func TestDelayNodePersistsAndFires(t *testing.T) {
	tc, server, orgID, envID, _ := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	deploymentID := seedDelayDeployment(t, tc, orgID, envID, "delay-issue-33")
	svc := execution.NewService(tc.pool, tc.service)
	run, _, err := svc.CreateRun(context.Background(), orgID, envID, "delay-flow", "delay-issue-33-1", &deploymentID, map[string]any{}, &tenant.AuditContext{ActorType: tenant.IdentityTypeMachine})
	if err != nil {
		t.Fatal(err)
	}

	var stepID string
	var stepState, waitReason, runStatus, runReason, timerState string
	var dueAt time.Time
	var attempts int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT id::text,state,COALESCE(wait_reason,'') FROM run_steps WHERE run_id=$1::uuid`, run.ID).Scan(&stepID, &stepState, &waitReason); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT status,COALESCE(reason_code,'') FROM runs WHERE id=$1::uuid`, run.ID).Scan(&runStatus, &runReason); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT state,due_at FROM timers WHERE run_id=$1::uuid AND kind='DELAY'`, run.ID).Scan(&timerState, &dueAt); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT count(*) FROM task_attempts WHERE step_id=$1::uuid`, stepID).Scan(&attempts)
	}); err != nil {
		t.Fatal(err)
	}
	if stepState != "WAITING" || waitReason != "DELAY" {
		t.Fatalf("expected WAITING/DELAY, got %s/%s", stepState, waitReason)
	}
	if runStatus != "WAITING" || runReason != "DELAY" {
		t.Fatalf("expected run WAITING/DELAY, got %s/%s", runStatus, runReason)
	}
	if timerState != "PENDING" || !dueAt.After(time.Now()) {
		t.Fatalf("expected future PENDING timer, got %s %s", timerState, dueAt)
	}
	if attempts != 0 {
		t.Fatalf("delay must not create attempts, got %d", attempts)
	}
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE timers SET due_at=clock_timestamp()-INTERVAL '1 second' WHERE run_id=$1::uuid AND kind='DELAY' AND state='PENDING'`, run.ID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	engine := execution.NewWorkerEngine(tc.pool)
	if fired, err := engine.FireDueDelayTimers(context.Background(), orgID); err != nil || fired != 1 {
		t.Fatalf("expected one delay fire, got %d/%v", fired, err)
	}
	if fired, err := execution.NewWorkerEngine(tc.pool).FireDueDelayTimers(context.Background(), orgID); err != nil || fired != 0 {
		t.Fatalf("duplicate delay fire must be a no-op, got %d/%v", fired, err)
	}

	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT state FROM run_steps WHERE id=$1::uuid`, stepID).Scan(&stepState); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid`, run.ID).Scan(&runStatus); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT state FROM timers WHERE run_id=$1::uuid AND kind='DELAY' ORDER BY id DESC LIMIT 1`, run.ID).Scan(&timerState); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT due_at FROM timers WHERE run_id=$1::uuid AND kind='DELAY' ORDER BY id DESC LIMIT 1`, run.ID).Scan(&dueAt)
	}); err != nil {
		t.Fatal(err)
	}
	if stepState != "SUCCEEDED" || runStatus != "SUCCEEDED" || timerState != "FIRED" {
		t.Fatalf("delay settlement incomplete: step=%s run=%s timer=%s", stepState, runStatus, timerState)
	}
}

// TestDelayDeadlineCannotReopen verifies that an overdue run wins over a due
// delay and that the pending timer is cancelled by the existing deadline path.
func TestDelayDeadlineCannotReopen(t *testing.T) {
	tc, server, orgID, envID, _ := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()
	deploymentID := seedDelayDeployment(t, tc, orgID, envID, "delay-issue-33-deadline")
	run, _, err := execution.NewService(tc.pool, tc.service).CreateRun(context.Background(), orgID, envID, "delay-flow", "delay-issue-33-2", &deploymentID, map[string]any{}, &tenant.AuditContext{ActorType: tenant.IdentityTypeMachine})
	if err != nil {
		t.Fatal(err)
	}
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE runs SET deadline_at=clock_timestamp()-INTERVAL '1 minute' WHERE id=$1::uuid`, run.ID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	engine := execution.NewWorkerEngine(tc.pool)
	if _, err := engine.ReconcileExpiredLeases(context.Background(), orgID); err != nil {
		t.Fatal(err)
	}
	if fired, err := engine.FireDueDelayTimers(context.Background(), orgID); err != nil || fired != 0 {
		t.Fatalf("expired run delay must not fire, got %d/%v", fired, err)
	}
	var status, reason string
	var pending int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		var reasonPtr *string
		if err := tx.QueryRow(ctx, `SELECT status,reason_code FROM runs WHERE id=$1::uuid`, run.ID).Scan(&status, &reasonPtr); err != nil {
			return err
		}
		if reasonPtr != nil {
			reason = *reasonPtr
		}
		return tx.QueryRow(ctx, `SELECT count(*) FROM timers WHERE run_id=$1::uuid AND state='PENDING'`, run.ID).Scan(&pending)
	}); err != nil {
		t.Fatal(err)
	}
	if status != "FAILED" || reason != "RUN_DEADLINE_EXCEEDED" || pending != 0 {
		t.Fatalf("deadline settlement invalid: status=%s reason=%s pending=%d", status, reason, pending)
	}
}

// TestDelayDueWhilePausedPreservesDeadline proves the paused-due race:
// a due delay remains pending while PAUSED, keeps its original due_at, and
// fires exactly once only after resume.
func TestDelayDueWhilePausedPreservesDeadline(t *testing.T) {
	tc, server, orgID, envID, _ := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	deploymentID := seedDelayDeployment(t, tc, orgID, envID, "delay-issue-33-paused")
	engine := execution.NewWorkerEngine(tc.pool)
	run, _, err := execution.NewService(tc.pool, tc.service).CreateRun(
		context.Background(), orgID, envID, "delay-flow", "delay-issue-33-3", &deploymentID,
		map[string]any{}, &tenant.AuditContext{ActorType: tenant.IdentityTypeMachine},
	)
	if err != nil {
		t.Fatal(err)
	}

	var stepID string
	var dueAt time.Time
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT id::text FROM run_steps WHERE run_id=$1::uuid`, run.ID).Scan(&stepID); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT due_at FROM timers WHERE run_id=$1::uuid AND kind='DELAY' AND state='PENDING'`, run.ID).Scan(&dueAt)
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := engine.PauseRun(context.Background(), orgID, run.ID, execution.PauseRunRequest{ExpectedRevision: run.Revision}, &tenant.AuditContext{ActorType: tenant.IdentityTypeHuman}); err != nil {
		t.Fatalf("pause delay run: %v", err)
	}
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE timers SET due_at=clock_timestamp()-INTERVAL '1 second' WHERE run_id=$1::uuid AND kind='DELAY' AND state='PENDING'`, run.ID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if fired, err := engine.FireDueDelayTimers(context.Background(), orgID); err != nil || fired != 0 {
		t.Fatalf("paused delay must not fire, got %d/%v", fired, err)
	}

	var pausedStatus, timerState string
	var pausedDueAt time.Time
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid`, run.ID).Scan(&pausedStatus); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT state,due_at FROM timers WHERE run_id=$1::uuid AND kind='DELAY'`, run.ID).Scan(&timerState, &pausedDueAt)
	}); err != nil {
		t.Fatal(err)
	}
	if pausedStatus != "PAUSED" || timerState != "PENDING" {
		t.Fatalf("paused delay state invalid: run=%s timer=%s", pausedStatus, timerState)
	}
	if !pausedDueAt.Before(dueAt) {
		t.Fatalf("test did not move timer due: original=%s paused=%s", dueAt, pausedDueAt)
	}

	if _, err := engine.ResumeRun(context.Background(), orgID, run.ID, execution.ResumeRunRequest{ExpectedRevision: run.Revision + 1}, &tenant.AuditContext{ActorType: tenant.IdentityTypeHuman}); err != nil {
		t.Fatalf("resume delay run: %v", err)
	}
	if fired, err := engine.FireDueDelayTimers(context.Background(), orgID); err != nil || fired != 1 {
		t.Fatalf("resumed due delay must fire once, got %d/%v", fired, err)
	}
	if fired, err := engine.FireDueDelayTimers(context.Background(), orgID); err != nil || fired != 0 {
		t.Fatalf("resumed delay duplicate fire must be a no-op, got %d/%v", fired, err)
	}

	var stepState, finalStatus string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT state FROM run_steps WHERE id=$1::uuid`, stepID).Scan(&stepState); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid`, run.ID).Scan(&finalStatus)
	}); err != nil {
		t.Fatal(err)
	}
	if stepState != "SUCCEEDED" || finalStatus != "SUCCEEDED" {
		t.Fatalf("resumed delay did not complete: step=%s run=%s", stepState, finalStatus)
	}
}

// TestDelaySchedulerRestartUsesPersistedTimer exercises the scheduler
// lifecycle boundary: one scheduler instance is stopped, a due timestamp is
// left in PostgreSQL, and a newly created scheduler instance settles it once.
func TestDelaySchedulerRestartUsesPersistedTimer(t *testing.T) {
	tc, server, orgID, envID, _ := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	deploymentID := seedDelayDeployment(t, tc, orgID, envID, "delay-issue-33-scheduler-restart")
	systemPool, err := pgxpool.New(context.Background(), tc.systemURL)
	if err != nil {
		t.Fatal(err)
	}
	defer systemPool.Close()
	run, _, err := execution.NewService(tc.pool, tc.service).CreateRun(
		context.Background(), orgID, envID, "delay-flow", "delay-issue-33-4", &deploymentID,
		map[string]any{}, &tenant.AuditContext{ActorType: tenant.IdentityTypeMachine},
	)
	if err != nil {
		t.Fatal(err)
	}

	first := scheduling.NewReconciler(systemPool, time.Hour, nil)
	first.SetTenantSweep(func(ctx context.Context, tenantID string) error {
		_, err := execution.NewWorkerEngine(tc.pool).FireDueDelayTimers(ctx, tenantID)
		return err
	})
	ctx, cancel := context.WithCancel(context.Background())
	if err := first.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	cancel() // Simulate the scheduler/control-plane process stopping.

	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE timers SET due_at=clock_timestamp()-INTERVAL '1 second' WHERE run_id=$1::uuid AND kind='DELAY' AND state='PENDING'`, run.ID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	second := scheduling.NewReconciler(systemPool, time.Hour, nil)
	second.SetTenantSweep(func(ctx context.Context, tenantID string) error {
		_, err := execution.NewWorkerEngine(tc.pool).FireDueDelayTimers(ctx, tenantID)
		return err
	})
	if err := second.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fired, err := execution.NewWorkerEngine(tc.pool).FireDueDelayTimers(context.Background(), orgID); err != nil || fired != 0 {
		t.Fatalf("recreated scheduler must settle delay exactly once, got duplicate=%d/%v", fired, err)
	}

	var status string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid`, run.ID).Scan(&status)
	}); err != nil {
		t.Fatal(err)
	}
	if status != "SUCCEEDED" {
		t.Fatalf("recreated scheduler did not settle delay run: %s", status)
	}
}
