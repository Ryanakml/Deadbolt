package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
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
