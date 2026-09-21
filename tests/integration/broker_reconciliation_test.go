package integration_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/outbox"
	"github.com/Ryanakml/Deadbolt/internal/scheduling"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
)

func reconciliationTwoNodeManifest() string {
	return `{"tasks":[{"name":"task-a","entrypoint":"tasks/a.js","recovery":"safe","timeoutMs":60000,"retry":{"maxAttempts":3}}],"workflows":[{"name":"workflow-a","nodes":[{"id":"upstream","type":"task","task":"task-a"},{"id":"downstream","type":"task","task":"task-a","after":["upstream"]}]}]}`
}

// seedReconciliationRun deliberately represents the durable state left by a
// completion-to-downstream scheduling gap: upstream is committed, downstream
// remains BLOCKED, and no broker message is required to repair it.
func seedReconciliationRun(t *testing.T, tc *tenantTestContext, orgID, envID, digest, upstreamState string) (runID, downstreamID string) {
	t.Helper()
	err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		var deploymentID, upstreamID string
		if err := tx.QueryRow(ctx, `INSERT INTO deployments
			(organization_id,environment_id,manifest_hash,bundle_digest,manifest,protocol_version,runtime_version)
			VALUES ($1,$2,$3,$4,$5::jsonb,1,'1.0') RETURNING id::text`, orgID, envID, "manifest-"+digest, digest, reconciliationTwoNodeManifest()).Scan(&deploymentID); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `INSERT INTO runs
			(organization_id,environment_id,deployment_id,workflow_name,status,input)
			VALUES ($1,$2,$3,'workflow-a','RUNNING','{}') RETURNING id::text`, orgID, envID, deploymentID).Scan(&runID); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `INSERT INTO run_steps
			(organization_id,environment_id,run_id,node_id,state,output)
			VALUES ($1,$2,$3,'upstream',$4,'{}') RETURNING id::text`, orgID, envID, runID, upstreamState).Scan(&upstreamID); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `INSERT INTO run_steps
			(organization_id,environment_id,run_id,node_id,state)
			VALUES ($1,$2,$3,'downstream','BLOCKED') RETURNING id::text`, orgID, envID, runID).Scan(&downstreamID)
	})
	if err != nil {
		t.Fatal(err)
	}
	return runID, downstreamID
}

func reconciliationStepState(t *testing.T, tc *tenantTestContext, orgID, stepID string) string {
	t.Helper()
	var state string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT state FROM run_steps WHERE id=$1::uuid`, stepID).Scan(&state)
	}); err != nil {
		t.Fatal(err)
	}
	return state
}

func TestReconcileReadyWorkSurvivesBrokerDataLossAndIsIdempotent(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	// Use a real JetStream boundary, create its stream, then erase it. The
	// repair below must use only committed PostgreSQL state.
	_, _, js := startRealNATSServer(t)
	if _, err := outbox.EnsureStream(js, outbox.StreamName, []string{outbox.SubjectPrefix + ">"}, time.Minute); err != nil {
		t.Fatalf("create JetStream stream: %v", err)
	}
	if err := js.DeleteStream(outbox.StreamName); err != nil {
		t.Fatalf("delete JetStream stream: %v", err)
	}

	runID, downstreamID := seedReconciliationRun(t, tc, orgID, envID, "broker-data-loss", "SUCCEEDED")
	engine := execution.NewWorkerEngine(tc.pool)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := engine.ReconcileReadyWork(context.Background(), orgID)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent reconciliation failed: %v", err)
		}
	}
	if state := reconciliationStepState(t, tc, orgID, downstreamID); state != "READY" {
		t.Fatalf("expected downstream READY after broker data loss, got %s", state)
	}

	var readyEvents, outboxIntents int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM run_events WHERE run_id=$1::uuid AND event_type='STEP_READY'`, runID).Scan(&readyEvents); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE payload->>'runId'=$1::text AND payload->>'eventType'='STEP_READY'`, runID).Scan(&outboxIntents)
	}); err != nil {
		t.Fatal(err)
	}
	if readyEvents != 1 || outboxIntents != 1 {
		t.Fatalf("reconciliation duplicated durable effects: events=%d outbox=%d", readyEvents, outboxIntents)
	}
	if repaired, err := engine.ReconcileReadyWork(context.Background(), orgID); err != nil || repaired != 0 {
		t.Fatalf("repeat reconciliation must be a no-op, repaired=%d err=%v", repaired, err)
	}
}

func TestReconcileReadyWorkRotatesPastUnrepairableBatch(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	// Fifty active runs cannot advance because their upstream work is still
	// running. The 51st can advance. It is marked as already observed so the
	// first bounded scan must process the blocked batch first.
	for i := 0; i < 50; i++ {
		seedReconciliationRun(t, tc, orgID, envID, fmt.Sprintf("stalled-%d", i), "RUNNING")
	}
	_, repairableStep := seedReconciliationRun(t, tc, orgID, envID, "repairable-after-batch", "SUCCEEDED")
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE runs SET reconciliation_checked_at=clock_timestamp()
			WHERE id=(SELECT run_id FROM run_steps WHERE id=$1::uuid)`, repairableStep)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	engine := execution.NewWorkerEngine(tc.pool)
	if repaired, err := engine.ReconcileReadyWork(context.Background(), orgID); err != nil || repaired != 0 {
		t.Fatalf("first batch should only observe stalled runs, repaired=%d err=%v", repaired, err)
	}
	if repaired, err := engine.ReconcileReadyWork(context.Background(), orgID); err != nil || repaired != 1 {
		t.Fatalf("second batch did not reach repairable run, repaired=%d err=%v", repaired, err)
	}
	if state := reconciliationStepState(t, tc, orgID, repairableStep); state != "READY" {
		t.Fatalf("repairable run starved behind first batch: state=%s", state)
	}
}

func TestSchedulerRestartRepairsCommittedCompletionWithoutBrokerHistory(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	// The upstream completion is already durable while the previous scheduler is
	// gone. A fresh scheduler instance must discover the tenant and repair the
	// dependent work without replaying a broker history.
	_, downstreamID := seedReconciliationRun(t, tc, orgID, envID, "scheduler-restart", "SUCCEEDED")
	ctx := context.Background()
	systemPool, err := pgxpool.New(ctx, tc.systemURL)
	if err != nil {
		t.Fatalf("connect scheduler pool: %v", err)
	}
	defer systemPool.Close()

	restarted := scheduling.NewReconciler(systemPool, time.Second, nil)
	engine := execution.NewWorkerEngine(tc.pool)
	restarted.SetSlowTenantSweep(func(ctx context.Context, tenantID string) error {
		_, err := engine.ReconcileReadyWork(ctx, tenantID)
		return err
	})
	if err := restarted.SlowSweep(ctx); err != nil {
		t.Fatalf("restarted scheduler slow sweep: %v", err)
	}
	if state := reconciliationStepState(t, tc, orgID, downstreamID); state != "READY" {
		t.Fatalf("scheduler restart stranded downstream work in %s", state)
	}
}

func TestReconcileReadyWorkFailsClosedWhenDatabaseUnavailable(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	_, downstreamID := seedReconciliationRun(t, tc, orgID, envID, "database-unavailable", "SUCCEEDED")
	engine := execution.NewWorkerEngine(tc.pool)
	// Closing the only runtime pool simulates an unavailable application
	// database boundary. Reconciliation must return an error before it can grant
	// execution, and the independent migrator connection verifies no mutation.
	tc.runtimePool.Close()
	if _, err := engine.ReconcileReadyWork(context.Background(), orgID); err == nil {
		t.Fatal("database-unavailable reconciliation unexpectedly succeeded")
	}
	var state string
	if err := tc.database.QueryRow(`SELECT state FROM run_steps WHERE id=$1::uuid`, downstreamID).Scan(&state); err != nil {
		t.Fatalf("read step after database outage: %v", err)
	}
	if state != "BLOCKED" {
		t.Fatalf("database outage mutated execution state to %s", state)
	}
}
