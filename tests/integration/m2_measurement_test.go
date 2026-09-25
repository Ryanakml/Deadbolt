package integration_test

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

// TestM2ClaimContentionMeasurement is the Issue #23 SP-02 M2 remeasurement:
// it drives the current admission/claim implementation (env admission lock,
// indexed live-lease counting, per-env FIFO, scheduler round-robin,
// QUOTA_WAIT, 10-lease cap, DefaultSlots clamp) with concurrent workers in
// two environments plus a concurrent reconciler sweep, and records claim
// throughput, latency distribution, lock contention signals and ownership
// integrity. Timing is reported, never asserted strictly; integrity
// (all work claimed exactly once, no errors) is asserted.
func TestM2ClaimContentionMeasurement(t *testing.T) {
	tc, server, orgID, envID1, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()
	ctx := context.Background()

	var projID string
	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT project_id FROM environments WHERE id=$1::uuid`, envID1).Scan(&projID)
	})
	env2, err := tc.service.CreateEnvironment(ctx, orgID, projID, tenant.EnvDevelopment, 10)
	if err != nil {
		t.Fatalf("create second env: %v", err)
	}
	envID2 := env2.ID
	adminKey2 := bootstrapTestKey(t, tc.service, orgID, envID2, []string{
		tenant.CapDeploymentsRegister,
		tenant.CapDeploymentsActivateStaging,
		tenant.CapDeploymentsWrite,
		tenant.CapRunsCreate,
		tenant.CapRunsRead,
		tenant.CapAdminKey,
	})
	_ = adminKey2

	bundle := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	schema := map[string]any{"type": "object"}
	manifest := createLifecycleManifest(bundle,
		[]map[string]any{{"name": "m2-task", "entrypoint": "tasks/m2.js", "timeoutMs": 30000, "recovery": "idempotent", "idempotencyWindowMs": 305000, "inputSchema": schema, "outputSchema": schema}},
		[]map[string]any{{"manifestVersion": 1, "name": "m2-flow", "inputSchema": schema, "outputSchema": schema, "nodes": []map[string]any{{"id": "node-1", "type": "task", "task": "m2-task", "after": []any{}, "input": map[string]any{}}}, "output": map[string]any{"$ref": "step.output", "stepId": "node-1", "pointer": ""}}},
	)
	// The lifecycle fixture provisions staging with max_concurrency=5; the M2
	// profile measures both environments at the MVP cap 10.
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE environment_admissions SET max_concurrency=10
			WHERE environment_id=$1::uuid AND organization_id=$2::uuid`, envID1, orgID)
		return err
	}); err != nil {
		t.Fatalf("raise staging cap: %v", err)
	}
	depA := registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID1, "m2-flow", manifest)
	depB := registerAndActivateTestWorkflow(t, tc, server, adminKey2, orgID, envID2, "m2-flow", manifest)

	const runsPerEnv = 25
	seed := func(envID, depID string) {
		if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
			for i := 0; i < runsPerEnv; i++ {
				var runID string
				if err := tx.QueryRow(ctx, `INSERT INTO runs
					(organization_id,environment_id,deployment_id,workflow_name,status)
					VALUES ($1::uuid,$2::uuid,$3::uuid,'m2-flow','QUEUED') RETURNING id::text`,
					orgID, envID, depID).Scan(&runID); err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `INSERT INTO run_steps
					(organization_id,environment_id,run_id,node_id,state,eligible_at)
					VALUES ($1::uuid,$2::uuid,$3::uuid,'node-1','READY',clock_timestamp())`,
					orgID, envID, runID); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	seed(envID1, depA)
	seed(envID2, depB)

	w1, _ := enrollExecutionWorker(t, tc, server, orgID, envID1, "m2-w1")
	w2, _ := enrollExecutionWorker(t, tc, server, orgID, envID2, "m2-w2")
	advertiseDigest(t, tc, orgID, w1.SessionID, bundle)
	advertiseDigest(t, tc, orgID, w2.SessionID, bundle)

	// Query plans for the report: scheduler candidate selection and the
	// env-scoped claim snapshot, plus the covering indexes they rely on.
	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		rows, err := tx.Query(ctx, `EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)
			WITH candidates AS (
				SELECT r.id AS run_id,
					ROW_NUMBER() OVER (PARTITION BY r.environment_id ORDER BY r.reconciliation_checked_at NULLS FIRST, r.id) AS env_posn,
					r.reconciliation_checked_at AS checked_at
				FROM runs r
				JOIN deployments d ON d.id=r.deployment_id AND d.organization_id=r.organization_id
				WHERE r.organization_id=$1::uuid AND r.status IN ('QUEUED','RUNNING')
					AND EXISTS (
						SELECT 1 FROM run_steps blocked
						WHERE blocked.run_id=r.id AND blocked.organization_id=r.organization_id
							AND blocked.state='BLOCKED'
					)
			)
			SELECT r.id::text, r.workflow_name, d.manifest
			FROM candidates c
			JOIN runs r ON r.id=c.run_id AND r.organization_id=$1::uuid
			JOIN deployments d ON d.id=r.deployment_id AND d.organization_id=r.organization_id
			ORDER BY c.env_posn, c.checked_at NULLS FIRST, r.id
			LIMIT 50`, orgID)
		if err != nil {
			t.Logf("scheduler EXPLAIN failed: %v", err)
			return nil
		}
		defer rows.Close()
		t.Logf("=== M2 scheduler candidate plan ===")
		for rows.Next() {
			var line string
			_ = rows.Scan(&line)
			t.Logf("plan: %s", line)
		}
		return rows.Err()
	})
	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		rows, err := tx.Query(ctx, `EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)
			SELECT rs.id::text FROM run_steps rs
			JOIN runs r ON r.id=rs.run_id AND r.organization_id=rs.organization_id
			WHERE rs.organization_id=$1::uuid AND rs.environment_id=$2::uuid
				AND rs.state='READY' AND rs.eligible_at <= clock_timestamp()
			ORDER BY rs.eligible_at, rs.id LIMIT 2`, orgID, envID1)
		if err != nil {
			t.Logf("claim EXPLAIN failed: %v", err)
			return nil
		}
		defer rows.Close()
		t.Logf("=== M2 claim snapshot plan ===")
		for rows.Next() {
			var line string
			_ = rows.Scan(&line)
			t.Logf("plan: %s", line)
		}
		return rows.Err()
	})

	var pgVersion string
	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT version()`).Scan(&pgVersion)
	})
	t.Logf("M2 measurement env: go=%s os=%s arch=%s pg=%s envs=2 runsPerEnv=%d workers=2 slots=2 envCap=10",
		runtime.Version(), runtime.GOOS, runtime.GOARCH, pgVersion, runsPerEnv)

	engine := execution.NewWorkerEngine(tc.pool)
	var mu sync.Mutex
	var latencies []time.Duration
	var claimErrs atomic.Int32
	var reqSeq atomic.Int64
	// Full lifecycle per iteration (claim/start/complete) so live leases
	// free up and all seeded work drains despite the 10-lease env cap,
	// exactly like production workers under contention.
	claimAll := func(sess *testWorkerSession, envID string) []string {
		sctx := &worker.WorkerSessionContext{
			SessionID: sess.SessionID, WorkerID: sess.WorkerID,
			OrganizationID: orgID, EnvironmentID: envID, PoolName: "default",
			ExpiresAt: time.Now().Add(time.Hour),
		}
		var got []string
		for rounds := 0; rounds < 200; rounds++ {
			n := reqSeq.Add(1)
			start := time.Now()
			resp, err := engine.Claim(ctx, sctx, &worker.PollRequestDTO{
				ProtocolVersion: worker.ProtocolVersion, RequestID: fmt.Sprintf("m2-poll-%d", n),
				WorkerID: sess.WorkerID, SessionID: sess.SessionID,
				AvailableSlots: 2, DeploymentDigests: []string{bundle}, Pool: "default",
			})
			el := time.Since(start)
			mu.Lock()
			latencies = append(latencies, el)
			mu.Unlock()
			if err != nil {
				claimErrs.Add(1)
				t.Errorf("claim failed: %v", err)
				return got
			}
			if len(resp.Assignments) == 0 {
				return got
			}
			for _, a := range resp.Assignments {
				if _, err := engine.Start(ctx, sctx, &worker.StartRequestDTO{
					ProtocolVersion: worker.ProtocolVersion, RequestID: fmt.Sprintf("m2-start-%d", reqSeq.Add(1)),
					WorkerID: sess.WorkerID, SessionID: sess.SessionID,
					AttemptID: a.AttemptID, OwnershipEpoch: a.OwnershipEpoch,
				}); err != nil {
					claimErrs.Add(1)
					t.Errorf("start failed: %v", err)
					return got
				}
				comp := worker.CompleteRequestDTO{
					ProtocolVersion: worker.ProtocolVersion, RequestID: fmt.Sprintf("m2-comp-%d", reqSeq.Add(1)),
					WorkerID: sess.WorkerID, SessionID: sess.SessionID,
					AttemptID: a.AttemptID, OwnershipEpoch: a.OwnershipEpoch,
					Outcome: "SUCCEEDED", Output: map[string]any{},
				}
				comp.ResultDigest, _ = worker.CanonicalCompletionDigest(&comp)
				if _, err := engine.Complete(ctx, sctx, &comp); err != nil {
					claimErrs.Add(1)
					t.Errorf("complete failed: %v", err)
					return got
				}
				got = append(got, a.StepID)
			}
		}
		t.Errorf("worker %s did not drain after 200 rounds", sess.WorkerID)
		return got
	}

	wallStart := time.Now()
	var wg sync.WaitGroup
	var idsA, idsB []string
	wg.Add(3)
	go func() { defer wg.Done(); idsA = claimAll(w1, envID1) }()
	go func() { defer wg.Done(); idsB = claimAll(w2, envID2) }()
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			_, _ = engine.ReconcileExpiredLeases(ctx, orgID)
			_, _ = engine.ReconcileReadyWork(ctx, orgID)
			time.Sleep(50 * time.Millisecond)
		}
	}()
	wg.Wait()
	wall := time.Since(wallStart)

	total := len(idsA) + len(idsB)
	t.Logf("M2 result: claimed=%d/%d wall=%v throughput=%.1f claims/s claimRPCs=%d errors=%d",
		total, 2*runsPerEnv, wall, float64(total)/wall.Seconds(), len(latencies), claimErrs.Load())
	if total != 2*runsPerEnv {
		t.Fatalf("claimed=%d, want %d (envA=%d envB=%d)", total, 2*runsPerEnv, len(idsA), len(idsB))
	}
	if len(idsA) != runsPerEnv || len(idsB) != runsPerEnv {
		t.Fatalf("per-env claimed A=%d B=%d, want %d each", len(idsA), len(idsB), runsPerEnv)
	}
	if claimErrs.Load() != 0 {
		t.Fatalf("claim errors=%d, want 0 (includes deadlocks/serialization failures)", claimErrs.Load())
	}
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	pct := func(p float64) time.Duration {
		if len(sorted) == 0 {
			return 0
		}
		i := int(p * float64(len(sorted)-1))
		return sorted[i]
	}
	t.Logf("M2 claim latency: p50=%v p95=%v max=%v rpcs=%d", pct(0.5), pct(0.95), sorted[len(sorted)-1], len(sorted))

	var succeededRuns int
	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM runs WHERE organization_id=$1::uuid AND status='SUCCEEDED'`, orgID).Scan(&succeededRuns)
	})
	t.Logf("M2 terminal: succeededRuns=%d", succeededRuns)
	if succeededRuns != 2*runsPerEnv {
		t.Fatalf("succeeded=%d, want %d", succeededRuns, 2*runsPerEnv)
	}
	var dupSteps int
	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM (
			SELECT step_id FROM task_attempts WHERE organization_id=$1::uuid GROUP BY step_id HAVING count(*) > 1
		) d`, orgID).Scan(&dupSteps)
	})
	t.Logf("M2 ownership: stepsWithDuplicateAttempts=%d", dupSteps)
	if dupSteps != 0 {
		t.Fatalf("duplicate ownership on %d steps", dupSteps)
	}
	var activeLeases int
	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM task_leases l
			JOIN run_steps rs ON rs.id=l.step_id AND rs.organization_id=l.organization_id
			WHERE rs.organization_id=$1::uuid AND clock_timestamp() < l.expires_at`, orgID).Scan(&activeLeases)
	})
	t.Logf("M2 live leases after drain: %d (env cap 10 each)", activeLeases)
}
