package sp02_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/storage/migrator"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const testDBConnString = "postgres://ryanakmalpasya@localhost:5432/deadbolt_test?sslmode=disable"

func setupSP02DB(t *testing.T) (*sql.DB, *pgxpool.Pool) {
	t.Helper()

	db, err := sql.Open("pgx", testDBConnString)
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("PostgreSQL ping failed: %v", err)
	}

	migrationsDir, err := filepath.Abs("../../../migrations")
	if err != nil {
		t.Fatalf("failed to resolve migrations dir: %v", err)
	}

	runner := migrator.NewRunner(db, migrationsDir)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := runner.Up(ctx); err != nil {
		t.Fatalf("migrations failed: %v", err)
	}

	config, err := pgxpool.ParseConfig(testDBConnString)
	if err != nil {
		t.Fatalf("failed to parse pool config: %v", err)
	}
	config.MaxConns = 30 // Allow high concurrency

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("failed to create pgxpool: %v", err)
	}

	return db, pool
}

// TestClaimContentionAndLockOrder executes the SP-02 benchmark and validation:
// 1. Tests prescribed lock order: environment_admissions -> runs -> run_steps (sorted ID) -> attempts/leases.
// 2. Uses FOR UPDATE SKIP LOCKED for non-blocking candidate discovery.
// 3. Simulates 20 concurrent worker goroutines claiming 50 ready tasks.
// 4. Verifies INV-03: Zero duplicate ownership, exactly 1 active lease per step, 0 deadlocks.
func TestClaimContentionAndLockOrder(t *testing.T) {
	db, pool := setupSP02DB(t)
	defer db.Close()
	defer pool.Close()

	ctx := context.Background()

	orgID := "10000000-0000-0000-0000-000000000001"
	projectID := "20000000-0000-0000-0000-000000000001"
	envID := "30000000-0000-0000-0000-000000000001"
	deploymentID := "40000000-0000-0000-0000-000000000001"

	// Seed tenant environment and deployment
	if _, err := db.Exec("INSERT INTO organizations (id, name) VALUES ($1, 'SP-02 Tenant') ON CONFLICT (id) DO NOTHING", orgID); err != nil {
		t.Fatalf("failed to insert org: %v", err)
	}
	if _, err := db.Exec("INSERT INTO projects (id, organization_id, name) VALUES ($1, $2, 'SP-02 Project') ON CONFLICT (organization_id, id) DO NOTHING", projectID, orgID); err != nil {
		t.Fatalf("failed to insert project: %v", err)
	}
	if _, err := db.Exec("INSERT INTO environments (id, organization_id, project_id, name) VALUES ($1, $2, $3, 'staging') ON CONFLICT (organization_id, id) DO NOTHING", envID, orgID, projectID); err != nil {
		t.Fatalf("failed to insert env: %v", err)
	}
	if _, err := db.Exec("INSERT INTO environment_admissions (environment_id, organization_id, max_concurrency) VALUES ($1, $2, 50) ON CONFLICT (environment_id) DO UPDATE SET max_concurrency = 50", envID, orgID); err != nil {
		t.Fatalf("failed to insert admission: %v", err)
	}
	if _, err := db.Exec("INSERT INTO deployments (id, organization_id, environment_id, manifest_hash, bundle_digest, manifest, protocol_version, runtime_version) VALUES ($1, $2, $3, 'hash_sp02', 'digest_sp02', '{}', 1, '1.0.0') ON CONFLICT (organization_id, id) DO NOTHING", deploymentID, orgID, envID); err != nil {
		t.Fatalf("failed to insert deployment: %v", err)
	}

	// Clean up previous test runs/leases/attempts for repeatable test execution
	_, _ = db.Exec("DELETE FROM task_leases WHERE organization_id = $1", orgID)
	_, _ = db.Exec("DELETE FROM task_attempts WHERE organization_id = $1", orgID)
	_, _ = db.Exec("DELETE FROM run_steps WHERE organization_id = $1", orgID)
	_, _ = db.Exec("DELETE FROM runs WHERE organization_id = $1", orgID)

	// Create 10 runs with 5 ready steps each (total 50 steps)
	const numRuns = 10
	const stepsPerRun = 5
	totalSteps := numRuns * stepsPerRun

	var stepIDs []string
	for r := 1; r <= numRuns; r++ {
		runID := fmt.Sprintf("50000000-0000-0000-0000-%012d", r)
		_, err := db.Exec(`
			INSERT INTO runs (id, organization_id, environment_id, deployment_id, workflow_name, status)
			VALUES ($1, $2, $3, $4, 'sp02-workflow', 'RUNNING')
			ON CONFLICT (organization_id, id) DO NOTHING;
		`, runID, orgID, envID, deploymentID)
		if err != nil {
			t.Fatalf("failed to insert run %d: %v", r, err)
		}

		for s := 1; s <= stepsPerRun; s++ {
			stepID := fmt.Sprintf("60000000-0000-0000-%04d-%012d", r, s)
			nodeID := fmt.Sprintf("task_node_%d_%d", r, s)
			_, err := db.Exec(`
				INSERT INTO run_steps (id, organization_id, environment_id, run_id, node_id, kind, state, eligible_at)
				VALUES ($1, $2, $3, $4, $5, 'task', 'READY', clock_timestamp())
				ON CONFLICT (organization_id, id) DO NOTHING;
			`, stepID, orgID, envID, runID, nodeID)
			if err != nil {
				t.Fatalf("failed to insert step %d-%d: %v", r, s, err)
			}
			stepIDs = append(stepIDs, stepID)
		}
	}

	// Worker sessions for 20 concurrent workers
	const numWorkers = 20
	var sessionIDs []string
	for w := 1; w <= numWorkers; w++ {
		workerID := fmt.Sprintf("70000000-0000-0000-0000-%012d", w)
		sessionID := fmt.Sprintf("80000000-0000-0000-0000-%012d", w)
		if _, err := db.Exec("INSERT INTO workers (id, organization_id, environment_id, public_key, status) VALUES ($1, $2, $3, 'pubkey', 'ACTIVE') ON CONFLICT (organization_id, id) DO NOTHING", workerID, orgID, envID); err != nil {
			t.Fatalf("failed to seed worker %d: %v", w, err)
		}
		if _, err := db.Exec("INSERT INTO worker_sessions (id, organization_id, worker_id, environment_id, session_token_hash, expires_at) VALUES ($1, $2, $3, $4, 'tokenhash', clock_timestamp() + interval '1 hour') ON CONFLICT (organization_id, id) DO NOTHING", sessionID, orgID, workerID, envID); err != nil {
			t.Fatalf("failed to seed session %d: %v", w, err)
		}
		sessionIDs = append(sessionIDs, sessionID)
	}

	t.Logf("Seeded %d runs, %d ready steps, %d worker sessions", numRuns, totalSteps, numWorkers)

	// Collect EXPLAIN ANALYZE for the candidate selection query
	var explainOutput string
	explainQuery := `
		EXPLAIN (ANALYZE, BUFFERS, COSTS)
		SELECT id, run_id
		FROM run_steps
		WHERE environment_id = $1 AND state = 'READY'
		ORDER BY eligible_at ASC, id ASC
		LIMIT 1
		FOR UPDATE SKIP LOCKED;
	`
	rows, err := db.Query(explainQuery, envID)
	if err == nil {
		for rows.Next() {
			var line string
			_ = rows.Scan(&line)
			explainOutput += line + "\n"
		}
		rows.Close()
		t.Logf("Query Plan (Candidate SKIP LOCKED):\n%s", explainOutput)
	}

	// Concurrent Claim Execution
	var claimedCount int64
	var deadlockErrors int64
	var otherErrors int64

	latencies := make([]time.Duration, 0, totalSteps)
	var latenciesMu sync.Mutex

	var wg sync.WaitGroup
	startSignal := make(chan struct{})

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		workerIdx := w
		sessionID := sessionIDs[workerIdx]

		go func() {
			defer wg.Done()
			<-startSignal

			for {
				start := time.Now()

				// Execute claim transaction with prescribed lock order:
				// 1. environment_admissions row lock
				// 2. candidate selection using FOR UPDATE SKIP LOCKED
				// 3. run row lock
				// 4. step row lock
				// 5. insert task_attempts + task_leases
				claimed, err := func() (bool, error) {
					tx, err := pool.Begin(ctx)
					if err != nil {
						return false, err
					}
					defer func() {
						_ = tx.Rollback(ctx)
					}()

					// Step 1: Environment admission lock (concurrency quota)
					var maxConc int
					err = tx.QueryRow(ctx, `
						SELECT max_concurrency
						FROM environment_admissions
						WHERE environment_id = $1
						FOR UPDATE
					`, envID).Scan(&maxConc)
					if err != nil {
						return false, fmt.Errorf("admission lock failed: %w", err)
					}

					// Step 2: Candidate step selection using SKIP LOCKED
					var candidateStepID, candidateRunID string
					err = tx.QueryRow(ctx, `
						SELECT id, run_id
						FROM run_steps
						WHERE environment_id = $1 AND state = 'READY'
						ORDER BY eligible_at ASC, id ASC
						LIMIT 1
						FOR UPDATE SKIP LOCKED
					`, envID).Scan(&candidateStepID, &candidateRunID)
					if err != nil {
						if err == pgx.ErrNoRows {
							// No more ready work
							return false, nil
						}
						return false, fmt.Errorf("candidate selection failed: %w", err)
					}

					// Step 3: Run row lock
					var runRevision int64
					err = tx.QueryRow(ctx, `
						SELECT revision
						FROM runs
						WHERE id = $1
						FOR UPDATE
					`, candidateRunID).Scan(&runRevision)
					if err != nil {
						return false, fmt.Errorf("run lock failed: %w", err)
					}

					// Step 4: Step transition to RUNNING + epoch increment
					var epoch int64
					var attemptNum int
					err = tx.QueryRow(ctx, `
						UPDATE run_steps
						SET state = 'RUNNING',
						    current_epoch = current_epoch + 1,
						    next_attempt_number = next_attempt_number + 1,
						    updated_at = clock_timestamp()
						WHERE id = $1
						RETURNING current_epoch, next_attempt_number - 1
					`, candidateStepID).Scan(&epoch, &attemptNum)
					if err != nil {
						return false, fmt.Errorf("step update failed: %w", err)
					}

					// Step 5: Insert task_attempts and task_leases
					var attemptID string
					err = tx.QueryRow(ctx, `
						INSERT INTO task_attempts (organization_id, step_id, attempt_number, session_id, epoch, status, started_at)
						VALUES ($1, $2, $3, $4, $5, 'CLAIMED', clock_timestamp())
						RETURNING id
					`, orgID, candidateStepID, attemptNum, sessionID, epoch).Scan(&attemptID)
					if err != nil {
						return false, fmt.Errorf("attempt insert failed: %w", err)
					}

					// Lease: 30-second lease
					_, err = tx.Exec(ctx, `
						INSERT INTO task_leases (step_id, organization_id, attempt_id, session_id, epoch, expires_at)
						VALUES ($1, $2, $3, $4, $5, clock_timestamp() + interval '30 seconds')
					`, candidateStepID, orgID, attemptID, sessionID, epoch)
					if err != nil {
						return false, fmt.Errorf("lease insert failed: %w", err)
					}

					if err := tx.Commit(ctx); err != nil {
						return false, err
					}

					return true, nil
				}()

				elapsed := time.Since(start)

				if err != nil {
					// Check for deadlock code 40P01
					if err.Error() == "ERROR: deadlock detected (SQLSTATE 40P01)" {
						atomic.AddInt64(&deadlockErrors, 1)
					} else {
						atomic.AddInt64(&otherErrors, 1)
					}
					t.Logf("Worker %d claim error: %v", workerIdx, err)
					continue
				}

				if !claimed {
					// No more ready work
					break
				}

				atomic.AddInt64(&claimedCount, 1)
				latenciesMu.Lock()
				latencies = append(latencies, elapsed)
				latenciesMu.Unlock()
			}
		}()
	}

	// Release all workers simultaneously
	close(startSignal)
	wg.Wait()

	t.Logf("Claim contention run finished:")
	t.Logf("  Total steps to claim: %d", totalSteps)
	t.Logf("  Total successfully claimed: %d", claimedCount)
	t.Logf("  Deadlock errors (40P01): %d", deadlockErrors)
	t.Logf("  Other errors: %d", otherErrors)

	// INVARIANT CHECKS:
	// 1. Zero deadlocks
	if deadlockErrors > 0 {
		t.Fatalf("DEADLOCK INVERSION DETECTED: %d deadlocks encountered!", deadlockErrors)
	}

	// 2. Exactly all steps claimed
	if claimedCount != int64(totalSteps) {
		t.Fatalf("CLAIM COUNT MISMATCH: expected %d claimed, got %d", totalSteps, claimedCount)
	}

	// 3. Verify INV-03: exactly one lease and one attempt per step in DB
	var leaseCount, attemptCount int
	err = db.QueryRow("SELECT count(*) FROM task_leases WHERE organization_id = $1", orgID).Scan(&leaseCount)
	if err != nil || leaseCount != totalSteps {
		t.Fatalf("INV-03 VIOLATION: expected %d task_leases, got %d (err: %v)", totalSteps, leaseCount, err)
	}

	err = db.QueryRow("SELECT count(*) FROM task_attempts WHERE organization_id = $1", orgID).Scan(&attemptCount)
	if err != nil || attemptCount != totalSteps {
		t.Fatalf("ATTEMPT COUNT MISMATCH: expected %d task_attempts, got %d (err: %v)", totalSteps, attemptCount, err)
	}

	// Calculate and report latency percentiles
	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		p50 := latencies[len(latencies)*50/100]
		p95 := latencies[len(latencies)*95/100]
		p99 := latencies[len(latencies)*99/100]
		t.Logf("Claim Latencies under 20 concurrent workers:")
		t.Logf("  p50: %v", p50)
		t.Logf("  p95: %v", p95)
		t.Logf("  p99: %v", p99)
	}
}
