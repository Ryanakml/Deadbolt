package sp02_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/storage/migrator"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func getTestDatabaseURL() string {
	if s := os.Getenv("TEST_DATABASE_URL"); s != "" {
		return s
	}
	if s := os.Getenv("DATABASE_URL"); s != "" {
		return s
	}
	return "postgres://localhost:5432/deadbolt_test?sslmode=disable"
}

func setupSP02DB(t *testing.T) (*sql.DB, *pgxpool.Pool) {
	t.Helper()

	connStr := getTestDatabaseURL()
	db, err := sql.Open("pgx", connStr)
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

	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		t.Fatalf("failed to parse pool config: %v", err)
	}
	config.MaxConns = 35

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("failed to create pgxpool: %v", err)
	}

	return db, pool
}

// seedSP02Environment creates tenant, environment, admission quota, deployment, runs, and worker sessions.
func seedSP02Environment(t *testing.T, db *sql.DB, testPrefix int, orgID, projectID, envID, deploymentID string, numRuns, stepsPerRun, numWorkers int) []string {
	t.Helper()

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

	// Clean up previous runs/leases/attempts for repeatable test execution
	_, _ = db.Exec("DELETE FROM task_leases WHERE organization_id = $1", orgID)
	_, _ = db.Exec("DELETE FROM task_attempts WHERE organization_id = $1", orgID)
	_, _ = db.Exec("DELETE FROM run_steps WHERE organization_id = $1", orgID)
	_, _ = db.Exec("DELETE FROM runs WHERE organization_id = $1", orgID)

	for r := 1; r <= numRuns; r++ {
		runID := fmt.Sprintf("50000000-%04d-0000-0000-%012d", testPrefix, r)
		_, err := db.Exec(`
			INSERT INTO runs (id, organization_id, environment_id, deployment_id, workflow_name, status)
			VALUES ($1, $2, $3, $4, 'sp02-workflow', 'RUNNING')
			ON CONFLICT (organization_id, id) DO NOTHING;
		`, runID, orgID, envID, deploymentID)
		if err != nil {
			t.Fatalf("failed to insert run %d: %v", r, err)
		}

		for s := 1; s <= stepsPerRun; s++ {
			stepID := fmt.Sprintf("60000000-%04d-0000-%04d-%012d", testPrefix, r, s)
			nodeID := fmt.Sprintf("task_node_%d_%d", r, s)
			_, err := db.Exec(`
				INSERT INTO run_steps (id, organization_id, environment_id, run_id, node_id, kind, state, eligible_at)
				VALUES ($1, $2, $3, $4, $5, 'task', 'READY', clock_timestamp())
				ON CONFLICT (organization_id, id) DO NOTHING;
			`, stepID, orgID, envID, runID, nodeID)
			if err != nil {
				t.Fatalf("failed to insert step %d-%d: %v", r, s, err)
			}
		}
	}

	var sessionIDs []string
	for w := 1; w <= numWorkers; w++ {
		workerID := fmt.Sprintf("70000000-%04d-0000-0000-%012d", testPrefix, w)
		sessionID := fmt.Sprintf("80000000-%04d-0000-0000-%012d", testPrefix, w)
		if _, err := db.Exec("INSERT INTO workers (id, organization_id, environment_id, public_key, status) VALUES ($1, $2, $3, 'pubkey', 'ACTIVE') ON CONFLICT (organization_id, id) DO NOTHING", workerID, orgID, envID); err != nil {
			t.Fatalf("failed to seed worker %d: %v", w, err)
		}
		if _, err := db.Exec("INSERT INTO worker_sessions (id, organization_id, worker_id, environment_id, session_token_hash, expires_at) VALUES ($1, $2, $3, $4, 'tokenhash', clock_timestamp() + interval '1 hour') ON CONFLICT (organization_id, id) DO NOTHING", sessionID, orgID, workerID, envID); err != nil {
			t.Fatalf("failed to seed session %d: %v", w, err)
		}
		sessionIDs = append(sessionIDs, sessionID)
	}

	return sessionIDs
}

// TestClaimContentionAndPrescribedLockOrder executes the SP-02 claim contention validation:
//  1. Candidate Discovery: Non-locking scan of eligible READY steps.
//  2. Prescribed Authoritative Lock Order:
//     environment_admissions (FOR UPDATE)
//     -> runs (FOR UPDATE)
//     -> run_steps (FOR UPDATE)
//     -> revalidate state (detect & retry stale candidates)
//     -> insert task_attempts + task_leases
//  3. Simulates 20 concurrent worker goroutines claiming 50 ready tasks across 10 runs.
//  4. Verifies Invariant INV-03: Zero duplicate ownership, exactly 1 active lease per step, 0 deadlocks.
func TestClaimContentionAndPrescribedLockOrder(t *testing.T) {
	db, pool := setupSP02DB(t)
	defer db.Close()
	defer pool.Close()

	ctx := context.Background()

	orgID := "10000000-0000-0000-0000-000000000001"
	projectID := "20000000-0000-0000-0000-000000000001"
	envID := "30000000-0000-0000-0000-000000000001"
	deploymentID := "40000000-0000-0000-0000-000000000001"

	const numRuns = 10
	const stepsPerRun = 5
	const totalSteps = numRuns * stepsPerRun
	const numWorkers = 20

	sessionIDs := seedSP02Environment(t, db, 1, orgID, projectID, envID, deploymentID, numRuns, stepsPerRun, numWorkers)
	t.Logf("Seeded %d runs, %d ready steps, %d worker sessions", numRuns, totalSteps, numWorkers)

	var claimedCount int64
	var deadlockErrors int64
	var unexpectedErrors int64
	var staleCandidateRetries int64

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

				// Step 1: Candidate Discovery (Non-locking scan)
				var candidateStepID, candidateRunID string
				err := pool.QueryRow(ctx, `
					SELECT id, run_id
					FROM run_steps
					WHERE environment_id = $1 AND state = 'READY'
					ORDER BY eligible_at ASC, id ASC
					LIMIT 1
				`, envID).Scan(&candidateStepID, &candidateRunID)

				if err != nil {
					if err == pgx.ErrNoRows {
						// No more ready work candidates
						break
					}
					var pgErr *pgconn.PgError
					if errors.As(err, &pgErr) && pgErr.Code == "40P01" {
						atomic.AddInt64(&deadlockErrors, 1)
					} else {
						atomic.AddInt64(&unexpectedErrors, 1)
						t.Errorf("candidate query unexpected error: %v", err)
					}
					break
				}

				// Step 2: Authoritative Claim Transaction with Prescribed Lock Order
				// Order: environment_admissions -> runs -> run_steps -> task_attempts/leases
				type claimResult int
				const (
					resultSuccess claimResult = iota
					resultStale
					resultError
				)

				res, txErr := func() (claimResult, error) {
					tx, err := pool.Begin(ctx)
					if err != nil {
						return resultError, err
					}
					defer func() {
						_ = tx.Rollback(ctx)
					}()

					// 2a. Environment admission lock (concurrency quota)
					var maxConc int
					err = tx.QueryRow(ctx, `
						SELECT max_concurrency
						FROM environment_admissions
						WHERE environment_id = $1
						FOR UPDATE
					`, envID).Scan(&maxConc)
					if err != nil {
						return resultError, fmt.Errorf("admission lock failed: %w", err)
					}

					// 2b. Run row lock
					var runRevision int64
					var runStatus string
					err = tx.QueryRow(ctx, `
						SELECT revision, status
						FROM runs
						WHERE id = $1
						FOR UPDATE
					`, candidateRunID).Scan(&runRevision, &runStatus)
					if err != nil {
						return resultError, fmt.Errorf("run lock failed: %w", err)
					}

					// If run is no longer RUNNING, candidate is stale
					if runStatus != "RUNNING" {
						return resultStale, nil
					}

					// 2c. Step row lock
					var stepState string
					var currentEpoch int64
					var nextAttemptNum int
					err = tx.QueryRow(ctx, `
						SELECT state, current_epoch, next_attempt_number
						FROM run_steps
						WHERE id = $1
						FOR UPDATE
					`, candidateStepID).Scan(&stepState, &currentEpoch, &nextAttemptNum)
					if err != nil {
						return resultError, fmt.Errorf("step lock failed: %w", err)
					}

					// 2d. Revalidate step state: must still be READY
					if stepState != "READY" {
						// Another worker claimed or updated this step concurrently
						return resultStale, nil
					}

					// 2e. Update step to RUNNING with epoch increment
					newEpoch := currentEpoch + 1
					newAttempt := nextAttemptNum
					_, err = tx.Exec(ctx, `
						UPDATE run_steps
						SET state = 'RUNNING',
						    current_epoch = $2,
						    next_attempt_number = next_attempt_number + 1,
						    updated_at = clock_timestamp()
						WHERE id = $1
					`, candidateStepID, newEpoch)
					if err != nil {
						return resultError, fmt.Errorf("step update failed: %w", err)
					}

					// 2f. Insert task_attempts
					var attemptID string
					err = tx.QueryRow(ctx, `
						INSERT INTO task_attempts (organization_id, step_id, attempt_number, session_id, epoch, status, started_at)
						VALUES ($1, $2, $3, $4, $5, 'CLAIMED', clock_timestamp())
						RETURNING id
					`, orgID, candidateStepID, newAttempt, sessionID, newEpoch).Scan(&attemptID)
					if err != nil {
						return resultError, fmt.Errorf("attempt insert failed: %w", err)
					}

					// 2g. Insert task_leases
					_, err = tx.Exec(ctx, `
						INSERT INTO task_leases (step_id, organization_id, attempt_id, session_id, epoch, expires_at)
						VALUES ($1, $2, $3, $4, $5, clock_timestamp() + interval '30 seconds')
					`, candidateStepID, orgID, attemptID, sessionID, newEpoch)
					if err != nil {
						return resultError, fmt.Errorf("lease insert failed: %w", err)
					}

					if err := tx.Commit(ctx); err != nil {
						return resultError, err
					}

					return resultSuccess, nil
				}()

				elapsed := time.Since(start)

				if txErr != nil {
					var pgErr *pgconn.PgError
					if errors.As(txErr, &pgErr) && pgErr.Code == "40P01" {
						atomic.AddInt64(&deadlockErrors, 1)
						t.Errorf("DEADLOCK 40P01 detected during claim: %v", txErr)
					} else {
						atomic.AddInt64(&unexpectedErrors, 1)
						t.Errorf("unexpected claim error: %v", txErr)
					}
					continue
				}

				if res == resultStale {
					atomic.AddInt64(&staleCandidateRetries, 1)
					continue
				}

				if res == resultSuccess {
					atomic.AddInt64(&claimedCount, 1)
					latenciesMu.Lock()
					latencies = append(latencies, elapsed)
					latenciesMu.Unlock()
				}
			}
		}()
	}

	// Release all workers simultaneously
	close(startSignal)
	wg.Wait()

	t.Logf("SP-02 Claim Contention Run Results:")
	t.Logf("  Total steps to claim: %d", totalSteps)
	t.Logf("  Total successfully claimed: %d", claimedCount)
	t.Logf("  Stale candidate retries: %d", staleCandidateRetries)
	t.Logf("  Deadlock errors (40P01): %d", deadlockErrors)
	t.Logf("  Unexpected DB errors: %d", unexpectedErrors)

	// INVARIANT VERIFICATIONS:
	// 1. Zero deadlocks
	if deadlockErrors > 0 {
		t.Fatalf("DEADLOCK VIOLATION: %d deadlocks (40P01) encountered!", deadlockErrors)
	}

	// 2. Zero unexpected errors
	if unexpectedErrors > 0 {
		t.Fatalf("UNEXPECTED ERROR VIOLATION: %d unexpected DB errors encountered!", unexpectedErrors)
	}

	// 3. Exactly all steps claimed
	if claimedCount != int64(totalSteps) {
		t.Fatalf("CLAIM COUNT MISMATCH: expected %d claimed, got %d", totalSteps, claimedCount)
	}

	// 4. Verify INV-03: exactly one lease and one attempt per step in DB
	var leaseCount, attemptCount int
	err := db.QueryRow("SELECT count(*) FROM task_leases WHERE organization_id = $1", orgID).Scan(&leaseCount)
	if err != nil || leaseCount != totalSteps {
		t.Fatalf("INV-03 VIOLATION: expected %d task_leases, got %d (err: %v)", totalSteps, leaseCount, err)
	}

	err = db.QueryRow("SELECT count(*) FROM task_attempts WHERE organization_id = $1", orgID).Scan(&attemptCount)
	if err != nil || attemptCount != totalSteps {
		t.Fatalf("ATTEMPT COUNT MISMATCH: expected %d task_attempts, got %d (err: %v)", totalSteps, attemptCount, err)
	}

	// Latency percentiles
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

// TestMixedPathContention tests multi-path concurrent operations on the same runs and steps:
// Path 1: Task Claimers (admission -> run -> step -> attempt/lease)
// Path 2: Task Completers (run -> step -> lease delete -> succeed attempt)
// Path 3: Reconciler / Observer (run -> steps in ascending order)
// Verifies that strict lock ordering prevents deadlocks across conflicting paths.
func TestMixedPathContention(t *testing.T) {
	db, pool := setupSP02DB(t)
	defer db.Close()
	defer pool.Close()

	ctx := context.Background()

	orgID := "10000000-0000-0000-0000-000000000002"
	projectID := "20000000-0000-0000-0000-000000000002"
	envID := "30000000-0000-0000-0000-000000000002"
	deploymentID := "40000000-0000-0000-0000-000000000002"

	const numRuns = 10
	const stepsPerRun = 5
	const totalSteps = numRuns * stepsPerRun
	const numWorkers = 15

	sessionIDs := seedSP02Environment(t, db, 2, orgID, projectID, envID, deploymentID, numRuns, stepsPerRun, numWorkers)

	var claimedCount int64
	var completedCount int64
	var reconcilerScans int64
	var deadlockErrors int64
	var unexpectedErrors int64

	var wg sync.WaitGroup
	startSignal := make(chan struct{})
	stopSignal := make(chan struct{})

	// 1. Launch 10 Claim Workers
	for w := 0; w < 10; w++ {
		wg.Add(1)
		sessionID := sessionIDs[w]

		go func() {
			defer wg.Done()
			<-startSignal

			for {
				select {
				case <-stopSignal:
					return
				default:
				}

				// Non-locking candidate discovery
				var candidateStepID, candidateRunID string
				err := pool.QueryRow(ctx, `
					SELECT id, run_id
					FROM run_steps
					WHERE environment_id = $1 AND state = 'READY'
					ORDER BY eligible_at ASC, id ASC
					LIMIT 1
				`, envID).Scan(&candidateStepID, &candidateRunID)

				if err != nil {
					if err == pgx.ErrNoRows {
						// Check if all steps completed
						if atomic.LoadInt64(&completedCount) >= int64(totalSteps) {
							return
						}
						time.Sleep(5 * time.Millisecond)
						continue
					}
					var pgErr *pgconn.PgError
					if errors.As(err, &pgErr) && pgErr.Code == "40P01" {
						atomic.AddInt64(&deadlockErrors, 1)
					}
					return
				}

				// Authoritative claim: admission -> run -> step
				tx, err := pool.Begin(ctx)
				if err != nil {
					return
				}

				var maxConc int
				_ = tx.QueryRow(ctx, "SELECT max_concurrency FROM environment_admissions WHERE environment_id = $1 FOR UPDATE", envID).Scan(&maxConc)

				var runStatus string
				_ = tx.QueryRow(ctx, "SELECT status FROM runs WHERE id = $1 FOR UPDATE", candidateRunID).Scan(&runStatus)

				var stepState string
				var epoch int64
				var nextAttempt int
				err = tx.QueryRow(ctx, `
					SELECT state, current_epoch, next_attempt_number
					FROM run_steps
					WHERE id = $1
					FOR UPDATE
				`, candidateStepID).Scan(&stepState, &epoch, &nextAttempt)

				if err != nil || stepState != "READY" || runStatus != "RUNNING" {
					_ = tx.Rollback(ctx)
					continue
				}

				newEpoch := epoch + 1
				_, err = tx.Exec(ctx, `
					UPDATE run_steps
					SET state = 'RUNNING',
					    current_epoch = $2,
					    next_attempt_number = next_attempt_number + 1,
					    updated_at = clock_timestamp()
					WHERE id = $1
				`, candidateStepID, newEpoch)
				if err != nil {
					_ = tx.Rollback(ctx)
					continue
				}

				var attemptID string
				err = tx.QueryRow(ctx, `
					INSERT INTO task_attempts (organization_id, step_id, attempt_number, session_id, epoch, status, started_at)
					VALUES ($1, $2, $3, $4, $5, 'CLAIMED', clock_timestamp())
					RETURNING id
				`, orgID, candidateStepID, nextAttempt, sessionID, newEpoch).Scan(&attemptID)
				if err != nil {
					_ = tx.Rollback(ctx)
					continue
				}

				_, err = tx.Exec(ctx, `
					INSERT INTO task_leases (step_id, organization_id, attempt_id, session_id, epoch, expires_at)
					VALUES ($1, $2, $3, $4, $5, clock_timestamp() + interval '30 seconds')
				`, candidateStepID, orgID, attemptID, sessionID, newEpoch)
				if err != nil {
					_ = tx.Rollback(ctx)
					continue
				}

				if err := tx.Commit(ctx); err == nil {
					atomic.AddInt64(&claimedCount, 1)
				} else {
					var pgErr *pgconn.PgError
					if errors.As(err, &pgErr) && pgErr.Code == "40P01" {
						atomic.AddInt64(&deadlockErrors, 1)
					}
				}
			}
		}()
	}

	// 2. Launch 5 Completion Workers
	// Lock order for completion: run -> step -> delete lease -> update attempt -> update step
	for c := 0; c < 5; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startSignal

			for {
				select {
				case <-stopSignal:
					return
				default:
				}

				// Find a running step candidate
				var stepID, runID string
				var epoch int64
				err := pool.QueryRow(ctx, `
					SELECT id, run_id, current_epoch
					FROM run_steps
					WHERE environment_id = $1 AND state = 'RUNNING'
					LIMIT 1
				`, envID).Scan(&stepID, &runID, &epoch)

				if err != nil {
					if err == pgx.ErrNoRows {
						if atomic.LoadInt64(&completedCount) >= int64(totalSteps) {
							return
						}
						time.Sleep(5 * time.Millisecond)
						continue
					}
					return
				}

				// Authoritative completion transaction: run FOR UPDATE -> step FOR UPDATE
				tx, err := pool.Begin(ctx)
				if err != nil {
					return
				}

				var runRev int64
				err = tx.QueryRow(ctx, "SELECT revision FROM runs WHERE id = $1 FOR UPDATE", runID).Scan(&runRev)
				if err != nil {
					_ = tx.Rollback(ctx)
					continue
				}

				var stepState string
				var stepEpoch int64
				err = tx.QueryRow(ctx, "SELECT state, current_epoch FROM run_steps WHERE id = $1 FOR UPDATE", stepID).Scan(&stepState, &stepEpoch)
				if err != nil || stepState != "RUNNING" {
					_ = tx.Rollback(ctx)
					continue
				}

				// Delete active lease
				_, err = tx.Exec(ctx, "DELETE FROM task_leases WHERE step_id = $1", stepID)
				if err != nil {
					_ = tx.Rollback(ctx)
					continue
				}

				// Mark step succeeded
				_, err = tx.Exec(ctx, "UPDATE run_steps SET state = 'SUCCEEDED', updated_at = clock_timestamp() WHERE id = $1", stepID)
				if err != nil {
					_ = tx.Rollback(ctx)
					continue
				}

				// Mark attempt succeeded
				_, err = tx.Exec(ctx, "UPDATE task_attempts SET status = 'SUCCEEDED', completed_at = clock_timestamp() WHERE step_id = $1 AND epoch = $2", stepID, stepEpoch)
				if err != nil {
					_ = tx.Rollback(ctx)
					continue
				}

				if err := tx.Commit(ctx); err == nil {
					atomic.AddInt64(&completedCount, 1)
				} else {
					var pgErr *pgconn.PgError
					if errors.As(err, &pgErr) && pgErr.Code == "40P01" {
						atomic.AddInt64(&deadlockErrors, 1)
					}
				}
			}
		}()
	}

	// 3. Launch 2 Reconcilers / Observers
	// Lock order for reconciler: run -> steps in ascending id order
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startSignal

			for {
				select {
				case <-stopSignal:
					return
				default:
				}

				for runIdx := 1; runIdx <= numRuns; runIdx++ {
					runID := fmt.Sprintf("50000000-%04d-0000-0000-%012d", 2, runIdx)

					func() {
						tx, err := pool.Begin(ctx)
						if err != nil {
							return
						}
						defer func() {
							_ = tx.Rollback(ctx)
						}()

						// Lock run
						var rev int64
						err = tx.QueryRow(ctx, "SELECT revision FROM runs WHERE id = $1 FOR UPDATE", runID).Scan(&rev)
						if err != nil {
							return
						}

						// Lock steps belonging to this run in deterministic ASCENDING order
						rows, err := tx.Query(ctx, "SELECT id, state FROM run_steps WHERE run_id = $1 ORDER BY id ASC FOR UPDATE", runID)
						if err != nil {
							var pgErr *pgconn.PgError
							if errors.As(err, &pgErr) && pgErr.Code == "40P01" {
								atomic.AddInt64(&deadlockErrors, 1)
							}
							return
						}
						rows.Close()

						_ = tx.Commit(ctx)
						atomic.AddInt64(&reconcilerScans, 1)
					}()

					if atomic.LoadInt64(&completedCount) >= int64(totalSteps) {
						return
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
		}()
	}

	// Release all roles simultaneously
	close(startSignal)

	// Wait for completion or timeout
	doneCh := make(chan struct{})
	go func() {
		for {
			if atomic.LoadInt64(&completedCount) >= int64(totalSteps) {
				close(stopSignal)
				close(doneCh)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	select {
	case <-doneCh:
	case <-time.After(30 * time.Second):
		close(stopSignal)
		t.Fatalf("Mixed path contention test timed out! Completed: %d/%d", atomic.LoadInt64(&completedCount), totalSteps)
	}

	wg.Wait()

	t.Logf("Mixed-Path Contention Run Results:")
	t.Logf("  Total steps completed: %d / %d", completedCount, totalSteps)
	t.Logf("  Total claim operations: %d", claimedCount)
	t.Logf("  Total reconciler scans: %d", reconcilerScans)
	t.Logf("  Deadlock errors (40P01): %d", deadlockErrors)
	t.Logf("  Unexpected errors: %d", unexpectedErrors)

	if deadlockErrors > 0 {
		t.Fatalf("DEADLOCK DETECTED in mixed-path contention: %d occurrences", deadlockErrors)
	}

	if completedCount != int64(totalSteps) {
		t.Fatalf("COMPLETION MISMATCH: expected %d completed, got %d", totalSteps, completedCount)
	}

	// Verify all leases deleted
	var activeLeases int
	err := db.QueryRow("SELECT count(*) FROM task_leases WHERE organization_id = $1", orgID).Scan(&activeLeases)
	if err != nil || activeLeases != 0 {
		t.Fatalf("LEAKED LEASES: expected 0 active leases, got %d (err: %v)", activeLeases, err)
	}
}
