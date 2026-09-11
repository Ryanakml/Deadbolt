package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/storage/migrator"
	"github.com/Ryanakml/Deadbolt/internal/storage/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func setupTestDB(t *testing.T) (*sql.DB, *pgxpool.Pool, string) {
	t.Helper()

	migrationsDir, err := filepath.Abs("../../migrations")
	if err != nil {
		t.Fatalf("failed to resolve migrations dir: %v", err)
	}
	bootstrapPath, err := filepath.Abs("../../scripts/bootstrap-db-roles.sql")
	if err != nil {
		t.Fatalf("failed to resolve bootstrap path: %v", err)
	}

	// 1. Setup isolated database 'deadbolt_integration_test' with admin bootstrap script
	migratorURL, runtimeURL, systemURL, err := testdb.SetupIsolatedDatabase("deadbolt_integration_test", bootstrapPath)
	if err != nil {
		t.Skipf("PostgreSQL isolated database setup skipped: %v", err)
	}

	// 2. Connect strictly as deadbolt_migrator to run migrations
	db, err := sql.Open("pgx", migratorURL)
	if err != nil {
		t.Fatalf("failed to open db as deadbolt_migrator: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("failed to ping db as deadbolt_migrator: %v", err)
	}

	runner := migrator.NewRunner(db, migrationsDir)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := runner.Up(ctx); err != nil {
		t.Fatalf("migrations as deadbolt_migrator failed: %v", err)
	}

	// 3. Connect runtime pool strictly as deadbolt_runtime (no DDL, no BYPASSRLS)
	runtimePool, err := pgxpool.New(context.Background(), runtimeURL)
	if err != nil {
		t.Fatalf("failed to create runtime pgxpool: %v", err)
	}

	return db, runtimePool, systemURL
}

// TestCleanDatabaseMigrationAsDeadboltMigrator verifies that deadbolt_migrator can
// successfully apply the entire migration chain from scratch under an advisory lock.
func TestCleanDatabaseMigrationAsDeadboltMigrator(t *testing.T) {
	db, runtimePool, _ := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	runner := migrator.NewRunner(db, "../../migrations")
	v, err := runner.Version(context.Background())
	if err != nil {
		t.Fatalf("failed to get migration version: %v", err)
	}
	if v != 4 {
		t.Fatalf("expected latest migration version 4, got %d", v)
	}
}

// TestMigrationAdvisoryLockBlocking verifies single-migrator serialization via pinned session advisory lock.
// When an active session holds the advisory lock, Goose migration execution on another runner is blocked
// and cannot execute migration SQL until the lock is released.
func TestMigrationAdvisoryLockBlocking(t *testing.T) {
	db, runtimePool, _ := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	runnerA := migrator.NewRunner(db, "../../migrations")
	runnerB := migrator.NewRunner(db, "../../migrations")

	ctx := context.Background()

	// 0. Roll down to version 3 so that migration 4 is pending
	if err := runnerB.DownTo(ctx, 3); err != nil {
		t.Fatalf("failed to prepare pending migration state: %v", err)
	}
	v, err := runnerB.Version(ctx)
	if err != nil || v != 3 {
		t.Fatalf("expected initial version 3, got %d (err: %v)", v, err)
	}

	// 1. Runner A acquires advisory lock
	if err := runnerA.AcquireAdvisoryLock(ctx); err != nil {
		t.Fatalf("Runner A failed to acquire advisory lock: %v", err)
	}

	// 2. Runner B attempts to run pending Goose migration Up with short timeout -> must time out / be blocked.
	// This proves that migration execution itself cannot bypass the advisory lock critical section.
	timeoutCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	err = runnerB.Up(timeoutCtx)
	cancel()
	if err == nil {
		t.Fatalf("CONCURRENCY VIOLATION: Runner B executed migration Up while Runner A was holding advisory lock!")
	}

	// Verify that version is STILL 3 (no migration SQL was executed by Runner B)
	v, err = runnerA.Version(ctx)
	if err != nil || v != 3 {
		t.Fatalf("CONCURRENCY VIOLATION: migration SQL executed outside lock; version is %d, expected 3", v)
	}

	// Also verify DownTo is blocked while Runner A holds the lock
	timeoutDownCtx, cancelDown := context.WithTimeout(ctx, 1500*time.Millisecond)
	err = runnerB.DownTo(timeoutDownCtx, 2)
	cancelDown()
	if err == nil {
		t.Fatalf("CONCURRENCY VIOLATION: Runner B executed migration DownTo while Runner A was holding advisory lock!")
	}

	// Verify that version is STILL 3 (no rollback SQL was executed by Runner B)
	v, err = runnerA.Version(ctx)
	if err != nil || v != 3 {
		t.Fatalf("CONCURRENCY VIOLATION: migration SQL executed outside lock; version is %d, expected 3", v)
	}

	// 3. Runner A releases lock
	if err := runnerA.ReleaseAdvisoryLock(ctx); err != nil {
		t.Fatalf("Runner A failed to release advisory lock: %v", err)
	}

	// 4. Runner B now succeeds in running migration Up under its own session advisory lock
	acquireCtx, cancelAcquire := context.WithTimeout(ctx, 5*time.Second)
	defer cancelAcquire()
	if err := runnerB.Up(acquireCtx); err != nil {
		t.Fatalf("Runner B failed to run migration Up after Runner A released: %v", err)
	}

	// Verify version is now upgraded to 4
	v, err = runnerB.Version(ctx)
	if err != nil || v != 4 {
		t.Fatalf("expected version 4 after migration Up, got %d (err: %v)", v, err)
	}
}

// TestRuntimeNoDDLPrivileges verifies that deadbolt_runtime cannot perform DDL operations.
func TestRuntimeNoDDLPrivileges(t *testing.T) {
	db, runtimePool, _ := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	ctx := context.Background()

	// deadbolt_runtime must NOT have CREATE TABLE permission
	_, err := runtimePool.Exec(ctx, "CREATE TABLE forbidden_table (id INT)")
	if err == nil {
		t.Fatalf("SECURITY VIOLATION: deadbolt_runtime was able to execute DDL (CREATE TABLE)!")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("expected permission denied error, got: %v", err)
	}

	// deadbolt_runtime must NOT be able to alter RLS
	_, err = runtimePool.Exec(ctx, "ALTER TABLE organizations DISABLE ROW LEVEL SECURITY")
	if err == nil {
		t.Fatalf("SECURITY VIOLATION: deadbolt_runtime was able to disable RLS!")
	}
	if !strings.Contains(err.Error(), "must be owner") && !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("expected owner/permission denied error, got: %v", err)
	}
}

// TestRLSFailsClosed verifies that queries executed without SET LOCAL app.current_organization_id fail closed (0 rows).
func TestRLSFailsClosed(t *testing.T) {
	db, runtimePool, _ := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	ctx := context.Background()
	sPool := storage.NewPool(runtimePool)

	testOrgID := "00000000-0000-0000-0000-000000000001"
	err := sPool.WithTenantTx(ctx, testOrgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO organizations (id, name) VALUES ($1, 'Tenant Alpha') ON CONFLICT (id) DO NOTHING", testOrgID)
		return err
	})
	if err != nil {
		t.Fatalf("failed to insert test organization: %v", err)
	}

	// Execute query directly without setting tenant context -> must fail closed (0 rows returned)
	var count int
	err = runtimePool.QueryRow(ctx, "SELECT count(*) FROM organizations").Scan(&count)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("FAIL-CLOSED VIOLATION: expected 0 organizations without tenant context, got %d", count)
	}
}

// TestConnectionPoolReuseF27 verifies Failure Mode F-27:
// When a pooled connection is used by Tenant A, returned to the pool, and then reused,
// the tenant context does not leak.
func TestConnectionPoolReuseF27(t *testing.T) {
	db, runtimePool, _ := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	ctx := context.Background()
	sPool := storage.NewPool(runtimePool)

	orgA := "00000000-0000-0000-0000-00000000000a"
	orgB := "00000000-0000-0000-0000-00000000000b"

	_ = sPool.WithTenantTx(ctx, orgA, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO organizations (id, name) VALUES ($1, 'Org A') ON CONFLICT (id) DO NOTHING", orgA)
		return err
	})

	_ = sPool.WithTenantTx(ctx, orgB, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO organizations (id, name) VALUES ($1, 'Org B') ON CONFLICT (id) DO NOTHING", orgB)
		return err
	})

	conn, err := runtimePool.Acquire(ctx)
	if err != nil {
		t.Fatalf("failed to acquire connection: %v", err)
	}

	// Transaction 1: Run as Org A
	tx1, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin tx1: %v", err)
	}
	_, err = tx1.Exec(ctx, "SELECT set_config('app.current_organization_id', $1, true)", orgA)
	if err != nil {
		t.Fatalf("failed to set context: %v", err)
	}
	var nameA string
	err = tx1.QueryRow(ctx, "SELECT name FROM organizations WHERE id = $1", orgA).Scan(&nameA)
	if err != nil || nameA != "Org A" {
		t.Fatalf("expected to read Org A, got %v, err: %v", nameA, err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("commit tx1 failed: %v", err)
	}

	// Transaction 2 on the SAME CONNECTION without set_config: Must NOT see Org A
	tx2, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin tx2: %v", err)
	}
	var count int
	err = tx2.QueryRow(ctx, "SELECT count(*) FROM organizations").Scan(&count)
	if err != nil {
		t.Fatalf("tx2 count failed: %v", err)
	}
	_ = tx2.Rollback(ctx)
	conn.Release()

	if count != 0 {
		t.Fatalf("F-27 VIOLATION: connection retained tenant context from previous transaction! count=%d", count)
	}
}

// TestCompositeForeignKeys verifies that foreign-ID substitution across organizations fails
// across environments, task_attempts, task_leases, and artifacts.
func TestCompositeForeignKeys(t *testing.T) {
	db, runtimePool, _ := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	ctx := context.Background()
	sPool := storage.NewPool(runtimePool)

	orgA := "00000000-0000-0000-0000-000000000001"
	orgB := "00000000-0000-0000-0000-000000000002"
	projectA := "11111111-1111-1111-1111-111111111111"
	envA := "22222222-2222-2222-2222-222222222221"
	workerA := "33333333-3333-3333-3333-333333333331"
	sessionA := "44444444-4444-4444-4444-444444444441"
	deploymentA := "55555555-5555-5555-5555-555555555551"
	runA := "66666666-6666-6666-6666-666666666661"
	stepA := "77777777-7777-7777-7777-777777777771"

	// 1. Seed Org A hierarchy
	err := sPool.WithTenantTx(ctx, orgA, func(ctx context.Context, tx storage.Tx) error {
		_, _ = tx.Exec(ctx, "INSERT INTO organizations (id, name) VALUES ($1, 'Org A') ON CONFLICT DO NOTHING", orgA)
		_, _ = tx.Exec(ctx, "INSERT INTO projects (id, organization_id, name) VALUES ($1, $2, 'Project A') ON CONFLICT DO NOTHING", projectA, orgA)
		_, _ = tx.Exec(ctx, "INSERT INTO environments (id, organization_id, project_id, name) VALUES ($1, $2, $3, 'staging') ON CONFLICT DO NOTHING", envA, orgA, projectA)
		_, _ = tx.Exec(ctx, "INSERT INTO workers (id, organization_id, environment_id, public_key) VALUES ($1, $2, $3, 'pk') ON CONFLICT DO NOTHING", workerA, orgA, envA)
		_, _ = tx.Exec(ctx, "INSERT INTO worker_sessions (id, organization_id, worker_id, environment_id, session_token_hash, expires_at) VALUES ($1, $2, $3, $4, 'h', clock_timestamp() + interval '1 hour') ON CONFLICT DO NOTHING", sessionA, orgA, workerA, envA)
		_, _ = tx.Exec(ctx, "INSERT INTO deployments (id, organization_id, environment_id, manifest_hash, bundle_digest, manifest, protocol_version, runtime_version) VALUES ($1, $2, $3, 'h', 'd', '{}', 1, '1.0') ON CONFLICT DO NOTHING", deploymentA, orgA, envA)
		_, _ = tx.Exec(ctx, "INSERT INTO runs (id, organization_id, environment_id, deployment_id, workflow_name) VALUES ($1, $2, $3, $4, 'wf') ON CONFLICT DO NOTHING", runA, orgA, envA, deploymentA)
		_, _ = tx.Exec(ctx, "INSERT INTO run_steps (id, organization_id, environment_id, run_id, node_id) VALUES ($1, $2, $3, $4, 'node1') ON CONFLICT DO NOTHING", stepA, orgA, envA, runA)
		return nil
	})
	if err != nil {
		t.Fatalf("failed to seed Org A: %v", err)
	}

	// 2. Seed Org B basic org/project/env
	envB := "22222222-2222-2222-2222-222222222222"
	projectB := "11111111-1111-1111-1111-111111111112"
	runB := "66666666-6666-6666-6666-666666666662"
	stepB := "77777777-7777-7777-7777-777777777772"
	deploymentB := "55555555-5555-5555-5555-555555555552"

	err = sPool.WithTenantTx(ctx, orgB, func(ctx context.Context, tx storage.Tx) error {
		_, _ = tx.Exec(ctx, "INSERT INTO organizations (id, name) VALUES ($1, 'Org B') ON CONFLICT DO NOTHING", orgB)
		_, _ = tx.Exec(ctx, "INSERT INTO projects (id, organization_id, name) VALUES ($1, $2, 'Project B') ON CONFLICT DO NOTHING", projectB, orgB)
		_, _ = tx.Exec(ctx, "INSERT INTO environments (id, organization_id, project_id, name) VALUES ($1, $2, $3, 'staging') ON CONFLICT DO NOTHING", envB, orgB, projectB)
		_, _ = tx.Exec(ctx, "INSERT INTO deployments (id, organization_id, environment_id, manifest_hash, bundle_digest, manifest, protocol_version, runtime_version) VALUES ($1, $2, $3, 'h2', 'd2', '{}', 1, '1.0') ON CONFLICT DO NOTHING", deploymentB, orgB, envB)
		_, _ = tx.Exec(ctx, "INSERT INTO runs (id, organization_id, environment_id, deployment_id, workflow_name) VALUES ($1, $2, $3, $4, 'wf2') ON CONFLICT DO NOTHING", runB, orgB, envB, deploymentB)
		_, _ = tx.Exec(ctx, "INSERT INTO run_steps (id, organization_id, environment_id, run_id, node_id) VALUES ($1, $2, $3, $4, 'nodeB') ON CONFLICT DO NOTHING", stepB, orgB, envB, runB)
		return nil
	})
	if err != nil {
		t.Fatalf("failed to seed Org B: %v", err)
	}

	// Test A: Environment under Org B pointing to Project A (Org A)
	err = sPool.WithTenantTx(ctx, orgB, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO environments (id, organization_id, project_id, name) VALUES ($1, $2, $3, 'dev')",
			"22222222-2222-2222-2222-222222222223", orgB, projectA)
		return err
	})
	if err == nil {
		t.Fatalf("COMPOSITE FK VIOLATION: environment in Org B referenced Project in Org A!")
	}

	// Test B: Task attempt under Org B referencing session A (Org A)
	err = sPool.WithTenantTx(ctx, orgB, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO task_attempts (organization_id, step_id, attempt_number, session_id, epoch, status)
			VALUES ($1, $2, 1, $3, 1, 'CLAIMED')
		`, orgB, stepB, sessionA)
		return err
	})
	if err == nil {
		t.Fatalf("COMPOSITE FK VIOLATION: task_attempt in Org B referenced session in Org A!")
	}

	// Test C: Task lease under Org B referencing session A (Org A)
	var attemptB string
	_ = sPool.WithTenantTx(ctx, orgB, func(ctx context.Context, tx storage.Tx) error {
		_ = tx.QueryRow(ctx, `
			INSERT INTO task_attempts (organization_id, step_id, attempt_number, epoch, status)
			VALUES ($1, $2, 2, 2, 'CLAIMED') RETURNING id
		`, orgB, stepB).Scan(&attemptB)
		return nil
	})

	err = sPool.WithTenantTx(ctx, orgB, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO task_leases (step_id, organization_id, attempt_id, session_id, epoch, expires_at)
			VALUES ($1, $2, $3, $4, 2, clock_timestamp() + interval '30 seconds')
		`, stepB, orgB, attemptB, sessionA)
		return err
	})
	if err == nil {
		t.Fatalf("COMPOSITE FK VIOLATION: task_lease in Org B referenced session in Org A!")
	}

	// Test D: Artifact under Org B referencing run A (Org A)
	err = sPool.WithTenantTx(ctx, orgB, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO artifacts (organization_id, environment_id, run_id, storage_key, size_bytes, sha256_hash)
			VALUES ($1, $2, $3, 'key', 100, 'hash')
		`, orgB, envB, runA)
		return err
	})
	if err == nil {
		t.Fatalf("COMPOSITE FK VIOLATION: artifact in Org B referenced run in Org A!")
	}
}

// TestRestrictedDiscoveryFunctions verifies Blueprint §24.3 via the production Go helpers:
// 1. storage.DiscoverUserMemberships invokes app.discover_user_memberships($1) as deadbolt_runtime without tenant context.
// 2. storage.EnumerateTenantsForScheduler invokes app.enumerate_scheduler_tenants() as deadbolt_system.
// 3. deadbolt_system has NO direct table access on tenant tables (e.g. public.runs).
func TestRestrictedDiscoveryFunctions(t *testing.T) {
	db, runtimePool, systemURL := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	ctx := context.Background()

	org1 := "00000000-1111-0000-0000-000000000001"
	org2 := "00000000-1111-0000-0000-000000000002"
	targetUser := "aaaaaaaa-0000-0000-0000-000000000001"
	otherUser := "bbbbbbbb-0000-0000-0000-000000000001"

	// Seed organizations and memberships via migrator (bypass RLS for seeding)
	_, _ = db.Exec("INSERT INTO organizations (id, name) VALUES ($1, 'Tenant Discovery 1') ON CONFLICT DO NOTHING", org1)
	_, _ = db.Exec("INSERT INTO organizations (id, name) VALUES ($1, 'Tenant Discovery 2') ON CONFLICT DO NOTHING", org2)
	_, _ = db.Exec("INSERT INTO organization_members (organization_id, user_id, role, status) VALUES ($1, $2, 'Admin', 'ACTIVE') ON CONFLICT DO NOTHING", org1, targetUser)
	_, _ = db.Exec("INSERT INTO organization_members (organization_id, user_id, role, status) VALUES ($1, $2, 'Developer', 'ACTIVE') ON CONFLICT DO NOTHING", org2, targetUser)
	_, _ = db.Exec("INSERT INTO organization_members (organization_id, user_id, role, status) VALUES ($1, $2, 'Viewer', 'ACTIVE') ON CONFLICT DO NOTHING", org1, otherUser)

	// 1. Test production Go helper storage.DiscoverUserMemberships using runtimePool without tenant context
	memberships, err := storage.DiscoverUserMemberships(ctx, runtimePool, targetUser)
	if err != nil {
		t.Fatalf("storage.DiscoverUserMemberships failed: %v", err)
	}
	if len(memberships) != 2 {
		t.Fatalf("expected exactly 2 memberships for target user, got %d", len(memberships))
	}
	for _, m := range memberships {
		if m.OrganizationID != org1 && m.OrganizationID != org2 {
			t.Fatalf("unexpected organization in discovery: %v", m.OrganizationID)
		}
		if m.Status != "ACTIVE" {
			t.Fatalf("unexpected non-active status: %v", m.Status)
		}
	}

	// 2. Test production Go helper storage.EnumerateTenantsForScheduler using dedicated systemPool
	systemPool, err := pgxpool.New(ctx, systemURL)
	if err != nil {
		t.Fatalf("failed to connect as deadbolt_system: %v", err)
	}
	defer systemPool.Close()

	tenants, err := storage.EnumerateTenantsForScheduler(ctx, systemPool)
	if err != nil {
		t.Fatalf("storage.EnumerateTenantsForScheduler failed as deadbolt_system: %v", err)
	}
	if len(tenants) < 2 {
		t.Fatalf("expected at least 2 tenants enumerated, got %d", len(tenants))
	}

	// 3. Verify deadbolt_system CANNOT query tenant tables directly
	var forbiddenCount int
	err = systemPool.QueryRow(ctx, "SELECT count(*) FROM public.runs").Scan(&forbiddenCount)
	if err == nil {
		t.Fatalf("SECURITY VIOLATION: deadbolt_system was able to select from public.runs!")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("expected permission denied error for deadbolt_system on public.runs, got: %v", err)
	}

	err = systemPool.QueryRow(ctx, "SELECT count(*) FROM public.organizations").Scan(&forbiddenCount)
	if err == nil {
		t.Fatalf("SECURITY VIOLATION: deadbolt_system was able to select from public.organizations!")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("expected permission denied error for deadbolt_system on public.organizations, got: %v", err)
	}

	// 4. Verify deadbolt_runtime CANNOT execute scheduler-only discovery function
	_, err = storage.EnumerateTenantsForScheduler(ctx, runtimePool)
	if err == nil {
		t.Fatalf("SECURITY VIOLATION: deadbolt_runtime was able to execute EnumerateTenantsForScheduler!")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("expected permission denied error for deadbolt_runtime on enumerate_scheduler_tenants, got: %v", err)
	}

	// 5. Verify deadbolt_system CANNOT execute user membership discovery function
	_, err = storage.DiscoverUserMemberships(ctx, systemPool, targetUser)
	if err == nil {
		t.Fatalf("SECURITY VIOLATION: deadbolt_system was able to execute DiscoverUserMemberships!")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("expected permission denied error for deadbolt_system on discover_user_memberships, got: %v", err)
	}
}

// TestIntermediateMigrationStepUpgradeSafety verifies forward migration safety.
// Note: Deadbolt M0 establishes the initial repository database baseline; no prior production
// release exists. To verify forward migration-chain safety and non-destructive schema evolution,
// this test verifies that intermediate migration states (e.g. schema version 2) can hold populated
// tenant data and subsequently be upgraded to latest version 4 with 100% data preservation and RLS integrity.
func TestIntermediateMigrationStepUpgradeSafety(t *testing.T) {
	db, runtimePool, _ := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	ctx := context.Background()
	runner := migrator.NewRunner(db, "../../migrations")

	// 1. Roll back to version 2
	if err := runner.DownTo(ctx, 2); err != nil {
		t.Fatalf("failed to down-to 2: %v", err)
	}

	v, err := runner.Version(ctx)
	if err != nil || v != 2 {
		t.Fatalf("expected version 2 after down-to, got %d (err: %v)", v, err)
	}

	// 2. Insert data into version 2 schema (organizations, projects, environments, deployments, workflow_definitions)
	upgradeOrgID := "90000000-0000-0000-0000-000000000001"
	upgradeProjectID := "91000000-0000-0000-0000-000000000001"
	upgradeEnvID := "92000000-0000-0000-0000-000000000001"
	upgradeDeployID := "93000000-0000-0000-0000-000000000001"

	inserts := []struct {
		query string
		args  []any
	}{
		{"INSERT INTO organizations (id, name) VALUES ($1, 'Upgrade Org') ON CONFLICT DO NOTHING", []any{upgradeOrgID}},
		{"INSERT INTO projects (id, organization_id, name) VALUES ($1, $2, 'Upgrade Project') ON CONFLICT DO NOTHING", []any{upgradeProjectID, upgradeOrgID}},
		{"INSERT INTO environments (id, organization_id, project_id, name) VALUES ($1, $2, $3, 'production') ON CONFLICT DO NOTHING", []any{upgradeEnvID, upgradeOrgID, upgradeProjectID}},
		{"INSERT INTO deployments (id, organization_id, environment_id, manifest_hash, bundle_digest, manifest, protocol_version, runtime_version) VALUES ($1, $2, $3, 'hash_up', 'digest_up', '{}', 1, '1.0') ON CONFLICT DO NOTHING", []any{upgradeDeployID, upgradeOrgID, upgradeEnvID}},
		{"INSERT INTO workflow_definitions (id, organization_id, deployment_id, name, input_schema, output_schema, nodes) VALUES ('94000000-0000-0000-0000-000000000001', $1, $2, 'upgrade-wf', '{}', '{}', '{}') ON CONFLICT DO NOTHING", []any{upgradeOrgID, upgradeDeployID}},
	}

	for i, ins := range inserts {
		if _, err := db.Exec(ins.query, ins.args...); err != nil {
			t.Fatalf("failed to insert data into version 2 schema (stmt %d): %v", i, err)
		}
	}

	// 3. Migrate forward to latest version 4 as deadbolt_migrator
	if err := runner.Up(ctx); err != nil {
		t.Fatalf("failed to migrate from version 2 to latest: %v", err)
	}

	latestV, err := runner.Version(ctx)
	if err != nil || latestV != 4 {
		t.Fatalf("expected version 4 after Up, got %d (err: %v)", latestV, err)
	}

	// 4. Verify pre-existing data is preserved intact
	var orgName string
	err = db.QueryRow("SELECT name FROM organizations WHERE id = $1", upgradeOrgID).Scan(&orgName)
	if err != nil || orgName != "Upgrade Org" {
		t.Fatalf("pre-existing org data corrupted after upgrade: got %s, err: %v", orgName, err)
	}

	var deployHash string
	err = db.QueryRow("SELECT manifest_hash FROM deployments WHERE id = $1", upgradeDeployID).Scan(&deployHash)
	if err != nil || deployHash != "hash_up" {
		t.Fatalf("pre-existing deployment data corrupted after upgrade: got %s, err: %v", deployHash, err)
	}

	// 5. Verify runtime RLS functions properly on pre-existing and newly created tables
	sPool := storage.NewPool(runtimePool)
	err = sPool.WithTenantTx(ctx, upgradeOrgID, func(ctx context.Context, tx storage.Tx) error {
		// Read pre-existing data under RLS
		var name string
		if err := tx.QueryRow(ctx, "SELECT name FROM organizations WHERE id = $1", upgradeOrgID).Scan(&name); err != nil {
			return fmt.Errorf("read org under RLS failed: %w", err)
		}

		// Insert into newly created table in migration 3 (runs) referencing pre-existing deployment
		runID := "95000000-0000-0000-0000-000000000001"
		_, err := tx.Exec(ctx, `
			INSERT INTO runs (id, organization_id, environment_id, deployment_id, workflow_name)
			VALUES ($1, $2, $3, $4, 'upgrade-wf')
		`, runID, upgradeOrgID, upgradeEnvID, upgradeDeployID)
		if err != nil {
			return fmt.Errorf("insert run into upgraded schema failed: %w", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RLS validation on upgraded database failed: %v", err)
	}
}
