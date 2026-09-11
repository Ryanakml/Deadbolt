package integration_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/storage/migrator"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	migratorConnString = "postgres://ryanakmalpasya@localhost:5432/deadbolt_test?sslmode=disable"
	runtimeConnString  = "postgres://deadbolt_runtime@localhost:5432/deadbolt_test?sslmode=disable"
)

func setupTestDB(t *testing.T) (*sql.DB, *pgxpool.Pool) {
	t.Helper()

	db, err := sql.Open("pgx", migratorConnString)
	if err != nil {
		t.Skipf("PostgreSQL deadbolt_test not available: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("PostgreSQL deadbolt_test ping failed: %v", err)
	}

	migrationsDir, err := filepath.Abs("../../migrations")
	if err != nil {
		t.Fatalf("failed to resolve migrations dir: %v", err)
	}

	// 1. Run migrations using migrator runner under advisory lock
	runner := migrator.NewRunner(db, migrationsDir)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := runner.Up(ctx); err != nil {
		t.Fatalf("migration failed: %v", err)
	}

	// 2. Ensure deadbolt_runtime role exists and has DML-only grants
	_, err = db.Exec(`
		DO $$
		BEGIN
			IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'deadbolt_runtime') THEN
				CREATE ROLE deadbolt_runtime WITH LOGIN;
			END IF;
		END $$;
		GRANT CONNECT ON DATABASE deadbolt_test TO deadbolt_runtime;
		GRANT USAGE ON SCHEMA public, app TO deadbolt_runtime;
		GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO deadbolt_runtime;
		ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO deadbolt_runtime;
		GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA app TO deadbolt_runtime;
		ALTER DEFAULT PRIVILEGES IN SCHEMA app GRANT EXECUTE ON FUNCTIONS TO deadbolt_runtime;
	`)
	if err != nil {
		t.Fatalf("failed to grant deadbolt_runtime privileges: %v", err)
	}

	// 3. Connect runtime pool strictly as deadbolt_runtime (no DDL, no BYPASSRLS)
	runtimePool, err := pgxpool.New(context.Background(), runtimeConnString)
	if err != nil {
		t.Fatalf("failed to create runtime pgxpool: %v", err)
	}

	return db, runtimePool
}

// TestMigrationAdvisoryLock verifies that migration runner acquires and releases the advisory lock.
func TestMigrationAdvisoryLock(t *testing.T) {
	db, runtimePool := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	runner := migrator.NewRunner(db, "../../migrations")
	ctx := context.Background()

	// Verify advisory lock acquisition
	if err := runner.AcquireAdvisoryLock(ctx); err != nil {
		t.Fatalf("AcquireAdvisoryLock failed: %v", err)
	}

	if err := runner.ReleaseAdvisoryLock(ctx); err != nil {
		t.Fatalf("ReleaseAdvisoryLock failed: %v", err)
	}
}

// TestRuntimeNoDDLPrivileges verifies that deadbolt_runtime cannot perform DDL operations.
func TestRuntimeNoDDLPrivileges(t *testing.T) {
	db, runtimePool := setupTestDB(t)
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
	db, runtimePool := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	ctx := context.Background()
	sPool := storage.NewPool(runtimePool)

	testOrgID := "00000000-0000-0000-0000-000000000001"
	// Insert an organization inside tenant context
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
	db, runtimePool := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	ctx := context.Background()
	sPool := storage.NewPool(runtimePool)

	orgA := "00000000-0000-0000-0000-00000000000a"
	orgB := "00000000-0000-0000-0000-00000000000b"

	// Insert Org A
	_ = sPool.WithTenantTx(ctx, orgA, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO organizations (id, name) VALUES ($1, 'Org A') ON CONFLICT (id) DO NOTHING", orgA)
		return err
	})

	// Insert Org B
	_ = sPool.WithTenantTx(ctx, orgB, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO organizations (id, name) VALUES ($1, 'Org B') ON CONFLICT (id) DO NOTHING", orgB)
		return err
	})

	// Acquire connection from pool, execute query as Org A
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

	// Transaction 2 on the SAME CONNECTION without set_config: Must NOT see Org A!
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

// TestCompositeForeignKeys verifies that foreign-ID substitution across organizations fails.
func TestCompositeForeignKeys(t *testing.T) {
	db, runtimePool := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	ctx := context.Background()
	sPool := storage.NewPool(runtimePool)

	orgA := "00000000-0000-0000-0000-000000000001"
	orgB := "00000000-0000-0000-0000-000000000002"
	projectA := "11111111-1111-1111-1111-111111111111"

	// Create project under Org A
	err := sPool.WithTenantTx(ctx, orgA, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO organizations (id, name) VALUES ($1, 'Org A') ON CONFLICT DO NOTHING", orgA)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, "INSERT INTO projects (id, organization_id, name) VALUES ($1, $2, 'Project A') ON CONFLICT DO NOTHING", projectA, orgA)
		return err
	})
	if err != nil {
		t.Fatalf("failed to create Org A project: %v", err)
	}

	// Attempt to create an environment under Org B referencing Project A (belonging to Org A)
	// Must fail due to composite foreign key: FOREIGN KEY (organization_id, project_id) REFERENCES projects(organization_id, id)
	err = sPool.WithTenantTx(ctx, orgB, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO organizations (id, name) VALUES ($1, 'Org B') ON CONFLICT DO NOTHING", orgB)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, "INSERT INTO environments (id, organization_id, project_id, name) VALUES ($1, $2, $3, 'staging')",
			"22222222-2222-2222-2222-222222222222", orgB, projectA)
		return err
	})

	if err == nil {
		t.Fatalf("COMPOSITE FOREIGN KEY VIOLATION: expected error when referencing Project A from Org B, but insert succeeded!")
	}
}
