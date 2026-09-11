package migrator

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

// MigrationAdvisoryLockID is a constant 64-bit integer used for single-migrator serialization.
// See Blueprint §26.3: "A single migrator uses an advisory lock; app runtime has no DDL permission."
const MigrationAdvisoryLockID int64 = 7142893

type Runner struct {
	db            *sql.DB
	migrationsDir string
	lockConn      *sql.Conn
	mu            sync.Mutex
}

func NewRunner(db *sql.DB, migrationsDir string) *Runner {
	return &Runner{
		db:            db,
		migrationsDir: migrationsDir,
	}
}

// AcquireAdvisoryLock acquires an exclusive session-level advisory lock on PostgreSQL
// pinned to a dedicated physical connection.
func (r *Runner) AcquireAdvisoryLock(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.lockConn != nil {
		return fmt.Errorf("migration advisory lock already held by this runner")
	}

	conn, err := r.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to obtain dedicated connection for advisory lock: %w", err)
	}

	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", MigrationAdvisoryLockID); err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to acquire migration advisory lock %d: %w", MigrationAdvisoryLockID, err)
	}

	r.lockConn = conn
	return nil
}

// ReleaseAdvisoryLock releases the exclusive session-level advisory lock on the pinned connection.
func (r *Runner) ReleaseAdvisoryLock(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.lockConn == nil {
		return nil
	}

	defer func() {
		_ = r.lockConn.Close()
		r.lockConn = nil
	}()

	if _, err := r.lockConn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", MigrationAdvisoryLockID); err != nil {
		return fmt.Errorf("failed to release migration advisory lock %d: %w", MigrationAdvisoryLockID, err)
	}

	return nil
}

// Up runs all pending migrations under the pinned session advisory lock.
func (r *Runner) Up(ctx context.Context) error {
	if err := r.AcquireAdvisoryLock(ctx); err != nil {
		return err
	}
	defer func() {
		_ = r.ReleaseAdvisoryLock(context.Background())
	}()

	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("failed to set goose dialect: %w", err)
	}

	if _, err := os.Stat(r.migrationsDir); err != nil {
		return fmt.Errorf("migrations directory not found at %s: %w", r.migrationsDir, err)
	}

	if err := goose.UpContext(ctx, r.db, r.migrationsDir); err != nil {
		return fmt.Errorf("goose up failed: %w", err)
	}

	return nil
}

// UpTo runs migrations up to a specific version under the pinned session advisory lock.
func (r *Runner) UpTo(ctx context.Context, version int64) error {
	if err := r.AcquireAdvisoryLock(ctx); err != nil {
		return err
	}
	defer func() {
		_ = r.ReleaseAdvisoryLock(context.Background())
	}()

	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("failed to set goose dialect: %w", err)
	}

	if _, err := os.Stat(r.migrationsDir); err != nil {
		return fmt.Errorf("migrations directory not found at %s: %w", r.migrationsDir, err)
	}

	if err := goose.UpToContext(ctx, r.db, r.migrationsDir, version); err != nil {
		return fmt.Errorf("goose up-to %d failed: %w", version, err)
	}

	return nil
}

// DownTo rolls back migrations down to a specific version under the pinned session advisory lock.
func (r *Runner) DownTo(ctx context.Context, version int64) error {
	if err := r.AcquireAdvisoryLock(ctx); err != nil {
		return err
	}
	defer func() {
		_ = r.ReleaseAdvisoryLock(context.Background())
	}()

	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("failed to set goose dialect: %w", err)
	}

	if err := goose.DownToContext(ctx, r.db, r.migrationsDir, version); err != nil {
		return fmt.Errorf("goose down-to %d failed: %w", version, err)
	}

	return nil
}

// Version returns the current database migration version.
func (r *Runner) Version(ctx context.Context) (int64, error) {
	if err := goose.SetDialect("postgres"); err != nil {
		return 0, fmt.Errorf("failed to set goose dialect: %w", err)
	}
	return goose.GetDBVersion(r.db)
}
