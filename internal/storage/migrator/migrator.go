package migrator

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

// MigrationAdvisoryLockID is a constant 64-bit integer used for single-migrator serialization.
// See Blueprint §26.3: "A single migrator uses an advisory lock; app runtime has no DDL permission."
const MigrationAdvisoryLockID int64 = 7142893

type Runner struct {
	db            *sql.DB
	migrationsDir string
}

func NewRunner(db *sql.DB, migrationsDir string) *Runner {
	return &Runner{
		db:            db,
		migrationsDir: migrationsDir,
	}
}

// AcquireAdvisoryLock acquires an exclusive session-level advisory lock on PostgreSQL.
func (r *Runner) AcquireAdvisoryLock(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, "SELECT pg_advisory_lock($1)", MigrationAdvisoryLockID)
	if err != nil {
		return fmt.Errorf("failed to acquire migration advisory lock %d: %w", MigrationAdvisoryLockID, err)
	}
	return nil
}

// ReleaseAdvisoryLock releases the exclusive session-level advisory lock.
func (r *Runner) ReleaseAdvisoryLock(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", MigrationAdvisoryLockID)
	if err != nil {
		return fmt.Errorf("failed to release migration advisory lock %d: %w", MigrationAdvisoryLockID, err)
	}
	return nil
}

// Up runs all pending migrations under the advisory lock.
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

// Version returns the current database migration version.
func (r *Runner) Version(ctx context.Context) (int64, error) {
	if err := goose.SetDialect("postgres"); err != nil {
		return 0, fmt.Errorf("failed to set goose dialect: %w", err)
	}
	return goose.GetDBVersion(r.db)
}
