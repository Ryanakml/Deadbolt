package storage

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Tx is an alias for pgx.Tx
type Tx = pgx.Tx

// Pool wraps pgxpool.Pool with tenant isolation and execution helpers.
type Pool struct {
	*pgxpool.Pool
}

func NewPool(pool *pgxpool.Pool) *Pool {
	return &Pool{Pool: pool}
}

// WithTenantTx executes fn within a transaction where the tenant context is set using SET LOCAL.
// SET LOCAL ensures the setting applies only to the current transaction.
// When the transaction completes (commit or rollback), the connection returned to the pool
// does not retain the tenant context, preventing connection-pool context leakage (Failure Mode F-27).
func (p *Pool) WithTenantTx(ctx context.Context, orgID string, fn func(ctx context.Context, tx pgx.Tx) error) error {
	tx, err := p.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	// Set tenant context locally for this transaction using set_config(..., is_local=true)
	_, err = tx.Exec(ctx, "SELECT set_config('app.current_organization_id', $1, true)", orgID)
	if err != nil {
		return fmt.Errorf("failed to set tenant context: %w", err)
	}

	if err := fn(ctx, tx); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
