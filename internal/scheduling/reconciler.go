package scheduling

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Reconciler executes periodic authoritative reconciliation sweeps across active tenants (Blueprint §24.3 & §25.2).
// The heartbeat is updated ONLY when a reconciliation sweep successfully queries the database.
type Reconciler struct {
	pool          *pgxpool.Pool
	sweepInterval time.Duration
	ticker        *atomic.Int64
	logger        *log.Logger
}

// NewReconciler creates a scheduler reconciler wired to the database pool.
func NewReconciler(pool *pgxpool.Pool, sweepInterval time.Duration, logger *log.Logger) *Reconciler {
	if logger == nil {
		logger = log.Default()
	}
	return &Reconciler{
		pool:          pool,
		sweepInterval: sweepInterval,
		ticker:        &atomic.Int64{},
		logger:        logger,
	}
}

// Ticker returns the atomic heartbeat ticker updated exclusively upon successful sweep iterations.
func (r *Reconciler) Ticker() *atomic.Int64 {
	return r.ticker
}

// Sweep executes an authoritative sweep iteration against the database.
// The heartbeat is updated ONLY when the query succeeds.
func (r *Reconciler) Sweep(ctx context.Context) error {
	if r.pool == nil {
		return fmt.Errorf("scheduler sweep failed: database connection pool is nil")
	}

	tenants, err := storage.EnumerateTenantsForScheduler(ctx, r.pool)
	if err != nil {
		return fmt.Errorf("scheduler tenant enumeration failed: %w", err)
	}

	// Update heartbeat only after successful database enumeration
	r.ticker.Store(time.Now().UnixNano())
	r.logger.Printf("[SCHEDULER] Reconciliation sweep successful across %d tenant(s)", len(tenants))
	return nil
}

// Run executes the continuous background reconciliation loop until context cancellation.
func (r *Reconciler) Run(ctx context.Context) error {
	if r.pool == nil {
		return fmt.Errorf("cannot start reconciler: database pool is nil")
	}

	// Immediate first sweep
	if err := r.Sweep(ctx); err != nil {
		r.logger.Printf("[SCHEDULER] Warning: Initial sweep failed: %v", err)
	}

	ticker := time.NewTicker(r.sweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := r.Sweep(ctx); err != nil {
				r.logger.Printf("[SCHEDULER] Error: Sweep iteration failed: %v", err)
				// Heartbeat intentionally NOT updated on failure
			}
		}
	}
}
