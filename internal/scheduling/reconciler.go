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
	pool            *pgxpool.Pool
	sweepInterval   time.Duration
	ticker          *atomic.Int64
	wakeup          chan struct{}
	logger          *log.Logger
	tenantSweep     func(context.Context, string) error
	slowTenantSweep func(context.Context, string) error
}

// SetTenantSweep adds bounded per-tenant maintenance to the existing
// authoritative reconciliation pass. A hook must complete one small unit of
// work; it is called once per discovered tenant and may safely be retried.
func (r *Reconciler) SetTenantSweep(hook func(context.Context, string) error) {
	if r != nil {
		r.tenantSweep = hook
	}
}

// SetSlowTenantSweep registers bounded work that is intentionally run on the
// five-second reconciliation cadence. Keeping this separate from lease/deadline
// recovery prevents log retention and graph repair from delaying ownership
// fencing.
func (r *Reconciler) SetSlowTenantSweep(hook func(context.Context, string) error) {
	if r != nil {
		r.slowTenantSweep = hook
	}
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
		wakeup:        make(chan struct{}, 1),
		logger:        logger,
	}
}

// Wake requests an early authoritative sweep. Wake-ups are deliberately
// coalesced and lossy: PostgreSQL remains authoritative and the periodic sweep
// is still the safety net if a hint or wake signal is missed.
func (r *Reconciler) Wake() {
	if r == nil {
		return
	}
	select {
	case r.wakeup <- struct{}{}:
	default:
	}
}

// Ticker returns the atomic heartbeat ticker updated exclusively upon successful sweep iterations.
func (r *Reconciler) Ticker() *atomic.Int64 {
	return r.ticker
}

// Sweep executes an authoritative sweep iteration against the database.
// The heartbeat is updated ONLY when the query succeeds.
func (r *Reconciler) Sweep(ctx context.Context) error {
	return r.sweep(ctx, r.tenantSweep)
}

// SlowSweep performs the bounded five-second graph/outbox maintenance pass.
func (r *Reconciler) SlowSweep(ctx context.Context) error {
	return r.sweep(ctx, r.slowTenantSweep)
}

func (r *Reconciler) sweep(ctx context.Context, hook func(context.Context, string) error) error {
	if r.pool == nil {
		return fmt.Errorf("scheduler sweep failed: database connection pool is nil")
	}

	tenants, err := storage.EnumerateTenantsForScheduler(ctx, r.pool)
	if err != nil {
		return fmt.Errorf("scheduler tenant enumeration failed: %w", err)
	}
	if hook != nil {
		for _, orgID := range tenants {
			if err := hook(ctx, orgID); err != nil {
				return fmt.Errorf("scheduler tenant maintenance for %s failed: %w", orgID, err)
			}
		}
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

	interval := r.sweepInterval
	if r.ticker.Load() == 0 {
		interval = 500 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	slowTicker := time.NewTicker(5 * time.Second)
	defer slowTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.wakeup:
			if err := r.Sweep(ctx); err != nil {
				r.logger.Printf("[SCHEDULER] Wake-up sweep failed: %v", err)
			}
		case <-ticker.C:
			if err := r.Sweep(ctx); err != nil {
				r.logger.Printf("[SCHEDULER] Error: Sweep iteration failed: %v", err)
				// Heartbeat intentionally NOT updated on failure
			} else if interval != r.sweepInterval {
				// Once initialized, switch ticker to standard sweepInterval
				interval = r.sweepInterval
				ticker.Reset(interval)
			}
		case <-slowTicker.C:
			if err := r.SlowSweep(ctx); err != nil {
				r.logger.Printf("[SCHEDULER] Slow sweep failed: %v", err)
			}
		}
	}
}
