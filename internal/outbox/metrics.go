package outbox

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Metrics holds the outbox and scheduler observability counters and gauges (Blueprint §25.2).
type Metrics struct {
	pool             *pgxpool.Pool
	publishedTotal   atomic.Int64
	failuresTotal    atomic.Int64
	lastDispatchTime atomic.Int64
	schedulerTime    *atomic.Int64
	taskLogDrops     func() int64
	oldestPendingAge atomic.Int64 // in seconds
	pendingCount     atomic.Int64
}

// NewMetrics creates a new Metrics collector instance.
func NewMetrics(pool *pgxpool.Pool) *Metrics {
	m := &Metrics{pool: pool}
	m.lastDispatchTime.Store(time.Now().Unix())
	return m
}

// SetSchedulerHeartbeat connects the metrics exposition to the authoritative
// reconciler heartbeat. It is intentionally separate from dispatcher sweeps.
func (m *Metrics) SetSchedulerHeartbeat(heartbeat *atomic.Int64) {
	if m != nil {
		m.schedulerTime = heartbeat
	}
}

// SetTaskLogDroppedCounter attaches the worker-owned counter to this existing
// /metrics exposition. Keeping a callback avoids a second metrics subsystem.
func (m *Metrics) SetTaskLogDroppedCounter(counter func() int64) {
	if m != nil {
		m.taskLogDrops = counter
	}
}

// IncPublished increments the successful outbox publication counter.
func (m *Metrics) IncPublished() {
	if m != nil {
		m.publishedTotal.Add(1)
	}
}

// IncFailures increments the outbox publication failure counter.
func (m *Metrics) IncFailures() {
	if m != nil {
		m.failuresTotal.Add(1)
	}
}

// RecordSweep updates the last dispatcher sweep timestamp.
func (m *Metrics) RecordSweep() {
	if m != nil {
		m.lastDispatchTime.Store(time.Now().Unix())
	}
}

// PublishedCount returns the total number of published events.
func (m *Metrics) PublishedCount() int64 {
	if m == nil {
		return 0
	}
	return m.publishedTotal.Load()
}

// FailuresCount returns the total number of publish failures.
func (m *Metrics) FailuresCount() int64 {
	if m == nil {
		return 0
	}
	return m.failuresTotal.Load()
}

// OutboxAgeSeconds returns the current age of the oldest unpublished event in seconds.
func (m *Metrics) OutboxAgeSeconds() int64 {
	if m == nil {
		return 0
	}
	return m.oldestPendingAge.Load()
}

// PendingCount returns the current count of pending outbox events.
func (m *Metrics) PendingCount() int64 {
	if m == nil {
		return 0
	}
	return m.pendingCount.Load()
}

// UpdateDatabaseGauges queries PostgreSQL for the current pending outbox age and pending count.
func (m *Metrics) UpdateDatabaseGauges(ctx context.Context) error {
	if m == nil || m.pool == nil {
		return nil
	}
	var (
		count     int64
		maxAgeSec float64
	)
	query := `SELECT pending_count, oldest_age_seconds FROM app.get_outbox_metrics()`
	if err := m.pool.QueryRow(ctx, query).Scan(&count, &maxAgeSec); err != nil {
		return err
	}
	m.pendingCount.Store(count)
	if maxAgeSec > 0 {
		m.oldestPendingAge.Store(int64(maxAgeSec))
	} else {
		m.oldestPendingAge.Store(0)
	}
	return nil
}

// FormatPrometheus writes standard Prometheus text format metrics for scrape endpoints.
func (m *Metrics) FormatPrometheus() string {
	if m == nil {
		return ""
	}
	now := time.Now().Unix()
	dispatchLag := now - m.lastDispatchTime.Load()
	if dispatchLag < 0 {
		dispatchLag = 0
	}
	schedulerLag := dispatchLag
	if m.schedulerTime != nil {
		schedulerLag = now - m.schedulerTime.Load()/1e9
		if schedulerLag < 0 {
			schedulerLag = 0
		}
	}

	var taskLogDrops int64
	if m.taskLogDrops != nil {
		taskLogDrops = m.taskLogDrops()
	}

	return fmt.Sprintf(`# HELP deadbolt_outbox_published_total Total number of outbox events published to NATS JetStream.
# TYPE deadbolt_outbox_published_total counter
deadbolt_outbox_published_total %d

# HELP deadbolt_outbox_publish_failures_total Total number of failed outbox publication attempts.
# TYPE deadbolt_outbox_publish_failures_total counter
deadbolt_outbox_publish_failures_total %d

# HELP deadbolt_outbox_age_seconds Current age in seconds of the oldest unpublished outbox event.
# TYPE deadbolt_outbox_age_seconds gauge
deadbolt_outbox_age_seconds %d

# HELP deadbolt_outbox_pending_count Current number of unpublished outbox events waiting for dispatch.
# TYPE deadbolt_outbox_pending_count gauge
deadbolt_outbox_pending_count %d

# HELP deadbolt_outbox_dispatcher_loop_lag_seconds Seconds elapsed since the last outbox dispatcher sweep.
# TYPE deadbolt_outbox_dispatcher_loop_lag_seconds gauge
deadbolt_outbox_dispatcher_loop_lag_seconds %d

# HELP deadbolt_scheduler_loop_lag_seconds Seconds elapsed since the last authoritative scheduler sweep.
# TYPE deadbolt_scheduler_loop_lag_seconds gauge
deadbolt_scheduler_loop_lag_seconds %d

# HELP deadbolt_task_logs_dropped_total Total number of newly dropped task diagnostic log records.
# TYPE deadbolt_task_logs_dropped_total counter
deadbolt_task_logs_dropped_total %d
`,
		m.publishedTotal.Load(),
		m.failuresTotal.Load(),
		m.oldestPendingAge.Load(),
		m.pendingCount.Load(),
		dispatchLag,
		schedulerLag,
		taskLogDrops,
	)
}

// ServeHTTP implements http.Handler to serve /metrics.
func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	_ = m.UpdateDatabaseGauges(ctx)

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(m.FormatPrometheus()))
}
