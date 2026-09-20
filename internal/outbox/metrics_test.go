package outbox

import (
	"context"
	"regexp"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func metricValue(t *testing.T, text, name string) int64 {
	t.Helper()
	re := regexp.MustCompile("(?m)^" + name + ` ([0-9]+)$`)
	m := re.FindStringSubmatch(text)
	if len(m) != 2 {
		t.Fatalf("metric %s not found in output:\n%s", name, text)
	}
	value, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		t.Fatalf("parse metric %s: %v", name, err)
	}
	return value
}

func TestMetricsDispatcherAndSchedulerHeartbeatsAreIndependent(t *testing.T) {
	m := NewMetrics(nil)
	now := time.Now()
	m.lastDispatchTime.Store(now.Add(-2 * time.Minute).Unix())
	var scheduler atomic.Int64
	scheduler.Store(now.Add(-3 * time.Minute).UnixNano())
	m.SetSchedulerHeartbeat(&scheduler)

	before := m.FormatPrometheus()
	dispatchBefore := metricValue(t, before, "deadbolt_outbox_dispatcher_loop_lag_seconds")
	schedulerBefore := metricValue(t, before, "deadbolt_scheduler_loop_lag_seconds")
	if schedulerBefore <= dispatchBefore {
		t.Fatalf("scheduler heartbeat should be independently stale: scheduler=%d dispatcher=%d", schedulerBefore, dispatchBefore)
	}

	// Exercise DispatchBatch's empty successful-claim path directly. The hook
	// stands in for the database claim function while preserving public behavior.
	d := NewDispatcher(nil, nil, DefaultConfig(), m, nil)
	d.claimRecords = func(context.Context, int) ([]OutboxEventRecord, error) { return nil, nil }
	if published, err := d.DispatchBatch(context.Background(), 1); err != nil || published != 0 {
		t.Fatalf("empty dispatch sweep returned published=%d err=%v", published, err)
	}
	after := m.FormatPrometheus()
	dispatchAfter := metricValue(t, after, "deadbolt_outbox_dispatcher_loop_lag_seconds")
	schedulerAfter := metricValue(t, after, "deadbolt_scheduler_loop_lag_seconds")
	if dispatchAfter >= dispatchBefore {
		t.Fatalf("dispatcher heartbeat did not refresh: before=%d after=%d", dispatchBefore, dispatchAfter)
	}
	if schedulerAfter < schedulerBefore-1 {
		t.Fatalf("scheduler heartbeat changed when only dispatcher swept: before=%d after=%d", schedulerBefore, schedulerAfter)
	}
}

func TestMetricsExposeSlowSweepInFlightSeparatelyFromFastLoopLag(t *testing.T) {
	m := NewMetrics(nil)
	var inFlight atomic.Bool
	inFlight.Store(true)
	m.SetSchedulerSlowSweepStatus(inFlight.Load)
	if got := metricValue(t, m.FormatPrometheus(), "deadbolt_scheduler_slow_sweep_in_flight"); got != 1 {
		t.Fatalf("slow sweep status = %d, want 1", got)
	}
	inFlight.Store(false)
	if got := metricValue(t, m.FormatPrometheus(), "deadbolt_scheduler_slow_sweep_in_flight"); got != 0 {
		t.Fatalf("slow sweep status = %d, want 0", got)
	}
}
