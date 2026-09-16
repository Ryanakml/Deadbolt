package outbox

import (
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

	// DispatchBatch records this same sweep after an empty successful claim, so
	// an idle outbox does not create false dispatcher lag.
	m.RecordSweep()
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
