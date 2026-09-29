package integration_test

// Occurrence history HTTP surface (Issue #35, Blueprint §17, §20).
//
// The engine decides slots; this is how the dashboard reads what it
// decided. Tests drive the real mux with real API keys: history is scoped
// to the schedule's environment and tenant, paged newest-first, and the
// skipped reasons the UI renders come from the same rows the engine wrote.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/controlplane"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
)

type occurrenceHTTPFixture struct {
	*occurrenceFixture
	server    *httptest.Server
	operator  string
	developer string
}

func setupOccurrenceHTTP(t *testing.T) *occurrenceHTTPFixture {
	t.Helper()
	f := setupOccurrenceFixture(t)
	server := httptest.NewServer(controlplane.BuildMux(f.tenant.authCfg, f.tenant.runtimePool, nil, nil))
	operator := bootstrapTestKey(t, f.tenant.service, f.orgID, f.envID, []string{tenant.CapSchedulesWrite, tenant.CapRunsRead}).PlaintextKey
	developer := bootstrapTestKey(t, f.tenant.service, f.orgID, f.envID, []string{tenant.CapRunsCreate, tenant.CapRunsRead}).PlaintextKey
	t.Cleanup(func() {
		server.Close()
		f.tenant.cleanup()
	})
	return &occurrenceHTTPFixture{occurrenceFixture: f, server: server, operator: operator, developer: developer}
}

func (f *occurrenceHTTPFixture) get(t *testing.T, path, key string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest("GET", f.server.URL+path, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("X-Organization-ID", f.orgID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// makeDueAgo forces the pending slot to a past offset so the next
// evaluation has genuinely new work.
func (f *occurrenceHTTPFixture) makeDueAgo(t *testing.T, scheduleID, ago string) {
	t.Helper()
	if err := f.tenant.pool.WithTenantTx(context.Background(), f.orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE schedules SET next_due_at=clock_timestamp()-$1::interval
			WHERE id=$2::uuid AND organization_id=$3::uuid`, ago, scheduleID, f.orgID)
		return err
	}); err != nil {
		t.Fatalf("make due: %v", err)
	}
}

func TestOccurrenceHTTPListsStartedAndSkippedReasons(t *testing.T) {
	f := setupOccurrenceHTTP(t)
	ctx := context.Background()

	// Per-minute cron so the forced second slot is genuinely distinct. With
	// a daily cron, travelling backwards coalesces back to the handled slot
	// (misfire policy, correctly) instead of exercising overlap.
	s := f.schedule(t, "* * * * *")
	f.makeDue(t, s.ID)
	if _, err := f.engine.EvaluateSchedule(ctx, f.orgID, f.envID, s.ID); err != nil {
		t.Fatalf("first evaluate: %v", err)
	}
	// The first run is still nonterminal, so the next slot skips by policy.
	// Both rows must be visible with the exact reasons the UI renders.
	f.makeDueAgo(t, s.ID, "90 seconds")
	if _, err := f.engine.EvaluateSchedule(ctx, f.orgID, f.envID, s.ID); err != nil {
		t.Fatalf("second evaluate: %v", err)
	}

	status, body := f.get(t, "/v1/schedules/"+s.ID+"/occurrences?environment="+f.envID, f.operator)
	if status != http.StatusOK {
		t.Fatalf("occurrences status = %d, body = %v", status, body)
	}
	items, _ := body["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("expected 2 occurrences, got %v", body)
	}
	// History is newest slot first. The forced second slot sits 90s in the
	// past, so it sorts after the first: one STARTED row linked to its run
	// and one SKIPPED_OVERLAP row with no run, in that wire order.
	first, _ := items[0].(map[string]any)
	second, _ := items[1].(map[string]any)
	if first["status"] != "STARTED" {
		t.Fatalf("newest occurrence = %v, want STARTED", first)
	}
	runID, _ := first["runId"].(string)
	if runID == "" {
		t.Fatalf("started occurrence must link its run: %v", first)
	}
	if second["status"] != "SKIPPED" || second["skippedReason"] != "SKIPPED_OVERLAP" {
		t.Fatalf("older occurrence = %v, want SKIPPED/SKIPPED_OVERLAP", second)
	}
	if second["runId"] != nil {
		t.Fatalf("skipped occurrence must carry no run: %v", second)
	}
	for _, item := range items {
		m, _ := item.(map[string]any)
		if m["dueAt"] == nil || m["revision"] == nil {
			t.Fatalf("occurrence must carry dueAt and revision: %v", m)
		}
	}
	if body["nextCursor"] != nil {
		t.Fatalf("single page must have null nextCursor: %v", body)
	}
}

func TestOccurrenceHTTPKeysetPagination(t *testing.T) {
	f := setupOccurrenceHTTP(t)
	ctx := context.Background()

	s := f.schedule(t, "* * * * *")
	f.makeDue(t, s.ID)
	if _, err := f.engine.EvaluateSchedule(ctx, f.orgID, f.envID, s.ID); err != nil {
		t.Fatalf("first evaluate: %v", err)
	}
	f.makeDueAgo(t, s.ID, "90 seconds")
	if _, err := f.engine.EvaluateSchedule(ctx, f.orgID, f.envID, s.ID); err != nil {
		t.Fatalf("second evaluate: %v", err)
	}

	status, first := f.get(t, "/v1/schedules/"+s.ID+"/occurrences?environment="+f.envID+"&limit=1", f.operator)
	if status != http.StatusOK {
		t.Fatalf("page 1 status = %d, body = %v", status, first)
	}
	cursor, _ := first["nextCursor"].(string)
	if cursor == "" {
		t.Fatalf("page 1 must carry a cursor: %v", first)
	}
	status, second := f.get(t, "/v1/schedules/"+s.ID+"/occurrences?environment="+f.envID+"&limit=1&cursor="+cursor, f.operator)
	if status != http.StatusOK {
		t.Fatalf("page 2 status = %d, body = %v", status, second)
	}
	items, _ := second["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("page 2 must hold the last row: %v", second)
	}
	if second["nextCursor"] != nil {
		t.Fatalf("last page must have null nextCursor: %v", second)
	}
}

func TestOccurrenceHTTPEnforcesScopeAndCapability(t *testing.T) {
	f := setupOccurrenceHTTP(t)

	s := f.schedule(t, "0 12 * * *")

	// A key without schedules:write learns nothing about history.
	if status, body := f.get(t, "/v1/schedules/"+s.ID+"/occurrences?environment="+f.envID, f.developer); status != http.StatusForbidden {
		t.Fatalf("developer status = %d, want 403: %v", status, body)
	}
	// Unknown schedule is 404, not an empty list.
	if status, body := f.get(t, "/v1/schedules/00000000-0000-0000-0000-000000000000/occurrences?environment="+f.envID, f.operator); status != http.StatusNotFound {
		t.Fatalf("unknown schedule status = %d, want 404: %v", status, body)
	}
	// Stale cursor is a 400, never a silent empty page.
	if status, body := f.get(t, "/v1/schedules/"+s.ID+"/occurrences?environment="+f.envID+"&cursor=00000000-0000-0000-0000-000000000000", f.operator); status != http.StatusBadRequest {
		t.Fatalf("bad cursor status = %d, want 400: %v", status, body)
	} else if body["code"] != "INVALID_CURSOR" {
		t.Fatalf("bad cursor code = %v, want INVALID_CURSOR", body)
	}
	// Missing environment is 400.
	if status, body := f.get(t, "/v1/schedules/"+s.ID+"/occurrences", f.operator); status != http.StatusBadRequest {
		t.Fatalf("missing env status = %d, want 400: %v", status, body)
	}
}

func TestOccurrenceHTTPCrossTenantInvisible(t *testing.T) {
	f := setupOccurrenceHTTP(t)
	ctx := context.Background()

	s := f.schedule(t, "0 12 * * *")
	f.makeDue(t, s.ID)
	if _, err := f.engine.EvaluateSchedule(ctx, f.orgID, f.envID, s.ID); err != nil {
		t.Fatalf("evaluate: %v", err)
	}

	other := setupTenantContext(t)
	defer other.cleanup()
	org, err := other.service.CreateOrganization(ctx, newUUID(t), "Other Org")
	if err != nil {
		t.Fatalf("create org: %v", err)
	}
	proj, err := other.service.CreateProject(ctx, org.ID, "Other Proj")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	env, err := other.service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvStaging, 10)
	if err != nil {
		t.Fatalf("create env: %v", err)
	}
	otherKey := bootstrapTestKey(t, other.service, org.ID, env.ID, []string{tenant.CapSchedulesWrite}).PlaintextKey

	req, err := http.NewRequest("GET", f.server.URL+"/v1/schedules/"+s.ID+"/occurrences?environment="+env.ID, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+otherKey)
	req.Header.Set("X-Organization-ID", org.ID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-tenant status = %d, want 404", resp.StatusCode)
	}
}

func TestScheduleHTTPExposesPersistedDueTimes(t *testing.T) {
	f := setupOccurrenceHTTP(t)

	s := f.schedule(t, "0 12 * * *")
	status, body := f.get(t, "/v1/schedules?environment="+f.envID, f.operator)
	if status != http.StatusOK {
		t.Fatalf("list status = %d, body = %v", status, body)
	}
	items, _ := body["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 schedule, got %v", body)
	}
	item, _ := items[0].(map[string]any)
	// The UI countdown renders from this persisted slot, never from a
	// client-side recomputation.
	if item["nextDueAt"] == nil {
		t.Fatalf("active schedule must expose nextDueAt: %v", item)
	}
	if item["revision"] != float64(s.Revision) {
		t.Fatalf("revision = %v, want %d", item["revision"], s.Revision)
	}
}
