package integration_test

// Schedule HTTP surface (Issue #34, Blueprint §17, §20, §24.2).
//
// These tests drive the real mux with real API keys, so they prove the
// declared capability is actually enforced by the route chain and not just
// asserted in a comment. A schedule is a standing grant to create runs
// without an operator present, so the role boundary is the point of the
// test, not an afterthought.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/controlplane"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
)

type scheduleHTTPFixture struct {
	orgID     string
	envID     string
	server    *httptest.Server
	tenant    *tenantTestContext
	operator  string // plaintext key with schedules:write
	developer string
	viewer    string
}

func setupScheduleHTTP(t *testing.T) *scheduleHTTPFixture {
	t.Helper()
	tc := setupTenantContext(t)
	org, err := tc.service.CreateOrganization(t.Context(), newUUID(t), "SchedHTTP Corp")
	if err != nil {
		t.Fatalf("create organization: %v", err)
	}
	proj, err := tc.service.CreateProject(t.Context(), org.ID, "SchedHTTP Proj")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	env, err := tc.service.CreateEnvironment(t.Context(), org.ID, proj.ID, tenant.EnvStaging, 10)
	if err != nil {
		t.Fatalf("create environment: %v", err)
	}
	f := &scheduleHTTPFixture{
		orgID:  org.ID,
		envID:  env.ID,
		tenant: tc,
		// Use the canonical production constructor: it is what registers the
		// schedule routes, so this exercises the real route chain.
		server: httptest.NewServer(controlplane.BuildMux(tc.authCfg, tc.runtimePool, nil, nil)),
	}
	f.operator = bootstrapTestKey(t, tc.service, org.ID, env.ID, []string{tenant.CapSchedulesWrite, tenant.CapRunsRead}).PlaintextKey
	f.developer = bootstrapTestKey(t, tc.service, org.ID, env.ID, []string{tenant.CapRunsCreate, tenant.CapRunsRead}).PlaintextKey
	f.viewer = bootstrapTestKey(t, tc.service, org.ID, env.ID, []string{tenant.CapRunsRead}).PlaintextKey
	return f
}

func (f *scheduleHTTPFixture) do(t *testing.T, method, path, key string, body any) (int, map[string]any) {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(raw)
	} else {
		reader = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, f.server.URL+path, reader)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("X-Organization-ID", f.orgID)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	// Every mutating route goes through durable command idempotency (§20.1),
	// so a mutating call must carry the header.
	if method != "GET" {
		req.Header.Set("Idempotency-Key", newUUID(t))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func (f *scheduleHTTPFixture) create(t *testing.T, key string) map[string]any {
	t.Helper()
	status, body := f.do(t, "POST", "/v1/schedules?environment="+f.envID, key, map[string]any{
		"workflow": "nightly-recon",
		"cron":     "0 12 * * *",
		"timezone": "UTC",
	})
	if status != http.StatusOK {
		t.Fatalf("create status = %d, body = %v", status, body)
	}
	return body
}

func TestScheduleHTTPCreateListUpdateDelete(t *testing.T) {
	f := setupScheduleHTTP(t)
	defer f.tenant.cleanup()
	defer f.server.Close()

	created := f.create(t, f.operator)
	id, _ := created["id"].(string)
	if id == "" {
		t.Fatalf("create response has no id: %v", created)
	}
	// The wire names come from the OpenAPI Schedule schema, not the columns.
	if created["workflow"] != "nightly-recon" {
		t.Fatalf("workflow = %v", created["workflow"])
	}
	if created["cron"] != "0 12 * * *" {
		t.Fatalf("cron = %v", created["cron"])
	}
	if created["overlapPolicy"] != "skip-overlap" || created["misfirePolicy"] != "coalesce-one" {
		t.Fatalf("policies must be the §17 constants: %v", created)
	}
	if created["paused"] != false {
		t.Fatalf("a new schedule must be active: %v", created)
	}
	if rev, _ := created["revision"].(float64); rev != 1 {
		t.Fatalf("revision = %v, want 1", created["revision"])
	}

	status, list := f.do(t, "GET", "/v1/schedules?environment="+f.envID, f.operator, nil)
	if status != http.StatusOK {
		t.Fatalf("list status = %d, body = %v", status, list)
	}
	items, _ := list["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("list returned %d items, want 1: %v", len(items), list)
	}
	if _, ok := list["nextCursor"]; !ok {
		t.Fatalf("list must carry nextCursor: %v", list)
	}

	// PATCH nests the desired state under `configuration`.
	status, updated := f.do(t, "PATCH", "/v1/schedules/"+id+"?environment="+f.envID, f.operator, map[string]any{
		"expectedRevision": 1,
		"configuration": map[string]any{
			"workflow": "nightly-recon",
			"cron":     "30 6 * * *",
			"timezone": "UTC",
		},
	})
	if status != http.StatusOK {
		t.Fatalf("patch status = %d, body = %v", status, updated)
	}
	if rev, _ := updated["revision"].(float64); rev != 2 {
		t.Fatalf("edit must advance revision, got %v", updated["revision"])
	}

	// A stale revision is refused rather than silently applied.
	status, conflict := f.do(t, "PATCH", "/v1/schedules/"+id+"?environment="+f.envID, f.operator, map[string]any{
		"expectedRevision": 1,
		"configuration":    map[string]any{"workflow": "nightly-recon", "cron": "0 1 * * *", "timezone": "UTC"},
	})
	if status != http.StatusConflict || conflict["code"] != "REVISION_CONFLICT" {
		t.Fatalf("stale patch = %d %v, want 409 REVISION_CONFLICT", status, conflict)
	}

	status, deleted := f.do(t, "DELETE", "/v1/schedules/"+id+"?environment="+f.envID, f.operator, nil)
	if status != http.StatusOK {
		t.Fatalf("delete status = %d, body = %v", status, deleted)
	}
	if deleted["deleted"] != true {
		t.Fatalf("delete response = %v", deleted)
	}
}

func TestScheduleHTTPPauseResume(t *testing.T) {
	f := setupScheduleHTTP(t)
	defer f.tenant.cleanup()
	defer f.server.Close()

	id, _ := f.create(t, f.operator)["id"].(string)

	status, paused := f.do(t, "POST", "/v1/schedules/"+id+"/pause?environment="+f.envID, f.operator, map[string]any{"expectedRevision": 1})
	if status != http.StatusOK {
		t.Fatalf("pause status = %d, body = %v", status, paused)
	}
	if paused["paused"] != true {
		t.Fatalf("pause response = %v", paused)
	}

	// Pausing twice is a conflict, not a silent success.
	status, again := f.do(t, "POST", "/v1/schedules/"+id+"/pause?environment="+f.envID, f.operator, map[string]any{})
	if status != http.StatusConflict {
		t.Fatalf("double pause = %d %v, want 409", status, again)
	}

	status, resumed := f.do(t, "POST", "/v1/schedules/"+id+"/resume?environment="+f.envID, f.operator, map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("resume status = %d, body = %v", status, resumed)
	}
	if resumed["paused"] != false {
		t.Fatalf("resume response = %v", resumed)
	}
}

func TestScheduleHTTPRefusesCallerWithoutCapability(t *testing.T) {
	f := setupScheduleHTTP(t)
	defer f.tenant.cleanup()
	defer f.server.Close()

	// A key that cannot write schedules must be refused on every route, not
	// only on create: list and delete are equally privileged reads/writes of
	// a standing run grant.
	for name, key := range map[string]string{"runs:create key": f.developer, "runs:read key": f.viewer} {
		t.Run(name, func(t *testing.T) {
			status, body := f.do(t, "POST", "/v1/schedules?environment="+f.envID, key, map[string]any{
				"workflow": "nightly-recon", "cron": "0 12 * * *", "timezone": "UTC",
			})
			if status != http.StatusForbidden {
				t.Fatalf("create = %d %v, want 403", status, body)
			}
			if body["code"] != "FORBIDDEN" {
				t.Fatalf("error code = %v, want FORBIDDEN", body["code"])
			}
			if status, body := f.do(t, "GET", "/v1/schedules?environment="+f.envID, key, nil); status != http.StatusForbidden {
				t.Fatalf("list = %d %v, want 403", status, body)
			}
			if status, body := f.do(t, "DELETE", "/v1/schedules/00000000-0000-4000-8000-000000000000?environment="+f.envID, key, nil); status != http.StatusForbidden {
				t.Fatalf("delete = %d %v, want 403", status, body)
			}
		})
	}

	// And the authorized caller still works, so the refusals above are the
	// capability and not a broken fixture.
	f.create(t, f.operator)
}

func TestScheduleHTTPRejectsInvalidDefinition(t *testing.T) {
	f := setupScheduleHTTP(t)
	defer f.tenant.cleanup()
	defer f.server.Close()

	cases := []struct {
		name string
		body map[string]any
		want string
	}{
		{"bad cron", map[string]any{"workflow": "w", "cron": "nope", "timezone": "UTC"}, "INVALID_CRON"},
		{"bad timezone", map[string]any{"workflow": "w", "cron": "0 12 * * *", "timezone": "Mars/Olympus"}, "INVALID_TIMEZONE"},
		{"missing workflow", map[string]any{"cron": "0 12 * * *", "timezone": "UTC"}, "INVALID_WORKFLOW"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, body := f.do(t, "POST", "/v1/schedules?environment="+f.envID, f.operator, tc.body)
			if status != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, body = %v, want 422", status, body)
			}
			if body["code"] != tc.want {
				t.Fatalf("code = %v, want %v", body["code"], tc.want)
			}
		})
	}
}

func TestScheduleHTTPRequiresEnvironment(t *testing.T) {
	f := setupScheduleHTTP(t)
	defer f.tenant.cleanup()
	defer f.server.Close()

	status, body := f.do(t, "POST", "/v1/schedules", f.operator, map[string]any{
		"workflow": "w", "cron": "0 12 * * *", "timezone": "UTC",
	})
	if status != http.StatusBadRequest || body["code"] != "MISSING_ENVIRONMENT" {
		t.Fatalf("= %d %v, want 400 MISSING_ENVIRONMENT", status, body)
	}
}

func TestScheduleHTTPRoleCapabilityMapGrantsWrite(t *testing.T) {
	// A schedule is a standing grant to create runs unattended. Developer and
	// Viewer must not hold schedules:write; Operator and above must, otherwise
	// the declared API surface would be unreachable for every role.
	for _, role := range []string{tenant.RoleViewer, tenant.RoleDeveloper} {
		if tenant.CanRolePerform(role, tenant.CapSchedulesWrite) {
			t.Fatalf("SECURITY: role %q must not hold schedules:write", role)
		}
	}
	for _, role := range []string{tenant.RoleOperator, tenant.RoleAdmin, tenant.RoleOwner} {
		if !tenant.CanRolePerform(role, tenant.CapSchedulesWrite) {
			t.Fatalf("role %q must hold schedules:write", role)
		}
	}
}

func TestScheduleHTTPCrossTenantInvisible(t *testing.T) {
	f := setupScheduleHTTP(t)
	defer f.tenant.cleanup()
	defer f.server.Close()

	id, _ := f.create(t, f.operator)["id"].(string)

	otherOrg, err := f.tenant.service.CreateOrganization(t.Context(), newUUID(t), "Other HTTP Org")
	if err != nil {
		t.Fatalf("create other org: %v", err)
	}
	otherProj, err := f.tenant.service.CreateProject(t.Context(), otherOrg.ID, "Other Proj")
	if err != nil {
		t.Fatalf("create other project: %v", err)
	}
	otherEnv, err := f.tenant.service.CreateEnvironment(t.Context(), otherOrg.ID, otherProj.ID, tenant.EnvStaging, 10)
	if err != nil {
		t.Fatalf("create other environment: %v", err)
	}
	other := &scheduleHTTPFixture{
		orgID:  otherOrg.ID,
		envID:  otherEnv.ID,
		tenant: f.tenant,
		server: f.server,
		operator: bootstrapTestKey(t, f.tenant.service, otherOrg.ID, otherEnv.ID,
			[]string{tenant.CapSchedulesWrite}).PlaintextKey,
	}
	status, body := other.do(t, "DELETE", "/v1/schedules/"+id+"?environment="+other.envID, other.operator, nil)
	if status != http.StatusNotFound || body["code"] != "SCHEDULE_NOT_FOUND" {
		t.Fatalf("cross-tenant delete = %d %v, want 404 SCHEDULE_NOT_FOUND", status, body)
	}

	// The original schedule must survive the attempt.
	status, list := f.do(t, "GET", "/v1/schedules?environment="+f.envID, f.operator, nil)
	if status != http.StatusOK {
		t.Fatalf("list = %d %v", status, list)
	}
	if items, _ := list["items"].([]any); len(items) != 1 {
		t.Fatalf("cross-tenant delete must not remove the schedule, list = %v", list)
	}
}

func TestScheduleHTTPUnknownFieldRejected(t *testing.T) {
	f := setupScheduleHTTP(t)
	defer f.tenant.cleanup()
	defer f.server.Close()

	// overlapPolicy and misfirePolicy are fixed by §17. Accepting them from a
	// caller would imply a choice that does not exist, so an unknown field is
	// refused rather than ignored.
	status, body := f.do(t, "POST", "/v1/schedules?environment="+f.envID, f.operator, map[string]any{
		"workflow": "w", "cron": "0 12 * * *", "timezone": "UTC",
		"overlapPolicy": "run-anyway",
	})
	if status != http.StatusBadRequest || body["code"] != "INVALID_REQUEST" {
		t.Fatalf("= %d %v, want 400 INVALID_REQUEST for a caller-supplied policy", status, body)
	}
	_ = fmt.Sprint()
}
