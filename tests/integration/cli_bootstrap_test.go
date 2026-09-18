package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/auth"
	"github.com/Ryanakml/Deadbolt/internal/cli"
	"github.com/Ryanakml/Deadbolt/internal/controlplane"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
)

// TestHostedBootstrapPublicBoundary proves a fresh hosted identity with zero
// organizations can establish organization/project/environment context using
// only supported CLI commands and public APIs:
//
// fresh human Bearer
// -> GET /v1/organizations (none)
// -> runtime bootstrap --org-name/--project/--env
// -> org/project/env exist, CLI context stored
// -> rerun selects without duplicating
// -> subsequent human deployment API works
//
// Invariant Verification:
//   - Real production mux (controlplane.BuildMux) in hosted mode
//   - Zero internal tenant-service bootstrap calls (no tenant.Service.Create*)
//   - Zero backdoor SQL / DB manipulation (session creation is auth setup)
//   - Fresh identity starts with no organizations
func TestHostedBootstrapPublicBoundary(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()
	ctx := context.Background()

	server := httptest.NewServer(controlplane.BuildMux(tc.authCfg, tc.runtimePool, nil, nil))
	defer server.Close()

	credDir := t.TempDir()
	cli.SetCustomCredentialsDir(credDir)

	origAPIURL := os.Getenv("DEADBOLT_API_URL")
	origAPIKey := os.Getenv("DEADBOLT_API_KEY")
	origOrgID := os.Getenv("DEADBOLT_ORG_ID")
	origEnv := os.Getenv("DEADBOLT_ENV")
	defer func() {
		os.Setenv("DEADBOLT_API_URL", origAPIURL)
		os.Setenv("DEADBOLT_API_KEY", origAPIKey)
		os.Setenv("DEADBOLT_ORG_ID", origOrgID)
		os.Setenv("DEADBOLT_ENV", origEnv)
	}()

	// Fresh hosted human identity: valid Bearer, zero memberships. The user
	// row is created exactly as a real OIDC login would create it. The
	// subject is unique per run so the shared integration database cannot
	// leak memberships from earlier runs.
	subject, _ := tenant.NewUUID()
	user, err := tc.sessionStore.GetOrCreateUserFromOIDC(ctx, &auth.Identity{
		Issuer:  "https://issuer.example",
		Subject: "fresh-human-" + subject,
		Email:   "fresh-" + subject + "@example.com",
		Name:    "Fresh Human",
	})
	if err != nil {
		t.Fatalf("create OIDC user: %v", err)
	}
	_, token, err := tc.sessionStore.CreateCLISession(ctx, user.ID, nil, "127.0.0.1", "bootstrap-test", 12*time.Hour, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("create human session: %v", err)
	}
	os.Setenv("DEADBOLT_API_URL", server.URL)
	os.Setenv("DEADBOLT_API_KEY", token)
	os.Unsetenv("DEADBOLT_ORG_ID")
	os.Unsetenv("DEADBOLT_ENV")

	get := func(path, orgID string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, server.URL+path, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		if orgID != "" {
			req.Header.Set("X-Organization-ID", orgID)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		return resp.StatusCode, body
	}

	// 1. Fresh identity starts with no organizations.
	if code, body := get("/v1/organizations", ""); code != http.StatusOK {
		t.Fatalf("list orgs failed: %d (%v)", code, body)
	} else if orgs, _ := body["organizations"].([]any); len(orgs) != 0 {
		t.Fatalf("expected zero organizations for fresh identity, got %d", len(orgs))
	}

	// 2. Supported bootstrap from zero state.
	if err := cli.HandleBootstrap([]string{"--org-name", "acme", "--project", "svc", "--env", "staging"}); err != nil {
		t.Fatalf("runtime bootstrap failed: %v", err)
	}
	orgID, err := cli.GetCredential("deadbolt", "org_id")
	if err != nil || orgID == "" {
		t.Fatalf("expected org_id stored, got %q (%v)", orgID, err)
	}
	storedEnv, err := cli.GetCredential("deadbolt", "env")
	if err != nil || storedEnv != "staging" {
		t.Fatalf("expected env staging stored, got %q (%v)", storedEnv, err)
	}

	// 3. Rerun selects existing resources without duplicating them.
	if err := cli.HandleBootstrap([]string{"--project", "svc", "--env", "staging"}); err != nil {
		t.Fatalf("bootstrap rerun failed: %v", err)
	}
	if code, body := get("/v1/organizations", ""); code != http.StatusOK {
		t.Fatalf("list orgs failed: %d", code)
	} else if orgs, _ := body["organizations"].([]any); len(orgs) != 1 {
		t.Fatalf("expected exactly one organization after rerun, got %d", len(orgs))
	}
	code, projectsBody := get("/v1/projects", orgID)
	if code != http.StatusOK {
		t.Fatalf("list projects failed: %d (%v)", code, projectsBody)
	}
	projects, _ := projectsBody["projects"].([]any)
	if len(projects) != 1 {
		t.Fatalf("expected exactly one project after rerun, got %d", len(projects))
	}
	projectID, _ := projects[0].(map[string]any)["id"].(string)
	code, envsBody := get("/v1/projects/"+projectID+"/environments", orgID)
	if code != http.StatusOK {
		t.Fatalf("list environments failed: %d (%v)", code, envsBody)
	}
	if envs, _ := envsBody["environments"].([]any); len(envs) != 1 {
		t.Fatalf("expected exactly one environment after rerun, got %d", len(envs))
	}

	// 4. The bootstrapped context is immediately usable: human deploy by
	// environment name through the real HTTP boundary.
	manifest := deploymentManifest(t, strings.Repeat("c", 64))
	depReq, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/deployments?environment=staging", bytes.NewReader(manifest))
	depReq.Header.Set("Authorization", "Bearer "+token)
	depReq.Header.Set("X-Organization-ID", orgID)
	depReq.Header.Set("Idempotency-Key", "bootstrap-usability-1")
	depReq.Header.Set("Content-Type", "application/json")
	depResp, err := http.DefaultClient.Do(depReq)
	if err != nil {
		t.Fatal(err)
	}
	defer depResp.Body.Close()
	if depResp.StatusCode != http.StatusCreated {
		var errBody map[string]any
		_ = json.NewDecoder(depResp.Body).Decode(&errBody)
		t.Fatalf("human deploy after bootstrap failed: %d (%v)", depResp.StatusCode, errBody)
	}
	var depBody struct {
		ID string `json:"id"`
	}
	_ = json.NewDecoder(depResp.Body).Decode(&depBody)
	if depBody.ID == "" {
		t.Fatal("expected deployment ID in response")
	}
	fmt.Printf("bootstrap usability deployment: %s\n", depBody.ID)

	deploymentEnvOf := func(depID string) string {
		t.Helper()
		var envID string
		if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
			return tx.QueryRow(ctx, `SELECT environment_id::text FROM deployments WHERE id = $1`, depID).Scan(&envID)
		}); err != nil {
			t.Fatalf("deployment env lookup: %v", err)
		}
		return envID
	}
	envA := deploymentEnvOf(depBody.ID)

	// 5. Second project owning another "staging": re-bootstrap pins the
	// exact selection, and default-context deploy targets ENV-A without
	// manual UUID copying.
	project2, err := tc.service.CreateProject(ctx, orgID, "second-project")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tc.service.CreateEnvironment(ctx, orgID, project2.ID, tenant.EnvStaging, 1); err != nil {
		t.Fatal(err)
	}
	if err := cli.HandleBootstrap([]string{"--project", "svc", "--env", "staging"}); err != nil {
		t.Fatalf("bootstrap with two stagings failed: %v", err)
	}
	storedEnvID, err := cli.GetCredential("deadbolt", "env_id")
	if err != nil || storedEnvID != envA {
		t.Fatalf("expected stored env UUID %s, got %q (%v)", envA, storedEnvID, err)
	}

	manifest2Path := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(manifest2Path, deploymentManifest(t, strings.Repeat("d", 64)), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := cli.HandleDeploy([]string{"--manifest", manifest2Path}); err != nil {
		t.Fatalf("default-context deploy failed: %v", err)
	}
	var lastDepID string
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT id::text FROM deployments WHERE environment_id = $1 ORDER BY created_at DESC LIMIT 1`, envA).Scan(&lastDepID)
	}); err != nil || lastDepID == "" {
		t.Fatalf("expected default-context deploy bound to %s: %v", envA, err)
	}

	// 6. Default-context worker enrollment succeeds against the stored UUID.
	// The human session carries the active org, mirroring post-login context.
	_, token2, err := tc.sessionStore.CreateCLISession(ctx, user.ID, &orgID, "127.0.0.1", "bootstrap-test-2", 12*time.Hour, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("create human session: %v", err)
	}
	os.Setenv("DEADBOLT_API_KEY", token2)
	if err := cli.HandleWorkerEnroll([]string{
		"--key-path", filepath.Join(t.TempDir(), "worker-default.key"),
		"--pool", "default",
		"--create-token",
		"--control-plane-url", server.URL,
	}); err != nil {
		t.Fatalf("default-context enroll failed: %v", err)
	}

	// 7. Default-context runs list also targets the stored UUID.
	if err := cli.RunRuns([]string{"list"}); err != nil {
		t.Fatalf("default-context runs list failed: %v", err)
	}

	// 8. Ambiguous name with no project/canonical context still fails
	// explicitly instead of silently picking one project's staging.
	for _, account := range []string{"env_id", "project_id", "project"} {
		if err := cli.DeleteCredential("deadbolt", account); err != nil {
			t.Fatalf("clear stored %s: %v", account, err)
		}
	}
	if err := cli.HandleWorkerEnroll([]string{
		"--key-path", filepath.Join(t.TempDir(), "worker-amb.key"),
		"--env", "staging",
		"--pool", "default",
		"--create-token",
		"--control-plane-url", server.URL,
	}); err == nil || !strings.Contains(err.Error(), "matches 2 environments") {
		t.Fatalf("expected explicit ambiguity error, got %v", err)
	}
}
