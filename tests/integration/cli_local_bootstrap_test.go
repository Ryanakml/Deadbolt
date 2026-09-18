package integration_test

import (
	"context"
	"net"
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
)

// TestLocalBootstrapPublicBoundary proves the complete local/dev-auth bootstrap journey
// across the real public HTTP boundary:
//
// dev login
//
//	-> create/select organization via /v1
//	-> create/select project via /v1
//	-> create/select development environment via /v1
//	-> create least-privilege API key via /v1
//	-> persist credentials in keychain/credentials store
//	-> use generated credential for public environment discovery and worker enrollment
//	-> use generated credential for public worker list
//
// Invariant Verification:
//   - Uses real production mux (controlplane.BuildMux) in ModeLocal with DevAuthEnabled=true
//   - Zero internal tenant-service calls (no tenant.Service.Create*)
//   - Zero backdoor SQL / DB manipulation
//   - All requests use correct /v1 routes and required idempotency headers
//   - Created API key contains only canonical least-privilege capabilities (no wildcard "*")
//   - Verification of least-privilege discovery via GET /v1/projects and GET /v1/projects/{id}/environments
func TestLocalBootstrapPublicBoundary(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	// 1. Configure control plane mux for local dev mode with dynamic listener
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	serverURL := "http://" + listener.Addr().String()

	tc.authCfg.RuntimeMode = auth.ModeLocal
	tc.authCfg.DevAuthEnabled = true
	tc.authCfg.CookieSecure = false
	tc.authCfg.AllowedOrigins = []string{serverURL, "http://127.0.0.1:8080"}

	server := httptest.NewUnstartedServer(controlplane.BuildMux(tc.authCfg, tc.runtimePool, nil, nil))
	server.Listener = listener
	server.Start()
	defer server.Close()

	// 2. Set up isolated CLI credentials store and workspace
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

	os.Setenv("DEADBOLT_API_URL", server.URL)
	os.Unsetenv("DEADBOLT_API_KEY")
	os.Unsetenv("DEADBOLT_ORG_ID")
	os.Unsetenv("DEADBOLT_ENV")

	// 3. Step: runtime login (local dev bootstrap via /api/auth/dev-login and /v1 public routes)
	t.Log("==> Executing: runtime login (local dev-auth mode)")
	err = cli.RunLogin([]string{
		"--control-plane-url", server.URL,
		"--email", "developer@local.dev",
		"--env", "development",
	})
	if err != nil {
		t.Fatalf("local dev login failed: %v", err)
	}

	// 4. Verify credentials stored in credentials store
	storedKey, err := cli.GetCredential("deadbolt", "api_key")
	if err != nil || storedKey == "" {
		t.Fatalf("expected api_key stored in credentials, got: %v", err)
	}
	storedOrg, err := cli.GetCredential("deadbolt", "org_id")
	if err != nil || storedOrg == "" {
		t.Fatalf("expected org_id stored in credentials, got: %v", err)
	}
	storedEnv, err := cli.GetCredential("deadbolt", "env")
	if err != nil || storedEnv != "development" {
		t.Fatalf("expected env 'development' stored in credentials, got: %s (err: %v)", storedEnv, err)
	}

	// Verify the stored API key has standard Deadbolt prefix db_<env>_
	if len(storedKey) < 8 || !strings.HasPrefix(storedKey, "db_") {
		t.Fatalf("stored api key should have standard db_ prefix, got: %s", storedKey)
	}

	// Verify the bootstrapped key carries exactly the canonical
	// least-privilege capabilities: no wildcard, no excess grants.
	expectedCaps := map[string]bool{
		"org:read": true, "deployments:register": true, "deployments:write": true,
		"deployments:activate:staging": true, "runs:create": true, "runs:read": true,
		"payload:read": true, "workers:read": true, "workers:drain": true, "admin:key": true,
	}
	err = tc.pool.WithTenantTx(context.Background(), storedOrg, func(ctx context.Context, tx storage.Tx) error {
		rows, err := tx.Query(ctx, `SELECT capabilities FROM api_keys WHERE organization_id = $1`, storedOrg)
		if err != nil {
			return err
		}
		defer rows.Close()
		found := false
		for rows.Next() {
			var caps []string
			if err := rows.Scan(&caps); err != nil {
				return err
			}
			found = true
			if len(caps) != len(expectedCaps) {
				t.Fatalf("expected %d least-privilege capabilities, got %v", len(expectedCaps), caps)
			}
			for _, c := range caps {
				if c == "*" || !expectedCaps[c] {
					t.Fatalf("non-least-privilege capability granted: %q (full set %v)", c, caps)
				}
			}
		}
		if !found {
			t.Fatal("expected at least one API key row for the bootstrapped org")
		}
		return rows.Err()
	})
	if err != nil {
		t.Fatal(err)
	}

	// 5. Step: Use generated credentials for subsequent real public CLI requests
	// Call runtime worker enroll --env development --create-token
	// This exercises public environment resolution (GET /v1/projects, GET /v1/projects/{id}/environments)
	// which requires "org:read", followed by token creation which requires "deployments:write".
	t.Log("==> Executing: runtime worker enroll --env development --create-token")
	workerKeyPath := filepath.Join(credDir, "worker-test.key")
	err = cli.HandleWorkerEnroll([]string{
		"--key-path", workerKeyPath,
		"--env", "development",
		"--create-token",
		"--control-plane-url", server.URL,
	})
	if err != nil {
		t.Fatalf("runtime worker enroll failed using local bootstrap credentials: %v", err)
	}

	// Verify worker key was created on disk
	if _, err := os.Stat(workerKeyPath); os.IsNotExist(err) {
		t.Fatalf("expected worker private key at %s", workerKeyPath)
	}

	// 6. Step: Use generated credentials for runtime worker list
	// This exercises GET /v1/workers which requires "workers:read"
	t.Log("==> Executing: runtime worker list")
	err = cli.HandleWorker([]string{
		"list",
		"--env", "development",
		"--control-plane-url", server.URL,
	})
	if err != nil {
		t.Fatalf("runtime worker list failed: %v", err)
	}

	// Wait briefly to ensure no async leaks
	time.Sleep(100 * time.Millisecond)
	t.Log("✓ Local bootstrap successfully completed through real public HTTP boundary with least-privilege credentials!")
}
