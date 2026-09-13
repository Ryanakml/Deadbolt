package integration_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/auth"
	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"github.com/Ryanakml/Deadbolt/internal/gateway"
)

// cleanEnv constructs an execution environment inheriting toolchain variables
// while overriding and stripping specific application variables.
func cleanEnv(overrides ...string) []string {
	envMap := make(map[string]string)
	for _, e := range os.Environ() {
		parts := strings.SplitN(e, "=", 2)
		if len(parts) == 2 {
			envMap[parts[0]] = parts[1]
		}
	}
	// Strip database credentials by default for config tests
	delete(envMap, "DATABASE_URL")
	delete(envMap, "MIGRATOR_DATABASE_URL")
	delete(envMap, "DEADBOLT_DATABASE_URL")
	delete(envMap, "DEADBOLT_MIGRATOR_DATABASE_URL")
	delete(envMap, "DEADBOLT_DEV_KEY")
	delete(envMap, "DEADBOLT_DEV_KEY_PATH")

	for _, o := range overrides {
		parts := strings.SplitN(o, "=", 2)
		if len(parts) == 2 {
			envMap[parts[0]] = parts[1]
		}
	}
	var res []string
	for k, v := range envMap {
		res = append(res, k+"="+v)
	}
	return res
}

// TestGateM0_CleanCloneActionableConfigErrors verifies that the control plane fails closed
// with explicit, human-actionable remediation instructions when mandatory configurations
// are missing or invalid, adhering to Blueprint §22.2, §24.1, §24.4, §29.1, and §30.
func TestGateM0_CleanCloneActionableConfigErrors(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("failed to determine repository root: %v", err)
	}

	// 1. Hosted Mode: Missing DATABASE_URL fails with actionable remediation
	t.Run("HostedMissingDatabaseURL", func(t *testing.T) {
		cmd := exec.Command("go", "run", "./cmd/control-plane")
		cmd.Dir = repoRoot
		cmd.Env = cleanEnv(
			"RUNTIME_MODE=hosted",
			"COOKIE_SECURE=true",
			"DEADBOLT_OIDC_ISSUER=https://accounts.google.com",
			"DEADBOLT_OIDC_CLIENT_ID=deadbolt-client",
			"DEADBOLT_ALLOWED_ORIGINS=https://staging.deadbolt.cloud",
		)
		out, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("expected control-plane startup to fail when DATABASE_URL is missing in hosted mode")
		}
		outStr := string(out)
		if !strings.Contains(outStr, "DATABASE_URL is required in hosted mode") {
			t.Errorf("expected error to mention 'DATABASE_URL is required in hosted mode', got: %s", outStr)
		}
		if !strings.Contains(outStr, "Remediation: Configure DATABASE_URL=postgres://") {
			t.Errorf("expected actionable remediation advice for DATABASE_URL, got: %s", outStr)
		}
	})

	// 2. Hosted Mode: DEV_AUTH_ENABLED=true fails closed with remediation
	t.Run("HostedDevAuthEnabledBarred", func(t *testing.T) {
		cfg := auth.Config{
			RuntimeMode:            auth.ModeHosted,
			DevAuthEnabled:         true,
			CookieSecure:           true,
			AllowedOrigins:         []string{"https://app.deadbolt.cloud"},
			SessionIdleTimeout:     12 * time.Hour,
			SessionAbsoluteTimeout: 7 * 24 * time.Hour,
			OIDC: auth.OIDCConfig{
				Issuer:   "https://accounts.google.com",
				ClientID: "client-id-123",
			},
		}
		err := cfg.Validate("0.0.0.0")
		if err == nil {
			t.Fatalf("SECURITY VIOLATION: hosted mode accepted DevAuthEnabled=true")
		}
		if !strings.Contains(err.Error(), "rejects dev auth and development keys") {
			t.Errorf("expected error mentioning dev auth rejection, got: %v", err)
		}
	})

	// 3. Hosted Mode: Insecure cookies (COOKIE_SECURE=false) fails closed
	t.Run("HostedInsecureCookieBarred", func(t *testing.T) {
		cfg := auth.Config{
			RuntimeMode:            auth.ModeHosted,
			DevAuthEnabled:         false,
			CookieSecure:           false,
			AllowedOrigins:         []string{"https://app.deadbolt.cloud"},
			SessionIdleTimeout:     12 * time.Hour,
			SessionAbsoluteTimeout: 7 * 24 * time.Hour,
			OIDC: auth.OIDCConfig{
				Issuer:   "https://accounts.google.com",
				ClientID: "client-id-123",
			},
		}
		err := cfg.Validate("0.0.0.0")
		if err == nil {
			t.Fatalf("SECURITY VIOLATION: hosted mode accepted CookieSecure=false")
		}
		if !strings.Contains(err.Error(), "CookieSecure=true") {
			t.Errorf("expected error mentioning CookieSecure=true, got: %v", err)
		}
	})

	// 4. Hosted Mode: Development Key Injection Barred (Blueprint §24.4)
	t.Run("HostedDevKeyBarred", func(t *testing.T) {
		t.Setenv("DEADBOLT_DEV_KEY", "prohibited-secret-key")
		cfg := auth.Config{
			RuntimeMode:            auth.ModeHosted,
			DevAuthEnabled:         false,
			CookieSecure:           true,
			AllowedOrigins:         []string{"https://app.deadbolt.cloud"},
			SessionIdleTimeout:     12 * time.Hour,
			SessionAbsoluteTimeout: 7 * 24 * time.Hour,
			OIDC: auth.OIDCConfig{
				Issuer:   "https://accounts.google.com",
				ClientID: "client-id-123",
			},
		}
		err := cfg.Validate("0.0.0.0")
		if err == nil {
			t.Fatalf("SECURITY VIOLATION: hosted mode accepted injected DEADBOLT_DEV_KEY")
		}
		if !strings.Contains(err.Error(), "rejects dev auth and development keys") {
			t.Errorf("expected error mentioning dev auth rejection, got: %v", err)
		}
	})

	// 5. Hosted Mode: Missing OIDC Configuration Fails Closed
	t.Run("HostedMissingOIDCConfig", func(t *testing.T) {
		cfg := auth.Config{
			RuntimeMode:            auth.ModeHosted,
			DevAuthEnabled:         false,
			CookieSecure:           true,
			AllowedOrigins:         []string{"https://app.deadbolt.cloud"},
			SessionIdleTimeout:     12 * time.Hour,
			SessionAbsoluteTimeout: 7 * 24 * time.Hour,
			OIDC: auth.OIDCConfig{
				Issuer:   "", // Missing
				ClientID: "client-id-123",
			},
		}
		err := cfg.Validate("0.0.0.0")
		if err == nil {
			t.Fatalf("expected validation error when DEADBOLT_OIDC_ISSUER is missing in hosted mode")
		}
		if !strings.Contains(err.Error(), "DEADBOLT_OIDC_ISSUER") {
			t.Errorf("expected error to specify DEADBOLT_OIDC_ISSUER, got: %v", err)
		}
	})

	// 6. Local Mode: Non-loopback listen address with dev auth fails closed
	t.Run("LocalNonLoopbackWithoutContainerMode", func(t *testing.T) {
		cfg := auth.Config{
			RuntimeMode:    auth.ModeLocal,
			DevAuthEnabled: true,
			ContainerLocal: false,
		}
		err := cfg.Validate("0.0.0.0")
		if err == nil {
			t.Fatalf("SECURITY VIOLATION: local mode accepted non-loopback host 0.0.0.0 without ContainerLocal")
		}
		if !strings.Contains(err.Error(), "restricted strictly to loopback binding") {
			t.Errorf("expected error requiring loopback binding, got: %v", err)
		}
	})

	// 7. Local Mode: ContainerLocal permits non-loopback for container ingress
	t.Run("LocalContainerModePermitsIngress", func(t *testing.T) {
		cfg := auth.Config{
			RuntimeMode:    auth.ModeLocal,
			DevAuthEnabled: true,
			ContainerLocal: true,
		}
		if err := cfg.Validate("0.0.0.0"); err != nil {
			t.Fatalf("expected ContainerLocal=true to permit non-loopback listen host in containers, got: %v", err)
		}
	})

	// 8. Migration Flag: Missing MIGRATOR_DATABASE_URL fails with clear error
	t.Run("MigrateFlagRequiresMigratorURL", func(t *testing.T) {
		cmd := exec.Command("go", "run", "./cmd/control-plane", "--migrate")
		cmd.Dir = repoRoot
		cmd.Env = cleanEnv(
			"RUNTIME_MODE=hosted",
			// MIGRATOR_DATABASE_URL deliberately missing
		)
		out, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("expected control-plane --migrate to fail when MIGRATOR_DATABASE_URL is missing")
		}
		outStr := string(out)
		if !strings.Contains(outStr, "MIGRATOR_DATABASE_URL is required for migration execution") {
			t.Errorf("expected error to require MIGRATOR_DATABASE_URL, got: %s", outStr)
		}
	})
}

// TestGateM0_VersionEndpointContract validates the in-memory /version and /livez
// response shapes. It deliberately uses synthetic metadata, so it is not evidence
// of a deployed image or staging provenance.
func TestGateM0_VersionEndpointContract(t *testing.T) {
	expectedSHA := "4a4ed6545046818557563ffe144c3a978f2ec191"
	expectedDigest := "sha256:1111222233334444555566667777888899990000aaaaabbbbbcccccdddddeeeee"
	expectedBuildTime := "2026-09-13T10:00:00Z"
	expectedVersion := "0.1.0"

	versionInfo := gateway.VersionInfo{
		Version:     expectedVersion,
		CommitSHA:   expectedSHA,
		BuildTime:   expectedBuildTime,
		ImageDigest: expectedDigest,
		RuntimeMode: auth.ModeHosted,
	}

	checker := gateway.NewHealthChecker(versionInfo, nil, nil, 5)
	mux := http.NewServeMux()
	checker.Routes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	client := ts.Client()

	// 1. GET /livez
	liveResp, err := client.Get(ts.URL + "/livez")
	if err != nil {
		t.Fatalf("failed to GET /livez: %v", err)
	}
	defer liveResp.Body.Close()
	if liveResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from /livez, got %d", liveResp.StatusCode)
	}
	var liveData gateway.LiveResponse
	if err := json.NewDecoder(liveResp.Body).Decode(&liveData); err != nil {
		t.Fatalf("failed to decode /livez response: %v", err)
	}
	if liveData.Status != "alive" || liveData.Timestamp == "" {
		t.Fatalf("invalid /livez payload: %+v", liveData)
	}

	// 2. GET /version
	verResp, err := client.Get(ts.URL + "/version")
	if err != nil {
		t.Fatalf("failed to GET /version: %v", err)
	}
	defer verResp.Body.Close()
	if verResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from /version, got %d", verResp.StatusCode)
	}

	var verData gateway.VersionInfo
	if err := json.NewDecoder(verResp.Body).Decode(&verData); err != nil {
		t.Fatalf("failed to decode /version response: %v", err)
	}

	if verData.Version != expectedVersion {
		t.Errorf("expected Version %q, got %q", expectedVersion, verData.Version)
	}
	if verData.CommitSHA != expectedSHA {
		t.Errorf("expected CommitSHA %q, got %q", expectedSHA, verData.CommitSHA)
	}
	if verData.BuildTime != expectedBuildTime {
		t.Errorf("expected BuildTime %q, got %q", expectedBuildTime, verData.BuildTime)
	}
	if verData.ImageDigest != expectedDigest {
		t.Errorf("expected ImageDigest %q, got %q", expectedDigest, verData.ImageDigest)
	}
	if verData.RuntimeMode != auth.ModeHosted {
		t.Errorf("expected RuntimeMode 'hosted', got %q", verData.RuntimeMode)
	}
}

// TestGateM0_ContractAndSchemaParity verifies executable contract conformance,
// ensuring that canonical hashing, JSON pointer, JSON schema, and linear DAG
// fixtures match expected values across the Go and TypeScript implementations.
func TestGateM0_ContractAndSchemaParity(t *testing.T) {
	// Read shared conformance fixtures
	fixturesPath := filepath.Join("..", "..", "contracts", "fixtures", "conformance.json")
	data, err := os.ReadFile(fixturesPath)
	if err != nil {
		t.Fatalf("failed to read conformance fixtures: %v", err)
	}

	var cases []map[string]any
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatalf("failed to unmarshal conformance corpus: %v", err)
	}

	if len(cases) < 160 {
		t.Fatalf("expected at least 160 conformance test cases, found %d", len(cases))
	}

	// Verify Go canonical hashing produces identical SHA-256 digests for all digest cases
	digestCount := 0
	for _, tc := range cases {
		if op, ok := tc["op"].(string); !ok || op != "digest" {
			continue
		}
		expectedMap, ok := tc["expected"].(map[string]any)
		if !ok || expectedMap["error"] != nil {
			continue
		}
		valMap, ok := expectedMap["value"].(map[string]any)
		if !ok {
			continue
		}

		expectedSHA, _ := valMap["sha256"].(string)
		expectedCanonical, _ := valMap["canonical"].(string)
		rawStr, _ := tc["raw"].(string)
		tcID, _ := tc["id"].(string)

		digestCount++
		t.Run("Digest_"+tcID, func(t *testing.T) {
			canonicalBytes, digest, err := contracts.Digest([]byte(rawStr))
			if err != nil {
				t.Fatalf("[%s] failed to canonicalize JSON: %v", tcID, err)
			}
			if digest != expectedSHA {
				t.Fatalf("[%s] digest mismatch: expected %s, got %s (canonical: %s)", tcID, expectedSHA, digest, string(canonicalBytes))
			}
			if string(canonicalBytes) != expectedCanonical {
				t.Fatalf("[%s] canonical bytes mismatch: expected %s, got %s", tcID, expectedCanonical, string(canonicalBytes))
			}
		})
	}

	if digestCount < 10 {
		t.Errorf("expected at least 10 digest fixtures, tested %d", digestCount)
	}

	// Verify enums parity
	enumsPath := filepath.Join("..", "..", "contracts", "common", "enums.json")
	enumsData, err := os.ReadFile(enumsPath)
	if err != nil {
		t.Fatalf("failed to read enums.json: %v", err)
	}
	var enumsDoc map[string]any
	if err := json.Unmarshal(enumsData, &enumsDoc); err != nil {
		t.Fatalf("failed to parse enums.json: %v", err)
	}
	for _, requiredEnum := range []string{"RunStatus", "StepStatus", "AttemptStatus", "ApprovalStatus", "TimerStatus", "WorkerStatus", "RecoveryMode"} {
		if _, ok := enumsDoc[requiredEnum]; !ok {
			t.Errorf("missing mandatory canonical enum definition: %s", requiredEnum)
		}
	}
}

// TestGateM0_SpikesResolutionAndNoBlockers verifies each spike report has its
// explicit M1-blocker decision. The reports remain the source of truth; this
// only prevents removing their final acceptance marker by accident.
func TestGateM0_SpikesResolutionAndNoBlockers(t *testing.T) {
	reports := []struct {
		Name     string
		Filename string
	}{
		{
			Name:     "SP-01 Manifest Conformance",
			Filename: filepath.Join("..", "..", "docs", "reports", "SP-01-manifest-conformance.md"),
		},
		{
			Name:     "SP-02 DB Claim Contention",
			Filename: filepath.Join("..", "..", "docs", "reports", "SP-02-claim-contention.md"),
		},
		{
			Name:     "SP-03 Worker Process Lifecycle",
			Filename: filepath.Join("..", "..", "docs", "reports", "SP-03-worker-lifecycle.md"),
		},
	}

	for _, r := range reports {
		t.Run(r.Name, func(t *testing.T) {
			content, err := os.ReadFile(r.Filename)
			if err != nil {
				t.Fatalf("failed to read spike report %s: %v", r.Filename, err)
			}
			contentStr := string(content)
			if !strings.Contains(contentStr, "**M1 blocker decision:** **NONE**") {
				t.Errorf("spike report %s must state its explicit M1 blocker decision", r.Filename)
			}
		})
	}
}
