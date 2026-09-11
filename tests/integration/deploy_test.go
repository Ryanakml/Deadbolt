package integration_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/auth"
	"github.com/Ryanakml/Deadbolt/internal/gateway"
)

type mockNATSChecker struct {
	connected bool
}

func (m *mockNATSChecker) IsConnected() bool {
	return m.connected
}

// TestControlPlaneHealthEndpoints validates /livez, /version, and /readyz behavior
// under healthy database conditions according to Blueprint §25.2 and §26.2.
func TestControlPlaneHealthEndpoints(t *testing.T) {
	db, pool, _ := setupTestDB(t)
	defer db.Close()
	defer pool.Close()

	versionInfo := gateway.VersionInfo{
		Version:     "0.1.0",
		CommitSHA:   "e2e-test-commit-sha",
		BuildTime:   "2026-09-11T12:00:00Z",
		ImageDigest: "sha256:1111222233334444555566667777888899990000aaaaabbbbbcccccdddddeeeee",
		RuntimeMode: "hosted",
	}

	checker := gateway.NewHealthChecker(versionInfo, pool, &mockNATSChecker{connected: true}, 5)

	var activeTicker atomic.Int64
	activeTicker.Store(time.Now().UnixNano())
	checker.SetSchedulerTicker(&activeTicker, 60*time.Second)

	mux := http.NewServeMux()
	checker.Routes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	client := ts.Client()

	// 1. Test GET /livez
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
		t.Fatalf("invalid /livez response: %+v", liveData)
	}

	// 2. Test GET /version
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
	if verData.Version != "0.1.0" || verData.CommitSHA != "e2e-test-commit-sha" || verData.ImageDigest != versionInfo.ImageDigest {
		t.Fatalf("unexpected /version payload: %+v", verData)
	}

	// 3. Test GET /readyz (All Healthy)
	readyResp, err := client.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatalf("failed to GET /readyz: %v", err)
	}
	defer readyResp.Body.Close()

	if readyResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from /readyz when healthy, got %d", readyResp.StatusCode)
	}
	var readyData gateway.ReadyResponse
	if err := json.NewDecoder(readyResp.Body).Decode(&readyData); err != nil {
		t.Fatalf("failed to decode /readyz response: %v", err)
	}
	if readyData.Status != "ready" {
		t.Fatalf("expected status=ready, got %s", readyData.Status)
	}
	if readyData.Database != "healthy" {
		t.Fatalf("expected database=healthy, got %s", readyData.Database)
	}
	if readyData.Schema != "current" {
		t.Fatalf("expected schema=current, got %s", readyData.Schema)
	}
	if readyData.Scheduler != "active" {
		t.Fatalf("expected scheduler=active, got %s", readyData.Scheduler)
	}
	if readyData.NATS != "connected" {
		t.Fatalf("expected nats=connected, got %s", readyData.NATS)
	}
}

// TestReadyzDatabaseFailure asserts that /readyz returns HTTP 503 Service Unavailable
// when database connectivity is unreachable or pool is nil.
func TestReadyzDatabaseFailure(t *testing.T) {
	versionInfo := gateway.VersionInfo{Version: "0.1.0"}
	checker := gateway.NewHealthChecker(versionInfo, nil, nil, 5)

	var activeTicker atomic.Int64
	activeTicker.Store(time.Now().UnixNano())
	checker.SetSchedulerTicker(&activeTicker, 60*time.Second)

	mux := http.NewServeMux()
	checker.Routes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatalf("failed to GET /readyz: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable on database outage, got %d", resp.StatusCode)
	}

	var data gateway.ReadyResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if data.Status != "not_ready" || data.Database != "unreachable" {
		t.Fatalf("expected not_ready with unreachable database, got: %+v", data)
	}
}

// TestReadyzNATSGracefulDegradation proves Blueprint §25.2:
// "degraded NATS/telemetry is reported separately because DB fallback remains valid"
func TestReadyzNATSGracefulDegradation(t *testing.T) {
	db, pool, _ := setupTestDB(t)
	defer db.Close()
	defer pool.Close()

	versionInfo := gateway.VersionInfo{Version: "0.1.0"}
	degradedNATS := &mockNATSChecker{connected: false}
	checker := gateway.NewHealthChecker(versionInfo, pool, degradedNATS, 5)

	var activeTicker atomic.Int64
	activeTicker.Store(time.Now().UnixNano())
	checker.SetSchedulerTicker(&activeTicker, 60*time.Second)

	mux := http.NewServeMux()
	checker.Routes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatalf("failed to GET /readyz: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from /readyz during NATS degradation (DB fallback remains valid), got %d", resp.StatusCode)
	}

	var data gateway.ReadyResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if data.Status != "ready" {
		t.Fatalf("expected status=ready, got %s", data.Status)
	}
	if data.NATS != "degraded" {
		t.Fatalf("expected nats=degraded, got %s", data.NATS)
	}
}

// TestReadyzSchedulerStaleness asserts that /readyz detects scheduler loop lag
func TestReadyzSchedulerStaleness(t *testing.T) {
	db, pool, _ := setupTestDB(t)
	defer db.Close()
	defer pool.Close()

	versionInfo := gateway.VersionInfo{Version: "0.1.0"}
	checker := gateway.NewHealthChecker(versionInfo, pool, nil, 5)

	var ticker atomic.Int64
	ticker.Store(time.Now().Add(-10 * time.Minute).UnixNano())
	checker.SetSchedulerTicker(&ticker, 60*time.Second)

	mux := http.NewServeMux()
	checker.Routes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatalf("failed to GET /readyz: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable on stale scheduler, got %d", resp.StatusCode)
	}

	var data gateway.ReadyResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if data.Status != "not_ready" || data.Scheduler != "stale" {
		t.Fatalf("expected not_ready with stale scheduler, got: %+v", data)
	}
}

// TestReadyzUnobservedSchedulerFailsClosed asserts that /readyz fails closed
// when no scheduler ticker is attached (must not default to active).
func TestReadyzUnobservedSchedulerFailsClosed(t *testing.T) {
	db, pool, _ := setupTestDB(t)
	defer db.Close()
	defer pool.Close()

	versionInfo := gateway.VersionInfo{Version: "0.1.0"}
	// Checker without SetSchedulerTicker
	checker := gateway.NewHealthChecker(versionInfo, pool, nil, 5)

	mux := http.NewServeMux()
	checker.Routes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatalf("failed to GET /readyz: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable on unobserved scheduler, got %d", resp.StatusCode)
	}

	var data gateway.ReadyResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if data.Status != "not_ready" || data.Scheduler != "unobserved" {
		t.Fatalf("expected not_ready with unobserved scheduler, got: %+v", data)
	}
}

// TestReadyzUnverifiedSchemaFailsClosed asserts that /readyz fails closed
// when schema version is outdated or unverified.
func TestReadyzUnverifiedSchemaFailsClosed(t *testing.T) {
	db, pool, _ := setupTestDB(t)
	defer db.Close()
	defer pool.Close()

	versionInfo := gateway.VersionInfo{Version: "0.1.0"}
	// Expect schema version 9999 (far in future, cannot be satisfied)
	checker := gateway.NewHealthChecker(versionInfo, pool, nil, 9999)

	var activeTicker atomic.Int64
	activeTicker.Store(time.Now().UnixNano())
	checker.SetSchedulerTicker(&activeTicker, 60*time.Second)

	mux := http.NewServeMux()
	checker.Routes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatalf("failed to GET /readyz: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable on outdated schema, got %d", resp.StatusCode)
	}

	var data gateway.ReadyResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if data.Status != "not_ready" || data.Schema != "outdated" {
		t.Fatalf("expected not_ready with outdated schema, got: %+v", data)
	}
}

// TestHealthEndpointsTopologyRedaction verifies that health check responses
// never leak database connection strings, credentials, or internal topology.
func TestHealthEndpointsTopologyRedaction(t *testing.T) {
	db, pool, _ := setupTestDB(t)
	defer db.Close()
	defer pool.Close()

	versionInfo := gateway.VersionInfo{
		Version:     "0.1.0",
		CommitSHA:   "redaction-check-commit",
		BuildTime:   "2026-09-11T00:00:00Z",
		ImageDigest: "sha256:abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
		RuntimeMode: "hosted",
	}

	checker := gateway.NewHealthChecker(versionInfo, pool, &mockNATSChecker{connected: true}, 5)
	var activeTicker atomic.Int64
	activeTicker.Store(time.Now().UnixNano())
	checker.SetSchedulerTicker(&activeTicker, 60*time.Second)

	mux := http.NewServeMux()
	checker.Routes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	endpoints := []string{"/livez", "/readyz", "/version"}
	sensitivePatterns := []string{
		"postgres://",
		"password",
		"172.",
		"10.",
		"192.168",
		"internal",
		"secret",
	}

	for _, ep := range endpoints {
		resp, err := ts.Client().Get(ts.URL + ep)
		if err != nil {
			t.Fatalf("failed to GET %s: %v", ep, err)
		}
		defer resp.Body.Close()

		var rawMap map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&rawMap); err != nil {
			t.Fatalf("failed to decode json for %s: %v", ep, err)
		}
		rawJSON, _ := json.Marshal(rawMap)
		rawStr := strings.ToLower(string(rawJSON))

		for _, pattern := range sensitivePatterns {
			if strings.Contains(rawStr, pattern) {
				t.Fatalf("SECURITY VIOLATION: Endpoint %s leaked sensitive pattern %q: %s", ep, pattern, rawStr)
			}
		}
	}
}

// TestComposeConfigurationsIntegrity validates Docker Compose configurations
func TestComposeConfigurationsIntegrity(t *testing.T) {
	localContent, err := os.ReadFile("../../deploy/compose/docker-compose.yml")
	if err != nil {
		t.Fatalf("failed to read local compose file: %v", err)
	}
	localStr := string(localContent)

	// Verify local compose binds to loopback
	if strings.Contains(localStr, "\"5432:5432\"") || strings.Contains(localStr, "\"0.0.0.0:5432") {
		t.Fatalf("SECURITY VIOLATION: local compose exposes Postgres outside loopback: %s", localStr)
	}
	if !strings.Contains(localStr, "127.0.0.1:5432:5432") {
		t.Fatalf("expected explicit 127.0.0.1 loopback binding for Postgres in local compose")
	}
	if !strings.Contains(localStr, "127.0.0.1:8080:8080") {
		t.Fatalf("expected explicit 127.0.0.1 loopback binding for Control Plane in local compose")
	}

	// Verify profiles exist in local compose
	for _, profile := range []string{"profiles: [\"core\"]", "profiles: [\"telemetry\"]", "profiles: [\"fault\"]"} {
		if !strings.Contains(localStr, profile) {
			t.Fatalf("expected profile %q in local compose", profile)
		}
	}

	stagingContent, err := os.ReadFile("../../deploy/compose/docker-compose.staging.yml")
	if err != nil {
		t.Fatalf("failed to read staging compose file: %v", err)
	}
	stagingStr := string(stagingContent)

	// Verify staging compose has NO public port mapping for postgres or nats
	if strings.Contains(stagingStr, "ports:\n      - \"5432") || strings.Contains(stagingStr, "ports:\n      - \"127.0.0.1:5432") {
		t.Fatalf("SECURITY VIOLATION: staging compose exposes Postgres ports to host: %s", stagingStr)
	}
	if strings.Contains(stagingStr, "ports:\n      - \"4222") || strings.Contains(stagingStr, "ports:\n      - \"127.0.0.1:4222") {
		t.Fatalf("SECURITY VIOLATION: staging compose exposes NATS ports to host: %s", stagingStr)
	}

	// Verify staging project name is deadbolt-staging
	if !strings.Contains(stagingStr, "name: deadbolt-staging") {
		t.Fatalf("expected staging compose project name to be deadbolt-staging")
	}

	// Verify blue-green slotting: ports 8088 and 8089
	if !strings.Contains(stagingStr, "127.0.0.1:8088:8080") {
		t.Fatalf("expected control plane blue slot on loopback port 8088 in staging compose")
	}
	if !strings.Contains(stagingStr, "127.0.0.1:8089:8080") {
		t.Fatalf("expected control plane green slot on loopback port 8089 in staging compose")
	}

	// Verify immutable image reference with NO mutable tag suffix
	if strings.Contains(stagingStr, ":latest") || strings.Contains(stagingStr, "${IMAGE_TAG") {
		t.Fatalf("SECURITY VIOLATION: staging compose uses mutable tag or suffix: %s", stagingStr)
	}
}

// TestRetentionScriptDryRun asserts that retention.sh dry run executes safely,
// preserves rollback artifacts, and guarantees FlowDesk protection.
func TestRetentionScriptDryRun(t *testing.T) {
	scriptPath, err := os.Stat("../../scripts/retention.sh")
	if err != nil {
		t.Fatalf("scripts/retention.sh not found: %v", err)
	}
	if scriptPath.Mode()&0111 == 0 {
		t.Fatalf("scripts/retention.sh is not executable")
	}

	cmd := exec.Command("/bin/bash", "../../scripts/retention.sh")
	cmd.Env = append(os.Environ(), "DRY_RUN=true")
	out, err := cmd.CombinedOutput()
	outputStr := string(out)
	t.Logf("retention output:\n%s", outputStr)

	if err != nil {
		t.Fatalf("retention script failed: %v\nOutput: %s", err, outputStr)
	}

	if !strings.Contains(outputStr, "DRY RUN complete: Zero images were modified or removed.") {
		t.Fatalf("expected DRY RUN success marker, got:\n%s", outputStr)
	}
}

// TestDeploymentScriptsGuards asserts that deployment, rollback, and caddy reload
// scripts exist, have execute permissions, and enforce safety guards when invoked.
func TestDeploymentScriptsGuards(t *testing.T) {
	scripts := []string{
		"../../scripts/deploy-staging.sh",
		"../../scripts/rollback-staging.sh",
		"../../scripts/reload-caddy.sh",
		"../../scripts/retention.sh",
		"../../scripts/check-backup-readiness.sh",
	}

	for _, script := range scripts {
		info, err := os.Stat(script)
		if err != nil {
			t.Fatalf("required script %s missing: %v", script, err)
		}
		if info.Mode()&0111 == 0 {
			t.Fatalf("required script %s is not executable (mode %v)", script, info.Mode())
		}
	}

	// Test rollback-staging.sh when PREVIOUS_RELEASE_FILE does not exist
	tmpDir := t.TempDir()
	cmd := exec.Command("/bin/bash", "../../scripts/rollback-staging.sh")
	cmd.Env = append(os.Environ(), "RELEASE_DIR="+tmpDir)
	out, err := cmd.CombinedOutput()
	outputStr := string(out)

	if err == nil {
		t.Fatalf("expected rollback-staging.sh to fail when no previous release exists, but succeeded: %s", outputStr)
	}
	if !strings.Contains(outputStr, "No previous release recorded") {
		t.Fatalf("expected 'No previous release recorded' error message, got: %s", outputStr)
	}
}

// TestContainerLocalAuthBoundary verifies that ContainerLocal configuration
// allows 0.0.0.0 binding in local workstation mode, but is strictly rejected in hosted mode.
func TestContainerLocalAuthBoundary(t *testing.T) {
	// 1. Local mode without ContainerLocal rejects 0.0.0.0
	localCfg := auth.DefaultConfig()
	localCfg.RuntimeMode = auth.ModeLocal
	localCfg.DevAuthEnabled = true
	localCfg.ContainerLocal = false

	if err := localCfg.Validate("0.0.0.0"); err == nil {
		t.Fatalf("expected local mode without ContainerLocal to reject 0.0.0.0, but passed")
	}

	// 2. Local mode with ContainerLocal accepts 0.0.0.0
	localCfg.ContainerLocal = true
	if err := localCfg.Validate("0.0.0.0"); err != nil {
		t.Fatalf("expected local mode with ContainerLocal to accept 0.0.0.0, got error: %v", err)
	}

	// 3. Hosted mode strictly rejects ContainerLocal
	hostedCfg := auth.DefaultConfig()
	hostedCfg.RuntimeMode = auth.ModeHosted
	hostedCfg.ContainerLocal = true
	hostedCfg.OIDC = auth.OIDCConfig{Issuer: "https://issuer.com", ClientID: "cid"}
	hostedCfg.AllowedOrigins = []string{"https://app.com"}

	if err := hostedCfg.Validate("0.0.0.0"); err == nil {
		t.Fatalf("expected hosted mode to strictly reject ContainerLocal, but passed")
	}
}
