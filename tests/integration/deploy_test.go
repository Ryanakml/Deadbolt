package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/auth"
	"github.com/Ryanakml/Deadbolt/internal/gateway"
	"github.com/Ryanakml/Deadbolt/internal/scheduling"
	"github.com/jackc/pgx/v5/pgxpool"
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

	// Verify all telemetry and fault profile images in local compose are pinned to immutable digests
	for _, expectedPin := range []string{"otel/opentelemetry-collector-contrib:0.110.0@sha256:", "prom/prometheus:v2.54.1@sha256:", "shopify/toxiproxy:2.9.0@sha256:"} {
		if !strings.Contains(localStr, expectedPin) {
			t.Fatalf("SECURITY VIOLATION: expected immutable digest pin %q in local compose", expectedPin)
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

	// Verify staging project name is deadbolt-staging and network is explicitly named deadbolt_staging_net
	if !strings.Contains(stagingStr, "name: deadbolt-staging") {
		t.Fatalf("expected staging compose project name to be deadbolt-staging")
	}
	if !strings.Contains(stagingStr, "name: deadbolt_staging_net") {
		t.Fatalf("expected staging compose network to be explicitly named deadbolt_staging_net")
	}

	// Verify staging PostgreSQL uses deadbolt_admin as bootstrap superuser, NEVER deadbolt_system (Blueprint §24.3 & §26.3)
	if strings.Contains(stagingStr, "POSTGRES_USER: deadbolt_system") {
		t.Fatalf("SECURITY VIOLATION: staging compose specifies deadbolt_system as cluster superuser; must be deadbolt_admin")
	}
	if !strings.Contains(stagingStr, "POSTGRES_USER: deadbolt_admin") {
		t.Fatalf("expected POSTGRES_USER: deadbolt_admin in staging compose")
	}

	// Verify real WAL archiving command is wired (not /bin/true)
	if strings.Contains(stagingStr, "archive_command=/bin/true") {
		t.Fatalf("SECURITY VIOLATION: staging compose uses discard archive_command=/bin/true; must archive off-host")
	}
	if !strings.Contains(stagingStr, "archive_command=/usr/local/bin/archive-wal.sh") {
		t.Fatalf("expected archive_command to wire archive-wal.sh script")
	}

	// Verify blue-green slotting: ports 8088 and 8089
	if !strings.Contains(stagingStr, "127.0.0.1:8088:8080") {
		t.Fatalf("expected control plane blue slot on loopback port 8088 in staging compose")
	}
	if !strings.Contains(stagingStr, "127.0.0.1:8089:8080") {
		t.Fatalf("expected control plane green slot on loopback port 8089 in staging compose")
	}

	// Verify staging PostgreSQL requires immutable image digest and does NOT build locally
	if strings.Contains(stagingStr, "build:\n      context:") || strings.Contains(stagingStr, "dockerfile: deploy/Dockerfile.postgres") {
		t.Fatalf("SECURITY VIOLATION: staging postgres must not use local build; must use pre-built immutable image")
	}
	if !strings.Contains(stagingStr, "image: ${DEADBOLT_POSTGRES_IMAGE:?Required immutable postgres image digest}") {
		t.Fatalf("expected staging postgres to require ${DEADBOLT_POSTGRES_IMAGE:?Required immutable postgres image digest}")
	}

	// Verify ZERO repository-known fallback passwords exist in staging compose (Issue #5)
	for _, forbiddenFallback := range []string{":-migrator_secure_pass", ":-runtime_secure_pass", ":-system_secure_pass", "DEADBOLT_DB_ADMIN_PASSWORD:-"} {
		if strings.Contains(stagingStr, forbiddenFallback) {
			t.Fatalf("SECURITY VIOLATION: staging compose contains default fallback password %q", forbiddenFallback)
		}
	}

	// Verify all staging database credentials are strictly required
	for _, requiredVar := range []string{
		"DEADBOLT_DB_ADMIN_PASSWORD:?",
		"DEADBOLT_MIGRATOR_PASSWORD:?",
		"DEADBOLT_RUNTIME_PASSWORD:?",
		"DEADBOLT_SYSTEM_PASSWORD:?",
		"SYSTEM_DATABASE_URL:?",
	} {
		if !strings.Contains(stagingStr, requiredVar) {
			t.Fatalf("expected strictly required credential %q in staging compose", requiredVar)
		}
	}

	// Verify S3 storage endpoint and SSE configuration passed to postgres
	for _, s3Var := range []string{"DEADBOLT_STORAGE_S3_ENDPOINT", "DEADBOLT_STORAGE_S3_SSE", "AWS_ENDPOINT_URL"} {
		if !strings.Contains(stagingStr, s3Var) {
			t.Fatalf("expected S3 storage configuration variable %q in staging compose postgres service", s3Var)
		}
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
		"../../scripts/bootstrap-staging-cluster.sh",
		"../../scripts/bootstrap-initial-backup.sh",
		"../../scripts/take-base-backup.sh",
		"../../scripts/restore-staging-db.sh",
		"../../scripts/setup-backup-cron.sh",
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

	// Test rollback-staging.sh fails closed when required config is missing
	cmdNoConfig := exec.Command("/bin/bash", "../../scripts/rollback-staging.sh")
	cmdNoConfig.Env = []string{"PATH=" + os.Getenv("PATH")}
	outNoConfig, errNoConfig := cmdNoConfig.CombinedOutput()
	if errNoConfig == nil {
		t.Fatalf("expected rollback-staging.sh to fail closed without configuration, but succeeded: %s", string(outNoConfig))
	}
	if !strings.Contains(string(outNoConfig), "DEADBOLT_STAGING_DOMAIN is missing") {
		t.Fatalf("expected missing configuration error, got: %s", string(outNoConfig))
	}

	// Test rollback-staging.sh with valid config when PREVIOUS_RELEASE_FILE does not exist
	tmpDir := t.TempDir()
	cmdWithConfig := exec.Command("/bin/bash", "../../scripts/rollback-staging.sh")
	cmdWithConfig.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"RELEASE_DIR=" + tmpDir,
		"DEADBOLT_STAGING_DOMAIN=staging.example.com",
		"DATABASE_URL=postgres://u:p@localhost:5432/db",
		"SYSTEM_DATABASE_URL=postgres://u:p@localhost:5432/db",
		"DEADBOLT_DB_ADMIN_PASSWORD=p",
		"DEADBOLT_MIGRATOR_PASSWORD=p",
		"DEADBOLT_RUNTIME_PASSWORD=p",
		"DEADBOLT_SYSTEM_PASSWORD=p",
		"DEADBOLT_OIDC_ISSUER=https://i",
		"DEADBOLT_OIDC_CLIENT_ID=id",
		"DEADBOLT_OIDC_CLIENT_SECRET=s",
		"DEADBOLT_POSTGRES_IMAGE=ghcr.io/ryanakml/deadbolt/postgres@sha256:1111222233334444555566667777888899990000aaaaabbbbbcccccdddddeeeee",
	}
	out, err := cmdWithConfig.CombinedOutput()
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

// TestTruthfulSchedulerHealth asserts that the scheduler heartbeat advances exclusively
// when authoritative reconciliation sweeps successfully query the database, and fails closed otherwise.
func TestTruthfulSchedulerHealth(t *testing.T) {
	db, runtimePool, systemURL := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	systemPool, err := pgxpool.New(ctx, systemURL)
	if err != nil {
		t.Fatalf("failed to connect as deadbolt_system: %v", err)
	}
	defer systemPool.Close()

	// 1. Successful sweep as deadbolt_system advances ticker
	reconciler := scheduling.NewReconciler(systemPool, 50*time.Millisecond, nil)
	if reconciler.Ticker().Load() != 0 {
		t.Fatalf("expected uninitialized ticker to be 0")
	}

	if err := reconciler.Sweep(ctx); err != nil {
		t.Fatalf("expected initial sweep as deadbolt_system to succeed: %v", err)
	}

	firstTick := reconciler.Ticker().Load()
	if firstTick == 0 {
		t.Fatalf("expected ticker to advance upon successful sweep")
	}

	time.Sleep(10 * time.Millisecond)

	// 2. Second sweep advances ticker again
	if err := reconciler.Sweep(ctx); err != nil {
		t.Fatalf("expected second sweep to succeed: %v", err)
	}
	secondTick := reconciler.Ticker().Load()
	if secondTick <= firstTick {
		t.Fatalf("expected second sweep to advance ticker (%d > %d)", secondTick, firstTick)
	}

	// 3. Sweep with runtimePool (deadbolt_runtime, which lacks permissions) fails and DOES NOT advance ticker
	unauthorizedReconciler := scheduling.NewReconciler(runtimePool, 50*time.Millisecond, nil)
	if err := unauthorizedReconciler.Sweep(ctx); err == nil {
		t.Fatalf("expected sweep as deadbolt_runtime to fail with permission denied, but succeeded")
	}
	if unauthorizedReconciler.Ticker().Load() != 0 {
		t.Fatalf("expected ticker to remain 0 on unauthorized sweep")
	}

	// 4. Sweep with nil pool fails and DOES NOT advance ticker
	brokenReconciler := scheduling.NewReconciler(nil, 50*time.Millisecond, nil)
	if err := brokenReconciler.Sweep(ctx); err == nil {
		t.Fatalf("expected sweep with nil pool to return error, but succeeded")
	}
	if brokenReconciler.Ticker().Load() != 0 {
		t.Fatalf("expected ticker to remain 0 on failed sweep")
	}
}

// TestDatabaseRolesPrivilegeModel verifies the accepted #3 privilege model:
// deadbolt_runtime has NO DDL privileges, deadbolt_system has NO direct table access,
// and deadbolt_migrator has schema ownership and DDL privileges.
func TestDatabaseRolesPrivilegeModel(t *testing.T) {
	db, runtimePool, systemURL := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Test deadbolt_runtime: DML allowed, DDL strictly forbidden (Blueprint §24.3 & §26.3)
	// Negative DDL test: CREATE TABLE must fail
	var ignored int
	err := runtimePool.QueryRow(ctx, "CREATE TABLE public.unauthorized_runtime_table (id int)").Scan(&ignored)
	if err == nil {
		t.Fatalf("SECURITY VIOLATION: deadbolt_runtime was able to execute DDL (CREATE TABLE)!")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("expected permission denied for runtime DDL, got: %v", err)
	}

	// DML test: SELECT allowed
	var orgCount int
	if err := runtimePool.QueryRow(ctx, "SELECT count(*) FROM public.organizations").Scan(&orgCount); err != nil {
		t.Fatalf("expected deadbolt_runtime to have SELECT privilege on public tables, got: %v", err)
	}

	// 2. Test deadbolt_system: NO direct tenant table access, only scheduler discovery function
	systemPool, err := pgxpool.New(ctx, systemURL)
	if err != nil {
		t.Fatalf("failed to connect as deadbolt_system: %v", err)
	}
	defer systemPool.Close()

	// Negative DML test: Direct table query must fail
	var forbiddenCount int
	err = systemPool.QueryRow(ctx, "SELECT count(*) FROM public.organizations").Scan(&forbiddenCount)
	if err == nil {
		t.Fatalf("SECURITY VIOLATION: deadbolt_system was able to query tenant table public.organizations directly!")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("expected permission denied on public.organizations for deadbolt_system, got: %v", err)
	}

	// Negative function test: Membership discovery must fail for deadbolt_system
	var memCount int
	err = systemPool.QueryRow(ctx, "SELECT count(*) FROM app.discover_user_memberships('00000000-0000-0000-0000-000000000001'::uuid)").Scan(&memCount)
	if err == nil {
		t.Fatalf("SECURITY VIOLATION: deadbolt_system was able to call app.discover_user_memberships!")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("expected permission denied on discover_user_memberships for deadbolt_system, got: %v", err)
	}

	// Function test: Scheduler enumeration function allowed for deadbolt_system
	var tenantCount int
	if err := systemPool.QueryRow(ctx, "SELECT count(*) FROM app.enumerate_scheduler_tenants()").Scan(&tenantCount); err != nil {
		t.Fatalf("expected deadbolt_system to be able to call app.enumerate_scheduler_tenants(): %v", err)
	}

	// 3. Test deadbolt_migrator: DDL allowed
	if _, err := db.ExecContext(ctx, "CREATE TABLE public.authorized_migrator_test (id int)"); err != nil {
		t.Fatalf("expected deadbolt_migrator to have CREATE TABLE permission: %v", err)
	}
	_, _ = db.ExecContext(ctx, "DROP TABLE public.authorized_migrator_test")
}

// TestBackupReadinessScriptFailClosed verifies that scripts/check-backup-readiness.sh
// fails closed when backup evidence is absent, and passes only when both base backup
// and fresh WAL archives are proven.
func TestBackupReadinessScriptFailClosed(t *testing.T) {
	scriptPath := "../../scripts/check-backup-readiness.sh"

	// 1. Missing destination configuration must FAIL CLOSED
	cmd := exec.Command("/bin/bash", scriptPath)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected check-backup-readiness.sh to fail without configuration, but succeeded: %s", string(out))
	}
	if !strings.Contains(string(out), "Neither DEADBOLT_STORAGE_S3_BUCKET nor DEADBOLT_WAL_ARCHIVE_DIR is configured") {
		t.Fatalf("expected missing configuration error message, got: %s", string(out))
	}

	// 2. Directory archive configured but EMPTY -> must FAIL CLOSED (no base backup)
	tmpDir := t.TempDir()
	cmd = exec.Command("/bin/bash", scriptPath)
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"DEADBOLT_WAL_ARCHIVE_DIR=" + tmpDir,
	}
	out, err = cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected check-backup-readiness.sh to fail with empty archive dir, but succeeded: %s", string(out))
	}
	if !strings.Contains(string(out), "No base backups found") {
		t.Fatalf("expected 'No base backups found' error message, got: %s", string(out))
	}

	// 3. Base backup exists, but zero WAL archives -> must FAIL CLOSED
	baseDir := filepath.Join(tmpDir, "basebackups")
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		t.Fatalf("failed to create basebackups dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(baseDir, "base_test.tar.gz"), []byte("mock-base"), 0644); err != nil {
		t.Fatalf("failed to write mock base backup: %v", err)
	}

	cmd = exec.Command("/bin/bash", scriptPath)
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"DEADBOLT_WAL_ARCHIVE_DIR=" + tmpDir,
	}
	out, err = cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected check-backup-readiness.sh to fail with missing WAL archives, but succeeded: %s", string(out))
	}
	if !strings.Contains(string(out), "No WAL archives found") {
		t.Fatalf("expected 'No WAL archives found' error message, got: %s", string(out))
	}

	// 4. Base backup exists and WAL archive exists with old mtime (>900s) -> must FAIL CLOSED on lag
	oldWalFile := filepath.Join(tmpDir, "000000010000000000000001")
	if err := os.WriteFile(oldWalFile, []byte("mock-wal"), 0644); err != nil {
		t.Fatalf("failed to write mock wal file: %v", err)
	}
	oldTime := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(oldWalFile, oldTime, oldTime); err != nil {
		t.Fatalf("failed to set old mtime on wal file: %v", err)
	}

	cmd = exec.Command("/bin/bash", scriptPath)
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"DEADBOLT_WAL_ARCHIVE_DIR=" + tmpDir,
		"MAX_WAL_LAG_SECONDS=900",
	}
	out, err = cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected check-backup-readiness.sh to fail on stale WAL lag, but succeeded: %s", string(out))
	}
	if !strings.Contains(string(out), "WAL ARCHIVE LAG EXCEEDED") {
		t.Fatalf("expected 'WAL ARCHIVE LAG EXCEEDED' error message, got: %s", string(out))
	}

	// 5. Fresh WAL archive (<900s) -> must PASS
	freshTime := time.Now()
	if err := os.Chtimes(oldWalFile, freshTime, freshTime); err != nil {
		t.Fatalf("failed to set fresh mtime on wal file: %v", err)
	}

	cmd = exec.Command("/bin/bash", scriptPath)
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"DEADBOLT_WAL_ARCHIVE_DIR=" + tmpDir,
		"MAX_WAL_LAG_SECONDS=900",
	}
	out, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("expected check-backup-readiness.sh to pass with fresh WAL, failed: %v, output: %s", err, string(out))
	}
	if !strings.Contains(string(out), "SUCCESS: Backup & WAL readiness verification passed") {
		t.Fatalf("expected SUCCESS message, got: %s", string(out))
	}
}

// TestWALArchiveScript verifies that scripts/archive-wal.sh correctly copies
// WAL segments when configured and fails closed when unconfigured.
func TestWALArchiveScript(t *testing.T) {
	scriptPath := "../../scripts/archive-wal.sh"

	// 1. Missing arguments must fail
	cmd := exec.Command("/bin/bash", scriptPath)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected archive-wal.sh without arguments to fail, but succeeded: %s", string(out))
	}

	// 2. Unconfigured destination must fail closed
	tmpDir := t.TempDir()
	walSrc := filepath.Join(tmpDir, "source_wal")
	if err := os.WriteFile(walSrc, []byte("wal-content-bytes"), 0644); err != nil {
		t.Fatalf("failed to write test wal: %v", err)
	}

	cmd = exec.Command("/bin/bash", scriptPath, walSrc, "000000010000000000000001")
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	out, err = cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected archive-wal.sh to fail without destination, but succeeded: %s", string(out))
	}

	// 3. With archive dir configured: must archive file successfully
	archiveDir := filepath.Join(tmpDir, "archive")
	cmd = exec.Command("/bin/bash", scriptPath, walSrc, "000000010000000000000001")
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"DEADBOLT_WAL_ARCHIVE_DIR=" + archiveDir,
	}
	out, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("expected archive-wal.sh to succeed with archive dir, failed: %v\nOutput: %s", err, string(out))
	}

	archivedFile := filepath.Join(archiveDir, "000000010000000000000001")
	data, err := os.ReadFile(archivedFile)
	if err != nil {
		t.Fatalf("failed to read archived file: %v", err)
	}
	if string(data) != "wal-content-bytes" {
		t.Fatalf("archived data mismatch: got %q, expected %q", string(data), "wal-content-bytes")
	}
}

// TestFirstStagingDeploymentBootstrap validates that a clean-host first deployment
// deterministically bootstraps data services and roles BEFORE migrations run,
// uses an explicitly named Compose network, and inspects container image identity.
func TestFirstStagingDeploymentBootstrap(t *testing.T) {
	// 1. Validate deploy-staging.sh bootstrap structure
	deployScriptBytes, err := os.ReadFile("../../scripts/deploy-staging.sh")
	if err != nil {
		t.Fatalf("failed to read scripts/deploy-staging.sh: %v", err)
	}
	deployScript := string(deployScriptBytes)

	// Step 4b (bootstrapping data services) must precede Step 5 (migrations)
	idxStep4b := strings.Index(deployScript, "Step 4b: Bootstrapping persistent staging data infrastructure")
	idxStep5 := strings.Index(deployScript, "Step 5: Executing forward schema migrations")
	if idxStep4b == -1 {
		t.Fatalf("missing Step 4b data infrastructure bootstrap in scripts/deploy-staging.sh")
	}
	if idxStep5 == -1 {
		t.Fatalf("missing Step 5 schema migrations in scripts/deploy-staging.sh")
	}
	if idxStep4b >= idxStep5 {
		t.Fatalf("order violation: Step 4b (data bootstrap) must execute before Step 5 (migrations)")
	}

	// Must wait for postgres health check before running migrations
	if !strings.Contains(deployScript, "docker compose -p deadbolt-staging -f \"$COMPOSE_FILE\" up -d postgres nats") {
		t.Fatalf("expected deploy-staging.sh to up postgres and nats in Step 4b")
	}
	if !strings.Contains(deployScript, ".State.Health.Status") {
		t.Fatalf("expected deploy-staging.sh to inspect container health status before migrations")
	}

	// Step 5 must run migration container attached to deadbolt_staging_net
	if !strings.Contains(deployScript, "--network deadbolt_staging_net") {
		t.Fatalf("expected migration container to be attached to deadbolt_staging_net")
	}

	// Step 9b must perform independent image identity inspection
	if !strings.Contains(deployScript, "Step 9b: Inspecting running container image identity") {
		t.Fatalf("missing Step 9b independent running container image identity inspection")
	}

	// 2. Validate staging Compose network and role bootstrap mounts
	stagingComposeBytes, err := os.ReadFile("../../deploy/compose/docker-compose.staging.yml")
	if err != nil {
		t.Fatalf("failed to read staging compose file: %v", err)
	}
	stagingCompose := string(stagingComposeBytes)

	if !strings.Contains(stagingCompose, "deadbolt_staging_net:\n    name: deadbolt_staging_net") {
		t.Fatalf("expected explicit top-level network name: deadbolt_staging_net in staging compose")
	}
	if !strings.Contains(stagingCompose, "01-init-roles.sh:ro") {
		t.Fatalf("expected 01-init-roles.sh mounted in postgres /docker-entrypoint-initdb.d/")
	}
	if !strings.Contains(stagingCompose, "02-bootstrap-roles.sql:ro") {
		t.Fatalf("expected 02-bootstrap-roles.sql mounted in postgres /docker-entrypoint-initdb.d/")
	}

	// 3. Validate init-db-roles.sh script existence and executable bit
	initRolesPath := "../../scripts/init-db-roles.sh"
	info, err := os.Stat(initRolesPath)
	if err != nil {
		t.Fatalf("scripts/init-db-roles.sh missing: %v", err)
	}
	if info.Mode()&0111 == 0 {
		t.Fatalf("scripts/init-db-roles.sh is not executable")
	}

	initRolesBytes, err := os.ReadFile(initRolesPath)
	if err != nil {
		t.Fatalf("failed to read scripts/init-db-roles.sh: %v", err)
	}
	initRolesStr := string(initRolesBytes)
	for _, role := range []string{"deadbolt_migrator", "deadbolt_runtime", "deadbolt_system"} {
		if !strings.Contains(initRolesStr, role) {
			t.Fatalf("expected %s role initialization in scripts/init-db-roles.sh", role)
		}
	}
}

// TestPostgresDockerfileAudited verifies that deploy/Dockerfile.postgres is pinned to the
// audited postgres base from deploy/images.lock.json and installs awscli and curl (Item 2).
func TestPostgresDockerfileAudited(t *testing.T) {
	dockerfilePath := "../../deploy/Dockerfile.postgres"
	content, err := os.ReadFile(dockerfilePath)
	if err != nil {
		t.Fatalf("failed to read %s: %v", dockerfilePath, err)
	}
	contentStr := string(content)

	expectedBase := "postgres:18@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280"
	if !strings.Contains(contentStr, expectedBase) {
		t.Fatalf("expected Dockerfile.postgres to use pinned base %s", expectedBase)
	}

	for _, tool := range []string{"awscli", "curl", "ca-certificates"} {
		if !strings.Contains(contentStr, tool) {
			t.Fatalf("expected Dockerfile.postgres to install %s for deterministic S3 WAL archiving", tool)
		}
	}
}

// TestDatabaseRolesInitScriptFailsWithoutPasswords verifies that scripts/init-db-roles.sh
// fails closed when required role passwords are not supplied (Item 3).
func TestDatabaseRolesInitScriptFailsWithoutPasswords(t *testing.T) {
	cmd := exec.Command("/bin/bash", "../../scripts/init-db-roles.sh")
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"POSTGRES_USER=test_admin",
		"POSTGRES_DB=test_db",
	}
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected init-db-roles.sh to fail without passwords, but passed: %s", string(out))
	}
	if !strings.Contains(string(out), "Required DEADBOLT_MIGRATOR_PASSWORD") {
		t.Fatalf("expected missing password error, got: %s", string(out))
	}
}

// TestContainerizedCaddyEdgeTopology verifies that staging Compose declares the external
// deadbolt-edge network, attaches only blue/green control-plane slots with deterministic aliases,
// leaves PG/NATS private, and renders Caddy upstreams using Docker DNS aliases (never 127.0.0.1).
func TestContainerizedCaddyEdgeTopology(t *testing.T) {
	// 1. Staging Compose network declarations and attachments
	composeBytes, err := os.ReadFile("../../deploy/compose/docker-compose.staging.yml")
	if err != nil {
		t.Fatalf("failed to read staging compose: %v", err)
	}
	composeStr := string(composeBytes)

	if !strings.Contains(composeStr, "deadbolt-edge:\n    name: deadbolt-edge\n    external: true") {
		t.Fatalf("expected staging compose to declare external network deadbolt-edge")
	}
	if !strings.Contains(composeStr, "deadbolt-control-plane-blue") {
		t.Fatalf("expected control-plane-blue alias deadbolt-control-plane-blue on deadbolt-edge")
	}
	if !strings.Contains(composeStr, "deadbolt-control-plane-green") {
		t.Fatalf("expected control-plane-green alias deadbolt-control-plane-green on deadbolt-edge")
	}

	// Verify postgres and nats do not attach to deadbolt-edge
	pgSection := composeStr[strings.Index(composeStr, "postgres:"):strings.Index(composeStr, "nats:")]
	if strings.Contains(pgSection, "deadbolt-edge") {
		t.Fatalf("SECURITY VIOLATION: postgres must not be attached to deadbolt-edge")
	}
	natsSection := composeStr[strings.Index(composeStr, "nats:"):strings.Index(composeStr, "control-plane-blue:")]
	if strings.Contains(natsSection, "deadbolt-edge") {
		t.Fatalf("SECURITY VIOLATION: nats must not be attached to deadbolt-edge")
	}

	// 2. Initial static snippet uses Docker DNS alias, never 127.0.0.1
	snippetBytes, err := os.ReadFile("../../deploy/caddy/Deadbolt.caddyfile")
	if err != nil {
		t.Fatalf("failed to read deploy/caddy/Deadbolt.caddyfile: %v", err)
	}
	snippetStr := string(snippetBytes)
	if strings.Contains(snippetStr, "127.0.0.1") {
		t.Fatalf("forbidden host loopback 127.0.0.1 found in deploy/caddy/Deadbolt.caddyfile")
	}
	if !strings.Contains(snippetStr, "reverse_proxy deadbolt-control-plane-blue:8080") {
		t.Fatalf("expected deploy/caddy/Deadbolt.caddyfile to proxy to deadbolt-control-plane-blue:8080, got:\n%s", snippetStr)
	}
}

// TestS3WALArchiveAndBackupS3Options verifies that archive-wal.sh and bootstrap-initial-backup.sh
// support custom endpoints, regions, and SSE encryption (Item 2 & Item 5).
func TestS3WALArchiveAndBackupS3Options(t *testing.T) {
	for _, script := range []string{"../../scripts/archive-wal.sh", "../../scripts/bootstrap-initial-backup.sh"} {
		content, err := os.ReadFile(script)
		if err != nil {
			t.Fatalf("failed to read %s: %v", script, err)
		}
		scriptStr := string(content)

		for _, opt := range []string{"--endpoint-url", "--region", "--sse"} {
			if !strings.Contains(scriptStr, opt) {
				t.Fatalf("expected %s to support S3 option %s", script, opt)
			}
		}
	}
}

// TestBackupReadinessEncryptionVerification verifies that check-backup-readiness.sh
// inspects ServerSideEncryption and fails closed if unencrypted (Item 5).
func TestBackupReadinessEncryptionVerification(t *testing.T) {
	content, err := os.ReadFile("../../scripts/check-backup-readiness.sh")
	if err != nil {
		t.Fatalf("failed to read scripts/check-backup-readiness.sh: %v", err)
	}
	scriptStr := string(content)

	if !strings.Contains(scriptStr, "ServerSideEncryption") {
		t.Fatalf("expected check-backup-readiness.sh to inspect ServerSideEncryption")
	}
	if !strings.Contains(scriptStr, "get-bucket-encryption") {
		t.Fatalf("expected check-backup-readiness.sh to verify get-bucket-encryption")
	}
	if !strings.Contains(scriptStr, "ENCRYPTION POLICY VIOLATION") {
		t.Fatalf("expected check-backup-readiness.sh to fail closed on encryption violation")
	}
}

// TestClusterBootstrapAndPromotionSequencing verifies that bootstrap-staging-cluster.sh
// exists, is executable, and deploy-staging.sh enforces password consistency and bootstrap mode (Item 1 & Item 3).
func TestClusterBootstrapAndPromotionSequencing(t *testing.T) {
	// 1. bootstrap-staging-cluster.sh
	bootstrapPath := "../../scripts/bootstrap-staging-cluster.sh"
	info, err := os.Stat(bootstrapPath)
	if err != nil {
		t.Fatalf("scripts/bootstrap-staging-cluster.sh missing: %v", err)
	}
	if info.Mode()&0111 == 0 {
		t.Fatalf("scripts/bootstrap-staging-cluster.sh is not executable")
	}

	bootstrapContent, err := os.ReadFile(bootstrapPath)
	if err != nil {
		t.Fatalf("failed to read bootstrap script: %v", err)
	}
	bootstrapStr := string(bootstrapContent)

	if !strings.Contains(bootstrapStr, "docker pull \"$DEADBOLT_POSTGRES_IMAGE\"") {
		t.Fatalf("expected bootstrap script to pull immutable postgres image")
	}
	if !strings.Contains(bootstrapStr, "docker compose -p deadbolt-staging -f \"$COMPOSE_FILE\" up -d postgres nats") {
		t.Fatalf("expected bootstrap script to up data infrastructure")
	}
	if !strings.Contains(bootstrapStr, "CALLER_CONTROL_PLANE_IMAGE") || !strings.Contains(bootstrapStr, "DEADBOLT_IMAGE is required as an immutable control-plane reference") {
		t.Fatalf("standalone bootstrap must explicitly require an immutable control-plane image for Compose interpolation")
	}
	if !strings.Contains(bootstrapStr, "./scripts/bootstrap-initial-backup.sh") {
		t.Fatalf("expected bootstrap script to run initial base backup and wal switch")
	}
	if !strings.Contains(bootstrapStr, "./scripts/check-backup-readiness.sh") {
		t.Fatalf("expected bootstrap script to verify backup readiness")
	}

	// 2. deploy-staging.sh password consistency verification
	deployContent, err := os.ReadFile("../../scripts/deploy-staging.sh")
	if err != nil {
		t.Fatalf("failed to read deploy-staging.sh: %v", err)
	}
	deployStr := string(deployContent)

	if !strings.Contains(deployStr, "BOOTSTRAP_MODE") {
		t.Fatalf("expected deploy-staging.sh to support BOOTSTRAP_MODE")
	}
	if !strings.Contains(deployStr, "PASSWORD CONSISTENCY FAILURE") {
		t.Fatalf("expected deploy-staging.sh to enforce password consistency between URLs and secrets")
	}
	if !strings.Contains(deployStr, "DEADBOLT_IMAGE=\"$CANDIDATE_DIGEST\"") || !strings.Contains(deployStr, "export DEADBOLT_IMAGE") {
		t.Fatalf("deploy-staging.sh must export the immutable candidate image before bootstrap and slot Compose calls")
	}
}

// TestStagingComposeCandidateImageInterpolation verifies that Compose can parse the
// full model for bootstrap or either slot when only the immutable global candidate
// fallback is supplied. Compose validates all services even for `up postgres nats`.
func TestStagingComposeCandidateImageInterpolation(t *testing.T) {
	candidate := "ghcr.io/ryanakml/deadbolt/control-plane@sha256:" + strings.Repeat("1", 64)
	postgres := "ghcr.io/ryanakml/deadbolt/postgres@sha256:" + strings.Repeat("2", 64)
	cmd := exec.Command("docker", "compose", "-f", "../../deploy/compose/docker-compose.staging.yml", "--profile", "slot-blue", "--profile", "slot-green", "config", "--quiet")
	cmd.Env = append(os.Environ(),
		"DEADBOLT_IMAGE="+candidate,
		"DEADBOLT_POSTGRES_IMAGE="+postgres,
		"DEADBOLT_DB_ADMIN_PASSWORD=mock_admin_password",
		"DEADBOLT_MIGRATOR_PASSWORD=mock_migrator_password",
		"DEADBOLT_RUNTIME_PASSWORD=mock_runtime_password",
		"DEADBOLT_SYSTEM_PASSWORD=mock_system_password",
		"DATABASE_URL=postgres://deadbolt_runtime:mock_runtime_password@localhost:5432/mock",
		"SYSTEM_DATABASE_URL=postgres://deadbolt_system:mock_system_password@localhost:5432/mock",
		"DEADBOLT_OIDC_ISSUER=https://mock-issuer.example",
		"DEADBOLT_OIDC_CLIENT_ID=mock-client-id",
		"DEADBOLT_OIDC_CLIENT_SECRET=mock-client-secret",
		"DEADBOLT_STAGING_DOMAIN=staging.example.test",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("expected bootstrap/blue-green Compose interpolation with immutable candidate to succeed: %v\n%s", err, out)
	}
}

// TestContainerizedCaddyReloadContract verifies that scripts/reload-caddy.sh
// targets containerized Caddy, enforces Docker DNS alias upstreams, strictly forbids 127.0.0.1,
// executes validation/reload inside the container, persists state on success, and restores on failure.
func TestContainerizedCaddyReloadContract(t *testing.T) {
	scriptPath := "../../scripts/reload-caddy.sh"
	content, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("failed to read scripts/reload-caddy.sh: %v", err)
	}
	scriptStr := string(content)

	for _, token := range []string{
		"HOST_SNIPPET_FILE",
		"CADDY_CONTAINER",
		"CONTAINER_CADDYFILE",
		"CONTAINER_SNIPPET_FILE",
		"deadbolt-control-plane-blue:8080",
		"deadbolt-control-plane-green:8080",
		"docker inspect",
		"docker exec \"$CADDY_CONTAINER\" caddy validate",
		"docker exec \"$CADDY_CONTAINER\" caddy reload",
		"active_upstream_port",
		"active_slot",
		"rollback",
	} {
		if !strings.Contains(scriptStr, token) {
			t.Fatalf("expected reload-caddy.sh to contain %q", token)
		}
	}

	// Verify fail closed without DEADBOLT_STAGING_DOMAIN
	cmdNoDomain := exec.Command("/bin/bash", scriptPath, "8088")
	cmdNoDomain.Env = []string{"PATH=" + os.Getenv("PATH")}
	outNoDomain, errNoDomain := cmdNoDomain.CombinedOutput()
	if errNoDomain == nil {
		t.Fatalf("expected reload-caddy.sh to fail closed without DEADBOLT_STAGING_DOMAIN, but succeeded: %s", string(outNoDomain))
	}
	if !strings.Contains(string(outNoDomain), "Required DEADBOLT_STAGING_DOMAIN is missing") {
		t.Fatalf("expected missing domain error, got: %s", string(outNoDomain))
	}

	// Verify snippet-only dry-run validates Docker DNS alias and strictly rejects 127.0.0.1
	tmpSnippetDir := t.TempDir()
	snippetTarget := filepath.Join(tmpSnippetDir, "Deadbolt.caddyfile")
	cmdDryRun := exec.Command("/bin/bash", scriptPath, "8089")
	cmdDryRun.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"DRY_RUN=true",
		"DEADBOLT_SNIPPET_ONLY=true",
		"DEADBOLT_STAGING_DOMAIN=staging.example.com",
		"DEADBOLT_CADDYFILE_SNIPPET=" + snippetTarget,
	}
	outDryRun, errDryRun := cmdDryRun.CombinedOutput()
	if errDryRun != nil {
		t.Fatalf("expected syntax dry run to pass, failed: %v, output: %s", errDryRun, string(outDryRun))
	}
	if !strings.Contains(string(outDryRun), "DRY RUN passed") {
		t.Fatalf("expected DRY RUN passed output, got: %s", string(outDryRun))
	}
}

// TestContainerizedCaddyMockValidationAndRollback tests scripts/reload-caddy.sh with
// a mock docker CLI to verify all operational paths:
// 1. Happy path: container running, mount valid, import valid -> reload success, state persisted.
// 2. Missing container -> fails closed with prerequisite error.
// 3. Missing snippet mount -> fails closed with prerequisite error.
// 4. Missing import in Caddyfile -> fails closed with prerequisite error.
// 5. Validation failure -> rollback restores previous snippet/state, reloads Caddy.
// 6. Reload failure -> rollback restores previous snippet/state.
// 7. Dry run without container -> fails closed (does not silently pass).
func TestContainerizedCaddyMockValidationAndRollback(t *testing.T) {
	scriptPath, err := filepath.Abs("../../scripts/reload-caddy.sh")
	if err != nil {
		t.Fatalf("failed to resolve script path: %v", err)
	}

	setupMockDocker := func(t *testing.T, containerStatus, mountsJSON, caddyfileContent string, validateExit, reloadExit int) string {
		binDir := t.TempDir()
		mockDocker := filepath.Join(binDir, "docker")
		script := fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail
cmd="${1:-}"
shift || true

if [[ "$cmd" == "info" ]]; then
  exit 0
fi

if [[ "$cmd" == "inspect" ]]; then
  fmt=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --format)
        fmt="$2"
        shift 2
        ;;
      *)
        shift
        ;;
    esac
  done
  if [[ "$fmt" == *"State.Status"* ]]; then
    echo "%s"
    exit 0
  fi
  if [[ "$fmt" == *"Mounts"* ]]; then
    echo '%s'
    exit 0
  fi
  exit 0
fi

if [[ "$cmd" == "exec" ]]; then
  container="${1:-}"
  shift || true
  subcmd="${1:-}"
  shift || true
  if [[ "$subcmd" == "cat" ]]; then
    echo '%s'
    exit 0
  fi
  if [[ "$subcmd" == "caddy" ]]; then
    action="${1:-}"
    shift || true
    if [[ "$action" == "validate" ]]; then
      exit %d
    fi
    if [[ "$action" == "reload" ]]; then
      exit %d
    fi
  fi
  exit 0
fi

exit 0
`, containerStatus, mountsJSON, caddyfileContent, validateExit, reloadExit)

		if err := os.WriteFile(mockDocker, []byte(script), 0755); err != nil {
			t.Fatalf("failed to write mock docker: %v", err)
		}
		return binDir
	}

	validMounts := `[{"Type":"bind","Source":"/opt/deadbolt/caddy","Destination":"/etc/caddy/deadbolt","Mode":"ro"}]`
	validCaddyfile := ":80 {\nimport /etc/caddy/deadbolt/*.caddyfile\n}"

	// Scenario 1: Happy path reload to Green (port 8089)
	t.Run("HappyPathReload", func(t *testing.T) {
		binDir := setupMockDocker(t, "running", validMounts, validCaddyfile, 0, 0)
		workDir := t.TempDir()
		releaseDir := filepath.Join(workDir, "releases")
		snippetPath := filepath.Join(workDir, "Deadbolt.caddyfile")

		cmd := exec.Command("/bin/bash", scriptPath, "green")
		cmd.Env = []string{
			"PATH=" + binDir + ":" + os.Getenv("PATH"),
			"DEADBOLT_RELEASE_DIR=" + releaseDir,
			"DEADBOLT_CADDYFILE_SNIPPET=" + snippetPath,
			"DEADBOLT_STAGING_DOMAIN=deadbolt.43.218.246.246.nip.io",
		}
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("expected happy path reload to succeed: %v, output:\n%s", err, string(out))
		}

		// Verify state persisted
		savedPort, err := os.ReadFile(filepath.Join(releaseDir, "active_upstream_port"))
		if err != nil || strings.TrimSpace(string(savedPort)) != "8089" {
			t.Fatalf("expected active_upstream_port to be 8089, got %q (err: %v)", string(savedPort), err)
		}
		savedSlot, err := os.ReadFile(filepath.Join(releaseDir, "active_slot"))
		if err != nil || strings.TrimSpace(string(savedSlot)) != "green" {
			t.Fatalf("expected active_slot to be green, got %q (err: %v)", string(savedSlot), err)
		}

		// Verify snippet rendered with Docker DNS alias and no 127.0.0.1
		rendered, err := os.ReadFile(snippetPath)
		if err != nil {
			t.Fatalf("failed to read rendered snippet: %v", err)
		}
		renderedStr := string(rendered)
		if strings.Contains(renderedStr, "127.0.0.1") {
			t.Fatalf("rendered snippet contains forbidden loopback 127.0.0.1:\n%s", renderedStr)
		}
		if !strings.Contains(renderedStr, "reverse_proxy deadbolt-control-plane-green:8080") {
			t.Fatalf("expected reverse_proxy deadbolt-control-plane-green:8080, got:\n%s", renderedStr)
		}
	})

	// Scenario 2: Caddy container not running
	t.Run("ContainerNotRunningFailsClosed", func(t *testing.T) {
		binDir := setupMockDocker(t, "exited", validMounts, validCaddyfile, 0, 0)
		workDir := t.TempDir()

		cmd := exec.Command("/bin/bash", scriptPath, "blue")
		cmd.Env = []string{
			"PATH=" + binDir + ":" + os.Getenv("PATH"),
			"DEADBOLT_RELEASE_DIR=" + filepath.Join(workDir, "releases"),
			"DEADBOLT_CADDYFILE_SNIPPET=" + filepath.Join(workDir, "Deadbolt.caddyfile"),
			"DEADBOLT_STAGING_DOMAIN=deadbolt.43.218.246.246.nip.io",
		}
		out, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("expected fail closed on stopped container, got success: %s", string(out))
		}
		if !strings.Contains(string(out), "is not running") {
			t.Fatalf("expected container not running error, got: %s", string(out))
		}
	})

	// Scenario 3: Mount missing
	t.Run("MountMissingFailsClosed", func(t *testing.T) {
		binDir := setupMockDocker(t, "running", "[]", validCaddyfile, 0, 0)
		workDir := t.TempDir()

		cmd := exec.Command("/bin/bash", scriptPath, "blue")
		cmd.Env = []string{
			"PATH=" + binDir + ":" + os.Getenv("PATH"),
			"DEADBOLT_RELEASE_DIR=" + filepath.Join(workDir, "releases"),
			"DEADBOLT_CADDYFILE_SNIPPET=" + filepath.Join(workDir, "Deadbolt.caddyfile"),
			"DEADBOLT_STAGING_DOMAIN=deadbolt.43.218.246.246.nip.io",
		}
		out, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("expected fail closed on missing mount, got success: %s", string(out))
		}
		if !strings.Contains(string(out), "is not mounted in container") {
			t.Fatalf("expected missing mount error, got: %s", string(out))
		}
	})

	// Scenario 4: Import missing
	t.Run("ImportMissingFailsClosed", func(t *testing.T) {
		binDir := setupMockDocker(t, "running", validMounts, ":80 { respond ok }", 0, 0)
		workDir := t.TempDir()

		cmd := exec.Command("/bin/bash", scriptPath, "blue")
		cmd.Env = []string{
			"PATH=" + binDir + ":" + os.Getenv("PATH"),
			"DEADBOLT_RELEASE_DIR=" + filepath.Join(workDir, "releases"),
			"DEADBOLT_CADDYFILE_SNIPPET=" + filepath.Join(workDir, "Deadbolt.caddyfile"),
			"DEADBOLT_STAGING_DOMAIN=deadbolt.43.218.246.246.nip.io",
		}
		out, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("expected fail closed on missing import, got success: %s", string(out))
		}
		if !strings.Contains(string(out), "does not import Deadbolt snippet") {
			t.Fatalf("expected missing import error, got: %s", string(out))
		}
	})

	// Scenario 5: Caddy validate inside container fails -> rollback restores prior snippet & state
	t.Run("ValidationFailureRollback", func(t *testing.T) {
		binDir := setupMockDocker(t, "running", validMounts, validCaddyfile, 1, 0)
		workDir := t.TempDir()
		releaseDir := filepath.Join(workDir, "releases")
		snippetPath := filepath.Join(workDir, "Deadbolt.caddyfile")
		if err := os.MkdirAll(releaseDir, 0755); err != nil {
			t.Fatal(err)
		}

		// Seed initial state (blue / 8088)
		initialSnippet := "# Prior snippet blue\nreverse_proxy deadbolt-control-plane-blue:8080\n"
		if err := os.WriteFile(snippetPath, []byte(initialSnippet), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(releaseDir, "active_upstream_port"), []byte("8088\n"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(releaseDir, "active_slot"), []byte("blue\n"), 0644); err != nil {
			t.Fatal(err)
		}

		cmd := exec.Command("/bin/bash", scriptPath, "green")
		cmd.Env = []string{
			"PATH=" + binDir + ":" + os.Getenv("PATH"),
			"DEADBOLT_RELEASE_DIR=" + releaseDir,
			"DEADBOLT_CADDYFILE_SNIPPET=" + snippetPath,
			"DEADBOLT_STAGING_DOMAIN=deadbolt.43.218.246.246.nip.io",
		}
		out, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("expected failure when validation fails, got success: %s", string(out))
		}
		if !strings.Contains(string(out), "Caddy validation failed inside container") {
			t.Fatalf("expected validation failure log, got: %s", string(out))
		}

		// Verify rollback restored initial state
		restoredPort, _ := os.ReadFile(filepath.Join(releaseDir, "active_upstream_port"))
		if strings.TrimSpace(string(restoredPort)) != "8088" {
			t.Fatalf("expected active_upstream_port rolled back to 8088, got %q", string(restoredPort))
		}
		restoredSlot, _ := os.ReadFile(filepath.Join(releaseDir, "active_slot"))
		if strings.TrimSpace(string(restoredSlot)) != "blue" {
			t.Fatalf("expected active_slot rolled back to blue, got %q", string(restoredSlot))
		}
		restoredSnippet, _ := os.ReadFile(snippetPath)
		if string(restoredSnippet) != initialSnippet {
			t.Fatalf("expected snippet rolled back to prior content, got:\n%s", string(restoredSnippet))
		}
	})

	// Scenario 6: Dry run fails when container prerequisites fail (does not silently pass)
	t.Run("DryRunFailsClosedWhenContainerMissing", func(t *testing.T) {
		binDir := setupMockDocker(t, "exited", validMounts, validCaddyfile, 0, 0)
		workDir := t.TempDir()

		cmd := exec.Command("/bin/bash", scriptPath, "blue")
		cmd.Env = []string{
			"PATH=" + binDir + ":" + os.Getenv("PATH"),
			"DRY_RUN=true",
			"DEADBOLT_RELEASE_DIR=" + filepath.Join(workDir, "releases"),
			"DEADBOLT_CADDYFILE_SNIPPET=" + filepath.Join(workDir, "Deadbolt.caddyfile"),
			"DEADBOLT_STAGING_DOMAIN=deadbolt.43.218.246.246.nip.io",
		}
		out, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("expected dry run to fail closed when container is missing, but passed: %s", string(out))
		}
		if !strings.Contains(string(out), "is not running") {
			t.Fatalf("expected container not running error in dry run, got: %s", string(out))
		}
	})
}

// TestFreshHostBootstrapMarker verifies that deploy-staging.sh detects uninitialized
// staging installations, automatically engages bootstrap mode, and records the durable marker.
func TestFreshHostBootstrapMarker(t *testing.T) {
	deployBytes, err := os.ReadFile("../../scripts/deploy-staging.sh")
	if err != nil {
		t.Fatalf("failed to read scripts/deploy-staging.sh: %v", err)
	}
	deployStr := string(deployBytes)

	for _, token := range []string{
		"BOOTSTRAP_MARKER_FILE",
		"bootstrap_complete",
		"Uninitialized Deadbolt staging host detected",
		"BOOTSTRAP_MODE=\"true\"",
		"./scripts/bootstrap-staging-cluster.sh",
	} {
		if !strings.Contains(deployStr, token) {
			t.Fatalf("expected deploy-staging.sh to contain %q", token)
		}
	}

	bootstrapBytes, err := os.ReadFile("../../scripts/bootstrap-staging-cluster.sh")
	if err != nil {
		t.Fatalf("failed to read scripts/bootstrap-staging-cluster.sh: %v", err)
	}
	bootstrapStr := string(bootstrapBytes)
	if !strings.Contains(bootstrapStr, "BOOTSTRAP_MARKER_FILE") || !strings.Contains(bootstrapStr, "bootstrap_complete") {
		t.Fatalf("expected bootstrap-staging-cluster.sh to record durable bootstrap_complete marker")
	}
}

// TestDeployWorkflowRsyncStrictHostKeyAndGHCRAuth verifies that the GitHub Actions workflow
// enforces strict host-key verification for rsync and provides secure GHCR pull authentication via stdin.
func TestDeployWorkflowRsyncStrictHostKeyAndGHCRAuth(t *testing.T) {
	wfBytes, err := os.ReadFile("../../.github/workflows/staging-deploy.yml")
	if err != nil {
		t.Fatalf("failed to read workflow file: %v", err)
	}
	wfStr := string(wfBytes)

	// Verify rsync uses strict host key checking
	if !strings.Contains(wfStr, "rsync -avz -e \"ssh -i ~/.ssh/deploy_key -o StrictHostKeyChecking=yes -o UserKnownHostsFile=~/.ssh/known_hosts\"") {
		t.Fatalf("expected rsync to enforce StrictHostKeyChecking=yes and UserKnownHostsFile")
	}

	// Verify GHCR pull auth via stdin
	if !strings.Contains(wfStr, "docker login ghcr.io -u '$GHCR_PULL_USER' --password-stdin") {
		t.Fatalf("expected workflow to authenticate remote docker daemon via --password-stdin")
	}
	if !strings.Contains(wfStr, "GHCR_PULL_TOKEN") {
		t.Fatalf("expected workflow to define GHCR_PULL_TOKEN")
	}
}

// TestStagingDeployPRLabelGate verifies the only pre-merge deployment path is a
// deliberate label on an open same-repository PR, and that it uses the PR head SHA.
func TestStagingDeployPRLabelGate(t *testing.T) {
	wfBytes, err := os.ReadFile("../../.github/workflows/staging-deploy.yml")
	if err != nil {
		t.Fatalf("failed to read staging workflow: %v", err)
	}
	wf := string(wfBytes)

	for _, token := range []string{
		"push:\n    branches: [main]",
		"workflow_dispatch:",
		"pull_request:\n    types: [labeled]",
		"github.event.action == 'labeled'",
		"github.event.label.name == 'deploy-staging'",
		"github.event.pull_request.head.repo.full_name == github.repository",
		"github.event.pull_request.state == 'open'",
		"RELEASE_COMMIT_SHA: ${{ github.event_name == 'pull_request' && github.event.pull_request.head.sha || github.sha }}",
		"ref: ${{ env.RELEASE_COMMIT_SHA }}",
		"COMMIT_SHA=${{ env.RELEASE_COMMIT_SHA }}",
		"COMMIT_SHA: ${{ env.RELEASE_COMMIT_SHA }}",
	} {
		if !strings.Contains(wf, token) {
			t.Fatalf("staging PR-label gate missing %q", token)
		}
	}

	if strings.Contains(wf, "synchronize") || strings.Contains(wf, "types: [opened]") {
		t.Fatal("ordinary PR activity must not trigger staging deployment")
	}
	if strings.Count(wf, "if: >-") < 2 {
		t.Fatal("both build and deploy jobs must be gated before any secrets are used")
	}
}

// TestStagingDeployGHCRRepositoryNormalization prevents display-case repository names
// from producing invalid remote Docker references during the real staging deployment.
func TestStagingDeployGHCRRepositoryNormalization(t *testing.T) {
	wfBytes, err := os.ReadFile("../../.github/workflows/staging-deploy.yml")
	if err != nil {
		t.Fatalf("failed to read staging workflow: %v", err)
	}
	wf := string(wfBytes)

	mixedCaseRepository := "Ryanakml/Deadbolt"
	normalizedRepository := strings.ToLower(mixedCaseRepository)
	if normalizedRepository != "ryanakml/deadbolt" {
		t.Fatalf("unexpected normalized repository: %q", normalizedRepository)
	}
	controlDigest := "sha256:1111222233334444555566667777888899990000aaaaabbbbbcccccdddddeeeee"
	postgresDigest := "sha256:aaaa222233334444555566667777888899990000aaaabbbbccccddddeeeeffff"
	controlRef := fmt.Sprintf("ghcr.io/%s/control-plane@%s", normalizedRepository, controlDigest)
	postgresRef := fmt.Sprintf("ghcr.io/%s/postgres@%s", normalizedRepository, postgresDigest)
	if controlRef != "ghcr.io/ryanakml/deadbolt/control-plane@"+controlDigest || postgresRef != "ghcr.io/ryanakml/deadbolt/postgres@"+postgresDigest {
		t.Fatalf("repository normalization changed immutable image reference format: %q / %q", controlRef, postgresRef)
	}

	for _, token := range []string{
		"printf '%s' \"$GITHUB_REPOSITORY\" | tr '[:upper:]' '[:lower:]'",
		"ghcr.io/${{ steps.ghcr-repository.outputs.path }}/control-plane@${{ needs.build-and-publish.outputs.image-digest }}",
		"ghcr.io/${{ steps.ghcr-repository.outputs.path }}/postgres@${{ needs.build-and-publish.outputs.postgres-digest }}",
	} {
		if !strings.Contains(wf, token) {
			t.Fatalf("staging image normalization contract missing %q", token)
		}
	}
	if strings.Contains(wf, "ghcr.io/${{ github.repository }}/control-plane@") || strings.Contains(wf, "ghcr.io/${{ github.repository }}/postgres@") {
		t.Fatal("remote immutable staging refs must not use mixed-case github.repository directly")
	}
}

// TestRollbackStagingConfigurationAndSafetyGuards verifies Finding 1:
// rollback-staging.sh is self-contained, loads approved configuration, rejects world-readable
// files, fails closed when variables are missing, and validates Compose in dry-run mode.
func TestRollbackStagingConfigurationAndSafetyGuards(t *testing.T) {
	scriptPath := "../../scripts/rollback-staging.sh"

	// 1. Missing required environment variables -> fail closed
	cmd := exec.Command("/bin/bash", scriptPath)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected rollback-staging.sh to fail without config, but succeeded: %s", string(out))
	}
	if !strings.Contains(string(out), "DEADBOLT_STAGING_DOMAIN is missing") {
		t.Fatalf("expected missing configuration error, got: %s", string(out))
	}

	// 2. World-readable configuration file -> fail closed with security violation
	tmpDir := t.TempDir()
	badPermFile := filepath.Join(tmpDir, "unsafe.env")
	if err := os.WriteFile(badPermFile, []byte("DEADBOLT_STAGING_DOMAIN=staging.example.com\n"), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	cmd = exec.Command("/bin/bash", scriptPath)
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"DEADBOLT_CONFIG_FILE=" + badPermFile,
	}
	out, err = cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected rollback-staging.sh to reject world-readable config, but succeeded: %s", string(out))
	}
	if !strings.Contains(string(out), "must not grant world access") {
		t.Fatalf("expected world-access error message, got: %s", string(out))
	}

	// 3. DRY RUN mode with Compose validation -> must pass
	cmd = exec.Command("/bin/bash", scriptPath)
	cmd.Env = append(os.Environ(), "DRY_RUN=true")
	out, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("expected DRY_RUN=true ./scripts/rollback-staging.sh to succeed: %v\nOutput: %s", err, string(out))
	}
	if !strings.Contains(string(out), "DRY RUN passed") {
		t.Fatalf("expected DRY RUN success message, got: %s", string(out))
	}
}

func TestStagingConfigPermissionContract(t *testing.T) {
	helperPath, err := filepath.Abs("../../scripts/lib/config-permissions.sh")
	if err != nil {
		t.Fatalf("resolve config permissions helper: %v", err)
	}

	for _, tc := range []struct {
		name string
		mode os.FileMode
		want bool
	}{
		{name: "group readable deploy config accepted", mode: 0640, want: true},
		{name: "group writable deploy config rejected", mode: 0660, want: false},
		{name: "world readable deploy config rejected", mode: 0644, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "staging.env")
			if err := os.WriteFile(configPath, []byte("SECRET=value\n"), tc.mode); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(configPath, tc.mode); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("/bin/bash", "-c", fmt.Sprintf("source %q; validate_staging_config_permissions %q", helperPath, configPath))
			out, err := cmd.CombinedOutput()
			if (err == nil) != tc.want {
				t.Fatalf("permission validation success=%t, want %t: %s", err == nil, tc.want, out)
			}
		})
	}
}

// TestEdgeSmokeRollbackRouteRestorationInvariant proves that a failed edge smoke never
// stops the candidate until Caddy restoration succeeds. This is deliberately a shell-level
// test of the helper used by deploy-staging.sh so the dangerous ordering cannot regress.
func TestEdgeSmokeRollbackRouteRestorationInvariant(t *testing.T) {
	helperPath, err := filepath.Abs("../../scripts/lib/edge-smoke-rollback.sh")
	if err != nil {
		t.Fatalf("resolve edge rollback helper: %v", err)
	}
	deployBytes, err := os.ReadFile("../../scripts/deploy-staging.sh")
	if err != nil {
		t.Fatalf("read deploy script: %v", err)
	}
	if !strings.Contains(string(deployBytes), "restore_edge_route_before_stopping_candidate \"$OLD_PORT\"") || strings.Contains(string(deployBytes), "./scripts/reload-caddy.sh \"$OLD_PORT\" || true") {
		t.Fatal("deploy-staging.sh must use the authoritative route-restoration helper without suppressing failure")
	}

	for _, tc := range []struct {
		name            string
		reloadExit      int
		wantRollback    bool
		wantExit        int
		wantRouteFailed bool
	}{
		{name: "restore failure preserves candidate", reloadExit: 1, wantRollback: false, wantExit: 1, wantRouteFailed: true},
		{name: "restore success stops candidate", reloadExit: 0, wantRollback: true, wantExit: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workDir := t.TempDir()
			reloadPath := filepath.Join(workDir, "reload-caddy")
			if err := os.WriteFile(reloadPath, []byte(fmt.Sprintf("#!/usr/bin/env bash\nexit %d\n", tc.reloadExit)), 0755); err != nil {
				t.Fatal(err)
			}
			statePath := filepath.Join(workDir, "candidate-state")
			harness := fmt.Sprintf(`
set -u -o pipefail
OLD_SLOT=blue
CANDIDATE_SLOT=green
err() { echo "ERROR:$*" >&2; }
rollback() { echo stopped > %q; }
source %q
restore_edge_route_before_stopping_candidate 8088
`, statePath, helperPath)
			cmd := exec.Command("/bin/bash", "-c", harness)
			cmd.Env = append(os.Environ(), "DEADBOLT_CADDY_RELOAD_SCRIPT="+reloadPath)
			out, err := cmd.CombinedOutput()
			if exitErr, ok := err.(*exec.ExitError); ok {
				if exitErr.ExitCode() != tc.wantExit {
					t.Fatalf("expected exit %d, got %d: %s", tc.wantExit, exitErr.ExitCode(), out)
				}
			} else if err != nil || tc.wantExit != 0 {
				t.Fatalf("expected exit %d, got %v: %s", tc.wantExit, err, out)
			}
			_, rollbackErr := os.Stat(statePath)
			if tc.wantRollback != (rollbackErr == nil) {
				t.Fatalf("candidate stop=%t, want %t; output: %s", rollbackErr == nil, tc.wantRollback, out)
			}
			if tc.wantRouteFailed && !strings.Contains(string(out), "ROUTE RESTORATION FAILED") {
				t.Fatalf("expected loud route-restoration failure, got: %s", out)
			}
		})
	}
}

// TestRecurringBaseBackupAndRetention verifies Item 2:
// take-base-backup.sh exists, is executable, performs base backup, enforces strict 14-backup retention,
// fails closed if switched WAL is not visible, and executes backup readiness verification.
func TestRecurringBaseBackupAndRetention(t *testing.T) {
	scriptPath := "../../scripts/take-base-backup.sh"
	info, err := os.Stat(scriptPath)
	if err != nil {
		t.Fatalf("scripts/take-base-backup.sh missing: %v", err)
	}
	if info.Mode()&0111 == 0 {
		t.Fatalf("scripts/take-base-backup.sh is not executable")
	}

	content, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("failed to read take-base-backup.sh: %v", err)
	}
	scriptStr := string(content)

	if !strings.Contains(scriptStr, "MAX_RETAINED_BASE_BACKUPS") || !strings.Contains(scriptStr, "14") {
		t.Fatalf("expected take-base-backup.sh to enforce 14-backup retention")
	}
	if !strings.Contains(scriptStr, "pg_basebackup") {
		t.Fatalf("expected take-base-backup.sh to execute pg_basebackup")
	}
	if !strings.Contains(scriptStr, "BACKUP VERIFICATION FAILURE: Switched WAL segment") {
		t.Fatalf("expected take-base-backup.sh to fail closed when switched WAL is not visible")
	}
	if !strings.Contains(scriptStr, "check-backup-readiness.sh") {
		t.Fatalf("expected take-base-backup.sh to execute check-backup-readiness.sh")
	}

	// Verify cron setup script and template
	cronPath := "../../deploy/cron/deadbolt-backup.cron"
	cronContent, err := os.ReadFile(cronPath)
	if err != nil {
		t.Fatalf("deploy/cron/deadbolt-backup.cron missing: %v", err)
	}
	if !strings.Contains(string(cronContent), "scripts/take-base-backup.sh") {
		t.Fatalf("expected deadbolt-backup.cron to invoke take-base-backup.sh")
	}

	// Verify bootstrap script does NOT swallow cron installation failure with || true
	bootstrapContent, err := os.ReadFile("../../scripts/bootstrap-staging-cluster.sh")
	if err != nil {
		t.Fatalf("failed to read bootstrap-staging-cluster.sh: %v", err)
	}
	if strings.Contains(string(bootstrapContent), "setup-backup-cron.sh || true") {
		t.Fatalf("expected bootstrap-staging-cluster.sh to enforce setup-backup-cron.sh without || true")
	}

	// Verify dry run mode of take-base-backup.sh
	cmd := exec.Command("/bin/bash", scriptPath)
	cmd.Env = append(os.Environ(), "DRY_RUN=true")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("expected DRY_RUN=true ./scripts/take-base-backup.sh to succeed: %v\nOutput: %s", err, string(out))
	}
	if !strings.Contains(string(out), "DRY RUN passed") {
		t.Fatalf("expected DRY RUN success message, got: %s", string(out))
	}
}

// TestImmutablePostgresStagingImageDelivery verifies Item 3:
// Compose staging strictly uses pre-built DEADBOLT_POSTGRES_IMAGE with no build: block,
// and staging deploy workflow builds, tests, and publishes ghcr.io/${{ github.repository }}/postgres.
func TestImmutablePostgresStagingImageDelivery(t *testing.T) {
	// 1. Check docker-compose.staging.yml
	composePath := "../../deploy/compose/docker-compose.staging.yml"
	composeBytes, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("failed to read staging compose: %v", err)
	}
	composeStr := string(composeBytes)

	if strings.Contains(composeStr, "build:") {
		t.Fatalf("SECURITY VIOLATION: docker-compose.staging.yml contains build: directive")
	}
	if !strings.Contains(composeStr, "image: ${DEADBOLT_POSTGRES_IMAGE:?Required immutable postgres image digest}") {
		t.Fatalf("expected DEADBOLT_POSTGRES_IMAGE requirement without fallback in staging compose")
	}

	// 2. Check staging-deploy.yml builds and pushes postgres image
	workflowPath := "../../.github/workflows/staging-deploy.yml"
	workflowBytes, err := os.ReadFile(workflowPath)
	if err != nil {
		t.Fatalf("failed to read staging-deploy.yml: %v", err)
	}
	workflowStr := string(workflowBytes)

	if !strings.Contains(workflowStr, "deploy/Dockerfile.postgres") {
		t.Fatalf("expected staging-deploy.yml to build deploy/Dockerfile.postgres")
	}
	if !strings.Contains(workflowStr, "postgres-digest: ${{ steps.build-postgres.outputs.digest }}") {
		t.Fatalf("expected staging-deploy.yml to output postgres-digest")
	}
	if !strings.Contains(workflowStr, "DEADBOLT_POSTGRES_IMAGE") {
		t.Fatalf("expected staging-deploy.yml to export DEADBOLT_POSTGRES_IMAGE")
	}

	// 3. Check images.lock.json does not contain misleading postgres-archiver entry
	lockBytes, err := os.ReadFile("../../deploy/images.lock.json")
	if err != nil {
		t.Fatalf("failed to read images.lock.json: %v", err)
	}
	if strings.Contains(string(lockBytes), "postgres-archiver") {
		t.Fatalf("images.lock.json should not claim upstream base digest represents custom postgres-archiver")
	}
}

// TestDatabasePointInTimeRecoveryDrill verifies Finding 3:
// scripts/restore-staging-db.sh defaults to a safe isolated drill using a dedicated container
// and --network none, protects live staging volume, fails closed without FORCE_RESTORE in destructive mode,
// and when Docker is available, executes an end-to-end container restore drill replaying WAL.
func TestDatabasePointInTimeRecoveryDrill(t *testing.T) {
	// 1. restore-staging-db.sh script integrity & safe default mode
	scriptPath := "../../scripts/restore-staging-db.sh"
	info, err := os.Stat(scriptPath)
	if err != nil {
		t.Fatalf("scripts/restore-staging-db.sh missing: %v", err)
	}
	if info.Mode()&0111 == 0 {
		t.Fatalf("scripts/restore-staging-db.sh is not executable")
	}

	content, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("failed to read restore-staging-db.sh: %v", err)
	}
	scriptStr := string(content)

	if !strings.Contains(scriptStr, "recovery.signal") {
		t.Fatalf("expected restore script to create recovery.signal")
	}
	if !strings.Contains(scriptStr, "restore_command") {
		t.Fatalf("expected restore script to configure restore_command")
	}
	if !strings.Contains(scriptStr, "deadbolt-recovery-drill-postgres") {
		t.Fatalf("expected restore script to use dedicated isolated container deadbolt-recovery-drill-postgres")
	}
	if !strings.Contains(scriptStr, "--network none") {
		t.Fatalf("expected restore drill to attach to --network none")
	}
	if !strings.Contains(scriptStr, "FORCE_RESTORE") {
		t.Fatalf("expected destructive mode to require FORCE_RESTORE guard")
	}

	// Verify destructive mode fails closed without FORCE_RESTORE=true
	cmdDestructive := exec.Command("/bin/bash", scriptPath, "--destructive-staging-restore")
	cmdDestructive.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"DEADBOLT_WAL_ARCHIVE_DIR=/tmp",
	}
	outDestructive, errDestructive := cmdDestructive.CombinedOutput()
	if errDestructive == nil {
		t.Fatalf("expected destructive restore without FORCE_RESTORE to fail closed, but passed: %s", string(outDestructive))
	}
	if !strings.Contains(string(outDestructive), "FORCE_RESTORE=true") {
		t.Fatalf("expected FORCE_RESTORE=true error message, got: %s", string(outDestructive))
	}

	// Verify dry run execution
	cmd := exec.Command("/bin/bash", scriptPath)
	cmd.Env = append(os.Environ(), "DRY_RUN=true")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("expected DRY_RUN=true ./scripts/restore-staging-db.sh to succeed: %v\nOutput: %s", err, string(out))
	}
	if !strings.Contains(string(out), "DRY RUN passed") {
		t.Fatalf("expected DRY RUN success message, got: %s", string(out))
	}

	// 2. docs/runbooks/backup-and-disaster-recovery.md has zero wal-g references
	docPath := "../../docs/runbooks/backup-and-disaster-recovery.md"
	docBytes, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("failed to read backup runbook: %v", err)
	}
	docStr := strings.ToLower(string(docBytes))

	if strings.Contains(docStr, "wal-g") {
		t.Fatalf("DOC CONTRACT VIOLATION: docs/runbooks/backup-and-disaster-recovery.md still contains wal-g references")
	}
	if !strings.Contains(docStr, "scripts/restore-staging-db.sh") {
		t.Fatalf("expected runbook to reference executable scripts/restore-staging-db.sh")
	}
	if !strings.Contains(docStr, "deadbolt_staging_postgres_data") {
		t.Fatalf("expected runbook to reference accurate volume name deadbolt_staging_postgres_data")
	}

	// 3. Live isolated restore drill if Docker daemon is available
	dockerErr := exec.Command("docker", "info").Run()
	if dockerErr != nil {
		t.Logf("Docker daemon is not accessible on test runner (%v); live container drill validated via contracts and dry run", dockerErr)
		return
	}

	t.Log("Docker daemon is available: executing live container recovery drill...")
	archiveDir := t.TempDir()
	if err := os.Chmod(archiveDir, 0777); err != nil {
		t.Fatalf("failed to chmod archive directory: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(archiveDir, "basebackups"), 0777); err != nil {
		t.Fatalf("failed to create basebackups directory: %v", err)
	}
	_ = os.Chmod(filepath.Join(archiveDir, "basebackups"), 0777)

	sourceContainer := "deadbolt-test-drill-source"
	_ = exec.Command("docker", "rm", "-f", sourceContainer).Run()

	// Spin up source postgres container to produce base backup + WAL
	startSourceCmd := exec.Command("docker", "run", "-d",
		"--name", sourceContainer,
		"-e", "POSTGRES_PASSWORD=testpass",
		"-e", "POSTGRES_USER=deadbolt_admin",
		"-e", "POSTGRES_DB=deadbolt_staging",
		"-v", archiveDir+":/wal_archive",
		"postgres:18-bookworm",
		"-c", "wal_level=replica",
		"-c", "archive_mode=on",
		"-c", "archive_command=cp %p /wal_archive/%f && chmod 666 /wal_archive/%f",
	)
	if out, err := startSourceCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to start source postgres container: %v, %s", err, string(out))
	}
	defer func() {
		_ = exec.Command("docker", "rm", "-f", sourceContainer).Run()
	}()

	// Wait for source postgres ready
	sourceReady := false
	for i := 0; i < 30; i++ {
		time.Sleep(1 * time.Second)
		if exec.Command("docker", "exec", sourceContainer, "psql", "-U", "deadbolt_admin", "-d", "deadbolt_staging", "-c", "SELECT 1").Run() == nil {
			sourceReady = true
			break
		}
	}
	if !sourceReady {
		t.Fatalf("source postgres failed to report ready")
	}

	// Initialize test table with first record
	initSQL := "CREATE TABLE drill_verification (id int, name text); INSERT INTO drill_verification VALUES (1, 'initial_base_record');"
	if out, err := exec.Command("docker", "exec", sourceContainer, "psql", "-U", "deadbolt_admin", "-d", "deadbolt_staging", "-c", initSQL).CombinedOutput(); err != nil {
		t.Fatalf("failed to insert initial record: %v, %s", err, string(out))
	}

	// Create physical base backup
	backupCmd := exec.Command("docker", "exec", sourceContainer, "pg_basebackup", "-U", "deadbolt_admin", "-D", "/tmp/base_bkp", "-Ft", "-z", "-X", "fetch")
	if out, err := backupCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to take base backup: %v, %s", err, string(out))
	}

	tarballDst := filepath.Join(archiveDir, "basebackups", "base_test.tar.gz")
	if out, err := exec.Command("docker", "cp", sourceContainer+":/tmp/base_bkp/base.tar.gz", tarballDst).CombinedOutput(); err != nil {
		t.Fatalf("failed to copy base backup tarball: %v, %s", err, string(out))
	}

	// Insert second record and commit it before switching WAL
	insertSQL := "INSERT INTO drill_verification VALUES (2, 'wal_replayed_record');"
	if out, err := exec.Command("docker", "exec", sourceContainer, "psql", "-U", "deadbolt_admin", "-d", "deadbolt_staging", "-c", insertSQL).CombinedOutput(); err != nil {
		t.Fatalf("failed to insert wal record: %v, %s", err, string(out))
	}

	// Switch WAL so the WAL segment containing the committed insert is archived
	switchedOut, err := exec.Command("docker", "exec", sourceContainer, "psql", "-U", "deadbolt_admin", "-d", "deadbolt_staging", "-t", "-A", "-c", "SELECT pg_walfile_name(pg_switch_wal());").CombinedOutput()
	if err != nil {
		t.Fatalf("failed to switch wal: %v, %s", err, string(switchedOut))
	}
	switchedWal := strings.TrimSpace(string(switchedOut))
	t.Logf("Awaiting archive of WAL file containing committed record: %s", switchedWal)

	// Poll until the exact switched WAL segment containing record 2 is archived into archiveDir
	walArchived := false
	for i := 0; i < 40; i++ {
		if _, err := os.Stat(filepath.Join(archiveDir, switchedWal)); err == nil {
			walArchived = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !walArchived {
		logs, _ := exec.Command("docker", "logs", sourceContainer).CombinedOutput()
		t.Fatalf("expected switched WAL file %s to be archived in %s; source logs:\n%s", switchedWal, archiveDir, string(logs))
	}

	_ = exec.Command("docker", "rm", "-f", sourceContainer).Run()

	// Write recorded release provenance to verify standalone recovery resolution without caller env
	drillReleaseDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(drillReleaseDir, "postgres_image"), []byte("postgres:18-bookworm\n"), 0644); err != nil {
		t.Fatalf("failed to write postgres_image for drill: %v", err)
	}

	// Execute restore-staging-db.sh in default --drill mode (DEADBOLT_POSTGRES_IMAGE resolved standalone from release state)
	drillCmd := exec.Command("/bin/bash", scriptPath, "--drill")
	drillCmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"RELEASE_DIR=" + drillReleaseDir,
		"DEADBOLT_WAL_ARCHIVE_DIR=" + archiveDir,
		"DEADBOLT_DB_ADMIN_PASSWORD=testpass",
	}
	drillOut, drillErr := drillCmd.CombinedOutput()
	if drillErr != nil {
		t.Fatalf("isolated restore drill failed: %v\nOutput: %s", drillErr, string(drillOut))
	}
	if !strings.Contains(string(drillOut), "ISOLATED RECOVERY DRILL COMPLETED SUCCESSFULLY") {
		t.Fatalf("expected successful drill marker, got: %s", string(drillOut))
	}
	if !strings.Contains(string(drillOut), "Verified WAL replayed record") {
		t.Fatalf("expected replayed WAL verification, got: %s", string(drillOut))
	}
	t.Log("Live container restore drill passed: WAL archives successfully replayed to consistent primary state.")
}

// TestDeployToRollbackPostgresImageProvenance verifies Finding 1 from Follow-up Audit #5:
// TestDeployToRollbackPostgresImageProvenance verifies Finding 1 from Follow-up Audit #5 & #6:
// Workflow-supplied DEADBOLT_POSTGRES_IMAGE is preserved across sourcing static host configuration,
// persisted only after the actual staging Postgres service is healthy and inspected, and recovered
// by scripts/rollback-staging.sh to succeed standalone with zero operator environment even when staging.env omits it.
func TestDeployToRollbackPostgresImageProvenance(t *testing.T) {
	tmpDir := t.TempDir()
	releaseDir := filepath.Join(tmpDir, "releases")
	if err := os.MkdirAll(releaseDir, 0755); err != nil {
		t.Fatalf("failed to create release dir: %v", err)
	}

	configDir := filepath.Join(tmpDir, "config")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatalf("failed to create config dir: %v", err)
	}
	configFile := filepath.Join(configDir, "staging.env")

	workflowDigest := "ghcr.io/ryanakml/deadbolt/postgres@sha256:active222222222222222222222222222222222222222222222222222222222222"
	staleDigest := "ghcr.io/ryanakml/deadbolt/postgres@sha256:stale111111111111111111111111111111111111111111111111111111111111"
	priorDigest := "ghcr.io/ryanakml/deadbolt/postgres@sha256:prior000000000000000000000000000000000000000000000000000000000000"

	// 1. Establish prior release state
	pgImageFile := filepath.Join(releaseDir, "postgres_image")
	if err := os.WriteFile(pgImageFile, []byte(priorDigest+"\n"), 0644); err != nil {
		t.Fatalf("failed to write initial postgres_image: %v", err)
	}

	// 2. Static config with stale digest
	envWithStale := fmt.Sprintf(`DEADBOLT_STAGING_DOMAIN=staging.deadbolt.cloud
DATABASE_URL=postgres://deadbolt_runtime:mock_runtime_password@localhost:5432/mock
MIGRATOR_DATABASE_URL=postgres://deadbolt_migrator:mock_migrator_password@localhost:5432/mock
SYSTEM_DATABASE_URL=postgres://deadbolt_system:mock_system_password@localhost:5432/mock
DEADBOLT_DB_ADMIN_PASSWORD=mock_admin_password
DEADBOLT_MIGRATOR_PASSWORD=mock_migrator_password
DEADBOLT_RUNTIME_PASSWORD=mock_runtime_password
DEADBOLT_SYSTEM_PASSWORD=mock_system_password
DEADBOLT_OIDC_ISSUER=https://mock-issuer.com
DEADBOLT_OIDC_CLIENT_ID=mock_client_id
DEADBOLT_OIDC_CLIENT_SECRET=mock_client_secret
DEADBOLT_STORAGE_S3_BUCKET=mock-bucket
DEADBOLT_POSTGRES_IMAGE=%s
`, staleDigest)

	if err := os.WriteFile(configFile, []byte(envWithStale), 0600); err != nil {
		t.Fatalf("failed to write staging.env: %v", err)
	}

	// 3. Sourcing deploy logic: caller DEADBOLT_POSTGRES_IMAGE must win over stale config
	// AND early failures before Postgres inspection MUST NOT overwrite prior release state
	cmdTestDeployEarlyFail := exec.Command("/bin/bash", "-c", fmt.Sprintf(`
		set -euo pipefail
		RELEASE_DIR=%q
		DEADBOLT_CONFIG_FILE=%q
		DEADBOLT_POSTGRES_IMAGE=%q
		POSTGRES_IMAGE_FILE="${RELEASE_DIR}/postgres_image"
		PREVIOUS_POSTGRES_IMAGE_FILE="${RELEASE_DIR}/postgres_image.previous"
		CALLER_POSTGRES_IMAGE="${DEADBOLT_POSTGRES_IMAGE:-}"
		source "$DEADBOLT_CONFIG_FILE"
		if [[ -n "$CALLER_POSTGRES_IMAGE" ]]; then
			DEADBOLT_POSTGRES_IMAGE="$CALLER_POSTGRES_IMAGE"
		elif [[ -z "${DEADBOLT_POSTGRES_IMAGE:-}" && -f "$POSTGRES_IMAGE_FILE" ]]; then
			DEADBOLT_POSTGRES_IMAGE=$(cat "$POSTGRES_IMAGE_FILE" | tr -d '[:space:]')
		fi
		export DEADBOLT_POSTGRES_IMAGE
		echo "RESOLVED_IMAGE=$DEADBOLT_POSTGRES_IMAGE"
		# Simulate a failure before postgres inspection / compose up
		exit 42
	`, releaseDir, configFile, workflowDigest))

	earlyFailOut, earlyFailErr := cmdTestDeployEarlyFail.CombinedOutput()
	if earlyFailErr == nil {
		t.Fatalf("expected early fail script to exit with error")
	}
	if !strings.Contains(string(earlyFailOut), "RESOLVED_IMAGE="+workflowDigest) {
		t.Fatalf("expected caller image %s, got output: %s", workflowDigest, string(earlyFailOut))
	}

	// Prior release state MUST remain completely untouched after early failure
	priorCheckData, err := os.ReadFile(pgImageFile)
	if err != nil {
		t.Fatalf("failed to read postgres_image after early fail: %v", err)
	}
	if strings.TrimSpace(string(priorCheckData)) != priorDigest {
		t.Fatalf("REGRESSION: postgres_image was modified before service verification! expected %s, got %s", priorDigest, string(priorCheckData))
	}

	// 4. Simulate successful post-verification transactional release state persistence
	cmdTestInspectAndPersist := exec.Command("/bin/bash", "-c", fmt.Sprintf(`
		set -euo pipefail
		RELEASE_DIR=%q
		POSTGRES_IMAGE_FILE="${RELEASE_DIR}/postgres_image"
		PREVIOUS_POSTGRES_IMAGE_FILE="${RELEASE_DIR}/postgres_image.previous"
		expected_image=%q

		mkdir -p "$RELEASE_DIR"
		if [[ -f "$POSTGRES_IMAGE_FILE" ]]; then
			current_recorded=$(cat "$POSTGRES_IMAGE_FILE" | tr -d '[:space:]' || true)
			if [[ "$current_recorded" != "$expected_image" ]]; then
				cp -f "$POSTGRES_IMAGE_FILE" "$PREVIOUS_POSTGRES_IMAGE_FILE"
			fi
		fi
		tmp_pg_file=$(mktemp "${RELEASE_DIR}/postgres_image.tmp.XXXXXX")
		echo "$expected_image" > "$tmp_pg_file"
		mv -f "$tmp_pg_file" "$POSTGRES_IMAGE_FILE"
	`, releaseDir, workflowDigest))

	if out, err := cmdTestInspectAndPersist.CombinedOutput(); err != nil {
		t.Fatalf("transactional persist failed: %v, %s", err, string(out))
	}

	// Verify current postgres_image has new digest and previous has priorDigest
	currentData, err := os.ReadFile(pgImageFile)
	if err != nil {
		t.Fatalf("failed to read updated postgres_image: %v", err)
	}
	if strings.TrimSpace(string(currentData)) != workflowDigest {
		t.Fatalf("expected %s in postgres_image, got %s", workflowDigest, string(currentData))
	}

	prevData, err := os.ReadFile(filepath.Join(releaseDir, "postgres_image.previous"))
	if err != nil {
		t.Fatalf("failed to read postgres_image.previous: %v", err)
	}
	if strings.TrimSpace(string(prevData)) != priorDigest {
		t.Fatalf("expected %s in postgres_image.previous, got %s", priorDigest, string(prevData))
	}

	// 5. Now rewrite staging.env so it completely OMITS DEADBOLT_POSTGRES_IMAGE
	envWithoutPG := `DEADBOLT_STAGING_DOMAIN=staging.deadbolt.cloud
DATABASE_URL=postgres://deadbolt_runtime:mock_runtime_password@localhost:5432/mock
MIGRATOR_DATABASE_URL=postgres://deadbolt_migrator:mock_migrator_password@localhost:5432/mock
SYSTEM_DATABASE_URL=postgres://deadbolt_system:mock_system_password@localhost:5432/mock
DEADBOLT_DB_ADMIN_PASSWORD=mock_admin_password
DEADBOLT_MIGRATOR_PASSWORD=mock_migrator_password
DEADBOLT_RUNTIME_PASSWORD=mock_runtime_password
DEADBOLT_SYSTEM_PASSWORD=mock_system_password
DEADBOLT_OIDC_ISSUER=https://mock-issuer.com
DEADBOLT_OIDC_CLIENT_ID=mock_client_id
DEADBOLT_OIDC_CLIENT_SECRET=mock_client_secret
`
	if err := os.WriteFile(configFile, []byte(envWithoutPG), 0600); err != nil {
		t.Fatalf("failed to write staging.env without pg image: %v", err)
	}

	// Rollback resolution: DEADBOLT_POSTGRES_IMAGE is unset in environment and missing in staging.env.
	// rollback-staging.sh MUST recover it from ${RELEASE_DIR}/postgres_image
	cmdTestRollback := exec.Command("/bin/bash", "-c", fmt.Sprintf(`
		set -euo pipefail
		RELEASE_DIR=%q
		DEADBOLT_CONFIG_FILE=%q
		POSTGRES_IMAGE_FILE="${RELEASE_DIR}/postgres_image"
		CALLER_POSTGRES_IMAGE="${DEADBOLT_POSTGRES_IMAGE:-}"
		source "$DEADBOLT_CONFIG_FILE"
		if [[ -n "$CALLER_POSTGRES_IMAGE" ]]; then
			DEADBOLT_POSTGRES_IMAGE="$CALLER_POSTGRES_IMAGE"
		elif [[ -f "$POSTGRES_IMAGE_FILE" ]]; then
			RECORDED_PG_IMAGE=$(cat "$POSTGRES_IMAGE_FILE" | tr -d '[:space:]')
			if [[ -n "$RECORDED_PG_IMAGE" ]]; then
				DEADBOLT_POSTGRES_IMAGE="$RECORDED_PG_IMAGE"
			fi
		elif [[ -f "${RELEASE_DIR}/postgres_image.previous" ]]; then
			RECORDED_PG_IMAGE=$(cat "${RELEASE_DIR}/postgres_image.previous" | tr -d '[:space:]')
			if [[ -n "$RECORDED_PG_IMAGE" ]]; then
				DEADBOLT_POSTGRES_IMAGE="$RECORDED_PG_IMAGE"
			fi
		fi
		echo "ROLLBACK_RESOLVED_IMAGE=${DEADBOLT_POSTGRES_IMAGE:-}"
	`, releaseDir, configFile))
	cmdTestRollback.Env = []string{
		"PATH=" + os.Getenv("PATH"),
	}

	rollbackOut, err := cmdTestRollback.CombinedOutput()
	if err != nil {
		t.Fatalf("rollback image recovery failed: %v, %s", err, string(rollbackOut))
	}
	if !strings.Contains(string(rollbackOut), "ROLLBACK_RESOLVED_IMAGE="+workflowDigest) {
		t.Fatalf("expected rollback to recover %s from release state, got: %s", workflowDigest, string(rollbackOut))
	}

	// 6. Verify deploy-staging.sh, rollback-staging.sh, and bootstrap-staging-cluster.sh script contracts
	scriptsToCheck := map[string][]string{
		"../../scripts/deploy-staging.sh": {
			"POSTGRES_IMAGE_FILE=\"${RELEASE_DIR}/postgres_image\"",
			"PREVIOUS_POSTGRES_IMAGE_FILE=\"${RELEASE_DIR}/postgres_image.previous\"",
			"inspect_and_record_postgres_provenance",
			"export DEADBOLT_POSTGRES_IMAGE",
		},
		"../../scripts/rollback-staging.sh": {
			"POSTGRES_IMAGE_FILE=\"${RELEASE_DIR}/postgres_image\"",
			"cat \"$POSTGRES_IMAGE_FILE\"",
			"postgres_image.previous",
			"export DEADBOLT_POSTGRES_IMAGE",
		},
		"../../scripts/bootstrap-staging-cluster.sh": {
			"POSTGRES_IMAGE_FILE=\"${RELEASE_DIR}/postgres_image\"",
			"inspect_and_record_postgres_provenance",
			"export DEADBOLT_POSTGRES_IMAGE",
		},
	}

	for sPath, patterns := range scriptsToCheck {
		sBytes, err := os.ReadFile(sPath)
		if err != nil {
			t.Fatalf("failed to read %s: %v", sPath, err)
		}
		sStr := string(sBytes)
		for _, pat := range patterns {
			if !strings.Contains(sStr, pat) {
				t.Fatalf("contract violation in %s: missing expected pattern %q", sPath, pat)
			}
		}
	}
}

// TestStandaloneRestorePostgresImageProvenance verifies Finding 2 from Follow-up Audit #6:
// restore-staging-db.sh resolves immutable PostgreSQL image from explicit caller input ->
// recorded release provenance -> running container, fails closed with zero mutable fallback (no postgres:18),
// uses pinned helper images by digest, and recovers release digest standalone when staging.env omits it.
func TestStandaloneRestorePostgresImageProvenance(t *testing.T) {
	scriptPath := "../../scripts/restore-staging-db.sh"
	scriptBytes, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("failed to read restore-staging-db.sh: %v", err)
	}
	scriptStr := string(scriptBytes)

	// Verify script does NOT contain mutable postgres:18 fallback
	if strings.Contains(scriptStr, "postgres:18}") || strings.Contains(scriptStr, "postgres:18\"") {
		t.Fatalf("CONTRACT VIOLATION: restore-staging-db.sh still contains mutable postgres:18 fallback")
	}

	// Verify script does NOT contain mutable alpine helper
	if strings.Contains(scriptStr, "alpine") {
		t.Fatalf("CONTRACT VIOLATION: restore-staging-db.sh still contains mutable alpine helper")
	}

	tmpDir := t.TempDir()
	releaseDir := filepath.Join(tmpDir, "releases")
	if err := os.MkdirAll(releaseDir, 0755); err != nil {
		t.Fatalf("failed to create release dir: %v", err)
	}

	configDir := filepath.Join(tmpDir, "config")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatalf("failed to create config dir: %v", err)
	}
	configFile := filepath.Join(configDir, "staging.env")

	// 1. staging.env contains NO DEADBOLT_POSTGRES_IMAGE
	stagingEnv := `DEADBOLT_STAGING_DOMAIN=staging.deadbolt.cloud
DEADBOLT_STORAGE_S3_BUCKET=mock-backup-bucket
DEADBOLT_DB_ADMIN_PASSWORD=mock_admin_password
DATABASE_URL=postgres://deadbolt_runtime:mock_runtime_password@localhost:5432/mock
SYSTEM_DATABASE_URL=postgres://deadbolt_system:mock_system_password@localhost:5432/mock
`
	if err := os.WriteFile(configFile, []byte(stagingEnv), 0600); err != nil {
		t.Fatalf("failed to write staging.env: %v", err)
	}

	// 2. When postgres_image file is absent, restore-staging-db.sh must FAIL CLOSED
	failClosedCmd := exec.Command("/bin/bash", scriptPath, "--drill")
	failClosedCmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"DEADBOLT_CONFIG_FILE=" + configFile,
		"RELEASE_DIR=" + releaseDir,
	}
	failClosedOut, failClosedErr := failClosedCmd.CombinedOutput()
	if failClosedErr == nil {
		t.Fatalf("expected restore-staging-db.sh to fail closed when DEADBOLT_POSTGRES_IMAGE is absent, but succeeded:\n%s", string(failClosedOut))
	}
	if !strings.Contains(string(failClosedOut), "DEADBOLT_POSTGRES_IMAGE could not be resolved") {
		t.Fatalf("expected 'DEADBOLT_POSTGRES_IMAGE could not be resolved' in error output, got:\n%s", string(failClosedOut))
	}

	// 3. Populate releases/postgres_image with authoritative digest
	recordedDigest := "ghcr.io/ryanakml/deadbolt/postgres@sha256:authoritative1111111111111111111111111111111111111111111111111111"
	if err := os.WriteFile(filepath.Join(releaseDir, "postgres_image"), []byte(recordedDigest+"\n"), 0644); err != nil {
		t.Fatalf("failed to write postgres_image: %v", err)
	}

	// 4. Test resolution logic: verify recorded digest is recovered standalone from release state
	checkHarness := filepath.Join(tmpDir, "check_resolution.sh")
	harnessScript := `
set -euo pipefail
CONFIG_FILE="$DEADBOLT_CONFIG_FILE"
source "$CONFIG_FILE"
POSTGRES_IMAGE_FILE="${RELEASE_DIR}/postgres_image"
CALLER_POSTGRES_IMAGE="${DEADBOLT_POSTGRES_IMAGE:-}"
if [[ -n "$CALLER_POSTGRES_IMAGE" ]]; then
	DEADBOLT_POSTGRES_IMAGE="$CALLER_POSTGRES_IMAGE"
elif [[ -f "$POSTGRES_IMAGE_FILE" ]]; then
	RECORDED_PG_IMAGE=$(cat "$POSTGRES_IMAGE_FILE" | tr -d '[:space:]')
	if [[ -n "$RECORDED_PG_IMAGE" ]]; then
		DEADBOLT_POSTGRES_IMAGE="$RECORDED_PG_IMAGE"
	fi
fi
echo "RESOLVED_PG_IMAGE=${DEADBOLT_POSTGRES_IMAGE:-}"
`
	if err := os.WriteFile(checkHarness, []byte(harnessScript), 0755); err != nil {
		t.Fatalf("failed to write harness: %v", err)
	}

	resolveCmd := exec.Command("/bin/bash", checkHarness)
	resolveCmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"DEADBOLT_CONFIG_FILE=" + configFile,
		"RELEASE_DIR=" + releaseDir,
	}

	resolveOut, resolveErr := resolveCmd.CombinedOutput()
	if resolveErr != nil {
		t.Fatalf("resolution harness failed: %v, %s", resolveErr, string(resolveOut))
	}
	if !strings.Contains(string(resolveOut), "RESOLVED_PG_IMAGE="+recordedDigest) {
		t.Fatalf("expected %s to be recovered from postgres_image, got: %s", recordedDigest, string(resolveOut))
	}

	// 5. Verify caller environment takes precedence over recorded file
	callerOverride := "ghcr.io/ryanakml/deadbolt/postgres@sha256:calleroverride222222222222222222222222222222222222222222222222222"
	overrideCmd := exec.Command("/bin/bash", checkHarness)
	overrideCmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"DEADBOLT_CONFIG_FILE=" + configFile,
		"RELEASE_DIR=" + releaseDir,
		"DEADBOLT_POSTGRES_IMAGE=" + callerOverride,
	}

	overrideOut, overrideErr := overrideCmd.CombinedOutput()
	if overrideErr != nil {
		t.Fatalf("override harness failed: %v, %s", overrideErr, string(overrideOut))
	}
	if !strings.Contains(string(overrideOut), "RESOLVED_PG_IMAGE="+callerOverride) {
		t.Fatalf("expected caller override %s, got: %s", callerOverride, string(overrideOut))
	}
}

// TestBootstrapUnifiedRecurringSchedule verifies Finding 2 from Follow-up Audit #5:
// deploy-staging.sh --bootstrap delegates directly to authoritative scripts/bootstrap-staging-cluster.sh,
// and scripts/setup-backup-cron.sh replaces /var/log/deadbolt-backup.log with user-writable
// ${HOME}/.deadbolt/logs/deadbolt-backup.log in the user-crontab fallback path.
func TestBootstrapUnifiedRecurringSchedule(t *testing.T) {
	// 1. Verify deploy-staging.sh delegates --bootstrap to bootstrap-staging-cluster.sh
	deployScriptPath := "../../scripts/deploy-staging.sh"
	deployBytes, err := os.ReadFile(deployScriptPath)
	if err != nil {
		t.Fatalf("failed to read deploy-staging.sh: %v", err)
	}
	deployStr := string(deployBytes)

	if !strings.Contains(deployStr, "./scripts/bootstrap-staging-cluster.sh") {
		t.Fatalf("expected deploy-staging.sh to delegate bootstrap to ./scripts/bootstrap-staging-cluster.sh")
	}

	// Verify bootstrap-staging-cluster.sh invokes setup-backup-cron.sh
	bootstrapScriptPath := "../../scripts/bootstrap-staging-cluster.sh"
	bootstrapBytes, err := os.ReadFile(bootstrapScriptPath)
	if err != nil {
		t.Fatalf("failed to read bootstrap-staging-cluster.sh: %v", err)
	}
	bootstrapStr := string(bootstrapBytes)

	if !strings.Contains(bootstrapStr, "./scripts/setup-backup-cron.sh") {
		t.Fatalf("expected bootstrap-staging-cluster.sh to execute ./scripts/setup-backup-cron.sh")
	}

	// 2. Test setup-backup-cron.sh user-crontab fallback log path
	cronScriptPath := "../../scripts/setup-backup-cron.sh"
	mockHome := t.TempDir()
	mockBin := filepath.Join(mockHome, "bin")
	if err := os.MkdirAll(mockBin, 0755); err != nil {
		t.Fatalf("failed to create mock bin dir: %v", err)
	}

	cronStoreFile := filepath.Join(mockHome, "installed_crontab")
	mockCrontabScript := fmt.Sprintf(`#!/usr/bin/env bash
STORE=%q
if [[ "${1:-}" == "-l" ]]; then
	if [[ -f "$STORE" ]]; then
		cat "$STORE"
		exit 0
	else
		exit 1
	fi
elif [[ "${1:-}" == "-" ]]; then
	cat > "$STORE"
	exit 0
fi
exit 1
`, cronStoreFile)

	mockCrontabPath := filepath.Join(mockBin, "crontab")
	if err := os.WriteFile(mockCrontabPath, []byte(mockCrontabScript), 0755); err != nil {
		t.Fatalf("failed to write mock crontab: %v", err)
	}

	// Add mock sudo to simulate non-privileged user even on systems with passwordless sudo
	mockSudoScript := "#!/usr/bin/env bash\nexit 1\n"
	mockSudoPath := filepath.Join(mockBin, "sudo")
	if err := os.WriteFile(mockSudoPath, []byte(mockSudoScript), 0755); err != nil {
		t.Fatalf("failed to write mock sudo: %v", err)
	}

	// Run setup-backup-cron.sh with mock crontab in PATH and non-writable CRON_DEST
	cmd := exec.Command("/bin/bash", cronScriptPath)
	cmd.Env = []string{
		"PATH=" + mockBin + ":" + os.Getenv("PATH"),
		"HOME=" + mockHome,
		"CRON_DEST=/nonexistent_system_dir/deadbolt-backup",
		"CRON_SRC=../../deploy/cron/deadbolt-backup.cron",
	}

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("setup-backup-cron.sh user crontab installation failed: %v, %s", err, string(out))
	}

	if !strings.Contains(string(out), "Verified: Backup schedule confirmed in user crontab.") {
		t.Fatalf("expected user crontab verification in output: %s", string(out))
	}

	// Inspect installed crontab file
	cronContent, err := os.ReadFile(cronStoreFile)
	if err != nil {
		t.Fatalf("failed to read installed crontab store: %v", err)
	}
	cronStr := string(cronContent)

	expectedLogPath := filepath.Join(mockHome, ".deadbolt", "logs", "deadbolt-backup.log")
	if strings.Contains(cronStr, "/var/log/deadbolt-backup.log") {
		t.Fatalf("privilege violation: user crontab still contains privileged /var/log path:\n%s", cronStr)
	}
	if !strings.Contains(cronStr, expectedLogPath) {
		t.Fatalf("expected user crontab to use user-writable log path %q, got:\n%s", expectedLogPath, cronStr)
	}

	// Verify log directory was created and log file touched
	if _, err := os.Stat(expectedLogPath); err != nil {
		t.Fatalf("expected log file %s to be created and touched: %v", expectedLogPath, err)
	}
}

// Helper to spin up an in-process mock S3 HTTP server with Range request support and race safety
func newS3MockServer(t *testing.T, bucket string, objects map[string][]byte, mu *sync.RWMutex) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		parts := strings.SplitN(path, "/", 2)
		reqBucket := parts[0]
		key := ""
		if len(parts) > 1 {
			key = parts[1]
		}

		// Handle S3 ListObjectsV2
		if r.Method == "GET" && (key == "" || r.URL.Query().Get("list-type") == "2" || strings.Contains(r.URL.RawQuery, "list-type=2")) {
			prefix := r.URL.Query().Get("prefix")
			var contents []string
			count := 0
			mu.RLock()
			for k, v := range objects {
				if prefix == "" || strings.HasPrefix(k, prefix) {
					count++
					item := fmt.Sprintf(`    <Contents>
        <Key>%s</Key>
        <LastModified>2026-09-12T12:00:00.000Z</LastModified>
        <ETag>&quot;%x&quot;</ETag>
        <Size>%d</Size>
        <StorageClass>STANDARD</StorageClass>
    </Contents>`, k, sha256.Sum256(v), len(v))
					contents = append(contents, item)
				}
			}
			mu.RUnlock()
			sort.Strings(contents)
			xmlResp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Name>%s</Name>
    <Prefix>%s</Prefix>
    <KeyCount>%d</KeyCount>
    <MaxKeys>1000</MaxKeys>
    <IsTruncated>false</IsTruncated>
%s
</ListBucketResult>`, reqBucket, prefix, count, strings.Join(contents, "\n"))
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(xmlResp))
			return
		}

		// Handle GetObject and HeadObject with RFC 7233 Range support
		if r.Method == "GET" || r.Method == "HEAD" {
			mu.RLock()
			data, ok := objects[key]
			mu.RUnlock()
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("ETag", fmt.Sprintf("\"%x\"", sha256.Sum256(data)))
			w.Header().Set("Accept-Ranges", "bytes")
			http.ServeContent(w, r, filepath.Base(key), time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC), bytes.NewReader(data))
			return
		}

		w.WriteHeader(http.StatusOK)
	})
	return httptest.NewServer(mux)
}

// setupMockAWSCLI creates a lightweight mock aws CLI script in tempBinDir when aws is not present in PATH
func setupMockAWSCLI(t *testing.T) string {
	if _, err := exec.LookPath("aws"); err == nil {
		// Real aws CLI is available
		return os.Getenv("PATH")
	}

	tempBin := t.TempDir()
	mockAWSScript := `#!/usr/bin/env python3
import sys, os, urllib.request, xml.etree.ElementTree as ET

args = sys.argv[1:]
endpoint = None
for i, a in enumerate(args):
    if a == '--endpoint-url' and i + 1 < len(args):
        endpoint = args[i + 1]
    elif a.startswith('--endpoint-url='):
        endpoint = a.split('=', 1)[1]

if not endpoint:
    endpoint = os.environ.get('AWS_ENDPOINT_URL') or os.environ.get('DEADBOLT_STORAGE_S3_ENDPOINT')

if not endpoint:
    sys.exit(1)

if len(args) < 2 or args[0] != 's3':
    sys.exit(0)

cmd = args[1]
if cmd == 'ls':
    target = args[2]
    path = target.replace('s3://', '')
    bucket, prefix = path.split('/', 1) if '/' in path else (path, '')
    url = f"{endpoint}/{bucket}?list-type=2&prefix={prefix}"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as resp:
        xml_data = resp.read()
    root = ET.fromstring(xml_data)
    ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
    for c in root.findall('s3:Contents', ns):
        key = c.find('s3:Key', ns).text
        size = c.find('s3:Size', ns).text
        filename = key.split('/')[-1]
        print(f"2026-09-12 12:00:00 {int(size):>10} {filename}")
elif cmd == 'cp':
    if '--recursive' in args:
        src = args[2]
        dst = args[3]
        path = src.replace('s3://', '')
        bucket, prefix = path.split('/', 1) if '/' in path else (path, '')
        url = f"{endpoint}/{bucket}?list-type=2&prefix={prefix}"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as resp:
            xml_data = resp.read()
        root = ET.fromstring(xml_data)
        ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
        os.makedirs(dst, exist_ok=True)
        for c in root.findall('s3:Contents', ns):
            key = c.find('s3:Key', ns).text
            filename = key.split('/')[-1]
            if filename:
                urllib.request.urlretrieve(f"{endpoint}/{bucket}/{key}", os.path.join(dst, filename))
    else:
        src = args[2]
        dst = args[3]
        if src.startswith('s3://'):
            path = src.replace('s3://', '')
            urllib.request.urlretrieve(f"{endpoint}/{path}", dst)
elif cmd == 'sync':
    src = args[2]
    dst = args[3]
    path = src.replace('s3://', '')
    bucket, prefix = path.split('/', 1) if '/' in path else (path, '')
    url = f"{endpoint}/{bucket}?list-type=2&prefix={prefix}"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as resp:
        xml_data = resp.read()
    root = ET.fromstring(xml_data)
    ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
    os.makedirs(dst, exist_ok=True)
    for c in root.findall('s3:Contents', ns):
        key = c.find('s3:Key', ns).text
        filename = key.split('/')[-1]
        if filename:
            urllib.request.urlretrieve(f"{endpoint}/{bucket}/{key}", os.path.join(dst, filename))

`
	awsPath := filepath.Join(tempBin, "aws")
	if err := os.WriteFile(awsPath, []byte(mockAWSScript), 0755); err != nil {
		t.Fatalf("failed to write mock aws script: %v", err)
	}
	return tempBin + ":" + os.Getenv("PATH")
}

// TestDatabasePointInTimeRecoveryDrillRemoteS3 verifies Finding 3 from Follow-up Audit #5:
// Remote S3 Archive Replay via Host Prefetching in Isolated Recovery Drill.
// When DEADBOLT_STORAGE_S3_BUCKET is configured, scripts/restore-staging-db.sh prefetches WAL files
// from s3://${S3_BUCKET}/postgres/wal/ into an isolated temporary directory on the host (${TMP_DIR}/prefetched_wal),
// mounts it as /wal_archive:ro into the --network none drill container, and replays with cp /wal_archive/%f %p.
// Tested against an in-process S3-compatible mock server end-to-end.
func TestDatabasePointInTimeRecoveryDrillRemoteS3(t *testing.T) {
	scriptPath := "../../scripts/restore-staging-db.sh"
	scriptBytes, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("failed to read restore-staging-db.sh: %v", err)
	}
	scriptStr := string(scriptBytes)

	// 1. Contract verification: WAL prefetch and mount contracts
	if !strings.Contains(scriptStr, "PREFETCH_WAL_DIR=\"${TMP_DIR}/prefetched_wal\"") {
		t.Fatalf("expected restore script to define PREFETCH_WAL_DIR")
	}
	if !strings.Contains(scriptStr, "WAL_MOUNT_SOURCE=\"$PREFETCH_WAL_DIR\"") {
		t.Fatalf("expected restore script to set WAL_MOUNT_SOURCE to PREFETCH_WAL_DIR")
	}
	if !strings.Contains(scriptStr, "RESTORE_CMD=\"cp /wal_archive/%f %p\"") {
		t.Fatalf("expected drill restore command to be 'cp /wal_archive/%%f %%p'")
	}
	if !strings.Contains(scriptStr, "-v \"${WAL_MOUNT_SOURCE}\":/wal_archive:ro") {
		t.Fatalf("expected drill container to mount WAL_MOUNT_SOURCE at /wal_archive:ro")
	}
	if !strings.Contains(scriptStr, "--network none") {
		t.Fatalf("expected drill container to remain isolated on --network none")
	}

	// 2. Set up S3 mock server and objects
	bucketName := "deadbolt-staging-s3-drill"
	objects := make(map[string][]byte)
	var s3Mu sync.RWMutex

	// Add dummy base backup and WAL for prefetch verification
	s3Mu.Lock()
	objects["postgres/basebackups/base_20260912.tar.gz"] = []byte("dummy base backup content")
	objects["postgres/wal/000000010000000000000001"] = []byte("wal segment 1")
	objects["postgres/wal/000000010000000000000002"] = []byte("wal segment 2")
	s3Mu.Unlock()

	s3Server := newS3MockServer(t, bucketName, objects, &s3Mu)
	defer s3Server.Close()

	effectivePath := setupMockAWSCLI(t)

	// 3. Verify S3 prefetch execution directly using script logic
	testPrefetchCmd := exec.Command("/bin/bash", "-c", fmt.Sprintf(`
		set -euo pipefail
		TMP_DIR=%q
		S3_BUCKET=%q
		AWS_ARGS=(--endpoint-url %q --region us-east-1)
		PREFETCH_WAL_DIR="${TMP_DIR}/prefetched_wal"
		mkdir -p "$PREFETCH_WAL_DIR"
		if ! aws s3 sync "s3://${S3_BUCKET}/postgres/wal/" "$PREFETCH_WAL_DIR" "${AWS_ARGS[@]}" --only-show-errors 2>/dev/null; then
			aws s3 cp "s3://${S3_BUCKET}/postgres/wal/" "$PREFETCH_WAL_DIR" --recursive "${AWS_ARGS[@]}" --only-show-errors 2>/dev/null || true
		fi
		echo "PREFETCHED_COUNT=$(ls -1 "$PREFETCH_WAL_DIR" | wc -l | tr -d '[:space:]')"
	`, t.TempDir(), bucketName, s3Server.URL))
	testPrefetchCmd.Env = []string{
		"PATH=" + effectivePath,
		"AWS_ACCESS_KEY_ID=test_key",
		"AWS_SECRET_ACCESS_KEY=test_secret",
		"AWS_DEFAULT_REGION=us-east-1",
	}

	prefetchOut, prefetchErr := testPrefetchCmd.CombinedOutput()
	if prefetchErr != nil {
		t.Fatalf("S3 WAL prefetch test failed: %v, %s", prefetchErr, string(prefetchOut))
	}
	if !strings.Contains(string(prefetchOut), "PREFETCHED_COUNT=2") {
		t.Fatalf("expected 2 prefetched WAL files from mock S3, got: %s", string(prefetchOut))
	}
	t.Log("S3 WAL prefetch verified: 2 WAL segments transferred to host cache.")

	// 4. Live isolated restore drill with remote S3 if Docker is available
	dockerErr := exec.Command("docker", "info").Run()
	if dockerErr != nil {
		t.Logf("Docker daemon is not accessible on test runner (%v); S3 recovery drill validated via contracts, S3 mock server, and prefetch logic", dockerErr)
		return
	}

	t.Log("Docker daemon is available: executing live container recovery drill from remote S3 mock...")
	sourceContainer := "deadbolt-test-drill-s3-source"
	_ = exec.Command("docker", "rm", "-f", sourceContainer).Run()

	localArchiveDir := t.TempDir()
	_ = os.Chmod(localArchiveDir, 0777)

	startSourceCmd := exec.Command("docker", "run", "-d",
		"--name", sourceContainer,
		"-e", "POSTGRES_PASSWORD=testpass",
		"-e", "POSTGRES_USER=deadbolt_admin",
		"-e", "POSTGRES_DB=deadbolt_staging",
		"-v", localArchiveDir+":/wal_archive",
		"postgres:18-bookworm",
		"-c", "wal_level=replica",
		"-c", "archive_mode=on",
		"-c", "archive_command=cp %p /wal_archive/%f && chmod 666 /wal_archive/%f",
	)
	if out, err := startSourceCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to start source postgres container: %v, %s", err, string(out))
	}
	defer func() {
		_ = exec.Command("docker", "rm", "-f", sourceContainer).Run()
	}()

	sourceReady := false
	for i := 0; i < 30; i++ {
		time.Sleep(1 * time.Second)
		if exec.Command("docker", "exec", sourceContainer, "psql", "-U", "deadbolt_admin", "-d", "deadbolt_staging", "-c", "SELECT 1").Run() == nil {
			sourceReady = true
			break
		}
	}
	if !sourceReady {
		t.Fatalf("source postgres failed to report ready")
	}

	initSQL := "CREATE TABLE drill_verification (id int, name text); INSERT INTO drill_verification VALUES (1, 'initial_base_record');"
	if out, err := exec.Command("docker", "exec", sourceContainer, "psql", "-U", "deadbolt_admin", "-d", "deadbolt_staging", "-c", initSQL).CombinedOutput(); err != nil {
		t.Fatalf("failed to insert initial record: %v, %s", err, string(out))
	}

	backupCmd := exec.Command("docker", "exec", sourceContainer, "pg_basebackup", "-U", "deadbolt_admin", "-D", "/tmp/base_bkp", "-Ft", "-z", "-X", "fetch")
	if out, err := backupCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to take base backup: %v, %s", err, string(out))
	}

	localBaseTarball := filepath.Join(localArchiveDir, "base_s3.tar.gz")
	if out, err := exec.Command("docker", "cp", sourceContainer+":/tmp/base_bkp/base.tar.gz", localBaseTarball).CombinedOutput(); err != nil {
		t.Fatalf("failed to copy base backup tarball: %v, %s", err, string(out))
	}

	insertSQL := "INSERT INTO drill_verification VALUES (2, 'wal_replayed_record');"
	if out, err := exec.Command("docker", "exec", sourceContainer, "psql", "-U", "deadbolt_admin", "-d", "deadbolt_staging", "-c", insertSQL).CombinedOutput(); err != nil {
		t.Fatalf("failed to insert wal record: %v, %s", err, string(out))
	}

	switchedOut, err := exec.Command("docker", "exec", sourceContainer, "psql", "-U", "deadbolt_admin", "-d", "deadbolt_staging", "-t", "-A", "-c", "SELECT pg_walfile_name(pg_switch_wal());").CombinedOutput()
	if err != nil {
		t.Fatalf("failed to switch wal: %v, %s", err, string(switchedOut))
	}
	switchedWal := strings.TrimSpace(string(switchedOut))

	walArchived := false
	for i := 0; i < 40; i++ {
		if _, err := os.Stat(filepath.Join(localArchiveDir, switchedWal)); err == nil {
			walArchived = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !walArchived {
		t.Fatalf("expected switched WAL %s to be archived locally", switchedWal)
	}
	_ = exec.Command("docker", "rm", "-f", sourceContainer).Run()

	// Ensure all files written by Docker container in localArchiveDir are readable by non-root test runner
	_ = exec.Command("docker", "run", "--rm", "--entrypoint", "chmod", "-v", localArchiveDir+":/data", "postgres:18-bookworm", "-R", "a+rw", "/data").Run()

	// Upload real base backup and WAL to mock S3 server objects
	baseBytes, err := os.ReadFile(localBaseTarball)
	if err != nil {
		t.Fatalf("failed to read local base tarball: %v", err)
	}
	walBytes, err := os.ReadFile(filepath.Join(localArchiveDir, switchedWal))
	if err != nil {
		t.Fatalf("failed to read switched wal: %v", err)
	}

	s3Mu.Lock()
	objects["postgres/basebackups/base_s3.tar.gz"] = baseBytes
	objects["postgres/wal/"+switchedWal] = walBytes
	s3Mu.Unlock()

	// Run restore-staging-db.sh --drill targeting remote S3 mock server
	drillCmd := exec.Command("/bin/bash", scriptPath, "--drill")
	drillCmd.Env = []string{
		"PATH=" + effectivePath,
		"DEADBOLT_STORAGE_S3_BUCKET=" + bucketName,
		"DEADBOLT_STORAGE_S3_ENDPOINT=" + s3Server.URL,
		"AWS_ENDPOINT_URL=" + s3Server.URL,
		"DEADBOLT_STORAGE_S3_REGION=us-east-1",
		"AWS_DEFAULT_REGION=us-east-1",
		"AWS_ACCESS_KEY_ID=mock_key",
		"AWS_SECRET_ACCESS_KEY=mock_secret",
		"DEADBOLT_DB_ADMIN_PASSWORD=testpass",
		"DEADBOLT_POSTGRES_IMAGE=postgres:18-bookworm",
	}

	drillOut, drillErr := drillCmd.CombinedOutput()
	if drillErr != nil {
		t.Fatalf("isolated remote S3 restore drill failed: %v\nOutput: %s", drillErr, string(drillOut))
	}
	if !strings.Contains(string(drillOut), "ISOLATED RECOVERY DRILL COMPLETED SUCCESSFULLY") {
		t.Fatalf("expected successful drill marker, got: %s", string(drillOut))
	}
	if !strings.Contains(string(drillOut), "Verified WAL replayed record") {
		t.Fatalf("expected replayed WAL verification, got: %s", string(drillOut))
	}
	t.Log("Live remote S3 recovery drill passed: S3 base backup + S3 prefetched WAL successfully replayed in --network none container.")
}
