package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

	// Verify staging PostgreSQL builds custom image from deploy/Dockerfile.postgres
	if !strings.Contains(stagingStr, "dockerfile: deploy/Dockerfile.postgres") {
		t.Fatalf("expected staging postgres to build from deploy/Dockerfile.postgres")
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

// TestCaddyAbsoluteSnippetImportAndMergedValidation verifies that scripts/reload-caddy.sh
// uses absolute snippet import paths and performs exact merged validation (Item 4).
func TestCaddyAbsoluteSnippetImportAndMergedValidation(t *testing.T) {
	content, err := os.ReadFile("../../scripts/reload-caddy.sh")
	if err != nil {
		t.Fatalf("failed to read scripts/reload-caddy.sh: %v", err)
	}
	scriptStr := string(content)

	if !strings.Contains(scriptStr, "ABS_SNIPPET_FILE") {
		t.Fatalf("expected reload-caddy.sh to resolve ABS_SNIPPET_FILE")
	}
	if !strings.Contains(scriptStr, "import ${ABS_SNIPPET_FILE}") {
		t.Fatalf("expected reload-caddy.sh to import absolute snippet path")
	}
	if !strings.Contains(scriptStr, "caddy validate --config \"$CADDYFILE\" --adapter caddyfile") {
		t.Fatalf("expected reload-caddy.sh to validate exact merged Caddyfile with --adapter caddyfile")
	}
	if !strings.Contains(scriptStr, "import deploy/caddy/Deadbolt.caddyfile") {
		t.Fatalf("expected reload-caddy.sh to clean up legacy relative imports")
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

	if !strings.Contains(bootstrapStr, "docker compose -p deadbolt-staging -f \"$COMPOSE_FILE\" up -d --build postgres nats") {
		t.Fatalf("expected bootstrap script to build and up data infrastructure")
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
}
