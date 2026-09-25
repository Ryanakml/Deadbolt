package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/artifacts"
	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

// artifactTestConfig resolves the real S3-compatible endpoint under test.
// CI provides MinIO as a service; locally it runs on loopback. The suite
// fails closed (skips only outside CI) when no store is reachable, and CI
// always provides one.
func artifactTestConfig(t *testing.T) artifacts.S3Config {
	t.Helper()
	endpoint := os.Getenv("DEADBOLT_ARTIFACTS_S3_ENDPOINT")
	if endpoint == "" {
		endpoint = "127.0.0.1:9000"
	}
	bucket := os.Getenv("DEADBOLT_ARTIFACTS_S3_BUCKET")
	if bucket == "" {
		bucket = "deadbolt-artifacts-test"
	}
	access := os.Getenv("DEADBOLT_ARTIFACTS_S3_ACCESS_KEY")
	if access == "" {
		access = "miniotest"
	}
	secret := os.Getenv("DEADBOLT_ARTIFACTS_S3_SECRET_KEY")
	if secret == "" {
		secret = "miniotest123"
	}
	return artifacts.S3Config{
		Endpoint: endpoint, Bucket: bucket,
		AccessKey: access, SecretKey: secret, Region: "us-east-1",
	}
}

func setupArtifactSuite(t *testing.T, suffix string) (*tenantTestContext, *httptest.Server, string, string, *artifacts.Service, artifacts.ObjectStore) {
	t.Helper()
	cfg := artifactTestConfig(t)
	t.Setenv("DEADBOLT_ARTIFACTS_S3_ENDPOINT", cfg.Endpoint)
	t.Setenv("DEADBOLT_ARTIFACTS_S3_BUCKET", cfg.Bucket)
	t.Setenv("DEADBOLT_ARTIFACTS_S3_ACCESS_KEY", cfg.AccessKey)
	t.Setenv("DEADBOLT_ARTIFACTS_S3_SECRET_KEY", cfg.SecretKey)
	t.Setenv("DEADBOLT_ARTIFACTS_S3_REGION", cfg.Region)
	store := artifacts.NewS3Store(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := store.EnsureBucket(ctx); err != nil {
		if os.Getenv("CI") != "" {
			t.Fatalf("CI must provide S3-compatible storage: %v", err)
		}
		t.Skipf("S3-compatible storage unavailable: %v", err)
	}
	_ = suffix
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	return tc, server, orgID, envID, artifacts.NewService(tc.pool, store), store
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func artifactManifest() string {
	return `{"targetOS":"linux","targetArchitecture":"amd64","tasks":[{"name":"task-a","entrypoint":"tasks/a.js","recovery":"safe","timeoutMs":60000,"inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"retry":{"maxAttempts":3,"initialDelayMs":1000,"maxDelayMs":30000}}],"workflows":[{"name":"workflow-a","inputSchema":{"type":"object"},"nodes":[{"id":"node-a","type":"task","task":"task-a"}]}]}`
}

type artifactIDs struct {
	runID, stepID, attemptID string
	epoch                    int64
}

func claimStartedArtifactAttempt(t *testing.T, tc *tenantTestContext, server *httptest.Server, orgID, envID, digest, deploymentID, reqSuffix string) (*testWorkerSession, artifactIDs) {
	t.Helper()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "artifact-"+reqSuffix)
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)
	a := claimExecution(t, server, session, digest, "artifact-claim-"+reqSuffix)
	startNode(t, server, session, a.AttemptID, a.OwnershipEpoch)
	return session, artifactIDs{runID: runID, stepID: stepID, attemptID: a.AttemptID, epoch: a.OwnershipEpoch}
}

func postArtifactJSON(t *testing.T, server *httptest.Server, token, path string, body map[string]any) (int, map[string]any) {
	t.Helper()
	return postArtifactJSONWithKey(t, server, token, path, body, "")
}

func postArtifactJSONWithKey(t *testing.T, server *httptest.Server, token, path string, body map[string]any, idemKey string) (int, map[string]any) {
	t.Helper()
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, server.URL+path, bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	if idemKey == "" {
		// Unique per call so independent reservations never collide on the
		// durable command identity; idempotency tests pass explicit keys.
		idemKey = fmt.Sprintf("artifact-test-%s-%d", path, time.Now().UnixNano())
	}
	req.Header.Set("Idempotency-Key", idemKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("artifact request failed: %v", err)
	}
	defer resp.Body.Close()
	var parsed map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&parsed)
	return resp.StatusCode, parsed
}

func putObjectBytes(t *testing.T, url string, data []byte) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("PUT status %d", resp.StatusCode)
	}
}

// TestArtifactReservePutFinalizeAssociateDownload proves the M2 happy path
// against real S3-compatible storage: reserve → PUT → finalize → Complete
// with artifactId → typed reference output → scoped download of the bytes.
func TestArtifactReservePutFinalizeAssociateDownload(t *testing.T) {
	tc, server, orgID, envID, artifactSvc, _ := setupArtifactSuite(t, "happy")
	defer tc.cleanup()
	defer server.Close()
	_ = artifactSvc
	const digest = "bundle-artifact-happy-22"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, artifactManifest())
	session, ids := claimStartedArtifactAttempt(t, tc, server, orgID, envID, digest, deploymentID, "happy")

	payload := bytes.Repeat([]byte("0123456789abcdef"), 64) // 1 KiB
	status, created := postArtifactJSON(t, server, session.SessionToken, "/v1/artifacts", map[string]any{
		"runId": ids.runID, "attemptId": ids.attemptID, "ownershipEpoch": ids.epoch,
		"sizeBytes": len(payload), "sha256": sha256Hex(payload),
	})
	if status != http.StatusOK {
		t.Fatalf("reserve expected 200, got %d (%v)", status, created)
	}
	artifactID, _ := created["id"].(string)
	uploadURL, _ := created["uploadUrl"].(string)
	if artifactID == "" || uploadURL == "" {
		t.Fatalf("reserve must return id and uploadUrl: %v", created)
	}
	if strings.Contains(uploadURL, "127.0.0.1:8080") || strings.Contains(uploadURL, server.URL) {
		t.Fatalf("upload URL must target object storage, not the control plane: %s", uploadURL)
	}
	putObjectBytes(t, uploadURL, payload)

	status, finalized := postArtifactJSON(t, server, session.SessionToken, "/v1/artifacts/"+artifactID+"/finalize", map[string]any{
		"attemptId": ids.attemptID, "ownershipEpoch": ids.epoch, "sha256": sha256Hex(payload),
	})
	if status != http.StatusOK || finalized["status"] != "READY" {
		t.Fatalf("finalize expected READY, got %d (%v)", status, finalized)
	}

	completion := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "artifact-complete-happy",
		WorkerID: session.WorkerID, SessionID: session.SessionID,
		AttemptID: ids.attemptID, OwnershipEpoch: ids.epoch,
		Outcome: "SUCCEEDED", ArtifactID: artifactID,
	}
	completion.ResultDigest, _ = worker.CanonicalCompletionDigest(&completion)
	var compResp worker.CompleteResponseDTO
	if status := postWorkerJSON(t, server, "/worker/v1/complete", session.SessionToken, completion, &compResp); status != http.StatusOK {
		t.Fatalf("artifact completion expected 200, got %d", status)
	}
	var stepOutput []byte
	var stepState string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT state, output FROM run_steps WHERE id=$1::uuid`, ids.stepID).Scan(&stepState, &stepOutput)
	}); err != nil {
		t.Fatal(err)
	}
	if stepState != "SUCCEEDED" {
		t.Fatalf("expected SUCCEEDED, got %s", stepState)
	}
	var out map[string]any
	_ = json.Unmarshal(stepOutput, &out)
	if out["$artifact"] != artifactID {
		t.Fatalf("step output must be the typed reference, got %s", string(stepOutput))
	}

	// Scoped download returns a signed URL; the bytes round-trip exactly.
	var dl map[string]any
	{
		req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/artifacts/"+artifactID, nil)
		req.Header.Set("Authorization", "Bearer "+session.SessionToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("download URL expected 200, got %d", resp.StatusCode)
		}
		_ = json.NewDecoder(resp.Body).Decode(&dl)
	}
	downloadURL, _ := dl["downloadUrl"].(string)
	if downloadURL == "" {
		t.Fatalf("missing downloadUrl: %v", dl)
	}
	getResp, err := http.DefaultClient.Get(downloadURL)
	if err != nil {
		t.Fatal(err)
	}
	defer getResp.Body.Close()
	got, _ := io.ReadAll(getResp.Body)
	if string(got) != string(payload) {
		t.Fatalf("downloaded bytes differ")
	}
	if cd := getResp.Header.Get("Content-Disposition"); cd == "" {
		t.Fatalf("download must force attachment disposition")
	}
	if ct := getResp.Header.Get("Content-Type"); ct != "application/octet-stream" {
		t.Fatalf("download must use a safe content type, got %q", ct)
	}
}

// TestArtifactFinalizeVerifiesSizeAndChecksum proves finalize fails closed:
// wrong bytes and short uploads keep the row PENDING for re-upload.
func TestArtifactFinalizeVerifiesSizeAndChecksum(t *testing.T) {
	tc, server, orgID, envID, _, _ := setupArtifactSuite(t, "verify")
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "artifact-verify")
	const digest = "bundle-artifact-verify-22"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, artifactManifest())
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)
	a := claimExecution(t, server, session, digest, "artifact-verify-claim")
	startNode(t, server, session, a.AttemptID, a.OwnershipEpoch)

	payload := bytes.Repeat([]byte("v"), 128)
	status, created := postArtifactJSON(t, server, session.SessionToken, "/v1/artifacts", map[string]any{
		"runId": runID, "attemptId": a.AttemptID, "ownershipEpoch": a.OwnershipEpoch,
		"sizeBytes": len(payload), "sha256": sha256Hex(payload),
	})
	if status != http.StatusOK {
		t.Fatalf("reserve expected 200, got %d (%v)", status, created)
	}
	artifactID, _ := created["id"].(string)
	putObjectBytes(t, created["uploadUrl"].(string), bytes.Repeat([]byte("X"), 128))
	status, body := postArtifactJSON(t, server, session.SessionToken, "/v1/artifacts/"+artifactID+"/finalize", map[string]any{
		"attemptId": a.AttemptID, "ownershipEpoch": a.OwnershipEpoch, "sha256": sha256Hex(payload),
	})
	if status != http.StatusUnprocessableEntity || body["code"] != "CHECKSUM_MISMATCH" {
		t.Fatalf("corrupt bytes must be 422 CHECKSUM_MISMATCH, got %d (%v)", status, body)
	}
	putObjectBytes(t, created["uploadUrl"].(string), payload[:64])
	status, body = postArtifactJSON(t, server, session.SessionToken, "/v1/artifacts/"+artifactID+"/finalize", map[string]any{
		"attemptId": a.AttemptID, "ownershipEpoch": a.OwnershipEpoch, "sha256": sha256Hex(payload),
	})
	if status != http.StatusUnprocessableEntity || body["code"] != "SIZE_MISMATCH" {
		t.Fatalf("short upload must be 422 SIZE_MISMATCH, got %d (%v)", status, body)
	}
	var artifactStatus string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status FROM artifacts WHERE id=$1::uuid`, artifactID).Scan(&artifactStatus)
	}); err != nil {
		t.Fatal(err)
	}
	if artifactStatus != "PENDING_UPLOAD" {
		t.Fatalf("failed verification must leave the row pending, got %s", artifactStatus)
	}
}

// TestArtifactCapsQuotaAndShape proves admission limits: objects over 100MiB
// are rejected before any upload, malformed digests conflict, and the 1 GiB
// environment reservation is enforced before a row or URL is minted.
func TestArtifactCapsQuotaAndShape(t *testing.T) {
	tc, server, orgID, envID, _, _ := setupArtifactSuite(t, "caps")
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "artifact-caps")
	const digest = "bundle-artifact-caps-22"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, artifactManifest())
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	defer func() {
		_ = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
			_, _ = tx.Exec(context.Background(), `UPDATE runs SET status='CANCELLED' WHERE id=$1::uuid`, runID)
			return nil
		})
	}()
	advertiseDigest(t, tc, orgID, session.SessionID, digest)
	a := claimExecution(t, server, session, digest, "artifact-caps-claim")
	startNode(t, server, session, a.AttemptID, a.OwnershipEpoch)
	payload := []byte("x")

	status, body := postArtifactJSON(t, server, session.SessionToken, "/v1/artifacts", map[string]any{
		"runId": runID, "attemptId": a.AttemptID, "ownershipEpoch": a.OwnershipEpoch,
		"sizeBytes": 100<<20 + 1, "sha256": sha256Hex(payload),
	})
	if status != http.StatusRequestEntityTooLarge || body["code"] != "ARTIFACT_TOO_LARGE" {
		t.Fatalf("oversize must be 413, got %d (%v)", status, body)
	}
	status, body = postArtifactJSON(t, server, session.SessionToken, "/v1/artifacts", map[string]any{
		"runId": runID, "attemptId": a.AttemptID, "ownershipEpoch": a.OwnershipEpoch,
		"sizeBytes": len(payload), "sha256": "not-hex",
	})
	if status != http.StatusConflict || body["code"] != "RESERVATION_MISMATCH" {
		t.Fatalf("malformed digest must be 409, got %d (%v)", status, body)
	}
	// Fill the environment reservation with a direct row, then prove a new
	// reservation is refused with 429 before minting anything.
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO artifacts
			(organization_id, environment_id, run_id, step_id, storage_key, size_bytes, sha256_hash, status)
			VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, 'quota-filler', $5, $6, 'READY')`,
			orgID, envID, runID, stepID, int64(1<<30), sha256Hex(payload))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	status, body = postArtifactJSON(t, server, session.SessionToken, "/v1/artifacts", map[string]any{
		"runId": runID, "attemptId": a.AttemptID, "ownershipEpoch": a.OwnershipEpoch,
		"sizeBytes": len(payload), "sha256": sha256Hex(payload),
	})
	if status != 429 || body["code"] != "STORAGE_QUOTA_EXCEEDED" {
		t.Fatalf("exhausted quota must be 429, got %d (%v)", status, body)
	}
}

// TestArtifactQuotaAdmissionIsSerialized proves that concurrent reservations
// cannot both pass the read/check followed by insert race at the quota edge.
func TestArtifactQuotaAdmissionIsSerialized(t *testing.T) {
	tc, server, orgID, envID, _, _ := setupArtifactSuite(t, "quota-race")
	defer tc.cleanup()
	defer server.Close()

	const digest = "bundle-artifact-quota-race-22"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, artifactManifest())
	first, firstIDs := claimStartedArtifactAttempt(t, tc, server, orgID, envID, digest, deploymentID, "quota-race-a")
	second, secondIDs := claimStartedArtifactAttempt(t, tc, server, orgID, envID, digest, deploymentID, "quota-race-b")
	defer func() {
		_ = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
			_, _ = tx.Exec(context.Background(), `UPDATE runs SET status='CANCELLED' WHERE id IN ($1::uuid, $2::uuid)`, firstIDs.runID, secondIDs.runID)
			return nil
		})
	}()

	// Leave exactly 100 bytes available. Two concurrent 100-byte reservations
	// must result in one success and one quota rejection.
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO artifacts
			(organization_id, environment_id, run_id, step_id, storage_key, size_bytes, sha256_hash, status)
			VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, 'quota-race-filler', $5, $6, 'READY')`,
			orgID, envID, firstIDs.runID, firstIDs.stepID, int64((1<<30)-100), sha256Hex([]byte("filler")))
		return err
	}); err != nil {
		t.Fatal(err)
	}

	type result struct {
		status int
		body   map[string]any
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	reserve := func(session *testWorkerSession, ids artifactIDs, key string) {
		defer wg.Done()
		status, body := postArtifactJSONWithKey(t, server, session.SessionToken, "/v1/artifacts", map[string]any{
			"runId": ids.runID, "attemptId": ids.attemptID, "ownershipEpoch": ids.epoch,
			"sizeBytes": 100, "sha256": sha256Hex(bytes.Repeat([]byte("q"), 100)),
		}, key)
		results <- result{status: status, body: body}
	}
	wg.Add(2)
	go reserve(first, firstIDs, "artifact-quota-race-a")
	go reserve(second, secondIDs, "artifact-quota-race-b")
	wg.Wait()
	close(results)

	successes, rejected := 0, 0
	for got := range results {
		switch got.status {
		case http.StatusOK:
			successes++
		case http.StatusTooManyRequests:
			if got.body["code"] != "STORAGE_QUOTA_EXCEEDED" {
				t.Fatalf("quota loser returned unexpected body: %v", got.body)
			}
			rejected++
		default:
			t.Fatalf("concurrent reserve returned unexpected status %d (%v)", got.status, got.body)
		}
	}
	if successes != 1 || rejected != 1 {
		t.Fatalf("quota race results: successes=%d rejected=%d", successes, rejected)
	}

	var total int64
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT COALESCE(SUM(size_bytes), 0) FROM artifacts
			WHERE organization_id=$1::uuid AND environment_id=$2::uuid
			AND status IN ('PENDING_UPLOAD','READY')`, orgID, envID).Scan(&total)
	}); err != nil {
		t.Fatal(err)
	}
	if total > artifacts.EnvStorageQuotaBytes {
		t.Fatalf("committed reservation total exceeded quota: %d", total)
	}
}

// TestArtifactOwnershipAndIsolation proves F-21 for artifacts: stale epochs,
// foreign sessions, lapsed ownership, cross-environment and cross-tenant
// access are all denied without leaking scope.
func TestArtifactOwnershipAndIsolation(t *testing.T) {
	tc, server, orgID, envID, _, _ := setupArtifactSuite(t, "isolation")
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "artifact-owner")
	foreign, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "artifact-foreign")
	const digest = "bundle-artifact-isolation-22"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, artifactManifest())
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)
	advertiseDigest(t, tc, orgID, foreign.SessionID, digest)
	a := claimExecution(t, server, session, digest, "artifact-isolation-claim")
	startNode(t, server, session, a.AttemptID, a.OwnershipEpoch)
	payload := []byte("owned-bytes")
	status, created := postArtifactJSON(t, server, session.SessionToken, "/v1/artifacts", map[string]any{
		"runId": runID, "attemptId": a.AttemptID, "ownershipEpoch": a.OwnershipEpoch,
		"sizeBytes": len(payload), "sha256": sha256Hex(payload),
	})
	if status != http.StatusOK {
		t.Fatalf("reserve expected 200, got %d", status)
	}
	artifactID, _ := created["id"].(string)
	putObjectBytes(t, created["uploadUrl"].(string), payload)

	finalize := func(token string, attemptID string, epoch int64) (int, map[string]any) {
		return postArtifactJSON(t, server, token, "/v1/artifacts/"+artifactID+"/finalize", map[string]any{
			"attemptId": attemptID, "ownershipEpoch": epoch, "sha256": sha256Hex(payload),
		})
	}
	if status, body := finalize(session.SessionToken, a.AttemptID, a.OwnershipEpoch+99); status != http.StatusConflict || body["code"] != "NOT_OWNED" {
		t.Fatalf("stale epoch must be 409 NOT_OWNED, got %d (%v)", status, body)
	}
	if status, body := finalize(foreign.SessionToken, a.AttemptID, a.OwnershipEpoch); status != http.StatusConflict || body["code"] != "NOT_OWNED" {
		t.Fatalf("foreign session must be 409 NOT_OWNED, got %d (%v)", status, body)
	}
	if status, body := finalize(session.SessionToken, a.AttemptID, a.OwnershipEpoch); status != http.StatusOK {
		t.Fatalf("finalize expected 200, got %d (%v)", status, body)
	}
	// Lapsed ownership: a second reservation whose attempt is LOST before
	// finalize can no longer be finalized by its original owner.
	status, second := postArtifactJSON(t, server, session.SessionToken, "/v1/artifacts", map[string]any{
		"runId": runID, "attemptId": a.AttemptID, "ownershipEpoch": a.OwnershipEpoch,
		"sizeBytes": len(payload), "sha256": sha256Hex(payload),
	})
	if status != http.StatusOK {
		t.Fatalf("second reserve expected 200, got %d", status)
	}
	putObjectBytes(t, second["uploadUrl"].(string), payload)
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE task_leases SET expires_at=clock_timestamp()-INTERVAL '1 second' WHERE attempt_id=$1::uuid`, a.AttemptID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	engine := execution.NewWorkerEngine(tc.pool)
	if _, err := engine.ReconcileExpiredLeases(context.Background(), orgID); err != nil {
		t.Fatal(err)
	}
	secondID, _ := second["id"].(string)
	finalizeSecond := func() (int, map[string]any) {
		return postArtifactJSON(t, server, session.SessionToken, "/v1/artifacts/"+secondID+"/finalize", map[string]any{
			"attemptId": a.AttemptID, "ownershipEpoch": a.OwnershipEpoch, "sha256": sha256Hex(payload),
		})
	}
	if status, body := finalizeSecond(); status != http.StatusConflict {
		t.Fatalf("lapsed ownership must be rejected, got %d (%v)", status, body)
	}

	// Cross-environment machine access reads as not found.
	project2, err := tc.service.CreateProject(context.Background(), orgID, "Artifact Env Project 2")
	if err != nil {
		t.Fatal(err)
	}
	env2, err := tc.service.CreateEnvironment(context.Background(), orgID, project2.ID, tenant.EnvDevelopment, 10)
	if err != nil {
		t.Fatal(err)
	}
	otherKey := bootstrapTestKey(t, tc.service, orgID, env2.ID, []string{tenant.CapPayloadRead, tenant.CapArtifactsWrite})
	get := func(token, id string) int {
		req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/artifacts/"+id, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	if status := get(otherKey.PlaintextKey, artifactID); status != http.StatusNotFound {
		t.Fatalf("cross-environment read must be 404, got %d", status)
	}
	// Cross-tenant access reads as not found.
	ownerB, _ := tenant.NewUUID()
	orgB, err := tc.service.CreateOrganization(context.Background(), ownerB, "Artifact Foreign Org")
	if err != nil {
		t.Fatal(err)
	}
	projB, _ := tc.service.CreateProject(context.Background(), orgB.ID, "Foreign Project")
	envB, _ := tc.service.CreateEnvironment(context.Background(), orgB.ID, projB.ID, tenant.EnvStaging, 10)
	foreignKey := bootstrapTestKey(t, tc.service, orgB.ID, envB.ID, []string{tenant.CapPayloadRead, tenant.CapArtifactsWrite})
	if status := get(foreignKey.PlaintextKey, artifactID); status != http.StatusNotFound {
		t.Fatalf("cross-tenant read must be 404, got %d", status)
	}
	// Unknown IDs read as not found.
	if status := get(otherKey.PlaintextKey, "00000000-0000-0000-0000-000000000000"); status != http.StatusNotFound {
		t.Fatalf("unknown artifact must be 404, got %d", status)
	}
	// A viewer without payload:read is forbidden at the route.
	viewerKey := bootstrapTestKey(t, tc.service, orgID, envID, []string{tenant.CapRunsRead})
	if status := get(viewerKey.PlaintextKey, artifactID); status != http.StatusForbidden {
		t.Fatalf("payload gate must be 403, got %d", status)
	}
}

// TestArtifactOrphanGCCollectsSafely proves F-20 and lifecycle retention:
// unfinalized uploads past grace expire, referenced READY artifacts survive,
// and unreferenced READY artifacts past grace are deleted with their bytes.
func TestArtifactOrphanGCCollectsSafely(t *testing.T) {
	tc, server, orgID, envID, artifactSvc, store := setupArtifactSuite(t, "gc")
	defer tc.cleanup()
	defer server.Close()
	ctx := context.Background()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "artifact-gc")
	const digest = "bundle-artifact-gc-22"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, artifactManifest())
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)
	a := claimExecution(t, server, session, digest, "artifact-gc-claim")
	startNode(t, server, session, a.AttemptID, a.OwnershipEpoch)

	// F-20: a successful PUT with no DB association (crash before finalize)
	// leaves an orphan that GC must collect, including its bytes.
	payload := []byte("orphaned-bytes")
	status, created := postArtifactJSON(t, server, session.SessionToken, "/v1/artifacts", map[string]any{
		"runId": runID, "attemptId": a.AttemptID, "ownershipEpoch": a.OwnershipEpoch,
		"sizeBytes": len(payload), "sha256": sha256Hex(payload),
	})
	if status != http.StatusOK {
		t.Fatalf("reserve expected 200, got %d", status)
	}
	orphanID, _ := created["id"].(string)
	putObjectBytes(t, created["uploadUrl"].(string), payload)
	var orphanKey string
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT storage_key FROM artifacts WHERE id=$1::uuid`, orphanID).Scan(&orphanKey); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE artifacts SET created_at=clock_timestamp()-INTERVAL '25 hours' WHERE id=$1::uuid`, orphanID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Stat(ctx, orphanKey); err != nil {
		t.Fatalf("object must exist before GC: %v", err)
	}

	// An unreferenced READY artifact past grace is collected with its bytes.
	// Reserved and finalized while the attempt is still live.
	status, lone := postArtifactJSON(t, server, session.SessionToken, "/v1/artifacts", map[string]any{
		"runId": runID, "attemptId": a.AttemptID, "ownershipEpoch": a.OwnershipEpoch,
		"sizeBytes": len(payload), "sha256": sha256Hex(payload),
	})
	if status != http.StatusOK {
		t.Fatalf("reserve expected 200, got %d (%v)", status, lone)
	}
	loneID, _ := lone["id"].(string)
	putObjectBytes(t, lone["uploadUrl"].(string), payload)
	if status, _ := postArtifactJSON(t, server, session.SessionToken, "/v1/artifacts/"+loneID+"/finalize", map[string]any{
		"attemptId": a.AttemptID, "ownershipEpoch": a.OwnershipEpoch, "sha256": sha256Hex(payload),
	}); status != http.StatusOK {
		t.Fatalf("finalize expected 200, got %d", status)
	}
	var loneKey string
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT storage_key FROM artifacts WHERE id=$1::uuid`, loneID).Scan(&loneKey); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE artifacts SET created_at=clock_timestamp()-INTERVAL '25 hours' WHERE id=$1::uuid`, loneID)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	// A finalized, referenced artifact past grace must survive.
	status, created2 := postArtifactJSON(t, server, session.SessionToken, "/v1/artifacts", map[string]any{
		"runId": runID, "attemptId": a.AttemptID, "ownershipEpoch": a.OwnershipEpoch,
		"sizeBytes": len(payload), "sha256": sha256Hex(payload),
	})
	if status != http.StatusOK {
		t.Fatalf("reserve expected 200, got %d", status)
	}
	keptID, _ := created2["id"].(string)
	putObjectBytes(t, created2["uploadUrl"].(string), payload)
	if status, _ := postArtifactJSON(t, server, session.SessionToken, "/v1/artifacts/"+keptID+"/finalize", map[string]any{
		"attemptId": a.AttemptID, "ownershipEpoch": a.OwnershipEpoch, "sha256": sha256Hex(payload),
	}); status != http.StatusOK {
		t.Fatalf("finalize expected 200, got %d", status)
	}
	completion := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "artifact-gc-complete",
		WorkerID: session.WorkerID, SessionID: session.SessionID,
		AttemptID: a.AttemptID, OwnershipEpoch: a.OwnershipEpoch,
		Outcome: "SUCCEEDED", ArtifactID: keptID,
	}
	completion.ResultDigest, _ = worker.CanonicalCompletionDigest(&completion)
	var compResp worker.CompleteResponseDTO
	if status := postWorkerJSON(t, server, "/worker/v1/complete", session.SessionToken, completion, &compResp); status != http.StatusOK {
		t.Fatalf("artifact completion expected 200, got %d", status)
	}
	var keptKey string
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT storage_key FROM artifacts WHERE id=$1::uuid`, keptID).Scan(&keptKey); err != nil {
			return err
		}
		// Age it past grace while it stays referenced by the step output.
		_, err := tx.Exec(ctx, `UPDATE artifacts SET created_at=clock_timestamp()-INTERVAL '25 hours' WHERE id=$1::uuid`, keptID)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	collected, err := artifactSvc.CollectGarbage(ctx, orgID, 50, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if collected != 2 {
		t.Fatalf("expected orphan + unreferenced collected, got %d", collected)
	}
	stateOf := func(id string) string {
		var s string
		if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
			return tx.QueryRow(ctx, `SELECT status FROM artifacts WHERE id=$1::uuid`, id).Scan(&s)
		}); err != nil {
			t.Fatal(err)
		}
		return s
	}
	if stateOf(orphanID) != "EXPIRED" || stateOf(loneID) != "DELETED" || stateOf(keptID) != "READY" {
		t.Fatalf("lifecycle wrong: orphan=%s lone=%s kept=%s",
			stateOf(orphanID), stateOf(loneID), stateOf(keptID))
	}
	if _, err := store.Stat(ctx, orphanKey); err == nil {
		t.Fatalf("orphan bytes must be deleted")
	}
	if _, err := store.Stat(ctx, loneKey); err == nil {
		t.Fatalf("unreferenced bytes must be deleted")
	}
	if _, err := store.Stat(ctx, keptKey); err != nil {
		t.Fatalf("referenced bytes must survive: %v", err)
	}
}

func consumerManifest() string {
	return `{"targetOS":"linux","targetArchitecture":"amd64","tasks":[{"name":"task-a","entrypoint":"tasks/a.js","recovery":"safe","timeoutMs":60000,"inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"retry":{"maxAttempts":3,"initialDelayMs":1000,"maxDelayMs":30000}},{"name":"task-b","entrypoint":"tasks/b.js","recovery":"safe","timeoutMs":60000,"inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"retry":{"maxAttempts":3,"initialDelayMs":1000,"maxDelayMs":30000}}],"workflows":[{"name":"workflow-a","inputSchema":{"type":"object"},"nodes":[{"id":"a","type":"task","task":"task-a"},{"id":"b","type":"task","task":"task-b","after":["a"],"input":{"doc":{"$ref":"step.output","stepId":"a","pointer":""}}}]}]}`
}

// TestArtifactCorruptBlocksConsumer proves a missing object behind a READY
// row fails the consumer with an integrity error while the successful
// producer is never rerun.
func TestArtifactCorruptBlocksConsumer(t *testing.T) {
	tc, server, orgID, envID, artifactSvc, store := setupArtifactSuite(t, "consumer")
	defer tc.cleanup()
	defer server.Close()
	ctx := context.Background()
	_ = artifactSvc
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "artifact-consumer")
	const digest = "bundle-artifact-consumer-22"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, consumerManifest())
	runID, stepA := seedExecutionRun(t, tc, orgID, envID, deploymentID, "a")
	defer func() {
		_ = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
			_, _ = tx.Exec(ctx, `UPDATE runs SET status='CANCELLED' WHERE id=$1::uuid`, runID)
			return nil
		})
	}()
	var stepB string
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `INSERT INTO run_steps
			(organization_id,environment_id,run_id,node_id,state,eligible_at)
			VALUES ($1,$2,$3,'b','BLOCKED',NULL) RETURNING id::text`, orgID, envID, runID).Scan(&stepB)
	}); err != nil {
		t.Fatal(err)
	}
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a := claimExecution(t, server, session, digest, "artifact-producer-claim")
	startNode(t, server, session, a.AttemptID, a.OwnershipEpoch)
	payload := bytes.Repeat([]byte("producer-bytes-"), 64)
	status, created := postArtifactJSON(t, server, session.SessionToken, "/v1/artifacts", map[string]any{
		"runId": runID, "attemptId": a.AttemptID, "ownershipEpoch": a.OwnershipEpoch,
		"sizeBytes": len(payload), "sha256": sha256Hex(payload),
	})
	if status != http.StatusOK {
		t.Fatalf("reserve expected 200, got %d", status)
	}
	artifactID, _ := created["id"].(string)
	putObjectBytes(t, created["uploadUrl"].(string), payload)
	if status, _ := postArtifactJSON(t, server, session.SessionToken, "/v1/artifacts/"+artifactID+"/finalize", map[string]any{
		"attemptId": a.AttemptID, "ownershipEpoch": a.OwnershipEpoch, "sha256": sha256Hex(payload),
	}); status != http.StatusOK {
		t.Fatalf("finalize expected 200, got %d", status)
	}
	completion := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "artifact-producer-complete",
		WorkerID: session.WorkerID, SessionID: session.SessionID,
		AttemptID: a.AttemptID, OwnershipEpoch: a.OwnershipEpoch,
		Outcome: "SUCCEEDED", ArtifactID: artifactID,
	}
	completion.ResultDigest, _ = worker.CanonicalCompletionDigest(&completion)
	var compResp worker.CompleteResponseDTO
	if status := postWorkerJSON(t, server, "/worker/v1/complete", session.SessionToken, completion, &compResp); status != http.StatusOK {
		t.Fatalf("producer completion expected 200, got %d", status)
	}

	// Lose the bytes out-of-band while the READY row survives.
	var storageKey string
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT storage_key FROM artifacts WHERE id=$1::uuid`, artifactID).Scan(&storageKey)
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, storageKey); err != nil {
		t.Fatal(err)
	}

	// The consumer unblocks (producer succeeded) but its claim must fail
	// closed on integrity without spending an attempt on work it cannot do.
	var polled worker.PollResponseDTO
	if status := postWorkerJSON(t, server, "/worker/v1/poll", session.SessionToken, worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "consumer-poll", WorkerID: session.WorkerID,
		SessionID: session.SessionID, AvailableSlots: 1, DeploymentDigests: []string{digest}, Pool: "default",
	}, &polled); status != http.StatusOK {
		t.Fatalf("poll expected 200, got %d", status)
	}
	if len(polled.Assignments) != 0 {
		t.Fatalf("consumer with missing bytes must not be dispatched, got %d assignments", len(polled.Assignments))
	}
	var stateB, producerState string
	var producerAttempts, consumerAttempts int
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT state FROM run_steps WHERE id=$1::uuid`, stepB).Scan(&stateB); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT state FROM run_steps WHERE id=$1::uuid`, stepA).Scan(&producerState); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_attempts WHERE step_id=$1::uuid`, stepB).Scan(&consumerAttempts); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT count(*) FROM task_attempts WHERE step_id=$1::uuid`, stepA).Scan(&producerAttempts)
	}); err != nil {
		t.Fatal(err)
	}
	if stateB != "FAILED" {
		t.Fatalf("consumer must fail on integrity, got %s", stateB)
	}
	if producerState != "SUCCEEDED" || producerAttempts != 1 || consumerAttempts != 0 {
		t.Fatalf("producer must not rerun: state=%s attempts=%d consumerAttempts=%d",
			producerState, producerAttempts, consumerAttempts)
	}
}
