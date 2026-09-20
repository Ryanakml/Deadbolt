package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/auth"
	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

// reconcileManifest builds a single-task deployment manifest with reconcile
// recovery for unknown-outcome hold tests.
func reconcileManifest(maxAttempts int, timeoutMs int64) string {
	return fmt.Sprintf(`{"targetOS":"linux","targetArchitecture":"amd64","tasks":[{"name":"task-a","entrypoint":"tasks/a.js","recovery":"reconcile","timeoutMs":%d,"inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"retry":{"maxAttempts":%d,"initialDelayMs":1000,"maxDelayMs":30000}}],"workflows":[{"name":"workflow-a","inputSchema":{"type":"object"},"nodes":[{"id":"node-a","type":"task","task":"task-a"}]}]}`, timeoutMs, maxAttempts)
}

// reconcileHumanToken creates an OIDC user, grants the role, and returns a
// human CLI session token plus the user ID for audit assertions.
func reconcileHumanToken(t *testing.T, tc *tenantTestContext, orgID, role, suffix string) (string, string) {
	t.Helper()
	ctx := context.Background()
	subject, _ := tenant.NewUUID()
	user, err := tc.sessionStore.GetOrCreateUserFromOIDC(ctx, &auth.Identity{
		Issuer: "https://issuer.example", Subject: "recon-" + suffix + "-" + subject,
		Email: "recon-" + suffix + "-" + subject + "@example.com", Name: "Recon Human",
	})
	if err != nil {
		t.Fatalf("create OIDC user: %v", err)
	}
	if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO organization_members (organization_id, user_id, role, status) VALUES ($1, $2, $3, 'ACTIVE')`, orgID, user.ID, role)
		return err
	}); err != nil {
		t.Fatalf("create membership: %v", err)
	}
	_, token, err := tc.sessionStore.CreateCLISession(ctx, user.ID, &orgID, "127.0.0.1", "recon-test", 12*time.Hour, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("create human session: %v", err)
	}
	return token, user.ID
}

var resolveKeySeq int64

func resolveCaseHTTP(t *testing.T, server *httptest.Server, token, orgID, caseID string, body map[string]any) (int, map[string]any) {
	t.Helper()
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/reconciliation-cases/"+caseID+"/resolve", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Organization-ID", orgID)
	req.Header.Set("Content-Type", "application/json")
	resolveKeySeq++
	req.Header.Set("Idempotency-Key", fmt.Sprintf("resolve-%s-%d", caseID, atomic.AddInt64(&resolveKeySeq, 1)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("resolve request failed: %v", err)
	}
	defer resp.Body.Close()
	var parsed map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&parsed)
	return resp.StatusCode, parsed
}

func openReconciliationCase(t *testing.T, tc *tenantTestContext, orgID, stepID string) (string, int64) {
	t.Helper()
	var id string
	var rev int64
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT id::text, revision FROM reconciliation_cases WHERE step_id=$1::uuid AND organization_id=$2::uuid AND status='OPEN'`, stepID, orgID).Scan(&id, &rev)
	}); err != nil {
		t.Fatalf("no OPEN case for step: %v", err)
	}
	return id, rev
}

func advertiseDigest(t *testing.T, tc *tenantTestContext, orgID, sessionID, digest string) {
	t.Helper()
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3) ON CONFLICT DO NOTHING`, sessionID, orgID, digest)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

// TestResolveConfirmSucceeded proves Blueprint §14.3 action 1: a held LOST
// attempt keeps its status while the step succeeds with
// completion_source=RECONCILIATION, and a single-node run completes.
func TestResolveConfirmSucceeded(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "resolve-succeed")
	const digest = "bundle-resolve-succeed-19"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, reconcileManifest(3, 60000))
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "resolve-succeed-claim")
	firstOp := a1.OperationID
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)
	// Lease loss keeps the attempt LOST for the preservation assertion.
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE task_leases SET expires_at=clock_timestamp()-INTERVAL '1 second' WHERE attempt_id=$1::uuid`, a1.AttemptID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	engine := execution.NewWorkerEngine(tc.pool)
	if _, err := engine.ReconcileExpiredLeases(context.Background(), orgID); err != nil {
		t.Fatal(err)
	}
	caseID, rev := openReconciliationCase(t, tc, orgID, stepID)
	_ = firstOp

	token, userID := reconcileHumanToken(t, tc, orgID, tenant.RoleOperator, "succeed")
	status, body := resolveCaseHTTP(t, server, token, orgID, caseID, map[string]any{
		"action": "confirm_succeeded", "evidence": "prov-pay-1",
		"result": map[string]any{"ok": true}, "expectedRevision": rev,
	})
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d (%v)", status, body)
	}
	if body["resolved"] != true || body["caseId"] != caseID || int64(body["revision"].(float64)) != rev+1 {
		t.Fatalf("unexpected resolve response: %v", body)
	}

	var stepState, completionSource, runStatus, attemptStatus string
	var rawOutput []byte
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT state, completion_source, output FROM run_steps WHERE id=$1::uuid`, stepID).Scan(&stepState, &completionSource, &rawOutput); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT status FROM task_attempts WHERE id=$1::uuid`, a1.AttemptID).Scan(&attemptStatus)
	}); err != nil {
		t.Fatal(err)
	}
	if stepState != "SUCCEEDED" || completionSource != "RECONCILIATION" {
		t.Fatalf("expected SUCCEEDED/RECONCILIATION, got %s/%s", stepState, completionSource)
	}
	if attemptStatus != "LOST" {
		t.Fatalf("held attempt must stay LOST, got %s", attemptStatus)
	}
	if runStatus != "SUCCEEDED" {
		t.Fatalf("single-node run should complete, got %s", runStatus)
	}
	var out map[string]any
	_ = json.Unmarshal(rawOutput, &out)
	if out["ok"] != true {
		t.Fatalf("run output should carry the confirmed result, got %s", string(rawOutput))
	}
	// Case closed with the deciding actor; audit trail bound.
	var resolution, actorID string
	var auditCount int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT resolution, actor_id::text FROM reconciliation_cases WHERE id=$1::uuid`, caseID).Scan(&resolution, &actorID); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT count(*) FROM audit_events WHERE target_id=$1::uuid AND action='reconciliation.resolve'`, caseID).Scan(&auditCount)
	}); err != nil {
		t.Fatal(err)
	}
	if resolution != "SUCCEED" || actorID != userID || auditCount != 1 {
		t.Fatalf("case/audit binding wrong: resolution=%s actor=%s audit=%d", resolution, actorID, auditCount)
	}
}

// TestResolveConfirmNotExecutedRetry proves Blueprint §14.3 action 2: the
// retry keeps the operation ID, parks a fresh timer, and the next claim
// creates attempt 2.
func TestResolveConfirmNotExecutedRetry(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "resolve-retry")
	const digest = "bundle-resolve-retry-19"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, reconcileManifest(3, 60000))
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "resolve-retry-claim")
	firstOp := a1.OperationID
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)
	completeWithError(t, server, session, a1, "PROVIDER_500", true, "UNKNOWN", "")
	caseID, rev := openReconciliationCase(t, tc, orgID, stepID)

	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleOperator, "retry")
	status, body := resolveCaseHTTP(t, server, token, orgID, caseID, map[string]any{
		"action": "confirm_not_executed_retry", "evidence": "prov-no-charge",
		"expectedRevision": rev,
	})
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d (%v)", status, body)
	}
	stepState, _, _, _, timerState, dueAt, timerOp, _, _, nextAttempt := queryRetryState(t, tc, orgID, stepID, runID)
	if stepState != "WAITING" || timerState != "PENDING" || dueAt == nil {
		t.Fatalf("retry intent must park timer, got step %s timer %s", stepState, timerState)
	}
	if timerOp != firstOp {
		t.Fatalf("operation ID changed on resolve-retry: %s vs %s", firstOp, timerOp)
	}
	if nextAttempt != 2 {
		t.Fatalf("expected next attempt 2, got %d", nextAttempt)
	}
	// Fire and claim: attempt 2 carries the same operation ID.
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE timers SET due_at=clock_timestamp()-INTERVAL '1 second' WHERE step_id=$1::uuid AND state='PENDING'`, stepID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	engine := execution.NewWorkerEngine(tc.pool)
	if _, err := engine.FireDueRetryTimers(context.Background(), orgID); err != nil {
		t.Fatal(err)
	}
	a2 := claimExecution(t, server, session, digest, "resolve-retry-claim-2")
	if a2.OperationID != firstOp {
		t.Fatalf("operation ID changed across resolve-retry: %s vs %s", firstOp, a2.OperationID)
	}
}

// TestResolveFailRun proves Blueprint §14.3 action 3 with a visible reason.
func TestResolveFailRun(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "resolve-fail")
	const digest = "bundle-resolve-fail-19"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, reconcileManifest(3, 60000))
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "resolve-fail-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)
	completeWithError(t, server, session, a1, "PROVIDER_500", true, "UNKNOWN", "")
	caseID, rev := openReconciliationCase(t, tc, orgID, stepID)

	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleOperator, "fail")
	status, _ := resolveCaseHTTP(t, server, token, orgID, caseID, map[string]any{
		"action": "fail_run", "evidence": "prov-confirmed-duplicate",
		"expectedRevision": rev,
	})
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}
	var stepState, runStatus string
	var reasonCode *string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT state FROM run_steps WHERE id=$1::uuid`, stepID).Scan(&stepState); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT status, reason_code FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus, &reasonCode)
	}); err != nil {
		t.Fatal(err)
	}
	if stepState != "FAILED" || runStatus != "FAILED" || reasonCode == nil || *reasonCode != "RECONCILIATION_FAILED" {
		t.Fatalf("expected FAILED/RECONCILIATION_FAILED, got %s/%s/%v", stepState, runStatus, reasonCode)
	}
}

// TestResolveRevisionConflictAndDoubleResolve proves optimistic concurrency:
// stale revisions and repeated decisions return 409 with the latest state.
func TestResolveRevisionConflictAndDoubleResolve(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "resolve-conflict")
	const digest = "bundle-resolve-conflict-19"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, reconcileManifest(3, 60000))
	_, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "resolve-conflict-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)
	completeWithError(t, server, session, a1, "PROVIDER_500", true, "UNKNOWN", "")
	caseID, rev := openReconciliationCase(t, tc, orgID, stepID)
	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleOperator, "conflict")

	status, body := resolveCaseHTTP(t, server, token, orgID, caseID, map[string]any{
		"action": "fail_run", "evidence": "x", "expectedRevision": rev + 99,
	})
	if status != http.StatusConflict || body["code"] != "REVISION_CONFLICT" {
		t.Fatalf("expected 409 REVISION_CONFLICT, got %d (%v)", status, body)
	}
	status, _ = resolveCaseHTTP(t, server, token, orgID, caseID, map[string]any{
		"action": "fail_run", "evidence": "x", "expectedRevision": rev,
	})
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}
	status, body = resolveCaseHTTP(t, server, token, orgID, caseID, map[string]any{
		"action": "fail_run", "evidence": "x", "expectedRevision": rev + 1,
	})
	if status != http.StatusConflict || body["code"] != "CASE_RESOLVED" {
		t.Fatalf("expected 409 CASE_RESOLVED, got %d (%v)", status, body)
	}
}

// TestResolveConcurrentSingleWinner proves two concurrent resolvers produce
// exactly one decision; the loser sees the committed state via 409.
func TestResolveConcurrentSingleWinner(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "resolve-race")
	const digest = "bundle-resolve-race-19"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, reconcileManifest(3, 60000))
	_, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "resolve-race-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)
	completeWithError(t, server, session, a1, "PROVIDER_500", true, "UNKNOWN", "")
	caseID, rev := openReconciliationCase(t, tc, orgID, stepID)
	tokenA, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleOperator, "race-a")
	tokenB, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleAdmin, "race-b")

	type outcome struct {
		status int
		code   string
		fatal  string
	}
	results := make(chan outcome, 2)
	start := make(chan struct{})
	for _, tok := range []string{tokenA, tokenB} {
		go func(tok string) {
			// No t.* calls inside goroutines: report outcomes through the
			// channel so the race detector stays quiet.
			var o outcome
			defer func() { results <- o }()
			<-start
			raw, _ := json.Marshal(map[string]any{
				"action": "fail_run", "evidence": "race", "expectedRevision": rev,
			})
			atomic.AddInt64(&resolveKeySeq, 1)
			req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/reconciliation-cases/"+caseID+"/resolve", bytes.NewReader(raw))
			if err != nil {
				o.fatal = err.Error()
				return
			}
			req.Header.Set("Authorization", "Bearer "+tok)
			req.Header.Set("X-Organization-ID", orgID)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", fmt.Sprintf("resolve-race-%d", atomic.AddInt64(&resolveKeySeq, 1)))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				o.fatal = err.Error()
				return
			}
			defer resp.Body.Close()
			var parsed map[string]any
			_ = json.NewDecoder(resp.Body).Decode(&parsed)
			o.status = resp.StatusCode
			if code, _ := parsed["code"].(string); code != "" {
				o.code = code
			}
		}(tok)
	}
	close(start)
	first := <-results
	second := <-results
	if first.fatal != "" || second.fatal != "" {
		t.Fatalf("concurrent resolve transport failed: %q %q", first.fatal, second.fatal)
	}
	codes := map[int]int{}
	for _, o := range []outcome{first, second} {
		codes[o.status]++
	}
	if codes[http.StatusOK] != 1 || codes[http.StatusConflict] != 1 {
		t.Fatalf("expected one 200 and one 409, got %+v / %+v", first, second)
	}
}

// TestResolveRetryBudgetExhaustedDenied proves exhausted budget cannot be
// bypassed: the retry is refused with 409 and the hold stays open.
func TestResolveRetryBudgetExhaustedDenied(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "resolve-budget")
	const digest = "bundle-resolve-budget-19"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, reconcileManifest(1, 60000))
	_, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "resolve-budget-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)
	// maxAttempts=1: hold still forms (ambiguous outcomes never blind-retry),
	// but no retry remains.
	completeWithError(t, server, session, a1, "PROVIDER_500", true, "UNKNOWN", "")
	caseID, rev := openReconciliationCase(t, tc, orgID, stepID)
	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleOperator, "budget")

	status, body := resolveCaseHTTP(t, server, token, orgID, caseID, map[string]any{
		"action": "confirm_not_executed_retry", "evidence": "prov-no-charge",
		"expectedRevision": rev,
	})
	if status != http.StatusConflict || body["code"] != "BUDGET_EXHAUSTED" {
		t.Fatalf("expected 409 BUDGET_EXHAUSTED, got %d (%v)", status, body)
	}
	var caseStatus string
	var pendingTimers int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT status FROM reconciliation_cases WHERE id=$1::uuid`, caseID).Scan(&caseStatus); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT count(*) FROM timers WHERE step_id=$1::uuid AND state='PENDING'`, stepID).Scan(&pendingTimers)
	}); err != nil {
		t.Fatal(err)
	}
	if caseStatus != "OPEN" || pendingTimers != 0 {
		t.Fatalf("denied retry must leave hold open without timer: %s timers=%d", caseStatus, pendingTimers)
	}
}

// TestResolveDeniedActors proves machine keys and under-privileged humans
// cannot resolve holds.
func TestResolveDeniedActors(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, adminKey := enrollExecutionWorker(t, tc, server, orgID, envID, "resolve-denied")
	_ = adminKey
	const digest = "bundle-resolve-denied-19"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, reconcileManifest(3, 60000))
	_, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "resolve-denied-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)
	completeWithError(t, server, session, a1, "PROVIDER_500", true, "UNKNOWN", "")
	caseID, rev := openReconciliationCase(t, tc, orgID, stepID)

	machineKey := bootstrapTestKey(t, tc.service, orgID, envID, []string{tenant.CapRunsCreate, tenant.CapRunsRead})
	status, body := resolveCaseHTTP(t, server, machineKey.PlaintextKey, orgID, caseID, map[string]any{
		"action": "fail_run", "evidence": "x", "expectedRevision": rev,
	})
	if status != http.StatusForbidden {
		t.Fatalf("machine key must be denied, got %d (%v)", status, body)
	}
	devToken, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleDeveloper, "denied-dev")
	status, body = resolveCaseHTTP(t, server, devToken, orgID, caseID, map[string]any{
		"action": "fail_run", "evidence": "x", "expectedRevision": rev,
	})
	if status != http.StatusForbidden {
		t.Fatalf("developer must be denied runs:reconcile, got %d (%v)", status, body)
	}
	// Hold untouched by denied attempts.
	var caseStatus string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status FROM reconciliation_cases WHERE id=$1::uuid`, caseID).Scan(&caseStatus)
	}); err != nil {
		t.Fatal(err)
	}
	if caseStatus != "OPEN" {
		t.Fatalf("denied attempts must not touch the hold, got %s", caseStatus)
	}
}

// TestResolveValidationFailures proves missing evidence/result and
// schema-invalid results are rejected without touching the hold.
func TestResolveValidationFailures(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "resolve-validation")
	const digest = "bundle-resolve-validation-19"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, reconcileManifest(3, 60000))
	_, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "resolve-validation-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)
	completeWithError(t, server, session, a1, "PROVIDER_500", true, "UNKNOWN", "")
	caseID, rev := openReconciliationCase(t, tc, orgID, stepID)
	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleOperator, "validation")

	for _, tc2 := range []struct {
		name   string
		body   map[string]any
		status int
	}{
		{"missing-evidence", map[string]any{"action": "fail_run", "expectedRevision": rev}, http.StatusBadRequest},
		{"missing-result", map[string]any{"action": "confirm_succeeded", "evidence": "prov-x", "expectedRevision": rev}, http.StatusBadRequest},
		{"bad-action", map[string]any{"action": "retry_anyway", "evidence": "prov-x", "expectedRevision": rev}, http.StatusBadRequest},
		{"schema-violation", map[string]any{"action": "confirm_succeeded", "evidence": "prov-x", "result": "not-an-object", "expectedRevision": rev}, http.StatusUnprocessableEntity},
	} {
		status, _ := resolveCaseHTTP(t, server, token, orgID, caseID, tc2.body)
		if status != tc2.status {
			t.Fatalf("%s: expected %d, got %d", tc2.name, tc2.status, status)
		}
	}
	var caseStatus string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status FROM reconciliation_cases WHERE id=$1::uuid`, caseID).Scan(&caseStatus)
	}); err != nil {
		t.Fatal(err)
	}
	if caseStatus != "OPEN" {
		t.Fatalf("invalid resolutions must leave the hold open, got %s", caseStatus)
	}
}

func twoNodeHoldManifest() string {
	return `{"targetOS":"linux","targetArchitecture":"amd64","tasks":[{"name":"t-a","entrypoint":"tasks/a.js","recovery":"reconcile","timeoutMs":60000,"inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"retry":{"maxAttempts":3,"initialDelayMs":1000,"maxDelayMs":30000}},{"name":"t-b","entrypoint":"tasks/b.js","recovery":"reconcile","timeoutMs":60000,"inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"retry":{"maxAttempts":3,"initialDelayMs":1000,"maxDelayMs":30000}}],"workflows":[{"name":"workflow-a","inputSchema":{"type":"object"},"nodes":[{"id":"a","type":"task","task":"t-a"},{"id":"b","type":"task","task":"t-b","after":["a"]}]}]}`
}

// seedTwoHoldRun creates a run with two WAITING/RECONCILIATION steps, each
// with an OPEN case, modeling multiple unknown outcomes in one run.
func seedTwoHoldRun(t *testing.T, tc *tenantTestContext, orgID, envID, deploymentID string) (string, string, string, string, string) {
	t.Helper()
	var runID, stepA, stepB, caseA, caseB string
	err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `INSERT INTO runs
			(organization_id,environment_id,deployment_id,workflow_name,status,reason_code)
			VALUES ($1,$2,$3,'workflow-a','WAITING','RECONCILIATION') RETURNING id::text`, orgID, envID, deploymentID).Scan(&runID); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `INSERT INTO run_steps
			(organization_id,environment_id,run_id,node_id,state,wait_reason)
			VALUES ($1,$2,$3,'a','WAITING','RECONCILIATION') RETURNING id::text`, orgID, envID, runID).Scan(&stepA); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `INSERT INTO run_steps
			(organization_id,environment_id,run_id,node_id,state,wait_reason)
			VALUES ($1,$2,$3,'b','WAITING','RECONCILIATION') RETURNING id::text`, orgID, envID, runID).Scan(&stepB); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `INSERT INTO reconciliation_cases
			(organization_id,environment_id,step_id,reason,evidence,status)
			VALUES ($1,$2,$3,'AMBIGUOUS_OUTCOME','{}','OPEN') RETURNING id::text`, orgID, envID, stepA).Scan(&caseA); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `INSERT INTO reconciliation_cases
			(organization_id,environment_id,step_id,reason,evidence,status)
			VALUES ($1,$2,$3,'AMBIGUOUS_OUTCOME','{}','OPEN') RETURNING id::text`, orgID, envID, stepB).Scan(&caseB)
	})
	if err != nil {
		t.Fatal(err)
	}
	return runID, stepA, stepB, caseA, caseB
}

// TestMultiHoldReleaseOnlyWhenAllResolve proves the hold lifts only after the
// last OPEN case resolves: a partial resolution keeps the run parked.
func TestMultiHoldReleaseOnlyWhenAllResolve(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	const digest = "bundle-multi-hold-19"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, twoNodeHoldManifest())
	runID, _, _, caseA, caseB := seedTwoHoldRun(t, tc, orgID, envID, deploymentID)
	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleOperator, "multi")

	var revA, revB int64
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT revision FROM reconciliation_cases WHERE id=$1::uuid`, caseA).Scan(&revA); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT revision FROM reconciliation_cases WHERE id=$1::uuid`, caseB).Scan(&revB)
	}); err != nil {
		t.Fatal(err)
	}

	status, _ := resolveCaseHTTP(t, server, token, orgID, caseA, map[string]any{
		"action": "confirm_succeeded", "evidence": "prov-a",
		"result": map[string]any{"x": 1}, "expectedRevision": revA,
	})
	if status != http.StatusOK {
		t.Fatalf("first resolve expected 200, got %d", status)
	}
	var runStatus, runReason string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, reason_code FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus, &runReason)
	}); err != nil {
		t.Fatal(err)
	}
	if runStatus != "WAITING" || runReason != "RECONCILIATION" {
		t.Fatalf("partial resolution must keep the hold, got %s/%s", runStatus, runReason)
	}

	status, _ = resolveCaseHTTP(t, server, token, orgID, caseB, map[string]any{
		"action": "confirm_succeeded", "evidence": "prov-b",
		"result": map[string]any{"y": 2}, "expectedRevision": revB,
	})
	if status != http.StatusOK {
		t.Fatalf("second resolve expected 200, got %d", status)
	}
	var finalStatus string
	var rawOutput []byte
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT status, output FROM runs WHERE id=$1::uuid`, runID).Scan(&finalStatus, &rawOutput); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if finalStatus != "SUCCEEDED" {
		t.Fatalf("run should complete after all holds resolve, got %s", finalStatus)
	}
	var out map[string]any
	_ = json.Unmarshal(rawOutput, &out)
	if out["y"] != float64(2) {
		t.Fatalf("run output should carry the last confirmed result, got %s", string(rawOutput))
	}
}

// TestDeadlineDuringHoldFailsRun proves the run deadline stays active while
// held: resolving past the deadline is refused, and the sweeper terminalizes
// the run and closes its holds.
func TestDeadlineDuringHoldFailsRun(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "resolve-deadline")
	const digest = "bundle-resolve-deadline-19"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, reconcileManifest(3, 60000))
	runID, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	a1 := claimExecution(t, server, session, digest, "resolve-deadline-claim")
	startNode(t, server, session, a1.AttemptID, a1.OwnershipEpoch)
	completeWithError(t, server, session, a1, "PROVIDER_500", true, "UNKNOWN", "")
	caseID, rev := openReconciliationCase(t, tc, orgID, stepID)
	token, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleOperator, "deadline")

	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE runs SET deadline_at=clock_timestamp()-INTERVAL '1 second' WHERE id=$1::uuid`, runID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	status, body := resolveCaseHTTP(t, server, token, orgID, caseID, map[string]any{
		"action": "confirm_succeeded", "evidence": "prov-x",
		"result": map[string]any{"ok": true}, "expectedRevision": rev,
	})
	if status != http.StatusConflict || body["code"] != "RUN_DEADLINE_EXCEEDED" {
		t.Fatalf("post-deadline resolve must be refused, got %d (%v)", status, body)
	}

	engine := execution.NewWorkerEngine(tc.pool)
	if _, err := engine.ReconcileExpiredLeases(context.Background(), orgID); err != nil {
		t.Fatal(err)
	}
	var runStatus, caseStatus string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT status FROM reconciliation_cases WHERE id=$1::uuid`, caseID).Scan(&caseStatus)
	}); err != nil {
		t.Fatal(err)
	}
	if runStatus != "FAILED" || caseStatus != "RESOLVED" {
		t.Fatalf("overdue held run must terminalize with closed holds, got %s/%s", runStatus, caseStatus)
	}
}

func siblingManifest() string {
	return `{"targetOS":"linux","targetArchitecture":"amd64","tasks":[{"name":"t-a","entrypoint":"tasks/a.js","recovery":"safe","timeoutMs":60000,"inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"retry":{"maxAttempts":3,"initialDelayMs":1000,"maxDelayMs":30000}},{"name":"t-b","entrypoint":"tasks/b.js","recovery":"reconcile","timeoutMs":60000,"inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"retry":{"maxAttempts":3,"initialDelayMs":1000,"maxDelayMs":30000}}],"workflows":[{"name":"workflow-a","inputSchema":{"type":"object"},"nodes":[{"id":"a","type":"task","task":"t-a"},{"id":"b","type":"task","task":"t-b"}]}]}`
}

func attemptNode(t *testing.T, tc *tenantTestContext, orgID, attemptID string) string {
	t.Helper()
	var node string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT rs.node_id FROM run_steps rs
			JOIN task_attempts a ON a.step_id=rs.id WHERE a.id=$1::uuid`, attemptID).Scan(&node)
	}); err != nil {
		t.Fatal(err)
	}
	return node
}

// TestSiblingCompletionDuringHoldAndDrain proves siblings may finish while a
// hold is open, new claims stay blocked run-wide, and the run parks in
// WAITING/RECONCILIATION only after the last sibling drains.
func TestSiblingCompletionDuringHoldAndDrain(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "resolve-sibling")
	const digest = "bundle-resolve-sibling-19"
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, siblingManifest())
	runID, _ := seedExecutionRun(t, tc, orgID, envID, deploymentID, "a")
	var stepB string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `INSERT INTO run_steps
			(organization_id,environment_id,run_id,node_id,state,eligible_at)
			VALUES ($1,$2,$3,'b','READY',clock_timestamp()) RETURNING id::text`, orgID, envID, runID).Scan(&stepB)
	}); err != nil {
		t.Fatal(err)
	}
	advertiseDigest(t, tc, orgID, session.SessionID, digest)

	first := claimExecution(t, server, session, digest, "sibling-claim-1")
	second := claimExecution(t, server, session, digest, "sibling-claim-2")
	claims := map[string]worker.AssignmentDTO{
		attemptNode(t, tc, orgID, first.AttemptID):  first,
		attemptNode(t, tc, orgID, second.AttemptID): second,
	}
	claimA, claimB := claims["a"], claims["b"]
	if claimA.AttemptID == "" || claimB.AttemptID == "" {
		t.Fatalf("expected claims for both siblings, got %v", claims)
	}
	startNode(t, server, session, claimA.AttemptID, claimA.OwnershipEpoch)
	startNode(t, server, session, claimB.AttemptID, claimB.OwnershipEpoch)

	// B's unknown outcome holds while A is still running: the run must stay
	// RUNNING so the sibling can finish.
	completeWithError(t, server, session, claimB, "PROVIDER_500", true, "UNKNOWN", "")
	var runStatus string
	var runReason *string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, reason_code FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus, &runReason)
	}); err != nil {
		t.Fatal(err)
	}
	if runStatus != "RUNNING" {
		t.Fatalf("run must stay RUNNING while a sibling drains, got %s/%v", runStatus, runReason)
	}
	openReconciliationCase(t, tc, orgID, stepB)

	// New claims stay blocked run-wide despite the RUNNING status.
	var blocked worker.PollResponseDTO
	if status := postWorkerJSON(t, server, "/worker/v1/poll", session.SessionToken, worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "sibling-blocked-poll", WorkerID: session.WorkerID,
		SessionID: session.SessionID, AvailableSlots: 1, DeploymentDigests: []string{digest}, Pool: "default",
	}, &blocked); status != http.StatusOK || len(blocked.Assignments) != 0 {
		t.Fatalf("claims must be blocked during the hold, got status %d n=%d", status, len(blocked.Assignments))
	}

	// The sibling finishes normally; the run then parks on the hold.
	completion := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "sibling-complete-a",
		WorkerID: session.WorkerID, SessionID: session.SessionID,
		AttemptID: claimA.AttemptID, OwnershipEpoch: claimA.OwnershipEpoch,
		Outcome: "SUCCEEDED", Output: map[string]any{"ok": true},
	}
	completion.ResultDigest, _ = worker.CanonicalCompletionDigest(&completion)
	var compResp worker.CompleteResponseDTO
	if status := postWorkerJSON(t, server, "/worker/v1/complete", session.SessionToken, completion, &compResp); status != http.StatusOK {
		t.Fatalf("sibling completion must be accepted during hold, got %d", status)
	}
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, reason_code FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus, &runReason)
	}); err != nil {
		t.Fatal(err)
	}
	if runStatus != "WAITING" || runReason == nil || *runReason != "RECONCILIATION" {
		t.Fatalf("run must park after siblings drain, got %s/%v", runStatus, runReason)
	}
}
