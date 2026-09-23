package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/artifacts"
	"github.com/Ryanakml/Deadbolt/internal/controlplane"
	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/recovery"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
	"github.com/Ryanakml/Deadbolt/tests/fixtures/externaleffect"
)

func disasterManifest(taskName, recoveryPolicy string) string {
	return fmt.Sprintf(`{"targetOS":"linux","targetArchitecture":"amd64","tasks":[{"name":"%s","entrypoint":"tasks/main.js","recovery":"%s","timeoutMs":10000,"inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"retry":{"maxAttempts":3,"initialDelayMs":1000,"maxDelayMs":30000}}],"workflows":[{"name":"wf-disaster","inputSchema":{"type":"object"},"nodes":[{"id":"node-1","type":"task","task":"%s"}]}]}`, taskName, recoveryPolicy, taskName)
}

func setupDisasterRecoveryHarness(t *testing.T) (*tenantTestContext, *httptest.Server, *recovery.Manager, string, string, string, string, string) {
	t.Helper()
	tc := setupTenantContext(t)

	// Ensure system_recovery_controls and latest migrations are active
	ctx := context.Background()
	_, _ = tc.pool.Exec(ctx, `INSERT INTO system_recovery_controls (id, mode, admission_enabled, dispatch_enabled, schedules_enabled)
		VALUES (1, 'ACTIVE', TRUE, TRUE, TRUE)
		ON CONFLICT (id) DO UPDATE SET mode = 'ACTIVE', admission_enabled = TRUE, dispatch_enabled = TRUE, schedules_enabled = TRUE`)

	prodMux := controlplane.BuildMux(tc.authCfg, tc.runtimePool, nil, nil)
	server := httptest.NewServer(prodMux)
	tc.cleanup = func() {
		server.Close()
		tc.database.Close()
		tc.runtimePool.Close()
	}

	recoveryMgr := recovery.NewManager(tc.pool)

	// Create org, project, env, API key
	owner, _ := tenant.NewUUID()
	org, err := tc.service.CreateOrganization(ctx, owner, "Disaster Recovery Org")
	if err != nil {
		t.Fatalf("create org: %v", err)
	}
	proj, err := tc.service.CreateProject(ctx, org.ID, "Disaster Project")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	env, err := tc.service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvProduction, 10)
	if err != nil {
		t.Fatalf("create env: %v", err)
	}

	caps := []string{
		tenant.CapRunsCreate, tenant.CapRunsRead, tenant.CapRunsControl,
		tenant.CapDeploymentsRegister, tenant.CapDeploymentsActivateProd,
		tenant.CapWorkersRead, tenant.CapWorkersDrain, tenant.CapArtifactsWrite, tenant.CapPayloadRead,
	}
	apiKey := bootstrapTestKey(t, tc.service, org.ID, env.ID, caps)

	humanToken, _ := reconcileHumanToken(t, tc, org.ID, tenant.RoleOperator, "dr-op")

	return tc, server, recoveryMgr, org.ID, env.ID, apiKey.PlaintextKey, humanToken, proj.ID
}

// TestDisasterRecoveryPointRestoresAndEntersHold proves:
// 1. Restoring a known point disables admission, schedules, dispatch, and sessions.
// 2. All restored nonterminal runs enter WAITING/RECONCILIATION holds with recorded uncertainty window.
// 3. Terminal runs remain untouched (INV-09).
// 4. Admission returns 503 ADMISSION_DISABLED and worker claims return 0 assignments.
func TestDisasterRecoveryPointRestoresAndEntersHold(t *testing.T) {
	tc, server, recoveryMgr, orgID, envID, apiKey, _, _ := setupDisasterRecoveryHarness(t)
	defer tc.cleanup()
	ctx := context.Background()

	// 1. Deploy workflow
	manifest := disasterManifest("task-reconcile", "reconcile")
	bundleDigest := "sha256:1111222233334444555566667777888899990000aaaaabbbbbcccccdddddeeeee"
	var depID string
	err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `INSERT INTO deployments (organization_id, environment_id, manifest_hash, bundle_digest, manifest, protocol_version, runtime_version, status)
			VALUES ($1::uuid, $2::uuid, 'manifest-1', $3, $4::jsonb, 1, '1.0', 'ACTIVE')
			RETURNING id::text`, orgID, envID, bundleDigest, manifest).Scan(&depID)
	})
	if err != nil {
		t.Fatalf("create deployment: %v", err)
	}

	// 2. Seed runs: one RUNNING (nonterminal), one QUEUED (nonterminal), one SUCCEEDED (terminal)
	var runningRunID, queuedRunID, succeededRunID string
	var runningStepID, queuedStepID, succeededStepID string

	err = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		// Nonterminal RUNNING run
		if err := tx.QueryRow(ctx, `INSERT INTO runs (organization_id, environment_id, deployment_id, workflow_name, status, input)
			VALUES ($1::uuid, $2::uuid, $3::uuid, 'wf-disaster', 'RUNNING', '{}'::jsonb)
			RETURNING id::text`, orgID, envID, depID).Scan(&runningRunID); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `INSERT INTO run_steps (organization_id, environment_id, run_id, node_id, state)
			VALUES ($1::uuid, $2::uuid, $3::uuid, 'node-1', 'RUNNING')
			RETURNING id::text`, orgID, envID, runningRunID).Scan(&runningStepID); err != nil {
			return err
		}

		// Nonterminal QUEUED run
		if err := tx.QueryRow(ctx, `INSERT INTO runs (organization_id, environment_id, deployment_id, workflow_name, status, input)
			VALUES ($1::uuid, $2::uuid, $3::uuid, 'wf-disaster', 'QUEUED', '{}'::jsonb)
			RETURNING id::text`, orgID, envID, depID).Scan(&queuedRunID); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `INSERT INTO run_steps (organization_id, environment_id, run_id, node_id, state)
			VALUES ($1::uuid, $2::uuid, $3::uuid, 'node-1', 'READY')
			RETURNING id::text`, orgID, envID, queuedRunID).Scan(&queuedStepID); err != nil {
			return err
		}

		// Terminal SUCCEEDED run (INV-09: must remain untouched)
		if err := tx.QueryRow(ctx, `INSERT INTO runs (organization_id, environment_id, deployment_id, workflow_name, status, input)
			VALUES ($1::uuid, $2::uuid, $3::uuid, 'wf-disaster', 'SUCCEEDED', '{}'::jsonb)
			RETURNING id::text`, orgID, envID, depID).Scan(&succeededRunID); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `INSERT INTO run_steps (organization_id, environment_id, run_id, node_id, state)
			VALUES ($1::uuid, $2::uuid, $3::uuid, 'node-1', 'SUCCEEDED')
			RETURNING id::text`, orgID, envID, succeededRunID).Scan(&succeededStepID); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed runs: %v", err)
	}

	// Seed pre-disaster worker session
	workerSessionID, _ := tenant.NewUUID()
	workerID, _ := tenant.NewUUID()
	err = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO workers (id, organization_id, environment_id, public_key, pool_name)
			VALUES ($1::uuid, $2::uuid, $3::uuid, 'dr-worker-pubkey', 'default')`, workerID, orgID, envID)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO worker_sessions (id, worker_id, organization_id, environment_id, session_token_hash, created_at, expires_at)
			VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, 'tokenhash', clock_timestamp() - interval '10 minutes', clock_timestamp() + interval '1 hour')`,
			workerSessionID, workerID, orgID, envID)
		return err
	})
	if err != nil {
		t.Fatalf("seed worker session: %v", err)
	}

	// 3. Initiate Disaster Recovery Prepare
	recoveryPoint := time.Now().Add(-5 * time.Minute).UTC()
	incidentAt := time.Now().UTC()

	prepReport, err := recoveryMgr.PrepareDisasterRecovery(ctx, recovery.PrepareRequest{
		RecoveryPoint: recoveryPoint,
		IncidentAt:    incidentAt,
		OperatorNotes: "Simulated restore from 5-minute-old DB snapshot",
	})
	if err != nil {
		t.Fatalf("PrepareDisasterRecovery failed: %v", err)
	}

	if prepReport.Status != "READ_ONLY" {
		t.Errorf("expected status READ_ONLY, got %s", prepReport.Status)
	}
	if prepReport.RestoredRunsCount < 2 {
		t.Errorf("expected at least 2 restored nonterminal runs, got %d", prepReport.RestoredRunsCount)
	}
	if prepReport.RevokedWorkerSessionsCount < 1 {
		t.Errorf("expected at least 1 revoked worker session, got %d", prepReport.RevokedWorkerSessionsCount)
	}

	// 4. Verify system controls: admission and dispatch are disabled, mode is READ_ONLY
	controls, err := recoveryMgr.GetControls(ctx)
	if err != nil {
		t.Fatalf("GetControls failed: %v", err)
	}
	if controls.AdmissionEnabled || controls.DispatchEnabled || controls.SchedulesEnabled || controls.Mode != "READ_ONLY" {
		t.Fatalf("expected all controls disabled and mode READ_ONLY, got %+v", controls)
	}

	// 5. Verify worker session was revoked
	var workerRevokedAt *time.Time
	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT revoked_at FROM worker_sessions WHERE id = $1::uuid`, workerSessionID).Scan(&workerRevokedAt)
	})
	if workerRevokedAt == nil {
		t.Fatalf("expected pre-disaster worker session to be revoked, got nil")
	}

	// 6. Verify nonterminal runs are WAITING with RECONCILIATION
	var run1Status, run1Reason string
	var run2Status, run2Reason string
	var succStatus, succReason string
	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		_ = tx.QueryRow(ctx, `SELECT status, COALESCE(reason_code, '') FROM runs WHERE id = $1::uuid`, runningRunID).Scan(&run1Status, &run1Reason)
		_ = tx.QueryRow(ctx, `SELECT status, COALESCE(reason_code, '') FROM runs WHERE id = $1::uuid`, queuedRunID).Scan(&run2Status, &run2Reason)
		_ = tx.QueryRow(ctx, `SELECT status, COALESCE(reason_code, '') FROM runs WHERE id = $1::uuid`, succeededRunID).Scan(&succStatus, &succReason)
		return nil
	})

	if run1Status != "WAITING" || run1Reason != "RECONCILIATION" {
		t.Errorf("runningRun: expected WAITING/RECONCILIATION, got %s/%s", run1Status, run1Reason)
	}
	if run2Status != "WAITING" || run2Reason != "RECONCILIATION" {
		t.Errorf("queuedRun: expected WAITING/RECONCILIATION, got %s/%s", run2Status, run2Reason)
	}
	if succStatus != "SUCCEEDED" {
		t.Errorf("succeededRun: terminal state violated! expected SUCCEEDED, got %s", succStatus)
	}

	// 7. Verify reconciliation cases were opened with DISASTER_RECOVERY_HOLD and uncertainty window
	var case1Reason, case1EvidenceJSON string
	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT reason, evidence::text FROM reconciliation_cases WHERE step_id = $1::uuid AND status = 'OPEN'`, runningStepID).Scan(&case1Reason, &case1EvidenceJSON)
	})
	if case1Reason != "DISASTER_RECOVERY_HOLD" {
		t.Fatalf("expected reconciliation reason DISASTER_RECOVERY_HOLD, got %s", case1Reason)
	}

	var ev map[string]any
	if err := json.Unmarshal([]byte(case1EvidenceJSON), &ev); err != nil {
		t.Fatalf("parse evidence JSON: %v", err)
	}
	if ev["disasterRecovery"] != true {
		t.Errorf("expected disasterRecovery=true in evidence")
	}
	if ev["uncertaintyWindowStart"] == nil || ev["uncertaintyWindowEnd"] == nil {
		t.Errorf("expected uncertaintyWindow in evidence, got %+v", ev)
	}

	// 8. Test HTTP admission gating returns 503 ADMISSION_DISABLED
	createRunReq, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/wf-disaster/runs", bytes.NewReader([]byte(`{"environment":"production","input":{}}`)))
	createRunReq.Header.Set("Authorization", "Bearer "+apiKey)
	createRunReq.Header.Set("Content-Type", "application/json")
	createRunReq.Header.Set("Idempotency-Key", "dr-admission-test-1")
	resp, err := http.DefaultClient.Do(createRunReq)
	if err != nil {
		t.Fatalf("create run request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable during disaster recovery, got %d", resp.StatusCode)
	}
	var errResp map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&errResp)
	if errResp["code"] != "ADMISSION_DISABLED" {
		t.Errorf("expected error code ADMISSION_DISABLED, got %v", errResp)
	}
}

// TestDisasterRecoveryExternalEffectSurvivesRestore proves:
// 1. Separate external-effect ledger survives older DB snapshot.
// 2. Disaster reconciliation hold prevents naive re-execution of side effects.
// 3. Operator reviews external ledger and resolves case with confirm_succeeded.
// 4. Run completes with completion_source = 'RECONCILIATION' and 0 duplicate external side effects.
func TestDisasterRecoveryExternalEffectSurvivesRestore(t *testing.T) {
	tc, server, recoveryMgr, orgID, envID, _, humanToken, _ := setupDisasterRecoveryHarness(t)
	defer tc.cleanup()
	ctx := context.Background()

	// 1. Start external-effect fixture backed by disk ledger outside PostgreSQL
	extDir := t.TempDir()
	extFixture, err := externaleffect.NewFixture(extDir)
	if err != nil {
		t.Fatalf("create external effect fixture: %v", err)
	}
	defer extFixture.Close()

	// 2. Simulate worker having executed external side effect (e.g. payment / remote charge)
	// BEFORE the crash/restore occurred
	idempotencyKey := fmt.Sprintf("ext-payment-%d", time.Now().UnixNano())
	rec, err := extFixture.ExecuteEffect(idempotencyKey, "CHARGE_CUSTOMER", map[string]any{"amount": 5000, "currency": "USD"})
	if err != nil {
		t.Fatalf("execute external effect: %v", err)
	}
	if rec.Duplicate {
		t.Fatalf("expected initial effect not to be duplicate")
	}

	// 3. Now simulate the database travelling back in time (restored DB snapshot older than external effect)
	// Create workflow and run that was RUNNING at recovery point
	manifest := disasterManifest("charge-task", "reconcile")
	bundleDigest := "sha256:2222333344445555666677778888999900001111bbbbbcccccdddddeeeeefffff"
	var depID, runID, stepID string
	err = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `INSERT INTO deployments (organization_id, environment_id, manifest_hash, bundle_digest, manifest, protocol_version, runtime_version, status)
			VALUES ($1::uuid, $2::uuid, 'manifest-2', $3, $4::jsonb, 1, '1.0', 'ACTIVE')
			RETURNING id::text`, orgID, envID, bundleDigest, manifest).Scan(&depID); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `INSERT INTO runs (organization_id, environment_id, deployment_id, workflow_name, status, input)
			VALUES ($1::uuid, $2::uuid, $3::uuid, 'wf-disaster', 'RUNNING', '{}'::jsonb)
			RETURNING id::text`, orgID, envID, depID).Scan(&runID); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `INSERT INTO run_steps (organization_id, environment_id, run_id, node_id, state)
			VALUES ($1::uuid, $2::uuid, $3::uuid, 'node-1', 'RUNNING')
			RETURNING id::text`, orgID, envID, runID).Scan(&stepID)
	})
	if err != nil {
		t.Fatalf("seed DB: %v", err)
	}

	// 4. Initiate disaster recovery
	recoveryPoint := time.Now().Add(-10 * time.Minute).UTC()
	incidentAt := time.Now().UTC()
	_, err = recoveryMgr.PrepareDisasterRecovery(ctx, recovery.PrepareRequest{
		RecoveryPoint: recoveryPoint,
		IncidentAt:    incidentAt,
		OperatorNotes: "Restore older than external charge",
	})
	if err != nil {
		t.Fatalf("prepare disaster recovery: %v", err)
	}

	// 5. Verify the external effect ledger survived outside PostgreSQL
	// Re-instantiate fixture from the same directory to simulate service reboot / persistence
	survivedFixture, err := externaleffect.NewFixture(extDir)
	if err != nil {
		t.Fatalf("reload external fixture: %v", err)
	}
	defer survivedFixture.Close()

	if survivedFixture.Count() != 1 {
		t.Fatalf("expected 1 record in external ledger after restore, got %d", survivedFixture.Count())
	}
	survivedRec, exists := survivedFixture.Get(idempotencyKey)
	if !exists {
		t.Fatalf("expected idempotency key %s to exist in external ledger", idempotencyKey)
	}

	// 6. Verify run step is on hold with open reconciliation case
	caseID, rev := openReconciliationCase(t, tc, orgID, stepID)

	// 7. Operator reviews external effect ledger, sees the external charge succeeded,
	// and resolves the reconciliation case with action 'confirm_succeeded'
	outputPayload := map[string]any{
		"chargeId":       "ch_123456",
		"status":         "CHARGED",
		"externalLedger": true,
		"executedAt":     survivedRec.ExecutedAt.Format(time.RFC3339),
	}
	resolveBody := map[string]any{
		"action":           "confirm_succeeded",
		"evidence":         "ext-ledger-" + idempotencyKey,
		"reason":           "Operator verified charge in survived external effect ledger",
		"result":           outputPayload,
		"expectedRevision": rev,
	}

	// Verify pre-disaster human session was revoked by disaster recovery
	staleStatus, _ := resolveCaseHTTP(t, server, humanToken, orgID, caseID, resolveBody)
	if staleStatus != http.StatusUnauthorized {
		t.Fatalf("expected pre-disaster session to be rejected with 401 Unauthorized, got %d", staleStatus)
	}

	// Post-disaster operator logs in to resolve reconciliation cases
	postOpToken, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleOperator, "post-dr-op")
	status, res := resolveCaseHTTP(t, server, postOpToken, orgID, caseID, resolveBody)
	if status != http.StatusOK {
		t.Fatalf("expected 200 OK resolving reconciliation case, got %d: %v", status, res)
	}

	// 8. Verify the step and run completed as SUCCEEDED with completion_source = 'RECONCILIATION'
	var finalRunStatus string
	var stepState string
	var completionSource *string
	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		_ = tx.QueryRow(ctx, `SELECT status FROM runs WHERE id = $1::uuid`, runID).Scan(&finalRunStatus)
		_ = tx.QueryRow(ctx, `SELECT state, completion_source FROM run_steps WHERE id = $1::uuid`, stepID).Scan(&stepState, &completionSource)
		return nil
	})

	if finalRunStatus != "SUCCEEDED" {
		t.Fatalf("expected run status SUCCEEDED, got %s", finalRunStatus)
	}
	if stepState != "SUCCEEDED" {
		t.Fatalf("expected step state SUCCEEDED, got %s", stepState)
	}
	if completionSource == nil || *completionSource != "RECONCILIATION" {
		t.Fatalf("expected completion_source 'RECONCILIATION', got %v", completionSource)
	}

	// 9. Verify external ledger has NO duplicate attempts (0 duplicate side effects)
	if survivedFixture.DuplicateCount() != 0 {
		t.Fatalf("duplicate external side effect occurred! expected 0 duplicates, got %d", survivedFixture.DuplicateCount())
	}
}

// TestDisasterRecoveryGradualResumptionAndRPOGap proves:
// 1. Operator records requests accepted after recovery point (RPO gap).
// 2. Explicit disclaimer is attached: "no claim of zero RPO: requests accepted during the uncertainty window cannot be reconstructed from database alone".
// 3. Gradual resumption transitions: READ_ONLY -> RESUMING -> ACTIVE.
func TestDisasterRecoveryGradualResumptionAndRPOGap(t *testing.T) {
	tc, server, recoveryMgr, _, _, apiKey, _, _ := setupDisasterRecoveryHarness(t)
	defer tc.cleanup()
	ctx := context.Background()

	// 1. Prepare disaster recovery
	recPoint := time.Now().Add(-15 * time.Minute).UTC()
	incAt := time.Now().UTC()
	report, err := recoveryMgr.PrepareDisasterRecovery(ctx, recovery.PrepareRequest{
		RecoveryPoint: recPoint,
		IncidentAt:    incAt,
		OperatorNotes: "Drill with missing 15m delta",
	})
	if err != nil {
		t.Fatalf("prepare failed: %v", err)
	}

	// 2. Record absent-after-recovery-point request IDs (RPO gap)
	rpoRequestIDs := []string{"req-customer-abc-1", "req-customer-def-2"}
	updatedReport, err := recoveryMgr.ReconcileRPOGapRecords(ctx, report.ID, rpoRequestIDs, "Identified from edge access logs")
	if err != nil {
		t.Fatalf("ReconcileRPOGapRecords failed: %v", err)
	}

	if len(updatedReport.RPOGapRequests) != 2 {
		t.Errorf("expected 2 RPO gap requests, got %d", len(updatedReport.RPOGapRequests))
	}
	expectedDisclaimer := "no claim of zero RPO: requests accepted during the uncertainty window cannot be reconstructed from database alone"
	if updatedReport.RPONote != expectedDisclaimer {
		t.Errorf("expected RPO disclaimer %q, got %q", expectedDisclaimer, updatedReport.RPONote)
	}

	// 3. Gradual Resumption: transition to RESUMING
	controls, err := recoveryMgr.GradualResume(ctx, "RESUMING")
	if err != nil {
		t.Fatalf("GradualResume to RESUMING failed: %v", err)
	}
	if controls.Mode != "RESUMING" || controls.AdmissionEnabled || !controls.DispatchEnabled {
		t.Errorf("expected RESUMING with admission=false, dispatch=true; got %+v", controls)
	}

	// Admission still gated in RESUMING
	allowed, err := recoveryMgr.IsAdmissionAllowed(ctx)
	if err != nil || allowed {
		t.Fatalf("expected admission to still be gated during RESUMING")
	}

	// 4. Full Resumption: transition to ACTIVE
	controls, err = recoveryMgr.GradualResume(ctx, "ACTIVE")
	if err != nil {
		t.Fatalf("GradualResume to ACTIVE failed: %v", err)
	}
	if controls.Mode != "ACTIVE" || !controls.AdmissionEnabled || !controls.DispatchEnabled || !controls.SchedulesEnabled {
		t.Errorf("expected ACTIVE with all controls enabled; got %+v", controls)
	}

	allowed, err = recoveryMgr.IsAdmissionAllowed(ctx)
	if err != nil || !allowed {
		t.Fatalf("expected admission allowed when ACTIVE")
	}

	// 5. Test HTTP recovery endpoints
	getReq, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/system/recovery", nil)
	getReq.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("get recovery request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from GET /v1/system/recovery, got %d", resp.StatusCode)
	}
}

// TestDisasterRecoveryIntegrityAndDeletionLedgerHooks proves:
// 1. Schema, tenant boundary, and artifact reference integrity checks pass.
// 2. Deletion ledger review hook captures pending deletions to review before open access (Blueprint §18.3 & §27.3).
func TestDisasterRecoveryIntegrityAndDeletionLedgerHooks(t *testing.T) {
	tc, _, recoveryMgr, orgID, _, _, _, _ := setupDisasterRecoveryHarness(t)
	defer tc.cleanup()
	ctx := context.Background()

	reportBefore, err := recoveryMgr.VerifyIntegrity(ctx)
	if err != nil {
		t.Fatalf("VerifyIntegrity before failed: %v", err)
	}

	// Seed pending deletion in deletion ledger
	resourceID, _ := tenant.NewUUID()
	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO deletion_ledger (organization_id, resource_type, resource_id, purge_status, purge_due_at)
			VALUES ($1::uuid, 'PROJECT', $2::uuid, 'PENDING', clock_timestamp() + interval '7 days')`, orgID, resourceID)
		return err
	})

	report, err := recoveryMgr.VerifyIntegrity(ctx)
	if err != nil {
		t.Fatalf("VerifyIntegrity failed: %v", err)
	}

	if !report.SchemaValid {
		t.Errorf("expected SchemaValid=true")
	}
	if !report.TenantIsolationValid {
		t.Errorf("expected TenantIsolationValid=true")
	}
	if !report.ArtifactsValid {
		t.Errorf("expected ArtifactsValid=true")
	}
	if !report.DeletionLedgerValid {
		t.Errorf("expected DeletionLedgerValid=true")
	}
	if !report.OverallPassed {
		t.Errorf("expected OverallPassed=true")
	}
	if report.PendingDeletionCount != reportBefore.PendingDeletionCount+1 {
		t.Errorf("expected pending deletion count to increment by 1, before=%d, after=%d", reportBefore.PendingDeletionCount, report.PendingDeletionCount)
	}
}

// TestDisasterRecoveryRestoreScriptContractAndDryRun proves:
// restore-staging-db.sh supports --disaster-recovery flag and passes contract validation in dry run.
func TestDisasterRecoveryRestoreScriptContractAndDryRun(t *testing.T) {
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("failed to get repo root: %v", err)
	}
	scriptPath := filepath.Join(repoRoot, "scripts", "restore-staging-db.sh")
	if _, err := os.Stat(scriptPath); err != nil {
		t.Fatalf("restore script missing: %v", err)
	}

	cmd := exec.Command(scriptPath, "--disaster-recovery")
	cmd.Env = append(os.Environ(), "DRY_RUN=true")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("restore script dry run failed: %v\nOutput:\n%s", err, string(out))
	}

	outputStr := string(out)
	if !bytes.Contains(out, []byte("disaster recovery protocol enabled")) {
		t.Errorf("expected disaster recovery message in output, got: %s", outputStr)
	}
	if !bytes.Contains(out, []byte("DRY RUN passed")) {
		t.Errorf("expected DRY RUN passed in output, got: %s", outputStr)
	}
}

// TestDisasterRecoveryTenantCannotInvokeClusterRecovery proves INV-01: normal
// tenant-scoped credentials (API key with runs:control, Owner, Operator) must
// not be able to invoke cluster-wide disaster recovery mutations. Mutations
// live behind the host-operator recovery CLI, not tenant HTTP routes.
func TestDisasterRecoveryTenantCannotInvokeClusterRecovery(t *testing.T) {
	tc, server, _, orgID, envID, apiKey, _, _ := setupDisasterRecoveryHarness(t)
	defer tc.cleanup()
	ctx := context.Background()
	_ = ctx
	_ = envID

	ownerToken, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleOwner, "dr-no-owner")
	operatorToken, _ := reconcileHumanToken(t, tc, orgID, tenant.RoleOperator, "dr-no-op")

	mutating := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPost, "/v1/system/recovery/prepare", `{"recoveryPoint":"2026-09-22T14:00:00Z","incidentAt":"2026-09-22T14:30:00Z"}`},
		{http.MethodPost, "/v1/system/recovery/rpo-gap", `{"incidentId":"00000000-0000-0000-0000-000000000000","requestIds":[]}`},
		{http.MethodPost, "/v1/system/recovery/resume", `{"mode":"ACTIVE"}`},
		{http.MethodPost, "/api/v1/system/recovery/prepare", `{"recoveryPoint":"2026-09-22T14:00:00Z","incidentAt":"2026-09-22T14:30:00Z"}`},
	}
	creds := map[string]string{
		"tenant-api-key": apiKey,
		"owner":          ownerToken,
		"operator":       operatorToken,
	}
	for _, tc2 := range mutating {
		for name, cred := range creds {
			req, _ := http.NewRequest(tc2.method, server.URL+tc2.path, bytes.NewReader([]byte(tc2.body)))
			req.Header.Set("Authorization", "Bearer "+cred)
			req.Header.Set("X-Organization-ID", orgID)
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("%s %s as %s: request failed: %v", tc2.method, tc2.path, name, err)
			}
			buf := new(bytes.Buffer)
			_, _ = buf.ReadFrom(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
				t.Fatalf("%s %s as %s: tenant credential must not invoke cluster recovery, got %d: %s",
					tc2.method, tc2.path, name, resp.StatusCode, buf.String())
			}
			if strings.Contains(buf.String(), orgID) {
				t.Fatalf("%s %s as %s: error response leaks organization ID: %s", tc2.method, tc2.path, name, buf.String())
			}
		}
	}

	// Read-only visibility remains available to tenant operators.
	getReq, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/system/recovery", nil)
	getReq.Header.Set("Authorization", "Bearer "+apiKey)
	getReq.Header.Set("X-Organization-ID", orgID)
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("read-only recovery visibility failed: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for tenant read-only recovery visibility, got %d", getResp.StatusCode)
	}
}

// TestDisasterRecoveryGatesFailClosed proves admission and dispatch gates deny
// work when recovery controls are unreadable, and that READ_ONLY blocks both
// CreateRun (503) and Claim (zero assignments / 503).
func TestDisasterRecoveryGatesFailClosed(t *testing.T) {
	tc, server, recoveryMgr, orgID, envID, apiKey, _, _ := setupDisasterRecoveryHarness(t)
	defer tc.cleanup()
	ctx := context.Background()

	restoreControls := func() {
		_, _ = tc.pool.Exec(ctx, `INSERT INTO system_recovery_controls (id, mode, admission_enabled, dispatch_enabled, schedules_enabled)
			VALUES (1, 'ACTIVE', TRUE, TRUE, TRUE)
			ON CONFLICT (id) DO UPDATE SET mode='ACTIVE', admission_enabled=TRUE, dispatch_enabled=TRUE, schedules_enabled=TRUE`)
	}
	defer restoreControls()

	// 1. Controls row missing -> CreateRun must not admit (503, typed code).
	if _, err := tc.pool.Exec(ctx, `DELETE FROM system_recovery_controls WHERE id = 1`); err != nil {
		t.Fatalf("delete controls row: %v", err)
	}
	createReq, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/wf-disaster/runs",
		bytes.NewReader([]byte(`{"environment":"production","input":{}}`)))
	createReq.Header.Set("Authorization", "Bearer "+apiKey)
	createReq.Header.Set("Content-Type", "application/json")
	createReq.Header.Set("Idempotency-Key", "dr-failclosed-1")
	resp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatalf("create run request failed: %v", err)
	}
	func() {
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("missing controls row: expected 503, got %d", resp.StatusCode)
		}
		var parsed map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&parsed)
		if parsed["code"] != "RECOVERY_CONTROLS_UNAVAILABLE" {
			t.Fatalf("missing controls row: expected RECOVERY_CONTROLS_UNAVAILABLE, got %v", parsed)
		}
	}()

	// 2. Controls row missing -> engine Claim must not grant ownership.
	engine := execution.NewWorkerEngine(tc.pool)
	fakeSession := &worker.WorkerSessionContext{
		SessionID: "00000000-0000-0000-0000-000000000001", WorkerID: "00000000-0000-0000-0000-000000000002",
		OrganizationID: orgID, EnvironmentID: envID, PoolName: "default",
		ExpiresAt: time.Now().Add(time.Hour),
	}
	fakeReq := &worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "dr-failclosed-poll",
		WorkerID: fakeSession.WorkerID, SessionID: fakeSession.SessionID,
		AvailableSlots: 1, Pool: "default",
	}
	if _, err := engine.Claim(ctx, fakeSession, fakeReq); err == nil {
		t.Fatalf("missing controls row: engine Claim must fail closed, got nil error")
	} else if !strings.Contains(err.Error(), "RECOVERY_CONTROLS_UNAVAILABLE") {
		t.Fatalf("missing controls row: expected RECOVERY_CONTROLS_UNAVAILABLE, got %v", err)
	}

	// 3. READ_ONLY -> CreateRun rejected with ADMISSION_DISABLED.
	restoreControls()
	if _, err := recoveryMgr.PrepareDisasterRecovery(ctx, recovery.PrepareRequest{
		RecoveryPoint: time.Now().Add(-time.Minute).UTC(),
		IncidentAt:    time.Now().UTC(),
	}); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	createReq2, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/wf-disaster/runs",
		bytes.NewReader([]byte(`{"environment":"production","input":{}}`)))
	createReq2.Header.Set("Authorization", "Bearer "+apiKey)
	createReq2.Header.Set("Content-Type", "application/json")
	createReq2.Header.Set("Idempotency-Key", "dr-failclosed-2")
	resp2, err := http.DefaultClient.Do(createReq2)
	if err != nil {
		t.Fatalf("create run request failed: %v", err)
	}
	func() {
		defer resp2.Body.Close()
		if resp2.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("READ_ONLY: expected 503, got %d", resp2.StatusCode)
		}
	}()

	// 4. READ_ONLY -> Claim returns zero assignments and no error.
	claimed, err := engine.Claim(ctx, fakeSession, fakeReq)
	if err != nil {
		t.Fatalf("READ_ONLY: Claim must return empty assignments without error, got %v", err)
	}
	if len(claimed.Assignments) != 0 {
		t.Fatalf("READ_ONLY: expected 0 assignments, got %d", len(claimed.Assignments))
	}
}

// TestDisasterRecoveryPolicyResolutionPerTask proves recovery evidence records
// the correct policy for safe/idempotent/reconcile tasks where node ID differs
// from task name, and that unresolvable policy stays "unknown" yet held.
func TestDisasterRecoveryPolicyResolutionPerTask(t *testing.T) {
	tc, _, recoveryMgr, orgID, envID, _, _, _ := setupDisasterRecoveryHarness(t)
	defer tc.cleanup()
	ctx := context.Background()

	cases := []struct {
		name       string
		task       string
		policy     string
		wantPolicy string
		wantSource string
	}{
		{name: "safe", task: "task-charge-safe", policy: "safe", wantPolicy: "safe", wantSource: "deployment-manifest"},
		{name: "idempotent", task: "task-charge-idem", policy: "idempotent", wantPolicy: "idempotent", wantSource: "deployment-manifest"},
		{name: "reconcile", task: "task-charge-recon", policy: "reconcile", wantPolicy: "reconcile", wantSource: "deployment-manifest"},
	}
	stepIDs := make(map[string]string)
	for i, c := range cases {
		manifest := disasterManifest(c.task, c.policy)
		digest := fmt.Sprintf("sha256:policy-%d-1111222233334444555566667777888899990000aaaaabbbbbcccccdddddeeeee", i)
		var depID, runID, stepID string
		err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
			if err := tx.QueryRow(ctx, `INSERT INTO deployments (organization_id, environment_id, manifest_hash, bundle_digest, manifest, protocol_version, runtime_version, status)
				VALUES ($1::uuid, $2::uuid, $3, $4, $5::jsonb, 1, '1.0', 'ACTIVE')
				RETURNING id::text`, orgID, envID, fmt.Sprintf("manifest-policy-%d", i), digest, manifest).Scan(&depID); err != nil {
				return err
			}
			if err := tx.QueryRow(ctx, `INSERT INTO runs (organization_id, environment_id, deployment_id, workflow_name, status, input)
				VALUES ($1::uuid, $2::uuid, $3::uuid, 'wf-disaster', 'RUNNING', '{}'::jsonb)
				RETURNING id::text`, orgID, envID, depID).Scan(&runID); err != nil {
				return err
			}
			return tx.QueryRow(ctx, `INSERT INTO run_steps (organization_id, environment_id, run_id, node_id, state)
				VALUES ($1::uuid, $2::uuid, $3::uuid, 'node-1', 'RUNNING')
				RETURNING id::text`, orgID, envID, runID).Scan(&stepID)
		})
		if err != nil {
			t.Fatalf("seed %s: %v", c.name, err)
		}
		stepIDs[c.name] = stepID
	}

	// Unresolvable policy: manifest has no matching node/task.
	var unknownStepID string
	err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		var depID, runID string
		emptyManifest := `{"targetOS":"linux","targetArchitecture":"amd64","tasks":[],"workflows":[{"name":"wf-disaster","inputSchema":{"type":"object"},"nodes":[]}]}`
		if err := tx.QueryRow(ctx, `INSERT INTO deployments (organization_id, environment_id, manifest_hash, bundle_digest, manifest, protocol_version, runtime_version, status)
			VALUES ($1::uuid, $2::uuid, 'manifest-policy-unknown', 'sha256:policy-unknown', $3::jsonb, 1, '1.0', 'ACTIVE')
			RETURNING id::text`, orgID, envID, emptyManifest).Scan(&depID); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `INSERT INTO runs (organization_id, environment_id, deployment_id, workflow_name, status, input)
			VALUES ($1::uuid, $2::uuid, $3::uuid, 'wf-disaster', 'RUNNING', '{}'::jsonb)
			RETURNING id::text`, orgID, envID, depID).Scan(&runID); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `INSERT INTO run_steps (organization_id, environment_id, run_id, node_id, state)
			VALUES ($1::uuid, $2::uuid, $3::uuid, 'node-9', 'RUNNING')
			RETURNING id::text`, orgID, envID, runID).Scan(&unknownStepID)
	})
	if err != nil {
		t.Fatalf("seed unknown: %v", err)
	}

	if _, err := recoveryMgr.PrepareDisasterRecovery(ctx, recovery.PrepareRequest{
		RecoveryPoint: time.Now().Add(-time.Minute).UTC(),
		IncidentAt:    time.Now().UTC(),
	}); err != nil {
		t.Fatalf("prepare: %v", err)
	}

	for _, c := range cases {
		var reason, evidenceJSON string
		_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
			return tx.QueryRow(ctx, `SELECT reason, evidence::text FROM reconciliation_cases WHERE step_id = $1::uuid AND status = 'OPEN'`, stepIDs[c.name]).Scan(&reason, &evidenceJSON)
		})
		if reason != "DISASTER_RECOVERY_HOLD" {
			t.Fatalf("%s: expected DISASTER_RECOVERY_HOLD, got %s", c.name, reason)
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(evidenceJSON), &ev); err != nil {
			t.Fatalf("%s: parse evidence: %v", c.name, err)
		}
		if ev["recoveryPolicy"] != c.wantPolicy {
			t.Fatalf("%s: expected recoveryPolicy %q, got %v (node ID must not be used as task name)", c.name, c.wantPolicy, ev["recoveryPolicy"])
		}
		if ev["policySource"] != c.wantSource {
			t.Fatalf("%s: expected policySource %q, got %v", c.name, c.wantSource, ev["policySource"])
		}
	}

	var unknownReason, unknownEvidence string
	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT reason, evidence::text FROM reconciliation_cases WHERE step_id = $1::uuid AND status = 'OPEN'`, unknownStepID).Scan(&unknownReason, &unknownEvidence)
	})
	if unknownReason != "DISASTER_RECOVERY_HOLD" {
		t.Fatalf("unknown: expected hold, got %s", unknownReason)
	}
	var unknownEv map[string]any
	if err := json.Unmarshal([]byte(unknownEvidence), &unknownEv); err != nil {
		t.Fatalf("unknown: parse evidence: %v", err)
	}
	if unknownEv["recoveryPolicy"] != "unknown" {
		t.Fatalf("unknown: expected recoveryPolicy \"unknown\" (fail closed, never \"safe\"), got %v", unknownEv["recoveryPolicy"])
	}
}

// seedReadyArtifactRun claims, starts, and completes an artifact upload so the
// run stays nonterminal (RUNNING) with a READY artifact attached.
func seedReadyArtifactRun(t *testing.T, tc *tenantTestContext, server *httptest.Server, orgID, envID, suffix string) (runID, stepID, artifactID, storageKey string) {
	t.Helper()
	ctx := context.Background()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "dr-artifact-"+suffix)
	digest := "bundle-dr-artifact-" + suffix
	deploymentID := seedRetryDeployment(t, tc, orgID, envID, digest, artifactManifest())
	runID, stepID = seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")
	advertiseDigest(t, tc, orgID, session.SessionID, digest)
	a := claimExecution(t, server, session, digest, "dr-artifact-claim-"+suffix)
	startNode(t, server, session, a.AttemptID, a.OwnershipEpoch)

	payload := bytes.Repeat([]byte("r"), 64)
	status, created := postArtifactJSON(t, server, session.SessionToken, "/v1/artifacts", map[string]any{
		"runId": runID, "attemptId": a.AttemptID, "ownershipEpoch": a.OwnershipEpoch,
		"sizeBytes": len(payload), "sha256": sha256Hex(payload),
	})
	if status != http.StatusOK {
		t.Fatalf("reserve expected 200, got %d", status)
	}
	artifactID, _ = created["id"].(string)
	putObjectBytes(t, created["uploadUrl"].(string), payload)
	if status, _ := postArtifactJSON(t, server, session.SessionToken, "/v1/artifacts/"+artifactID+"/finalize", map[string]any{
		"attemptId": a.AttemptID, "ownershipEpoch": a.OwnershipEpoch, "sha256": sha256Hex(payload),
	}); status != http.StatusOK {
		t.Fatalf("finalize expected 200, got %d", status)
	}
	_ = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT storage_key FROM artifacts WHERE id = $1::uuid`, artifactID).Scan(&storageKey)
	})
	if storageKey == "" {
		t.Fatalf("expected storage key for artifact %s", artifactID)
	}
	return runID, stepID, artifactID, storageKey
}

// TestDisasterRecoveryArtifactIntegrityValid proves a valid referenced READY
// artifact passes restored artifact verification against real S3 storage.
func TestDisasterRecoveryArtifactIntegrityValid(t *testing.T) {
	tc, server, orgID, envID, _, store := setupArtifactSuite(t, "integrity-valid")
	defer tc.cleanup()
	defer server.Close()
	ctx := context.Background()
	_, _, _, _ = seedReadyArtifactRun(t, tc, server, orgID, envID, "valid")
	mgr := recovery.NewManager(tc.pool)
	mgr.SetArtifacts(artifacts.NewService(tc.pool, store))
	report, err := mgr.VerifyIntegrity(ctx)
	if err != nil {
		t.Fatalf("VerifyIntegrity failed: %v", err)
	}
	if !report.ArtifactsValid {
		t.Fatalf("expected ArtifactsValid=true for intact referenced object: %+v", report.Checks)
	}
	if !report.OverallPassed {
		t.Fatalf("expected OverallPassed=true: %+v", report.Checks)
	}
}

// TestDisasterRecoveryArtifactIntegrityMissing proves a referenced artifact
// whose S3 object is missing fails restored artifact verification.
func TestDisasterRecoveryArtifactIntegrityMissing(t *testing.T) {
	tc, server, orgID, envID, _, store := setupArtifactSuite(t, "integrity-missing")
	defer tc.cleanup()
	defer server.Close()
	ctx := context.Background()
	_, _, _, storageKey := seedReadyArtifactRun(t, tc, server, orgID, envID, "missing")
	if err := store.Delete(ctx, storageKey); err != nil {
		t.Fatalf("delete S3 object: %v", err)
	}
	mgr := recovery.NewManager(tc.pool)
	mgr.SetArtifacts(artifacts.NewService(tc.pool, store))
	report, err := mgr.VerifyIntegrity(ctx)
	if err != nil {
		t.Fatalf("VerifyIntegrity failed: %v", err)
	}
	if report.ArtifactsValid {
		t.Fatalf("expected ArtifactsValid=false for missing referenced object")
	}
	if report.OverallPassed {
		t.Fatalf("expected OverallPassed=false for missing referenced object")
	}
}

// TestDisasterRecoveryArtifactIntegrityCorrupt proves a same-size SHA mismatch
// fails restored artifact verification.
func TestDisasterRecoveryArtifactIntegrityCorrupt(t *testing.T) {
	tc, server, orgID, envID, _, store := setupArtifactSuite(t, "integrity-corrupt")
	defer tc.cleanup()
	defer server.Close()
	ctx := context.Background()
	_, _, _, storageKey := seedReadyArtifactRun(t, tc, server, orgID, envID, "corrupt")
	putURL, _, err := store.PresignPut(ctx, storageKey, "application/octet-stream", 5*time.Minute)
	if err != nil {
		t.Fatalf("presign put: %v", err)
	}
	putObjectBytes(t, putURL, bytes.Repeat([]byte("q"), 64))
	mgr := recovery.NewManager(tc.pool)
	mgr.SetArtifacts(artifacts.NewService(tc.pool, store))
	report, err := mgr.VerifyIntegrity(ctx)
	if err != nil {
		t.Fatalf("VerifyIntegrity failed: %v", err)
	}
	if report.ArtifactsValid {
		t.Fatalf("expected ArtifactsValid=false for corrupt same-size object")
	}
	if report.OverallPassed {
		t.Fatalf("expected OverallPassed=false for corrupt same-size object")
	}
}

// TestDisasterRecoveryCompatibleBinaryRollbackSmoke proves:
// rollback-staging.sh passes dry-run smoke validation without data rollback.
// (Additional contract check; real binary compatibility is proven by
// TestDisasterRecoveryRealBinaryRollbackSmoke.)
func TestDisasterRecoveryCompatibleBinaryRollbackSmoke(t *testing.T) {
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("failed to get repo root: %v", err)
	}
	scriptPath := filepath.Join(repoRoot, "scripts", "rollback-staging.sh")
	if _, err := os.Stat(scriptPath); err != nil {
		t.Fatalf("rollback script missing: %v", err)
	}

	cmd := exec.Command(scriptPath)
	cmd.Env = append(os.Environ(), "DRY_RUN=true")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("rollback script dry run failed: %v\nOutput:\n%s", err, string(out))
	}

	outputStr := string(out)
	if !bytes.Contains(out, []byte("DRY RUN passed")) {
		t.Errorf("expected DRY RUN passed in output, got: %s", outputStr)
	}
}
