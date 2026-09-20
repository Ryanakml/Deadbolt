package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
	"github.com/jackc/pgx/v5"
)

// Audited reconciliation resolution for unknown outcomes (Blueprint §14.3).
//
// A hold (WAITING/RECONCILIATION + OPEN reconciliation_cases row) is resolved
// by an identifiable human holding runs:reconcile through exactly one of:
//   - confirm_succeeded: submit a schema-valid result plus an evidence
//     reference. The failed attempt stays LOST/TIMED_OUT; the step becomes
//     SUCCEEDED with completion_source=RECONCILIATION.
//   - confirm_not_executed_retry: declare the effect did not occur and create
//     a retry intent within the remaining budget. The operation ID is unchanged.
//   - fail_run: stop the workflow with a visible reason.
//
// Every transition binds expectedRevision (409 on mismatch), the evidence
// reference, the decision reason, and the actor. Machine callers are rejected
// by the HTTP layer; this engine additionally refuses machine audit contexts.

var (
	ErrCaseNotFound        = errors.New("CASE_NOT_FOUND: Reconciliation case not found")
	ErrCaseResolved        = errors.New("CASE_RESOLVED: Reconciliation case is already resolved")
	ErrRevisionConflict    = errors.New("REVISION_CONFLICT: Expected revision does not match current case revision")
	ErrRunTerminal         = errors.New("RUN_TERMINAL: Run is already terminal")
	ErrBudgetExhausted     = errors.New("BUDGET_EXHAUSTED: No retry attempts remain")
	ErrInvalidAction       = errors.New("INVALID_ACTION: Unknown reconciliation action")
	ErrMissingEvidence     = errors.New("MISSING_EVIDENCE: Evidence reference is required")
	ErrMissingReason       = errors.New("MISSING_REASON: Decision reason is required")
	ErrReasonTooLong       = errors.New("INVALID_REASON: Decision reason exceeds 280 characters")
	ErrMissingResult       = errors.New("MISSING_RESULT: Result is required to confirm success")
	ErrResultTooLarge      = errors.New("RESULT_TOO_LARGE: Resolution result exceeds 256 KiB")
	ErrResultSchema        = errors.New("RESULT_SCHEMA_VIOLATION: Result does not conform to output schema")
	ErrWindowInsufficient  = errors.New("INSUFFICIENT_WINDOW: Idempotency window cannot cover another attempt")
	ErrMachineForbidden    = errors.New("MACHINE_FORBIDDEN: Machine keys cannot resolve reconciliation cases")
	ErrRunDeadlineExceeded = errors.New("RUN_DEADLINE_EXCEEDED: Run deadline has passed")
	ErrInvalidRecovery     = errors.New("INVALID_RECOVERY_POLICY: Task recovery policy is absent or invalid")
)

// maxDecisionReasonRunes bounds the human decision reason (OpenAPI
// maxLength 280). Canonical machine reason codes stay stable; free-form
// operator text never replaces them.
const maxDecisionReasonRunes = 280

// Reconciliation actions as named by the OpenAPI resolve contract.
const (
	ResolveActionSucceed = "confirm_succeeded"
	ResolveActionRetry   = "confirm_not_executed_retry"
	ResolveActionFail    = "fail_run"
)

// Case resolutions as stored in reconciliation_cases.resolution.
const (
	CaseResolutionSucceed = "SUCCEED"
	CaseResolutionRetry   = "RETRY"
	CaseResolutionFail    = "FAIL"
)

// ResolveReconciliationRequest mirrors ResolveReconciliationRequest in
// contracts/openapi/control-plane.yaml: evidence is the external reference
// string, reason is the bounded human decision reason, result is required
// only for confirm_succeeded.
type ResolveReconciliationRequest struct {
	Action           string `json:"action"`
	Evidence         string `json:"evidence"`
	Reason           string `json:"reason"`
	Result           any    `json:"result,omitempty"`
	ExpectedRevision int64  `json:"expectedRevision"`
}

// ResolveReconciliationResponse mirrors the 200 resolve response.
type ResolveReconciliationResponse struct {
	CaseID   string `json:"caseId"`
	Resolved bool   `json:"resolved"`
	Revision int64  `json:"revision"`
}

type resolveCaseRow struct {
	id            string
	environmentID string
	stepID        string
	attemptID     *string
	reason        string
	status        string
	revision      int64
}

// ResolveReconciliationCase applies one audited human decision to an OPEN
// reconciliation case. Lock order follows Blueprint §11.2: run → step →
// case → timer. The hold is released only when no OPEN case remains for the
// run; otherwise the run stays WAITING/RECONCILIATION.
func (e *WorkerEngine) ResolveReconciliationCase(
	ctx context.Context,
	orgID, caseID string,
	req ResolveReconciliationRequest,
	audit *tenant.AuditContext,
) (*ResolveReconciliationResponse, error) {
	if audit == nil {
		return nil, tenant.ErrAuditRequired
	}
	if audit.ActorType == tenant.IdentityTypeMachine {
		return nil, ErrMachineForbidden
	}
	if audit.ActorID == nil || strings.TrimSpace(*audit.ActorID) == "" {
		return nil, tenant.ErrAuditRequired
	}
	action := strings.TrimSpace(req.Action)
	if action != ResolveActionSucceed && action != ResolveActionRetry && action != ResolveActionFail {
		return nil, ErrInvalidAction
	}
	if strings.TrimSpace(req.Evidence) == "" {
		return nil, ErrMissingEvidence
	}
	if strings.TrimSpace(req.Reason) == "" {
		return nil, ErrMissingReason
	}
	if utf8.RuneCountInString(req.Reason) > maxDecisionReasonRunes {
		return nil, ErrReasonTooLong
	}
	if action == ResolveActionSucceed && req.Result == nil {
		return nil, ErrMissingResult
	}

	var resp *ResolveReconciliationResponse
	var resolvedRunID string
	mutate := func(ctx context.Context, tx storage.Tx) error {
		r, rid, err := e.resolveReconciliationCaseTx(ctx, tx, orgID, caseID, req, audit)
		if err != nil {
			return err
		}
		resp, resolvedRunID = r, rid
		return nil
	}
	// Route through the canonical tenant command path when available so the
	// recorded outcome replays for retried command identities instead of
	// re-entering the engine as a second decision.
	if e.commands != nil {
		replayed, err := e.commands.WithCommandTx(ctx, orgID, "", http.StatusOK, mutate,
			func() any { return resp },
			func(raw json.RawMessage) error { return json.Unmarshal(raw, &resp) })
		if err != nil {
			return nil, err
		}
		if !replayed && e.hub != nil {
			e.hub.Publish(resolvedRunID)
		}
		return resp, nil
	}
	if err := e.pool.WithTenantTx(ctx, orgID, mutate); err != nil {
		return nil, err
	}
	if e.hub != nil {
		e.hub.Publish(resolvedRunID)
	}
	return resp, nil
}

func (e *WorkerEngine) resolveReconciliationCaseTx(
	ctx context.Context,
	tx storage.Tx,
	orgID, caseID string,
	req ResolveReconciliationRequest,
	audit *tenant.AuditContext,
) (*ResolveReconciliationResponse, string, error) {

	// Candidate read without locks; ownership is taken in lock order below.
	var row resolveCaseRow
	var status string
	err := tx.QueryRow(ctx, `SELECT id::text, environment_id::text, step_id::text,
			attempt_id::text, reason, status, revision
		FROM reconciliation_cases
		WHERE id=$1::uuid AND organization_id=$2::uuid`,
		caseID, orgID).Scan(&row.id, &row.environmentID, &row.stepID, &row.attemptID, &row.reason, &status, &row.revision)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, "", ErrCaseNotFound
		}
		return nil, "", err
	}
	row.status = status

	// Lock run → step, then revalidate the case under its own lock.
	var runID, nodeID, runStatus, runReason string
	var runDeadline *time.Time
	if err := tx.QueryRow(ctx, `SELECT r.id::text, rs.node_id, r.status,
			COALESCE(r.reason_code,''), r.deadline_at
		FROM runs r JOIN run_steps rs ON rs.run_id=r.id AND rs.organization_id=r.organization_id
		JOIN reconciliation_cases rc ON rc.step_id=rs.id AND rc.organization_id=rs.organization_id
		WHERE rc.id=$1::uuid AND rc.organization_id=$2::uuid
		FOR UPDATE OF r, rs`, caseID, orgID).Scan(&runID, &nodeID, &runStatus, &runReason, &runDeadline); err != nil {
		return nil, "", err
	}
	var locked resolveCaseRow
	var lockedStatus string
	if err := tx.QueryRow(ctx, `SELECT id::text, environment_id::text, step_id::text,
			attempt_id::text, reason, status, revision FROM reconciliation_cases
		WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`,
		caseID, orgID).Scan(&locked.id, &locked.environmentID, &locked.stepID, &locked.attemptID, &locked.reason, &lockedStatus, &locked.revision); err != nil {
		return nil, "", err
	}
	locked.status = lockedStatus
	row = locked

	if row.status != "OPEN" {
		return nil, "", ErrCaseResolved
	}
	if row.revision != req.ExpectedRevision {
		return nil, "", ErrRevisionConflict
	}
	switch runStatus {
	case "SUCCEEDED", "FAILED", "CANCELLED":
		return nil, "", ErrRunTerminal
	}
	var dbNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return nil, "", err
	}
	if runDeadline != nil && !dbNow.Before(*runDeadline) {
		return nil, "", ErrRunDeadlineExceeded
	}

	var manifest deploymentManifest
	var workflowName string
	var manifestBytes []byte
	if err := tx.QueryRow(ctx, `SELECT d.manifest, r.workflow_name
		FROM runs r JOIN deployments d ON d.id=r.deployment_id AND d.organization_id=r.organization_id
		WHERE r.id=$1::uuid AND r.organization_id=$2::uuid`, runID, orgID).Scan(&manifestBytes, &workflowName); err != nil {
		return nil, "", err
	}
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return nil, "", fmt.Errorf("decode manifest: %w", err)
	}
	policy := manifest.taskRetryPolicy(workflowName, nodeID)
	if !IsValidRecoveryPolicy(policy.Recovery) {
		return nil, "", ErrInvalidRecovery
	}

	var resolution string
	action := strings.TrimSpace(req.Action)
	switch action {
	case ResolveActionSucceed:
		resolution = CaseResolutionSucceed
		if err := resolveCaseSucceedTx(ctx, tx, orgID, runID, row, nodeID, manifest, workflowName, req, audit); err != nil {
			return nil, "", err
		}
	case ResolveActionRetry:
		resolution = CaseResolutionRetry
		if err := resolveCaseRetryTx(ctx, tx, orgID, runID, row, nodeID, policy, req, audit, dbNow, runDeadline); err != nil {
			return nil, "", err
		}
	default:
		resolution = CaseResolutionFail
		if err := resolveCaseFailTx(ctx, tx, orgID, runID, row, nodeID, req, audit); err != nil {
			return nil, "", err
		}
	}

	var openCases int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM reconciliation_cases rc
		JOIN run_steps rs ON rs.id=rc.step_id AND rs.organization_id=rc.organization_id
		WHERE rs.run_id=$1::uuid AND rc.organization_id=$2::uuid AND rc.status='OPEN'`,
		runID, orgID).Scan(&openCases); err != nil {
		return nil, "", err
	}
	if openCases == 0 {
		// Last hold released: continue the workflow from the resolved
		// success, then leave the hold reason behind.
		if action == ResolveActionSucceed {
			if err := advanceAfterStepSuccessTx(ctx, tx, orgID, runID, req.Result); err != nil {
				return nil, "", err
			}
		}
		if err := releaseHoldTx(ctx, tx, orgID, runID, action); err != nil {
			return nil, "", err
		}
	}

	var newRevision int64
	if err := tx.QueryRow(ctx, `SELECT revision FROM reconciliation_cases
		WHERE id=$1::uuid AND organization_id=$2::uuid`, caseID, orgID).Scan(&newRevision); err != nil {
		return nil, "", err
	}
	_ = resolution
	return &ResolveReconciliationResponse{CaseID: caseID, Resolved: true, Revision: newRevision}, runID, nil
}

// resolveCaseSucceedTx records a human-confirmed success. The held attempt
// keeps its LOST/TIMED_OUT status (INV-09); the step becomes SUCCEEDED with
// completion_source=RECONCILIATION and the schema-valid result.
func resolveCaseSucceedTx(
	ctx context.Context, tx storage.Tx, orgID, runID string,
	row resolveCaseRow, nodeID string,
	manifest deploymentManifest, workflowName string,
	req ResolveReconciliationRequest, audit *tenant.AuditContext,
) error {
	_, outputSchema := manifest.taskSchemas(workflowName, nodeID)
	if outputSchema != nil && contracts.ValidatePayload(outputSchema, req.Result) != nil {
		return ErrResultSchema
	}
	canonicalResult, err := contracts.CanonicalizeGeneric(req.Result)
	if err != nil || len(canonicalResult) > worker.MaxInlinePayloadBytes {
		return ErrResultTooLarge
	}
	if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='SUCCEEDED', output=$1::jsonb,
		wait_reason=NULL, completion_source='RECONCILIATION', updated_at=clock_timestamp()
		WHERE id=$2::uuid AND organization_id=$3::uuid AND state='WAITING' AND wait_reason='RECONCILIATION'`,
		string(canonicalResult), row.stepID, orgID); err != nil {
		return err
	}
	if err := markCaseResolvedTx(ctx, tx, orgID, row, CaseResolutionSucceed, audit); err != nil {
		return err
	}
	if err := appendRunEvent(ctx, tx, orgID, runID, "STEP_SUCCEEDED", map[string]any{
		"stepId": row.stepID, "nodeId": nodeID, "resolution": CaseResolutionSucceed,
		"completionSource": "RECONCILIATION", "caseId": row.id,
	}); err != nil {
		return err
	}
	return appendCaseAuditTx(ctx, tx, orgID, row, req, audit, map[string]any{
		"resolution": CaseResolutionSucceed,
	})
}

// resolveCaseRetryTx records a human declaration that the effect did not
// occur and creates one retry intent within the remaining budget. The
// operation ID is unchanged. Exhausted budget, insufficient idempotency
// window, or insufficient run deadline refuse with 409 and leave the hold open.
func resolveCaseRetryTx(
	ctx context.Context, tx storage.Tx, orgID, runID string,
	row resolveCaseRow, nodeID string,
	policy RetryPolicy,
	req ResolveReconciliationRequest, audit *tenant.AuditContext,
	dbNow time.Time, runDeadline *time.Time,
) error {
	var nextAttemptNumber int
	var validUntil *time.Time
	var stepState, waitReason string
	if err := tx.QueryRow(ctx, `SELECT next_attempt_number, idempotency_valid_until,
		state, COALESCE(wait_reason,'') FROM run_steps
		WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`,
		row.stepID, orgID).Scan(&nextAttemptNumber, &validUntil, &stepState, &waitReason); err != nil {
		return err
	}
	if stepState != "WAITING" || waitReason != "RECONCILIATION" {
		return ErrCaseResolved
	}
	// No hidden retry past the budget (Blueprint §14.3).
	if !HasRetryBudget(nextAttemptNumber, policy.MaxAttempts) {
		return ErrBudgetExhausted
	}
	if strings.ToLower(strings.TrimSpace(policy.Recovery)) == "idempotent" {
		if policy.IdempotencyWindowMs == nil {
			return ErrWindowInsufficient
		}
		if validUntil == nil {
			derived, err := deriveIdempotencyDeadlineTx(ctx, tx, orgID, row.stepID, *policy.IdempotencyWindowMs)
			if err != nil {
				return err
			}
			if derived == nil {
				return ErrWindowInsufficient
			}
			validUntil = derived
		}
		if InsufficientIdempotencyWindow(dbNow, *validUntil, policy.TimeoutMs) {
			return ErrWindowInsufficient
		}
	}

	// One jittered backoff intent, chosen once and persisted. Human resolves
	// carry no Retry-After; the policy cap still applies.
	failedAttemptNumber := nextAttemptNumber - 1
	if failedAttemptNumber < 1 {
		failedAttemptNumber = 1
	}
	delayMs := ComputeRetryDelayMs(failedAttemptNumber, policy.InitialDelayMs, policy.MaxDelayMs, SampleJitter(), nil)
	dueAt := dbNow.Add(time.Duration(delayMs) * time.Millisecond)
	if InsufficientRunDeadline(dueAt, policy.TimeoutMs, runDeadline) {
		return ErrRunDeadlineExceeded
	}

	var environmentID string
	if err := tx.QueryRow(ctx, `SELECT environment_id::text FROM runs
		WHERE id=$1::uuid AND organization_id=$2::uuid`, runID, orgID).Scan(&environmentID); err != nil {
		return err
	}
	operationID := stableOperationID(environmentID, runID, nodeID)
	if _, err := tx.Exec(ctx, `INSERT INTO timers
		(organization_id, environment_id, kind, reference_id, run_id, step_id, attempt_number, operation_id, reason, due_at, state)
		VALUES ($1::uuid,$2::uuid,'RETRY_BACKOFF',$3::uuid,$4::uuid,$3::uuid,$5,$6,$7,$8,'PENDING')
		ON CONFLICT DO NOTHING`,
		orgID, environmentID, row.stepID, runID, nextAttemptNumber, operationID, "RECONCILIATION_RETRY", dueAt); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='WAITING', wait_reason='RETRY_BACKOFF',
		eligible_at=$1, updated_at=clock_timestamp()
		WHERE id=$2::uuid AND organization_id=$3::uuid AND state='WAITING' AND wait_reason='RECONCILIATION'`,
		dueAt, row.stepID, orgID); err != nil {
		return err
	}
	if err := markCaseResolvedTx(ctx, tx, orgID, row, CaseResolutionRetry, audit); err != nil {
		return err
	}
	if err := appendRunEvent(ctx, tx, orgID, runID, "STEP_WAITING", map[string]any{
		"stepId": row.stepID, "nodeId": nodeID, "reason": "RETRY_BACKOFF",
		"dueAt": dueAt.UTC().Format(time.RFC3339Nano), "attemptNumber": nextAttemptNumber,
		"operationId": operationID, "resolution": CaseResolutionRetry, "caseId": row.id,
	}); err != nil {
		return err
	}
	if err := appendRunEvent(ctx, tx, orgID, runID, "CASE_RESOLVED", map[string]any{
		"caseId": row.id, "stepId": row.stepID, "resolution": CaseResolutionRetry,
	}); err != nil {
		return err
	}
	return appendCaseAuditTx(ctx, tx, orgID, row, req, audit, map[string]any{
		"resolution": CaseResolutionRetry, "dueAt": dueAt.UTC().Format(time.RFC3339Nano),
	})
}

// resolveCaseFailTx records a human fail decision and terminalizes the run.
// Other OPEN cases for the run are mooted by the failure and closed with the
// same actor and reason.
func resolveCaseFailTx(
	ctx context.Context, tx storage.Tx, orgID, runID string,
	row resolveCaseRow, nodeID string,
	req ResolveReconciliationRequest, audit *tenant.AuditContext,
) error {
	var actorID *string
	if audit.ActorID != nil && strings.TrimSpace(*audit.ActorID) != "" {
		actorID = audit.ActorID
	}
	if err := markCaseResolvedTx(ctx, tx, orgID, row, CaseResolutionFail, audit); err != nil {
		return err
	}
	if err := closeOpenCasesAsFailTx(ctx, tx, orgID, runID, actorID, "RECONCILIATION_FAILED"); err != nil {
		return err
	}
	if err := failRunForStepTx(ctx, tx, orgID, runID, row.stepID, "RECONCILIATION_FAILED", actorID); err != nil {
		return err
	}
	if err := appendRunEvent(ctx, tx, orgID, runID, "CASE_RESOLVED", map[string]any{
		"caseId": row.id, "stepId": row.stepID, "resolution": CaseResolutionFail,
		"decisionReason": strings.TrimSpace(req.Reason),
	}); err != nil {
		return err
	}
	return appendCaseAuditTx(ctx, tx, orgID, row, req, audit, map[string]any{
		"resolution": CaseResolutionFail,
	})
}

// markCaseResolvedTx transitions one OPEN case to RESOLVED with the actor
// and a revision bump. The hold's original evidence column stays untouched:
// resolution evidence and the human decision reason belong to the resolution
// audit record, never to an overwrite of what the hold observed.
func markCaseResolvedTx(
	ctx context.Context, tx storage.Tx, orgID string,
	row resolveCaseRow, resolution string, audit *tenant.AuditContext,
) error {
	var actorID *string
	if audit.ActorID != nil && strings.TrimSpace(*audit.ActorID) != "" {
		actorID = audit.ActorID
	}
	tag, err := tx.Exec(ctx, `UPDATE reconciliation_cases SET status='RESOLVED', resolution=$1,
		actor_id=$2, revision=revision+1, resolved_at=clock_timestamp()
		WHERE id=$3::uuid AND organization_id=$4::uuid AND status='OPEN'`,
		resolution, actorID, row.id, orgID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrCaseResolved
	}
	return nil
}

// closeOpenCasesAsFailTx moots every remaining OPEN case of a terminalizing
// run. System terminalization passes a nil actor; human fail decisions pass
// the deciding actor through. History is emitted only when at least one OPEN
// case actually closes, identifying the affected cases.
func closeOpenCasesAsFailTx(ctx context.Context, tx storage.Tx, orgID, runID string, actorID *string, reason string) error {
	rows, err := tx.Query(ctx, `UPDATE reconciliation_cases rc SET status='RESOLVED', resolution='FAIL',
		actor_id=$1, revision=revision+1, resolved_at=clock_timestamp()
		FROM run_steps rs
		WHERE rc.step_id=rs.id AND rc.organization_id=rs.organization_id
			AND rs.run_id=$2::uuid AND rc.organization_id=$3::uuid AND rc.status='OPEN'
		RETURNING rc.id::text`, actorID, runID, orgID)
	if err != nil {
		return err
	}
	closed := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		closed = append(closed, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if len(closed) == 0 {
		return nil
	}
	return appendRunEvent(ctx, tx, orgID, runID, "CASE_RESOLVED", map[string]any{
		"resolution": CaseResolutionFail, "reason": reason, "scope": "run",
		"caseIds": closed,
	})
}

// releaseHoldTx leaves WAITING/RECONCILIATION once the last OPEN case of the
// run resolves. Retry intents already parked the step in RETRY_BACKOFF, so
// only a lingering hold reason is recomputed here.
func releaseHoldTx(ctx context.Context, tx storage.Tx, orgID, runID, action string) error {
	var status, reason string
	if err := tx.QueryRow(ctx, `SELECT status, COALESCE(reason_code,'') FROM runs
		WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, runID, orgID).Scan(&status, &reason); err != nil {
		return err
	}
	if status != "WAITING" || reason != "RECONCILIATION" {
		return nil
	}
	target := "RUNNING"
	var hasStarted bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM task_attempts ta
		JOIN run_steps rs ON rs.id=ta.step_id AND rs.organization_id=ta.organization_id
		WHERE rs.run_id=$1::uuid AND ta.organization_id=$2::uuid AND ta.started_at IS NOT NULL)`,
		runID, orgID).Scan(&hasStarted); err != nil {
		return err
	}
	if !hasStarted {
		target = "QUEUED"
	}
	if action == ResolveActionRetry {
		target = "WAITING"
		reason = "RETRY_BACKOFF"
		if _, err := tx.Exec(ctx, `UPDATE runs SET status='WAITING', reason_code='RETRY_BACKOFF',
			updated_at=clock_timestamp() WHERE id=$1::uuid AND organization_id=$2::uuid`, runID, orgID); err != nil {
			return err
		}
		return appendRunEvent(ctx, tx, orgID, runID, "RUN_RESUMED", map[string]any{
			"trigger": action,
		})
	}
	if _, err := tx.Exec(ctx, `UPDATE runs SET status=$1, reason_code=NULL, updated_at=clock_timestamp()
		WHERE id=$2::uuid AND organization_id=$3::uuid`, target, runID, orgID); err != nil {
		return err
	}
	return appendRunEvent(ctx, tx, orgID, runID, "RUN_RESUMED", map[string]any{
		"trigger": action,
	})
}

// failOverdueHeldRunsTx terminalizes runs whose deadline passed while held
// for reconciliation (or parked in retry backoff) with no active attempts.
// The run deadline stays active while held (Blueprint §14.3).
func failOverdueHeldRunsTx(ctx context.Context, tx storage.Tx, orgID string) (int, []string, error) {
	rows, err := tx.Query(ctx, `SELECT DISTINCT r.id::text
		FROM runs r
		WHERE r.organization_id=$1::uuid
			AND r.status IN ('QUEUED','RUNNING','WAITING')
			AND r.deadline_at IS NOT NULL AND r.deadline_at <= clock_timestamp()
			AND NOT EXISTS (SELECT 1 FROM task_attempts a
				JOIN run_steps rs ON rs.id=a.step_id AND rs.organization_id=a.organization_id
				WHERE rs.run_id=r.id AND a.organization_id=r.organization_id
					AND a.status IN ('CLAIMED','RUNNING'))
			AND EXISTS (SELECT 1 FROM reconciliation_cases rc
				JOIN run_steps rs ON rs.id=rc.step_id AND rs.organization_id=rc.organization_id
				WHERE rs.run_id=r.id AND rc.organization_id=r.organization_id AND rc.status='OPEN')
		LIMIT 50`, orgID)
	if err != nil {
		return 0, nil, fmt.Errorf("query overdue held runs: %w", err)
	}
	runIDs := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, nil, err
		}
		runIDs = append(runIDs, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, nil, err
	}
	rows.Close()
	affected := make([]string, 0, len(runIDs))
	for _, runID := range runIDs {
		// Lock order: run → step (Blueprint §11.2).
		var runStatus string
		if err := tx.QueryRow(ctx, `SELECT status FROM runs
			WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, runID, orgID).Scan(&runStatus); err != nil {
			continue
		}
		switch runStatus {
		case "SUCCEEDED", "FAILED", "CANCELLED":
			continue
		}
		var stepID string
		if err := tx.QueryRow(ctx, `SELECT rs.id::text FROM run_steps rs
			JOIN reconciliation_cases rc ON rc.step_id=rs.id AND rc.organization_id=rs.organization_id
			WHERE rs.run_id=$1::uuid AND rs.organization_id=$2::uuid AND rc.status='OPEN'
			ORDER BY rs.id LIMIT 1 FOR UPDATE OF rs`, runID, orgID).Scan(&stepID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			return 0, nil, err
		}
		if err := failRunForStepTx(ctx, tx, orgID, runID, stepID, "RUN_DEADLINE_EXCEEDED", nil); err != nil {
			return 0, nil, err
		}
		affected = append(affected, runID)
	}
	return len(affected), affected, nil
}

// appendCaseAuditTx records the human decision in the append-only audit trail
// with actor, action, target case, reason, and evidence reference.
func appendCaseAuditTx(
	ctx context.Context, tx storage.Tx, orgID string,
	row resolveCaseRow, req ResolveReconciliationRequest, audit *tenant.AuditContext, extra map[string]any,
) error {
	meta := map[string]any{
		"actor_type":     audit.ActorType,
		"role":           audit.Role,
		"capabilities":   audit.Capabilities,
		"action":         strings.TrimSpace(req.Action),
		"evidence":       strings.TrimSpace(req.Evidence),
		"decisionReason": strings.TrimSpace(req.Reason),
		"holdReason":     row.reason,
	}
	for k, v := range extra {
		meta[k] = v
	}
	encoded, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	var actorID *string
	if audit.ActorID != nil && strings.TrimSpace(*audit.ActorID) != "" {
		actorID = audit.ActorID
	}
	_, err = tx.Exec(ctx, `INSERT INTO audit_events
		(organization_id, actor_id, action, target_type, target_id, correlation_id, reason, metadata)
		VALUES ($1::uuid, $2, 'reconciliation.resolve', 'reconciliation_case', $3::uuid, $4, $5, $6::jsonb)`,
		orgID, actorID, row.id, audit.CorrelationID, strings.TrimSpace(req.Action), string(encoded))
	return err
}

// settleHoldAfterCompletionTx parks a run in WAITING/RECONCILIATION once its
// last sibling attempt settles while a reconciliation hold is open. It never
// touches terminal runs and emits the hold transition only when the run
// actually changes state.
func settleHoldAfterCompletionTx(ctx context.Context, tx storage.Tx, orgID, runID string) error {
	var status, reason string
	if err := tx.QueryRow(ctx, `SELECT status, COALESCE(reason_code,'') FROM runs
		WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, runID, orgID).Scan(&status, &reason); err != nil {
		return err
	}
	switch status {
	case "SUCCEEDED", "FAILED", "CANCELLED", "CANCELLING", "PAUSING", "PAUSED":
		return nil
	}
	if status == "WAITING" && reason == "RECONCILIATION" {
		return nil
	}
	var openCases int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM reconciliation_cases rc
		JOIN run_steps rs ON rs.id=rc.step_id AND rs.organization_id=rc.organization_id
		WHERE rs.run_id=$1::uuid AND rc.organization_id=$2::uuid AND rc.status='OPEN'`,
		runID, orgID).Scan(&openCases); err != nil {
		return err
	}
	if openCases == 0 {
		return nil
	}
	var activeAttempts int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_attempts a
		JOIN run_steps rs ON rs.id=a.step_id AND rs.organization_id=a.organization_id
		WHERE rs.run_id=$1::uuid AND a.organization_id=$2::uuid
			AND a.status IN ('CLAIMED','RUNNING')`, runID, orgID).Scan(&activeAttempts); err != nil {
		return err
	}
	if activeAttempts > 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `UPDATE runs SET status='WAITING', reason_code='RECONCILIATION',
		updated_at=clock_timestamp() WHERE id=$1::uuid AND organization_id=$2::uuid`, runID, orgID); err != nil {
		return err
	}
	return appendRunEvent(ctx, tx, orgID, runID, "RUN_WAITING", map[string]any{
		"reason": "RECONCILIATION",
	})
}
