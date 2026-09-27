package execution

// Approval control nodes (Blueprint §16.3, M4 / Issue #32).
//
// An approval is a control node owned by the control plane. It never holds a
// runner, a lease, or an attempt. When its dependencies are satisfied the
// engine persists one PENDING decision and parks the step at
// WAITING/APPROVAL; an identifiable human later decides it through the API.
//
// Three properties drive the whole file:
//
//  1. Waiting is durable state, not a live process. A run may be PAUSED, the
//     control plane restarted, and every worker dead, and the approval is
//     still decidable from PostgreSQL alone.
//  2. Approve and reject are both successful step outcomes. The node's job was
//     to collect a valid human decision; whether that decision is good for the
//     business is the next choice's problem, not this step's.
//  3. Expiry is enforced against database time inside the decision
//     transaction, before the decision is accepted. The sweeper is cleanup and
//     progression, never the thing that makes an expired decision invalid.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/jackc/pgx/v5"
)

var (
	// ErrApprovalNotFound is returned when the approval is absent in scope.
	ErrApprovalNotFound = errors.New("APPROVAL_NOT_FOUND: Approval not found")
	// ErrApprovalTerminal is returned for a decided or cancelled approval.
	ErrApprovalTerminal = errors.New("APPROVAL_TERMINAL: Approval is already resolved")
	// ErrApprovalConflict is returned when a committed decision contradicts
	// the requested one (Blueprint §16.3, INV-10).
	ErrApprovalConflict = errors.New("APPROVAL_CONFLICT: A different decision is already committed")
	// ErrApprovalExpired is returned when database time is already past the
	// approval deadline. The caller's transaction fails the run.
	ErrApprovalExpired = errors.New("APPROVAL_EXPIRED: Approval request expired")
	// ErrApprovalHumanOnly rejects machine keys and worker sessions
	// (Blueprint §16.3, §24.2).
	ErrApprovalHumanOnly = errors.New("APPROVAL_HUMAN_ONLY: Approval decisions require an identifiable human actor")
	// errApprovalExpiredSignal aborts the decision transaction after the
	// deadline was observed, so the expiry settlement is committed separately
	// instead of being rolled back with this error.
	errApprovalExpiredSignal = errors.New("APPROVAL_EXPIRED_SIGNAL: approval deadline already passed")
)

// approvalExpiryIDs carries the identity needed to settle an expired approval
// in its own committed transaction.
type approvalExpiryIDs struct {
	runID, stepID, nodeID, approvalID string
}

// approvalWaitMs resolves the approval wait for one node: the declared value
// when present, otherwise the 24h default. The result is still clamped against
// the remaining run lifetime by the caller, because the run deadline is the
// real upper bound (Blueprint §15.2).
func approvalWaitMs(cfg *approvalNodeConfig) int64 {
	if cfg != nil && cfg.ExpiresInMs > 0 {
		if cfg.ExpiresInMs > contracts.ApprovalMaxWaitMs {
			return contracts.ApprovalMaxWaitMs
		}
		return cfg.ExpiresInMs
	}
	return contracts.ApprovalDefaultWaitMs
}

// approvalRequiredPermission returns the permission persisted with the record.
// Anything other than the canonical approval decision capability is rejected at
// manifest validation, so this only normalizes the default.
func approvalRequiredPermission(cfg *approvalNodeConfig) string {
	if cfg != nil && cfg.RequiredPermission == contracts.ApprovalDecisionCapability {
		return contracts.ApprovalDecisionCapability
	}
	return contracts.ApprovalDecisionCapability
}

// openApprovalTx persists the single PENDING decision for an approval node and
// parks the step at WAITING/APPROVAL. It is idempotent: a re-evaluation of the
// same node finds the unique (step_id) row and leaves it untouched, so
// scheduler restarts and reconciler sweeps cannot mint a second request.
//
// The wait is stored relative to database time at first eligibility and is
// never recomputed, mirroring retry timers (Blueprint §15.1, §17).
func openApprovalTx(
	ctx context.Context,
	tx storage.Tx,
	organizationID, runID string,
	node workflowNode,
	st *dagEvalStep,
) (bool, error) {
	if node.Approval == nil {
		// Validator rejects a config-less approval at registration; fail closed
		// here too in case a manifest predates that rule.
		if err := failRunForStepTx(ctx, tx, organizationID, runID, st.id, "INVALID_MANIFEST", nil); err != nil {
			return false, err
		}
		return false, fmt.Errorf("approval node %s has no config", node.ID)
	}

	payload := node.Approval.Payload
	if payload == nil {
		payload = map[string]any{}
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return false, err
	}

	// Bound the declared wait by the run deadline that is already locked by the
	// caller. Approval can never outlive its run (Blueprint §15.2).
	var runDeadline *time.Time
	if err := tx.QueryRow(ctx, `SELECT deadline_at FROM runs
		WHERE id=$1::uuid AND organization_id=$2::uuid`, runID, organizationID).Scan(&runDeadline); err != nil {
		return false, err
	}

	waitMs := approvalWaitMs(node.Approval)
	permission := approvalRequiredPermission(node.Approval)

	var approvalID string
	err = tx.QueryRow(ctx, `
		INSERT INTO approvals (
			organization_id, environment_id, step_id, payload,
			required_permission, status, expires_at, revision
		)
		SELECT r.organization_id, r.environment_id, rs.id, $3::jsonb,
			$4, 'PENDING',
			LEAST(
				clock_timestamp() + ($5::bigint * INTERVAL '1 millisecond'),
				COALESCE($6::timestamptz, 'infinity'::timestamptz)
			),
			1
		FROM run_steps rs
		JOIN runs r ON r.id = rs.run_id AND r.organization_id = rs.organization_id
		WHERE rs.id = $1::uuid AND rs.organization_id = $2::uuid
		ON CONFLICT (step_id) DO NOTHING
		RETURNING id::text`,
		st.id, organizationID, string(payloadBytes), permission, waitMs, runDeadline,
	).Scan(&approvalID)
	if errors.Is(err, pgx.ErrNoRows) {
		// Already requested. The step is already WAITING; nothing to redo.
		return false, nil
	}
	if err != nil {
		return false, err
	}

	tag, err := tx.Exec(ctx, `UPDATE run_steps
		SET state='WAITING', wait_reason='APPROVAL', eligible_at=NULL, updated_at=clock_timestamp()
		WHERE id=$1::uuid AND organization_id=$2::uuid AND state='BLOCKED'`, st.id, organizationID)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		// Another transaction moved the step first. The approval row stays as
		// the durable record of the request; the step is no longer ours to park.
		return false, nil
	}
	st.state = "WAITING"

	if err := appendRunEvent(ctx, tx, organizationID, runID, "APPROVAL_REQUESTED", map[string]any{
		"stepId":             st.id,
		"nodeId":             node.ID,
		"approval":           approvalID,
		"requiredPermission": permission,
		"expiresInMs":        waitMs,
	}); err != nil {
		return false, err
	}
	if err := settleRunWaitingForApprovalTx(ctx, tx, organizationID, runID); err != nil {
		return false, err
	}
	return true, nil
}

// settleRunWaitingForApprovalTx recomputes the run status after an approval
// request. A run whose only remaining work is durable waiting is WAITING, not
// RUNNING: no process is alive and none should be implied (Blueprint §10.2).
func settleRunWaitingForApprovalTx(ctx context.Context, tx storage.Tx, organizationID, runID string) error {
	return recomputeRunStatusAfterControlTx(ctx, tx, organizationID, runID)
}

// recomputeRunStatusAfterControlTx applies the §10.2 status priority for a run
// whose state changed through a control action rather than a worker attempt.
//
// The ordering matters and is not cosmetic. Rule 6 ("active attempt or ready
// work means RUNNING") outranks rule 7 ("otherwise WAITING with the durable
// wait reason"). Getting this backwards strands a run in WAITING while a
// READY task step exists, and the claim scan only admits RUNNING/QUEUED runs,
// so the run would never progress again.
func recomputeRunStatusAfterControlTx(ctx context.Context, tx storage.Tx, organizationID, runID string) error {
	var status, reason string
	if err := tx.QueryRow(ctx, `SELECT status, COALESCE(reason_code,'') FROM runs
		WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, runID, organizationID).
		Scan(&status, &reason); err != nil {
		return err
	}
	switch status {
	case "SUCCEEDED", "FAILED", "CANCELLED", "CANCELLING":
		// Terminal and cancelling states are settled by their own paths and are
		// never reopened by a recompute (§9 INV-09).
		return nil
	}

	var nonterminal, liveAttempts, readyTasks int
	if err := tx.QueryRow(ctx, `SELECT
			count(*) FILTER (WHERE state NOT IN ('SUCCEEDED','FAILED','CANCELLED','SKIPPED')),
			count(*) FILTER (WHERE state='RUNNING')
		FROM run_steps WHERE run_id=$1::uuid AND organization_id=$2::uuid`,
		runID, organizationID).Scan(&nonterminal, &liveAttempts); err != nil {
		return err
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_attempts a
		JOIN run_steps rs ON rs.id=a.step_id AND rs.organization_id=a.organization_id
		WHERE rs.run_id=$1::uuid AND a.organization_id=$2::uuid
		  AND a.status IN ('CLAIMED','RUNNING')`, runID, organizationID).Scan(&liveAttempts); err != nil {
		return err
	}
	// Only task steps are claimable work. An approval or another control node
	// that is WAITING is not work a worker can pick up.
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM run_steps
		WHERE run_id=$1::uuid AND organization_id=$2::uuid AND state='READY' AND kind='task'`,
		runID, organizationID).Scan(&readyTasks); err != nil {
		return err
	}
	if nonterminal == 0 {
		// Every step reached a terminal state; the normal terminalizer owns the
		// terminal transition and run output validation.
		return nil
	}

	target, nextReason := "WAITING", durableWaitReasonForRunTx(ctx, tx, organizationID, runID)
	if liveAttempts > 0 || readyTasks > 0 {
		target, nextReason = "RUNNING", ""
	}
	if nextReason == "" {
		nextReason = "APPROVAL"
	}
	if target == status && reason == nextReason {
		return nil
	}
	_, err := tx.Exec(ctx, `UPDATE runs
		SET status=$1, reason_code=$2, updated_at=clock_timestamp()
		WHERE id=$3::uuid AND organization_id=$4::uuid`, target, nextReason, runID, organizationID)
	return err
}

// durableWaitReasonForRunTx names the reason a run is genuinely waiting, so the
// status is never a bare WAITING with no explanation (Blueprint §10.1).
func durableWaitReasonForRunTx(ctx context.Context, tx storage.Tx, organizationID, runID string) string {
	var pendingApproval int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM approvals a
		JOIN run_steps rs ON rs.id=a.step_id AND rs.organization_id=a.organization_id
		WHERE rs.run_id=$1::uuid AND a.organization_id=$2::uuid AND a.status='PENDING'`,
		runID, organizationID).Scan(&pendingApproval); err == nil && pendingApproval > 0 {
		return "APPROVAL"
	}
	var waitingStep *string
	if err := tx.QueryRow(ctx, `SELECT wait_reason FROM run_steps
		WHERE run_id=$1::uuid AND organization_id=$2::uuid AND state='WAITING'
		ORDER BY id LIMIT 1`, runID, organizationID).Scan(&waitingStep); err == nil && waitingStep != nil {
		return *waitingStep
	}
	return ""
}

// DecideApprovalRequest is the API-shaped decision command.
type DecideApprovalRequest struct {
	Decision         string `json:"decision"`
	ExpectedRevision int64  `json:"expectedRevision"`
	Comment          string `json:"comment"`
}

// ApprovalDTO is the redacted approval representation returned by the API.
type ApprovalDTO struct {
	ID                 string     `json:"id"`
	RunID              string     `json:"runId"`
	StepID             string     `json:"stepId"`
	NodeID             string     `json:"nodeId"`
	WorkflowName       string     `json:"workflowName"`
	Status             string     `json:"status"`
	RequiredPermission string     `json:"requiredPermission"`
	Payload            any        `json:"payload"`
	Revision           int64      `json:"revision"`
	ExpiresAt          *time.Time `json:"expiresAt"`
	DecidedAt          *time.Time `json:"decidedAt"`
	DecisionComment    *string    `json:"decisionComment"`
	DecisionReason     *string    `json:"decisionReason"`
	ActorID            *string    `json:"actorId"`
	CreatedAt          time.Time  `json:"createdAt"`
}

// DecideApproval commits exactly one human decision for an approval.
//
// Lock order follows Blueprint §11.2: the run row is locked first, then the
// step, then the approval row. An approval decision never holds the approval
// lock and then reaches for the run, because that inverts the documented order.
func (e *WorkerEngine) DecideApproval(
	ctx context.Context,
	orgID, approvalID string,
	req DecideApprovalRequest,
	audit *tenant.AuditContext,
) (*ApprovalDTO, error) {
	if audit == nil {
		return nil, tenant.ErrAuditRequired
	}
	// Blueprint §16.3 / §24.2: a decision requires an identifiable human.
	// Machine keys and worker sessions are refused before any state is read,
	// so no approval row is ever locked for a forbidden actor.
	if audit.ActorType != tenant.IdentityTypeHuman ||
		audit.ActorID == nil || strings.TrimSpace(*audit.ActorID) == "" {
		return nil, ErrApprovalHumanOnly
	}
	decision := req.Decision
	if decision != contracts.ApprovalDecisionApproved && decision != contracts.ApprovalDecisionRejected {
		return nil, errors.New("INVALID_DECISION: decision must be approved or rejected")
	}

	var resp *ApprovalDTO
	var expired approvalExpiryIDs
	mutate := func(ctx context.Context, tx storage.Tx) error {
		d, ids, err := e.decideApprovalTx(ctx, tx, orgID, approvalID, req, decision, audit)
		// The expiry sentinel carries the identity needed to settle the record.
		// It must be recognized before the generic error path, otherwise the
		// settlement would be dropped along with the aborted transaction.
		if errors.Is(err, errApprovalExpiredSignal) && ids != nil {
			expired = *ids
			return errApprovalExpiredSignal
		}
		if err != nil {
			return err
		}
		resp = d
		return nil
	}
	var execErr error
	if e.commands != nil {
		_, execErr = e.commands.WithCommandTx(ctx, orgID, "", http.StatusOK, mutate,
			func() any { return resp },
			func(raw json.RawMessage) error { return json.Unmarshal(raw, &resp) })
	} else {
		execErr = e.pool.WithTenantTx(ctx, orgID, mutate)
	}
	if errors.Is(execErr, errApprovalExpiredSignal) {
		// Commit the expiry in a fresh transaction, then refuse the decision.
		if _, settleErr := e.settleExpiredApproval(ctx, orgID, expired.runID, expired.stepID, expired.nodeID, expired.approvalID); settleErr != nil {
			return nil, settleErr
		}
		return nil, ErrApprovalExpired
	}
	if execErr != nil {
		return nil, execErr
	}
	return resp, nil
}

// settleExpiredApproval commits the APPROVAL_EXPIRED terminal path for a request
// whose deadline passed while it was pending, then reports that the run was
// failed. Running it in its own transaction is what keeps the settlement from
// being rolled back by the refusal of the decision that observed it.
func (e *WorkerEngine) settleExpiredApproval(
	ctx context.Context,
	orgID, runID, stepID, nodeID, approvalID string,
) (bool, error) {
	settled := false
	err := e.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		// Preserve the documented lock order: run, then step, then approval.
		if _, err := tx.Exec(ctx, `SELECT id FROM runs
			WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, runID, orgID); err != nil {
			return err
		}
		if err := expireApprovalTx(ctx, tx, orgID, runID, stepID, nodeID, approvalID); err != nil {
			return err
		}
		settled = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return settled, nil
}

func (e *WorkerEngine) decideApprovalTx(
	ctx context.Context,
	tx storage.Tx,
	orgID, approvalID string,
	req DecideApprovalRequest,
	decision string,
	audit *tenant.AuditContext,
) (*ApprovalDTO, *approvalExpiryIDs, error) {
	// Resolve the approval to its run/step before locking, so the lock order
	// can be run -> step -> approval (§11.2).
	var runID, stepID string
	if err := tx.QueryRow(ctx, `SELECT rs.run_id::text, rs.id::text
		FROM approvals a JOIN run_steps rs ON rs.id = a.step_id AND rs.organization_id = a.organization_id
		WHERE a.id=$1::uuid AND a.organization_id=$2::uuid`,
		approvalID, orgID).Scan(&runID, &stepID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, ErrApprovalNotFound
		}
		return nil, nil, err
	}

	var runStatus, runReason string
	var runRevision int64
	if err := tx.QueryRow(ctx, `SELECT status, COALESCE(reason_code,''), revision FROM runs
		WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, runID, orgID).
		Scan(&runStatus, &runReason, &runRevision); err != nil {
		return nil, nil, err
	}
	switch runStatus {
	case "SUCCEEDED", "FAILED", "CANCELLED":
		return nil, nil, ErrRunTerminal
	case "CANCELLING":
		return nil, nil, ErrRunCancelling
	}

	var stepNodeID, stepState, stepKind string
	if err := tx.QueryRow(ctx, `SELECT node_id, state, kind FROM run_steps
		WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, stepID, orgID).
		Scan(&stepNodeID, &stepState, &stepKind); err != nil {
		return nil, nil, err
	}

	var status, permission string
	var revision int64
	var payloadBytes []byte
	var expiresAt *time.Time
	var decidedAt *time.Time
	var comment, reasonText *string
	var actorID *string
	if err := tx.QueryRow(ctx, `SELECT status, revision, payload, required_permission,
		expires_at, decided_at, decision_comment, decision_reason, actor_id
		FROM approvals WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`,
		approvalID, orgID).
		Scan(&status, &revision, &payloadBytes, &permission, &expiresAt, &decidedAt, &comment, &reasonText, &actorID); err != nil {
		return nil, nil, err
	}

	// Expiry is evaluated against database time inside this transaction and
	// before the decision is honoured, so a delayed sweeper can never admit a
	// decision on an expired request (Blueprint §16.3).
	expired, err := approvalExpiredNowTx(ctx, tx, expiresAt)
	if err != nil {
		return nil, nil, err
	}
	if expired && status == "PENDING" {
		return nil, &approvalExpiryIDs{
			runID: runID, stepID: stepID, nodeID: stepNodeID, approvalID: approvalID,
		}, errApprovalExpiredSignal
	}

	if status != "PENDING" {
		// A decided approval is terminal: a cancelled one is closed, and a
		// contradicting decision is a conflict (Blueprint §16.3, INV-10).
		if status == "CANCELLED" || status == "EXPIRED" {
			return nil, nil, ErrApprovalTerminal
		}
		committedApproved := status == "APPROVED"
		requestedApproved := decision == contracts.ApprovalDecisionApproved
		if committedApproved != requestedApproved {
			return nil, nil, ErrApprovalConflict
		}
		// Repeating the same decision is idempotent, and deliberately ignores a
		// stale expectedRevision: a double-clicking browser resends the
		// revision it first read, and the intent it expressed is already
		// satisfied. Revision guards new decisions, not replays of a committed
		// one.
		returnApproval, err := queryApprovalDTO(ctx, tx, orgID, approvalID)
		return returnApproval, nil, err
	}

	if req.ExpectedRevision != revision {
		return nil, nil, ErrRevisionConflict
	}

	// The step may already be settled by a cancel or a terminal run path; a
	// decision must never reopen it (§9 INV-09).
	if stepState != "WAITING" {
		return nil, nil, ErrApprovalTerminal
	}

	now, err := dbNowTx(ctx, tx)
	if err != nil {
		return nil, nil, err
	}
	decidedStatus := "APPROVED"
	if decision == contracts.ApprovalDecisionRejected {
		decidedStatus = "REJECTED"
	}

	newComment := req.Comment
	tag, err := tx.Exec(ctx, `UPDATE approvals
		SET status=$1, decided_at=$2, decision_comment=$3, actor_id=$4::uuid,
		    revision=revision+1
		WHERE id=$5::uuid AND organization_id=$6::uuid AND status='PENDING'`,
		decidedStatus, now, newComment, *audit.ActorID, approvalID, orgID)
	if err != nil {
		return nil, nil, err
	}
	if tag.RowsAffected() == 0 {
		// A concurrent transaction decided first; the row lock makes the
		// single-winner outcome explicit rather than last-writer-wins.
		return nil, nil, ErrApprovalConflict
	}

	// Both approve and reject are successful step outcomes (Blueprint §16.3).
	// Reject is a business result, not a technical error, so the run keeps
	// going and the following choice decides what it means.
	decisionOut := map[string]any{
		"decision":  decision,
		"actorId":   *audit.ActorID,
		"decidedAt": now.UTC().Format(time.RFC3339Nano),
	}
	if req.Comment != "" {
		decisionOut["comment"] = req.Comment
	}
	if err := validateApprovalOutputTx(ctx, tx, orgID, runID, stepID, stepNodeID, decisionOut); err != nil {
		return nil, nil, err
	}
	outBytes, err := json.Marshal(decisionOut)
	if err != nil {
		return nil, nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE run_steps
		SET state='SUCCEEDED', wait_reason=NULL, output=$1::jsonb, updated_at=clock_timestamp()
		WHERE id=$2::uuid AND organization_id=$3::uuid AND state='WAITING'`,
		string(outBytes), stepID, orgID); err != nil {
		return nil, nil, err
	}

	if err := appendRunEvent(ctx, tx, orgID, runID, "APPROVAL_DECIDED", map[string]any{
		"stepId":   stepID,
		"nodeId":   stepNodeID,
		"approval": approvalID,
		"decision": decision,
		"actorId":  *audit.ActorID,
	}); err != nil {
		return nil, nil, err
	}
	if err := appendRunEvent(ctx, tx, orgID, runID, "STEP_SUCCEEDED", map[string]any{
		"stepId": stepID, "nodeId": stepNodeID, "output": decisionOut,
	}); err != nil {
		return nil, nil, err
	}
	if err := appendApprovalAuditTx(ctx, tx, orgID, runID, approvalID, stepID, stepNodeID,
		decidedStatus, newComment, audit); err != nil {
		return nil, nil, err
	}

	// The decision is durable, so downstream nodes may be scheduled in the
	// same transaction (INV-06).
	if err := advanceAfterStepSuccessTx(ctx, tx, orgID, runID, decisionOut); err != nil {
		return nil, nil, err
	}
	// The approval is no longer a durable wait, so the run must leave WAITING
	// whenever claimable work now exists (Blueprint §10.2 rules 6 and 7).
	if err := recomputeRunStatusAfterControlTx(ctx, tx, orgID, runID); err != nil {
		return nil, nil, err
	}
	decided, err := queryApprovalDTO(ctx, tx, orgID, approvalID)
	return decided, nil, err
}

// validateApprovalOutputTx enforces the node's declared decision schema. A
// malformed decision output is a manifest bug, so it fails the run rather than
// committing an output no downstream schema can trust.
func validateApprovalOutputTx(ctx context.Context, tx storage.Tx, orgID, runID, stepID, nodeID string, out map[string]any) error {
	var manifestBytes []byte
	if err := tx.QueryRow(ctx, `SELECT d.manifest FROM runs r
		JOIN deployments d ON d.id=r.deployment_id AND d.organization_id=r.organization_id
		WHERE r.id=$1::uuid AND r.organization_id=$2::uuid`, runID, orgID).Scan(&manifestBytes); err != nil {
		return err
	}
	var manifest deploymentManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return err
	}
	for _, wf := range manifest.Workflows {
		for _, n := range wf.Nodes {
			if n.ID != nodeID || n.Approval == nil || n.Approval.OutputSchema == nil {
				continue
			}
			if err := contracts.ValidatePayload(n.Approval.OutputSchema, out); err != nil {
				return failRunForStepTx(ctx, tx, orgID, runID, stepID, "SCHEMA_VALIDATION_ERROR", nil)
			}
		}
	}
	return nil
}

func appendApprovalAuditTx(
	ctx context.Context,
	tx storage.Tx,
	orgID, runID, approvalID, stepID, nodeID, decision, comment string,
	audit *tenant.AuditContext,
) error {
	meta, err := json.Marshal(map[string]any{
		"actor_type":   audit.ActorType,
		"role":         audit.Role,
		"capabilities": audit.Capabilities,
		"runId":        runID,
		"stepId":       stepID,
		"nodeId":       nodeID,
		"decision":     decision,
		"comment":      comment,
	})
	if err != nil {
		return err
	}
	var actorID *string
	if audit.ActorID != nil && strings.TrimSpace(*audit.ActorID) != "" {
		actorID = audit.ActorID
	}
	_, err = tx.Exec(ctx, `INSERT INTO audit_events
		(organization_id, actor_id, action, target_type, target_id, correlation_id, reason, metadata)
		VALUES ($1::uuid, $2, 'approval.decide', 'approval', $3::uuid, $4, $5, $6::jsonb)`,
		orgID, actorID, approvalID, audit.CorrelationID, decision, string(meta))
	return err
}

func queryApprovalDTO(ctx context.Context, tx storage.Tx, orgID, approvalID string) (*ApprovalDTO, error) {
	var d ApprovalDTO
	var payloadBytes []byte
	if err := tx.QueryRow(ctx, `SELECT a.id::text, rs.run_id::text, rs.id::text, rs.node_id,
		r.workflow_name, a.status, a.required_permission, a.payload, a.revision,
		a.expires_at, a.decided_at, a.decision_comment, a.decision_reason, a.actor_id::text, a.created_at
		FROM approvals a
		JOIN run_steps rs ON rs.id=a.step_id AND rs.organization_id=a.organization_id
		JOIN runs r ON r.id=rs.run_id AND r.organization_id=rs.organization_id
		WHERE a.id=$1::uuid AND a.organization_id=$2::uuid`, approvalID, orgID).
		Scan(&d.ID, &d.RunID, &d.StepID, &d.NodeID, &d.WorkflowName, &d.Status,
			&d.RequiredPermission, &payloadBytes, &d.Revision, &d.ExpiresAt, &d.DecidedAt,
			&d.DecisionComment, &d.DecisionReason, &d.ActorID, &d.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrApprovalNotFound
		}
		return nil, err
	}
	if len(payloadBytes) > 0 {
		var parsed any
		if err := json.Unmarshal(payloadBytes, &parsed); err == nil {
			d.Payload = parsed
		}
	}
	return &d, nil
}

// GetApproval returns one approval scoped to the caller's organization.
func (e *WorkerEngine) GetApproval(ctx context.Context, orgID, approvalID string) (*ApprovalDTO, error) {
	var d *ApprovalDTO
	err := e.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		var err error
		d, err = queryApprovalDTO(ctx, tx, orgID, approvalID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return d, nil
}

// ListApprovals returns approvals for one environment, newest first. The
// environment scope comes from verified authority, never from caller input
// alone (Blueprint INV-01).
func (e *WorkerEngine) ListApprovals(
	ctx context.Context,
	orgID, environmentID, status string,
	limit int,
) ([]ApprovalDTO, error) {
	if limit <= 0 || limit > 100 {
		limit = 25
	}
	out := []ApprovalDTO{}
	err := e.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		rows, err := tx.Query(ctx, `SELECT a.id::text FROM approvals a
			WHERE a.organization_id=$1::uuid AND a.environment_id=$2::uuid
			  AND ($3 = '' OR a.status = $3)
			ORDER BY a.created_at DESC
			LIMIT $4`, orgID, environmentID, status, limit)
		if err != nil {
			return err
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		for _, id := range ids {
			d, err := queryApprovalDTO(ctx, tx, orgID, id)
			if err != nil {
				return err
			}
			out = append(out, *d)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// approvalExpiredNowTx compares database time against the approval deadline.
// Ownership of time belongs to the database, never to a worker, a browser, or
// a Go process clock (Blueprint §13.1).
func approvalExpiredNowTx(ctx context.Context, tx storage.Tx, expiresAt *time.Time) (bool, error) {
	if expiresAt == nil {
		return false, nil
	}
	var expired bool
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp() >= $1::timestamptz`, *expiresAt).Scan(&expired); err != nil {
		return false, err
	}
	return expired, nil
}

func dbNowTx(ctx context.Context, tx storage.Tx) (*time.Time, error) {
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return nil, err
	}
	return &now, nil
}

// expireApprovalTx commits the APPROVAL_EXPIRED terminal path: the approval
// becomes EXPIRED and the run fails. Waiting for a person has no technical
// failure, so an unanswered request is reported as an explicit reason rather
// than silently retried (Blueprint §16.3).
func expireApprovalTx(
	ctx context.Context,
	tx storage.Tx,
	orgID, runID, stepID, nodeID, approvalID string,
) error {
	tag, err := tx.Exec(ctx, `UPDATE approvals
		SET status='EXPIRED', revision=revision+1
		WHERE id=$1::uuid AND organization_id=$2::uuid AND status='PENDING'`,
		approvalID, orgID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	if err := appendRunEvent(ctx, tx, orgID, runID, "APPROVAL_EXPIRED", map[string]any{
		"stepId": stepID, "nodeId": nodeID, "approval": approvalID,
	}); err != nil {
		return err
	}
	return failRunForStepTx(ctx, tx, orgID, runID, stepID, "APPROVAL_EXPIRED", nil)
}

// SweepExpiredApprovals settles approvals whose deadline has passed. The sweep
// is cleanup and progression only: an expired approval is already invalid for
// decisions because DecideApproval compares database time itself.
func (e *WorkerEngine) SweepExpiredApprovals(ctx context.Context, organizationID string) (int, error) {
	swept := 0
	err := e.pool.WithTenantTx(ctx, organizationID, func(ctx context.Context, tx storage.Tx) error {
		rows, err := tx.Query(ctx, `SELECT a.id::text, a.step_id::text, rs.run_id::text, rs.node_id
			FROM approvals a
			JOIN run_steps rs ON rs.id=a.step_id AND rs.organization_id=a.organization_id
			WHERE a.organization_id=$1::uuid AND a.status='PENDING'
			  AND a.expires_at IS NOT NULL
			  AND a.expires_at <= clock_timestamp()
			ORDER BY a.expires_at
			LIMIT 50`, organizationID)
		if err != nil {
			return err
		}
		type dueApproval struct {
			id, stepID, runID, nodeID string
		}
		var due []dueApproval
		for rows.Next() {
			var d dueApproval
			if err := rows.Scan(&d.id, &d.stepID, &d.runID, &d.nodeID); err != nil {
				rows.Close()
				return err
			}
			due = append(due, d)
		}
		rows.Close()
		for _, d := range due {
			// Lock the run first to preserve the documented order (§11.2).
			if _, err := tx.Exec(ctx, `SELECT id FROM runs
				WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, d.runID, organizationID); err != nil {
				return err
			}
			if err := expireApprovalTx(ctx, tx, organizationID, d.runID, d.stepID, d.nodeID, d.id); err != nil {
				return err
			}
			swept++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return swept, nil
}
