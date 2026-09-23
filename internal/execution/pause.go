package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/jackc/pgx/v5"
)

// Pause/resume API and execution control semantics (Blueprint §10.2, §15.3, §20.1).
//
// Pause atomically blocks new task claims while preserving in-flight work.
// An in-flight claim (CLAIMED or RUNNING) may Start and finish normally.
// The run transitions to PAUSING while any active attempt remains, draining
// to PAUSED once all active attempts finish (unless completion or unrecoverable
// failure terminalizes the run first per Blueprint §10.2 state priority).
//
// Retry backoff timers, reconciliation holds, and human approvals may record
// while paused without launching new nodes or resetting due_at. The overall run
// deadline never freezes while paused.
//
// Resume clears the durable pause flag and recomputes the canonical run state:
// QUEUED if the run never started, RUNNING if ready work or live attempts exist,
// WAITING with the stored durable reason (RETRY_BACKOFF, RECONCILIATION, etc.),
// or terminal if all workflow nodes have completed.
//
// Cancel remains authoritative: a pause never blocks durable cancellation.

var (
	ErrRunNotPaused  = errors.New("RUN_NOT_PAUSED: Run is not paused or pausing")
	ErrRunCancelling = errors.New("RUN_CANCELLING: Run cancellation is in progress")
)

// PauseRunRequest mirrors RevisionCommand in
// contracts/openapi/control-plane.yaml.
type PauseRunRequest struct {
	ExpectedRevision int64 `json:"expectedRevision"`
}

// ResumeRunRequest mirrors RevisionCommand in
// contracts/openapi/control-plane.yaml.
type ResumeRunRequest struct {
	ExpectedRevision int64 `json:"expectedRevision"`
}

// PauseRun requests durable pause on an active run with revision check
// and idempotency support.
func (e *WorkerEngine) PauseRun(
	ctx context.Context,
	orgID, runID string,
	req PauseRunRequest,
	audit *tenant.AuditContext,
) (*RunDTO, error) {
	if audit == nil {
		return nil, tenant.ErrAuditRequired
	}
	var resp *RunDTO
	mutate := func(ctx context.Context, tx storage.Tx) error {
		r, err := e.pauseRunTx(ctx, tx, orgID, runID, req.ExpectedRevision, audit)
		if err != nil {
			return err
		}
		resp = r
		return nil
	}
	if e.commands != nil {
		_, err := e.commands.WithCommandTx(ctx, orgID, "", http.StatusOK, mutate,
			func() any { return resp },
			func(raw json.RawMessage) error { return json.Unmarshal(raw, &resp) })
		if err != nil {
			return nil, err
		}
		return resp, nil
	}
	if err := e.pool.WithTenantTx(ctx, orgID, mutate); err != nil {
		return nil, err
	}
	return resp, nil
}

func (e *WorkerEngine) pauseRunTx(
	ctx context.Context, tx storage.Tx,
	orgID, runID string, expectedRevision int64,
	audit *tenant.AuditContext,
) (*RunDTO, error) {
	var status, reason string
	var revision int64
	var environmentID string
	var pauseRequested bool
	if err := tx.QueryRow(ctx, `SELECT status, COALESCE(reason_code,''), revision, environment_id::text, pause_requested
		FROM runs
		WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`,
		runID, orgID).Scan(&status, &reason, &revision, &environmentID, &pauseRequested); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrRunNotFound
		}
		return nil, err
	}
	switch status {
	case "SUCCEEDED", "FAILED", "CANCELLED":
		return nil, ErrRunTerminal
	case "CANCELLING":
		return nil, ErrRunCancelling
	}
	if revision != expectedRevision {
		return nil, ErrRevisionConflict
	}
	if pauseRequested && (status == "PAUSING" || status == "PAUSED") {
		// Duplicate pause request with matching revision: idempotent.
		return queryRunDTO(ctx, tx, orgID, runID)
	}

	var liveAttempts int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_attempts a
		JOIN run_steps rs ON rs.id=a.step_id AND rs.organization_id=a.organization_id
		WHERE rs.run_id=$1::uuid AND a.organization_id=$2::uuid
			AND a.status IN ('CLAIMED','RUNNING')`, runID, orgID).Scan(&liveAttempts); err != nil {
		return nil, err
	}

	targetStatus := "PAUSED"
	eventType := "RUN_PAUSED"
	payload := map[string]any{"reason": "PAUSE_REQUESTED"}
	if liveAttempts > 0 {
		targetStatus = "PAUSING"
		eventType = "RUN_PAUSING"
		payload["activeAttempts"] = liveAttempts
	}

	if _, err := tx.Exec(ctx, `UPDATE runs SET status=$1, reason_code='PAUSE_REQUESTED',
		pause_requested=true, revision=revision+1, updated_at=clock_timestamp()
		WHERE id=$2::uuid AND organization_id=$3::uuid`, targetStatus, runID, orgID); err != nil {
		return nil, err
	}

	if err := appendRunEvent(ctx, tx, orgID, runID, eventType, payload); err != nil {
		return nil, err
	}
	if err := appendPauseAuditTx(ctx, tx, orgID, runID, audit, targetStatus, liveAttempts); err != nil {
		return nil, err
	}

	return queryRunDTO(ctx, tx, orgID, runID)
}

func appendPauseAuditTx(ctx context.Context, tx storage.Tx, orgID, runID string, audit *tenant.AuditContext, targetStatus string, activeAttempts int) error {
	meta, err := json.Marshal(map[string]any{
		"actor_type":     audit.ActorType,
		"role":           audit.Role,
		"capabilities":   audit.Capabilities,
		"targetStatus":   targetStatus,
		"activeAttempts": activeAttempts,
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
		VALUES ($1::uuid, $2, 'run.pause', 'run', $3::uuid, $4, 'PAUSE_REQUESTED', $5::jsonb)`,
		orgID, actorID, runID, audit.CorrelationID, string(meta))
	return err
}

// ResumeRun resumes a paused or pausing run with revision check and state
// recomputation per Blueprint §10.2.
func (e *WorkerEngine) ResumeRun(
	ctx context.Context,
	orgID, runID string,
	req ResumeRunRequest,
	audit *tenant.AuditContext,
) (*RunDTO, error) {
	if audit == nil {
		return nil, tenant.ErrAuditRequired
	}
	var resp *RunDTO
	mutate := func(ctx context.Context, tx storage.Tx) error {
		r, err := e.resumeRunTx(ctx, tx, orgID, runID, req.ExpectedRevision, audit)
		if err != nil {
			return err
		}
		resp = r
		return nil
	}
	if e.commands != nil {
		_, err := e.commands.WithCommandTx(ctx, orgID, "", http.StatusOK, mutate,
			func() any { return resp },
			func(raw json.RawMessage) error { return json.Unmarshal(raw, &resp) })
		if err != nil {
			return nil, err
		}
		return resp, nil
	}
	if err := e.pool.WithTenantTx(ctx, orgID, mutate); err != nil {
		return nil, err
	}
	return resp, nil
}

func (e *WorkerEngine) resumeRunTx(
	ctx context.Context, tx storage.Tx,
	orgID, runID string, expectedRevision int64,
	audit *tenant.AuditContext,
) (*RunDTO, error) {
	var status, reason, workflowName string
	var revision int64
	var environmentID string
	var pauseRequested bool
	var manifestBytes, rawRunInput []byte
	if err := tx.QueryRow(ctx, `SELECT r.status, COALESCE(r.reason_code,''), r.revision,
			r.environment_id::text, r.pause_requested, r.workflow_name, d.manifest, r.input
		FROM runs r
		JOIN deployments d ON d.id=r.deployment_id AND d.organization_id=r.organization_id
		WHERE r.id=$1::uuid AND r.organization_id=$2::uuid FOR UPDATE OF r`,
		runID, orgID).Scan(&status, &reason, &revision, &environmentID, &pauseRequested,
		&workflowName, &manifestBytes, &rawRunInput); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrRunNotFound
		}
		return nil, err
	}
	switch status {
	case "SUCCEEDED", "FAILED", "CANCELLED":
		return nil, ErrRunTerminal
	case "CANCELLING":
		return nil, ErrRunCancelling
	}
	if revision != expectedRevision {
		return nil, ErrRevisionConflict
	}
	if !pauseRequested && status != "PAUSING" && status != "PAUSED" {
		return nil, ErrRunNotPaused
	}

	// 1. Check if all workflow nodes are already terminal (Blueprint §10.2 rule 4).
	var manifest deploymentManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return nil, fmt.Errorf("decode manifest for resume: %w", err)
	}
	var targetWorkflow *workflowManifest
	for i := range manifest.Workflows {
		if manifest.Workflows[i].Name == workflowName {
			targetWorkflow = &manifest.Workflows[i]
			break
		}
	}
	var runInput any
	if len(rawRunInput) > 0 {
		_ = json.Unmarshal(rawRunInput, &runInput)
	}

	stepRows, err := tx.Query(ctx, `SELECT id::text, node_id, state, output
		FROM run_steps WHERE run_id=$1::uuid AND organization_id=$2::uuid
		FOR UPDATE`, runID, orgID)
	if err != nil {
		return nil, err
	}
	type stepData struct {
		id     string
		nodeID string
		state  string
		output any
	}
	stepsByNode := make(map[string]*stepData)
	outputsMap := make(map[string]any)
	for stepRows.Next() {
		var s stepData
		var rawOut []byte
		if err := stepRows.Scan(&s.id, &s.nodeID, &s.state, &rawOut); err != nil {
			stepRows.Close()
			return nil, err
		}
		if len(rawOut) > 0 && string(rawOut) != "null" {
			_ = json.Unmarshal(rawOut, &s.output)
			outputsMap[s.nodeID] = s.output
		}
		stepsByNode[s.nodeID] = &s
	}
	stepRows.Close()

	if targetWorkflow != nil {
		states := make(map[string]string, len(stepsByNode))
		for nodeID, s := range stepsByNode {
			states[nodeID] = s.state
		}
		settled, err := settleRunTerminalTx(ctx, tx, orgID, runID, targetWorkflow, states, outputsMap, runInput, nil)
		if err != nil {
			return nil, err
		}
		if settled {
			// Run settled terminal directly. Clear pause flag and bump revision.
			if _, err := tx.Exec(ctx, `UPDATE runs SET pause_requested=false, revision=revision+1, updated_at=clock_timestamp()
				WHERE id=$1::uuid AND organization_id=$2::uuid`, runID, orgID); err != nil {
				return nil, err
			}
			return queryRunDTO(ctx, tx, orgID, runID)
		}
	}

	// 2. Recompute active, ready, or waiting state (Blueprint §10.2 rules 6-7).
	var activeAttempts int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_attempts a
		JOIN run_steps rs ON rs.id=a.step_id AND rs.organization_id=a.organization_id
		WHERE rs.run_id=$1::uuid AND a.organization_id=$2::uuid
			AND a.status IN ('CLAIMED','RUNNING')`, runID, orgID).Scan(&activeAttempts); err != nil {
		return nil, err
	}

	var readySteps int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM run_steps
		WHERE run_id=$1::uuid AND organization_id=$2::uuid
			AND state='READY'`, runID, orgID).Scan(&readySteps); err != nil {
		return nil, err
	}

	var hasStarted bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM task_attempts a
		JOIN run_steps rs ON rs.id=a.step_id AND rs.organization_id=a.organization_id
		WHERE rs.run_id=$1::uuid AND a.organization_id=$2::uuid
			AND (a.started_at IS NOT NULL OR a.status IN ('RUNNING','SUCCEEDED','FAILED','TIMED_OUT','LOST'))
	)`, runID, orgID).Scan(&hasStarted); err != nil {
		return nil, err
	}

	var openCases int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM reconciliation_cases rc
		JOIN run_steps rs ON rs.id=rc.step_id AND rs.organization_id=rc.organization_id
		WHERE rs.run_id=$1::uuid AND rc.organization_id=$2::uuid AND rc.status='OPEN'`,
		runID, orgID).Scan(&openCases); err != nil {
		return nil, err
	}

	var pendingTimers int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM timers
		WHERE run_id=$1::uuid AND organization_id=$2::uuid
			AND state='PENDING' AND kind='RETRY_BACKOFF'`, runID, orgID).Scan(&pendingTimers); err != nil {
		return nil, err
	}

	var targetStatus string
	var targetReason *string

	if activeAttempts > 0 {
		targetStatus = "RUNNING"
		targetReason = nil
	} else if readySteps > 0 {
		if hasStarted {
			targetStatus = "RUNNING"
		} else {
			targetStatus = "QUEUED"
		}
		targetReason = nil
	} else if openCases > 0 {
		targetStatus = "WAITING"
		r := "RECONCILIATION"
		targetReason = &r
	} else if pendingTimers > 0 {
		targetStatus = "WAITING"
		r := "RETRY_BACKOFF"
		targetReason = &r
	} else {
		targetStatus = "WAITING"
		targetReason = nil
	}

	if _, err := tx.Exec(ctx, `UPDATE runs SET status=$1, reason_code=$2,
		pause_requested=false, revision=revision+1, updated_at=clock_timestamp()
		WHERE id=$3::uuid AND organization_id=$4::uuid`, targetStatus, targetReason, runID, orgID); err != nil {
		return nil, err
	}

	payload := map[string]any{"status": targetStatus}
	if targetReason != nil {
		payload["reason"] = *targetReason
	}
	if err := appendRunEvent(ctx, tx, orgID, runID, "RUN_RESUMED", payload); err != nil {
		return nil, err
	}
	if err := appendResumeAuditTx(ctx, tx, orgID, runID, audit, targetStatus); err != nil {
		return nil, err
	}

	return queryRunDTO(ctx, tx, orgID, runID)
}

func appendResumeAuditTx(ctx context.Context, tx storage.Tx, orgID, runID string, audit *tenant.AuditContext, targetStatus string) error {
	meta, err := json.Marshal(map[string]any{
		"actor_type":   audit.ActorType,
		"role":         audit.Role,
		"capabilities": audit.Capabilities,
		"targetStatus": targetStatus,
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
		VALUES ($1::uuid, $2, 'run.resume', 'run', $3::uuid, $4, 'RESUME_REQUESTED', $5::jsonb)`,
		orgID, actorID, runID, audit.CorrelationID, string(meta))
	return err
}

// settlePauseAfterCompletionTx transitions a PAUSING run to PAUSED once all
// in-flight attempts have drained, provided the run has not already terminalized
// (Blueprint §10.2 rule 5).
func settlePauseAfterCompletionTx(ctx context.Context, tx storage.Tx, orgID, runID string) error {
	var status string
	var pauseRequested bool
	if err := tx.QueryRow(ctx, `SELECT status, pause_requested FROM runs
		WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, runID, orgID).Scan(&status, &pauseRequested); err != nil {
		return err
	}
	if !pauseRequested || status != "PAUSING" {
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
	tag, err := tx.Exec(ctx, `UPDATE runs SET status='PAUSED', updated_at=clock_timestamp()
		WHERE id=$1::uuid AND organization_id=$2::uuid AND status='PAUSING'`, runID, orgID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	return appendRunEvent(ctx, tx, orgID, runID, "RUN_PAUSED", map[string]any{
		"reason": "PAUSE_REQUESTED",
	})
}
