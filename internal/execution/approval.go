package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/jackc/pgx/v5"
)

var (
	ErrApprovalNotFound  = errors.New("APPROVAL_NOT_FOUND: Approval not found")
	ErrApprovalExpired   = errors.New("APPROVAL_EXPIRED: Approval expired")
	ErrApprovalCancelled = errors.New("APPROVAL_CANCELLED: Approval cancelled")
	ErrApprovalConflict  = errors.New("APPROVAL_CONFLICT: Approval already has a different decision")
	ErrApprovalRevision  = errors.New("APPROVAL_REVISION_CONFLICT: Approval revision is stale")
	ErrApprovalActor     = errors.New("APPROVAL_ACTOR_FORBIDDEN: Only an identifiable human may decide")
)

func activateApprovalTx(ctx context.Context, tx storage.Tx, orgID, runID, stepID string, node *workflowNode) error {
	payload := any(map[string]any{})
	schema := any(map[string]any{"type": "string", "enum": []string{"approved", "rejected"}})
	permission := "approvals:decide"
	if node != nil && node.Approval != nil {
		if v, ok := node.Approval["payload"]; ok {
			payload = v
		}
		if v, ok := node.Approval["decisionSchema"]; ok {
			schema = v
		}
		if v, ok := node.Approval["requiredPermission"].(string); ok && v != "" {
			permission = v
		}
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `INSERT INTO approvals
		(organization_id, environment_id, step_id, payload, decision_schema, required_permission, expires_at)
		SELECT $1::uuid, r.environment_id, $2::uuid, $3::jsonb, $4::jsonb, $5,
			LEAST(clock_timestamp()+INTERVAL '24 hours', COALESCE(r.deadline_at, 'infinity'::timestamptz))
		FROM runs r WHERE r.id=$6::uuid AND r.organization_id=$1::uuid
		ON CONFLICT (step_id) DO NOTHING`, orgID, stepID, payloadJSON, schemaJSON, permission, runID)
	if err != nil {
		return fmt.Errorf("create approval: %w", err)
	}
	if result.RowsAffected() == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='WAITING', wait_reason='APPROVAL', eligible_at=NULL, updated_at=clock_timestamp() WHERE id=$1::uuid AND organization_id=$2::uuid AND state='BLOCKED'`, stepID, orgID); err != nil {
		return err
	}
	return appendRunEvent(ctx, tx, orgID, runID, "APPROVAL_REQUESTED", map[string]any{"stepId": stepID, "requiredPermission": permission})
}

func (e *WorkerEngine) DecideApproval(ctx context.Context, orgID, approvalID string, req ApprovalDecisionRequest, audit *tenant.AuditContext) (*ApprovalDTO, error) {
	if audit == nil || audit.ActorType != tenant.IdentityTypeHuman || audit.ActorID == nil || strings.TrimSpace(*audit.ActorID) == "" {
		return nil, ErrApprovalActor
	}
	decision := strings.ToLower(strings.TrimSpace(req.Decision))
	if decision != "approved" && decision != "rejected" {
		return nil, fmt.Errorf("invalid decision")
	}
	if len(req.Comment) > 280 {
		return nil, fmt.Errorf("comment exceeds 280 characters")
	}
	var result *ApprovalDTO
	var expiredErr error
	err := e.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		var runID, stepID, status string
		var revision int64
		if err := tx.QueryRow(ctx, `SELECT r.id::text, rs.id::text, a.status, a.revision FROM approvals a JOIN run_steps rs ON rs.id=a.step_id AND rs.organization_id=a.organization_id JOIN runs r ON r.id=rs.run_id AND r.organization_id=rs.organization_id WHERE a.id=$1::uuid AND a.organization_id=$2::uuid FOR UPDATE OF r, rs, a`, approvalID, orgID).Scan(&runID, &stepID, &status, &revision); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrApprovalNotFound
			}
			return err
		}
		if status != "PENDING" {
			var oldDecision *string
			_ = tx.QueryRow(ctx, `SELECT decision FROM approvals WHERE id=$1::uuid AND organization_id=$2::uuid`, approvalID, orgID).Scan(&oldDecision)
			if status == "APPROVED" || status == "REJECTED" {
				if oldDecision != nil && *oldDecision == decision {
					return scanApproval(ctx, tx, orgID, approvalID, &result)
				}
				return ErrApprovalConflict
			}
			if status == "EXPIRED" {
				return ErrApprovalExpired
			}
			return ErrApprovalCancelled
		}
		if req.ExpectedRevision != 0 && req.ExpectedRevision != revision {
			return ErrApprovalRevision
		}
		var expired bool
		if err := tx.QueryRow(ctx, `SELECT expires_at <= clock_timestamp() FROM approvals WHERE id=$1::uuid AND organization_id=$2::uuid`, approvalID, orgID).Scan(&expired); err != nil {
			return err
		}
		if expired {
			if err := expireApprovalTxNoError(ctx, tx, orgID, runID, stepID, approvalID); err != nil {
				return err
			}
			expiredErr = ErrApprovalExpired
			return scanApproval(ctx, tx, orgID, approvalID, &result)
		}
		comment := strings.TrimSpace(req.Comment)
		if _, err := tx.Exec(ctx, `UPDATE approvals SET status=upper($1), decision=$1, actor_id=$2::uuid, decided_at=clock_timestamp(), comment=NULLIF($3,''), revision=revision+1 WHERE id=$4::uuid AND organization_id=$5::uuid AND status='PENDING'`, decision, *audit.ActorID, comment, approvalID, orgID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='SUCCEEDED', wait_reason=NULL, output=jsonb_build_object('decision',$1::text,'actorId',$2::uuid,'decidedAt',clock_timestamp(),'comment',NULLIF($3,'')), updated_at=clock_timestamp() WHERE id=$4::uuid AND organization_id=$5::uuid AND state='WAITING' AND wait_reason='APPROVAL'`, decision, *audit.ActorID, comment, stepID, orgID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE runs SET status='RUNNING', reason_code=NULL, revision=revision+1, updated_at=clock_timestamp() WHERE id=$1::uuid AND organization_id=$2::uuid AND status='WAITING' AND reason_code='APPROVAL'`, runID, orgID); err != nil {
			return err
		}
		if err := appendRunEvent(ctx, tx, orgID, runID, "APPROVAL_DECIDED", map[string]any{"approvalId": approvalID, "stepId": stepID, "decision": decision, "actorId": *audit.ActorID, "comment": comment}); err != nil {
			return err
		}
		if err := advanceAfterStepSuccessTx(ctx, tx, orgID, runID, map[string]any{"decision": decision, "actorId": *audit.ActorID, "comment": comment}); err != nil {
			return err
		}
		return scanApproval(ctx, tx, orgID, approvalID, &result)
	})
	if err == nil && expiredErr != nil {
		err = expiredErr
	}
	return result, err
}

func (e *WorkerEngine) ListApprovals(ctx context.Context, orgID, environmentID string, includeClosed bool) (*ApprovalListResponseDTO, error) {
	var out ApprovalListResponseDTO
	err := e.pool.WithTenantReadOnlyRepeatableReadTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		q := `SELECT a.id::text FROM approvals a WHERE a.organization_id=$1::uuid AND a.environment_id=$2::uuid`
		if !includeClosed {
			q += ` AND a.status='PENDING'`
		}
		q += ` ORDER BY a.created_at ASC, a.id ASC LIMIT 100`
		rows, err := tx.Query(ctx, q, orgID, environmentID)
		if err != nil {
			return err
		}
		defer rows.Close()
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		rows.Close()
		for _, id := range ids {
			var a *ApprovalDTO
			if err := scanApproval(ctx, tx, orgID, id, &a); err != nil {
				return err
			}
			out.Items = append(out.Items, *a)
		}
		return nil
	})
	return &out, err
}

func (e *WorkerEngine) GetApproval(ctx context.Context, orgID, approvalID string) (*ApprovalDTO, error) {
	var out *ApprovalDTO
	err := e.pool.WithTenantReadOnlyRepeatableReadTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error { return scanApproval(ctx, tx, orgID, approvalID, &out) })
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrApprovalNotFound
	}
	return out, err
}

func scanApproval(ctx context.Context, tx storage.Tx, orgID, approvalID string, out **ApprovalDTO) error {
	var a ApprovalDTO
	var rawPayload, rawSchema []byte
	var decidedAt *time.Time
	var expiresAt time.Time
	if err := tx.QueryRow(ctx, `SELECT id::text, step_id::text, rs.run_id::text, a.environment_id::text, payload, decision_schema, required_permission, status, decision, actor_id::text, decided_at, comment, expires_at, revision FROM approvals a JOIN run_steps rs ON rs.id=a.step_id AND rs.organization_id=a.organization_id WHERE a.id=$1::uuid AND a.organization_id=$2::uuid`, approvalID, orgID).Scan(&a.ID, &a.StepID, &a.RunID, &a.EnvironmentID, &rawPayload, &rawSchema, &a.RequiredPermission, &a.Status, &a.Decision, &a.ActorID, &decidedAt, &a.Comment, &expiresAt, &a.Revision); err != nil {
		return err
	}
	_ = json.Unmarshal(rawPayload, &a.Payload)
	_ = json.Unmarshal(rawSchema, &a.DecisionSchema)
	if decidedAt != nil {
		s := decidedAt.UTC().Format(time.RFC3339Nano)
		a.DecidedAt = &s
	}
	a.ExpiresAt = expiresAt.UTC().Format(time.RFC3339Nano)
	*out = &a
	return nil
}

func expireApprovalTx(ctx context.Context, tx storage.Tx, orgID, runID, stepID, approvalID string) error {
	if _, err := tx.Exec(ctx, `UPDATE approvals SET status='EXPIRED', revision=revision+1 WHERE id=$1::uuid AND organization_id=$2::uuid AND status='PENDING'`, approvalID, orgID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='FAILED', wait_reason='APPROVAL_EXPIRED', updated_at=clock_timestamp() WHERE id=$1::uuid AND organization_id=$2::uuid AND state='WAITING'`, stepID, orgID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE runs SET status='FAILED', reason_code='APPROVAL_EXPIRED', revision=revision+1, updated_at=clock_timestamp() WHERE id=$1::uuid AND organization_id=$2::uuid AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED')`, runID, orgID); err != nil {
		return err
	}
	if err := appendRunEvent(ctx, tx, orgID, runID, "APPROVAL_EXPIRED", map[string]any{"approvalId": approvalID, "stepId": stepID}); err != nil {
		return err
	}
	return ErrApprovalExpired
}

func expireDueApprovalsTx(ctx context.Context, tx storage.Tx, orgID string) (int, []string, error) {
	rows, err := tx.Query(ctx, `SELECT r.id::text, rs.id::text, a.id::text FROM approvals a JOIN run_steps rs ON rs.id=a.step_id AND rs.organization_id=a.organization_id JOIN runs r ON r.id=rs.run_id AND r.organization_id=rs.organization_id WHERE a.organization_id=$1::uuid AND a.status='PENDING' AND a.expires_at <= clock_timestamp() ORDER BY r.id, rs.id, a.id LIMIT 50 FOR UPDATE OF r, rs, a`, orgID)
	if err != nil {
		return 0, nil, err
	}
	type item struct{ runID, stepID, approvalID string }
	var items []item
	for rows.Next() {
		var i item
		if err := rows.Scan(&i.runID, &i.stepID, &i.approvalID); err != nil {
			rows.Close()
			return 0, nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, nil, err
	}
	rows.Close()
	var runs []string
	for _, i := range items {
		if err := expireApprovalTxNoError(ctx, tx, orgID, i.runID, i.stepID, i.approvalID); err != nil {
			return 0, nil, err
		}
		runs = append(runs, i.runID)
	}
	return len(items), runs, nil
}

func expireApprovalTxNoError(ctx context.Context, tx storage.Tx, orgID, runID, stepID, approvalID string) error {
	if _, err := tx.Exec(ctx, `UPDATE approvals SET status='EXPIRED', revision=revision+1 WHERE id=$1::uuid AND organization_id=$2::uuid AND status='PENDING'`, approvalID, orgID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='FAILED', wait_reason='APPROVAL_EXPIRED', updated_at=clock_timestamp() WHERE id=$1::uuid AND organization_id=$2::uuid AND state='WAITING'`, stepID, orgID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE runs SET status='FAILED', reason_code='APPROVAL_EXPIRED', revision=revision+1, updated_at=clock_timestamp() WHERE id=$1::uuid AND organization_id=$2::uuid AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED')`, runID, orgID); err != nil {
		return err
	}
	return appendRunEvent(ctx, tx, orgID, runID, "APPROVAL_EXPIRED", map[string]any{"approvalId": approvalID, "stepId": stepID})
}
