// The transaction body of CreateRun, shared with schedule occurrence
// evaluation so a scheduled run and an operator-initiated run follow exactly
// one implementation. See CreateRunInTx.

package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"github.com/Ryanakml/Deadbolt/internal/recovery"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
)

// CreateRunInTx performs run creation inside a caller-supplied transaction,
// so a schedule occurrence, the run it creates, and the schedule's next due
// time can all commit atomically (Blueprint §17).
//
// The body is the transaction body of CreateRun, unchanged and kept in one
// place: a single implementation of admission, deployment resolution, run and
// step insertion, the outbox hint, and the audit record, so a scheduled run
// cannot drift from an operator-initiated one. A caller that also needs a
// cross-row decision must already hold the environment admission lock, since
// §17 fixes the order as environment, then schedule, then run.
func (s *Service) CreateRunInTx(
	ctx context.Context,
	tx storage.Tx,
	orgID, envID string,
	env *tenant.Environment,
	workflowName, idempotencyKey, keyHash, requestHash string,
	explicitDeploymentID *string,
	canonicalInput []byte,
	input any,
	audit *tenant.AuditContext,
) (*RunDTO, bool, error) {
	var resultRun *RunDTO
	var isReplay bool
	// 1. Check idempotency record under lock
	// PostgreSQL advisory transaction locks serialize a previously unseen key
	// too; SELECT FOR UPDATE alone cannot lock a missing row.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, envID+":CREATE_RUN:"+keyHash); err != nil {
		return nil, false, fmt.Errorf("lock idempotency identity: %w", err)
	}
	var storedReqHash, storedRunID string
	err := tx.QueryRow(ctx, `SELECT request_hash, response_identity::text
			FROM idempotency_records
			WHERE environment_id = $1::uuid AND operation_type='CREATE_RUN' AND key_hash = $2
			FOR UPDATE`, envID, keyHash).Scan(&storedReqHash, &storedRunID)
	if err == nil {
		if storedReqHash != requestHash {
			return nil, false, ErrIdempotencyConflict
		}
		// Exact replay: load and return the original run, not a new one.
		isReplay = true
		resultRun, err = queryRunDTO(ctx, tx, orgID, storedRunID)
		return resultRun, isReplay, err
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, fmt.Errorf("query idempotency: %w", err)
	}

	// Disaster recovery admission gate (Blueprint §27.3). Fail closed:
	// a missing or unreadable controls row must not admit new runs.
	var admissionEnabled bool
	var recMode string
	if err := tx.QueryRow(ctx, `SELECT admission_enabled, mode FROM system_recovery_controls WHERE id = 1`).Scan(&admissionEnabled, &recMode); err != nil {
		return nil, false, fmt.Errorf("%w: read system recovery controls: %v", recovery.ErrRecoveryControlsUnavailable, err)
	}
	if !admissionEnabled || recMode == "READ_ONLY" || recMode == "DISASTER_RECOVERY" {
		return nil, false, recovery.ErrAdmissionDisabled
	}

	// 2. Serialize all environment admission decisions. The lock must be
	// acquired before counting nonterminal runs or recent creates so
	// concurrent CreateRun requests cannot pass the same cap together.
	var admissionEnvironmentID string
	if err := tx.QueryRow(ctx, `SELECT environment_id::text
			FROM environment_admissions
			WHERE environment_id = $1::uuid AND organization_id = $2::uuid
			FOR UPDATE`, envID, orgID).Scan(&admissionEnvironmentID); err != nil {
		return nil, false, fmt.Errorf("lock environment admission: %w", err)
	}
	var remainingTokens float64
	if err := tx.QueryRow(ctx, `UPDATE environment_admissions
			SET create_rate_tokens = LEAST(10::double precision,
				create_rate_tokens + EXTRACT(EPOCH FROM (clock_timestamp() - create_rate_updated_at)) * 5) - 1,
				create_rate_updated_at = clock_timestamp(), updated_at = clock_timestamp()
			WHERE environment_id = $1::uuid AND organization_id = $2::uuid
			  AND LEAST(10::double precision,
				create_rate_tokens + EXTRACT(EPOCH FROM (clock_timestamp() - create_rate_updated_at)) * 5) >= 1
			RETURNING create_rate_tokens`, envID, orgID).Scan(&remainingTokens); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, ErrCreateRateLimited
		}
		return nil, false, fmt.Errorf("debit create-run rate bucket: %w", err)
	}
	var nonterminalRuns int
	if err := tx.QueryRow(ctx, `SELECT count(*)
			FROM runs
			WHERE environment_id = $1::uuid AND organization_id = $2::uuid
			  AND status IN ('QUEUED','RUNNING','WAITING','PAUSING','PAUSED','CANCELLING')`, envID, orgID).Scan(&nonterminalRuns); err != nil {
		return nil, false, fmt.Errorf("count nonterminal runs: %w", err)
	}
	if nonterminalRuns >= maxNonterminalRuns {
		return nil, false, ErrRunQuotaExceeded
	}
	// 3. Resolve deployment once
	var deploymentID string
	var manifestJSON []byte
	if explicitDeploymentID != nil && *explicitDeploymentID != "" {
		err = tx.QueryRow(ctx, `SELECT id::text, manifest FROM deployments
				WHERE id = $1::uuid AND organization_id = $2::uuid AND environment_id = $3::uuid`,
			*explicitDeploymentID, orgID, envID).Scan(&deploymentID, &manifestJSON)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, false, ErrDeploymentNotFound
			}
			return nil, false, fmt.Errorf("query explicit deployment: %w", err)
		}
	} else {
		err = tx.QueryRow(ctx, `SELECT d.id::text, d.manifest
				FROM workflow_channels c
				JOIN deployments d ON d.id = c.active_deployment_id AND d.organization_id = c.organization_id
				WHERE c.environment_id = $1::uuid AND c.organization_id = $2::uuid AND c.workflow_name = $3`,
			envID, orgID, workflowName).Scan(&deploymentID, &manifestJSON)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, false, ErrNoActiveDeployment
			}
			return nil, false, fmt.Errorf("query active deployment: %w", err)
		}
	}

	// 3. Parse manifest and validate input against workflow inputSchema
	var manifest deploymentManifest
	if err := json.Unmarshal(manifestJSON, &manifest); err != nil {
		return nil, false, fmt.Errorf("decode manifest: %w", err)
	}
	var targetWorkflow *workflowManifest
	for i := range manifest.Workflows {
		if manifest.Workflows[i].Name == workflowName {
			targetWorkflow = &manifest.Workflows[i]
			break
		}
	}
	if targetWorkflow == nil {
		return nil, false, ErrWorkflowNotFound
	}
	if len(targetWorkflow.Nodes) > maxWorkflowNodes {
		return nil, false, ErrWorkflowTooLarge
	}

	if targetWorkflow.InputSchema != nil {
		if err := contracts.ValidatePayload(targetWorkflow.InputSchema, input); err != nil {
			return nil, false, ErrSchemaViolation
		}
	}

	// 4. Create Run with the V1 lifetime default: 7d from acceptance.
	var runID string
	var createdAt time.Time
	var deadlineAt *time.Time
	runQuery := `INSERT INTO runs (
			organization_id, environment_id, deployment_id, workflow_name,
			idempotency_key, status, revision, input, last_event_sequence,
			deadline_at, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, $3::uuid, $4,
			$5, 'QUEUED', 1, $6::jsonb, 1,
			clock_timestamp()+($7::bigint*INTERVAL '1 millisecond'),
			clock_timestamp(), clock_timestamp()
		) RETURNING id::text, created_at, deadline_at`
	if err := tx.QueryRow(ctx, runQuery, orgID, envID, deploymentID, workflowName, idempotencyKey, canonicalInput, RunLifetimeMs).
		Scan(&runID, &createdAt, &deadlineAt); err != nil {
		return nil, false, fmt.Errorf("insert run: %w", err)
	}

	// 5. Create run_steps from graph
	stepIDs := make(map[string]string, len(targetWorkflow.Nodes))
	evalSteps := make(map[string]*dagEvalStep, len(targetWorkflow.Nodes))
	hasControlNode := false
	for _, node := range targetWorkflow.Nodes {
		kind := node.Type
		if kind == "" {
			kind = "task"
		}
		if kind == "choice" || kind == "merge" || kind == "delay" {
			hasControlNode = true
		}
		initState := "BLOCKED"
		var eligibleAt any = nil
		if len(node.After) == 0 && kind == "task" {
			initState = "READY"
			eligibleAt = time.Now()
		}
		stepQuery := `INSERT INTO run_steps (
				organization_id, environment_id, run_id, node_id,
				kind, state, eligible_at, created_at, updated_at
			) VALUES (
				$1::uuid, $2::uuid, $3::uuid, $4,
				$5, $6, $7, clock_timestamp(), clock_timestamp()
			) RETURNING id::text`
		var stepID string
		if err := tx.QueryRow(ctx, stepQuery, orgID, envID, runID, node.ID, kind, initState, eligibleAt).Scan(&stepID); err != nil {
			return nil, false, fmt.Errorf("insert step %s: %w", node.ID, err)
		}
		stepIDs[node.ID] = stepID
		evalSteps[node.ID] = &dagEvalStep{id: stepID, state: initState}
	}

	// 6. Record idempotency
	idempotencyInsert := `INSERT INTO idempotency_records (
			organization_id, environment_id, operation_type, key_hash, request_hash,
			response_identity, expires_at, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'CREATE_RUN', $3, $4,
			$5::uuid, 'infinity', clock_timestamp()
		)`
	if _, err := tx.Exec(ctx, idempotencyInsert, orgID, envID, keyHash, requestHash, runID); err != nil {
		return nil, false, fmt.Errorf("insert idempotency: %w", err)
	}

	// 7. Initial run_event
	eventInsert := `INSERT INTO run_events (
			organization_id, run_id, sequence, event_type, payload
		) VALUES (
			$1::uuid, $2::uuid, 1, 'RUN_CREATED',
			jsonb_build_object('runId', $2::uuid, 'workflowName', $3::text, 'deploymentId', $4::uuid)
		)`
	if _, err := tx.Exec(ctx, eventInsert, orgID, runID, workflowName, deploymentID); err != nil {
		return nil, false, fmt.Errorf("insert run event: %w", err)
	}

	// 8. Outbox hint
	outboxInsert := `INSERT INTO outbox_events (
			organization_id, subject, payload
		) VALUES (
			$1::uuid, 'execution.state_changed',
			jsonb_build_object('runId', $2::uuid, 'eventType', 'RUN_CREATED')
		)`
	if _, err := tx.Exec(ctx, outboxInsert, orgID, runID); err != nil {
		return nil, false, fmt.Errorf("insert outbox: %w", err)
	}

	// 9. Audit event
	var actorID *string
	if audit.ActorID != nil && *audit.ActorID != "" {
		actorID = audit.ActorID
	}
	auditMeta, err := json.Marshal(map[string]any{
		"actor_type":     audit.ActorType,
		"role":           audit.Role,
		"capabilities":   audit.Capabilities,
		"environment_id": envID,
		"workflow_name":  workflowName,
		"deployment_id":  deploymentID,
	})
	if err != nil {
		return nil, false, fmt.Errorf("marshal audit metadata: %w", err)
	}
	auditInsert := `INSERT INTO audit_events (
			organization_id, actor_id, action, target_type, target_id, correlation_id, reason, metadata
		) VALUES (
			$1::uuid, $2, 'run.create', 'run', $3::uuid, $4, $5, $6::jsonb
		)`
	if _, err := tx.Exec(ctx, auditInsert, orgID, actorID, runID, audit.CorrelationID, audit.Reason, auditMeta); err != nil {
		return nil, false, fmt.Errorf("insert audit: %w", err)
	}

	var deadlineStr *string
	if deadlineAt != nil {
		d := deadlineAt.UTC().Format(time.RFC3339)
		deadlineStr = &d
	}

	resultRun = &RunDTO{
		ID:             runID,
		OrganizationID: orgID,
		ProjectID:      env.ProjectID,
		EnvironmentID:  envID,
		WorkflowName:   workflowName,
		DeploymentID:   deploymentID,
		Status:         contracts.RunStatusQUEUED,
		Revision:       1,
		CreatedAt:      createdAt.UTC().Format(time.RFC3339),
		DeadlineAt:     deadlineStr,
	}

	if hasControlNode {
		outputsMap := make(map[string]any)
		if _, runFailed, err := evaluateBlockedDAGTx(ctx, tx, orgID, runID, workflowName, targetWorkflow, &manifest, input, evalSteps, outputsMap, ""); err != nil {
			return nil, false, fmt.Errorf("evaluate initial DAG: %w", err)
		} else if runFailed {
			var status string
			_ = tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid AND organization_id=$2::uuid`, runID, orgID).Scan(&status)
			if status != "" {
				resultRun.Status = contracts.RunStatus(status)
			} else {
				resultRun.Status = contracts.RunStatusFAILED
			}
		} else {
			states := make(map[string]string, len(evalSteps))
			for nodeID, st := range evalSteps {
				states[nodeID] = st.state
			}
			if settled, err := settleRunTerminalTx(ctx, tx, orgID, runID, targetWorkflow, states, outputsMap, input, nil); err != nil {
				return nil, false, fmt.Errorf("settle initial DAG: %w", err)
			} else if settled {
				var status string
				_ = tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid AND organization_id=$2::uuid`, runID, orgID).Scan(&status)
				if status != "" {
					resultRun.Status = contracts.RunStatus(status)
				}
			}
		}
	}

	if s.beforeCreateCommit != nil {
		if err := s.beforeCreateCommit(); err != nil {
			return nil, false, err
		}
	}
	return resultRun, isReplay, nil
}
