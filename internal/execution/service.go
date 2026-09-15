package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/jackc/pgx/v5"
)

var (
	ErrMissingIdempotencyKey = errors.New("MISSING_IDEMPOTENCY_KEY: Idempotency-Key header is required")
	ErrIdempotencyConflict   = errors.New("IDEMPOTENCY_CONFLICT: Idempotency key already used with different payload")
	ErrNoActiveDeployment    = errors.New("NO_ACTIVE_DEPLOYMENT: No active deployment found for workflow")
	ErrDeploymentNotFound    = errors.New("DEPLOYMENT_NOT_FOUND: Deployment not found")
	ErrWorkflowNotFound      = errors.New("WORKFLOW_NOT_FOUND: Workflow not found in deployment")
	ErrRunNotFound           = errors.New("RUN_NOT_FOUND: Run not found")
	ErrSchemaViolation       = errors.New("SCHEMA_VIOLATION: Input does not conform to workflow schema")
	ErrEnvironmentNotFound   = errors.New("ENVIRONMENT_NOT_FOUND: Environment not found")
	ErrPayloadTooLarge       = errors.New("PAYLOAD_TOO_LARGE: Inline JSON payload exceeds 256 KiB")
)

const maxInlinePayloadBytes = 256 << 10

type Service struct {
	pool    *storage.Pool
	tenants *tenant.Service
}

func NewService(pool *storage.Pool, tenants *tenant.Service) *Service {
	return &Service{
		pool:    pool,
		tenants: tenants,
	}
}

type payloadHashModel struct {
	Workflow     string  `json:"workflow"`
	DeploymentID *string `json:"deploymentId,omitempty"`
	Input        any     `json:"input"`
}

func (s *Service) resolveEnvironment(ctx context.Context, orgID, envParam string) (*tenant.Environment, error) {
	env, err := s.tenants.GetEnvironment(ctx, orgID, envParam)
	if err == nil {
		return env, nil
	}
	// Fallback to query by name
	var out tenant.Environment
	err = s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		query := `
			SELECT e.id, e.organization_id, e.project_id, e.name, COALESCE(ea.max_concurrency, 10), e.created_at, e.updated_at
			FROM environments e
			LEFT JOIN environment_admissions ea ON ea.environment_id = e.id AND ea.organization_id = e.organization_id
			WHERE e.organization_id = $1 AND e.name = $2
		`
		scanErr := tx.QueryRow(ctx, query, orgID, envParam).
			Scan(&out.ID, &out.OrganizationID, &out.ProjectID, &out.Name, &out.MaxConcurrency, &out.CreatedAt, &out.UpdatedAt)
		if errors.Is(scanErr, pgx.ErrNoRows) {
			return ErrEnvironmentNotFound
		}
		return scanErr
	})
	if err != nil {
		return nil, ErrEnvironmentNotFound
	}
	return &out, nil
}

// CreateRun implements Blueprint §8 & §14.2 & §20.1:
//  1. Authenticates & resolves environment
//  2. Computes JCS canonical payload hash
//  3. In single transaction: checks idempotency, resolves deployment once,
//     inserts run, run_steps (root READY, others BLOCKED), idempotency_record,
//     run_events, and outbox_events.
//
// Returns (run, isReplay, error).
func (s *Service) CreateRun(
	ctx context.Context,
	orgID, envParam, workflowName, idempotencyKey string,
	explicitDeploymentID *string,
	input any,
	audit *tenant.AuditContext,
) (*RunDTO, bool, error) {
	if audit == nil {
		return nil, false, tenant.ErrAuditRequired
	}
	if idempotencyKey == "" {
		return nil, false, ErrMissingIdempotencyKey
	}

	env, err := s.resolveEnvironment(ctx, orgID, envParam)
	if err != nil {
		return nil, false, ErrEnvironmentNotFound
	}
	envID := env.ID

	hashPayload := map[string]any{
		"workflow": workflowName,
		"input":    input,
	}
	if explicitDeploymentID != nil {
		hashPayload["deploymentId"] = *explicitDeploymentID
	}
	canonicalPayload, err := contracts.CanonicalizeGeneric(hashPayload)
	if err != nil {
		return nil, false, fmt.Errorf("canonicalize request: %w", err)
	}
	requestHash := contracts.SHA256Hex(canonicalPayload)
	keyHash := contracts.SHA256Hex([]byte(idempotencyKey))

	canonicalInput, err := contracts.CanonicalizeGeneric(input)
	if err != nil {
		return nil, false, fmt.Errorf("canonicalize input: %w", err)
	}
	if len(canonicalInput) > maxInlinePayloadBytes {
		return nil, false, ErrPayloadTooLarge
	}

	var resultRun *RunDTO
	var isReplay bool

	err = s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		// 1. Check idempotency record under lock
		// PostgreSQL advisory transaction locks serialize a previously unseen key
		// too; SELECT FOR UPDATE alone cannot lock a missing row.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, envID+":CREATE_RUN:"+keyHash); err != nil {
			return fmt.Errorf("lock idempotency identity: %w", err)
		}
		var storedReqHash, storedRunID string
		err := tx.QueryRow(ctx, `SELECT request_hash, response_identity::text
			FROM idempotency_records
			WHERE environment_id = $1::uuid AND operation_type='CREATE_RUN' AND key_hash = $2
			FOR UPDATE`, envID, keyHash).Scan(&storedReqHash, &storedRunID)
		if err == nil {
			if storedReqHash != requestHash {
				return ErrIdempotencyConflict
			}
			// Exact replay: load and return original run
			isReplay = true
			resultRun, err = queryRunDTO(ctx, tx, orgID, storedRunID)
			return err
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("query idempotency: %w", err)
		}

		// 2. Resolve deployment once
		var deploymentID string
		var manifestJSON []byte
		if explicitDeploymentID != nil && *explicitDeploymentID != "" {
			err = tx.QueryRow(ctx, `SELECT id::text, manifest FROM deployments
				WHERE id = $1::uuid AND organization_id = $2::uuid AND environment_id = $3::uuid`,
				*explicitDeploymentID, orgID, envID).Scan(&deploymentID, &manifestJSON)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrDeploymentNotFound
				}
				return fmt.Errorf("query explicit deployment: %w", err)
			}
		} else {
			err = tx.QueryRow(ctx, `SELECT d.id::text, d.manifest
				FROM workflow_channels c
				JOIN deployments d ON d.id = c.active_deployment_id AND d.organization_id = c.organization_id
				WHERE c.environment_id = $1::uuid AND c.organization_id = $2::uuid AND c.workflow_name = $3`,
				envID, orgID, workflowName).Scan(&deploymentID, &manifestJSON)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrNoActiveDeployment
				}
				return fmt.Errorf("query active deployment: %w", err)
			}
		}

		// 3. Parse manifest and validate input against workflow inputSchema
		var manifest deploymentManifest
		if err := json.Unmarshal(manifestJSON, &manifest); err != nil {
			return fmt.Errorf("decode manifest: %w", err)
		}
		var targetWorkflow *workflowManifest
		for i := range manifest.Workflows {
			if manifest.Workflows[i].Name == workflowName {
				targetWorkflow = &manifest.Workflows[i]
				break
			}
		}
		if targetWorkflow == nil {
			return ErrWorkflowNotFound
		}

		if targetWorkflow.InputSchema != nil {
			if err := contracts.ValidatePayload(targetWorkflow.InputSchema, input); err != nil {
				return ErrSchemaViolation
			}
		}

		// 4. Create Run
		var runID string
		var createdAt time.Time
		var deadlineAt *time.Time
		runQuery := `INSERT INTO runs (
			organization_id, environment_id, deployment_id, workflow_name,
			idempotency_key, status, revision, input, last_event_sequence,
			created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, $3::uuid, $4,
			$5, 'QUEUED', 1, $6::jsonb, 1,
			clock_timestamp(), clock_timestamp()
		) RETURNING id::text, created_at, deadline_at`
		if err := tx.QueryRow(ctx, runQuery, orgID, envID, deploymentID, workflowName, idempotencyKey, canonicalInput).
			Scan(&runID, &createdAt, &deadlineAt); err != nil {
			return fmt.Errorf("insert run: %w", err)
		}

		// 5. Create run_steps from graph
		for _, node := range targetWorkflow.Nodes {
			kind := node.Type
			if kind == "" {
				kind = "task"
			}
			initState := "BLOCKED"
			var eligibleAt any = nil
			if len(node.After) == 0 {
				initState = "READY"
				eligibleAt = time.Now()
			}
			stepQuery := `INSERT INTO run_steps (
				organization_id, environment_id, run_id, node_id,
				kind, state, eligible_at, created_at, updated_at
			) VALUES (
				$1::uuid, $2::uuid, $3::uuid, $4,
				$5, $6, $7, clock_timestamp(), clock_timestamp()
			)`
			if _, err := tx.Exec(ctx, stepQuery, orgID, envID, runID, node.ID, kind, initState, eligibleAt); err != nil {
				return fmt.Errorf("insert step %s: %w", node.ID, err)
			}
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
			return fmt.Errorf("insert idempotency: %w", err)
		}

		// 7. Initial run_event
		eventInsert := `INSERT INTO run_events (
			organization_id, run_id, sequence, event_type, payload
		) VALUES (
			$1::uuid, $2::uuid, 1, 'RUN_CREATED',
			jsonb_build_object('runId', $2::uuid, 'workflowName', $3::text, 'deploymentId', $4::uuid)
		)`
		if _, err := tx.Exec(ctx, eventInsert, orgID, runID, workflowName, deploymentID); err != nil {
			return fmt.Errorf("insert run event: %w", err)
		}

		// 8. Outbox hint
		outboxInsert := `INSERT INTO outbox_events (
			organization_id, subject, payload
		) VALUES (
			$1::uuid, 'execution.state_changed',
			jsonb_build_object('runId', $2::uuid, 'eventType', 'RUN_CREATED')
		)`
		if _, err := tx.Exec(ctx, outboxInsert, orgID, runID); err != nil {
			return fmt.Errorf("insert outbox: %w", err)
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
			return fmt.Errorf("marshal audit metadata: %w", err)
		}
		auditInsert := `INSERT INTO audit_events (
			organization_id, actor_id, action, target_type, target_id, correlation_id, reason, metadata
		) VALUES (
			$1::uuid, $2, 'run.create', 'run', $3::uuid, $4, $5, $6::jsonb
		)`
		if _, err := tx.Exec(ctx, auditInsert, orgID, actorID, runID, audit.CorrelationID, audit.Reason, auditMeta); err != nil {
			return fmt.Errorf("insert audit: %w", err)
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
		return nil
	})

	if err != nil {
		return nil, false, err
	}
	return resultRun, isReplay, nil
}

func (s *Service) GetRun(ctx context.Context, orgID, runID string) (*RunSnapshotDTO, error) {
	var snapshot *RunSnapshotDTO
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		run, err := queryRunDTO(ctx, tx, orgID, runID)
		if err != nil {
			return err
		}

		var lastEventSeq int64
		var rawOutput, rawError []byte
		err = tx.QueryRow(ctx, `SELECT last_event_sequence, output, reason_code FROM runs
			WHERE id = $1::uuid AND organization_id = $2::uuid`, runID, orgID).Scan(&lastEventSeq, &rawOutput, &run.ReasonCode)
		if err != nil {
			return err
		}

		var output any
		if len(rawOutput) > 0 && string(rawOutput) != "null" {
			_ = json.Unmarshal(rawOutput, &output)
		}

		var runError any
		if len(rawError) > 0 && string(rawError) != "null" {
			_ = json.Unmarshal(rawError, &runError)
		}

		rows, err := tx.Query(ctx, `SELECT id::text, node_id, state, current_epoch
			FROM run_steps
			WHERE run_id = $1::uuid AND organization_id = $2::uuid
			ORDER BY created_at ASC, id ASC`, runID, orgID)
		if err != nil {
			return fmt.Errorf("query steps: %w", err)
		}
		defer rows.Close()

		steps := make([]RunStepDTO, 0)
		for rows.Next() {
			var st RunStepDTO
			var stateStr string
			if err := rows.Scan(&st.ID, &st.NodeID, &stateStr, &st.CurrentEpoch); err != nil {
				return err
			}
			st.Status = contracts.StepStatus(stateStr)
			steps = append(steps, st)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		snapshot = &RunSnapshotDTO{
			RunDTO:            *run,
			LastEventSequence: lastEventSeq,
			Steps:             steps,
			Output:            output,
			Error:             runError,
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func queryRunDTO(ctx context.Context, tx storage.Tx, orgID, runID string) (*RunDTO, error) {
	var run RunDTO
	var statusStr string
	var createdAt time.Time
	var deadlineAt *time.Time
	query := `SELECT r.id::text, r.organization_id::text, e.project_id::text, r.environment_id::text,
		r.workflow_name, r.deployment_id::text, r.status, r.reason_code, r.revision,
		r.created_at, r.deadline_at
		FROM runs r
		JOIN environments e ON e.id = r.environment_id AND e.organization_id = r.organization_id
		WHERE r.id = $1::uuid AND r.organization_id = $2::uuid`
	err := tx.QueryRow(ctx, query, runID, orgID).Scan(
		&run.ID, &run.OrganizationID, &run.ProjectID, &run.EnvironmentID,
		&run.WorkflowName, &run.DeploymentID, &statusStr, &run.ReasonCode, &run.Revision,
		&createdAt, &deadlineAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrRunNotFound
		}
		return nil, fmt.Errorf("query run: %w", err)
	}
	run.Status = contracts.RunStatus(statusStr)
	run.CreatedAt = createdAt.UTC().Format(time.RFC3339)
	if deadlineAt != nil {
		d := deadlineAt.UTC().Format(time.RFC3339)
		run.DeadlineAt = &d
	}
	return &run, nil
}
