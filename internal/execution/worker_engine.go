package execution

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/worker"
	"github.com/jackc/pgx/v5"
)

const defaultAttemptTimeout = 5 * time.Minute

// WorkerEngine is the PostgreSQL execution authority used by the worker HTTP
// adapter. State transitions, leases, history, and wake-up hints are committed
// together here instead of in the transport layer.
type WorkerEngine struct {
	pool                 *storage.Pool
	hub                  *EventHub
	beforeCompleteCommit func() error
	afterCompleteCommit  func() error
}

func NewWorkerEngine(pool *storage.Pool, hub ...*EventHub) *WorkerEngine {
	var h *EventHub
	if len(hub) > 0 && hub[0] != nil {
		h = hub[0]
	}
	return &WorkerEngine{pool: pool, hub: h}
}

func (e *WorkerEngine) SetHub(hub *EventHub) {
	e.hub = hub
}

// SetBeforeCompleteCommitHookForTest injects a deterministic failure after all
// completion writes have been staged but before the transaction is allowed to
// commit. Production constructors leave it nil.
func (e *WorkerEngine) SetBeforeCompleteCommitHookForTest(hook func() error) {
	e.beforeCompleteCommit = hook
}

// SetAfterCompleteCommitHookForTest injects a caller-visible failure only after
// Complete has committed. Production constructors leave it nil.
func (e *WorkerEngine) SetAfterCompleteCommitHookForTest(hook func() error) {
	e.afterCompleteCommit = hook
}

type workflowNode struct {
	ID    string   `json:"id"`
	Type  string   `json:"type"`
	Task  string   `json:"task"`
	After []string `json:"after"`
	Input any      `json:"input"`
}

type workflowManifest struct {
	Name         string         `json:"name"`
	InputSchema  any            `json:"inputSchema"`
	OutputSchema any            `json:"outputSchema"`
	Nodes        []workflowNode `json:"nodes"`
	Output       any            `json:"output"`
}

type deploymentManifest struct {
	TargetOS           string   `json:"targetOS"`
	TargetArchitecture string   `json:"targetArchitecture"`
	SecretNames        []string `json:"secretNames"`
	Tasks              []struct {
		Name         string `json:"name"`
		Entrypoint   string `json:"entrypoint"`
		Recovery     string `json:"recovery"`
		TimeoutMs    int64  `json:"timeoutMs"`
		InputSchema  any    `json:"inputSchema"`
		OutputSchema any    `json:"outputSchema"`
		Retry        struct {
			MaxAttempts    int   `json:"maxAttempts"`
			InitialDelayMs int64 `json:"initialDelayMs"`
			MaxDelayMs     int64 `json:"maxDelayMs"`
		} `json:"retry"`
	} `json:"tasks"`
	Workflows []workflowManifest `json:"workflows"`
}

func (m deploymentManifest) taskRecoveryPolicy(workflowName, nodeID string) (string, int) {
	taskName := nodeID
	for _, wf := range m.Workflows {
		if wf.Name == workflowName {
			for _, n := range wf.Nodes {
				if n.ID == nodeID && n.Task != "" {
					taskName = n.Task
					break
				}
			}
			break
		}
	}
	for _, task := range m.Tasks {
		if task.Name == taskName {
			policy := task.Recovery
			if policy == "" {
				policy = "safe"
			}
			maxAttempts := task.Retry.MaxAttempts
			if maxAttempts <= 0 {
				maxAttempts = 3
			}
			return policy, maxAttempts
		}
	}
	return "safe", 3
}

func (m deploymentManifest) taskPolicy(workflowName, nodeID string) (string, int64) {
	taskName := nodeID
	for _, workflowDefinition := range m.Workflows {
		if workflowDefinition.Name != workflowName {
			continue
		}
		for _, node := range workflowDefinition.Nodes {
			if node.ID == nodeID && node.Task != "" {
				taskName = node.Task
				break
			}
		}
	}
	for _, task := range m.Tasks {
		if task.Name == taskName {
			timeout := task.TimeoutMs
			if timeout <= 0 {
				timeout = defaultAttemptTimeout.Milliseconds()
			}
			return task.Entrypoint, timeout
		}
	}
	return nodeID, defaultAttemptTimeout.Milliseconds()
}

func (m deploymentManifest) taskSchemas(workflowName, nodeID string) (any, any) {
	taskName := nodeID
	for _, wf := range m.Workflows {
		if wf.Name != workflowName {
			continue
		}
		for _, node := range wf.Nodes {
			if node.ID == nodeID && node.Task != "" {
				taskName = node.Task
				break
			}
		}
	}
	for _, task := range m.Tasks {
		if task.Name == taskName {
			return task.InputSchema, task.OutputSchema
		}
	}
	return nil, nil
}

func stableOperationID(environmentID, runID, nodeID string) string {
	digest := sha256.Sum256([]byte("deadbolt-operation:v1:" + environmentID + ":" + runID + ":" + nodeID))
	return "op_" + hex.EncodeToString(digest[:])
}

func targetArchitecture(os, architecture string) string {
	os = strings.ToLower(strings.TrimSpace(os))
	architecture = strings.ToLower(strings.TrimSpace(architecture))
	if os == "" {
		os = "linux"
	}
	switch architecture {
	case "":
		return ""
	case "amd64", "x64":
		return os + "/amd64"
	case "arm64":
		return os + "/arm64"
	default:
		return architecture
	}
}

type claimMatch struct {
	stepID       string
	runID        string
	nodeID       string
	input        any
	bundle       string
	manifest     []byte
	workflowName string
	runDeadline  *time.Time
}

func (e *WorkerEngine) Claim(ctx context.Context, session *worker.WorkerSessionContext, req *worker.PollRequestDTO) (*worker.PollResponseDTO, error) {
	if req.WorkerID != session.WorkerID || req.SessionID != session.SessionID {
		return nil, worker.ErrUnauthorized
	}
	assignments := make([]worker.AssignmentDTO, 0)
	if req.AvailableSlots <= 0 {
		return &worker.PollResponseDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: req.RequestID, Assignments: assignments}, nil
	}
	var reconciledRuns []string

	err := e.pool.WithTenantTx(ctx, session.OrganizationID, func(ctx context.Context, tx storage.Tx) error {
		var workerStatus string
		var revokedAt *time.Time
		var sessionExpiry, dbNow time.Time
		if err := tx.QueryRow(ctx, `SELECT w.status,ws.revoked_at,ws.expires_at
			FROM workers w JOIN worker_sessions ws ON ws.worker_id=w.id AND ws.organization_id=w.organization_id
			WHERE w.id=$1::uuid AND w.organization_id=$2::uuid AND ws.id=$3::uuid
			FOR UPDATE OF w,ws`, session.WorkerID, session.OrganizationID, session.SessionID).Scan(&workerStatus, &revokedAt, &sessionExpiry); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
			return err
		}
		if revokedAt != nil {
			return worker.ErrSessionRevoked
		}
		if !dbNow.Before(sessionExpiry) {
			return worker.ErrSessionExpired
		}
		if workerStatus == "REVOKED" {
			return worker.ErrWorkerRevoked
		}
		if workerStatus != "ACTIVE" {
			return nil
		}

		// Serialize admission before locking runs and their steps, then derive
		// capacity from live leases while holding that admission row.
		var environmentID string
		var maxConcurrency int
		if err := tx.QueryRow(ctx, `SELECT environment_id::text,max_concurrency FROM environment_admissions
			WHERE environment_id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, session.EnvironmentID, session.OrganizationID).Scan(&environmentID, &maxConcurrency); err != nil {
			return err
		}

		// Reconcile expired leases and unstarted claims before evaluating capacity and ready steps
		if _, runs, err := e.reconcileExpiredLeasesTx(ctx, tx, session.OrganizationID); err != nil {
			return err
		} else {
			reconciledRuns = append(reconciledRuns, runs...)
		}

		var activeLeases int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_leases l
			JOIN run_steps rs ON rs.id=l.step_id AND rs.organization_id=l.organization_id
			WHERE rs.environment_id=$1::uuid AND rs.organization_id=$2::uuid
				AND clock_timestamp() < l.expires_at`, environmentID, session.OrganizationID).Scan(&activeLeases); err != nil {
			return err
		}
		claimLimit := req.AvailableSlots
		if remaining := maxConcurrency - activeLeases; remaining < claimLimit {
			claimLimit = remaining
		}
		if claimLimit <= 0 {
			return nil
		}

		rows, err := tx.Query(ctx, `SELECT rs.id::text, rs.run_id::text, rs.node_id, r.input,
				d.bundle_digest, d.manifest, r.workflow_name, r.deadline_at
			FROM run_steps rs
			JOIN runs r ON r.id=rs.run_id AND r.organization_id=rs.organization_id
			JOIN deployments d ON d.id=r.deployment_id AND d.organization_id=r.organization_id
			JOIN worker_deployments wd ON wd.session_id=$1::uuid AND wd.bundle_digest=d.bundle_digest
			WHERE rs.organization_id=$2::uuid AND rs.environment_id=$3::uuid
				AND rs.state='READY' AND rs.eligible_at <= clock_timestamp()
				AND r.status IN ('QUEUED','RUNNING')
				AND (r.deadline_at IS NULL OR clock_timestamp() < r.deadline_at)
			ORDER BY rs.eligible_at, rs.id
			LIMIT $4
			FOR UPDATE OF r, rs SKIP LOCKED`, session.SessionID, session.OrganizationID, environmentID, claimLimit)
		if err != nil {
			return fmt.Errorf("lock claim candidates: %w", err)
		}

		matches := make([]claimMatch, 0, claimLimit)
		for rows.Next() {
			var match claimMatch
			var inputJSON []byte
			if err := rows.Scan(&match.stepID, &match.runID, &match.nodeID, &inputJSON, &match.bundle, &match.manifest, &match.workflowName, &match.runDeadline); err != nil {
				rows.Close()
				return err
			}
			if len(inputJSON) > 0 {
				if err := json.Unmarshal(inputJSON, &match.input); err != nil {
					rows.Close()
					return fmt.Errorf("decode run input: %w", err)
				}
			}
			matches = append(matches, match)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()

		for _, match := range matches {
			var manifest deploymentManifest
			if err := json.Unmarshal(match.manifest, &manifest); err != nil {
				return fmt.Errorf("decode deployment manifest: %w", err)
			}
			entrypoint, timeoutMs := manifest.taskPolicy(match.workflowName, match.nodeID)
			assignmentInput := match.input
			var targetNode *workflowNode
			for _, wf := range manifest.Workflows {
				if wf.Name != match.workflowName {
					continue
				}
				for i := range wf.Nodes {
					if wf.Nodes[i].ID == match.nodeID {
						targetNode = &wf.Nodes[i]
						break
					}
				}
				break
			}
			if targetNode != nil && targetNode.Input != nil {
				outRows, err := tx.Query(ctx, `SELECT node_id, output FROM run_steps
					WHERE run_id=$1::uuid AND organization_id=$2::uuid AND output IS NOT NULL`,
					match.runID, session.OrganizationID)
				if err != nil {
					return fmt.Errorf("load mapped step outputs: %w", err)
				}
				outputsMap := make(map[string]any)
				for outRows.Next() {
					var nodeID string
					var rawOutput []byte
					if err := outRows.Scan(&nodeID, &rawOutput); err != nil {
						outRows.Close()
						return err
					}
					var output any
					if err := json.Unmarshal(rawOutput, &output); err != nil {
						outRows.Close()
						return fmt.Errorf("decode mapped step output: %w", err)
					}
					outputsMap[nodeID] = output
				}
				if err := outRows.Err(); err != nil {
					outRows.Close()
					return err
				}
				outRows.Close()
				mapped, err := contracts.MapInput(targetNode.Input, match.input, outputsMap)
				if err != nil {
					return fmt.Errorf("map task input: %w", err)
				}
				inputSchema, _ := manifest.taskSchemas(match.workflowName, match.nodeID)
				if inputSchema != nil && contracts.ValidatePayload(inputSchema, mapped) != nil {
					return fmt.Errorf("map task input: %w", ErrSchemaViolation)
				}
				assignmentInput = mapped
			}

			var epoch int64
			var attemptNumber int
			if err := tx.QueryRow(ctx, `UPDATE run_steps
				SET state='RUNNING', wait_reason=NULL, current_epoch=current_epoch+1,
					next_attempt_number=next_attempt_number+1, updated_at=clock_timestamp()
				WHERE id=$1::uuid AND organization_id=$2::uuid AND state='READY'
				RETURNING current_epoch, next_attempt_number-1`, match.stepID, session.OrganizationID).Scan(&epoch, &attemptNumber); err != nil {
				return err
			}

			var attemptID string
			var claimDeadline time.Time
			if err := tx.QueryRow(ctx, `INSERT INTO task_attempts
				(organization_id,step_id,attempt_number,session_id,epoch,status,claim_start_deadline_at,attempt_timeout_ms)
				VALUES ($1::uuid,$2::uuid,$3,$4::uuid,$5,'CLAIMED',clock_timestamp()+INTERVAL '5 seconds',$6)
				RETURNING id::text,claim_start_deadline_at`, session.OrganizationID, match.stepID, attemptNumber, session.SessionID, epoch, timeoutMs).Scan(&attemptID, &claimDeadline); err != nil {
				return fmt.Errorf("insert claimed attempt: %w", err)
			}

			var leaseExpiry time.Time
			if err := tx.QueryRow(ctx, `INSERT INTO task_leases
				(step_id,organization_id,attempt_id,session_id,epoch,expires_at)
				VALUES ($1::uuid,$2::uuid,$3::uuid,$4::uuid,$5,clock_timestamp()+INTERVAL '30 seconds')
				RETURNING expires_at`, match.stepID, session.OrganizationID, attemptID, session.SessionID, epoch).Scan(&leaseExpiry); err != nil {
				return fmt.Errorf("insert attempt lease: %w", err)
			}
			if err := appendRunEvent(ctx, tx, session.OrganizationID, match.runID, "TASK_CLAIMED", map[string]any{
				"attemptId": attemptID, "stepId": match.stepID, "epoch": epoch, "sessionId": session.SessionID,
			}); err != nil {
				return err
			}

			runDeadline := ""
			if match.runDeadline != nil {
				runDeadline = match.runDeadline.UTC().Format(time.RFC3339Nano)
			}

			assignments = append(assignments, worker.AssignmentDTO{
				RunID: match.runID, StepID: match.stepID, AttemptID: attemptID, OwnershipEpoch: epoch,
				TaskEntrypoint: entrypoint, Input: assignmentInput, DeploymentDigest: match.bundle, BundleDigest: match.bundle,
				OperationID: stableOperationID(session.EnvironmentID, match.runID, match.nodeID),
				LeaseTTLMs:  worker.DefaultLeaseTTL.Milliseconds(), LeaseExpiresAt: leaseExpiry.UTC().Format(time.RFC3339Nano),
				ClaimStartDeadlineAt: claimDeadline.UTC().Format(time.RFC3339Nano), AttemptTimeoutMs: timeoutMs,
				RunDeadlineAt:      runDeadline,
				TraceContext:       worker.TraceContextDTO{Traceparent: "00-00000000000000000000000000000000-0000000000000000-01"},
				TargetArchitecture: targetArchitecture(manifest.TargetOS, manifest.TargetArchitecture), SecretNames: manifest.SecretNames,
				TargetOS: manifest.TargetOS,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if e.hub != nil {
		published := make(map[string]struct{})
		for _, a := range assignments {
			published[a.RunID] = struct{}{}
			e.hub.Publish(a.RunID)
		}
		for _, rID := range reconciledRuns {
			if _, ok := published[rID]; !ok {
				published[rID] = struct{}{}
				e.hub.Publish(rID)
			}
		}
	}
	return &worker.PollResponseDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: req.RequestID, Assignments: assignments}, nil
}

func (e *WorkerEngine) Start(ctx context.Context, session *worker.WorkerSessionContext, req *worker.StartRequestDTO) (*worker.StartResponseDTO, error) {
	if req.WorkerID != session.WorkerID || req.SessionID != session.SessionID {
		return nil, worker.ErrUnauthorized
	}
	var deadline time.Time
	var runID string
	err := e.pool.WithTenantTx(ctx, session.OrganizationID, func(ctx context.Context, tx storage.Tx) error {
		var status, ownerSession string
		var epoch int64
		var claimDeadline, storedDeadline, runDeadline *time.Time
		var timeoutMs int64
		err := tx.QueryRow(ctx, `SELECT r.id::text,a.status,a.epoch,a.session_id::text,
				a.claim_start_deadline_at,a.deadline_at,a.attempt_timeout_ms,r.deadline_at
			FROM runs r JOIN run_steps rs ON rs.run_id=r.id AND rs.organization_id=r.organization_id
			JOIN task_attempts a ON a.step_id=rs.id AND a.organization_id=rs.organization_id
			WHERE a.id=$1::uuid AND a.organization_id=$2::uuid
			FOR UPDATE OF r,rs,a`, req.AttemptID, session.OrganizationID).Scan(
			&runID, &status, &epoch, &ownerSession, &claimDeadline, &storedDeadline, &timeoutMs, &runDeadline)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return worker.ErrAttemptNotFound
			}
			return err
		}
		var leaseExpiry time.Time
		var leaseEpoch int64
		var leaseSession string
		if err := tx.QueryRow(ctx, `SELECT epoch,session_id::text,expires_at FROM task_leases
			WHERE attempt_id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, req.AttemptID, session.OrganizationID).Scan(
			&leaseEpoch, &leaseSession, &leaseExpiry); err != nil {
			return worker.ErrStaleOwnership
		}
		var dbNow time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
			return err
		}
		if epoch != req.OwnershipEpoch || ownerSession != session.SessionID || leaseEpoch != req.OwnershipEpoch || leaseSession != session.SessionID || !dbNow.Before(leaseExpiry) {
			return worker.ErrStaleOwnership
		}
		if status == "RUNNING" {
			if storedDeadline == nil || !dbNow.Before(*storedDeadline) {
				return worker.ErrStaleOwnership
			}
			deadline = *storedDeadline
			return nil
		}
		if status != "CLAIMED" {
			return worker.ErrStaleOwnership
		}
		if claimDeadline == nil || !dbNow.Before(*claimDeadline) || (runDeadline != nil && !dbNow.Before(*runDeadline)) {
			return worker.ErrStartDeadlineExceeded
		}

		if err := tx.QueryRow(ctx, `UPDATE task_attempts a SET status='RUNNING',started_at=clock_timestamp(),
				deadline_at=LEAST(clock_timestamp()+a.attempt_timeout_ms*INTERVAL '1 millisecond',
					COALESCE(r.deadline_at,'infinity'::timestamptz))
			FROM run_steps rs JOIN runs r ON r.id=rs.run_id AND r.organization_id=rs.organization_id
			WHERE a.id=$1::uuid AND a.organization_id=$2::uuid AND rs.id=a.step_id
			RETURNING a.deadline_at`, req.AttemptID, session.OrganizationID).Scan(&deadline); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE runs SET status='RUNNING',reason_code=NULL,updated_at=clock_timestamp()
			WHERE id=$1::uuid AND organization_id=$2::uuid AND status='QUEUED'`, runID, session.OrganizationID); err != nil {
			return err
		}
		return appendRunEvent(ctx, tx, session.OrganizationID, runID, "TASK_STARTED", map[string]any{
			"attemptId": req.AttemptID, "epoch": req.OwnershipEpoch, "deadlineAt": deadline.UTC().Format(time.RFC3339Nano), "timeoutMs": timeoutMs,
		})
	})
	if err != nil {
		return nil, err
	}
	if e.hub != nil {
		e.hub.Publish(runID)
	}
	return &worker.StartResponseDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: req.RequestID,
		AttemptID: req.AttemptID, OwnershipEpoch: req.OwnershipEpoch, Accepted: true,
		AttemptDeadlineAt: deadline.UTC().Format(time.RFC3339Nano), LeaseTTLMs: worker.DefaultLeaseTTL.Milliseconds()}, nil
}

func stop(item worker.HeartbeatAttemptDTO, reason string) worker.StopCommandDTO {
	return worker.StopCommandDTO{AttemptID: item.AttemptID, OwnershipEpoch: item.OwnershipEpoch, Reason: reason, GraceTimeoutMs: 10000}
}

func (e *WorkerEngine) Heartbeat(ctx context.Context, session *worker.WorkerSessionContext, req *worker.HeartbeatRequestDTO) (*worker.HeartbeatResponseDTO, error) {
	if req.WorkerID != session.WorkerID || req.SessionID != session.SessionID {
		return nil, worker.ErrUnauthorized
	}
	renewals := make([]worker.LeaseRenewalDTO, 0, len(req.Attempts))
	stops := make([]worker.StopCommandDTO, 0)
	err := e.pool.WithTenantTx(ctx, session.OrganizationID, func(ctx context.Context, tx storage.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE workers SET last_seen_at=clock_timestamp()
			WHERE id=$1::uuid AND organization_id=$2::uuid`, session.WorkerID, session.OrganizationID); err != nil {
			return err
		}
		for _, item := range req.Attempts {
			var status, ownerSession string
			var epoch int64
			var leaseExpiry time.Time
			var attemptDeadline, runDeadline *time.Time
			err := tx.QueryRow(ctx, `SELECT a.status,a.epoch,a.session_id::text,l.expires_at,a.deadline_at,r.deadline_at
				FROM runs r JOIN run_steps rs ON rs.run_id=r.id AND rs.organization_id=r.organization_id
				JOIN task_attempts a ON a.step_id=rs.id AND a.organization_id=rs.organization_id
				JOIN task_leases l ON l.attempt_id=a.id AND l.organization_id=a.organization_id
				WHERE a.id=$1::uuid AND a.organization_id=$2::uuid
				FOR UPDATE OF r,rs,a,l`, item.AttemptID, session.OrganizationID).Scan(
				&status, &epoch, &ownerSession, &leaseExpiry, &attemptDeadline, &runDeadline)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					stops = append(stops, stop(item, "LEASE_NOT_FOUND"))
					continue
				}
				return err
			}
			var dbNow time.Time
			if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
				return err
			}
			if epoch != item.OwnershipEpoch || ownerSession != session.SessionID {
				stops = append(stops, stop(item, "STALE_OWNERSHIP"))
				continue
			}
			if status != "RUNNING" || attemptDeadline == nil {
				stops = append(stops, stop(item, "ATTEMPT_NOT_RUNNING"))
				continue
			}
			if !dbNow.Before(leaseExpiry) || !dbNow.Before(*attemptDeadline) || (runDeadline != nil && !dbNow.Before(*runDeadline)) {
				stops = append(stops, stop(item, "DEADLINE_EXPIRED"))
				continue
			}

			var stopReason string
			err = tx.QueryRow(ctx, `SELECT reason FROM stop_commands
				WHERE attempt_id=$1::uuid AND organization_id=$2::uuid AND acked_at IS NULL LIMIT 1`, item.AttemptID, session.OrganizationID).Scan(&stopReason)
			if err == nil {
				stops = append(stops, stop(item, stopReason))
				continue
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				return err
			}

			var newExpiry time.Time
			if err := tx.QueryRow(ctx, `UPDATE task_leases l SET expires_at=LEAST(
				clock_timestamp()+INTERVAL '30 seconds',a.deadline_at,COALESCE(r.deadline_at,'infinity'::timestamptz))
				FROM task_attempts a JOIN run_steps rs ON rs.id=a.step_id AND rs.organization_id=a.organization_id
				JOIN runs r ON r.id=rs.run_id AND r.organization_id=rs.organization_id
				WHERE l.attempt_id=a.id AND l.attempt_id=$1::uuid AND l.organization_id=$2::uuid
				RETURNING l.expires_at`, item.AttemptID, session.OrganizationID).Scan(&newExpiry); err != nil {
				return err
			}
			renewals = append(renewals, worker.LeaseRenewalDTO{AttemptID: item.AttemptID, OwnershipEpoch: item.OwnershipEpoch,
				LeaseTTLMs: worker.DefaultLeaseTTL.Milliseconds(), LeaseExpiresAt: newExpiry.UTC().Format(time.RFC3339Nano)})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &worker.HeartbeatResponseDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: req.RequestID, Renewals: renewals, Stops: stops}, nil
}

func validOutcome(outcome string) bool {
	switch outcome {
	case "SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED":
		return true
	default:
		return false
	}
}

func terminalAttempt(status string) bool {
	switch status {
	case "SUCCEEDED", "FAILED", "TIMED_OUT", "LOST", "CANCELLED":
		return true
	default:
		return false
	}
}

func (e *WorkerEngine) Complete(ctx context.Context, session *worker.WorkerSessionContext, req *worker.CompleteRequestDTO) (*worker.CompleteResponseDTO, error) {
	if req.WorkerID != session.WorkerID || req.SessionID != session.SessionID {
		return nil, worker.ErrUnauthorized
	}
	if !validOutcome(req.Outcome) {
		return nil, worker.ErrInvalidOutcome
	}
	if req.Output != nil {
		canonicalOutput, err := contracts.CanonicalizeGeneric(req.Output)
		if err != nil || len(canonicalOutput) > worker.MaxInlinePayloadBytes {
			return nil, worker.ErrPayloadTooLarge
		}
	}
	computedDigest, err := worker.CanonicalCompletionDigest(req)
	if err != nil || req.ResultDigest != computedDigest {
		return nil, worker.ErrResultConflict
	}
	var runID string
	err = e.pool.WithTenantTx(ctx, session.OrganizationID, func(ctx context.Context, tx storage.Tx) error {
		var stepID, nodeID, status, ownerSession, workflowName string
		var manifestBytes []byte
		var epoch int64
		var storedDigest *string
		var attemptDeadline *time.Time
		err := tx.QueryRow(ctx, `SELECT r.id::text,rs.id::text,rs.node_id,a.status,a.epoch,a.session_id::text,a.outcome_digest,a.deadline_at,d.manifest,r.workflow_name
			FROM runs r JOIN run_steps rs ON rs.run_id=r.id AND rs.organization_id=r.organization_id
			JOIN task_attempts a ON a.step_id=rs.id AND a.organization_id=rs.organization_id
			JOIN deployments d ON d.id=r.deployment_id AND d.organization_id=r.organization_id
			WHERE a.id=$1::uuid AND a.organization_id=$2::uuid
			FOR UPDATE OF r,rs,a`, req.AttemptID, session.OrganizationID).Scan(
			&runID, &stepID, &nodeID, &status, &epoch, &ownerSession, &storedDigest, &attemptDeadline, &manifestBytes, &workflowName)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return worker.ErrAttemptNotFound
			}
			return err
		}
		// A duplicate ACK is safe only for the same authenticated ownership.
		if epoch != req.OwnershipEpoch || ownerSession != session.SessionID {
			return worker.ErrStaleOwnership
		}
		if terminalAttempt(status) {
			// Stored digest names the submitted result, while status names the
			// engine decision after schema validation. A deterministic validation
			// failure must still ACK an identical authorized delivery.
			if storedDigest != nil && *storedDigest == req.ResultDigest {
				return nil
			}
			return worker.ErrResultConflict
		}
		if status != "RUNNING" || attemptDeadline == nil {
			return worker.ErrStaleOwnership
		}

		var leaseEpoch int64
		var leaseSession string
		var leaseExpiry time.Time
		if err := tx.QueryRow(ctx, `SELECT epoch,session_id::text,expires_at FROM task_leases
			WHERE attempt_id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, req.AttemptID, session.OrganizationID).Scan(
			&leaseEpoch, &leaseSession, &leaseExpiry); err != nil {
			return worker.ErrStaleOwnership
		}
		var dbNow time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
			return err
		}
		if leaseEpoch != req.OwnershipEpoch || leaseSession != session.SessionID || !dbNow.Before(leaseExpiry) || !dbNow.Before(*attemptDeadline) {
			return worker.ErrStaleOwnership
		}
		// Validate a successful worker result before it can change durable state.
		// Contract/schema failures are deterministic terminal failures, never retryable
		// successes with a bad payload.
		if req.Outcome == "SUCCEEDED" {
			var manifest deploymentManifest
			if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
				return fmt.Errorf("decode manifest: %w", err)
			}
			_, outputSchema := manifest.taskSchemas(workflowName, nodeID)
			if outputSchema != nil && contracts.ValidatePayload(outputSchema, req.Output) != nil {
				req.Outcome = "FAILED"
				req.Output = nil
				req.Error = &worker.TaskErrorDTO{Code: "OUTPUT_SCHEMA_VIOLATION", Message: "Task output does not conform to output schema", Retryable: false, EffectStatus: "NOT_APPLIED"}
			}
		}

		errorJSON := "null"
		if req.Error != nil {
			encoded, err := json.Marshal(req.Error)
			if err != nil {
				return err
			}
			errorJSON = string(encoded)
		}
		if _, err := tx.Exec(ctx, `UPDATE task_attempts SET status=$1,outcome_digest=$2,error=$3::jsonb,
			completed_at=clock_timestamp() WHERE id=$4::uuid AND organization_id=$5::uuid`,
			req.Outcome, req.ResultDigest, errorJSON, req.AttemptID, session.OrganizationID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM task_leases WHERE attempt_id=$1::uuid AND organization_id=$2::uuid`, req.AttemptID, session.OrganizationID); err != nil {
			return err
		}
		stepState := "FAILED"
		if req.Outcome == "SUCCEEDED" {
			stepState = "SUCCEEDED"
		} else if req.Outcome == "CANCELLED" {
			stepState = "CANCELLED"
		}
		outputJSON := "null"
		if req.Output != nil {
			encoded, err := json.Marshal(req.Output)
			if err != nil {
				return err
			}
			outputJSON = string(encoded)
		}
		if _, err := tx.Exec(ctx, `UPDATE run_steps SET state=$1,output=$2::jsonb,wait_reason=NULL,updated_at=clock_timestamp()
			WHERE id=$3::uuid AND organization_id=$4::uuid`, stepState, outputJSON, stepID, session.OrganizationID); err != nil {
			return err
		}
		if err := appendRunEvent(ctx, tx, session.OrganizationID, runID, "TASK_COMPLETED", map[string]any{
			"attemptId": req.AttemptID, "stepId": stepID, "epoch": req.OwnershipEpoch, "outcome": req.Outcome, "resultDigest": req.ResultDigest,
		}); err != nil {
			return err
		}

		if req.Outcome == "SUCCEEDED" {
			var manifestBytes []byte
			var workflowName string
			var rawRunInput []byte
			if err := tx.QueryRow(ctx, `SELECT d.manifest, r.workflow_name, r.input
				FROM runs r JOIN deployments d ON d.id=r.deployment_id AND d.organization_id=r.organization_id
				WHERE r.id=$1::uuid AND r.organization_id=$2::uuid`, runID, session.OrganizationID).Scan(&manifestBytes, &workflowName, &rawRunInput); err != nil {
				return err
			}
			var manifest deploymentManifest
			if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
				return fmt.Errorf("decode manifest: %w", err)
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

			type stepData struct {
				id     string
				nodeID string
				state  string
				output any
			}
			stepRows, err := tx.Query(ctx, `SELECT id::text, node_id, state, output
				FROM run_steps WHERE run_id=$1::uuid AND organization_id=$2::uuid
				FOR UPDATE`, runID, session.OrganizationID)
			if err != nil {
				return err
			}
			stepsByNode := make(map[string]*stepData)
			outputsMap := make(map[string]any)
			for stepRows.Next() {
				var s stepData
				var rawOut []byte
				if err := stepRows.Scan(&s.id, &s.nodeID, &s.state, &rawOut); err != nil {
					stepRows.Close()
					return err
				}
				if len(rawOut) > 0 && string(rawOut) != "null" {
					_ = json.Unmarshal(rawOut, &s.output)
					outputsMap[s.nodeID] = s.output
				}
				stepsByNode[s.nodeID] = &s
			}
			stepRows.Close()

			if targetWorkflow != nil {
				// Advance BLOCKED steps whose dependencies in 'after' are all SUCCEEDED
				for _, node := range targetWorkflow.Nodes {
					st, ok := stepsByNode[node.ID]
					if !ok || st.state != "BLOCKED" {
						continue
					}
					allDepsMet := true
					for _, depID := range node.After {
						depStep, depExists := stepsByNode[depID]
						if !depExists || (depStep.state != "SUCCEEDED" && depStep.state != "SKIPPED") {
							allDepsMet = false
							break
						}
					}
					if allDepsMet {
						// Evaluate input mapping if present
						if node.Input != nil {
							mapped, mapErr := contracts.MapInput(node.Input, runInput, outputsMap)
							inputSchema, _ := manifest.taskSchemas(workflowName, node.ID)
							if mapErr != nil || (inputSchema != nil && contracts.ValidatePayload(inputSchema, mapped) != nil) {
								// Non-retryable mapping error per Blueprint §10.4 & §14.2
								if _, uErr := tx.Exec(ctx, `UPDATE run_steps SET state='FAILED',wait_reason='INPUT_MAPPING_ERROR',updated_at=clock_timestamp()
									WHERE id=$1::uuid AND organization_id=$2::uuid`, st.id, session.OrganizationID); uErr != nil {
									return uErr
								}
								if _, uErr := tx.Exec(ctx, `UPDATE runs SET status='FAILED',reason_code='INPUT_MAPPING_ERROR',updated_at=clock_timestamp()
									WHERE id=$1::uuid AND organization_id=$2::uuid`, runID, session.OrganizationID); uErr != nil {
									return uErr
								}
								if err := appendRunEvent(ctx, tx, session.OrganizationID, runID, "RUN_FAILED", map[string]any{
									"reason": "INPUT_MAPPING_ERROR", "nodeId": node.ID,
								}); err != nil {
									return err
								}
								return nil
							}
						}
						// Unblock to READY
						if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='READY',wait_reason=NULL,eligible_at=clock_timestamp(),updated_at=clock_timestamp()
							WHERE id=$1::uuid AND organization_id=$2::uuid`, st.id, session.OrganizationID); err != nil {
							return err
						}
						st.state = "READY"
						if err := appendRunEvent(ctx, tx, session.OrganizationID, runID, "STEP_READY", map[string]any{
							"stepId": st.id, "nodeId": node.ID,
						}); err != nil {
							return err
						}
					}
				}

				// Check if all nodes are terminal
				allTerminal := true
				allSucceeded := true
				for _, node := range targetWorkflow.Nodes {
					st, ok := stepsByNode[node.ID]
					if !ok || (st.state != "SUCCEEDED" && st.state != "SKIPPED" && st.state != "FAILED" && st.state != "CANCELLED") {
						allTerminal = false
						break
					}
					if st.state != "SUCCEEDED" && st.state != "SKIPPED" {
						allSucceeded = false
					}
				}

				if allTerminal {
					if allSucceeded {
						var finalOutput any = req.Output
						if targetWorkflow.Output != nil {
							mappedOut, outErr := contracts.MapInput(targetWorkflow.Output, runInput, outputsMap)
							if outErr != nil {
								// Output mapping error -> fail run
								if _, uErr := tx.Exec(ctx, `UPDATE runs SET status='FAILED',reason_code='OUTPUT_MAPPING_ERROR',updated_at=clock_timestamp()
									WHERE id=$1::uuid AND organization_id=$2::uuid`, runID, session.OrganizationID); uErr != nil {
									return uErr
								}
								return appendRunEvent(ctx, tx, session.OrganizationID, runID, "RUN_FAILED", map[string]any{
									"reason": "OUTPUT_MAPPING_ERROR",
								})
							}
							finalOutput = mappedOut
						}
						if targetWorkflow.OutputSchema != nil && contracts.ValidatePayload(targetWorkflow.OutputSchema, finalOutput) != nil {
							if _, err := tx.Exec(ctx, `UPDATE runs SET status='FAILED',reason_code='OUTPUT_SCHEMA_VIOLATION',updated_at=clock_timestamp()
								WHERE id=$1::uuid AND organization_id=$2::uuid`, runID, session.OrganizationID); err != nil {
								return err
							}
							return appendRunEvent(ctx, tx, session.OrganizationID, runID, "RUN_FAILED", map[string]any{"reason": "OUTPUT_SCHEMA_VIOLATION"})
						}
						finalJSON, err := contracts.CanonicalizeGeneric(finalOutput)
						if err != nil || len(finalJSON) > worker.MaxInlinePayloadBytes {
							return worker.ErrPayloadTooLarge
						}
						if _, err := tx.Exec(ctx, `UPDATE runs SET status='SUCCEEDED',output=$1::jsonb,reason_code=NULL,updated_at=clock_timestamp()
							WHERE id=$2::uuid AND organization_id=$3::uuid`, string(finalJSON), runID, session.OrganizationID); err != nil {
							return err
						}
						if err := appendRunEvent(ctx, tx, session.OrganizationID, runID, "RUN_COMPLETED", map[string]any{
							"status": "SUCCEEDED",
						}); err != nil {
							return err
						}
					} else {
						if _, err := tx.Exec(ctx, `UPDATE runs SET status='FAILED',reason_code='STEP_FAILED',updated_at=clock_timestamp()
							WHERE id=$1::uuid AND organization_id=$2::uuid`, runID, session.OrganizationID); err != nil {
							return err
						}
						if err := appendRunEvent(ctx, tx, session.OrganizationID, runID, "RUN_FAILED", map[string]any{
							"status": "FAILED",
						}); err != nil {
							return err
						}
					}
				}
			}
		} else {
			// Step failed or cancelled: fail run, cancel remaining nonterminal steps
			errorCode := "TASK_FAILED"
			if req.Error != nil && req.Error.Code != "" {
				errorCode = req.Error.Code
			}
			if _, err := tx.Exec(ctx, `UPDATE runs SET status='FAILED',reason_code=$1,updated_at=clock_timestamp()
				WHERE id=$2::uuid AND organization_id=$3::uuid`, errorCode, runID, session.OrganizationID); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='CANCELLED',updated_at=clock_timestamp()
				WHERE run_id=$1::uuid AND organization_id=$2::uuid AND state IN ('BLOCKED','READY','WAITING')`,
				runID, session.OrganizationID); err != nil {
				return err
			}
			if err := appendRunEvent(ctx, tx, session.OrganizationID, runID, "RUN_FAILED", map[string]any{
				"status": "FAILED", "reason": errorCode, "stepId": stepID,
			}); err != nil {
				return err
			}
		}
		if e.beforeCompleteCommit != nil {
			return e.beforeCompleteCommit()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if e.hub != nil {
		e.hub.Publish(runID)
	}
	if e.afterCompleteCommit != nil {
		if err := e.afterCompleteCommit(); err != nil {
			return nil, err
		}
	}
	return &worker.CompleteResponseDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: req.RequestID,
		AttemptID: req.AttemptID, OwnershipEpoch: req.OwnershipEpoch, Accepted: true, ResultDigest: req.ResultDigest}, nil
}

func (e *WorkerEngine) StopAck(ctx context.Context, session *worker.WorkerSessionContext, req *worker.StopAckRequestDTO) (*worker.AckResponseDTO, error) {
	if req.WorkerID != session.WorkerID || req.SessionID != session.SessionID {
		return nil, worker.ErrUnauthorized
	}
	err := e.pool.WithTenantTx(ctx, session.OrganizationID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE stop_commands SET acked_at=COALESCE(acked_at,clock_timestamp()),
			termination_confirmed_at=CASE WHEN $1 THEN clock_timestamp() ELSE termination_confirmed_at END
			WHERE attempt_id=$2::uuid AND organization_id=$3::uuid`,
			req.ProcessStopped, req.AttemptID, session.OrganizationID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &worker.AckResponseDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: req.RequestID, Accepted: true}, nil
}

type lostAttempt struct {
	attemptID string
	stepID    string
	runID     string
	epoch     int64
}

// FenceWorkerSessions records ownership loss and emits a durable recovery
// handoff before the caller revokes old sessions. It intentionally accepts the
// caller transaction so reconnect/revoke cannot commit half of the transition.
func (e *WorkerEngine) FenceWorkerSessions(ctx context.Context, tx storage.Tx, organizationID, workerID, reason string) error {
	var lockedWorkerID string
	if err := tx.QueryRow(ctx, `SELECT id::text FROM workers
		WHERE id=$1::uuid AND organization_id=$2::uuid FOR UPDATE`, workerID, organizationID).Scan(&lockedWorkerID); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT a.id::text,rs.id::text,r.id::text,a.epoch
		FROM runs r JOIN run_steps rs ON rs.run_id=r.id AND rs.organization_id=r.organization_id
		JOIN task_attempts a ON a.step_id=rs.id AND a.organization_id=rs.organization_id
		JOIN worker_sessions ws ON ws.id=a.session_id AND ws.organization_id=a.organization_id
		WHERE ws.worker_id=$1::uuid AND ws.organization_id=$2::uuid
			AND ws.revoked_at IS NULL AND a.status IN ('CLAIMED','RUNNING')
		ORDER BY r.id,rs.id,a.id FOR UPDATE OF r,rs,a`, workerID, organizationID)
	if err != nil {
		return err
	}
	lost := make([]lostAttempt, 0)
	for rows.Next() {
		var item lostAttempt
		if err := rows.Scan(&item.attemptID, &item.stepID, &item.runID, &item.epoch); err != nil {
			rows.Close()
			return err
		}
		lost = append(lost, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	for _, item := range lost {
		if _, err := tx.Exec(ctx, `UPDATE task_attempts SET status='LOST',completed_at=clock_timestamp(),
			error=jsonb_build_object('code','OWNERSHIP_LOST','reason',$1::text)
			WHERE id=$2::uuid AND organization_id=$3::uuid AND status IN ('CLAIMED','RUNNING')`, reason, item.attemptID, organizationID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM task_leases WHERE attempt_id=$1::uuid AND organization_id=$2::uuid`, item.attemptID, organizationID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='WAITING',wait_reason='RECOVERY_HANDOFF',updated_at=clock_timestamp()
			WHERE id=$1::uuid AND organization_id=$2::uuid AND current_epoch=$3 AND state='RUNNING'`, item.stepID, organizationID, item.epoch); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE runs SET status='WAITING',reason_code='RECOVERY_HANDOFF',updated_at=clock_timestamp()
			WHERE id=$1::uuid AND organization_id=$2::uuid AND status IN ('QUEUED','RUNNING')`, item.runID, organizationID); err != nil {
			return err
		}
		if err := appendRunEvent(ctx, tx, organizationID, item.runID, "TASK_LOST", map[string]any{
			"attemptId": item.attemptID, "stepId": item.stepID, "epoch": item.epoch, "reason": reason, "recovery": "HANDOFF_REQUIRED",
		}); err != nil {
			return err
		}
	}
	return nil
}

type expiredAttemptCandidate struct {
	runID                string
	stepID               string
	attemptID            string
	attemptStatus        string
	attemptEpoch         int64
	startedAt            *time.Time
	claimStartDeadlineAt *time.Time
	attemptDeadlineAt    *time.Time
	leaseExpiresAt       *time.Time
	nextAttemptNumber    int
	currentEpoch         int64
	nodeID               string
	workflowName         string
	manifestBytes        []byte
	runStatus            string
	runDeadlineAt        *time.Time
}

// ReconcileExpiredLeases checks for expired leases and unstarted claims across the
// organization, closes them authoritatively as LOST/TIMED_OUT, and restores schedulability
// per Blueprint §13.3.
func (e *WorkerEngine) ReconcileExpiredLeases(ctx context.Context, organizationID string) (int, error) {
	var totalReclaimed int
	var affectedRuns []string
	err := e.pool.WithTenantTx(ctx, organizationID, func(ctx context.Context, tx storage.Tx) error {
		reclaimed, runs, err := e.reconcileExpiredLeasesTx(ctx, tx, organizationID)
		if err != nil {
			return err
		}
		totalReclaimed = reclaimed
		affectedRuns = runs
		return nil
	})
	if err != nil {
		return 0, err
	}
	if e.hub != nil {
		for _, runID := range affectedRuns {
			e.hub.Publish(runID)
		}
	}
	return totalReclaimed, nil
}

func (e *WorkerEngine) reconcileExpiredLeasesTx(ctx context.Context, tx storage.Tx, organizationID string) (int, []string, error) {
	rows, err := tx.Query(ctx, `SELECT
			r.id::text, rs.id::text, a.id::text, a.status, a.epoch, a.started_at,
			a.claim_start_deadline_at, a.deadline_at, l.expires_at,
			rs.next_attempt_number, rs.current_epoch, rs.node_id,
			r.workflow_name, d.manifest, r.status, r.deadline_at
		FROM runs r
		JOIN run_steps rs ON rs.run_id=r.id AND rs.organization_id=r.organization_id
		JOIN task_attempts a ON a.step_id=rs.id AND a.organization_id=rs.organization_id
		LEFT JOIN task_leases l ON l.attempt_id=a.id AND l.organization_id=a.organization_id
		JOIN deployments d ON d.id=r.deployment_id AND d.organization_id=r.organization_id
		WHERE r.organization_id=$1::uuid
			AND (r.status IN ('QUEUED','RUNNING') OR (r.status='WAITING' AND r.reason_code='RECOVERY_HANDOFF'))
			AND a.status IN ('CLAIMED','RUNNING')
			AND (
				(a.status='CLAIMED' AND (a.claim_start_deadline_at <= clock_timestamp() OR (l.expires_at IS NOT NULL AND l.expires_at <= clock_timestamp()) OR l.step_id IS NULL))
				OR (a.status='RUNNING' AND l.expires_at IS NOT NULL AND l.expires_at <= clock_timestamp())
				OR (a.status='RUNNING' AND a.deadline_at IS NOT NULL AND a.deadline_at <= clock_timestamp())
				OR (r.deadline_at IS NOT NULL AND r.deadline_at <= clock_timestamp())
			)
		ORDER BY r.id, rs.id, a.id
		LIMIT 50
		FOR UPDATE OF r, rs, a SKIP LOCKED`, organizationID)
	if err != nil {
		return 0, nil, fmt.Errorf("query expired attempts: %w", err)
	}

	candidates := make([]expiredAttemptCandidate, 0)
	for rows.Next() {
		var c expiredAttemptCandidate
		if err := rows.Scan(
			&c.runID, &c.stepID, &c.attemptID, &c.attemptStatus, &c.attemptEpoch, &c.startedAt,
			&c.claimStartDeadlineAt, &c.attemptDeadlineAt, &c.leaseExpiresAt,
			&c.nextAttemptNumber, &c.currentEpoch, &c.nodeID,
			&c.workflowName, &c.manifestBytes, &c.runStatus, &c.runDeadlineAt,
		); err != nil {
			rows.Close()
			return 0, nil, fmt.Errorf("scan expired attempt candidate: %w", err)
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, nil, err
	}
	rows.Close()

	type handoffCandidate struct {
		runID             string
		stepID            string
		nextAttemptNumber int
		currentEpoch      int64
		nodeID            string
		workflowName      string
		manifestBytes     []byte
		runStatus         string
		runDeadlineAt     *time.Time
		hasStartedAttempt bool
	}

	// Also sweep steps in WAITING with wait_reason='RECOVERY_HANDOFF'
	handoffRows, err := tx.Query(ctx, `SELECT
			r.id::text, rs.id::text, rs.next_attempt_number, rs.current_epoch, rs.node_id,
			r.workflow_name, d.manifest, r.status, r.deadline_at,
			EXISTS (SELECT 1 FROM task_attempts ta JOIN run_steps rst ON rst.id=ta.step_id WHERE rst.run_id=r.id AND ta.started_at IS NOT NULL)
		FROM runs r
		JOIN run_steps rs ON rs.run_id=r.id AND rs.organization_id=r.organization_id
		JOIN deployments d ON d.id=r.deployment_id AND d.organization_id=r.organization_id
		WHERE r.organization_id=$1::uuid
			AND rs.state='WAITING' AND rs.wait_reason='RECOVERY_HANDOFF'
			AND (r.status IN ('QUEUED','RUNNING') OR (r.status='WAITING' AND r.reason_code='RECOVERY_HANDOFF'))
		ORDER BY r.id, rs.id
		LIMIT 50
		FOR UPDATE OF r, rs SKIP LOCKED`, organizationID)
	if err != nil {
		return 0, nil, fmt.Errorf("query recovery handoff steps: %w", err)
	}

	handoffs := make([]handoffCandidate, 0)
	for handoffRows.Next() {
		var h handoffCandidate
		if err := handoffRows.Scan(
			&h.runID, &h.stepID, &h.nextAttemptNumber, &h.currentEpoch, &h.nodeID,
			&h.workflowName, &h.manifestBytes, &h.runStatus, &h.runDeadlineAt,
			&h.hasStartedAttempt,
		); err != nil {
			handoffRows.Close()
			return 0, nil, fmt.Errorf("scan recovery handoff candidate: %w", err)
		}
		handoffs = append(handoffs, h)
	}
	if err := handoffRows.Err(); err != nil {
		handoffRows.Close()
		return 0, nil, err
	}
	handoffRows.Close()

	if len(candidates) == 0 && len(handoffs) == 0 {
		return 0, nil, nil
	}

	var dbNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return 0, nil, err
	}

	reclaimedRunsMap := make(map[string]struct{})
	reclaimedCount := 0

	for _, c := range candidates {
		reclaimedRunsMap[c.runID] = struct{}{}
		reclaimedCount++

		var manifest deploymentManifest
		if len(c.manifestBytes) > 0 {
			_ = json.Unmarshal(c.manifestBytes, &manifest)
		}
		policy, maxAttempts := manifest.taskRecoveryPolicy(c.workflowName, c.nodeID)

		// 1. Overall run deadline exceeded
		if c.runDeadlineAt != nil && !dbNow.Before(*c.runDeadlineAt) {
			if _, err := tx.Exec(ctx, `UPDATE task_attempts SET status='LOST', completed_at=clock_timestamp(),
				error=jsonb_build_object('code','RUN_DEADLINE_EXCEEDED','message','Run deadline exceeded','retryable',false,'effectStatus','UNKNOWN')
				WHERE id=$1::uuid AND organization_id=$2::uuid`, c.attemptID, organizationID); err != nil {
				return 0, nil, err
			}
			if _, err := tx.Exec(ctx, `DELETE FROM task_leases WHERE step_id=$1::uuid AND organization_id=$2::uuid`, c.stepID, organizationID); err != nil {
				return 0, nil, err
			}
			if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='CANCELLED', updated_at=clock_timestamp()
				WHERE run_id=$1::uuid AND organization_id=$2::uuid AND state IN ('BLOCKED','READY','WAITING','RUNNING')`, c.runID, organizationID); err != nil {
				return 0, nil, err
			}
			if _, err := tx.Exec(ctx, `UPDATE runs SET status='FAILED', reason_code='RUN_DEADLINE_EXCEEDED', updated_at=clock_timestamp()
				WHERE id=$1::uuid AND organization_id=$2::uuid`, c.runID, organizationID); err != nil {
				return 0, nil, err
			}
			if err := appendRunEvent(ctx, tx, organizationID, c.runID, "TASK_LOST", map[string]any{
				"attemptId": c.attemptID, "stepId": c.stepID, "epoch": c.attemptEpoch, "reason": "RUN_DEADLINE_EXCEEDED", "recovery": "TERMINAL",
			}); err != nil {
				return 0, nil, err
			}
			if err := appendRunEvent(ctx, tx, organizationID, c.runID, "RUN_FAILED", map[string]any{
				"status": "FAILED", "reason": "RUN_DEADLINE_EXCEEDED", "stepId": c.stepID,
			}); err != nil {
				return 0, nil, err
			}
			continue
		}

		// 2. Unstarted claim (status == CLAIMED or started_at == nil)
		if c.attemptStatus == "CLAIMED" || c.startedAt == nil {
			if _, err := tx.Exec(ctx, `UPDATE task_attempts SET status='LOST', completed_at=clock_timestamp(),
				error=jsonb_build_object('code','START_DEADLINE_EXCEEDED','message','Claim start deadline exceeded without Start','retryable',true,'effectStatus','NOT_APPLIED')
				WHERE id=$1::uuid AND organization_id=$2::uuid`, c.attemptID, organizationID); err != nil {
				return 0, nil, err
			}
			if _, err := tx.Exec(ctx, `DELETE FROM task_leases WHERE step_id=$1::uuid AND organization_id=$2::uuid`, c.stepID, organizationID); err != nil {
				return 0, nil, err
			}
			if err := appendRunEvent(ctx, tx, organizationID, c.runID, "TASK_LOST", map[string]any{
				"attemptId": c.attemptID, "stepId": c.stepID, "epoch": c.attemptEpoch, "reason": "START_DEADLINE_EXCEEDED", "recovery": "RETRY",
			}); err != nil {
				return 0, nil, err
			}

			// Unstarted claims never executed any side effects; safe to retry if attempt budget remains
			if c.nextAttemptNumber <= maxAttempts {
				if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='READY', wait_reason=NULL, eligible_at=clock_timestamp(), updated_at=clock_timestamp()
					WHERE id=$1::uuid AND organization_id=$2::uuid AND state IN ('RUNNING','WAITING')`, c.stepID, organizationID); err != nil {
					return 0, nil, err
				}
				// Normalize parent run if it was WAITING with RECOVERY_HANDOFF
				if _, err := tx.Exec(ctx, `UPDATE runs SET status=CASE
					WHEN EXISTS (SELECT 1 FROM task_attempts ta JOIN run_steps rst ON rst.id=ta.step_id WHERE rst.run_id=$1::uuid AND ta.started_at IS NOT NULL) THEN 'RUNNING'
					ELSE 'QUEUED'
				END, reason_code=NULL, updated_at=clock_timestamp()
				WHERE id=$1::uuid AND organization_id=$2::uuid AND status='WAITING' AND reason_code='RECOVERY_HANDOFF'`, c.runID, organizationID); err != nil {
					return 0, nil, err
				}
				if err := appendRunEvent(ctx, tx, organizationID, c.runID, "STEP_READY", map[string]any{
					"stepId": c.stepID, "nodeId": c.nodeID, "reason": "CLAIM_EXPIRED_REQUEUED",
				}); err != nil {
					return 0, nil, err
				}
			} else {
				// Budget exhausted
				if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='FAILED', wait_reason='MAX_ATTEMPTS_EXCEEDED', updated_at=clock_timestamp()
					WHERE id=$1::uuid AND organization_id=$2::uuid`, c.stepID, organizationID); err != nil {
					return 0, nil, err
				}
				if _, err := tx.Exec(ctx, `UPDATE runs SET status='FAILED', reason_code='START_DEADLINE_EXCEEDED', updated_at=clock_timestamp()
					WHERE id=$1::uuid AND organization_id=$2::uuid`, c.runID, organizationID); err != nil {
					return 0, nil, err
				}
				if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='CANCELLED', updated_at=clock_timestamp()
					WHERE run_id=$1::uuid AND organization_id=$2::uuid AND state IN ('BLOCKED','READY','WAITING')`, c.runID, organizationID); err != nil {
					return 0, nil, err
				}
				if err := appendRunEvent(ctx, tx, organizationID, c.runID, "RUN_FAILED", map[string]any{
					"status": "FAILED", "reason": "START_DEADLINE_EXCEEDED", "stepId": c.stepID,
				}); err != nil {
					return 0, nil, err
				}
			}
			continue
		}

		// 3. Running attempt (started_at != nil): either attempt deadline exceeded or lease expired
		isTimeout := c.attemptDeadlineAt != nil && !dbNow.Before(*c.attemptDeadlineAt)
		outcome := "LOST"
		reasonCode := "LEASE_EXPIRED"
		message := "Task lease expired without renewal"
		retryable := true
		if isTimeout {
			outcome = "TIMED_OUT"
			reasonCode = "ATTEMPT_TIMED_OUT"
			message = "Attempt execution deadline exceeded"
			retryable = false
		}

		if _, err := tx.Exec(ctx, `UPDATE task_attempts SET status=$1, completed_at=clock_timestamp(),
			error=jsonb_build_object('code',$2::text,'message',$3::text,'retryable',$4::boolean,'effectStatus','UNKNOWN')
			WHERE id=$5::uuid AND organization_id=$6::uuid`, outcome, reasonCode, message, retryable, c.attemptID, organizationID); err != nil {
			return 0, nil, err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM task_leases WHERE step_id=$1::uuid AND organization_id=$2::uuid`, c.stepID, organizationID); err != nil {
			return 0, nil, err
		}
		if err := appendRunEvent(ctx, tx, organizationID, c.runID, "TASK_LOST", map[string]any{
			"attemptId": c.attemptID, "stepId": c.stepID, "epoch": c.attemptEpoch, "reason": reasonCode, "recovery": policy,
		}); err != nil {
			return 0, nil, err
		}

		if policy == "reconcile" {
			if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='WAITING', wait_reason='RECONCILIATION', updated_at=clock_timestamp()
				WHERE id=$1::uuid AND organization_id=$2::uuid`, c.stepID, organizationID); err != nil {
				return 0, nil, err
			}
			if _, err := tx.Exec(ctx, `UPDATE runs SET status='WAITING', reason_code='RECONCILIATION', updated_at=clock_timestamp()
				WHERE id=$1::uuid AND organization_id=$2::uuid AND status IN ('QUEUED','RUNNING')`, c.runID, organizationID); err != nil {
				return 0, nil, err
			}
			if err := appendRunEvent(ctx, tx, organizationID, c.runID, "STEP_WAITING", map[string]any{
				"stepId": c.stepID, "nodeId": c.nodeID, "reason": "RECONCILIATION",
			}); err != nil {
				return 0, nil, err
			}
		} else {
			if c.nextAttemptNumber <= maxAttempts {
				if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='READY', wait_reason=NULL, eligible_at=clock_timestamp(), updated_at=clock_timestamp()
					WHERE id=$1::uuid AND organization_id=$2::uuid AND state IN ('RUNNING','WAITING')`, c.stepID, organizationID); err != nil {
					return 0, nil, err
				}
				// Normalize parent run if it was WAITING with RECOVERY_HANDOFF
				if _, err := tx.Exec(ctx, `UPDATE runs SET status=CASE
					WHEN EXISTS (SELECT 1 FROM task_attempts ta JOIN run_steps rst ON rst.id=ta.step_id WHERE rst.run_id=$1::uuid AND ta.started_at IS NOT NULL) THEN 'RUNNING'
					ELSE 'QUEUED'
				END, reason_code=NULL, updated_at=clock_timestamp()
				WHERE id=$1::uuid AND organization_id=$2::uuid AND status='WAITING' AND reason_code='RECOVERY_HANDOFF'`, c.runID, organizationID); err != nil {
					return 0, nil, err
				}
				if err := appendRunEvent(ctx, tx, organizationID, c.runID, "STEP_READY", map[string]any{
					"stepId": c.stepID, "nodeId": c.nodeID, "reason": "LEASE_EXPIRED_REQUEUED",
				}); err != nil {
					return 0, nil, err
				}
			} else {
				if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='FAILED', wait_reason='MAX_ATTEMPTS_EXCEEDED', updated_at=clock_timestamp()
					WHERE id=$1::uuid AND organization_id=$2::uuid`, c.stepID, organizationID); err != nil {
					return 0, nil, err
				}
				if _, err := tx.Exec(ctx, `UPDATE runs SET status='FAILED', reason_code=$1, updated_at=clock_timestamp()
					WHERE id=$2::uuid AND organization_id=$3::uuid`, reasonCode, c.runID, organizationID); err != nil {
					return 0, nil, err
				}
				if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='CANCELLED', updated_at=clock_timestamp()
					WHERE run_id=$1::uuid AND organization_id=$2::uuid AND state IN ('BLOCKED','READY','WAITING')`, c.runID, organizationID); err != nil {
					return 0, nil, err
				}
				if err := appendRunEvent(ctx, tx, organizationID, c.runID, "RUN_FAILED", map[string]any{
					"status": "FAILED", "reason": reasonCode, "stepId": c.stepID,
				}); err != nil {
					return 0, nil, err
				}
			}
		}
	}

	for _, h := range handoffs {
		reclaimedRunsMap[h.runID] = struct{}{}
		reclaimedCount++

		var manifest deploymentManifest
		if len(h.manifestBytes) > 0 {
			_ = json.Unmarshal(h.manifestBytes, &manifest)
		}
		_, maxAttempts := manifest.taskRecoveryPolicy(h.workflowName, h.nodeID)

		if h.runDeadlineAt != nil && !dbNow.Before(*h.runDeadlineAt) {
			if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='CANCELLED', updated_at=clock_timestamp()
				WHERE run_id=$1::uuid AND organization_id=$2::uuid AND state IN ('BLOCKED','READY','WAITING','RUNNING')`, h.runID, organizationID); err != nil {
				return 0, nil, err
			}
			if _, err := tx.Exec(ctx, `UPDATE runs SET status='FAILED', reason_code='RUN_DEADLINE_EXCEEDED', updated_at=clock_timestamp()
				WHERE id=$1::uuid AND organization_id=$2::uuid`, h.runID, organizationID); err != nil {
				return 0, nil, err
			}
			if err := appendRunEvent(ctx, tx, organizationID, h.runID, "RUN_FAILED", map[string]any{
				"status": "FAILED", "reason": "RUN_DEADLINE_EXCEEDED", "stepId": h.stepID,
			}); err != nil {
				return 0, nil, err
			}
			continue
		}

		if h.nextAttemptNumber <= maxAttempts {
			if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='READY', wait_reason=NULL, eligible_at=clock_timestamp(), updated_at=clock_timestamp()
				WHERE id=$1::uuid AND organization_id=$2::uuid AND state='WAITING' AND wait_reason='RECOVERY_HANDOFF'`, h.stepID, organizationID); err != nil {
				return 0, nil, err
			}

			targetRunStatus := "QUEUED"
			if h.hasStartedAttempt {
				targetRunStatus = "RUNNING"
			}
			if _, err := tx.Exec(ctx, `UPDATE runs SET status=$1, reason_code=NULL, updated_at=clock_timestamp()
				WHERE id=$2::uuid AND organization_id=$3::uuid AND status='WAITING' AND reason_code='RECOVERY_HANDOFF'`, targetRunStatus, h.runID, organizationID); err != nil {
				return 0, nil, err
			}

			if err := appendRunEvent(ctx, tx, organizationID, h.runID, "STEP_READY", map[string]any{
				"stepId": h.stepID, "nodeId": h.nodeID, "reason": "RECOVERY_HANDOFF_RESOLVED",
			}); err != nil {
				return 0, nil, err
			}
		} else {
			if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='FAILED', wait_reason='MAX_ATTEMPTS_EXCEEDED', updated_at=clock_timestamp()
				WHERE id=$1::uuid AND organization_id=$2::uuid`, h.stepID, organizationID); err != nil {
				return 0, nil, err
			}
			if _, err := tx.Exec(ctx, `UPDATE runs SET status='FAILED', reason_code='MAX_ATTEMPTS_EXCEEDED', updated_at=clock_timestamp()
				WHERE id=$1::uuid AND organization_id=$2::uuid`, h.runID, organizationID); err != nil {
				return 0, nil, err
			}
			if _, err := tx.Exec(ctx, `UPDATE run_steps SET state='CANCELLED', updated_at=clock_timestamp()
				WHERE run_id=$1::uuid AND organization_id=$2::uuid AND state IN ('BLOCKED','READY','WAITING')`, h.runID, organizationID); err != nil {
				return 0, nil, err
			}
			if err := appendRunEvent(ctx, tx, organizationID, h.runID, "RUN_FAILED", map[string]any{
				"status": "FAILED", "reason": "MAX_ATTEMPTS_EXCEEDED", "stepId": h.stepID,
			}); err != nil {
				return 0, nil, err
			}
		}
	}

	affectedRuns := make([]string, 0, len(reclaimedRunsMap))
	for rID := range reclaimedRunsMap {
		affectedRuns = append(affectedRuns, rID)
	}
	return reclaimedCount, affectedRuns, nil
}

func appendRunEvent(ctx context.Context, tx storage.Tx, organizationID, runID, eventType string, payload any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	var sequence int64
	if err := tx.QueryRow(ctx, `UPDATE runs SET last_event_sequence=last_event_sequence+1,updated_at=clock_timestamp()
		WHERE id=$1::uuid AND organization_id=$2::uuid RETURNING last_event_sequence`, runID, organizationID).Scan(&sequence); err != nil {
		return err
	}
	var eventID string
	if err := tx.QueryRow(ctx, `INSERT INTO run_events (organization_id,run_id,sequence,event_type,payload)
		VALUES ($1::uuid,$2::uuid,$3,$4,$5::jsonb) RETURNING id::text`, organizationID, runID, sequence, eventType, string(encoded)).Scan(&eventID); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox_events (organization_id,event_id,subject,payload)
		VALUES ($1::uuid,$2::uuid,'execution.state_changed',jsonb_build_object(
			'runId',$3::text,'sequence',$4::bigint,'eventType',$5::text))`, organizationID, eventID, runID, sequence, eventType)
	return err
}
