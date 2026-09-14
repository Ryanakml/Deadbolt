package execution

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/worker"
	"github.com/jackc/pgx/v5"
)

const defaultAttemptTimeout = 5 * time.Minute

// WorkerEngine is the PostgreSQL execution authority used by the worker HTTP
// adapter. State transitions, leases, history, and wake-up hints are committed
// together here instead of in the transport layer.
type WorkerEngine struct {
	pool *storage.Pool
}

func NewWorkerEngine(pool *storage.Pool) *WorkerEngine {
	return &WorkerEngine{pool: pool}
}

type deploymentManifest struct {
	TargetArchitecture string   `json:"targetArchitecture"`
	SecretNames        []string `json:"secretNames"`
	Tasks              []struct {
		Name       string `json:"name"`
		Entrypoint string `json:"entrypoint"`
		TimeoutMs  int64  `json:"timeoutMs"`
	} `json:"tasks"`
	Workflows []struct {
		Name  string `json:"name"`
		Nodes []struct {
			ID   string `json:"id"`
			Task string `json:"task"`
		} `json:"nodes"`
	} `json:"workflows"`
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

func stableOperationID(environmentID, runID, nodeID string) string {
	digest := sha256.Sum256([]byte("deadbolt-operation:v1:" + environmentID + ":" + runID + ":" + nodeID))
	return "op_" + hex.EncodeToString(digest[:])
}

func targetArchitecture(architecture string) string {
	architecture = strings.ToLower(strings.TrimSpace(architecture))
	switch architecture {
	case "":
		return ""
	case "amd64", "x64":
		return runtime.GOOS + "/amd64"
	case "arm64":
		return runtime.GOOS + "/arm64"
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
				TaskEntrypoint: entrypoint, Input: match.input, DeploymentDigest: match.bundle, BundleDigest: match.bundle,
				OperationID: stableOperationID(session.EnvironmentID, match.runID, match.nodeID),
				LeaseTTLMs:  worker.DefaultLeaseTTL.Milliseconds(), LeaseExpiresAt: leaseExpiry.UTC().Format(time.RFC3339Nano),
				ClaimStartDeadlineAt: claimDeadline.UTC().Format(time.RFC3339Nano), AttemptTimeoutMs: timeoutMs,
				RunDeadlineAt:      runDeadline,
				TraceContext:       worker.TraceContextDTO{Traceparent: "00-00000000000000000000000000000000-0000000000000000-01"},
				TargetArchitecture: targetArchitecture(manifest.TargetArchitecture), SecretNames: manifest.SecretNames,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &worker.PollResponseDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: req.RequestID, Assignments: assignments}, nil
}

func (e *WorkerEngine) Start(ctx context.Context, session *worker.WorkerSessionContext, req *worker.StartRequestDTO) (*worker.StartResponseDTO, error) {
	if req.WorkerID != session.WorkerID || req.SessionID != session.SessionID {
		return nil, worker.ErrUnauthorized
	}
	var deadline time.Time
	err := e.pool.WithTenantTx(ctx, session.OrganizationID, func(ctx context.Context, tx storage.Tx) error {
		var runID, status, ownerSession string
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
	err := e.pool.WithTenantTx(ctx, session.OrganizationID, func(ctx context.Context, tx storage.Tx) error {
		var runID, stepID, status, ownerSession string
		var epoch int64
		var storedDigest *string
		var attemptDeadline *time.Time
		err := tx.QueryRow(ctx, `SELECT r.id::text,rs.id::text,a.status,a.epoch,a.session_id::text,a.outcome_digest,a.deadline_at
			FROM runs r JOIN run_steps rs ON rs.run_id=r.id AND rs.organization_id=r.organization_id
			JOIN task_attempts a ON a.step_id=rs.id AND a.organization_id=rs.organization_id
			WHERE a.id=$1::uuid AND a.organization_id=$2::uuid
			FOR UPDATE OF r,rs,a`, req.AttemptID, session.OrganizationID).Scan(
			&runID, &stepID, &status, &epoch, &ownerSession, &storedDigest, &attemptDeadline)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return worker.ErrAttemptNotFound
			}
			return err
		}
		if terminalAttempt(status) {
			if status == req.Outcome && storedDigest != nil && *storedDigest == req.ResultDigest {
				return nil
			}
			return worker.ErrResultConflict
		}
		if status != "RUNNING" || epoch != req.OwnershipEpoch || ownerSession != session.SessionID || attemptDeadline == nil {
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

		var allTerminal, allSuccessful bool
		if err := tx.QueryRow(ctx, `SELECT
			bool_and(state IN ('SUCCEEDED','FAILED','CANCELLED','SKIPPED')),
			bool_and(state IN ('SUCCEEDED','SKIPPED'))
			FROM run_steps WHERE run_id=$1::uuid AND organization_id=$2::uuid`, runID, session.OrganizationID).Scan(&allTerminal, &allSuccessful); err != nil {
			return err
		}
		if allTerminal {
			runState := "FAILED"
			if allSuccessful {
				runState = "SUCCEEDED"
			}
			if _, err := tx.Exec(ctx, `UPDATE runs SET status=$1,updated_at=clock_timestamp()
				WHERE id=$2::uuid AND organization_id=$3::uuid`, runState, runID, session.OrganizationID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
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
