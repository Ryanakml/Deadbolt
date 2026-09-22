package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/jackc/pgx/v5"
)

const (
	MaxLogLineSizeBytes    = 16 * 1024   // 16 KiB per line bound (Blueprint / F-25)
	MaxAttemptLogSizeBytes = 1024 * 1024 // 1 MiB per attempt cumulative budget
)

type DeploymentReconciler interface {
	ReconcileAvailability(ctx context.Context, orgID, envID string) error
}

type EnrollmentTokenInfo struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expiresAt"`
	Pool      string    `json:"pool"`
}

type Service struct {
	pool             *storage.Pool
	deployments      DeploymentReconciler
	engine           ExecutionEngine
	droppedLogsTotal atomic.Int64
}

func NewService(pool *storage.Pool, deployments DeploymentReconciler, engine ExecutionEngine) *Service {
	return &Service{
		pool:        pool,
		deployments: deployments,
		engine:      engine,
	}
}

// DroppedLogsCount returns the cumulative number of dropped log records due to line or attempt budget limits.
func (s *Service) DroppedLogsCount() int64 {
	return s.droppedLogsTotal.Load()
}

// CreateEnrollmentToken generates a 10-minute single-use hashed enrollment token
// within the authenticated organization context.
func (s *Service) CreateEnrollmentToken(ctx context.Context, orgID, envID, poolName string, createdBy *string) (*EnrollmentTokenInfo, error) {
	if poolName == "" {
		poolName = "default"
	}
	rawToken, tokenHash, err := GenerateEnrollmentToken()
	if err != nil {
		return nil, err
	}
	var expiresAt time.Time

	err = s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		query := `INSERT INTO worker_enrollments (organization_id, environment_id, token_hash, pool_name, expires_at, created_by)
		          VALUES ($1::uuid, $2::uuid, $3, $4, clock_timestamp()+INTERVAL '10 minutes', ($5)::uuid)
		          RETURNING expires_at`
		return tx.QueryRow(ctx, query, orgID, envID, tokenHash, poolName, createdBy).Scan(&expiresAt)
	})
	if err != nil {
		return nil, fmt.Errorf("insert worker enrollment: %w", err)
	}

	return &EnrollmentTokenInfo{
		Token:     rawToken,
		ExpiresAt: expiresAt,
		Pool:      poolName,
	}, nil
}

// CreateChallenge generates and persists a single-use 5-minute challenge nonce.
func (s *Service) CreateChallenge(ctx context.Context, req *ChallengeRequestDTO) (*ChallengeResponseDTO, error) {
	if (req.WorkerID == "") == (req.PublicKey == "") {
		return nil, ErrChallengeInvalid
	}
	if req.PublicKey != "" {
		if _, err := DecodePublicKey(req.PublicKey); err != nil {
			return nil, ErrChallengeInvalid
		}
	}
	nonce, err := GenerateChallengeNonce()
	if err != nil {
		return nil, err
	}
	var expiresAt time.Time

	// worker_challenges is a system table tracking nonces before session authentication.
	// Using standard pool query without tenant RLS context.
	query := `INSERT INTO worker_challenges (nonce, worker_id, public_key, expires_at)
	          VALUES ($1, $2, $3, clock_timestamp()+INTERVAL '5 minutes')
	          RETURNING expires_at`
	var workerID, pubKey *string
	if req.WorkerID != "" {
		workerID = &req.WorkerID
	}
	if req.PublicKey != "" {
		pubKey = &req.PublicKey
	}

	err = s.pool.QueryRow(ctx, query, nonce, workerID, pubKey).Scan(&expiresAt)
	if err != nil {
		return nil, fmt.Errorf("insert challenge nonce: %w", err)
	}

	return &ChallengeResponseDTO{
		ProtocolVersion: ProtocolVersion,
		RequestID:       req.RequestID,
		Nonce:           nonce,
		ExpiresAt:       expiresAt.UTC().Format(time.RFC3339),
	}, nil
}

// EnrollWorker binds a worker public key, validates proof of possession, and issues a 15m session.
func (s *Service) EnrollWorker(ctx context.Context, req *EnrollRequestDTO) (*SessionResponseDTO, error) {
	// 1. Verify and consume challenge nonce atomically
	var nonceWorkerID, noncePubKey *string
	nonceQuery := `UPDATE worker_challenges
	               SET used_at = clock_timestamp()
	               WHERE nonce = $1 AND used_at IS NULL AND expires_at > clock_timestamp()
	               RETURNING worker_id, public_key`
	err := s.pool.QueryRow(ctx, nonceQuery, req.Nonce).Scan(&nonceWorkerID, &noncePubKey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrChallengeExpired
		}
		return nil, fmt.Errorf("verify challenge: %w", err)
	}
	if nonceWorkerID != nil || noncePubKey == nil {
		return nil, ErrInvalidSignature
	}

	// 2. Verify signature
	pubKey, err := DecodePublicKey(req.PublicKey)
	if err != nil {
		return nil, err
	}
	boundKey, err := DecodePublicKey(*noncePubKey)
	if err != nil || !bytes.Equal(pubKey, boundKey) {
		return nil, ErrInvalidSignature
	}
	if !VerifyChallengeSignature(pubKey, req.Nonce, req.Signature) {
		return nil, ErrInvalidSignature
	}

	// 3. Verify and consume enrollment token via security definer function
	tokenHash := HashToken(req.EnrollmentToken)
	var orgID, envID, poolName, enrollmentID string
	tokenQuery := `SELECT id::text, organization_id::text, environment_id::text, pool_name
	               FROM app.consume_enrollment_token($1)`
	err = s.pool.QueryRow(ctx, tokenQuery, tokenHash).Scan(&enrollmentID, &orgID, &envID, &poolName)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrEnrollmentInvalid
		}
		return nil, fmt.Errorf("verify enrollment token: %w", err)
	}

	// 4. Create worker and session under tenant context
	var workerID, sessionID, rawSessionToken string
	var sessionExpiresAt time.Time

	err = s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := lockEnvironmentAdmission(ctx, tx, orgID, envID); err != nil {
			return err
		}
		if err := enforceWorkerSessionQuota(ctx, tx, orgID, envID); err != nil {
			return err
		}
		// Insert worker
		workerQuery := `INSERT INTO workers (organization_id, environment_id, public_key, pool_name, status, last_seen_at)
		                VALUES ($1::uuid, $2::uuid, $3, $4, 'ACTIVE', clock_timestamp())
		                RETURNING id::text`
		if err := tx.QueryRow(ctx, workerQuery, orgID, envID, EncodePublicKey(pubKey), poolName).Scan(&workerID); err != nil {
			return fmt.Errorf("insert worker: %w", err)
		}

		// Create session
		var tokenHash string
		rawSessionToken, tokenHash, err = GenerateSessionToken()
		if err != nil {
			return err
		}
		sessionQuery := `INSERT INTO worker_sessions (organization_id, worker_id, environment_id, session_token_hash, expires_at)
		                 VALUES ($1::uuid, $2::uuid, $3::uuid, $4, clock_timestamp()+INTERVAL '15 minutes')
		                 RETURNING id::text, expires_at`
		if err := tx.QueryRow(ctx, sessionQuery, orgID, workerID, envID, tokenHash).Scan(&sessionID, &sessionExpiresAt); err != nil {
			return fmt.Errorf("insert worker session: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	if s.deployments != nil {
		_ = s.deployments.ReconcileAvailability(ctx, orgID, envID)
	}

	return &SessionResponseDTO{
		ProtocolVersion: ProtocolVersion,
		RequestID:       req.RequestID,
		WorkerID:        workerID,
		EnvironmentID:   envID,
		SessionID:       sessionID,
		SessionToken:    rawSessionToken,
		ExpiresAt:       sessionExpiresAt.UTC().Format(time.RFC3339),
	}, nil
}

// CreateSession re-authenticates an enrolled worker via signed challenge nonce and issues a fresh 15m session.
// Reconnecting revokes leases and sessions from previous connection per Blueprint §12.1.
func (s *Service) CreateSession(ctx context.Context, req *SessionRequestDTO) (*SessionResponseDTO, error) {
	// 1. Verify and consume challenge nonce
	var nonceWorkerID, noncePubKey *string
	nonceQuery := `UPDATE worker_challenges
	               SET used_at = clock_timestamp()
	               WHERE nonce = $1 AND used_at IS NULL AND expires_at > clock_timestamp()
	               RETURNING worker_id, public_key`
	err := s.pool.QueryRow(ctx, nonceQuery, req.Nonce).Scan(&nonceWorkerID, &noncePubKey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrChallengeExpired
		}
		return nil, fmt.Errorf("verify challenge: %w", err)
	}
	if nonceWorkerID == nil || *nonceWorkerID != req.WorkerID || noncePubKey != nil {
		return nil, ErrInvalidSignature
	}

	// 2. Fetch worker info via security definer function
	var workerID, orgID, envID, pubKeyHex, status string
	workerQuery := `SELECT worker_id::text, organization_id::text, environment_id::text, public_key, status
	                FROM app.lookup_worker_for_session($1)`
	err = s.pool.QueryRow(ctx, workerQuery, req.WorkerID).Scan(&workerID, &orgID, &envID, &pubKeyHex, &status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrWorkerNotFound
		}
		return nil, fmt.Errorf("fetch worker: %w", err)
	}

	if status == "REVOKED" {
		return nil, ErrWorkerRevoked
	}

	// 3. Verify signature using stored worker public key
	pubKey, err := DecodePublicKey(pubKeyHex)
	if err != nil {
		return nil, err
	}
	if !VerifyChallengeSignature(pubKey, req.Nonce, req.Signature) {
		return nil, ErrInvalidSignature
	}

	// 4. In a tenant transaction: revoke old sessions, cancel old leases, issue fresh session
	var sessionID, rawSessionToken string
	var sessionExpiresAt time.Time

	err = s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		// Admission is locked before fencing attempts/sessions so reconnects
		// cannot race the per-environment worker-session cap.
		if err := lockEnvironmentAdmission(ctx, tx, orgID, envID); err != nil {
			return err
		}
		if fencer, ok := s.engine.(SessionFencer); ok && fencer != nil {
			if err := fencer.FenceWorkerSessions(ctx, tx, orgID, req.WorkerID, "RECONNECT"); err != nil {
				return fmt.Errorf("fence reconnecting worker: %w", err)
			}
		}

		// Revoke old sessions
		_, err := tx.Exec(ctx, `UPDATE worker_sessions SET revoked_at = clock_timestamp()
		                        WHERE worker_id = $1::uuid AND organization_id = $2::uuid AND revoked_at IS NULL`, req.WorkerID, orgID)
		if err != nil {
			return fmt.Errorf("revoke old worker sessions: %w", err)
		}
		if err := enforceWorkerSessionQuota(ctx, tx, orgID, envID); err != nil {
			return err
		}

		// Create fresh session
		var tokenHash string
		rawSessionToken, tokenHash, err = GenerateSessionToken()
		if err != nil {
			return err
		}
		sessionQuery := `INSERT INTO worker_sessions (organization_id, worker_id, environment_id, session_token_hash, expires_at)
		                 VALUES ($1::uuid, $2::uuid, $3::uuid, $4, clock_timestamp()+INTERVAL '15 minutes')
		                 RETURNING id::text, expires_at`
		if err := tx.QueryRow(ctx, sessionQuery, orgID, req.WorkerID, envID, tokenHash).Scan(&sessionID, &sessionExpiresAt); err != nil {
			return fmt.Errorf("insert worker session: %w", err)
		}

		// Update worker last_seen_at
		_, err = tx.Exec(ctx, `UPDATE workers SET last_seen_at = clock_timestamp() WHERE id = $1::uuid AND organization_id = $2::uuid`, req.WorkerID, orgID)
		return err
	})
	if err != nil {
		return nil, err
	}

	if s.deployments != nil {
		_ = s.deployments.ReconcileAvailability(ctx, orgID, envID)
	}

	return &SessionResponseDTO{
		ProtocolVersion: ProtocolVersion,
		RequestID:       req.RequestID,
		WorkerID:        req.WorkerID,
		EnvironmentID:   envID,
		SessionID:       sessionID,
		SessionToken:    rawSessionToken,
		ExpiresAt:       sessionExpiresAt.UTC().Format(time.RFC3339),
	}, nil
}

func lockEnvironmentAdmission(ctx context.Context, tx storage.Tx, orgID, envID string) error {
	var lockedID string
	if err := tx.QueryRow(ctx, `SELECT environment_id::text
		FROM environment_admissions
		WHERE environment_id=$1::uuid AND organization_id=$2::uuid
		FOR UPDATE`, envID, orgID).Scan(&lockedID); err != nil {
		return fmt.Errorf("lock environment admission: %w", err)
	}
	return nil
}

func enforceWorkerSessionQuota(ctx context.Context, tx storage.Tx, orgID, envID string) error {
	var active int
	if err := tx.QueryRow(ctx, `SELECT count(*)
		FROM worker_sessions
		WHERE organization_id=$1::uuid AND environment_id=$2::uuid
		  AND revoked_at IS NULL AND expires_at > clock_timestamp()`, orgID, envID).Scan(&active); err != nil {
		return fmt.Errorf("count active worker sessions: %w", err)
	}
	if active >= MaxWorkerSessions {
		return ErrSessionQuotaExceeded
	}
	return nil
}

// AuthenticateSession verifies a Bearer session token against database records.
func (s *Service) AuthenticateSession(ctx context.Context, rawSessionToken string) (*WorkerSessionContext, error) {
	tokenHash := HashToken(rawSessionToken)
	query := `SELECT session_id::text, worker_id::text, organization_id::text, environment_id::text,
	                 pool_name, expires_at, revoked_at, worker_status, clock_timestamp()
	          FROM app.authenticate_worker_session($1)`

	var sessionID, workerID, orgID, envID, poolName, workerStatus string
	var expiresAt time.Time
	var dbNow time.Time
	var revokedAt *time.Time

	err := s.pool.QueryRow(ctx, query, tokenHash).Scan(
		&sessionID, &workerID, &orgID, &envID, &poolName, &expiresAt, &revokedAt, &workerStatus, &dbNow,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrUnauthorized
		}
		return nil, fmt.Errorf("query session: %w", err)
	}

	if workerStatus == "REVOKED" {
		return nil, ErrWorkerRevoked
	}
	if revokedAt != nil {
		return nil, ErrSessionRevoked
	}
	if !dbNow.Before(expiresAt) {
		return nil, ErrSessionExpired
	}

	return &WorkerSessionContext{
		SessionID:      sessionID,
		WorkerID:       workerID,
		OrganizationID: orgID,
		EnvironmentID:  envID,
		PoolName:       poolName,
		ExpiresAt:      expiresAt,
	}, nil
}

// PollAssignments advertises worker bundle digests and waits for eligible assignments.
func (s *Service) PollAssignments(ctx context.Context, sessionCtx *WorkerSessionContext, req *PollRequestDTO) (*PollResponseDTO, error) {
	if req.WorkerID != sessionCtx.WorkerID || req.SessionID != sessionCtx.SessionID || req.Pool != sessionCtx.PoolName {
		return nil, ErrUnauthorized
	}
	workerStatus, err := s.workerAssignmentStatus(ctx, sessionCtx)
	if err != nil {
		return nil, err
	}
	if workerStatus == "REVOKED" {
		return nil, ErrWorkerRevoked
	}
	if workerStatus != "ACTIVE" {
		return &PollResponseDTO{ProtocolVersion: ProtocolVersion, RequestID: req.RequestID, Assignments: []AssignmentDTO{}}, nil
	}
	if err := s.advertiseDeployments(ctx, sessionCtx, req.DeploymentDigests); err != nil {
		return nil, err
	}
	if s.deployments != nil {
		_ = s.deployments.ReconcileAvailability(ctx, sessionCtx.OrganizationID, sessionCtx.EnvironmentID)
	}
	if req.AvailableSlots <= 0 {
		return &PollResponseDTO{ProtocolVersion: ProtocolVersion, RequestID: req.RequestID, Assignments: []AssignmentDTO{}}, nil
	}
	if s.engine == nil {
		return nil, ErrExecutionEngineUnavailable
	}
	deadline := time.NewTimer(DefaultPollTimeout)
	defer deadline.Stop()
	for {
		res, err := s.engine.Claim(ctx, sessionCtx, req)
		if err != nil || len(res.Assignments) > 0 {
			return res, err
		}
		wait := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			wait.Stop()
			return res, ctx.Err()
		case <-deadline.C:
			wait.Stop()
			return res, nil
		case <-wait.C:
		}
	}
}

func (s *Service) advertiseDeployments(ctx context.Context, sessionCtx *WorkerSessionContext, digests []string) error {
	return s.pool.WithTenantTx(ctx, sessionCtx.OrganizationID, func(ctx context.Context, tx storage.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE workers SET last_seen_at = clock_timestamp()
			WHERE id=$1::uuid AND organization_id=$2::uuid`, sessionCtx.WorkerID, sessionCtx.OrganizationID); err != nil {
			return err
		}
		// A poll's DeploymentDigests is the session's CURRENT advertised bundle
		// set, not an append-only history. Replace persisted compatibility
		// atomically so a worker that drops V1 and advertises only V2 is no
		// longer eligible for V1-pinned runs via a stale worker_deployments row.
		if len(digests) == 0 {
			if _, err := tx.Exec(ctx, `DELETE FROM worker_deployments
				WHERE session_id=$1::uuid AND organization_id=$2::uuid`, sessionCtx.SessionID, sessionCtx.OrganizationID); err != nil {
				return fmt.Errorf("clear worker deployments: %w", err)
			}
			return nil
		}
		if _, err := tx.Exec(ctx, `DELETE FROM worker_deployments
			WHERE session_id=$1::uuid AND organization_id=$2::uuid AND NOT (bundle_digest = ANY($3::text[]))`,
			sessionCtx.SessionID, sessionCtx.OrganizationID, digests); err != nil {
			return fmt.Errorf("remove stale worker deployments: %w", err)
		}
		for _, digest := range digests {
			if _, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id, organization_id, bundle_digest)
				VALUES ($1::uuid, $2::uuid, $3) ON CONFLICT (session_id, bundle_digest) DO NOTHING`,
				sessionCtx.SessionID, sessionCtx.OrganizationID, digest); err != nil {
				return fmt.Errorf("upsert worker deployment: %w", err)
			}
		}
		return nil
	})
}

func (s *Service) workerAssignmentStatus(ctx context.Context, sessionCtx *WorkerSessionContext) (string, error) {
	var status string
	err := s.pool.WithTenantTx(ctx, sessionCtx.OrganizationID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status FROM workers WHERE id=$1::uuid AND organization_id=$2::uuid`, sessionCtx.WorkerID, sessionCtx.OrganizationID).Scan(&status)
	})
	return status, err
}

// StartAttempt delegates the authoritative transition to the execution engine.
func (s *Service) StartAttempt(ctx context.Context, sessionCtx *WorkerSessionContext, req *StartRequestDTO) (*StartResponseDTO, error) {
	if s.engine == nil {
		return nil, ErrExecutionEngineUnavailable
	}
	return s.engine.Start(ctx, sessionCtx, req)
}

// Heartbeat delegates lease fencing and renewal to the execution engine.
func (s *Service) Heartbeat(ctx context.Context, sessionCtx *WorkerSessionContext, req *HeartbeatRequestDTO) (*HeartbeatResponseDTO, error) {
	if s.engine == nil {
		return nil, ErrExecutionEngineUnavailable
	}
	return s.engine.Heartbeat(ctx, sessionCtx, req)
}

// CompleteAttempt delegates result commitment to the execution engine.
func (s *Service) CompleteAttempt(ctx context.Context, sessionCtx *WorkerSessionContext, req *CompleteRequestDTO) (*CompleteResponseDTO, error) {
	if s.engine == nil {
		return nil, ErrExecutionEngineUnavailable
	}
	return s.engine.Complete(ctx, sessionCtx, req)
}

// StopAck records process termination confirmation for a stop command.
func (s *Service) StopAck(ctx context.Context, sessionCtx *WorkerSessionContext, req *StopAckRequestDTO) (*AckResponseDTO, error) {
	if s.engine == nil {
		return nil, ErrExecutionEngineUnavailable
	}
	return s.engine.StopAck(ctx, sessionCtx, req)
}

// RecordLogs processes a bounded log batch from a worker.
func (s *Service) RecordLogs(ctx context.Context, sessionCtx *WorkerSessionContext, req *LogBatchRequestDTO) (*AckResponseDTO, error) {
	if req.WorkerID != sessionCtx.WorkerID || req.SessionID != sessionCtx.SessionID {
		return nil, ErrUnauthorized
	}

	if len(req.Records) == 0 {
		return &AckResponseDTO{
			ProtocolVersion: ProtocolVersion,
			RequestID:       req.RequestID,
			Accepted:        true,
		}, nil
	}

	var droppedCount int
	var budgetExhausted bool
	var newlyDropped int

	err := s.pool.WithTenantTx(ctx, sessionCtx.OrganizationID, func(ctx context.Context, tx storage.Tx) error {
		var stepID, runID, envID string
		err := tx.QueryRow(ctx, `
			SELECT a.step_id::text, rs.run_id::text, rs.environment_id::text
			FROM task_attempts a
			JOIN run_steps rs ON rs.id = a.step_id AND rs.organization_id = a.organization_id
			WHERE a.id = $1::uuid AND a.session_id = $2::uuid AND a.organization_id = $3::uuid
		`, req.AttemptID, sessionCtx.SessionID, sessionCtx.OrganizationID).Scan(&stepID, &runID, &envID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrUnauthorized
			}
			return fmt.Errorf("verify attempt ownership: %w", err)
		}
		// Serialize accounting for one attempt. This prevents concurrent batches
		// from both observing spare budget and collectively exceeding 1 MiB.
		var lockedAttemptID string
		if err := tx.QueryRow(ctx, `
			SELECT id FROM task_attempts
			WHERE id = $1::uuid AND organization_id = $2::uuid
			FOR UPDATE
		`, req.AttemptID, sessionCtx.OrganizationID).Scan(&lockedAttemptID); err != nil {
			return fmt.Errorf("lock attempt log budget: %w", err)
		}

		var currentBytes int64
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE(SUM(octet_length(message)), 0)::bigint
			FROM task_logs
			WHERE attempt_id = $1::uuid AND organization_id = $2::uuid
		`, req.AttemptID, sessionCtx.OrganizationID).Scan(&currentBytes); err != nil {
			return fmt.Errorf("query existing log bytes: %w", err)
		}

		var insertedAny bool
		for _, rec := range req.Records {
			// A retry of an accepted record is a no-op even after the attempt has
			// reached its byte budget.
			var alreadyStored bool
			if err := tx.QueryRow(ctx, `
				SELECT EXISTS(
					SELECT 1 FROM task_logs
					WHERE attempt_id = $1::uuid AND sequence = $2 AND organization_id = $3::uuid
				)
			`, req.AttemptID, rec.Sequence, sessionCtx.OrganizationID).Scan(&alreadyStored); err != nil {
				return fmt.Errorf("check existing task log: %w", err)
			}
			if alreadyStored {
				continue
			}

			var priorDropReason string
			priorDropErr := tx.QueryRow(ctx, `
				SELECT reason FROM task_log_drop_receipts
				WHERE attempt_id = $1::uuid AND sequence = $2 AND organization_id = $3::uuid
			`, req.AttemptID, rec.Sequence, sessionCtx.OrganizationID).Scan(&priorDropReason)
			if priorDropErr != nil && !errors.Is(priorDropErr, pgx.ErrNoRows) {
				return fmt.Errorf("check existing dropped task log: %w", priorDropErr)
			}
			if priorDropErr == nil {
				droppedCount++
				budgetExhausted = budgetExhausted || priorDropReason == "BUDGET_EXHAUSTED"
				continue
			}

			msgBytes := int64(len([]byte(rec.Message)))
			dropReason := ""
			if msgBytes > MaxLogLineSizeBytes {
				dropReason = "LINE_TOO_LARGE"
			} else if currentBytes+msgBytes > MaxAttemptLogSizeBytes {
				dropReason = "BUDGET_EXHAUSTED"
			}
			if dropReason != "" {
				droppedCount++
				budgetExhausted = budgetExhausted || dropReason == "BUDGET_EXHAUSTED"
				tag, err := tx.Exec(ctx, `
					INSERT INTO task_log_drop_receipts (organization_id, attempt_id, sequence, reason)
					VALUES ($1::uuid, $2::uuid, $3, $4)
					ON CONFLICT (attempt_id, sequence) DO NOTHING
				`, sessionCtx.OrganizationID, req.AttemptID, rec.Sequence, dropReason)
				if err != nil {
					return fmt.Errorf("record dropped task log: %w", err)
				}
				if tag.RowsAffected() > 0 {
					newlyDropped++
				}
				continue
			}

			ts, parseErr := time.Parse(time.RFC3339Nano, rec.Timestamp)
			if parseErr != nil {
				ts = time.Now()
			}
			lvl := "info"
			switch rec.Level {
			case "debug", "info", "warn", "error":
				lvl = rec.Level
			}

			tag, insErr := tx.Exec(ctx, `
				INSERT INTO task_logs (
					organization_id, environment_id, run_id, step_id, attempt_id,
					sequence, timestamp, level, message
				) VALUES (
					$1::uuid, $2::uuid, $3::uuid, $4::uuid, $5::uuid,
					$6, $7, $8, $9
				) ON CONFLICT (attempt_id, sequence) DO NOTHING
			`, sessionCtx.OrganizationID, envID, runID, stepID, req.AttemptID, rec.Sequence, ts, lvl, rec.Message)
			if insErr != nil {
				return fmt.Errorf("insert task log: %w", insErr)
			}
			if tag.RowsAffected() > 0 {
				currentBytes += msgBytes
				insertedAny = true
			}
		}

		if insertedAny {
			_, err = tx.Exec(ctx, `
				UPDATE task_attempts
				SET logs_recorded = TRUE
				WHERE id = $1::uuid AND organization_id = $2::uuid AND logs_recorded = FALSE
			`, req.AttemptID, sessionCtx.OrganizationID)
			if err != nil {
				return fmt.Errorf("mark logs recorded: %w", err)
			}
		}
		if newlyDropped > 0 {
			_, err = tx.Exec(ctx, `
				UPDATE task_attempts
				SET dropped_log_count = dropped_log_count + $1,
					log_budget_exhausted = log_budget_exhausted OR $2
				WHERE id = $3::uuid AND organization_id = $4::uuid
			`, newlyDropped, budgetExhausted, req.AttemptID, sessionCtx.OrganizationID)
			if err != nil {
				return fmt.Errorf("record task log drop state: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		if errors.Is(err, ErrUnauthorized) {
			return nil, ErrUnauthorized
		}
		return nil, fmt.Errorf("record logs: %w", err)
	}

	if newlyDropped > 0 {
		s.droppedLogsTotal.Add(int64(newlyDropped))
	}

	return &AckResponseDTO{
		ProtocolVersion: ProtocolVersion,
		RequestID:       req.RequestID,
		Accepted:        true,
		DroppedCount:    droppedCount,
		BudgetExhausted: budgetExhausted,
	}, nil
}

// RevokeWorker marks a worker REVOKED and revokes all its active sessions and leases.
func (s *Service) RevokeWorker(ctx context.Context, orgID, workerID string) error {
	return s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		if fencer, ok := s.engine.(SessionFencer); ok && fencer != nil {
			if err := fencer.FenceWorkerSessions(ctx, tx, orgID, workerID, "REVOKED"); err != nil {
				return fmt.Errorf("fence revoked worker: %w", err)
			}
		}

		_, err := tx.Exec(ctx, `UPDATE workers SET status = 'REVOKED' WHERE id = $1::uuid AND organization_id = $2::uuid`, workerID, orgID)
		if err != nil {
			return err
		}

		_, err = tx.Exec(ctx, `UPDATE worker_sessions SET revoked_at = clock_timestamp() WHERE worker_id = $1::uuid AND organization_id = $2::uuid`, workerID, orgID)
		if err != nil {
			return err
		}
		return nil
	})
}

// DrainWorker marks a worker DRAINING so it finishes active attempts but claims no new work.
func (s *Service) DrainWorker(ctx context.Context, orgID, workerID string) error {
	return s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE workers SET status = 'DRAINING' WHERE id = $1::uuid AND organization_id = $2::uuid`, workerID, orgID)
		return err
	})
}
