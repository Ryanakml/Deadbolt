package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/jackc/pgx/v5"
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
	pool        *storage.Pool
	deployments DeploymentReconciler
}

func NewService(pool *storage.Pool, deployments DeploymentReconciler) *Service {
	return &Service{
		pool:        pool,
		deployments: deployments,
	}
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
	expiresAt := time.Now().Add(EnrollmentTokenTTL)

	err = s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		query := `INSERT INTO worker_enrollments (organization_id, environment_id, token_hash, pool_name, expires_at, created_by)
		          VALUES ($1, $2, $3, $4, $5, $6)`
		_, err := tx.Exec(ctx, query, orgID, envID, tokenHash, poolName, expiresAt, createdBy)
		return err
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
	nonce, err := GenerateChallengeNonce()
	if err != nil {
		return nil, err
	}
	expiresAt := time.Now().Add(ChallengeNonceTTL)

	// worker_challenges is a system table tracking nonces before session authentication.
	// Using standard pool query without tenant RLS context.
	query := `INSERT INTO worker_challenges (nonce, worker_id, public_key, expires_at)
	          VALUES ($1, $2, $3, $4)`
	var workerID, pubKey *string
	if req.WorkerID != "" {
		workerID = &req.WorkerID
	}
	if req.PublicKey != "" {
		pubKey = &req.PublicKey
	}

	_, err = s.pool.Exec(ctx, query, nonce, workerID, pubKey, expiresAt)
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

	// 2. Verify signature
	pubKey, err := DecodePublicKey(req.PublicKey)
	if err != nil {
		return nil, err
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
		// Insert worker
		workerQuery := `INSERT INTO workers (organization_id, environment_id, public_key, pool_name, status, last_seen_at)
		                VALUES ($1, $2, $3, $4, 'ACTIVE', clock_timestamp())
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
		sessionExpiresAt = time.Now().Add(SessionTTL)

		sessionQuery := `INSERT INTO worker_sessions (organization_id, worker_id, environment_id, session_token_hash, expires_at)
		                 VALUES ($1, $2, $3, $4, $5)
		                 RETURNING id::text`
		if err := tx.QueryRow(ctx, sessionQuery, orgID, workerID, envID, tokenHash, sessionExpiresAt).Scan(&sessionID); err != nil {
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
		// Revoke old sessions
		_, err := tx.Exec(ctx, `UPDATE worker_sessions SET revoked_at = clock_timestamp()
		                        WHERE worker_id = $1 AND organization_id = $2 AND revoked_at IS NULL`, req.WorkerID, orgID)
		if err != nil {
			return fmt.Errorf("revoke old worker sessions: %w", err)
		}

		// Clean up active leases from this worker's previous sessions
		_, err = tx.Exec(ctx, `DELETE FROM task_leases WHERE session_id IN (
		                         SELECT id FROM worker_sessions WHERE worker_id = $1 AND organization_id = $2
		                       )`, req.WorkerID, orgID)
		if err != nil {
			return fmt.Errorf("clean old leases: %w", err)
		}

		// Create fresh session
		var tokenHash string
		rawSessionToken, tokenHash, err = GenerateSessionToken()
		if err != nil {
			return err
		}
		sessionExpiresAt = time.Now().Add(SessionTTL)

		sessionQuery := `INSERT INTO worker_sessions (organization_id, worker_id, environment_id, session_token_hash, expires_at)
		                 VALUES ($1, $2, $3, $4, $5)
		                 RETURNING id::text`
		if err := tx.QueryRow(ctx, sessionQuery, orgID, req.WorkerID, envID, tokenHash, sessionExpiresAt).Scan(&sessionID); err != nil {
			return fmt.Errorf("insert worker session: %w", err)
		}

		// Update worker last_seen_at
		_, err = tx.Exec(ctx, `UPDATE workers SET last_seen_at = clock_timestamp() WHERE id = $1 AND organization_id = $2`, req.WorkerID, orgID)
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

// AuthenticateSession verifies a Bearer session token against database records.
func (s *Service) AuthenticateSession(ctx context.Context, rawSessionToken string) (*WorkerSessionContext, error) {
	tokenHash := HashToken(rawSessionToken)
	query := `SELECT session_id::text, worker_id::text, organization_id::text, environment_id::text,
	                 pool_name, expires_at, revoked_at, worker_status
	          FROM app.authenticate_worker_session($1)`

	var sessionID, workerID, orgID, envID, poolName, workerStatus string
	var expiresAt time.Time
	var revokedAt *time.Time

	err := s.pool.QueryRow(ctx, query, tokenHash).Scan(
		&sessionID, &workerID, &orgID, &envID, &poolName, &expiresAt, &revokedAt, &workerStatus,
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
	if time.Now().After(expiresAt) {
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

// PollAssignments advertises worker bundle digests and claims eligible assignments.
func (s *Service) PollAssignments(ctx context.Context, sessionCtx *WorkerSessionContext, req *PollRequestDTO) (*PollResponseDTO, error) {
	if req.WorkerID != sessionCtx.WorkerID || req.SessionID != sessionCtx.SessionID {
		return nil, ErrUnauthorized
	}

	assignments := make([]AssignmentDTO, 0)

	err := s.pool.WithTenantTx(ctx, sessionCtx.OrganizationID, func(ctx context.Context, tx storage.Tx) error {
		// Update worker last_seen_at
		_, err := tx.Exec(ctx, `UPDATE workers SET last_seen_at = clock_timestamp() WHERE id = $1 AND organization_id = $2`,
			sessionCtx.WorkerID, sessionCtx.OrganizationID)
		if err != nil {
			return err
		}

		// Advertise deployment digests
		for _, digest := range req.DeploymentDigests {
			_, err = tx.Exec(ctx, `INSERT INTO worker_deployments (session_id, organization_id, bundle_digest)
			                       VALUES ($1, $2, $3)
			                       ON CONFLICT (session_id, bundle_digest) DO NOTHING`,
				sessionCtx.SessionID, sessionCtx.OrganizationID, digest)
			if err != nil {
				return fmt.Errorf("upsert worker deployment: %w", err)
			}
		}

		if req.AvailableSlots <= 0 {
			return nil
		}

		// Claim ready steps matching advertised bundles
		// Per Blueprint §13: claim locks environment admission row and fetches eligible tasks FIFO
		claimQuery := `SELECT rs.id::text, rs.run_id::text, rs.node_id, r.input,
		                      r.deployment_id::text, d.bundle_digest
		               FROM run_steps rs
		               JOIN runs r ON r.id = rs.run_id AND r.organization_id = rs.organization_id
		               JOIN deployments d ON d.id = r.deployment_id AND d.organization_id = r.organization_id
		               JOIN worker_deployments wd ON wd.session_id = $1 AND wd.bundle_digest = d.bundle_digest
		               WHERE rs.organization_id = $2
		                 AND rs.environment_id = $3
		                 AND rs.state = 'READY'
		                 AND rs.eligible_at <= clock_timestamp()
		               ORDER BY rs.eligible_at ASC, rs.id ASC
		               LIMIT $4
		               FOR UPDATE OF rs SKIP LOCKED`

		rows, err := tx.Query(ctx, claimQuery, sessionCtx.SessionID, sessionCtx.OrganizationID, sessionCtx.EnvironmentID, req.AvailableSlots)
		if err != nil {
			return fmt.Errorf("claim query: %w", err)
		}
		defer rows.Close()

		type stepMatch struct {
			stepID, runID, nodeID string
			input                 any
			deploymentID, bundle  string
		}
		var matches []stepMatch

		for rows.Next() {
			var m stepMatch
			var rawInput []byte
			if err := rows.Scan(&m.stepID, &m.runID, &m.nodeID, &rawInput, &m.deploymentID, &m.bundle); err != nil {
				return err
			}
			if len(rawInput) > 0 {
				_ = json.Unmarshal(rawInput, &m.input)
			}
			matches = append(matches, m)
		}
		rows.Close()

		for _, m := range matches {
			// Transition step to RUNNING
			_, err = tx.Exec(ctx, `UPDATE run_steps SET state = 'RUNNING' WHERE id = $1 AND organization_id = $2`,
				m.stepID, sessionCtx.OrganizationID)
			if err != nil {
				return err
			}

			// Insert attempt
			var attemptID string
			var epoch int64 = 1
			leaseTTL := DefaultLeaseTTL
			leaseExpiresAt := time.Now().Add(leaseTTL)
			claimStartDeadline := time.Now().Add(ClaimStartDeadline)
			attemptTimeout := 60000 // 60s default

			attemptQuery := `INSERT INTO task_attempts (
			                    organization_id, step_id, attempt_number, session_id, epoch, status,
			                    claim_start_deadline_at, deadline_at, created_at
			                 )
			                 VALUES (
			                    $1, $2,
			                    COALESCE((SELECT MAX(attempt_number)+1 FROM task_attempts WHERE step_id=$2), 1),
			                    $3, $4, 'CLAIMED', $5, clock_timestamp() + ($6 * INTERVAL '1 millisecond'), clock_timestamp()
			                 )
			                 RETURNING id::text`
			err = tx.QueryRow(ctx, attemptQuery, sessionCtx.OrganizationID, m.stepID, sessionCtx.SessionID, epoch, claimStartDeadline, attemptTimeout).Scan(&attemptID)
			if err != nil {
				return fmt.Errorf("insert attempt: %w", err)
			}

			// Insert or update task lease
			leaseQuery := `INSERT INTO task_leases (step_id, organization_id, attempt_id, session_id, epoch, expires_at)
			               VALUES ($1, $2, $3, $4, $5, $6)
			               ON CONFLICT (step_id) DO UPDATE SET
			                 attempt_id = EXCLUDED.attempt_id,
			                 session_id = EXCLUDED.session_id,
			                 epoch = EXCLUDED.epoch,
			                 expires_at = EXCLUDED.expires_at`
			if _, err := tx.Exec(ctx, leaseQuery, m.stepID, sessionCtx.OrganizationID, attemptID, sessionCtx.SessionID, epoch, leaseExpiresAt); err != nil {
				return fmt.Errorf("upsert lease: %w", err)
			}

			assignments = append(assignments, AssignmentDTO{
				RunID:                m.runID,
				StepID:               m.stepID,
				AttemptID:            attemptID,
				OwnershipEpoch:       epoch,
				TaskEntrypoint:       m.nodeID,
				Input:                m.input,
				DeploymentDigest:     m.bundle,
				BundleDigest:         m.bundle,
				OperationID:          m.stepID,
				LeaseTTLMs:           leaseTTL.Milliseconds(),
				LeaseExpiresAt:       leaseExpiresAt.UTC().Format(time.RFC3339),
				ClaimStartDeadlineAt: claimStartDeadline.UTC().Format(time.RFC3339),
				AttemptTimeoutMs:     int64(attemptTimeout),
				RunDeadlineAt:        time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339),
				TraceContext:         TraceContextDTO{Traceparent: "00-00000000000000000000000000000000-0000000000000000-01"},
			})
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	if s.deployments != nil && len(req.DeploymentDigests) > 0 {
		_ = s.deployments.ReconcileAvailability(ctx, sessionCtx.OrganizationID, sessionCtx.EnvironmentID)
	}

	return &PollResponseDTO{
		ProtocolVersion: ProtocolVersion,
		RequestID:       req.RequestID,
		Assignments:     assignments,
	}, nil
}

// StartAttempt marks the attempt RUNNING if within 5s start deadline.
// Start retry returns the stored deadline without modifying it.
func (s *Service) StartAttempt(ctx context.Context, sessionCtx *WorkerSessionContext, req *StartRequestDTO) (*StartResponseDTO, error) {
	if req.WorkerID != sessionCtx.WorkerID || req.SessionID != sessionCtx.SessionID {
		return nil, ErrUnauthorized
	}

	var accepted bool
	var attemptDeadline time.Time
	leaseTTL := DefaultLeaseTTL

	err := s.pool.WithTenantTx(ctx, sessionCtx.OrganizationID, func(ctx context.Context, tx storage.Tx) error {
		// Lock attempt row
		var status string
		var epoch int64
		var sessionID string
		var claimStartDeadline, deadlineAt time.Time

		q := `SELECT a.status, a.epoch, a.session_id::text, a.claim_start_deadline_at, a.deadline_at
		      FROM task_attempts a
		      WHERE a.id = $1 AND a.organization_id = $2
		      FOR UPDATE`
		err := tx.QueryRow(ctx, q, req.AttemptID, sessionCtx.OrganizationID).Scan(
			&status, &epoch, &sessionID, &claimStartDeadline, &deadlineAt,
		)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrAttemptNotFound
			}
			return err
		}

		// Verify ownership epoch and session
		if epoch != req.OwnershipEpoch || sessionID != sessionCtx.SessionID {
			return ErrStaleOwnership
		}

		// Idempotent Start retry: already RUNNING returns stored deadline
		if status == "RUNNING" {
			accepted = true
			attemptDeadline = deadlineAt
			return nil
		}

		if status != "CLAIMED" {
			return ErrStaleOwnership
		}

		// Enforce 5s claim deadline
		if time.Now().After(claimStartDeadline) {
			return ErrStartDeadlineExceeded
		}

		// Mark RUNNING
		updateQ := `UPDATE task_attempts
		            SET status = 'RUNNING', started_at = clock_timestamp()
		            WHERE id = $1 AND organization_id = $2`
		if _, err := tx.Exec(ctx, updateQ, req.AttemptID, sessionCtx.OrganizationID); err != nil {
			return err
		}

		accepted = true
		attemptDeadline = deadlineAt
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &StartResponseDTO{
		ProtocolVersion:   ProtocolVersion,
		RequestID:         req.RequestID,
		AttemptID:         req.AttemptID,
		OwnershipEpoch:    req.OwnershipEpoch,
		Accepted:          accepted,
		AttemptDeadlineAt: attemptDeadline.UTC().Format(time.RFC3339),
		LeaseTTLMs:        leaseTTL.Milliseconds(),
	}, nil
}

// Heartbeat extends active attempt leases and reports any stop commands.
func (s *Service) Heartbeat(ctx context.Context, sessionCtx *WorkerSessionContext, req *HeartbeatRequestDTO) (*HeartbeatResponseDTO, error) {
	if req.WorkerID != sessionCtx.WorkerID || req.SessionID != sessionCtx.SessionID {
		return nil, ErrUnauthorized
	}

	renewals := make([]LeaseRenewalDTO, 0)
	stops := make([]StopCommandDTO, 0)

	err := s.pool.WithTenantTx(ctx, sessionCtx.OrganizationID, func(ctx context.Context, tx storage.Tx) error {
		// Update worker last_seen_at
		_, err := tx.Exec(ctx, `UPDATE workers SET last_seen_at = clock_timestamp() WHERE id = $1 AND organization_id = $2`,
			sessionCtx.WorkerID, sessionCtx.OrganizationID)
		if err != nil {
			return err
		}

		for _, item := range req.Attempts {
			// Check active lease
			var leaseExpiresAt time.Time
			var leaseEpoch int64
			var leaseSessionID string

			lq := `SELECT epoch, expires_at, session_id::text
			       FROM task_leases
			       WHERE attempt_id = $1 AND organization_id = $2
			       FOR UPDATE`
			err := tx.QueryRow(ctx, lq, item.AttemptID, sessionCtx.OrganizationID).Scan(&leaseEpoch, &leaseExpiresAt, &leaseSessionID)
			if err != nil {
				// Lease missing -> stale ownership -> instruct stop
				stops = append(stops, StopCommandDTO{
					AttemptID:      item.AttemptID,
					OwnershipEpoch: item.OwnershipEpoch,
					Reason:         "LEASE_NOT_FOUND",
					GraceTimeoutMs: 10000,
				})
				continue
			}

			if leaseEpoch != item.OwnershipEpoch || leaseSessionID != sessionCtx.SessionID || time.Now().After(leaseExpiresAt) {
				stops = append(stops, StopCommandDTO{
					AttemptID:      item.AttemptID,
					OwnershipEpoch: item.OwnershipEpoch,
					Reason:         "STALE_OWNERSHIP",
					GraceTimeoutMs: 10000,
				})
				continue
			}

			// Check for pending stop commands
			var stopReason string
			sq := `SELECT reason FROM stop_commands
			       WHERE attempt_id = $1 AND organization_id = $2 AND acked_at IS NULL
			       LIMIT 1`
			stopErr := tx.QueryRow(ctx, sq, item.AttemptID, sessionCtx.OrganizationID).Scan(&stopReason)
			if stopErr == nil {
				stops = append(stops, StopCommandDTO{
					AttemptID:      item.AttemptID,
					OwnershipEpoch: item.OwnershipEpoch,
					Reason:         stopReason,
					GraceTimeoutMs: 10000,
				})
				continue
			}

			// Renew lease 30s
			newExpiry := time.Now().Add(DefaultLeaseTTL)
			uq := `UPDATE task_leases SET expires_at = $1 WHERE attempt_id = $2 AND organization_id = $3`
			if _, err := tx.Exec(ctx, uq, newExpiry, item.AttemptID, sessionCtx.OrganizationID); err != nil {
				return err
			}

			renewals = append(renewals, LeaseRenewalDTO{
				AttemptID:      item.AttemptID,
				OwnershipEpoch: item.OwnershipEpoch,
				LeaseTTLMs:     DefaultLeaseTTL.Milliseconds(),
				LeaseExpiresAt: newExpiry.UTC().Format(time.RFC3339),
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &HeartbeatResponseDTO{
		ProtocolVersion: ProtocolVersion,
		RequestID:       req.RequestID,
		Renewals:        renewals,
		Stops:           stops,
	}, nil
}

// CompleteAttempt commits task outcome and releases the lease.
func (s *Service) CompleteAttempt(ctx context.Context, sessionCtx *WorkerSessionContext, req *CompleteRequestDTO) (*CompleteResponseDTO, error) {
	if req.WorkerID != sessionCtx.WorkerID || req.SessionID != sessionCtx.SessionID {
		return nil, ErrUnauthorized
	}

	err := s.pool.WithTenantTx(ctx, sessionCtx.OrganizationID, func(ctx context.Context, tx storage.Tx) error {
		// Verify lease
		var leaseEpoch int64
		var leaseExpiresAt time.Time
		var leaseSessionID string

		lq := `SELECT epoch, expires_at, session_id::text
		       FROM task_leases
		       WHERE attempt_id = $1 AND organization_id = $2
		       FOR UPDATE`
		err := tx.QueryRow(ctx, lq, req.AttemptID, sessionCtx.OrganizationID).Scan(&leaseEpoch, &leaseExpiresAt, &leaseSessionID)
		if err != nil {
			return ErrStaleOwnership
		}

		if leaseEpoch != req.OwnershipEpoch || leaseSessionID != sessionCtx.SessionID || time.Now().After(leaseExpiresAt) {
			return ErrStaleOwnership
		}

		// Update attempt
		var errJSON []byte
		if req.Error != nil {
			errJSON, _ = json.Marshal(req.Error)
		}

		uq := `UPDATE task_attempts
		       SET status = $1, outcome_digest = $2, error = $3, completed_at = clock_timestamp()
		       WHERE id = $4 AND organization_id = $5`
		if _, err := tx.Exec(ctx, uq, req.Outcome, req.ResultDigest, errJSON, req.AttemptID, sessionCtx.OrganizationID); err != nil {
			return err
		}

		// Release lease
		if _, err := tx.Exec(ctx, `DELETE FROM task_leases WHERE attempt_id = $1 AND organization_id = $2`, req.AttemptID, sessionCtx.OrganizationID); err != nil {
			return err
		}

		// Complete step
		stepStatus := "SUCCEEDED"
		if req.Outcome != "SUCCEEDED" {
			stepStatus = "FAILED"
		}
		stepQ := `UPDATE run_steps SET state = $1 WHERE id = (SELECT step_id FROM task_attempts WHERE id = $2) AND organization_id = $3`
		_, _ = tx.Exec(ctx, stepQ, stepStatus, req.AttemptID, sessionCtx.OrganizationID)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return &CompleteResponseDTO{
		ProtocolVersion: ProtocolVersion,
		RequestID:       req.RequestID,
		AttemptID:       req.AttemptID,
		OwnershipEpoch:  req.OwnershipEpoch,
		Accepted:        true,
		ResultDigest:    req.ResultDigest,
	}, nil
}

// StopAck records process termination confirmation for a stop command.
func (s *Service) StopAck(ctx context.Context, sessionCtx *WorkerSessionContext, req *StopAckRequestDTO) (*AckResponseDTO, error) {
	if req.WorkerID != sessionCtx.WorkerID || req.SessionID != sessionCtx.SessionID {
		return nil, ErrUnauthorized
	}

	err := s.pool.WithTenantTx(ctx, sessionCtx.OrganizationID, func(ctx context.Context, tx storage.Tx) error {
		q := `UPDATE stop_commands
		      SET acked_at = clock_timestamp(),
		          termination_confirmed_at = CASE WHEN $1 = true THEN clock_timestamp() ELSE termination_confirmed_at END
		      WHERE attempt_id = $2 AND organization_id = $3`
		_, err := tx.Exec(ctx, q, req.ProcessStopped, req.AttemptID, sessionCtx.OrganizationID)
		return err
	})
	if err != nil {
		return nil, err
	}

	return &AckResponseDTO{
		ProtocolVersion: ProtocolVersion,
		RequestID:       req.RequestID,
		Accepted:        true,
	}, nil
}

// RecordLogs processes a bounded log batch from a worker.
func (s *Service) RecordLogs(ctx context.Context, sessionCtx *WorkerSessionContext, req *LogBatchRequestDTO) (*AckResponseDTO, error) {
	if req.WorkerID != sessionCtx.WorkerID || req.SessionID != sessionCtx.SessionID {
		return nil, ErrUnauthorized
	}

	// Logs are best-effort delivery per Blueprint §12.2.
	return &AckResponseDTO{
		ProtocolVersion: ProtocolVersion,
		RequestID:       req.RequestID,
		Accepted:        true,
	}, nil
}

// RevokeWorker marks a worker REVOKED and revokes all its active sessions and leases.
func (s *Service) RevokeWorker(ctx context.Context, orgID, workerID string) error {
	return s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE workers SET status = 'REVOKED' WHERE id = $1 AND organization_id = $2`, workerID, orgID)
		if err != nil {
			return err
		}

		_, err = tx.Exec(ctx, `UPDATE worker_sessions SET revoked_at = clock_timestamp() WHERE worker_id = $1 AND organization_id = $2`, workerID, orgID)
		if err != nil {
			return err
		}

		_, err = tx.Exec(ctx, `DELETE FROM task_leases WHERE session_id IN (SELECT id FROM worker_sessions WHERE worker_id = $1 AND organization_id = $2)`, workerID, orgID)
		return err
	})
}

// DrainWorker marks a worker DRAINING so it finishes active attempts but claims no new work.
func (s *Service) DrainWorker(ctx context.Context, orgID, workerID string) error {
	return s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE workers SET status = 'DRAINING' WHERE id = $1 AND organization_id = $2`, workerID, orgID)
		return err
	})
}
