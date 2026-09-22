package worker

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrUnauthorized          = errors.New("UNAUTHORIZED: Invalid worker session or credentials")
	ErrSessionExpired        = errors.New("SESSION_EXPIRED: Worker session has expired")
	ErrSessionRevoked        = errors.New("SESSION_REVOKED: Worker session has been revoked")
	ErrWorkerRevoked         = errors.New("WORKER_REVOKED: Worker has been revoked")
	ErrStaleOwnership        = errors.New("STALE_OWNERSHIP: Attempt ownership epoch or session does not match active lease")
	ErrStartDeadlineExceeded = errors.New("START_DEADLINE_EXCEEDED: Start request was not received within 5s of claim")
	ErrChallengeExpired      = errors.New("CHALLENGE_EXPIRED: Challenge nonce is invalid, expired, or already used")
	ErrChallengeInvalid      = errors.New("CHALLENGE_INVALID: Specify exactly one valid workerId or publicKey")
	ErrEnrollmentInvalid     = errors.New("ENROLLMENT_TOKEN_INVALID: Enrollment token is invalid, expired, or already used")
	ErrWorkerNotFound        = errors.New("WORKER_NOT_FOUND: Worker identity not found")
	ErrAttemptNotFound       = errors.New("ATTEMPT_NOT_FOUND: Attempt not found or outside session scope")
	ErrLeaseExpiredServer    = errors.New("LEASE_EXPIRED: Lease has expired on the control plane")
	ErrResultConflict        = errors.New("RESULT_CONFLICT: Attempt already has a different terminal result")
	ErrInvalidOutcome        = errors.New("INVALID_OUTCOME: Attempt outcome is not supported")
	ErrPayloadTooLarge       = errors.New("PAYLOAD_TOO_LARGE: Inline result exceeds 256 KiB")
)

const MaxInlinePayloadBytes = 256 << 10

const (
	SessionTTL         = 15 * time.Minute
	DefaultLeaseTTL    = 30 * time.Second
	HeartbeatInterval  = 5 * time.Second
	ClaimStartDeadline = 5 * time.Second
	DefaultPollTimeout = 20 * time.Second
	DefaultSlots       = 2
	DrainGracePeriod   = 60 * time.Second
)

// WorkerSessionContext contains authoritative scope derived from an authenticated session.
type WorkerSessionContext struct {
	SessionID      string
	WorkerID       string
	OrganizationID string
	EnvironmentID  string
	PoolName       string
	ExpiresAt      time.Time
}

// SessionTokenPrefix marks worker session bearer tokens, distinguishing
// them from tenant API keys and human CLI sessions for auth dispatch.
const SessionTokenPrefix = "dbs_"

// IsSessionTokenFormat reports whether raw (without the Bearer scheme) has
// worker session token shape. It performs no cryptographic verification.
func IsSessionTokenFormat(raw string) bool {
	return strings.HasPrefix(strings.TrimSpace(raw), SessionTokenPrefix)
}

// GenerateSessionToken produces a 32-byte cryptographically secure session token
// and its SHA-256 hash.
func GenerateSessionToken() (rawToken string, tokenHash string, err error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", "", fmt.Errorf("crypto rand session token: %w", err)
	}
	rawToken = "dbs_" + hex.EncodeToString(b)
	digest := sha256.Sum256([]byte(strings.TrimSpace(rawToken)))
	tokenHash = hex.EncodeToString(digest[:])
	return rawToken, tokenHash, nil
}
