package artifacts

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/jackc/pgx/v5"
)

// Artifact lifecycle and integrity contract (Blueprint §18.2).
//
//   - Inline JSON stays 256 KiB; larger payloads use typed references.
//   - One object is at most 100 MiB; an environment reserves at most 1 GiB.
//   - Upload URLs are single-object presigned PUTs valid for 5 minutes.
//   - Finalize verifies size and SHA-256 before any result may reference the
//     artifact. S3 upload is never inside a DB transaction.
//   - PENDING_UPLOAD → READY → (DELETED), or PENDING_UPLOAD → EXPIRED.
//     Unreferenced uploads are collected after a 24h grace period;
//     artifacts referenced by run outputs are retained.

const (
	// MaxObjectBytes is the initial per-object maximum (100 MiB).
	MaxObjectBytes = 100 << 20
	// InlineJSONLimitBytes mirrors the inline payload ceiling; larger
	// results must travel as artifact references instead of JSONB.
	InlineJSONLimitBytes = 256 << 10
	// EnvStorageQuotaBytes bounds pre-upload reservation per environment.
	EnvStorageQuotaBytes = 1 << 30
	// PresignPutTTL is the single-object upload window (5 minutes).
	PresignPutTTL = 5 * time.Minute
	// PresignGetTTL bounds scoped download URLs (5 minutes).
	PresignGetTTL = 5 * time.Minute
	// OrphanGracePeriod retains unreferenced uploads for 24h before GC.
	OrphanGracePeriod = 24 * time.Hour
)

var (
	ErrObjectNotFound      = errors.New("OBJECT_NOT_FOUND: Object is absent from storage")
	ErrArtifactNotFound    = errors.New("ARTIFACT_NOT_FOUND: Artifact not found")
	ErrArtifactNotReady    = errors.New("ARTIFACT_NOT_READY: Artifact is not finalized")
	ErrArtifactExpired     = errors.New("ARTIFACT_EXPIRED: Artifact is no longer retained")
	ErrTooLarge            = errors.New("ARTIFACT_TOO_LARGE: Object exceeds the 100 MiB maximum")
	ErrQuotaExceeded       = errors.New("STORAGE_QUOTA_EXCEEDED: Environment artifact quota is exhausted")
	ErrSizeMismatch        = errors.New("SIZE_MISMATCH: Stored object size differs from reservation")
	ErrChecksumMismatch    = errors.New("CHECKSUM_MISMATCH: Stored object SHA-256 differs from reservation")
	ErrReservationMismatch = errors.New("RESERVATION_MISMATCH: Finalize does not match the reservation")
	ErrNotOwned            = errors.New("NOT_OWNED: Attempt does not own this artifact operation")
	ErrStoreUnavailable    = errors.New("ARTIFACT_STORE_UNAVAILABLE: Object storage is not configured")
)

var sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ArtifactRef is the typed reference stored in step outputs for large
// results. The $artifact key is platform-reserved and never interpreted as
// an input-mapping descriptor when carried as a value.
const ArtifactRefKey = "$artifact"

// IsArtifactRef reports whether v is a typed artifact reference.
func IsArtifactRef(v any) (string, bool) {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return "", false
	}
	id, ok := m[ArtifactRefKey].(string)
	if !ok || id == "" {
		return "", false
	}
	return id, true
}

// CollectArtifactRefs walks arbitrary JSON and returns every artifact ID it
// references, so consumer admission can verify integrity before dispatch.
func CollectArtifactRefs(v any) []string {
	var out []string
	var walk func(any)
	walk = func(n any) {
		if id, ok := IsArtifactRef(n); ok {
			out = append(out, id)
			return
		}
		switch t := n.(type) {
		case map[string]any:
			for _, x := range t {
				walk(x)
			}
		case []any:
			for _, x := range t {
				walk(x)
			}
		}
	}
	walk(v)
	return out
}

// Artifact is the durable upload record.
type Artifact struct {
	ID            string
	RunID         string
	EnvironmentID string
	StepID        string
	AttemptID     *string
	StorageKey    string
	SizeBytes     int64
	SHA256        string
	Status        string
	ExpiresAt     *time.Time
	CreatedAt     time.Time
}

// Service owns artifact reservation, verification, scoped URLs, and GC.
type Service struct {
	pool  *storage.Pool
	store ObjectStore
}

// NewService builds the artifact service. A nil store fails closed with
// ErrStoreUnavailable on every operation.
func NewService(pool *storage.Pool, store ObjectStore) *Service {
	return &Service{pool: pool, store: store}
}

func randomStorageKey(environmentID, runID string) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/%s/%s", environmentID, runID, hex.EncodeToString(b[:])), nil
}

func validSHA(s string) bool {
	return sha256Pattern.MatchString(strings.ToLower(strings.TrimSpace(s)))
}

// Reserve creates the PENDING_UPLOAD record, enforces the 100 MiB object cap
// and the 1 GiB environment reservation, and mints the single-object upload
// URL. The attempt must be live and its epoch must match: only valid current
// ownership may reserve. The step and environment are derived from the
// authoritative attempt/run rows, never trusted from the caller.
func (s *Service) Reserve(
	ctx context.Context, orgID, runID, attemptID string,
	epoch int64, sizeBytes int64, sha string, sessionID *string, expectedEnv string,
) (*Artifact, string, time.Time, error) {
	if s.store == nil {
		return nil, "", time.Time{}, ErrStoreUnavailable
	}
	if sizeBytes < 0 || sizeBytes > MaxObjectBytes {
		return nil, "", time.Time{}, ErrTooLarge
	}
	if !validSHA(sha) {
		return nil, "", time.Time{}, fmt.Errorf("%w: sha256 must be 64 lowercase hex", ErrReservationMismatch)
	}
	sha = strings.ToLower(strings.TrimSpace(sha))

	var out *Artifact
	var uploadURL string
	var expiresAt time.Time
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		var attemptEpoch int64
		var attemptStatus, stepID, attemptRun, environmentID string
		var attemptSession *string
		err := tx.QueryRow(ctx, `SELECT a.epoch, a.status, a.step_id::text, rs.run_id::text, rs.environment_id::text,
				a.session_id::text
			FROM task_attempts a JOIN run_steps rs ON rs.id=a.step_id AND rs.organization_id=a.organization_id
			WHERE a.id=$1::uuid AND a.organization_id=$2::uuid`,
			attemptID, orgID).Scan(&attemptEpoch, &attemptStatus, &stepID, &attemptRun, &environmentID, &attemptSession)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotOwned
			}
			return err
		}
		if attemptRun != runID {
			return ErrReservationMismatch
		}
		if sessionID != nil && (attemptSession == nil || *attemptSession != *sessionID) {
			return ErrNotOwned
		}
		// Authoritative environment comes from the attempt's step; a
		// non-empty expectation (worker session, machine key scope) must
		// match before any row or quota is touched.
		if expectedEnv != "" && environmentID != expectedEnv {
			return ErrNotOwned
		}
		if attemptEpoch != epoch {
			return ErrNotOwned
		}
		if attemptStatus != "CLAIMED" && attemptStatus != "RUNNING" {
			return ErrNotOwned
		}
		var reserved int64
		if err := tx.QueryRow(ctx, `SELECT COALESCE(SUM(size_bytes),0) FROM artifacts
			WHERE organization_id=$1::uuid AND environment_id=$2::uuid
				AND status IN ('PENDING_UPLOAD','READY')`, orgID, environmentID).Scan(&reserved); err != nil {
			return err
		}
		if reserved+sizeBytes > EnvStorageQuotaBytes {
			return ErrQuotaExceeded
		}
		key, err := randomStorageKey(environmentID, runID)
		if err != nil {
			return err
		}
		var a Artifact
		var created time.Time
		err = tx.QueryRow(ctx, `INSERT INTO artifacts
			(organization_id, environment_id, run_id, step_id, attempt_id, storage_key, size_bytes, sha256_hash, status)
			VALUES ($1::uuid,$2::uuid,$3::uuid,$4::uuid,$5::uuid,$6,$7,$8,'PENDING_UPLOAD')
			RETURNING id::text, storage_key, size_bytes, sha256_hash, status, created_at`,
			orgID, environmentID, runID, stepID, attemptID, key, sizeBytes, sha).Scan(
			&a.ID, &a.StorageKey, &a.SizeBytes, &a.SHA256, &a.Status, &created)
		if err != nil {
			return err
		}
		a.RunID, a.StepID, a.CreatedAt = runID, stepID, created
		a.EnvironmentID = environmentID
		a.AttemptID = &attemptID
		out = &a
		uploadURL, expiresAt, err = s.store.PresignPut(ctx, key, "application/octet-stream", PresignPutTTL)
		return err
	})
	if err != nil {
		return nil, "", time.Time{}, err
	}
	return out, uploadURL, expiresAt, nil
}

// Finalize verifies the uploaded object size and SHA-256, then marks the
// artifact READY. The finalizing attempt must still own the artifact; size
// or digest mismatches fail closed and leave the row pending for re-upload.
func (s *Service) Finalize(
	ctx context.Context, orgID, artifactID, attemptID string, epoch int64, sha string, sessionID *string, expectedEnv string,
) (*Artifact, error) {
	if s.store == nil {
		return nil, ErrStoreUnavailable
	}
	var out *Artifact
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		var a Artifact
		var attemptRef *string
		var attemptEpoch int64
		var attemptStatus string
		var attemptSession *string
		err := tx.QueryRow(ctx, `SELECT a.id::text, a.run_id::text, a.step_id::text, a.attempt_id::text,
				a.storage_key, a.size_bytes, a.sha256_hash, a.status, a.expires_at, a.created_at,
				ta.epoch, ta.status, ta.session_id::text, a.environment_id::text
			FROM artifacts a JOIN task_attempts ta ON ta.id=a.attempt_id AND ta.organization_id=a.organization_id
			WHERE a.id=$1::uuid AND a.organization_id=$2::uuid FOR UPDATE OF a`,
			artifactID, orgID).Scan(
			&a.ID, &a.RunID, &a.StepID, &attemptRef, &a.StorageKey, &a.SizeBytes, &a.SHA256,
			&a.Status, &a.ExpiresAt, &a.CreatedAt, &attemptEpoch, &attemptStatus, &attemptSession, &a.EnvironmentID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrArtifactNotFound
			}
			return err
		}
		if a.Status != "PENDING_UPLOAD" {
			return ErrReservationMismatch
		}
		if attemptRef == nil || *attemptRef != attemptID || attemptEpoch != epoch {
			return ErrNotOwned
		}
		if sessionID != nil && (attemptSession == nil || *attemptSession != *sessionID) {
			return ErrNotOwned
		}
		if expectedEnv != "" && a.EnvironmentID != expectedEnv {
			return ErrNotOwned
		}
		if attemptStatus != "CLAIMED" && attemptStatus != "RUNNING" {
			return ErrNotOwned
		}
		if !validSHA(sha) || strings.ToLower(strings.TrimSpace(sha)) != a.SHA256 {
			return ErrReservationMismatch
		}
		size, err := s.store.Stat(ctx, a.StorageKey)
		if err != nil {
			if errors.Is(err, ErrObjectNotFound) {
				return ErrObjectNotFound
			}
			return err
		}
		if size != a.SizeBytes {
			return ErrSizeMismatch
		}
		body, err := s.store.Fetch(ctx, a.StorageKey)
		if err != nil {
			return err
		}
		h := sha256.New()
		streamed, err := io.Copy(h, body)
		body.Close()
		if err != nil {
			return err
		}
		if streamed != a.SizeBytes || hex.EncodeToString(h.Sum(nil)) != a.SHA256 {
			return ErrChecksumMismatch
		}
		if _, err := tx.Exec(ctx, `UPDATE artifacts SET status='READY'
			WHERE id=$1::uuid AND organization_id=$2::uuid`, artifactID, orgID); err != nil {
			return err
		}
		a.Status = "READY"
		a.AttemptID = attemptRef
		out = &a
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DownloadURL mints a short-lived attachment download URL for a READY
// artifact and reports its environment so the caller enforces scope.
func (s *Service) DownloadURL(
	ctx context.Context, orgID, artifactID string,
) (url string, environmentID string, expiresAt time.Time, err error) {
	if s.store == nil {
		return "", "", time.Time{}, ErrStoreUnavailable
	}
	var key, status, env string
	txErr := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT storage_key, status, environment_id::text FROM artifacts
			WHERE id=$1::uuid AND organization_id=$2::uuid`,
			artifactID, orgID).Scan(&key, &status, &env)
	})
	if txErr != nil {
		if errors.Is(txErr, pgx.ErrNoRows) {
			return "", "", time.Time{}, ErrArtifactNotFound
		}
		return "", "", time.Time{}, txErr
	}
	switch status {
	case "READY":
	case "EXPIRED", "DELETED":
		return "", "", time.Time{}, ErrArtifactExpired
	default:
		return "", "", time.Time{}, ErrArtifactNotReady
	}
	filename := "artifact-" + artifactID + ".bin"
	url, expiresAt, err = s.store.PresignGet(ctx, key, filename, PresignGetTTL)
	if err != nil {
		return "", "", time.Time{}, err
	}
	return url, env, expiresAt, nil
}

// CollectGarbage deletes one bounded batch of safe orphans: unfinalized
// uploads past the 24h grace, and finalized artifacts past grace that no run
// output references. Referenced artifacts (including all active-run outputs)
// are retained. Object deletion is best-effort; row transitions commit.
func (s *Service) CollectGarbage(ctx context.Context, orgID string, batchSize int, now time.Time) (int, error) {
	if batchSize <= 0 {
		batchSize = 50
	}
	type victim struct {
		id, key, status string
	}
	var collected int
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		rows, err := tx.Query(ctx, `SELECT id::text, storage_key, status FROM artifacts
			WHERE organization_id=$1::uuid AND (
				(status='PENDING_UPLOAD' AND created_at < $2)
				OR (status='READY' AND created_at < $2
					AND NOT EXISTS (SELECT 1 FROM run_steps rs
						WHERE rs.organization_id=$1::uuid
							AND rs.output @> jsonb_build_object('$artifact', artifacts.id::text)))
			)
			ORDER BY created_at ASC, id ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED`, orgID, now.Add(-OrphanGracePeriod), batchSize)
		if err != nil {
			return err
		}
		victims := make([]victim, 0)
		for rows.Next() {
			var v victim
			if err := rows.Scan(&v.id, &v.key, &v.status); err != nil {
				rows.Close()
				return err
			}
			victims = append(victims, v)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		for _, v := range victims {
			// Best effort: a missing object is already the desired end state.
			if s.store != nil {
				_ = s.store.Delete(ctx, v.key)
			}
			next := "EXPIRED"
			if v.status == "READY" {
				next = "DELETED"
			}
			if _, err := tx.Exec(ctx, `UPDATE artifacts SET status=$1
				WHERE id=$2::uuid AND organization_id=$3::uuid`, next, v.id, orgID); err != nil {
				return err
			}
			collected++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return collected, nil
}

// LookupForCompletion loads a READY artifact for result association,
// enforcing step binding and current attempt ownership.
func (s *Service) LookupForCompletion(
	ctx context.Context, orgID, stepID, attemptID string, epoch int64, artifactID string,
) (*Artifact, error) {
	var a Artifact
	var attemptRef *string
	var attemptEpoch int64
	var attemptStatus string
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		err := tx.QueryRow(ctx, `SELECT a.id::text, a.run_id::text, a.step_id::text, a.attempt_id::text,
				a.storage_key, a.size_bytes, a.sha256_hash, a.status, a.expires_at, a.created_at,
				ta.epoch, ta.status
			FROM artifacts a JOIN task_attempts ta ON ta.id=a.attempt_id AND ta.organization_id=a.organization_id
			WHERE a.id=$1::uuid AND a.organization_id=$2::uuid`,
			artifactID, orgID).Scan(
			&a.ID, &a.RunID, &a.StepID, &attemptRef, &a.StorageKey, &a.SizeBytes, &a.SHA256,
			&a.Status, &a.ExpiresAt, &a.CreatedAt, &attemptEpoch, &attemptStatus)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrArtifactNotFound
			}
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if a.Status != "READY" {
		return nil, ErrArtifactNotReady
	}
	if a.StepID != stepID || attemptRef == nil || *attemptRef != attemptID || attemptEpoch != epoch {
		return nil, ErrNotOwned
	}
	if attemptStatus != "RUNNING" && attemptStatus != "CLAIMED" {
		return nil, ErrNotOwned
	}
	a.AttemptID = attemptRef
	return &a, nil
}

// ReadyArtifactIDsTx is the transaction-scoped variant used by engine
// admission paths that already hold locks.
func (s *Service) ReadyArtifactIDsTx(ctx context.Context, tx storage.Tx, orgID string, ids []string) (map[string]bool, error) {
	ready := make(map[string]bool, len(ids))
	if len(ids) == 0 {
		return ready, nil
	}
	rows, err := tx.Query(ctx, `SELECT id::text FROM artifacts
		WHERE organization_id=$1::uuid AND id = ANY($2::uuid[]) AND status='READY'`,
		orgID, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ready[id] = true
	}
	return ready, rows.Err()
}

// VerifyReferences enforces consumer integrity admission: every referenced
// artifact must be READY and its stored object must still exist at the
// reserved size. A missing or truncated object fails the consumer without
// rerunning the already-successful producer.
func (s *Service) VerifyReferences(ctx context.Context, orgID string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	type ref struct {
		id, key string
		size    int64
	}
	refs := make([]ref, 0, len(ids))
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		rows, err := tx.Query(ctx, `SELECT id::text, storage_key, size_bytes FROM artifacts
			WHERE organization_id=$1::uuid AND id = ANY($2::uuid[]) AND status='READY'`,
			orgID, ids)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var r ref
			if err := rows.Scan(&r.id, &r.key, &r.size); err != nil {
				return err
			}
			refs = append(refs, r)
		}
		return rows.Err()
	})
	if err != nil {
		return err
	}
	if len(refs) != len(ids) {
		return ErrArtifactNotFound
	}
	if s.store == nil {
		return ErrStoreUnavailable
	}
	for _, r := range refs {
		size, err := s.store.Stat(ctx, r.key)
		if err != nil {
			if errors.Is(err, ErrObjectNotFound) {
				return ErrObjectNotFound
			}
			return err
		}
		if size != r.size {
			return ErrSizeMismatch
		}
	}
	return nil
}

// ReadyArtifactIDs loads the READY artifact IDs for the given step IDs,
// used by consumer integrity admission.
func (s *Service) ReadyArtifactIDs(ctx context.Context, orgID string, ids []string) (map[string]bool, error) {
	ready := make(map[string]bool, len(ids))
	if len(ids) == 0 {
		return ready, nil
	}
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		got, err := s.ReadyArtifactIDsTx(ctx, tx, orgID, ids)
		if err != nil {
			return err
		}
		for id := range got {
			ready[id] = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ready, nil
}
