package artifacts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

// HTTP boundary for scoped artifacts. Two caller families share the routes:
//   - worker sessions (Bearer dbs_…): bound to their environment and to the
//     attempt ownership named in the body;
//   - tenant callers (API keys, human CLI sessions, BFF cookies): gated by
//     the route capability (artifacts:write / payload:read) plus explicit
//     environment matching below.
//
// Presigned URLs carry signature material and are never logged.

type CreateRequestDTO struct {
	RunID          string `json:"runId"`
	AttemptID      string `json:"attemptId"`
	OwnershipEpoch int64  `json:"ownershipEpoch"`
	SizeBytes      int64  `json:"sizeBytes"`
	SHA256         string `json:"sha256"`
}

type CreateResponseDTO struct {
	ID        string `json:"id"`
	UploadURL string `json:"uploadUrl"`
	ExpiresAt string `json:"expiresAt"`
}

type FinalizeRequestDTO struct {
	AttemptID      string `json:"attemptId"`
	OwnershipEpoch int64  `json:"ownershipEpoch"`
	SHA256         string `json:"sha256"`
}

type FinalizeResponseDTO struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type DownloadResponseDTO struct {
	ID          string `json:"id"`
	DownloadURL string `json:"downloadUrl"`
	ExpiresAt   string `json:"expiresAt"`
}

type caller struct {
	orgID     string
	envID     string
	sessionID *string
	human     bool
}

type HTTPHandler struct {
	service *Service
	tenants *tenant.Service
}

func NewHTTPHandler(s *Service, tenants *tenant.Service) *HTTPHandler {
	return &HTTPHandler{service: s, tenants: tenants}
}

// resolveCaller prefers an authenticated worker session when present,
// otherwise falls back to the tenant caller installed by RequireAuth.
func resolveCaller(r *http.Request) (caller, bool) {
	if sess, ok := worker.WorkerSessionFromContext(r.Context()); ok && sess != nil {
		return caller{orgID: sess.OrganizationID, envID: sess.EnvironmentID, sessionID: &sess.SessionID}, true
	}
	if id, ok := tenant.CallerFromContext(r.Context()); ok && id != nil {
		return caller{orgID: id.OrganizationID, envID: id.EnvironmentID, human: id.Type == tenant.IdentityTypeHuman}, true
	}
	return caller{}, false
}

// commandScope builds the durable command scope matching the tenant
// middleware for tenant callers and a stable worker scope for sessions.
// Tenant replay stays per-actor (key/user); worker replay stays per-worker so
// retries of the same logical publication replay instead of duplicating.
func commandScope(r *http.Request, c caller) (string, bool) {
	if sess, ok := worker.WorkerSessionFromContext(r.Context()); ok && sess != nil {
		if sess.OrganizationID == "" || sess.WorkerID == "" {
			return "", false
		}
		return "org:" + sess.OrganizationID + ":worker:" + sess.WorkerID, true
	}
	if id, ok := tenant.CallerFromContext(r.Context()); ok && id != nil {
		org := c.orgID
		if org == "" {
			org = id.OrganizationID
		}
		if id.Type == tenant.IdentityTypeMachine {
			if id.KeyID == "" || org == "" {
				return "", false
			}
			return "org:" + org + ":key:" + id.KeyID, true
		}
		if id.UserID == "" || org == "" {
			return "", false
		}
		return "org:" + org + ":user:" + id.UserID, true
	}
	return "", false
}

func normalizedCommandPath(r *http.Request) string {
	path := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/api/v1"), "/v1")
	if query := r.URL.Query().Encode(); query != "" {
		path += "?" + query
	}
	if path == "" {
		path = "/"
	}
	return path
}

func readRawBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		errJSON(w, r, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "Request body exceeds transport limit")
		return nil, false
	}
	return raw, true
}

func decodeRaw(w http.ResponseWriter, r *http.Request, raw []byte, dst any) bool {
	parsed, err := contracts.ParseJSON(raw)
	if err != nil {
		errJSON(w, r, http.StatusBadRequest, "INVALID_REQUEST", "Invalid request body")
		return false
	}
	canonical, _ := json.Marshal(parsed)
	if err := json.NewDecoder(bytes.NewReader(canonical)).Decode(dst); err != nil {
		errJSON(w, r, http.StatusBadRequest, "INVALID_REQUEST", "Invalid request body")
		return false
	}
	return true
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	raw, ok := readRawBody(w, r)
	if !ok {
		return false
	}
	return decodeRaw(w, r, raw, dst)
}

func writeIdempotencyConflict(w http.ResponseWriter, r *http.Request) {
	errJSON(w, r, http.StatusConflict, "IDEMPOTENCY_CONFLICT", "Idempotency-Key was already used with different request content")
}

// scopeEnv enforces the environment boundary without leaking other scopes:
// worker and machine callers must match the artifact environment exactly;
// organization members (route-checked) pass. Mismatches read as not found.
func scopeEnv(env, callerEnv string, human bool) bool {
	if human {
		return true
	}
	return env != "" && env == callerEnv
}

func writeServiceError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ErrArtifactNotFound):
		errJSON(w, r, http.StatusNotFound, "ARTIFACT_NOT_FOUND", "Artifact not found")
	case errors.Is(err, ErrArtifactExpired):
		errJSON(w, r, http.StatusNotFound, "ARTIFACT_EXPIRED", "Artifact is no longer retained")
	case errors.Is(err, ErrArtifactNotReady):
		errJSON(w, r, http.StatusConflict, "ARTIFACT_NOT_READY", "Artifact is not finalized")
	case errors.Is(err, ErrNotOwned):
		errJSON(w, r, http.StatusConflict, "NOT_OWNED", "Attempt does not own this artifact operation")
	case errors.Is(err, ErrTooLarge):
		errJSON(w, r, http.StatusRequestEntityTooLarge, "ARTIFACT_TOO_LARGE", err.Error())
	case errors.Is(err, ErrQuotaExceeded):
		errJSON(w, r, 429, "STORAGE_QUOTA_EXCEEDED", err.Error())
	case errors.Is(err, ErrSizeMismatch):
		errJSON(w, r, http.StatusUnprocessableEntity, "SIZE_MISMATCH", err.Error())
	case errors.Is(err, ErrChecksumMismatch):
		errJSON(w, r, http.StatusUnprocessableEntity, "CHECKSUM_MISMATCH", err.Error())
	case errors.Is(err, ErrReservationMismatch):
		errJSON(w, r, http.StatusConflict, "RESERVATION_MISMATCH", err.Error())
	case errors.Is(err, ErrObjectNotFound):
		errJSON(w, r, http.StatusUnprocessableEntity, "OBJECT_NOT_FOUND", "Uploaded object is absent from storage")
	case errors.Is(err, ErrStoreUnavailable):
		errJSON(w, r, http.StatusServiceUnavailable, "ARTIFACT_STORE_UNAVAILABLE", err.Error())
	default:
		errJSON(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal server error")
	}
}

// Create reserves an upload: it enforces caps/quota, binds live attempt
// ownership, and mints the single-object presigned PUT. It participates in
// the canonical durable command system: same key + same body replays the
// recorded reservation without a second row or quota charge; same key +
// different body conflicts.
func (h *HTTPHandler) Create(w http.ResponseWriter, r *http.Request) {
	c, ok := resolveCaller(r)
	if !ok {
		errJSON(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication required")
		return
	}
	idemKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if idemKey == "" {
		errJSON(w, r, http.StatusBadRequest, "MISSING_IDEMPOTENCY_KEY", "Idempotency-Key header is required")
		return
	}
	raw, ok := readRawBody(w, r)
	if !ok {
		return
	}
	var req CreateRequestDTO
	if !decodeRaw(w, r, raw, &req) {
		return
	}
	if req.RunID == "" || req.AttemptID == "" {
		errJSON(w, r, http.StatusBadRequest, "INVALID_REQUEST", "runId and attemptId are required")
		return
	}
	expectedEnv := ""
	if !c.human {
		expectedEnv = c.envID
	}
	cmdPath := normalizedCommandPath(r)
	cmdOp := r.Method + " " + cmdPath
	cmdFP := tenant.RequestFingerprint(r.Method, cmdPath, raw)
	scope, ok := commandScope(r, c)
	if !ok || h.tenants == nil {
		errJSON(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal server error")
		return
	}
	cmdCtx := tenant.ContextWithCommand(r.Context(), tenant.Command{
		Scope: scope, Key: idemKey, Fingerprint: cmdFP, Operation: cmdOp,
	})
	var resp CreateResponseDTO
	var artEnv string
	mutate := func(ctx context.Context, tx storage.Tx) error {
		art, uploadURL, expiresAt, err := h.service.ReserveTx(
			ctx, tx, c.orgID, req.RunID, req.AttemptID,
			req.OwnershipEpoch, req.SizeBytes, req.SHA256, c.sessionID, expectedEnv,
		)
		if err != nil {
			return err
		}
		artEnv = art.EnvironmentID
		resp = CreateResponseDTO{
			ID: art.ID, UploadURL: uploadURL, ExpiresAt: expiresAt.UTC().Format(time.RFC3339Nano),
		}
		return nil
	}
	outcome := func() any { return &resp }
	replay := func(raw json.RawMessage) error { return json.Unmarshal(raw, &resp) }
	_, err := h.tenants.WithCommandTx(cmdCtx, c.orgID, "", http.StatusOK, mutate, outcome, replay)
	if err != nil {
		if errors.Is(err, tenant.ErrIdempotencyConflict) {
			writeIdempotencyConflict(w, r)
			return
		}
		writeServiceError(w, r, err)
		return
	}
	// New reservations enforce scope; replays are per-scope so the original
	// scope check still applies.
	if artEnv != "" && !scopeEnv(artEnv, c.envID, c.human) {
		errJSON(w, r, http.StatusNotFound, "ARTIFACT_NOT_FOUND", "Artifact scope not found")
		return
	}
	if resp.ID == "" {
		// Replay of a redacted outcome should never happen for artifacts, but
		// fail closed rather than emitting an empty reservation.
		errJSON(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal server error")
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// Finalize verifies the uploaded object size and SHA-256, then marks the
// artifact READY. Verification failures fail closed and leave the row
// pending for re-upload without recording a command. Successful READY
// transitions participate in the durable command system so same-key replays
// return the recorded READY outcome instead of RESERVATION_MISMATCH.
func (h *HTTPHandler) Finalize(w http.ResponseWriter, r *http.Request) {
	c, ok := resolveCaller(r)
	if !ok {
		errJSON(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication required")
		return
	}
	artifactID := r.PathValue("id")
	if artifactID == "" {
		errJSON(w, r, http.StatusBadRequest, "INVALID_ARTIFACT_ID", "Artifact ID is required")
		return
	}
	idemKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if idemKey == "" {
		errJSON(w, r, http.StatusBadRequest, "MISSING_IDEMPOTENCY_KEY", "Idempotency-Key header is required")
		return
	}
	raw, ok := readRawBody(w, r)
	if !ok {
		return
	}
	var req FinalizeRequestDTO
	if !decodeRaw(w, r, raw, &req) {
		return
	}
	if req.AttemptID == "" {
		errJSON(w, r, http.StatusBadRequest, "INVALID_REQUEST", "attemptId is required")
		return
	}
	expectedEnv := ""
	if !c.human {
		expectedEnv = c.envID
	}
	cmdPath := normalizedCommandPath(r)
	cmdOp := r.Method + " " + cmdPath
	cmdFP := tenant.RequestFingerprint(r.Method, cmdPath, raw)
	scope, ok := commandScope(r, c)
	if !ok || h.tenants == nil {
		errJSON(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal server error")
		return
	}
	// Fast replay without provider I/O: a completed command with the same
	// identity returns its READY outcome; a fingerprint mismatch conflicts.
	if completed, err := h.tenants.GetCompletedCommand(r.Context(), c.orgID, scope, idemKey); err == nil && completed != nil {
		if completed.Fingerprint != cmdFP || completed.Operation != cmdOp {
			writeIdempotencyConflict(w, r)
			return
		}
		var replayed FinalizeResponseDTO
		if err := json.Unmarshal(completed.Outcome, &replayed); err != nil {
			errJSON(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal server error")
			return
		}
		// Scope for replays is per-actor, so the original environment check
		// still holds; verify the row remains visible before returning.
		if _, env, _, derr := h.service.DownloadURL(r.Context(), c.orgID, replayed.ID); derr == nil {
			if !scopeEnv(env, c.envID, c.human) {
				errJSON(w, r, http.StatusNotFound, "ARTIFACT_NOT_FOUND", "Artifact scope not found")
				return
			}
		}
		writeJSON(w, completed.ResponseCode, replayed)
		return
	}
	// Snapshot + provider verification outside any command transaction.
	snapshot, err := h.service.SnapshotForFinalize(
		r.Context(), c.orgID, artifactID, req.AttemptID,
		req.OwnershipEpoch, req.SHA256, c.sessionID, expectedEnv,
	)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	if verr := h.service.VerifySnapshot(r.Context(), snapshot); verr != nil {
		writeServiceError(w, r, verr)
		return
	}
	cmdCtx := tenant.ContextWithCommand(r.Context(), tenant.Command{
		Scope: scope, Key: idemKey, Fingerprint: cmdFP, Operation: cmdOp,
	})
	var resp FinalizeResponseDTO
	mutate := func(ctx context.Context, tx storage.Tx) error {
		art, err := h.service.TransitionForCommand(ctx, tx, c.orgID, artifactID, req.AttemptID, req.OwnershipEpoch, c.sessionID, expectedEnv, snapshot)
		if err != nil {
			return err
		}
		if !scopeEnv(art.EnvironmentID, c.envID, c.human) {
			return ErrArtifactNotFound
		}
		resp = FinalizeResponseDTO{ID: art.ID, Status: art.Status}
		return nil
	}
	outcome := func() any { return &resp }
	replay := func(raw json.RawMessage) error { return json.Unmarshal(raw, &resp) }
	_, err = h.tenants.WithCommandTx(cmdCtx, c.orgID, "", http.StatusOK, mutate, outcome, replay)
	if err != nil {
		if errors.Is(err, tenant.ErrIdempotencyConflict) {
			writeIdempotencyConflict(w, r)
			return
		}
		writeServiceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// Download mints a short-lived attachment download URL for a READY artifact.
// The bytes themselves are never proxied through the control plane and never
// execute in the dashboard origin.
func (h *HTTPHandler) Download(w http.ResponseWriter, r *http.Request) {
	c, ok := resolveCaller(r)
	if !ok {
		errJSON(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication required")
		return
	}
	artifactID := r.PathValue("id")
	if artifactID == "" {
		errJSON(w, r, http.StatusBadRequest, "INVALID_ARTIFACT_ID", "Artifact ID is required")
		return
	}
	url, env, expiresAt, err := h.service.DownloadURL(r.Context(), c.orgID, artifactID)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	if !scopeEnv(env, c.envID, c.human) {
		errJSON(w, r, http.StatusNotFound, "ARTIFACT_NOT_FOUND", "Artifact scope not found")
		return
	}
	writeJSON(w, http.StatusOK, DownloadResponseDTO{
		ID: artifactID, DownloadURL: url, ExpiresAt: expiresAt.UTC().Format(time.RFC3339Nano),
	})
}

func writeJSON(w http.ResponseWriter, s int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(s)
	_ = json.NewEncoder(w).Encode(v)
}

func errJSON(w http.ResponseWriter, r *http.Request, s int, c, m string) {
	writeJSON(w, s, map[string]any{
		"code":      c,
		"message":   m,
		"requestId": tenant.RequestIDFromContext(r.Context()),
		"details":   map[string]any{},
		"retryable": s >= 500,
	})
}
