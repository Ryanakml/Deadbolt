package artifacts

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
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

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		errJSON(w, r, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "Request body exceeds transport limit")
		return false
	}
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
// ownership, and mints the single-object presigned PUT.
func (h *HTTPHandler) Create(w http.ResponseWriter, r *http.Request) {
	c, ok := resolveCaller(r)
	if !ok {
		errJSON(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication required")
		return
	}
	var req CreateRequestDTO
	if !decodeJSON(w, r, &req) {
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
	art, uploadURL, expiresAt, err := h.service.Reserve(
		r.Context(), c.orgID, req.RunID, req.AttemptID,
		req.OwnershipEpoch, req.SizeBytes, req.SHA256, c.sessionID, expectedEnv,
	)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	if !scopeEnv(art.EnvironmentID, c.envID, c.human) {
		errJSON(w, r, http.StatusNotFound, "ARTIFACT_NOT_FOUND", "Artifact scope not found")
		return
	}
	writeJSON(w, http.StatusOK, CreateResponseDTO{
		ID: art.ID, UploadURL: uploadURL, ExpiresAt: expiresAt.UTC().Format(time.RFC3339Nano),
	})
}

// Finalize verifies the uploaded object size and SHA-256, then marks the
// artifact READY. Verification failures fail closed and leave the row
// pending for re-upload.
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
	var req FinalizeRequestDTO
	if !decodeJSON(w, r, &req) {
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
	art, err := h.service.Finalize(
		r.Context(), c.orgID, artifactID, req.AttemptID,
		req.OwnershipEpoch, req.SHA256, c.sessionID, expectedEnv,
	)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	if !scopeEnv(art.EnvironmentID, c.envID, c.human) {
		errJSON(w, r, http.StatusNotFound, "ARTIFACT_NOT_FOUND", "Artifact scope not found")
		return
	}
	writeJSON(w, http.StatusOK, FinalizeResponseDTO{ID: art.ID, Status: art.Status})
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
