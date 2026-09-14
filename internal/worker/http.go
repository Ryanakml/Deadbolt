package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/Ryanakml/Deadbolt/internal/tenant"
)

type workerSessionCtxKey struct{}

func WithWorkerSession(ctx context.Context, session *WorkerSessionContext) context.Context {
	return context.WithValue(ctx, workerSessionCtxKey{}, session)
}

func WorkerSessionFromContext(ctx context.Context) (*WorkerSessionContext, bool) {
	s, ok := ctx.Value(workerSessionCtxKey{}).(*WorkerSessionContext)
	return s, ok
}

type HTTPHandler struct {
	service *Service
	tenants *tenant.Service
}

func NewHTTPHandler(service *Service, tenants *tenant.Service) *HTTPHandler {
	return &HTTPHandler{
		service: service,
		tenants: tenants,
	}
}

func (h *HTTPHandler) RegisterRoutes(mux *http.ServeMux) {
	// Worker protocol endpoints
	mux.HandleFunc("POST /worker/v1/challenge", h.HandleChallenge)
	mux.HandleFunc("POST /worker/v1/enroll", h.HandleEnroll)
	mux.HandleFunc("POST /worker/v1/session", h.HandleSession)
	mux.HandleFunc("POST /worker/v1/poll", h.RequireWorkerSession(h.HandlePoll))
	mux.HandleFunc("POST /worker/v1/start", h.RequireWorkerSession(h.HandleStart))
	mux.HandleFunc("POST /worker/v1/heartbeat", h.RequireWorkerSession(h.HandleHeartbeat))
	mux.HandleFunc("POST /worker/v1/complete", h.RequireWorkerSession(h.HandleComplete))
	mux.HandleFunc("POST /worker/v1/stop-ack", h.RequireWorkerSession(h.HandleStopAck))
	mux.HandleFunc("POST /worker/v1/logs", h.RequireWorkerSession(h.HandleLogs))
}

func (h *HTTPHandler) RequireWorkerSession(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			h.writeError(w, r, http.StatusUnauthorized, "UNAUTHORIZED", "Missing or invalid Bearer token", false)
			return
		}
		rawToken := strings.TrimPrefix(authHeader, "Bearer ")
		sessionCtx, err := h.service.AuthenticateSession(r.Context(), rawToken)
		if err != nil {
			if errors.Is(err, ErrWorkerRevoked) {
				h.writeError(w, r, http.StatusForbidden, "WORKER_REVOKED", "Worker has been revoked", false)
				return
			}
			if errors.Is(err, ErrSessionRevoked) {
				h.writeError(w, r, http.StatusUnauthorized, "SESSION_REVOKED", "Session has been revoked", false)
				return
			}
			if errors.Is(err, ErrSessionExpired) {
				h.writeError(w, r, http.StatusUnauthorized, "SESSION_EXPIRED", "Session has expired", false)
				return
			}
			h.writeError(w, r, http.StatusUnauthorized, "UNAUTHORIZED", "Invalid session token", false)
			return
		}

		ctx := WithWorkerSession(r.Context(), sessionCtx)
		next(w, r.WithContext(ctx))
	}
}

func (h *HTTPHandler) HandleChallenge(w http.ResponseWriter, r *http.Request) {
	var req ChallengeRequestDTO
	if err := h.readJSON(w, r, &req); err != nil {
		return
	}
	if req.ProtocolVersion != ProtocolVersion {
		h.writeError(w, r, http.StatusBadRequest, "INVALID_PROTOCOL_VERSION", "protocolVersion must be 1", false)
		return
	}

	res, err := h.service.CreateChallenge(r.Context(), &req)
	if err != nil {
		h.writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), true)
		return
	}
	h.writeJSON(w, http.StatusOK, res)
}

func (h *HTTPHandler) HandleEnroll(w http.ResponseWriter, r *http.Request) {
	var req EnrollRequestDTO
	if err := h.readJSON(w, r, &req); err != nil {
		return
	}
	if req.ProtocolVersion != ProtocolVersion {
		h.writeError(w, r, http.StatusBadRequest, "INVALID_PROTOCOL_VERSION", "protocolVersion must be 1", false)
		return
	}

	res, err := h.service.EnrollWorker(r.Context(), &req)
	if err != nil {
		if errors.Is(err, ErrChallengeExpired) {
			h.writeError(w, r, http.StatusBadRequest, "CHALLENGE_EXPIRED", err.Error(), false)
			return
		}
		if errors.Is(err, ErrInvalidSignature) {
			h.writeError(w, r, http.StatusUnauthorized, "INVALID_SIGNATURE", err.Error(), false)
			return
		}
		if errors.Is(err, ErrEnrollmentInvalid) {
			h.writeError(w, r, http.StatusUnauthorized, "ENROLLMENT_TOKEN_INVALID", err.Error(), false)
			return
		}
		h.writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), true)
		return
	}
	h.writeJSON(w, http.StatusOK, res)
}

func (h *HTTPHandler) HandleSession(w http.ResponseWriter, r *http.Request) {
	var req SessionRequestDTO
	if err := h.readJSON(w, r, &req); err != nil {
		return
	}
	if req.ProtocolVersion != ProtocolVersion {
		h.writeError(w, r, http.StatusBadRequest, "INVALID_PROTOCOL_VERSION", "protocolVersion must be 1", false)
		return
	}

	res, err := h.service.CreateSession(r.Context(), &req)
	if err != nil {
		if errors.Is(err, ErrChallengeExpired) {
			h.writeError(w, r, http.StatusBadRequest, "CHALLENGE_EXPIRED", err.Error(), false)
			return
		}
		if errors.Is(err, ErrWorkerNotFound) {
			h.writeError(w, r, http.StatusNotFound, "WORKER_NOT_FOUND", err.Error(), false)
			return
		}
		if errors.Is(err, ErrWorkerRevoked) {
			h.writeError(w, r, http.StatusForbidden, "WORKER_REVOKED", err.Error(), false)
			return
		}
		if errors.Is(err, ErrInvalidSignature) {
			h.writeError(w, r, http.StatusUnauthorized, "INVALID_SIGNATURE", err.Error(), false)
			return
		}
		h.writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), true)
		return
	}
	h.writeJSON(w, http.StatusOK, res)
}

func (h *HTTPHandler) HandlePoll(w http.ResponseWriter, r *http.Request) {
	sessionCtx, _ := WorkerSessionFromContext(r.Context())
	var req PollRequestDTO
	if err := h.readJSON(w, r, &req); err != nil {
		return
	}
	if req.ProtocolVersion != ProtocolVersion {
		h.writeError(w, r, http.StatusBadRequest, "INVALID_PROTOCOL_VERSION", "protocolVersion must be 1", false)
		return
	}

	res, err := h.service.PollAssignments(r.Context(), sessionCtx, &req)
	if err != nil {
		h.writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), true)
		return
	}
	h.writeJSON(w, http.StatusOK, res)
}

func (h *HTTPHandler) HandleStart(w http.ResponseWriter, r *http.Request) {
	sessionCtx, _ := WorkerSessionFromContext(r.Context())
	var req StartRequestDTO
	if err := h.readJSON(w, r, &req); err != nil {
		return
	}
	if req.ProtocolVersion != ProtocolVersion {
		h.writeError(w, r, http.StatusBadRequest, "INVALID_PROTOCOL_VERSION", "protocolVersion must be 1", false)
		return
	}

	res, err := h.service.StartAttempt(r.Context(), sessionCtx, &req)
	if err != nil {
		if errors.Is(err, ErrAttemptNotFound) {
			h.writeError(w, r, http.StatusNotFound, "ATTEMPT_NOT_FOUND", err.Error(), false)
			return
		}
		if errors.Is(err, ErrStaleOwnership) {
			h.writeError(w, r, http.StatusConflict, "STALE_OWNERSHIP", err.Error(), false)
			return
		}
		if errors.Is(err, ErrStartDeadlineExceeded) {
			h.writeError(w, r, http.StatusConflict, "START_DEADLINE_EXCEEDED", err.Error(), false)
			return
		}
		h.writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), true)
		return
	}
	h.writeJSON(w, http.StatusOK, res)
}

func (h *HTTPHandler) HandleHeartbeat(w http.ResponseWriter, r *http.Request) {
	sessionCtx, _ := WorkerSessionFromContext(r.Context())
	var req HeartbeatRequestDTO
	if err := h.readJSON(w, r, &req); err != nil {
		return
	}
	if req.ProtocolVersion != ProtocolVersion {
		h.writeError(w, r, http.StatusBadRequest, "INVALID_PROTOCOL_VERSION", "protocolVersion must be 1", false)
		return
	}

	res, err := h.service.Heartbeat(r.Context(), sessionCtx, &req)
	if err != nil {
		h.writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), true)
		return
	}
	h.writeJSON(w, http.StatusOK, res)
}

func (h *HTTPHandler) HandleComplete(w http.ResponseWriter, r *http.Request) {
	sessionCtx, _ := WorkerSessionFromContext(r.Context())
	var req CompleteRequestDTO
	if err := h.readJSON(w, r, &req); err != nil {
		return
	}
	if req.ProtocolVersion != ProtocolVersion {
		h.writeError(w, r, http.StatusBadRequest, "INVALID_PROTOCOL_VERSION", "protocolVersion must be 1", false)
		return
	}

	res, err := h.service.CompleteAttempt(r.Context(), sessionCtx, &req)
	if err != nil {
		if errors.Is(err, ErrStaleOwnership) {
			h.writeError(w, r, http.StatusConflict, "STALE_OWNERSHIP", err.Error(), false)
			return
		}
		h.writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), true)
		return
	}
	h.writeJSON(w, http.StatusOK, res)
}

func (h *HTTPHandler) HandleStopAck(w http.ResponseWriter, r *http.Request) {
	sessionCtx, _ := WorkerSessionFromContext(r.Context())
	var req StopAckRequestDTO
	if err := h.readJSON(w, r, &req); err != nil {
		return
	}
	if req.ProtocolVersion != ProtocolVersion {
		h.writeError(w, r, http.StatusBadRequest, "INVALID_PROTOCOL_VERSION", "protocolVersion must be 1", false)
		return
	}

	res, err := h.service.StopAck(r.Context(), sessionCtx, &req)
	if err != nil {
		h.writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), true)
		return
	}
	h.writeJSON(w, http.StatusOK, res)
}

func (h *HTTPHandler) HandleLogs(w http.ResponseWriter, r *http.Request) {
	sessionCtx, _ := WorkerSessionFromContext(r.Context())
	var req LogBatchRequestDTO
	if err := h.readJSON(w, r, &req); err != nil {
		return
	}
	if req.ProtocolVersion != ProtocolVersion {
		h.writeError(w, r, http.StatusBadRequest, "INVALID_PROTOCOL_VERSION", "protocolVersion must be 1", false)
		return
	}

	res, err := h.service.RecordLogs(r.Context(), sessionCtx, &req)
	if err != nil {
		h.writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), true)
		return
	}
	h.writeJSON(w, http.StatusOK, res)
}

// Admin handler for generating enrollment tokens
func (h *HTTPHandler) HandleCreateEnrollmentToken(w http.ResponseWriter, r *http.Request) {
	caller, ok := tenant.CallerFromContext(r.Context())
	if !ok {
		h.writeError(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication required", false)
		return
	}

	// Require admin:key or admin:member or Admin/Owner role per Blueprint §24.2
	if caller.Role != tenant.RoleAdmin && caller.Role != tenant.RoleOwner &&
		!tenant.CanAPIKeyPerform(caller.Capabilities, tenant.CapAdminKey) &&
		!tenant.CanAPIKeyPerform(caller.Capabilities, tenant.CapAdminMember) {
		h.writeError(w, r, http.StatusForbidden, "FORBIDDEN", "Worker enrollment requires Admin or Owner authority", false)
		return
	}

	envID := r.PathValue("envId")
	if envID == "" {
		h.writeError(w, r, http.StatusBadRequest, "MISSING_ENVIRONMENT", "Environment ID is required", false)
		return
	}

	var in struct {
		Pool string `json:"pool"`
	}
	_ = json.NewDecoder(r.Body).Decode(&in)

	var actorID *string
	if caller.Type == tenant.IdentityTypeHuman {
		actorID = &caller.UserID
	}

	tokenInfo, err := h.service.CreateEnrollmentToken(r.Context(), caller.OrganizationID, envID, in.Pool, actorID)
	if err != nil {
		h.writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), true)
		return
	}

	h.writeJSON(w, http.StatusCreated, tokenInfo)
}

// Admin handler for revoking a worker
func (h *HTTPHandler) HandleRevokeWorker(w http.ResponseWriter, r *http.Request) {
	caller, ok := tenant.CallerFromContext(r.Context())
	if !ok {
		h.writeError(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication required", false)
		return
	}

	if caller.Role != tenant.RoleAdmin && caller.Role != tenant.RoleOwner &&
		!tenant.CanAPIKeyPerform(caller.Capabilities, tenant.CapAdminKey) {
		h.writeError(w, r, http.StatusForbidden, "FORBIDDEN", "Revoking a worker requires Admin or Owner authority", false)
		return
	}

	workerID := r.PathValue("workerId")
	if workerID == "" {
		h.writeError(w, r, http.StatusBadRequest, "MISSING_WORKER_ID", "Worker ID is required", false)
		return
	}

	if err := h.service.RevokeWorker(r.Context(), caller.OrganizationID, workerID); err != nil {
		h.writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), true)
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]any{"revoked": true, "workerId": workerID})
}

// Operator/Admin handler for draining a worker
func (h *HTTPHandler) HandleDrainWorker(w http.ResponseWriter, r *http.Request) {
	caller, ok := tenant.CallerFromContext(r.Context())
	if !ok {
		h.writeError(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication required", false)
		return
	}

	if !tenant.CanRolePerform(caller.Role, tenant.CapWorkersDrain) &&
		!tenant.CanAPIKeyPerform(caller.Capabilities, tenant.CapWorkersDrain) {
		h.writeError(w, r, http.StatusForbidden, "FORBIDDEN", "Draining a worker requires workers:drain capability", false)
		return
	}

	workerID := r.PathValue("workerId")
	if workerID == "" {
		h.writeError(w, r, http.StatusBadRequest, "MISSING_WORKER_ID", "Worker ID is required", false)
		return
	}

	if err := h.service.DrainWorker(r.Context(), caller.OrganizationID, workerID); err != nil {
		h.writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), true)
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]any{"draining": true, "workerId": workerID})
}

func (h *HTTPHandler) readJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20) // 2MB max
	defer r.Body.Close()
	data, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, r, http.StatusBadRequest, "MALFORMED_REQUEST", "Failed to read request body", false)
		return err
	}
	if err := json.Unmarshal(data, dst); err != nil {
		h.writeError(w, r, http.StatusBadRequest, "MALFORMED_REQUEST", "Invalid JSON payload", false)
		return err
	}
	return nil
}

func (h *HTTPHandler) writeJSON(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}

func (h *HTTPHandler) writeError(w http.ResponseWriter, r *http.Request, status int, code, message string, retryable bool) {
	reqID := tenant.RequestIDFromContext(r.Context())
	if reqID == "" {
		reqID = r.Header.Get("X-Request-ID")
	}
	if reqID == "" {
		reqID = "req_anon"
	}
	env := ErrorEnvelopeDTO{
		Code:      code,
		Message:   message,
		RequestID: reqID,
		Details:   map[string]any{},
		Retryable: retryable,
	}
	h.writeJSON(w, status, env)
}
