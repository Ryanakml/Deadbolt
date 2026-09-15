package execution

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/Ryanakml/Deadbolt/internal/tenant"
)

type HTTPHandler struct {
	service *Service
	tenants *tenant.Service
}

func NewHTTPHandler(s *Service, tenants *tenant.Service) *HTTPHandler {
	return &HTTPHandler{service: s, tenants: tenants}
}

func (h *HTTPHandler) CreateRun(w http.ResponseWriter, r *http.Request) {
	caller, ok := tenant.CallerFromContext(r.Context())
	if !ok {
		errJSON(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication required")
		return
	}

	workflowName := r.PathValue("name")
	if workflowName == "" {
		errJSON(w, r, http.StatusBadRequest, "INVALID_REQUEST", "Workflow name is required")
		return
	}

	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey == "" {
		errJSON(w, r, http.StatusBadRequest, "MISSING_IDEMPOTENCY_KEY", "Idempotency-Key header is required")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	var req CreateRunRequestDTO
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		errJSON(w, r, http.StatusBadRequest, "INVALID_REQUEST", "Invalid request body")
		return
	}
	if req.Environment == "" {
		errJSON(w, r, http.StatusBadRequest, "MISSING_ENVIRONMENT", "environment is required")
		return
	}

	if !h.allowed(r, caller, req.Environment, tenant.CapRunsCreate) {
		errJSON(w, r, http.StatusForbidden, "FORBIDDEN", "Run creation is not permitted")
		return
	}

	run, _, err := h.service.CreateRun(
		r.Context(),
		caller.OrganizationID,
		req.Environment,
		workflowName,
		idempotencyKey,
		req.DeploymentID,
		req.Input,
		auditFromCaller(caller, r),
	)
	if err != nil {
		if errors.Is(err, ErrMissingIdempotencyKey) {
			errJSON(w, r, http.StatusBadRequest, "MISSING_IDEMPOTENCY_KEY", err.Error())
			return
		}
		if errors.Is(err, ErrIdempotencyConflict) {
			errJSON(w, r, http.StatusConflict, "IDEMPOTENCY_CONFLICT", "Idempotency key already used with different payload")
			return
		}
		if errors.Is(err, ErrSchemaViolation) {
			errJSON(w, r, http.StatusUnprocessableEntity, "SCHEMA_VIOLATION", "Input does not conform to workflow input schema")
			return
		}
		if errors.Is(err, ErrNoActiveDeployment) {
			errJSON(w, r, http.StatusNotFound, "NO_ACTIVE_DEPLOYMENT", "No active deployment found for workflow")
			return
		}
		if errors.Is(err, ErrDeploymentNotFound) {
			errJSON(w, r, http.StatusNotFound, "DEPLOYMENT_NOT_FOUND", "Specified deployment was not found")
			return
		}
		if errors.Is(err, ErrWorkflowNotFound) {
			errJSON(w, r, http.StatusNotFound, "WORKFLOW_NOT_FOUND", "Workflow was not found in deployment")
			return
		}
		if errors.Is(err, ErrEnvironmentNotFound) {
			errJSON(w, r, http.StatusNotFound, "ENVIRONMENT_NOT_FOUND", "Environment not found")
			return
		}
		errJSON(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal server error")
		return
	}

	writeJSON(w, http.StatusAccepted, run)
}

func (h *HTTPHandler) GetRun(w http.ResponseWriter, r *http.Request) {
	caller, ok := tenant.CallerFromContext(r.Context())
	if !ok {
		errJSON(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication required")
		return
	}

	runID := r.PathValue("id")
	if runID == "" {
		errJSON(w, r, http.StatusBadRequest, "INVALID_RUN_ID", "Run ID is required")
		return
	}

	snapshot, err := h.service.GetRun(r.Context(), caller.OrganizationID, runID)
	if err != nil {
		if errors.Is(err, ErrRunNotFound) {
			errJSON(w, r, http.StatusNotFound, "RUN_NOT_FOUND", "Run not found")
			return
		}
		errJSON(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal server error")
		return
	}

	if caller.Type == tenant.IdentityTypeMachine && caller.EnvironmentID != "" && caller.EnvironmentID != snapshot.EnvironmentID {
		errJSON(w, r, http.StatusNotFound, "RUN_NOT_FOUND", "Run not found")
		return
	}

	writeJSON(w, http.StatusOK, snapshot)
}

func (h *HTTPHandler) allowed(r *http.Request, c *tenant.CallerIdentity, envParam, cap string) bool {
	if c.OrganizationID == "" {
		return false
	}
	env, err := h.service.resolveEnvironment(r.Context(), c.OrganizationID, envParam)
	if err != nil {
		return false
	}
	if c.Type == tenant.IdentityTypeMachine {
		return c.EnvironmentID == env.ID && tenant.CanAPIKeyPerform(c.Capabilities, cap)
	}
	m, err := h.tenants.GetMember(r.Context(), c.OrganizationID, c.UserID)
	return err == nil && m.Status == tenant.StatusActive && tenant.CanRolePerform(m.Role, cap)
}

func auditFromCaller(c *tenant.CallerIdentity, r *http.Request) *tenant.AuditContext {
	var id *string
	if c.Type == tenant.IdentityTypeMachine {
		id = &c.KeyID
	} else {
		id = &c.UserID
	}
	return &tenant.AuditContext{
		ActorID:       id,
		ActorType:     c.Type,
		Role:          c.Role,
		Capabilities:  c.Capabilities,
		CorrelationID: tenant.RequestIDFromContext(r.Context()),
	}
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
