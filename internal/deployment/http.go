package deployment

import (
	"encoding/json"
	"errors"
	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"io"
	"net/http"
)

type HTTPHandler struct {
	service *Service
	tenants *tenant.Service
}

func NewHTTPHandler(s *Service, tenants *tenant.Service) *HTTPHandler {
	return &HTTPHandler{service: s, tenants: tenants}
}
func (h *HTTPHandler) Register(w http.ResponseWriter, r *http.Request) {
	caller, ok := tenant.CallerFromContext(r.Context())
	if !ok {
		errJSON(w, r, 401, "UNAUTHENTICATED", "Authentication required")
		return
	}
	env := r.URL.Query().Get("environment")
	if env == "" {
		errJSON(w, r, 400, "MISSING_ENVIRONMENT", "Environment is required")
		return
	}
	if !h.allowed(r, caller, env, tenant.CapDeploymentsRegister) {
		errJSON(w, r, 403, "FORBIDDEN", "Deployment registration is not permitted")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		errJSON(w, r, 400, "INVALID_MANIFEST", "Invalid manifest")
		return
	}
	d, created, err := h.service.Register(r.Context(), caller.OrganizationID, env, body)
	if err != nil {
		writeServiceErr(w, r, err)
		return
	}
	writeJSON(w, map[bool]int{true: 201, false: 200}[created], d)
}
func (h *HTTPHandler) Activate(w http.ResponseWriter, r *http.Request) {
	caller, ok := tenant.CallerFromContext(r.Context())
	if !ok {
		errJSON(w, r, 401, "UNAUTHENTICATED", "Authentication required")
		return
	}
	env := r.URL.Query().Get("environment")
	var in struct {
		DeploymentID      string `json:"deploymentId"`
		ExpectedRevision  int64  `json:"expectedRevision"`
		AllowSingleWorker bool   `json:"allowSingleWorker"`
	}
	if env == "" || json.NewDecoder(r.Body).Decode(&in) != nil || in.DeploymentID == "" {
		errJSON(w, r, 400, "INVALID_REQUEST", "deploymentId, expectedRevision, and environment are required")
		return
	}
	environment, err := h.tenants.GetEnvironment(r.Context(), caller.OrganizationID, env)
	if err != nil {
		errJSON(w, r, 404, "NOT_FOUND", "Environment not found")
		return
	}
	cap := tenant.CapDeploymentsActivateStaging
	if environment.Name == tenant.EnvProduction {
		cap = tenant.CapDeploymentsActivateProd
	}
	if !h.allowed(r, caller, env, cap) {
		errJSON(w, r, 403, "FORBIDDEN", "Deployment activation is not permitted")
		return
	}
	out, err := h.service.Activate(r.Context(), caller.OrganizationID, env, r.PathValue("name"), in.DeploymentID, in.ExpectedRevision, in.AllowSingleWorker)
	if err != nil {
		writeServiceErr(w, r, err)
		return
	}
	writeJSON(w, 200, out)
}
func (h *HTTPHandler) allowed(r *http.Request, c *tenant.CallerIdentity, env, cap string) bool {
	if c.OrganizationID == "" {
		return false
	}
	if c.Type == tenant.IdentityTypeMachine {
		return c.EnvironmentID == env && tenant.CanAPIKeyPerform(c.Capabilities, cap)
	}
	m, err := h.tenants.GetMember(r.Context(), c.OrganizationID, c.UserID)
	return err == nil && m.Status == tenant.StatusActive && tenant.CanRolePerform(m.Role, cap)
}
func writeServiceErr(w http.ResponseWriter, r *http.Request, e error) {
	code, status := "INTERNAL_ERROR", 500
	if _, ok := e.(*contracts.Error); ok {
		code, status = "INVALID_MANIFEST", 422
	}
	if errors.Is(e, ErrImmutable) {
		code, status = "IMMUTABLE_CONTENT_CONFLICT", 409
	}
	if errors.Is(e, ErrConflict) {
		code, status = "REVISION_CONFLICT", 409
	}
	if errors.Is(e, ErrPreflight) {
		code, status = "WORKER_PREFLIGHT_FAILED", 409
	}
	if errors.Is(e, ErrNotFound) {
		code, status = "NOT_FOUND", 404
	}
	errJSON(w, r, status, code, "Request could not be completed")
}
func writeJSON(w http.ResponseWriter, s int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(s)
	_ = json.NewEncoder(w).Encode(v)
}
func errJSON(w http.ResponseWriter, r *http.Request, s int, c, m string) {
	writeJSON(w, s, map[string]any{"code": c, "message": m, "requestId": tenant.RequestIDFromContext(r.Context()), "details": map[string]any{}, "retryable": s >= 500})
}
