package tenant

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/Ryanakml/Deadbolt/internal/auth"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
)

type contextKey string

const (
	callerIdentityKey contextKey = "deadbolt.tenant.caller_identity"
)

type IdentityType string

const (
	IdentityTypeHuman   IdentityType = "HUMAN"
	IdentityTypeMachine IdentityType = "MACHINE"
)

// CallerIdentity captures the verified identity and authorized scope for a request.
type CallerIdentity struct {
	Type           IdentityType `json:"type"`
	UserID         string       `json:"user_id,omitempty"`
	Role           string       `json:"role,omitempty"`
	OrganizationID string       `json:"organization_id"`
	EnvironmentID  string       `json:"environment_id,omitempty"`
	Capabilities   []string     `json:"capabilities"`
}

// CallerFromContext extracts the authenticated CallerIdentity from context.
func CallerFromContext(ctx context.Context) (*CallerIdentity, bool) {
	id, ok := ctx.Value(callerIdentityKey).(*CallerIdentity)
	return id, ok && id != nil
}

// ContextWithCaller sets the authenticated CallerIdentity into context.
func ContextWithCaller(ctx context.Context, id *CallerIdentity) context.Context {
	return context.WithValue(ctx, callerIdentityKey, id)
}

// HTTPHandler provides REST endpoints for tenant, project, environment, and API key management.
type HTTPHandler struct {
	service      *Service
	pool         *pgxpool.Pool
	sessionStore *auth.SessionStore
	authCfg      auth.Config
}

// NewHTTPHandler constructs a new HTTPHandler.
func NewHTTPHandler(service *Service, pool *pgxpool.Pool, sessionStore *auth.SessionStore, authCfg auth.Config) *HTTPHandler {
	return &HTTPHandler{
		service:      service,
		pool:         pool,
		sessionStore: sessionStore,
		authCfg:      authCfg,
	}
}

// Routes mounts all tenant management routes onto an http.Handler.
func (h *HTTPHandler) Routes() http.Handler {
	mux := http.NewServeMux()

	// Organization CRUD
	mux.HandleFunc("POST /api/v1/organizations", h.RequireAuth(h.HandleCreateOrganization))
	mux.HandleFunc("GET /api/v1/organizations", h.RequireAuth(h.HandleListOrganizations))
	mux.HandleFunc("GET /api/v1/organizations/{id}", h.RequireAuth(h.RequireOrgScope(CapOrgRead, h.HandleGetOrganization)))
	mux.HandleFunc("PATCH /api/v1/organizations/{id}", h.RequireAuth(h.RequireOrgScope(CapOrgUpdate, h.HandleUpdateOrganization)))
	mux.HandleFunc("DELETE /api/v1/organizations/{id}", h.RequireAuth(h.RequireOrgScope(CapOrgDelete, h.HandleDeleteOrganization)))

	// Organization Members
	mux.HandleFunc("GET /api/v1/organizations/{id}/members", h.RequireAuth(h.RequireOrgScope(CapOrgRead, h.HandleListMembers)))
	mux.HandleFunc("POST /api/v1/organizations/{id}/members", h.RequireAuth(h.RequireOrgScope(CapAdminMember, h.HandleAddMember)))
	mux.HandleFunc("PATCH /api/v1/organizations/{id}/members/{userId}", h.RequireAuth(h.RequireOrgScope(CapAdminMember, h.HandleUpdateMemberRole)))
	mux.HandleFunc("DELETE /api/v1/organizations/{id}/members/{userId}", h.RequireAuth(h.RequireOrgScope(CapAdminMember, h.HandleRemoveMember)))

	// Projects
	mux.HandleFunc("GET /api/v1/projects", h.RequireAuth(h.RequireOrgScope(CapOrgRead, h.HandleListProjects)))
	mux.HandleFunc("POST /api/v1/projects", h.RequireAuth(h.RequireOrgScope(CapAdminProject, h.HandleCreateProject)))

	// Environments
	mux.HandleFunc("GET /api/v1/projects/{projectId}/environments", h.RequireAuth(h.RequireOrgScope(CapOrgRead, h.HandleListEnvironments)))
	mux.HandleFunc("POST /api/v1/projects/{projectId}/environments", h.RequireAuth(h.RequireOrgScope(CapAdminProject, h.HandleCreateEnvironment)))

	// API Keys
	mux.HandleFunc("POST /api/v1/environments/{envId}/api-keys", h.RequireAuth(h.RequireOrgScope(CapAdminKey, h.HandleCreateAPIKey)))
	mux.HandleFunc("GET /api/v1/environments/{envId}/api-keys", h.RequireAuth(h.RequireOrgScope(CapAdminKey, h.HandleListAPIKeys)))
	mux.HandleFunc("DELETE /api/v1/api-keys/{id}", h.RequireAuth(h.RequireOrgScope(CapAdminKey, h.HandleRevokeAPIKey)))

	// Viewer Payload Protection Demonstration / Enforcement Endpoint
	mux.HandleFunc("GET /api/v1/environments/{envId}/payload-preview", h.RequireAuth(h.RequireOrgScope(CapPayloadRead, h.HandlePayloadPreview)))

	return mux
}

// RequireAuth authenticates the caller via Bearer API Key or Session Cookie.
func (h *HTTPHandler) RequireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// 1. Check Bearer API Key first
		authHeader := r.Header.Get("Authorization")
		if strings.HasPrefix(authHeader, "Bearer ") {
			rawKey := strings.TrimPrefix(authHeader, "Bearer ")
			apiKey, err := h.service.AuthenticateAPIKey(ctx, rawKey)
			if err != nil {
				if errors.Is(err, ErrKeyRevoked) {
					writeJSONError(w, http.StatusUnauthorized, "API_KEY_REVOKED", err.Error())
					return
				}
				if errors.Is(err, ErrKeyExpired) {
					writeJSONError(w, http.StatusUnauthorized, "API_KEY_EXPIRED", err.Error())
					return
				}
				writeJSONError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Invalid or unrecognized API key")
				return
			}

			caller := &CallerIdentity{
				Type:           IdentityTypeMachine,
				OrganizationID: apiKey.OrganizationID,
				EnvironmentID:  apiKey.EnvironmentID,
				Capabilities:   apiKey.Capabilities,
			}
			next.ServeHTTP(w, r.WithContext(ContextWithCaller(ctx, caller)))
			return
		}

		// 2. Check BFF Session Cookie
		cookie, err := r.Cookie(h.authCfg.SessionCookieName())
		if err == nil && cookie.Value != "" && h.sessionStore != nil {
			sess, err := h.sessionStore.ValidateSession(ctx, cookie.Value, h.authCfg.SessionIdleTimeout)
			if err == nil && sess != nil {
				caller := &CallerIdentity{
					Type:   IdentityTypeHuman,
					UserID: sess.UserID,
				}
				if sess.ActiveOrganizationID != nil {
					caller.OrganizationID = *sess.ActiveOrganizationID
				}
				next.ServeHTTP(w, r.WithContext(ContextWithCaller(ctx, caller)))
				return
			}
		}

		writeJSONError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication required")
	}
}

// RequireOrgScope resolves the active organization ID, checks tenant membership, and enforces RBAC capability.
func (h *HTTPHandler) RequireOrgScope(requiredCap string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		caller, ok := CallerFromContext(r.Context())
		if !ok || caller == nil {
			writeJSONError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Caller identity not found")
			return
		}

		var targetOrgID string
		if strings.HasPrefix(r.URL.Path, "/api/v1/organizations/") {
			parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v1/organizations/"), "/")
			if len(parts) > 0 && parts[0] != "" {
				targetOrgID = parts[0]
			}
		}
		if targetOrgID == "" {
			targetOrgID = r.Header.Get("X-Organization-ID")
		}
		if targetOrgID == "" {
			targetOrgID = r.URL.Query().Get("org_id")
		}
		if targetOrgID == "" && caller.OrganizationID != "" {
			targetOrgID = caller.OrganizationID
		}

		if targetOrgID == "" {
			writeJSONError(w, http.StatusBadRequest, "MISSING_ORGANIZATION_ID", "Organization context is required")
			return
		}

		// Machine identity cross-tenant and capability check
		if caller.Type == IdentityTypeMachine {
			if caller.OrganizationID != targetOrgID {
				writeJSONError(w, http.StatusForbidden, "CROSS_TENANT_ACCESS_DENIED", "API key does not belong to target organization")
				return
			}

			// Check environment scope if envId is in path
			routeEnvID := r.PathValue("envId")
			if routeEnvID != "" && caller.EnvironmentID != "" && caller.EnvironmentID != routeEnvID {
				writeJSONError(w, http.StatusForbidden, "CROSS_TENANT_ACCESS_DENIED", "API key does not belong to target environment")
				return
			}

			if requiredCap != "" && !CanAPIKeyPerform(caller.Capabilities, requiredCap) {
				writeJSONError(w, http.StatusForbidden, "FORBIDDEN", fmt.Sprintf("API key lacks required capability %q", requiredCap))
				return
			}

			next.ServeHTTP(w, r)
			return
		}

		// Human identity role and capability check
		member, err := h.service.GetMember(r.Context(), targetOrgID, caller.UserID)
		if err != nil || member.Status != StatusActive {
			writeJSONError(w, http.StatusForbidden, "FORBIDDEN_ORGANIZATION_MEMBERSHIP", "User is not an active member of the target organization")
			return
		}

		caller.OrganizationID = targetOrgID
		caller.Role = member.Role
		caller.Capabilities = RoleCapabilities(member.Role)

		if requiredCap != "" && !CanRolePerform(caller.Role, requiredCap) {
			writeJSONError(w, http.StatusForbidden, "FORBIDDEN", fmt.Sprintf("Role %q lacks required capability %q", caller.Role, requiredCap))
			return
		}

		next.ServeHTTP(w, r.WithContext(ContextWithCaller(r.Context(), caller)))
	}
}

// -------------------------------------------------------------------------
// Organization Handlers
// -------------------------------------------------------------------------

func (h *HTTPHandler) HandleCreateOrganization(w http.ResponseWriter, r *http.Request) {
	caller, _ := CallerFromContext(r.Context())
	if caller.Type != IdentityTypeHuman {
		writeJSONError(w, http.StatusForbidden, "MACHINE_CREATION_FORBIDDEN", "Only human users can create organizations")
		return
	}

	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		writeJSONError(w, http.StatusBadRequest, "INVALID_REQUEST", "Organization name is required")
		return
	}

	org, err := h.service.CreateOrganization(r.Context(), caller.UserID, req.Name)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, org)
}

func (h *HTTPHandler) HandleListOrganizations(w http.ResponseWriter, r *http.Request) {
	caller, _ := CallerFromContext(r.Context())
	if caller.Type != IdentityTypeHuman {
		writeJSONError(w, http.StatusForbidden, "MACHINE_LIST_FORBIDDEN", "Only human users can enumerate organizations")
		return
	}

	memberships, err := storage.DiscoverUserMemberships(r.Context(), h.pool, caller.UserID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to discover memberships")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"organizations": memberships,
	})
}

func (h *HTTPHandler) HandleGetOrganization(w http.ResponseWriter, r *http.Request) {
	orgID := r.PathValue("id")
	org, err := h.service.GetOrganization(r.Context(), orgID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeJSONError(w, http.StatusNotFound, "NOT_FOUND", "Organization not found")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, org)
}

func (h *HTTPHandler) HandleUpdateOrganization(w http.ResponseWriter, r *http.Request) {
	orgID := r.PathValue("id")
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		writeJSONError(w, http.StatusBadRequest, "INVALID_REQUEST", "Organization name is required")
		return
	}

	org, err := h.service.UpdateOrganization(r.Context(), orgID, req.Name)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeJSONError(w, http.StatusNotFound, "NOT_FOUND", "Organization not found")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, org)
}

func (h *HTTPHandler) HandleDeleteOrganization(w http.ResponseWriter, r *http.Request) {
	orgID := r.PathValue("id")
	err := h.service.DeleteOrganization(r.Context(), orgID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeJSONError(w, http.StatusNotFound, "NOT_FOUND", "Organization not found")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// -------------------------------------------------------------------------
// Member Handlers
// -------------------------------------------------------------------------

func (h *HTTPHandler) HandleListMembers(w http.ResponseWriter, r *http.Request) {
	orgID := r.PathValue("id")
	members, err := h.service.ListMembers(r.Context(), orgID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"members": members})
}

func (h *HTTPHandler) HandleAddMember(w http.ResponseWriter, r *http.Request) {
	orgID := r.PathValue("id")
	var req struct {
		UserID string `json:"user_id"`
		Role   string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || req.Role == "" {
		writeJSONError(w, http.StatusBadRequest, "INVALID_REQUEST", "user_id and role are required")
		return
	}

	member, err := h.service.AddMember(r.Context(), orgID, req.UserID, req.Role)
	if err != nil {
		if errors.Is(err, ErrInvalidRole) {
			writeJSONError(w, http.StatusBadRequest, "INVALID_ROLE", err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, member)
}

func (h *HTTPHandler) HandleUpdateMemberRole(w http.ResponseWriter, r *http.Request) {
	orgID := r.PathValue("id")
	targetUserID := r.PathValue("userId")
	var req struct {
		Role string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Role == "" {
		writeJSONError(w, http.StatusBadRequest, "INVALID_REQUEST", "role is required")
		return
	}

	err := h.service.UpdateMemberRole(r.Context(), orgID, targetUserID, req.Role)
	if err != nil {
		if errors.Is(err, ErrLastOwnerDemotion) {
			writeJSONError(w, http.StatusForbidden, "LAST_OWNER_DEMOTION_FORBIDDEN", err.Error())
			return
		}
		if errors.Is(err, ErrInvalidRole) {
			writeJSONError(w, http.StatusBadRequest, "INVALID_ROLE", err.Error())
			return
		}
		if errors.Is(err, ErrNotFound) {
			writeJSONError(w, http.StatusNotFound, "NOT_FOUND", "Member not found")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

func (h *HTTPHandler) HandleRemoveMember(w http.ResponseWriter, r *http.Request) {
	orgID := r.PathValue("id")
	targetUserID := r.PathValue("userId")

	err := h.service.RemoveMember(r.Context(), orgID, targetUserID)
	if err != nil {
		if errors.Is(err, ErrLastOwnerRemoval) {
			writeJSONError(w, http.StatusForbidden, "LAST_OWNER_REMOVAL_FORBIDDEN", err.Error())
			return
		}
		if errors.Is(err, ErrNotFound) {
			writeJSONError(w, http.StatusNotFound, "NOT_FOUND", "Member not found")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// -------------------------------------------------------------------------
// Project Handlers
// -------------------------------------------------------------------------

func (h *HTTPHandler) HandleListProjects(w http.ResponseWriter, r *http.Request) {
	caller, _ := CallerFromContext(r.Context())
	projects, err := h.service.ListProjects(r.Context(), caller.OrganizationID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"projects": projects})
}

func (h *HTTPHandler) HandleCreateProject(w http.ResponseWriter, r *http.Request) {
	caller, _ := CallerFromContext(r.Context())
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		writeJSONError(w, http.StatusBadRequest, "INVALID_REQUEST", "Project name is required")
		return
	}

	project, err := h.service.CreateProject(r.Context(), caller.OrganizationID, req.Name)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, project)
}

// -------------------------------------------------------------------------
// Environment Handlers
// -------------------------------------------------------------------------

func (h *HTTPHandler) HandleListEnvironments(w http.ResponseWriter, r *http.Request) {
	caller, _ := CallerFromContext(r.Context())
	projectID := r.PathValue("projectId")

	envs, err := h.service.ListEnvironments(r.Context(), caller.OrganizationID, projectID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"environments": envs})
}

func (h *HTTPHandler) HandleCreateEnvironment(w http.ResponseWriter, r *http.Request) {
	caller, _ := CallerFromContext(r.Context())
	projectID := r.PathValue("projectId")
	var req struct {
		Name           string `json:"name"`
		MaxConcurrency int    `json:"max_concurrency"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		writeJSONError(w, http.StatusBadRequest, "INVALID_REQUEST", "Environment name is required")
		return
	}

	env, err := h.service.CreateEnvironment(r.Context(), caller.OrganizationID, projectID, req.Name, req.MaxConcurrency)
	if err != nil {
		if errors.Is(err, ErrInvalidEnvironment) {
			writeJSONError(w, http.StatusBadRequest, "INVALID_ENVIRONMENT", err.Error())
			return
		}
		if errors.Is(err, ErrNotFound) {
			writeJSONError(w, http.StatusNotFound, "NOT_FOUND", "Project not found")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, env)
}

// -------------------------------------------------------------------------
// API Key Handlers
// -------------------------------------------------------------------------

func (h *HTTPHandler) HandleCreateAPIKey(w http.ResponseWriter, r *http.Request) {
	caller, _ := CallerFromContext(r.Context())
	envID := r.PathValue("envId")

	var req struct {
		Capabilities []string `json:"capabilities"`
		ExpiryDays   int      `json:"expiry_days"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "INVALID_REQUEST", "Invalid JSON payload")
		return
	}

	key, err := h.service.CreateAPIKey(r.Context(), caller.OrganizationID, envID, req.Capabilities, req.ExpiryDays)
	if err != nil {
		if errors.Is(err, ErrMachineKeyRestricted) {
			writeJSONError(w, http.StatusForbidden, "MACHINE_KEY_UNAUTHORIZED", err.Error())
			return
		}
		if errors.Is(err, ErrNotFound) {
			writeJSONError(w, http.StatusNotFound, "NOT_FOUND", "Environment not found")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, key)
}

func (h *HTTPHandler) HandleListAPIKeys(w http.ResponseWriter, r *http.Request) {
	caller, _ := CallerFromContext(r.Context())
	envID := r.PathValue("envId")

	keys, err := h.service.ListAPIKeys(r.Context(), caller.OrganizationID, envID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"api_keys": keys})
}

func (h *HTTPHandler) HandleRevokeAPIKey(w http.ResponseWriter, r *http.Request) {
	caller, _ := CallerFromContext(r.Context())
	keyID := r.PathValue("id")

	err := h.service.RevokeAPIKey(r.Context(), caller.OrganizationID, keyID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeJSONError(w, http.StatusNotFound, "NOT_FOUND", "API key not found")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HTTPHandler) HandlePayloadPreview(w http.ResponseWriter, r *http.Request) {
	// Reached only if caller has CapPayloadRead capability
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"payload": "confidential_task_output_data",
	})
}

// -------------------------------------------------------------------------
// JSON Helpers
// -------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeJSONError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}
