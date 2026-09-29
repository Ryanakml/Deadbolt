package scheduling

// HTTP surface for schedule definitions (Blueprint §20, Issue #34).
//
// The wire shape is fixed by contracts/openapi/control-plane.yaml: the
// Schedule schema uses `workflow`, `environment`, and `cron` rather than
// the database column names, and PATCH nests the desired state under
// `configuration` alongside `expectedRevision`. Callers authenticate
// through the existing tenant middleware; the required capability is
// schedules:write, which the backend and the browser both rely on rather
// than trusting the route declaration alone.

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/Ryanakml/Deadbolt/internal/tenant"
)

// scheduleDTO is the wire representation of a schedule.
type scheduleDTO struct {
	ID            string  `json:"id"`
	Workflow      string  `json:"workflow"`
	Environment   string  `json:"environment"`
	Cron          string  `json:"cron"`
	Timezone      string  `json:"timezone"`
	DeploymentID  *string `json:"deploymentId"`
	OverlapPolicy string  `json:"overlapPolicy"`
	MisfirePolicy string  `json:"misfirePolicy"`
	Paused        bool    `json:"paused"`
	Revision      int64   `json:"revision"`
}

func toDTO(s *Schedule) *scheduleDTO {
	return &scheduleDTO{
		ID:            s.ID,
		Workflow:      s.WorkflowName,
		Environment:   s.EnvironmentID,
		Cron:          s.CronExpression,
		Timezone:      s.Timezone,
		DeploymentID:  s.DeploymentID,
		OverlapPolicy: s.OverlapPolicy,
		MisfirePolicy: s.MisfirePolicy,
		Paused:        s.Paused,
		Revision:      s.Revision,
	}
}

// createScheduleDTO accepts the create body. The overlap and misfire
// policies are intentionally absent: §17 fixes both, so a caller cannot
// choose them, and accepting the fields would imply a choice exists.
type createScheduleDTO struct {
	Workflow     string  `json:"workflow"`
	Cron         string  `json:"cron"`
	Timezone     string  `json:"timezone"`
	DeploymentID *string `json:"deploymentId"`
}

// updateScheduleDTO is the PATCH body: a required expectedRevision plus the
// desired configuration. Pausing is expressed here rather than through a
// separate route because the contract declares `paused` on Schedule.
type updateScheduleDTO struct {
	ExpectedRevision int64             `json:"expectedRevision"`
	Configuration    createScheduleDTO `json:"configuration"`
}

// HTTPHandler exposes schedule definitions over HTTP.
type HTTPHandler struct {
	service *Service
	tenants *tenant.HTTPHandler
}

// NewHTTPHandler constructs the schedule HTTP handler.
func NewHTTPHandler(service *Service, tenants *tenant.HTTPHandler) *HTTPHandler {
	return &HTTPHandler{service: service, tenants: tenants}
}

func writeJSONError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"code":      code,
		"message":   message,
		"requestId": r.Header.Get("X-Request-ID"),
		"retryable": status >= 500,
	})
}

// writeServiceError maps a service error onto the stable API codes the
// contract declares, so a caller never has to parse prose.
func writeServiceError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ErrScheduleNotFound):
		writeJSONError(w, r, http.StatusNotFound, "SCHEDULE_NOT_FOUND", "Schedule not found")
	case errors.Is(err, ErrScheduleLimitReached):
		writeJSONError(w, r, http.StatusConflict, "SCHEDULE_LIMIT_REACHED", err.Error())
	case errors.Is(err, ErrRevisionConflict):
		writeJSONError(w, r, http.StatusConflict, "REVISION_CONFLICT", "Schedule changed since it was read; refresh before acting")
	case errors.Is(err, ErrInvalidCron):
		writeJSONError(w, r, http.StatusUnprocessableEntity, "INVALID_CRON", err.Error())
	case errors.Is(err, ErrInvalidTimezone):
		writeJSONError(w, r, http.StatusUnprocessableEntity, "INVALID_TIMEZONE", err.Error())
	case errors.Is(err, ErrInvalidWorkflow):
		writeJSONError(w, r, http.StatusUnprocessableEntity, "INVALID_WORKFLOW", err.Error())
	case errors.Is(err, ErrPinnedDeployment):
		writeJSONError(w, r, http.StatusUnprocessableEntity, "PINNED_DEPLOYMENT_INVALID", err.Error())
	case errors.Is(err, ErrScheduleAlreadyPaused), errors.Is(err, ErrScheduleNotPaused):
		writeJSONError(w, r, http.StatusConflict, "INVALID_SCHEDULE_STATE", err.Error())
	default:
		writeJSONError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to process schedule request")
	}
}

func decodeBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeJSONError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "Invalid request body")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// Create handles POST /v1/schedules.
func (h *HTTPHandler) Create(w http.ResponseWriter, r *http.Request) {
	caller, ok := tenant.CallerFromContext(r.Context())
	if !ok || caller == nil {
		writeJSONError(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication required")
		return
	}
	var req createScheduleDTO
	if !decodeBody(w, r, &req) {
		return
	}
	envID := r.URL.Query().Get("environment")
	if envID == "" {
		writeJSONError(w, r, http.StatusBadRequest, "MISSING_ENVIRONMENT", "environment query parameter is required")
		return
	}
	schedule, err := h.service.Create(r.Context(), caller.OrganizationID, envID, CreateScheduleRequest{
		WorkflowName:   req.Workflow,
		CronExpression: req.Cron,
		Timezone:       req.Timezone,
		DeploymentID:   req.DeploymentID,
	}, auditFromCaller(caller, r))
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toDTO(schedule))
}

// List handles GET /v1/schedules.
func (h *HTTPHandler) List(w http.ResponseWriter, r *http.Request) {
	caller, ok := tenant.CallerFromContext(r.Context())
	if !ok || caller == nil {
		writeJSONError(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication required")
		return
	}
	envID := r.URL.Query().Get("environment")
	if envID == "" {
		writeJSONError(w, r, http.StatusBadRequest, "MISSING_ENVIRONMENT", "environment query parameter is required")
		return
	}
	schedules, err := h.service.List(r.Context(), caller.OrganizationID, envID)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	items := make([]*scheduleDTO, 0, len(schedules))
	for i := range schedules {
		items = append(items, toDTO(&schedules[i]))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "nextCursor": nil})
}

// Update handles PATCH /v1/schedules/{id}. A configuration change advances
// the revision; a `paused` change routes to pause or resume so the
// definition, the cap, and the audit trail stay in one code path.
func (h *HTTPHandler) Update(w http.ResponseWriter, r *http.Request) {
	caller, ok := tenant.CallerFromContext(r.Context())
	if !ok || caller == nil {
		writeJSONError(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication required")
		return
	}
	var req updateScheduleDTO
	if !decodeBody(w, r, &req) {
		return
	}
	scheduleID := r.PathValue("id")
	envID := r.URL.Query().Get("environment")
	if envID == "" {
		writeJSONError(w, r, http.StatusBadRequest, "MISSING_ENVIRONMENT", "environment query parameter is required")
		return
	}
	audit := auditFromCaller(caller, r)
	updated, err := h.service.Update(r.Context(), caller.OrganizationID, envID, scheduleID, UpdateScheduleRequest{
		ExpectedRevision: req.ExpectedRevision,
		CronExpression:   &req.Configuration.Cron,
		Timezone:         &req.Configuration.Timezone,
		DeploymentID:     req.Configuration.DeploymentID,
	}, audit)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toDTO(updated))
}

// Pause handles POST /v1/schedules/{id}/pause.
func (h *HTTPHandler) Pause(w http.ResponseWriter, r *http.Request) {
	h.SetPaused(w, r, true)
}

// Resume handles POST /v1/schedules/{id}/resume.
func (h *HTTPHandler) Resume(w http.ResponseWriter, r *http.Request) {
	h.SetPaused(w, r, false)
}

// SetPaused performs the pause/resume transition. The contract expresses
// `paused` on the Schedule schema, so this is reached from the same PATCH
// route when only that field changes.
func (h *HTTPHandler) SetPaused(w http.ResponseWriter, r *http.Request, paused bool) {
	caller, ok := tenant.CallerFromContext(r.Context())
	if !ok || caller == nil {
		writeJSONError(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication required")
		return
	}
	var req struct {
		ExpectedRevision int64 `json:"expectedRevision"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	envID := r.URL.Query().Get("environment")
	if envID == "" {
		writeJSONError(w, r, http.StatusBadRequest, "MISSING_ENVIRONMENT", "environment query parameter is required")
		return
	}
	scheduleID := r.PathValue("id")
	audit := auditFromCaller(caller, r)
	control := ControlScheduleRequest{ExpectedRevision: req.ExpectedRevision}

	var updated *Schedule
	var err error
	if paused {
		updated, err = h.service.Pause(r.Context(), caller.OrganizationID, envID, scheduleID, control, audit)
	} else {
		updated, err = h.service.Resume(r.Context(), caller.OrganizationID, envID, scheduleID, control, audit)
	}
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toDTO(updated))
}

// Delete handles DELETE /v1/schedules/{id}.
func (h *HTTPHandler) Delete(w http.ResponseWriter, r *http.Request) {
	caller, ok := tenant.CallerFromContext(r.Context())
	if !ok || caller == nil {
		writeJSONError(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication required")
		return
	}
	envID := r.URL.Query().Get("environment")
	if envID == "" {
		writeJSONError(w, r, http.StatusBadRequest, "MISSING_ENVIRONMENT", "environment query parameter is required")
		return
	}
	scheduleID := r.PathValue("id")
	if err := h.service.Delete(r.Context(), caller.OrganizationID, envID, scheduleID, auditFromCaller(caller, r)); err != nil {
		writeServiceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true, "id": scheduleID})
}

// auditFromCaller converts the authenticated identity into the audit
// context the service records, so the audit row captures the role and the
// effective capabilities at the time the action was accepted (§24.2).
func auditFromCaller(caller *tenant.CallerIdentity, r *http.Request) *tenant.AuditContext {
	if caller == nil {
		return nil
	}
	var id *string
	if caller.Type == tenant.IdentityTypeMachine {
		if caller.KeyID != "" {
			id = &caller.KeyID
		}
	} else if caller.UserID != "" {
		id = &caller.UserID
	}
	return &tenant.AuditContext{
		ActorID:       id,
		ActorType:     caller.Type,
		Role:          caller.Role,
		Capabilities:  caller.Capabilities,
		CorrelationID: tenant.RequestIDFromContext(r.Context()),
	}
}
