package execution

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
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
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		errJSON(w, r, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "Request body exceeds transport limit")
		return
	}
	parsed, err := contracts.ParseJSON(raw)
	if err != nil {
		errJSON(w, r, http.StatusBadRequest, "INVALID_REQUEST", "Invalid request body")
		return
	}
	canonical, _ := json.Marshal(parsed)
	var req CreateRunRequestDTO
	dec := json.NewDecoder(bytes.NewReader(canonical))
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
		if errors.Is(err, tenant.ErrAuditRequired) {
			errJSON(w, r, http.StatusUnauthorized, "AUDIT_REQUIRED", "Audit context is required")
			return
		}
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
		if errors.Is(err, ErrPayloadTooLarge) {
			errJSON(w, r, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "Inline JSON payload exceeds 256 KiB")
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
	if !h.hasPayloadRead(r, caller, snapshot.EnvironmentID) {
		snapshot.Output = nil
		snapshot.Error = nil
	}

	writeJSON(w, http.StatusOK, snapshot)
}

func (h *HTTPHandler) ListRuns(w http.ResponseWriter, r *http.Request) {
	caller, ok := tenant.CallerFromContext(r.Context())
	if !ok {
		errJSON(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication required")
		return
	}

	envParam := r.URL.Query().Get("environment")
	if envParam == "" {
		errJSON(w, r, http.StatusBadRequest, "MISSING_ENVIRONMENT", "environment query parameter is required")
		return
	}

	if !h.allowed(r, caller, envParam, tenant.CapRunsRead) {
		errJSON(w, r, http.StatusForbidden, "FORBIDDEN", "Reading runs is not permitted")
		return
	}

	var cursor *string
	if c := r.URL.Query().Get("cursor"); c != "" {
		cursor = &c
	}
	limit := 25
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	resp, err := h.service.ListRuns(r.Context(), caller.OrganizationID, envParam, cursor, limit)
	if err != nil {
		if errors.Is(err, ErrEnvironmentNotFound) {
			errJSON(w, r, http.StatusNotFound, "ENVIRONMENT_NOT_FOUND", "Environment not found")
			return
		}
		if errors.Is(err, ErrInvalidCursor) {
			errJSON(w, r, http.StatusBadRequest, "INVALID_CURSOR", "Cursor is invalid")
			return
		}
		errJSON(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal server error")
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *HTTPHandler) ListWorkers(w http.ResponseWriter, r *http.Request) {
	caller, ok := tenant.CallerFromContext(r.Context())
	if !ok {
		errJSON(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication required")
		return
	}

	envParam := r.URL.Query().Get("environment")
	if envParam == "" {
		errJSON(w, r, http.StatusBadRequest, "MISSING_ENVIRONMENT", "environment query parameter is required")
		return
	}

	if !h.allowed(r, caller, envParam, tenant.CapWorkersRead) {
		errJSON(w, r, http.StatusForbidden, "FORBIDDEN", "Reading workers is not permitted")
		return
	}

	var cursor *string
	if c := r.URL.Query().Get("cursor"); c != "" {
		cursor = &c
	}
	limit := 25
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	resp, err := h.service.ListWorkers(r.Context(), caller.OrganizationID, envParam, cursor, limit)
	if err != nil {
		if errors.Is(err, ErrEnvironmentNotFound) {
			errJSON(w, r, http.StatusNotFound, "ENVIRONMENT_NOT_FOUND", "Environment not found")
			return
		}
		if errors.Is(err, ErrInvalidCursor) {
			errJSON(w, r, http.StatusBadRequest, "INVALID_CURSOR", "Cursor is invalid")
			return
		}
		errJSON(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal server error")
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *HTTPHandler) GetRunEvents(w http.ResponseWriter, r *http.Request) {
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

	var cursor int64 = 0
	if c := r.URL.Query().Get("cursor"); c != "" {
		if parsed, err := strconv.ParseInt(c, 10, 64); err == nil && parsed >= 0 {
			cursor = parsed
		}
	}
	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	hasPayloadRead := h.hasPayloadRead(r, caller, snapshot.EnvironmentID)
	resp, err := h.service.GetRunEvents(r.Context(), caller.OrganizationID, runID, cursor, limit, hasPayloadRead)
	if err != nil {
		if errors.Is(err, ErrRunNotFound) {
			errJSON(w, r, http.StatusNotFound, "RUN_NOT_FOUND", "Run not found")
			return
		}
		errJSON(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal server error")
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *HTTPHandler) StreamRunEvents(w http.ResponseWriter, r *http.Request) {
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

	flusher, ok := w.(http.Flusher)
	if !ok {
		errJSON(w, r, http.StatusInternalServerError, "STREAMING_UNSUPPORTED", "Streaming not supported")
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

	hasPayloadRead := h.hasPayloadRead(r, caller, snapshot.EnvironmentID)

	var lastSeenSeq int64 = 0
	lastEventID := r.Header.Get("Last-Event-ID")
	if lastEventID == "" {
		lastEventID = r.URL.Query().Get("lastEventId")
	}
	if lastEventID == "" {
		lastEventID = r.URL.Query().Get("cursor")
	}
	if lastEventID != "" {
		if parsed, err := strconv.ParseInt(lastEventID, 10, 64); err == nil && parsed >= 0 {
			lastSeenSeq = parsed
		}
	}

	// Retention gap check per REQ-EVENT-01
	minSeq, _, bErr := h.service.GetRunEventRetentionBounds(r.Context(), caller.OrganizationID, runID)
	if bErr == nil && minSeq > 0 && lastSeenSeq > 0 && lastSeenSeq < minSeq-1 {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		resyncData, _ := json.Marshal(map[string]any{
			"reason":                 "RETENTION_GAP",
			"code":                   "RESYNC_REQUIRED",
			"lastAvailableSequence": minSeq,
		})
		fmt.Fprintf(w, "event: resync\ndata: %s\n\n", string(resyncData))
		flusher.Flush()
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	notifyCh, cleanup := h.service.Hub().Subscribe(runID)
	defer cleanup()

	currentSeq := lastSeenSeq
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		// Drain all available events from PostgreSQL
		for {
			eventsResp, err := h.service.GetRunEvents(r.Context(), caller.OrganizationID, runID, currentSeq, 50, hasPayloadRead)
			if err != nil || len(eventsResp.Events) == 0 {
				break
			}
			for _, ev := range eventsResp.Events {
				dataBytes, _ := json.Marshal(ev.Payload)
				fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", ev.Sequence, ev.Type, string(dataBytes))
				currentSeq = ev.Sequence
			}
			flusher.Flush()
			if !eventsResp.HasMore {
				break
			}
		}

		select {
		case <-r.Context().Done():
			return
		case <-notifyCh:
			// Woken up by event commit in worker_engine! Loop and drain from DB.
		case <-ticker.C:
			// SSE keepalive comment to maintain connection through proxies
			fmt.Fprintf(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}

func (h *HTTPHandler) GetRunLogs(w http.ResponseWriter, r *http.Request) {
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

	// Strict payload:read boundary enforcement
	if !h.hasPayloadRead(r, caller, snapshot.EnvironmentID) {
		errJSON(w, r, http.StatusForbidden, "FORBIDDEN", "Viewing run logs requires payload:read capability")
		return
	}

	var stepID, attemptID, cursor *string
	if s := r.URL.Query().Get("stepId"); s != "" {
		stepID = &s
	}
	if a := r.URL.Query().Get("attemptId"); a != "" {
		attemptID = &a
	}
	if c := r.URL.Query().Get("cursor"); c != "" {
		cursor = &c
	}
	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	resp, err := h.service.GetRunLogs(r.Context(), caller.OrganizationID, runID, stepID, attemptID, cursor, limit)
	if err != nil {
		if errors.Is(err, ErrRunNotFound) {
			errJSON(w, r, http.StatusNotFound, "RUN_NOT_FOUND", "Run not found")
			return
		}
		errJSON(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal server error")
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *HTTPHandler) hasPayloadRead(r *http.Request, c *tenant.CallerIdentity, environmentID string) bool {
	if c.Type == tenant.IdentityTypeMachine {
		return c.EnvironmentID == environmentID && tenant.CanAPIKeyPerform(c.Capabilities, tenant.CapPayloadRead)
	}
	m, err := h.tenants.GetMember(r.Context(), c.OrganizationID, c.UserID)
	return err == nil && m.Status == tenant.StatusActive && tenant.CanRolePerform(m.Role, tenant.CapPayloadRead)
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
	if c == nil {
		return nil
	}
	var id *string
	if c.Type == tenant.IdentityTypeMachine {
		if c.KeyID != "" {
			id = &c.KeyID
		}
	} else {
		if c.UserID != "" {
			id = &c.UserID
		}
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
