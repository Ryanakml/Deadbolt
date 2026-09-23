package recovery

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/Ryanakml/Deadbolt/internal/tenant"
)

type HTTPHandler struct {
	manager   *Manager
	tenantSvc *tenant.Service
}

func NewHTTPHandler(manager *Manager, tenantSvc *tenant.Service) *HTTPHandler {
	return &HTTPHandler{
		manager:   manager,
		tenantSvc: tenantSvc,
	}
}

func (h *HTTPHandler) writeJSON(w http.ResponseWriter, status int, val any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(val)
}

func (h *HTTPHandler) writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	reqID := tenant.RequestIDFromContext(r.Context())
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]string{
			"code":       code,
			"message":    message,
			"request_id": reqID,
		},
	})
}

// GetRecovery returns the current disaster recovery controls status.
func (h *HTTPHandler) GetRecovery(w http.ResponseWriter, r *http.Request) {
	controls, err := h.manager.GetControls(r.Context())
	if err != nil {
		h.writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	h.writeJSON(w, http.StatusOK, controls)
}

// PrepareRecovery executes disaster recovery preparation:
// disables admission/dispatch, revokes sessions, and marks nonterminal runs
// with disaster holds.
func (h *HTTPHandler) PrepareRecovery(w http.ResponseWriter, r *http.Request) {
	var req PrepareRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "Failed to parse JSON body")
		return
	}
	report, err := h.manager.PrepareDisasterRecovery(r.Context(), req)
	if err != nil {
		if errors.Is(err, ErrInvalidTimeRange) {
			h.writeError(w, r, http.StatusBadRequest, "INVALID_TIME_RANGE", err.Error())
			return
		}
		h.writeError(w, r, http.StatusInternalServerError, "PREPARE_RECOVERY_FAILED", err.Error())
		return
	}
	h.writeJSON(w, http.StatusOK, report)
}

// VerifyIntegrity runs integrity checks: schema, tenant boundary, artifacts, and deletion ledger.
func (h *HTTPHandler) VerifyIntegrity(w http.ResponseWriter, r *http.Request) {
	report, err := h.manager.VerifyIntegrity(r.Context())
	if err != nil {
		h.writeError(w, r, http.StatusInternalServerError, "VERIFY_FAILED", err.Error())
		return
	}
	h.writeJSON(w, http.StatusOK, report)
}

type RPOGapRequestPayload struct {
	IncidentID   string   `json:"incidentId"`
	RequestIDs   []string `json:"requestIds"`
	OperatorNote string   `json:"operatorNote,omitempty"`
}

// ReconcileRPOGap updates an incident with request IDs accepted during the uncertainty window.
func (h *HTTPHandler) ReconcileRPOGap(w http.ResponseWriter, r *http.Request) {
	var req RPOGapRequestPayload
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "Failed to parse JSON body")
		return
	}
	report, err := h.manager.ReconcileRPOGapRecords(r.Context(), req.IncidentID, req.RequestIDs, req.OperatorNote)
	if err != nil {
		if errors.Is(err, ErrIncidentNotFound) {
			h.writeError(w, r, http.StatusNotFound, "INCIDENT_NOT_FOUND", err.Error())
			return
		}
		h.writeError(w, r, http.StatusInternalServerError, "RPO_GAP_RECONCILE_FAILED", err.Error())
		return
	}
	h.writeJSON(w, http.StatusOK, report)
}

type ResumeRequestPayload struct {
	Mode string `json:"mode"`
}

// Resume updates the recovery controls to READ_ONLY, RESUMING, or ACTIVE.
func (h *HTTPHandler) Resume(w http.ResponseWriter, r *http.Request) {
	var req ResumeRequestPayload
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "Failed to parse JSON body")
		return
	}
	controls, err := h.manager.GradualResume(r.Context(), req.Mode)
	if err != nil {
		if errors.Is(err, ErrInvalidRecoveryMode) {
			h.writeError(w, r, http.StatusBadRequest, "INVALID_RECOVERY_MODE", err.Error())
			return
		}
		h.writeError(w, r, http.StatusInternalServerError, "RESUME_FAILED", err.Error())
		return
	}
	h.writeJSON(w, http.StatusOK, controls)
}
