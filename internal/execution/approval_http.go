package execution

// HTTP surface for approval control nodes (Blueprint §16.3, §20.1).
//
// The decision endpoint is deliberately narrow. It accepts a human decision,
// compares `expectedRevision` against the durable approval revision, and
// refuses machine callers before any row is locked. There is no public
// approval link, no anonymous action, and no worker-authorized path.

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
)

// HandleDecideApproval commits one approve/reject decision.
//
// POST /v1/approvals/{id}/decision
func (h *HTTPHandler) HandleDecideApproval(w http.ResponseWriter, r *http.Request) {
	caller, ok := tenant.CallerFromContext(r.Context())
	if !ok {
		errJSON(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication required")
		return
	}
	if h.engine == nil {
		errJSON(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal server error")
		return
	}
	// Blueprint §16.3 / §24.2: an approval decision is a business act by an
	// identifiable person. A machine key holding the capability is still
	// refused, because §24.2 withholds approval machine keys in V1.
	if caller.Type != tenant.IdentityTypeHuman {
		errJSON(w, r, http.StatusForbidden, "APPROVAL_HUMAN_ONLY",
			"Approval decisions require an identifiable human actor")
		return
	}

	approvalID := r.PathValue("id")
	if approvalID == "" {
		errJSON(w, r, http.StatusBadRequest, "INVALID_APPROVAL_ID", "Approval ID is required")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
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
	var req DecideApprovalRequest
	if err := json.NewDecoder(bytes.NewReader(canonical)).Decode(&req); err != nil {
		errJSON(w, r, http.StatusBadRequest, "INVALID_REQUEST", "Invalid request body")
		return
	}
	if req.Decision != contracts.ApprovalDecisionApproved && req.Decision != contracts.ApprovalDecisionRejected {
		errJSON(w, r, http.StatusUnprocessableEntity, "INVALID_DECISION", "decision must be approved or rejected")
		return
	}

	resp, err := h.engine.DecideApproval(
		r.Context(), caller.OrganizationID, approvalID, req, auditFromCaller(caller, r),
	)
	if err != nil {
		switch {
		case errors.Is(err, ErrApprovalNotFound):
			// A foreign approval is indistinguishable from a missing one so the
			// endpoint cannot confirm that another tenant's id exists (F-21).
			errJSON(w, r, http.StatusNotFound, "APPROVAL_NOT_FOUND", "Approval not found")
		case errors.Is(err, ErrApprovalHumanOnly):
			errJSON(w, r, http.StatusForbidden, "APPROVAL_HUMAN_ONLY",
				"Approval decisions require an identifiable human actor")
		case errors.Is(err, ErrApprovalConflict):
			errJSON(w, r, http.StatusConflict, "APPROVAL_CONFLICT",
				"A different decision is already committed for this approval")
		case errors.Is(err, ErrApprovalTerminal):
			errJSON(w, r, http.StatusConflict, "APPROVAL_TERMINAL", "Approval is already resolved")
		case errors.Is(err, ErrApprovalExpired):
			errJSON(w, r, http.StatusConflict, "APPROVAL_EXPIRED", "Approval request expired")
		case errors.Is(err, ErrRevisionConflict):
			errJSON(w, r, http.StatusConflict, "REVISION_CONFLICT",
				"Approval changed since it was read; refresh before acting")
		case errors.Is(err, ErrRunTerminal):
			errJSON(w, r, http.StatusConflict, "RUN_TERMINAL", "Run is already terminal")
		case errors.Is(err, ErrRunCancelling):
			errJSON(w, r, http.StatusConflict, "RUN_CANCELLING", "Run cancellation is in progress")
		case errors.Is(err, tenant.ErrAuditRequired):
			errJSON(w, r, http.StatusUnauthorized, "AUDIT_REQUIRED", "Audit context is required")
		case errors.Is(err, tenant.ErrIdempotencyConflict):
			errJSON(w, r, http.StatusConflict, "IDEMPOTENCY_CONFLICT",
				"Idempotency key already used with different request content")
		default:
			errJSON(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal server error")
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"id":       resp.ID,
		"status":   resp.Status,
		"revision": resp.Revision,
	})
}

// HandleGetApproval returns one approval in the caller's organization scope.
func (h *HTTPHandler) HandleGetApproval(w http.ResponseWriter, r *http.Request) {
	caller, ok := tenant.CallerFromContext(r.Context())
	if !ok {
		errJSON(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication required")
		return
	}
	if h.engine == nil {
		errJSON(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal server error")
		return
	}
	approvalID := r.PathValue("id")
	if approvalID == "" {
		errJSON(w, r, http.StatusBadRequest, "INVALID_APPROVAL_ID", "Approval ID is required")
		return
	}
	approval, err := h.engine.GetApproval(r.Context(), caller.OrganizationID, approvalID)
	if err != nil {
		if errors.Is(err, ErrApprovalNotFound) {
			errJSON(w, r, http.StatusNotFound, "APPROVAL_NOT_FOUND", "Approval not found")
			return
		}
		errJSON(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal server error")
		return
	}
	writeJSON(w, http.StatusOK, approval)
}

// HandleListApprovals returns the approvals inbox for one environment. The
// environment is resolved through the same authorized path as run listing, so
// scope always comes from verified authority rather than a caller-supplied
// organization identifier (Blueprint INV-01).
func (h *HTTPHandler) HandleListApprovals(w http.ResponseWriter, r *http.Request) {
	caller, ok := tenant.CallerFromContext(r.Context())
	if !ok {
		errJSON(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication required")
		return
	}
	if h.engine == nil {
		errJSON(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal server error")
		return
	}

	envParam := r.URL.Query().Get("environment")
	if envParam == "" {
		errJSON(w, r, http.StatusBadRequest, "MISSING_ENVIRONMENT", "environment query parameter is required")
		return
	}
	allowed, authErr := h.allowed(r, caller, envParam, tenant.CapApprovalsDecide)
	if !h.denyAmbiguousOrForbidden(w, r, allowed, authErr, "FORBIDDEN", "Reading approvals is not permitted") {
		return
	}
	env, err := h.service.resolveEnvironment(r.Context(), caller.OrganizationID, envParam)
	if err != nil {
		errJSON(w, r, http.StatusNotFound, "ENVIRONMENT_NOT_FOUND", "Environment not found")
		return
	}

	status := r.URL.Query().Get("status")
	limit := 25
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, convErr := strconv.Atoi(l); convErr == nil && parsed > 0 {
			limit = parsed
		}
	}
	items, err := h.engine.ListApprovals(r.Context(), caller.OrganizationID, env.ID, status, limit)
	if err != nil {
		errJSON(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}
