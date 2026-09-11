package auth

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DevAuthHandler handles mock local development authentication
type DevAuthHandler struct {
	cfg   Config
	store *SessionStore
	pool  *pgxpool.Pool
}

// NewDevAuthHandler creates a new DevAuthHandler
func NewDevAuthHandler(cfg Config, store *SessionStore, pool *pgxpool.Pool) *DevAuthHandler {
	return &DevAuthHandler{
		cfg:   cfg,
		store: store,
		pool:  pool,
	}
}

// HandleDevLogin handles local dev login requests on loopback interfaces
func (h *DevAuthHandler) HandleDevLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		WriteSanitizedError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed")
		return
	}

	// 1. Strict hosted mode boundary check
	if h.cfg.RuntimeMode != ModeLocal || !h.cfg.DevAuthEnabled {
		WriteSanitizedError(w, http.StatusForbidden, "DEV_AUTH_DISABLED", "Dev auth is not permitted in hosted mode")
		return
	}

	// 2. Strict loopback boundary check
	if !isLoopbackHost(r.RemoteAddr) {
		WriteSanitizedError(w, http.StatusForbidden, "LOOPBACK_REQUIRED", "Dev auth is restricted strictly to loopback callers")
		return
	}

	var req struct {
		Email string `json:"email"`
		Name  string `json:"name"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Email == "" {
		req.Email = "dev-admin@deadbolt.local"
	}
	if req.Name == "" {
		req.Name = "Local Development Admin"
	}

	// 3. Create or find mock user & identity
	mockIdentity := &Identity{
		Issuer:  "https://local-dev.deadbolt.internal",
		Subject: "dev-sub-" + req.Email,
		Email:   req.Email,
		Name:    req.Name,
		RawClaims: map[string]interface{}{
			"dev_mode": true,
		},
	}

	user, err := h.store.GetOrCreateUserFromOIDC(r.Context(), mockIdentity)
	if err != nil {
		WriteSanitizedError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create dev user")
		return
	}

	memberships, err := storage.DiscoverUserMemberships(r.Context(), h.pool, user.ID)
	if err != nil {
		memberships = []storage.UserMembership{}
	}
	var defaultOrgID *string
	if len(memberships) > 0 {
		defaultOrgID = &memberships[0].OrganizationID
	}

	// 4. Create local dev session
	sess, rawToken, rawCSRF, err := h.store.CreateSession(
		r.Context(),
		user.ID,
		defaultOrgID,
		r.RemoteAddr,
		r.UserAgent(),
		h.cfg.SessionIdleTimeout,
		h.cfg.SessionAbsoluteTimeout,
	)
	if err != nil {
		WriteSanitizedError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create dev session")
		return
	}

	// Prominent security banner in logs (Blueprint §22.2)
	log.Printf("==================================================================")
	log.Printf("[DEADBOLT DEV AUTH] Active local-dev session issued for %s (%s)", user.Email, user.ID)
	log.Printf("WARNING: Dev auth must NEVER be enabled in production or hosted mode!")
	log.Printf("==================================================================")

	// Set session cookie
	maxAge := int(h.cfg.SessionAbsoluteTimeout.Seconds())
	http.SetCookie(w, &http.Cookie{
		Name:     SessionCookieName,
		Value:    rawToken,
		Path:     "/",
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   h.cfg.CookieSecure,
		SameSite: http.SameSiteLaxMode,
	})

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Deadbolt-Auth-Mode", "local-dev")
	w.Header().Set("X-CSRF-Token", rawCSRF)

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"mode":                   "local-dev",
		"session_id":             sess.ID,
		"user":                   user,
		"active_organization_id": sess.ActiveOrganizationID,
		"csrf_token":             rawCSRF,
		"warning":                "Local development mode active. Do not use production credentials.",
	})
	_ = fmt.Sprintf("")
}
