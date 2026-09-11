package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
)

type contextKey string

const (
	SessionContextKey contextKey = "deadbolt.auth.session"
	PKCECookieName               = "__Host-deadbolt_pkce"
)

// SessionFromContext extracts the authenticated Session from request context
func SessionFromContext(ctx context.Context) (*Session, bool) {
	s, ok := ctx.Value(SessionContextKey).(*Session)
	return s, ok
}

// ContextWithSession stores the authenticated Session in request context
func ContextWithSession(ctx context.Context, s *Session) context.Context {
	return context.WithValue(ctx, SessionContextKey, s)
}

// BFFHandler manages the Go BFF auth routes and middleware
type BFFHandler struct {
	cfg   Config
	oidc  *OIDCClient
	store *SessionStore
	pool  *pgxpool.Pool
}

// NewBFFHandler initializes a new BFFHandler
func NewBFFHandler(cfg Config, oidc *OIDCClient, store *SessionStore, pool *pgxpool.Pool) *BFFHandler {
	if cfg.SessionIdleTimeout <= 0 {
		cfg.SessionIdleTimeout = DefaultSessionIdleTimeout
	}
	if cfg.SessionAbsoluteTimeout <= 0 {
		cfg.SessionAbsoluteTimeout = DefaultSessionAbsoluteTimeout
	}
	return &BFFHandler{
		cfg:   cfg,
		oidc:  oidc,
		store: store,
		pool:  pool,
	}
}

// SetSessionCookie sets the secure __Host-runtime_session cookie
func (h *BFFHandler) SetSessionCookie(w http.ResponseWriter, rawSessionToken string, maxAge int) {
	http.SetCookie(w, &http.Cookie{
		Name:     SessionCookieName,
		Value:    rawSessionToken,
		Path:     "/",
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   h.cfg.CookieSecure,
		SameSite: http.SameSiteLaxMode,
	})
}

// HandleLogin initiates the OIDC authorization code flow with PKCE
func (h *BFFHandler) HandleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteSanitizedError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed")
		return
	}

	pkce, err := GeneratePKCE()
	if err != nil {
		WriteSanitizedError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to initialize auth request")
		return
	}

	// Store PKCE in temporary short-lived HttpOnly cookie (10 minutes)
	pkcePayload, _ := json.Marshal(pkce)
	encodedPKCE := base64.RawURLEncoding.EncodeToString(pkcePayload)
	http.SetCookie(w, &http.Cookie{
		Name:     PKCECookieName,
		Value:    encodedPKCE,
		Path:     "/",
		MaxAge:   600,
		HttpOnly: true,
		Secure:   h.cfg.CookieSecure,
		SameSite: http.SameSiteLaxMode,
	})

	authURL, err := h.oidc.BuildAuthorizationURL(r.Context(), pkce)
	if err != nil {
		WriteSanitizedError(w, http.StatusInternalServerError, "OIDC_ERROR", "Failed to build authorization URL")
		return
	}

	http.Redirect(w, r, authURL, http.StatusFound)
}

// HandleCallback processes the OIDC authorization callback
func (h *BFFHandler) HandleCallback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteSanitizedError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed")
		return
	}

	code := r.URL.Query().Get("code")
	state := r.URL.Query().Get("state")
	if code == "" || state == "" {
		WriteSanitizedError(w, http.StatusBadRequest, "INVALID_REQUEST", "Missing code or state parameter")
		return
	}

	// Retrieve and clear PKCE cookie
	pkceCookie, err := r.Cookie(PKCECookieName)
	if err != nil || pkceCookie.Value == "" {
		WriteSanitizedError(w, http.StatusBadRequest, "OIDC_STATE_MISMATCH", "Missing or expired PKCE state")
		return
	}
	// Clear PKCE cookie
	http.SetCookie(w, &http.Cookie{
		Name:     PKCECookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   h.cfg.CookieSecure,
		SameSite: http.SameSiteLaxMode,
	})

	rawPKCE, err := base64.RawURLEncoding.DecodeString(pkceCookie.Value)
	if err != nil {
		WriteSanitizedError(w, http.StatusBadRequest, "OIDC_STATE_MISMATCH", "Invalid authorization cookie format")
		return
	}

	var pkce PKCEParams
	if err := json.Unmarshal(rawPKCE, &pkce); err != nil || pkce.State != state {
		WriteSanitizedError(w, http.StatusBadRequest, "OIDC_STATE_MISMATCH", "Invalid authorization state")
		return
	}

	// Exchange code + verifier with OIDC provider
	identity, err := h.oidc.ExchangeAndVerify(r.Context(), code, pkce.CodeVerifier, pkce.Nonce)
	if err != nil {
		WriteSanitizedError(w, http.StatusUnauthorized, "OIDC_VERIFICATION_FAILED", "Identity token verification failed")
		return
	}

	// Upsert user and oidc_identities
	user, err := h.store.GetOrCreateUserFromOIDC(r.Context(), identity)
	if err != nil {
		WriteSanitizedError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to persist user identity")
		return
	}

	// Discover user memberships across organizations
	memberships, err := storage.DiscoverUserMemberships(r.Context(), h.pool, user.ID)
	if err != nil {
		WriteSanitizedError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to discover user memberships")
		return
	}

	var defaultOrgID *string
	if len(memberships) > 0 {
		defaultOrgID = &memberships[0].OrganizationID
	}

	ip := r.RemoteAddr
	userAgent := r.UserAgent()

	// Create new session
	sess, rawToken, rawCSRF, err := h.store.CreateSession(
		r.Context(),
		user.ID,
		defaultOrgID,
		ip,
		userAgent,
		h.cfg.SessionIdleTimeout,
		h.cfg.SessionAbsoluteTimeout,
	)
	if err != nil {
		WriteSanitizedError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to establish session")
		return
	}

	// Issue __Host-runtime_session cookie
	maxAge := int(h.cfg.SessionAbsoluteTimeout.Seconds())
	h.SetSessionCookie(w, rawToken, maxAge)

	// In response header provide CSRF token for single-page dashboard apps
	w.Header().Set("X-CSRF-Token", rawCSRF)

	// Redirect to application root or dashboard
	http.Redirect(w, r, "/", http.StatusFound)
	_ = sess
}

// HandleLogout terminates the user session in the database and clears the session cookie
func (h *BFFHandler) HandleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		WriteSanitizedError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed")
		return
	}

	// If session exists in context, revoke it in DB
	if sess, ok := SessionFromContext(r.Context()); ok && sess != nil {
		_ = h.store.RevokeSession(r.Context(), sess.ID, "LOGOUT")
	} else if cookie, err := r.Cookie(SessionCookieName); err == nil && cookie.Value != "" {
		if sess, err := h.store.ValidateSession(r.Context(), cookie.Value, h.cfg.SessionIdleTimeout); err == nil {
			_ = h.store.RevokeSession(r.Context(), sess.ID, "LOGOUT")
		}
	}

	// Clear session cookie
	ClearSessionCookie(w, h.cfg.CookieSecure)
	w.WriteHeader(http.StatusNoContent)
}

// HandleGetSession returns current authenticated user and session metadata
func (h *BFFHandler) HandleGetSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteSanitizedError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed")
		return
	}

	sess, ok := SessionFromContext(r.Context())
	if !ok || sess == nil {
		WriteSanitizedError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "No active session")
		return
	}

	// Discover user memberships
	memberships, err := storage.DiscoverUserMemberships(r.Context(), h.pool, sess.UserID)
	if err != nil {
		memberships = []storage.UserMembership{}
	}

	var u User
	err = h.pool.QueryRow(r.Context(), `
		SELECT id, COALESCE(email, ''), COALESCE(name, ''), created_at, updated_at
		FROM users WHERE id = $1
	`, sess.UserID).Scan(&u.ID, &u.Email, &u.Name, &u.CreatedAt, &u.UpdatedAt)
	if err != nil {
		WriteSanitizedError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load user info")
		return
	}

	resp := map[string]interface{}{
		"user":                   u,
		"active_organization_id": sess.ActiveOrganizationID,
		"memberships":            memberships,
		"created_at":             sess.CreatedAt,
		"last_seen_at":           sess.LastSeenAt,
		"idle_expires_at":        sess.IdleExpiresAt,
		"absolute_expires_at":    sess.AbsoluteExpiresAt,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// HandleSwitchOrg switches the active organization and rotates session ID
func (h *BFFHandler) HandleSwitchOrg(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		WriteSanitizedError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed")
		return
	}

	sess, ok := SessionFromContext(r.Context())
	if !ok || sess == nil {
		WriteSanitizedError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "No active session")
		return
	}

	var req struct {
		OrganizationID string `json:"organization_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.OrganizationID == "" {
		WriteSanitizedError(w, http.StatusBadRequest, "INVALID_REQUEST", "Invalid organization_id payload")
		return
	}

	// Verify user is an active member of requested organization
	memberships, err := storage.DiscoverUserMemberships(r.Context(), h.pool, sess.UserID)
	if err != nil {
		WriteSanitizedError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to verify organization membership")
		return
	}

	var targetFound bool
	for _, m := range memberships {
		if m.OrganizationID == req.OrganizationID && m.Status == "ACTIVE" {
			targetFound = true
			break
		}
	}
	if !targetFound {
		WriteSanitizedError(w, http.StatusForbidden, "FORBIDDEN_ORGANIZATION_MEMBERSHIP", "User is not an active member of the requested organization")
		return
	}

	ip := r.RemoteAddr
	userAgent := r.UserAgent()

	// Rotate session with new organization ID
	newSess, rawSessionToken, rawCSRFToken, err := h.store.RotateSession(
		r.Context(),
		sess.ID,
		sess.UserID,
		&req.OrganizationID,
		ip,
		userAgent,
		h.cfg.SessionIdleTimeout,
		h.cfg.SessionAbsoluteTimeout,
	)
	if err != nil {
		WriteSanitizedError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to rotate session")
		return
	}

	// Set rotated cookie
	maxAge := int(h.cfg.SessionAbsoluteTimeout.Seconds())
	h.SetSessionCookie(w, rawSessionToken, maxAge)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-CSRF-Token", rawCSRFToken)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"session_id":             newSess.ID,
		"active_organization_id": newSess.ActiveOrganizationID,
		"csrf_token":             rawCSRFToken,
	})
}

// RequireAuth middleware extracts the session cookie and validates against DB revocation & timeouts
func (h *BFFHandler) RequireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie(SessionCookieName)
		if err != nil || cookie.Value == "" {
			WriteSanitizedError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Missing session cookie")
			return
		}

		sess, err := h.store.ValidateSession(r.Context(), cookie.Value, h.cfg.SessionIdleTimeout)
		if err != nil {
			// Clear invalid cookie
			ClearSessionCookie(w, h.cfg.CookieSecure)

			if errors.Is(err, ErrSessionRevoked) {
				WriteSanitizedError(w, http.StatusUnauthorized, "SESSION_REVOKED", "Session has been revoked")
				return
			}
			if errors.Is(err, ErrSessionIdleTimeout) {
				WriteSanitizedError(w, http.StatusUnauthorized, "SESSION_IDLE_TIMEOUT", "Session idle timeout exceeded")
				return
			}
			if errors.Is(err, ErrSessionExpired) {
				WriteSanitizedError(w, http.StatusUnauthorized, "SESSION_EXPIRED", "Session absolute expiration reached")
				return
			}
			WriteSanitizedError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Invalid session")
			return
		}

		ctx := ContextWithSession(r.Context(), sess)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequireCSRFAndOrigin middleware verifies allowed Origin and X-CSRF-Token on mutating requests
func (h *BFFHandler) RequireCSRFAndOrigin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only check mutating methods: POST, PUT, PATCH, DELETE
		method := strings.ToUpper(r.Method)
		if method == http.MethodGet || method == http.MethodHead || method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}

		sess, ok := SessionFromContext(r.Context())
		if !ok || sess == nil {
			WriteSanitizedError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication required")
			return
		}

		// 1. Origin check
		origin := r.Header.Get("Origin")
		if origin == "" {
			origin = r.Header.Get("Referer")
		}
		if origin == "" || !h.cfg.IsOriginAllowed(origin) {
			WriteSanitizedError(w, http.StatusForbidden, "ORIGIN_FORBIDDEN", fmt.Sprintf("Origin %q is not allowlisted", origin))
			return
		}

		// 2. CSRF Token check
		csrfToken := r.Header.Get("X-CSRF-Token")
		if csrfToken == "" || !ValidateCSRFToken(sess, csrfToken) {
			WriteSanitizedError(w, http.StatusForbidden, "CSRF_VALIDATION_FAILED", "Missing or invalid X-CSRF-Token header")
			return
		}

		next.ServeHTTP(w, r)
	})
}
