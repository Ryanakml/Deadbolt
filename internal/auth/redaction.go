package auth

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
)

// RedactedHeaders lists headers that must never be exposed or logged in plain text
var RedactedHeaders = map[string]bool{
	"authorization":   true,
	"cookie":          true,
	"set-cookie":      true,
	"x-csrf-token":    true,
	"idempotency-key": false, // safe
}

// ErrorResponse represents a sanitized standard error payload
type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// WriteSanitizedError writes a clean error response without exposing tokens, cookies, or secrets
func WriteSanitizedError(w http.ResponseWriter, statusCode int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(ErrorResponse{
		Code:    code,
		Message: message,
	})
}

// SanitizeHeader returns a redacted string if the header contains sensitive data
func SanitizeHeader(headerName, headerValue string) string {
	lower := strings.ToLower(headerName)
	if RedactedHeaders[lower] {
		return "[REDACTED]"
	}
	return headerValue
}

// ClearSessionCookies sets expired cookies to cleanly clear both session and CSRF cookies
func ClearSessionCookies(w http.ResponseWriter, cfg Config) {
	http.SetCookie(w, &http.Cookie{
		Name:     cfg.SessionCookieName(),
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   cfg.CookieSecure,
		SameSite: http.SameSiteLaxMode,
	})
	http.SetCookie(w, &http.Cookie{
		Name:     cfg.CSRFCookieName(),
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: false,
		Secure:   cfg.CookieSecure,
		SameSite: http.SameSiteLaxMode,
	})
}

// ClearSessionCookie sets an expired cookie for the session
func ClearSessionCookie(w http.ResponseWriter, secure bool) {
	name := SessionCookieName
	if !secure {
		name = LocalSessionCookieName
	}
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteLaxMode,
	})
}

// SecurityReason defines a typed enumeration of safe, allowlisted event reasons.
// Arbitrary error strings, upstream HTTP response bodies, tokens, secrets,
// and other-tenant identifiers must never be logged.
type SecurityReason string

const (
	ReasonTokenVerificationFailed   SecurityReason = "token_verification_failed"
	ReasonTokenExchangeFailed       SecurityReason = "token_exchange_failed"
	ReasonUnauthorizedOrgMembership SecurityReason = "unauthorized_org_membership"
	ReasonPKCEGenerationFailed      SecurityReason = "pkce_generation_failed"
	ReasonAuthURLBuildFailed        SecurityReason = "auth_url_build_failed"
	ReasonMissingCodeOrState        SecurityReason = "missing_code_or_state"
	ReasonPKCEMissingOrExpired      SecurityReason = "pkce_cookie_missing_or_expired"
	ReasonPKCEDecodeFailed          SecurityReason = "invalid_cookie_encoding"
	ReasonPKCEStateMismatch         SecurityReason = "state_mismatch"
	ReasonUserPersistenceFailed     SecurityReason = "user_persistence_failed"
	ReasonMembershipDiscoveryFailed SecurityReason = "membership_discovery_failed"
	ReasonSessionCreationFailed     SecurityReason = "session_creation_failed"
	ReasonSessionMissing            SecurityReason = "session_missing"
	ReasonSessionRevocationFailed   SecurityReason = "session_revocation_failed"
	ReasonSessionRotationFailed     SecurityReason = "session_rotation_failed"
	ReasonCookieMissing             SecurityReason = "cookie_missing"
	ReasonSessionRevoked            SecurityReason = "session_revoked"
	ReasonSessionIdleTimeout        SecurityReason = "session_idle_timeout"
	ReasonSessionAbsoluteTimeout    SecurityReason = "session_absolute_timeout"
	ReasonUnrecognizedSession       SecurityReason = "unrecognized_session"
	ReasonUnauthenticatedMutation   SecurityReason = "unauthenticated_mutation"
	ReasonOriginNotAllowlisted      SecurityReason = "origin_not_allowlisted"
	ReasonCSRFValidationFailed      SecurityReason = "csrf_validation_failed"
	ReasonLoginSuccess              SecurityReason = "login_success"
	ReasonLogoutSuccess             SecurityReason = "logout_success"
	ReasonOrgSwitchSuccess          SecurityReason = "org_switch_success"
)

var allowlistedReasons = map[SecurityReason]bool{
	ReasonTokenVerificationFailed:   true,
	ReasonTokenExchangeFailed:       true,
	ReasonUnauthorizedOrgMembership: true,
	ReasonPKCEGenerationFailed:      true,
	ReasonAuthURLBuildFailed:        true,
	ReasonMissingCodeOrState:        true,
	ReasonPKCEMissingOrExpired:      true,
	ReasonPKCEDecodeFailed:          true,
	ReasonPKCEStateMismatch:         true,
	ReasonUserPersistenceFailed:     true,
	ReasonMembershipDiscoveryFailed: true,
	ReasonSessionCreationFailed:     true,
	ReasonSessionMissing:            true,
	ReasonSessionRevocationFailed:   true,
	ReasonSessionRotationFailed:     true,
	ReasonCookieMissing:             true,
	ReasonSessionRevoked:            true,
	ReasonSessionIdleTimeout:        true,
	ReasonSessionAbsoluteTimeout:    true,
	ReasonUnrecognizedSession:       true,
	ReasonUnauthenticatedMutation:   true,
	ReasonOriginNotAllowlisted:      true,
	ReasonCSRFValidationFailed:      true,
	ReasonLoginSuccess:              true,
	ReasonLogoutSuccess:             true,
	ReasonOrgSwitchSuccess:          true,
}

// LogSecurityEvent writes an audit log entry for authentication events, strictly redacting all sensitive data
// and enforcing safe, allowlisted reason codes. Raw error bodies, secrets, and other-tenant data are strictly barred.
func LogSecurityEvent(logger *log.Logger, event string, r *http.Request, reason SecurityReason) {
	if logger == nil {
		return
	}
	remoteAddr := ""
	method := ""
	path := ""
	if r != nil {
		remoteAddr = r.RemoteAddr
		method = r.Method
		path = r.URL.Path
	}

	safeReason := reason
	if !allowlistedReasons[reason] {
		safeReason = "unclassified_security_event"
	}

	logger.Printf("[AUTH_SECURITY] event=%s reason=%s remote_addr=%s method=%s path=%s",
		event, safeReason, remoteAddr, method, path)
}
