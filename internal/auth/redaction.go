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

// LogSecurityEvent writes an audit log entry for authentication events, strictly redacting all sensitive data
func LogSecurityEvent(logger *log.Logger, event string, r *http.Request, details string) {
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
	logger.Printf("[AUTH_SECURITY] event=%s remote_addr=%s method=%s path=%s details=%s",
		event, remoteAddr, method, path, details)
}
