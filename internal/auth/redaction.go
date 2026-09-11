package auth

import (
	"encoding/json"
	"net/http"
	"strings"
)

// RedactedHeaders lists headers that must never be exposed or logged in plain text
var RedactedHeaders = map[string]bool{
	"authorization":         true,
	"cookie":                true,
	"set-cookie":            true,
	"x-csrf-token":          true,
	"idempotency-key":       false, // safe
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

// ClearSessionCookie sets an expired cookie to cleanly clear the session from the client
func ClearSessionCookie(w http.ResponseWriter, secure bool) {
	http.SetCookie(w, &http.Cookie{
		Name:     SessionCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteLaxMode,
	})
}
