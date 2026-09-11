package integration_test

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/auth"
	"github.com/Ryanakml/Deadbolt/internal/auth/oidcfixture"
)

// TestOIDCLoginAndSessionLifecycle verifies full OIDC PKCE flow, JWKS verification,
// user upsert, session establishment, session rotation, and DB-backed revocation.
func TestOIDCLoginAndSessionLifecycle(t *testing.T) {
	db, runtimePool, _ := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	// 1. Start local OIDC fixture server
	fixture, err := oidcfixture.NewFixtureServer("deadbolt-dashboard-client")
	if err != nil {
		t.Fatalf("failed to start OIDC fixture server: %v", err)
	}
	defer fixture.Close()

	// 2. Configure BFF and OIDC client
	cfg := auth.Config{
		RuntimeMode: auth.ModeHosted,
		OIDC: auth.OIDCConfig{
			Issuer:       fixture.URL(),
			ClientID:     fixture.ClientID(),
			ClientSecret: "fixture-client-secret",
			RedirectURL:  "http://localhost:8080/api/auth/callback",
		},
		AllowedOrigins:         []string{"http://localhost:3000", "http://localhost:8080"},
		DevAuthEnabled:         false,
		CookieSecure:           true,
		SessionIdleTimeout:     12 * time.Hour,
		SessionAbsoluteTimeout: 7 * 24 * time.Hour,
	}
	if err := cfg.Validate("127.0.0.1"); err != nil {
		t.Fatalf("config validation failed: %v", err)
	}

	oidcClient := auth.NewOIDCClient(cfg.OIDC, nil)
	store := auth.NewSessionStore(runtimePool)
	bff := auth.NewBFFHandler(cfg, oidcClient, store, runtimePool)

	// 3. Initiate Login (GET /api/auth/login)
	loginReq := httptest.NewRequest(http.MethodGet, "/api/auth/login", nil)
	loginRec := httptest.NewRecorder()
	bff.HandleLogin(loginRec, loginReq)

	if loginRec.Code != http.StatusFound {
		t.Fatalf("expected 302 redirect from /api/auth/login, got %d", loginRec.Code)
	}

	// Verify PKCE temporary cookie was set
	cookies := loginRec.Result().Cookies()
	var pkceCookie *http.Cookie
	for _, c := range cookies {
		if c.Name == auth.PKCECookieName {
			pkceCookie = c
			break
		}
	}
	if pkceCookie == nil {
		t.Fatalf("expected %s cookie to be set during login", auth.PKCECookieName)
	}

	rawPKCE, err := base64.RawURLEncoding.DecodeString(pkceCookie.Value)
	if err != nil {
		t.Fatalf("failed to base64 decode PKCE cookie: %v", err)
	}

	var pkce auth.PKCEParams
	if err := json.Unmarshal(rawPKCE, &pkce); err != nil {
		t.Fatalf("failed to unmarshal PKCE cookie: %v", err)
	}
	if pkce.CodeVerifier == "" || pkce.State == "" || pkce.Nonce == "" {
		t.Fatalf("PKCE parameters incomplete: %+v", pkce)
	}

	// 4. Pre-register auth code in OIDC fixture bound to PKCE S256 code challenge
	authCode := "valid-test-code-99"
	fixture.RegisterAuthCodeWithPKCE(authCode, oidcfixture.TokenClaimOverrides{
		Subject: "usr-sub-alpha",
		Email:   "alpha@deadbolt.local",
		Name:    "Alpha User",
		Nonce:   pkce.Nonce,
		Expiry:  1 * time.Hour,
	}, pkce.CodeChallenge, "S256")

	// 5. Callback (GET /api/auth/callback?code=...&state=...)
	callbackReq := httptest.NewRequest(http.MethodGet, "/api/auth/callback?code="+authCode+"&state="+pkce.State, nil)
	callbackReq.AddCookie(pkceCookie)
	callbackRec := httptest.NewRecorder()
	bff.HandleCallback(callbackRec, callbackReq)

	if callbackRec.Code != http.StatusFound {
		t.Fatalf("expected 302 redirect from /api/auth/callback, got %d (body: %s)", callbackRec.Code, callbackRec.Body.String())
	}

	// Verify __Host-runtime_session cookie attributes (HttpOnly, Secure, SameSite=Lax, Path=/)
	var sessionCookie *http.Cookie
	var csrfCookie *http.Cookie
	for _, c := range callbackRec.Result().Cookies() {
		if c.Name == auth.SessionCookieName {
			sessionCookie = c
		}
		if c.Name == auth.CSRFCookieName {
			csrfCookie = c
		}
	}
	if sessionCookie == nil || sessionCookie.Value == "" {
		t.Fatalf("expected %s cookie to be issued on callback", auth.SessionCookieName)
	}
	if !sessionCookie.HttpOnly {
		t.Fatalf("expected session cookie HttpOnly=true")
	}
	if !sessionCookie.Secure {
		t.Fatalf("expected session cookie Secure=true")
	}
	if sessionCookie.SameSite != http.SameSiteLaxMode {
		t.Fatalf("expected session cookie SameSite=Lax, got %v", sessionCookie.SameSite)
	}
	if sessionCookie.Path != "/" {
		t.Fatalf("expected session cookie Path=/, got %s", sessionCookie.Path)
	}

	// Verify readable __Host-csrf_token cookie for SPA bootstrap (HttpOnly=false, Secure=true, SameSite=Lax)
	if csrfCookie == nil || csrfCookie.Value == "" {
		t.Fatalf("expected %s readable cookie to be issued on callback", auth.CSRFCookieName)
	}
	if csrfCookie.HttpOnly {
		t.Fatalf("expected CSRF bootstrap cookie HttpOnly=false so SPA JS can read it")
	}
	if !csrfCookie.Secure {
		t.Fatalf("expected CSRF bootstrap cookie Secure=true")
	}

	rawCSRF := callbackRec.Header().Get("X-CSRF-Token")
	if rawCSRF == "" || rawCSRF != csrfCookie.Value {
		t.Fatalf("expected X-CSRF-Token header to match readable cookie, got header=%q cookie=%q", rawCSRF, csrfCookie.Value)
	}

	// 6. Verify User and OIDC Identity in DB
	var dbUserID string
	err = db.QueryRow("SELECT user_id FROM oidc_identities WHERE issuer = $1 AND subject = $2", fixture.URL(), "usr-sub-alpha").Scan(&dbUserID)
	if err != nil {
		t.Fatalf("failed to find oidc_identity in DB: %v", err)
	}

	// 7. Verify Session in DB and access via GET /api/auth/session
	sessionReq := httptest.NewRequest(http.MethodGet, "/api/auth/session", nil)
	sessionReq.AddCookie(sessionCookie)
	sessionRec := httptest.NewRecorder()
	bff.RequireAuth(http.HandlerFunc(bff.HandleGetSession)).ServeHTTP(sessionRec, sessionReq)

	if sessionRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /api/auth/session, got %d (body: %s)", sessionRec.Code, sessionRec.Body.String())
	}
	var sessResp map[string]interface{}
	if err := json.NewDecoder(sessionRec.Body).Decode(&sessResp); err != nil {
		t.Fatalf("failed to decode session response: %v", err)
	}
	userObj := sessResp["user"].(map[string]interface{})
	if userObj["id"] != dbUserID || userObj["email"] != "alpha@deadbolt.local" {
		t.Fatalf("unexpected user in session payload: %+v", userObj)
	}

	// 8. Test Session Rotation on Organization Switch
	testOrgID := "00000000-0000-0000-0000-000000000099"
	_, _ = db.Exec("INSERT INTO organizations (id, name) VALUES ($1, 'Rotation Test Org') ON CONFLICT DO NOTHING", testOrgID)
	_, _ = db.Exec("INSERT INTO organization_members (organization_id, user_id, role, status) VALUES ($1, $2, 'Admin', 'ACTIVE') ON CONFLICT DO NOTHING", testOrgID, dbUserID)

	switchPayload, _ := json.Marshal(map[string]string{"organization_id": testOrgID})
	switchReq := httptest.NewRequest(http.MethodPost, "/api/auth/switch-org", bytes.NewReader(switchPayload))
	switchReq.AddCookie(sessionCookie)
	switchReq.Header.Set("Origin", "http://localhost:3000")
	switchReq.Header.Set("X-CSRF-Token", rawCSRF)
	switchRec := httptest.NewRecorder()

	bff.RequireAuth(bff.RequireCSRFAndOrigin(http.HandlerFunc(bff.HandleSwitchOrg))).ServeHTTP(switchRec, switchReq)
	if switchRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /api/auth/switch-org, got %d (body: %s)", switchRec.Code, switchRec.Body.String())
	}

	var rotatedCookie *http.Cookie
	var rotatedCSRFCookie *http.Cookie
	for _, c := range switchRec.Result().Cookies() {
		if c.Name == auth.SessionCookieName {
			rotatedCookie = c
		}
		if c.Name == auth.CSRFCookieName {
			rotatedCSRFCookie = c
		}
	}
	if rotatedCookie == nil || rotatedCookie.Value == sessionCookie.Value {
		t.Fatalf("CONCURRENCY/SECURITY VIOLATION: session cookie was not rotated on privilege change")
	}
	if rotatedCSRFCookie == nil || rotatedCSRFCookie.Value == rawCSRF {
		t.Fatalf("expected CSRF cookie to rotate on org switch")
	}
	newCSRF := rotatedCSRFCookie.Value

	// Verify old session is marked ROTATED in DB
	oldHash := auth.HashToken(sessionCookie.Value)
	var oldRevokedReason string
	err = db.QueryRow("SELECT revocation_reason FROM auth_sessions WHERE session_token_hash = $1", oldHash).Scan(&oldRevokedReason)
	if err != nil || oldRevokedReason != "ROTATED" {
		t.Fatalf("expected old session marked ROTATED in DB, got err=%v, reason=%s", err, oldRevokedReason)
	}

	// Verify old session cookie is rejected
	rejectedOldReq := httptest.NewRequest(http.MethodGet, "/api/auth/session", nil)
	rejectedOldReq.AddCookie(sessionCookie)
	rejectedOldRec := httptest.NewRecorder()
	bff.RequireAuth(http.HandlerFunc(bff.HandleGetSession)).ServeHTTP(rejectedOldRec, rejectedOldReq)
	if rejectedOldRec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized for rotated old session, got %d", rejectedOldRec.Code)
	}

	// 9. Test Logout Security Boundaries: Requires Auth + CSRF + Origin
	// 9a. Logout without active session -> 401
	unauthLogoutReq := httptest.NewRequest(http.MethodPost, "/api/auth/logout", nil)
	unauthLogoutRec := httptest.NewRecorder()
	bff.RequireAuth(bff.RequireCSRFAndOrigin(http.HandlerFunc(bff.HandleLogout))).ServeHTTP(unauthLogoutRec, unauthLogoutReq)
	if unauthLogoutRec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 on unauthenticated logout, got %d", unauthLogoutRec.Code)
	}

	// 9b. Logout with session but missing/untrusted Origin -> 403 ORIGIN_FORBIDDEN
	badOriginLogoutReq := httptest.NewRequest(http.MethodPost, "/api/auth/logout", nil)
	badOriginLogoutReq.AddCookie(rotatedCookie)
	badOriginLogoutReq.Header.Set("Origin", "https://untrusted-attacker.com")
	badOriginLogoutReq.Header.Set("X-CSRF-Token", newCSRF)
	badOriginLogoutRec := httptest.NewRecorder()
	bff.RequireAuth(bff.RequireCSRFAndOrigin(http.HandlerFunc(bff.HandleLogout))).ServeHTTP(badOriginLogoutRec, badOriginLogoutReq)
	if badOriginLogoutRec.Code != http.StatusForbidden || !strings.Contains(badOriginLogoutRec.Body.String(), "ORIGIN_FORBIDDEN") {
		t.Fatalf("expected 403 ORIGIN_FORBIDDEN on logout, got %d (%s)", badOriginLogoutRec.Code, badOriginLogoutRec.Body.String())
	}

	// 9c. Logout with session, allowed Origin, but invalid CSRF token -> 403 CSRF_VALIDATION_FAILED
	badCSRFLogoutReq := httptest.NewRequest(http.MethodPost, "/api/auth/logout", nil)
	badCSRFLogoutReq.AddCookie(rotatedCookie)
	badCSRFLogoutReq.Header.Set("Origin", "http://localhost:3000")
	badCSRFLogoutReq.Header.Set("X-CSRF-Token", "forged-csrf-token")
	badCSRFLogoutRec := httptest.NewRecorder()
	bff.RequireAuth(bff.RequireCSRFAndOrigin(http.HandlerFunc(bff.HandleLogout))).ServeHTTP(badCSRFLogoutRec, badCSRFLogoutReq)
	if badCSRFLogoutRec.Code != http.StatusForbidden || !strings.Contains(badCSRFLogoutRec.Body.String(), "CSRF_VALIDATION_FAILED") {
		t.Fatalf("expected 403 CSRF_VALIDATION_FAILED on logout, got %d (%s)", badCSRFLogoutRec.Code, badCSRFLogoutRec.Body.String())
	}

	// 9d. Valid Logout -> 204 No Content, session revoked, cookies cleared
	validLogoutReq := httptest.NewRequest(http.MethodPost, "/api/auth/logout", nil)
	validLogoutReq.AddCookie(rotatedCookie)
	validLogoutReq.Header.Set("Origin", "http://localhost:3000")
	validLogoutReq.Header.Set("X-CSRF-Token", newCSRF)
	validLogoutRec := httptest.NewRecorder()
	bff.RequireAuth(bff.RequireCSRFAndOrigin(http.HandlerFunc(bff.HandleLogout))).ServeHTTP(validLogoutRec, validLogoutReq)

	if validLogoutRec.Code != http.StatusNoContent {
		t.Fatalf("expected 204 No Content from logout, got %d (body: %s)", validLogoutRec.Code, validLogoutRec.Body.String())
	}

	// Verify session is marked LOGOUT in DB
	newHash := auth.HashToken(rotatedCookie.Value)
	var newRevokedReason string
	err = db.QueryRow("SELECT revocation_reason FROM auth_sessions WHERE session_token_hash = $1", newHash).Scan(&newRevokedReason)
	if err != nil || newRevokedReason != "LOGOUT" {
		t.Fatalf("expected session marked LOGOUT in DB, got err=%v, reason=%s", err, newRevokedReason)
	}

	// Verify subsequent requests with revoked session fail
	postLogoutReq := httptest.NewRequest(http.MethodGet, "/api/auth/session", nil)
	postLogoutReq.AddCookie(rotatedCookie)
	postLogoutRec := httptest.NewRecorder()
	bff.RequireAuth(http.HandlerFunc(bff.HandleGetSession)).ServeHTTP(postLogoutRec, postLogoutReq)
	if postLogoutRec.Code != http.StatusUnauthorized || !strings.Contains(postLogoutRec.Body.String(), "SESSION_REVOKED") {
		t.Fatalf("expected 401 SESSION_REVOKED after logout, got %d (%s)", postLogoutRec.Code, postLogoutRec.Body.String())
	}
}

// TestCSRFAndOriginEnforcement verifies mutation Origin checks, CSRF validation,
// and the dedicated CSRF bootstrap endpoint.
func TestCSRFAndOriginEnforcement(t *testing.T) {
	db, runtimePool, _ := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	ctx := context.Background()

	cfg := auth.Config{
		RuntimeMode:    auth.ModeHosted,
		AllowedOrigins: []string{"https://app.deadbolt.cloud", "http://localhost:3000"},
		CookieSecure:   true,
	}
	store := auth.NewSessionStore(runtimePool)
	bff := auth.NewBFFHandler(cfg, nil, store, runtimePool)

	// Create test user and active session
	user, err := store.GetOrCreateUserFromOIDC(ctx, &auth.Identity{
		Issuer:  "https://oidc.example.com",
		Subject: "csrf-test-user",
		Email:   "csrf@deadbolt.local",
	})
	if err != nil {
		t.Fatalf("failed to create test user: %v", err)
	}

	sess, rawToken, rawCSRF, err := store.CreateSession(ctx, user.ID, nil, "127.0.0.1", "test-agent", 12*time.Hour, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}
	_ = sess

	cookie := &http.Cookie{Name: auth.SessionCookieName, Value: rawToken}

	targetHandler := bff.RequireAuth(bff.RequireCSRFAndOrigin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"success"}`))
	})))

	// 1. Valid Origin + Valid CSRF Token -> 200 OK
	req1 := httptest.NewRequest(http.MethodPost, "/api/mutation", strings.NewReader(`{}`))
	req1.AddCookie(cookie)
	req1.Header.Set("Origin", "https://app.deadbolt.cloud")
	req1.Header.Set("X-CSRF-Token", rawCSRF)
	rec1 := httptest.NewRecorder()
	targetHandler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK with valid CSRF and Origin, got %d (body: %s)", rec1.Code, rec1.Body.String())
	}

	// 2. Disallowed Origin -> 403 Forbidden (ORIGIN_FORBIDDEN)
	req2 := httptest.NewRequest(http.MethodPost, "/api/mutation", strings.NewReader(`{}`))
	req2.AddCookie(cookie)
	req2.Header.Set("Origin", "https://evil-attacker.com")
	req2.Header.Set("X-CSRF-Token", rawCSRF)
	rec2 := httptest.NewRecorder()
	targetHandler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusForbidden || !strings.Contains(rec2.Body.String(), "ORIGIN_FORBIDDEN") {
		t.Fatalf("expected 403 ORIGIN_FORBIDDEN, got %d (body: %s)", rec2.Code, rec2.Body.String())
	}

	// 3. Missing Origin / Referer -> 403 Forbidden
	req3 := httptest.NewRequest(http.MethodPost, "/api/mutation", strings.NewReader(`{}`))
	req3.AddCookie(cookie)
	req3.Header.Set("X-CSRF-Token", rawCSRF)
	rec3 := httptest.NewRecorder()
	targetHandler.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusForbidden || !strings.Contains(rec3.Body.String(), "ORIGIN_FORBIDDEN") {
		t.Fatalf("expected 403 ORIGIN_FORBIDDEN on missing origin, got %d", rec3.Code)
	}

	// 4. Invalid CSRF Token -> 403 Forbidden (CSRF_VALIDATION_FAILED)
	req4 := httptest.NewRequest(http.MethodPost, "/api/mutation", strings.NewReader(`{}`))
	req4.AddCookie(cookie)
	req4.Header.Set("Origin", "https://app.deadbolt.cloud")
	req4.Header.Set("X-CSRF-Token", "forged-csrf-token-1234")
	rec4 := httptest.NewRecorder()
	targetHandler.ServeHTTP(rec4, req4)
	if rec4.Code != http.StatusForbidden || !strings.Contains(rec4.Body.String(), "CSRF_VALIDATION_FAILED") {
		t.Fatalf("expected 403 CSRF_VALIDATION_FAILED, got %d (body: %s)", rec4.Code, rec4.Body.String())
	}

	// 5. Dedicated CSRF Bootstrap Endpoint: GET /api/auth/csrf
	csrfReq := httptest.NewRequest(http.MethodGet, "/api/auth/csrf", nil)
	csrfReq.AddCookie(cookie)
	csrfRec := httptest.NewRecorder()
	bff.RequireAuth(http.HandlerFunc(bff.HandleGetCSRF)).ServeHTTP(csrfRec, csrfReq)

	if csrfRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from GET /api/auth/csrf, got %d (body: %s)", csrfRec.Code, csrfRec.Body.String())
	}
	var csrfResp map[string]string
	if err := json.NewDecoder(csrfRec.Body).Decode(&csrfResp); err != nil || csrfResp["csrf_token"] == "" {
		t.Fatalf("failed to decode valid csrf token from bootstrap endpoint: %+v", csrfResp)
	}
	bootstrappedCSRF := csrfResp["csrf_token"]

	// Verify the newly bootstrapped CSRF token successfully authorizes a subsequent mutation
	mutReq := httptest.NewRequest(http.MethodPost, "/api/mutation", strings.NewReader(`{}`))
	mutReq.AddCookie(cookie)
	mutReq.Header.Set("Origin", "https://app.deadbolt.cloud")
	mutReq.Header.Set("X-CSRF-Token", bootstrappedCSRF)
	mutRec := httptest.NewRecorder()
	targetHandler.ServeHTTP(mutRec, mutReq)
	if mutRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK with bootstrapped CSRF token, got %d", mutRec.Code)
	}
}

// TestCORSAllowlistAndPreflight verifies allowlisted CORS preflight and headers.
func TestCORSAllowlistAndPreflight(t *testing.T) {
	cfg := auth.Config{
		RuntimeMode:    auth.ModeHosted,
		AllowedOrigins: []string{"https://app.deadbolt.cloud", "http://localhost:3000"},
		CookieSecure:   true,
	}
	bff := auth.NewBFFHandler(cfg, nil, nil, nil)

	handler := bff.CORSMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))

	// 1. Allowed origin OPTIONS preflight
	preflightReq := httptest.NewRequest(http.MethodOptions, "/api/auth/session", nil)
	preflightReq.Header.Set("Origin", "https://app.deadbolt.cloud")
	preflightRec := httptest.NewRecorder()
	handler.ServeHTTP(preflightRec, preflightReq)

	if preflightRec.Code != http.StatusNoContent {
		t.Fatalf("expected 204 No Content on valid CORS preflight, got %d", preflightRec.Code)
	}
	if preflightRec.Header().Get("Access-Control-Allow-Origin") != "https://app.deadbolt.cloud" {
		t.Fatalf("expected Access-Control-Allow-Origin header matching origin, got %q", preflightRec.Header().Get("Access-Control-Allow-Origin"))
	}
	if preflightRec.Header().Get("Access-Control-Allow-Credentials") != "true" {
		t.Fatalf("expected Access-Control-Allow-Credentials=true")
	}

	// 2. Untrusted origin OPTIONS preflight -> 403 ORIGIN_FORBIDDEN
	badPreflightReq := httptest.NewRequest(http.MethodOptions, "/api/auth/session", nil)
	badPreflightReq.Header.Set("Origin", "https://malicious-site.com")
	badPreflightRec := httptest.NewRecorder()
	handler.ServeHTTP(badPreflightRec, badPreflightReq)

	if badPreflightRec.Code != http.StatusForbidden || !strings.Contains(badPreflightRec.Body.String(), "ORIGIN_FORBIDDEN") {
		t.Fatalf("expected 403 ORIGIN_FORBIDDEN on untrusted CORS preflight, got %d (%s)", badPreflightRec.Code, badPreflightRec.Body.String())
	}

	// 3. Normal request from allowed origin receives CORS response headers
	getReq := httptest.NewRequest(http.MethodGet, "/api/auth/session", nil)
	getReq.Header.Set("Origin", "http://localhost:3000")
	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", getRec.Code)
	}
	if getRec.Header().Get("Access-Control-Allow-Origin") != "http://localhost:3000" {
		t.Fatalf("expected CORS headers on allowed origin request, got %q", getRec.Header().Get("Access-Control-Allow-Origin"))
	}
}

// TestSessionIdleAndAbsoluteExpiry verifies 12h idle timeout and 7d absolute expiry.
func TestSessionIdleAndAbsoluteExpiry(t *testing.T) {
	db, runtimePool, _ := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	ctx := context.Background()
	store := auth.NewSessionStore(runtimePool)

	user, err := store.GetOrCreateUserFromOIDC(ctx, &auth.Identity{
		Issuer:  "https://oidc.example.com",
		Subject: "timeout-user",
		Email:   "timeout@deadbolt.local",
	})
	if err != nil {
		t.Fatalf("failed to create test user: %v", err)
	}

	// 1. Idle Expiry Test
	sessIdle, rawIdleToken, _, err := store.CreateSession(ctx, user.ID, nil, "127.0.0.1", "agent", 10*time.Millisecond, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("failed to create idle test session: %v", err)
	}
	_ = sessIdle

	// Immediate validation succeeds
	v1, err := store.ValidateSession(ctx, rawIdleToken, 10*time.Millisecond)
	if err != nil || v1 == nil {
		t.Fatalf("expected session to be valid immediately, got %v", err)
	}

	// Wait for idle window to lapse
	time.Sleep(25 * time.Millisecond)
	_, err = store.ValidateSession(ctx, rawIdleToken, 10*time.Millisecond)
	if !errors.Is(err, auth.ErrSessionIdleTimeout) {
		t.Fatalf("expected ErrSessionIdleTimeout after idle lapse, got: %v", err)
	}

	// 2. Absolute Expiry Test
	sessAbs, rawAbsToken, _, err := store.CreateSession(ctx, user.ID, nil, "127.0.0.1", "agent", 1*time.Hour, 15*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to create abs test session: %v", err)
	}
	_ = sessAbs

	time.Sleep(25 * time.Millisecond)
	_, err = store.ValidateSession(ctx, rawAbsToken, 1*time.Hour)
	if !errors.Is(err, auth.ErrSessionExpired) {
		t.Fatalf("expected ErrSessionExpired after absolute lifetime passed, got: %v", err)
	}
}

// TestHostedStartupRejectsDevAuthAndInsecureCookies verifies hosted mode safeguards and development key rejection.
func TestHostedStartupRejectsDevAuthAndInsecureCookies(t *testing.T) {
	// Base valid hosted config
	baseValidCfg := func() auth.Config {
		return auth.Config{
			RuntimeMode:            auth.ModeHosted,
			DevAuthEnabled:         false,
			CookieSecure:           true,
			AllowedOrigins:         []string{"https://app.deadbolt.cloud"},
			SessionIdleTimeout:     12 * time.Hour,
			SessionAbsoluteTimeout: 7 * 24 * time.Hour,
			OIDC: auth.OIDCConfig{
				Issuer:   "https://accounts.google.com",
				ClientID: "client-id-123",
			},
		}
	}

	// 1. Hosted mode with DevAuthEnabled=true must be rejected
	cfg1 := baseValidCfg()
	cfg1.DevAuthEnabled = true
	if err := cfg1.Validate("127.0.0.1"); err == nil || !strings.Contains(err.Error(), "rejects dev auth and development keys") {
		t.Fatalf("SECURITY VIOLATION: hosted mode accepted DevAuthEnabled=true, got err=%v", err)
	}

	// 2. Hosted mode with CookieSecure=false must be rejected to prevent __Host- bypass
	cfg2 := baseValidCfg()
	cfg2.CookieSecure = false
	if err := cfg2.Validate("127.0.0.1"); err == nil || !strings.Contains(err.Error(), "CookieSecure=true") {
		t.Fatalf("SECURITY VIOLATION: hosted mode accepted CookieSecure=false, got err=%v", err)
	}

	// 3. Hosted mode with DevKey configured must be rejected (Blueprint §24.4)
	cfg3 := baseValidCfg()
	cfg3.DevKey = "raw-development-key-secret-999"
	if err := cfg3.Validate("127.0.0.1"); err == nil || !strings.Contains(err.Error(), "rejects dev auth and development keys") {
		t.Fatalf("SECURITY VIOLATION: hosted mode accepted DevKey, got err=%v", err)
	}

	// 4. Hosted mode with DevKeyPath configured must be rejected (Blueprint §24.4)
	cfg4 := baseValidCfg()
	cfg4.DevKeyPath = "/etc/deadbolt/dev.key"
	if err := cfg4.Validate("127.0.0.1"); err == nil || !strings.Contains(err.Error(), "rejects dev auth and development keys") {
		t.Fatalf("SECURITY VIOLATION: hosted mode accepted DevKeyPath, got err=%v", err)
	}

	// 5. Hosted mode with DEADBOLT_DEV_KEY environment variable set must be rejected
	t.Setenv("DEADBOLT_DEV_KEY", "env-injected-dev-secret")
	cfg5 := baseValidCfg()
	if err := cfg5.Validate("127.0.0.1"); err == nil || !strings.Contains(err.Error(), "rejects dev auth and development keys") {
		t.Fatalf("SECURITY VIOLATION: hosted mode accepted DEADBOLT_DEV_KEY env var, got err=%v", err)
	}

	// 6. Hosted mode with DEADBOLT_DEV_KEY_PATH environment variable set must be rejected
	t.Setenv("DEADBOLT_DEV_KEY", "")
	t.Setenv("DEADBOLT_DEV_KEY_PATH", "/tmp/dev.key")
	cfg6 := baseValidCfg()
	if err := cfg6.Validate("127.0.0.1"); err == nil || !strings.Contains(err.Error(), "rejects dev auth and development keys") {
		t.Fatalf("SECURITY VIOLATION: hosted mode accepted DEADBOLT_DEV_KEY_PATH env var, got err=%v", err)
	}
	t.Setenv("DEADBOLT_DEV_KEY_PATH", "")

	// 7. Hosted mode rejects ReadDevKey()
	hostedCfg := baseValidCfg()
	if _, err := hostedCfg.ReadDevKey(); err == nil {
		t.Fatalf("SECURITY VIOLATION: ReadDevKey succeeded in hosted mode")
	}

	// 8. Local mode permits ReadDevKey() from direct field, file, and environment
	localCfg := auth.Config{
		RuntimeMode: auth.ModeLocal,
		DevKey:      "local-direct-key-value",
	}
	keyVal, err := localCfg.ReadDevKey()
	if err != nil || keyVal != "local-direct-key-value" {
		t.Fatalf("expected ReadDevKey from field to return key, got: %q, err=%v", keyVal, err)
	}

	// Test reading from file in local mode
	tmpDir := t.TempDir()
	keyFile := filepath.Join(tmpDir, ".deadbolt-dev-key")
	if err := os.WriteFile(keyFile, []byte("  file-based-dev-key-12345 \n"), 0600); err != nil {
		t.Fatalf("failed to write dev key file: %v", err)
	}

	localFileCfg := auth.Config{
		RuntimeMode: auth.ModeLocal,
		DevKeyPath:  keyFile,
	}
	if err := localFileCfg.Validate("127.0.0.1"); err != nil {
		t.Fatalf("local file config validation failed: %v", err)
	}
	keyValFile, err := localFileCfg.ReadDevKey()
	if err != nil || keyValFile != "file-based-dev-key-12345" {
		t.Fatalf("expected trimmed dev key from file, got: %q, err=%v", keyValFile, err)
	}
}

// TestLocalDevAuthLoopbackAndCookieNaming verifies loopback restrictions and browser-conforming local cookie names.
func TestLocalDevAuthLoopbackAndCookieNaming(t *testing.T) {
	db, runtimePool, _ := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	store := auth.NewSessionStore(runtimePool)

	// 1. Insecure local mode uses non-__Host- cookie names so conforming browsers accept them over plain HTTP
	localInsecureCfg := auth.Config{
		RuntimeMode:    auth.ModeLocal,
		DevAuthEnabled: true,
		CookieSecure:   false,
	}

	// 1a. Listen host validation: empty string or wildcard 0.0.0.0 must be rejected
	if err := localInsecureCfg.Validate(""); err == nil {
		t.Fatalf("expected empty listen host to be rejected when dev auth is enabled")
	}
	if err := localInsecureCfg.Validate("0.0.0.0"); err == nil {
		t.Fatalf("expected wildcard listen host 0.0.0.0 to be rejected when dev auth is enabled")
	}
	if err := localInsecureCfg.Validate("192.168.1.50"); err == nil {
		t.Fatalf("expected remote IP listen host to be rejected when dev auth is enabled")
	}

	// Valid loopback listen host passes
	if err := localInsecureCfg.Validate("127.0.0.1:8080"); err != nil {
		t.Fatalf("expected loopback listen host 127.0.0.1:8080 to pass, got: %v", err)
	}

	// 1b. Verify cookie names for insecure local mode do NOT carry __Host- prefix
	if strings.HasPrefix(localInsecureCfg.SessionCookieName(), "__Host-") {
		t.Fatalf("expected non-__Host- cookie name for insecure local mode, got %q", localInsecureCfg.SessionCookieName())
	}
	if strings.HasPrefix(localInsecureCfg.CSRFCookieName(), "__Host-") {
		t.Fatalf("expected non-__Host- cookie name for insecure local mode, got %q", localInsecureCfg.CSRFCookieName())
	}

	devHandler := auth.NewDevAuthHandler(localInsecureCfg, store, runtimePool)

	// 2. Caller from external IP (e.g. 192.168.1.10) must be rejected with 403 LOOPBACK_REQUIRED
	remoteReq := httptest.NewRequest(http.MethodPost, "/api/auth/dev-login", strings.NewReader(`{}`))
	remoteReq.RemoteAddr = "192.168.1.10:52412"
	remoteRec := httptest.NewRecorder()
	devHandler.HandleDevLogin(remoteRec, remoteReq)

	if remoteRec.Code != http.StatusForbidden || !strings.Contains(remoteRec.Body.String(), "LOOPBACK_REQUIRED") {
		t.Fatalf("expected 403 LOOPBACK_REQUIRED for remote caller, got %d (%s)", remoteRec.Code, remoteRec.Body.String())
	}

	// 3. Caller from loopback 127.0.0.1 succeeds
	loopbackReq := httptest.NewRequest(http.MethodPost, "/api/auth/dev-login", strings.NewReader(`{"email":"local-tester@deadbolt.local"}`))
	loopbackReq.RemoteAddr = "127.0.0.1:49152"
	loopbackRec := httptest.NewRecorder()
	devHandler.HandleDevLogin(loopbackRec, loopbackReq)

	if loopbackRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for loopback caller, got %d (%s)", loopbackRec.Code, loopbackRec.Body.String())
	}

	// Verify issued cookies match local naming
	var issuedLocalSession, issuedLocalCSRF bool
	for _, c := range loopbackRec.Result().Cookies() {
		if c.Name == auth.LocalSessionCookieName {
			issuedLocalSession = true
		}
		if c.Name == auth.LocalCSRFCookieName {
			issuedLocalCSRF = true
		}
	}
	if !issuedLocalSession || !issuedLocalCSRF {
		t.Fatalf("expected local session and CSRF cookies to be issued")
	}
}

// TestNegativeAuthResponsesAndLogRedaction proves negative responses and logs contain
// no tokens, cookies, or secrets.
func TestNegativeAuthResponsesAndLogRedaction(t *testing.T) {
	db, runtimePool, _ := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	logBuf := &bytes.Buffer{}
	logger := log.New(logBuf, "", 0)

	cfg := auth.Config{
		RuntimeMode:    auth.ModeHosted,
		AllowedOrigins: []string{"https://app.deadbolt.cloud"},
		CookieSecure:   true,
	}
	store := auth.NewSessionStore(runtimePool)
	bff := auth.NewBFFHandler(cfg, nil, store, runtimePool)
	bff.SetLogger(logger)

	fakeToken := "sensitive-token-alpha-9988776655"
	fakeCSRF := "sensitive-csrf-secret-11223344"

	// 1. Request with invalid token
	req := httptest.NewRequest(http.MethodGet, "/api/auth/session", nil)
	req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: fakeToken})
	rec := httptest.NewRecorder()
	bff.RequireAuth(http.HandlerFunc(bff.HandleGetSession)).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", rec.Code)
	}

	// 2. Mutating request with forbidden origin and fake CSRF
	mutReq := httptest.NewRequest(http.MethodPost, "/api/auth/logout", nil)
	mutReq.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: fakeToken})
	mutReq.Header.Set("Origin", "https://untrusted-attacker.com")
	mutReq.Header.Set("X-CSRF-Token", fakeCSRF)
	mutReq.Header.Set("Authorization", "Bearer sensitive-access-token-999")
	mutRec := httptest.NewRecorder()
	bff.RequireAuth(bff.RequireCSRFAndOrigin(http.HandlerFunc(bff.HandleLogout))).ServeHTTP(mutRec, mutReq)

	// Assert response bodies contain no tokens or sensitive terms
	for _, body := range []string{rec.Body.String(), mutRec.Body.String()} {
		if strings.Contains(body, fakeToken) {
			t.Fatalf("SECURITY VIOLATION: response leaked session token: %s", body)
		}
		if strings.Contains(body, fakeCSRF) {
			t.Fatalf("SECURITY VIOLATION: response leaked csrf token: %s", body)
		}
		if strings.Contains(body, "password") || strings.Contains(body, "secret") {
			t.Fatalf("SECURITY VIOLATION: response leaked sensitive terms: %s", body)
		}
	}

	// Inspect audit logs
	logs := logBuf.String()
	if !strings.Contains(logs, "[AUTH_SECURITY]") {
		t.Fatalf("expected security audit log events to be recorded")
	}
	if strings.Contains(logs, fakeToken) {
		t.Fatalf("SECURITY VIOLATION: server logs leaked raw session token: %s", logs)
	}
	if strings.Contains(logs, fakeCSRF) {
		t.Fatalf("SECURITY VIOLATION: server logs leaked raw csrf token: %s", logs)
	}
	if strings.Contains(logs, "sensitive-access-token-999") {
		t.Fatalf("SECURITY VIOLATION: server logs leaked authorization header: %s", logs)
	}

	// 3. Upstream OIDC error containing fake tokens/secrets/codes in raw error response
	sentinelOIDCSecret := "SENTINEL_OIDC_SECRET_TOKEN_99999"
	sentinelOIDCCode := "SENTINEL_AUTH_CODE_LEAK_88888"

	// Start an OIDC server whose /token endpoint returns an error echoing the secret/code
	leakyFixtureMux := http.NewServeMux()
	leakyFixtureMux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"issuer":                 "http://" + r.Host,
			"authorization_endpoint": "http://" + r.Host + "/authorize",
			"token_endpoint":         "http://" + r.Host + "/token",
			"jwks_uri":               "http://" + r.Host + "/jwks.json",
		})
	})
	leakyFixtureMux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		// Leaky upstream error payload containing sensitive sentinel secrets
		_, _ = w.Write([]byte(fmt.Sprintf(`{"error":"invalid_grant","error_description":"failed to exchange code %s with secret %s"}`, sentinelOIDCCode, sentinelOIDCSecret)))
	})
	leakyServer := httptest.NewServer(leakyFixtureMux)
	defer leakyServer.Close()

	leakyCfg := auth.Config{
		RuntimeMode: auth.ModeHosted,
		OIDC: auth.OIDCConfig{
			Issuer:       leakyServer.URL,
			ClientID:     "test-client",
			ClientSecret: "test-secret",
			RedirectURL:  "https://app.deadbolt.cloud/api/auth/callback",
		},
		AllowedOrigins: []string{"https://app.deadbolt.cloud"},
		CookieSecure:   true,
	}
	leakyOIDC := auth.NewOIDCClient(leakyCfg.OIDC, leakyServer.Client())
	leakyBFF := auth.NewBFFHandler(leakyCfg, leakyOIDC, store, runtimePool)
	leakyBFF.SetLogger(logger)

	// Craft callback request with PKCE state
	pkce := auth.PKCEParams{
		CodeVerifier: "test-verifier-12345678901234567890123456789012",
		State:        "test-state-123",
		Nonce:        "test-nonce-123",
	}
	pkcePayload, _ := json.Marshal(pkce)
	encodedPKCE := base64.RawURLEncoding.EncodeToString(pkcePayload)

	cbReq := httptest.NewRequest(http.MethodGet, "/api/auth/callback?code="+sentinelOIDCCode+"&state=test-state-123", nil)
	cbReq.AddCookie(&http.Cookie{Name: auth.PKCECookieName, Value: encodedPKCE})
	cbRec := httptest.NewRecorder()
	leakyBFF.HandleCallback(cbRec, cbReq)

	if cbRec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized on OIDC exchange failure, got %d", cbRec.Code)
	}

	// 4. Denied cross-tenant organization switch attempt using foreign org ID sentinel
	foreignOrgSentinel := "foreign-tenant-sentinel-uuid-77777777-8888"

	// Create real user and active session
	user, err := store.GetOrCreateUserFromOIDC(context.Background(), &auth.Identity{
		Issuer:  "https://accounts.google.com",
		Subject: "test-user-redaction-sub",
		Email:   "redaction-test@deadbolt.cloud",
	})
	if err != nil {
		t.Fatalf("failed to create test user: %v", err)
	}
	_, sessionToken, realCSRF, err := store.CreateSession(
		context.Background(),
		user.ID,
		nil,
		"127.0.0.1",
		"test-agent",
		12*time.Hour,
		7*24*time.Hour,
	)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	switchReqBody := fmt.Sprintf(`{"organization_id":%q}`, foreignOrgSentinel)
	switchReq := httptest.NewRequest(http.MethodPost, "/api/auth/switch-org", strings.NewReader(switchReqBody))
	switchReq.Header.Set("Origin", "https://app.deadbolt.cloud")
	switchReq.Header.Set("X-CSRF-Token", realCSRF)
	switchReq.Header.Set("Content-Type", "application/json")
	switchReq.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: sessionToken})
	switchRec := httptest.NewRecorder()

	bff.RequireAuth(bff.RequireCSRFAndOrigin(http.HandlerFunc(bff.HandleSwitchOrg))).ServeHTTP(switchRec, switchReq)

	if switchRec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for unauthorized org switch, got %d (%s)", switchRec.Code, switchRec.Body.String())
	}

	// Assert that neither the response bodies NOR the audit logs contain the sentinels
	allLogs := logBuf.String()
	if strings.Contains(allLogs, sentinelOIDCSecret) {
		t.Fatalf("SECURITY VIOLATION: server logs leaked upstream OIDC secret sentinel: %s", allLogs)
	}
	if strings.Contains(allLogs, sentinelOIDCCode) {
		t.Fatalf("SECURITY VIOLATION: server logs leaked upstream OIDC auth code sentinel: %s", allLogs)
	}
	if strings.Contains(allLogs, foreignOrgSentinel) {
		t.Fatalf("SECURITY VIOLATION: server logs leaked foreign tenant ID sentinel: %s", allLogs)
	}

	// Verify structured reasons are recorded
	if !strings.Contains(allLogs, "reason=token_verification_failed") {
		t.Fatalf("expected structured reason=token_verification_failed in logs, got:\n%s", allLogs)
	}
	if !strings.Contains(allLogs, "reason=unauthorized_org_membership") {
		t.Fatalf("expected structured reason=unauthorized_org_membership in logs, got:\n%s", allLogs)
	}
}

// TestOIDCTokenVerificationNegativeCases tests rejection of invalid signatures,
// corrupted signatures, missing sub, missing exp, and claim mismatches.
func TestOIDCTokenVerificationNegativeCases(t *testing.T) {
	fixture, err := oidcfixture.NewFixtureServer("test-client-id")
	if err != nil {
		t.Fatalf("failed to start fixture: %v", err)
	}
	defer fixture.Close()

	ctx := context.Background()
	oidcClient := auth.NewOIDCClient(auth.OIDCConfig{
		Issuer:   fixture.URL(),
		ClientID: fixture.ClientID(),
	}, nil)

	// 1. Untrusted RSA key signature (signed by completely independent private key not in JWKS)
	rogueKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate rogue key: %v", err)
	}
	rogueToken := signCustomToken(t, rogueKey, "fixture-key-1", fixture.URL(), fixture.ClientID(), "user1", "user1@deadbolt.local", "nonce1", 1*time.Hour)
	_, err = oidcClient.VerifyIDToken(ctx, rogueToken, "nonce1")
	if err == nil || !strings.Contains(err.Error(), "signature verification failed") {
		t.Fatalf("expected signature verification failure for untrusted key, got: %v", err)
	}

	// 2. Corrupted token signature bytes
	tokenParts := strings.Split(rogueToken, ".")
	corruptedToken := tokenParts[0] + "." + tokenParts[1] + ".AAAA" + tokenParts[2][4:]
	_, err = oidcClient.VerifyIDToken(ctx, corruptedToken, "nonce1")
	if err == nil {
		t.Fatalf("expected rejection for corrupted signature")
	}

	// 3. Missing / empty subject claim
	emptySubToken, _ := fixture.SignIDToken(fixture.URL(), fixture.ClientID(), "", "user1@deadbolt.local", "User 1", "nonce1", 1*time.Hour)
	_, err = oidcClient.VerifyIDToken(ctx, emptySubToken, "nonce1")
	if err == nil || !strings.Contains(err.Error(), "missing required subject") {
		t.Fatalf("expected error for empty subject, got: %v", err)
	}

	// 4. Missing / expired token
	expiredToken, _ := fixture.SignIDToken(fixture.URL(), fixture.ClientID(), "user1", "user1@deadbolt.local", "User 1", "nonce1", -10*time.Minute)
	_, err = oidcClient.VerifyIDToken(ctx, expiredToken, "nonce1")
	if err == nil || !strings.Contains(err.Error(), "token expired") {
		t.Fatalf("expected token expired error, got: %v", err)
	}

	// 5. Mismatched issuer
	badIssuerToken, _ := fixture.SignIDToken("https://rogue-issuer.com", fixture.ClientID(), "user1", "user1@deadbolt.local", "User 1", "nonce1", 1*time.Hour)
	_, err = oidcClient.VerifyIDToken(ctx, badIssuerToken, "nonce1")
	if err == nil || !strings.Contains(err.Error(), "issuer mismatch") {
		t.Fatalf("expected issuer mismatch error, got: %v", err)
	}

	// 6. Mismatched audience
	badAudToken, _ := fixture.SignIDToken(fixture.URL(), "wrong-client-id", "user1", "user1@deadbolt.local", "User 1", "nonce1", 1*time.Hour)
	_, err = oidcClient.VerifyIDToken(ctx, badAudToken, "nonce1")
	if err == nil || !strings.Contains(err.Error(), "audience mismatch") {
		t.Fatalf("expected audience mismatch error, got: %v", err)
	}

	// 7. Mismatched nonce
	badNonceToken, _ := fixture.SignIDToken(fixture.URL(), fixture.ClientID(), "user1", "user1@deadbolt.local", "User 1", "wrong-nonce", 1*time.Hour)
	_, err = oidcClient.VerifyIDToken(ctx, badNonceToken, "expected-nonce")
	if err == nil || !strings.Contains(err.Error(), "nonce mismatch") {
		t.Fatalf("expected nonce mismatch error, got: %v", err)
	}
}

// TestPKCEProviderNegativeCases proves provider-side PKCE verification, code replay rejection,
// and unknown code rejection.
func TestPKCEProviderNegativeCases(t *testing.T) {
	fixture, err := oidcfixture.NewFixtureServer("pkce-test-client")
	if err != nil {
		t.Fatalf("failed to start fixture: %v", err)
	}
	defer fixture.Close()

	pkce, err := auth.GeneratePKCE()
	if err != nil {
		t.Fatalf("failed to generate PKCE: %v", err)
	}

	authCodeWrong := "pkce-test-auth-code-wrong"
	fixture.RegisterAuthCodeWithPKCE(authCodeWrong, oidcfixture.TokenClaimOverrides{
		Subject: "user-pkce-wrong",
		Nonce:   pkce.Nonce,
		Expiry:  1 * time.Hour,
	}, pkce.CodeChallenge, "S256")

	authCodeValid := "pkce-test-auth-code-valid"
	fixture.RegisterAuthCodeWithPKCE(authCodeValid, oidcfixture.TokenClaimOverrides{
		Subject: "user-pkce-valid",
		Nonce:   pkce.Nonce,
		Expiry:  1 * time.Hour,
	}, pkce.CodeChallenge, "S256")

	tokenURL := fixture.URL() + "/token"

	// 1. Wrong code_verifier -> Rejected by fixture
	formWrong := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {authCodeWrong},
		"client_id":     {"pkce-test-client"},
		"code_verifier": {"wrong-code-verifier-value"},
	}
	resp1, err := http.PostForm(tokenURL, formWrong)
	if err != nil {
		t.Fatalf("failed to post token exchange: %v", err)
	}
	defer resp1.Body.Close()
	if resp1.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for wrong code_verifier, got %d", resp1.StatusCode)
	}

	// 2. Valid code_verifier -> Succeeds and marks code as used
	formCorrect := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {authCodeValid},
		"client_id":     {"pkce-test-client"},
		"code_verifier": {pkce.CodeVerifier},
	}
	resp2, err := http.PostForm(tokenURL, formCorrect)
	if err != nil {
		t.Fatalf("failed to post token exchange: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for valid code_verifier, got %d", resp2.StatusCode)
	}

	// 3. Replay attack: reusing the same authorization code -> Rejected with 400
	resp3, err := http.PostForm(tokenURL, formCorrect)
	if err != nil {
		t.Fatalf("failed to post token replay: %v", err)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request on code replay, got %d", resp3.StatusCode)
	}
}

// TestBrowserSessionSmokeAndRedirectFlow executes an end-to-end browser session journey:
// browser visits /api/auth/login -> follows redirects -> receives cookies in jar ->
// reads CSRF token from cookie -> performs authenticated mutation -> performs logout.
func TestBrowserSessionSmokeAndRedirectFlow(t *testing.T) {
	db, runtimePool, _ := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	// 1. Setup OIDC Provider Fixture
	fixture, err := oidcfixture.NewFixtureServer("browser-smoke-client")
	if err != nil {
		t.Fatalf("failed to start fixture: %v", err)
	}
	defer fixture.Close()

	// 2. Setup dynamic handler so bff configuration can use actual server URL
	var currentBFF *auth.BFFHandler

	mux := http.NewServeMux()
	mux.Handle("/api/auth/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		currentBFF.Routes().ServeHTTP(w, r)
	}))
	mux.Handle("/api/mutation", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		currentBFF.RequireAuth(currentBFF.RequireCSRFAndOrigin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"result": "mutation-successful"})
		}))).ServeHTTP(w, r)
	}))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<html><body>Deadbolt SPA Root</body></html>"))
	})

	bffServer := httptest.NewTLSServer(mux)
	defer bffServer.Close()

	// Configure BFF with actual TLS server URL
	cfg := auth.Config{
		RuntimeMode: auth.ModeHosted,
		OIDC: auth.OIDCConfig{
			Issuer:       fixture.URL(),
			ClientID:     fixture.ClientID(),
			ClientSecret: "browser-client-secret",
			RedirectURL:  bffServer.URL + "/api/auth/callback",
		},
		AllowedOrigins:         []string{bffServer.URL},
		CookieSecure:           true,
		SessionIdleTimeout:     12 * time.Hour,
		SessionAbsoluteTimeout: 7 * 24 * time.Hour,
	}

	store := auth.NewSessionStore(runtimePool)
	oidcClient := auth.NewOIDCClient(cfg.OIDC, fixture.Client())
	currentBFF = auth.NewBFFHandler(cfg, oidcClient, store, runtimePool)

	// 3. Browser Client with Cookie Jar using bffServer.Client() which trusts the TLS cert
	browser := bffServer.Client()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("failed to create cookie jar: %v", err)
	}
	browser.Jar = jar

	// Browser step 1: Browse to /api/auth/login
	loginResp, err := browser.Get(bffServer.URL + "/api/auth/login")
	if err != nil {
		t.Fatalf("failed browser login request: %v", err)
	}
	defer loginResp.Body.Close()

	// Following the 302 chain (login -> fixture authorize -> bff callback -> app root /)
	if loginResp.StatusCode != http.StatusOK {
		t.Fatalf("expected browser to reach app root with 200 OK, got %d", loginResp.StatusCode)
	}

	// Browser step 2: Inspect cookies in the browser's cookie jar
	parsedServerURL, _ := url.Parse(bffServer.URL)
	jarCookies := jar.Cookies(parsedServerURL)

	var sessionCookieVal string
	var csrfCookieVal string
	for _, c := range jarCookies {
		if c.Name == auth.SessionCookieName {
			sessionCookieVal = c.Value
		}
		if c.Name == auth.CSRFCookieName {
			csrfCookieVal = c.Value
		}
	}

	if sessionCookieVal == "" {
		t.Fatalf("expected session cookie %s in browser cookie jar", auth.SessionCookieName)
	}
	if csrfCookieVal == "" {
		t.Fatalf("expected readable CSRF cookie %s in browser cookie jar", auth.CSRFCookieName)
	}

	// Browser step 3: Read CSRF token from document.cookie / jar, and perform state mutation
	mutReq, err := http.NewRequest(http.MethodPost, bffServer.URL+"/api/mutation", strings.NewReader(`{"action":"run"}`))
	if err != nil {
		t.Fatalf("failed to build mutation request: %v", err)
	}
	mutReq.Header.Set("Origin", bffServer.URL)
	mutReq.Header.Set("X-CSRF-Token", csrfCookieVal) // read directly from browser cookie
	mutResp, err := browser.Do(mutReq)
	if err != nil {
		t.Fatalf("failed to execute authenticated mutation: %v", err)
	}
	defer mutResp.Body.Close()

	if mutResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from authenticated mutation, got %d", mutResp.StatusCode)
	}

	// Browser step 4: Perform authenticated logout
	logoutReq, err := http.NewRequest(http.MethodPost, bffServer.URL+"/api/auth/logout", nil)
	if err != nil {
		t.Fatalf("failed to build logout request: %v", err)
	}
	logoutReq.Header.Set("Origin", bffServer.URL)
	logoutReq.Header.Set("X-CSRF-Token", csrfCookieVal)
	logoutResp, err := browser.Do(logoutReq)
	if err != nil {
		t.Fatalf("failed to execute logout: %v", err)
	}
	defer logoutResp.Body.Close()

	if logoutResp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204 No Content from logout, got %d", logoutResp.StatusCode)
	}

	// Browser step 5: Verify subsequent request to /api/auth/session is rejected with 401
	sessCheckResp, err := browser.Get(bffServer.URL + "/api/auth/session")
	if err != nil {
		t.Fatalf("failed to check session after logout: %v", err)
	}
	defer sessCheckResp.Body.Close()

	if sessCheckResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized after logout, got %d", sessCheckResp.StatusCode)
	}
}

// signCustomToken signs a custom JWT with an arbitrary RSA private key for negative signature tests
func signCustomToken(t *testing.T, key *rsa.PrivateKey, kid, issuer, audience, subject, email, nonce string, expiry time.Duration) string {
	t.Helper()
	now := time.Now()
	headerJSON, _ := json.Marshal(map[string]interface{}{
		"alg": "RS256",
		"typ": "JWT",
		"kid": kid,
	})
	payloadJSON, _ := json.Marshal(map[string]interface{}{
		"iss":   issuer,
		"sub":   subject,
		"aud":   audience,
		"exp":   now.Add(expiry).Unix(),
		"iat":   now.Unix(),
		"email": email,
		"nonce": nonce,
	})
	content := base64.RawURLEncoding.EncodeToString(headerJSON) + "." + base64.RawURLEncoding.EncodeToString(payloadJSON)
	h := sha256.Sum256([]byte(content))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, h[:])
	if err != nil {
		t.Fatalf("failed to sign custom token: %v", err)
	}
	return content + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// TestRealChromeBrowserSmoke drives a real headless Google Chrome / Chromium browser
// via Chrome DevTools Protocol over WebSocket to verify the complete browser authentication
// lifecycle: 302 redirect chain, HttpOnly session cookie hiding in DOM, JS-readable CSRF bootstrap cookie,
// authenticated mutation fetch, authenticated logout, and session revocation.
func TestRealChromeBrowserSmoke(t *testing.T) {
	db, runtimePool, _ := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	// Find node executable
	nodePath, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node executable not found; skipping browser smoke test")
	}

	// Start OIDC fixture server
	fixture, err := oidcfixture.NewFixtureServer("chrome-smoke-client")
	if err != nil {
		t.Fatalf("failed to start fixture server: %v", err)
	}
	defer fixture.Close()

	var currentBFF *auth.BFFHandler
	mux := http.NewServeMux()
	mux.Handle("/api/auth/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		currentBFF.Routes().ServeHTTP(w, r)
	}))
	mux.Handle("/api/mutation", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		currentBFF.RequireAuth(currentBFF.RequireCSRFAndOrigin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok", "action": "browser-smoke-mutation"})
		}))).ServeHTTP(w, r)
	}))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<!DOCTYPE html><html><body><h1>Deadbolt Dashboard</h1></body></html>"))
	})

	bffServer := httptest.NewTLSServer(mux)
	defer bffServer.Close()

	cfg := auth.Config{
		RuntimeMode: auth.ModeHosted,
		OIDC: auth.OIDCConfig{
			Issuer:       fixture.URL(),
			ClientID:     fixture.ClientID(),
			ClientSecret: "chrome-client-secret",
			RedirectURL:  bffServer.URL + "/api/auth/callback",
		},
		AllowedOrigins:         []string{bffServer.URL},
		CookieSecure:           true,
		SessionIdleTimeout:     12 * time.Hour,
		SessionAbsoluteTimeout: 7 * 24 * time.Hour,
	}

	store := auth.NewSessionStore(runtimePool)
	oidcClient := auth.NewOIDCClient(cfg.OIDC, fixture.Client())
	currentBFF = auth.NewBFFHandler(cfg, oidcClient, store, runtimePool)

	// Execute browser-smoke.mjs script pointing to bffServer.URL
	scriptPath, err := filepath.Abs("../../scripts/browser-smoke.mjs")
	if err != nil {
		t.Fatalf("failed to resolve browser-smoke.mjs path: %v", err)
	}
	if _, err := os.Stat(scriptPath); err != nil {
		t.Fatalf("browser-smoke.mjs not found at %s: %v", scriptPath, err)
	}

	cmd := exec.Command(nodePath, scriptPath, bffServer.URL)
	out, err := cmd.CombinedOutput()
	outputStr := string(out)
	t.Logf("browser-smoke output:\n%s", outputStr)

	if err != nil {
		t.Fatalf("browser smoke test execution failed: %v\nOutput: %s", err, outputStr)
	}

	if strings.Contains(outputStr, "Chrome/Chromium executable not found") {
		t.Log("Chrome/Chromium executable not found in this environment; smoke skipped safely.")
		return
	}

	if !strings.Contains(outputStr, "SUCCESS: Real Chrome browser smoke completed successfully!") {
		t.Fatalf("expected browser smoke test to succeed, but success marker was not found in output:\n%s", outputStr)
	}
}
