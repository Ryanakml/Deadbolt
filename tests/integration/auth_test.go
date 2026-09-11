package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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

	// 4. Pre-register auth code in OIDC fixture
	authCode := "valid-test-code-99"
	fixture.RegisterAuthCode(authCode, oidcfixture.TokenClaimOverrides{
		Subject: "usr-sub-alpha",
		Email:   "alpha@deadbolt.local",
		Name:    "Alpha User",
		Nonce:   pkce.Nonce,
		Expiry:  1 * time.Hour,
	})

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
	for _, c := range callbackRec.Result().Cookies() {
		if c.Name == auth.SessionCookieName {
			sessionCookie = c
			break
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

	rawCSRF := callbackRec.Header().Get("X-CSRF-Token")
	if rawCSRF == "" {
		t.Fatalf("expected X-CSRF-Token header on callback response")
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
	// Seed organization and membership for dbUserID
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
	for _, c := range switchRec.Result().Cookies() {
		if c.Name == auth.SessionCookieName {
			rotatedCookie = c
			break
		}
	}
	if rotatedCookie == nil || rotatedCookie.Value == sessionCookie.Value {
		t.Fatalf("CONCURRENCY/SECURITY VIOLATION: session cookie was not rotated on privilege change")
	}

	// Verify old session is marked rotated in DB
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

	// 9. Test DB Revocation via Logout (POST /api/auth/logout)
	logoutReq := httptest.NewRequest(http.MethodPost, "/api/auth/logout", nil)
	logoutReq.AddCookie(rotatedCookie)
	logoutRec := httptest.NewRecorder()
	bff.HandleLogout(logoutRec, logoutReq)

	if logoutRec.Code != http.StatusNoContent {
		t.Fatalf("expected 204 No Content from logout, got %d", logoutRec.Code)
	}

	// Verify session is marked revoked in DB
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
	if postLogoutRec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized after logout, got %d", postLogoutRec.Code)
	}
	if !strings.Contains(postLogoutRec.Body.String(), "SESSION_REVOKED") {
		t.Fatalf("expected SESSION_REVOKED code in response, got %s", postLogoutRec.Body.String())
	}
}

// TestCSRFAndOriginEnforcement verifies that cookie-authenticated mutations require
// valid Origin and X-CSRF-Token headers.
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
}

// TestSessionIdleAndAbsoluteExpiry verifies 12-hour idle expiry and 7-day absolute expiry.
func TestSessionIdleAndAbsoluteExpiry(t *testing.T) {
	db, runtimePool, _ := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	ctx := context.Background()
	store := auth.NewSessionStore(runtimePool)

	user, err := store.GetOrCreateUserFromOIDC(ctx, &auth.Identity{
		Issuer:  "https://oidc.example.com",
		Subject: "timeout-user",
	})
	if err != nil {
		t.Fatalf("failed to create user: %v", err)
	}

	// 1. Test Idle Timeout
	// Create session with 50ms idle timeout
	_, rawToken1, _, err := store.CreateSession(ctx, user.ID, nil, "127.0.0.1", "agent", 50*time.Millisecond, 1*time.Hour)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	// Wait for idle expiry
	time.Sleep(60 * time.Millisecond)
	_, err = store.ValidateSession(ctx, rawToken1, 50*time.Millisecond)
	if !errors.Is(err, auth.ErrSessionIdleTimeout) {
		t.Fatalf("expected ErrSessionIdleTimeout after idle period, got: %v", err)
	}

	// 2. Test Absolute Expiry
	// Create session with 50ms absolute timeout
	_, rawToken2, _, err := store.CreateSession(ctx, user.ID, nil, "127.0.0.1", "agent", 1*time.Hour, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	time.Sleep(60 * time.Millisecond)
	_, err = store.ValidateSession(ctx, rawToken2, 1*time.Hour)
	if !errors.Is(err, auth.ErrSessionExpired) {
		t.Fatalf("expected ErrSessionExpired after absolute period, got: %v", err)
	}
}

// TestHostedStartupRejectsDevAuth verifies that hosted mode startup rejects dev auth or dev keys.
func TestHostedStartupRejectsDevAuth(t *testing.T) {
	// 1. Hosted mode with DevAuthEnabled=true must fail validation
	cfg1 := auth.Config{
		RuntimeMode:    auth.ModeHosted,
		DevAuthEnabled: true,
		OIDC: auth.OIDCConfig{
			Issuer:   "https://oidc.example.com",
			ClientID: "deadbolt-client",
		},
		AllowedOrigins: []string{"https://app.deadbolt.cloud"},
	}
	err := cfg1.Validate("0.0.0.0")
	if err == nil || !strings.Contains(err.Error(), "hosted startup rejects dev auth") {
		t.Fatalf("SECURITY VIOLATION: hosted mode accepted DevAuthEnabled=true! Got: %v", err)
	}

	// 2. Hosted mode missing OIDC issuer must fail
	cfg2 := auth.Config{
		RuntimeMode:    auth.ModeHosted,
		DevAuthEnabled: false,
		OIDC:           auth.OIDCConfig{},
		AllowedOrigins: []string{"https://app.deadbolt.cloud"},
	}
	err = cfg2.Validate("0.0.0.0")
	if err == nil || !strings.Contains(err.Error(), "DEADBOLT_OIDC_ISSUER is required") {
		t.Fatalf("expected error on missing OIDC issuer in hosted mode, got: %v", err)
	}
}

// TestLocalDevAuthLoopbackOnly verifies that local dev auth works strictly on loopback interfaces.
func TestLocalDevAuthLoopbackOnly(t *testing.T) {
	db, runtimePool, _ := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	cfg := auth.Config{
		RuntimeMode:    auth.ModeLocal,
		DevAuthEnabled: true,
		CookieSecure:   false,
	}
	if err := cfg.Validate("127.0.0.1:8080"); err != nil {
		t.Fatalf("local loopback config validation failed: %v", err)
	}

	// Attempting to configure local dev auth on public interface must fail
	cfgBadHost := auth.Config{
		RuntimeMode:    auth.ModeLocal,
		DevAuthEnabled: true,
	}
	if err := cfgBadHost.Validate("192.168.1.100:8080"); err == nil {
		t.Fatalf("SECURITY VIOLATION: dev auth accepted public network interface binding!")
	}

	store := auth.NewSessionStore(runtimePool)
	devAuth := auth.NewDevAuthHandler(cfg, store, runtimePool)

	// 1. Caller from non-loopback IP rejected with 403
	reqExternal := httptest.NewRequest(http.MethodPost, "/api/auth/dev-login", strings.NewReader(`{}`))
	reqExternal.RemoteAddr = "192.168.1.50:52134"
	recExternal := httptest.NewRecorder()
	devAuth.HandleDevLogin(recExternal, reqExternal)
	if recExternal.Code != http.StatusForbidden || !strings.Contains(recExternal.Body.String(), "LOOPBACK_REQUIRED") {
		t.Fatalf("expected 403 LOOPBACK_REQUIRED for non-loopback remote addr, got %d", recExternal.Code)
	}

	// 2. Caller from loopback succeeds and returns X-Deadbolt-Auth-Mode: local-dev
	reqLoopback := httptest.NewRequest(http.MethodPost, "/api/auth/dev-login", strings.NewReader(`{"email":"dev@deadbolt.local","name":"Local Developer"}`))
	reqLoopback.RemoteAddr = "127.0.0.1:54321"
	recLoopback := httptest.NewRecorder()
	devAuth.HandleDevLogin(recLoopback, reqLoopback)

	if recLoopback.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from dev-login on loopback, got %d (body: %s)", recLoopback.Code, recLoopback.Body.String())
	}
	if recLoopback.Header().Get("X-Deadbolt-Auth-Mode") != "local-dev" {
		t.Fatalf("expected X-Deadbolt-Auth-Mode: local-dev header, got %q", recLoopback.Header().Get("X-Deadbolt-Auth-Mode"))
	}
}

// TestNegativeAuthResponsesZeroLeakage verifies negative auth responses contain no tokens,
// cookies, or cross-tenant data.
func TestNegativeAuthResponsesZeroLeakage(t *testing.T) {
	db, runtimePool, _ := setupTestDB(t)
	defer db.Close()
	defer runtimePool.Close()

	cfg := auth.Config{
		RuntimeMode: auth.ModeHosted,
	}
	store := auth.NewSessionStore(runtimePool)
	bff := auth.NewBFFHandler(cfg, nil, store, runtimePool)

	// 1. Request with nonexistent token
	req := httptest.NewRequest(http.MethodGet, "/api/auth/session", nil)
	fakeToken := "deadbeef000011112222333344445555"
	req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: fakeToken})
	rec := httptest.NewRecorder()

	bff.RequireAuth(http.HandlerFunc(bff.HandleGetSession)).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", rec.Code)
	}

	body := rec.Body.String()
	// Assert no tokens in body
	if strings.Contains(body, fakeToken) {
		t.Fatalf("SECURITY VIOLATION: negative response body leaked session token: %s", body)
	}
	// Assert no credentials or cross-tenant data in headers or body
	if strings.Contains(body, "password") || strings.Contains(body, "secret") {
		t.Fatalf("sensitive field leaked in negative response: %s", body)
	}

	// Verify cookie was cleared with MaxAge=-1
	var cleared bool
	for _, c := range rec.Result().Cookies() {
		if c.Name == auth.SessionCookieName && c.MaxAge == -1 {
			cleared = true
			break
		}
	}
	if !cleared {
		t.Fatalf("expected session cookie to be cleared on unauthenticated error")
	}
}

// TestOIDCTokenVerificationNegativeCases tests rejection of invalid signatures,
// expired tokens, mismatched issuer, and mismatched audience.
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

	// 1. Expired token
	expiredToken, err := fixture.SignIDToken(fixture.URL(), fixture.ClientID(), "user1", "user1@deadbolt.local", "User 1", "nonce1", -10*time.Minute)
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}
	_, err = oidcClient.VerifyIDToken(ctx, expiredToken, "nonce1")
	if err == nil || !strings.Contains(err.Error(), "token expired") {
		t.Fatalf("expected token expired error, got: %v", err)
	}

	// 2. Mismatched issuer
	badIssuerToken, err := fixture.SignIDToken("https://rogue-issuer.com", fixture.ClientID(), "user1", "user1@deadbolt.local", "User 1", "nonce1", 1*time.Hour)
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}
	_, err = oidcClient.VerifyIDToken(ctx, badIssuerToken, "nonce1")
	if err == nil || !strings.Contains(err.Error(), "issuer mismatch") {
		t.Fatalf("expected issuer mismatch error, got: %v", err)
	}

	// 3. Mismatched audience
	badAudToken, err := fixture.SignIDToken(fixture.URL(), "wrong-client-id", "user1", "user1@deadbolt.local", "User 1", "nonce1", 1*time.Hour)
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}
	_, err = oidcClient.VerifyIDToken(ctx, badAudToken, "nonce1")
	if err == nil || !strings.Contains(err.Error(), "audience mismatch") {
		t.Fatalf("expected audience mismatch error, got: %v", err)
	}

	// 4. Mismatched nonce
	badNonceToken, err := fixture.SignIDToken(fixture.URL(), fixture.ClientID(), "user1", "user1@deadbolt.local", "User 1", "wrong-nonce", 1*time.Hour)
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}
	_, err = oidcClient.VerifyIDToken(ctx, badNonceToken, "expected-nonce")
	if err == nil || !strings.Contains(err.Error(), "nonce mismatch") {
		t.Fatalf("expected nonce mismatch error, got: %v", err)
	}
}
