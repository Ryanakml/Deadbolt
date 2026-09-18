package cli

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestControlPlaneURLPersistence proves `runtime login --control-plane-url`
// persists the non-secret endpoint so later invocations reload it instead of
// falling back to localhost, while an explicit environment value keeps
// precedence.
func TestControlPlaneURLPersistence(t *testing.T) {
	isolatedCredentials(t)

	if err := StoreControlPlaneURL("https://staging.example:443/"); err != nil {
		t.Fatalf("store control plane URL: %v", err)
	}
	t.Setenv("DEADBOLT_API_URL", "")
	if got := LoadConfig().APIURL; got != "https://staging.example:443" {
		t.Fatalf("expected persisted endpoint to load, got %q", got)
	}

	t.Setenv("DEADBOLT_API_URL", "http://localhost:9999")
	if got := LoadConfig().APIURL; got != "http://localhost:9999" {
		t.Fatalf("expected environment override to win, got %q", got)
	}

	if err := StoreControlPlaneURL("://not-a-url"); err == nil {
		t.Fatal("expected invalid endpoint to be rejected, got nil")
	}
}

// TestMutatingCommandsSendIdempotencyKeys proves deploy and activation carry
// a per-command Idempotency-Key instead of depending on accidental
// middleware behavior.
func TestMutatingCommandsSendIdempotencyKeys(t *testing.T) {
	isolatedCredentials(t)
	t.Setenv("DEADBOLT_API_KEY", "test-key")
	t.Setenv("DEADBOLT_ORG_ID", "org-1")
	t.Setenv("DEADBOLT_ENV", "staging")

	var keys []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"dep-1","manifestHash":"h","bundleDigest":"b","status":"REGISTERED","createdAt":"2026-09-17T00:00:00Z","name":"wf","activeDeploymentId":"dep-1","revision":1}`))
	}))
	defer server.Close()

	if err := HandleDeployments([]string{"activate", "dep-1", "--workflow", "wf", "--env", "staging", "--control-plane-url", server.URL}); err != nil {
		t.Fatalf("activate failed: %v", err)
	}
	if err := HandleDeployments([]string{"activate", "dep-1", "--workflow", "wf", "--env", "staging", "--control-plane-url", server.URL}); err != nil {
		t.Fatalf("second activate failed: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("expected 2 activate requests, got %d", len(keys))
	}
	for i, k := range keys {
		if k == "" {
			t.Fatalf("activate request %d missing Idempotency-Key", i)
		}
	}
	if keys[0] == keys[1] {
		t.Fatal("expected distinct idempotency keys per logical command invocation")
	}
	if !strings.HasPrefix(keys[0], "dep-act-") {
		t.Fatalf("expected conventional dep-act key prefix, got %q", keys[0])
	}
}

// TestActivateConflictReportsCorrelation proves a revision conflict carries
// the command key and server request ID so an unexpected conflict can be
// joined to durable server records instead of retried blindly.
func TestActivateConflictReportsCorrelation(t *testing.T) {
	isolatedCredentials(t)
	t.Setenv("DEADBOLT_API_KEY", "test-key")
	t.Setenv("DEADBOLT_ORG_ID", "org-1")
	t.Setenv("DEADBOLT_ENV", "staging")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Request-ID", "req-correlation-1")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"code":"REVISION_CONFLICT","message":"conflict"}`))
	}))
	defer server.Close()

	err := HandleDeployments([]string{"activate", "dep-1", "--workflow", "wf", "--env", "staging", "--control-plane-url", server.URL})
	if err == nil || !strings.Contains(err.Error(), "Revision conflict") {
		t.Fatalf("expected revision conflict error, got %v", err)
	}
	if !strings.Contains(err.Error(), "dep-act-") {
		t.Fatalf("expected command key in conflict error, got %v", err)
	}
	if !strings.Contains(err.Error(), "req-correlation-1") {
		t.Fatalf("expected request ID in conflict error, got %v", err)
	}
}

// TestHostedCallbackListenerUsesPinnedLoopbackPort proves hosted browser
// login binds the exact loopback callback registered with the identity
// provider (http://127.0.0.1:8765/callback) instead of a random port.
func TestHostedCallbackListenerUsesPinnedLoopbackPort(t *testing.T) {
	listener, redirectURI, err := hostedCallbackListener()
	if err != nil {
		t.Fatalf("hostedCallbackListener failed: %v", err)
	}
	defer listener.Close()

	if expected := "http://127.0.0.1:8765/callback"; redirectURI != expected {
		t.Fatalf("expected redirect URI %q, got %q", expected, redirectURI)
	}
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok || addr.IP.String() != "127.0.0.1" || addr.Port != hostedLoopbackCallbackPort {
		t.Fatalf("expected loopback listener on 127.0.0.1:%d, got %v", hostedLoopbackCallbackPort, listener.Addr())
	}

	// A second concurrent login must fail instead of silently falling back to
	// a random unregistered port.
	second, _, err := hostedCallbackListener()
	if err == nil {
		second.Close()
		t.Fatal("expected second hosted callback listener to fail while the fixed port is occupied")
	}
}

// isolatedCredentials redirects the credential store to a temp dir for the
// duration of a test and restores the previous setting afterwards.
func isolatedCredentials(t *testing.T) {
	t.Helper()
	prev := customCredentialsDir
	SetCustomCredentialsDir(t.TempDir())
	t.Cleanup(func() { SetCustomCredentialsDir(prev) })
}

func mustCredential(t *testing.T, account string) string {
	t.Helper()
	v, err := GetCredential("deadbolt", account)
	if err != nil {
		t.Fatalf("expected credential %q to exist: %v", account, err)
	}
	return v
}

func mustNoCredential(t *testing.T, account string) {
	t.Helper()
	if v, err := GetCredential("deadbolt", account); err == nil {
		t.Fatalf("expected credential %q to be absent, got %q", account, v)
	} else if !errors.Is(err, ErrCredentialNotFound) {
		t.Fatalf("expected ErrCredentialNotFound for %q, got %v", account, err)
	}
}

// TestCommitHostedLoginContextReplacesOrg proves a successful login carrying
// an organization deterministically replaces stale context.
func TestCommitHostedLoginContextReplacesOrg(t *testing.T) {
	isolatedCredentials(t)
	if err := StoreCredential("deadbolt", "api_key", "old-key"); err != nil {
		t.Fatal(err)
	}
	if err := StoreCredential("deadbolt", "org_id", "old-org"); err != nil {
		t.Fatal(err)
	}
	if err := StoreCredential("deadbolt", "env", "old-env"); err != nil {
		t.Fatal(err)
	}

	orgID, envName, err := commitHostedLoginContext(hostedTokenResponse{
		AccessToken:    "new-key",
		OrganizationID: "new-org",
		Environment:    "new-env",
	}, "", "")
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if orgID != "new-org" || envName != "new-env" {
		t.Fatalf("unexpected resolved context org=%q env=%q", orgID, envName)
	}
	if got := mustCredential(t, "api_key"); got != "new-key" {
		t.Fatalf("api_key not replaced, got %q", got)
	}
	if got := mustCredential(t, "org_id"); got != "new-org" {
		t.Fatalf("org_id not replaced, got %q", got)
	}
	if got := mustCredential(t, "env"); got != "new-env" {
		t.Fatalf("env not replaced, got %q", got)
	}
}

// TestCommitHostedLoginContextRemovesStaleOrg proves a successful login with
// no organization removes stale organization/environment context instead of
// letting a previous login leak into the new session.
func TestCommitHostedLoginContextRemovesStaleOrg(t *testing.T) {
	isolatedCredentials(t)
	if err := StoreCredential("deadbolt", "api_key", "old-key"); err != nil {
		t.Fatal(err)
	}
	if err := StoreCredential("deadbolt", "org_id", "old-org"); err != nil {
		t.Fatal(err)
	}
	if err := StoreCredential("deadbolt", "env", "old-env"); err != nil {
		t.Fatal(err)
	}

	if _, _, err := commitHostedLoginContext(hostedTokenResponse{AccessToken: "new-key"}, "", ""); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if got := mustCredential(t, "api_key"); got != "new-key" {
		t.Fatalf("api_key not replaced, got %q", got)
	}
	mustNoCredential(t, "org_id")
	mustNoCredential(t, "env")
}

// TestCommitHostedLoginContextRespectsOverrides proves explicit CLI flags win
// over token response values.
func TestCommitHostedLoginContextRespectsOverrides(t *testing.T) {
	isolatedCredentials(t)
	orgID, envName, err := commitHostedLoginContext(hostedTokenResponse{
		AccessToken:    "new-key",
		OrganizationID: "token-org",
		Environment:    "token-env",
	}, "flag-org", "flag-env")
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if orgID != "flag-org" || envName != "flag-env" {
		t.Fatalf("unexpected resolved context org=%q env=%q", orgID, envName)
	}
	if got := mustCredential(t, "org_id"); got != "flag-org" {
		t.Fatalf("org_id override not stored, got %q", got)
	}
	if got := mustCredential(t, "env"); got != "flag-env" {
		t.Fatalf("env override not stored, got %q", got)
	}
}

// TestCommitHostedLoginContextRejectsEmptyToken proves a tokenless exchange
// fails without partially corrupting previous valid context.
func TestCommitHostedLoginContextRejectsEmptyToken(t *testing.T) {
	isolatedCredentials(t)
	if err := StoreCredential("deadbolt", "api_key", "old-key"); err != nil {
		t.Fatal(err)
	}
	if err := StoreCredential("deadbolt", "org_id", "old-org"); err != nil {
		t.Fatal(err)
	}

	if _, _, err := commitHostedLoginContext(hostedTokenResponse{}, "", ""); err == nil {
		t.Fatal("expected error for empty access token, got nil")
	}
	if got := mustCredential(t, "api_key"); got != "old-key" {
		t.Fatalf("prior api_key corrupted, got %q", got)
	}
	if got := mustCredential(t, "org_id"); got != "old-org" {
		t.Fatalf("prior org_id corrupted, got %q", got)
	}
}

// faultCredentialOps installs a credential fault hook failing only the given
// service/account/op triples. It returns a restore function.
func faultCredentialOps(t *testing.T, faults ...string) {
	t.Helper()
	set := make(map[string]bool, len(faults))
	for _, f := range faults {
		set[f] = true
	}
	credentialFault = func(service, account, op string) error {
		if set[service+"/"+account+"/"+op] {
			return errors.New("injected credential failure")
		}
		return nil
	}
	t.Cleanup(func() { credentialFault = nil })
}

func seedLoginContext(t *testing.T) {
	t.Helper()
	for account, value := range map[string]string{"api_key": "old-key", "org_id": "old-org", "env": "old-env"} {
		if err := StoreCredential("deadbolt", account, value); err != nil {
			t.Fatal(err)
		}
	}
}

// TestCommitHostedLoginContextRollsBackOrgFailure proves a failure storing
// the organization reports an error and restores the previous api_key
// instead of leaving a partially replaced success.
func TestCommitHostedLoginContextRollsBackOrgFailure(t *testing.T) {
	isolatedCredentials(t)
	seedLoginContext(t)
	faultCredentialOps(t, "deadbolt/org_id/store")

	_, _, err := commitHostedLoginContext(hostedTokenResponse{AccessToken: "new-key", OrganizationID: "new-org", Environment: "new-env"}, "", "")
	if err == nil {
		t.Fatal("expected error for org store failure, got nil")
	}
	if got := mustCredential(t, "api_key"); got != "old-key" {
		t.Fatalf("api_key not rolled back, got %q", got)
	}
	if got := mustCredential(t, "org_id"); got != "old-org" {
		t.Fatalf("org_id changed despite failure, got %q", got)
	}
	if got := mustCredential(t, "env"); got != "old-env" {
		t.Fatalf("env changed despite failure, got %q", got)
	}
}

// TestCommitHostedLoginContextRollsBackEnvFailure proves a failure clearing
// stale environment context restores both api_key and org_id.
func TestCommitHostedLoginContextRollsBackEnvFailure(t *testing.T) {
	isolatedCredentials(t)
	seedLoginContext(t)
	faultCredentialOps(t, "deadbolt/env/delete")

	_, _, err := commitHostedLoginContext(hostedTokenResponse{AccessToken: "new-key"}, "", "")
	if err == nil {
		t.Fatal("expected error for env delete failure, got nil")
	}
	if got := mustCredential(t, "api_key"); got != "old-key" {
		t.Fatalf("api_key not rolled back, got %q", got)
	}
	if got := mustCredential(t, "org_id"); got != "old-org" {
		t.Fatalf("org_id not rolled back, got %q", got)
	}
	if got := mustCredential(t, "env"); got != "old-env" {
		t.Fatalf("env changed despite failure, got %q", got)
	}
}

// TestCommitHostedLoginContextApiKeyFailureWritesNothing proves a failure on
// the first write leaves all previous context untouched.
func TestCommitHostedLoginContextApiKeyFailureWritesNothing(t *testing.T) {
	isolatedCredentials(t)
	seedLoginContext(t)
	faultCredentialOps(t, "deadbolt/api_key/store")

	_, _, err := commitHostedLoginContext(hostedTokenResponse{AccessToken: "new-key", OrganizationID: "new-org"}, "", "")
	if err == nil {
		t.Fatal("expected error for api_key store failure, got nil")
	}
	if got := mustCredential(t, "api_key"); got != "old-key" {
		t.Fatalf("prior api_key corrupted, got %q", got)
	}
	if got := mustCredential(t, "org_id"); got != "old-org" {
		t.Fatalf("prior org_id corrupted, got %q", got)
	}
}

// TestDeleteCredentialNotFoundIsSuccess proves deleting an absent credential
// succeeds while injected real failures are reported.
func TestDeleteCredentialNotFoundIsSuccess(t *testing.T) {
	isolatedCredentials(t)
	if err := DeleteCredential("deadbolt", "no-such-account"); err != nil {
		t.Fatalf("expected nil for absent credential, got %v", err)
	}
	faultCredentialOps(t, "deadbolt/no-such-account/delete")
	if err := DeleteCredential("deadbolt", "no-such-account"); err == nil {
		t.Fatal("expected injected delete failure to be reported, got nil")
	}
}

// TestIsNotFoundOutputMatchers unit-tests the native absence predicates used
// to distinguish genuine absence from real deletion failures.
func TestIsNotFoundOutputMatchers(t *testing.T) {
	if !isKeychainNotFoundOutput("security: SecKeychainSearchCopyNext: The specified item could not be found in the keychain.") {
		t.Fatal("expected keychain absence to match")
	}
	if isKeychainNotFoundOutput("security: SecKeychainItemModifyContent: auth failed.") {
		t.Fatal("keychain auth failure must not match absence")
	}
	if isKeychainNotFoundOutput("") {
		t.Fatal("empty output must not match absence")
	}
	if !isSecretToolNotFoundOutput("No such secret") {
		t.Fatal("expected secret-tool absence to match")
	}
	if isSecretToolNotFoundOutput("Cannot autolaunch D-Bus") {
		t.Fatal("D-Bus failure must not match absence")
	}
}

// TestLoginAPIKeyProvisioningIsExplicitPartialUpdate locks the audited
// semantics of `runtime login --api-key`: only passed values are stored,
// retained context is intentional, never accidental.
func TestLoginAPIKeyProvisioningIsExplicitPartialUpdate(t *testing.T) {
	isolatedCredentials(t)
	seedLoginContext(t)

	if err := RunLogin([]string{"--api-key", "rotated-key"}); err != nil {
		t.Fatalf("api-key provisioning failed: %v", err)
	}
	if got := mustCredential(t, "api_key"); got != "rotated-key" {
		t.Fatalf("api_key not replaced, got %q", got)
	}
	if got := mustCredential(t, "org_id"); got != "old-org" {
		t.Fatalf("org_id must be retained without --org, got %q", got)
	}
	if got := mustCredential(t, "env"); got != "old-env" {
		t.Fatalf("env must be retained without --env, got %q", got)
	}

	if err := RunLogin([]string{"--api-key", "rotated-key-2", "--org", "new-org", "--env", "new-env"}); err != nil {
		t.Fatalf("api-key provisioning with flags failed: %v", err)
	}
	if got := mustCredential(t, "org_id"); got != "new-org" {
		t.Fatalf("org_id flag not stored, got %q", got)
	}
	if got := mustCredential(t, "env"); got != "new-env" {
		t.Fatalf("env flag not stored, got %q", got)
	}
}
