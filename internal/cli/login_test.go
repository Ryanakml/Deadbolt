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

	if err := HandleDeployments([]string{"activate", "dep-1", "--workflow", "wf", "--env", "11111111-1111-1111-1111-111111111111", "--control-plane-url", server.URL}); err != nil {
		t.Fatalf("activate failed: %v", err)
	}
	if err := HandleDeployments([]string{"activate", "dep-1", "--workflow", "wf", "--env", "11111111-1111-1111-1111-111111111111", "--control-plane-url", server.URL}); err != nil {
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

	err := HandleDeployments([]string{"activate", "dep-1", "--workflow", "wf", "--env", "11111111-1111-1111-1111-111111111111", "--control-plane-url", server.URL})
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

// TestCommitHostedLoginContextClearsCanonicalIDs proves an org-less login
// removes stored canonical IDs as well as names, so no stale bootstrap
// context can survive it.
func TestCommitHostedLoginContextClearsCanonicalIDs(t *testing.T) {
	isolatedCredentials(t)
	seedLoginContext(t)
	for account, value := range map[string]string{"env_id": "old-env-id", "project_id": "old-proj-id", "project": "old-proj"} {
		if err := StoreCredential("deadbolt", account, value); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := commitHostedLoginContext(hostedTokenResponse{AccessToken: "new-key"}, "", ""); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	for _, account := range []string{"org_id", "env", "env_id", "project_id", "project"} {
		mustNoCredential(t, account)
	}
	if got := mustCredential(t, "api_key"); got != "new-key" {
		t.Fatalf("api_key not replaced, got %q", got)
	}
}

// TestCommitHostedLoginContextClearsEnvOnOrgSwitch proves logging into a
// different organization drops environment context from the previous org
// instead of attaching it to the new one.
func TestCommitHostedLoginContextClearsEnvOnOrgSwitch(t *testing.T) {
	isolatedCredentials(t)
	for account, value := range map[string]string{
		"api_key": "old-key", "org_id": "org-a",
		"env": "staging", "env_id": "env-a", "project_id": "proj-a", "project": "proj-a",
	} {
		if err := StoreCredential("deadbolt", account, value); err != nil {
			t.Fatal(err)
		}
	}
	orgID, _, err := commitHostedLoginContext(hostedTokenResponse{AccessToken: "new-key", OrganizationID: "org-b"}, "", "")
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if orgID != "org-b" {
		t.Fatalf("expected org-b, got %q", orgID)
	}
	if got := mustCredential(t, "api_key"); got != "new-key" {
		t.Fatalf("api_key not replaced, got %q", got)
	}
	if got := mustCredential(t, "org_id"); got != "org-b" {
		t.Fatalf("org_id not replaced, got %q", got)
	}
	for _, account := range []string{"env", "env_id", "project_id", "project"} {
		mustNoCredential(t, account)
	}
}

// TestSnapshotCredentialDistinguishesAbsenceFromBackendFailure proves the
// three snapshot outcomes: present carries the value, genuine absence is
// non-existent, and backend failures surface instead of masquerading as
// absence.
func TestSnapshotCredentialDistinguishesAbsenceFromBackendFailure(t *testing.T) {
	isolatedCredentials(t)
	if err := StoreCredential("deadbolt", "api_key", "present-key"); err != nil {
		t.Fatal(err)
	}

	snap, err := snapshotCredential("deadbolt", "api_key")
	if err != nil || !snap.exists || snap.value != "present-key" {
		t.Fatalf("expected PRESENT(present-key), got %+v (%v)", snap, err)
	}
	snap, err = snapshotCredential("deadbolt", "missing-account")
	if err != nil || snap.exists {
		t.Fatalf("expected ABSENT, got %+v (%v)", snap, err)
	}

	faultCredentialOps(t, "deadbolt/org_id/get")
	if _, err := snapshotCredential("deadbolt", "org_id"); err == nil {
		t.Fatal("expected backend read failure to surface, got nil")
	} else if errors.Is(err, ErrCredentialNotFound) {
		t.Fatalf("backend failure must not map to ErrCredentialNotFound, got %v", err)
	}
}

// TestCommitAbortsBeforeMutationOnSnapshotFailure proves a backend read
// failure during snapshotting aborts hosted context replacement with zero
// mutation attempts and untouched previous context.
func TestCommitAbortsBeforeMutationOnSnapshotFailure(t *testing.T) {
	isolatedCredentials(t)
	seedLoginContext(t)
	mutations := 0
	prev := credentialFault
	credentialFault = func(service, account, op string) error {
		if op == "store" || op == "delete" {
			mutations++
		}
		if service == "deadbolt" && account == "org_id" && op == "get" {
			return errors.New("injected backend read failure")
		}
		if prev != nil {
			return prev(service, account, op)
		}
		return nil
	}
	t.Cleanup(func() { credentialFault = nil })

	_, _, err := commitHostedLoginContext(hostedTokenResponse{AccessToken: "new-key", OrganizationID: "new-org"}, "", "")
	if err == nil {
		t.Fatal("expected snapshot failure to abort, got nil")
	}
	if mutations != 0 {
		t.Fatalf("expected zero mutation attempts, got %d", mutations)
	}
	credentialFault = nil
	if got := mustCredential(t, "api_key"); got != "old-key" {
		t.Fatalf("prior api_key touched, got %q", got)
	}
	if got := mustCredential(t, "org_id"); got != "old-org" {
		t.Fatalf("prior org_id touched, got %q", got)
	}
}

// TestCommitSurfacesRollbackFailure proves that when both the primary write
// and the rollback fail, both errors stay visible without leaking values.
func TestCommitSurfacesRollbackFailure(t *testing.T) {
	isolatedCredentials(t)
	seedLoginContext(t)
	keyStores := 0
	credentialFault = func(service, account, op string) error {
		if service == "deadbolt" && account == "api_key" && op == "store" {
			keyStores++
			if keyStores > 1 {
				return errors.New("injected rollback failure")
			}
		}
		if service == "deadbolt" && account == "org_id" && op == "store" {
			return errors.New("injected primary failure")
		}
		return nil
	}
	t.Cleanup(func() { credentialFault = nil })

	_, _, err := commitHostedLoginContext(hostedTokenResponse{AccessToken: "new-key", OrganizationID: "new-org"}, "", "")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "store org context") || !strings.Contains(msg, "rollback") {
		t.Fatalf("expected primary and rollback failures visible, got %q", msg)
	}
	for _, secret := range []string{"old-key", "new-key", "old-org", "new-org"} {
		if strings.Contains(msg, secret) {
			t.Fatalf("error leaked credential value %q: %s", secret, msg)
		}
	}
}

// TestLoginAPIKeyPartialFailureReportsError proves explicit manual
// provisioning never prints success after a requested write failed.
func TestLoginAPIKeyPartialFailureReportsError(t *testing.T) {
	isolatedCredentials(t)
	seedLoginContext(t)
	faultCredentialOps(t, "deadbolt/org_id/store")

	if err := RunLogin([]string{"--api-key", "rotated-key", "--org", "new-org"}); err == nil {
		t.Fatal("expected error for failed org write, got nil")
	}
	if got := mustCredential(t, "api_key"); got != "rotated-key" {
		t.Fatalf("first write should stand, got %q", got)
	}
}
