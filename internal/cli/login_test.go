package cli

import (
	"errors"
	"net"
	"testing"
)

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
