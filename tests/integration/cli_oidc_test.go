package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/auth"
	"github.com/Ryanakml/Deadbolt/internal/auth/oidcfixture"
	"github.com/Ryanakml/Deadbolt/internal/controlplane"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
)

func TestPublicCLIOIDCExchangesPKCEForHumanBearer(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()
	fixture, err := oidcfixture.NewFixtureServer("deadbolt-cli")
	if err != nil {
		t.Fatal(err)
	}
	defer fixture.Close()
	tc.authCfg.RuntimeMode = auth.ModeHosted
	tc.authCfg.OIDC = auth.OIDCConfig{Issuer: fixture.URL(), ClientID: "dashboard-bff", CLIClientID: "deadbolt-cli"}
	tc.authCfg.AllowedOrigins = []string{"https://dashboard.example"}
	tc.authCfg.CookieSecure = true
	server := httptest.NewServer(controlplane.BuildMux(tc.authCfg, tc.runtimePool, nil, nil))
	defer server.Close()

	var metadata struct {
		Issuer   string `json:"issuer"`
		ClientID string `json:"clientId"`
	}
	getJSON(t, http.MethodGet, server.URL+"/api/auth/cli/config", nil, &metadata, http.StatusOK)
	if metadata.Issuer != fixture.URL() || metadata.ClientID != "deadbolt-cli" {
		t.Fatalf("unexpected public metadata: %#v", metadata)
	}

	requestCode := func(stateOverride string) (string, *auth.PKCEParams, string) {
		pkce, err := auth.GeneratePKCE()
		if err != nil {
			t.Fatal(err)
		}
		callback := httptest.NewServer(http.NewServeMux())
		defer callback.Close()
		codeCh := make(chan string, 1)
		callback.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			codeCh <- r.URL.Query().Get("code")
			w.WriteHeader(http.StatusOK)
		})
		client := auth.NewOIDCClient(auth.OIDCConfig{Issuer: metadata.Issuer, ClientID: metadata.ClientID, RedirectURL: callback.URL + "/callback"}, fixture.Client())
		authorizeURL, err := client.BuildAuthorizationURL(context.Background(), pkce)
		if err != nil {
			t.Fatal(err)
		}
		if stateOverride != "" {
			u, _ := url.Parse(authorizeURL)
			q := u.Query()
			q.Set("state", stateOverride)
			u.RawQuery = q.Encode()
			authorizeURL = u.String()
		}
		resp, err := fixture.Client().Get(authorizeURL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return <-codeCh, pkce, callback.URL + "/callback"
	}

	code, pkce, redirect := requestCode("")
	wrong := postCLIToken(t, server.URL, code, "wrong-verifier", redirect, pkce.Nonce)
	if wrong.Code != http.StatusUnauthorized {
		t.Fatalf("wrong verifier status=%d", wrong.Code)
	}
	code, pkce, redirect = requestCode("")
	valid := postCLIToken(t, server.URL, code, pkce.CodeVerifier, redirect, pkce.Nonce)
	if valid.Code != http.StatusOK {
		t.Fatalf("valid exchange status=%d: %s", valid.Code, valid.Body.String())
	}
	var token struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(valid.Body.Bytes(), &token); err != nil || len(token.AccessToken) < 7 || token.AccessToken[:6] != "dbcli_" {
		t.Fatalf("expected dbcli bearer")
	}

	create, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/organizations", bytes.NewBufferString(`{"name":"CLI OIDC Org"}`))
	create.Header.Set("Authorization", "Bearer "+token.AccessToken)
	create.Header.Set("Idempotency-Key", "cli-oidc-create-org")
	create.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(create)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("dbcli human bearer could not call /v1: %d", response.StatusCode)
	}
}

func postCLIToken(t *testing.T, base, code, verifier, redirect, nonce string) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(map[string]string{"code": code, "code_verifier": verifier, "redirect_uri": redirect, "nonce": nonce})
	request, _ := http.NewRequest(http.MethodPost, base+"/api/auth/cli/token", bytes.NewReader(b))
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	recorder := httptest.NewRecorder()
	recorder.Code = response.StatusCode
	_, _ = recorder.Body.ReadFrom(response.Body)
	return recorder
}

func getJSON(t *testing.T, method, target string, body *bytes.Buffer, dest any, expected int) {
	t.Helper()
	var requestBody io.Reader
	if body != nil {
		requestBody = body
	}
	request, _ := http.NewRequest(method, target, requestBody)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != expected {
		t.Fatalf("%s %s got %d", method, target, response.StatusCode)
	}
	if err := json.NewDecoder(response.Body).Decode(dest); err != nil {
		t.Fatal(err)
	}
}

// TestCLITokenOrgSelectionAcrossMembershipStates proves CLI token issuance
// binds an active organization only for exactly one ACTIVE membership. Zero,
// several, or suspended-only memberships yield an empty organization so the
// CLI must select explicitly instead of inheriting an arbitrary one.
func TestCLITokenOrgSelectionAcrossMembershipStates(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()
	fixture, err := oidcfixture.NewFixtureServer("deadbolt-cli")
	if err != nil {
		t.Fatal(err)
	}
	defer fixture.Close()
	tc.authCfg.RuntimeMode = auth.ModeHosted
	tc.authCfg.OIDC = auth.OIDCConfig{Issuer: fixture.URL(), ClientID: "dashboard-bff", CLIClientID: "deadbolt-cli"}
	server := httptest.NewServer(controlplane.BuildMux(tc.authCfg, tc.runtimePool, nil, nil))
	defer server.Close()

	// Unique subject root per run: the shared integration database persists
	// across runs, so constant subjects would inherit earlier memberships.
	runID, _ := tenant.NewUUID()
	sub := func(name string) string {
		return fmt.Sprintf("%s-%s", name, runID[:8])
	}

	exchange := func(subject string) (token, orgID string) {
		t.Helper()
		pkce, err := auth.GeneratePKCE()
		if err != nil {
			t.Fatal(err)
		}
		code := "cli-matrix-" + subject
		fixture.RegisterAuthCodeWithPKCE(code, oidcfixture.TokenClaimOverrides{
			Subject: subject, Email: subject + "@example.com", Name: subject, Nonce: pkce.Nonce, Expiry: time.Hour,
		}, pkce.CodeChallenge, "S256")
		rec := postCLIToken(t, server.URL, code, pkce.CodeVerifier, "http://127.0.0.1:8765/callback", pkce.Nonce)
		if rec.Code != http.StatusOK {
			t.Fatalf("exchange failed: %d (%s)", rec.Code, rec.Body.String())
		}
		var body struct {
			AccessToken    string `json:"access_token"`
			OrganizationID string `json:"organization_id"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body.AccessToken == "" {
			t.Fatalf("invalid token response: %v", err)
		}
		return body.AccessToken, body.OrganizationID
	}
	userIDOf := func(subject string) string {
		t.Helper()
		var id string
		if err := tc.runtimePool.QueryRow(context.Background(), `SELECT user_id::text FROM oidc_identities WHERE issuer = $1 AND subject = $2`, fixture.URL(), subject).Scan(&id); err != nil {
			t.Fatalf("user lookup failed: %v", err)
		}
		return id
	}
	ctx := context.Background()
	mkOrg := func(name string) string {
		t.Helper()
		owner, _ := tenant.NewUUID()
		org, err := tc.service.CreateOrganization(ctx, owner, name)
		if err != nil {
			t.Fatalf("create org: %v", err)
		}
		return org.ID
	}
	addMembership := func(orgID, userID, role, status string) {
		t.Helper()
		if err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
			_, err := tx.Exec(ctx, `INSERT INTO organization_members (organization_id, user_id, role, status) VALUES ($1, $2, $3, $4)`, orgID, userID, role, status)
			return err
		}); err != nil {
			t.Fatalf("create membership: %v", err)
		}
	}

	// 1. Zero memberships: no active org issued.
	if _, orgID := exchange(sub("cli-matrix-zero")); orgID != "" {
		t.Fatalf("zero memberships must yield empty organization, got %q", orgID)
	}

	// 2. Exactly one ACTIVE membership: selected.
	exchange(sub("cli-matrix-one"))
	orgOne := mkOrg("CLI Matrix One")
	addMembership(orgOne, userIDOf(sub("cli-matrix-one")), "Owner", "ACTIVE")
	if _, orgID := exchange(sub("cli-matrix-one")); orgID != orgOne {
		t.Fatalf("single active membership must be selected, got %q", orgID)
	}

	// 3. Two ACTIVE memberships: unset for explicit selection.
	exchange(sub("cli-matrix-two"))
	uidTwo := userIDOf(sub("cli-matrix-two"))
	addMembership(mkOrg("CLI Matrix Two A"), uidTwo, "Owner", "ACTIVE")
	addMembership(mkOrg("CLI Matrix Two B"), uidTwo, "Viewer", "ACTIVE")
	if _, orgID := exchange(sub("cli-matrix-two")); orgID != "" {
		t.Fatalf("several memberships must yield empty organization, got %q", orgID)
	}

	// 4. Suspended plus active: only the active one is selected.
	exchange(sub("cli-matrix-mix"))
	uidMix := userIDOf(sub("cli-matrix-mix"))
	addMembership(mkOrg("CLI Matrix Old"), uidMix, "Owner", "SUSPENDED")
	orgMix := mkOrg("CLI Matrix New")
	addMembership(orgMix, uidMix, "Developer", "ACTIVE")
	if _, orgID := exchange(sub("cli-matrix-mix")); orgID != orgMix {
		t.Fatalf("suspended membership must not be selected, got %q", orgID)
	}
}
