package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/auth"
)

// RunLogin handles "runtime login [--local] [--api-key <key>] [--org <org_id>] [--env <env>] [--control-plane-url <url>]"
func RunLogin(args []string) error {
	fs := flag.NewFlagSet("login", flag.ContinueOnError)
	localFlag := fs.Bool("local", false, "Use local developer authentication (loopback only)")
	apiKeyFlag := fs.String("api-key", "", "Directly configure an API key into secure credentials store")
	orgFlag := fs.String("org", "", "Set default organization ID")
	envFlag := fs.String("env", "", "Set default environment (e.g. development, staging, production)")
	cpURLFlag := fs.String("control-plane-url", "", "Control plane base URL (defaults to DEADBOLT_API_URL or http://localhost:8080)")
	emailFlag := fs.String("email", "dev-admin@deadbolt.local", "Email address for local developer session")

	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := LoadConfig()
	if *cpURLFlag != "" {
		cfg.APIURL = strings.TrimRight(*cpURLFlag, "/")
		if err := StoreControlPlaneURL(cfg.APIURL); err != nil {
			return fmt.Errorf("save control plane endpoint: %w", err)
		}
	}

	// 1. Direct API key provisioning
	if *apiKeyFlag != "" {
		if err := StoreCredential("deadbolt", "api_key", *apiKeyFlag); err != nil {
			return fmt.Errorf("failed to save API key to secure credentials store: %w", err)
		}
		if *orgFlag != "" {
			if err := StoreCredential("deadbolt", "org_id", *orgFlag); err != nil {
				return err
			}
		}
		if *envFlag != "" {
			if err := StoreCredential("deadbolt", "env", *envFlag); err != nil {
				return err
			}
		}
		fmt.Println("✓ API key securely saved to credentials store.")
		if *orgFlag != "" {
			fmt.Printf("  Default Organization: %s\n", *orgFlag)
		}
		if *envFlag != "" {
			fmt.Printf("  Default Environment:  %s\n", *envFlag)
		}
		return nil
	}

	// Check if local dev mode
	isLocal := *localFlag || isLocalURL(cfg.APIURL)

	// 2. CI / Non-interactive check (Blueprint §22 / Issue #15)
	if !isLocal && isCIOrNonInteractive() {
		return fmt.Errorf("interactive browser login is not supported in non-interactive / CI environments. Configure the DEADBOLT_API_KEY environment variable or pass --api-key <key>")
	}

	if isLocal {
		return runLocalDevLogin(cfg, *emailFlag, *orgFlag, *envFlag)
	}

	return runHostedBrowserLogin(cfg, *orgFlag, *envFlag)
}

func isCIOrNonInteractive() bool {
	if os.Getenv("CI") != "" || os.Getenv("CONTINUOUS_INTEGRATION") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		return true
	}
	// Check if stdin is a character device
	fileInfo, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (fileInfo.Mode() & os.ModeCharDevice) == 0
}

func isLocalURL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := u.Hostname()
	return host == "localhost" || host == "127.0.0.1" || host == "::1" || strings.HasPrefix(host, "127.")
}

// runLocalDevLogin handles local dev login via /api/auth/dev-login and bootstraps local dev entities via public HTTP endpoints
func runLocalDevLogin(cfg Config, email string, customOrg string, customEnv string) error {
	fmt.Printf("Authenticating with local Deadbolt control plane at %s...\n", cfg.APIURL)

	loginURL := fmt.Sprintf("%s/api/auth/dev-login", cfg.APIURL)
	reqBody, _ := json.Marshal(map[string]string{
		"email": email,
		"name":  "Local Development Admin",
	})

	httpReq, err := http.NewRequest("POST", loginURL, bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Origin", cfg.APIURL)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed to connect to local control plane at %s: %w. Ensure control plane is running", cfg.APIURL, err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("local dev login failed: %s", FormatAPIError(resp.StatusCode, bodyBytes))
	}

	// Extract session cookie & CSRF token
	cookieMap := make(map[string]*http.Cookie)
	for _, c := range resp.Cookies() {
		cookieMap[c.Name] = c
	}

	var devResp struct {
		Mode                 string  `json:"mode"`
		SessionID            string  `json:"session_id"`
		ActiveOrganizationID *string `json:"active_organization_id"`
		CSRFToken            string  `json:"csrf_token"`
	}
	if err := json.Unmarshal(bodyBytes, &devResp); err != nil {
		return fmt.Errorf("failed to parse dev login response: %w", err)
	}

	csrfToken := devResp.CSRFToken
	for name, c := range cookieMap {
		if strings.Contains(name, "csrf") && csrfToken == "" {
			csrfToken = c.Value
		}
		if strings.Contains(name, "session") {
			_ = StoreCredential("deadbolt", "session_token", c.Value)
		}
	}
	if csrfToken != "" {
		_ = StoreCredential("deadbolt", "csrf_token", csrfToken)
	}

	// Helper for authenticated requests using session cookie & CSRF
	doAuthReq := func(method, path string, payload any) (*http.Response, []byte, error) {
		var bodyReader io.Reader
		if payload != nil {
			data, _ := json.Marshal(payload)
			bodyReader = bytes.NewReader(data)
		}
		r, err := http.NewRequest(method, cfg.APIURL+path, bodyReader)
		if err != nil {
			return nil, nil, err
		}
		for _, c := range cookieMap {
			r.AddCookie(c)
		}
		if csrfToken != "" {
			r.Header.Set("X-CSRF-Token", csrfToken)
		}
		r.Header.Set("Origin", cfg.APIURL)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Accept", "application/json")
		if method != http.MethodGet && method != http.MethodHead {
			r.Header.Set("Idempotency-Key", fmt.Sprintf("local-bootstrap-%d", time.Now().UnixNano()))
		}

		res, err := client.Do(r)
		if err != nil {
			return nil, nil, err
		}
		defer res.Body.Close()
		for _, c := range res.Cookies() {
			cookieMap[c.Name] = c
			if strings.Contains(c.Name, "csrf") {
				csrfToken = c.Value
			}
		}
		b, err := io.ReadAll(res.Body)
		return res, b, err
	}

	orgID := ""
	if devResp.ActiveOrganizationID != nil && *devResp.ActiveOrganizationID != "" {
		orgID = *devResp.ActiveOrganizationID
	}
	if customOrg != "" {
		orgID = customOrg
	}

	// If user has no active organization, create one
	if orgID == "" {
		res, resBody, err := doAuthReq("POST", "/v1/organizations", map[string]string{"name": "Local Development Org"})
		if err != nil {
			return fmt.Errorf("local bootstrap create organization: %w", err)
		}
		if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusCreated {
			return fmt.Errorf("local bootstrap create organization: status %d: %s", res.StatusCode, string(resBody))
		}
		var newOrg struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(resBody, &newOrg); err != nil || newOrg.ID == "" {
			return fmt.Errorf("local bootstrap parse created organization: %w", err)
		}
		orgID = newOrg.ID
		// Switch session to new org
		res, resBody, err = doAuthReq("POST", "/api/auth/switch-org", map[string]string{"organization_id": orgID})
		if err != nil {
			return fmt.Errorf("local bootstrap switch organization: %w", err)
		}
		if res.StatusCode != http.StatusOK {
			return fmt.Errorf("local bootstrap switch organization: status %d: %s", res.StatusCode, string(resBody))
		}
	}
	if orgID == "" {
		return fmt.Errorf("local bootstrap did not create or select an organization")
	}

	targetEnv := "development"
	if customEnv != "" {
		targetEnv = customEnv
	}

	if err := StoreCredential("deadbolt", "org_id", orgID); err != nil {
		return err
	}

	// List or create project
	var projectID string
	res, resBody, err := doAuthReq("GET", "/v1/projects", nil)
	if err != nil {
		return fmt.Errorf("local bootstrap list projects: %w", err)
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("local bootstrap list projects: status %d: %s", res.StatusCode, string(resBody))
	}
	var prjList struct {
		Projects []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"projects"`
	}
	if err := json.Unmarshal(resBody, &prjList); err != nil {
		return fmt.Errorf("local bootstrap parse projects: %w", err)
	}
	if len(prjList.Projects) > 0 {
		projectID = prjList.Projects[0].ID
	}

	if projectID == "" {
		res, resBody, err := doAuthReq("POST", "/v1/projects", map[string]string{"name": "default"})
		if err != nil {
			return fmt.Errorf("local bootstrap create project: %w", err)
		}
		if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusCreated {
			return fmt.Errorf("local bootstrap create project: status %d: %s", res.StatusCode, string(resBody))
		}
		var newPrj struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(resBody, &newPrj); err != nil || newPrj.ID == "" {
			return fmt.Errorf("local bootstrap parse created project: %w", err)
		}
		projectID = newPrj.ID
	}

	// Find or create environment
	var envID string
	res, resBody, err = doAuthReq("GET", fmt.Sprintf("/v1/projects/%s/environments", projectID), nil)
	if err != nil {
		return fmt.Errorf("local bootstrap list environments: %w", err)
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("local bootstrap list environments: status %d: %s", res.StatusCode, string(resBody))
	}
	var envList struct {
		Environments []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"environments"`
	}
	if err := json.Unmarshal(resBody, &envList); err != nil {
		return fmt.Errorf("local bootstrap parse environments: %w", err)
	}
	for _, e := range envList.Environments {
		if e.Name == targetEnv {
			envID = e.ID
			break
		}
	}

	if envID == "" {
		res, resBody, err = doAuthReq("POST", fmt.Sprintf("/v1/projects/%s/environments", projectID), map[string]any{
			"name":            targetEnv,
			"max_concurrency": 10,
		})
		if err != nil {
			return fmt.Errorf("local bootstrap create environment %s: %w", targetEnv, err)
		}
		if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusCreated {
			return fmt.Errorf("local bootstrap create environment %s: status %d: %s", targetEnv, res.StatusCode, string(resBody))
		}
		var newEnv struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(resBody, &newEnv); err != nil || newEnv.ID == "" {
			return fmt.Errorf("local bootstrap parse created environment: %w", err)
		}
		envID = newEnv.ID
	}

	// Generate an API key for CLI commands if env exists
	res, resBody, err = doAuthReq("POST", fmt.Sprintf("/v1/environments/%s/api-keys", envID), map[string]any{
		"capabilities": []string{
			"org:read",
			"deployments:register",
			"deployments:write",
			"deployments:activate:staging",
			"runs:create",
			"runs:read",
			"payload:read",
			"workers:read",
			"workers:drain",
			"admin:key",
		},
		"expiry_days": 365,
	})
	if err != nil {
		return fmt.Errorf("local bootstrap create api key: %w", err)
	}
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusCreated {
		return fmt.Errorf("local bootstrap create api key: status %d: %s", res.StatusCode, string(resBody))
	}
	var keyResp struct {
		Key          string `json:"key"`
		PlaintextKey string `json:"plaintextKey"`
	}
	if err := json.Unmarshal(resBody, &keyResp); err != nil {
		return fmt.Errorf("local bootstrap parse api key response: %w", err)
	}
	rawKey := keyResp.Key
	if rawKey == "" {
		rawKey = keyResp.PlaintextKey
	}
	if rawKey == "" {
		return fmt.Errorf("local bootstrap created api key is empty in response: %s", string(resBody))
	}
	if err := StoreCredential("deadbolt", "api_key", rawKey); err != nil {
		return err
	}

	if err := StoreCredential("deadbolt", "env", targetEnv); err != nil {
		return err
	}
	if _, err := GetCredential("deadbolt", "api_key"); err != nil {
		return fmt.Errorf("local bootstrap did not create a usable API key: %w", err)
	}

	fmt.Println("✓ Successfully authenticated to local Deadbolt environment.")
	fmt.Printf("  User:         %s\n", email)
	if orgID != "" {
		fmt.Printf("  Organization: %s\n", orgID)
	}
	fmt.Printf("  Environment:  %s\n", targetEnv)
	fmt.Println("  Credentials stored securely in OS credentials store.")
	return nil
}

// hostedLoopbackCallbackPort is the fixed loopback port registered as the
// Allowed Callback URL (http://127.0.0.1:8765/callback) on the Auth0 Native
// Application for hosted Deadbolt CLI login. A random port would not match
// the registered callback and the identity provider would refuse the flow.
const hostedLoopbackCallbackPort = 8765

// hostedCallbackListener binds the fixed loopback callback for hosted browser
// login and returns the exact redirect URI registered with the provider.
func hostedCallbackListener() (net.Listener, string, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", hostedLoopbackCallbackPort))
	if err != nil {
		return nil, "", fmt.Errorf("failed to start local callback server on 127.0.0.1:%d (is another login already in progress?): %w", hostedLoopbackCallbackPort, err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	return listener, fmt.Sprintf("http://127.0.0.1:%d/callback", port), nil
}

// runHostedBrowserLogin executes PKCE authorization code flow with loopback redirect
func runHostedBrowserLogin(cfg Config, customOrg string, customEnv string) error {
	pkce, err := auth.GeneratePKCE()
	if err != nil {
		return fmt.Errorf("failed to initialize PKCE security parameters: %w", err)
	}

	// Bind the fixed loopback callback registered with the identity provider.
	listener, redirectURI, err := hostedCallbackListener()
	if err != nil {
		return err
	}
	defer listener.Close()

	metadataResp, err := http.Get(cfg.APIURL + "/api/auth/cli/config")
	if err != nil {
		return fmt.Errorf("fetch hosted CLI OIDC configuration: %w", err)
	}
	defer metadataResp.Body.Close()
	metadataBody, _ := io.ReadAll(metadataResp.Body)
	if metadataResp.StatusCode != http.StatusOK {
		return fmt.Errorf("hosted CLI login unavailable: %s", FormatAPIError(metadataResp.StatusCode, metadataBody))
	}
	var metadata struct {
		Issuer   string `json:"issuer"`
		ClientID string `json:"clientId"`
	}
	if err := json.Unmarshal(metadataBody, &metadata); err != nil || metadata.Issuer == "" || metadata.ClientID == "" {
		return fmt.Errorf("invalid hosted CLI OIDC configuration")
	}
	publicOIDC := auth.NewOIDCClient(auth.OIDCConfig{Issuer: metadata.Issuer, ClientID: metadata.ClientID, RedirectURL: redirectURI}, nil)
	authURL, err := publicOIDC.BuildAuthorizationURL(context.Background(), pkce)
	if err != nil {
		return fmt.Errorf("build hosted OIDC authorization URL: %w", err)
	}

	fmt.Println("Opening your browser for Deadbolt authentication...")
	fmt.Printf("If the browser does not open automatically, visit:\n\n  %s\n\n", authURL)

	// Attempt to open browser
	_ = openBrowser(authURL)

	codeChan := make(chan string, 1)
	errChan := make(chan error, 1)

	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/callback" {
				http.NotFound(w, r)
				return
			}

			q := r.URL.Query()
			state := q.Get("state")
			if state != pkce.State {
				errChan <- fmt.Errorf("state parameter mismatch; possible CSRF attack")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte("Authentication failed: state parameter mismatch."))
				return
			}

			code := q.Get("code")
			if code == "" {
				errChan <- fmt.Errorf("no authorization code returned from identity provider")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte("Authentication failed: missing authorization code."))
				return
			}

			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<!DOCTYPE html><html><body style="font-family:sans-serif;text-align:center;padding-top:50px;">
<h2>Authentication Successful</h2>
<p>You may close this tab and return to your terminal.</p>
</body></html>`))

			codeChan <- code
		}),
	}

	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	var code string
	select {
	case code = <-codeChan:
		// Succeeded
	case err := <-errChan:
		return err
	case <-time.After(2 * time.Minute):
		return fmt.Errorf("authentication timed out waiting for browser callback (2 minutes)")
	}

	_ = server.Shutdown(context.Background())

	// The control plane performs the public-client exchange and issues a distinct
	// dbcli_ human bearer session after signature/audience/nonce verification.
	tokenURL := fmt.Sprintf("%s/api/auth/cli/token", cfg.APIURL)
	tokenData, _ := json.Marshal(map[string]string{"code": code, "code_verifier": pkce.CodeVerifier, "redirect_uri": redirectURI, "nonce": pkce.Nonce})
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Post(tokenURL, "application/json", bytes.NewReader(tokenData))
	if err != nil {
		return fmt.Errorf("failed to exchange authorization code: %w", err)
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("token exchange failed: %s", FormatAPIError(resp.StatusCode, respBytes))
	}

	var tokenResp hostedTokenResponse
	if err := json.Unmarshal(respBytes, &tokenResp); err != nil {
		return fmt.Errorf("failed to parse token exchange response: %w", err)
	}

	orgID, envName, err := commitHostedLoginContext(tokenResp, customOrg, customEnv)
	if err != nil {
		return err
	}

	fmt.Println("✓ Successfully authenticated via browser.")
	if orgID != "" {
		fmt.Printf("  Organization: %s\n", orgID)
	}
	if envName != "" {
		fmt.Printf("  Environment:  %s\n", envName)
	}
	fmt.Println("  Credentials saved securely in OS credentials store.")

	return nil
}

// hostedTokenResponse is the control-plane token exchange result for a
// successful hosted browser login.
type hostedTokenResponse struct {
	AccessToken    string `json:"access_token"`
	OrganizationID string `json:"organization_id"`
	Environment    string `json:"environment"`
}

// commitHostedLoginContext deterministically replaces the authenticated CLI
// context after a successful hosted login. Values present in the new login
// overwrite previous context; values absent from the new login remove stale
// context so a prior organization or environment can never leak into the new
// session. Nothing is written when the exchange yielded no access token.
func commitHostedLoginContext(tokenResp hostedTokenResponse, customOrg, customEnv string) (orgID, envName string, err error) {
	if tokenResp.AccessToken == "" {
		return "", "", fmt.Errorf("hosted login did not return an access token")
	}
	orgID = tokenResp.OrganizationID
	if customOrg != "" {
		orgID = customOrg
	}
	envName = tokenResp.Environment
	if customEnv != "" {
		envName = customEnv
	}
	if err := StoreCredential("deadbolt", "api_key", tokenResp.AccessToken); err != nil {
		return "", "", err
	}
	if orgID != "" {
		if err := StoreCredential("deadbolt", "org_id", orgID); err != nil {
			return "", "", err
		}
	} else if err := DeleteCredential("deadbolt", "org_id"); err != nil {
		return "", "", err
	}
	if envName != "" {
		if err := StoreCredential("deadbolt", "env", envName); err != nil {
			return "", "", err
		}
	} else if err := DeleteCredential("deadbolt", "env"); err != nil {
		return "", "", err
	}
	return orgID, envName, nil
}

func openBrowser(targetURL string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", targetURL)
	case "windows":
		cmd = exec.Command("cmd", "/c", "start", targetURL)
	default:
		cmd = exec.Command("xdg-open", targetURL)
	}
	return cmd.Start()
}
