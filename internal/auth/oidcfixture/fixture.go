package oidcfixture

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"
)

// AuthCodeRecord holds state for a issued one-time authorization code
type AuthCodeRecord struct {
	Code                string
	ClientID            string
	RedirectURI         string
	CodeChallenge       string
	CodeChallengeMethod string
	Claims              TokenClaimOverrides
	Used                bool
	ExpiresAt           time.Time
}

// FixtureServer implements an in-memory OIDC provider for integration tests
type FixtureServer struct {
	server     *httptest.Server
	privateKey *rsa.PrivateKey
	keyID      string
	clientID   string
	mu         sync.Mutex
	codes      map[string]*AuthCodeRecord
}

// TokenClaimOverrides allows overriding claims during code exchange
type TokenClaimOverrides struct {
	Subject string
	Email   string
	Name    string
	Nonce   string
	Expiry  time.Duration
}

// NewFixtureServer spins up a local HTTP test server mimicking an OIDC provider
func NewFixtureServer(clientID string) (*FixtureServer, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("failed to generate fixture RSA key: %w", err)
	}

	fs := &FixtureServer{
		privateKey: key,
		keyID:      "fixture-key-1",
		clientID:   clientID,
		codes:      make(map[string]*AuthCodeRecord),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", fs.handleDiscovery)
	mux.HandleFunc("/jwks.json", fs.handleJWKS)
	mux.HandleFunc("/token", fs.handleToken)
	mux.HandleFunc("/authorize", fs.handleAuthorize)

	fs.server = httptest.NewServer(mux)
	return fs, nil
}

// Close shuts down the test server
func (fs *FixtureServer) Close() {
	if fs.server != nil {
		fs.server.Close()
	}
}

// URL returns the base issuer URL
func (fs *FixtureServer) URL() string {
	return fs.server.URL
}

// Client returns the http.Client configured for the test server
func (fs *FixtureServer) Client() *http.Client {
	return fs.server.Client()
}

// ClientID returns the configured client ID
func (fs *FixtureServer) ClientID() string {
	return fs.clientID
}

// RegisterAuthCode registers a pre-seeded auth code with expected claims for exchange
func (fs *FixtureServer) RegisterAuthCode(code string, claims TokenClaimOverrides) {
	fs.RegisterAuthCodeWithPKCE(code, claims, "", "")
}

// RegisterAuthCodeWithPKCE registers an auth code with explicit PKCE code challenge
func (fs *FixtureServer) RegisterAuthCodeWithPKCE(code string, claims TokenClaimOverrides, challenge, method string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.codes[code] = &AuthCodeRecord{
		Code:                code,
		ClientID:            fs.clientID,
		CodeChallenge:       challenge,
		CodeChallengeMethod: method,
		Claims:              claims,
		Used:                false,
		ExpiresAt:           time.Now().Add(10 * time.Minute),
	}
}

func (fs *FixtureServer) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"issuer":                 fs.server.URL,
		"authorization_endpoint": fs.server.URL + "/authorize",
		"token_endpoint":         fs.server.URL + "/token",
		"jwks_uri":               fs.server.URL + "/jwks.json",
	})
}

func (fs *FixtureServer) handleJWKS(w http.ResponseWriter, r *http.Request) {
	nBytes := fs.privateKey.PublicKey.N.Bytes()
	eBytes := big.NewInt(int64(fs.privateKey.PublicKey.E)).Bytes()

	nB64 := base64.RawURLEncoding.EncodeToString(nBytes)
	eB64 := base64.RawURLEncoding.EncodeToString(eBytes)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"keys": []map[string]interface{}{
			{
				"kty": "RSA",
				"alg": "RS256",
				"use": "sig",
				"kid": fs.keyID,
				"n":   nB64,
				"e":   eB64,
			},
		},
	})
}

func (fs *FixtureServer) handleAuthorize(w http.ResponseWriter, r *http.Request) {
	clientID := r.URL.Query().Get("client_id")
	redirectURI := r.URL.Query().Get("redirect_uri")
	state := r.URL.Query().Get("state")
	nonce := r.URL.Query().Get("nonce")
	codeChallenge := r.URL.Query().Get("code_challenge")
	codeChallengeMethod := r.URL.Query().Get("code_challenge_method")

	if clientID != fs.clientID {
		http.Error(w, "unauthorized_client", http.StatusBadRequest)
		return
	}
	if redirectURI == "" {
		http.Error(w, "invalid_request: missing redirect_uri", http.StatusBadRequest)
		return
	}
	if codeChallenge == "" || codeChallengeMethod != "S256" {
		http.Error(w, "invalid_request: RFC 7636 PKCE S256 challenge required", http.StatusBadRequest)
		return
	}

	// Generate random one-time auth code
	codeBytes := make([]byte, 16)
	_, _ = rand.Read(codeBytes)
	code := fmt.Sprintf("fixture-auth-code-%x", codeBytes)

	fs.mu.Lock()
	fs.codes[code] = &AuthCodeRecord{
		Code:                code,
		ClientID:            clientID,
		RedirectURI:         redirectURI,
		CodeChallenge:       codeChallenge,
		CodeChallengeMethod: codeChallengeMethod,
		Claims: TokenClaimOverrides{
			Subject: "usr-fixture-sub-001",
			Email:   "tester@deadbolt.local",
			Name:    "Deadbolt Test User",
			Nonce:   nonce,
			Expiry:  1 * time.Hour,
		},
		Used:      false,
		ExpiresAt: time.Now().Add(10 * time.Minute),
	}
	fs.mu.Unlock()

	target := fmt.Sprintf("%s?code=%s&state=%s", redirectURI, code, state)
	http.Redirect(w, r, target, http.StatusFound)
}

func (fs *FixtureServer) handleToken(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	grantType := r.Form.Get("grant_type")
	if grantType != "authorization_code" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":             "unsupported_grant_type",
			"error_description": "Only authorization_code grant is supported",
		})
		return
	}

	code := r.Form.Get("code")
	fs.mu.Lock()
	record, found := fs.codes[code]
	var alreadyUsed bool
	if found {
		if record.Used {
			alreadyUsed = true
		} else {
			record.Used = true
		}
	}
	fs.mu.Unlock()

	if !found {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":             "invalid_grant",
			"error_description": "Unknown or invalid authorization code",
		})
		return
	}

	if alreadyUsed {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":             "invalid_grant",
			"error_description": "Authorization code has already been used (replay rejected)",
		})
		return
	}

	if time.Now().After(record.ExpiresAt) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":             "invalid_grant",
			"error_description": "Authorization code expired",
		})
		return
	}

	// Verify PKCE code_verifier if code_challenge was configured
	if record.CodeChallenge != "" {
		codeVerifier := r.Form.Get("code_verifier")
		if codeVerifier == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error":             "invalid_request",
				"error_description": "Missing code_verifier for PKCE-bound authorization code",
			})
			return
		}

		h := sha256.Sum256([]byte(codeVerifier))
		calculatedChallenge := base64.RawURLEncoding.EncodeToString(h[:])
		if calculatedChallenge != record.CodeChallenge {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error":             "invalid_grant",
				"error_description": "PKCE verification failed: code_verifier does not match code_challenge",
			})
			return
		}
	}

	claims := record.Claims
	if claims.Subject == "" {
		claims.Subject = "usr-fixture-sub-default"
	}
	if claims.Email == "" {
		claims.Email = "tester@deadbolt.local"
	}
	if claims.Name == "" {
		claims.Name = "Deadbolt Test User"
	}
	if claims.Expiry <= 0 {
		claims.Expiry = 1 * time.Hour
	}

	idToken, err := fs.SignIDToken(fs.server.URL, fs.clientID, claims.Subject, claims.Email, claims.Name, claims.Nonce, claims.Expiry)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "signing_error",
		})
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"id_token":     idToken,
		"access_token": "fixture-access-token",
		"token_type":   "Bearer",
		"expires_in":   3600,
	})
}

// SignIDToken signs a JWT with custom parameters using the fixture's RSA private key
func (fs *FixtureServer) SignIDToken(issuer, audience, subject, email, name, nonce string, expiry time.Duration) (string, error) {
	now := time.Now()
	exp := now.Add(expiry)

	header := map[string]interface{}{
		"alg": "RS256",
		"typ": "JWT",
		"kid": fs.keyID,
	}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	headerB64 := base64.RawURLEncoding.EncodeToString(headerJSON)

	payload := map[string]interface{}{
		"iss":   issuer,
		"sub":   subject,
		"aud":   audience,
		"exp":   exp.Unix(),
		"iat":   now.Unix(),
		"email": email,
		"name":  name,
	}
	if nonce != "" {
		payload["nonce"] = nonce
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadJSON)

	signedContent := headerB64 + "." + payloadB64
	hasher := sha256.New()
	hasher.Write([]byte(signedContent))
	hashed := hasher.Sum(nil)

	sigBytes, err := rsa.SignPKCS1v15(rand.Reader, fs.privateKey, crypto.SHA256, hashed)
	if err != nil {
		return "", err
	}
	sigB64 := base64.RawURLEncoding.EncodeToString(sigBytes)

	return signedContent + "." + sigB64, nil
}
