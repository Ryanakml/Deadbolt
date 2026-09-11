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

// FixtureServer implements an in-memory OIDC provider for integration tests
type FixtureServer struct {
	server     *httptest.Server
	privateKey *rsa.PrivateKey
	keyID      string
	clientID   string
	mu         sync.Mutex
	codes      map[string]TokenClaimOverrides
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
		codes:      make(map[string]TokenClaimOverrides),
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

// ClientID returns the configured client ID
func (fs *FixtureServer) ClientID() string {
	return fs.clientID
}

// RegisterAuthCode registers a pre-seeded auth code with expected claims for exchange
func (fs *FixtureServer) RegisterAuthCode(code string, claims TokenClaimOverrides) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.codes[code] = claims
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
	redirectURI := r.URL.Query().Get("redirect_uri")
	state := r.URL.Query().Get("state")
	nonce := r.URL.Query().Get("nonce")

	code := "fixture-auth-code-12345"
	fs.RegisterAuthCode(code, TokenClaimOverrides{
		Subject: "usr-fixture-sub-001",
		Email:   "tester@deadbolt.local",
		Name:    "Deadbolt Test User",
		Nonce:   nonce,
		Expiry:  1 * time.Hour,
	})

	target := fmt.Sprintf("%s?code=%s&state=%s", redirectURI, code, state)
	http.Redirect(w, r, target, http.StatusFound)
}

func (fs *FixtureServer) handleToken(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	code := r.Form.Get("code")
	fs.mu.Lock()
	claims, found := fs.codes[code]
	delete(fs.codes, code)
	fs.mu.Unlock()

	if !found {
		// Default claims if not explicitly pre-registered
		claims = TokenClaimOverrides{
			Subject: "usr-fixture-sub-default",
			Email:   "tester@deadbolt.local",
			Name:    "Deadbolt Test User",
			Expiry:  1 * time.Hour,
		}
	}

	idToken, err := fs.SignIDToken(fs.server.URL, fs.clientID, claims.Subject, claims.Email, claims.Name, claims.Nonce, claims.Expiry)
	if err != nil {
		http.Error(w, "signing error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
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
