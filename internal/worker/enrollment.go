package worker

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"
)

type WorkerIdentity struct {
	WorkerID string `json:"workerId"`
}

func IdentityPath(keyPath string) string {
	return keyPath + ".identity"
}

func SaveWorkerIdentity(keyPath, workerID string) error {
	data, err := json.Marshal(WorkerIdentity{WorkerID: workerID})
	if err != nil {
		return err
	}
	return os.WriteFile(IdentityPath(keyPath), data, 0o600)
}

func LoadWorkerIdentity(keyPath string) (string, error) {
	data, err := os.ReadFile(IdentityPath(keyPath))
	if err != nil {
		return "", err
	}
	var identity WorkerIdentity
	if err := json.Unmarshal(data, &identity); err != nil {
		return "", fmt.Errorf("decode worker identity: %w", err)
	}
	if identity.WorkerID == "" {
		return "", fmt.Errorf("worker identity is empty")
	}
	return identity.WorkerID, nil
}

var (
	ErrInsecureKeyPermissions = errors.New("INSECURE_KEY_PERMISSIONS: Worker private key file must have 0600 permissions")
	ErrInvalidPublicKey       = errors.New("INVALID_PUBLIC_KEY: Public key is malformed or wrong size")
	ErrInvalidSignature       = errors.New("INVALID_SIGNATURE: Signature verification failed")
	ErrInvalidPrivateKey      = errors.New("INVALID_PRIVATE_KEY: Private key is malformed")
)

const (
	EnrollmentTokenTTL = 10 * time.Minute
	ChallengeNonceTTL  = 5 * time.Minute
)

// GenerateEnrollmentToken produces a 32-byte cryptographically secure random token
// and its SHA-256 hash.
func GenerateEnrollmentToken() (rawToken string, tokenHash string, err error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", "", fmt.Errorf("crypto rand: %w", err)
	}
	rawToken = "dbt_" + hex.EncodeToString(b)
	tokenHash = HashToken(rawToken)
	return rawToken, tokenHash, nil
}

// HashToken computes the SHA-256 hex digest of a token.
func HashToken(token string) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(token)))
	return hex.EncodeToString(digest[:])
}

// GenerateWorkerKeyPair creates a fresh Ed25519 keypair for a worker agent.
func GenerateWorkerKeyPair() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate ed25519 key: %w", err)
	}
	return pub, priv, nil
}

// EncodePublicKey formats an Ed25519 public key as a lowercase hex string.
func EncodePublicKey(pub ed25519.PublicKey) string {
	return hex.EncodeToString(pub)
}

// DecodePublicKey parses an Ed25519 public key from a hex string.
func DecodePublicKey(s string) (ed25519.PublicKey, error) {
	b, err := hex.DecodeString(strings.TrimSpace(s))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidPublicKey, err)
	}
	if len(b) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("%w: expected length %d, got %d", ErrInvalidPublicKey, ed25519.PublicKeySize, len(b))
	}
	return ed25519.PublicKey(b), nil
}

// SavePrivateKey writes the private key to a file with strict 0600 permissions.
func SavePrivateKey(path string, priv ed25519.PrivateKey) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open private key file: %w", err)
	}
	defer f.Close()

	if _, err := f.WriteString(hex.EncodeToString(priv) + "\n"); err != nil {
		return fmt.Errorf("write private key file: %w", err)
	}

	// On POSIX systems, explicitly chmod to guarantee 0600 regardless of umask
	if runtime.GOOS != "windows" {
		if err := f.Chmod(0o600); err != nil {
			return fmt.Errorf("chmod 0600 private key: %w", err)
		}
	}
	return nil
}

// LoadPrivateKey reads an Ed25519 private key from disk and enforces 0600 permissions.
func LoadPrivateKey(path string) (ed25519.PrivateKey, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat private key file: %w", err)
	}

	// Enforce 0600: no permissions for group or others on POSIX
	if runtime.GOOS != "windows" {
		perm := fi.Mode().Perm()
		if perm&0o077 != 0 {
			return nil, fmt.Errorf("%w: current mode is %04o", ErrInsecureKeyPermissions, perm)
		}
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read private key file: %w", err)
	}

	b, err := hex.DecodeString(strings.TrimSpace(string(data)))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidPrivateKey, err)
	}
	if len(b) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("%w: expected length %d, got %d", ErrInvalidPrivateKey, ed25519.PrivateKeySize, len(b))
	}
	return ed25519.PrivateKey(b), nil
}

// GenerateChallengeNonce generates a 32-byte cryptographically secure random nonce.
func GenerateChallengeNonce() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto rand challenge nonce: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// SignChallenge signs a challenge nonce using an Ed25519 private key.
func SignChallenge(priv ed25519.PrivateKey, nonce string) string {
	msg := []byte("deadbolt-challenge:" + strings.TrimSpace(nonce))
	sig := ed25519.Sign(priv, msg)
	return hex.EncodeToString(sig)
}

// VerifyChallengeSignature verifies an Ed25519 signature over a challenge nonce.
func VerifyChallengeSignature(pub ed25519.PublicKey, nonce string, sigHex string) bool {
	sig, err := hex.DecodeString(strings.TrimSpace(sigHex))
	if err != nil || len(sig) != ed25519.SignatureSize {
		return false
	}
	msg := []byte("deadbolt-challenge:" + strings.TrimSpace(nonce))
	return ed25519.Verify(pub, msg, sig)
}
