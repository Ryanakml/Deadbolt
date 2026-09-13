package tenant

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrInvalidKeyFormat   = errors.New("INVALID_KEY_FORMAT: Invalid API key format")
	ErrUnrecognizedPrefix = errors.New("UNRECOGNIZED_PREFIX: Could not extract prefix from API key")
)

// GenerateAPIKeyMaterial produces >= 256 bits of entropy, generates prefix, plaintext key, and SHA-256 hash.
// Plaintext format: <prefix>_<secret_b64>
// Prefix format: db_<env>_<8hex>
func GenerateAPIKeyMaterial(env string) (prefix, plaintextKey, hashedSecret string, err error) {
	// 1. Generate 32 bytes (256 bits) of cryptographic entropy for the secret
	secretBytes := make([]byte, 32)
	if _, err := rand.Read(secretBytes); err != nil {
		return "", "", "", fmt.Errorf("failed to generate random secret: %w", err)
	}

	// 2. Generate 4 bytes (8 hex chars) for unique prefix disambiguation
	prefixBytes := make([]byte, 4)
	if _, err := rand.Read(prefixBytes); err != nil {
		return "", "", "", fmt.Errorf("failed to generate random prefix: %w", err)
	}

	prefixHex := hex.EncodeToString(prefixBytes)
	prefix = fmt.Sprintf("db_%s_%s", env, prefixHex)

	// 3. Construct plaintext token: prefix + "_" + base64url(secret)
	secretPart := base64.RawURLEncoding.EncodeToString(secretBytes)
	plaintextKey = fmt.Sprintf("%s_%s", prefix, secretPart)

	// 4. Compute SHA-256 hash for database storage
	hashedSecret = HashAPIKey(plaintextKey)

	return prefix, plaintextKey, hashedSecret, nil
}

// HashAPIKey computes the hex-encoded SHA-256 hash of a plaintext API key.
func HashAPIKey(plaintextKey string) string {
	h := sha256.Sum256([]byte(plaintextKey))
	return hex.EncodeToString(h[:])
}

// VerifyAPIKey verifies a plaintext API key against an expected SHA-256 hash in constant time.
func VerifyAPIKey(plaintextKey, expectedHash string) bool {
	candidateHash := HashAPIKey(plaintextKey)
	return subtle.ConstantTimeCompare([]byte(candidateHash), []byte(expectedHash)) == 1
}

// ExtractPrefix parses the prefix from a plaintext API key.
// Expected format: db_<env>_<8hex>_<secret> -> db_<env>_<8hex>
func ExtractPrefix(plaintextKey string) (string, error) {
	parts := strings.Split(plaintextKey, "_")
	// Format: ["db", "<env>", "<8hex>", "<secret>"]
	if len(parts) < 4 || parts[0] != "db" {
		return "", ErrInvalidKeyFormat
	}
	prefix := fmt.Sprintf("%s_%s_%s", parts[0], parts[1], parts[2])
	return prefix, nil
}

// ValidCanonicalCapabilities contains all permitted capability strings per Blueprint §24.2.
var ValidCanonicalCapabilities = map[string]struct{}{
	CapRunCreate:             {},
	CapRunRead:               {},
	CapRunControl:            {},
	CapPayloadRead:           {},
	CapDeployRegister:        {},
	CapDeployActivateStaging: {},
	CapDeployActivateProd:    {},
	CapWorkerDrain:           {},
	CapApprovalDecide:        {},
	CapReconcileResolve:      {},
	CapOrgRead:               {},
	CapOrgUpdate:             {},
	CapOrgDelete:             {},
	CapAdminMember:           {},
	CapAdminKey:              {},
	CapAdminProject:          {},
}

// ValidateKeyCapabilities checks that:
// 1. All capabilities are recognized canonical capabilities.
// 2. Machine keys are not granted human approval or reconciliation decisions (Blueprint §24.2).
func ValidateKeyCapabilities(caps []string, isMachineKey bool) error {
	for _, c := range caps {
		if _, ok := ValidCanonicalCapabilities[c]; !ok {
			return fmt.Errorf("%w: unknown capability %q", ErrForbidden, c)
		}
		if isMachineKey {
			if c == CapApprovalDecide || c == CapReconcileResolve {
				return ErrMachineKeyRestricted
			}
		}
	}
	return nil
}

// DummyHash is a fixed 64-character SHA-256 hex string used for constant-time comparison on nonexistent keys.
const DummyHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// SanitizeCapabilities deduplicates and trims capabilities.
func SanitizeCapabilities(caps []string) []string {
	seen := make(map[string]struct{}, len(caps))
	var result []string
	for _, c := range caps {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		if _, exists := seen[c]; !exists {
			seen[c] = struct{}{}
			result = append(result, c)
		}
	}
	return result
}

// DefaultExpiryDuration returns the default 90-day expiry duration per Blueprint §24.4.
const DefaultExpiryDays = 90

func CalculateExpiry(days int) *time.Time {
	if days <= 0 {
		days = DefaultExpiryDays
	}
	t := time.Now().UTC().AddDate(0, 0, days)
	return &t
}
