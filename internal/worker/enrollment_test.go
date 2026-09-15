package worker_test

import (
	"crypto/ed25519"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/worker"
)

func TestEnrollmentTokenGenerationAndHashing(t *testing.T) {
	rawToken, tokenHash, err := worker.GenerateEnrollmentToken()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.HasPrefix(rawToken, "dbt_") {
		t.Errorf("expected token prefix dbt_, got: %s", rawToken)
	}

	if len(tokenHash) != 64 {
		t.Errorf("expected 64-char sha256 hex digest, got length %d: %s", len(tokenHash), tokenHash)
	}

	expectedHash := worker.HashToken(rawToken)
	if tokenHash != expectedHash {
		t.Errorf("hash mismatch: got %s, expected %s", tokenHash, expectedHash)
	}
}

func TestWorkerKeypairGenerationAndSerialization(t *testing.T) {
	pub, priv, err := worker.GenerateWorkerKeyPair()
	if err != nil {
		t.Fatalf("generate keypair error: %v", err)
	}

	if len(pub) != ed25519.PublicKeySize {
		t.Errorf("expected pub key size %d, got %d", ed25519.PublicKeySize, len(pub))
	}
	if len(priv) != ed25519.PrivateKeySize {
		t.Errorf("expected priv key size %d, got %d", ed25519.PrivateKeySize, len(priv))
	}

	encoded := worker.EncodePublicKey(pub)
	decoded, err := worker.DecodePublicKey(encoded)
	if err != nil {
		t.Fatalf("decode pub key error: %v", err)
	}

	if !pub.Equal(decoded) {
		t.Errorf("decoded pub key does not equal original")
	}

	// Invalid pub key
	if _, err := worker.DecodePublicKey("invalid_hex"); err == nil {
		t.Errorf("expected error on invalid hex")
	}
	if _, err := worker.DecodePublicKey("deadbeef"); err == nil {
		t.Errorf("expected error on truncated pub key")
	}
}

func TestPrivateKeyFilePersistenceAnd0600Permissions(t *testing.T) {
	tmpDir := t.TempDir()
	keyPath := filepath.Join(tmpDir, "worker.key")

	_, priv, err := worker.GenerateWorkerKeyPair()
	if err != nil {
		t.Fatalf("generate keypair error: %v", err)
	}

	if err := worker.SavePrivateKey(keyPath, priv); err != nil {
		t.Fatalf("save private key error: %v", err)
	}

	fi, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("stat key file error: %v", err)
	}

	if runtime.GOOS != "windows" {
		perm := fi.Mode().Perm()
		if perm != 0o600 {
			t.Errorf("expected file mode 0600, got: %04o", perm)
		}
	}

	loaded, err := worker.LoadPrivateKey(keyPath)
	if err != nil {
		t.Fatalf("load private key error: %v", err)
	}

	if !priv.Equal(loaded) {
		t.Errorf("loaded private key does not equal original")
	}

	// Insecure permissions test on POSIX
	if runtime.GOOS != "windows" {
		_ = os.Chmod(keyPath, 0o666)
		_, err := worker.LoadPrivateKey(keyPath)
		if err == nil {
			t.Errorf("expected error when loading key with insecure 0666 permissions")
		}
	}
}

func TestChallengeNonceSigningAndVerification(t *testing.T) {
	pub, priv, err := worker.GenerateWorkerKeyPair()
	if err != nil {
		t.Fatalf("generate key error: %v", err)
	}

	nonce, err := worker.GenerateChallengeNonce()
	if err != nil {
		t.Fatalf("generate nonce error: %v", err)
	}

	sig := worker.SignChallenge(priv, nonce)
	if len(sig) == 0 {
		t.Fatalf("expected non-empty signature")
	}

	if !worker.VerifyChallengeSignature(pub, nonce, sig) {
		t.Errorf("signature verification failed for valid key and nonce")
	}

	// Tampered nonce
	if worker.VerifyChallengeSignature(pub, nonce+"tampered", sig) {
		t.Errorf("signature verification must fail on tampered nonce")
	}

	// Wrong key
	pub2, _, _ := worker.GenerateWorkerKeyPair()
	if worker.VerifyChallengeSignature(pub2, nonce, sig) {
		t.Errorf("signature verification must fail on different public key")
	}
}
