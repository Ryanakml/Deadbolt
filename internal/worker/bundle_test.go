package worker_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/worker"
)

func TestVerifyBundleDigest(t *testing.T) {
	content := []byte("tar-archive-mock-content-for-testing")
	hasher := sha256.New()
	hasher.Write(content)
	expectedHex := hex.EncodeToString(hasher.Sum(nil))

	// 1. Valid matching digest
	digest, err := worker.VerifyBundleDigest(bytes.NewReader(content), expectedHex)
	if err != nil {
		t.Fatalf("expected valid digest check to pass: %v", err)
	}
	if digest != "sha256:"+expectedHex {
		t.Fatalf("expected canonical prefix 'sha256:%s', got %s", expectedHex, digest)
	}

	// 2. Corrupted content mismatch
	corrupted := []byte("tampered-content")
	_, err = worker.VerifyBundleDigest(bytes.NewReader(corrupted), expectedHex)
	if err != worker.ErrBundleDigestMismatch {
		t.Fatalf("expected ErrBundleDigestMismatch, got %v", err)
	}
}

func TestVerifyArchitecture(t *testing.T) {
	// 1. Direct match
	if err := worker.VerifyArchitecture("linux/amd64", "linux/amd64"); err != nil {
		t.Fatalf("expected direct architecture match to succeed: %v", err)
	}

	// 2. Alias normalization
	if err := worker.VerifyArchitecture("linux-x64", "linux/amd64"); err != nil {
		t.Fatalf("expected alias 'linux-x64' to match 'linux/amd64': %v", err)
	}
	if err := worker.VerifyArchitecture("darwin-arm64", "darwin/arm64"); err != nil {
		t.Fatalf("expected alias 'darwin-arm64' to match 'darwin/arm64': %v", err)
	}

	// 3. Mismatch
	if err := worker.VerifyArchitecture("linux/amd64", "linux/arm64"); err != worker.ErrArchitectureMismatch {
		t.Fatalf("expected ErrArchitectureMismatch for amd64 vs arm64, got %v", err)
	}
	if err := worker.VerifyArchitecture("windows/amd64", "linux/amd64"); err != worker.ErrArchitectureMismatch {
		t.Fatalf("expected ErrArchitectureMismatch for windows vs linux, got %v", err)
	}
}
