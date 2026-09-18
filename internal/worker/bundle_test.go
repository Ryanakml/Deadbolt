package worker_test

import (
	"archive/tar"
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

	// 4. Bare architecture aliases
	if err := worker.VerifyArchitecture("arm64", "darwin/arm64"); err != nil {
		t.Fatalf("expected bare 'arm64' to be accepted on darwin/arm64: %v", err)
	}
	if err := worker.VerifyArchitecture("amd64", "darwin/arm64"); err != worker.ErrArchitectureMismatch {
		t.Fatalf("expected bare 'amd64' to be rejected on darwin/arm64, got %v", err)
	}
}

func TestBundlePlatformMetadataSerializationAndExtraction(t *testing.T) {
	// Canonical bytes are deterministic
	b1, err := worker.CanonicalPlatformBytes("linux", "arm64")
	if err != nil {
		t.Fatalf("CanonicalPlatformBytes failed: %v", err)
	}
	b2, err := worker.CanonicalPlatformBytes("linux", "arm64")
	if err != nil {
		t.Fatalf("CanonicalPlatformBytes failed: %v", err)
	}
	if !bytes.Equal(b1, b2) {
		t.Fatalf("CanonicalPlatformBytes is not deterministic")
	}

	// Pack in a mock tar archive
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	hdr := &tar.Header{
		Name:     worker.BundlePlatformPath,
		Mode:     0o644,
		Size:     int64(len(b1)),
		Typeflag: tar.TypeReg,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("write tar header: %v", err)
	}
	if _, err := tw.Write(b1); err != nil {
		t.Fatalf("write tar body: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}

	// Read back using ReadBundlePlatform
	plat, err := worker.ReadBundlePlatform(bytes.NewReader(tarBuf.Bytes()))
	if err != nil {
		t.Fatalf("ReadBundlePlatform failed: %v", err)
	}
	if plat.TargetArchitecture != "arm64" || plat.TargetOS != "linux" {
		t.Fatalf("unexpected extracted platform: %+v", plat)
	}

	// Missing platform in empty tar returns ErrNoBundlePlatform
	var emptyBuf bytes.Buffer
	twEmpty := tar.NewWriter(&emptyBuf)
	twEmpty.Close()
	_, err = worker.ReadBundlePlatform(bytes.NewReader(emptyBuf.Bytes()))
	if err != worker.ErrNoBundlePlatform {
		t.Fatalf("expected ErrNoBundlePlatform, got: %v", err)
	}
}
