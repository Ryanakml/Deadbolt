package worker_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/worker"
)

// TestEmbeddedBundlePlatformRejectsIncompatibleArch proves the worker enforces
// the architecture embedded inside the bundle artifact, not just the manifest
// claim: a tar bundle whose .deadbolt/platform.json names an incompatible
// target must fail closed with WORKER_PREFLIGHT_FAILED semantics before any
// customer code can start, even when the manifest-declared TargetArch is
// host-compatible.
func TestEmbeddedBundlePlatformRejectsIncompatibleArch(t *testing.T) {
	mismatchedArch := "amd64"
	if runtime.GOARCH == "amd64" {
		mismatchedArch = "arm64"
	}
	// Sanity: the chosen arch must be rejected on this host, or the test
	// would be vacuous.
	if err := worker.VerifyArchitecture(mismatchedArch, ""); err != worker.ErrArchitectureMismatch {
		t.Fatalf("test setup: expected %q to mismatch host %s", mismatchedArch, worker.CurrentHostArchitecture())
	}

	platformBytes, err := worker.CanonicalPlatformBytes("linux", mismatchedArch)
	if err != nil {
		t.Fatalf("CanonicalPlatformBytes: %v", err)
	}

	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	for name, content := range map[string][]byte{
		"tasks/hello.js":          []byte("export default async function task() { return {}; }\n"),
		worker.BundlePlatformPath: platformBytes,
	} {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(content)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatalf("write tar header %s: %v", name, err)
		}
		if _, err := tw.Write(content); err != nil {
			t.Fatalf("write tar body %s: %v", name, err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}
	bundlePath := filepath.Join(t.TempDir(), "bundle.tar")
	if err := os.WriteFile(bundlePath, tarBuf.Bytes(), 0o600); err != nil {
		t.Fatalf("write bundle: %v", err)
	}
	digest := sha256.Sum256(tarBuf.Bytes())

	s := worker.NewProcessSupervisor("definitely-not-node", "ignored")
	s.LeaseTracker = worker.NewLeaseTracker(time.Now().Add(time.Minute), 0, 0)
	s.StartAckFn = func(context.Context, string, int64) error { return nil }
	started := false
	s.OnProcessStart = func(int) { started = true }

	input := &worker.TaskInput{
		AttemptID:  "platform-mismatch",
		Entrypoint: "tasks/hello.js",
		Bundle: &worker.BundleSpec{
			Path:       bundlePath,
			SHA256:     hex.EncodeToString(digest[:]),
			TargetArch: worker.CurrentHostArchitecture(),
			Entrypoint: "tasks/hello.js",
		},
	}
	_, _, err = s.ExecuteAttempt(context.Background(), input, 1)
	if !errors.Is(err, worker.ErrArchitectureMismatch) || started {
		t.Fatalf("expected ErrArchitectureMismatch before process start, got err=%v started=%v", err, started)
	}
}
