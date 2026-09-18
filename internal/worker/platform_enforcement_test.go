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
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/worker"
)

func writeTarBundle(t *testing.T, files map[string][]byte) (string, string) {
	t.Helper()
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		content := files[name]
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
	return bundlePath, hex.EncodeToString(digest[:])
}

func platformTestSupervisor(t *testing.T, started *bool) *worker.ProcessSupervisor {
	t.Helper()
	s := worker.NewProcessSupervisor("definitely-not-node", "ignored")
	s.LeaseTracker = worker.NewLeaseTracker(time.Now().Add(time.Minute), 0, 0)
	s.StartAckFn = func(context.Context, string, int64) error { return nil }
	s.OnProcessStart = func(int) { *started = true }
	return s
}

func platformTestInput(bundlePath, digest, targetArch, targetOS string) *worker.TaskInput {
	return &worker.TaskInput{
		AttemptID:  "platform-verify",
		Entrypoint: "tasks/hello.js",
		Bundle: &worker.BundleSpec{
			Path:       bundlePath,
			SHA256:     digest,
			TargetArch: targetArch,
			TargetOS:   targetOS,
			Entrypoint: "tasks/hello.js",
		},
	}
}

// TestTarBundlePlatformVerification proves tar bundle platform verification
// fails closed before customer code starts: missing or malformed embedded
// metadata is rejected, the embedded target must exactly equal the assigned
// execution target, and a fully matching bundle passes preflight.
func TestTarBundlePlatformVerification(t *testing.T) {
	// Host-compatible assignment arch on any host; the opposite arch is
	// guaranteed host-incompatible, mirroring the original amd64/arm64 bug.
	compatibleArch := runtime.GOARCH
	mismatchedArch := "amd64"
	if runtime.GOARCH == "amd64" {
		mismatchedArch = "arm64"
	}
	if err := worker.VerifyArchitecture(mismatchedArch, ""); err != worker.ErrArchitectureMismatch {
		t.Fatalf("test setup: expected %q to mismatch host %s", mismatchedArch, worker.CurrentHostArchitecture())
	}

	hello := []byte("export default async function task() { return {}; }\n")

	t.Run("missing platform metadata is rejected", func(t *testing.T) {
		bundlePath, digest := writeTarBundle(t, map[string][]byte{
			"tasks/hello.js": hello,
		})
		var started bool
		_, _, err := platformTestSupervisor(t, &started).ExecuteAttempt(
			context.Background(), platformTestInput(bundlePath, digest, compatibleArch, "linux"), 1)
		if !errors.Is(err, worker.ErrNoBundlePlatform) || started {
			t.Fatalf("expected ErrNoBundlePlatform before process start, got err=%v started=%v", err, started)
		}
	})

	t.Run("malformed platform metadata is rejected", func(t *testing.T) {
		bundlePath, digest := writeTarBundle(t, map[string][]byte{
			"tasks/hello.js":          hello,
			worker.BundlePlatformPath: []byte("{not valid json"),
		})
		var started bool
		_, _, err := platformTestSupervisor(t, &started).ExecuteAttempt(
			context.Background(), platformTestInput(bundlePath, digest, compatibleArch, "linux"), 1)
		if !errors.Is(err, worker.ErrBundlePlatformInvalid) || started {
			t.Fatalf("expected ErrBundlePlatformInvalid before process start, got err=%v started=%v", err, started)
		}
	})

	t.Run("embedded arch differing from assignment arch is rejected", func(t *testing.T) {
		platformBytes, err := worker.CanonicalPlatformBytes("linux", mismatchedArch)
		if err != nil {
			t.Fatalf("CanonicalPlatformBytes: %v", err)
		}
		bundlePath, digest := writeTarBundle(t, map[string][]byte{
			"tasks/hello.js":          hello,
			worker.BundlePlatformPath: platformBytes,
		})
		var started bool
		// Assignment arch is host-compatible, so only embedded identity
		// equality can reject this bundle.
		_, _, err = platformTestSupervisor(t, &started).ExecuteAttempt(
			context.Background(), platformTestInput(bundlePath, digest, compatibleArch, "linux"), 1)
		if !errors.Is(err, worker.ErrBundlePlatformMismatch) || started {
			t.Fatalf("expected ErrBundlePlatformMismatch before process start, got err=%v started=%v", err, started)
		}
	})

	t.Run("embedded OS differing from assignment OS is rejected", func(t *testing.T) {
		// Both pairs are arch-compatible with the host, so only OS identity
		// equality rejects this bundle. Without it, host-compatibility alone
		// would permit execution.
		platformBytes, err := worker.CanonicalPlatformBytes("darwin", compatibleArch)
		if err != nil {
			t.Fatalf("CanonicalPlatformBytes: %v", err)
		}
		bundlePath, digest := writeTarBundle(t, map[string][]byte{
			"tasks/hello.js":          hello,
			worker.BundlePlatformPath: platformBytes,
		})
		var started bool
		_, _, err = platformTestSupervisor(t, &started).ExecuteAttempt(
			context.Background(), platformTestInput(bundlePath, digest, compatibleArch, "linux"), 1)
		if !errors.Is(err, worker.ErrBundlePlatformMismatch) || started {
			t.Fatalf("expected ErrBundlePlatformMismatch before process start, got err=%v started=%v", err, started)
		}
	})

	t.Run("matching embedded platform passes preflight", func(t *testing.T) {
		platformBytes, err := worker.CanonicalPlatformBytes("linux", compatibleArch)
		if err != nil {
			t.Fatalf("CanonicalPlatformBytes: %v", err)
		}
		bundlePath, digest := writeTarBundle(t, map[string][]byte{
			"tasks/hello.js":          hello,
			worker.BundlePlatformPath: platformBytes,
		})
		var started bool
		_, _, err = platformTestSupervisor(t, &started).ExecuteAttempt(
			context.Background(), platformTestInput(bundlePath, digest, compatibleArch, "linux"), 1)
		// "definitely-not-node" cannot spawn, so reaching the launch attempt
		// proves every preflight gate (digest, entrypoint, platform identity,
		// host compatibility) passed.
		if err == nil || !strings.Contains(err.Error(), "start runner") || started {
			t.Fatalf("expected preflight to pass and reach process start, got err=%v started=%v", err, started)
		}
	})
}
