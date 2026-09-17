package cli

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestResolveRunnerPathPrecedence(t *testing.T) {
	stub := filepath.Join(t.TempDir(), "index.js")
	if err := os.WriteFile(stub, []byte("// stub runner"), 0o644); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(t.TempDir(), "other.js")
	if err := os.WriteFile(other, []byte("// other runner"), 0o644); err != nil {
		t.Fatal(err)
	}

	t.Setenv("DEADBOLT_RUNNER_PATH", stub)
	if got, err := resolveRunnerPath(other); err != nil || got != other {
		t.Fatalf("expected explicit flag to win, got %q (%v)", got, err)
	}
	if got, err := resolveRunnerPath(""); err != nil || got != stub {
		t.Fatalf("expected DEADBOLT_RUNNER_PATH to be used, got %q (%v)", got, err)
	}

	t.Setenv("DEADBOLT_RUNNER_PATH", filepath.Join(t.TempDir(), "missing.js"))
	if _, err := resolveRunnerPath(""); err == nil || !strings.Contains(err.Error(), "--runner-path") {
		t.Fatalf("expected actionable missing-runner error, got %v", err)
	}
}

func TestResolveDevComposePath(t *testing.T) {
	t.Setenv("DEADBOLT_COMPOSE_FILE", "")
	t.Setenv("DEADBOLT_DEV_ASSETS", "")

	custom := filepath.Join(t.TempDir(), "compose.yaml")
	if err := os.WriteFile(custom, []byte("services: {}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got, _, err := resolveDevComposePath(custom); err != nil || got != custom {
		t.Fatalf("expected explicit flag to win, got %q (%v)", got, err)
	}

	t.Setenv("DEADBOLT_COMPOSE_FILE", custom)
	if got, _, err := resolveDevComposePath(""); err != nil || got != custom {
		t.Fatalf("expected DEADBOLT_COMPOSE_FILE to be honored, got %q (%v)", got, err)
	}

	// Without any input and without the repository opt-in, resolution must
	// fail fast with an actionable error instead of silently falling back
	// into source-checkout paths.
	t.Setenv("DEADBOLT_COMPOSE_FILE", "")
	if _, _, err := resolveDevComposePath(""); err == nil || !strings.Contains(err.Error(), "not installed") {
		t.Fatalf("expected actionable missing-assets error, got %v", err)
	}
}

// TestPackagedWorkerStartDiscoveryOutsideRepo proves the installed layout
// works with no repository present: a real `runtime` binary executed from a
// directory outside the repo resolves companion assets relative to the
// executable, fails fast when they are absent, and proceeds past discovery
// once the packaged layout exists. It never touches the network: identity
// bootstrap fails locally first.
func TestPackagedWorkerStartDiscoveryOutsideRepo(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not available to build the packaged binary")
	}
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}

	outside := t.TempDir()
	prefix := t.TempDir()
	binPath := filepath.Join(prefix, "bin", "runtime")
	if err := os.MkdirAll(filepath.Dir(binPath), 0o755); err != nil {
		t.Fatal(err)
	}
	build := exec.Command("go", "build", "-o", binPath, "./cmd/runtime")
	build.Dir = repoRoot
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build packaged runtime: %v\n%s", err, out)
	}

	run := func(args ...string) (string, int) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, binPath, args...)
		cmd.Dir = outside
		home := t.TempDir()
		creds := t.TempDir()
		cmd.Env = append(os.Environ(),
			"HOME="+home,
			"DEADBOLT_RUNNER_PATH=",
			"DEADBOLT_CREDENTIALS_DIR="+creds,
			"DEADBOLT_COMPOSE_FILE=",
			"DEADBOLT_DEV_ASSETS=",
		)
		out, err := cmd.CombinedOutput()
		if ctx.Err() == context.DeadlineExceeded {
			t.Fatalf("packaged runtime hung; output:\n%s", out)
		}
		code := 0
		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				code = exitErr.ExitCode()
			} else {
				t.Fatalf("run packaged runtime: %v", err)
			}
		}
		return string(out), code
	}
	workerArgs := []string{"worker", "start", "--key-path", filepath.Join(outside, "missing.key"), "--control-plane-url", "http://127.0.0.1:9/", "--bundle-dir", filepath.Join(outside, "bundles")}

	// 1. No companion assets: fail fast with the actionable runner message.
	out, code := run(workerArgs...)
	if code == 0 || !strings.Contains(out, "Node runner is not installed") {
		t.Fatalf("expected fail-fast missing-runner error, got code=%d output:\n%s", code, out)
	}

	// 2. Packaged layout present: discovery passes, failure moves past it.
	runnerDir := filepath.Join(prefix, "share", "deadbolt", "runner")
	if err := os.MkdirAll(runnerDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runnerDir, "index.js"), []byte("// packaged stub runner"), 0o644); err != nil {
		t.Fatal(err)
	}
	out, code = run(workerArgs...)
	if code == 0 {
		t.Fatalf("expected worker start to fail without identity, got success:\n%s", out)
	}
	if strings.Contains(out, "Node runner is not installed") {
		t.Fatalf("runner discovery should have passed with packaged layout:\n%s", out)
	}
	if !strings.Contains(out, "ENROLLMENT_TOKEN_REQUIRED") {
		t.Fatalf("expected local identity bootstrap failure after discovery, got:\n%s", out)
	}
}
