package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWatchFingerprintTracksSourceAndIgnoresGeneratedArtifacts(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "workflow.json"), []byte(`{"name":"demo"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	first, err := watchFingerprint(root)
	if err != nil {
		t.Fatal(err)
	}
	if first == "" {
		t.Fatal("expected an initial fingerprint")
	}

	if err := os.MkdirAll(filepath.Join(root, "bundles"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "bundles", "old.tar"), []byte("generated"), 0o644); err != nil {
		t.Fatal(err)
	}
	second, err := watchFingerprint(root)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("generated bundle changes must not trigger a new source deployment")
	}

	if err := os.WriteFile(filepath.Join(root, "tasks.js"), []byte("export const run = 1"), 0o644); err != nil {
		t.Fatal(err)
	}
	third, err := watchFingerprint(root)
	if err != nil {
		t.Fatal(err)
	}
	if third == second {
		t.Fatal("source changes must trigger a new deployment fingerprint")
	}
}

func TestWatchFingerprintRejectsMissingRoot(t *testing.T) {
	_, err := watchFingerprint(filepath.Join(t.TempDir(), "missing"))
	if err == nil || !strings.Contains(err.Error(), "watch directory") {
		t.Fatalf("expected walk error, got %v", err)
	}
}

func TestHandleDevResetRequiresExplicitConfirmation(t *testing.T) {
	err := HandleDevLifecycle([]string{"reset"})
	if err == nil || !strings.Contains(err.Error(), "--confirm-reset") {
		t.Fatalf("expected explicit reset confirmation error, got %v", err)
	}
}
