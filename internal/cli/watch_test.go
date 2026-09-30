package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

func TestWatchActivationCarriesForwardChannelRevision(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ExpectedRevision int64 `json:"expectedRevision"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"revision": request.ExpectedRevision + 1})
	}))
	defer server.Close()

	cfg := Config{APIURL: server.URL}
	revision := int64(0)
	for i := 0; i < 3; i++ {
		var err error
		revision, err = activateDeploymentWithRevision(cfg, "deployment-v"+string(rune('1'+i)), "workflow", "environment", revision)
		if err != nil {
			t.Fatalf("activation %d failed: %v", i+1, err)
		}
		if revision != int64(i+1) {
			t.Fatalf("activation %d returned revision %d, want %d", i+1, revision, i+1)
		}
	}
}

func TestWorkerIsOperational(t *testing.T) {
	for _, test := range []struct {
		status string
		want   bool
	}{
		{status: "ACTIVE", want: true},
		{status: "ONLINE", want: true},
		{status: "DRAINING", want: false},
		{status: "OFFLINE", want: false},
		{status: "REVOKED", want: false},
	} {
		if got := workerIsOperational(test.status); got != test.want {
			t.Errorf("workerIsOperational(%q) = %t, want %t", test.status, got, test.want)
		}
	}
	if !workerSupportsDeployment("ACTIVE", []string{"bundle-v1"}, "bundle-v1") {
		t.Fatal("active worker advertising the digest must be compatible")
	}
	if workerSupportsDeployment("DRAINING", []string{"bundle-v1"}, "bundle-v1") {
		t.Fatal("draining worker must not be reported as compatible")
	}
	if workerSupportsDeployment("ACTIVE", []string{"bundle-v2"}, "bundle-v1") {
		t.Fatal("worker without the requested digest must not be compatible")
	}
}
