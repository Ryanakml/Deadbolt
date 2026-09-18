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

func TestIsUUIDString(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want bool
	}{
		{"9c37261a-0d2d-4627-af63-2e69f4cdddfb", true},
		{"AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA", true},
		{"staging", false},
		{"", false},
		{"9c37261a-0d2d-4627-af63-2e69f4cdddf", false},
		{"9c37261a_0d2d_4627_af63_2e69f4cdddfb", false},
		{"zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz", false},
	} {
		if got := isUUIDString(tc.in); got != tc.want {
			t.Errorf("isUUIDString(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

type stubDiscoveryProject struct {
	ID   string
	Name string
	Envs []bootstrapEnvRef
}

// stubEnvDiscovery serves canned projects/environments and counts requests.
func stubEnvDiscovery(t *testing.T, projects []stubDiscoveryProject) (*httptest.Server, *int) {
	t.Helper()
	calls := new(int)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects", func(w http.ResponseWriter, r *http.Request) {
		*calls++
		type proj struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		}
		var out []proj
		for _, p := range projects {
			out = append(out, proj{ID: p.ID, Name: p.Name})
		}
		if out == nil {
			out = []proj{}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"projects": out})
	})
	mux.HandleFunc("/v1/projects/", func(w http.ResponseWriter, r *http.Request) {
		*calls++
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/projects/"), "/environments")
		for _, p := range projects {
			if p.ID == id {
				envs := p.Envs
				if envs == nil {
					envs = []bootstrapEnvRef{}
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"environments": envs})
				return
			}
		}
		w.WriteHeader(http.StatusNotFound)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server, calls
}

func twoProjectFixture() []stubDiscoveryProject {
	return []stubDiscoveryProject{
		{ID: "proj-a", Name: "project-a", Envs: []bootstrapEnvRef{{ID: "env-a", Name: "staging"}}},
		{ID: "proj-b", Name: "project-b", Envs: []bootstrapEnvRef{{ID: "env-b", Name: "staging"}}},
	}
}

func TestResolveEnvSelectionPrecedence(t *testing.T) {
	t.Setenv("DEADBOLT_PROJECT", "")
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "deadbolt.config.json"), []byte(`{"project":"project-a"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)

	newCfg := func(server *httptest.Server) Config {
		return Config{APIURL: server.URL, APIKey: "x"}
	}

	t.Run("explicit UUID is exact without listing", func(t *testing.T) {
		server, calls := stubEnvDiscovery(t, twoProjectFixture())
		sel, err := resolveEnvSelection(newCfg(server), "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
		if err != nil || sel.EnvID != "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" {
			t.Fatalf("got %+v (%v)", sel, err)
		}
		if *calls != 0 {
			t.Fatalf("UUID must not trigger discovery, got %d calls", *calls)
		}
	})

	t.Run("stored UUID wins without listing", func(t *testing.T) {
		server, calls := stubEnvDiscovery(t, twoProjectFixture())
		cfg := newCfg(server)
		cfg.EnvID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
		cfg.Env = "staging"
		sel, err := resolveEnvSelection(cfg, "")
		if err != nil || sel.EnvID != cfg.EnvID {
			t.Fatalf("got %+v (%v)", sel, err)
		}
		if *calls != 0 {
			t.Fatalf("stored UUID must not trigger discovery, got %d calls", *calls)
		}
	})

	t.Run("explicit name uses config project scope", func(t *testing.T) {
		server, _ := stubEnvDiscovery(t, twoProjectFixture())
		sel, err := resolveEnvSelection(newCfg(server), "staging")
		if err != nil || sel.EnvID != "env-a" || sel.ProjectID != "proj-a" {
			t.Fatalf("got %+v (%v)", sel, err)
		}
	})

	t.Run("ambiguous name without scope fails explicitly", func(t *testing.T) {
		server, _ := stubEnvDiscovery(t, []stubDiscoveryProject{
			{ID: "proj-a", Name: "project-a", Envs: []bootstrapEnvRef{{ID: "env-a", Name: "staging"}}},
			{ID: "proj-b", Name: "project-b", Envs: []bootstrapEnvRef{{ID: "env-b", Name: "staging"}}},
		})
		// Hide the config-file scope for this subtest.
		empty := t.TempDir()
		t.Chdir(empty)
		_, err := resolveEnvSelection(newCfg(server), "staging")
		if err == nil || !strings.Contains(err.Error(), "matches 2 environments") {
			t.Fatalf("expected explicit ambiguity error, got %v", err)
		}
	})

	t.Run("unknown name fails closed", func(t *testing.T) {
		server, _ := stubEnvDiscovery(t, twoProjectFixture())
		if _, err := resolveEnvSelection(newCfg(server), "nope"); err == nil {
			t.Fatal("expected not-found error, got nil")
		}
	})
}
