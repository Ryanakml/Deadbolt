package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// localBootstrapStub is an in-memory public-boundary fake for local dev
// bootstrap: dev-login, projects, environments, api-keys and worker
// enrollment token issuance. It records creation counts and enrollment paths.
type localBootstrapStub struct {
	mu             sync.Mutex
	orgID          string
	projects       map[string]string            // name -> id
	projectNames   map[string]string            // id -> name
	envs           map[string]map[string]string // projectID -> envName -> envID
	projectCreates int
	envCreates     int
	enrollPaths    []string
}

func newLocalBootstrapStub(orgID string) *localBootstrapStub {
	return &localBootstrapStub{
		orgID:        orgID,
		projects:     map[string]string{},
		projectNames: map[string]string{},
		envs:         map[string]map[string]string{},
	}
}

func (s *localBootstrapStub) projectIDFor(name string) string {
	return "proj-" + name
}

func (s *localBootstrapStub) envIDFor(projectName, envName string) string {
	return "env-" + projectName + "-" + envName
}

func (s *localBootstrapStub) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/auth/dev-login", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"mode":                   "local",
			"session_id":             "sess-1",
			"active_organization_id": s.orgID,
			"csrf_token":             "csrf-1",
		})
	})
	mux.HandleFunc("/v1/projects", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if r.Method == http.MethodGet {
			type proj struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			}
			var out []proj
			for name, id := range s.projects {
				out = append(out, proj{ID: id, Name: name})
			}
			if out == nil {
				out = []proj{}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"projects": out})
			return
		}
		var payload struct {
			Name string `json:"name"`
		}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		name := strings.TrimSpace(payload.Name)
		if name == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if id, ok := s.projects[name]; ok {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"id": id, "name": name})
			return
		}
		id := s.projectIDFor(name)
		s.projects[name] = id
		s.projectNames[id] = name
		if _, ok := s.envs[id]; !ok {
			s.envs[id] = map[string]string{}
		}
		s.projectCreates++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"id": id, "name": name})
	})
	mux.HandleFunc("/v1/projects/", func(w http.ResponseWriter, r *http.Request) {
		// Environments list/create: /v1/projects/<id>/environments
		path := strings.TrimPrefix(r.URL.Path, "/v1/projects/")
		parts := strings.SplitN(path, "/", 2)
		if len(parts) != 2 || parts[1] != "environments" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		projectID := parts[0]
		s.mu.Lock()
		defer s.mu.Unlock()
		projectName := s.projectNames[projectID]
		if projectName == "" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":"NOT_FOUND","message":"project not found"}`))
			return
		}
		if _, ok := s.envs[projectID]; !ok {
			s.envs[projectID] = map[string]string{}
		}
		if r.Method == http.MethodGet {
			type env struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			}
			var out []env
			for name, id := range s.envs[projectID] {
				out = append(out, env{ID: id, Name: name})
			}
			if out == nil {
				out = []env{}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"environments": out})
			return
		}
		var payload struct {
			Name string `json:"name"`
		}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		name := strings.TrimSpace(payload.Name)
		if name == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if id, ok := s.envs[projectID][name]; ok {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"id": id, "name": name})
			return
		}
		id := s.envIDFor(projectName, name)
		s.envs[projectID][name] = id
		s.envCreates++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"id": id, "name": name})
	})
	mux.HandleFunc("/v1/environments/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/v1/environments/")
		if strings.HasSuffix(rest, "/api-keys") && r.Method == http.MethodPost {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"key": "db_development_localtestkey123"})
			return
		}
		if strings.HasSuffix(rest, "/worker-enrollments") && r.Method == http.MethodPost {
			s.mu.Lock()
			s.enrollPaths = append(s.enrollPaths, r.URL.Path)
			s.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{
				"token":      "enroll-token-123",
				"workerPool": "default",
				"expiresAt":  "2030-01-01T00:00:00Z",
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc("/worker/v1/challenge", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"protocolVersion": 1,
			"requestId":       "req-chal",
			"nonce":           "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
			"expiresAt":       "2030-01-01T00:00:00Z",
		})
	})
	mux.HandleFunc("/worker/v1/enroll", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"protocolVersion": 1,
			"requestId":       "req-enr",
			"workerId":        "worker-test-1",
			"environmentId":   "env-x",
			"sessionId":       "sess-w-1",
			"sessionToken":    "tok-w-1",
			"expiresAt":       "2030-01-01T00:00:00Z",
		})
	})
	return mux
}

func setupLocalBootstrapTest(t *testing.T, stub *localBootstrapStub, workspaceProject string) (server *httptest.Server, workspaceDir string) {
	t.Helper()
	isolatedCredentials(t)
	server = httptest.NewServer(stub.handler())
	t.Cleanup(server.Close)
	t.Setenv("DEADBOLT_API_URL", server.URL)
	t.Setenv("DEADBOLT_API_KEY", "")
	t.Setenv("DEADBOLT_ORG_ID", "")
	t.Setenv("DEADBOLT_ENV", "")
	t.Setenv("DEADBOLT_PROJECT", "")
	t.Setenv("DEADBOLT_CREDENTIALS_DIR", "")
	workspaceDir = t.TempDir()
	if workspaceProject != "" {
		cfgJSON := `{"project":` + strconvQuote(workspaceProject) + `,"workflow":"customer-onboarding"}`
		if err := os.WriteFile(filepath.Join(workspaceDir, "deadbolt.config.json"), []byte(cfgJSON), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(workspaceDir)
	return server, workspaceDir
}

func strconvQuote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func mustStoredContext(t *testing.T) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, account := range []string{"org_id", "project_id", "project", "env_id", "env", "api_key"} {
		v, err := GetCredential("deadbolt", account)
		if err != nil || v == "" {
			t.Fatalf("expected canonical credential %q to be stored, got %q (%v)", account, v, err)
		}
		out[account] = v
	}
	return out
}

// TestLocalBootstrapSelectsWorkspaceProject reproduces the exact packaged
// acceptance failure: workspace order-service, zero pre-existing projects.
// Bootstrap must create order-service/development and persist canonical IDs so
// automatic worker enrollment with omitted --env targets the stored env.
func TestLocalBootstrapSelectsWorkspaceProject(t *testing.T) {
	stub := newLocalBootstrapStub("org-1")
	server, _ := setupLocalBootstrapTest(t, stub, "order-service")

	if err := runLocalDevLogin(Config{APIURL: server.URL}, "dev-admin@deadbolt.local", "", "development"); err != nil {
		t.Fatalf("local bootstrap failed: %v", err)
	}
	stub.mu.Lock()
	if _, ok := stub.projects["order-service"]; !ok {
		stub.mu.Unlock()
		t.Fatalf("expected server to contain project order-service, got %v", stub.projects)
	}
	if _, ok := stub.projects["default"]; ok {
		stub.mu.Unlock()
		t.Fatal("bootstrap must not create fallback project default inside a workspace")
	}
	stub.mu.Unlock()

	stored := mustStoredContext(t)
	if stored["org_id"] != "org-1" {
		t.Fatalf("org_id = %q, want org-1", stored["org_id"])
	}
	if stored["project"] != "order-service" {
		t.Fatalf("project = %q, want order-service", stored["project"])
	}
	if stored["project_id"] != "proj-order-service" {
		t.Fatalf("project_id = %q, want proj-order-service", stored["project_id"])
	}
	if stored["env"] != "development" {
		t.Fatalf("env = %q, want development", stored["env"])
	}
	if stored["env_id"] != "env-order-service-development" {
		t.Fatalf("env_id = %q, want env-order-service-development", stored["env_id"])
	}
	if stored["api_key"] == "" {
		t.Fatal("api_key must be non-empty")
	}

	// Automatic worker enrollment with omitted --env must target stored env_id
	// and must not fail with project "order-service" was not found.
	cfg := LoadConfig()
	sel, err := resolveEnvSelection(cfg, "")
	if err != nil {
		t.Fatalf("resolveEnvSelection with omitted env failed: %v", err)
	}
	if sel.EnvID != stored["env_id"] {
		t.Fatalf("omitted --env resolved to %q, want stored %q", sel.EnvID, stored["env_id"])
	}

	credDir, _ := GetCredentialsDirectory()
	keyPath := filepath.Join(credDir, "worker-a.key")
	if err := HandleWorkerEnroll([]string{
		"--key-path", keyPath,
		"--pool", "default",
		"--create-token",
		"--control-plane-url", server.URL,
	}); err != nil {
		t.Fatalf("automatic worker enrollment with omitted --env failed: %v", err)
	}
	// The failure this guards against surfaced as:
	// project "order-service" was not found in the authenticated organization.
	// Success above plus the exact-path assertion below proves it is gone.
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.enrollPaths) != 1 {
		t.Fatalf("expected 1 enrollment token request, got %v", stub.enrollPaths)
	}
	wantPath := "/v1/environments/" + stored["env_id"] + "/worker-enrollments"
	if stub.enrollPaths[0] != wantPath {
		t.Fatalf("enrollment path = %q, want %q", stub.enrollPaths[0], wantPath)
	}
}

// TestLocalBootstrapDoesNotSelectFirstProject proves an existing unrelated
// project never wins over the workspace identity.
func TestLocalBootstrapDoesNotSelectFirstProject(t *testing.T) {
	stub := newLocalBootstrapStub("org-1")
	// Pre-existing unrelated project with its own development env.
	stub.projects["some-old-project"] = "proj-some-old-project"
	stub.projectNames["proj-some-old-project"] = "some-old-project"
	stub.envs["proj-some-old-project"] = map[string]string{"development": "env-some-old-project-development"}
	server, _ := setupLocalBootstrapTest(t, stub, "order-service")

	if err := runLocalDevLogin(Config{APIURL: server.URL}, "dev-admin@deadbolt.local", "", "development"); err != nil {
		t.Fatalf("local bootstrap failed: %v", err)
	}
	stored := mustStoredContext(t)
	if stored["project"] != "order-service" {
		t.Fatalf("project = %q, want order-service (must not pick projects[0])", stored["project"])
	}
	if stored["project_id"] != "proj-order-service" {
		t.Fatalf("project_id = %q, want proj-order-service", stored["project_id"])
	}
	if stored["env_id"] != "env-order-service-development" {
		t.Fatalf("env_id = %q, want env-order-service-development", stored["env_id"])
	}
	if stored["env_id"] == "env-some-old-project-development" {
		t.Fatal("bootstrap incorrectly reused unrelated project's environment")
	}
}

// TestLocalBootstrapIdempotent proves reruns select rather than duplicate.
func TestLocalBootstrapIdempotent(t *testing.T) {
	stub := newLocalBootstrapStub("org-1")
	server, _ := setupLocalBootstrapTest(t, stub, "order-service")

	if err := runLocalDevLogin(Config{APIURL: server.URL}, "dev-admin@deadbolt.local", "", "development"); err != nil {
		t.Fatalf("first bootstrap failed: %v", err)
	}
	first := mustStoredContext(t)
	stub.mu.Lock()
	pc1, ec1 := stub.projectCreates, stub.envCreates
	stub.mu.Unlock()

	if err := runLocalDevLogin(Config{APIURL: server.URL}, "dev-admin@deadbolt.local", "", "development"); err != nil {
		t.Fatalf("second bootstrap failed: %v", err)
	}
	second := mustStoredContext(t)
	stub.mu.Lock()
	pc2, ec2 := stub.projectCreates, stub.envCreates
	stub.mu.Unlock()

	for k, v := range first {
		if second[k] != v {
			t.Fatalf("canonical %s changed across reruns: %q vs %q", k, v, second[k])
		}
	}
	if pc2 != pc1 {
		t.Fatalf("projectCreates changed on rerun: %d vs %d", pc1, pc2)
	}
	if ec2 != ec1 {
		t.Fatalf("envCreates changed on rerun: %d vs %d", pc1, ec1)
	}
}

// TestAutomaticWorkerEnrollIgnoresStaleProjectName proves stored EnvID wins
// when the local project name later mismatches.
func TestAutomaticWorkerEnrollIgnoresStaleProjectName(t *testing.T) {
	stub := newLocalBootstrapStub("org-1")
	server, workspaceDir := setupLocalBootstrapTest(t, stub, "order-service")

	if err := runLocalDevLogin(Config{APIURL: server.URL}, "dev-admin@deadbolt.local", "", "development"); err != nil {
		t.Fatalf("bootstrap failed: %v", err)
	}
	stored := mustStoredContext(t)

	// Simulate a stale/mismatched workspace name after canonical context exists.
	if err := os.WriteFile(filepath.Join(workspaceDir, "deadbolt.config.json"), []byte(`{"project":"stale-name"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := LoadConfig()
	sel, err := resolveEnvSelection(cfg, "")
	if err != nil {
		t.Fatalf("omitted --env must use stored EnvID despite stale project name: %v", err)
	}
	if sel.EnvID != stored["env_id"] {
		t.Fatalf("expected stored EnvID %q despite stale name, got %q", stored["env_id"], sel.EnvID)
	}

	credDir, _ := GetCredentialsDirectory()
	keyPath := filepath.Join(credDir, "worker-b.key")
	stub.mu.Lock()
	stub.enrollPaths = nil
	stub.mu.Unlock()
	if err := HandleWorkerEnroll([]string{
		"--key-path", keyPath,
		"--pool", "default",
		"--create-token",
		"--control-plane-url", server.URL,
	}); err != nil {
		t.Fatalf("automatic enrollment with stale project name failed: %v", err)
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.enrollPaths) != 1 || stub.enrollPaths[0] != "/v1/environments/"+stored["env_id"]+"/worker-enrollments" {
		t.Fatalf("expected enrollment against stored env, got %v", stub.enrollPaths)
	}
}

// TestLocalBootstrapFallsBackToDefaultOutsideWorkspace preserves standalone
// local login outside a workspace.
func TestLocalBootstrapFallsBackToDefaultOutsideWorkspace(t *testing.T) {
	stub := newLocalBootstrapStub("org-1")
	server, _ := setupLocalBootstrapTest(t, stub, "")

	if err := runLocalDevLogin(Config{APIURL: server.URL}, "dev-admin@deadbolt.local", "", "development"); err != nil {
		t.Fatalf("bootstrap failed: %v", err)
	}
	stored := mustStoredContext(t)
	if stored["project"] != "default" {
		t.Fatalf("outside workspace expected fallback project default, got %q", stored["project"])
	}
	if stored["project_id"] != "proj-default" {
		t.Fatalf("project_id = %q, want proj-default", stored["project_id"])
	}
}

// TestLocalBootstrapReplacesStaleOrgContext proves a new organization never
// inherits canonical project/env IDs from a previous local org.
func TestLocalBootstrapReplacesStaleOrgContext(t *testing.T) {
	stub := newLocalBootstrapStub("org-new")
	server, _ := setupLocalBootstrapTest(t, stub, "order-service")
	for account, value := range map[string]string{
		"api_key": "old-key", "org_id": "org-old",
		"project_id": "proj-old", "project": "old-proj",
		"env_id": "env-old", "env": "old-env",
	} {
		if err := StoreCredential("deadbolt", account, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := runLocalDevLogin(Config{APIURL: server.URL}, "dev-admin@deadbolt.local", "", "development"); err != nil {
		t.Fatalf("bootstrap failed: %v", err)
	}
	stored := mustStoredContext(t)
	if stored["org_id"] != "org-new" {
		t.Fatalf("org_id = %q, want org-new", stored["org_id"])
	}
	if stored["project_id"] == "proj-old" || stored["env_id"] == "env-old" {
		t.Fatalf("stale org context survived: %+v", stored)
	}
	if stored["project"] != "order-service" || stored["env_id"] != "env-order-service-development" {
		t.Fatalf("unexpected canonical context: %+v", stored)
	}
}

// TestLocalBootstrapAtomicPersistence proves a mid-commit storage failure
// fails closed and restores prior context instead of partial success.
func TestLocalBootstrapAtomicPersistence(t *testing.T) {
	stub := newLocalBootstrapStub("org-1")
	server, _ := setupLocalBootstrapTest(t, stub, "order-service")
	for account, value := range map[string]string{
		"api_key": "old-key", "org_id": "old-org",
		"project_id": "old-proj", "project": "old-svc",
		"env_id": "old-env", "env": "old-staging",
	} {
		if err := StoreCredential("deadbolt", account, value); err != nil {
			t.Fatal(err)
		}
	}
	faultCredentialOps(t, "deadbolt/env_id/store")
	if err := runLocalDevLogin(Config{APIURL: server.URL}, "dev-admin@deadbolt.local", "", "development"); err == nil {
		t.Fatal("expected bootstrap to fail on injected store failure, got nil")
	}
	for account, want := range map[string]string{
		"api_key": "old-key", "org_id": "old-org",
		"project_id": "old-proj", "project": "old-svc",
		"env_id": "old-env", "env": "old-staging",
	} {
		v, err := GetCredential("deadbolt", account)
		if err != nil || v != want {
			t.Fatalf("%s not restored: got %q (%v), want %q", account, v, err, want)
		}
	}
}
