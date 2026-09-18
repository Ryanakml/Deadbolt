package cli

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSelectBootstrapOrg(t *testing.T) {
	orgA := bootstrapOrg{ID: "org-a", Name: "Alpha", Role: "Owner", Status: "ACTIVE"}
	orgB := bootstrapOrg{ID: "org-b", Name: "Beta", Role: "Owner", Status: "ACTIVE"}
	suspended := bootstrapOrg{ID: "org-s", Name: "Old", Role: "Viewer", Status: "SUSPENDED"}

	// Zero memberships requires an explicit creation name.
	if _, _, err := selectBootstrapOrg(nil, "", "", ""); err == nil || !strings.Contains(err.Error(), "--org-name") {
		t.Fatalf("expected --org-name guidance for zero orgs, got %v", err)
	}
	org, created, err := selectBootstrapOrg(nil, "", "Fresh", "")
	if err != nil || !created || org.Name != "Fresh" {
		t.Fatalf("expected creation path, got %+v %v %v", org, created, err)
	}

	// Exactly one active membership is selected; suspended ones do not count.
	org, created, err = selectBootstrapOrg([]bootstrapOrg{suspended, orgA}, "", "", "")
	if err != nil || created || org.ID != "org-a" {
		t.Fatalf("expected deterministic single selection, got %+v %v %v", org, created, err)
	}

	// Several memberships require explicit selection, never a silent pick.
	if _, _, err := selectBootstrapOrg([]bootstrapOrg{orgA, orgB}, "", "", ""); err == nil || !strings.Contains(err.Error(), "--org") {
		t.Fatalf("expected explicit --org guidance for several orgs, got %v", err)
	}

	// Explicit selection matches by ID or name.
	for _, flag := range []string{"org-b", "Beta"} {
		org, created, err = selectBootstrapOrg([]bootstrapOrg{orgA, orgB}, flag, "", "")
		if err != nil || created || org.ID != "org-b" {
			t.Fatalf("expected explicit selection of org-b for flag %q, got %+v %v %v", flag, org, created, err)
		}
	}
	if _, _, err := selectBootstrapOrg([]bootstrapOrg{orgA}, "nope", "", ""); err == nil {
		t.Fatal("expected error for unknown --org, got nil")
	}
}

// stubBootstrapServer serves canned tenant discovery/creation endpoints and
// records mutation order.
func stubBootstrapServer(t *testing.T, createdIDs map[string]string, mutations *[]string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/organizations", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"organizations":[]}`))
			return
		}
		*mutations = append(*mutations, "POST organizations")
		_, _ = w.Write([]byte(`{"id":"` + createdIDs["org"] + `","name":"acme"}`))
	})
	mux.HandleFunc("/v1/projects", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"projects":[]}`))
			return
		}
		*mutations = append(*mutations, "POST projects")
		_, _ = w.Write([]byte(`{"id":"` + createdIDs["project"] + `","name":"svc"}`))
	})
	mux.HandleFunc("/v1/projects/proj-new/environments", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"environments":[]}`))
			return
		}
		*mutations = append(*mutations, "POST environments")
		_, _ = w.Write([]byte(`{"id":"` + createdIDs["env"] + `","name":"staging"}`))
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func bootstrapTestEnv(t *testing.T, server *httptest.Server) {
	t.Helper()
	t.Setenv("DEADBOLT_API_URL", server.URL)
	t.Setenv("DEADBOLT_API_KEY", "test-key")
	t.Setenv("DEADBOLT_ORG_ID", "")
	t.Setenv("DEADBOLT_ENV", "")
}

// TestHandleBootstrapWritesCoherentContext proves a clean bootstrap persists
// the full canonical selection coherently.
func TestHandleBootstrapWritesCoherentContext(t *testing.T) {
	isolatedCredentials(t)
	ids := map[string]string{"org": "org-new", "project": "proj-new", "env": "env-new"}
	var mutations []string
	server := stubBootstrapServer(t, ids, &mutations)
	bootstrapTestEnv(t, server)

	if err := HandleBootstrap([]string{"--org-name", "acme", "--project", "svc", "--env", "staging"}); err != nil {
		t.Fatalf("bootstrap failed: %v", err)
	}
	for account, want := range map[string]string{
		"org_id": "org-new", "project_id": "proj-new", "project": "svc", "env_id": "env-new", "env": "staging",
	} {
		if got := mustCredential(t, account); got != want {
			t.Fatalf("%s: got %q want %q", account, got, want)
		}
	}
}

// TestHandleBootstrapContextWriteIsAtomic proves a mid-write storage failure
// aborts bootstrap and restores all prior context instead of persisting a
// contradictory mix.
func TestHandleBootstrapContextWriteIsAtomic(t *testing.T) {
	isolatedCredentials(t)
	for account, value := range map[string]string{
		"org_id": "old-org", "project_id": "old-proj", "project": "old-svc",
		"env_id": "old-env", "env": "old-staging",
	} {
		if err := StoreCredential("deadbolt", account, value); err != nil {
			t.Fatal(err)
		}
	}
	ids := map[string]string{"org": "org-new", "project": "proj-new", "env": "env-new"}
	var mutations []string
	server := stubBootstrapServer(t, ids, &mutations)
	bootstrapTestEnv(t, server)

	var stores []string
	faulted := false
	credentialFault = func(service, account, op string) error {
		if op == "store" {
			stores = append(stores, account)
		}
		if !faulted && service == "deadbolt" && account == "env_id" && op == "store" {
			faulted = true
			return errors.New("injected env_id failure")
		}
		return nil
	}
	t.Cleanup(func() { credentialFault = nil })

	if err := HandleBootstrap([]string{"--org-name", "acme", "--project", "svc", "--env", "staging"}); err == nil {
		t.Fatal("expected bootstrap to fail, got nil")
	}
	// Deterministic order: org, project IDs/names before env identity.
	// (Rollback restores append further entries; only the primary prefix
	// is asserted here.)
	wantOrder := []string{"org_id", "project_id", "project", "env_id"}
	if len(stores) < len(wantOrder) {
		t.Fatalf("expected primary stores %v, got %v", wantOrder, stores)
	}
	for i := range wantOrder {
		if stores[i] != wantOrder[i] {
			t.Fatalf("expected primary stores %v, got %v", wantOrder, stores)
		}
	}
	for account, want := range map[string]string{
		"org_id": "old-org", "project_id": "old-proj", "project": "old-svc",
		"env_id": "old-env", "env": "old-staging",
	} {
		if got := mustCredential(t, account); got != want {
			t.Fatalf("%s not restored: got %q want %q", account, got, want)
		}
	}
}
