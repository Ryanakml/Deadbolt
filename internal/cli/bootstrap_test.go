package cli

import (
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
