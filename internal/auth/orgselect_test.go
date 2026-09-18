package auth

import (
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/storage"
)

func TestSelectDefaultOrganization(t *testing.T) {
	orgA := storage.UserMembership{OrganizationID: "org-a", OrganizationName: "Alpha", Role: "Owner", Status: "ACTIVE"}
	orgB := storage.UserMembership{OrganizationID: "org-b", OrganizationName: "Beta", Role: "Viewer", Status: "ACTIVE"}

	if got := SelectDefaultOrganization(nil); got != nil {
		t.Fatalf("zero memberships must yield nil, got %q", *got)
	}
	if got := SelectDefaultOrganization([]storage.UserMembership{}); got != nil {
		t.Fatalf("empty memberships must yield nil, got %q", *got)
	}
	if got := SelectDefaultOrganization([]storage.UserMembership{orgA}); got == nil || *got != "org-a" {
		t.Fatalf("exactly one active membership must be selected, got %v", got)
	}
	if got := SelectDefaultOrganization([]storage.UserMembership{orgA, orgB}); got != nil {
		t.Fatalf("several memberships must yield nil for explicit selection, got %q", *got)
	}
	suspendedOnly := storage.UserMembership{OrganizationID: "org-s", OrganizationName: "Old", Role: "Viewer", Status: "SUSPENDED"}
	if got := SelectDefaultOrganization([]storage.UserMembership{suspendedOnly}); got != nil {
		t.Fatalf("suspended-only memberships must yield nil, got %q", *got)
	}
	if got := SelectDefaultOrganization([]storage.UserMembership{suspendedOnly, orgA}); got == nil || *got != "org-a" {
		t.Fatalf("suspended memberships must not be selected, got %v", got)
	}
	blank := storage.UserMembership{OrganizationID: "", OrganizationName: "Ghost", Role: "Owner", Status: "ACTIVE"}
	if got := SelectDefaultOrganization([]storage.UserMembership{blank}); got != nil {
		t.Fatalf("blank organization ID must yield nil, got %q", *got)
	}
}
