package auth

import (
	"strings"

	"github.com/Ryanakml/Deadbolt/internal/storage"
)

// SelectDefaultOrganization implements the single-active-membership rule for
// fresh sessions: it returns the organization ID only when the identity has
// exactly one ACTIVE membership. Zero memberships, several memberships, or
// only suspended memberships yield nil so that explicit selection remains
// reachable instead of silently binding an arbitrary organization.
func SelectDefaultOrganization(memberships []storage.UserMembership) *string {
	var selected *string
	active := 0
	for i := range memberships {
		if !strings.EqualFold(strings.TrimSpace(memberships[i].Status), "ACTIVE") {
			continue
		}
		if memberships[i].OrganizationID == "" {
			continue
		}
		active++
		if active == 1 {
			id := memberships[i].OrganizationID
			selected = &id
		}
	}
	if active != 1 {
		return nil
	}
	return selected
}
