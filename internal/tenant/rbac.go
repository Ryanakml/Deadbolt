package tenant

import (
	"fmt"
)

// roleCapabilities stores the canonical set of capabilities for each role per Blueprint §24.2.
var roleCapabilities = map[string]map[string]struct{}{
	RoleViewer: {
		CapRunsRead:      {},
		CapWorkflowsRead: {},
		CapWorkersRead:   {},
		CapOrgRead:       {},
	},
	RoleDeveloper: {
		CapRunsRead:                   {},
		CapWorkflowsRead:             {},
		CapWorkersRead:               {},
		CapOrgRead:                   {},
		CapRunsCreate:                {},
		CapRunsControl:               {},
		CapPayloadRead:               {},
		CapDeploymentsRegister:        {},
		CapDeploymentsActivateStaging: {},
	},
	RoleOperator: {
		CapRunsRead:                   {},
		CapWorkflowsRead:             {},
		CapWorkersRead:               {},
		CapOrgRead:                   {},
		CapRunsCreate:                {},
		CapRunsControl:               {},
		CapPayloadRead:               {},
		CapDeploymentsRegister:        {},
		CapDeploymentsActivateStaging: {},
		CapDeploymentsActivateProd:    {},
		CapDeploymentsWrite:           {},
		CapWorkersDrain:               {},
		CapApprovalsDecide:            {},
		CapRunsReconcile:              {},
	},
	RoleAdmin: {
		CapRunsRead:                   {},
		CapWorkflowsRead:             {},
		CapWorkersRead:               {},
		CapOrgRead:                   {},
		CapRunsCreate:                {},
		CapRunsControl:               {},
		CapPayloadRead:               {},
		CapDeploymentsRegister:        {},
		CapDeploymentsActivateStaging: {},
		CapDeploymentsActivateProd:    {},
		CapDeploymentsWrite:           {},
		CapWorkersDrain:               {},
		CapApprovalsDecide:            {},
		CapRunsReconcile:              {},
		CapAdminMember:               {},
		CapAdminKey:                  {},
		CapAdminProject:              {},
		CapOrgUpdate:                 {},
	},
	RoleOwner: {
		CapRunsRead:                   {},
		CapWorkflowsRead:             {},
		CapWorkersRead:               {},
		CapOrgRead:                   {},
		CapRunsCreate:                {},
		CapRunsControl:               {},
		CapPayloadRead:               {},
		CapDeploymentsRegister:        {},
		CapDeploymentsActivateStaging: {},
		CapDeploymentsActivateProd:    {},
		CapDeploymentsWrite:           {},
		CapWorkersDrain:               {},
		CapApprovalsDecide:            {},
		CapRunsReconcile:              {},
		CapAdminMember:               {},
		CapAdminKey:                  {},
		CapAdminProject:              {},
		CapOrgUpdate:                 {},
		CapOrgDelete:                 {},
	},
}

// IsValidRole validates if a given string is a canonical role.
func IsValidRole(role string) bool {
	_, ok := roleCapabilities[role]
	return ok
}

// RoleCapabilities returns the list of all capabilities granted to a canonical role.
func RoleCapabilities(role string) []string {
	capsMap, ok := roleCapabilities[role]
	if !ok {
		return nil
	}
	caps := make([]string, 0, len(capsMap))
	for c := range capsMap {
		caps = append(caps, c)
	}
	return caps
}

// CanRolePerform checks whether a canonical role is authorized for a specific capability.
func CanRolePerform(role string, capability string) bool {
	capsMap, ok := roleCapabilities[role]
	if !ok {
		return false
	}
	_, allowed := capsMap[capability]
	return allowed
}

// CanAPIKeyPerform checks whether an API key's capability slice contains the requested capability.
func CanAPIKeyPerform(keyCapabilities []string, capability string) bool {
	for _, c := range keyCapabilities {
		if c == capability {
			return true
		}
	}
	return false
}

// RequireRoleCapability returns ErrForbidden if the role does not have the specified capability.
func RequireRoleCapability(role string, capability string) error {
	if !CanRolePerform(role, capability) {
		return fmt.Errorf("%w: role %q lacks capability %q", ErrForbidden, role, capability)
	}
	return nil
}
