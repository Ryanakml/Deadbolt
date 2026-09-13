package tenant

import (
	"errors"
	"time"
)

// Canonical Roles per Blueprint §24.2
const (
	RoleViewer    = "Viewer"
	RoleDeveloper = "Developer"
	RoleOperator  = "Operator"
	RoleAdmin     = "Admin"
	RoleOwner     = "Owner"
)

// Member Statuses
const (
	StatusActive    = "ACTIVE"
	StatusSuspended = "SUSPENDED"
)

// Canonical Environments
const (
	EnvDevelopment = "development"
	EnvStaging     = "staging"
	EnvProduction  = "production"
)

// Canonical Capabilities per Blueprint §24.2
const (
	CapRunCreate             = "run:create"
	CapRunRead               = "run:read"
	CapRunControl            = "run:control"
	CapPayloadRead           = "payload:read"
	CapDeployRegister        = "deployment:register"
	CapDeployActivateStaging = "deployment:activate:staging"
	CapDeployActivateProd    = "deployment:activate:production"
	CapWorkerDrain           = "worker:drain"
	CapApprovalDecide        = "approval:decide"
	CapReconcileResolve      = "reconciliation:resolve"
)

// Domain Errors
var (
	ErrLastOwnerDemotion    = errors.New("LAST_OWNER_DEMOTION_FORBIDDEN: At least one active Owner must remain in the organization")
	ErrLastOwnerRemoval     = errors.New("LAST_OWNER_REMOVAL_FORBIDDEN: Cannot remove the last active Owner of an organization")
	ErrLastOwnerSuspension  = errors.New("LAST_OWNER_SUSPENSION_FORBIDDEN: Cannot suspend the last active Owner of an organization")
	ErrMachineKeyRestricted = errors.New("MACHINE_KEY_UNAUTHORIZED: Machine API keys cannot be granted approval or reconciliation capabilities")
	ErrCrossTenantDenied    = errors.New("CROSS_TENANT_ACCESS_DENIED: Resource does not belong to the authenticated organization")
	ErrEnvironmentMismatch  = errors.New("ENVIRONMENT_MISMATCH: Provided environment does not match the API key's scoped environment")
	ErrInvalidRole          = errors.New("INVALID_ROLE: Role must be Viewer, Developer, Operator, Admin, or Owner")
	ErrInvalidStatus        = errors.New("INVALID_STATUS: Status must be ACTIVE or SUSPENDED")
	ErrInvalidEnvironment   = errors.New("INVALID_ENVIRONMENT: Environment must be development, staging, or production")
	ErrKeyExpired           = errors.New("API_KEY_EXPIRED: The provided API key has expired")
	ErrKeyRevoked           = errors.New("API_KEY_REVOKED: The provided API key has been revoked")
	ErrUnauthorized         = errors.New("UNAUTHORIZED: Authentication is required")
	ErrForbidden            = errors.New("FORBIDDEN: Insufficient permissions for requested operation")
	ErrNotFound             = errors.New("NOT_FOUND: The requested resource was not found")
)

// Organization represents a top-level tenant entity.
type Organization struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Member represents an organization member identity and role binding.
type Member struct {
	ID             string    `json:"id"`
	OrganizationID string    `json:"organization_id"`
	UserID         string    `json:"user_id"`
	Role           string    `json:"role"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// Project represents a project namespace scoped to an organization.
type Project struct {
	ID             string    `json:"id"`
	OrganizationID string    `json:"organization_id"`
	Name           string    `json:"name"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// Environment represents an execution boundary scoped to an organization and project.
type Environment struct {
	ID             string    `json:"id"`
	OrganizationID string    `json:"organization_id"`
	ProjectID      string    `json:"project_id"`
	Name           string    `json:"name"`
	MaxConcurrency int       `json:"max_concurrency"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// APIKey represents a machine identity scoped to an organization and exactly one environment.
type APIKey struct {
	ID              string     `json:"id"`
	OrganizationID  string     `json:"organization_id"`
	EnvironmentID   string     `json:"environment_id"`
	EnvironmentName string     `json:"environment_name"`
	Prefix          string     `json:"prefix"`
	HashedSecret    string     `json:"-"`
	Capabilities    []string   `json:"capabilities"`
	ExpiresAt       *time.Time `json:"expires_at,omitempty"`
	RevokedAt       *time.Time `json:"revoked_at,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	LastUsedAt      *time.Time `json:"last_used_at,omitempty"`
}

// APIKeySummary is the redacted representation safe for UI and listing endpoints.
type APIKeySummary struct {
	ID              string     `json:"id"`
	EnvironmentID   string     `json:"environment_id"`
	EnvironmentName string     `json:"environment_name"`
	Prefix          string     `json:"prefix"`
	Capabilities    []string   `json:"capabilities"`
	ExpiresAt       *time.Time `json:"expires_at,omitempty"`
	RevokedAt       *time.Time `json:"revoked_at,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	LastUsedAt      *time.Time `json:"last_used_at,omitempty"`
}

// GeneratedKey is returned exactly once upon API key creation.
type GeneratedKey struct {
	ID              string     `json:"id"`
	PlaintextKey    string     `json:"key"` // Shown only once
	Prefix          string     `json:"prefix"`
	EnvironmentID   string     `json:"environment_id"`
	EnvironmentName string     `json:"environment_name"`
	Capabilities    []string   `json:"capabilities"`
	ExpiresAt       *time.Time `json:"expires_at,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
}

// ErrorEnvelope represents the standard RFC/OpenAPI 3.1.0 error envelope per Blueprint §20.1 and §25.1.
type ErrorEnvelope struct {
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	RequestID string         `json:"requestId"`
	Details   map[string]any `json:"details"`
	Retryable bool           `json:"retryable"`
}
