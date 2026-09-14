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

// Canonical Capabilities per Blueprint §20.1, §24.2, and contracts/openapi/control-plane.yaml
const (
	// Workflow Runs
	CapRunsCreate    = "runs:create"
	CapRunsRead      = "runs:read"
	CapRunsControl   = "runs:control"
	CapRunsReconcile = "runs:reconcile"

	// Payload & Artifacts
	CapPayloadRead    = "payload:read"
	CapArtifactsWrite = "artifacts:write"

	// Deployments
	CapDeploymentsWrite           = "deployments:write"
	CapDeploymentsRegister        = "deployments:register"
	CapDeploymentsActivateStaging = "deployments:activate:staging"
	CapDeploymentsActivateProd    = "deployments:activate:production"

	// Workflows & Workers
	CapWorkflowsRead = "workflows:read"
	CapWorkersRead   = "workers:read"
	CapWorkersDrain  = "workers:drain"

	// Approvals, Schedules, Webhooks
	CapApprovalsDecide = "approvals:decide"
	CapSchedulesWrite  = "schedules:write"
	CapWebhooksWrite   = "webhooks:write"

	// Administrative capabilities
	CapOrgRead      = "org:read"
	CapOrgUpdate    = "org:update"
	CapOrgDelete    = "org:delete"
	CapAdminMember  = "admin:member"
	CapAdminKey     = "admin:key"
	CapAdminProject = "admin:project"

	// Backward-compatible aliases
	CapRunCreate             = CapRunsCreate
	CapRunRead               = CapRunsRead
	CapRunControl            = CapRunsControl
	CapDeployRegister        = CapDeploymentsRegister
	CapDeployActivateStaging = CapDeploymentsActivateStaging
	CapDeployActivateProd    = CapDeploymentsActivateProd
	CapWorkerDrain           = CapWorkersDrain
	CapApprovalDecide        = CapApprovalsDecide
	CapReconcileResolve      = CapRunsReconcile
)

// Domain Errors
var (
	ErrLastOwnerDemotion    = errors.New("LAST_OWNER_DEMOTION_FORBIDDEN: At least one active Owner must remain in the organization")
	ErrLastOwnerRemoval     = errors.New("LAST_OWNER_REMOVAL_FORBIDDEN: Cannot remove the last active Owner of an organization")
	ErrLastOwnerSuspension  = errors.New("LAST_OWNER_SUSPENSION_FORBIDDEN: Cannot suspend the last active Owner of an organization")
	ErrMachineKeyRestricted = errors.New("MACHINE_KEY_UNAUTHORIZED: Machine API keys cannot be granted approval or reconciliation capabilities")
	ErrCapabilityElevation  = errors.New("CAPABILITY_ELEVATION_FORBIDDEN: Requested API key capabilities must be a subset of the creator's capabilities")
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
	ErrAuditRequired        = errors.New("AUDIT_REQUIRED: Audit context is required for lifecycle mutation")
	ErrIdempotencyConflict  = errors.New("IDEMPOTENCY_CONFLICT: Idempotency-Key was already used with different request content")
	ErrCommandStorage       = errors.New("COMMAND_STORAGE_FAILURE: tenant command storage failed")
)

// RevokedKeyError is returned when an API key was verified by cryptographic hash
// but has been revoked. Retaining the key reference enables deterministic replay
// of the mutation (rotation or revocation) that revoked this key (Blueprint §20.1).
type RevokedKeyError struct {
	Key *APIKey
}

func (e *RevokedKeyError) Error() string {
	return ErrKeyRevoked.Error()
}

func (e *RevokedKeyError) Unwrap() error {
	return ErrKeyRevoked
}

func (e *RevokedKeyError) Is(target error) bool {
	return target == ErrKeyRevoked
}

type IdentityType string

const (
	IdentityTypeHuman   IdentityType = "HUMAN"
	IdentityTypeMachine IdentityType = "MACHINE"
)

// AuditContext holds actor identity and correlation metadata for immutable audit logging.
type AuditContext struct {
	ActorID       *string      // User ID (UUID) or Key ID (UUID)
	ActorType     IdentityType // HUMAN or MACHINE
	Role          string       // e.g. "Admin", "Owner", or "" for machine
	Capabilities  []string     // effective capabilities at time of action
	CorrelationID string       // Request ID / X-Request-ID / Idempotency-Key
	Reason        string       // Optional reason
}

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
	PlaintextKey    string     `json:"key,omitempty"` // Shown only once
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
