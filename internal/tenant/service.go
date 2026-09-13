package tenant

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/jackc/pgx/v5"
)

// Service provides organization, member, project, environment, and API key operations.
type Service struct {
	pool *storage.Pool
}

// NewService constructs a new tenant service.
func NewService(pool *storage.Pool) *Service {
	return &Service{pool: pool}
}

// CreateOrganization creates a new organization and assigns the creator as an active Owner.
// Runs under transaction-local RLS scoping for the newly generated organization ID.
func (s *Service) CreateOrganization(ctx context.Context, userID string, name string) (*Organization, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrForbidden)
	}
	if userID == "" {
		return nil, fmt.Errorf("%w: creator user_id is required", ErrUnauthorized)
	}

	orgID, err := NewUUID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate org id: %w", err)
	}

	var org Organization
	err = s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		// 1. Insert organization
		queryOrg := `
			INSERT INTO organizations (id, name)
			VALUES ($1, $2)
			RETURNING id, name, created_at, updated_at
		`
		if err := tx.QueryRow(ctx, queryOrg, orgID, name).Scan(&org.ID, &org.Name, &org.CreatedAt, &org.UpdatedAt); err != nil {
			return fmt.Errorf("failed to insert organization: %w", err)
		}

		// 2. Insert creator as Owner
		queryMember := `
			INSERT INTO organization_members (organization_id, user_id, role, status)
			VALUES ($1, $2, $3, $4)
		`
		if _, err := tx.Exec(ctx, queryMember, orgID, userID, RoleOwner, StatusActive); err != nil {
			return fmt.Errorf("failed to insert owner member: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	return &org, nil
}

// GetOrganization fetches an organization by ID within tenant context.
func (s *Service) GetOrganization(ctx context.Context, orgID string) (*Organization, error) {
	var org Organization
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		query := `SELECT id, name, created_at, updated_at FROM organizations WHERE id = $1`
		err := tx.QueryRow(ctx, query, orgID).Scan(&org.ID, &org.Name, &org.CreatedAt, &org.UpdatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return &org, nil
}

// UpdateOrganization updates an organization's name.
func (s *Service) UpdateOrganization(ctx context.Context, orgID string, name string) (*Organization, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrForbidden)
	}

	var org Organization
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		query := `
			UPDATE organizations
			SET name = $2, updated_at = clock_timestamp()
			WHERE id = $1
			RETURNING id, name, created_at, updated_at
		`
		err := tx.QueryRow(ctx, query, orgID, name).Scan(&org.ID, &org.Name, &org.CreatedAt, &org.UpdatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return &org, nil
}

// DeleteOrganization deletes an organization and all its cascade-dependent children.
func (s *Service) DeleteOrganization(ctx context.Context, orgID string) error {
	return s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		tag, err := tx.Exec(ctx, `DELETE FROM organizations WHERE id = $1`, orgID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// ListMembers lists all members of an organization.
func (s *Service) ListMembers(ctx context.Context, orgID string) ([]Member, error) {
	var members []Member
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		query := `
			SELECT id, organization_id, user_id, role, status, created_at, updated_at
			FROM organization_members
			WHERE organization_id = $1
			ORDER BY created_at ASC
		`
		rows, err := tx.Query(ctx, query, orgID)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var m Member
			if err := rows.Scan(&m.ID, &m.OrganizationID, &m.UserID, &m.Role, &m.Status, &m.CreatedAt, &m.UpdatedAt); err != nil {
				return err
			}
			members = append(members, m)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return members, nil
}

// AddMember adds a user to an organization with a canonical role.
func (s *Service) AddMember(ctx context.Context, orgID string, userID string, role string) (*Member, error) {
	if !IsValidRole(role) {
		return nil, ErrInvalidRole
	}

	var m Member
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		query := `
			INSERT INTO organization_members (organization_id, user_id, role, status)
			VALUES ($1, $2, $3, $4)
			RETURNING id, organization_id, user_id, role, status, created_at, updated_at
		`
		return tx.QueryRow(ctx, query, orgID, userID, role, StatusActive).
			Scan(&m.ID, &m.OrganizationID, &m.UserID, &m.Role, &m.Status, &m.CreatedAt, &m.UpdatedAt)
	})
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// UpdateMemberRole updates a member's role while strictly enforcing Last Owner Defense.
func (s *Service) UpdateMemberRole(ctx context.Context, orgID string, targetUserID string, newRole string) error {
	if !IsValidRole(newRole) {
		return ErrInvalidRole
	}

	return s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		// Acquire row lock on organization to serialize member modifications and prevent race conditions
		var lockedOrgID string
		if err := tx.QueryRow(ctx, `SELECT id FROM organizations WHERE id = $1 FOR UPDATE`, orgID).Scan(&lockedOrgID); err != nil {
			return err
		}

		// 1. Get current member role and status
		var currentRole, status string
		queryCurr := `SELECT role, status FROM organization_members WHERE organization_id = $1 AND user_id = $2`
		err := tx.QueryRow(ctx, queryCurr, orgID, targetUserID).Scan(&currentRole, &status)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		// 2. Last Owner Defense: If target user is an active Owner and new role is NOT Owner
		if currentRole == RoleOwner && status == StatusActive && newRole != RoleOwner {
			var ownerCount int
			countQuery := `
				SELECT COUNT(*)
				FROM organization_members
				WHERE organization_id = $1 AND role = $2 AND status = $3
			`
			if err := tx.QueryRow(ctx, countQuery, orgID, RoleOwner, StatusActive).Scan(&ownerCount); err != nil {
				return err
			}
			if ownerCount <= 1 {
				return ErrLastOwnerDemotion
			}
		}

		// 3. Update role
		updateQuery := `
			UPDATE organization_members
			SET role = $3, updated_at = clock_timestamp()
			WHERE organization_id = $1 AND user_id = $2
		`
		tag, err := tx.Exec(ctx, updateQuery, orgID, targetUserID, newRole)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// UpdateMemberStatus updates a member's status (ACTIVE / SUSPENDED) while strictly enforcing Last Owner Defense.
func (s *Service) UpdateMemberStatus(ctx context.Context, orgID string, targetUserID string, newStatus string) error {
	if newStatus != StatusActive && newStatus != StatusSuspended {
		return ErrInvalidStatus
	}

	return s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		// Acquire row lock on organization to serialize member modifications
		var lockedOrgID string
		if err := tx.QueryRow(ctx, `SELECT id FROM organizations WHERE id = $1 FOR UPDATE`, orgID).Scan(&lockedOrgID); err != nil {
			return err
		}

		// 1. Get current member role and status
		var currentRole, status string
		queryCurr := `SELECT role, status FROM organization_members WHERE organization_id = $1 AND user_id = $2`
		err := tx.QueryRow(ctx, queryCurr, orgID, targetUserID).Scan(&currentRole, &status)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		// 2. Last Owner Defense: If target user is an active Owner and new status is SUSPENDED
		if currentRole == RoleOwner && status == StatusActive && newStatus == StatusSuspended {
			var ownerCount int
			countQuery := `
				SELECT COUNT(*)
				FROM organization_members
				WHERE organization_id = $1 AND role = $2 AND status = $3
			`
			if err := tx.QueryRow(ctx, countQuery, orgID, RoleOwner, StatusActive).Scan(&ownerCount); err != nil {
				return err
			}
			if ownerCount <= 1 {
				return ErrLastOwnerSuspension
			}
		}

		// 3. Update status
		updateQuery := `
			UPDATE organization_members
			SET status = $3, updated_at = clock_timestamp()
			WHERE organization_id = $1 AND user_id = $2
		`
		tag, err := tx.Exec(ctx, updateQuery, orgID, targetUserID, newStatus)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// RemoveMember removes a member while strictly enforcing Last Owner Defense.
func (s *Service) RemoveMember(ctx context.Context, orgID string, targetUserID string) error {
	return s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		// Acquire row lock on organization to serialize member modifications and prevent race conditions
		var lockedOrgID string
		if err := tx.QueryRow(ctx, `SELECT id FROM organizations WHERE id = $1 FOR UPDATE`, orgID).Scan(&lockedOrgID); err != nil {
			return err
		}

		// 1. Get current member role and status
		var currentRole, status string
		queryCurr := `SELECT role, status FROM organization_members WHERE organization_id = $1 AND user_id = $2`
		err := tx.QueryRow(ctx, queryCurr, orgID, targetUserID).Scan(&currentRole, &status)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		// 2. Last Owner Defense: If target is an active Owner
		if currentRole == RoleOwner && status == StatusActive {
			var ownerCount int
			countQuery := `
				SELECT COUNT(*)
				FROM organization_members
				WHERE organization_id = $1 AND role = $2 AND status = $3
			`
			if err := tx.QueryRow(ctx, countQuery, orgID, RoleOwner, StatusActive).Scan(&ownerCount); err != nil {
				return err
			}
			if ownerCount <= 1 {
				return ErrLastOwnerRemoval
			}
		}

		// 3. Delete member
		deleteQuery := `DELETE FROM organization_members WHERE organization_id = $1 AND user_id = $2`
		tag, err := tx.Exec(ctx, deleteQuery, orgID, targetUserID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// GetMember fetches a member by user ID scoped to the organization.
func (s *Service) GetMember(ctx context.Context, orgID string, userID string) (*Member, error) {
	var m Member
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		query := `
			SELECT id, organization_id, user_id, role, status, created_at, updated_at
			FROM organization_members
			WHERE organization_id = $1 AND user_id = $2
		`
		err := tx.QueryRow(ctx, query, orgID, userID).
			Scan(&m.ID, &m.OrganizationID, &m.UserID, &m.Role, &m.Status, &m.CreatedAt, &m.UpdatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// CreateProject creates a new project namespace within an organization.
func (s *Service) CreateProject(ctx context.Context, orgID string, name string) (*Project, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrForbidden)
	}

	var p Project
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		query := `
			INSERT INTO projects (organization_id, name)
			VALUES ($1, $2)
			RETURNING id, organization_id, name, created_at, updated_at
		`
		return tx.QueryRow(ctx, query, orgID, name).
			Scan(&p.ID, &p.OrganizationID, &p.Name, &p.CreatedAt, &p.UpdatedAt)
	})
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// ListProjects lists all projects for an organization.
func (s *Service) ListProjects(ctx context.Context, orgID string) ([]Project, error) {
	var projects []Project
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		query := `
			SELECT id, organization_id, name, created_at, updated_at
			FROM projects
			WHERE organization_id = $1
			ORDER BY name ASC
		`
		rows, err := tx.Query(ctx, query, orgID)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var p Project
			if err := rows.Scan(&p.ID, &p.OrganizationID, &p.Name, &p.CreatedAt, &p.UpdatedAt); err != nil {
				return err
			}
			projects = append(projects, p)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return projects, nil
}

// GetProject gets a project by ID scoped to the organization.
func (s *Service) GetProject(ctx context.Context, orgID string, projectID string) (*Project, error) {
	var p Project
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		query := `
			SELECT id, organization_id, name, created_at, updated_at
			FROM projects
			WHERE organization_id = $1 AND id = $2
		`
		err := tx.QueryRow(ctx, query, orgID, projectID).
			Scan(&p.ID, &p.OrganizationID, &p.Name, &p.CreatedAt, &p.UpdatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// GetProjectForEnvironment resolves the single project associated with an environment.
func (s *Service) GetProjectForEnvironment(ctx context.Context, orgID string, envID string) (*Project, error) {
	var p Project
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		query := `
			SELECT p.id, p.organization_id, p.name, p.created_at, p.updated_at
			FROM projects p
			JOIN environments e ON e.project_id = p.id AND e.organization_id = p.organization_id
			WHERE e.organization_id = $1 AND e.id = $2
		`
		err := tx.QueryRow(ctx, query, orgID, envID).Scan(&p.ID, &p.OrganizationID, &p.Name, &p.CreatedAt, &p.UpdatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// CreateEnvironment creates an environment and provisions its environment_admissions concurrency quota.
func (s *Service) CreateEnvironment(ctx context.Context, orgID string, projectID string, name string, maxConcurrency int) (*Environment, error) {
	if name != EnvDevelopment && name != EnvStaging && name != EnvProduction {
		return nil, ErrInvalidEnvironment
	}
	if maxConcurrency <= 0 {
		maxConcurrency = 10
	}

	var env Environment
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		// Verify project exists
		var exists int
		checkProj := `SELECT 1 FROM projects WHERE organization_id = $1 AND id = $2`
		err := tx.QueryRow(ctx, checkProj, orgID, projectID).Scan(&exists)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		// Insert environment
		insertEnv := `
			INSERT INTO environments (organization_id, project_id, name)
			VALUES ($1, $2, $3)
			RETURNING id, organization_id, project_id, name, created_at, updated_at
		`
		if err := tx.QueryRow(ctx, insertEnv, orgID, projectID, name).
			Scan(&env.ID, &env.OrganizationID, &env.ProjectID, &env.Name, &env.CreatedAt, &env.UpdatedAt); err != nil {
			return err
		}

		// Insert admission quota lock row
		insertQuota := `
			INSERT INTO environment_admissions (environment_id, organization_id, max_concurrency)
			VALUES ($1, $2, $3)
		`
		if _, err := tx.Exec(ctx, insertQuota, env.ID, orgID, maxConcurrency); err != nil {
			return err
		}

		env.MaxConcurrency = maxConcurrency
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &env, nil
}

// ListEnvironments lists environments for a project within an organization.
func (s *Service) ListEnvironments(ctx context.Context, orgID string, projectID string) ([]Environment, error) {
	var envs []Environment
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		query := `
			SELECT e.id, e.organization_id, e.project_id, e.name, COALESCE(ea.max_concurrency, 10), e.created_at, e.updated_at
			FROM environments e
			LEFT JOIN environment_admissions ea ON ea.environment_id = e.id AND ea.organization_id = e.organization_id
			WHERE e.organization_id = $1 AND e.project_id = $2
			ORDER BY e.name ASC
		`
		rows, err := tx.Query(ctx, query, orgID, projectID)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var e Environment
			if err := rows.Scan(&e.ID, &e.OrganizationID, &e.ProjectID, &e.Name, &e.MaxConcurrency, &e.CreatedAt, &e.UpdatedAt); err != nil {
				return err
			}
			envs = append(envs, e)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return envs, nil
}

// GetEnvironment fetches an environment by ID scoped to the organization.
func (s *Service) GetEnvironment(ctx context.Context, orgID string, envID string) (*Environment, error) {
	var env Environment
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		query := `
			SELECT e.id, e.organization_id, e.project_id, e.name, COALESCE(ea.max_concurrency, 10), e.created_at, e.updated_at
			FROM environments e
			LEFT JOIN environment_admissions ea ON ea.environment_id = e.id AND ea.organization_id = e.organization_id
			WHERE e.organization_id = $1 AND e.id = $2
		`
		err := tx.QueryRow(ctx, query, orgID, envID).
			Scan(&env.ID, &env.OrganizationID, &env.ProjectID, &env.Name, &env.MaxConcurrency, &env.CreatedAt, &env.UpdatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return &env, nil
}

// CreateAPIKey generates a cryptographically secure, environment-scoped API key.
// Plaintext secret is returned exactly once in GeneratedKey.
// Requested capabilities must be a non-empty subset of creatorCapabilities, and machine-only restrictions apply.
// Fails closed: audit context and creatorCaps are strictly required.
func (s *Service) CreateAPIKey(ctx context.Context, orgID string, envID string, capabilities []string, expiryDays int, creatorCaps []string, audit *AuditContext) (*GeneratedKey, error) {
	if audit == nil {
		return nil, ErrAuditRequired
	}
	if len(creatorCaps) == 0 {
		return nil, fmt.Errorf("%w: creator capabilities cannot be empty", ErrForbidden)
	}

	// 1. Sanitize & check creator capability subset (prevent privilege elevation)
	capabilities = SanitizeCapabilities(capabilities)
	if len(capabilities) == 0 {
		return nil, fmt.Errorf("%w: requested capabilities cannot be empty", ErrForbidden)
	}

	creatorMap := make(map[string]bool, len(creatorCaps))
	for _, c := range creatorCaps {
		creatorMap[c] = true
	}
	for _, reqCap := range capabilities {
		if !creatorMap[reqCap] {
			return nil, ErrCapabilityElevation
		}
	}

	// 2. Machine keys barred from approval & reconciliation
	if err := ValidateKeyCapabilities(capabilities, true); err != nil {
		return nil, err
	}

	return s.createAPIKeyInternal(ctx, orgID, envID, capabilities, expiryDays, audit)
}

// BootstrapAPIKey creates an initial API key for test fixture setup or system bootstrap.
// It bypasses the creator capability subset check, but still enforces machine capability
// restrictions, requires an audit context, and logs the bootstrap event.
func (s *Service) BootstrapAPIKey(ctx context.Context, orgID string, envID string, capabilities []string, expiryDays int, audit *AuditContext) (*GeneratedKey, error) {
	if audit == nil {
		audit = &AuditContext{
			Reason: "system_bootstrap",
		}
	}

	capabilities = SanitizeCapabilities(capabilities)
	if len(capabilities) == 0 {
		return nil, fmt.Errorf("%w: capabilities cannot be empty", ErrForbidden)
	}

	if err := ValidateKeyCapabilities(capabilities, true); err != nil {
		return nil, err
	}

	return s.createAPIKeyInternal(ctx, orgID, envID, capabilities, expiryDays, audit)
}

func (s *Service) createAPIKeyInternal(ctx context.Context, orgID string, envID string, capabilities []string, expiryDays int, audit *AuditContext) (*GeneratedKey, error) {
	var genKey GeneratedKey
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		// Look up environment to get name for prefix
		var envName string
		queryEnv := `SELECT name FROM environments WHERE organization_id = $1 AND id = $2`
		err := tx.QueryRow(ctx, queryEnv, orgID, envID).Scan(&envName)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		// Generate key material (>= 256 bits entropy, prefix, plaintext, and SHA-256 hash)
		prefix, plaintextKey, hashedSecret, err := GenerateAPIKeyMaterial(envName)
		if err != nil {
			return fmt.Errorf("failed to generate key material: %w", err)
		}

		expiresAt := CalculateExpiry(expiryDays)

		// Insert into database
		insertQuery := `
			INSERT INTO api_keys (organization_id, environment_id, prefix, hashed_secret, capabilities, expires_at)
			VALUES ($1, $2, $3, $4, $5, $6)
			RETURNING id, created_at
		`
		if err := tx.QueryRow(ctx, insertQuery, orgID, envID, prefix, hashedSecret, capabilities, expiresAt).
			Scan(&genKey.ID, &genKey.CreatedAt); err != nil {
			return fmt.Errorf("failed to insert api key: %w", err)
		}

		// Write immutable audit event atomically (secrets strictly excluded)
		metaBytes, _ := json.Marshal(map[string]any{
			"environment_id":     envID,
			"environment":        envName,
			"prefix":             prefix,
			"capabilities":       capabilities,
			"expires_at":         expiresAt,
			"actor_type":         audit.ActorType,
			"actor_role":         audit.Role,
			"actor_capabilities": audit.Capabilities,
			"idempotency_key":    audit.CorrelationID,
		})
		auditQuery := `
			INSERT INTO audit_events (
				organization_id, actor_id, action, target_type, target_id, correlation_id, reason, metadata
			) VALUES ($1, $2, 'api_key.create', 'api_key', $3, $4, $5, $6)
		`
		if _, err := tx.Exec(ctx, auditQuery, orgID, audit.ActorID, genKey.ID, audit.CorrelationID, audit.Reason, metaBytes); err != nil {
			return fmt.Errorf("failed to write audit event for api key create: %w", err)
		}

		genKey.PlaintextKey = plaintextKey
		genKey.Prefix = prefix
		genKey.EnvironmentID = envID
		genKey.EnvironmentName = envName
		genKey.Capabilities = capabilities
		genKey.ExpiresAt = expiresAt
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &genKey, nil
}

// ListAPIKeys lists API keys for an environment in redacted summary format.
func (s *Service) ListAPIKeys(ctx context.Context, orgID string, envID string) ([]APIKeySummary, error) {
	var keys []APIKeySummary
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		query := `
			SELECT ak.id, ak.environment_id, e.name, ak.prefix, ak.capabilities, ak.expires_at, ak.revoked_at, ak.created_at, ak.last_used_at
			FROM api_keys ak
			JOIN environments e ON e.id = ak.environment_id AND e.organization_id = ak.organization_id
			WHERE ak.organization_id = $1 AND ak.environment_id = $2
			ORDER BY ak.created_at DESC
		`
		rows, err := tx.Query(ctx, query, orgID, envID)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var k APIKeySummary
			if err := rows.Scan(&k.ID, &k.EnvironmentID, &k.EnvironmentName, &k.Prefix, &k.Capabilities, &k.ExpiresAt, &k.RevokedAt, &k.CreatedAt, &k.LastUsedAt); err != nil {
				return err
			}
			keys = append(keys, k)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// RevokeAPIKey revokes an API key, verifying caller environment scope if caller is a machine key.
func (s *Service) RevokeAPIKey(ctx context.Context, orgID string, keyID string, callerEnvID string, audit *AuditContext) error {
	if audit == nil {
		return ErrAuditRequired
	}
	return s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		// 1. Resolve target key's environment to enforce authoritative environment scoping
		var envID string
		var prefix string
		checkQuery := `SELECT environment_id, prefix FROM api_keys WHERE organization_id = $1 AND id = $2`
		err := tx.QueryRow(ctx, checkQuery, orgID, keyID).Scan(&envID, &prefix)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		if callerEnvID != "" && envID != callerEnvID {
			return ErrEnvironmentMismatch
		}

		// 2. Revoke key
		query := `
			UPDATE api_keys
			SET revoked_at = clock_timestamp()
			WHERE organization_id = $1 AND id = $2 AND revoked_at IS NULL
		`
		if _, err := tx.Exec(ctx, query, orgID, keyID); err != nil {
			return err
		}

		// 3. Write immutable audit event atomically
		metaBytes, _ := json.Marshal(map[string]any{
			"key_id":             keyID,
			"environment_id":     envID,
			"prefix":             prefix,
			"actor_type":         audit.ActorType,
			"actor_role":         audit.Role,
			"actor_capabilities": audit.Capabilities,
			"idempotency_key":    audit.CorrelationID,
		})
		auditQuery := `
			INSERT INTO audit_events (
				organization_id, actor_id, action, target_type, target_id, correlation_id, reason, metadata
			) VALUES ($1, $2, 'api_key.revoke', 'api_key', $3, $4, $5, $6)
		`
		if _, err := tx.Exec(ctx, auditQuery, orgID, audit.ActorID, keyID, audit.CorrelationID, audit.Reason, metaBytes); err != nil {
			return fmt.Errorf("failed to write audit event for api key revoke: %w", err)
		}

		return nil
	})
}

// RotateAPIKey atomically revokes the existing key and generates a new key with identical environment & capabilities.
// Authoritative environment check ensures machine callers cannot rotate keys outside their scoped environment.
// Privilege escalation defense: target key capabilities must be a subset of caller capabilities.
func (s *Service) RotateAPIKey(ctx context.Context, orgID string, keyID string, expiryDays int, callerCaps []string, callerEnvID string, audit *AuditContext) (*GeneratedKey, error) {
	if audit == nil {
		return nil, ErrAuditRequired
	}
	if len(callerCaps) == 0 {
		return nil, fmt.Errorf("%w: caller capabilities cannot be empty", ErrForbidden)
	}

	var genKey GeneratedKey
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		// 1. Select existing key with row lock
		var envID string
		var caps []string
		var revokedAt *time.Time
		query := `
			SELECT environment_id, capabilities, revoked_at
			FROM api_keys
			WHERE organization_id = $1 AND id = $2
			FOR UPDATE
		`
		err := tx.QueryRow(ctx, query, orgID, keyID).Scan(&envID, &caps, &revokedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if revokedAt != nil {
			return ErrKeyRevoked
		}

		// 2. Authoritative Environment check for machine callers
		if callerEnvID != "" && envID != callerEnvID {
			return ErrEnvironmentMismatch
		}

		// 3. Prevent Privilege Escalation: target key capabilities must be a subset of caller capabilities
		callerMap := make(map[string]bool, len(callerCaps))
		for _, c := range callerCaps {
			callerMap[c] = true
		}
		for _, targetCap := range caps {
			if !callerMap[targetCap] {
				return ErrCapabilityElevation
			}
		}

		// 4. Machine key restrictions apply to machine callers
		if callerEnvID != "" {
			if err := ValidateKeyCapabilities(caps, true); err != nil {
				return err
			}
		}

		// 5. Lookup environment name
		var envName string
		queryEnv := `SELECT name FROM environments WHERE organization_id = $1 AND id = $2`
		err = tx.QueryRow(ctx, queryEnv, orgID, envID).Scan(&envName)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		// 6. Generate new key material
		prefix, plaintextKey, hashedSecret, err := GenerateAPIKeyMaterial(envName)
		if err != nil {
			return fmt.Errorf("failed to generate key material: %w", err)
		}
		expiresAt := CalculateExpiry(expiryDays)

		// 7. Insert new key
		insertQuery := `
			INSERT INTO api_keys (organization_id, environment_id, prefix, hashed_secret, capabilities, expires_at)
			VALUES ($1, $2, $3, $4, $5, $6)
			RETURNING id, created_at
		`
		if err := tx.QueryRow(ctx, insertQuery, orgID, envID, prefix, hashedSecret, caps, expiresAt).
			Scan(&genKey.ID, &genKey.CreatedAt); err != nil {
			return fmt.Errorf("failed to insert rotated api key: %w", err)
		}

		// 8. Revoke old key atomically
		revokeQuery := `
			UPDATE api_keys
			SET revoked_at = clock_timestamp()
			WHERE organization_id = $1 AND id = $2
		`
		if _, err := tx.Exec(ctx, revokeQuery, orgID, keyID); err != nil {
			return fmt.Errorf("failed to revoke rotated api key: %w", err)
		}

		// 9. Write immutable audit event atomically
		metaBytes, _ := json.Marshal(map[string]any{
			"old_key_id":         keyID,
			"new_key_id":         genKey.ID,
			"environment_id":     envID,
			"environment":        envName,
			"new_prefix":         prefix,
			"capabilities":       caps,
			"expires_at":         expiresAt,
			"actor_type":         audit.ActorType,
			"actor_role":         audit.Role,
			"actor_capabilities": audit.Capabilities,
			"idempotency_key":    audit.CorrelationID,
		})
		auditQuery := `
			INSERT INTO audit_events (
				organization_id, actor_id, action, target_type, target_id, correlation_id, reason, metadata
			) VALUES ($1, $2, 'api_key.rotate', 'api_key', $3, $4, $5, $6)
		`
		if _, err := tx.Exec(ctx, auditQuery, orgID, audit.ActorID, genKey.ID, audit.CorrelationID, audit.Reason, metaBytes); err != nil {
			return fmt.Errorf("failed to write audit event for api key rotate: %w", err)
		}

		genKey.PlaintextKey = plaintextKey
		genKey.Prefix = prefix
		genKey.EnvironmentID = envID
		genKey.EnvironmentName = envName
		genKey.Capabilities = caps
		genKey.ExpiresAt = expiresAt
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &genKey, nil
}

// AuthenticateAPIKey verifies a plaintext API key and returns the authenticated APIKey entity.
// It executes the restricted app.authenticate_api_key discovery function, verifies the hash in constant time,
// checks revocation and expiration, and updates last_used_at under transaction-local RLS.
func (s *Service) AuthenticateAPIKey(ctx context.Context, plaintextKey string) (*APIKey, error) {
	prefix, err := ExtractPrefix(plaintextKey)
	if err != nil {
		_ = VerifyAPIKey(plaintextKey, DummyHash)
		return nil, ErrUnauthorized
	}

	// 1. Query key record by prefix via restricted SECURITY DEFINER function
	query := `
		SELECT id, organization_id, environment_id, environment_name, prefix, hashed_secret, capabilities, expires_at, revoked_at, created_at, last_used_at
		FROM app.authenticate_api_key($1)
	`
	var key APIKey
	err = s.pool.QueryRow(ctx, query, prefix).Scan(
		&key.ID,
		&key.OrganizationID,
		&key.EnvironmentID,
		&key.EnvironmentName,
		&key.Prefix,
		&key.HashedSecret,
		&key.Capabilities,
		&key.ExpiresAt,
		&key.RevokedAt,
		&key.CreatedAt,
		&key.LastUsedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		_ = VerifyAPIKey(plaintextKey, DummyHash)
		return nil, ErrUnauthorized
	}
	if err != nil {
		return nil, fmt.Errorf("failed to lookup api key: %w", err)
	}

	// 2. Constant-time hash verification
	if !VerifyAPIKey(plaintextKey, key.HashedSecret) {
		return nil, ErrUnauthorized
	}

	// 3. Check revocation status
	if key.RevokedAt != nil {
		return nil, ErrKeyRevoked
	}

	// 4. Check expiration status
	if key.ExpiresAt != nil && time.Now().UTC().After(*key.ExpiresAt) {
		return nil, ErrKeyExpired
	}

	// 5. Update last_used_at within transaction-local tenant context (throttled to 1 minute to avoid write lock contention)
	now := time.Now().UTC()
	_ = s.pool.WithTenantTx(ctx, key.OrganizationID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `
			UPDATE api_keys
			SET last_used_at = clock_timestamp()
			WHERE organization_id = $1 AND id = $2
			  AND (last_used_at IS NULL OR last_used_at < clock_timestamp() - INTERVAL '1 minute')
		`, key.OrganizationID, key.ID)
		return err
	})
	key.LastUsedAt = &now

	return &key, nil
}

// CheckIdempotency returns true if a mutation with the given idempotency key / correlation ID has already been recorded.
func (s *Service) CheckIdempotency(ctx context.Context, orgID string, idempKey string) (bool, error) {
	if idempKey == "" || orgID == "" {
		return false, nil
	}
	var found bool
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		var exists int
		err := tx.QueryRow(ctx, `SELECT 1 FROM audit_events WHERE organization_id = $1 AND correlation_id = $2 LIMIT 1`, orgID, idempKey).Scan(&exists)
		if errors.Is(err, pgx.ErrNoRows) {
			found = false
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		return nil
	})
	return found, err
}
