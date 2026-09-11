package storage

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// UserMembership represents an organization membership for a human identity.
type UserMembership struct {
	OrganizationID   string
	OrganizationName string
	Role             string
	Status           string
}

// DiscoverUserMemberships discovers organization memberships for a specific verified user.
// Uses a direct scoped query bypassing generic tenant context.
func DiscoverUserMemberships(ctx context.Context, pool *pgxpool.Pool, userID string) ([]UserMembership, error) {
	query := `
		SELECT m.organization_id, o.name, m.role, m.status
		FROM organization_members m
		JOIN organizations o ON o.id = m.organization_id
		WHERE m.user_id = $1 AND m.status = 'ACTIVE'
		ORDER BY o.name ASC
	`
	rows, err := pool.Query(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to discover user memberships: %w", err)
	}
	defer rows.Close()

	var memberships []UserMembership
	for rows.Next() {
		var m UserMembership
		if err := rows.Scan(&m.OrganizationID, &m.OrganizationName, &m.Role, &m.Status); err != nil {
			return nil, fmt.Errorf("failed to scan membership row: %w", err)
		}
		memberships = append(memberships, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("membership rows iteration error: %w", err)
	}
	return memberships, nil
}

// EnumerateTenantsForScheduler discovers all active organizations for scheduler background processing.
// Per Blueprint §24.3:
// "The scheduler uses a dedicated system identity to enumerate tenant IDs through a limited function,
// then processes each tenant in a per-tenant transaction through RLS."
func EnumerateTenantsForScheduler(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	query := `SELECT id FROM organizations ORDER BY created_at ASC`
	rows, err := pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to enumerate tenants for scheduler: %w", err)
	}
	defer rows.Close()

	var orgIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan organization id: %w", err)
		}
		orgIDs = append(orgIDs, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("tenant rows iteration error: %w", err)
	}
	return orgIDs, nil
}
