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

// DiscoverUserMemberships discovers organization memberships for a specific verified user
// by invoking the restricted app.discover_user_memberships function (Blueprint §24.3).
func DiscoverUserMemberships(ctx context.Context, pool *pgxpool.Pool, userID string) ([]UserMembership, error) {
	query := `
		SELECT organization_id, organization_name, role, status
		FROM app.discover_user_memberships($1)
		ORDER BY organization_name ASC
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

// EnumerateTenantsForScheduler discovers active tenant IDs for scheduler background processing
// by invoking the restricted app.enumerate_scheduler_tenants function (Blueprint §24.3).
func EnumerateTenantsForScheduler(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	query := `SELECT organization_id FROM app.enumerate_scheduler_tenants()`
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
