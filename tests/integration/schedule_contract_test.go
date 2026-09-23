package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/scheduling"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
)

func TestScheduleSchemaAndPolicies(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, err := tc.service.CreateOrganization(ctx, ownerID, "Schedule Org")
	if err != nil {
		t.Fatalf("failed to create organization: %v", err)
	}

	proj, err := tc.service.CreateProject(ctx, org.ID, "Schedule Proj")
	if err != nil {
		t.Fatalf("failed to create project: %v", err)
	}

	env, err := tc.service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvProduction, 10)
	if err != nil {
		t.Fatalf("failed to create environment: %v", err)
	}

	// 1. Insert schedule with Blueprint §17 policies
	var scheduleID string
	var overlapPolicy, misfirePolicy string
	var revision int64
	dueAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `
			INSERT INTO schedules (
				organization_id, environment_id, workflow_name,
				cron_expression, timezone, overlap_policy, misfire_policy,
				next_due_at, revision
			) VALUES (
				$1, $2, 'nightly-reconciliation',
				'0 12 * * *', 'America/New_York', 'skip-overlap', 'coalesce-one',
				$3, 1
			) RETURNING id::text, overlap_policy, misfire_policy, revision
		`, org.ID, env.ID, dueAt).Scan(&scheduleID, &overlapPolicy, &misfirePolicy, &revision)
	})
	if err != nil {
		t.Fatalf("failed to insert schedule: %v", err)
	}

	if overlapPolicy != scheduling.OverlapPolicySkipOverlap {
		t.Errorf("expected overlap policy %s, got %s", scheduling.OverlapPolicySkipOverlap, overlapPolicy)
	}
	if misfirePolicy != scheduling.MisfirePolicyCoalesceOne {
		t.Errorf("expected misfire policy %s, got %s", scheduling.MisfirePolicyCoalesceOne, misfirePolicy)
	}
	if revision != 1 {
		t.Errorf("expected revision 1, got %d", revision)
	}

	// 2. Insert schedule occurrence and verify occurrence key format
	occKey := scheduling.FormatOccurrenceKey(scheduleID, revision, dueAt)
	var occID string

	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `
			INSERT INTO schedule_occurrences (
				organization_id, schedule_id, occurrence_key,
				due_at, revision, status
			) VALUES (
				$1, $2::uuid, $3,
				$4, $5, 'STARTED'
			) RETURNING id::text
		`, org.ID, scheduleID, occKey, dueAt, revision).Scan(&occID)
	})
	if err != nil {
		t.Fatalf("failed to insert schedule occurrence: %v", err)
	}

	// 3. Verify unique constraint: duplicate insertion must fail (INV-10)
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO schedule_occurrences (
				organization_id, schedule_id, occurrence_key,
				due_at, revision, status
			) VALUES (
				$1, $2::uuid, $3,
				$4, $5, 'STARTED'
			)
		`, org.ID, scheduleID, occKey, dueAt, revision)
		return err
	})
	if err == nil {
		t.Fatalf("expected unique constraint violation on duplicate occurrence_key, got nil error")
	}
	if !strings.Contains(err.Error(), "duplicate key value") && !strings.Contains(err.Error(), "unique") {
		t.Errorf("expected unique constraint violation error, got %v", err)
	}
}

func TestScheduleOverlapAndMisfireAccounting(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, _ := tc.service.CreateOrganization(ctx, ownerID, "Accounting Org")
	proj, _ := tc.service.CreateProject(ctx, org.ID, "Accounting Proj")
	env, _ := tc.service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvProduction, 10)

	var scheduleID string
	err := tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `
			INSERT INTO schedules (
				organization_id, environment_id, workflow_name,
				cron_expression, timezone
			) VALUES (
				$1, $2, 'accounting-job',
				'*/15 * * * *', 'UTC'
			) RETURNING id::text
		`, org.ID, env.ID).Scan(&scheduleID)
	})
	if err != nil {
		t.Fatalf("failed to insert schedule: %v", err)
	}

	// 1. Record SKIPPED_OVERLAP occurrence
	dueAtOverlap := time.Date(2026, 7, 1, 10, 15, 0, 0, time.UTC)
	occKeyOverlap := scheduling.FormatOccurrenceKey(scheduleID, 1, dueAtOverlap)

	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO schedule_occurrences (
				organization_id, schedule_id, occurrence_key,
				due_at, revision, status, skipped_reason
			) VALUES (
				$1, $2::uuid, $3,
				$4, 1, 'SKIPPED', $5
			)
		`, org.ID, scheduleID, occKeyOverlap, dueAtOverlap, scheduling.SkippedReasonOverlap)
		return err
	})
	if err != nil {
		t.Fatalf("failed to record skipped overlap occurrence: %v", err)
	}

	// 2. Record coalesced occurrence after downtime with skipped_count = 3
	dueAtMisfire := time.Date(2026, 7, 1, 14, 0, 0, 0, time.UTC)
	occKeyMisfire := scheduling.FormatOccurrenceKey(scheduleID, 1, dueAtMisfire)

	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO schedule_occurrences (
				organization_id, schedule_id, occurrence_key,
				due_at, revision, status, skipped_reason, skipped_count
			) VALUES (
				$1, $2::uuid, $3,
				$4, 1, 'STARTED', $5, $6
			)
		`, org.ID, scheduleID, occKeyMisfire, dueAtMisfire, scheduling.SkippedReasonMisfire, 3)
		return err
	})
	if err != nil {
		t.Fatalf("failed to record misfire coalesced occurrence: %v", err)
	}

	// 3. Query back and verify recorded reasons and counts
	var skippedReason string
	var skippedCount int
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `
			SELECT skipped_reason, skipped_count
			FROM schedule_occurrences
			WHERE schedule_id = $1::uuid AND occurrence_key = $2
		`, scheduleID, occKeyMisfire).Scan(&skippedReason, &skippedCount)
	})
	if err != nil {
		t.Fatalf("failed to query misfire occurrence: %v", err)
	}

	if skippedReason != scheduling.SkippedReasonMisfire {
		t.Errorf("expected skipped_reason %s, got %s", scheduling.SkippedReasonMisfire, skippedReason)
	}
	if skippedCount != 3 {
		t.Errorf("expected skipped_count 3, got %d", skippedCount)
	}
}

func TestScheduleTenantIsolation(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	ctx := context.Background()
	owner1, _ := tenant.NewUUID()
	org1, _ := tc.service.CreateOrganization(ctx, owner1, "Tenant 1")
	proj1, _ := tc.service.CreateProject(ctx, org1.ID, "Proj 1")
	env1, _ := tc.service.CreateEnvironment(ctx, org1.ID, proj1.ID, tenant.EnvProduction, 10)

	owner2, _ := tenant.NewUUID()
	org2, _ := tc.service.CreateOrganization(ctx, owner2, "Tenant 2")

	// Insert schedule into Tenant 1
	var scheduleID string
	err := tc.pool.WithTenantTx(ctx, org1.ID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `
			INSERT INTO schedules (organization_id, environment_id, workflow_name, cron_expression, timezone)
			VALUES ($1, $2, 'tenant1-wf', '0 * * * *', 'UTC')
			RETURNING id::text
		`, org1.ID, env1.ID).Scan(&scheduleID)
	})
	if err != nil {
		t.Fatalf("failed to insert schedule: %v", err)
	}

	// Query from Tenant 2: RLS must return 0 rows
	err = tc.pool.WithTenantTx(ctx, org2.ID, func(ctx context.Context, tx storage.Tx) error {
		var count int
		err := tx.QueryRow(ctx, `SELECT count(*) FROM schedules WHERE id = $1::uuid`, scheduleID).Scan(&count)
		if err != nil {
			return err
		}
		if count != 0 {
			t.Errorf("RLS violation: Tenant 2 was able to view Tenant 1's schedule!")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error querying under tenant 2: %v", err)
	}
}
