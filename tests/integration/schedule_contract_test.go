package integration_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
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

// TestScheduleCanonicalIdentity_UniqueConstraint proves Blueprint §17 / INV-10 at the DB
// boundary: the canonical (schedule_id, revision, due_at) identity is enforced even when
// callers supply different free-form occurrence_key strings.
func TestScheduleCanonicalIdentity_UniqueConstraint(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, err := tc.service.CreateOrganization(ctx, ownerID, "Canonical Org")
	if err != nil {
		t.Fatalf("failed to create organization: %v", err)
	}
	proj, err := tc.service.CreateProject(ctx, org.ID, "Canonical Proj")
	if err != nil {
		t.Fatalf("failed to create project: %v", err)
	}
	env, err := tc.service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvProduction, 10)
	if err != nil {
		t.Fatalf("failed to create environment: %v", err)
	}

	var scheduleID string
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `
			INSERT INTO schedules (organization_id, environment_id, workflow_name, cron_expression, timezone)
			VALUES ($1, $2, 'canonical-wf', '0 * * * *', 'UTC')
			RETURNING id::text
		`, org.ID, env.ID).Scan(&scheduleID)
	})
	if err != nil {
		t.Fatalf("failed to insert schedule: %v", err)
	}

	dueAt := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	keyA := scheduling.FormatOccurrenceKey(scheduleID, 1, dueAt)
	keyB := keyA + ":distinct-freeform-suffix"

	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO schedule_occurrences (organization_id, schedule_id, occurrence_key, due_at, revision, status)
			VALUES ($1, $2::uuid, $3, $4, 1, 'STARTED')
		`, org.ID, scheduleID, keyA, dueAt)
		return err
	})
	if err != nil {
		t.Fatalf("failed to insert first occurrence: %v", err)
	}

	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO schedule_occurrences (organization_id, schedule_id, occurrence_key, due_at, revision, status)
			VALUES ($1, $2::uuid, $3, $4, 1, 'STARTED')
		`, org.ID, scheduleID, keyB, dueAt)
		return err
	})
	if err == nil {
		t.Fatalf("expected canonical uniqueness violation for same (schedule_id, revision, due_at) with different occurrence_key, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate key value") && !strings.Contains(err.Error(), "uq_schedule_occurrences_canonical_identity") {
		t.Fatalf("expected canonical unique violation, got: %v", err)
	}
	t.Logf("canonical identity enforced: distinct occurrence_key strings for same (schedule_id, revision, due_at) rejected: %v", err)
}

// TestScheduleConcurrentDuplicateEvaluatorsINV10_Postgres is the authoritative INV-10 proof:
// N duplicate schedulers race with separate transactions/connections to claim the same
// (schedule_id, revision, scheduled_at_utc) occurrence using distinct occurrence_key strings.
// Exactly one transaction commits; all others fail on the canonical uniqueness constraint,
// yielding exactly one committed logical occurrence/action.
func TestScheduleConcurrentDuplicateEvaluatorsINV10_Postgres(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, err := tc.service.CreateOrganization(ctx, ownerID, "INV10 Org")
	if err != nil {
		t.Fatalf("failed to create organization: %v", err)
	}
	proj, err := tc.service.CreateProject(ctx, org.ID, "INV10 Proj")
	if err != nil {
		t.Fatalf("failed to create project: %v", err)
	}
	env, err := tc.service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvProduction, 10)
	if err != nil {
		t.Fatalf("failed to create environment: %v", err)
	}

	var scheduleID string
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `
			INSERT INTO schedules (organization_id, environment_id, workflow_name, cron_expression, timezone)
			VALUES ($1, $2, 'inv10-wf', '0 * * * *', 'UTC')
			RETURNING id::text
		`, org.ID, env.ID).Scan(&scheduleID)
	})
	if err != nil {
		t.Fatalf("failed to insert schedule: %v", err)
	}

	dueAt := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	const racers = 10
	start := make(chan struct{})
	var wg sync.WaitGroup
	var committed atomic.Int32
	var rejected atomic.Int32
	errCh := make(chan error, racers)

	wg.Add(racers)
	for i := 0; i < racers; i++ {
		go func(racer int) {
			defer wg.Done()
			<-start
			// Distinct free-form key per racer proves the canonical columns (not occurrence_key)
			// are the actual duplicate guard.
			distinctKey := fmt.Sprintf("%s:racer-%d", scheduling.FormatOccurrenceKey(scheduleID, 1, dueAt), racer)
			err := tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
				_, err := tx.Exec(ctx, `
					INSERT INTO schedule_occurrences (organization_id, schedule_id, occurrence_key, due_at, revision, status)
					VALUES ($1, $2::uuid, $3, $4, 1, 'STARTED')
				`, org.ID, scheduleID, distinctKey, dueAt)
				return err
			})
			if err == nil {
				committed.Add(1)
			} else if strings.Contains(err.Error(), "duplicate key value") {
				rejected.Add(1)
			} else {
				errCh <- fmt.Errorf("racer %d unexpected error: %w", racer, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(errCh)
	for e := range errCh {
		t.Fatalf("concurrent race hit unexpected error: %v", e)
	}

	if committed.Load() != 1 {
		t.Fatalf("INV-10 violation: expected exactly 1 committed occurrence across %d duplicate evaluators, got %d (rejected=%d)", racers, committed.Load(), rejected.Load())
	}
	if rejected.Load() != racers-1 {
		t.Fatalf("expected %d rejected racers, got %d", racers-1, rejected.Load())
	}

	var count int
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM schedule_occurrences WHERE schedule_id = $1::uuid AND revision = 1 AND due_at = $2`, scheduleID, dueAt).Scan(&count)
	})
	if err != nil {
		t.Fatalf("failed to count committed occurrences: %v", err)
	}
	if count != 1 {
		t.Fatalf("INV-10 violation: expected exactly 1 committed row for (schedule_id, revision, due_at), got %d", count)
	}
	t.Logf("INV-10 verified via PostgreSQL: %d duplicate evaluators raced on separate transactions/connections; exactly 1 committed logical occurrence/action for (%s, 1, %s)", racers, scheduleID, dueAt.Format(time.RFC3339))
}

// TestScheduleDeploymentIntegrity_Negative enforces Blueprint §18.1 at the DB boundary:
// schedules.deployment_id must reference an existing deployment in the same organization
// AND environment. Nonexistent, cross-tenant, and cross-environment references fail.
func TestScheduleDeploymentIntegrity_Negative(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	ctx := context.Background()
	ownerA, _ := tenant.NewUUID()
	orgA, err := tc.service.CreateOrganization(ctx, ownerA, "Deploy Org A")
	if err != nil {
		t.Fatalf("failed to create org A: %v", err)
	}
	projA, err := tc.service.CreateProject(ctx, orgA.ID, "Deploy Proj A")
	if err != nil {
		t.Fatalf("failed to create project A: %v", err)
	}
	envA1, err := tc.service.CreateEnvironment(ctx, orgA.ID, projA.ID, tenant.EnvProduction, 10)
	if err != nil {
		t.Fatalf("failed to create env A1: %v", err)
	}
	envA2, err := tc.service.CreateEnvironment(ctx, orgA.ID, projA.ID, tenant.EnvStaging, 10)
	if err != nil {
		t.Fatalf("failed to create env A2: %v", err)
	}

	ownerB, _ := tenant.NewUUID()
	orgB, err := tc.service.CreateOrganization(ctx, ownerB, "Deploy Org B")
	if err != nil {
		t.Fatalf("failed to create org B: %v", err)
	}
	projB, err := tc.service.CreateProject(ctx, orgB.ID, "Deploy Proj B")
	if err != nil {
		t.Fatalf("failed to create project B: %v", err)
	}
	envB, err := tc.service.CreateEnvironment(ctx, orgB.ID, projB.ID, tenant.EnvProduction, 10)
	if err != nil {
		t.Fatalf("failed to create env B: %v", err)
	}

	createDeployment := func(orgID, envID, hash string) string {
		var id string
		err := tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
			return tx.QueryRow(ctx, `
				INSERT INTO deployments (organization_id, environment_id, manifest_hash, bundle_digest, manifest, protocol_version, runtime_version)
				VALUES ($1, $2, $3, 'digest', '{}', 1, '1.0')
				RETURNING id::text
			`, orgID, envID, hash).Scan(&id)
		})
		if err != nil {
			t.Fatalf("failed to create deployment %s: %v", hash, err)
		}
		return id
	}

	deployA1 := createDeployment(orgA.ID, envA1.ID, "hash-a1")

	// Positive: same org + same env pins successfully.
	err = tc.pool.WithTenantTx(ctx, orgA.ID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO schedules (organization_id, environment_id, workflow_name, cron_expression, timezone, deployment_id)
			VALUES ($1, $2, 'pinned-ok', '0 * * * *', 'UTC', $3::uuid)
		`, orgA.ID, envA1.ID, deployA1)
		return err
	})
	if err != nil {
		t.Fatalf("valid pinned deployment reference was rejected: %v", err)
	}

	// Negative 1: nonexistent deployment.
	err = tc.pool.WithTenantTx(ctx, orgA.ID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO schedules (organization_id, environment_id, workflow_name, cron_expression, timezone, deployment_id)
			VALUES ($1, $2, 'pinned-ghost', '0 * * * *', 'UTC', '00000000-0000-0000-0000-000000000099'::uuid)
		`, orgA.ID, envA1.ID)
		return err
	})
	if err == nil {
		t.Fatalf("expected FK violation for nonexistent deployment_id, got nil")
	}
	if !strings.Contains(err.Error(), "foreign key") && !strings.Contains(err.Error(), "fk_schedules_pinned_deployment") {
		t.Fatalf("expected foreign key violation for nonexistent deployment, got: %v", err)
	}

	// Negative 2: another tenant's deployment.
	err = tc.pool.WithTenantTx(ctx, orgB.ID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO schedules (organization_id, environment_id, workflow_name, cron_expression, timezone, deployment_id)
			VALUES ($1, $2, 'pinned-cross-tenant', '0 * * * *', 'UTC', $3::uuid)
		`, orgB.ID, envB.ID, deployA1)
		return err
	})
	if err == nil {
		t.Fatalf("expected FK violation for cross-tenant deployment_id, got nil")
	}
	if !strings.Contains(err.Error(), "foreign key") && !strings.Contains(err.Error(), "fk_schedules_pinned_deployment") {
		t.Fatalf("expected foreign key violation for cross-tenant deployment, got: %v", err)
	}

	// Negative 3: same org but different environment.
	err = tc.pool.WithTenantTx(ctx, orgA.ID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO schedules (organization_id, environment_id, workflow_name, cron_expression, timezone, deployment_id)
			VALUES ($1, $2, 'pinned-cross-env', '0 * * * *', 'UTC', $3::uuid)
		`, orgA.ID, envA2.ID, deployA1)
		return err
	})
	if err == nil {
		t.Fatalf("expected FK violation for cross-environment deployment_id, got nil")
	}
	if !strings.Contains(err.Error(), "foreign key") && !strings.Contains(err.Error(), "fk_schedules_pinned_deployment") {
		t.Fatalf("expected foreign key violation for cross-environment deployment, got: %v", err)
	}
	t.Logf("deployment_id integrity enforced: nonexistent, cross-tenant, and cross-environment references all rejected")
}
