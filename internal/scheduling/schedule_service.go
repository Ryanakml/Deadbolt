package scheduling

// Schedule definition mutations: create, edit, pause, resume, delete.
// The occurrence transaction that consumes these definitions is in
// engine.go; everything here is about establishing and governing a
// definition, within the environment admission lock so the active-schedule
// cap cannot be raced past by two concurrent creates.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
)

// Create registers a recurring schedule and computes its first due time.
//
// The active-schedule cap is checked while holding the environment
// admission lock, so two concurrent creates in the same environment cannot
// both observe 99 and both insert, ending above the hard cap.
func (s *Service) Create(ctx context.Context, orgID, envID string, req CreateScheduleRequest, audit *tenant.AuditContext) (*Schedule, error) {
	if req.WorkflowName == "" {
		return nil, fmt.Errorf("%w: workflowName is required", ErrInvalidWorkflow)
	}
	spec, err := validate(req.CronExpression, req.Timezone)
	if err != nil {
		return nil, err
	}
	timezone := req.Timezone
	if timezone == "" {
		timezone = "UTC"
	}

	var created *Schedule
	err = s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		// §17: the scheduler takes the environment admission lock before the
		// schedule lock. Creating a definition is an admission decision, so
		// it takes the same lock the evaluator will later take.
		if err := acquireAdmissionLock(ctx, tx, envID); err != nil {
			return err
		}

		// A pinned deployment must live in this organization and environment.
		// The composite foreign key from 00025 enforces it at the database
		// boundary, so an invalid or cross-tenant pin can never be stored.
		if req.DeploymentID != nil && *req.DeploymentID != "" {
			var ok bool
			if err := tx.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1 FROM deployments
					WHERE id=$1::uuid AND organization_id=$2::uuid AND environment_id=$3::uuid
				)`, *req.DeploymentID, orgID, envID).Scan(&ok); err != nil {
				return fmt.Errorf("failed to verify pinned deployment: %w", err)
			}
			if !ok {
				return fmt.Errorf("%w: deployment %s is not in this organization and environment", ErrPinnedDeployment, *req.DeploymentID)
			}
		}

		var activeCount int
		if err := tx.QueryRow(ctx, `
			SELECT count(*) FROM schedules
			WHERE environment_id=$1::uuid AND paused=false`, envID).Scan(&activeCount); err != nil {
			return fmt.Errorf("failed to count active schedules: %w", err)
		}
		if activeCount >= MaxActiveSchedulesPerEnvironment {
			return fmt.Errorf("%w: environment already holds %d active schedules (limit %d)",
				ErrScheduleLimitReached, activeCount, MaxActiveSchedulesPerEnvironment)
		}

		// The first due time is computed from database time so a schedule
		// created on a skewed host still anchors to server time.
		var nextDue time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&nextDue); err != nil {
			return fmt.Errorf("failed to read database time: %w", err)
		}
		first, err := spec.NextOccurrence(nextDue)
		if err != nil {
			return fmt.Errorf("%w: cannot compute first occurrence: %v", ErrInvalidCron, err)
		}

		row := tx.QueryRow(ctx, `
			INSERT INTO schedules (
				organization_id, environment_id, workflow_name, cron_expression,
				timezone, deployment_id, overlap_policy, misfire_policy, next_due_at
			) VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9)
			RETURNING `+scheduleColumns,
			orgID, envID, req.WorkflowName, req.CronExpression, timezone,
			emptyToNil(req.DeploymentID), OverlapPolicySkipOverlap, MisfirePolicyCoalesceOne, first)
		created, err = scanSchedule(row)
		if err != nil {
			return fmt.Errorf("failed to create schedule: %w", err)
		}

		return writeAudit(ctx, tx, orgID, "schedule.create", created.ID, audit, map[string]any{
			"workflow_name":     created.WorkflowName,
			"cron_expression":   created.CronExpression,
			"timezone":          created.Timezone,
			"deployment_pinned": created.DeploymentID != nil,
			"first_due_at":      first.UTC().Format(time.RFC3339Nano),
			"revision":          created.Revision,
		})
	})
	if err != nil {
		return nil, err
	}
	return created, nil
}

// Update edits a definition. Every accepted edit advances the revision, and
// by §17 that affects only future occurrences: occurrences already created
// keep their own revision, and runs they produced are untouched.
func (s *Service) Update(ctx context.Context, orgID, envID, scheduleID string, req UpdateScheduleRequest, audit *tenant.AuditContext) (*Schedule, error) {
	var updated *Schedule
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		current, err := lockSchedule(ctx, tx, orgID, envID, scheduleID)
		if err != nil {
			return err
		}
		if req.ExpectedRevision != 0 && current.Revision != req.ExpectedRevision {
			return fmt.Errorf("%w: schedule is at revision %d", ErrRevisionConflict, current.Revision)
		}

		cronExpr := current.CronExpression
		if req.CronExpression != nil {
			cronExpr = *req.CronExpression
		}
		timezone := current.Timezone
		if req.Timezone != nil {
			timezone = *req.Timezone
		}
		spec, err := validate(cronExpr, timezone)
		if err != nil {
			return err
		}
		if timezone == "" {
			timezone = "UTC"
		}

		deploymentID := current.DeploymentID
		if req.DeploymentID != nil {
			deploymentID = emptyToNil(req.DeploymentID)
			if deploymentID != nil {
				var ok bool
				if err := tx.QueryRow(ctx, `
					SELECT EXISTS (
						SELECT 1 FROM deployments
						WHERE id=$1::uuid AND organization_id=$2::uuid AND environment_id=$3::uuid
					)`, *deploymentID, orgID, envID).Scan(&ok); err != nil {
					return fmt.Errorf("failed to verify pinned deployment: %w", err)
				}
				if !ok {
					return fmt.Errorf("%w: deployment %s is not in this organization and environment", ErrPinnedDeployment, *deploymentID)
				}
			}
		}

		// Recompute the next due time for the new expression, but only if the
		// schedule is live: a paused schedule has no pending due time to
		// advance, and resume will compute it then.
		var nextDue any
		if !current.Paused {
			var now time.Time
			if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
				return fmt.Errorf("failed to read database time: %w", err)
			}
			first, err := spec.NextOccurrence(now)
			if err != nil {
				return fmt.Errorf("%w: cannot compute next occurrence: %v", ErrInvalidCron, err)
			}
			nextDue = first
		}

		row := tx.QueryRow(ctx, `
			UPDATE schedules SET
				cron_expression=$1, timezone=$2, deployment_id=$3,
				revision=revision+1, next_due_at=$4, updated_at=clock_timestamp()
			WHERE id=$5::uuid AND organization_id=$6::uuid
			RETURNING `+scheduleColumns,
			cronExpr, timezone, deploymentID, nextDue, scheduleID, orgID)
		updated, err = scanSchedule(row)
		if err != nil {
			return fmt.Errorf("failed to update schedule: %w", err)
		}

		return writeAudit(ctx, tx, orgID, "schedule.update", scheduleID, audit, map[string]any{
			"cron_expression": updated.CronExpression,
			"timezone":        updated.Timezone,
			"revision_from":   current.Revision,
			"revision_to":     updated.Revision,
			"note":            "applies to future occurrences only",
		})
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

// Pause stops future occurrences without touching anything already
// created. It is idempotent in intent but reports the state it found so a
// caller can distinguish a real transition from a no-op.
func (s *Service) Pause(ctx context.Context, orgID, envID, scheduleID string, req ControlScheduleRequest, audit *tenant.AuditContext) (*Schedule, error) {
	return s.setPaused(ctx, orgID, envID, scheduleID, req, true, audit)
}

// Resume re-arms a paused schedule and computes its next due time from
// database time at the moment of resume, not from the pause instant.
func (s *Service) Resume(ctx context.Context, orgID, envID, scheduleID string, req ControlScheduleRequest, audit *tenant.AuditContext) (*Schedule, error) {
	return s.setPaused(ctx, orgID, envID, scheduleID, req, false, audit)
}

func (s *Service) setPaused(ctx context.Context, orgID, envID, scheduleID string, req ControlScheduleRequest, paused bool, audit *tenant.AuditContext) (*Schedule, error) {
	var updated *Schedule
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		// Pausing can free an active slot, and resuming can consume one, so
		// the cap is decided under the same admission lock the evaluator uses.
		if err := acquireAdmissionLock(ctx, tx, envID); err != nil {
			return err
		}
		current, err := lockSchedule(ctx, tx, orgID, envID, scheduleID)
		if err != nil {
			return err
		}
		if req.ExpectedRevision != 0 && current.Revision != req.ExpectedRevision {
			return fmt.Errorf("%w: schedule is at revision %d", ErrRevisionConflict, current.Revision)
		}
		if current.Paused == paused {
			if paused {
				return ErrScheduleAlreadyPaused
			}
			return ErrScheduleNotPaused
		}

		var nextDue any
		action := "schedule.resume"
		if paused {
			action = "schedule.pause"
			nextDue = nil
		} else {
			var activeCount int
			if err := tx.QueryRow(ctx, `
				SELECT count(*) FROM schedules
				WHERE environment_id=$1::uuid AND paused=false`, envID).Scan(&activeCount); err != nil {
				return fmt.Errorf("failed to count active schedules: %w", err)
			}
			if activeCount >= MaxActiveSchedulesPerEnvironment {
				return fmt.Errorf("%w: environment already holds %d active schedules (limit %d)",
					ErrScheduleLimitReached, activeCount, MaxActiveSchedulesPerEnvironment)
			}
			spec, err := validate(current.CronExpression, current.Timezone)
			if err != nil {
				return err
			}
			var now time.Time
			if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
				return fmt.Errorf("failed to read database time: %w", err)
			}
			first, err := spec.NextOccurrence(now)
			if err != nil {
				return fmt.Errorf("%w: cannot compute next occurrence: %v", ErrInvalidCron, err)
			}
			nextDue = first
		}

		row := tx.QueryRow(ctx, `
			UPDATE schedules SET paused=$1, next_due_at=$2, updated_at=clock_timestamp()
			WHERE id=$3::uuid AND organization_id=$4::uuid
			RETURNING `+scheduleColumns,
			paused, nextDue, scheduleID, orgID)
		updated, err = scanSchedule(row)
		if err != nil {
			return fmt.Errorf("failed to update schedule: %w", err)
		}
		return writeAudit(ctx, tx, orgID, action, scheduleID, audit, map[string]any{
			"paused":      paused,
			"revision":    updated.Revision,
			"next_due_at": nullableTime(updated.NextDueAt),
		})
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

// Delete removes a definition. Occurrences are kept as history by the
// foreign key, and running executions are unaffected.
func (s *Service) Delete(ctx context.Context, orgID, envID, scheduleID string, audit *tenant.AuditContext) error {
	return s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		if _, err := lockSchedule(ctx, tx, orgID, envID, scheduleID); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `
			DELETE FROM schedules WHERE id=$1::uuid AND organization_id=$2::uuid`, scheduleID, orgID)
		if err != nil {
			return fmt.Errorf("failed to delete schedule: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return ErrScheduleNotFound
		}
		return writeAudit(ctx, tx, orgID, "schedule.delete", scheduleID, audit, map[string]any{})
	})
}

// Get returns one schedule scoped to the organization and environment.
// The read runs inside a tenant-scoped transaction because schedules carry
// row level security: a bare pooled connection has no organization context
// and would see no rows at all.
func (s *Service) Get(ctx context.Context, orgID, envID, scheduleID string) (*Schedule, error) {
	var schedule *Schedule
	err := s.pool.WithTenantReadOnlyRepeatableReadTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		row := tx.QueryRow(ctx, `
			SELECT `+scheduleColumns+` FROM schedules
			WHERE id=$1::uuid AND organization_id=$2::uuid AND environment_id=$3::uuid`,
			scheduleID, orgID, envID)
		scanned, err := scanSchedule(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrScheduleNotFound
		}
		if err != nil {
			return fmt.Errorf("failed to read schedule: %w", err)
		}
		schedule = scanned
		return nil
	})
	if err != nil {
		return nil, err
	}
	return schedule, nil
}

// List returns schedules for an environment, newest first.
func (s *Service) List(ctx context.Context, orgID, envID string) ([]Schedule, error) {
	var out []Schedule
	err := s.pool.WithTenantReadOnlyRepeatableReadTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT `+scheduleColumns+` FROM schedules
			WHERE organization_id=$1::uuid AND environment_id=$2::uuid
			ORDER BY created_at DESC, id`, orgID, envID)
		if err != nil {
			return fmt.Errorf("failed to list schedules: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var sch Schedule
			if err := rows.Scan(&sch.ID, &sch.OrganizationID, &sch.EnvironmentID, &sch.WorkflowName,
				&sch.CronExpression, &sch.Timezone, &sch.Paused, &sch.Revision, &sch.DeploymentID,
				&sch.OverlapPolicy, &sch.MisfirePolicy, &sch.NextDueAt, &sch.LastOccurrence,
				&sch.CreatedAt, &sch.UpdatedAt); err != nil {
				return fmt.Errorf("failed to scan schedule: %w", err)
			}
			out = append(out, sch)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// lockSchedule takes the per-schedule lock §17 requires before any create-run
// or state change, and reads the current row. The environment admission lock
// must already be held by the caller, preserving environment -> schedule order.
func lockSchedule(ctx context.Context, tx storage.Tx, orgID, envID, scheduleID string) (*Schedule, error) {
	row := tx.QueryRow(ctx, `
		SELECT `+scheduleColumns+` FROM schedules
		WHERE id=$1::uuid AND organization_id=$2::uuid AND environment_id=$3::uuid
		FOR UPDATE`, scheduleID, orgID, envID)
	schedule, err := scanSchedule(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrScheduleNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to lock schedule: %w", err)
	}
	return schedule, nil
}
