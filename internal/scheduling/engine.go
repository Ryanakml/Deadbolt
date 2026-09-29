package scheduling

// Occurrence evaluation (Blueprint §17, Issue #34).
//
// One transaction decides one schedule: lock, choose the occurrence, insert
// it uniquely, resolve the deployment, apply overlap and quota, create the
// run, and advance the next due time. §17 is explicit that "create-run
// occurs in the same transaction that advances the schedule", and the lock
// order is environment admission, then schedule, then run — so two
// evaluators pointed at the same schedule serialize here rather than racing
// to create two runs for one slot.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/recovery"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
)

// Outcome is the per-schedule result of one evaluation pass.
type Outcome string

const (
	// OutcomeStarted means a run was created for the occurrence.
	OutcomeStarted Outcome = "STARTED"
	// OutcomeSkippedOverlap means the previous scheduled run was still
	// nonterminal, so §17's skip-overlap policy skipped this slot.
	OutcomeSkippedOverlap Outcome = "SKIPPED_OVERLAP"
	// OutcomeSkippedQuota means admission refused the run, so the slot was
	// recorded as skipped instead of being queued into a hidden backlog.
	OutcomeSkippedQuota Outcome = "SKIPPED_QUOTA"
	// OutcomeAlreadyClaimed means another evaluator won the unique
	// occurrence; this pass advanced the schedule and created nothing.
	OutcomeAlreadyClaimed Outcome = "ALREADY_CLAIMED"
	// OutcomePaused means the schedule is paused and was not evaluated.
	OutcomePaused Outcome = "PAUSED"
	// OutcomeErrorPaused means no usable deployment exists, so the schedule
	// was marked error and paused until an operator fixes it (§17).
	OutcomeErrorPaused Outcome = "ERROR_PAUSED"
	// OutcomeNotDue means nothing was due yet.
	OutcomeNotDue Outcome = "NOT_DUE"
)

// Engine evaluates due schedules and creates their runs.
type Engine struct {
	pool    *storage.Pool
	runs    *execution.Service
	tenants *tenant.Service
	logger  *log.Logger
}

// NewEngine constructs the occurrence engine. It reuses the execution
// service so a scheduled run takes the identical path as an
// operator-initiated one.
func NewEngine(pool *storage.Pool, runs *execution.Service, tenants *tenant.Service) *Engine {
	return &Engine{pool: pool, runs: runs, tenants: tenants}
}

// SetLogger attaches a logger. A schedule that cannot be evaluated is
// surfaced through it rather than passing in silence, which is how a
// permanently broken schedule would otherwise look identical to a healthy
// one that simply was not due.
func (e *Engine) SetLogger(l *log.Logger) { e.logger = l }

func (e *Engine) logf(format string, args ...any) {
	if e.logger != nil {
		e.logger.Printf(format, args...)
	}
}

// EvaluateDue processes every due schedule in an organization and returns
// the number of schedules it acted on.
func (e *Engine) EvaluateDue(ctx context.Context, orgID string) (int, error) {
	type due struct {
		scheduleID string
		envID      string
	}
	var candidates []due
	err := e.pool.WithTenantReadOnlyRepeatableReadTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id::text, environment_id::text FROM schedules
			WHERE organization_id=$1::uuid AND paused=false AND next_due_at IS NOT NULL
			  AND next_due_at <= clock_timestamp()
			ORDER BY next_due_at, id`, orgID)
		if err != nil {
			return fmt.Errorf("find due schedules: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var d due
			if err := rows.Scan(&d.scheduleID, &d.envID); err != nil {
				return fmt.Errorf("scan due schedule: %w", err)
			}
			candidates = append(candidates, d)
		}
		return rows.Err()
	})
	if err != nil {
		return 0, err
	}

	acted := 0
	for _, c := range candidates {
		outcome, err := e.evaluateOne(ctx, orgID, c.envID, c.scheduleID)
		if err != nil {
			// One broken schedule must not stop the others, but it must not
			// disappear either: an unlogged failure here is indistinguishable
			// from a schedule that was never due.
			e.logf("[SCHEDULER] Schedule %s in environment %s failed evaluation: %v", c.scheduleID, c.envID, err)
			acted++
			continue
		}
		if outcome != OutcomeNotDue && outcome != OutcomeAlreadyClaimed {
			acted++
		}
	}
	return acted, nil
}

// EvaluateSchedule evaluates one named schedule. Exposed for the operator
// trigger and for tests that need a deterministic entry point.
func (e *Engine) EvaluateSchedule(ctx context.Context, orgID, envID, scheduleID string) (Outcome, error) {
	return e.evaluateOne(ctx, orgID, envID, scheduleID)
}

func (e *Engine) evaluateOne(ctx context.Context, orgID, envID, scheduleID string) (Outcome, error) {
	var outcome Outcome
	err := e.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		// §17 order: environment admission, then schedule, then run.
		if err := acquireAdmissionLock(ctx, tx, envID); err != nil {
			return err
		}
		schedule, err := lockSchedule(ctx, tx, orgID, envID, scheduleID)
		if err != nil {
			return err
		}
		outcome, err = e.evaluateLocked(ctx, tx, orgID, envID, schedule)
		return err
	})
	return outcome, err
}

// evaluateLocked runs the whole decision while the schedule row is locked.
func (e *Engine) evaluateLocked(ctx context.Context, tx storage.Tx, orgID, envID string, schedule *Schedule) (Outcome, error) {
	spec, err := validate(schedule.CronExpression, schedule.Timezone)
	if err != nil {
		// A definition that no longer parses cannot be evaluated. §17 says a
		// broken schedule is marked error and paused until fixed, and the same
		// treatment is right for a definition the platform can no longer read.
		return e.markErrorPaused(ctx, tx, orgID, schedule, "INVALID_SCHEDULE")
	}

	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return OutcomeNotDue, fmt.Errorf("read database time: %w", err)
	}
	if schedule.NextDueAt == nil {
		return OutcomeNotDue, nil
	}
	if schedule.NextDueAt.After(now) {
		return OutcomeNotDue, nil
	}

	// Misfire policy. A schedule that fell behind creates at most one run
	// for the latest missed slot and records how many it coalesced, rather
	// than replaying a backlog after downtime.
	dueAt := *schedule.NextDueAt
	skippedCount := 0
	if schedule.LastOccurrence != nil && dueAt.Before(now) {
		coalesced, err := spec.CoalesceMissed(*schedule.LastOccurrence, now)
		if err != nil {
			return OutcomeNotDue, fmt.Errorf("coalesce missed occurrences: %w", err)
		}
		if coalesced.CoalescedOccurrence != nil {
			dueAt = *coalesced.CoalescedOccurrence
			skippedCount = coalesced.SkippedCount
		}
	}

	// Claim the occurrence. Two evaluators are made safe by unique identity on
	// (schedule_id, revision, due_at) from 00025 and on
	// (schedule_id, occurrence_key) from 00004, which are redundant with each
	// other but both enforced. The conflict target is therefore deliberately
	// unqualified: naming only one of them would let the other surface as a
	// unique violation instead of a no-op.
	occurrenceKey := FormatOccurrenceKey(schedule.ID, schedule.Revision, dueAt)
	var occurrenceID string
	err = tx.QueryRow(ctx, `
		INSERT INTO schedule_occurrences
			(organization_id, schedule_id, occurrence_key, due_at, revision, status, skipped_count)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7)
		ON CONFLICT DO NOTHING
		RETURNING id::text`,
		orgID, schedule.ID, occurrenceKey, dueAt, schedule.Revision,
		OccurrenceStatusPending, skippedCount).Scan(&occurrenceID)
	if errors.Is(err, pgx.ErrNoRows) {
		// Another evaluator already claimed this slot. Still advance the
		// schedule so the winner's commit is not undone, and create nothing.
		if err := e.advanceSchedule(ctx, tx, orgID, schedule, spec, dueAt); err != nil {
			return OutcomeNotDue, err
		}
		return OutcomeAlreadyClaimed, nil
	}
	if err != nil {
		return OutcomeNotDue, fmt.Errorf("insert occurrence: %w", err)
	}

	// Overlap policy: if the previous scheduled run is still nonterminal,
	// skip this slot rather than stacking another run behind it.
	prevStatus, err := e.previousScheduledRunStatus(ctx, tx, orgID, envID, schedule.ID)
	if err != nil {
		return OutcomeNotDue, err
	}
	action, skipReason := EvaluateOverlap(prevStatus)
	if action == "SKIP" {
		if err := e.skipOccurrence(ctx, tx, orgID, schedule, occurrenceID, dueAt, spec, skipReason); err != nil {
			return OutcomeNotDue, err
		}
		return OutcomeSkippedOverlap, nil
	}

	// Deployment: an explicit pin wins, otherwise the active deployment at
	// the time the occurrence fires.
	deploymentID, ok, err := e.resolveDeployment(ctx, tx, orgID, envID, schedule)
	if err != nil {
		return OutcomeNotDue, err
	}
	if !ok {
		return e.markErrorPaused(ctx, tx, orgID, schedule, "NO_ACTIVE_DEPLOYMENT")
	}

	env, err := e.tenants.GetEnvironment(ctx, orgID, envID)
	if err != nil {
		return OutcomeNotDue, fmt.Errorf("resolve environment: %w", err)
	}

	// A deterministic idempotency key per occurrence means a retry of this
	// same slot can never create a second run, even across a crash between
	// the run insert and the schedule advance.
	idemKey := "schedule:" + occurrenceKey
	audit := &tenant.AuditContext{
		ActorType:     tenant.IdentityTypeMachine,
		Reason:        "schedule occurrence",
		CorrelationID: occurrenceKey,
	}
	// §17 gives a schedule no input payload, so the run starts from an empty
	// object. It is canonicalized here rather than left nil because
	// runs.input is NOT NULL and CreateRunInTx expects the same pre-canonical
	// bytes an operator-initiated create would have produced.
	scheduledInput := map[string]any{}
	canonicalInput, err := contracts.CanonicalizeGeneric(scheduledInput)
	if err != nil {
		return OutcomeNotDue, fmt.Errorf("canonicalize scheduled input: %w", err)
	}
	run, _, err := e.runs.CreateRunInTx(ctx, tx, orgID, envID, env,
		schedule.WorkflowName, idemKey, idemKey, idemKey, &deploymentID, canonicalInput, scheduledInput, audit)

	// Quota and disaster-recovery admission refusals are recorded as a
	// skipped occurrence, not as a run failure and not as a queued backlog.
	if err != nil {
		if isAdmissionRefusal(err) {
			if skipErr := e.skipOccurrence(ctx, tx, orgID, schedule, occurrenceID, dueAt, spec, SkippedReasonQuota); skipErr != nil {
				return OutcomeNotDue, skipErr
			}
			if skipErr := recordOccurrenceAlert(ctx, tx, orgID, schedule, occurrenceID, SkippedReasonQuota); skipErr != nil {
				return OutcomeNotDue, skipErr
			}
			return OutcomeSkippedQuota, nil
		}
		if errors.Is(err, execution.ErrNoActiveDeployment) {
			return e.markErrorPaused(ctx, tx, orgID, schedule, "NO_ACTIVE_DEPLOYMENT")
		}
		// §17 marks a schedule that cannot produce a run as error and paused
		// until an operator fixes it. That covers more than a missing
		// deployment: a workflow whose input schema requires caller-supplied
		// fields can never be satisfied by a schedule, since §17 gives a
		// schedule no input payload, so retrying it every pass would spin
		// forever. Only conditions that are permanent for this definition are
		// paused; anything else is returned so the caller sees it and the
		// transaction rolls back rather than committing a silent no-op.
		var reason string
		switch {
		case errors.Is(err, execution.ErrSchemaViolation):
			reason = "WORKFLOW_REQUIRES_INPUT"
		case errors.Is(err, execution.ErrWorkflowNotFound):
			reason = "WORKFLOW_NOT_FOUND"
		case errors.Is(err, execution.ErrDeploymentNotFound):
			reason = "PINNED_DEPLOYMENT_INVALID"
		}
		if reason != "" {
			e.logf("[SCHEDULER] Schedule %s cannot create a run (%s): %v", schedule.ID, reason, err)
			return e.markErrorPaused(ctx, tx, orgID, schedule, reason)
		}
		return OutcomeNotDue, err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE schedule_occurrences SET status=$1, run_id=$2::uuid
		WHERE id=$3::uuid AND organization_id=$4::uuid`,
		OccurrenceStatusStarted, run.ID, occurrenceID, orgID); err != nil {
		return OutcomeNotDue, fmt.Errorf("link occurrence to run: %w", err)
	}

	if err := e.advanceSchedule(ctx, tx, orgID, schedule, spec, dueAt); err != nil {
		return OutcomeNotDue, err
	}
	return OutcomeStarted, nil
}

// isAdmissionRefusal reports whether err means admission declined the run
// rather than the request being invalid. Those slots are skipped with an
// alert instead of becoming a hidden backlog.
func isAdmissionRefusal(err error) bool {
	return errors.Is(err, execution.ErrRunQuotaExceeded) ||
		errors.Is(err, execution.ErrCreateRateLimited) ||
		errors.Is(err, recovery.ErrAdmissionDisabled)
}

// previousScheduledRunStatus finds the run this schedule most recently
// created, which is what the overlap policy compares against.
func (e *Engine) previousScheduledRunStatus(ctx context.Context, tx storage.Tx, orgID, envID, scheduleID string) (string, error) {
	var status *string
	err := tx.QueryRow(ctx, `
		SELECT r.status FROM runs r
		JOIN schedule_occurrences o ON o.run_id = r.id
		WHERE o.schedule_id=$1::uuid AND o.organization_id=$2::uuid
		ORDER BY o.due_at DESC LIMIT 1`, scheduleID, orgID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read previous scheduled run: %w", err)
	}
	if status == nil {
		return "", nil
	}
	return *status, nil
}

// resolveDeployment returns the deployment an occurrence should pin: the
// explicit pin when set, otherwise whatever is active right now. The second
// return is false when the workflow has no active deployment, which §17
// treats as an error that pauses the schedule.
func (e *Engine) resolveDeployment(ctx context.Context, tx storage.Tx, orgID, envID string, schedule *Schedule) (string, bool, error) {
	if schedule.DeploymentID != nil && *schedule.DeploymentID != "" {
		return *schedule.DeploymentID, true, nil
	}
	var id string
	err := tx.QueryRow(ctx, `
		SELECT d.id::text FROM workflow_channels c
		JOIN deployments d ON d.id = c.active_deployment_id AND d.organization_id = c.organization_id
		WHERE c.environment_id = $1::uuid AND c.organization_id = $2::uuid
		  AND c.workflow_name = $3`, envID, orgID, schedule.WorkflowName).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("resolve active deployment: %w", err)
	}
	return id, true, nil
}

// advanceSchedule moves next_due_at past the occurrence just handled and
// records last_occurrence_at, so the next pass starts from a clean point.
func (e *Engine) advanceSchedule(ctx context.Context, tx storage.Tx, orgID string, schedule *Schedule, spec *ScheduleSpec, handled time.Time) error {
	next, err := spec.NextOccurrence(handled)
	if err != nil {
		return fmt.Errorf("compute next occurrence: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE schedules
		SET next_due_at=$1, last_occurrence_at=$2, updated_at=clock_timestamp()
		WHERE id=$3::uuid AND organization_id=$4::uuid`,
		next, handled.UTC(), schedule.ID, orgID); err != nil {
		return fmt.Errorf("advance schedule: %w", err)
	}
	return nil
}

// skipOccurrence records a slot that was deliberately not run, and still
// advances the schedule so the slot is not retried forever.
func (e *Engine) skipOccurrence(ctx context.Context, tx storage.Tx, orgID string, schedule *Schedule, occurrenceID string, dueAt time.Time, spec *ScheduleSpec, reason string) error {
	if _, err := tx.Exec(ctx, `
		UPDATE schedule_occurrences
		SET status=$1, skipped_reason=$2
		WHERE id=$3::uuid AND organization_id=$4::uuid`,
		OccurrenceStatusSkipped, reason, occurrenceID, orgID); err != nil {
		return fmt.Errorf("mark occurrence skipped: %w", err)
	}
	return e.advanceSchedule(ctx, tx, orgID, schedule, spec, dueAt)
}

// markErrorPaused records the schedule as broken and stops it being
// evaluated until an operator fixes it (§17).
func (e *Engine) markErrorPaused(ctx context.Context, tx storage.Tx, orgID string, schedule *Schedule, reason string) (Outcome, error) {
	if _, err := tx.Exec(ctx, `
		UPDATE schedules
		SET paused=true, next_due_at=NULL, updated_at=clock_timestamp()
		WHERE id=$1::uuid AND organization_id=$2::uuid`, schedule.ID, orgID); err != nil {
		return OutcomeNotDue, fmt.Errorf("pause broken schedule: %w", err)
	}
	if err := writeAudit(ctx, tx, orgID, "schedule.error_pause", schedule.ID, &tenant.AuditContext{
		ActorType: tenant.IdentityTypeMachine,
		Reason:    reason,
	}, map[string]any{"reason": reason}); err != nil {
		return OutcomeNotDue, err
	}
	return OutcomeErrorPaused, nil
}

// recordOccurrenceAlert leaves an auditable trace of a skipped slot so an
// operator can see a schedule silently losing runs to quota.
func recordOccurrenceAlert(ctx context.Context, tx storage.Tx, orgID string, schedule *Schedule, occurrenceID, reason string) error {
	return writeAudit(ctx, tx, orgID, "schedule.occurrence_skipped", schedule.ID, &tenant.AuditContext{
		ActorType: tenant.IdentityTypeMachine,
		Reason:    reason,
	}, map[string]any{
		"occurrence_id":  occurrenceID,
		"skipped_reason": reason,
		"workflow_name":  schedule.WorkflowName,
	})
}
