package scheduling

// Durable recurring schedules (Blueprint §17, Issue #34).
//
// The cron parser, DST handling, coalesce-one misfire policy, and
// skip-overlap policy already exist and are proven against the SP-05
// fixtures. This file adds the part that was missing: a scoped, audited
// service that persists a schedule definition and enforces the V1 limits
// around it. The occurrence transaction that consumes these definitions
// lives in engine.go.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
)

// MaxActiveSchedulesPerEnvironment is the V1 hard cap from Blueprint §27.1
// ("Active schedules per environment: 100"). A tenant may hold a lower
// limit, but the hard cap can never be raised through an API payload, so
// this constant is deliberately not configurable per request.
const MaxActiveSchedulesPerEnvironment = 100

// Canonical schedule error codes, surfaced verbatim to operators and
// mapped to stable API errors.
var (
	ErrScheduleNotFound      = errors.New("SCHEDULE_NOT_FOUND")
	ErrScheduleLimitReached  = errors.New("SCHEDULE_LIMIT_REACHED")
	ErrInvalidSchedule       = errors.New("INVALID_SCHEDULE")
	ErrInvalidCron           = errors.New("INVALID_CRON")
	ErrInvalidTimezone       = errors.New("INVALID_TIMEZONE")
	ErrInvalidWorkflow       = errors.New("INVALID_WORKFLOW")
	ErrPinnedDeployment      = errors.New("PINNED_DEPLOYMENT_INVALID")
	ErrScheduleAlreadyPaused = errors.New("SCHEDULE_ALREADY_PAUSED")
	ErrScheduleNotPaused     = errors.New("SCHEDULE_NOT_PAUSED")
	ErrRevisionConflict      = errors.New("REVISION_CONFLICT")
	ErrNoActiveDeployment    = errors.New("NO_ACTIVE_DEPLOYMENT")
	ErrInvalidCursor         = errors.New("INVALID_CURSOR")
)

// Schedule is a recurring schedule definition as stored in the database.
type Schedule struct {
	ID             string     `json:"id"`
	OrganizationID string     `json:"organizationId"`
	EnvironmentID  string     `json:"environmentId"`
	WorkflowName   string     `json:"workflowName"`
	CronExpression string     `json:"cronExpression"`
	Timezone       string     `json:"timezone"`
	Paused         bool       `json:"paused"`
	Revision       int64      `json:"revision"`
	DeploymentID   *string    `json:"deploymentId"`
	OverlapPolicy  string     `json:"overlapPolicy"`
	MisfirePolicy  string     `json:"misfirePolicy"`
	NextDueAt      *time.Time `json:"nextDueAt"`
	LastOccurrence *time.Time `json:"lastOccurrenceAt"`
	CreatedAt      time.Time  `json:"createdAt"`
	UpdatedAt      time.Time  `json:"updatedAt"`
}

// CreateScheduleRequest is the V1 create payload. DeploymentID is optional:
// when absent the occurrence pins whatever deployment is active at the time
// the occurrence fires, which is the §17 default.
type CreateScheduleRequest struct {
	WorkflowName   string  `json:"workflowName"`
	CronExpression string  `json:"cronExpression"`
	Timezone       string  `json:"timezone"`
	DeploymentID   *string `json:"deploymentId"`
}

// UpdateScheduleRequest edits a schedule. Any edit advances the revision,
// which by §17 affects only future occurrences: existing runs and already
// created occurrences keep the revision they were created under.
type UpdateScheduleRequest struct {
	ExpectedRevision int64   `json:"expectedRevision"`
	CronExpression   *string `json:"cronExpression"`
	Timezone         *string `json:"timezone"`
	DeploymentID     *string `json:"deploymentId"`
}

// ControlScheduleRequest pauses or resumes a schedule.
type ControlScheduleRequest struct {
	ExpectedRevision int64 `json:"expectedRevision"`
}

// Service persists and governs schedule definitions.
type Service struct {
	pool *storage.Pool
}

// NewService constructs the schedule service.
func NewService(pool *storage.Pool) *Service {
	return &Service{pool: pool}
}

// validate parses the cron expression and timezone through the SP-05
// evaluator, so an invalid definition is rejected at the API boundary
// rather than at evaluation time. It returns the spec so the caller can
// compute the first due time.
func validate(cronExpr, timezone string) (*ScheduleSpec, error) {
	if timezone == "" {
		timezone = "UTC"
	}
	spec, err := ParseSchedule(cronExpr, timezone)
	if err != nil {
		// ParseSchedule reports a bad zone and a bad expression the same way,
		// so classify here to keep the operator-facing reason codes distinct.
		if _, lerr := time.LoadLocation(timezone); lerr != nil {
			return nil, fmt.Errorf("%w: %s", ErrInvalidTimezone, timezone)
		}
		return nil, fmt.Errorf("%w: %s", ErrInvalidCron, cronExpr)
	}
	return spec, nil
}

// writeAudit records a schedule mutation. Schedule changes are operator
// actions, so they are audited the same way tenant resources are.
func writeAudit(ctx context.Context, tx storage.Tx, orgID, action, scheduleID string, audit *tenant.AuditContext, meta map[string]any) error {
	if audit == nil {
		audit = &tenant.AuditContext{}
	}
	actorID := audit.ActorID
	meta["actor_type"] = string(audit.ActorType)
	meta["actor_role"] = audit.Role
	if len(audit.Capabilities) > 0 {
		meta["actor_capabilities"] = audit.Capabilities
	}
	meta["actor_scope"] = "organization:" + orgID
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("failed to marshal schedule audit metadata: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO audit_events (
			organization_id, actor_id, action, target_type, target_id, correlation_id, reason, metadata
		) VALUES ($1, $2, $3, 'schedule', $4, $5, $6, $7)
	`, orgID, actorID, action, scheduleID, audit.CorrelationID, audit.Reason, metaBytes)
	if err != nil {
		return fmt.Errorf("failed to write audit event for %s: %w", action, err)
	}
	return nil
}

const scheduleColumns = `id::text, organization_id::text, environment_id::text, workflow_name,
	cron_expression, timezone, paused, revision, deployment_id::text, overlap_policy,
	misfire_policy, next_due_at, last_occurrence_at, created_at, updated_at`

func scanSchedule(row pgx.Row) (*Schedule, error) {
	var s Schedule
	err := row.Scan(&s.ID, &s.OrganizationID, &s.EnvironmentID, &s.WorkflowName,
		&s.CronExpression, &s.Timezone, &s.Paused, &s.Revision, &s.DeploymentID,
		&s.OverlapPolicy, &s.MisfirePolicy, &s.NextDueAt, &s.LastOccurrence,
		&s.CreatedAt, &s.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// isUniqueViolation reports whether err is a PostgreSQL unique constraint
// breach, optionally narrowed to a named index.
func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		return false
	}
	return constraint == "" || pgErr.ConstraintName == constraint
}
