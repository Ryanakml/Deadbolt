package scheduling

// Occurrence history reads (Blueprint §17, Issue #35).
//
// The occurrence engine in engine.go decides slots; this file exposes what
// it decided. Reads run inside a tenant-scoped read-only transaction —
// schedules carry row level security, so a bare pooled connection sees no
// rows — and a schedule in another environment or tenant is reported as
// not found rather than forbidden, matching the definition reads.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Ryanakml/Deadbolt/internal/storage"
)

// Occurrence is one decided slot: started, skipped, or still pending.
type Occurrence struct {
	ID            string    `json:"id"`
	ScheduleID    string    `json:"scheduleId"`
	DueAt         time.Time `json:"dueAt"`
	Revision      int64     `json:"revision"`
	Status        string    `json:"status"`
	SkippedReason *string   `json:"skippedReason"`
	SkippedCount  int       `json:"skippedCount"`
	RunID         *string   `json:"runId"`
	CreatedAt     time.Time `json:"createdAt"`
}

// OccurrencePage is one keyset page of occurrence history, newest first.
// NextCursor is nil when the page is the last one.
type OccurrencePage struct {
	Items      []Occurrence `json:"items"`
	NextCursor *string      `json:"nextCursor"`
}

// DefaultOccurrenceLimit is the page size when the caller passes none, and
// MaxOccurrenceLimit bounds a single read so history cannot be used to
// page the whole table in one request.
const (
	DefaultOccurrenceLimit = 25
	MaxOccurrenceLimit     = 100
)

// ListOccurrences returns occurrence history for one schedule, newest slot
// first. The cursor is an occurrence id from a previous page; an unknown
// cursor is INVALID_CURSOR rather than an empty page, so a caller holding
// a stale cursor notices instead of silently seeing nothing.
func (s *Service) ListOccurrences(ctx context.Context, orgID, envID, scheduleID string, cursor *string, limit int) (*OccurrencePage, error) {
	if limit <= 0 {
		limit = DefaultOccurrenceLimit
	}
	if limit > MaxOccurrenceLimit {
		limit = MaxOccurrenceLimit
	}
	page := &OccurrencePage{}
	err := s.pool.WithTenantReadOnlyRepeatableReadTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		var exists bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM schedules
				WHERE id=$1::uuid AND organization_id=$2::uuid AND environment_id=$3::uuid
			)`, scheduleID, orgID, envID).Scan(&exists); err != nil {
			return fmt.Errorf("verify schedule scope: %w", err)
		}
		if !exists {
			return fmt.Errorf("%w: schedule %s", ErrScheduleNotFound, scheduleID)
		}
		var anchorDue *time.Time
		var anchorID *string
		if cursor != nil && *cursor != "" {
			var due time.Time
			var id string
			err := tx.QueryRow(ctx, `
				SELECT due_at, id::text FROM schedule_occurrences
				WHERE id=$1::uuid AND organization_id=$2::uuid AND schedule_id=$3::uuid`,
				*cursor, orgID, scheduleID).Scan(&due, &id)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return fmt.Errorf("%w: unknown occurrence cursor", ErrInvalidCursor)
				}
				return fmt.Errorf("resolve occurrence cursor: %w", err)
			}
			anchorDue, anchorID = &due, &id
		}
		rows, err := tx.Query(ctx, `
			SELECT id::text, schedule_id::text, due_at, revision, status,
				skipped_reason, skipped_count, run_id::text, created_at
			FROM schedule_occurrences
			WHERE organization_id=$1::uuid AND schedule_id=$2::uuid
			  AND (($3::timestamptz IS NULL) OR (due_at, id) < ($3::timestamptz, $4::uuid))
			ORDER BY due_at DESC, id DESC
			LIMIT $5`, orgID, scheduleID, anchorDue, anchorID, limit+1)
		if err != nil {
			return fmt.Errorf("list occurrences: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var o Occurrence
			if err := rows.Scan(&o.ID, &o.ScheduleID, &o.DueAt, &o.Revision,
				&o.Status, &o.SkippedReason, &o.SkippedCount, &o.RunID, &o.CreatedAt); err != nil {
				return fmt.Errorf("scan occurrence: %w", err)
			}
			page.Items = append(page.Items, o)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("read occurrences: %w", err)
		}
		if len(page.Items) > limit {
			page.Items = page.Items[:limit]
			last := page.Items[len(page.Items)-1].ID
			page.NextCursor = &last
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if page.Items == nil {
		page.Items = []Occurrence{}
	}
	return page, nil
}
