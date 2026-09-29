package scheduling

// Small helpers shared by the schedule service and the occurrence engine.

import (
	"context"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/storage"
)

// acquireAdmissionLock takes the environment admission advisory lock that
// §17 requires the scheduler to hold before the schedule lock. Every
// schedule mutation and every occurrence evaluation that can create a run
// takes it first, so the environment -> schedule -> run order is never
// inverted and the active-schedule cap and the create-run rate cap are
// decided under the same serialization point.
func acquireAdmissionLock(ctx context.Context, tx storage.Tx, envID string) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, envID+":SCHEDULE_ADMISSION"); err != nil {
		return err
	}
	return nil
}

// emptyToNil normalizes an optional pointer so an empty string is stored as
// SQL NULL rather than as an empty UUID, which the composite foreign key
// would reject.
func emptyToNil(v *string) *string {
	if v == nil {
		return nil
	}
	if *v == "" {
		return nil
	}
	return v
}

// nullableTime renders an optional timestamp for audit metadata.
func nullableTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC().Format(time.RFC3339Nano)
}
