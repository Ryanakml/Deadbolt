-- +goose Up
-- M2 deadlines and durable cancellation (Issue #20).
-- Additive only: run-level stop-confirmation flag. NULL means not settled
-- (active runs and runs cancelled before this migration).
ALTER TABLE runs
    ADD COLUMN IF NOT EXISTS termination_confirmed BOOLEAN;

-- Overdue-run sweep support: bounded scan for deadline enforcement.
CREATE INDEX IF NOT EXISTS idx_runs_deadline_overdue
    ON runs (deadline_at)
    WHERE deadline_at IS NOT NULL;

-- Stop-settlement sweep support: unacked stop commands by deadline.
CREATE INDEX IF NOT EXISTS idx_stop_commands_unacked
    ON stop_commands (deadline_at)
    WHERE acked_at IS NULL;

-- +goose Down
DROP INDEX IF EXISTS idx_stop_commands_unacked;
DROP INDEX IF EXISTS idx_runs_deadline_overdue;
ALTER TABLE runs
    DROP COLUMN IF EXISTS termination_confirmed;
