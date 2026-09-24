-- +goose Up
-- M3 Pause and resume durable runs with revision-safe control actions (Issue #28).
-- Additive only: run-level pause requested durable flag.
ALTER TABLE runs
    ADD COLUMN IF NOT EXISTS pause_requested BOOLEAN NOT NULL DEFAULT false;

-- Index for active pause-requested runs
CREATE INDEX IF NOT EXISTS idx_runs_pause_requested
    ON runs (organization_id, pause_requested)
    WHERE pause_requested = true;

-- +goose Down
DROP INDEX IF EXISTS idx_runs_pause_requested;
ALTER TABLE runs
    DROP COLUMN IF EXISTS pause_requested;
