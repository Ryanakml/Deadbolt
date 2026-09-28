-- +goose Up
-- M4 durable user delay nodes (Issue #33). Additive timer provenance only.
ALTER TABLE run_steps DROP CONSTRAINT IF EXISTS run_steps_completion_source_check;
ALTER TABLE run_steps ADD CONSTRAINT run_steps_completion_source_check
    CHECK (completion_source IN ('WORKER', 'RECONCILIATION', 'TIMER'));

CREATE INDEX IF NOT EXISTS idx_timers_delay_pending
    ON timers (organization_id, due_at, id)
    WHERE kind = 'DELAY' AND state = 'PENDING';

-- +goose Down
DROP INDEX IF EXISTS idx_timers_delay_pending;
-- Preserve rollback compatibility for databases that already settled a delay.
-- TIMER is V1 provenance; pre-27 rows only understood WORKER and RECONCILIATION.
UPDATE run_steps SET completion_source = 'RECONCILIATION'
WHERE completion_source = 'TIMER';
ALTER TABLE run_steps DROP CONSTRAINT IF EXISTS run_steps_completion_source_check;
ALTER TABLE run_steps ADD CONSTRAINT run_steps_completion_source_check
    CHECK (completion_source IN ('WORKER', 'RECONCILIATION'));
