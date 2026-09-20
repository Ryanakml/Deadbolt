-- +goose Up
-- M2 durable retry timers, budgets, and idempotency-window admission (Issue #18).
-- Additive only: new nullable columns + partial unique identity for PENDING
-- timers. Existing rows are untouched; no backfill required.
ALTER TABLE run_steps
    ADD COLUMN IF NOT EXISTS idempotency_valid_until TIMESTAMPTZ;

ALTER TABLE timers
    ADD COLUMN IF NOT EXISTS run_id UUID,
    ADD COLUMN IF NOT EXISTS step_id UUID,
    ADD COLUMN IF NOT EXISTS attempt_number INT,
    ADD COLUMN IF NOT EXISTS operation_id TEXT,
    ADD COLUMN IF NOT EXISTS reason TEXT,
    ADD COLUMN IF NOT EXISTS fired_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp();

-- One pending timer per logical action: (org, kind, reference_id).
-- FIRED/CANCELLED history rows are exempt so retries over time do not conflict.
CREATE UNIQUE INDEX IF NOT EXISTS idx_timers_pending_identity
    ON timers (organization_id, kind, reference_id)
    WHERE state = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_timers_step
    ON timers (step_id)
    WHERE step_id IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS idx_timers_step;
DROP INDEX IF EXISTS idx_timers_pending_identity;
ALTER TABLE timers
    DROP COLUMN IF EXISTS updated_at,
    DROP COLUMN IF EXISTS fired_at,
    DROP COLUMN IF EXISTS reason,
    DROP COLUMN IF EXISTS operation_id,
    DROP COLUMN IF EXISTS attempt_number,
    DROP COLUMN IF EXISTS step_id,
    DROP COLUMN IF EXISTS run_id;
ALTER TABLE run_steps
    DROP COLUMN IF EXISTS idempotency_valid_until;
