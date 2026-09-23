-- +goose Up
-- Issue #23: durable per-environment create-run token bucket. The row is
-- already the admission serialization point, so refill/debit is performed
-- while that row is locked by the caller.
ALTER TABLE environment_admissions
    ADD COLUMN IF NOT EXISTS create_rate_tokens DOUBLE PRECISION NOT NULL DEFAULT 10
        CHECK (create_rate_tokens >= 0 AND create_rate_tokens <= 10),
    ADD COLUMN IF NOT EXISTS create_rate_updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp();

-- +goose Down
ALTER TABLE environment_admissions
    DROP COLUMN IF EXISTS create_rate_updated_at,
    DROP COLUMN IF EXISTS create_rate_tokens;
