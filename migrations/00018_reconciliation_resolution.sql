-- +goose Up
-- M2 audited reconciliation resolution (Issue #19).
-- Additive only: completion provenance on run_steps. Existing NULL rows mean
-- worker/normal-path completion and remain valid under the CHECK.
ALTER TABLE run_steps
    ADD COLUMN IF NOT EXISTS completion_source TEXT
        CHECK (completion_source IN ('WORKER', 'RECONCILIATION'));

-- +goose Down
ALTER TABLE run_steps
    DROP COLUMN IF EXISTS completion_source;
