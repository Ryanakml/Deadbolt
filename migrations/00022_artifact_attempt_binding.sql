-- +goose Up
-- M2 scoped artifacts (Issue #22).
-- Additive only: bind each upload to the attempt that owns it so only valid
-- current ownership can finalize and associate a READY artifact. Nullable so
-- pre-existing rows (if any) remain valid; new reservations always set it.
ALTER TABLE artifacts
    ADD COLUMN IF NOT EXISTS attempt_id UUID;

CREATE INDEX IF NOT EXISTS idx_artifacts_attempt
    ON artifacts (attempt_id)
    WHERE attempt_id IS NOT NULL;

-- Orphan-collection sweep support.
CREATE INDEX IF NOT EXISTS idx_artifacts_gc
    ON artifacts (created_at)
    WHERE status IN ('PENDING_UPLOAD', 'READY');

-- +goose Down
DROP INDEX IF EXISTS idx_artifacts_gc;
DROP INDEX IF EXISTS idx_artifacts_attempt;
ALTER TABLE artifacts
    DROP COLUMN IF EXISTS attempt_id;
