-- +goose Up
-- M4: durable human approval decisions. The original V1 placeholder table is
-- extended additively so old databases can migrate without losing waiting work.
ALTER TABLE approvals
    ALTER COLUMN required_permission SET DEFAULT 'approvals:decide',
    ADD COLUMN IF NOT EXISTS decision_schema JSONB NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS comment TEXT,
    ADD COLUMN IF NOT EXISTS decision TEXT,
    ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 1;

ALTER TABLE approvals
    ADD CONSTRAINT approvals_decision_matches_status CHECK (
        (status = 'PENDING' AND decision IS NULL AND actor_id IS NULL AND decided_at IS NULL)
        OR (status IN ('APPROVED', 'REJECTED') AND decision = lower(status) AND actor_id IS NOT NULL AND decided_at IS NOT NULL)
        OR (status IN ('EXPIRED', 'CANCELLED') AND decision IS NULL)
    );

ALTER TABLE approvals
    ADD CONSTRAINT approvals_comment_length CHECK (comment IS NULL OR char_length(comment) <= 280);

CREATE INDEX IF NOT EXISTS idx_approvals_pending_expiry
    ON approvals (organization_id, expires_at, id) WHERE status = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_approvals_environment_status
    ON approvals (organization_id, environment_id, status, created_at DESC);

-- +goose Down
DROP INDEX IF EXISTS idx_approvals_environment_status;
DROP INDEX IF EXISTS idx_approvals_pending_expiry;
ALTER TABLE approvals DROP CONSTRAINT IF EXISTS approvals_comment_length;
ALTER TABLE approvals DROP CONSTRAINT IF EXISTS approvals_decision_matches_status;
ALTER TABLE approvals
    DROP COLUMN IF EXISTS revision,
    DROP COLUMN IF EXISTS decision,
    DROP COLUMN IF EXISTS comment,
    DROP COLUMN IF EXISTS decision_schema;
