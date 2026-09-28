-- M4 / Issue #32: human approval control nodes.
--
-- Approval is a control node, not a task (Blueprint §16.3). It never holds a
-- runner, a lease, or an attempt: it stores a durable PENDING decision that
-- survives process death and is decided by an identifiable human.
--
-- This migration is strictly additive so release N remains readable by the
-- N-1 control plane binary (Blueprint §26.3). No column or table is dropped,
-- renamed, or retyped.

-- +goose Up

-- The decision API contract returns a revision and requires `expectedRevision`.
-- Without a durable revision there is no way to reject a decision made from a
-- stale screen (Blueprint §16.3, §23.4).
ALTER TABLE approvals ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 1;

-- Approve/reject output carries the human comment (Blueprint §16.3). The
-- pre-existing `decision_reason` stays as the durable rejection explanation.
ALTER TABLE approvals ADD COLUMN IF NOT EXISTS decision_comment TEXT;

-- `approvals:write` was never a canonical capability (Blueprint §24.2 lists
-- `approvals:decide` as the only approval decision permission). Persisting an
-- ungrantable permission made the stored requirement meaningless.
ALTER TABLE approvals ALTER COLUMN required_permission SET DEFAULT 'approvals:decide';

-- Expiry sweeper: bounded, indexed lookup of due PENDING approvals. The partial
-- predicate keeps the index small because decided approvals are never re-swept.
CREATE INDEX IF NOT EXISTS idx_approvals_pending_expiry
    ON approvals (organization_id, expires_at)
    WHERE status = 'PENDING';

-- Inspector / approvals inbox listing for one environment.
CREATE INDEX IF NOT EXISTS idx_approvals_env_status
    ON approvals (organization_id, environment_id, status, created_at DESC);

-- +goose Down
DROP INDEX IF EXISTS idx_approvals_env_status;
DROP INDEX IF EXISTS idx_approvals_pending_expiry;
ALTER TABLE approvals ALTER COLUMN required_permission SET DEFAULT 'approvals:write';
ALTER TABLE approvals DROP COLUMN IF EXISTS decision_comment;
ALTER TABLE approvals DROP COLUMN IF EXISTS revision;
