-- +goose Up
-- Reconciliation observations are scheduler metadata, not workflow state. They
-- make bounded graph scans fair when early runs remain active but cannot yet be
-- advanced (for example, while another dependency is still running).
ALTER TABLE runs
    ADD COLUMN IF NOT EXISTS reconciliation_checked_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_runs_reconciliation_fair
    ON runs (organization_id, reconciliation_checked_at NULLS FIRST, id)
    WHERE status IN ('QUEUED', 'RUNNING');

-- +goose Down
DROP INDEX IF EXISTS idx_runs_reconciliation_fair;
ALTER TABLE runs
    DROP COLUMN IF EXISTS reconciliation_checked_at;
