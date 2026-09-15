-- +goose Up
-- Scope replay identities by operation. Existing run records predate the
-- explicit operation column and therefore belong to CREATE_RUN.
ALTER TABLE idempotency_records
    ADD COLUMN IF NOT EXISTS operation_type TEXT NOT NULL DEFAULT 'CREATE_RUN';

ALTER TABLE idempotency_records
    DROP CONSTRAINT IF EXISTS idempotency_records_environment_id_key_hash_key;

CREATE UNIQUE INDEX IF NOT EXISTS idempotency_records_operation_key_unique
    ON idempotency_records (environment_id, operation_type, key_hash);

-- A replay identity for an active run must never expire. Terminal run records
-- are retained by the cleanup job only after terminal_at + 30 days.
ALTER TABLE idempotency_records
    ALTER COLUMN expires_at SET DEFAULT 'infinity';

-- +goose Down
DROP INDEX IF EXISTS idempotency_records_operation_key_unique;
ALTER TABLE idempotency_records
    DROP COLUMN IF EXISTS operation_type;
