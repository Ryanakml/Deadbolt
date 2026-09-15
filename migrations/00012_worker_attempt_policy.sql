-- +goose Up
ALTER TABLE task_attempts
    ADD COLUMN attempt_timeout_ms INT NOT NULL DEFAULT 300000
        CHECK (attempt_timeout_ms > 0);

CREATE INDEX idx_task_attempts_session_active
    ON task_attempts (session_id, status)
    WHERE status IN ('CLAIMED', 'RUNNING');

-- +goose Down
DROP INDEX IF EXISTS idx_task_attempts_session_active;
ALTER TABLE task_attempts DROP COLUMN IF EXISTS attempt_timeout_ms;
