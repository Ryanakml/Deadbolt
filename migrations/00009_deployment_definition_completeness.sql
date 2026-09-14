-- +goose Up
-- Keep normalized definitions complete enough for recovery and result mapping.
ALTER TABLE task_definitions ADD COLUMN idempotency_window_ms BIGINT;
ALTER TABLE workflow_definitions ADD COLUMN output_mapping JSONB NOT NULL DEFAULT '{}'::jsonb;

-- +goose Down
ALTER TABLE workflow_definitions DROP COLUMN IF EXISTS output_mapping;
ALTER TABLE task_definitions DROP COLUMN IF EXISTS idempotency_window_ms;
