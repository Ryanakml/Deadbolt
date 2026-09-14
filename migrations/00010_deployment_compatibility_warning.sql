-- +goose Up
ALTER TABLE deployments ADD COLUMN compatibility_warning_at TIMESTAMPTZ;

-- +goose Down
ALTER TABLE deployments DROP COLUMN IF EXISTS compatibility_warning_at;
