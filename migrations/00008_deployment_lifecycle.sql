-- +goose Up
-- Deployment content is immutable.  Availability is an observed worker fact;
-- ACTIVE is the channel pointer and never changes runs already created.
ALTER TABLE deployments
    ADD COLUMN status TEXT NOT NULL DEFAULT 'REGISTERED'
        CHECK (status IN ('REGISTERED', 'AVAILABLE', 'ACTIVE'));

CREATE UNIQUE INDEX deployments_environment_bundle_digest_unique
    ON deployments (environment_id, bundle_digest);

CREATE INDEX worker_deployments_bundle_digest_idx
    ON worker_deployments (bundle_digest, session_id);

-- +goose Down
DROP INDEX IF EXISTS worker_deployments_bundle_digest_idx;
DROP INDEX IF EXISTS deployments_environment_bundle_digest_unique;
ALTER TABLE deployments DROP COLUMN IF EXISTS status;
