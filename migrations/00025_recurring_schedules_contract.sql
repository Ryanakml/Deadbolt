-- +goose Up
-- SP-05 and recurring schedules fixture contract (Blueprint §17, §18.1, §33 & Issue #30).
-- Additive enhancements for overlap/misfire policies, revision tracking, and due time indices.

ALTER TABLE schedules ADD COLUMN IF NOT EXISTS deployment_id UUID;
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS overlap_policy TEXT NOT NULL DEFAULT 'skip-overlap' CHECK (overlap_policy IN ('skip-overlap'));
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS misfire_policy TEXT NOT NULL DEFAULT 'coalesce-one' CHECK (misfire_policy IN ('coalesce-one'));
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS next_due_at TIMESTAMPTZ;
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS last_occurrence_at TIMESTAMPTZ;

ALTER TABLE schedule_occurrences ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 1;
ALTER TABLE schedule_occurrences ADD COLUMN IF NOT EXISTS skipped_reason TEXT CHECK (skipped_reason IN ('SKIPPED_OVERLAP', 'SKIPPED_QUOTA', 'SKIPPED_MISFIRE'));
ALTER TABLE schedule_occurrences ADD COLUMN IF NOT EXISTS skipped_count INT NOT NULL DEFAULT 0;

-- Blueprint §18.1: pinned-deployment relation must carry organization + environment scope.
-- The referenced (organization_id, environment_id, id) tuple requires an explicit composite
-- uniqueness on deployments before it can be referenced by a composite foreign key.
ALTER TABLE deployments ADD CONSTRAINT uq_deployments_org_env_id UNIQUE (organization_id, environment_id, id);

-- Schedules pin a deployment within the same organization AND environment.
-- NULL deployment_id remains allowed (unpinned schedule); non-NULL values must reference
-- an existing deployment in the same org+env. Nonexistent, cross-tenant, and
-- cross-environment references fail at the DB boundary.
ALTER TABLE schedules ADD CONSTRAINT fk_schedules_pinned_deployment
  FOREIGN KEY (organization_id, environment_id, deployment_id)
  REFERENCES deployments (organization_id, environment_id, id)
  ON DELETE RESTRICT;

-- Blueprint §17 / INV-10: canonical occurrence identity is (schedule_id, revision, due_at).
-- Free-form occurrence_key alone cannot prevent two callers supplying different strings
-- for the same logical occurrence, so enforce the canonical columns directly.
CREATE UNIQUE INDEX IF NOT EXISTS uq_schedule_occurrences_canonical_identity ON schedule_occurrences (schedule_id, revision, due_at);

CREATE INDEX IF NOT EXISTS idx_schedules_due ON schedules (environment_id, paused, next_due_at);
CREATE INDEX IF NOT EXISTS idx_schedule_occurrences_schedule ON schedule_occurrences (schedule_id, due_at DESC);

-- +goose Down
DROP INDEX IF EXISTS idx_schedule_occurrences_schedule;
DROP INDEX IF EXISTS idx_schedules_due;
DROP INDEX IF EXISTS uq_schedule_occurrences_canonical_identity;
ALTER TABLE schedules DROP CONSTRAINT IF EXISTS fk_schedules_pinned_deployment;
ALTER TABLE deployments DROP CONSTRAINT IF EXISTS uq_deployments_org_env_id;

ALTER TABLE schedule_occurrences DROP COLUMN IF EXISTS skipped_count;
ALTER TABLE schedule_occurrences DROP COLUMN IF EXISTS skipped_reason;
ALTER TABLE schedule_occurrences DROP COLUMN IF EXISTS revision;

ALTER TABLE schedules DROP COLUMN IF EXISTS last_occurrence_at;
ALTER TABLE schedules DROP COLUMN IF EXISTS next_due_at;
ALTER TABLE schedules DROP COLUMN IF EXISTS misfire_policy;
ALTER TABLE schedules DROP COLUMN IF EXISTS overlap_policy;
ALTER TABLE schedules DROP COLUMN IF EXISTS deployment_id;
