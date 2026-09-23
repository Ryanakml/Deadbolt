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

CREATE INDEX IF NOT EXISTS idx_schedules_due ON schedules (environment_id, paused, next_due_at);
CREATE INDEX IF NOT EXISTS idx_schedule_occurrences_schedule ON schedule_occurrences (schedule_id, due_at DESC);

-- +goose Down
DROP INDEX IF EXISTS idx_schedule_occurrences_schedule;
DROP INDEX IF EXISTS idx_schedules_due;

ALTER TABLE schedule_occurrences DROP COLUMN IF EXISTS skipped_count;
ALTER TABLE schedule_occurrences DROP COLUMN IF EXISTS skipped_reason;
ALTER TABLE schedule_occurrences DROP COLUMN IF EXISTS revision;

ALTER TABLE schedules DROP COLUMN IF EXISTS last_occurrence_at;
ALTER TABLE schedules DROP COLUMN IF EXISTS next_due_at;
ALTER TABLE schedules DROP COLUMN IF EXISTS misfire_policy;
ALTER TABLE schedules DROP COLUMN IF EXISTS overlap_policy;
ALTER TABLE schedules DROP COLUMN IF EXISTS deployment_id;
