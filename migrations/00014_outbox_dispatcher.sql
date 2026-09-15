-- +goose Up
-- Outbox Dispatcher Security Definer Functions (Blueprint §11.1, §19.1, §24.3)
-- Narrowly scoped functions with fixed search_path = app, public, pg_temp, SECURITY DEFINER.

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION app.claim_outbox_batch(p_batch_size INT)
RETURNS TABLE (
    id UUID,
    organization_id UUID,
    event_id UUID,
    subject TEXT,
    payload JSONB,
    payload_version INT,
    attempts INT,
    next_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ
)
SECURITY DEFINER
SET search_path = app, public, pg_temp
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        o.id,
        o.organization_id,
        o.event_id,
        o.subject,
        o.payload,
        o.payload_version,
        o.attempts,
        o.next_at,
        o.created_at
    FROM outbox_events o
    WHERE o.published_at IS NULL
      AND o.next_at <= clock_timestamp()
    ORDER BY o.next_at ASC, o.id ASC
    LIMIT p_batch_size
    FOR UPDATE SKIP LOCKED;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION app.mark_outbox_published(p_id UUID)
RETURNS VOID
SECURITY DEFINER
SET search_path = app, public, pg_temp
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE outbox_events
    SET published_at = clock_timestamp()
    WHERE id = p_id;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION app.retry_outbox_event(p_id UUID, p_retry_delay INTERVAL)
RETURNS VOID
SECURITY DEFINER
SET search_path = app, public, pg_temp
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE outbox_events
    SET attempts = attempts + 1,
        next_at = clock_timestamp() + p_retry_delay
    WHERE id = p_id;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION app.get_outbox_metrics()
RETURNS TABLE (
    pending_count BIGINT,
    oldest_age_seconds DOUBLE PRECISION
)
SECURITY DEFINER
SET search_path = app, public, pg_temp
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        count(*)::bigint,
        COALESCE(EXTRACT(EPOCH FROM (clock_timestamp() - MIN(created_at))), 0.0)::double precision
    FROM outbox_events
    WHERE published_at IS NULL;
END;
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION app.claim_outbox_batch(INT) FROM PUBLIC;
REVOKE ALL ON FUNCTION app.mark_outbox_published(UUID) FROM PUBLIC;
REVOKE ALL ON FUNCTION app.retry_outbox_event(UUID, INTERVAL) FROM PUBLIC;
REVOKE ALL ON FUNCTION app.get_outbox_metrics() FROM PUBLIC;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'deadbolt_runtime') THEN
        GRANT EXECUTE ON FUNCTION app.claim_outbox_batch(INT) TO deadbolt_runtime;
        GRANT EXECUTE ON FUNCTION app.mark_outbox_published(UUID) TO deadbolt_runtime;
        GRANT EXECUTE ON FUNCTION app.retry_outbox_event(UUID, INTERVAL) TO deadbolt_runtime;
        GRANT EXECUTE ON FUNCTION app.get_outbox_metrics() TO deadbolt_runtime;
    END IF;
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'deadbolt_system') THEN
        GRANT EXECUTE ON FUNCTION app.claim_outbox_batch(INT) TO deadbolt_system;
        GRANT EXECUTE ON FUNCTION app.mark_outbox_published(UUID) TO deadbolt_system;
        GRANT EXECUTE ON FUNCTION app.retry_outbox_event(UUID, INTERVAL) TO deadbolt_system;
        GRANT EXECUTE ON FUNCTION app.get_outbox_metrics() TO deadbolt_system;
    END IF;
END $$;
-- +goose StatementEnd

-- +goose Down
DROP FUNCTION IF EXISTS app.get_outbox_metrics();
DROP FUNCTION IF EXISTS app.retry_outbox_event(UUID, INTERVAL);
DROP FUNCTION IF EXISTS app.mark_outbox_published(UUID);
DROP FUNCTION IF EXISTS app.claim_outbox_batch(INT);
