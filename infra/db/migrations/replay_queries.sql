-- SQL queries for event replay and state recovery
-- These queries are used by services to rebuild state on startup

-- ============================================================================
-- LEDGER EVENT REPLAY
-- ============================================================================

-- Replay all ledger events in sequence order
SELECT
    sequence,
    event_id,
    event_type,
    schema_version,
    timestamp_ns,
    causation_id,
    correlation_id,
    source,
    payload,
    created_at
FROM events
WHERE category = 'ledger'
ORDER BY sequence;

-- Replay ledger events from a specific sequence (for checkpoint-based replay)
SELECT
    sequence,
    event_id,
    event_type,
    payload
FROM events
WHERE category = 'ledger'
  AND sequence > $1  -- checkpoint_sequence
ORDER BY sequence;

-- Replay ledger events in batches (for large datasets)
SELECT
    sequence,
    event_id,
    event_type,
    payload
FROM events
WHERE category = 'ledger'
ORDER BY sequence
LIMIT $1  -- batch_size (e.g., 1000)
OFFSET $2;  -- offset

-- Get maximum sequence number for ledger (to verify replay completeness)
SELECT MAX(sequence) as max_sequence
FROM events
WHERE category = 'ledger';

-- Count total ledger events
SELECT COUNT(*) as total_events
FROM events
WHERE category = 'ledger';

-- ============================================================================
-- MATCHING EVENT REPLAY
-- ============================================================================

-- Replay all events for a specific market
SELECT
    sequence,
    event_id,
    event_type,
    schema_version,
    timestamp_ns,
    causation_id,
    correlation_id,
    source,
    payload,
    created_at
FROM events
WHERE stream_id = $1  -- 'matching:BTC/USDT'
ORDER BY sequence;

-- Replay matching events from a checkpoint
SELECT
    sequence,
    event_id,
    event_type,
    payload
FROM events
WHERE stream_id = $1  -- stream_id
  AND sequence > $2   -- checkpoint_sequence
ORDER BY sequence;

-- Get all active markets (for multi-market replay)
SELECT DISTINCT
    SUBSTRING(stream_id FROM 'matching:(.*)') as market_id
FROM events
WHERE stream_id LIKE 'matching:%'
ORDER BY market_id;

-- Get maximum trade_id from matching events (for next_trade_id initialization)
SELECT MAX((payload->>'trade_id')::bigint) as max_trade_id
FROM events
WHERE category = 'matching'
  AND event_type = 'trade_executed'
  AND stream_id = $1;  -- stream_id

-- Alternative: extract trade_id from metadata if stored differently
-- This depends on your JSONB structure
SELECT MAX(
    CASE
        WHEN payload->'metadata'->>'trade_id' IS NOT NULL
        THEN (payload->'metadata'->>'trade_id')::bigint
        ELSE NULL
    END
) as max_trade_id
FROM events
WHERE category = 'matching'
  AND event_type = 'trade_executed'
  AND stream_id = $1;

-- ============================================================================
-- IDEMPOTENCY CHECKS DURING REPLAY
-- ============================================================================

-- Check if event_id was already processed (for idempotency)
SELECT EXISTS(
    SELECT 1 FROM events
    WHERE event_id = $1::UUID
) as already_processed;

-- Get all processed idempotency keys for a category
SELECT idempotency_key, event_id, created_at
FROM idempotency_keys
WHERE category = $1  -- 'ledger' or 'matching'
ORDER BY created_at;

-- Check if a specific idempotency key exists
SELECT event_id
FROM idempotency_keys
WHERE idempotency_key = $1
  AND category = $2;

-- ============================================================================
-- STATE VERIFICATION QUERIES
-- ============================================================================

-- Verify sequence number consistency (no gaps)
WITH sequence_gaps AS (
    SELECT
        stream_id,
        sequence,
        LAG(sequence) OVER (PARTITION BY stream_id ORDER BY sequence) as prev_sequence,
        sequence - LAG(sequence) OVER (PARTITION BY stream_id ORDER BY sequence) as gap
    FROM events
    WHERE category = $1  -- 'ledger' or 'matching'
)
SELECT
    stream_id,
    sequence,
    prev_sequence,
    gap
FROM sequence_gaps
WHERE gap > 1 OR (prev_sequence IS NULL AND sequence > 1)
ORDER BY stream_id, sequence;

-- Verify event_id uniqueness (should always be unique)
SELECT
    event_id,
    COUNT(*) as count
FROM events
GROUP BY event_id
HAVING COUNT(*) > 1;

-- Verify stream sequence consistency
SELECT
    ss.stream_id,
    ss.last_sequence as tracked_sequence,
    MAX(e.sequence) as actual_max_sequence,
    COUNT(e.id) as total_events,
    CASE
        WHEN ss.last_sequence != MAX(e.sequence) THEN 'INCONSISTENT'
        ELSE 'OK'
    END as status
FROM stream_sequences ss
LEFT JOIN events e ON e.stream_id = ss.stream_id
WHERE ss.stream_id LIKE $1  -- 'ledger' or 'matching:%'
GROUP BY ss.stream_id, ss.last_sequence
ORDER BY ss.stream_id;

-- ============================================================================
-- CHECKPOINT MANAGEMENT
-- ============================================================================

-- Create a checkpoint table (optional, for faster recovery)
-- CREATE TABLE replay_checkpoints (
--     service_name VARCHAR(100) PRIMARY KEY,
--     stream_id VARCHAR(255) NOT NULL,
--     last_sequence BIGINT NOT NULL,
--     checkpoint_data JSONB,  -- Serialized state snapshot
--     created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
-- );

-- Save checkpoint
-- INSERT INTO replay_checkpoints (service_name, stream_id, last_sequence, checkpoint_data)
-- VALUES ($1, $2, $3, $4::JSONB)
-- ON CONFLICT (service_name) DO UPDATE
-- SET stream_id = EXCLUDED.stream_id,
--     last_sequence = EXCLUDED.last_sequence,
--     checkpoint_data = EXCLUDED.checkpoint_data,
--     created_at = NOW();

-- Load checkpoint
-- SELECT stream_id, last_sequence, checkpoint_data
-- FROM replay_checkpoints
-- WHERE service_name = $1;

-- ============================================================================
-- PERFORMANCE OPTIMIZATION QUERIES
-- ============================================================================

-- Get event count per stream (for planning batch sizes)
SELECT
    stream_id,
    COUNT(*) as event_count,
    MIN(sequence) as min_sequence,
    MAX(sequence) as max_sequence,
    MAX(sequence) - MIN(sequence) + 1 as sequence_range
FROM events
GROUP BY stream_id
ORDER BY event_count DESC;

-- Estimate replay time (if you track replay duration)
-- This would require additional instrumentation
-- SELECT
--     stream_id,
--     COUNT(*) as events,
--     AVG(replay_duration_ms) as avg_duration_per_event_ms,
--     COUNT(*) * AVG(replay_duration_ms) as estimated_total_ms
-- FROM replay_metrics
-- GROUP BY stream_id;

-- ============================================================================
-- MONITORING QUERIES
-- ============================================================================

-- Events processed per minute (for monitoring replay progress)
SELECT
    DATE_TRUNC('minute', created_at) as minute,
    category,
    COUNT(*) as events_per_minute
FROM events
WHERE created_at >= NOW() - INTERVAL '1 hour'
GROUP BY minute, category
ORDER BY minute DESC, category;

-- Replay progress tracking (if implementing progress logging)
-- This would require a separate progress table
-- CREATE TABLE replay_progress (
--     service_name VARCHAR(100),
--     stream_id VARCHAR(255),
--     current_sequence BIGINT,
--     total_events BIGINT,
--     started_at TIMESTAMPTZ,
--     updated_at TIMESTAMPTZ,
--     PRIMARY KEY (service_name, stream_id)
-- );

-- Update replay progress
-- INSERT INTO replay_progress (service_name, stream_id, current_sequence, total_events, started_at)
-- VALUES ($1, $2, $3, $4, NOW())
-- ON CONFLICT (service_name, stream_id) DO UPDATE
-- SET current_sequence = EXCLUDED.current_sequence,
--     updated_at = NOW();

-- ============================================================================
-- ERROR RECOVERY QUERIES
-- ============================================================================

-- Find events that failed to replay (if tracking failures)
-- This would require a replay_errors table
-- CREATE TABLE replay_errors (
--     id BIGSERIAL PRIMARY KEY,
--     event_id UUID NOT NULL,
--     stream_id VARCHAR(255) NOT NULL,
--     sequence BIGINT NOT NULL,
--     error_message TEXT,
--     retry_count INTEGER DEFAULT 0,
--     created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
-- );

-- Find events that need retry
-- SELECT e.*
-- FROM events e
-- JOIN replay_errors re ON re.event_id = e.event_id
-- WHERE re.retry_count < 3
-- ORDER BY re.created_at;

-- ============================================================================
-- EXAMPLE: COMPLETE REPLAY QUERY WITH VALIDATION
-- ============================================================================

-- Complete replay with validation (for ledger)
WITH event_sequence AS (
    SELECT
        sequence,
        event_id,
        event_type,
        payload,
        LAG(sequence) OVER (ORDER BY sequence) as prev_sequence,
        sequence - LAG(sequence) OVER (ORDER BY sequence) as gap
    FROM events
    WHERE category = 'ledger'
    ORDER BY sequence
)
SELECT
    sequence,
    event_id,
    event_type,
    payload,
    CASE
        WHEN gap > 1 THEN 'GAP_DETECTED'
        WHEN prev_sequence IS NULL AND sequence > 1 THEN 'MISSING_INITIAL'
        ELSE 'OK'
    END as validation_status
FROM event_sequence
ORDER BY sequence;
