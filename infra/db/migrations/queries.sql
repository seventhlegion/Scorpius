-- Common queries and utilities for event store
-- These queries can be used for monitoring, debugging, and operations

-- ============================================================================
-- STREAM MANAGEMENT
-- ============================================================================

-- Get current sequence number for all streams
SELECT
    stream_id,
    last_sequence,
    updated_at,
    (SELECT COUNT(*) FROM events WHERE events.stream_id = stream_sequences.stream_id) as actual_count
FROM stream_sequences
ORDER BY stream_id;

-- Get sequence number for a specific stream
SELECT last_sequence
FROM stream_sequences
WHERE stream_id = 'ledger';

-- Reset sequence number (USE WITH CAUTION - only for testing/development)
-- UPDATE stream_sequences SET last_sequence = 0 WHERE stream_id = 'ledger';

-- ============================================================================
-- EVENT REPLAY QUERIES
-- ============================================================================

-- Replay all ledger events
SELECT
    sequence,
    event_type,
    event_id,
    timestamp_ns,
    payload
FROM events
WHERE category = 'ledger'
ORDER BY sequence;

-- Replay events from a specific sequence number
SELECT *
FROM events
WHERE stream_id = 'ledger'
  AND sequence >= 1000
ORDER BY sequence
LIMIT 1000;

-- Replay events for a specific market
SELECT *
FROM events
WHERE stream_id = 'matching:BTC/USDT'
ORDER BY sequence;

-- Replay events within a time range
SELECT *
FROM events
WHERE timestamp_ns >= 1699123456789000000
  AND timestamp_ns <= 1699123556789000000
ORDER BY stream_id, sequence;

-- ============================================================================
-- IDEMPOTENCY CHECKS
-- ============================================================================

-- Check if event_id exists
SELECT EXISTS(
    SELECT 1 FROM events
    WHERE event_id = '550e8400-e29b-41d4-a716-446655440000'::UUID
) as exists;

-- Check if idempotency key exists
SELECT event_id, created_at
FROM idempotency_keys
WHERE idempotency_key = 'order-123'
  AND category = 'matching';

-- Find all idempotency keys for a specific event
SELECT idempotency_key, category, created_at
FROM idempotency_keys
WHERE event_id = '550e8400-e29b-41d4-a716-446655440000'::UUID;

-- ============================================================================
-- EVENT QUERIES BY PAYLOAD
-- ============================================================================

-- Find events for a specific order_id
SELECT
    sequence,
    event_type,
    event_id,
    timestamp_ns,
    payload
FROM events
WHERE payload->>'order_id' = '123'
ORDER BY sequence;

-- Find all trades for a user
SELECT *
FROM events
WHERE category = 'matching'
  AND event_type = 'trade_executed'
  AND (payload->>'buyer_id' = '456' OR payload->>'seller_id' = '456')
ORDER BY timestamp_ns;

-- Find all deposit events for a user
SELECT *
FROM events
WHERE category = 'ledger'
  AND event_type = 'funds_deposited'
  AND payload->'user_id'->>'0' = '456'
ORDER BY timestamp_ns;

-- Find events by correlation ID (distributed tracing)
SELECT
    sequence,
    event_type,
    category,
    event_id,
    timestamp_ns,
    payload
FROM events
WHERE correlation_id = 'correlation-123'::UUID
ORDER BY timestamp_ns;

-- ============================================================================
-- STATISTICS AND MONITORING
-- ============================================================================

-- Event counts by category
SELECT
    category,
    COUNT(*) as total_events,
    MIN(sequence) as min_sequence,
    MAX(sequence) as max_sequence,
    MIN(timestamp_ns) as first_event,
    MAX(timestamp_ns) as last_event
FROM events
GROUP BY category;

-- Event counts by stream
SELECT
    stream_id,
    COUNT(*) as total_events,
    MIN(sequence) as min_sequence,
    MAX(sequence) as max_sequence
FROM events
GROUP BY stream_id
ORDER BY stream_id;

-- Event counts by type
SELECT
    category,
    event_type,
    COUNT(*) as count,
    MIN(timestamp_ns) as first_seen,
    MAX(timestamp_ns) as last_seen
FROM events
GROUP BY category, event_type
ORDER BY category, count DESC;

-- Events per hour (for monitoring throughput)
SELECT
    DATE_TRUNC('hour', TO_TIMESTAMP(timestamp_ns / 1000000000.0)) as hour,
    category,
    COUNT(*) as events_per_hour
FROM events
WHERE timestamp_ns >= EXTRACT(EPOCH FROM NOW() - INTERVAL '24 hours') * 1000000000
GROUP BY hour, category
ORDER BY hour DESC, category;

-- ============================================================================
-- DATA INTEGRITY CHECKS
-- ============================================================================

-- Check for sequence gaps
WITH sequence_gaps AS (
    SELECT
        stream_id,
        sequence,
        LAG(sequence) OVER (PARTITION BY stream_id ORDER BY sequence) as prev_sequence,
        sequence - LAG(sequence) OVER (PARTITION BY stream_id ORDER BY sequence) as gap
    FROM events
    WHERE stream_id = 'ledger'
)
SELECT *
FROM sequence_gaps
WHERE gap > 1
ORDER BY sequence;

-- Check for duplicate event_ids (should never happen)
SELECT
    event_id,
    COUNT(*) as count
FROM events
GROUP BY event_id
HAVING COUNT(*) > 1;

-- Check sequence number consistency
SELECT
    ss.stream_id,
    ss.last_sequence as tracked_sequence,
    MAX(e.sequence) as actual_max_sequence,
    COUNT(e.id) as total_events
FROM stream_sequences ss
LEFT JOIN events e ON e.stream_id = ss.stream_id
GROUP BY ss.stream_id, ss.last_sequence
HAVING ss.last_sequence != MAX(e.sequence) OR MAX(e.sequence) IS NULL;

-- Check for events with invalid timestamps
SELECT
    event_id,
    event_type,
    timestamp_ns,
    TO_TIMESTAMP(timestamp_ns / 1000000000.0) as timestamp_readable
FROM events
WHERE timestamp_ns < 0
   OR timestamp_ns > EXTRACT(EPOCH FROM NOW() + INTERVAL '1 year') * 1000000000;

-- ============================================================================
-- CRASH RECOVERY QUERIES
-- ============================================================================

-- Find pending transactions
SELECT
    transaction_id,
    stream_id,
    event_ids,
    started_at,
    NOW() - started_at as age
FROM transaction_log
WHERE status = 'pending'
ORDER BY started_at;

-- Find events associated with pending transactions
SELECT
    tl.transaction_id,
    e.event_id,
    e.event_type,
    e.sequence,
    e.created_at
FROM transaction_log tl
CROSS JOIN LATERAL unnest(tl.event_ids) AS event_id
JOIN events e ON e.event_id = event_id
WHERE tl.status = 'pending'
ORDER BY tl.started_at, e.sequence;

-- Clean up old completed transactions (older than 7 days)
-- DELETE FROM transaction_log
-- WHERE status IN ('committed', 'rolled_back')
--   AND completed_at < NOW() - INTERVAL '7 days';

-- ============================================================================
-- MAINTENANCE QUERIES
-- ============================================================================

-- Get table sizes
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
  AND tablename IN ('events', 'stream_sequences', 'idempotency_keys', 'transaction_log')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Get index sizes
SELECT
    indexname,
    pg_size_pretty(pg_relation_size(indexname::regclass)) AS size
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename = 'events'
ORDER BY pg_relation_size(indexname::regclass) DESC;

-- Analyze table statistics
-- ANALYZE events;
-- ANALYZE stream_sequences;
-- ANALYZE idempotency_keys;

-- ============================================================================
-- ARCHIVAL QUERIES (for future use)
-- ============================================================================

-- Find events older than 90 days (candidate for archival)
SELECT
    stream_id,
    COUNT(*) as old_events,
    MIN(sequence) as min_sequence,
    MAX(sequence) as max_sequence
FROM events
WHERE created_at < NOW() - INTERVAL '90 days'
GROUP BY stream_id;

-- Get events for archival (example: events older than 90 days)
-- CREATE TABLE events_archive (LIKE events INCLUDING ALL);
-- INSERT INTO events_archive
-- SELECT * FROM events
-- WHERE created_at < NOW() - INTERVAL '90 days';
-- DELETE FROM events WHERE created_at < NOW() - INTERVAL '90 days';
