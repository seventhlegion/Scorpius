# Event Storage Schema

This PostgreSQL schema implements an append-only event store for the Scorpius exchange, supporting deterministic replay, idempotency, and crash recovery.

## Design Principles

### 1. Append-Only Model
- Events are **never updated or deleted** (immutable)
- All changes are represented as new events
- Enables complete audit trail and time-travel queries

### 2. Deterministic Replay
- Events are ordered by `sequence` number within each `stream_id`
- Sequence numbers are monotonically increasing and unique per stream
- Replay is deterministic: same events in same order = same state

### 3. Idempotency
- Global unique constraint on `event_id` (UUID)
- Business idempotency keys tracked in `idempotency_keys` table
- Duplicate events are rejected at the database level

### 4. Crash Recovery
- Transaction log tracks in-flight operations
- Sequence numbers are atomically incremented
- Can identify and recover from partial writes

## Schema Overview

### `events` Table
Main append-only event store. Stores both ledger and matching events as JSONB.

**Key Columns:**
- `stream_id`: Event stream identifier (e.g., `"ledger"`, `"matching:BTC/USDT"`)
- `sequence`: Monotonically increasing sequence number per stream
- `event_id`: Globally unique UUID for idempotency
- `category`: `'ledger'` or `'matching'`
- `event_type`: Event discriminator (e.g., `'funds_deposited'`, `'order_placed'`)
- `payload`: Complete event data as JSONB

**Constraints:**
- `UNIQUE (stream_id, sequence)` - ensures deterministic ordering
- `UNIQUE (event_id)` - ensures global idempotency

### `stream_sequences` Table
Tracks the current sequence number per event stream.

**Usage:**
- Atomically generates next sequence number
- Prevents race conditions in concurrent writes
- Used for crash recovery

### `idempotency_keys` Table
Tracks business idempotency keys (e.g., `order_id`, `trade_id`, `request_id`).

**Usage:**
- Fast lookup to check if an operation was already processed
- Prevents duplicate processing at application level
- Links business keys to event IDs

### `transaction_log` Table
Tracks in-flight transactions for crash recovery.

**Usage:**
- Identifies events written but not yet committed
- Helps recover from partial failures
- Can be cleaned up after successful processing

## Usage Patterns

### Writing Events

```sql
-- 1. Start a transaction
BEGIN;

-- 2. Get next sequence number
SELECT get_next_sequence_atomic('ledger') INTO next_seq;

-- 3. Insert event
INSERT INTO events (
    stream_id,
    sequence,
    event_type,
    category,
    event_id,
    schema_version,
    timestamp_ns,
    causation_id,
    correlation_id,
    source,
    payload
) VALUES (
    'ledger',
    next_seq,
    'funds_deposited',
    'ledger',
    '550e8400-e29b-41d4-a716-446655440000'::UUID,
    1,
    1699123456789000000,
    NULL,
    'correlation-123'::UUID,
    'ledger-service',
    '{"type": "funds_deposited", "metadata": {...}, "user_id": {...}, ...}'::JSONB
);

-- 4. Record idempotency key (if applicable)
INSERT INTO idempotency_keys (idempotency_key, category, event_id)
VALUES ('deposit-123', 'ledger', '550e8400-e29b-41d4-a716-446655440000'::UUID)
ON CONFLICT DO NOTHING;

-- 5. Commit transaction
COMMIT;
```

### Replaying Events

```sql
-- Replay all ledger events in order
SELECT * FROM ledger_events
ORDER BY sequence;

-- Replay events from a specific sequence
SELECT * FROM events
WHERE stream_id = 'ledger' AND sequence > 1000
ORDER BY sequence;

-- Replay events for a specific market
SELECT * FROM matching_events
WHERE stream_id = 'matching:BTC/USDT'
ORDER BY sequence;
```

### Idempotency Checks

```sql
-- Check if an event with this event_id already exists
SELECT EXISTS(
    SELECT 1 FROM events WHERE event_id = '550e8400-e29b-41d4-a716-446655440000'::UUID
);

-- Check if a business idempotency key was already processed
SELECT event_id FROM idempotency_keys
WHERE idempotency_key = 'order-123' AND category = 'matching';
```

### Querying Events

```sql
-- Find all events for a specific order
SELECT * FROM events
WHERE payload->>'order_id' = '123'
ORDER BY sequence;

-- Find all trades for a market
SELECT * FROM events
WHERE category = 'matching'
  AND event_type = 'trade_executed'
  AND stream_id = 'matching:BTC/USDT'
ORDER BY sequence;

-- Find events by correlation ID (distributed tracing)
SELECT * FROM events
WHERE correlation_id = 'correlation-123'::UUID
ORDER BY timestamp_ns;
```

## Stream ID Convention

**Ledger Events:**
- Stream ID: `"ledger"` (single stream for all ledger events)

**Matching Events:**
- Stream ID: `"matching:{MARKET_ID}"` (one stream per market)
- Example: `"matching:BTC/USDT"`, `"matching:ETH/USDT"`

This allows:
- Parallel processing of different markets
- Independent sequence numbers per market
- Efficient replay of specific markets

## Crash Recovery

### Scenario: Partial Write
If the application crashes after writing events but before committing:

1. **Identify pending transactions:**
   ```sql
   SELECT * FROM transaction_log
   WHERE status = 'pending'
   ORDER BY started_at;
   ```

2. **Check if events were written:**
   ```sql
   SELECT * FROM events
   WHERE event_id = ANY(SELECT unnest(event_ids) FROM transaction_log WHERE transaction_id = '...');
   ```

3. **Recover:**
   - If events exist: mark transaction as committed
   - If events missing: mark transaction as rolled back
   - Replay events to rebuild state

### Scenario: Sequence Number Gap
If sequence numbers have gaps (shouldn't happen, but can be detected):

```sql
-- Find gaps in sequence numbers
SELECT
    stream_id,
    sequence,
    LAG(sequence) OVER (PARTITION BY stream_id ORDER BY sequence) as prev_sequence,
    sequence - LAG(sequence) OVER (PARTITION BY stream_id ORDER BY sequence) as gap
FROM events
WHERE stream_id = 'ledger'
ORDER BY sequence;
```

## Performance Considerations

### Indexes
- `(stream_id, sequence)` - Primary replay path
- `event_id` - Idempotency checks
- `category`, `event_type` - Filtering
- `timestamp_ns` - Time-based queries
- `payload GIN` - JSONB queries

### Partitioning (Future)
For high-volume scenarios, consider partitioning `events` by:
- `category` (ledger vs matching)
- `stream_id` (per market)
- `created_at` (time-based)

### Archival
Old events can be archived to separate tables while maintaining replay capability:
- Keep sequence numbers continuous
- Archive events older than N days
- Maintain indexes on archived tables

## Schema Evolution

Events include `schema_version` to support evolution:

1. **Add new event types:** No schema change needed (JSONB is flexible)
2. **Modify event structure:** Increment `schema_version` in new events
3. **Replay compatibility:** Old events keep their original schema version
4. **Migration:** Write migration code that handles multiple schema versions

## Example: Complete Event Write Flow

```sql
BEGIN;

-- 1. Generate sequence number
SELECT get_next_sequence_atomic('ledger') INTO seq;

-- 2. Check idempotency
DO $$
DECLARE
    existing_event_id UUID;
BEGIN
    SELECT event_id INTO existing_event_id
    FROM idempotency_keys
    WHERE idempotency_key = 'order-123' AND category = 'ledger';

    IF existing_event_id IS NOT NULL THEN
        RAISE EXCEPTION 'Event already processed: %', existing_event_id;
    END IF;
END $$;

-- 3. Insert event
INSERT INTO events (...) VALUES (...);

-- 4. Record idempotency key
INSERT INTO idempotency_keys (...) VALUES (...);

-- 5. Update transaction log
INSERT INTO transaction_log (transaction_id, stream_id, event_ids, status)
VALUES (gen_random_uuid(), 'ledger', ARRAY['...'::UUID], 'committed');

COMMIT;
```
