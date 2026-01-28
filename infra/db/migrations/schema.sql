-- Event Sourcing Storage Schema for Scorpius Exchange
-- Supports deterministic replay, idempotency, and crash recovery

-- ============================================================================
-- EVENT STORAGE
-- ============================================================================

-- Main events table: append-only storage for all events
CREATE TABLE events (
    -- Primary key: sequence number within the event stream
    id BIGSERIAL PRIMARY KEY,

    -- Event stream identifier (e.g., 'ledger', 'matching:BTC/USDT')
    -- Allows separate sequence numbers per stream for parallel processing
    stream_id VARCHAR(255) NOT NULL,

    -- Sequential event number within the stream (for deterministic replay)
    -- Must be monotonically increasing per stream
    sequence BIGINT NOT NULL,

    -- Event type discriminator (e.g., 'funds_deposited', 'order_placed', 'trade_executed')
    event_type VARCHAR(100) NOT NULL,

    -- Event category: 'ledger' or 'matching'
    category VARCHAR(50) NOT NULL CHECK (category IN ('ledger', 'matching')),

    -- Unique event identifier (UUID v4) for idempotency checks
    event_id UUID NOT NULL,

    -- Schema version for this event type (enables schema evolution)
    schema_version INTEGER NOT NULL DEFAULT 1,

    -- Timestamp when event occurred (Unix epoch nanoseconds)
    timestamp_ns BIGINT NOT NULL,

    -- Causation ID: event/request that caused this event (for tracing)
    causation_id UUID,

    -- Correlation ID: groups related events across services (for distributed tracing)
    correlation_id UUID,

    -- Source service/component that generated the event
    source VARCHAR(100) NOT NULL,

    -- Full event payload as JSONB (stores the complete event data)
    payload JSONB NOT NULL,

    -- When the event was written to the database (for audit)
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Ensure sequence numbers are unique per stream
    UNIQUE (stream_id, sequence),

    -- Ensure event_id is globally unique for idempotency
    UNIQUE (event_id)
);

-- Indexes for efficient queries and replay
CREATE INDEX idx_events_stream_sequence ON events(stream_id, sequence);
CREATE INDEX idx_events_category ON events(category);
CREATE INDEX idx_events_event_type ON events(event_type);
CREATE INDEX idx_events_timestamp ON events(timestamp_ns);
CREATE INDEX idx_events_correlation_id ON events(correlation_id) WHERE correlation_id IS NOT NULL;
CREATE INDEX idx_events_causation_id ON events(causation_id) WHERE causation_id IS NOT NULL;
CREATE INDEX idx_events_created_at ON events(created_at);

-- GIN index for JSONB payload queries (useful for filtering by event data)
CREATE INDEX idx_events_payload_gin ON events USING GIN (payload);

-- ============================================================================
-- STREAM SEQUENCE TRACKING
-- ============================================================================

-- Tracks the current sequence number per stream
-- Used for generating next sequence numbers and crash recovery
CREATE TABLE stream_sequences (
    stream_id VARCHAR(255) PRIMARY KEY,
    last_sequence BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- IDEMPOTENCY TRACKING
-- ============================================================================

-- Tracks idempotency keys to prevent duplicate processing
-- Separate from events table for faster lookups
CREATE TABLE idempotency_keys (
    -- Business idempotency key (e.g., order_id, trade_id, request_id)
    idempotency_key VARCHAR(255) NOT NULL,

    -- Event category
    category VARCHAR(50) NOT NULL CHECK (category IN ('ledger', 'matching')),

    -- Associated event_id
    event_id UUID NOT NULL REFERENCES events(event_id) ON DELETE RESTRICT,

    -- When the idempotency key was first seen
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (idempotency_key, category)
);

CREATE INDEX idx_idempotency_event_id ON idempotency_keys(event_id);

-- ============================================================================
-- TRANSACTION LOG (for crash recovery)
-- ============================================================================

-- Tracks in-flight transactions for crash recovery
-- Helps identify events that were written but not yet processed
CREATE TABLE transaction_log (
    id BIGSERIAL PRIMARY KEY,
    transaction_id UUID NOT NULL UNIQUE,
    stream_id VARCHAR(255) NOT NULL,
    event_ids UUID[] NOT NULL,
    status VARCHAR(50) NOT NULL CHECK (status IN ('pending', 'committed', 'rolled_back')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,

    FOREIGN KEY (stream_id) REFERENCES stream_sequences(stream_id)
);

CREATE INDEX idx_transaction_log_status ON transaction_log(status) WHERE status = 'pending';
CREATE INDEX idx_transaction_log_stream ON transaction_log(stream_id, started_at);

-- ============================================================================
-- FUNCTIONS AND TRIGGERS
-- ============================================================================

-- Function to get and increment sequence number for a stream
-- Uses SELECT FOR UPDATE to prevent race conditions
CREATE OR REPLACE FUNCTION get_next_sequence(p_stream_id VARCHAR(255))
RETURNS BIGINT AS $$
DECLARE
    next_seq BIGINT;
BEGIN
    INSERT INTO stream_sequences (stream_id, last_sequence)
    VALUES (p_stream_id, 1)
    ON CONFLICT (stream_id) DO UPDATE
    SET last_sequence = stream_sequences.last_sequence + 1,
        updated_at = NOW()
    RETURNING last_sequence INTO next_seq;

    RETURN next_seq;
END;
$$ LANGUAGE plpgsql;

-- Function to atomically get next sequence (for use in transactions)
CREATE OR REPLACE FUNCTION get_next_sequence_atomic(p_stream_id VARCHAR(255))
RETURNS BIGINT AS $$
DECLARE
    next_seq BIGINT;
BEGIN
    UPDATE stream_sequences
    SET last_sequence = last_sequence + 1,
        updated_at = NOW()
    WHERE stream_id = p_stream_id
    RETURNING last_sequence INTO next_seq;

    IF next_seq IS NULL THEN
        INSERT INTO stream_sequences (stream_id, last_sequence)
        VALUES (p_stream_id, 1)
        RETURNING last_sequence INTO next_seq;
    END IF;

    RETURN next_seq;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- VIEWS FOR COMMON QUERIES
-- ============================================================================

-- View for ledger events only
CREATE VIEW ledger_events AS
SELECT
    id,
    stream_id,
    sequence,
    event_type,
    event_id,
    schema_version,
    timestamp_ns,
    causation_id,
    correlation_id,
    source,
    payload,
    created_at
FROM events
WHERE category = 'ledger'
ORDER BY stream_id, sequence;

-- View for matching events only
CREATE VIEW matching_events AS
SELECT
    id,
    stream_id,
    sequence,
    event_type,
    event_id,
    schema_version,
    timestamp_ns,
    causation_id,
    correlation_id,
    source,
    payload,
    created_at
FROM events
WHERE category = 'matching'
ORDER BY stream_id, sequence;

-- View for events by stream with latest sequence
CREATE VIEW stream_summary AS
SELECT
    ss.stream_id,
    ss.last_sequence,
    ss.updated_at,
    COUNT(e.id) as total_events,
    MIN(e.timestamp_ns) as first_event_time,
    MAX(e.timestamp_ns) as last_event_time
FROM stream_sequences ss
LEFT JOIN events e ON e.stream_id = ss.stream_id
GROUP BY ss.stream_id, ss.last_sequence, ss.updated_at;

-- ============================================================================
-- BALANCE READ MODEL
-- ============================================================================

-- Read-only balance snapshot table derived exclusively from ledger events.
-- This read model:
-- - Is rebuilt from scratch by replaying all ledger events
-- - Never validates or rejects events (validation is done in write model)
-- - Provides fast queries for user balances without replaying entire event stream
-- - Can be safely dropped and recreated at any time

CREATE TABLE balance_snapshots (
    -- Composite primary key: (user_id, asset)
    user_id BIGINT NOT NULL,
    asset VARCHAR(50) NOT NULL,

    -- Available balance (funds that can be used for trading/withdrawal)
    available_amount BIGINT NOT NULL DEFAULT 0,
    available_scale INTEGER NOT NULL DEFAULT 0,

    -- Locked balance (funds reserved for active orders)
    locked_amount BIGINT NOT NULL DEFAULT 0,
    locked_scale INTEGER NOT NULL DEFAULT 0,

    -- Metadata for tracking projection state
    last_processed_event_id UUID NOT NULL,
    last_processed_sequence BIGINT NOT NULL,
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (user_id, asset)
);

-- Indexes for efficient queries
CREATE INDEX idx_balance_snapshots_user_id ON balance_snapshots(user_id);
CREATE INDEX idx_balance_snapshots_asset ON balance_snapshots(asset);
CREATE INDEX idx_balance_snapshots_last_sequence ON balance_snapshots(last_processed_sequence);

-- View for easy querying with total balance
CREATE VIEW balance_totals AS
SELECT
    user_id,
    asset,
    available_amount,
    available_scale,
    locked_amount,
    locked_scale,
    available_amount + locked_amount AS total_amount,
    available_scale AS total_scale,
    last_processed_event_id,
    last_processed_sequence,
    last_updated_at
FROM balance_snapshots;

-- ============================================================================
-- CONSTRAINTS AND VALIDATION
-- ============================================================================

-- Ensure sequence numbers are positive
ALTER TABLE events ADD CONSTRAINT check_sequence_positive CHECK (sequence > 0);

-- Ensure timestamp is reasonable (not negative, not too far in future)
ALTER TABLE events ADD CONSTRAINT check_timestamp_reasonable
    CHECK (timestamp_ns >= 0 AND timestamp_ns <= EXTRACT(EPOCH FROM NOW() + INTERVAL '1 year') * 1000000000);

-- Ensure balance amounts are non-negative (read model constraint, not validation)
ALTER TABLE balance_snapshots ADD CONSTRAINT check_balance_non_negative
    CHECK (available_amount >= 0 AND locked_amount >= 0);

-- ============================================================================
-- TRADE READ MODEL
-- ============================================================================

-- Read-only trade table derived exclusively from matching engine events.
-- This read model:
-- - Is append-only: trades are immutable once created
-- - Is rebuilt from scratch by replaying all matching events
-- - Never validates or rejects events (validation is done in write model)
-- - Provides fast queries for trade history without replaying entire event stream
-- - Can be safely dropped and recreated at any time
-- - Fully auditable: complete history of all trades

CREATE TABLE trades (
    -- Primary key: trade_id
    trade_id BIGINT NOT NULL PRIMARY KEY,

    -- Market information
    market_id BIGINT NOT NULL,

    -- Order information
    maker_order_id BIGINT NOT NULL,
    taker_order_id BIGINT NOT NULL,

    -- User information
    buyer_id BIGINT NOT NULL,
    seller_id BIGINT NOT NULL,

    -- Trade execution details
    price_amount BIGINT NOT NULL,
    price_scale INTEGER NOT NULL,
    quantity_amount BIGINT NOT NULL,
    quantity_scale INTEGER NOT NULL,
    quote_amount_amount BIGINT NOT NULL,
    quote_amount_scale INTEGER NOT NULL,

    -- Timestamp when trade occurred (from event metadata)
    executed_at_ns BIGINT NOT NULL,

    -- Metadata for tracking projection state
    event_id UUID NOT NULL,
    event_sequence BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Ensure trade_id is unique (idempotency)
    UNIQUE (trade_id)
);

-- Indexes for efficient queries
CREATE INDEX idx_trades_market_id ON trades(market_id);
CREATE INDEX idx_trades_buyer_id ON trades(buyer_id);
CREATE INDEX idx_trades_seller_id ON trades(seller_id);
CREATE INDEX idx_trades_maker_order_id ON trades(maker_order_id);
CREATE INDEX idx_trades_taker_order_id ON trades(taker_order_id);
CREATE INDEX idx_trades_executed_at ON trades(executed_at_ns);
CREATE INDEX idx_trades_event_sequence ON trades(event_sequence);
CREATE INDEX idx_trades_buyer_market ON trades(buyer_id, market_id);
CREATE INDEX idx_trades_seller_market ON trades(seller_id, market_id);

-- View for easy querying with formatted amounts
CREATE VIEW trade_details AS
SELECT
    trade_id,
    market_id,
    maker_order_id,
    taker_order_id,
    buyer_id,
    seller_id,
    price_amount,
    price_scale,
    quantity_amount,
    quantity_scale,
    quote_amount_amount,
    quote_amount_scale,
    executed_at_ns,
    event_id,
    event_sequence,
    created_at
FROM trades;

-- ============================================================================
-- ORDER READ MODEL
-- ============================================================================

-- Read-only order snapshot table derived exclusively from matching engine events.
-- This read model:
-- - Is rebuilt from scratch by replaying all matching events
-- - Never validates or rejects events (validation is done in write model)
-- - Never used for matching or orderbook operations (read-only)
-- - Provides fast queries for order status without replaying entire event stream
-- - Can be safely dropped and recreated at any time
-- - Tracks order lifecycle states: New, PartiallyFilled, Filled, Cancelled

CREATE TABLE order_snapshots (
    -- Primary key: order_id
    order_id BIGINT NOT NULL PRIMARY KEY,

    -- Market and user information
    market_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,

    -- Order details
    side VARCHAR(10) NOT NULL CHECK (side IN ('Buy', 'Sell')),
    order_type VARCHAR(10) NOT NULL CHECK (order_type IN ('Limit', 'Market')),

    -- Price (NULL for market orders)
    price_amount BIGINT,
    price_scale INTEGER,

    -- Quantity and fill information
    quantity_amount BIGINT NOT NULL,
    quantity_scale INTEGER NOT NULL,
    filled_quantity_amount BIGINT NOT NULL DEFAULT 0,
    filled_quantity_scale INTEGER NOT NULL DEFAULT 0,

    -- Order status: New, PartiallyFilled, Filled, Cancelled
    status VARCHAR(20) NOT NULL CHECK (status IN ('New', 'PartiallyFilled', 'Filled', 'Cancelled')),

    -- Rejection reason (if status is effectively rejected, though OrderRejected events are tracked separately)
    rejection_reason VARCHAR(255),

    -- Timestamps
    created_at_ns BIGINT NOT NULL,
    updated_at_ns BIGINT NOT NULL,

    -- Metadata for tracking projection state
    last_processed_event_id UUID NOT NULL,
    last_processed_sequence BIGINT NOT NULL,
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for efficient queries
CREATE INDEX idx_order_snapshots_user_id ON order_snapshots(user_id);
CREATE INDEX idx_order_snapshots_market_id ON order_snapshots(market_id);
CREATE INDEX idx_order_snapshots_status ON order_snapshots(status);
CREATE INDEX idx_order_snapshots_user_market ON order_snapshots(user_id, market_id);
CREATE INDEX idx_order_snapshots_last_sequence ON order_snapshots(last_processed_sequence);
CREATE INDEX idx_order_snapshots_created_at ON order_snapshots(created_at_ns);

-- View for easy querying with remaining quantity
CREATE VIEW order_details AS
SELECT
    order_id,
    market_id,
    user_id,
    side,
    order_type,
    price_amount,
    price_scale,
    quantity_amount,
    quantity_scale,
    filled_quantity_amount,
    filled_quantity_scale,
    quantity_amount - filled_quantity_amount AS remaining_quantity_amount,
    quantity_scale AS remaining_quantity_scale,
    status,
    rejection_reason,
    created_at_ns,
    updated_at_ns,
    last_processed_event_id,
    last_processed_sequence,
    last_updated_at
FROM order_snapshots;

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE events IS 'Append-only event store for ledger and matching events';
COMMENT ON COLUMN events.stream_id IS 'Event stream identifier (e.g., "ledger" or "matching:BTC/USDT")';
COMMENT ON COLUMN events.sequence IS 'Monotonically increasing sequence number within the stream';
COMMENT ON COLUMN events.event_id IS 'Globally unique event identifier for idempotency';
COMMENT ON COLUMN events.payload IS 'Complete event data as JSONB (serialized LedgerEvent or MatchingEvent)';
COMMENT ON TABLE stream_sequences IS 'Tracks current sequence number per event stream';
COMMENT ON TABLE idempotency_keys IS 'Tracks business idempotency keys to prevent duplicate processing';
COMMENT ON TABLE transaction_log IS 'Tracks in-flight transactions for crash recovery';
COMMENT ON TABLE balance_snapshots IS 'Read-only balance read model derived exclusively from ledger events. Rebuildable from scratch, never used for validation.';
COMMENT ON COLUMN balance_snapshots.available_amount IS 'Available balance (funds available for trading/withdrawal)';
COMMENT ON COLUMN balance_snapshots.locked_amount IS 'Locked balance (funds reserved for active orders)';
COMMENT ON COLUMN balance_snapshots.last_processed_event_id IS 'Last event_id that updated this balance (for idempotency)';
COMMENT ON COLUMN balance_snapshots.last_processed_sequence IS 'Last sequence number processed for tracking projection progress';
COMMENT ON TABLE order_snapshots IS 'Read-only order read model derived exclusively from matching engine events. Rebuildable from scratch, never used for matching or validation.';
COMMENT ON COLUMN order_snapshots.status IS 'Order lifecycle state: New, PartiallyFilled, Filled, Cancelled';
COMMENT ON COLUMN order_snapshots.last_processed_event_id IS 'Last event_id that updated this order (for idempotency)';
COMMENT ON COLUMN order_snapshots.last_processed_sequence IS 'Last sequence number processed for tracking projection progress';
COMMENT ON TABLE trades IS 'Read-only append-only trade read model derived exclusively from matching engine events. Rebuildable from scratch, never used for matching or validation. Trades are immutable once created.';
COMMENT ON COLUMN trades.trade_id IS 'Unique trade identifier (primary key, ensures idempotency)';
COMMENT ON COLUMN trades.executed_at_ns IS 'Timestamp when trade was executed (from event metadata)';
COMMENT ON COLUMN trades.event_id IS 'Event ID that created this trade (for audit trail)';
COMMENT ON COLUMN trades.event_sequence IS 'Event sequence number (for ordering and replay)';
