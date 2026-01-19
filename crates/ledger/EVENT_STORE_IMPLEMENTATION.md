# Event Store Implementation

This document describes the event store and replay mechanism implementation for the ledger.

## Architecture

### Separation of Concerns

The implementation follows a clean architecture with clear separation:

1. **Domain Logic** (`core.rs`): Pure business logic, no persistence concerns
2. **Event Store Trait** (`event_store.rs`): Abstraction for persistence
3. **PostgreSQL Implementation** (`postgres_store.rs`): Concrete persistence layer
4. **Replay Logic** (`replay.rs`): Deterministic state reconstruction
5. **Event-Sourced Ledger** (`ledger_with_events.rs`): High-level API that combines events and state

### Components

#### EventStore Trait

The `EventStore` trait provides a clean abstraction for event persistence:

```rust
#[async_trait]
pub trait EventStore: Send + Sync {
    async fn append(&self, event: &LedgerEvent) -> EventStoreResult<u64>;
    async fn next_sequence(&self) -> EventStoreResult<u64>;
    async fn load_all(&self) -> EventStoreResult<Vec<LedgerEvent>>;
    async fn load_from(&self, from_sequence: u64) -> EventStoreResult<Vec<LedgerEvent>>;
    async fn exists(&self, event_id: &str) -> EventStoreResult<bool>;
    async fn max_sequence(&self) -> EventStoreResult<Option<u64>>;
}
```

#### PostgresEventStore

PostgreSQL implementation that:
- Stores events in the `events` table
- Uses atomic sequence number generation
- Enforces idempotency via `event_id` unique constraint
- Detects sequence gaps for data integrity

#### Replay Mechanism

The `replay` module provides deterministic state reconstruction:

- `replay_events()`: Rebuilds complete state from all events
- `replay_from()`: Rebuilds from a checkpoint
- `replay_event()`: Applies a single event to state (idempotent)

#### EventSourcedLedger

High-level API that:
- Emits events for all state changes
- Applies events to in-memory state
- Rebuilds state on initialization via replay

## Usage

### Initialization

```rust
// Create PostgreSQL connection pool
let pool = PgPool::connect("postgresql://...").await?;
let pool = Arc::new(pool);

// Create event store
let event_store = Arc::new(PostgresEventStore::new(pool));

// Create event-sourced ledger (replays all events)
let mut ledger = EventSourcedLedger::new(event_store, "ledger-service".to_string()).await?;
```

### Operations

All operations emit events and update state:

```rust
// Deposit funds
ledger.deposit(
    user_id,
    "USDT".to_string(),
    Money::new(10000, 6),
    None, // external_reference
    None, // causation_id
    Some(correlation_id), // correlation_id
).await?;

// Reserve funds
ledger.reserve(
    "order-123".to_string(),
    user_id,
    "USDT".to_string(),
    Money::new(5000, 6),
    None,
    Some(correlation_id),
).await?;

// Settle trade
ledger.settle_trade(
    "trade-456".to_string(),
    buyer_id,
    seller_id,
    "BTC".to_string(),
    "USDT".to_string(),
    base_amount,
    quote_amount,
    price,
    Some(maker_order_id),
    Some(taker_order_id),
    None,
    Some(correlation_id),
).await?;
```

## Deterministic Recovery

### Replay Process

1. **Load Events**: Query database for all events ordered by sequence
2. **Verify Ordering**: Check for sequence gaps (data integrity)
3. **Apply Events**: Replay each event to rebuild state
4. **Idempotency**: Skip events already processed (via `processed_requests`)

### Idempotency Guarantees

- **Event ID**: Global unique constraint prevents duplicate events
- **Business Keys**: `request_id`, `trade_id` tracked in `processed_requests`
- **Replay Safety**: Events can be replayed multiple times safely

### Error Handling

- **Sequence Gaps**: Detected and reported as `EventStoreError::SequenceGap`
- **Invalid State**: Validation errors during replay indicate data corruption
- **Duplicate Events**: Handled gracefully (idempotent no-op)

## Database Schema

The implementation uses the PostgreSQL schema defined in `infra/db/migrations/schema.sql`:

- `events` table: Append-only event store
- `stream_sequences` table: Atomic sequence number generation
- `idempotency_keys` table: Business key tracking (optional)

## Testing

### Unit Tests

Test replay logic with in-memory event store:

```rust
// Create in-memory event store for testing
struct InMemoryEventStore { ... }

impl EventStore for InMemoryEventStore { ... }

// Test replay
let events = vec![...];
let store = InMemoryEventStore::with_events(events);
let ledger = replay_events(&store).await?;
```

### Integration Tests

Test with real PostgreSQL:

```rust
#[tokio::test]
async fn test_replay() {
    let pool = create_test_pool().await;
    let store = PostgresEventStore::new(Arc::new(pool));

    // Append events
    store.append(&event1).await?;
    store.append(&event2).await?;

    // Replay
    let ledger = replay_events(&store).await?;

    // Verify state
    assert_eq!(ledger.state().balances.len(), 2);
}
```

## Performance Considerations

### Batch Loading

For large event streams, load events in batches:

```rust
const BATCH_SIZE: usize = 1000;
let mut from_sequence = 0;

loop {
    let events = store.load_from(from_sequence)
        .take(BATCH_SIZE)
        .await?;

    if events.is_empty() {
        break;
    }

    for event in events {
        replay_event(&mut ledger, &event)?;
    }

    from_sequence += events.len() as u64;
}
```

### Checkpointing

Save periodic checkpoints to speed up recovery:

```rust
// Save checkpoint every N events
if event_count % CHECKPOINT_INTERVAL == 0 {
    save_checkpoint(&ledger.state(), sequence).await?;
}

// Load from checkpoint on startup
let checkpoint = load_latest_checkpoint().await?;
let ledger = replay_from(&store, checkpoint.sequence, checkpoint.state).await?;
```

## Future Enhancements

1. **In-Memory Event Store**: For testing and development
2. **Event Snapshots**: Periodic state snapshots for faster recovery
3. **Event Projections**: Read models for queries
4. **Event Versioning**: Handle schema evolution
5. **Compaction**: Archive old events while maintaining replay capability
