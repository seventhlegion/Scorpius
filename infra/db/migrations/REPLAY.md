# Recovery via Replay

This document describes how to rebuild ledger and matching engine state by replaying stored events on service startup. The process is **deterministic** and **idempotent**, ensuring consistent state recovery.

## Overview

Event sourcing enables complete state reconstruction by replaying events in order. On service startup:

1. **Load events** from the database in sequence order
2. **Replay events** to rebuild in-memory state
3. **Verify consistency** and handle any errors
4. **Resume processing** from the last replayed event

## Design Principles

### Deterministic Replay
- Events are replayed in **exact sequence order** (`sequence` number)
- Same events in same order = **same final state**
- No randomness or non-deterministic operations during replay

### Idempotency
- Events can be safely replayed multiple times
- Duplicate events are detected and skipped
- State converges to correct value regardless of replay count

### Crash Recovery
- Replay can resume from any checkpoint
- Partial replays are safe (events are idempotent)
- Errors during replay are handled gracefully

## Ledger State Recovery

### State to Rebuild

The `Ledger` maintains:
- `balances`: `HashMap<(user_id, asset), Balance>` - User balances per asset
- `processed_requests`: `HashSet<String>` - Idempotency keys already processed

### Replay Process

```rust
// Pseudocode for ledger replay
async fn replay_ledger_events(db: &Database) -> Result<Ledger, Error> {
    let mut ledger = Ledger::new();
    let mut last_sequence = 0u64;

    // Load events from database in sequence order
    let events = db.query(
        "SELECT sequence, event_id, event_type, payload
         FROM events
         WHERE category = 'ledger'
         ORDER BY sequence"
    ).await?;

    for row in events {
        let sequence: u64 = row.get("sequence");
        let event_id: String = row.get("event_id");
        let event_type: String = row.get("event_type");
        let payload: serde_json::Value = row.get("payload");

        // Deserialize event
        let event: LedgerEvent = serde_json::from_value(payload)?;

        // Verify sequence is sequential (detect gaps)
        if sequence != last_sequence + 1 && last_sequence > 0 {
            return Err(Error::SequenceGap {
                expected: last_sequence + 1,
                actual: sequence,
            });
        }

        // Replay event (idempotent operation)
        replay_ledger_event(&mut ledger, &event)?;

        last_sequence = sequence;
    }

    Ok(ledger)
}
```

### Event Replay Handlers

```rust
fn replay_ledger_event(ledger: &mut Ledger, event: &LedgerEvent) -> Result<(), Error> {
    match event {
        LedgerEvent::FundsDeposited { metadata, user_id, asset, amount, .. } => {
            // Idempotency check: skip if already processed
            if ledger.processed_requests.contains(&metadata.event_id) {
                return Ok(()); // Already processed
            }

            // Update balance
            let balance = ledger.balance_mut(
                &user_id.to_string(),
                asset,
                Money::new(0, amount.scale)
            );
            balance.available = balance.available + *amount;

            // Mark as processed
            ledger.processed_requests.insert(metadata.event_id.clone());
        }

        LedgerEvent::FundsWithdrawn { metadata, user_id, asset, amount, .. } => {
            if ledger.processed_requests.contains(&metadata.event_id) {
                return Ok(());
            }

            let balance = ledger.balance_mut(
                &user_id.to_string(),
                asset,
                Money::new(0, amount.scale)
            );

            // Validate sufficient balance
            if balance.available < *amount {
                return Err(Error::InsufficientFunds {
                    user_id: user_id.to_string(),
                    asset: asset.clone(),
                    available: balance.available,
                    requested: *amount,
                });
            }

            balance.available = balance.available - *amount;
            ledger.processed_requests.insert(metadata.event_id.clone());
        }

        LedgerEvent::FundsReserved { metadata, request_id, user_id, asset, amount } => {
            // Use request_id as idempotency key
            if ledger.processed_requests.contains(request_id) {
                return Ok(());
            }

            let balance = ledger.balance_mut(
                &user_id.to_string(),
                asset,
                Money::new(0, amount.scale)
            );

            if balance.available < *amount {
                return Err(Error::InsufficientFunds {
                    user_id: user_id.to_string(),
                    asset: asset.clone(),
                    available: balance.available,
                    requested: *amount,
                });
            }

            balance.available = balance.available - *amount;
            balance.locked = balance.locked + *amount;
            ledger.processed_requests.insert(request_id.clone());
        }

        LedgerEvent::FundsReleased { metadata, request_id, user_id, asset, amount } => {
            if ledger.processed_requests.contains(request_id) {
                return Ok(());
            }

            let balance = ledger.balance_mut(
                &user_id.to_string(),
                asset,
                Money::new(0, amount.scale)
            );

            if balance.locked < *amount {
                return Err(Error::InsufficientLockedFunds {
                    user_id: user_id.to_string(),
                    asset: asset.clone(),
                    locked: balance.locked,
                    requested: *amount,
                });
            }

            balance.locked = balance.locked - *amount;
            balance.available = balance.available + *amount;
            ledger.processed_requests.insert(request_id.clone());
        }

        LedgerEvent::TradeSettled {
            metadata, trade_id, buyer_id, seller_id,
            base_asset, quote_asset, base_amount, quote_amount, ..
        } => {
            if ledger.processed_requests.contains(trade_id) {
                return Ok(());
            }

            // Buyer: locked quote -> available base
            {
                let buyer_quote = ledger.balance_mut(
                    &buyer_id.to_string(),
                    quote_asset,
                    Money::new(0, quote_amount.scale)
                );

                if buyer_quote.locked < *quote_amount {
                    return Err(Error::BuyerLockedInsufficient {
                        trade_id: trade_id.clone(),
                        asset: quote_asset.clone(),
                        locked: buyer_quote.locked,
                        required: *quote_amount,
                    });
                }

                buyer_quote.locked = buyer_quote.locked - *quote_amount;
            }

            {
                let buyer_base = ledger.balance_mut(
                    &buyer_id.to_string(),
                    base_asset,
                    Money::new(0, base_amount.scale)
                );
                buyer_base.available = buyer_base.available + *base_amount;
            }

            // Seller: locked base -> available quote
            {
                let seller_base = ledger.balance_mut(
                    &seller_id.to_string(),
                    base_asset,
                    Money::new(0, base_amount.scale)
                );

                if seller_base.locked < *base_amount {
                    return Err(Error::SellerLockedInsufficient {
                        trade_id: trade_id.clone(),
                        asset: base_asset.clone(),
                        locked: seller_base.locked,
                        required: *base_amount,
                    });
                }

                seller_base.locked = seller_base.locked - *base_amount;
            }

            {
                let seller_quote = ledger.balance_mut(
                    &seller_id.to_string(),
                    quote_asset,
                    Money::new(0, quote_amount.scale)
                );
                seller_quote.available = seller_quote.available + *quote_amount;
            }

            ledger.processed_requests.insert(trade_id.clone());
        }
    }

    Ok(())
}
```

## Matching Engine State Recovery

### State to Rebuild

The `MatchingEngine` maintains:
- `orderbook`: `OrderBook` - Bids and asks price levels
- `orders`: `HashMap<OrderId, Order>` - Active orders
- `next_trade_id`: `u64` - Next trade ID to assign

### Replay Process

```rust
async fn replay_matching_events(
    db: &Database,
    market_id: MarketId
) -> Result<MatchingEngine, Error> {
    let mut engine = MatchingEngine::new();
    let mut last_sequence = 0u64;
    let mut max_trade_id = 0u64;

    let stream_id = format!("matching:{}", market_id);

    // Load events for this market
    let events = db.query(
        "SELECT sequence, event_id, event_type, payload
         FROM events
         WHERE stream_id = $1
         ORDER BY sequence",
        &[&stream_id]
    ).await?;

    for row in events {
        let sequence: u64 = row.get("sequence");
        let event_id: String = row.get("event_id");
        let event_type: String = row.get("event_type");
        let payload: serde_json::Value = row.get("payload");

        let event: MatchingEvent = serde_json::from_value(payload)?;

        // Verify sequence
        if sequence != last_sequence + 1 && last_sequence > 0 {
            return Err(Error::SequenceGap {
                expected: last_sequence + 1,
                actual: sequence,
            });
        }

        // Track max trade_id for next_trade_id initialization
        if let Some(trade_id) = event.trade_id() {
            max_trade_id = max_trade_id.max(trade_id.0);
        }

        // Replay event
        replay_matching_event(&mut engine, &event)?;

        last_sequence = sequence;
    }

    // Set next_trade_id to max + 1
    engine.next_trade_id = max_trade_id + 1;

    Ok(engine)
}
```

### Event Replay Handlers

```rust
fn replay_matching_event(
    engine: &mut MatchingEngine,
    event: &MatchingEvent
) -> Result<(), Error> {
    match event {
        MatchingEvent::OrderPlaced {
            metadata, order_id, market_id, user_id, side, order_type,
            price, quantity, filled_quantity, status, ..
        } => {
            // Reconstruct order state
            let order = Order {
                id: *order_id,
                user_id: *user_id,
                market_id: *market_id,
                side: *side,
                order_type: *order_type,
                price: *price,
                quantity: *quantity,
                filled: *filled_quantity,
                status: *status,
            };

            // Only add to orderbook if not fully filled
            if status != &OrderStatus::Filled && order.remaining().amount > 0 {
                engine.orderbook.add(&order);
            }

            // Store order (even if filled, for history)
            engine.orders.insert(*order_id, order);
        }

        MatchingEvent::TradeExecuted {
            metadata, trade_id, maker_order_id, taker_order_id,
            price, quantity, ..
        } => {
            // Update maker order
            if let Some(maker_order) = engine.orders.get_mut(maker_order_id) {
                maker_order.filled = maker_order.filled + *quantity;

                if maker_order.remaining().amount == 0 {
                    maker_order.status = OrderStatus::Filled;
                    engine.orderbook.remove(*maker_order_id);
                } else {
                    maker_order.status = OrderStatus::PartiallyFilled;
                }
            }

            // Update taker order
            if let Some(taker_order) = engine.orders.get_mut(taker_order_id) {
                taker_order.filled = taker_order.filled + *quantity;

                if taker_order.remaining().amount == 0 {
                    taker_order.status = OrderStatus::Filled;
                    engine.orderbook.remove(*taker_order_id);
                } else {
                    taker_order.status = OrderStatus::PartiallyFilled;
                }
            }
        }

        MatchingEvent::OrderPartiallyFilled {
            metadata, order_id, filled_quantity, remaining_quantity, ..
        } => {
            // Update order fill status
            if let Some(order) = engine.orders.get_mut(order_id) {
                order.filled = *filled_quantity;
                order.status = OrderStatus::PartiallyFilled;
            }
        }

        MatchingEvent::OrderFilled {
            metadata, order_id, ..
        } => {
            // Remove from orderbook and mark as filled
            engine.orderbook.remove(*order_id);
            if let Some(order) = engine.orders.get_mut(order_id) {
                order.status = OrderStatus::Filled;
            }
        }

        MatchingEvent::OrderCanceled {
            metadata, order_id, canceled_quantity, ..
        } => {
            // Remove from orderbook
            engine.orderbook.remove(*order_id);

            // Update order status
            if let Some(order) = engine.orders.get_mut(order_id) {
                order.status = OrderStatus::Cancelled;
            }
        }

        MatchingEvent::OrderRejected {
            metadata, order_id, reason, ..
        } => {
            // Order was rejected, no state to update
            // Could log for audit purposes
            tracing::warn!("Order {} rejected during replay: {}", order_id, reason);
        }
    }

    Ok(())
}
```

## Service Startup Flow

### Ledger Service Startup

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    // Connect to database
    let db = connect_to_database().await?;

    // Replay events to rebuild state
    tracing::info!("Replaying ledger events to rebuild state...");
    let ledger = replay_ledger_events(&db).await?;
    tracing::info!("Ledger state rebuilt successfully");

    // Verify state consistency
    verify_ledger_state(&ledger, &db).await?;

    // Create service with recovered state
    let service = LedgerGrpcService::new(ledger);

    // Start gRPC server
    let addr: SocketAddr = "0.0.0.0:50052".parse()?;
    tracing::info!("Ledger gRPC service listening on {}", addr);

    Server::builder()
        .add_service(LedgerServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
```

### Matching Service Startup

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let db = connect_to_database().await?;

    // Get list of markets to recover
    let markets = get_active_markets(&db).await?;

    let mut engines: HashMap<MarketId, MatchingEngine> = HashMap::new();

    // Replay events for each market
    for market_id in markets {
        tracing::info!("Replaying events for market {:?}...", market_id);
        let engine = replay_matching_events(&db, market_id).await?;
        engines.insert(market_id, engine);
        tracing::info!("Market {:?} state rebuilt successfully", market_id);
    }

    // Create service with recovered engines
    let service = MatchingGrpcService::new(engines);

    // Start gRPC server
    let addr: SocketAddr = "0.0.0.0:50051".parse()?;
    tracing::info!("Matching gRPC service listening on {}", addr);

    Server::builder()
        .add_service(MatchingServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
```

## Deterministic Replay Guarantees

### Sequence Ordering
- Events are **always** loaded in `sequence` order
- Database query uses `ORDER BY sequence` to ensure deterministic order
- Sequence gaps are detected and reported as errors

### Idempotency
- Each event handler checks if the event was already processed
- Duplicate events are safely skipped
- State converges to correct value even if events are replayed multiple times

### No Side Effects
- Replay operations are **pure functions**
- No external API calls or non-deterministic operations
- Same events → same state, always

## Error Handling

### Sequence Gaps
If sequence numbers have gaps (shouldn't happen in normal operation):

```rust
if sequence != last_sequence + 1 && last_sequence > 0 {
    tracing::error!(
        "Sequence gap detected: expected {}, got {}",
        last_sequence + 1,
        sequence
    );

    // Options:
    // 1. Fail fast (recommended for production)
    return Err(Error::SequenceGap { expected, actual });

    // 2. Skip gap and continue (for recovery scenarios)
    // tracing::warn!("Skipping sequence gap, continuing replay");
    // continue;
}
```

### Invalid Event Data
If an event cannot be deserialized or is invalid:

```rust
match serde_json::from_value::<LedgerEvent>(payload) {
    Ok(event) => replay_ledger_event(&mut ledger, &event)?,
    Err(e) => {
        tracing::error!("Failed to deserialize event {}: {}", event_id, e);

        // Options:
        // 1. Fail fast
        return Err(Error::InvalidEvent { event_id, error: e });

        // 2. Skip invalid event (with logging)
        // tracing::warn!("Skipping invalid event {}", event_id);
        // continue;
    }
}
```

### State Validation Errors
If replay would result in invalid state (e.g., negative balance):

```rust
match replay_ledger_event(&mut ledger, &event) {
    Ok(()) => {},
    Err(Error::InsufficientFunds { .. }) => {
        // This indicates data corruption or missing events
        tracing::error!("State validation failed during replay");
        return Err(Error::StateValidationFailed);
    }
    Err(e) => return Err(e),
}
```

## State Verification

After replay, verify state consistency:

```rust
async fn verify_ledger_state(
    ledger: &Ledger,
    db: &Database
) -> Result<(), Error> {
    // Check that all balances are non-negative
    for ((user_id, asset), balance) in &ledger.balances {
        if balance.available.amount < 0 || balance.locked.amount < 0 {
            return Err(Error::InvalidBalance {
                user_id: user_id.clone(),
                asset: asset.clone(),
                balance: balance.clone(),
            });
        }
    }

    // Verify sequence number consistency
    let max_sequence: Option<i64> = db.query_one(
        "SELECT MAX(sequence) FROM events WHERE category = 'ledger'"
    ).await?;

    // Additional checks...

    Ok(())
}
```

## Checkpoint-Based Replay

For large event streams, support checkpoint-based replay:

```rust
async fn replay_from_checkpoint(
    db: &Database,
    checkpoint_sequence: u64
) -> Result<Ledger, Error> {
    let mut ledger = Ledger::new();

    // Load checkpoint state (if available)
    if let Some(checkpoint) = load_checkpoint(db, checkpoint_sequence).await? {
        ledger = checkpoint.state;
    }

    // Replay events after checkpoint
    let events = db.query(
        "SELECT sequence, event_id, event_type, payload
         FROM events
         WHERE category = 'ledger' AND sequence > $1
         ORDER BY sequence",
        &[&checkpoint_sequence]
    ).await?;

    // Replay remaining events...

    Ok(ledger)
}
```

## Performance Considerations

### Batch Loading
Load events in batches to avoid memory issues:

```rust
const BATCH_SIZE: usize = 1000;

let mut offset = 0;
loop {
    let events = db.query(
        "SELECT sequence, event_id, event_type, payload
         FROM events
         WHERE category = 'ledger'
         ORDER BY sequence
         LIMIT $1 OFFSET $2",
        &[&(BATCH_SIZE as i64), &(offset as i64)]
    ).await?;

    if events.is_empty() {
        break;
    }

    for event in events {
        replay_ledger_event(&mut ledger, &event)?;
    }

    offset += BATCH_SIZE;
}
```

### Parallel Market Replay
Replay different markets in parallel:

```rust
use futures::future::join_all;

let replay_tasks: Vec<_> = markets
    .into_iter()
    .map(|market_id| {
        let db = db.clone();
        tokio::spawn(async move {
            replay_matching_events(&db, market_id).await
        })
    })
    .collect();

let engines: Result<Vec<_>, _> = join_all(replay_tasks).await.into_iter().collect();
```

## Summary

The replay process ensures:

1. **Deterministic**: Same events → same state, always
2. **Idempotent**: Safe to replay multiple times
3. **Crash-resistant**: Can resume from any checkpoint
4. **Verifiable**: State can be validated after replay
5. **Performant**: Supports batch loading and parallel replay

This design enables reliable state recovery and ensures system consistency even after crashes or restarts.
