# Testing Guide

This guide explains how to test the implemented features of Scorpius Exchange.

## Quick Start

### 1. Setup Database

```bash
# Using Docker
docker run --name scorpius-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=scorpius_test \
  -p 5432:5432 \
  -d postgres:15

# Run schema migration
docker exec -i scorpius-postgres psql -U postgres -d scorpius_test < infra/db/migrations/schema.sql
```

### 2. Run All Tests

```bash
# Set database URL
export DATABASE_URL=postgresql://postgres:postgres@localhost/scorpius_test

# Run the test script
./scripts/test_all.sh
```

Or run tests individually:

```bash
# Ledger integration tests (requires database)
cargo test --package ledger --test integration_test

# Matching engine tests (no database)
cargo test --package matching --test integration_test

# Unit tests
cargo test --lib
```

## Test Coverage

### ✅ Implemented and Tested

#### Ledger Core

- ✅ Reserve funds (lock for orders)
- ✅ Release funds (unlock cancelled orders)
- ✅ Settle trade (transfer assets between buyer/seller)
- ✅ Idempotency (duplicate requests are safe)

#### Event Store

- ✅ Event persistence (all ledger operations emit events)
- ✅ Event replay (state can be rebuilt from events)
- ✅ Sequence numbers (deterministic ordering)

#### Balance Read Model

- ✅ Projection from ledger events
- ✅ Rebuild from scratch
- ✅ Query balances by user/asset

#### Matching Engine

- ✅ Place orders (buy/sell)
- ✅ Order matching (price-time priority)
- ✅ Partial fills
- ✅ Order cancellation
- ✅ Multiple trades per order

### ⚠️ Partially Implemented (Not Yet Tested)

#### Matching Events

- ⚠️ Event types defined but not persisted
- ⚠️ Event projector exists but has no events to process

#### gRPC Services

- ⚠️ Services exist but integration tests not yet written
- ⚠️ Idempotency checks exist but incomplete without event persistence

## Test Structure

```
crates/
├── ledger/
│   └── tests/
│       └── integration_test.rs    # Ledger + Event Store + Read Model tests
├── matching/
│   └── tests/
│       └── integration_test.rs    # Matching Engine tests
└── [other crates]/
    └── src/
        └── test.rs                 # Unit tests
```

## What Each Test Verifies

### Ledger Integration Tests

1. **test_ledger_reserve_release_flow**
   - Deposit funds
   - Reserve funds for order
   - Release funds (order cancelled)
   - Verify balances are correct

2. **test_ledger_settle_trade_flow**
   - Buyer deposits and reserves USDT
   - Seller deposits and reserves BTC
   - Settle trade
   - Verify both parties' balances

3. **test_event_persistence_and_replay**
   - Perform operations
   - Create new ledger instance (simulate restart)
   - Verify state was restored from events

4. **test_balance_read_model_projection**
   - Perform ledger operations
   - Project events to read model
   - Query read model and verify correctness

5. **test_idempotency**
   - Perform same operation twice
   - Verify state doesn't change

6. **test_end_to_end_order_flow**
   - Complete flow: deposit → reserve → settle trade
   - Verify read model reflects all changes

### Matching Engine Tests

1. **test_matching_engine_basic_order_placement**
   - Place order with no matches
   - Verify order is stored

2. **test_matching_engine_buy_sell_match**
   - Place sell order
   - Place matching buy order
   - Verify trade is created
   - Verify orders are filled

3. **test_matching_engine_partial_fill**
   - Place sell order for 5 units
   - Place buy order for 10 units
   - Verify partial fill

4. **test_matching_engine_price_priority**
   - Place multiple sell orders at different prices
   - Place buy order
   - Verify best price matches first

5. **test_matching_engine_cancel_order**
   - Place order
   - Cancel order
   - Verify cancellation

6. **test_matching_engine_multiple_trades_same_order**
   - Place multiple sell orders
   - Place large buy order
   - Verify multiple trades created

## Next Steps

After verifying these tests pass, you can:

1. **Implement Matching Event Persistence**
   - Modify matching engine to emit events
   - Store events in event store
   - Test event projector

2. **Add gRPC Integration Tests**
   - Test ledger gRPC service
   - Test matching gRPC service
   - Test gateway HTTP → gRPC flow

3. **Add End-to-End Tests**
   - Test complete order flow through gateway
   - Test idempotency across services
   - Test error handling and recovery

## Troubleshooting

### Database Connection Issues

```bash
# Check if PostgreSQL is running
docker ps | grep postgres

# Test connection
psql postgresql://postgres:postgres@localhost/scorpius_test -c "SELECT 1"
```

### Schema Issues

```bash
# Re-run schema migration
psql postgresql://postgres:postgres@localhost/scorpius_test -f infra/db/migrations/schema.sql
```

### Test Failures

1. Check database is clean: Tests clean up after themselves, but you can manually clean:

   ```sql
   DELETE FROM balance_snapshots WHERE user_id IN (1001, 1002, 1003);
   DELETE FROM events WHERE stream_id = 'ledger';
   ```

2. Check logs: Tests use `tracing`, set `RUST_LOG=debug` for more output:
   ```bash
   RUST_LOG=debug cargo test --package ledger --test integration_test
   ```
