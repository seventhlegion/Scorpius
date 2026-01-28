# Integration Tests

This directory contains integration tests that verify the system works end-to-end.

## Prerequisites

1. **PostgreSQL Database**: You need a PostgreSQL database running. You can use Docker:

```bash
docker run --name scorpius-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=scorpius_test \
  -p 5432:5432 \
  -d postgres:15
```

2. **Database Schema**: Run the schema migration:

```bash
psql -h localhost -U postgres -d scorpius_test -f infra/db/migrations/schema.sql
```

Or if using Docker:

```bash
docker exec -i scorpius-postgres psql -U postgres -d scorpius_test < infra/db/migrations/schema.sql
```

## Running Tests

### Ledger Integration Tests

Tests for ledger, event store, and balance read model:

```bash
cd crates/ledger
DATABASE_URL=postgresql://postgres:postgres@localhost/scorpius_test cargo test --test integration_test
```

### Matching Engine Tests

Tests for matching engine (unit tests, no database required):

```bash
cd crates/matching
cargo test --test integration_test
```

### All Tests Together

Run all tests from the workspace root:

```bash
# Set database URL
export DATABASE_URL=postgresql://postgres:postgres@localhost/scorpius_test

# Run ledger integration tests
cargo test --package ledger --test integration_test

# Run matching engine tests
cargo test --package matching --test integration_test

# Run unit tests
cargo test
```

## Test Coverage

### Ledger Tests (`crates/ledger/tests/integration_test.rs`)

- ✅ Reserve/Release flow
- ✅ Settle trade flow
- ✅ Event persistence and replay
- ✅ Balance read model projection
- ✅ Idempotency checks
- ✅ End-to-end order flow

### Matching Engine Tests (`crates/matching/tests/integration_test.rs`)

- ✅ Basic order placement
- ✅ Buy/sell matching
- ✅ Partial fills
- ✅ Price priority
- ✅ Order cancellation
- ✅ Multiple trades for same order

## Environment Variables

- `DATABASE_URL`: PostgreSQL connection string (default: `postgresql://postgres:postgres@localhost/scorpius_test`)

## Troubleshooting

### Database Connection Issues

If you get connection errors:

1. Verify PostgreSQL is running: `docker ps` or `pg_isready`
2. Check connection string format: `postgresql://user:password@host:port/database`
3. Ensure database exists: `createdb scorpius_test`

### Schema Issues

If tests fail with "relation does not exist":

1. Run the schema migration: `psql -d scorpius_test -f infra/db/migrations/schema.sql`
2. Check that all tables exist: `\dt` in psql

### Test Data Cleanup

Tests clean up after themselves, but if you need to manually clean:

```sql
DELETE FROM balance_snapshots WHERE user_id IN (1001, 1002, 1003);
DELETE FROM events WHERE stream_id = 'ledger' AND (payload->>'user_id')::text LIKE '100%';
```
