//! Integration tests for ledger, event store, and read models
//!
//! These tests require a PostgreSQL database. Set DATABASE_URL environment variable:
//! DATABASE_URL=postgresql://user:password@localhost/scorpius_test
//!
//! Run with: cargo test --test integration_test
//!
//! Enable detailed logging: RUST_LOG=debug cargo test --test integration_test

use ledger::balance_read_model::BalanceReadModel;
use ledger::ledger_with_events::EventSourcedLedger;
use ledger::postgres_store::PostgresEventStore;
use scorpius_core::{Money, UserId};
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::sync::{Arc, Mutex};
use std::time::Instant;

// Global mutex to ensure tests run sequentially and don't interfere with each other
// This prevents race conditions when multiple tests access the same database
static TEST_MUTEX: Mutex<()> = Mutex::new(());

/// Helper function to format money amounts for display
fn format_money(amount: i64, scale: u32) -> String {
    let divisor = 10_i64.pow(scale);
    let whole = amount / divisor;
    let fractional = amount % divisor;
    format!("{}.{:0width$}", whole, fractional, width = scale as usize)
}

/// Helper function to get event count from event store
async fn get_event_count(pool: &Pool<Postgres>, stream_id: &str) -> Result<u64, sqlx::Error> {
    let result = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM events WHERE stream_id = $1")
        .bind(stream_id)
        .fetch_one(pool)
        .await?;
    Ok(result as u64)
}

/// Helper function to get last sequence number from event store
async fn get_last_sequence(
    pool: &Pool<Postgres>,
    stream_id: &str,
) -> Result<Option<u64>, sqlx::Error> {
    let result = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT MAX(sequence) FROM events WHERE stream_id = $1",
    )
    .bind(stream_id)
    .fetch_one(pool)
    .await?;
    Ok(result.map(|s| s as u64))
}

/// Helper function to get all sequence numbers (for debugging sequence gaps)
async fn get_all_sequences(
    pool: &Pool<Postgres>,
    stream_id: &str,
) -> Result<Vec<i64>, sqlx::Error> {
    let rows = sqlx::query_scalar::<_, i64>(
        "SELECT sequence FROM events WHERE stream_id = $1 ORDER BY sequence",
    )
    .bind(stream_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Setup a test database connection pool
async fn setup_test_db() -> Arc<Pool<Postgres>> {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost/scorpius_test".to_string());

    eprintln!(
        "🔌 Connecting to database: {}",
        database_url.replace(":postgres@", ":****@")
    );
    let start = Instant::now();

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to connect to database: {}", e);
            eprintln!("💡 Make sure PostgreSQL is running and DATABASE_URL is correct");
            panic!("Database connection failed: {}", e);
        });

    eprintln!(
        "✅ Database connection established in {:?}",
        start.elapsed()
    );

    // Run migrations (schema.sql)
    // In a real scenario, you'd use sqlx migrations
    // For now, we assume the schema is already set up

    Arc::new(pool)
}

/// Clean up test data
/// This function performs aggressive cleanup to ensure a clean state for each test
async fn cleanup_test_data(pool: &Pool<Postgres>) {
    eprintln!("🧹 Cleaning up test data...");
    let start = Instant::now();

    // Use a transaction to ensure atomic cleanup
    let mut tx = match pool.begin().await {
        Ok(tx) => tx,
        Err(e) => {
            eprintln!("   ⚠ Failed to start cleanup transaction: {}", e);
            return;
        }
    };

    // 1. Clean up balance snapshots for test users
    match sqlx::query("DELETE FROM balance_snapshots WHERE user_id IN (1001, 1002, 1003)")
        .execute(&mut *tx)
        .await
    {
        Ok(result) => eprintln!("   ✓ Deleted {} balance snapshots", result.rows_affected()),
        Err(e) => eprintln!("   ⚠ Failed to delete balance snapshots: {}", e),
    }

    // 2. Clean up ALL events for the ledger stream (most important)
    match sqlx::query("DELETE FROM events WHERE stream_id = 'ledger'")
        .execute(&mut *tx)
        .await
    {
        Ok(result) => {
            let deleted = result.rows_affected();
            if deleted > 0 {
                eprintln!("   ✓ Deleted {} events from ledger stream", deleted);
            } else {
                eprintln!("   ✓ No events to delete (stream already clean)");
            }
        }
        Err(e) => eprintln!("   ⚠ Failed to delete events: {}", e),
    }

    // 3. Reset sequence counter for ledger stream to 0
    match sqlx::query(
        r#"
        INSERT INTO stream_sequences (stream_id, last_sequence)
        VALUES ('ledger', 0)
        ON CONFLICT (stream_id) DO UPDATE
        SET last_sequence = 0, updated_at = NOW()
        "#,
    )
    .execute(&mut *tx)
    .await
    {
        Ok(_) => eprintln!("   ✓ Reset sequence counter to 0"),
        Err(e) => eprintln!("   ⚠ Failed to reset sequence counter: {}", e),
    }

    // 4. Also clean up events for test users from other streams (backup cleanup)
    match sqlx::query(
        r#"
        DELETE FROM events
        WHERE (
            (payload->>'user_id')::text IN ('1001', '1002', '1003')
            OR (payload->>'buyer_id')::text IN ('1001', '1002', '1003')
            OR (payload->>'seller_id')::text IN ('1001', '1002', '1003')
        )
        AND stream_id != 'ledger'
        "#,
    )
    .execute(&mut *tx)
    .await
    {
        Ok(result) => {
            if result.rows_affected() > 0 {
                eprintln!(
                    "   ✓ Deleted {} events from other streams",
                    result.rows_affected()
                );
            }
        }
        Err(e) => eprintln!("   ⚠ Failed to delete backup events: {}", e),
    }

    // Commit the transaction
    match tx.commit().await {
        Ok(_) => {
            // Verify cleanup worked
            let actual_max = sqlx::query_scalar::<_, Option<i64>>(
                "SELECT MAX(sequence) FROM events WHERE stream_id = 'ledger'",
            )
            .fetch_one(pool)
            .await
            .ok()
            .flatten();

            let counter_value = sqlx::query_scalar::<_, Option<i64>>(
                "SELECT last_sequence FROM stream_sequences WHERE stream_id = 'ledger'",
            )
            .fetch_one(pool)
            .await
            .ok()
            .flatten();

            if counter_value == Some(0) && actual_max.is_none() {
                eprintln!("   ✓ Verified clean state: no events, counter=0");
            } else {
                eprintln!("   ⚠ Cleanup verification: max_event_seq={:?}, counter={:?} (expected: None, 0)", actual_max, counter_value);
            }
        }
        Err(e) => {
            eprintln!("   ⚠ Failed to commit cleanup transaction: {}", e);
        }
    }

    eprintln!("✅ Cleanup completed in {:?}", start.elapsed());
}

#[tokio::test]
async fn test_ledger_reserve_release_flow() {
    // Acquire mutex to ensure sequential test execution
    // Use unwrap_or_else to handle poisoned mutex (from previous test panics)
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|poisoned| {
        eprintln!("⚠ Mutex was poisoned from previous test, clearing...");
        poisoned.into_inner()
    });

    eprintln!("\n{}", "=".repeat(80));
    eprintln!("🧪 TEST: test_ledger_reserve_release_flow");
    eprintln!("{}", "=".repeat(80));
    let test_start = Instant::now();

    eprintln!("\n📋 Test Purpose: Verify reserve/release flow for order lifecycle");
    eprintln!("   - Deposit funds");
    eprintln!("   - Reserve funds for an order");
    eprintln!("   - Verify balance read model projection");
    eprintln!("   - Release funds (order cancellation)");
    eprintln!("   - Verify final balance state");

    let pool = setup_test_db().await;
    cleanup_test_data(&pool).await;

    eprintln!("\n📦 Creating event store and ledger...");
    let event_store = Arc::new(PostgresEventStore::with_stream_id(
        pool.clone(),
        "ledger".to_string(),
    ));
    eprintln!("   ✓ Event store created with stream_id: 'ledger'");

    let ledger_start = Instant::now();
    let mut ledger = EventSourcedLedger::new(event_store.clone(), "test".to_string())
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to create ledger: {:?}", e);
            eprintln!("   Error details: {:?}", e);
            panic!("Failed to create ledger: {:?}", e);
        });
    eprintln!("✅ Ledger created in {:?}", ledger_start.elapsed());

    // Check initial event count
    let initial_event_count = get_event_count(&pool, "ledger").await.unwrap_or(0);
    eprintln!(
        "   📊 Initial event count in store: {}",
        initial_event_count
    );

    let user_id = UserId(1001);
    let asset = "USDT";
    let deposit_amount = Money::new(10000, 2); // 100.00 USDT
    let reserve_amount = Money::new(5000, 2); // 50.00 USDT

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("📥 STEP 1: Deposit Funds");
    eprintln!("{}", "─".repeat(80));
    eprintln!("   User ID: {}", user_id.0);
    eprintln!("   Asset: {}", asset);
    eprintln!(
        "   Amount: {} ({})",
        deposit_amount.amount,
        format_money(deposit_amount.amount, deposit_amount.scale)
    );
    let deposit_start = Instant::now();

    ledger
        .deposit(user_id, asset.to_string(), deposit_amount, None, None, None)
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Deposit failed: {:?}", e);
            eprintln!(
                "   User: {}, Asset: {}, Amount: {}",
                user_id.0, asset, deposit_amount.amount
            );
            panic!("Failed to deposit: {:?}", e);
        });

    let deposit_duration = deposit_start.elapsed();
    eprintln!("✅ Deposit completed in {:?}", deposit_duration);

    // Verify event was persisted
    let after_deposit_count = get_event_count(&pool, "ledger").await.unwrap_or(0);
    let last_sequence = get_last_sequence(&pool, "ledger").await.unwrap_or(None);
    eprintln!(
        "   📊 Events after deposit: {} (added: {})",
        after_deposit_count,
        after_deposit_count - initial_event_count
    );
    eprintln!("   📊 Last sequence number: {:?}", last_sequence);

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("🔒 STEP 2: Reserve Funds for Order");
    eprintln!("{}", "─".repeat(80));
    eprintln!("   Order ID: order_1");
    eprintln!("   User ID: {}", user_id.0);
    eprintln!("   Asset: {}", asset);
    eprintln!(
        "   Amount: {} ({})",
        reserve_amount.amount,
        format_money(reserve_amount.amount, reserve_amount.scale)
    );
    let reserve_start = Instant::now();

    ledger
        .reserve(
            "order_1".to_string(),
            user_id,
            asset.to_string(),
            reserve_amount,
            None,
            None,
        )
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Reserve failed: {:?}", e);
            eprintln!(
                "   Order ID: order_1, User: {}, Asset: {}, Amount: {}",
                user_id.0, asset, reserve_amount.amount
            );
            panic!("Failed to reserve: {:?}", e);
        });

    let reserve_duration = reserve_start.elapsed();
    eprintln!("✅ Reserve completed in {:?}", reserve_duration);

    // Verify event was persisted
    let after_reserve_count = get_event_count(&pool, "ledger").await.unwrap_or(0);
    let last_sequence = get_last_sequence(&pool, "ledger").await.unwrap_or(None);
    eprintln!(
        "   📊 Events after reserve: {} (added: {})",
        after_reserve_count,
        after_reserve_count - after_deposit_count
    );
    eprintln!("   📊 Last sequence number: {:?}", last_sequence);

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("📊 STEP 3: Project Events to Balance Read Model");
    eprintln!("{}", "─".repeat(80));
    eprintln!(
        "   Projecting {} events to balance read model...",
        after_reserve_count
    );
    let read_model = BalanceReadModel::new(pool.clone());
    let project_start = Instant::now();

    let last_seq_before = get_last_sequence(&pool, "ledger").await.unwrap_or(None);
    read_model
        .rebuild_from_events(event_store.as_ref())
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to rebuild read model: {:?}", e);
            eprintln!("   Event count: {}", after_reserve_count);
            eprintln!("   Last sequence: {:?}", last_seq_before);
            panic!("Failed to rebuild read model: {:?}", e);
        });

    let project_duration = project_start.elapsed();
    eprintln!("✅ Read model rebuilt in {:?}", project_duration);
    eprintln!("   📊 Events processed: {}", after_reserve_count);

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("🔍 STEP 4: Verify Balance After Reserve");
    eprintln!("{}", "─".repeat(80));
    let balance = read_model
        .get_balance(user_id, asset)
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to get balance: {:?}", e);
            eprintln!("   User: {}, Asset: {}", user_id.0, asset);
            panic!("Failed to get balance: {:?}", e);
        })
        .unwrap_or_else(|| {
            eprintln!(
                "❌ Balance not found for user {} asset {}",
                user_id.0, asset
            );
            panic!("Balance should exist");
        });

    eprintln!("   📊 Balance Details:");
    eprintln!("      User ID: {}", user_id.0);
    eprintln!("      Asset: {}", asset);
    eprintln!(
        "      Available: {} ({})",
        balance.available.amount,
        format_money(balance.available.amount, balance.available.scale)
    );
    eprintln!(
        "      Locked: {} ({})",
        balance.locked.amount,
        format_money(balance.locked.amount, balance.locked.scale)
    );
    eprintln!(
        "      Total: {} ({})",
        balance.available.amount + balance.locked.amount,
        format_money(
            balance.available.amount + balance.locked.amount,
            balance.available.scale
        )
    );
    eprintln!(
        "      Last processed sequence: {}",
        balance.last_processed_sequence
    );

    eprintln!("\n   ✅ Expected Values:");
    eprintln!("      Available: 5000 (50.00 USDT)");
    eprintln!("      Locked: 5000 (50.00 USDT)");
    eprintln!("      Total: 10000 (100.00 USDT)");

    assert_eq!(
        balance.available.amount, 5000,
        "Available balance should be 5000 (50.00 USDT), got {}",
        balance.available.amount
    );
    assert_eq!(
        balance.locked.amount, 5000,
        "Locked balance should be 5000 (50.00 USDT), got {}",
        balance.locked.amount
    );
    eprintln!("✅ Balance verification PASSED ✓");

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("🔓 STEP 5: Release Funds (Order Cancelled)");
    eprintln!("{}", "─".repeat(80));
    eprintln!("   Request ID: cancel_order_1");
    eprintln!("   User ID: {}", user_id.0);
    eprintln!("   Asset: {}", asset);
    eprintln!(
        "   Amount: {} ({})",
        reserve_amount.amount,
        format_money(reserve_amount.amount, reserve_amount.scale)
    );
    let release_start = Instant::now();

    ledger
        .release(
            "cancel_order_1".to_string(),
            user_id,
            asset.to_string(),
            reserve_amount,
            None,
            None,
        )
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Release failed: {:?}", e);
            eprintln!(
                "   Request ID: cancel_order_1, User: {}, Asset: {}, Amount: {}",
                user_id.0, asset, reserve_amount.amount
            );
            panic!("Failed to release: {:?}", e);
        });

    let release_duration = release_start.elapsed();
    eprintln!("✅ Release completed in {:?}", release_duration);

    // Verify event was persisted
    let after_release_count = get_event_count(&pool, "ledger").await.unwrap_or(0);
    let last_sequence = get_last_sequence(&pool, "ledger").await.unwrap_or(None);
    eprintln!(
        "   📊 Events after release: {} (added: {})",
        after_release_count,
        after_release_count - after_reserve_count
    );
    eprintln!("   📊 Last sequence number: {:?}", last_sequence);

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("📊 STEP 6: Rebuild Read Model After Release");
    eprintln!("{}", "─".repeat(80));
    eprintln!(
        "   Projecting {} events to balance read model...",
        after_release_count
    );
    let project_start = Instant::now();

    let last_seq_before = get_last_sequence(&pool, "ledger").await.unwrap_or(None);
    read_model
        .rebuild_from_events(event_store.as_ref())
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to rebuild read model: {:?}", e);
            eprintln!("   Event count: {}", after_release_count);
            eprintln!("   Last sequence: {:?}", last_seq_before);
            panic!("Failed to rebuild read model: {:?}", e);
        });

    let project_duration = project_start.elapsed();
    eprintln!("✅ Read model rebuilt in {:?}", project_duration);
    eprintln!("   📊 Events processed: {}", after_release_count);

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("🔍 STEP 7: Verify Final Balance After Release");
    eprintln!("{}", "─".repeat(80));
    let balance = read_model
        .get_balance(user_id, asset)
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to get balance: {:?}", e);
            eprintln!("   User: {}, Asset: {}", user_id.0, asset);
            panic!("Failed to get balance: {:?}", e);
        })
        .unwrap_or_else(|| {
            eprintln!(
                "❌ Balance not found for user {} asset {}",
                user_id.0, asset
            );
            panic!("Balance should exist");
        });

    eprintln!("   📊 Final Balance Details:");
    eprintln!("      User ID: {}", user_id.0);
    eprintln!("      Asset: {}", asset);
    eprintln!(
        "      Available: {} ({})",
        balance.available.amount,
        format_money(balance.available.amount, balance.available.scale)
    );
    eprintln!(
        "      Locked: {} ({})",
        balance.locked.amount,
        format_money(balance.locked.amount, balance.locked.scale)
    );
    eprintln!(
        "      Total: {} ({})",
        balance.available.amount + balance.locked.amount,
        format_money(
            balance.available.amount + balance.locked.amount,
            balance.available.scale
        )
    );
    eprintln!(
        "      Last processed sequence: {}",
        balance.last_processed_sequence
    );

    eprintln!("\n   ✅ Expected Values:");
    eprintln!("      Available: 10000 (100.00 USDT)");
    eprintln!("      Locked: 0 (0.00 USDT)");
    eprintln!("      Total: 10000 (100.00 USDT)");

    assert_eq!(
        balance.available.amount, 10000,
        "Available balance should be 10000 (100.00 USDT), got {}",
        balance.available.amount
    );
    assert_eq!(
        balance.locked.amount, 0,
        "Locked balance should be 0, got {}",
        balance.locked.amount
    );
    eprintln!("✅ Final balance verification PASSED ✓");

    cleanup_test_data(&pool).await;

    eprintln!("\n{}", "=".repeat(80));
    eprintln!("✅ TEST COMPLETED SUCCESSFULLY");
    eprintln!("{}", "=".repeat(80));
    eprintln!("   Total test duration: {:?}", test_start.elapsed());
    eprintln!(
        "   Total events created: {}",
        after_release_count - initial_event_count
    );
    eprintln!("   Final event count: {}", after_release_count);
    eprintln!("{}\n", "=".repeat(80));
}

#[tokio::test]
async fn test_ledger_settle_trade_flow() {
    // Acquire mutex to ensure sequential test execution
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|poisoned| {
        eprintln!("⚠ Mutex was poisoned from previous test, clearing...");
        poisoned.into_inner()
    });

    eprintln!("\n{}", "=".repeat(80));
    eprintln!("🧪 TEST: test_ledger_settle_trade_flow");
    eprintln!("{}", "=".repeat(80));
    let test_start = Instant::now();

    eprintln!("\n📋 Test Purpose: Verify trade settlement flow");
    eprintln!("   - Buyer deposits and reserves quote asset (USDT)");
    eprintln!("   - Seller deposits and reserves base asset (BTC)");
    eprintln!("   - Settle trade between buyer and seller");
    eprintln!("   - Verify balances after settlement");

    let pool = setup_test_db().await;
    cleanup_test_data(&pool).await;

    eprintln!("\n📦 Creating event store and ledger...");
    let event_store = Arc::new(PostgresEventStore::with_stream_id(
        pool.clone(),
        "ledger".to_string(),
    ));
    eprintln!("   ✓ Event store created with stream_id: 'ledger'");

    let mut ledger = EventSourcedLedger::new(event_store.clone(), "test".to_string())
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to create ledger: {:?}", e);
            panic!("Failed to create ledger: {:?}", e);
        });
    eprintln!("✅ Ledger created");

    let initial_event_count = get_event_count(&pool, "ledger").await.unwrap_or(0);
    eprintln!("   📊 Initial event count: {}", initial_event_count);

    let buyer_id = UserId(1001);
    let seller_id = UserId(1002);

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("📥 STEP 1: Buyer Deposits and Reserves USDT");
    eprintln!("{}", "─".repeat(80));
    eprintln!("   Buyer ID: {}", buyer_id.0);
    eprintln!("   Asset: USDT");
    eprintln!("   Deposit: {} ({})", 10000, format_money(10000, 2));

    ledger
        .deposit(
            buyer_id,
            "USDT".to_string(),
            Money::new(10000, 2), // 100.00 USDT
            None,
            None,
            None,
        )
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Buyer deposit failed: {:?}", e);
            panic!("Failed to deposit: {:?}", e);
        });
    eprintln!("   ✅ Buyer deposit completed");

    eprintln!("   Reserving all USDT for buy order...");
    ledger
        .reserve(
            "buy_order_1".to_string(),
            buyer_id,
            "USDT".to_string(),
            Money::new(10000, 2), // Reserve all 100.00 USDT
            None,
            None,
        )
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Buyer reserve failed: {:?}", e);
            panic!("Failed to reserve: {:?}", e);
        });
    eprintln!("   ✅ Buyer reserve completed");

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("📥 STEP 2: Seller Deposits and Reserves BTC");
    eprintln!("{}", "─".repeat(80));
    eprintln!("   Seller ID: {}", seller_id.0);
    eprintln!("   Asset: BTC");
    eprintln!("   Deposit: {} ({})", 100000000, format_money(100000000, 8));

    ledger
        .deposit(
            seller_id,
            "BTC".to_string(),
            Money::new(100000000, 8), // 1.00000000 BTC
            None,
            None,
            None,
        )
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Seller deposit failed: {:?}", e);
            panic!("Failed to deposit: {:?}", e);
        });
    eprintln!("   ✅ Seller deposit completed");

    eprintln!("   Reserving all BTC for sell order...");
    ledger
        .reserve(
            "sell_order_1".to_string(),
            seller_id,
            "BTC".to_string(),
            Money::new(100000000, 8), // Reserve 1.00000000 BTC
            None,
            None,
        )
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Seller reserve failed: {:?}", e);
            panic!("Failed to reserve: {:?}", e);
        });
    eprintln!("   ✅ Seller reserve completed");

    let before_settle_count = get_event_count(&pool, "ledger").await.unwrap_or(0);
    eprintln!("   📊 Events before settlement: {}", before_settle_count);

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("💰 STEP 3: Settle Trade");
    eprintln!("{}", "─".repeat(80));
    eprintln!("   Trade ID: trade_1");
    eprintln!("   Buyer ID: {}", buyer_id.0);
    eprintln!("   Seller ID: {}", seller_id.0);
    eprintln!("   Base Asset: BTC");
    eprintln!("   Quote Asset: USDT");
    eprintln!(
        "   Base Amount: {} ({})",
        100000000,
        format_money(100000000, 8)
    );
    eprintln!("   Quote Amount: {} ({})", 10000, format_money(10000, 2));
    eprintln!("   Price: 10000 (100.00 USDT per BTC)");

    ledger
        .settle_trade(
            "trade_1".to_string(),
            buyer_id,
            seller_id,
            "BTC".to_string(),
            "USDT".to_string(),
            Money::new(100000000, 8), // 1.00000000 BTC
            Money::new(10000, 2),     // 100.00 USDT
            10000i64,                 // Price: 100.00 USDT per BTC (as i64)
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Trade settlement failed: {:?}", e);
            panic!("Failed to settle trade: {:?}", e);
        });

    let after_settle_count = get_event_count(&pool, "ledger").await.unwrap_or(0);
    eprintln!("   ✅ Trade settlement completed");
    eprintln!(
        "   📊 Events after settlement: {} (added: {})",
        after_settle_count,
        after_settle_count - before_settle_count
    );

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("📊 STEP 4: Project Events to Read Model");
    eprintln!("{}", "─".repeat(80));
    let read_model = BalanceReadModel::new(pool.clone());
    read_model
        .rebuild_from_events(event_store.as_ref())
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to rebuild read model: {:?}", e);
            // Debug: show all sequences to help diagnose the gap
            panic!("Failed to rebuild read model: {:?}", e);
        });
    eprintln!("   ✅ Read model rebuilt");
    eprintln!("   📊 Events processed: {}", after_settle_count);

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("🔍 STEP 5: Verify Buyer Balances");
    eprintln!("{}", "─".repeat(80));
    let buyer_btc = read_model
        .get_balance(buyer_id, "BTC")
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to get buyer BTC: {:?}", e);
            panic!("Failed to get buyer BTC: {:?}", e);
        })
        .unwrap_or_else(|| {
            eprintln!("❌ Buyer BTC balance not found");
            panic!("Buyer should have BTC");
        });

    eprintln!("   📊 Buyer BTC Balance:");
    eprintln!(
        "      Available: {} ({})",
        buyer_btc.available.amount,
        format_money(buyer_btc.available.amount, buyer_btc.available.scale)
    );
    eprintln!(
        "      Locked: {} ({})",
        buyer_btc.locked.amount,
        format_money(buyer_btc.locked.amount, buyer_btc.locked.scale)
    );
    eprintln!("      Expected: Available=100000000 (1.00000000 BTC), Locked=0");

    assert_eq!(
        buyer_btc.available.amount, 100000000,
        "Buyer should have 1.00000000 BTC available"
    );
    assert_eq!(buyer_btc.locked.amount, 0, "Buyer BTC should not be locked");
    eprintln!("   ✅ Buyer BTC balance verified");

    let buyer_usdt = read_model
        .get_balance(buyer_id, "USDT")
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to get buyer USDT: {:?}", e);
            panic!("Failed to get buyer USDT: {:?}", e);
        })
        .unwrap_or_else(|| {
            eprintln!("❌ Buyer USDT balance not found");
            panic!("Buyer should have USDT balance");
        });

    eprintln!("   📊 Buyer USDT Balance:");
    eprintln!(
        "      Available: {} ({})",
        buyer_usdt.available.amount,
        format_money(buyer_usdt.available.amount, buyer_usdt.available.scale)
    );
    eprintln!(
        "      Locked: {} ({})",
        buyer_usdt.locked.amount,
        format_money(buyer_usdt.locked.amount, buyer_usdt.locked.scale)
    );
    eprintln!("      Expected: Available=0, Locked=0 (all USDT was used)");

    assert_eq!(
        buyer_usdt.available.amount, 0,
        "Buyer should have no USDT available"
    );
    assert_eq!(
        buyer_usdt.locked.amount, 0,
        "Buyer should have no USDT locked"
    );
    eprintln!("   ✅ Buyer USDT balance verified");

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("🔍 STEP 6: Verify Seller Balances");
    eprintln!("{}", "─".repeat(80));
    let seller_usdt = read_model
        .get_balance(seller_id, "USDT")
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to get seller USDT: {:?}", e);
            panic!("Failed to get seller USDT: {:?}", e);
        })
        .unwrap_or_else(|| {
            eprintln!("❌ Seller USDT balance not found");
            panic!("Seller should have USDT");
        });

    eprintln!("   📊 Seller USDT Balance:");
    eprintln!(
        "      Available: {} ({})",
        seller_usdt.available.amount,
        format_money(seller_usdt.available.amount, seller_usdt.available.scale)
    );
    eprintln!(
        "      Locked: {} ({})",
        seller_usdt.locked.amount,
        format_money(seller_usdt.locked.amount, seller_usdt.locked.scale)
    );
    eprintln!("      Expected: Available=10000 (100.00 USDT), Locked=0");

    assert_eq!(
        seller_usdt.available.amount, 10000,
        "Seller should have 100.00 USDT available"
    );
    assert_eq!(
        seller_usdt.locked.amount, 0,
        "Seller USDT should not be locked"
    );
    eprintln!("   ✅ Seller USDT balance verified");

    let seller_btc = read_model
        .get_balance(seller_id, "BTC")
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to get seller BTC: {:?}", e);
            panic!("Failed to get seller BTC: {:?}", e);
        })
        .unwrap_or_else(|| {
            eprintln!("❌ Seller BTC balance not found");
            panic!("Seller should have BTC balance");
        });

    eprintln!("   📊 Seller BTC Balance:");
    eprintln!(
        "      Available: {} ({})",
        seller_btc.available.amount,
        format_money(seller_btc.available.amount, seller_btc.available.scale)
    );
    eprintln!(
        "      Locked: {} ({})",
        seller_btc.locked.amount,
        format_money(seller_btc.locked.amount, seller_btc.locked.scale)
    );
    eprintln!("      Expected: Available=0, Locked=0 (all BTC was sold)");

    assert_eq!(
        seller_btc.available.amount, 0,
        "Seller should have no BTC available"
    );
    assert_eq!(
        seller_btc.locked.amount, 0,
        "Seller should have no BTC locked"
    );
    eprintln!("   ✅ Seller BTC balance verified");

    cleanup_test_data(&pool).await;

    eprintln!("\n{}", "=".repeat(80));
    eprintln!("✅ TEST COMPLETED SUCCESSFULLY");
    eprintln!("{}", "=".repeat(80));
    eprintln!("   Total test duration: {:?}", test_start.elapsed());
    eprintln!(
        "   Total events created: {}",
        after_settle_count - initial_event_count
    );
    eprintln!("   Final event count: {}", after_settle_count);
    eprintln!("{}", "=".repeat(80));
}

#[tokio::test]
async fn test_event_persistence_and_replay() {
    // Acquire mutex to ensure sequential test execution
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|poisoned| {
        eprintln!("⚠ Mutex was poisoned from previous test, clearing...");
        poisoned.into_inner()
    });

    eprintln!("\n{}", "=".repeat(80));
    eprintln!("🧪 TEST: test_event_persistence_and_replay");
    eprintln!("{}", "=".repeat(80));
    let test_start = Instant::now();

    eprintln!("\n📋 Test Purpose: Verify event persistence and replay capability");
    eprintln!("   - Create ledger and perform operations");
    eprintln!("   - Simulate service restart (new ledger instance)");
    eprintln!("   - Verify state can be restored from persisted events");

    let pool = setup_test_db().await;
    cleanup_test_data(&pool).await;

    let event_store = Arc::new(PostgresEventStore::with_stream_id(
        pool.clone(),
        "ledger".to_string(),
    ));
    eprintln!("\n📦 Event store created with stream_id: 'ledger'");

    let user_id = UserId(1001);
    let initial_event_count = get_event_count(&pool, "ledger").await.unwrap_or(0);
    eprintln!("   📊 Initial event count: {}", initial_event_count);

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("📝 STEP 1: Create Ledger and Perform Operations");
    eprintln!("{}", "─".repeat(80));
    eprintln!("   User ID: {}", user_id.0);

    // 1. Create ledger and perform operations
    {
        let mut ledger = EventSourcedLedger::new(event_store.clone(), "test".to_string())
            .await
            .unwrap_or_else(|e| {
                eprintln!("❌ Failed to create ledger: {:?}", e);
                panic!("Failed to create ledger: {:?}", e);
            });
        eprintln!("   ✅ Ledger instance 1 created");

        eprintln!("   Depositing 100.00 USDT...");
        ledger
            .deposit(
                user_id,
                "USDT".to_string(),
                Money::new(10000, 2),
                None,
                None,
                None,
            )
            .await
            .unwrap_or_else(|e| {
                eprintln!("❌ Deposit failed: {:?}", e);
                panic!("Failed to deposit: {:?}", e);
            });
        eprintln!("   ✅ Deposit completed");

        eprintln!("   Reserving 30.00 USDT for order_1...");
        ledger
            .reserve(
                "order_1".to_string(),
                user_id,
                "USDT".to_string(),
                Money::new(3000, 2),
                None,
                None,
            )
            .await
            .unwrap_or_else(|e| {
                eprintln!("❌ Reserve failed: {:?}", e);
                panic!("Failed to reserve: {:?}", e);
            });
        eprintln!("   ✅ Reserve completed");
    }
    eprintln!("   📊 Ledger instance 1 dropped (simulating service shutdown)");

    let after_ops_count = get_event_count(&pool, "ledger").await.unwrap_or(0);
    eprintln!(
        "   📊 Events persisted: {} (added: {})",
        after_ops_count,
        after_ops_count - initial_event_count
    );

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("🔄 STEP 2: Create New Ledger Instance (Simulate Restart)");
    eprintln!("{}", "─".repeat(80));
    // 2. Create a new ledger instance (simulating restart)
    let _ledger2 = EventSourcedLedger::new(event_store.clone(), "test".to_string())
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to create ledger instance 2: {:?}", e);
            panic!("Failed to create ledger: {:?}", e);
        });
    eprintln!("   ✅ Ledger instance 2 created (should replay events)");

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("📊 STEP 3: Rebuild Read Model from Persisted Events");
    eprintln!("{}", "─".repeat(80));
    // 3. Verify state was restored from events using read model
    let read_model = BalanceReadModel::new(pool.clone());
    read_model
        .rebuild_from_events(event_store.as_ref())
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to rebuild read model: {:?}", e);
            // Debug: show all sequences to help diagnose the gap
            panic!("Failed to rebuild read model: {:?}", e);
        });
    eprintln!("   ✅ Read model rebuilt from {} events", after_ops_count);

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("🔍 STEP 4: Verify State Restored Correctly");
    eprintln!("{}", "─".repeat(80));
    let balance = read_model
        .get_balance(user_id, "USDT")
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to get balance: {:?}", e);
            panic!("Failed to get balance: {:?}", e);
        })
        .unwrap_or_else(|| {
            eprintln!("❌ Balance not found");
            panic!("Balance should exist");
        });

    eprintln!("   📊 Restored Balance:");
    eprintln!("      User ID: {}", user_id.0);
    eprintln!("      Asset: USDT");
    eprintln!(
        "      Available: {} ({})",
        balance.available.amount,
        format_money(balance.available.amount, balance.available.scale)
    );
    eprintln!(
        "      Locked: {} ({})",
        balance.locked.amount,
        format_money(balance.locked.amount, balance.locked.scale)
    );
    eprintln!(
        "      Last processed sequence: {}",
        balance.last_processed_sequence
    );

    eprintln!("\n   ✅ Expected Values:");
    eprintln!("      Available: 7000 (70.00 USDT)");
    eprintln!("      Locked: 3000 (30.00 USDT)");

    assert_eq!(
        balance.available.amount, 7000,
        "Available should be 7000 (70.00 USDT)"
    );
    assert_eq!(
        balance.locked.amount, 3000,
        "Locked should be 3000 (30.00 USDT)"
    );
    eprintln!("   ✅ State restoration verification PASSED ✓");

    cleanup_test_data(&pool).await;

    eprintln!("\n{}", "=".repeat(80));
    eprintln!("✅ TEST COMPLETED SUCCESSFULLY");
    eprintln!("{}", "=".repeat(80));
    eprintln!("   Total test duration: {:?}", test_start.elapsed());
    eprintln!(
        "   Events persisted: {}",
        after_ops_count - initial_event_count
    );
    eprintln!("{}", "=".repeat(80));
}

#[tokio::test]
async fn test_balance_read_model_projection() {
    // Acquire mutex to ensure sequential test execution
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|poisoned| {
        eprintln!("⚠ Mutex was poisoned from previous test, clearing...");
        poisoned.into_inner()
    });

    eprintln!("\n{}", "=".repeat(80));
    eprintln!("🧪 TEST: test_balance_read_model_projection");
    eprintln!("{}", "=".repeat(80));
    let test_start = Instant::now();

    eprintln!("\n📋 Test Purpose: Verify balance read model projection from events");
    eprintln!("   - Create ledger and perform operations");
    eprintln!("   - Project events to balance read model");
    eprintln!("   - Query read model and verify correctness");

    let pool = setup_test_db().await;
    cleanup_test_data(&pool).await;

    let event_store = Arc::new(PostgresEventStore::with_stream_id(
        pool.clone(),
        "ledger".to_string(),
    ));
    eprintln!("\n📦 Event store created with stream_id: 'ledger'");

    let read_model = BalanceReadModel::new(pool.clone());
    eprintln!("✅ Balance read model created");

    let user_id = UserId(1001);
    let initial_event_count = get_event_count(&pool, "ledger").await.unwrap_or(0);
    eprintln!("   📊 Initial event count: {}", initial_event_count);

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("📝 STEP 1: Create Ledger and Perform Operations");
    eprintln!("{}", "─".repeat(80));
    let mut ledger = EventSourcedLedger::new(event_store.clone(), "test".to_string())
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to create ledger: {:?}", e);
            panic!("Failed to create ledger: {:?}", e);
        });
    eprintln!("   ✅ Ledger created");

    eprintln!("   Depositing 100.00 USDT...");
    ledger
        .deposit(
            user_id,
            "USDT".to_string(),
            Money::new(10000, 2),
            None,
            None,
            None,
        )
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Deposit failed: {:?}", e);
            panic!("Failed to deposit: {:?}", e);
        });
    eprintln!("   ✅ Deposit completed");

    eprintln!("   Reserving 40.00 USDT for order_1...");
    ledger
        .reserve(
            "order_1".to_string(),
            user_id,
            "USDT".to_string(),
            Money::new(4000, 2),
            None,
            None,
        )
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Reserve failed: {:?}", e);
            panic!("Failed to reserve: {:?}", e);
        });
    eprintln!("   ✅ Reserve completed");

    let after_ops_count = get_event_count(&pool, "ledger").await.unwrap_or(0);
    eprintln!(
        "   📊 Events created: {} (added: {})",
        after_ops_count,
        after_ops_count - initial_event_count
    );

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("📊 STEP 2: Project Events to Read Model");
    eprintln!("{}", "─".repeat(80));
    eprintln!(
        "   Projecting {} events to balance read model...",
        after_ops_count
    );
    let project_start = Instant::now();

    read_model
        .rebuild_from_events(event_store.as_ref())
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to rebuild read model: {:?}", e);
            // Debug: show all sequences to help diagnose the gap
            panic!("Failed to rebuild read model: {:?}", e);
        });

    let project_duration = project_start.elapsed();
    eprintln!(
        "   ✅ Read model projection completed in {:?}",
        project_duration
    );
    eprintln!("   📊 Events processed: {}", after_ops_count);

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("🔍 STEP 3: Query Read Model and Verify");
    eprintln!("{}", "─".repeat(80));
    let balance = read_model
        .get_balance(user_id, "USDT")
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to get balance: {:?}", e);
            panic!("Failed to get balance: {:?}", e);
        })
        .unwrap_or_else(|| {
            eprintln!("❌ Balance not found");
            panic!("Balance should exist");
        });

    eprintln!("   📊 Projected Balance:");
    eprintln!("      User ID: {}", user_id.0);
    eprintln!("      Asset: USDT");
    eprintln!(
        "      Available: {} ({})",
        balance.available.amount,
        format_money(balance.available.amount, balance.available.scale)
    );
    eprintln!(
        "      Locked: {} ({})",
        balance.locked.amount,
        format_money(balance.locked.amount, balance.locked.scale)
    );
    eprintln!(
        "      Total: {} ({})",
        balance.available.amount + balance.locked.amount,
        format_money(
            balance.available.amount + balance.locked.amount,
            balance.available.scale
        )
    );
    eprintln!(
        "      Last processed sequence: {}",
        balance.last_processed_sequence
    );

    eprintln!("\n   ✅ Expected Values:");
    eprintln!("      Available: 6000 (60.00 USDT)");
    eprintln!("      Locked: 4000 (40.00 USDT)");
    eprintln!("      Total: 10000 (100.00 USDT)");

    assert_eq!(
        balance.available.amount, 6000,
        "Available should be 6000 (60.00 USDT)"
    );
    assert_eq!(
        balance.locked.amount, 4000,
        "Locked should be 4000 (40.00 USDT)"
    );
    eprintln!("   ✅ Read model projection verification PASSED ✓");

    cleanup_test_data(&pool).await;

    eprintln!("\n{}", "=".repeat(80));
    eprintln!("✅ TEST COMPLETED SUCCESSFULLY");
    eprintln!("{}", "=".repeat(80));
    eprintln!("   Total test duration: {:?}", test_start.elapsed());
    eprintln!("   Projection duration: {:?}", project_duration);
    eprintln!(
        "   Events processed: {}",
        after_ops_count - initial_event_count
    );
    eprintln!("{}", "=".repeat(80));
}

#[tokio::test]
async fn test_idempotency() {
    // Acquire mutex to ensure sequential test execution
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|poisoned| {
        eprintln!("⚠ Mutex was poisoned from previous test, clearing...");
        poisoned.into_inner()
    });

    eprintln!("\n{}", "=".repeat(80));
    eprintln!("🧪 TEST: test_idempotency");
    eprintln!("{}", "=".repeat(80));
    let test_start = Instant::now();

    eprintln!("\n📋 Test Purpose: Verify idempotency of reserve operations");
    eprintln!("   - Deposit funds");
    eprintln!("   - Reserve funds with request_id 'order_1'");
    eprintln!("   - Attempt to reserve again with same request_id");
    eprintln!("   - Verify state remains unchanged (idempotent)");

    let pool = setup_test_db().await;
    cleanup_test_data(&pool).await;

    let event_store = Arc::new(PostgresEventStore::with_stream_id(
        pool.clone(),
        "ledger".to_string(),
    ));
    eprintln!("\n📦 Event store created with stream_id: 'ledger'");

    let mut ledger = EventSourcedLedger::new(event_store.clone(), "test".to_string())
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to create ledger: {:?}", e);
            panic!("Failed to create ledger: {:?}", e);
        });
    eprintln!("✅ Ledger created");

    let user_id = UserId(1001);
    let initial_event_count = get_event_count(&pool, "ledger").await.unwrap_or(0);
    eprintln!("   📊 Initial event count: {}", initial_event_count);

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("📥 STEP 1: Deposit Funds");
    eprintln!("{}", "─".repeat(80));
    eprintln!("   User ID: {}", user_id.0);
    eprintln!("   Amount: {} ({})", 10000, format_money(10000, 2));

    ledger
        .deposit(
            user_id,
            "USDT".to_string(),
            Money::new(10000, 2),
            None,
            None,
            None,
        )
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Deposit failed: {:?}", e);
            panic!("Failed to deposit: {:?}", e);
        });
    eprintln!("   ✅ Deposit completed");

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("🔒 STEP 2: Reserve Funds (First Time)");
    eprintln!("{}", "─".repeat(80));
    eprintln!("   Request ID: order_1");
    eprintln!("   User ID: {}", user_id.0);
    eprintln!("   Amount: {} ({})", 5000, format_money(5000, 2));

    ledger
        .reserve(
            "order_1".to_string(),
            user_id,
            "USDT".to_string(),
            Money::new(5000, 2),
            None,
            None,
        )
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Reserve failed: {:?}", e);
            panic!("Failed to reserve: {:?}", e);
        });
    eprintln!("   ✅ Reserve completed");

    let after_first_reserve = get_event_count(&pool, "ledger").await.unwrap_or(0);
    eprintln!(
        "   📊 Events after first reserve: {} (added: {})",
        after_first_reserve,
        after_first_reserve - initial_event_count
    );

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("📊 STEP 3: Get Balance Before Idempotent Operation");
    eprintln!("{}", "─".repeat(80));
    let read_model = BalanceReadModel::new(pool.clone());
    read_model
        .rebuild_from_events(event_store.as_ref())
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to rebuild read model: {:?}", e);
            // Debug: show all sequences to help diagnose the gap
            panic!("Failed to rebuild read model: {:?}", e);
        });

    let balance_before = read_model
        .get_balance(user_id, "USDT")
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to get balance: {:?}", e);
            panic!("Failed to get balance: {:?}", e);
        })
        .unwrap_or_else(|| {
            eprintln!("❌ Balance not found");
            panic!("Balance should exist");
        });

    let available_before = balance_before.available.amount;
    let locked_before = balance_before.locked.amount;

    eprintln!("   📊 Balance Before Idempotent Reserve:");
    eprintln!(
        "      Available: {} ({})",
        available_before,
        format_money(available_before, balance_before.available.scale)
    );
    eprintln!(
        "      Locked: {} ({})",
        locked_before,
        format_money(locked_before, balance_before.locked.scale)
    );

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("🔄 STEP 4: Attempt Idempotent Reserve (Same Request ID)");
    eprintln!("{}", "─".repeat(80));
    eprintln!("   Request ID: order_1 (same as before)");
    eprintln!("   Expected: Operation should be idempotent (no state change)");

    ledger
        .reserve(
            "order_1".to_string(),
            user_id,
            "USDT".to_string(),
            Money::new(5000, 2),
            None,
            None,
        )
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Idempotent reserve failed: {:?}", e);
            panic!("Failed to reserve (idempotent): {:?}", e);
        });
    eprintln!("   ✅ Idempotent reserve completed (should be no-op)");

    let after_idempotent = get_event_count(&pool, "ledger").await.unwrap_or(0);
    eprintln!(
        "   📊 Events after idempotent reserve: {} (added: {})",
        after_idempotent,
        after_idempotent - after_first_reserve
    );
    eprintln!("   ℹ️  Note: If idempotent, no new events should be added");

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("🔍 STEP 5: Verify State Unchanged (Idempotency Check)");
    eprintln!("{}", "─".repeat(80));
    read_model
        .rebuild_from_events(event_store.as_ref())
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to rebuild read model: {:?}", e);
            // Debug: show all sequences to help diagnose the gap
            panic!("Failed to rebuild read model: {:?}", e);
        });

    let balance_after = read_model
        .get_balance(user_id, "USDT")
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to get balance: {:?}", e);
            panic!("Failed to get balance: {:?}", e);
        })
        .unwrap_or_else(|| {
            eprintln!("❌ Balance not found");
            panic!("Balance should exist");
        });

    eprintln!("   📊 Balance After Idempotent Reserve:");
    eprintln!(
        "      Available: {} ({})",
        balance_after.available.amount,
        format_money(
            balance_after.available.amount,
            balance_after.available.scale
        )
    );
    eprintln!(
        "      Locked: {} ({})",
        balance_after.locked.amount,
        format_money(balance_after.locked.amount, balance_after.locked.scale)
    );

    eprintln!("\n   ✅ Comparison:");
    eprintln!(
        "      Available: {} (before) vs {} (after) - {}",
        available_before,
        balance_after.available.amount,
        if available_before == balance_after.available.amount {
            "MATCH ✓"
        } else {
            "MISMATCH ✗"
        }
    );
    eprintln!(
        "      Locked: {} (before) vs {} (after) - {}",
        locked_before,
        balance_after.locked.amount,
        if locked_before == balance_after.locked.amount {
            "MATCH ✓"
        } else {
            "MISMATCH ✗"
        }
    );

    assert_eq!(
        balance_after.available.amount, available_before,
        "Available balance should not change (idempotent)"
    );
    assert_eq!(
        balance_after.locked.amount, locked_before,
        "Locked balance should not change (idempotent)"
    );
    eprintln!("   ✅ Idempotency verification PASSED ✓");

    cleanup_test_data(&pool).await;

    eprintln!("\n{}", "=".repeat(80));
    eprintln!("✅ TEST COMPLETED SUCCESSFULLY");
    eprintln!("{}", "=".repeat(80));
    eprintln!("   Total test duration: {:?}", test_start.elapsed());
    eprintln!(
        "   Events created: {}",
        after_idempotent - initial_event_count
    );
    eprintln!("{}", "=".repeat(80));
}

#[tokio::test]
async fn test_end_to_end_order_flow() {
    // Acquire mutex to ensure sequential test execution
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|poisoned| {
        eprintln!("⚠ Mutex was poisoned from previous test, clearing...");
        poisoned.into_inner()
    });

    eprintln!("\n{}", "=".repeat(80));
    eprintln!("🧪 TEST: test_end_to_end_order_flow");
    eprintln!("{}", "=".repeat(80));
    let test_start = Instant::now();

    eprintln!("\n📋 Test Purpose: End-to-end order flow verification");
    eprintln!("   - Buyer deposits and reserves quote asset (USDT)");
    eprintln!("   - Seller deposits and reserves base asset (BTC)");
    eprintln!("   - Trade execution and settlement");
    eprintln!("   - Verify final balances for both parties");

    let pool = setup_test_db().await;
    cleanup_test_data(&pool).await;

    let event_store = Arc::new(PostgresEventStore::with_stream_id(
        pool.clone(),
        "ledger".to_string(),
    ));
    eprintln!("\n📦 Event store created with stream_id: 'ledger'");

    let mut ledger = EventSourcedLedger::new(event_store.clone(), "test".to_string())
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to create ledger: {:?}", e);
            panic!("Failed to create ledger: {:?}", e);
        });
    eprintln!("✅ Ledger created");

    let buyer_id = UserId(1001);
    let seller_id = UserId(1002);
    let initial_event_count = get_event_count(&pool, "ledger").await.unwrap_or(0);
    eprintln!("   📊 Initial event count: {}", initial_event_count);

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("📥 STEP 1: Buyer Deposits and Reserves USDT");
    eprintln!("{}", "─".repeat(80));
    eprintln!("   Buyer ID: {}", buyer_id.0);
    eprintln!("   Asset: USDT");
    eprintln!("   Deposit: {} ({})", 10000, format_money(10000, 2));

    ledger
        .deposit(
            buyer_id,
            "USDT".to_string(),
            Money::new(10000, 2), // 100.00 USDT
            None,
            None,
            None,
        )
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Buyer deposit failed: {:?}", e);
            panic!("Failed to deposit: {:?}", e);
        });
    eprintln!("   ✅ Buyer deposit completed");

    eprintln!("   Reserving all USDT for buy_order_1...");
    ledger
        .reserve(
            "buy_order_1".to_string(),
            buyer_id,
            "USDT".to_string(),
            Money::new(10000, 2),
            None,
            None,
        )
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Buyer reserve failed: {:?}", e);
            panic!("Failed to reserve: {:?}", e);
        });
    eprintln!("   ✅ Buyer reserve completed");

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("📥 STEP 2: Seller Deposits and Reserves BTC");
    eprintln!("{}", "─".repeat(80));
    eprintln!("   Seller ID: {}", seller_id.0);
    eprintln!("   Asset: BTC");
    eprintln!("   Deposit: {} ({})", 100000000, format_money(100000000, 8));

    ledger
        .deposit(
            seller_id,
            "BTC".to_string(),
            Money::new(100000000, 8), // 1.00000000 BTC
            None,
            None,
            None,
        )
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Seller deposit failed: {:?}", e);
            panic!("Failed to deposit: {:?}", e);
        });
    eprintln!("   ✅ Seller deposit completed");

    eprintln!("   Reserving all BTC for sell_order_1...");
    ledger
        .reserve(
            "sell_order_1".to_string(),
            seller_id,
            "BTC".to_string(),
            Money::new(100000000, 8),
            None,
            None,
        )
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Seller reserve failed: {:?}", e);
            panic!("Failed to reserve: {:?}", e);
        });
    eprintln!("   ✅ Seller reserve completed");

    let before_trade_count = get_event_count(&pool, "ledger").await.unwrap_or(0);
    eprintln!(
        "   📊 Events before trade: {} (added: {})",
        before_trade_count,
        before_trade_count - initial_event_count
    );

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("💰 STEP 3: Execute Trade Settlement");
    eprintln!("{}", "─".repeat(80));
    eprintln!("   Trade ID: trade_1");
    eprintln!("   Buyer ID: {}", buyer_id.0);
    eprintln!("   Seller ID: {}", seller_id.0);
    eprintln!("   Base Asset: BTC");
    eprintln!("   Quote Asset: USDT");
    eprintln!(
        "   Base Amount: {} ({})",
        100000000,
        format_money(100000000, 8)
    );
    eprintln!("   Quote Amount: {} ({})", 10000, format_money(10000, 2));
    eprintln!("   Price: 10000 (100.00 USDT per BTC)");

    ledger
        .settle_trade(
            "trade_1".to_string(),
            buyer_id,
            seller_id,
            "BTC".to_string(),
            "USDT".to_string(),
            Money::new(100000000, 8), // 1.00000000 BTC
            Money::new(10000, 2),     // 100.00 USDT
            10000i64,                 // Price: 100.00 USDT per BTC (as i64)
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Trade settlement failed: {:?}", e);
            panic!("Failed to settle trade: {:?}", e);
        });

    let after_trade_count = get_event_count(&pool, "ledger").await.unwrap_or(0);
    eprintln!("   ✅ Trade settlement completed");
    eprintln!(
        "   📊 Events after trade: {} (added: {})",
        after_trade_count,
        after_trade_count - before_trade_count
    );

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("📊 STEP 4: Project Events to Read Model");
    eprintln!("{}", "─".repeat(80));
    let read_model = BalanceReadModel::new(pool.clone());
    read_model
        .rebuild_from_events(event_store.as_ref())
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to rebuild read model: {:?}", e);
            // Debug: show all sequences to help diagnose the gap
            panic!("Failed to rebuild read model: {:?}", e);
        });
    eprintln!("   ✅ Read model rebuilt");
    eprintln!("   📊 Events processed: {}", after_trade_count);

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("🔍 STEP 5: Verify Buyer Balances");
    eprintln!("{}", "─".repeat(80));
    let buyer_btc = read_model
        .get_balance(buyer_id, "BTC")
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to get buyer BTC: {:?}", e);
            panic!("Failed to get buyer BTC balance: {:?}", e);
        })
        .unwrap_or_else(|| {
            eprintln!("❌ Buyer BTC balance not found");
            panic!("Buyer should have BTC balance");
        });

    eprintln!("   📊 Buyer BTC Balance:");
    eprintln!(
        "      Available: {} ({})",
        buyer_btc.available.amount,
        format_money(buyer_btc.available.amount, buyer_btc.available.scale)
    );
    eprintln!(
        "      Locked: {} ({})",
        buyer_btc.locked.amount,
        format_money(buyer_btc.locked.amount, buyer_btc.locked.scale)
    );
    eprintln!("      Expected: Available=100000000 (1.00000000 BTC), Locked=0");

    assert_eq!(
        buyer_btc.available.amount, 100000000,
        "Buyer should have 1.00000000 BTC"
    );
    assert_eq!(buyer_btc.locked.amount, 0, "Buyer BTC should not be locked");
    eprintln!("   ✅ Buyer BTC balance verified");

    let buyer_usdt = read_model
        .get_balance(buyer_id, "USDT")
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to get buyer USDT: {:?}", e);
            panic!("Failed to get buyer USDT balance: {:?}", e);
        })
        .unwrap_or_else(|| {
            eprintln!("❌ Buyer USDT balance not found");
            panic!("Buyer should have USDT balance");
        });

    eprintln!("   📊 Buyer USDT Balance:");
    eprintln!(
        "      Available: {} ({})",
        buyer_usdt.available.amount,
        format_money(buyer_usdt.available.amount, buyer_usdt.available.scale)
    );
    eprintln!(
        "      Locked: {} ({})",
        buyer_usdt.locked.amount,
        format_money(buyer_usdt.locked.amount, buyer_usdt.locked.scale)
    );
    eprintln!("      Expected: Available=0, Locked=0 (all USDT used for trade)");

    assert_eq!(
        buyer_usdt.available.amount, 0,
        "Buyer should have no USDT available"
    );
    assert_eq!(
        buyer_usdt.locked.amount, 0,
        "Buyer should have no USDT locked"
    );
    eprintln!("   ✅ Buyer USDT balance verified");

    eprintln!("\n{}", "─".repeat(80));
    eprintln!("🔍 STEP 6: Verify Seller Balances");
    eprintln!("{}", "─".repeat(80));
    let seller_usdt = read_model
        .get_balance(seller_id, "USDT")
        .await
        .unwrap_or_else(|e| {
            eprintln!("❌ Failed to get seller USDT: {:?}", e);
            panic!("Failed to get seller USDT balance: {:?}", e);
        })
        .unwrap_or_else(|| {
            eprintln!("❌ Seller USDT balance not found");
            panic!("Seller should have USDT balance");
        });

    eprintln!("   📊 Seller USDT Balance:");
    eprintln!(
        "      Available: {} ({})",
        seller_usdt.available.amount,
        format_money(seller_usdt.available.amount, seller_usdt.available.scale)
    );
    eprintln!(
        "      Locked: {} ({})",
        seller_usdt.locked.amount,
        format_money(seller_usdt.locked.amount, seller_usdt.locked.scale)
    );
    eprintln!("      Expected: Available=10000 (100.00 USDT), Locked=0");

    assert_eq!(
        seller_usdt.available.amount, 10000,
        "Seller should have 100.00 USDT"
    );
    assert_eq!(
        seller_usdt.locked.amount, 0,
        "Seller USDT should not be locked"
    );
    eprintln!("   ✅ Seller USDT balance verified");

    cleanup_test_data(&pool).await;

    eprintln!("\n{}", "=".repeat(80));
    eprintln!("✅ TEST COMPLETED SUCCESSFULLY");
    eprintln!("{}", "=".repeat(80));
    eprintln!("   Total test duration: {:?}", test_start.elapsed());
    eprintln!(
        "   Total events created: {}",
        after_trade_count - initial_event_count
    );
    eprintln!("   Final event count: {}", after_trade_count);
    eprintln!("{}", "=".repeat(80));
}
