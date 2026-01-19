//! Balance Read Model
//!
//! A read-only projection of user balances derived exclusively from immutable ledger events.
//!
//! Key principles:
//! - **Read-only**: Never used for validation or business logic
//! - **Derived state**: Computed entirely from ledger events
//! - **Rebuildable**: Can be reconstructed from scratch by replaying all events
//! - **Idempotent**: Safe to reproject events multiple times
//!
//! The read model tracks:
//! - **Available balance**: Funds available for trading and withdrawals
//! - **Locked balance**: Funds reserved for active orders
//!
//! Balance changes are derived from these ledger events:
//! - `FundsDeposited`: Increases available balance
//! - `FundsWithdrawn`: Decreases available balance
//! - `FundsReserved`: Transfers from available to locked
//! - `FundsReleased`: Transfers from locked back to available
//! - `TradeSettled`: Settles trade between buyer and seller (both balances affected)

use crate::event_store::{EventStore, EventStoreError, EventStoreResult};
use scorpius_core::{ledger_event::LedgerEvent, BalanceSnapshot, Money, UserId};
use sqlx::{Pool, Postgres, Row};
use std::sync::Arc;
use uuid::Uuid;

/// Balance read model projection service
///
/// Projects ledger events into balance snapshots stored in the database.
/// The projection is idempotent and can be safely rebuilt from scratch.
pub struct BalanceReadModel {
    pool: Arc<Pool<Postgres>>,
}

impl BalanceReadModel {
    /// Create a new balance read model projection service
    pub fn new(pool: Arc<Pool<Postgres>>) -> Self {
        Self { pool }
    }

    /// Project a single ledger event onto the balance read model
    ///
    /// This function is idempotent: applying the same event multiple times
    /// will result in the same final state. The projection checks the
    /// last_processed_event_id to ensure idempotency.
    pub async fn project_event(&self, event: &LedgerEvent) -> EventStoreResult<()> {
        let event_id = event.event_id();
        let sequence = event.sequence();

        match event {
            LedgerEvent::FundsDeposited {
                user_id,
                asset,
                amount,
                ..
            } => {
                self.update_balance(
                    *user_id,
                    asset,
                    amount.scale,
                    |available, _locked| *available = *available + *amount,
                    event_id,
                    sequence,
                )
                .await?;
            }

            LedgerEvent::FundsWithdrawn {
                user_id,
                asset,
                amount,
                ..
            } => {
                self.update_balance(
                    *user_id,
                    asset,
                    amount.scale,
                    |available, _locked| *available = *available - *amount,
                    event_id,
                    sequence,
                )
                .await?;
            }

            LedgerEvent::FundsReserved {
                user_id,
                asset,
                amount,
                ..
            } => {
                self.update_balance(
                    *user_id,
                    asset,
                    amount.scale,
                    |available, locked| {
                        *available = *available - *amount;
                        *locked = *locked + *amount;
                    },
                    event_id,
                    sequence,
                )
                .await?;
            }

            LedgerEvent::FundsReleased {
                user_id,
                asset,
                amount,
                ..
            } => {
                self.update_balance(
                    *user_id,
                    asset,
                    amount.scale,
                    |available, locked| {
                        *available = *available + *amount;
                        *locked = *locked - *amount;
                    },
                    event_id,
                    sequence,
                )
                .await?;
            }

            LedgerEvent::TradeSettled {
                buyer_id,
                seller_id,
                base_asset,
                quote_asset,
                base_amount,
                quote_amount,
                ..
            } => {
                // Buyer: locked quote -> available base
                self.update_balance(
                    *buyer_id,
                    quote_asset,
                    quote_amount.scale,
                    |_available, locked| *locked = *locked - *quote_amount,
                    event_id,
                    sequence,
                )
                .await?;

                self.update_balance(
                    *buyer_id,
                    base_asset,
                    base_amount.scale,
                    |available, _locked| *available = *available + *base_amount,
                    event_id,
                    sequence,
                )
                .await?;

                // Seller: locked base -> available quote
                self.update_balance(
                    *seller_id,
                    base_asset,
                    base_amount.scale,
                    |_available, locked| *locked = *locked - *base_amount,
                    event_id,
                    sequence,
                )
                .await?;

                self.update_balance(
                    *seller_id,
                    quote_asset,
                    quote_amount.scale,
                    |available, _locked| *available = *available + *quote_amount,
                    event_id,
                    sequence,
                )
                .await?;
            }
        }

        Ok(())
    }

    /// Internal helper to update a balance atomically
    ///
    /// The update is idempotent: if the event was already processed (same or higher sequence),
    /// the update is skipped. This ensures safe replay of events.
    ///
    /// The `initial_scale` parameter is used when creating a new balance entry.
    /// It should match the scale of the Money values being operated on in the update function.
    async fn update_balance<F>(
        &self,
        user_id: UserId,
        asset: &str,
        initial_scale: u32,
        update_fn: F,
        event_id: &str,
        sequence: u64,
    ) -> EventStoreResult<()>
    where
        F: FnOnce(&mut Money, &mut Money),
    {
        // Parse event_id as UUID for database storage
        let event_uuid = Uuid::parse_str(event_id)
            .map_err(|e| EventStoreError::Database(format!("Invalid event_id UUID: {}", e)))?;

        // Use a transaction for atomic read-modify-write
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| EventStoreError::Database(e.to_string()))?;

        // Read current balance (if exists)
        let existing = sqlx::query(
            r#"
            SELECT available_amount, available_scale, locked_amount, locked_scale, last_processed_sequence
            FROM balance_snapshots
            WHERE user_id = $1 AND asset = $2
            FOR UPDATE
            "#,
        )
        .bind(user_id.0 as i64)
        .bind(asset)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| EventStoreError::Database(e.to_string()))?;

        // Check idempotency: skip if event already processed
        if let Some(row) = &existing {
            let existing_seq = row.get::<i64, _>("last_processed_sequence") as u64;
            if existing_seq >= sequence {
                // Event already processed, skip update
                tx.commit()
                    .await
                    .map_err(|e| EventStoreError::Database(e.to_string()))?;
                return Ok(());
            }
        }

        // Get current balances or initialize with zero
        let (mut available, mut locked) = if let Some(row) = existing {
            (
                Money::new(
                    row.get::<i64, _>("available_amount"),
                    row.get::<i32, _>("available_scale") as u32,
                ),
                Money::new(
                    row.get::<i64, _>("locked_amount"),
                    row.get::<i32, _>("locked_scale") as u32,
                ),
            )
        } else {
            // Initialize with zero balance using the provided scale
            // This ensures Money arithmetic operations will work correctly
            (Money::new(0, initial_scale), Money::new(0, initial_scale))
        };

        // Apply the update function to compute new balances
        update_fn(&mut available, &mut locked);

        // Normalize scales: ensure both balances use the same scale
        // Use the maximum scale of the two to preserve precision
        let target_scale = available.scale.max(locked.scale);

        if available.scale != target_scale {
            let scale_diff = target_scale - available.scale;
            available = Money::new(
                available
                    .amount
                    .checked_mul(10i64.pow(scale_diff))
                    .ok_or_else(|| {
                        EventStoreError::Database("Scale conversion overflow".to_string())
                    })?,
                target_scale,
            );
        }

        if locked.scale != target_scale {
            let scale_diff = target_scale - locked.scale;
            locked = Money::new(
                locked
                    .amount
                    .checked_mul(10i64.pow(scale_diff))
                    .ok_or_else(|| {
                        EventStoreError::Database("Scale conversion overflow".to_string())
                    })?,
                target_scale,
            );
        }

        // Ensure non-negative balances (read model constraint, not validation)
        if available.amount < 0 || locked.amount < 0 {
            return Err(EventStoreError::Database(format!(
                "Balance would become negative: user_id={}, asset={}, available={}, locked={}",
                user_id.0, asset, available.amount, locked.amount
            )));
        }

        // Upsert the updated balance
        sqlx::query(
            r#"
            INSERT INTO balance_snapshots (
                user_id, asset, available_amount, available_scale,
                locked_amount, locked_scale, last_processed_event_id, last_processed_sequence
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (user_id, asset) DO UPDATE
            SET
                available_amount = $3,
                available_scale = $4,
                locked_amount = $5,
                locked_scale = $6,
                last_processed_event_id = $7,
                last_processed_sequence = $8,
                last_updated_at = NOW()
            "#,
        )
        .bind(user_id.0 as i64)
        .bind(asset)
        .bind(available.amount)
        .bind(available.scale as i32)
        .bind(locked.amount)
        .bind(locked.scale as i32)
        .bind(&event_uuid)
        .bind(sequence as i64)
        .execute(&mut *tx)
        .await
        .map_err(|e| EventStoreError::Database(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| EventStoreError::Database(e.to_string()))?;

        Ok(())
    }

    /// Rebuild the entire balance read model from scratch
    ///
    /// This function:
    /// 1. Clears all existing balance snapshots
    /// 2. Replays all ledger events from the event store
    /// 3. Projects each event onto the read model
    ///
    /// This is safe to run at any time and ensures consistency.
    pub async fn rebuild_from_events<S: EventStore>(
        &self,
        event_store: &S,
    ) -> EventStoreResult<()> {
        // Clear existing snapshots
        sqlx::query("TRUNCATE TABLE balance_snapshots")
            .execute(self.pool.as_ref())
            .await
            .map_err(|e| EventStoreError::Database(e.to_string()))?;

        // Load all events in sequence order
        let events = event_store.load_all().await?;

        // Project each event
        for event in events {
            self.project_event(&event).await?;
        }

        Ok(())
    }

    /// Project new events since the last processed sequence
    ///
    /// This function is used to keep the read model up-to-date incrementally.
    /// It loads events from the specified sequence number onward and projects them.
    ///
    /// This is more efficient than rebuilding from scratch when you only need
    /// to catch up on recent events.
    pub async fn project_from_sequence<S: EventStore>(
        &self,
        event_store: &S,
        from_sequence: u64,
    ) -> EventStoreResult<()> {
        // Load events from the specified sequence
        let events = event_store.load_from(from_sequence).await?;

        // Project each event
        for event in events {
            self.project_event(&event).await?;
        }

        Ok(())
    }

    /// Sync the read model to catch up with any new events
    ///
    /// This checks the last processed sequence and projects any new events.
    /// Useful for periodically updating the read model.
    pub async fn sync_from_event_store<S: EventStore>(
        &self,
        event_store: &S,
    ) -> EventStoreResult<()> {
        // Get the last processed sequence, or start from 0 if no balances exist
        let from_sequence = self.last_processed_sequence().await?.unwrap_or(0);

        // Get the max sequence from the event store
        let max_sequence = event_store.max_sequence().await?;

        // If there are new events, project them
        if let Some(max_seq) = max_sequence {
            if max_seq > from_sequence {
                // Project events starting from the next sequence after the last processed
                self.project_from_sequence(event_store, from_sequence + 1)
                    .await?;
            }
        }

        Ok(())
    }

    /// Get a balance snapshot for a specific user and asset
    ///
    /// Returns None if the user has no balance for that asset.
    pub async fn get_balance(
        &self,
        user_id: UserId,
        asset: &str,
    ) -> EventStoreResult<Option<BalanceSnapshot>> {
        let row = sqlx::query(
            r#"
            SELECT user_id, asset, available_amount, available_scale,
                   locked_amount, locked_scale, last_processed_event_id, last_processed_sequence
            FROM balance_snapshots
            WHERE user_id = $1 AND asset = $2
            "#,
        )
        .bind(user_id.0 as i64)
        .bind(asset)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(|e| EventStoreError::Database(e.to_string()))?;

        if let Some(row) = row {
            Ok(Some(BalanceSnapshot {
                user_id: UserId(row.get::<i64, _>("user_id") as u64),
                asset: row.get("asset"),
                available: Money::new(
                    row.get::<i64, _>("available_amount"),
                    row.get::<i32, _>("available_scale") as u32,
                ),
                locked: Money::new(
                    row.get::<i64, _>("locked_amount"),
                    row.get::<i32, _>("locked_scale") as u32,
                ),
                last_processed_event_id: row.get::<Uuid, _>("last_processed_event_id").to_string(),
                last_processed_sequence: row.get::<i64, _>("last_processed_sequence") as u64,
            }))
        } else {
            Ok(None)
        }
    }

    /// Get all balances for a specific user
    pub async fn get_user_balances(
        &self,
        user_id: UserId,
    ) -> EventStoreResult<Vec<BalanceSnapshot>> {
        let rows = sqlx::query(
            r#"
            SELECT user_id, asset, available_amount, available_scale,
                   locked_amount, locked_scale, last_processed_event_id, last_processed_sequence
            FROM balance_snapshots
            WHERE user_id = $1
            ORDER BY asset
            "#,
        )
        .bind(user_id.0 as i64)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| EventStoreError::Database(e.to_string()))?;

        let mut balances = Vec::new();
        for row in rows {
            balances.push(BalanceSnapshot {
                user_id: UserId(row.get::<i64, _>("user_id") as u64),
                asset: row.get("asset"),
                available: Money::new(
                    row.get::<i64, _>("available_amount"),
                    row.get::<i32, _>("available_scale") as u32,
                ),
                locked: Money::new(
                    row.get::<i64, _>("locked_amount"),
                    row.get::<i32, _>("locked_scale") as u32,
                ),
                last_processed_event_id: row.get::<Uuid, _>("last_processed_event_id").to_string(),
                last_processed_sequence: row.get::<i64, _>("last_processed_sequence") as u64,
            });
        }

        Ok(balances)
    }

    /// Get the last processed sequence number across all balances
    ///
    /// Useful for tracking projection progress and detecting gaps.
    pub async fn last_processed_sequence(&self) -> EventStoreResult<Option<u64>> {
        let result = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(last_processed_sequence) FROM balance_snapshots",
        )
        .fetch_one(self.pool.as_ref())
        .await
        .map_err(|e| EventStoreError::Database(e.to_string()))?;

        Ok(result.map(|s| s as u64))
    }
}
