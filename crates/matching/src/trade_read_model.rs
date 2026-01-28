//! Trade Read Model
//!
//! A read-only append-only projection of trades derived exclusively from immutable matching engine events.
//!
//! Key principles:
//! - **Append-only**: Trades are immutable once created
//! - **Read-only**: Never used for matching or validation
//! - **Derived state**: Computed entirely from matching events
//! - **Rebuildable**: Can be reconstructed from scratch by replaying all events
//! - **Idempotent**: Safe to reproject events multiple times
//! - **Auditable**: Complete history of all trades
//!
//! Trades are derived from TradeExecuted events:
//! - Each TradeExecuted event creates a new trade record
//! - Trades are never updated or deleted
//! - Provides complete audit trail of all trade executions

use scorpius_core::{
    matching_event::MatchingEvent, MarketId, OrderId, TradeId, TradeSnapshot, UserId,
};
use sqlx::{Pool, Postgres, Row};
use std::sync::Arc;
use uuid::Uuid;

/// Error types for trade read model operations
#[derive(Debug, thiserror::Error)]
pub enum TradeReadModelError {
    #[error("Database error: {0}")]
    Database(String),

    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Result type for trade read model operations
pub type TradeReadModelResult<T> = Result<T, TradeReadModelError>;

/// Trade read model projection service
///
/// Projects matching events into trade records stored in the database.
/// The projection is idempotent and can be safely rebuilt from scratch.
/// Trades are append-only: once created, they are never modified.
pub struct TradeReadModel {
    pool: Arc<Pool<Postgres>>,
}

impl TradeReadModel {
    /// Create a new trade read model projection service
    pub fn new(pool: Arc<Pool<Postgres>>) -> Self {
        Self { pool }
    }

    /// Project a single matching event onto the trade read model
    ///
    /// This function is idempotent: applying the same event multiple times
    /// will result in the same final state. The projection checks the
    /// event_id to ensure idempotency.
    ///
    /// Only TradeExecuted events create trade records. Other events are ignored.
    pub async fn project_event(&self, event: &MatchingEvent) -> TradeReadModelResult<()> {
        match event {
            MatchingEvent::TradeExecuted {
                trade_id,
                market_id,
                maker_order_id,
                taker_order_id,
                buyer_id,
                seller_id,
                price,
                quantity,
                quote_amount,
                ..
            } => {
                self.insert_trade(
                    *trade_id,
                    *market_id,
                    *maker_order_id,
                    *taker_order_id,
                    *buyer_id,
                    *seller_id,
                    *price,
                    *quantity,
                    *quote_amount,
                    event.timestamp(),
                    event.event_id(),
                    event.sequence(),
                )
                .await?;
            }
            _ => {
                // Other events don't create trades
            }
        }

        Ok(())
    }

    /// Insert a trade record (idempotent)
    async fn insert_trade(
        &self,
        trade_id: TradeId,
        market_id: MarketId,
        maker_order_id: OrderId,
        taker_order_id: OrderId,
        buyer_id: UserId,
        seller_id: UserId,
        price: scorpius_core::Money,
        quantity: scorpius_core::Money,
        quote_amount: scorpius_core::Money,
        executed_at_ns: i64,
        event_id: &str,
        event_sequence: u64,
    ) -> TradeReadModelResult<()> {
        let event_uuid = Uuid::parse_str(event_id)
            .map_err(|e| TradeReadModelError::Database(format!("Invalid event_id UUID: {}", e)))?;

        // Use INSERT ... ON CONFLICT DO NOTHING for idempotency
        // If trade_id already exists, skip insertion
        sqlx::query(
            r#"
            INSERT INTO trades (
                trade_id, market_id, maker_order_id, taker_order_id,
                buyer_id, seller_id,
                price_amount, price_scale,
                quantity_amount, quantity_scale,
                quote_amount_amount, quote_amount_scale,
                executed_at_ns, event_id, event_sequence
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            ON CONFLICT (trade_id) DO NOTHING
            "#,
        )
        .bind(trade_id.0 as i64)
        .bind(market_id.0 as i64)
        .bind(maker_order_id.0 as i64)
        .bind(taker_order_id.0 as i64)
        .bind(buyer_id.0 as i64)
        .bind(seller_id.0 as i64)
        .bind(price.amount)
        .bind(price.scale as i32)
        .bind(quantity.amount)
        .bind(quantity.scale as i32)
        .bind(quote_amount.amount)
        .bind(quote_amount.scale as i32)
        .bind(executed_at_ns)
        .bind(&event_uuid)
        .bind(event_sequence as i64)
        .execute(self.pool.as_ref())
        .await
        .map_err(|e| TradeReadModelError::Database(e.to_string()))?;

        Ok(())
    }

    /// Get a trade by trade ID
    pub async fn get_trade(
        &self,
        trade_id: TradeId,
    ) -> TradeReadModelResult<Option<TradeSnapshot>> {
        let row = sqlx::query(
            r#"
            SELECT trade_id, market_id, maker_order_id, taker_order_id,
                   buyer_id, seller_id,
                   price_amount, price_scale,
                   quantity_amount, quantity_scale,
                   quote_amount_amount, quote_amount_scale,
                   executed_at_ns, event_id, event_sequence
            FROM trades
            WHERE trade_id = $1
            "#,
        )
        .bind(trade_id.0 as i64)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(|e| TradeReadModelError::Database(e.to_string()))?;

        if let Some(row) = row {
            Ok(Some(self.row_to_snapshot(row)?))
        } else {
            Ok(None)
        }
    }

    /// Get all trades for a specific user (as buyer or seller)
    pub async fn get_user_trades(
        &self,
        user_id: UserId,
    ) -> TradeReadModelResult<Vec<TradeSnapshot>> {
        let rows = sqlx::query(
            r#"
            SELECT trade_id, market_id, maker_order_id, taker_order_id,
                   buyer_id, seller_id,
                   price_amount, price_scale,
                   quantity_amount, quantity_scale,
                   quote_amount_amount, quote_amount_scale,
                   executed_at_ns, event_id, event_sequence
            FROM trades
            WHERE buyer_id = $1 OR seller_id = $1
            ORDER BY executed_at_ns DESC
            "#,
        )
        .bind(user_id.0 as i64)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| TradeReadModelError::Database(e.to_string()))?;

        let mut trades = Vec::new();
        for row in rows {
            trades.push(self.row_to_snapshot(row)?);
        }

        Ok(trades)
    }

    /// Get trades for a specific user and market
    pub async fn get_user_market_trades(
        &self,
        user_id: UserId,
        market_id: MarketId,
    ) -> TradeReadModelResult<Vec<TradeSnapshot>> {
        let rows = sqlx::query(
            r#"
            SELECT trade_id, market_id, maker_order_id, taker_order_id,
                   buyer_id, seller_id,
                   price_amount, price_scale,
                   quantity_amount, quantity_scale,
                   quote_amount_amount, quote_amount_scale,
                   executed_at_ns, event_id, event_sequence
            FROM trades
            WHERE (buyer_id = $1 OR seller_id = $1) AND market_id = $2
            ORDER BY executed_at_ns DESC
            "#,
        )
        .bind(user_id.0 as i64)
        .bind(market_id.0 as i64)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| TradeReadModelError::Database(e.to_string()))?;

        let mut trades = Vec::new();
        for row in rows {
            trades.push(self.row_to_snapshot(row)?);
        }

        Ok(trades)
    }

    /// Get trades for a specific market
    pub async fn get_market_trades(
        &self,
        market_id: MarketId,
        limit: Option<u64>,
    ) -> TradeReadModelResult<Vec<TradeSnapshot>> {
        let query = if let Some(limit) = limit {
            sqlx::query(
                r#"
                SELECT trade_id, market_id, maker_order_id, taker_order_id,
                       buyer_id, seller_id,
                       price_amount, price_scale,
                       quantity_amount, quantity_scale,
                       quote_amount_amount, quote_amount_scale,
                       executed_at_ns, event_id, event_sequence
                FROM trades
                WHERE market_id = $1
                ORDER BY executed_at_ns DESC
                LIMIT $2
                "#,
            )
            .bind(market_id.0 as i64)
            .bind(limit as i64)
        } else {
            sqlx::query(
                r#"
                SELECT trade_id, market_id, maker_order_id, taker_order_id,
                       buyer_id, seller_id,
                       price_amount, price_scale,
                       quantity_amount, quantity_scale,
                       quote_amount_amount, quote_amount_scale,
                       executed_at_ns, event_id, event_sequence
                FROM trades
                WHERE market_id = $1
                ORDER BY executed_at_ns DESC
                "#,
            )
            .bind(market_id.0 as i64)
        };

        let rows = query
            .fetch_all(self.pool.as_ref())
            .await
            .map_err(|e| TradeReadModelError::Database(e.to_string()))?;

        let mut trades = Vec::new();
        for row in rows {
            trades.push(self.row_to_snapshot(row)?);
        }

        Ok(trades)
    }

    /// Get trades involving a specific order (as maker or taker)
    pub async fn get_order_trades(
        &self,
        order_id: OrderId,
    ) -> TradeReadModelResult<Vec<TradeSnapshot>> {
        let rows = sqlx::query(
            r#"
            SELECT trade_id, market_id, maker_order_id, taker_order_id,
                   buyer_id, seller_id,
                   price_amount, price_scale,
                   quantity_amount, quantity_scale,
                   quote_amount_amount, quote_amount_scale,
                   executed_at_ns, event_id, event_sequence
            FROM trades
            WHERE maker_order_id = $1 OR taker_order_id = $1
            ORDER BY executed_at_ns ASC
            "#,
        )
        .bind(order_id.0 as i64)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| TradeReadModelError::Database(e.to_string()))?;

        let mut trades = Vec::new();
        for row in rows {
            trades.push(self.row_to_snapshot(row)?);
        }

        Ok(trades)
    }

    /// Get the last processed sequence number across all trades
    pub async fn last_processed_sequence(&self) -> TradeReadModelResult<Option<u64>> {
        let result = sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(event_sequence) FROM trades")
            .fetch_one(self.pool.as_ref())
            .await
            .map_err(|e| TradeReadModelError::Database(e.to_string()))?;

        Ok(result.map(|s| s as u64))
    }

    /// Rebuild the entire trade read model from scratch
    ///
    /// This function:
    /// 1. Clears all existing trades
    /// 2. Loads all matching events from the event store
    /// 3. Projects each TradeExecuted event onto the read model
    ///
    /// This is safe to run at any time and ensures consistency.
    /// Useful for:
    /// - Initial setup
    /// - Recovery after data corruption
    /// - Schema migrations
    pub async fn rebuild_from_events(&self, stream_id: &str) -> TradeReadModelResult<u64> {
        // Clear existing trades
        sqlx::query("TRUNCATE TABLE trades")
            .execute(self.pool.as_ref())
            .await
            .map_err(|e| TradeReadModelError::Database(e.to_string()))?;

        // Load all matching events for the stream
        let events = self.load_matching_events(stream_id, 0).await?;

        // Project each event (only TradeExecuted events create trades)
        for event in events {
            self.project_event(&event).await?;
        }

        // Get the count of processed events
        let max_sequence = self.get_max_sequence(stream_id).await?;

        Ok(max_sequence.unwrap_or(0))
    }

    /// Load matching events from the database
    async fn load_matching_events(
        &self,
        stream_id: &str,
        from_sequence: u64,
    ) -> TradeReadModelResult<Vec<MatchingEvent>> {
        let rows = sqlx::query(
            r#"
            SELECT sequence, payload
            FROM events
            WHERE stream_id = $1 AND category = 'matching' AND sequence >= $2
            ORDER BY sequence
            "#,
        )
        .bind(stream_id)
        .bind(from_sequence as i64)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| TradeReadModelError::Database(e.to_string()))?;

        let mut events = Vec::new();
        for row in rows {
            let payload: sqlx::types::Json<serde_json::Value> = row.get("payload");
            let event: MatchingEvent = serde_json::from_value(payload.0)
                .map_err(|e| TradeReadModelError::Serialization(e.to_string()))?;
            events.push(event);
        }

        Ok(events)
    }

    /// Get the maximum sequence number for a stream
    async fn get_max_sequence(&self, stream_id: &str) -> TradeReadModelResult<Option<u64>> {
        let result = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(sequence) FROM events WHERE stream_id = $1 AND category = 'matching'",
        )
        .bind(stream_id)
        .fetch_one(self.pool.as_ref())
        .await
        .map_err(|e| TradeReadModelError::Database(e.to_string()))?;

        Ok(result.map(|s| s as u64))
    }

    /// Project new events since the last processed sequence
    ///
    /// This function is used to keep the read model up-to-date incrementally.
    /// It loads events from the specified sequence number onward and projects them.
    pub async fn project_from_sequence(
        &self,
        stream_id: &str,
        from_sequence: u64,
    ) -> TradeReadModelResult<()> {
        let events = self.load_matching_events(stream_id, from_sequence).await?;

        for event in events {
            self.project_event(&event).await?;
        }

        Ok(())
    }

    /// Convert a database row to TradeSnapshot
    fn row_to_snapshot(&self, row: sqlx::postgres::PgRow) -> TradeReadModelResult<TradeSnapshot> {
        use scorpius_core::{MarketId, Money, OrderId, TradeId, UserId};

        Ok(TradeSnapshot {
            trade_id: TradeId(row.get::<i64, _>("trade_id") as u64),
            market_id: MarketId(row.get::<i64, _>("market_id") as u64),
            maker_order_id: OrderId(row.get::<i64, _>("maker_order_id") as u64),
            taker_order_id: OrderId(row.get::<i64, _>("taker_order_id") as u64),
            buyer_id: UserId(row.get::<i64, _>("buyer_id") as u64),
            seller_id: UserId(row.get::<i64, _>("seller_id") as u64),
            price: Money::new(
                row.get::<i64, _>("price_amount"),
                row.get::<i32, _>("price_scale") as u32,
            ),
            quantity: Money::new(
                row.get::<i64, _>("quantity_amount"),
                row.get::<i32, _>("quantity_scale") as u32,
            ),
            quote_amount: Money::new(
                row.get::<i64, _>("quote_amount_amount"),
                row.get::<i32, _>("quote_amount_scale") as u32,
            ),
            executed_at_ns: row.get("executed_at_ns"),
            event_id: row.get::<Uuid, _>("event_id").to_string(),
            event_sequence: row.get::<i64, _>("event_sequence") as u64,
        })
    }
}
