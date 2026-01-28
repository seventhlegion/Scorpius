//! Order Read Model
//!
//! A read-only projection of orders derived exclusively from immutable matching engine events.
//!
//! Key principles:
//! - **Read-only**: Never used for matching or validation
//! - **Derived state**: Computed entirely from matching events
//! - **Rebuildable**: Can be reconstructed from scratch by replaying all events
//! - **Idempotent**: Safe to reproject events multiple times
//!
//! The read model tracks order lifecycle states:
//! - **New**: Order placed and on the orderbook
//! - **PartiallyFilled**: Order partially filled, still on orderbook
//! - **Filled**: Order completely filled and removed
//! - **Cancelled**: Order canceled and removed
//!
//! Order changes are derived from these matching events:
//! - `OrderPlaced`: Creates new order snapshot
//! - `OrderPartiallyFilled`: Updates filled quantity and status
//! - `OrderFilled`: Marks order as filled
//! - `OrderCanceled`: Marks order as canceled
//! - `OrderRejected`: Tracks rejected orders (optional, for audit)

use scorpius_core::{
    matching_event::MatchingEvent, MarketId, Money, OrderId, OrderSnapshot, OrderStatus, OrderType,
    Side, UserId,
};
use sqlx::{Pool, Postgres, Row};
use std::sync::Arc;
use uuid::Uuid;

/// Error types for order read model operations
#[derive(Debug, thiserror::Error)]
pub enum OrderReadModelError {
    #[error("Database error: {0}")]
    Database(String),

    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Result type for order read model operations
pub type OrderReadModelResult<T> = Result<T, OrderReadModelError>;

/// Order read model projection service
///
/// Projects matching events into order snapshots stored in the database.
/// The projection is idempotent and can be safely rebuilt from scratch.
pub struct OrderReadModel {
    pool: Arc<Pool<Postgres>>,
}

impl OrderReadModel {
    /// Create a new order read model projection service
    pub fn new(pool: Arc<Pool<Postgres>>) -> Self {
        Self { pool }
    }

    /// Project a single matching event onto the order read model
    ///
    /// This function is idempotent: applying the same event multiple times
    /// will result in the same final state. The projection checks the
    /// last_processed_sequence to ensure idempotency.
    pub async fn project_event(&self, event: &MatchingEvent) -> OrderReadModelResult<()> {
        let event_id = event.event_id();
        let sequence = event.sequence();
        let timestamp = event.timestamp();

        match event {
            MatchingEvent::OrderPlaced {
                order_id,
                market_id,
                user_id,
                side,
                order_type,
                price,
                quantity,
                filled_quantity,
                status,
                ..
            } => {
                self.upsert_order(
                    *order_id,
                    *market_id,
                    *user_id,
                    *side,
                    *order_type,
                    *price,
                    *quantity,
                    *filled_quantity,
                    *status,
                    None,
                    timestamp,
                    event_id,
                    sequence,
                )
                .await?;
            }

            MatchingEvent::OrderPartiallyFilled {
                order_id,
                filled_quantity,
                remaining_quantity,
                ..
            } => {
                // Calculate total quantity from filled + remaining
                let total_quantity = self.add_money(*filled_quantity, *remaining_quantity);

                self.update_order_fill(
                    *order_id,
                    *filled_quantity,
                    total_quantity,
                    OrderStatus::PartiallyFilled,
                    timestamp,
                    event_id,
                    sequence,
                )
                .await?;
            }

            MatchingEvent::OrderFilled {
                order_id,
                filled_quantity,
                ..
            } => {
                // Get current order to preserve quantity
                let current = self.get_order(*order_id).await?;
                let total_quantity = current.map(|o| o.quantity).unwrap_or(*filled_quantity);

                self.update_order_fill(
                    *order_id,
                    *filled_quantity,
                    total_quantity,
                    OrderStatus::Filled,
                    timestamp,
                    event_id,
                    sequence,
                )
                .await?;
            }

            MatchingEvent::OrderCanceled {
                order_id,
                canceled_quantity,
                filled_quantity,
                ..
            } => {
                // Calculate total quantity from filled + canceled
                let total_quantity = self.add_money(*filled_quantity, *canceled_quantity);

                self.update_order_fill(
                    *order_id,
                    *filled_quantity,
                    total_quantity,
                    OrderStatus::Cancelled,
                    timestamp,
                    event_id,
                    sequence,
                )
                .await?;
            }

            MatchingEvent::OrderRejected {
                order_id,
                market_id,
                user_id,
                reason,
                ..
            } => {
                // Store rejected orders for audit trail
                // We don't have all order details, so we'll create a minimal snapshot
                self.upsert_order(
                    *order_id,
                    *market_id,
                    *user_id,
                    Side::Buy,        // Default, will be updated if OrderPlaced comes later
                    OrderType::Limit, // Default
                    None,
                    Money::new(0, 0), // Unknown quantity
                    Money::new(0, 0),
                    OrderStatus::Cancelled, // Use Cancelled to indicate rejected
                    Some(reason.clone()),
                    timestamp,
                    event_id,
                    sequence,
                )
                .await?;
            }

            MatchingEvent::TradeExecuted { .. } => {
                // TradeExecuted events don't directly update order state
                // Order state is updated via OrderPartiallyFilled/OrderFilled events
                // We could track trades separately if needed, but for order lifecycle
                // we rely on the explicit fill events
            }
        }

        Ok(())
    }

    /// Internal helper to add two Money values with different scales
    fn add_money(&self, a: Money, b: Money) -> Money {
        let max_scale = a.scale.max(b.scale);
        let a_normalized = self.normalize_money(a, max_scale);
        let b_normalized = self.normalize_money(b, max_scale);
        Money::new(a_normalized.amount + b_normalized.amount, max_scale)
    }

    /// Normalize money to a specific scale
    fn normalize_money(&self, money: Money, target_scale: u32) -> Money {
        if money.scale == target_scale {
            money
        } else if money.scale < target_scale {
            let scale_diff = target_scale - money.scale;
            Money::new(money.amount * 10i64.pow(scale_diff), target_scale)
        } else {
            let scale_diff = money.scale - target_scale;
            Money::new(money.amount / 10i64.pow(scale_diff), target_scale)
        }
    }

    /// Upsert an order snapshot
    async fn upsert_order(
        &self,
        order_id: OrderId,
        market_id: MarketId,
        user_id: UserId,
        side: Side,
        order_type: OrderType,
        price: Option<Money>,
        quantity: Money,
        filled_quantity: Money,
        status: OrderStatus,
        rejection_reason: Option<String>,
        timestamp_ns: i64,
        event_id: &str,
        sequence: u64,
    ) -> OrderReadModelResult<()> {
        let event_uuid = Uuid::parse_str(event_id)
            .map_err(|e| OrderReadModelError::Database(format!("Invalid event_id UUID: {}", e)))?;

        let side_str = match side {
            Side::Buy => "Buy",
            Side::Sell => "Sell",
        };

        let order_type_str = match order_type {
            OrderType::Limit => "Limit",
            OrderType::Market => "Market",
        };

        let status_str = match status {
            OrderStatus::New => "New",
            OrderStatus::PartiallyFilled => "PartiallyFilled",
            OrderStatus::Filled => "Filled",
            OrderStatus::Cancelled => "Cancelled",
        };

        sqlx::query(
            r#"
            INSERT INTO order_snapshots (
                order_id, market_id, user_id, side, order_type,
                price_amount, price_scale,
                quantity_amount, quantity_scale,
                filled_quantity_amount, filled_quantity_scale,
                status, rejection_reason,
                created_at_ns, updated_at_ns,
                last_processed_event_id, last_processed_sequence
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
            ON CONFLICT (order_id) DO UPDATE
            SET
                market_id = CASE
                    WHEN order_snapshots.last_processed_sequence < $17 THEN $2
                    ELSE order_snapshots.market_id
                END,
                user_id = CASE
                    WHEN order_snapshots.last_processed_sequence < $17 THEN $3
                    ELSE order_snapshots.user_id
                END,
                side = CASE
                    WHEN order_snapshots.last_processed_sequence < $17 THEN $4
                    ELSE order_snapshots.side
                END,
                order_type = CASE
                    WHEN order_snapshots.last_processed_sequence < $17 THEN $5
                    ELSE order_snapshots.order_type
                END,
                price_amount = CASE
                    WHEN order_snapshots.last_processed_sequence < $17 THEN $6
                    ELSE order_snapshots.price_amount
                END,
                price_scale = CASE
                    WHEN order_snapshots.last_processed_sequence < $17 THEN $7
                    ELSE order_snapshots.price_scale
                END,
                quantity_amount = CASE
                    WHEN order_snapshots.last_processed_sequence < $17 THEN $8
                    ELSE order_snapshots.quantity_amount
                END,
                quantity_scale = CASE
                    WHEN order_snapshots.last_processed_sequence < $17 THEN $9
                    ELSE order_snapshots.quantity_scale
                END,
                filled_quantity_amount = CASE
                    WHEN order_snapshots.last_processed_sequence < $17 THEN $10
                    ELSE order_snapshots.filled_quantity_amount
                END,
                filled_quantity_scale = CASE
                    WHEN order_snapshots.last_processed_sequence < $17 THEN $11
                    ELSE order_snapshots.filled_quantity_scale
                END,
                status = CASE
                    WHEN order_snapshots.last_processed_sequence < $17 THEN $12
                    ELSE order_snapshots.status
                END,
                rejection_reason = CASE
                    WHEN order_snapshots.last_processed_sequence < $17 THEN $13
                    ELSE order_snapshots.rejection_reason
                END,
                updated_at_ns = CASE
                    WHEN order_snapshots.last_processed_sequence < $17 THEN $15
                    ELSE order_snapshots.updated_at_ns
                END,
                last_processed_event_id = CASE
                    WHEN order_snapshots.last_processed_sequence < $17 THEN $16
                    ELSE order_snapshots.last_processed_event_id
                END,
                last_processed_sequence = GREATEST(order_snapshots.last_processed_sequence, $17),
                last_updated_at = NOW()
            "#,
        )
        .bind(order_id.0 as i64)
        .bind(market_id.0 as i64)
        .bind(user_id.0 as i64)
        .bind(side_str)
        .bind(order_type_str)
        .bind(price.map(|p| p.amount))
        .bind(price.map(|p| p.scale as i32))
        .bind(quantity.amount)
        .bind(quantity.scale as i32)
        .bind(filled_quantity.amount)
        .bind(filled_quantity.scale as i32)
        .bind(status_str)
        .bind(rejection_reason.as_deref())
        .bind(timestamp_ns)
        .bind(timestamp_ns) // updated_at_ns same as created_at_ns for new orders
        .bind(&event_uuid)
        .bind(sequence as i64)
        .execute(self.pool.as_ref())
        .await
        .map_err(|e| OrderReadModelError::Database(e.to_string()))?;

        Ok(())
    }

    /// Update order fill information
    async fn update_order_fill(
        &self,
        order_id: OrderId,
        filled_quantity: Money,
        total_quantity: Money,
        status: OrderStatus,
        timestamp_ns: i64,
        event_id: &str,
        sequence: u64,
    ) -> OrderReadModelResult<()> {
        let event_uuid = Uuid::parse_str(event_id)
            .map_err(|e| OrderReadModelError::Database(format!("Invalid event_id UUID: {}", e)))?;

        let status_str = match status {
            OrderStatus::New => "New",
            OrderStatus::PartiallyFilled => "PartiallyFilled",
            OrderStatus::Filled => "Filled",
            OrderStatus::Cancelled => "Cancelled",
        };

        // Use a transaction for atomic read-modify-write
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| OrderReadModelError::Database(e.to_string()))?;

        // Check if event already processed
        let existing = sqlx::query(
            "SELECT last_processed_sequence FROM order_snapshots WHERE order_id = $1 FOR UPDATE",
        )
        .bind(order_id.0 as i64)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| OrderReadModelError::Database(e.to_string()))?;

        if let Some(row) = existing {
            let existing_seq = row.get::<i64, _>("last_processed_sequence") as u64;
            if existing_seq >= sequence {
                // Event already processed, skip update
                tx.commit()
                    .await
                    .map_err(|e| OrderReadModelError::Database(e.to_string()))?;
                return Ok(());
            }
        }

        // Update the order
        sqlx::query(
            r#"
            UPDATE order_snapshots
            SET
                filled_quantity_amount = $1,
                filled_quantity_scale = $2,
                quantity_amount = $3,
                quantity_scale = $4,
                status = $5,
                updated_at_ns = $6,
                last_processed_event_id = $7,
                last_processed_sequence = $8,
                last_updated_at = NOW()
            WHERE order_id = $9
            "#,
        )
        .bind(filled_quantity.amount)
        .bind(filled_quantity.scale as i32)
        .bind(total_quantity.amount)
        .bind(total_quantity.scale as i32)
        .bind(status_str)
        .bind(timestamp_ns)
        .bind(&event_uuid)
        .bind(sequence as i64)
        .bind(order_id.0 as i64)
        .execute(&mut *tx)
        .await
        .map_err(|e| OrderReadModelError::Database(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| OrderReadModelError::Database(e.to_string()))?;

        Ok(())
    }

    /// Get an order snapshot by order ID
    pub async fn get_order(
        &self,
        order_id: OrderId,
    ) -> OrderReadModelResult<Option<OrderSnapshot>> {
        let row = sqlx::query(
            r#"
            SELECT order_id, market_id, user_id, side, order_type,
                   price_amount, price_scale,
                   quantity_amount, quantity_scale,
                   filled_quantity_amount, filled_quantity_scale,
                   status, rejection_reason,
                   created_at_ns, updated_at_ns,
                   last_processed_event_id, last_processed_sequence
            FROM order_snapshots
            WHERE order_id = $1
            "#,
        )
        .bind(order_id.0 as i64)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(|e| OrderReadModelError::Database(e.to_string()))?;

        if let Some(row) = row {
            Ok(Some(self.row_to_snapshot(row)?))
        } else {
            Ok(None)
        }
    }

    /// Get all orders for a specific user
    pub async fn get_user_orders(
        &self,
        user_id: UserId,
    ) -> OrderReadModelResult<Vec<OrderSnapshot>> {
        let rows = sqlx::query(
            r#"
            SELECT order_id, market_id, user_id, side, order_type,
                   price_amount, price_scale,
                   quantity_amount, quantity_scale,
                   filled_quantity_amount, filled_quantity_scale,
                   status, rejection_reason,
                   created_at_ns, updated_at_ns,
                   last_processed_event_id, last_processed_sequence
            FROM order_snapshots
            WHERE user_id = $1
            ORDER BY created_at_ns DESC
            "#,
        )
        .bind(user_id.0 as i64)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| OrderReadModelError::Database(e.to_string()))?;

        let mut orders = Vec::new();
        for row in rows {
            orders.push(self.row_to_snapshot(row)?);
        }

        Ok(orders)
    }

    /// Get orders for a specific user and market
    pub async fn get_user_market_orders(
        &self,
        user_id: UserId,
        market_id: MarketId,
    ) -> OrderReadModelResult<Vec<OrderSnapshot>> {
        let rows = sqlx::query(
            r#"
            SELECT order_id, market_id, user_id, side, order_type,
                   price_amount, price_scale,
                   quantity_amount, quantity_scale,
                   filled_quantity_amount, filled_quantity_scale,
                   status, rejection_reason,
                   created_at_ns, updated_at_ns,
                   last_processed_event_id, last_processed_sequence
            FROM order_snapshots
            WHERE user_id = $1 AND market_id = $2
            ORDER BY created_at_ns DESC
            "#,
        )
        .bind(user_id.0 as i64)
        .bind(market_id.0 as i64)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| OrderReadModelError::Database(e.to_string()))?;

        let mut orders = Vec::new();
        for row in rows {
            orders.push(self.row_to_snapshot(row)?);
        }

        Ok(orders)
    }

    /// Get orders by status
    pub async fn get_orders_by_status(
        &self,
        status: OrderStatus,
    ) -> OrderReadModelResult<Vec<OrderSnapshot>> {
        let status_str = match status {
            OrderStatus::New => "New",
            OrderStatus::PartiallyFilled => "PartiallyFilled",
            OrderStatus::Filled => "Filled",
            OrderStatus::Cancelled => "Cancelled",
        };

        let rows = sqlx::query(
            r#"
            SELECT order_id, market_id, user_id, side, order_type,
                   price_amount, price_scale,
                   quantity_amount, quantity_scale,
                   filled_quantity_amount, filled_quantity_scale,
                   status, rejection_reason,
                   created_at_ns, updated_at_ns,
                   last_processed_event_id, last_processed_sequence
            FROM order_snapshots
            WHERE status = $1
            ORDER BY created_at_ns DESC
            "#,
        )
        .bind(status_str)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| OrderReadModelError::Database(e.to_string()))?;

        let mut orders = Vec::new();
        for row in rows {
            orders.push(self.row_to_snapshot(row)?);
        }

        Ok(orders)
    }

    /// Get the last processed sequence number across all orders
    pub async fn last_processed_sequence(&self) -> OrderReadModelResult<Option<u64>> {
        let result = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(last_processed_sequence) FROM order_snapshots",
        )
        .fetch_one(self.pool.as_ref())
        .await
        .map_err(|e| OrderReadModelError::Database(e.to_string()))?;

        Ok(result.map(|s| s as u64))
    }

    /// Rebuild the entire order read model from scratch
    ///
    /// This function:
    /// 1. Clears all existing order snapshots
    /// 2. Loads all matching events from the event store
    /// 3. Projects each event onto the read model
    ///
    /// This is safe to run at any time and ensures consistency.
    /// Useful for:
    /// - Initial setup
    /// - Recovery after data corruption
    /// - Schema migrations
    pub async fn rebuild_from_events(&self, stream_id: &str) -> OrderReadModelResult<u64> {
        // Clear existing snapshots
        sqlx::query("TRUNCATE TABLE order_snapshots")
            .execute(self.pool.as_ref())
            .await
            .map_err(|e| OrderReadModelError::Database(e.to_string()))?;

        // Load all matching events for the stream
        let events = self.load_matching_events(stream_id, 0).await?;

        // Project each event
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
    ) -> OrderReadModelResult<Vec<MatchingEvent>> {
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
        .map_err(|e| OrderReadModelError::Database(e.to_string()))?;

        let mut events = Vec::new();
        for row in rows {
            let payload: sqlx::types::Json<serde_json::Value> = row.get("payload");
            let event: MatchingEvent = serde_json::from_value(payload.0)
                .map_err(|e| OrderReadModelError::Serialization(e.to_string()))?;
            events.push(event);
        }

        Ok(events)
    }

    /// Get the maximum sequence number for a stream
    async fn get_max_sequence(&self, stream_id: &str) -> OrderReadModelResult<Option<u64>> {
        let result = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(sequence) FROM events WHERE stream_id = $1 AND category = 'matching'",
        )
        .bind(stream_id)
        .fetch_one(self.pool.as_ref())
        .await
        .map_err(|e| OrderReadModelError::Database(e.to_string()))?;

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
    ) -> OrderReadModelResult<()> {
        let events = self.load_matching_events(stream_id, from_sequence).await?;

        for event in events {
            self.project_event(&event).await?;
        }

        Ok(())
    }

    /// Convert a database row to OrderSnapshot
    fn row_to_snapshot(&self, row: sqlx::postgres::PgRow) -> OrderReadModelResult<OrderSnapshot> {
        use scorpius_core::{MarketId, OrderId, OrderStatus, OrderType, Side, UserId};

        let side_str: String = row.get("side");
        let side = match side_str.as_str() {
            "Buy" => Side::Buy,
            "Sell" => Side::Sell,
            _ => {
                return Err(OrderReadModelError::Database(format!(
                    "Invalid side: {}",
                    side_str
                )))
            }
        };

        let order_type_str: String = row.get("order_type");
        let order_type = match order_type_str.as_str() {
            "Limit" => OrderType::Limit,
            "Market" => OrderType::Market,
            _ => {
                return Err(OrderReadModelError::Database(format!(
                    "Invalid order_type: {}",
                    order_type_str
                )))
            }
        };

        let status_str: String = row.get("status");
        let status = match status_str.as_str() {
            "New" => OrderStatus::New,
            "PartiallyFilled" => OrderStatus::PartiallyFilled,
            "Filled" => OrderStatus::Filled,
            "Cancelled" => OrderStatus::Cancelled,
            _ => {
                return Err(OrderReadModelError::Database(format!(
                    "Invalid status: {}",
                    status_str
                )))
            }
        };

        let price = match (
            row.get::<Option<i64>, _>("price_amount"),
            row.get::<Option<i32>, _>("price_scale"),
        ) {
            (Some(amount), Some(scale)) => Some(Money::new(amount, scale as u32)),
            _ => None,
        };

        Ok(OrderSnapshot {
            order_id: OrderId(row.get::<i64, _>("order_id") as u64),
            market_id: MarketId(row.get::<i64, _>("market_id") as u64),
            user_id: UserId(row.get::<i64, _>("user_id") as u64),
            side,
            order_type,
            price,
            quantity: Money::new(
                row.get::<i64, _>("quantity_amount"),
                row.get::<i32, _>("quantity_scale") as u32,
            ),
            filled_quantity: Money::new(
                row.get::<i64, _>("filled_quantity_amount"),
                row.get::<i32, _>("filled_quantity_scale") as u32,
            ),
            status,
            rejection_reason: row.get("rejection_reason"),
            created_at_ns: row.get("created_at_ns"),
            updated_at_ns: row.get("updated_at_ns"),
            last_processed_event_id: row.get::<Uuid, _>("last_processed_event_id").to_string(),
            last_processed_sequence: row.get::<i64, _>("last_processed_sequence") as u64,
        })
    }
}
