//! Matching Event Projector
//!
//! A service that continuously consumes matching engine events from the event store
//! and projects them onto both order and trade read models. Ensures:
//! - **Idempotency**: Events can be safely replayed
//! - **Transactional updates**: All updates are atomic
//! - **Deterministic replay**: Events are processed in sequence order
//! - **Fault tolerance**: Can resume from last processed sequence after failures
//! - **Correctness under retries**: Handles duplicate events gracefully
//! - **Crash recovery**: Can resume from checkpoint after crashes
//! - **Full rebuilds**: Can rebuild both read models from scratch

use crate::order_read_model::OrderReadModel;
use crate::trade_read_model::TradeReadModel;
use scorpius_core::{matching_event::MatchingEvent, ProjectorConfig};
use sqlx::{Pool, Postgres, Row};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::{interval, MissedTickBehavior};

/// Error types for matching event projector operations
#[derive(Debug, thiserror::Error)]
pub enum MatchingEventProjectorError {
    #[error("Database error: {0}")]
    Database(String),

    #[error("Order read model error: {0}")]
    OrderReadModel(String),

    #[error("Trade read model error: {0}")]
    TradeReadModel(String),

    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Result type for matching event projector operations
pub type MatchingEventProjectorResult<T> = Result<T, MatchingEventProjectorError>;

/// Projector service for consuming matching events and updating order and trade read models
pub struct MatchingEventProjector {
    pool: Arc<Pool<Postgres>>,
    order_read_model: OrderReadModel,
    trade_read_model: TradeReadModel,
    /// Stream ID for matching events (e.g., "matching:BTC/USDT")
    stream_id: String,
    /// Interval between polling for new events (when in continuous mode)
    poll_interval: Duration,
    /// Batch size for loading events per iteration
    batch_size: usize,
}

impl MatchingEventProjector {
    /// Create a new projector with default configuration
    pub fn new(pool: Arc<Pool<Postgres>>, stream_id: String) -> Self {
        Self::with_config(pool, stream_id, ProjectorConfig::default())
    }

    /// Create a new projector with custom configuration
    pub fn with_config(
        pool: Arc<Pool<Postgres>>,
        stream_id: String,
        config: ProjectorConfig,
    ) -> Self {
        let order_read_model = OrderReadModel::new(pool.clone());
        let trade_read_model = TradeReadModel::new(pool.clone());

        Self {
            pool,
            order_read_model,
            trade_read_model,
            stream_id,
            poll_interval: config.poll_interval,
            batch_size: config.batch_size,
        }
    }

    /// Project a single batch of events starting from the given sequence
    ///
    /// Returns the sequence number of the last processed event, or None if no events were processed.
    /// This is idempotent and transactional - if an event was already processed, it's safely skipped.
    pub async fn project_batch(
        &self,
        from_sequence: u64,
    ) -> MatchingEventProjectorResult<Option<u64>> {
        // Load events from the specified sequence
        let events = self.load_matching_events(from_sequence).await?;

        if events.is_empty() {
            return Ok(None);
        }

        // Track the last processed sequence
        let mut last_sequence = from_sequence;

        // Project each event to both read models
        // Each project_event call is idempotent, so we can safely process them one by one
        for event in events {
            let sequence = event.sequence();

            // Project to order read model (idempotent)
            self.order_read_model
                .project_event(&event)
                .await
                .map_err(|e| MatchingEventProjectorError::OrderReadModel(e.to_string()))?;

            // Project to trade read model (idempotent)
            self.trade_read_model
                .project_event(&event)
                .await
                .map_err(|e| MatchingEventProjectorError::TradeReadModel(e.to_string()))?;

            last_sequence = sequence;
        }

        Ok(Some(last_sequence))
    }

    /// Sync both read models with the event store
    ///
    /// This will catch up on all events since the last processed sequence.
    /// Uses the minimum of both read models' last processed sequences to ensure consistency.
    /// Returns the number of events processed.
    pub async fn sync(&self) -> MatchingEventProjectorResult<u64> {
        // Get the last processed sequence from both read models
        // Use the minimum to ensure we don't miss any events
        let order_seq = self
            .order_read_model
            .last_processed_sequence()
            .await
            .map_err(|e| MatchingEventProjectorError::OrderReadModel(e.to_string()))?;
        let trade_seq = self
            .trade_read_model
            .last_processed_sequence()
            .await
            .map_err(|e| MatchingEventProjectorError::TradeReadModel(e.to_string()))?;

        // Start from the minimum sequence to ensure both read models stay in sync
        let from_sequence = match (order_seq, trade_seq) {
            (Some(o), Some(t)) => o.min(t) + 1,
            (Some(o), None) => o + 1,
            (None, Some(t)) => t + 1,
            (None, None) => 0,
        };

        let mut processed_count = 0u64;
        let mut current_sequence = from_sequence;

        loop {
            // Load a batch of events
            let events = self.load_matching_events(current_sequence).await?;

            if events.is_empty() {
                break;
            }

            // Project the batch
            let batch_size = events.len();
            for event in events {
                let sequence = event.sequence();

                // Project to order read model (idempotent)
                self.order_read_model
                    .project_event(&event)
                    .await
                    .map_err(|e| MatchingEventProjectorError::OrderReadModel(e.to_string()))?;

                // Project to trade read model (idempotent)
                self.trade_read_model
                    .project_event(&event)
                    .await
                    .map_err(|e| MatchingEventProjectorError::TradeReadModel(e.to_string()))?;

                current_sequence = sequence + 1;
            }

            processed_count += batch_size as u64;

            // If we processed fewer events than batch size, we've caught up
            if batch_size < self.batch_size {
                break;
            }
        }

        Ok(processed_count)
    }

    /// Rebuild both read models from scratch
    ///
    /// This function:
    /// 1. Clears all existing order and trade snapshots
    /// 2. Replays all matching events from the event store
    /// 3. Projects each event onto both read models
    ///
    /// This is safe to run at any time and ensures consistency.
    /// Useful for:
    /// - Initial setup
    /// - Recovery after data corruption
    /// - Schema migrations
    pub async fn rebuild(&self) -> MatchingEventProjectorResult<u64> {
        // Rebuild order read model
        let order_count = self
            .order_read_model
            .rebuild_from_events(&self.stream_id)
            .await
            .map_err(|e| MatchingEventProjectorError::OrderReadModel(e.to_string()))?;

        // Rebuild trade read model
        let trade_count = self
            .trade_read_model
            .rebuild_from_events(&self.stream_id)
            .await
            .map_err(|e| MatchingEventProjectorError::TradeReadModel(e.to_string()))?;

        // Return the maximum sequence (both should be the same after rebuild)
        Ok(order_count.max(trade_count))
    }

    /// Run the projector in continuous mode
    ///
    /// This will continuously poll the event store for new events and project them.
    /// The projector can be stopped by dropping the receiver of the shutdown signal.
    ///
    /// # Arguments
    /// * `shutdown` - A watch receiver that signals when to stop (when it becomes true)
    ///
    /// # Returns
    /// * `Ok(())` if stopped cleanly
    /// * `Err(MatchingEventProjectorError)` if an error occurred during processing
    pub async fn run_continuous(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> MatchingEventProjectorResult<()> {
        let mut poll_interval = interval(self.poll_interval);
        poll_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        tracing::info!("Matching event projector received shutdown signal, stopping...");
                        break;
                    }
                }
                _ = poll_interval.tick() => {
                    match self.sync().await {
                        Ok(count) => {
                            if count > 0 {
                                tracing::debug!("Projected {} matching events", count);
                            }
                        }
                        Err(e) => {
                            tracing::error!("Error projecting matching events: {}", e);
                            // Continue processing - don't exit on transient errors
                            // In production, you might want to implement exponential backoff
                        }
                    }
                }
            }
        }

        // Final sync before shutdown
        tracing::info!("Performing final sync before shutdown...");
        let _ = self.sync().await;

        Ok(())
    }

    /// Run the projector once (one-time sync)
    ///
    /// This will catch up on all events since the last processed sequence and then return.
    pub async fn run_once(&self) -> MatchingEventProjectorResult<u64> {
        tracing::info!("Starting one-time matching event projector sync...");
        let count = self.sync().await?;
        tracing::info!(
            "Matching event projector sync complete, processed {} events",
            count
        );
        Ok(count)
    }

    /// Get the current lag (number of unprocessed events)
    ///
    /// Returns the difference between the max sequence in the event store
    /// and the minimum last processed sequence in both read models.
    pub async fn get_lag(&self) -> MatchingEventProjectorResult<Option<u64>> {
        let max_sequence = self.get_max_sequence().await?;

        let order_seq = self
            .order_read_model
            .last_processed_sequence()
            .await
            .map_err(|e| MatchingEventProjectorError::OrderReadModel(e.to_string()))?;
        let trade_seq = self
            .trade_read_model
            .last_processed_sequence()
            .await
            .map_err(|e| MatchingEventProjectorError::TradeReadModel(e.to_string()))?;

        let min_processed = match (order_seq, trade_seq) {
            (Some(o), Some(t)) => Some(o.min(t)),
            (Some(o), None) => Some(o),
            (None, Some(t)) => Some(t),
            (None, None) => None,
        };

        match (max_sequence, min_processed) {
            (Some(max), Some(min)) => Ok(Some(max.saturating_sub(min))),
            (Some(max), None) => Ok(Some(max + 1)), // +1 because we start from 0
            _ => Ok(None),
        }
    }

    /// Load matching events from the database
    async fn load_matching_events(
        &self,
        from_sequence: u64,
    ) -> MatchingEventProjectorResult<Vec<MatchingEvent>> {
        let rows = sqlx::query(
            r#"
            SELECT sequence, payload
            FROM events
            WHERE stream_id = $1 AND category = 'matching' AND sequence >= $2
            ORDER BY sequence
            LIMIT $3
            "#,
        )
        .bind(&self.stream_id)
        .bind(from_sequence as i64)
        .bind(self.batch_size as i64)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| MatchingEventProjectorError::Database(e.to_string()))?;

        let mut events = Vec::new();
        for row in rows {
            let payload: sqlx::types::Json<serde_json::Value> = row.get("payload");
            let event: MatchingEvent = serde_json::from_value(payload.0)
                .map_err(|e| MatchingEventProjectorError::Serialization(e.to_string()))?;
            events.push(event);
        }

        Ok(events)
    }

    /// Get the maximum sequence number for the stream
    async fn get_max_sequence(&self) -> MatchingEventProjectorResult<Option<u64>> {
        let result = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(sequence) FROM events WHERE stream_id = $1 AND category = 'matching'",
        )
        .bind(&self.stream_id)
        .fetch_one(self.pool.as_ref())
        .await
        .map_err(|e| MatchingEventProjectorError::Database(e.to_string()))?;

        Ok(result.map(|s| s as u64))
    }
}

#[cfg(test)]
mod tests {
    // Note: These tests would require a test database setup
    // They are provided as examples of how the projector should be used

    #[tokio::test]
    #[ignore] // Requires database setup
    async fn test_projector_sync() {
        // use crate::matching_event_projector::MatchingEventProjector;
        // use std::sync::Arc;
        // use sqlx::postgres::PgPoolOptions;
        //
        // let pool = setup_test_db().await;
        // let projector = MatchingEventProjector::new(
        //     Arc::new(pool),
        //     "matching:BTC/USDT".to_string()
        // );
        //
        // // Sync once
        // let count = projector.run_once().await.unwrap();
        // assert!(count >= 0);
    }

    #[tokio::test]
    #[ignore] // Requires database setup
    async fn test_projector_rebuild() {
        // use crate::matching_event_projector::MatchingEventProjector;
        // use std::sync::Arc;
        // use sqlx::postgres::PgPoolOptions;
        //
        // let pool = setup_test_db().await;
        // let projector = MatchingEventProjector::new(
        //     Arc::new(pool),
        //     "matching:BTC/USDT".to_string()
        // );
        //
        // // Rebuild from scratch
        // let count = projector.rebuild().await.unwrap();
        // assert!(count >= 0);
    }
}
