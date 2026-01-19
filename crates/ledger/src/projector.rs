//! Ledger Event Projector
//!
//! A service that continuously consumes ledger events from the event store
//! and projects them onto the balance read model. Ensures:
//! - **Idempotency**: Events can be safely replayed
//! - **Transactional updates**: All updates are atomic
//! - **Deterministic replay**: Events are processed in sequence order
//! - **Fault tolerance**: Can resume from last processed sequence after failures

use crate::balance_read_model::BalanceReadModel;
use crate::event_store::{EventStore, EventStoreResult};
use scorpius_core::ProjectorConfig;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::{interval, MissedTickBehavior};

/// Projector service for consuming ledger events and updating the balance read model
pub struct LedgerEventProjector<S: EventStore> {
    event_store: S,
    read_model: BalanceReadModel,
    /// Interval between polling for new events (when in continuous mode)
    poll_interval: Duration,
    /// Batch size for loading events per iteration
    batch_size: usize,
}

impl<S: EventStore> LedgerEventProjector<S> {
    /// Create a new projector with default configuration
    pub fn new(event_store: S, read_model: BalanceReadModel) -> Self {
        Self::with_config(event_store, read_model, ProjectorConfig::default())
    }

    /// Create a new projector with custom configuration
    pub fn with_config(
        event_store: S,
        read_model: BalanceReadModel,
        config: ProjectorConfig,
    ) -> Self {
        Self {
            event_store,
            read_model,
            poll_interval: config.poll_interval,
            batch_size: config.batch_size,
        }
    }

    /// Project a single batch of events starting from the given sequence
    ///
    /// Returns the sequence number of the last processed event, or None if no events were processed.
    /// This is idempotent and transactional - if an event was already processed, it's safely skipped.
    pub async fn project_batch(&self, from_sequence: u64) -> EventStoreResult<Option<u64>> {
        // Load events from the specified sequence
        let events = self.event_store.load_from(from_sequence).await?;

        if events.is_empty() {
            return Ok(None);
        }

        // Track the last processed sequence
        let mut last_sequence = from_sequence;

        // Project each event transactionally
        // Each project_event call is idempotent, so we can safely process them one by one
        for event in events {
            let sequence = event.sequence();

            // Project the event (idempotent operation)
            self.read_model.project_event(&event).await?;

            last_sequence = sequence;
        }

        Ok(Some(last_sequence))
    }

    /// Sync the read model with the event store
    ///
    /// This will catch up on all events since the last processed sequence.
    /// Returns the number of events processed.
    pub async fn sync(&self) -> EventStoreResult<u64> {
        let mut processed_count = 0u64;
        let mut from_sequence = self
            .read_model
            .last_processed_sequence()
            .await?
            .map(|s| s + 1)
            .unwrap_or(0);

        loop {
            // Load a batch of events
            let events = self.event_store.load_from(from_sequence).await?;

            if events.is_empty() {
                break;
            }

            // Project the batch
            let batch_size = events.len();
            for event in events {
                self.read_model.project_event(&event).await?;
            }

            processed_count += batch_size as u64;
            from_sequence += batch_size as u64;

            // If we processed fewer events than batch size, we've caught up
            if batch_size < self.batch_size {
                break;
            }
        }

        Ok(processed_count)
    }

    /// Rebuild the entire read model from scratch
    ///
    /// This will:
    /// 1. Clear all existing balance snapshots
    /// 2. Replay all events from the event store
    /// 3. Project each event onto the read model
    ///
    /// This is safe to run at any time and ensures consistency.
    /// Useful for:
    /// - Initial setup
    /// - Recovery after data corruption
    /// - Schema migrations
    pub async fn rebuild(&self) -> EventStoreResult<u64> {
        // Clear existing snapshots
        self.read_model
            .rebuild_from_events(&self.event_store)
            .await?;

        // Get the count of processed events
        let max_sequence = self.event_store.max_sequence().await?.unwrap_or(0);

        Ok(max_sequence)
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
    /// * `Err(EventStoreError)` if an error occurred during processing
    pub async fn run_continuous(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> EventStoreResult<()> {
        let mut poll_interval = interval(self.poll_interval);
        poll_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        tracing::info!("Projector received shutdown signal, stopping...");
                        break;
                    }
                }
                _ = poll_interval.tick() => {
                    match self.sync().await {
                        Ok(count) => {
                            if count > 0 {
                                tracing::debug!("Projected {} events", count);
                            }
                        }
                        Err(e) => {
                            tracing::error!("Error projecting events: {}", e);
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
    pub async fn run_once(&self) -> EventStoreResult<u64> {
        tracing::info!("Starting one-time projector sync...");
        let count = self.sync().await?;
        tracing::info!("Projector sync complete, processed {} events", count);
        Ok(count)
    }

    /// Get the current lag (number of unprocessed events)
    ///
    /// Returns the difference between the max sequence in the event store
    /// and the last processed sequence in the read model.
    pub async fn get_lag(&self) -> EventStoreResult<Option<u64>> {
        let max_sequence = self.event_store.max_sequence().await?;
        let last_processed = self.read_model.last_processed_sequence().await?;

        match (max_sequence, last_processed) {
            (Some(max), Some(last)) => Ok(Some(max.saturating_sub(last))),
            (Some(max), None) => Ok(Some(max + 1)), // +1 because we start from 0
            _ => Ok(None),
        }
    }
}
