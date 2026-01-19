use async_trait::async_trait;
use scorpius_core::{ledger_event::LedgerEvent, EventMetadata};

/// Error types for event store operations
#[derive(Debug, thiserror::Error)]
pub enum EventStoreError {
    #[error("Event with id {0} already exists")]
    DuplicateEvent(String),

    #[error("Sequence gap detected: expected {expected}, got {actual}")]
    SequenceGap { expected: u64, actual: u64 },

    #[error("Database error: {0}")]
    Database(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Invalid event: {0}")]
    InvalidEvent(String),
}

/// Result type for event store operations
pub type EventStoreResult<T> = Result<T, EventStoreError>;

/// Event store trait for persistence abstraction
///
/// This trait separates domain logic from persistence concerns,
/// allowing different storage backends (PostgreSQL, in-memory, etc.)
#[async_trait]
pub trait EventStore: Send + Sync {
    /// Append an event to the store
    ///
    /// Returns the sequence number assigned to the event.
    /// Fails if event_id already exists (idempotency check).
    async fn append(&self, event: &LedgerEvent) -> EventStoreResult<u64>;

    /// Get the next sequence number for the ledger stream
    ///
    /// This is used to generate sequence numbers before appending events.
    async fn next_sequence(&self) -> EventStoreResult<u64>;

    /// Load all events in sequence order
    ///
    /// Used for full state replay on startup.
    async fn load_all(&self) -> EventStoreResult<Vec<LedgerEvent>>;

    /// Load events from a specific sequence number
    ///
    /// Used for checkpoint-based replay or incremental updates.
    async fn load_from(&self, from_sequence: u64) -> EventStoreResult<Vec<LedgerEvent>>;

    /// Check if an event with the given event_id exists
    ///
    /// Used for idempotency checks.
    async fn exists(&self, event_id: &str) -> EventStoreResult<bool>;

    /// Get the current maximum sequence number
    ///
    /// Used to verify replay completeness.
    async fn max_sequence(&self) -> EventStoreResult<Option<u64>>;
}

/// Helper to create event metadata
pub fn create_metadata(
    sequence: u64,
    event_id: String,
    causation_id: Option<String>,
    correlation_id: Option<String>,
    source: String,
) -> EventMetadata {
    EventMetadata {
        sequence,
        event_id,
        schema_version: 1,
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64,
        causation_id,
        correlation_id,
        source,
    }
}
