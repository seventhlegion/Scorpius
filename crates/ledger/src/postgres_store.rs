use crate::event_store::{EventStore, EventStoreError, EventStoreResult};
use scorpius_core::ledger_event::LedgerEvent;
use sqlx::{Pool, Postgres, Row};
use std::sync::Arc;
use uuid::Uuid;

/// PostgreSQL implementation of the event store
///
/// Stores events in the `events` table with deterministic ordering
/// and idempotency guarantees.
pub struct PostgresEventStore {
    pool: Arc<Pool<Postgres>>,
    stream_id: String,
}

impl PostgresEventStore {
    /// Create a new PostgreSQL event store
    pub fn new(pool: Arc<Pool<Postgres>>) -> Self {
        Self {
            pool,
            stream_id: "ledger".to_string(),
        }
    }

    /// Create with a custom stream ID (for testing or multi-tenant scenarios)
    pub fn with_stream_id(pool: Arc<Pool<Postgres>>, stream_id: String) -> Self {
        Self { pool, stream_id }
    }
}

#[async_trait::async_trait]
impl EventStore for PostgresEventStore {
    async fn append(&self, event: &LedgerEvent) -> EventStoreResult<u64> {
        let event_id_str = event.event_id();
        let event_id = Uuid::parse_str(event_id_str)
            .map_err(|e| EventStoreError::Database(format!("Invalid event_id UUID: {}", e)))?;

        // Check idempotency: if event_id already exists, return existing sequence
        if let Some(existing_seq) = self.get_sequence_by_event_id(event_id_str).await? {
            return Ok(existing_seq);
        }

        // Use the sequence from the event's metadata (caller already allocated it)
        let sequence = event.sequence();

        // Serialize event to JSONB
        let payload_value = serde_json::to_value(event)
            .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
        let payload = sqlx::types::Json(payload_value);

        // Extract event type from the event
        let event_type = match event {
            LedgerEvent::FundsDeposited { .. } => "funds_deposited",
            LedgerEvent::FundsWithdrawn { .. } => "funds_withdrawn",
            LedgerEvent::FundsReserved { .. } => "funds_reserved",
            LedgerEvent::FundsReleased { .. } => "funds_released",
            LedgerEvent::TradeSettled { .. } => "trade_settled",
        };

        let metadata = event.metadata();

        // Parse optional UUIDs for causation_id and correlation_id
        let causation_id = metadata
            .causation_id
            .as_ref()
            .and_then(|s| Uuid::parse_str(s).ok());
        let correlation_id = metadata
            .correlation_id
            .as_ref()
            .and_then(|s| Uuid::parse_str(s).ok());

        // Insert event in a transaction
        let result = sqlx::query(
            r#"
            INSERT INTO events (
                stream_id, sequence, event_type, category, event_id,
                schema_version, timestamp_ns, causation_id, correlation_id,
                source, payload
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING sequence
            "#,
        )
        .bind(&self.stream_id)
        .bind(sequence as i64)
        .bind(event_type)
        .bind("ledger")
        .bind(event_id)
        .bind(metadata.schema_version as i32)
        .bind(metadata.timestamp)
        .bind(causation_id)
        .bind(correlation_id)
        .bind(&metadata.source)
        .bind(&payload)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(|e| EventStoreError::Database(e.to_string()))?;

        match result {
            Some(row) => Ok(row.get::<i64, _>("sequence") as u64),
            None => {
                // Event already exists, get its sequence
                self.get_sequence_by_event_id(event_id_str)
                    .await?
                    .ok_or_else(|| EventStoreError::DuplicateEvent(event_id_str.to_string()))
            }
        }
    }

    async fn next_sequence(&self) -> EventStoreResult<u64> {
        // Use the database function to atomically get next sequence
        let result = sqlx::query_scalar::<_, i64>("SELECT get_next_sequence_atomic($1)")
            .bind(&self.stream_id)
            .fetch_one(self.pool.as_ref())
            .await
            .map_err(|e| EventStoreError::Database(e.to_string()))?;

        Ok(result as u64)
    }

    async fn load_all(&self) -> EventStoreResult<Vec<LedgerEvent>> {
        self.load_from(0).await
    }

    async fn load_from(&self, from_sequence: u64) -> EventStoreResult<Vec<LedgerEvent>> {
        let rows = sqlx::query(
            r#"
            SELECT sequence, payload
            FROM events
            WHERE stream_id = $1 AND sequence >= $2
            ORDER BY sequence
            "#,
        )
        .bind(&self.stream_id)
        .bind(from_sequence as i64)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| EventStoreError::Database(e.to_string()))?;

        let mut events = Vec::new();
        let mut last_sequence: Option<u64> = None;

        for row in rows {
            let sequence = row.get::<i64, _>("sequence") as u64;

            // Verify sequence ordering (detect gaps within the loaded range)
            // Only check for gaps if we've already seen at least one event
            if let Some(last_seq) = last_sequence {
                if sequence != last_seq + 1 {
                    return Err(EventStoreError::SequenceGap {
                        expected: last_seq + 1,
                        actual: sequence,
                    });
                }
            } else {
                // First event: allow it to start from any sequence >= from_sequence
                // This handles cases where we're loading from a checkpoint
                if sequence < from_sequence {
                    return Err(EventStoreError::SequenceGap {
                        expected: from_sequence,
                        actual: sequence,
                    });
                }
            }

            // Deserialize event
            let payload: sqlx::types::Json<serde_json::Value> = row.get("payload");
            let event: LedgerEvent = serde_json::from_value(payload.0)
                .map_err(|e| EventStoreError::Serialization(e.to_string()))?;

            events.push(event);
            last_sequence = Some(sequence);
        }

        Ok(events)
    }

    async fn exists(&self, event_id: &str) -> EventStoreResult<bool> {
        let event_id_uuid = Uuid::parse_str(event_id)
            .map_err(|e| EventStoreError::Database(format!("Invalid event_id UUID: {}", e)))?;

        let result = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM events WHERE event_id = $1)",
        )
        .bind(event_id_uuid)
        .fetch_one(self.pool.as_ref())
        .await
        .map_err(|e| EventStoreError::Database(e.to_string()))?;

        Ok(result)
    }

    async fn max_sequence(&self) -> EventStoreResult<Option<u64>> {
        let result = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(sequence) FROM events WHERE stream_id = $1",
        )
        .bind(&self.stream_id)
        .fetch_one(self.pool.as_ref())
        .await
        .map_err(|e| EventStoreError::Database(e.to_string()))?;

        Ok(result.map(|s| s as u64))
    }
}

impl PostgresEventStore {
    /// Helper to get sequence number by event_id
    async fn get_sequence_by_event_id(&self, event_id: &str) -> EventStoreResult<Option<u64>> {
        let event_id_uuid = Uuid::parse_str(event_id)
            .map_err(|e| EventStoreError::Database(format!("Invalid event_id UUID: {}", e)))?;

        let result =
            sqlx::query_scalar::<_, i64>("SELECT sequence FROM events WHERE event_id = $1")
                .bind(event_id_uuid)
                .fetch_optional(self.pool.as_ref())
                .await
                .map_err(|e| EventStoreError::Database(e.to_string()))?;

        Ok(result.map(|s| s as u64))
    }
}
