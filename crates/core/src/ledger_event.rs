use crate::{Money, UserId};
use serde::{Deserialize, Serialize};

/// Event metadata for event sourcing, auditing, and distributed tracing.
/// All fields are required for proper event replay, idempotency, and audit trails.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventMetadata {
    /// Sequential event number for ordering and replay.
    /// Must be monotonically increasing and unique per ledger.
    pub sequence: u64,

    /// Unique event identifier (UUID v4 recommended).
    /// Used for deduplication and idempotency checks.
    pub event_id: String,

    /// Schema version of this event type.
    /// Enables schema evolution while maintaining backward compatibility.
    pub schema_version: u32,

    /// Timestamp when the event occurred (Unix epoch nanoseconds).
    /// Used for auditing and temporal queries.
    pub timestamp: i64,

    /// Causation ID: the event/request that caused this event.
    /// Enables tracing the chain of events that led to this one.
    pub causation_id: Option<String>,

    /// Correlation ID: groups related events across services.
    /// Useful for distributed tracing and debugging.
    pub correlation_id: Option<String>,

    /// Source service/component that generated this event.
    /// Useful for auditing and debugging.
    pub source: String,
}

/// Immutable ledger event schema for event sourcing.
///
/// Events represent completed financial actions and are:
/// - **Immutable**: Once created, events cannot be modified
/// - **Idempotent**: Events can be safely replayed using event_id
/// - **Replayable**: All state can be reconstructed from events
/// - **Auditable**: Full history of all financial actions
///
/// Each event variant contains:
/// - Business data: the financial action details
/// - Metadata: event sourcing and audit information
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LedgerEvent {
    /// Funds deposited into a user's account.
    /// This increases the available balance for the specified asset.
    FundsDeposited {
        metadata: EventMetadata,
        user_id: UserId,
        asset: String,
        amount: Money,
        /// Optional reference to external deposit (e.g., blockchain tx hash, bank transfer ID)
        external_reference: Option<String>,
    },

    /// Funds withdrawn from a user's account.
    /// This decreases the available balance for the specified asset.
    FundsWithdrawn {
        metadata: EventMetadata,
        user_id: UserId,
        asset: String,
        amount: Money,
        /// Optional reference to external withdrawal (e.g., blockchain tx hash, bank transfer ID)
        external_reference: Option<String>,
    },

    /// Funds reserved (locked) for an order.
    /// Transfers funds from available to locked balance.
    /// The request_id serves as the idempotency key.
    FundsReserved {
        metadata: EventMetadata,
        /// Idempotency key (typically order_id or request_id)
        request_id: String,
        user_id: UserId,
        asset: String,
        amount: Money,
    },

    /// Funds released (unlocked) back to available balance.
    /// Transfers funds from locked back to available balance.
    /// Typically occurs when an order is cancelled or partially filled.
    FundsReleased {
        metadata: EventMetadata,
        /// Idempotency key (typically order_id or request_id)
        request_id: String,
        user_id: UserId,
        asset: String,
        amount: Money,
    },

    /// Trade settled between buyer and seller.
    /// This is the final step in trade execution:
    /// - Buyer: locked quote asset → available base asset
    /// - Seller: locked base asset → available quote asset
    TradeSettled {
        metadata: EventMetadata,
        /// Idempotency key (trade_id from matching engine)
        trade_id: String,
        buyer_id: UserId,
        seller_id: UserId,
        base_asset: String,
        quote_asset: String,
        base_amount: Money,
        quote_amount: Money,
        /// Execution price (for audit and reporting)
        price: i64,
        /// Maker order ID (for audit trail)
        maker_order_id: Option<String>,
        /// Taker order ID (for audit trail)
        taker_order_id: Option<String>,
    },
}

impl LedgerEvent {
    /// Get the event metadata.
    pub fn metadata(&self) -> &EventMetadata {
        match self {
            LedgerEvent::FundsDeposited { metadata, .. }
            | LedgerEvent::FundsWithdrawn { metadata, .. }
            | LedgerEvent::FundsReserved { metadata, .. }
            | LedgerEvent::FundsReleased { metadata, .. }
            | LedgerEvent::TradeSettled { metadata, .. } => metadata,
        }
    }

    /// Get the event sequence number for ordering.
    pub fn sequence(&self) -> u64 {
        self.metadata().sequence
    }

    /// Get the unique event ID for idempotency.
    pub fn event_id(&self) -> &str {
        &self.metadata().event_id
    }

    /// Get the timestamp when the event occurred.
    pub fn timestamp(&self) -> i64 {
        self.metadata().timestamp
    }

    /// Get the idempotency key for this event type.
    /// Returns None for deposit/withdrawal events that use event_id directly.
    pub fn idempotency_key(&self) -> Option<&str> {
        match self {
            LedgerEvent::FundsReserved { request_id, .. } => Some(request_id),
            LedgerEvent::FundsReleased { request_id, .. } => Some(request_id),
            LedgerEvent::TradeSettled { trade_id, .. } => Some(trade_id),
            LedgerEvent::FundsDeposited { .. } | LedgerEvent::FundsWithdrawn { .. } => None,
        }
    }
}
