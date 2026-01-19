use crate::{
    ledger_event::EventMetadata, MarketId, Money, OrderId, OrderStatus, OrderType, Side, TradeId,
    UserId,
};
use serde::{Deserialize, Serialize};

/// Immutable matching engine event schema for event sourcing.
///
/// Events represent market facts (order lifecycle and trade execution) and are:
/// - **Immutable**: Once created, events cannot be modified
/// - **Idempotent**: Events can be safely replayed using event_id
/// - **Replayable**: All orderbook state can be reconstructed from events
/// - **Auditable**: Full history of all market activity
/// - **Pure market facts**: No balance or accounting logic (that's ledger's domain)
///
/// Each event variant contains:
/// - Business data: the market fact details
/// - Metadata: event sourcing and audit information
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MatchingEvent {
    /// Order placed on the orderbook.
    /// This event is emitted when an order is accepted and either:
    /// - Immediately matched (fully or partially)
    /// - Added to the orderbook as a resting order
    OrderPlaced {
        metadata: EventMetadata,
        order_id: OrderId,
        market_id: MarketId,
        user_id: UserId,
        side: Side,
        order_type: OrderType,
        /// Limit price (None for market orders)
        price: Option<Money>,
        quantity: Money,
        /// Quantity that was immediately filled (if any)
        filled_quantity: Money,
        /// Final status after placement (New, PartiallyFilled, or Filled)
        status: OrderStatus,
    },

    /// Trade executed between two orders.
    /// This represents a single match between a maker and taker order.
    /// Multiple TradeExecuted events may occur for a single order placement.
    TradeExecuted {
        metadata: EventMetadata,
        trade_id: TradeId,
        market_id: MarketId,
        /// Maker order ID (resting order on the book)
        maker_order_id: OrderId,
        /// Taker order ID (incoming order)
        taker_order_id: OrderId,
        /// Buyer user ID
        buyer_id: UserId,
        /// Seller user ID
        seller_id: UserId,
        /// Execution price
        price: Money,
        /// Quantity traded (base asset amount)
        quantity: Money,
        /// Quote amount (price * quantity)
        quote_amount: Money,
    },

    /// Order partially filled.
    /// Emitted when an order receives a fill but still has remaining quantity.
    /// This is informational - the actual trade is represented by TradeExecuted events.
    OrderPartiallyFilled {
        metadata: EventMetadata,
        order_id: OrderId,
        market_id: MarketId,
        user_id: UserId,
        /// Total quantity filled so far
        filled_quantity: Money,
        /// Remaining quantity still on the orderbook
        remaining_quantity: Money,
    },

    /// Order fully filled.
    /// Emitted when an order has been completely filled and removed from the orderbook.
    /// This is informational - the actual trades are represented by TradeExecuted events.
    OrderFilled {
        metadata: EventMetadata,
        order_id: OrderId,
        market_id: MarketId,
        user_id: UserId,
        /// Total quantity filled
        filled_quantity: Money,
    },

    /// Order canceled and removed from the orderbook.
    /// Emitted when an order is successfully canceled.
    OrderCanceled {
        metadata: EventMetadata,
        order_id: OrderId,
        market_id: MarketId,
        user_id: UserId,
        /// Quantity that was canceled (remaining unfilled quantity)
        canceled_quantity: Money,
        /// Quantity that was already filled before cancellation
        filled_quantity: Money,
    },

    /// Order rejected (invalid order parameters, market closed, etc.).
    /// Emitted when an order cannot be placed due to validation errors.
    OrderRejected {
        metadata: EventMetadata,
        order_id: OrderId,
        market_id: MarketId,
        user_id: UserId,
        /// Reason for rejection (e.g., "invalid_price", "market_closed", "insufficient_funds")
        reason: String,
    },
}

impl MatchingEvent {
    /// Get the event metadata.
    pub fn metadata(&self) -> &EventMetadata {
        match self {
            MatchingEvent::OrderPlaced { metadata, .. }
            | MatchingEvent::TradeExecuted { metadata, .. }
            | MatchingEvent::OrderPartiallyFilled { metadata, .. }
            | MatchingEvent::OrderFilled { metadata, .. }
            | MatchingEvent::OrderCanceled { metadata, .. }
            | MatchingEvent::OrderRejected { metadata, .. } => metadata,
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

    /// Get the market ID for this event.
    pub fn market_id(&self) -> MarketId {
        match self {
            MatchingEvent::OrderPlaced { market_id, .. }
            | MatchingEvent::TradeExecuted { market_id, .. }
            | MatchingEvent::OrderPartiallyFilled { market_id, .. }
            | MatchingEvent::OrderFilled { market_id, .. }
            | MatchingEvent::OrderCanceled { market_id, .. }
            | MatchingEvent::OrderRejected { market_id, .. } => *market_id,
        }
    }

    /// Get the order ID associated with this event (if applicable).
    pub fn order_id(&self) -> Option<OrderId> {
        match self {
            MatchingEvent::OrderPlaced { order_id, .. }
            | MatchingEvent::OrderPartiallyFilled { order_id, .. }
            | MatchingEvent::OrderFilled { order_id, .. }
            | MatchingEvent::OrderCanceled { order_id, .. }
            | MatchingEvent::OrderRejected { order_id, .. } => Some(*order_id),
            MatchingEvent::TradeExecuted { .. } => None,
        }
    }

    /// Get the trade ID if this is a TradeExecuted event.
    pub fn trade_id(&self) -> Option<TradeId> {
        match self {
            MatchingEvent::TradeExecuted { trade_id, .. } => Some(*trade_id),
            _ => None,
        }
    }
}
