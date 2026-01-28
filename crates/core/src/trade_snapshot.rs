use crate::{MarketId, Money, OrderId, TradeId, UserId};
use serde::{Deserialize, Serialize};

/// Trade snapshot representing an immutable trade execution
///
/// Represents a completed trade between two orders, derived exclusively from
/// immutable matching engine events. This is a read-only projection that
/// is never used for matching or validation.
///
/// Trades are append-only: once created, they cannot be modified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeSnapshot {
    pub trade_id: TradeId,
    pub market_id: MarketId,
    pub maker_order_id: OrderId,
    pub taker_order_id: OrderId,
    pub buyer_id: UserId,
    pub seller_id: UserId,
    pub price: Money,
    pub quantity: Money,
    pub quote_amount: Money,
    /// Timestamp when trade was executed (from event metadata)
    pub executed_at_ns: i64,
    /// Event ID that created this trade (for audit trail)
    pub event_id: String,
    /// Event sequence number (for ordering and replay)
    pub event_sequence: u64,
}

impl TradeSnapshot {
    /// Check if a user participated in this trade as buyer
    pub fn is_buyer(&self, user_id: UserId) -> bool {
        self.buyer_id == user_id
    }

    /// Check if a user participated in this trade as seller
    pub fn is_seller(&self, user_id: UserId) -> bool {
        self.seller_id == user_id
    }

    /// Check if a user participated in this trade (either as buyer or seller)
    pub fn involves_user(&self, user_id: UserId) -> bool {
        self.buyer_id == user_id || self.seller_id == user_id
    }

    /// Check if an order participated in this trade (either as maker or taker)
    pub fn involves_order(&self, order_id: OrderId) -> bool {
        self.maker_order_id == order_id || self.taker_order_id == order_id
    }
}
