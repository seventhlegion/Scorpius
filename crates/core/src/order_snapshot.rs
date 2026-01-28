use crate::{MarketId, Money, OrderId, OrderStatus, OrderType, Side, UserId};
use serde::{Deserialize, Serialize};

/// Order snapshot for a specific order
///
/// Represents the current state of an order, derived exclusively from
/// immutable matching engine events. This is a read-only projection that
/// is never used for matching or validation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderSnapshot {
    pub order_id: OrderId,
    pub market_id: MarketId,
    pub user_id: UserId,
    pub side: Side,
    pub order_type: OrderType,
    /// Limit price (None for market orders)
    pub price: Option<Money>,
    pub quantity: Money,
    pub filled_quantity: Money,
    pub status: OrderStatus,
    /// Rejection reason (if order was rejected)
    pub rejection_reason: Option<String>,
    /// Timestamp when order was created (from OrderPlaced event)
    pub created_at_ns: i64,
    /// Timestamp when order was last updated
    pub updated_at_ns: i64,
    pub last_processed_event_id: String,
    pub last_processed_sequence: u64,
}

impl OrderSnapshot {
    /// Get the remaining unfilled quantity
    pub fn remaining_quantity(&self) -> Money {
        // Ensure scales match for subtraction
        let max_scale = self.quantity.scale.max(self.filled_quantity.scale);
        let quantity = self.normalize_money(self.quantity, max_scale);
        let filled = self.normalize_money(self.filled_quantity, max_scale);

        Money::new(quantity.amount.saturating_sub(filled.amount), max_scale)
    }

    /// Check if the order is open (can still be filled or canceled)
    pub fn is_open(&self) -> bool {
        matches!(self.status, OrderStatus::New | OrderStatus::PartiallyFilled)
    }

    /// Check if the order is closed (filled or canceled)
    pub fn is_closed(&self) -> bool {
        matches!(self.status, OrderStatus::Filled | OrderStatus::Cancelled)
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
}
