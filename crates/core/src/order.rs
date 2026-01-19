use crate::{MarketId, Money, OrderId, UserId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    Limit,
    Market,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Cancelled,
}

#[derive(Debug, Clone)]
pub struct Order {
    pub id: OrderId,
    pub user_id: UserId,
    pub market_id: MarketId,
    pub side: Side,
    pub order_type: OrderType,
    pub price: Option<Money>,
    pub quantity: Money,
    pub filled: Money,
    pub status: OrderStatus,
}

impl Order {
    pub fn remaining(&self) -> Money {
        self.quantity - self.filled
    }
}
