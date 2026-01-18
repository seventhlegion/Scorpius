use crate::{MarketId, Money, OrderId, UserId};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderType {
    Limit,
    Market,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
