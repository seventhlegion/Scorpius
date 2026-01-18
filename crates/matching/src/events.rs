use crate::engine::Trade;
use scorpius_core::OrderId;

#[derive(Clone, Debug)]
pub enum MatchEvent {
    OrderFilled(OrderId),
    TradeExecuted(Trade),
    OrderPartiallyFilled(OrderId),
}
