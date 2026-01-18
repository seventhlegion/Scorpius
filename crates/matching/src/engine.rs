use crate::orderbook::OrderBook;
use scorpius_core::{Money, Order, OrderId, OrderStatus, Side, TradeId};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct Trade {
    pub trade_id: TradeId,
    pub buy_order_id: OrderId,
    pub sell_order_id: OrderId,
    pub price: Money,
    pub quantity: Money,
}

/// One matching engine per market
pub struct MatchingEngine {
    orderbook: OrderBook,
    orders: HashMap<OrderId, Order>,
    next_trade_id: u64,
}

impl MatchingEngine {
    pub fn new() -> Self {
        Self {
            orderbook: OrderBook::new(),
            orders: HashMap::new(),
            next_trade_id: 1,
        }
    }

    pub fn place_order(&mut self, mut order: Order) -> Vec<Trade> {
        let mut trades = Vec::new();
        order.status = OrderStatus::New;

        match order.side {
            Side::Buy => self.match_buy(&mut order, &mut trades),
            Side::Sell => self.match_sell(&mut order, &mut trades),
        }

        if order.remaining().amount > 0 && order.status != OrderStatus::Filled {
            self.orderbook.add(&order);
            self.orders.insert(order.id, order);
        }

        trades
    }

    fn match_buy(&mut self, order: &mut Order, trades: &mut Vec<Trade>) {
        let limit_price = order.price.expect("limit orders only");

        while let Some((best_price, queue)) = self.orderbook.best_ask_mut() {
            if best_price > limit_price {
                break;
            }

            if let Some(mut resting) = queue.pop_front() {
                let qty = std::cmp::min(order.remaining().amount, resting.remaining().amount);
                let quantity = Money::new(qty, order.quantity.scale);

                order.filled = order.filled + quantity;
                resting.filled = resting.filled + quantity;

                let trade = Trade {
                    trade_id: TradeId(self.next_trade_id),
                    buy_order_id: order.id,
                    sell_order_id: resting.id,
                    price: best_price,
                    quantity,
                };
                self.next_trade_id += 1;
                trades.push(trade);

                if resting.remaining().amount > 0 {
                    queue.push_front(resting);
                } else {
                    resting.status = OrderStatus::Filled;
                    // If queue is now empty, remove the price level to avoid infinite loop
                    if queue.is_empty() {
                        self.orderbook.asks.remove(&best_price);
                    }
                }

                if order.remaining().amount == 0 {
                    order.status = OrderStatus::Filled;
                    return;
                }
            } else {
                // Queue is empty, remove the price level and break
                self.orderbook.asks.remove(&best_price);
                break;
            }
        }

        order.status = OrderStatus::PartiallyFilled;
    }

    fn match_sell(&mut self, order: &mut Order, trades: &mut Vec<Trade>) {
        let limit_price = order.price.expect("limit orders only");

        while let Some((best_price, queue)) = self.orderbook.best_bid_mut() {
            if best_price < limit_price {
                break;
            }

            if let Some(mut resting) = queue.pop_front() {
                let qty = std::cmp::min(order.remaining().amount, resting.remaining().amount);
                let quantity = Money::new(qty, order.quantity.scale);

                order.filled = order.filled + quantity;
                resting.filled = resting.filled + quantity;

                let trade = Trade {
                    trade_id: TradeId(self.next_trade_id),
                    buy_order_id: resting.id,
                    sell_order_id: order.id,
                    price: best_price,
                    quantity,
                };
                self.next_trade_id += 1;
                trades.push(trade);

                if resting.remaining().amount > 0 {
                    queue.push_front(resting);
                } else {
                    resting.status = OrderStatus::Filled;
                    // If queue is now empty, remove the price level to avoid infinite loop
                    if queue.is_empty() {
                        self.orderbook.bids.remove(&best_price);
                    }
                }

                if order.remaining().amount == 0 {
                    order.status = OrderStatus::Filled;
                    return;
                }
            } else {
                // Queue is empty, remove the price level and break
                self.orderbook.bids.remove(&best_price);
                break;
            }
        }

        order.status = OrderStatus::PartiallyFilled;
    }

    pub fn cancel_order(&mut self, order_id: OrderId) -> bool {
        if self.orders.contains_key(&order_id) {
            self.orderbook.remove(order_id);
            self.orders.remove(&order_id);
            true
        } else {
            false
        }
    }
}
