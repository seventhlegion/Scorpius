use crate::price_level::PriceLevel;
use scorpius_core::{Money, Order, OrderId, Side};
use std::collections::{BTreeMap, VecDeque};

/// Price-time priority order book
pub struct OrderBook {
    pub bids: BTreeMap<Money, VecDeque<Order>>,
    pub asks: BTreeMap<Money, VecDeque<Order>>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    pub fn add(&mut self, order: &Order) {
        let price = order.price.expect("limit orders only");
        let book = match order.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };

        book.entry(price)
            .or_insert_with(VecDeque::new)
            .push_back(order.clone());
    }

    pub fn remove(&mut self, order_id: OrderId) {
        for book in [&mut self.bids, &mut self.asks] {
            for (_, queue) in book.iter_mut() {
                queue.retain(|o| o.id != order_id);
            }
        }
    }

    /// Returns the best ask (lowest sell price) with mutable access to its queue
    pub fn best_ask_mut(&mut self) -> Option<(Money, &mut VecDeque<Order>)> {
        // Get the first (lowest) key from asks
        let first_key = self.asks.keys().next().copied()?;
        self.asks
            .get_mut(&first_key)
            .map(|queue| (first_key, queue))
    }

    /// Returns the best bid (highest buy price) with mutable access to its queue
    pub fn best_bid_mut(&mut self) -> Option<(Money, &mut VecDeque<Order>)> {
        // Get the last (highest) key from bids
        let last_key = self.bids.keys().next_back().copied()?;
        self.bids.get_mut(&last_key).map(|queue| (last_key, queue))
    }
}
