#[cfg(test)]
mod tests {
    use super::super::*;
    use scorpius_core::*;

    fn money(amount: i64) -> Money {
        Money::new(amount, 2)
    }

    #[test]
    fn buy_matches_sell_at_same_price() {
        let mut engine = MatchingEngine::new();

        let sell = Order {
            id: OrderId(1),
            user_id: UserId(1),
            market_id: MarketId(1),
            side: Side::Sell,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        let buy = Order {
            id: OrderId(2),
            user_id: UserId(2),
            market_id: MarketId(1),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        engine.place_order(sell);
        let trades = engine.place_order(buy);

        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].quantity.amount, 10);
    }

    #[test]
    fn partial_fill_is_handled() {
        let mut engine = MatchingEngine::new();

        let sell = Order {
            id: OrderId(1),
            user_id: UserId(1),
            market_id: MarketId(1),
            side: Side::Sell,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(5),
            filled: money(0),
            status: OrderStatus::New,
        };

        let buy = Order {
            id: OrderId(2),
            user_id: UserId(2),
            market_id: MarketId(1),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        engine.place_order(sell);
        let trades = engine.place_order(buy);

        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].quantity.amount, 5);
    }

    #[test]
    fn sell_matches_buy_at_same_price() {
        let mut engine = MatchingEngine::new();

        let buy = Order {
            id: OrderId(1),
            user_id: UserId(1),
            market_id: MarketId(1),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        let sell = Order {
            id: OrderId(2),
            user_id: UserId(2),
            market_id: MarketId(1),
            side: Side::Sell,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        engine.place_order(buy);
        let trades = engine.place_order(sell);

        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].quantity.amount, 10);
        assert_eq!(trades[0].buy_order_id, OrderId(1));
        assert_eq!(trades[0].sell_order_id, OrderId(2));
        assert_eq!(trades[0].price, money(1000));
    }

    #[test]
    fn sell_partial_fill_when_sell_quantity_greater() {
        let mut engine = MatchingEngine::new();

        let buy = Order {
            id: OrderId(1),
            user_id: UserId(1),
            market_id: MarketId(1),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(5),
            filled: money(0),
            status: OrderStatus::New,
        };

        let sell = Order {
            id: OrderId(2),
            user_id: UserId(2),
            market_id: MarketId(1),
            side: Side::Sell,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        engine.place_order(buy);
        let trades = engine.place_order(sell);

        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].quantity.amount, 5);
    }

    #[test]
    fn buy_matches_when_willing_to_pay_more() {
        let mut engine = MatchingEngine::new();

        let sell = Order {
            id: OrderId(1),
            user_id: UserId(1),
            market_id: MarketId(1),
            side: Side::Sell,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        let buy = Order {
            id: OrderId(2),
            user_id: UserId(2),
            market_id: MarketId(1),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(money(1100)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        engine.place_order(sell);
        let trades = engine.place_order(buy);

        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].quantity.amount, 10);
        // Should match at ask price (better price)
        assert_eq!(trades[0].price, money(1000));
    }

    #[test]
    fn sell_matches_when_willing_to_take_less() {
        let mut engine = MatchingEngine::new();

        let buy = Order {
            id: OrderId(1),
            user_id: UserId(1),
            market_id: MarketId(1),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(money(1100)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        let sell = Order {
            id: OrderId(2),
            user_id: UserId(2),
            market_id: MarketId(1),
            side: Side::Sell,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        engine.place_order(buy);
        let trades = engine.place_order(sell);

        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].quantity.amount, 10);
        // Should match at bid price (better price)
        assert_eq!(trades[0].price, money(1100));
    }

    #[test]
    fn buy_does_not_match_when_price_too_low() {
        let mut engine = MatchingEngine::new();

        let sell = Order {
            id: OrderId(1),
            user_id: UserId(1),
            market_id: MarketId(1),
            side: Side::Sell,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        let buy = Order {
            id: OrderId(2),
            user_id: UserId(2),
            market_id: MarketId(1),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(money(900)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        engine.place_order(sell);
        let trades = engine.place_order(buy);

        assert_eq!(trades.len(), 0);
    }

    #[test]
    fn sell_does_not_match_when_price_too_high() {
        let mut engine = MatchingEngine::new();

        let buy = Order {
            id: OrderId(1),
            user_id: UserId(1),
            market_id: MarketId(1),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        let sell = Order {
            id: OrderId(2),
            user_id: UserId(2),
            market_id: MarketId(1),
            side: Side::Sell,
            order_type: OrderType::Limit,
            price: Some(money(1100)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        engine.place_order(buy);
        let trades = engine.place_order(sell);

        assert_eq!(trades.len(), 0);
    }

    #[test]
    fn multiple_orders_at_same_price_level() {
        let mut engine = MatchingEngine::new();

        let sell1 = Order {
            id: OrderId(1),
            user_id: UserId(1),
            market_id: MarketId(1),
            side: Side::Sell,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(5),
            filled: money(0),
            status: OrderStatus::New,
        };

        let sell2 = Order {
            id: OrderId(2),
            user_id: UserId(1),
            market_id: MarketId(1),
            side: Side::Sell,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(5),
            filled: money(0),
            status: OrderStatus::New,
        };

        let buy = Order {
            id: OrderId(3),
            user_id: UserId(2),
            market_id: MarketId(1),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        engine.place_order(sell1);
        engine.place_order(sell2);
        let trades = engine.place_order(buy);

        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].quantity.amount, 5);
        assert_eq!(trades[0].sell_order_id, OrderId(1));
        assert_eq!(trades[1].quantity.amount, 5);
        assert_eq!(trades[1].sell_order_id, OrderId(2));
    }

    #[test]
    fn multiple_partial_fills_from_different_orders() {
        let mut engine = MatchingEngine::new();

        let sell1 = Order {
            id: OrderId(1),
            user_id: UserId(1),
            market_id: MarketId(1),
            side: Side::Sell,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(3),
            filled: money(0),
            status: OrderStatus::New,
        };

        let sell2 = Order {
            id: OrderId(2),
            user_id: UserId(1),
            market_id: MarketId(1),
            side: Side::Sell,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(3),
            filled: money(0),
            status: OrderStatus::New,
        };

        let buy = Order {
            id: OrderId(3),
            user_id: UserId(2),
            market_id: MarketId(1),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        engine.place_order(sell1);
        engine.place_order(sell2);
        let trades = engine.place_order(buy);

        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].quantity.amount, 3);
        assert_eq!(trades[1].quantity.amount, 3);
        // Buy order should still have 4 remaining
    }

    #[test]
    fn order_added_to_orderbook_when_not_fully_matched() {
        let mut engine = MatchingEngine::new();

        let buy1 = Order {
            id: OrderId(1),
            user_id: UserId(1),
            market_id: MarketId(1),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        let buy2 = Order {
            id: OrderId(2),
            user_id: UserId(2),
            market_id: MarketId(1),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(money(1100)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        // Place first buy order - should be in orderbook
        engine.place_order(buy1);
        // Place second buy order - should be in orderbook
        engine.place_order(buy2);

        // Now place a sell that should match both
        let sell = Order {
            id: OrderId(3),
            user_id: UserId(3),
            market_id: MarketId(1),
            side: Side::Sell,
            order_type: OrderType::Limit,
            price: Some(money(900)),
            quantity: money(20),
            filled: money(0),
            status: OrderStatus::New,
        };

        let trades = engine.place_order(sell);
        // Should match both buy orders
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].buy_order_id, OrderId(2)); // Higher price first
        assert_eq!(trades[1].buy_order_id, OrderId(1));
    }

    #[test]
    fn cancel_order_removes_from_orderbook() {
        let mut engine = MatchingEngine::new();

        let buy = Order {
            id: OrderId(1),
            user_id: UserId(1),
            market_id: MarketId(1),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        engine.place_order(buy);

        // Cancel the order
        let cancelled = engine.cancel_order(OrderId(1));
        assert_eq!(cancelled, true);

        // Try to match with a sell - should not match
        let sell = Order {
            id: OrderId(2),
            user_id: UserId(2),
            market_id: MarketId(1),
            side: Side::Sell,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        let trades = engine.place_order(sell);
        assert_eq!(trades.len(), 0);
    }

    #[test]
    fn cancel_nonexistent_order_returns_false() {
        let mut engine = MatchingEngine::new();
        let cancelled = engine.cancel_order(OrderId(999));
        assert_eq!(cancelled, false);
    }

    #[test]
    fn trade_ids_increment_correctly() {
        let mut engine = MatchingEngine::new();

        let sell1 = Order {
            id: OrderId(1),
            user_id: UserId(1),
            market_id: MarketId(1),
            side: Side::Sell,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(5),
            filled: money(0),
            status: OrderStatus::New,
        };

        let sell2 = Order {
            id: OrderId(2),
            user_id: UserId(1),
            market_id: MarketId(1),
            side: Side::Sell,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(5),
            filled: money(0),
            status: OrderStatus::New,
        };

        let buy = Order {
            id: OrderId(3),
            user_id: UserId(2),
            market_id: MarketId(1),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        engine.place_order(sell1);
        engine.place_order(sell2);
        let trades = engine.place_order(buy);

        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].trade_id, TradeId(1));
        assert_eq!(trades[1].trade_id, TradeId(2));
    }

    #[test]
    fn buy_matches_at_exact_price_boundary() {
        let mut engine = MatchingEngine::new();

        let sell = Order {
            id: OrderId(1),
            user_id: UserId(1),
            market_id: MarketId(1),
            side: Side::Sell,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        let buy = Order {
            id: OrderId(2),
            user_id: UserId(2),
            market_id: MarketId(1),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        engine.place_order(sell);
        let trades = engine.place_order(buy);

        // Exact price match should work (ask price == limit price)
        assert_eq!(trades.len(), 1);
    }

    #[test]
    fn sell_matches_at_exact_price_boundary() {
        let mut engine = MatchingEngine::new();

        let buy = Order {
            id: OrderId(1),
            user_id: UserId(1),
            market_id: MarketId(1),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        let sell = Order {
            id: OrderId(2),
            user_id: UserId(2),
            market_id: MarketId(1),
            side: Side::Sell,
            order_type: OrderType::Limit,
            price: Some(money(1000)),
            quantity: money(10),
            filled: money(0),
            status: OrderStatus::New,
        };

        engine.place_order(buy);
        let trades = engine.place_order(sell);

        // Exact price match should work (bid price == limit price)
        assert_eq!(trades.len(), 1);
    }
}
