//! Integration tests for matching engine
//!
//! These tests verify the matching engine functionality independently
//! (without event persistence, which will be added later)

use matching::engine::MatchingEngine;
use scorpius_core::{MarketId, Money, Order, OrderId, OrderStatus, OrderType, Side, UserId};

fn money(amount: i64) -> Money {
    Money::new(amount, 2)
}

#[test]
fn test_matching_engine_basic_order_placement() {
    let mut engine = MatchingEngine::new();

    let order = Order {
        id: OrderId(1),
        user_id: UserId(1001),
        market_id: MarketId(1),
        side: Side::Buy,
        order_type: OrderType::Limit,
        price: Some(money(10000)), // 100.00
        quantity: money(10),       // 0.10
        filled: money(0),
        status: OrderStatus::New,
    };

    let trades = engine.place_order(order.clone());

    // No matches, order should be on the book
    assert_eq!(trades.len(), 0);

    // Verify order is stored (only if not fully filled)
    let stored_order = engine.get_order(order.id);
    assert!(
        stored_order.is_some(),
        "Order should be stored when not filled"
    );
    let stored = stored_order.unwrap();
    assert_eq!(stored.status, OrderStatus::New);
    assert_eq!(stored.remaining().amount, 10);
}

#[test]
fn test_matching_engine_buy_sell_match() {
    let mut engine = MatchingEngine::new();

    // Place sell order first
    let sell_order = Order {
        id: OrderId(1),
        user_id: UserId(1001),
        market_id: MarketId(1),
        side: Side::Sell,
        order_type: OrderType::Limit,
        price: Some(money(10000)), // 100.00
        quantity: money(10),       // 0.10
        filled: money(0),
        status: OrderStatus::New,
    };

    engine.place_order(sell_order);

    // Place buy order that matches
    let buy_order = Order {
        id: OrderId(2),
        user_id: UserId(1002),
        market_id: MarketId(1),
        side: Side::Buy,
        order_type: OrderType::Limit,
        price: Some(money(10000)), // 100.00 (matches)
        quantity: money(10),       // 0.10 (matches)
        filled: money(0),
        status: OrderStatus::New,
    };

    let trades = engine.place_order(buy_order.clone());

    // Should have one trade
    assert_eq!(trades.len(), 1);
    let trade = &trades[0];
    assert_eq!(trade.quantity.amount, 10);
    assert_eq!(trade.price.amount, 10000);
    assert_eq!(trade.buy_order_id, OrderId(2));
    assert_eq!(trade.sell_order_id, OrderId(1));

    // Buy order should be filled (and removed from orders map)
    let stored_buy = engine.get_order(OrderId(2));
    // Note: Fully filled orders are removed from the orders map
    // They're only kept if partially filled or new
    assert!(
        stored_buy.is_none(),
        "Filled orders are removed from the map"
    );

    // Sell order should be filled (and removed)
    let stored_sell = engine.get_order(OrderId(1));
    assert!(
        stored_sell.is_none(),
        "Filled orders are removed from the map"
    );
}

#[test]
fn test_matching_engine_partial_fill() {
    let mut engine = MatchingEngine::new();

    // Place sell order for 5 units
    let sell_order = Order {
        id: OrderId(1),
        user_id: UserId(1001),
        market_id: MarketId(1),
        side: Side::Sell,
        order_type: OrderType::Limit,
        price: Some(money(10000)),
        quantity: money(5), // 0.05
        filled: money(0),
        status: OrderStatus::New,
    };

    engine.place_order(sell_order);

    // Place buy order for 10 units (should partially fill)
    let buy_order = Order {
        id: OrderId(2),
        user_id: UserId(1002),
        market_id: MarketId(1),
        side: Side::Buy,
        order_type: OrderType::Limit,
        price: Some(money(10000)),
        quantity: money(10), // 0.10
        filled: money(0),
        status: OrderStatus::New,
    };

    let trades = engine.place_order(buy_order.clone());

    // Should have one trade for 5 units
    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].quantity.amount, 5);

    // Sell order should be filled (and removed from orders map)
    let stored_sell = engine.get_order(OrderId(1));
    assert!(stored_sell.is_none(), "Filled sell order is removed");

    // Buy order should be partially filled (still in map)
    let stored_buy = engine.get_order(OrderId(2));
    assert!(
        stored_buy.is_some(),
        "Partially filled order should still be in map"
    );
    let buy = stored_buy.unwrap();
    assert_eq!(buy.status, OrderStatus::PartiallyFilled);
    assert_eq!(buy.filled.amount, 5);
    assert_eq!(buy.remaining().amount, 5);
}

#[test]
fn test_matching_engine_price_priority() {
    let mut engine = MatchingEngine::new();

    // Place multiple sell orders at different prices
    let sell1 = Order {
        id: OrderId(1),
        user_id: UserId(1001),
        market_id: MarketId(1),
        side: Side::Sell,
        order_type: OrderType::Limit,
        price: Some(money(10200)), // Higher price
        quantity: money(10),
        filled: money(0),
        status: OrderStatus::New,
    };

    let sell2 = Order {
        id: OrderId(2),
        user_id: UserId(1002),
        market_id: MarketId(1),
        side: Side::Sell,
        order_type: OrderType::Limit,
        price: Some(money(10000)), // Lower price (should match first)
        quantity: money(10),
        filled: money(0),
        status: OrderStatus::New,
    };

    engine.place_order(sell1);
    engine.place_order(sell2);

    // Place buy order at 10100 (should match sell2 at 10000 first)
    let buy_order = Order {
        id: OrderId(3),
        user_id: UserId(1003),
        market_id: MarketId(1),
        side: Side::Buy,
        order_type: OrderType::Limit,
        price: Some(money(10100)),
        quantity: money(10),
        filled: money(0),
        status: OrderStatus::New,
    };

    let trades = engine.place_order(buy_order);

    // Should match with sell2 (lower price)
    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].sell_order_id, OrderId(2));
    assert_eq!(trades[0].price.amount, 10000); // Matched at sell2's price
}

#[test]
fn test_matching_engine_cancel_order() {
    let mut engine = MatchingEngine::new();

    let order = Order {
        id: OrderId(1),
        user_id: UserId(1001),
        market_id: MarketId(1),
        side: Side::Buy,
        order_type: OrderType::Limit,
        price: Some(money(10000)),
        quantity: money(10),
        filled: money(0),
        status: OrderStatus::New,
    };

    engine.place_order(order.clone());

    // Verify order exists
    assert!(engine.get_order(OrderId(1)).is_some());

    // Cancel order
    let success = engine.cancel_order(OrderId(1));
    assert!(success);

    // Verify order is cancelled (removed from map)
    let cancelled = engine.get_order(OrderId(1));
    assert!(
        cancelled.is_none(),
        "Cancelled orders are removed from the map"
    );

    // Try to cancel again (should fail - order doesn't exist)
    let success2 = engine.cancel_order(OrderId(1));
    assert!(!success2); // Order doesn't exist anymore
}

#[test]
fn test_matching_engine_multiple_trades_same_order() {
    let mut engine = MatchingEngine::new();

    // Place multiple sell orders
    for i in 1..=3 {
        let sell = Order {
            id: OrderId(i),
            user_id: UserId(1000 + i),
            market_id: MarketId(1),
            side: Side::Sell,
            order_type: OrderType::Limit,
            price: Some(money(10000)),
            quantity: money(5),
            filled: money(0),
            status: OrderStatus::New,
        };
        engine.place_order(sell);
    }

    // Place one large buy order that should match all three
    let buy_order = Order {
        id: OrderId(10),
        user_id: UserId(2000),
        market_id: MarketId(1),
        side: Side::Buy,
        order_type: OrderType::Limit,
        price: Some(money(10000)),
        quantity: money(20), // More than any single sell order
        filled: money(0),
        status: OrderStatus::New,
    };

    let trades = engine.place_order(buy_order.clone());

    // Should have 3 trades (one for each sell order)
    assert_eq!(trades.len(), 3);

    // All sell orders should be filled (and removed from map)
    for i in 1..=3 {
        let order = engine.get_order(OrderId(i));
        assert!(order.is_none(), "Filled sell orders are removed");
    }

    // Buy order should be partially filled (15 out of 20) - still in map
    let buy = engine.get_order(OrderId(10));
    assert!(
        buy.is_some(),
        "Partially filled buy order should still be in map"
    );
    let buy_order = buy.unwrap();
    assert_eq!(buy_order.status, OrderStatus::PartiallyFilled);
    assert_eq!(buy_order.filled.amount, 15);
    assert_eq!(buy_order.remaining().amount, 5);
}
