#[cfg(test)]
mod tests {
    use crate::core::Ledger;
    use scorpius_core::Money;

    fn money(v: i64) -> Money {
        Money::new(v, 2)
    }

    #[test]
    fn reserve_and_release_flow() {
        let mut ledger = Ledger::new();

        let bal = ledger.balance_mut("u1", "USDT", money(0));
        bal.available = money(100);

        ledger.reserve("r1", "u1", "USDT", money(40)).unwrap();
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).available.amount,
            60
        );
        assert_eq!(ledger.balance_mut("u1", "USDT", money(0)).locked.amount, 40);

        ledger.release("r2", "u1", "USDT", money(40)).unwrap();
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).available.amount,
            100
        );
        assert_eq!(ledger.balance_mut("u1", "USDT", money(0)).locked.amount, 0);
    }

    #[test]
    fn settle_trade_flow() {
        let mut ledger = Ledger::new();

        let buyer_usdt = ledger.balance_mut("buyer", "USDT", money(0));
        buyer_usdt.locked = money(100);

        let seller_btc = ledger.balance_mut("seller", "BTC", money(0));
        seller_btc.locked = money(1);

        ledger
            .settle_trade("t1", "buyer", "seller", "BTC", "USDT", money(1), money(100))
            .unwrap();

        assert_eq!(
            ledger
                .balance_mut("buyer", "BTC", money(0))
                .available
                .amount,
            1
        );
        assert_eq!(
            ledger
                .balance_mut("seller", "USDT", money(0))
                .available
                .amount,
            100
        );
    }

    #[test]
    fn reserve_insufficient_funds() {
        let mut ledger = Ledger::new();

        let bal = ledger.balance_mut("u1", "USDT", money(0));
        bal.available = money(50);

        let result = ledger.reserve("r1", "u1", "USDT", money(100));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "insufficient_funds");

        // Balance should remain unchanged
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).available.amount,
            50
        );
        assert_eq!(ledger.balance_mut("u1", "USDT", money(0)).locked.amount, 0);
    }

    #[test]
    fn release_insufficient_locked_funds() {
        let mut ledger = Ledger::new();

        let bal = ledger.balance_mut("u1", "USDT", money(0));
        bal.locked = money(30);

        let result = ledger.release("r1", "u1", "USDT", money(50));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "insufficient_locked_funds");

        // Balance should remain unchanged
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).available.amount,
            0
        );
        assert_eq!(ledger.balance_mut("u1", "USDT", money(0)).locked.amount, 30);
    }

    #[test]
    fn settle_trade_buyer_insufficient_locked() {
        let mut ledger = Ledger::new();

        let buyer_usdt = ledger.balance_mut("buyer", "USDT", money(0));
        buyer_usdt.locked = money(50); // Less than needed

        let seller_btc = ledger.balance_mut("seller", "BTC", money(0));
        seller_btc.locked = money(1);

        let result =
            ledger.settle_trade("t1", "buyer", "seller", "BTC", "USDT", money(1), money(100));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "buyer_locked_insufficient");
    }

    #[test]
    fn settle_trade_seller_insufficient_locked() {
        let mut ledger = Ledger::new();

        let buyer_usdt = ledger.balance_mut("buyer", "USDT", money(0));
        buyer_usdt.locked = money(100);

        let seller_btc = ledger.balance_mut("seller", "BTC", money(0));
        seller_btc.locked = money(0); // Less than needed

        let result =
            ledger.settle_trade("t1", "buyer", "seller", "BTC", "USDT", money(1), money(100));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "seller_locked_insufficient");
    }

    #[test]
    fn reserve_idempotency() {
        let mut ledger = Ledger::new();

        let bal = ledger.balance_mut("u1", "USDT", money(0));
        bal.available = money(100);

        // First reserve
        ledger.reserve("r1", "u1", "USDT", money(40)).unwrap();
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).available.amount,
            60
        );
        assert_eq!(ledger.balance_mut("u1", "USDT", money(0)).locked.amount, 40);

        // Second reserve with same request_id (should be no-op)
        ledger.reserve("r1", "u1", "USDT", money(40)).unwrap();
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).available.amount,
            60
        );
        assert_eq!(ledger.balance_mut("u1", "USDT", money(0)).locked.amount, 40);
    }

    #[test]
    fn release_idempotency() {
        let mut ledger = Ledger::new();

        let bal = ledger.balance_mut("u1", "USDT", money(0));
        bal.available = money(100);
        bal.locked = money(40);

        // First release
        ledger.release("r1", "u1", "USDT", money(40)).unwrap();
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).available.amount,
            140
        );
        assert_eq!(ledger.balance_mut("u1", "USDT", money(0)).locked.amount, 0);

        // Second release with same request_id (should be no-op)
        ledger.release("r1", "u1", "USDT", money(40)).unwrap();
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).available.amount,
            140
        );
        assert_eq!(ledger.balance_mut("u1", "USDT", money(0)).locked.amount, 0);
    }

    #[test]
    fn settle_trade_idempotency() {
        let mut ledger = Ledger::new();

        let buyer_usdt = ledger.balance_mut("buyer", "USDT", money(0));
        buyer_usdt.locked = money(100);

        let seller_btc = ledger.balance_mut("seller", "BTC", money(0));
        seller_btc.locked = money(1);

        // First settle
        ledger
            .settle_trade("t1", "buyer", "seller", "BTC", "USDT", money(1), money(100))
            .unwrap();

        let buyer_btc_before = ledger
            .balance_mut("buyer", "BTC", money(0))
            .available
            .amount;
        let seller_usdt_before = ledger
            .balance_mut("seller", "USDT", money(0))
            .available
            .amount;

        // Second settle with same trade_id (should be no-op)
        ledger
            .settle_trade("t1", "buyer", "seller", "BTC", "USDT", money(1), money(100))
            .unwrap();

        assert_eq!(
            ledger
                .balance_mut("buyer", "BTC", money(0))
                .available
                .amount,
            buyer_btc_before
        );
        assert_eq!(
            ledger
                .balance_mut("seller", "USDT", money(0))
                .available
                .amount,
            seller_usdt_before
        );
    }

    #[test]
    fn reserve_exact_amount() {
        let mut ledger = Ledger::new();

        let bal = ledger.balance_mut("u1", "USDT", money(0));
        bal.available = money(100);

        ledger.reserve("r1", "u1", "USDT", money(100)).unwrap();
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).available.amount,
            0
        );
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).locked.amount,
            100
        );
    }

    #[test]
    fn release_exact_amount() {
        let mut ledger = Ledger::new();

        let bal = ledger.balance_mut("u1", "USDT", money(0));
        bal.locked = money(100);

        ledger.release("r1", "u1", "USDT", money(100)).unwrap();
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).available.amount,
            100
        );
        assert_eq!(ledger.balance_mut("u1", "USDT", money(0)).locked.amount, 0);
    }

    #[test]
    fn multiple_reserves() {
        let mut ledger = Ledger::new();

        let bal = ledger.balance_mut("u1", "USDT", money(0));
        bal.available = money(100);

        ledger.reserve("r1", "u1", "USDT", money(30)).unwrap();
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).available.amount,
            70
        );
        assert_eq!(ledger.balance_mut("u1", "USDT", money(0)).locked.amount, 30);

        ledger.reserve("r2", "u1", "USDT", money(20)).unwrap();
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).available.amount,
            50
        );
        assert_eq!(ledger.balance_mut("u1", "USDT", money(0)).locked.amount, 50);

        ledger.reserve("r3", "u1", "USDT", money(10)).unwrap();
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).available.amount,
            40
        );
        assert_eq!(ledger.balance_mut("u1", "USDT", money(0)).locked.amount, 60);
    }

    #[test]
    fn multiple_releases() {
        let mut ledger = Ledger::new();

        let bal = ledger.balance_mut("u1", "USDT", money(0));
        bal.locked = money(100);

        ledger.release("r1", "u1", "USDT", money(30)).unwrap();
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).available.amount,
            30
        );
        assert_eq!(ledger.balance_mut("u1", "USDT", money(0)).locked.amount, 70);

        ledger.release("r2", "u1", "USDT", money(20)).unwrap();
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).available.amount,
            50
        );
        assert_eq!(ledger.balance_mut("u1", "USDT", money(0)).locked.amount, 50);

        ledger.release("r3", "u1", "USDT", money(10)).unwrap();
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).available.amount,
            60
        );
        assert_eq!(ledger.balance_mut("u1", "USDT", money(0)).locked.amount, 40);
    }

    #[test]
    fn multiple_users_and_assets() {
        let mut ledger = Ledger::new();

        // User 1, USDT
        let bal1 = ledger.balance_mut("u1", "USDT", money(0));
        bal1.available = money(1000);

        // User 2, BTC
        let bal2 = ledger.balance_mut("u2", "BTC", money(0));
        bal2.available = money(5);

        // Reserve from user 1
        ledger.reserve("r1", "u1", "USDT", money(500)).unwrap();
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).available.amount,
            500
        );
        assert_eq!(
            ledger.balance_mut("u2", "BTC", money(0)).available.amount,
            5
        );

        // Reserve from user 2
        ledger.reserve("r2", "u2", "BTC", money(2)).unwrap();
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).available.amount,
            500
        );
        assert_eq!(
            ledger.balance_mut("u2", "BTC", money(0)).available.amount,
            3
        );
        assert_eq!(ledger.balance_mut("u2", "BTC", money(0)).locked.amount, 2);
    }

    #[test]
    fn balance_initialization() {
        let mut ledger = Ledger::new();

        // Accessing non-existent balance should create it with zero values
        let bal = ledger.balance_mut("u1", "USDT", money(0));
        assert_eq!(bal.available.amount, 0);
        assert_eq!(bal.locked.amount, 0);

        // Setting available and checking
        bal.available = money(100);
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).available.amount,
            100
        );
    }

    #[test]
    fn complex_flow_with_multiple_operations() {
        let mut ledger = Ledger::new();

        // Setup: User has 1000 USDT available
        let bal = ledger.balance_mut("u1", "USDT", money(0));
        bal.available = money(1000);

        // Reserve 300 for an order
        ledger.reserve("order1", "u1", "USDT", money(300)).unwrap();
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).available.amount,
            700
        );
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).locked.amount,
            300
        );

        // Reserve another 200 for another order
        ledger.reserve("order2", "u1", "USDT", money(200)).unwrap();
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).available.amount,
            500
        );
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).locked.amount,
            500
        );

        // Release first order (cancelled) - use different request_id for release
        ledger
            .release("cancel_order1", "u1", "USDT", money(300))
            .unwrap();
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).available.amount,
            800
        );
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).locked.amount,
            200
        );

        // Reserve again for new order
        ledger.reserve("order3", "u1", "USDT", money(150)).unwrap();
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).available.amount,
            650
        );
        assert_eq!(
            ledger.balance_mut("u1", "USDT", money(0)).locked.amount,
            350
        );
    }

    #[test]
    fn settle_trade_with_existing_balances() {
        let mut ledger = Ledger::new();

        // Buyer has some BTC already and USDT locked
        let buyer_btc = ledger.balance_mut("buyer", "BTC", money(0));
        buyer_btc.available = money(2);

        let buyer_usdt = ledger.balance_mut("buyer", "USDT", money(0));
        buyer_usdt.locked = money(100);

        // Seller has some USDT already and BTC locked
        let seller_usdt = ledger.balance_mut("seller", "USDT", money(0));
        seller_usdt.available = money(50);

        let seller_btc = ledger.balance_mut("seller", "BTC", money(0));
        seller_btc.locked = money(1);

        // Settle trade
        ledger
            .settle_trade("t1", "buyer", "seller", "BTC", "USDT", money(1), money(100))
            .unwrap();

        // Buyer should have 3 BTC total (2 + 1)
        assert_eq!(
            ledger
                .balance_mut("buyer", "BTC", money(0))
                .available
                .amount,
            3
        );
        assert_eq!(
            ledger.balance_mut("buyer", "USDT", money(0)).locked.amount,
            0
        );

        // Seller should have 150 USDT total (50 + 100)
        assert_eq!(
            ledger
                .balance_mut("seller", "USDT", money(0))
                .available
                .amount,
            150
        );
        assert_eq!(
            ledger.balance_mut("seller", "BTC", money(0)).locked.amount,
            0
        );
    }

    // Balance Read Model Tests
    //
    // Note: These tests would require a test database setup
    // They are provided as examples of how the read model should be used

    #[tokio::test]
    #[ignore] // Requires database setup
    async fn test_balance_read_model_rebuild_from_scratch() {
        // This test demonstrates rebuilding the read model from events
        // use crate::balance_read_model::BalanceReadModel;
        // use crate::postgres_store::PostgresEventStore;
        // use scorpius_core::UserId;
        //
        // let pool = setup_test_db().await;
        // let event_store = PostgresEventStore::new(pool.clone());
        // let read_model = BalanceReadModel::new(pool);
        //
        // // Rebuild from all events
        // read_model.rebuild_from_events(&event_store).await.unwrap();
        //
        // // Verify balances are correct
        // let balance = read_model.get_balance(UserId(1), "USDT").await.unwrap();
        // assert_eq!(balance.unwrap().available.amount, 1000);
    }

    // Ledger Event Projector Tests
    //
    // Note: These tests would require a test database setup
    // They are provided as examples of how the projector should be used

    #[tokio::test]
    #[ignore] // Requires database setup
    async fn test_projector_sync() {
        // use crate::balance_read_model::BalanceReadModel;
        // use crate::postgres_store::PostgresEventStore;
        // use crate::projector::LedgerEventProjector;
        //
        // let pool = setup_test_db().await;
        // let event_store = PostgresEventStore::new(pool.clone());
        // let read_model = BalanceReadModel::new(pool.clone());
        // let projector = LedgerEventProjector::new(event_store, read_model);
        //
        // // Sync once
        // let count = projector.run_once().await.unwrap();
        // assert!(count >= 0);
    }

    #[tokio::test]
    #[ignore] // Requires database setup
    async fn test_projector_rebuild() {
        // use crate::balance_read_model::BalanceReadModel;
        // use crate::postgres_store::PostgresEventStore;
        // use crate::projector::LedgerEventProjector;
        //
        // let pool = setup_test_db().await;
        // let event_store = PostgresEventStore::new(pool.clone());
        // let read_model = BalanceReadModel::new(pool.clone());
        // let projector = LedgerEventProjector::new(event_store, read_model);
        //
        // // Rebuild from scratch
        // let count = projector.rebuild().await.unwrap();
        // assert!(count >= 0);
    }
}
