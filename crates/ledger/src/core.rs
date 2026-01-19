use std::collections::{HashMap, HashSet};

use scorpius_core::Money;

#[derive(Debug, Clone)]
pub struct Balance {
    pub available: Money,
    pub locked: Money,
}

impl Balance {
    pub fn new(zero: Money) -> Self {
        Self {
            available: zero.clone(),
            locked: zero,
        }
    }
}

#[derive(Debug)]
pub struct Ledger {
    /// (user_id, asset) -> Balance
    pub(crate) balances: HashMap<(String, String), Balance>,

    /// Idempotency keys (request_id / trade_id)
    pub(crate) processed_requests: HashSet<String>,
}

impl Ledger {
    pub fn new() -> Self {
        Self {
            balances: HashMap::new(),
            processed_requests: HashSet::new(),
        }
    }

    pub fn balance_mut(&mut self, user: &str, asset: &str, zero: Money) -> &mut Balance {
        self.balances
            .entry((user.to_string(), asset.to_string()))
            .or_insert_with(|| Balance::new(zero))
    }

    pub fn reserve(
        &mut self,
        request_id: &str,
        user: &str,
        asset: &str,
        amount: Money,
    ) -> Result<(), &'static str> {
        if self.processed_requests.contains(request_id) {
            return Ok(()); // idempotent no-op
        }

        let balance = self.balance_mut(user, asset, Money::new(0, amount.scale));

        if balance.available < amount {
            return Err("insufficient_funds");
        }

        balance.available = balance.available - amount;
        balance.locked = balance.locked + amount;

        self.processed_requests.insert(request_id.to_string());
        Ok(())
    }

    pub fn release(
        &mut self,
        request_id: &str,
        user: &str,
        asset: &str,
        amount: Money,
    ) -> Result<(), &'static str> {
        if self.processed_requests.contains(request_id) {
            return Ok(());
        }

        let balance = self.balance_mut(user, asset, Money::new(0, amount.scale));

        if balance.locked < amount {
            return Err("insufficient_locked_funds");
        }

        balance.locked = balance.locked - amount;
        balance.available = balance.available + amount;

        self.processed_requests.insert(request_id.to_string());
        Ok(())
    }

    pub fn settle_trade(
        &mut self,
        trade_id: &str,
        buyer: &str,
        seller: &str,
        base_asset: &str,
        quote_asset: &str,
        base_amount: Money,
        quote_amount: Money,
    ) -> Result<(), &'static str> {
        if self.processed_requests.contains(trade_id) {
            return Ok(());
        }

        // Buyer: locked quote -> base available
        {
            let buyer_quote =
                self.balance_mut(buyer, quote_asset, Money::new(0, quote_amount.scale));
            if buyer_quote.locked < quote_amount {
                return Err("buyer_locked_insufficient");
            }
            buyer_quote.locked = buyer_quote.locked - quote_amount;
        }

        {
            let buyer_base = self.balance_mut(buyer, base_asset, Money::new(0, base_amount.scale));
            buyer_base.available = buyer_base.available + base_amount;
        }

        // Seller: locked base -> quote available
        {
            let seller_base =
                self.balance_mut(seller, base_asset, Money::new(0, base_amount.scale));
            if seller_base.locked < base_amount {
                return Err("seller_locked_insufficient");
            }
            seller_base.locked = seller_base.locked - base_amount;
        }

        {
            let seller_quote =
                self.balance_mut(seller, quote_asset, Money::new(0, quote_amount.scale));
            seller_quote.available = seller_quote.available + quote_amount;
        }

        self.processed_requests.insert(trade_id.to_string());
        Ok(())
    }
}
