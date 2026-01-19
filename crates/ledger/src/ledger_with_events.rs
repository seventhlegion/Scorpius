use crate::core::Ledger;
use crate::event_store::{create_metadata, EventStore, EventStoreError, EventStoreResult};
use scorpius_core::{ledger_event::LedgerEvent, Money, UserId};
use std::sync::Arc;
use uuid::Uuid;

/// Ledger that uses event sourcing
///
/// All state changes are represented as events that are:
/// 1. Appended to the event store
/// 2. Applied to in-memory state
///
/// This ensures deterministic recovery via replay.
pub struct EventSourcedLedger<E: EventStore> {
    /// In-memory state (rebuilt via replay)
    state: Ledger,

    /// Event store for persistence
    event_store: Arc<E>,

    /// Source identifier for events
    source: String,
}

impl<E: EventStore> EventSourcedLedger<E> {
    /// Create a new event-sourced ledger
    ///
    /// State is rebuilt by replaying all events from the store.
    pub async fn new(
        event_store: Arc<E>,
        source: String,
    ) -> EventStoreResult<Self> {
        // Replay all events to rebuild state
        let state = crate::replay::replay_events(event_store.as_ref()).await?;

        Ok(Self {
            state,
            event_store,
            source,
        })
    }

    /// Create from existing state (for testing or migration scenarios)
    pub fn from_state(
        state: Ledger,
        event_store: Arc<E>,
        source: String,
    ) -> Self {
        Self {
            state,
            event_store,
            source,
        }
    }

    /// Deposit funds into a user's account
    pub async fn deposit(
        &mut self,
        user_id: UserId,
        asset: String,
        amount: Money,
        external_reference: Option<String>,
        causation_id: Option<String>,
        correlation_id: Option<String>,
    ) -> EventStoreResult<()> {
        // Generate event ID
        let event_id = Uuid::new_v4().to_string();

        // Get next sequence number
        let sequence = self.event_store.next_sequence().await?;

        // Create metadata
        let metadata = create_metadata(
            sequence,
            event_id.clone(),
            causation_id,
            correlation_id,
            self.source.clone(),
        );

        // Create event
        let event = LedgerEvent::FundsDeposited {
            metadata,
            user_id,
            asset: asset.clone(),
            amount,
            external_reference,
        };

        // Append to store (idempotent)
        self.event_store.append(&event).await?;

        // Apply to in-memory state
        self.apply_deposit(user_id, asset, amount, &event_id)?;

        Ok(())
    }

    /// Withdraw funds from a user's account
    pub async fn withdraw(
        &mut self,
        user_id: UserId,
        asset: String,
        amount: Money,
        external_reference: Option<String>,
        causation_id: Option<String>,
        correlation_id: Option<String>,
    ) -> EventStoreResult<()> {
        // Validate sufficient balance
        let balance = self.state.balance_mut(
            &user_id.to_string(),
            &asset,
            Money::new(0, amount.scale),
        );

        if balance.available < amount {
            return Err(EventStoreError::InvalidEvent(format!(
                "Insufficient funds: available={:?}, requested={:?}",
                balance.available, amount
            )));
        }

        // Generate event ID
        let event_id = Uuid::new_v4().to_string();
        let sequence = self.event_store.next_sequence().await?;

        let metadata = create_metadata(
            sequence,
            event_id.clone(),
            causation_id,
            correlation_id,
            self.source.clone(),
        );

        let event = LedgerEvent::FundsWithdrawn {
            metadata,
            user_id,
            asset: asset.clone(),
            amount,
            external_reference,
        };

        self.event_store.append(&event).await?;
        self.apply_withdraw(user_id, asset, amount, &event_id)?;

        Ok(())
    }

    /// Reserve (lock) funds for an order
    pub async fn reserve(
        &mut self,
        request_id: String,
        user_id: UserId,
        asset: String,
        amount: Money,
        causation_id: Option<String>,
        correlation_id: Option<String>,
    ) -> EventStoreResult<()> {
        // Check idempotency
        if self.state.processed_requests.contains(&request_id) {
            return Ok(()); // Already processed
        }

        // Validate sufficient balance
        let balance = self.state.balance_mut(
            &user_id.to_string(),
            &asset,
            Money::new(0, amount.scale),
        );

        if balance.available < amount {
            return Err(EventStoreError::InvalidEvent(format!(
                "Insufficient funds for reservation: available={:?}, requested={:?}",
                balance.available, amount
            )));
        }

        let event_id = Uuid::new_v4().to_string();
        let sequence = self.event_store.next_sequence().await?;

        let metadata = create_metadata(
            sequence,
            event_id,
            causation_id,
            correlation_id,
            self.source.clone(),
        );

        let event = LedgerEvent::FundsReserved {
            metadata,
            request_id: request_id.clone(),
            user_id,
            asset: asset.clone(),
            amount,
        };

        self.event_store.append(&event).await?;
        self.apply_reserve(request_id, user_id, asset, amount)?;

        Ok(())
    }

    /// Release (unlock) reserved funds
    pub async fn release(
        &mut self,
        request_id: String,
        user_id: UserId,
        asset: String,
        amount: Money,
        causation_id: Option<String>,
        correlation_id: Option<String>,
    ) -> EventStoreResult<()> {
        if self.state.processed_requests.contains(&request_id) {
            return Ok(());
        }

        let balance = self.state.balance_mut(
            &user_id.to_string(),
            &asset,
            Money::new(0, amount.scale),
        );

        if balance.locked < amount {
            return Err(EventStoreError::InvalidEvent(format!(
                "Insufficient locked funds: locked={:?}, requested={:?}",
                balance.locked, amount
            )));
        }

        let event_id = Uuid::new_v4().to_string();
        let sequence = self.event_store.next_sequence().await?;

        let metadata = create_metadata(
            sequence,
            event_id,
            causation_id,
            correlation_id,
            self.source.clone(),
        );

        let event = LedgerEvent::FundsReleased {
            metadata,
            request_id: request_id.clone(),
            user_id,
            asset: asset.clone(),
            amount,
        };

        self.event_store.append(&event).await?;
        self.apply_release(request_id, user_id, asset, amount)?;

        Ok(())
    }

    /// Settle a trade between buyer and seller
    pub async fn settle_trade(
        &mut self,
        trade_id: String,
        buyer_id: UserId,
        seller_id: UserId,
        base_asset: String,
        quote_asset: String,
        base_amount: Money,
        quote_amount: Money,
        price: i64,
        maker_order_id: Option<String>,
        taker_order_id: Option<String>,
        causation_id: Option<String>,
        correlation_id: Option<String>,
    ) -> EventStoreResult<()> {
        if self.state.processed_requests.contains(&trade_id) {
            return Ok(());
        }

        // Validate buyer has sufficient locked quote
        let buyer_quote = self.state.balance_mut(
            &buyer_id.to_string(),
            &quote_asset,
            Money::new(0, quote_amount.scale),
        );

        if buyer_quote.locked < quote_amount {
            return Err(EventStoreError::InvalidEvent(format!(
                "Buyer locked insufficient: locked={:?}, required={:?}",
                buyer_quote.locked, quote_amount
            )));
        }

        // Validate seller has sufficient locked base
        let seller_base = self.state.balance_mut(
            &seller_id.to_string(),
            &base_asset,
            Money::new(0, base_amount.scale),
        );

        if seller_base.locked < base_amount {
            return Err(EventStoreError::InvalidEvent(format!(
                "Seller locked insufficient: locked={:?}, required={:?}",
                seller_base.locked, base_amount
            )));
        }

        let event_id = Uuid::new_v4().to_string();
        let sequence = self.event_store.next_sequence().await?;

        let metadata = create_metadata(
            sequence,
            event_id,
            causation_id,
            correlation_id,
            self.source.clone(),
        );

        let event = LedgerEvent::TradeSettled {
            metadata,
            trade_id: trade_id.clone(),
            buyer_id,
            seller_id,
            base_asset: base_asset.clone(),
            quote_asset: quote_asset.clone(),
            base_amount,
            quote_amount,
            price,
            maker_order_id,
            taker_order_id,
        };

        self.event_store.append(&event).await?;
        self.apply_settle_trade(
            trade_id,
            buyer_id,
            seller_id,
            base_asset,
            quote_asset,
            base_amount,
            quote_amount,
        )?;

        Ok(())
    }

    /// Get the current ledger state (for queries)
    pub fn state(&self) -> &Ledger {
        &self.state
    }

    // Private methods to apply events to state

    fn apply_deposit(
        &mut self,
        user_id: UserId,
        asset: String,
        amount: Money,
        event_id: &str,
    ) -> Result<(), EventStoreError> {
        if self.state.processed_requests.contains(event_id) {
            return Ok(());
        }

        let balance = self.state.balance_mut(
            &user_id.to_string(),
            &asset,
            Money::new(0, amount.scale),
        );
        balance.available = balance.available + amount;
        self.state.processed_requests.insert(event_id.to_string());
        Ok(())
    }

    fn apply_withdraw(
        &mut self,
        user_id: UserId,
        asset: String,
        amount: Money,
        event_id: &str,
    ) -> Result<(), EventStoreError> {
        if self.state.processed_requests.contains(event_id) {
            return Ok(());
        }

        let balance = self.state.balance_mut(
            &user_id.to_string(),
            &asset,
            Money::new(0, amount.scale),
        );
        balance.available = balance.available - amount;
        self.state.processed_requests.insert(event_id.to_string());
        Ok(())
    }

    fn apply_reserve(
        &mut self,
        request_id: String,
        user_id: UserId,
        asset: String,
        amount: Money,
    ) -> Result<(), EventStoreError> {
        if self.state.processed_requests.contains(&request_id) {
            return Ok(());
        }

        let balance = self.state.balance_mut(
            &user_id.to_string(),
            &asset,
            Money::new(0, amount.scale),
        );
        balance.available = balance.available - amount;
        balance.locked = balance.locked + amount;
        self.state.processed_requests.insert(request_id);
        Ok(())
    }

    fn apply_release(
        &mut self,
        request_id: String,
        user_id: UserId,
        asset: String,
        amount: Money,
    ) -> Result<(), EventStoreError> {
        if self.state.processed_requests.contains(&request_id) {
            return Ok(());
        }

        let balance = self.state.balance_mut(
            &user_id.to_string(),
            &asset,
            Money::new(0, amount.scale),
        );
        balance.locked = balance.locked - amount;
        balance.available = balance.available + amount;
        self.state.processed_requests.insert(request_id);
        Ok(())
    }

    fn apply_settle_trade(
        &mut self,
        trade_id: String,
        buyer_id: UserId,
        seller_id: UserId,
        base_asset: String,
        quote_asset: String,
        base_amount: Money,
        quote_amount: Money,
    ) -> Result<(), EventStoreError> {
        if self.state.processed_requests.contains(&trade_id) {
            return Ok(());
        }

        // Buyer: locked quote -> available base
        {
            let buyer_quote = self.state.balance_mut(
                &buyer_id.to_string(),
                &quote_asset,
                Money::new(0, quote_amount.scale),
            );
            buyer_quote.locked = buyer_quote.locked - quote_amount;
        }

        {
            let buyer_base = self.state.balance_mut(
                &buyer_id.to_string(),
                &base_asset,
                Money::new(0, base_amount.scale),
            );
            buyer_base.available = buyer_base.available + base_amount;
        }

        // Seller: locked base -> available quote
        {
            let seller_base = self.state.balance_mut(
                &seller_id.to_string(),
                &base_asset,
                Money::new(0, base_amount.scale),
            );
            seller_base.locked = seller_base.locked - base_amount;
        }

        {
            let seller_quote = self.state.balance_mut(
                &seller_id.to_string(),
                &quote_asset,
                Money::new(0, quote_amount.scale),
            );
            seller_quote.available = seller_quote.available + quote_amount;
        }

        self.state.processed_requests.insert(trade_id);
        Ok(())
    }
}
