use crate::core::Ledger;
use crate::event_store::{EventStore, EventStoreError, EventStoreResult};
use scorpius_core::{ledger_event::LedgerEvent, Money};

/// Replay events to rebuild ledger state
///
/// This function deterministically rebuilds the entire ledger state
/// by replaying all events in sequence order.
pub async fn replay_events<E: EventStore>(
    event_store: &E,
) -> EventStoreResult<Ledger> {
    let mut ledger = Ledger::new();

    // Load all events in sequence order
    let events = event_store.load_all().await?;

    // Replay each event
    for event in events {
        replay_event(&mut ledger, &event)?;
    }

    Ok(ledger)
}

/// Replay events from a specific sequence number
///
/// Useful for incremental updates or checkpoint-based recovery.
pub async fn replay_from<E: EventStore>(
    event_store: &E,
    from_sequence: u64,
    initial_state: Ledger,
) -> EventStoreResult<Ledger> {
    let mut ledger = initial_state;

    // Load events from checkpoint
    let events = event_store.load_from(from_sequence).await?;

    // Replay remaining events
    for event in events {
        replay_event(&mut ledger, &event)?;
    }

    Ok(ledger)
}

/// Replay a single event to update ledger state
///
/// This is idempotent: replaying the same event multiple times
/// results in the same final state.
fn replay_event(ledger: &mut Ledger, event: &LedgerEvent) -> Result<(), EventStoreError> {
    match event {
        LedgerEvent::FundsDeposited {
            metadata,
            user_id,
            asset,
            amount,
            ..
        } => {
            // Idempotency check: skip if already processed
            if ledger.processed_requests.contains(&metadata.event_id) {
                return Ok(());
            }

            // Update balance
            let balance = ledger.balance_mut(
                &user_id.to_string(),
                asset,
                Money::new(0, amount.scale),
            );
            balance.available = balance.available + *amount;

            // Mark as processed
            ledger.processed_requests.insert(metadata.event_id.clone());
        }

        LedgerEvent::FundsWithdrawn {
            metadata,
            user_id,
            asset,
            amount,
            ..
        } => {
            if ledger.processed_requests.contains(&metadata.event_id) {
                return Ok(());
            }

            let balance = ledger.balance_mut(
                &user_id.to_string(),
                asset,
                Money::new(0, amount.scale),
            );

            // Validate sufficient balance
            if balance.available < *amount {
                return Err(EventStoreError::InvalidEvent(format!(
                    "Insufficient funds for withdrawal: user={}, asset={}, available={:?}, requested={:?}",
                    user_id, asset, balance.available, amount
                )));
            }

            balance.available = balance.available - *amount;
            ledger.processed_requests.insert(metadata.event_id.clone());
        }

        LedgerEvent::FundsReserved {
            metadata: _,
            request_id,
            user_id,
            asset,
            amount,
        } => {
            // Use request_id as idempotency key
            if ledger.processed_requests.contains(request_id) {
                return Ok(());
            }

            let balance = ledger.balance_mut(
                &user_id.to_string(),
                asset,
                Money::new(0, amount.scale),
            );

            if balance.available < *amount {
                return Err(EventStoreError::InvalidEvent(format!(
                    "Insufficient funds for reservation: user={}, asset={}, available={:?}, requested={:?}",
                    user_id, asset, balance.available, amount
                )));
            }

            balance.available = balance.available - *amount;
            balance.locked = balance.locked + *amount;
            ledger.processed_requests.insert(request_id.clone());
        }

        LedgerEvent::FundsReleased {
            metadata: _,
            request_id,
            user_id,
            asset,
            amount,
        } => {
            if ledger.processed_requests.contains(request_id) {
                return Ok(());
            }

            let balance = ledger.balance_mut(
                &user_id.to_string(),
                asset,
                Money::new(0, amount.scale),
            );

            if balance.locked < *amount {
                return Err(EventStoreError::InvalidEvent(format!(
                    "Insufficient locked funds for release: user={}, asset={}, locked={:?}, requested={:?}",
                    user_id, asset, balance.locked, amount
                )));
            }

            balance.locked = balance.locked - *amount;
            balance.available = balance.available + *amount;
            ledger.processed_requests.insert(request_id.clone());
        }

        LedgerEvent::TradeSettled {
            metadata,
            trade_id,
            buyer_id,
            seller_id,
            base_asset,
            quote_asset,
            base_amount,
            quote_amount,
            ..
        } => {
            if ledger.processed_requests.contains(trade_id) {
                return Ok(());
            }

            // Buyer: locked quote -> available base
            {
                let buyer_quote = ledger.balance_mut(
                    &buyer_id.to_string(),
                    quote_asset,
                    Money::new(0, quote_amount.scale),
                );

                if buyer_quote.locked < *quote_amount {
                    return Err(EventStoreError::InvalidEvent(format!(
                        "Buyer locked insufficient for trade: buyer={}, asset={}, locked={:?}, required={:?}",
                        buyer_id, quote_asset, buyer_quote.locked, quote_amount
                    )));
                }

                buyer_quote.locked = buyer_quote.locked - *quote_amount;
            }

            {
                let buyer_base = ledger.balance_mut(
                    &buyer_id.to_string(),
                    base_asset,
                    Money::new(0, base_amount.scale),
                );
                buyer_base.available = buyer_base.available + *base_amount;
            }

            // Seller: locked base -> available quote
            {
                let seller_base = ledger.balance_mut(
                    &seller_id.to_string(),
                    base_asset,
                    Money::new(0, base_amount.scale),
                );

                if seller_base.locked < *base_amount {
                    return Err(EventStoreError::InvalidEvent(format!(
                        "Seller locked insufficient for trade: seller={}, asset={}, locked={:?}, required={:?}",
                        seller_id, base_asset, seller_base.locked, base_amount
                    )));
                }

                seller_base.locked = seller_base.locked - *base_amount;
            }

            {
                let seller_quote = ledger.balance_mut(
                    &seller_id.to_string(),
                    quote_asset,
                    Money::new(0, quote_amount.scale),
                );
                seller_quote.available = seller_quote.available + *quote_amount;
            }

            ledger.processed_requests.insert(trade_id.clone());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use scorpius_core::{ledger_event::EventMetadata, UserId};
    use std::collections::HashMap;

    // Test helpers would go here
}
