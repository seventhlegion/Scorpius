use crate::{Money, UserId};
use serde::{Deserialize, Serialize};

/// Balance snapshot for a user's asset
///
/// Represents the current state of a user's balance for a specific asset,
/// derived exclusively from immutable ledger events. This is a read-only
/// projection that is never used for validation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BalanceSnapshot {
    pub user_id: UserId,
    pub asset: String,
    pub available: Money,
    pub locked: Money,
    pub last_processed_event_id: String,
    pub last_processed_sequence: u64,
}
