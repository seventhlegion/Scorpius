use std::sync::Arc;
use ledger::balance_read_model::BalanceReadModel;

/// Application state shared across all handlers
#[derive(Clone)]
pub struct AppState {
    /// Balance read model for querying balances
    pub read_model: Arc<BalanceReadModel>,
}
