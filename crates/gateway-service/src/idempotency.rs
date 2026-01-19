use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Stored response for idempotency
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdempotencyResponse {
    pub status_code: u16,
    pub body: Vec<u8>,
}

/// Operation state for crash recovery
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum OperationState {
    /// Reserved funds but matching not yet attempted
    Reserved { order_id: String },
    /// Matching attempted, may need release
    MatchingAttempted { order_id: String },
    /// Operation completed successfully
    Completed,
}

/// Trait for idempotency stores that can be swapped between implementations
#[async_trait]
pub trait IdempotencyStore: Send + Sync {
    /// Get a cached response for an idempotency key
    async fn get(&self, key: &str) -> Option<IdempotencyResponse>;

    /// Store a response for an idempotency key
    async fn set(&self, key: &str, response: IdempotencyResponse);

    /// Check if a key exists (useful for detecting concurrent requests)
    #[allow(dead_code)] // Part of trait interface for future Redis implementation
    async fn exists(&self, key: &str) -> bool;

    /// Store operation state for crash recovery
    async fn set_state(&self, key: &str, state: OperationState);

    /// Get operation state for crash recovery
    async fn get_state(&self, key: &str) -> Option<OperationState>;
}

/// In-memory idempotency store using DashMap for concurrent access
pub struct InMemoryIdempotencyStore {
    store: Arc<DashMap<String, IdempotencyResponse>>,
    state_store: Arc<DashMap<String, OperationState>>,
}

impl InMemoryIdempotencyStore {
    pub fn new() -> Self {
        Self {
            store: Arc::new(DashMap::new()),
            state_store: Arc::new(DashMap::new()),
        }
    }
}

#[async_trait]
impl IdempotencyStore for InMemoryIdempotencyStore {
    async fn get(&self, key: &str) -> Option<IdempotencyResponse> {
        self.store.get(key).map(|entry| entry.value().clone())
    }

    async fn set(&self, key: &str, response: IdempotencyResponse) {
        self.store.insert(key.to_string(), response);
    }

    async fn exists(&self, key: &str) -> bool {
        self.store.contains_key(key)
    }

    async fn set_state(&self, key: &str, state: OperationState) {
        self.state_store.insert(key.to_string(), state);
    }

    async fn get_state(&self, key: &str) -> Option<OperationState> {
        self.state_store.get(key).map(|entry| entry.value().clone())
    }
}

impl Default for InMemoryIdempotencyStore {
    fn default() -> Self {
        Self::new()
    }
}

// Future Redis implementation would look like:
// pub struct RedisIdempotencyStore {
//     client: redis::Client,
// }
//
// #[async_trait]
// impl IdempotencyStore for RedisIdempotencyStore {
//     async fn get(&self, key: &str) -> Option<IdempotencyResponse> {
//         // Redis GET implementation
//     }
//
//     async fn set(&self, key: &str, response: IdempotencyResponse) {
//         // Redis SET implementation with TTL
//     }
//
//     async fn exists(&self, key: &str) -> bool {
//         // Redis EXISTS implementation
//     }
// }
