use std::time::Duration;

/// Configuration for event projectors
#[derive(Debug, Clone)]
pub struct ProjectorConfig {
    /// Poll interval for continuous mode
    pub poll_interval: Duration,
    /// Batch size for loading events
    pub batch_size: usize,
}

impl Default for ProjectorConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(100),
            batch_size: 1000,
        }
    }
}
