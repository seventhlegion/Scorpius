//! Generated gRPC code for exchange services.
//! This crate defines service contracts only.

pub mod generated {
    include!("generated/exchange.matching.rs");
    include!("generated/exchange.ledger.rs");
    include!("generated/exchange.marketdata.rs");
}

/// File descriptor set for reflection, generated at build time
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("generated/descriptor.bin");
