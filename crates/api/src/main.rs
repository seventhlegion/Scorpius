//! Balance API Service
//!
//! A read-only HTTP API that exposes user balances using the balance read model.
//! This API:
//! - Does NOT call the ledger core (write model)
//! - Uses only the read model derived from ledger events
//! - Is safe under eventual consistency (balances may be slightly stale)
//! - Provides read-only GET endpoints

mod handlers;
mod state;

use axum::Router;
use std::net::SocketAddr;
use std::sync::Arc;
use sqlx::postgres::PgPoolOptions;

use state::AppState;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    // Database connection pool
    // TODO: Move to environment variables or config
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://localhost/scorpius".to_string());

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;

    // Create balance read model
    let read_model = ledger::balance_read_model::BalanceReadModel::new(Arc::new(pool));

    let state = AppState {
        read_model: Arc::new(read_model),
    };

    let app = Router::new()
        .route("/health", axum::routing::get(health_check))
        .route("/balances/:user_id", axum::routing::get(handlers::get_user_balances))
        .route(
            "/balances/:user_id/:asset",
            axum::routing::get(handlers::get_balance),
        )
        .with_state(state);

    let addr: SocketAddr = "0.0.0.0:8081".parse()?;
    tracing::info!("Balance API listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn health_check() -> &'static str {
    "OK"
}
