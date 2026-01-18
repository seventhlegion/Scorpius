use std::net::SocketAddr;
use std::sync::Arc;

use axum::{Json, Router, routing::post};
use serde::{Deserialize, Serialize};
use tonic::transport::Channel;

use proto::generated::{PlaceOrderRequest, matching_service_client::MatchingServiceClient};

#[derive(Clone)]
struct AppState {
    matching: Arc<MatchingServiceClient<Channel>>,
}

#[derive(Debug, Deserialize)]
struct PlaceOrderHttpRequest {
    user_id: String,
    market_id: String,
    side: String,       // "buy" | "sell"
    order_type: String, // "limit" | "market"
    price: i64,
    quantity: i64,
    scale: u32,
}

#[derive(Debug, Serialize)]
struct PlaceOrderHttpResponse {
    order_id: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let matching_channel = Channel::from_static("http://127.0.0.1:50051")
        .connect()
        .await?;

    let matching_client = MatchingServiceClient::new(matching_channel);

    let state = AppState {
        matching: Arc::new(matching_client),
    };

    let app = Router::new()
        .route("/orders", post(place_order))
        .with_state(state);

    let addr: SocketAddr = "0.0.0.0:8080".parse()?;
    tracing::info!("HTTP Gateway listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn place_order(
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(req): Json<PlaceOrderHttpRequest>,
) -> Result<Json<PlaceOrderHttpResponse>, axum::http::StatusCode> {
    let side = match req.side.as_str() {
        "buy" => 1,
        "sell" => 2,
        _ => return Err(axum::http::StatusCode::BAD_REQUEST),
    };

    let order_type = match req.order_type.as_str() {
        "limit" => 1,
        "market" => 2,
        _ => return Err(axum::http::StatusCode::BAD_REQUEST),
    };

    let grpc_req = PlaceOrderRequest {
        user_id: req.user_id,
        market_id: req.market_id,
        side,
        order_type,
        price: req.price,
        quantity: req.quantity,
        scale: req.scale,
    };

    let mut client = (*state.matching).clone();
    let resp = client
        .place_order(grpc_req)
        .await
        .map_err(|_| axum::http::StatusCode::BAD_GATEWAY)?;

    Ok(Json(PlaceOrderHttpResponse {
        order_id: resp.into_inner().order_id,
    }))
}
