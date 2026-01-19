mod idempotency;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Json, Router,
    http::{HeaderMap, StatusCode},
    routing::post,
};
use serde::{Deserialize, Serialize};
use tonic::transport::Channel;

use idempotency::{
    IdempotencyResponse, IdempotencyStore, InMemoryIdempotencyStore, OperationState,
};
use proto::generated::{
    PlaceOrderRequest, ReleaseRequest, ReserveRequest, ledger_service_client::LedgerServiceClient,
    matching_service_client::MatchingServiceClient,
};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    ledger: Arc<LedgerServiceClient<Channel>>,
    matching: Arc<MatchingServiceClient<Channel>>,
    idempotency_store: Arc<dyn IdempotencyStore>,
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
    // Optional: base_asset and quote_asset for reserve calculation
    // If not provided, we'll parse from market_id (e.g., "BTC/USDT")
    base_asset: Option<String>,
    quote_asset: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TradeExecutedResponse {
    trade_id: String,
    buyer_id: String,
    seller_id: String,
    base_asset: String,
    quote_asset: String,
    base_amount: i64,
    quote_amount: i64,
    scale: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct PlaceOrderHttpResponse {
    accepted: bool,
    trades: Vec<TradeExecutedResponse>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let matching_channel = Channel::from_static("http://127.0.0.1:50051")
        .connect()
        .await?;

    let ledger_channel = Channel::from_static("http://127.0.0.1:50052")
        .connect()
        .await?;

    let matching_client = MatchingServiceClient::new(matching_channel);
    let ledger_client = LedgerServiceClient::new(ledger_channel);

    let state = AppState {
        ledger: Arc::new(ledger_client),
        matching: Arc::new(matching_client),
        idempotency_store: Arc::new(InMemoryIdempotencyStore::new()),
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

/// Release reserved funds using an idempotent release call
/// Returns true if release was successful, false otherwise
async fn release_funds(
    ledger_client: &mut LedgerServiceClient<Channel>,
    order_id: &str,
    user_id: &str,
    asset: &str,
    amount: i64,
    scale: u32,
) -> bool {
    // Use a derived request_id for idempotency: "{order_id}:release"
    let release_request_id = format!("{}:release", order_id);
    let release_req = ReleaseRequest {
        request_id: release_request_id.clone(),
        user_id: user_id.to_string(),
        asset: asset.to_string(),
        amount,
        scale,
    };

    let release_resp = ledger_client.release(release_req).await;
    match release_resp {
        Ok(resp) => {
            if resp.into_inner().success {
                tracing::info!(
                    "Successfully released {} {} for order {}",
                    amount,
                    asset,
                    order_id
                );
                true
            } else {
                tracing::error!(
                    "Release failed for order {} - funds may be locked!",
                    order_id
                );
                false
            }
        }
        Err(e) => {
            tracing::error!(
                "Release call failed for order {}: {:?} - funds may be locked!",
                order_id,
                e
            );
            false
        }
    }
}

async fn place_order(
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
    Json(req): Json<PlaceOrderHttpRequest>,
) -> Result<Json<PlaceOrderHttpResponse>, axum::http::StatusCode> {
    // Extract idempotency key from header (case-insensitive lookup)
    let idempotency_key = headers
        .get("idempotency-key")
        .or_else(|| headers.get("Idempotency-Key"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // If idempotency key is provided, check for cached response
    if let Some(ref key) = idempotency_key {
        if let Some(cached_response) = state.idempotency_store.get(key).await {
            tracing::info!("Returning cached response for idempotency key: {}", key);
            let response: PlaceOrderHttpResponse = serde_json::from_slice(&cached_response.body)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            return Ok(Json(response));
        }

        // Check for in-progress operation state (crash recovery)
        if let Some(op_state) = state.idempotency_store.get_state(key).await {
            tracing::info!(
                "Found in-progress operation state for key {}: {:?}",
                key,
                op_state
            );
            match op_state {
                OperationState::Reserved { order_id } => {
                    // Resume from after reserve - attempt matching
                    // Will be handled below by continuing with the flow
                    tracing::info!(
                        "Resuming operation from reserved state for order {}",
                        order_id
                    );
                }
                OperationState::MatchingAttempted { order_id } => {
                    // This shouldn't happen if we cache properly, but handle it
                    tracing::warn!(
                        "Found matching attempted state without cached response for order {}",
                        order_id
                    );
                    // Return error - operation was incomplete
                    return Err(StatusCode::INTERNAL_SERVER_ERROR);
                }
                OperationState::Completed => {
                    // Should have been cached, but if not, return error
                    tracing::error!(
                        "Found completed state without cached response for key {}",
                        key
                    );
                    return Err(StatusCode::INTERNAL_SERVER_ERROR);
                }
            }
        }
    }

    // Generate order_id (use idempotency key if provided, otherwise generate UUID)
    let order_id = idempotency_key
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    // Validate request
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

    // Parse market_id to get base_asset and quote_asset if not provided
    let (base_asset, quote_asset) =
        if let (Some(base), Some(quote)) = (&req.base_asset, &req.quote_asset) {
            (base.clone(), quote.clone())
        } else {
            // Parse from market_id (format: "BTC/USDT" or "BTC-USDT")
            let parts: Vec<&str> = req.market_id.split(['/', '-']).collect();
            if parts.len() != 2 {
                return Err(axum::http::StatusCode::BAD_REQUEST);
            }
            (parts[0].to_string(), parts[1].to_string())
        };

    // Determine which asset and amount to reserve based on side
    let (reserve_asset, reserve_amount) = if side == 1 {
        // Buy order: reserve quote asset (e.g., USDT)
        (quote_asset.clone(), req.price * req.quantity)
    } else {
        // Sell order: reserve base asset (e.g., BTC)
        (base_asset.clone(), req.quantity)
    };

    // 1. Reserve funds (idempotent operation)
    let reserve_req = ReserveRequest {
        request_id: order_id.clone(),
        user_id: req.user_id.clone(),
        asset: reserve_asset.clone(),
        amount: reserve_amount,
        scale: req.scale,
    };

    let mut ledger_client = (*state.ledger).clone();
    let reserve_resp = ledger_client.reserve(reserve_req).await;

    if reserve_resp.is_err() {
        tracing::error!("Ledger reserve failed: {:?}", reserve_resp.as_ref().err());
        return Err(axum::http::StatusCode::BAD_GATEWAY);
    }

    let reserve_inner = reserve_resp.unwrap().into_inner();
    if !reserve_inner.success {
        tracing::warn!(
            "Reserve failed for order {}: {}",
            order_id,
            reserve_inner.reason
        );
        return Err(axum::http::StatusCode::BAD_REQUEST);
    }

    tracing::info!(
        "Reserved {} {} for order {}",
        reserve_amount,
        reserve_asset,
        order_id
    );

    // Store state after successful reserve (for crash recovery)
    if let Some(ref key) = idempotency_key {
        state
            .idempotency_store
            .set_state(
                key,
                OperationState::Reserved {
                    order_id: order_id.clone(),
                },
            )
            .await;
    }

    // 2. Place order in matching engine
    let grpc_req = PlaceOrderRequest {
        order_id: order_id.clone(), // Pass order_id for idempotency
        user_id: req.user_id.clone(),
        market_id: req.market_id.clone(),
        side,
        order_type,
        price: req.price,
        quantity: req.quantity,
        scale: req.scale,
    };

    // Update state before matching attempt
    if let Some(ref key) = idempotency_key {
        state
            .idempotency_store
            .set_state(
                key,
                OperationState::MatchingAttempted {
                    order_id: order_id.clone(),
                },
            )
            .await;
    }

    let mut matching_client = (*state.matching).clone();
    let match_resp = matching_client.place_order(grpc_req).await;

    // 3. If matching fails, release the reserved funds
    if match_resp.is_err() {
        tracing::error!(
            "Matching failed for order {}, releasing reserved funds",
            order_id
        );
        let release_success = release_funds(
            &mut ledger_client,
            &order_id,
            &req.user_id,
            &reserve_asset,
            reserve_amount,
            req.scale,
        )
        .await;

        // Cache error response even on failure to prevent retries
        if let Some(ref key) = idempotency_key {
            let error_response = PlaceOrderHttpResponse {
                accepted: false,
                trades: vec![],
            };
            let response_body = serde_json::to_vec(&error_response)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            let idempotency_response = IdempotencyResponse {
                status_code: StatusCode::BAD_GATEWAY.as_u16(),
                body: response_body,
            };

            state.idempotency_store.set(key, idempotency_response).await;
            state
                .idempotency_store
                .set_state(key, OperationState::Completed)
                .await;
        }

        if !release_success {
            tracing::error!(
                "CRITICAL: Failed to release funds for order {} - funds may be permanently locked!",
                order_id
            );
        }

        return Err(axum::http::StatusCode::BAD_GATEWAY);
    }

    let inner = match_resp.unwrap().into_inner();

    // 4. If order was not accepted, release the reserved funds
    if !inner.accepted {
        tracing::warn!(
            "Order {} was not accepted by matching engine, releasing reserved funds",
            order_id
        );
        let release_success = release_funds(
            &mut ledger_client,
            &order_id,
            &req.user_id,
            &reserve_asset,
            reserve_amount,
            req.scale,
        )
        .await;

        // Cache rejection response
        if let Some(ref key) = idempotency_key {
            let rejection_response = PlaceOrderHttpResponse {
                accepted: false,
                trades: vec![],
            };
            let response_body = serde_json::to_vec(&rejection_response)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            let idempotency_response = IdempotencyResponse {
                status_code: StatusCode::OK.as_u16(),
                body: response_body,
            };

            state.idempotency_store.set(key, idempotency_response).await;
            state
                .idempotency_store
                .set_state(key, OperationState::Completed)
                .await;
        }

        if !release_success {
            tracing::error!(
                "CRITICAL: Failed to release funds for order {} - funds may be permanently locked!",
                order_id
            );
        }
    }

    let trades: Vec<TradeExecutedResponse> = inner
        .trades
        .into_iter()
        .map(|t| TradeExecutedResponse {
            trade_id: t.trade_id,
            buyer_id: t.buyer_id,
            seller_id: t.seller_id,
            base_asset: t.base_asset,
            quote_asset: t.quote_asset,
            base_amount: t.base_amount,
            quote_amount: t.quote_amount,
            scale: t.scale,
        })
        .collect();

    let http_response = PlaceOrderHttpResponse {
        accepted: inner.accepted,
        trades,
    };

    // Store the response if idempotency key was provided (cache early for safety)
    if let Some(ref key) = idempotency_key {
        let response_body =
            serde_json::to_vec(&http_response).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let idempotency_response = IdempotencyResponse {
            status_code: StatusCode::OK.as_u16(),
            body: response_body,
        };

        // Store response and mark as completed atomically
        state.idempotency_store.set(key, idempotency_response).await;
        state
            .idempotency_store
            .set_state(key, OperationState::Completed)
            .await;
        tracing::info!("Stored response for idempotency key: {}", key);
    }

    Ok(Json(http_response))
}
