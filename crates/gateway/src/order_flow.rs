use axum::{Json, Router, routing::post};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tonic::transport::Channel;
use uuid::Uuid;

use proto::generated::{
    OrderType, PlaceOrderRequest, Side, matching_service_client::MatchingServiceClient,
};
use proto::generated::{ReserveRequest, ledger_service_client::LedgerServiceClient};

#[derive(Clone)]
pub struct AppState {
    pub ledger: Arc<LedgerServiceClient<Channel>>,
    pub matching: Arc<MatchingServiceClient<Channel>>,
}

#[derive(Deserialize)]
pub struct OrderRequest {
    pub user_id: String,
    pub side: String, // BUY / SELL
    pub base_asset: String,
    pub quote_asset: String,
    pub price: i64,
    pub amount: i64,
    pub scale: u32,
}

#[derive(Serialize)]
pub struct OrderResponse {
    pub accepted: bool,
    pub order_id: String,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/order", post(place_order))
        .with_state(Arc::new(state))
}

async fn place_order(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    Json(req): Json<OrderRequest>,
) -> Json<OrderResponse> {
    let order_id = Uuid::new_v4().to_string();

    // 1. Reserve funds
    let (asset, amount) = if req.side == "BUY" {
        (req.quote_asset.clone(), req.price * req.amount)
    } else {
        (req.base_asset.clone(), req.amount)
    };

    let reserve = ReserveRequest {
        request_id: order_id.clone(),
        user_id: req.user_id.clone(),
        asset,
        amount,
        scale: req.scale,
    };

    let mut ledger_client = (*state.ledger).clone();
    let reserve_resp = ledger_client.reserve(reserve).await;
    if reserve_resp.is_err() || !reserve_resp.unwrap().into_inner().success {
        return Json(OrderResponse {
            accepted: false,
            order_id,
        });
    }

    // 2. Place order
    let side = match Side::from_str_name(&req.side.to_uppercase()) {
        Some(side) => side as i32,
        None => {
            return Json(OrderResponse {
                accepted: false,
                order_id,
            });
        }
    };

    // Construct market_id from base_asset/quote_asset (e.g., "BTC/USDT")
    let market_id = format!("{}/{}", req.base_asset, req.quote_asset);

    let place = PlaceOrderRequest {
        order_id: order_id.clone(),
        user_id: req.user_id.clone(),
        market_id,
        side,
        order_type: OrderType::Limit as i32,
        price: req.price,
        quantity: req.amount,
        scale: req.scale,
    };

    let mut matching_client = (*state.matching).clone();
    let match_resp = matching_client.place_order(place).await;
    if match_resp.is_err() {
        return Json(OrderResponse {
            accepted: false,
            order_id,
        });
    }

    // NOTE: trade settlement callback would happen asynchronously

    Json(OrderResponse {
        accepted: true,
        order_id,
    })
}
