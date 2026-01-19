// crates/matching/src/grpc.rs

use std::sync::{Arc, Mutex};

use tonic::{Request, Response, Status};

use proto::generated::{
    matching_service_server::MatchingService, CancelOrderRequest, CancelOrderResponse,
    PlaceOrderRequest, PlaceOrderResponse, TradeExecuted,
};

use crate::engine::MatchingEngine;
// Use fully qualified paths to avoid shadowing std::core for tonic::async_trait macro

/// gRPC adapter around the synchronous MatchingEngine
#[derive(Clone)]
pub struct MatchingGrpcService {
    engine: Arc<Mutex<MatchingEngine>>,
}

impl MatchingGrpcService {
    pub fn new(engine: MatchingEngine) -> Self {
        Self {
            engine: Arc::new(Mutex::new(engine)),
        }
    }
}

#[tonic::async_trait]
impl MatchingService for MatchingGrpcService {
    async fn place_order(
        &self,
        request: Request<PlaceOrderRequest>,
    ) -> Result<Response<PlaceOrderResponse>, Status> {
        let req = request.into_inner();

        // Parse market_id to extract base and quote assets (format: "BASE/QUOTE")
        let (base_asset, quote_asset) = req
            .market_id
            .split_once('/')
            .ok_or_else(|| Status::invalid_argument("market_id must be in format 'BASE/QUOTE'"))?;

        // For now, we'll use a simple hash of the market_id string as the MarketId
        // This is a workaround since MarketId is u64 but we receive a string
        let market_id_hash = {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            req.market_id.hash(&mut hasher);
            scorpius_core::MarketId(hasher.finish())
        };

        // Parse order_id from request (idempotency key)
        // Hash the string order_id to get a u64 OrderId
        let order_id_u64 = {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            req.order_id.hash(&mut hasher);
            hasher.finish()
        };
        let order_id = scorpius_core::OrderId(order_id_u64);

        // Check if order already exists (idempotency check)
        let mut engine = self.engine.lock().unwrap();
        if let Some(existing_order) = engine.get_order(order_id) {
            // Order already exists, return existing trades (idempotent response)
            // For simplicity, return empty trades - in production, you'd want to
            // track and return the original trades
            return Ok(Response::new(PlaceOrderResponse {
                accepted: existing_order.status != scorpius_core::OrderStatus::Cancelled,
                trades: vec![], // Could be enhanced to return original trades
            }));
        }

        let order = scorpius_core::Order {
            id: order_id,
            user_id: scorpius_core::UserId(
                req.user_id
                    .parse()
                    .map_err(|_| Status::invalid_argument("invalid user_id"))?,
            ),
            market_id: market_id_hash,
            side: match req.side {
                1 => scorpius_core::Side::Buy,
                2 => scorpius_core::Side::Sell,
                _ => return Err(Status::invalid_argument("invalid side")),
            },
            order_type: match req.order_type {
                1 => scorpius_core::OrderType::Limit,
                2 => scorpius_core::OrderType::Market,
                _ => return Err(Status::invalid_argument("invalid order type")),
            },
            price: Some(scorpius_core::Money::new(req.price, req.scale)),
            quantity: scorpius_core::Money::new(req.quantity, req.scale),
            filled: scorpius_core::Money::new(0, req.scale),
            status: scorpius_core::OrderStatus::New,
        };

        let trades = engine.place_order(order.clone());

        // Convert trades to TradeExecuted messages
        let trade_executed: Vec<TradeExecuted> = trades
            .into_iter()
            .map(|trade| {
                // Get buyer and seller user IDs from the orders
                let buy_order = if trade.buy_order_id == order.id {
                    &order
                } else {
                    engine
                        .get_order(trade.buy_order_id)
                        .ok_or_else(|| Status::internal("buy order not found"))?
                };

                let sell_order = if trade.sell_order_id == order.id {
                    &order
                } else {
                    engine
                        .get_order(trade.sell_order_id)
                        .ok_or_else(|| Status::internal("sell order not found"))?
                };

                // Calculate quote_amount: price * quantity
                // Both are Money types, so we need to handle the scale correctly
                // quote_amount = (price.amount * quantity.amount) / 10^scale
                // We'll use the quantity's scale for the result
                let scale_factor = 10_i64.pow(trade.quantity.scale);
                let quote_amount = (trade.price.amount as i128 * trade.quantity.amount as i128)
                    .checked_div(scale_factor as i128)
                    .ok_or_else(|| Status::internal("quote_amount calculation overflow"))?
                    as i64;

                Ok(TradeExecuted {
                    trade_id: trade.trade_id.0.to_string(),
                    buyer_id: buy_order.user_id.0.to_string(),
                    seller_id: sell_order.user_id.0.to_string(),
                    base_asset: base_asset.to_string(),
                    quote_asset: quote_asset.to_string(),
                    base_amount: trade.quantity.amount,
                    quote_amount,
                    scale: trade.quantity.scale,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;

        Ok(Response::new(PlaceOrderResponse {
            accepted: true,
            trades: trade_executed,
        }))
    }

    async fn cancel_order(
        &self,
        request: Request<CancelOrderRequest>,
    ) -> Result<Response<CancelOrderResponse>, Status> {
        let req = request.into_inner();
        let order_id: u64 = req
            .order_id
            .parse()
            .map_err(|_| Status::invalid_argument("invalid order_id"))?;

        let mut engine = self.engine.lock().unwrap();
        let success = engine.cancel_order(scorpius_core::OrderId(order_id));

        Ok(Response::new(CancelOrderResponse { success }))
    }
}

// crates/matching/src/lib.rs (add this line)
// pub mod grpc;
