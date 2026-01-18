// crates/matching/src/grpc.rs

use std::sync::{Arc, Mutex};

use tonic::{Request, Response, Status};

use proto::generated::{
    matching_service_server::MatchingService, CancelOrderRequest, CancelOrderResponse,
    PlaceOrderRequest, PlaceOrderResponse,
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

        let order = scorpius_core::Order {
            id: scorpius_core::OrderId(rand::random::<u64>()),
            user_id: scorpius_core::UserId(
                req.user_id
                    .parse()
                    .map_err(|_| Status::invalid_argument("invalid user_id"))?,
            ),
            market_id: scorpius_core::MarketId(
                req.market_id
                    .parse()
                    .map_err(|_| Status::invalid_argument("invalid market_id"))?,
            ),
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

        let mut engine = self.engine.lock().unwrap();
        engine.place_order(order);

        Ok(Response::new(PlaceOrderResponse {
            order_id: "generated".into(),
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
