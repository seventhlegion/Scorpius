use std::sync::{Arc, Mutex};

use tonic::{Request, Response, Status};

use proto::generated::{
    ledger_service_server::LedgerService, ReleaseRequest, ReleaseResponse, ReserveRequest,
    ReserveResponse, SettleTradeRequest, SettleTradeResponse,
};

use crate::core::Ledger;
use scorpius_core::Money;

#[derive(Clone)]
pub struct LedgerGrpcService {
    ledger: Arc<Mutex<Ledger>>,
}

impl LedgerGrpcService {
    pub fn new(ledger: Ledger) -> Self {
        Self {
            ledger: Arc::new(Mutex::new(ledger)),
        }
    }
}

#[tonic::async_trait]
impl LedgerService for LedgerGrpcService {
    async fn reserve(
        &self,
        request: Request<ReserveRequest>,
    ) -> Result<Response<ReserveResponse>, Status> {
        let req = request.into_inner();
        let amount = Money::new(req.amount, req.scale);

        let mut ledger = self.ledger.lock().unwrap();
        match ledger.reserve(&req.request_id, &req.user_id, &req.asset, amount) {
            Ok(_) => Ok(Response::new(ReserveResponse {
                success: true,
                reason: "".into(),
            })),
            Err(e) => Ok(Response::new(ReserveResponse {
                success: false,
                reason: e.into(),
            })),
        }
    }

    async fn release(
        &self,
        request: Request<ReleaseRequest>,
    ) -> Result<Response<ReleaseResponse>, Status> {
        let req = request.into_inner();
        let amount = Money::new(req.amount, req.scale);

        let mut ledger = self.ledger.lock().unwrap();
        match ledger.release(&req.request_id, &req.user_id, &req.asset, amount) {
            Ok(_) => Ok(Response::new(ReleaseResponse { success: true })),
            Err(_) => Ok(Response::new(ReleaseResponse { success: false })),
        }
    }

    async fn settle_trade(
        &self,
        request: Request<SettleTradeRequest>,
    ) -> Result<Response<SettleTradeResponse>, Status> {
        let req = request.into_inner();
        let base = Money::new(req.base_amount, req.scale);
        let quote = Money::new(req.quote_amount, req.scale);

        let mut ledger = self.ledger.lock().unwrap();
        match ledger.settle_trade(
            &req.trade_id,
            &req.buyer_id,
            &req.seller_id,
            &req.base_asset,
            &req.quote_asset,
            base,
            quote,
        ) {
            Ok(_) => Ok(Response::new(SettleTradeResponse { success: true })),
            Err(_) => Ok(Response::new(SettleTradeResponse { success: false })),
        }
    }
}
