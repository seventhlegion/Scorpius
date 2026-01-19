// crates/ledger-service/src/main.rs

use std::net::SocketAddr;

use tonic::transport::Server;

use ledger::core::Ledger;
use ledger::grpc::LedgerGrpcService;

use proto::generated::ledger_service_server::LedgerServiceServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let addr: SocketAddr = "0.0.0.0:50052".parse()?;

    let ledger = Ledger::new();
    let service = LedgerGrpcService::new(ledger);

    tracing::info!("Ledger gRPC service listening on {}", addr);

    Server::builder()
        .add_service(LedgerServiceServer::new(service))
        .add_service(tonic_health::server::health_reporter().1)
        .add_service(
            tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
                .build()?,
        )
        .serve(addr)
        .await?;

    Ok(())
}
