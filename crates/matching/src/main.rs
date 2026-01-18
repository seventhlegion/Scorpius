use std::net::SocketAddr;

use tonic::transport::Server;

use matching::engine::MatchingEngine;
use matching::grpc::MatchingGrpcService;

use proto::generated::matching_service_server::MatchingServiceServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let addr: SocketAddr = "0.0.0.0:50051".parse()?;

    let engine = MatchingEngine::new();
    let service = MatchingGrpcService::new(engine);

    tracing::info!("Matching gRPC service listening on {}", addr);

    Server::builder()
        .add_service(MatchingServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}

// Cargo.toml (crates/matching-service)
// [dependencies]
// tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
// tonic = "0.11"
// tracing = "0.1"
// tracing-subscriber = "0.3"
// matching = { path = "../matching" }
// proto = { path = "../proto" }
