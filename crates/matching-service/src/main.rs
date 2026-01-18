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
