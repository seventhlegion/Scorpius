use std::env;
use std::path::PathBuf;

fn main() {
    // Set PROTOC if not already set (for Homebrew on macOS)
    if env::var("PROTOC").is_err() {
        // Try common Homebrew path first
        let homebrew_protoc = "/opt/homebrew/bin/protoc";
        if PathBuf::from(homebrew_protoc).exists() {
            env::set_var("PROTOC", homebrew_protoc);
        } else {
            // Try standard path
            let standard_protoc = "/usr/local/bin/protoc";
            if PathBuf::from(standard_protoc).exists() {
                env::set_var("PROTOC", standard_protoc);
            }
        }
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let proto_dir = manifest_dir.join("protos");
    let out_dir = manifest_dir.join("src/generated");

    // Verify proto files exist
    let proto_files = [
        proto_dir.join("matching.proto"),
        proto_dir.join("ledger.proto"),
        proto_dir.join("market-data.proto"),
    ];

    for proto_file in &proto_files {
        if !proto_file.exists() {
            panic!("Proto file not found: {}", proto_file.display());
        }
    }

    if !proto_dir.exists() {
        panic!("Proto directory not found: {}", proto_dir.display());
    }

    // Create output directory if it doesn't exist
    std::fs::create_dir_all(&out_dir).expect("Failed to create output directory");

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir(&out_dir)
        .compile(&proto_files, &[proto_dir])
        .unwrap_or_else(|e| panic!("Failed to compile proto files: {}", e));
}
