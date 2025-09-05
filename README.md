# OSM Import Rust Service

A minimal Rust implementation of the OSM Import gRPC service that's compatible with the existing Python version.

## Features

- **Simple & Clean**: Minimal codebase focused on the core functionality
- **gRPC Compatible**: Same API as the Python version
- **Background Processing**: Spawns Python processing tasks (can be replaced with native Rust later)
- **Docker Ready**: Simple containerized deployment

## Building

```bash
cargo build --release
```

## Running

```bash
# Set environment variables
export SERVER_PORT=8080
export RUST_LOG=info

# Run the service
cargo run
```

## Docker

```bash
# Build image
docker build -t osm-import-rust .

# Run container
docker run -p 8080:8080 -v ./data:/app/data osm-import-rust
```

## API

Same gRPC interface as the Python version:

- `Ping(PingRequest) -> PingResponse`
- `FetchImportBatch(FetchImportBatchRequest) -> FetchImportBatchResponse`

## Implementation Notes

This is a minimal drop-in replacement for the Python service. It handles:

1. **Request validation**: Date/ABC format validation
2. **File checking**: Checks for existing batch files and completion markers
3. **Background processing**: Spawns the Python `osm_batch.py` script when needed
4. **Response handling**: Returns appropriate batch content, completion, or pending status

The background processing currently delegates to the existing Python script. This can be replaced with native Rust processing in the future if needed.

## Differences from Python Version

- Written in Rust for better performance and memory safety
- Uses Tokio for async operations
- Maintains same file structure and API compatibility
- Delegates actual OSM processing to existing Python scripts
# osm_batching_tool
