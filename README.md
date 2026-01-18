# OSM Batching Tool

A high-performance Rust service for downloading, processing, and batching OpenStreetMap (OSM) data of Bangladesh via gRPC. This service provides efficient batch processing of OSM full imports and delta updates for geographic data pipelines.

## What This Project Does

The OSM Batching Tool is a complete solution for:

1. **OSM Data Downloads**: Automatically downloads OSM PBF files (full imports) and OSC.GZ files (delta updates) from Geofabrik
2. **Format Conversion**: Converts PBF files to XML using osmium-tool 
3. **Data Batching**: Splits large OSM XML files into manageable batches by element type (nodes, ways, relations)
4. **gRPC API**: Provides a gRPC interface for requesting specific batches with proper validation and status tracking
5. **Background Processing**: Handles long-running downloads and processing tasks asynchronously

## Project Structure

```
osm_batching_tool/
├── src/
│   ├── main.rs          # gRPC server implementation & request handling
│   └── lib.rs           # Core OSM processing logic (download, convert, batch)
├── proto/
│   └── osm_import.proto # gRPC service definitions
├── build.rs             # Protobuf compilation build script
├── Cargo.toml           # Rust dependencies and project config
└── README.md            # This file
```

### Core Components

**`src/main.rs`** - gRPC Service Layer:
- Implements the `OSMImport` gRPC service with `Ping` and `FetchImportBatch` endpoints
- Handles request validation for date formats (DDMMYY) and ABC formats (AAA/BBB/CCC)
- Manages file system checks for existing batches and completion markers
- Spawns background processing tasks for new import requests
- Returns appropriate responses: batch content, completion status, pending, or errors

**`src/lib.rs`** - OSM Processing Engine:
- `process_osm_import()`: Main orchestration function for full/delta imports
- `process_full_import()`: Downloads OSM PBF files and converts to XML
- `process_delta_import()`: Downloads OSC.GZ delta files and decompresses
- `batch_osm_xml()`: Core XML parsing and batching logic using quick-xml
- `download_file()`: Streaming file downloader with progress logging
- `convert_pbf_to_xml()`: Wrapper for osmium-tool PBF to XML conversion

**`proto/osm_import.proto`** - API Contract:
- Defines gRPC service interface
- `FetchImportBatchRequest`: Supports both full date imports and delta ABC imports
- `FetchImportBatchResponse`: Handles multiple response types (pending/content/complete/error)

## API Usage

### Full Import (Historical Data)
```bash
# Request batch 0 of nodes from Bangladesh data for September 1, 2025
grpcurl -plaintext -proto proto/osm_import.proto -d '{"batch_number": 0, "full_date": "250901", "element_type": "node"}' localhost:8080 osm_import.OSMImport/FetchImportBatch
```

### Delta Import (Updates)
```bash
# Request batch 0 of ways from delta update 000/000/001
grpcurl -plaintext -proto proto/osm_import.proto -d '{"batch_number": 0, "delta_abc": "000/000/001", "element_type": "way"}' localhost:8080 osm_import.OSMImport/FetchImportBatch
```

## Data Flow

1. **Request**: Client requests a specific batch via gRPC
2. **Validation**: Server validates date/ABC format and checks for existing files
3. **Background Processing** (if needed):
   - Download OSM data from Geofabrik servers
   - Convert PBF to XML format (full imports only)
   - Parse XML and split into batches by element type
   - Create completion markers when done
4. **Response**: Return batch XML content, completion status, or pending indicator

## File Organization

The service organizes data in this structure:
```
./data/
├── full/
│   └── 250901/                    # Date-based full import
│       ├── 250901.osm.pbf         # Downloaded PBF file
│       ├── 250901.osm             # Converted XML file
│       ├── lock                   # Processing lock file
│       └── batches/
│           ├── node/              # Node batches
│           ├── way/               # Way batches
│           └── relation/          # Relation batches
└── delta/
    └── 000_000_001/               # ABC-based delta update
        ├── 000_000_001.osc.gz     # Downloaded delta file
        ├── 000_000_001.osc        # Decompressed delta file
        └── batches/               # Same structure as full
```

## Dependencies

### System Requirements

**Required for Running:**

1. **osmium-tool** - Required for PBF to XML conversion at runtime
   ```bash
   # Windows (Conda - Recommended)
   conda install conda-forge::osmium-tool
   
   # Linux/Ubuntu
   sudo apt-get install osmium-tool
   
   # macOS
   brew install osmium-tool
   ```
   
   For detailed installation instructions and other platforms, see: https://osmcode.org/osmium-tool/

**Required for Building:**

2. **protoc** - Protocol Buffer compiler (needed to compile the project)
   ```bash
   # Windows
   # Download from: https://github.com/protocolbuffers/protobuf/releases
   # Or install via chocolatey: choco install protoc
   
   # Linux/Ubuntu
   sudo apt-get install protobuf-compiler
   
   # macOS
   brew install protobuf
   ```

**For Testing/Development:**

3. **grpcurl** - Command-line gRPC client (for testing the API)
   ```bash
   # Windows
   # Download from: https://github.com/fullstorydev/grpcurl/releases
   # Or install via chocolatey: choco install grpcurl
   
   # Linux/Ubuntu
   sudo apt-get install grpcurl
   
   # macOS
   brew install grpcurl
   ```

### Rust Dependencies

**Core Dependencies (handled by Cargo):**
- `tokio`: Async runtime for concurrent operations
- `tonic`: gRPC framework for service implementation
- `prost`: Protocol Buffer implementation
- `quick-xml`: Fast XML parsing for batching
- `reqwest`: HTTP client for file downloads
- `flate2`: GZ decompression for delta files
- `anyhow`: Error handling
- `tracing`: Structured logging

## Building and Running

```bash
# Development
cargo run

# Production build
cargo build --release

# Set environment variables
export SERVER_PORT=8080
export RUST_LOG=info
```

## Performance Characteristics

- **Streaming Downloads**: Large files downloaded with progress tracking
- **Memory Efficient**: XML parsing without loading entire files into memory  
- **Concurrent Processing**: Background tasks don't block gRPC requests
- **Batch Size Optimization**: 500 elements per batch (full), 1000 (delta)
- **Lock File Protection**: Prevents duplicate processing of same import

## Error Handling

The service handles various error conditions gracefully:
- Invalid date/ABC format validation
- Network failures during downloads
- Missing osmium-tool dependency
- Corrupted or incomplete files
- XML parsing errors
- File system permission issues

All errors are logged with structured tracing and returned as gRPC error responses to clients.