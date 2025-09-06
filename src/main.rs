use std::env;
use std::path::Path;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{info, error};
use osm_import_rust;

// Include generated protobuf code
pub mod osm_import {
    tonic::include_proto!("osm_import");
}

use osm_import::osm_import_server::{OsmImport, OsmImportServer};
use osm_import::{
    PingRequest, PingResponse, FetchImportBatchRequest, FetchImportBatchResponse,
    fetch_import_batch_request::ImportType,
    fetch_import_batch_response::Response as BatchResponse,
};


#[derive(Default, Clone)]
pub struct OSMImportService;

#[tonic::async_trait]
impl OsmImport for OSMImportService {
    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        Ok(Response::new(PingResponse {
            message: "Pong".to_string(),
        }))
    }

    async fn fetch_import_batch(
        &self,
        request: Request<FetchImportBatchRequest>,
    ) -> Result<Response<FetchImportBatchResponse>, Status> {
        let req: FetchImportBatchRequest = request.into_inner();
        
        // Parse import type and validate
        let (import_type, import_scope, import_arg_dir, extension) = match &req.import_type {
            Some(ImportType::FullDate(date)) => {
                if !date.chars().all(|c| c.is_ascii_digit()) || date.len() != 6 {
                    return Ok(Response::new(FetchImportBatchResponse {
                        response: Some(BatchResponse::Error("date arg invalid (expected ddmmyy)".to_string())),
                    }));
                }
                ("full", date.clone(), date.clone(), ".osm")
            }
            Some(ImportType::DeltaAbc(abc)) => {
                if abc.matches('/').count() != 2 || !abc.chars().all(|c| c.is_ascii_digit() || c == '/') {
                    return Ok(Response::new(FetchImportBatchResponse {
                        response: Some(BatchResponse::Error("abc arg invalid (expected AAA/BBB/CCC)".to_string())),
                    }));
                }
                ("delta", abc.clone(), abc.replace("/", "_"), ".osc")
            }
            None => {
                return Ok(Response::new(FetchImportBatchResponse {
                    response: Some(BatchResponse::Error("import type is unknown".to_string())),
                }));
            }
        };

        info!("📝 Processing request: type={}, scope={}, element_type={}, batch_number={}", 
            import_type, import_scope, req.element_type, req.batch_number);

        let import_dir = format!("./data/bangladesh/{}/{}", import_type, import_arg_dir);
        let import_lock_file = format!("{}/lock", import_dir);
        let import_file = format!("{}{}", import_arg_dir, extension);

        let batch_file = format!(
            "{}/batches/{}/{}.batch_{:06}.xml",
            import_dir, req.element_type, import_file, req.batch_number
        );

        let batches_complete_file = format!(
            "{}/batches/{}/{}.batches_complete",
            import_dir, req.element_type, import_file
        );

        info!("📁 File paths configured:");
        info!("   Import dir: {}", import_dir);
        info!("   Lock file: {}", import_lock_file);
        info!("   Batch file: {}", batch_file);
        info!("   Complete file: {}", batches_complete_file);

        // Check if batch file exists
        info!("🔍 Checking if batch file exists: {}", batch_file);

        if Path::new(&batch_file).exists() {
            info!("✅ Batch file found, reading content...");
            match tokio::fs::read_to_string(&batch_file).await {
                Ok(content) => {
                    info!("📖 Successfully read batch file ({} bytes)", content.len());
                    return Ok(Response::new(FetchImportBatchResponse {
                        response: Some(BatchResponse::BatchContent(content)),
                    }));
                }
                Err(e) => {
                    error!("❌ Failed to read batch file {}: {}", batch_file, e);
                    return Ok(Response::new(FetchImportBatchResponse {
                        response: Some(BatchResponse::Error("Failed to read batch file".to_string())),
                    }));
                }
            }
        } else {
            info!("❌ Batch file does not exist");
        }

        // Check if batches are complete
        info!("🔍 Checking if batches are complete: {}", batches_complete_file);
        if Path::new(&batches_complete_file).exists() {
            info!("✅ Batches are complete!");
            return Ok(Response::new(FetchImportBatchResponse {
                response: Some(BatchResponse::BatchesComplete("".to_string())),
            }));
        } else {
            info!("⏳ Batches not yet complete");
        }

        // If no lock file exists, spawn background processing
        info!("🔒 Checking for lock file: {}", import_lock_file);
        if !Path::new(&import_lock_file).exists() {
            info!("🚀 No lock file found - starting background processing for {} {}", import_type, import_scope);
            
            let import_type_clone = import_type.to_string();
            let import_scope_clone = import_scope.clone();
            let import_dir_clone = import_dir.clone();
            
            tokio::spawn(async move {
                info!("🎯 Background task started for {} {}", import_type_clone, import_scope_clone);
                if let Err(e) = osm_import_rust::process_osm_import(&import_type_clone, &import_scope_clone, &import_dir_clone).await {
                    error!("💥 Background processing failed for {} {}: {}", import_type_clone, import_scope_clone, e);
                } else {
                    info!("🎉 Background processing completed successfully for {} {}", import_type_clone, import_scope_clone);
                }
            });
        } else {
            info!("🔒 Lock file exists - processing already in progress");
        }

        Ok(Response::new(FetchImportBatchResponse {
            response: Some(BatchResponse::BatchesPending("".to_string())),
        }))
    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
            .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        )
        .init();

    let grpc_port = env::var("SERVER_PORT").unwrap_or_else(|_| "8080".to_string());
    let grpc_addr = format!("[::]:{}", grpc_port).parse()?;

    info!("Starting OSM Import Rust gRPC service on {}", grpc_addr);

    let osm_service = OSMImportService::default();

    // Start gRPC server
    Server::builder()
        .add_service(OsmImportServer::new(osm_service))
        .serve(grpc_addr)
        .await?;

    Ok(())
}


