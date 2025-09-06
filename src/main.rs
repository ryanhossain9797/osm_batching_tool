use osm_import_rust::{self, ImportOptions, OsmFileType};
use std::env;
use std::path::Path;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{error, info};

// Include generated protobuf code
pub mod osm_import {
    tonic::include_proto!("osm_import");
}

use osm_import::osm_import_server::{OsmImport, OsmImportServer};
use osm_import::{
    fetch_import_batch_request::ImportType, fetch_import_batch_response::Response as BatchResponse,
    FetchImportBatchRequest, FetchImportBatchResponse, PingRequest, PingResponse,
};

fn get_import_details(import_type: Option<ImportType>) -> Result<ImportOptions, String> {
    match import_type {
        Some(ImportType::FullDate(date)) => {
            if !date.chars().all(|c| c.is_ascii_digit()) || date.len() != 6 {
                Err("date arg invalid (expected ddmmyy)".to_string())
            } else {
                Ok(ImportOptions {
                    osm_file_type: OsmFileType::Full(date.clone()),
                    base_path: "./data/".to_string(),
                })
            }
        }
        Some(ImportType::DeltaAbc(abc)) => {
            if abc.matches('/').count() != 2 || !abc.chars().all(|c| c.is_ascii_digit() || c == '/')
            {
                Err("abc arg invalid (expected AAA/BBB/CCC)".to_string())
            } else {
                Ok(ImportOptions {
                    osm_file_type: OsmFileType::Delta(abc.clone()),
                    base_path: "./data/".to_string(),
                })
            }
        }
        None => Err("import type is unknown".to_string()),
    }
}

async fn try_get_batch_file(batch_file: &str) -> Option<Result<String, String>> {
    match (
        Path::new(&batch_file).exists(),
        tokio::fs::read_to_string(&batch_file).await,
    ) {
        (true, Ok(content)) => {
            info!("✅ Successfully read batch file ({} bytes)", content.len());
            Some(Ok(content))
        }
        (true, Err(e)) => {
            error!("❌ Batch file Exists but failed to read: {e}");
            Some(Err("Failed to read batch file".to_string()))
        }
        (false, _) => {
            info!("⚠️ Batch file does not exist {batch_file}");
            None
        }
    }
}

async fn maybe_start_background_processing(import_options: ImportOptions) {
    let import_lock_file = import_options.get_lock_file();
    if !Path::new(&import_lock_file).exists() {
        info!("🚀 No lock file found - starting background processing");
        tokio::spawn(async move {
            info!("🎯 Background task started");
            if let Err(e) = osm_import_rust::process_osm_import(&import_options).await {
                error!("💥 Background processing failed: {e}");
            } else {
                info!("🎉 Background processing completed successfully");
            }
        });
    } else {
        info!("🔒 Lock file exists - processing already in progress");
    }
}

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

        let import_options = match get_import_details(req.import_type) {
            Ok(details) => details,
            Err(e) => {
                return Ok(Response::new(FetchImportBatchResponse {
                    response: Some(BatchResponse::Error(e)),
                }))
            }
        };

        let batch_file =
            import_options.get_batch_file(&req.element_type, req.batch_number as usize);

        let batches_complete_file = import_options.get_batches_complete_file(&req.element_type);

        let maybe_existing_file = try_get_batch_file(&batch_file).await;

        let (should_attempt_import, response) = maybe_existing_file
            .map(|file_result| {
                (
                    false, //regardless of read success, file exists so no need to import
                    file_result
                        .map(BatchResponse::BatchContent) //map to batch content if read successfully
                        .unwrap_or_else(BatchResponse::Error), //map to error if read failed
                )
            })
            .unwrap_or_else(|| match Path::new(&batches_complete_file).exists() {
                true => (false, BatchResponse::BatchesComplete("".to_string())), //if batches complete file exists, file would never exist so no need to import
                false => (true, BatchResponse::BatchesPending("".to_string())), //if not complete, we should attempt to get the file, so attempt import
            });

        if should_attempt_import {
            maybe_start_background_processing(import_options).await;
        }

        Ok(Response::new(FetchImportBatchResponse {
            response: Some(response),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
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
