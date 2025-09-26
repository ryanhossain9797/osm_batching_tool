use osm_import_rust::{
    self, check_batch_file_status, BatchFileStatus, DeltaAbc, FullDate, ImportOptions, OsmFileType,
};
use std::env;
use tonic::{transport::Server, Request, Response, Status};
use tracing::info;

// Include generated protobuf code
pub mod osm_import {
    tonic::include_proto!("osm_import");
}

use osm_import::osm_import_server::{OsmImport, OsmImportServer};
use osm_import::{
    fetch_import_batch_request::ImportType, fetch_import_batch_response::Response as BatchResponse,
    FetchImportBatchRequest, FetchImportBatchResponse, PingRequest, PingResponse,
};

fn get_import_options(import_type: Option<ImportType>) -> Result<ImportOptions, String> {
    match import_type {
        Some(ImportType::FullDate(date)) => {
            let validated_date = FullDate::new(date)?;
            Ok(ImportOptions {
                osm_file_type: OsmFileType::Full(validated_date),
                base_path: "./data/".to_string(),
            })
        }
        Some(ImportType::DeltaAbc(abc)) => {
            let validated_abc = DeltaAbc::new(abc)?;
            Ok(ImportOptions {
                osm_file_type: OsmFileType::Delta(validated_abc),
                base_path: "./data/".to_string(),
            })
        }
        None => Err("import type is unknown".to_string()),
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

        match get_import_options(req.import_type) {
            Err(e) => Ok(Response::new(FetchImportBatchResponse {
                response: Some(BatchResponse::Error(e)),
            })),
            Ok(options) => {
                let batch_status =
                    check_batch_file_status(&options, &req.element_type, req.batch_number as usize)
                        .await;

                let (should_attempt_import, response) = match batch_status {
                    BatchFileStatus::FileReadSuccessfully(content) => {
                        (false, BatchResponse::BatchContent(content))
                    }
                    BatchFileStatus::FileReadError(error) => (false, BatchResponse::Error(error)),
                    BatchFileStatus::FileWillNeverExist => {
                        (false, BatchResponse::BatchesComplete("".to_string()))
                    }
                    BatchFileStatus::FileDoesNotExistYet => {
                        (true, BatchResponse::BatchesPending("".to_string()))
                    }
                };

                if should_attempt_import {
                    osm_import_rust::maybe_start_background_processing(options).await;
                }

                Ok(Response::new(FetchImportBatchResponse {
                    response: Some(response),
                }))
            }
        }
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
