use anyhow::Result;
use flate2::read::GzDecoder;
use reqwest;
use std::path::Path;
use tokio::fs;
use tracing::{error, info};

pub async fn download_file(url: &str, output_path: &str) -> Result<()> {
    use futures_util::StreamExt;
    use tokio::io::AsyncWriteExt;

    info!("Starting download: {} -> {}", url, output_path);

    // Create parent directories
    if let Some(parent) = Path::new(output_path).parent() {
        fs::create_dir_all(parent).await?;
    }

    let response = reqwest::get(url).await?;
    if !response.status().is_success() {
        anyhow::bail!("Download failed with status: {}", response.status());
    }

    // Get file size if available
    let total_size = response.content_length();
    if let Some(size) = total_size {
        info!("File size: {:.2} MB", size as f64 / 1_048_576.0);
    } else {
        info!("File size: unknown");
    }

    let mut file = tokio::fs::File::create(output_path).await?;
    let mut stream = response.bytes_stream();
    let mut downloaded = 0u64;
    let mut last_log_time = std::time::Instant::now();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let chunk_size = chunk.len() as u64;

        file.write_all(&chunk).await?;
        downloaded += chunk_size;

        // Log progress every 5 seconds or every 10MB
        let now = std::time::Instant::now();
        if now.duration_since(last_log_time).as_secs() >= 5 || downloaded % (10 * 1_048_576) == 0 {
            if let Some(total) = total_size {
                let percentage = (downloaded as f64 / total as f64) * 100.0;
                info!(
                    "Download progress: {:.1}% ({:.2}/{:.2} MB)",
                    percentage,
                    downloaded as f64 / 1_048_576.0,
                    total as f64 / 1_048_576.0
                );
            } else {
                info!("Downloaded: {:.2} MB", downloaded as f64 / 1_048_576.0);
            }
            last_log_time = now;
        }
    }

    file.flush().await?;
    info!(
        "Download completed: {} ({:.2} MB)",
        output_path,
        downloaded as f64 / 1_048_576.0
    );
    Ok(())
}

pub async fn decompress_gz(input_path: &str, output_path: &str) -> Result<()> {
    if Path::new(output_path).exists() {
        info!("Decompressed file already exists: {}", output_path);
        return Ok(());
    }

    info!("Decompressing {} to {}", input_path, output_path);

    let gz_data = fs::read(input_path).await?;
    let mut decoder = GzDecoder::new(&gz_data[..]);
    let mut decompressed = Vec::new();

    use std::io::Read;
    decoder.read_to_end(&mut decompressed)?;

    fs::write(output_path, decompressed).await?;
    info!("Successfully decompressed: {}", output_path);
    Ok(())
}

pub async fn convert_pbf_to_xml(pbf_file: &str, xml_file: &str) -> Result<()> {
    info!("🔄 Converting PBF to XML: {} -> {}", pbf_file, xml_file);

    // Check if PBF file exists and has reasonable size
    let pbf_metadata = fs::metadata(pbf_file).await?;
    let file_size_mb = pbf_metadata.len() as f64 / 1_048_576.0;
    info!("📊 PBF file size: {:.2} MB", file_size_mb);

    if pbf_metadata.len() < 1000 {
        error!(
            "❌ PBF file is suspiciously small ({} bytes) - likely a 404 error page",
            pbf_metadata.len()
        );
        anyhow::bail!("Downloaded PBF file appears to be invalid (too small)");
    }

    // Use osmium-tool to convert PBF to XML (matching Python implementation)
    let xml_temp_file = format!("{}.temp", xml_file);
    info!("🔍 Running osmium cat conversion...");

    let osmium_result = tokio::process::Command::new("osmium")
        .args(&[
            "cat",
            pbf_file,
            "-F",
            "osm.pbf",
            "-o",
            &xml_temp_file,
            "-f",
            "osm",
        ])
        .output()
        .await;

    match osmium_result {
        Ok(output) if output.status.success() => {
            info!("✅ Successfully converted PBF to XML using osmium-tool");
            // Move temp file to final location (atomic operation)
            fs::rename(&xml_temp_file, xml_file).await?;
            info!("✅ Moved temp file to final location: {}", xml_file);
            return Ok(());
        }
        Ok(output) => {
            error!(
                "❌ osmium-tool failed with exit code: {:?}",
                output.status.code()
            );
            error!("❌ stderr: {}", String::from_utf8_lossy(&output.stderr));
            error!("❌ stdout: {}", String::from_utf8_lossy(&output.stdout));

            // Clean up temp file if it exists
            if Path::new(&xml_temp_file).exists() {
                let _ = fs::remove_file(&xml_temp_file).await;
            }
        }
        Err(e) => {
            error!("❌ osmium-tool not available or failed to execute: {}", e);
        }
    }

    anyhow::bail!("PBF to XML conversion failed. Please install osmium-tool: 'sudo apt-get install osmium-tool' or similar for your OS.");
}
