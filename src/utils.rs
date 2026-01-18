use anyhow::Result;
use flate2::read::GzDecoder;
use reqwest;
use std::path::Path;
use tokio::fs;
use tracing::{error, info};

pub async fn download_file(url: &str, output_path: &str) -> Result<()> {
    use futures_util::StreamExt;
    use tokio::io::AsyncWriteExt;

    if let Some(parent) = Path::new(output_path).parent() {
        fs::create_dir_all(parent).await?;
    }

    let response = reqwest::get(url).await?;
    if !response.status().is_success() {
        anyhow::bail!("Download failed with status: {}", response.status());
    }

    let mut file = tokio::fs::File::create(output_path).await?;
    let mut stream = response.bytes_stream();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
    }

    file.flush().await?;
    Ok(())
}

pub async fn decompress_gz(input_path: &str, output_path: &str) -> Result<()> {
    if Path::new(output_path).exists() {
        return Ok(());
    }

    let gz_data = fs::read(input_path).await?;
    let mut decoder = GzDecoder::new(&gz_data[..]);
    let mut decompressed = Vec::new();

    use std::io::Read;
    decoder.read_to_end(&mut decompressed)?;

    fs::write(output_path, decompressed).await?;
    Ok(())
}

pub async fn convert_pbf_to_xml(pbf_file: &str, xml_file: &str) -> Result<()> {
    let pbf_metadata = fs::metadata(pbf_file).await?;

    if pbf_metadata.len() < 1000 {
        error!(
            "PBF file is suspiciously small ({} bytes) - likely a 404 error page",
            pbf_metadata.len()
        );
        anyhow::bail!("Downloaded PBF file appears to be invalid (too small)");
    }

    let xml_temp_file = format!("{}.temp", xml_file);

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
            fs::rename(&xml_temp_file, xml_file).await?;
            return Ok(());
        }
        Ok(output) => {
            error!(
                "osmium-tool failed with exit code: {:?}",
                output.status.code()
            );
            error!("stderr: {}", String::from_utf8_lossy(&output.stderr));
            error!("stdout: {}", String::from_utf8_lossy(&output.stdout));

            if Path::new(&xml_temp_file).exists() {
                let _ = fs::remove_file(&xml_temp_file).await;
            }
        }
        Err(e) => {
            error!("osmium-tool not available or failed to execute: {}", e);
        }
    }

    anyhow::bail!("PBF to XML conversion failed. Please install osmium-tool: 'sudo apt-get install osmium-tool' or similar for your OS.");
}
