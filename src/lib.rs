use anyhow::Result;
use quick_xml::events::Event;
use quick_xml::Reader;
use regex::Regex;
use std::path::Path;
use tokio::fs;
use tracing::{error, info, warn};

mod utils;

#[derive(Debug, Clone)]
pub struct FullDate(String);

#[derive(Debug, Clone)]
pub struct DeltaAbc(String);

impl FullDate {
    pub fn new(date: String) -> Result<Self, String> {
        let date_regex = Regex::new(r"^[0-9]{6}$").map_err(|_| "Failed to compile date regex")?;
        if !date_regex.is_match(&date) {
            return Err(format!("Invalid date format: {} (expected ddmmyy)", date));
        }
        Ok(FullDate(date))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl DeltaAbc {
    pub fn new(abc: String) -> Result<Self, String> {
        let abc_regex = Regex::new(r"^[0-9]{3}/[0-9]{3}/[0-9]{3}$")
            .map_err(|_| "Failed to compile ABC regex")?;
        if !abc_regex.is_match(&abc) {
            return Err(format!(
                "Invalid ABC format: {} (expected AAA/BBB/CCC)",
                abc
            ));
        }
        Ok(DeltaAbc(abc))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_underscore(&self) -> String {
        self.0.replace("/", "_")
    }
}

#[derive(Debug)]
pub enum BatchFileStatus {
    FileReadSuccessfully(String),
    FileReadError(String),
    FileDoesNotExistYet,
    FileWillNeverExist,
}

#[derive(Debug, Clone)]
struct RootElementInfo {
    tag: String,
    attributes: std::collections::HashMap<String, String>,
}

pub enum OsmFileType {
    Full(FullDate),
    Delta(DeltaAbc),
}

pub struct ImportOptions {
    pub osm_file_type: OsmFileType,
    pub base_path: String,
}
impl ImportOptions {
    fn get_import_type(&self) -> &str {
        match &self.osm_file_type {
            OsmFileType::Full(_) => "full",
            OsmFileType::Delta(_) => "delta",
        }
    }
    fn get_import_scope(&self) -> String {
        match &self.osm_file_type {
            OsmFileType::Full(date) => date.as_str().to_string(),
            OsmFileType::Delta(abc) => abc.as_underscore(),
        }
    }
    fn get_import_dir(&self) -> String {
        format!(
            "./data/{}/{}",
            self.get_import_type(),
            self.get_import_scope()
        )
    }

    fn get_filename_base(&self) -> String {
        match &self.osm_file_type {
            OsmFileType::Full(_) => format!("{}.osm", self.get_import_scope()),
            OsmFileType::Delta(_) => format!("{}.osc", self.get_import_scope()),
        }
    }

    pub fn get_lock_file(&self) -> String {
        format!("{}/lock", self.get_import_dir())
    }

    pub fn get_batch_file(&self, element_type: &str, batch_number: usize) -> String {
        format!(
            "{}/batches/{}/{}.batch_{:06}.xml",
            self.get_import_dir(),
            element_type,
            self.get_filename_base(),
            batch_number
        )
    }

    pub fn get_batches_complete_file(&self, element_type: &str) -> String {
        format!(
            "{}/batches/{}/{}.batches_complete",
            self.get_import_dir(),
            element_type,
            self.get_filename_base(),
        )
    }
}

pub async fn check_batch_file_status(
    import_options: &ImportOptions,
    element_type: &str,
    batch_number: usize,
) -> BatchFileStatus {
    let batch_file_path = import_options.get_batch_file(element_type, batch_number);
    let batches_complete_file_path = import_options.get_batches_complete_file(element_type);

    // First check if the specific batch file exists
    match (
        Path::new(&batch_file_path).exists(),
        tokio::fs::read_to_string(&batch_file_path).await,
    ) {
        (true, Ok(content)) => {
            info!("✅ Successfully read batch file ({} bytes)", content.len());
            BatchFileStatus::FileReadSuccessfully(content)
        }
        (true, Err(e)) => {
            error!("❌ Batch file exists but failed to read: {e}");
            BatchFileStatus::FileReadError("Failed to read batch file".to_string())
        }
        (false, _) => {
            info!("⚠️ Batch file does not exist: {batch_file_path}");

            // Check if batches are complete for this element type
            if Path::new(&batches_complete_file_path).exists() {
                info!("📋 Batches complete file exists - this batch will never exist");
                BatchFileStatus::FileWillNeverExist
            } else {
                info!("🔄 Batches not complete - should attempt import");
                BatchFileStatus::FileDoesNotExistYet
            }
        }
    }
}

pub async fn maybe_start_background_processing(import_options: ImportOptions) {
    let import_lock_file = import_options.get_lock_file();
    if !Path::new(&import_lock_file).exists() {
        info!("🚀 No lock file found - starting background processing");
        tokio::spawn(async move {
            info!("🎯 Background task started");
            if let Err(e) = process_osm_import(&import_options).await {
                error!("💥 Background processing failed: {e}");
            } else {
                info!("🎉 Background processing completed successfully");
            }
        });
    } else {
        info!("🔒 Lock file exists - processing already in progress");
    }
}

pub async fn process_osm_import(import_options: &ImportOptions) -> Result<()> {
    info!("🔧 Starting OSM import processing");
    let import_scope = import_options.get_import_scope();

    let import_dir = import_options.get_import_dir();

    info!("📁 Creating directories: {}", import_dir);

    fs::create_dir_all(&import_dir).await?;
    info!("✅ Directories created successfully");

    // Create lock file
    let lock_file_path = import_options.get_lock_file();

    info!("🔒 Creating lock file: {}", lock_file_path);
    fs::write(&lock_file_path, "locked").await?;
    info!("✅ Lock file created successfully");

    let result = match import_options.osm_file_type {
        OsmFileType::Full(_) => process_full_import(&import_scope, &import_dir).await,
        OsmFileType::Delta(_) => process_delta_import(&import_scope, &import_dir).await,
    };

    // Clean up lock file
    info!("🧹 Cleaning up lock file: {}", lock_file_path);
    match fs::remove_file(&lock_file_path).await {
        Ok(_) => info!("✅ Lock file removed successfully"),
        Err(e) => warn!("⚠️ Failed to remove lock file: {}", e),
    }

    result
}

async fn process_full_import(date: &str, import_dir: &str) -> Result<()> {
    info!("📅 Processing full import for date: {}", date);

    let osm_pbf_file = format!("{}/{}.osm.pbf", import_dir, date);
    let osm_xml_file = format!("{}/{}.osm", import_dir, date);

    info!("📝 File paths:");
    info!("   PBF file: {}", osm_pbf_file);
    info!("   XML file: {}", osm_xml_file);

    // Download OSM PBF file
    info!("⬇️ Downloading OSM PBF file...");
    download_osm_pbf(date, &osm_pbf_file).await?;
    info!("✅ Downloaded PBF file: {}", osm_pbf_file);

    // Convert PBF to XML using osmium (matching Python implementation)
    info!("🔄 Converting PBF to XML...");
    if !Path::new(&osm_xml_file).exists() {
        utils::convert_pbf_to_xml(&osm_pbf_file, &osm_xml_file).await?;
    } else {
        info!("✅ XML file already exists: {}", osm_xml_file);
    }

    // Process XML and create batches
    info!("🔄 Starting XML batching process...");
    batch_osm_xml(&osm_xml_file, import_dir, "full", 500).await?;
    info!("🎉 Completed batching for {}", osm_xml_file);

    Ok(())
}

async fn process_delta_import(abc: &str, import_dir: &str) -> Result<()> {
    info!("🔄 Processing delta import for: {}", abc);

    let a_b_c = abc.replace("/", "_");
    let osc_gz_file = format!("{}/{}.osc.gz", import_dir, a_b_c);
    let osc_file = format!("{}/{}.osc", import_dir, a_b_c);

    info!("📝 File paths:");
    info!("   OSC.GZ file: {}", osc_gz_file);
    info!("   OSC file: {}", osc_file);

    // Download delta OSC.GZ file
    info!("⬇️ Downloading delta OSC.GZ file...");
    download_osc_gz(abc, &osc_gz_file).await?;
    info!("✅ Downloaded: {}", osc_gz_file);

    // Decompress OSC.GZ file
    info!("📦 Decompressing OSC.GZ file...");
    utils::decompress_gz(&osc_gz_file, &osc_file).await?;
    info!("✅ Decompressed {} to {}", osc_gz_file, osc_file);

    // Process XML and create batches
    info!("🔄 Starting OSC XML batching process...");
    batch_osm_xml(&osc_file, import_dir, "delta", 1000).await?;
    info!("🎉 Completed batching for {}", osc_file);

    Ok(())
}

async fn download_osm_pbf(date: &str, output_path: &str) -> Result<()> {
    if Path::new(output_path).exists() {
        info!("File already exists: {}", output_path);
        return Ok(());
    }

    let url = format!(
        "https://download.geofabrik.de/asia/bangladesh-{}.osm.pbf",
        date
    );
    utils::download_file(&url, output_path).await
}

async fn download_osc_gz(abc: &str, output_path: &str) -> Result<()> {
    if Path::new(output_path).exists() {
        info!("File already exists: {}", output_path);
        return Ok(());
    }

    let url = format!(
        "https://download.geofabrik.de/asia/bangladesh-updates/{}.osc.gz",
        abc
    );
    utils::download_file(&url, output_path).await
}

async fn batch_osm_xml(
    input_file: &str,
    import_dir: &str,
    import_type: &str,
    elements_per_batch: usize,
) -> Result<()> {
    info!("🧩 Starting XML batching process");
    info!("   Input file: {}", input_file);
    info!("   Import dir: {}", import_dir);
    info!("   Import type: {}", import_type);
    info!("   Elements per batch: {}", elements_per_batch);

    let batches_dir = format!("{}/batches", import_dir);
    let input_filename = Path::new(input_file).file_name().unwrap().to_str().unwrap();

    // Check if all element types are already complete
    let mut all_complete = true;
    for element_type in &["node", "way", "relation"] {
        let complete_file = format!(
            "{}/{}/{}.batches_complete",
            batches_dir, element_type, input_filename
        );
        if !Path::new(&complete_file).exists() {
            all_complete = false;
            break;
        }
    }

    if all_complete {
        info!("✅ All batches are already complete - skipping processing");
        return Ok(());
    }

    if Path::new(&batches_dir).exists() {
        fs::remove_dir_all(&batches_dir).await?;
        info!("✅ Removed existing batches directory");
    }

    // Create batch directories
    fs::create_dir_all(&batches_dir).await?;
    for element_type in &["node", "way", "relation"] {
        let dir_path = format!("{}/{}", batches_dir, element_type);
        fs::create_dir_all(&dir_path).await?;
        info!("   Created: {}", dir_path);
    }

    info!("📖 Reading XML file: {}", input_file);
    let xml_content = fs::read_to_string(input_file).await?;

    let mut reader = Reader::from_str(&xml_content);
    reader.config_mut().trim_text(true);

    // Parse root element attributes first
    let root_element_info = parse_root_element(&xml_content)?;
    info!(
        "📋 Root element: {} with {} attributes",
        root_element_info.tag,
        root_element_info.attributes.len()
    );

    let mut batch_counts = std::collections::HashMap::new();
    let mut current_batches: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();

    // Initialize
    info!("🔧 Initializing parsing state...");
    for element_type in &["node", "way", "relation"] {
        batch_counts.insert(element_type.to_string(), 0);
        current_batches.insert(element_type.to_string(), Vec::new());
    }

    let mut buf = Vec::new();
    let mut current_element = String::new();
    let mut element_type = String::new();
    let mut in_element = false;
    let mut element_depth = 0; // Track nesting depth within an element
    let mut delta_container = String::new();
    let mut total_elements_processed = 0;
    let mut last_log_time = std::time::Instant::now();

    info!("🚀 Starting XML parsing...");

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let tag_name = String::from_utf8_lossy(e.local_name().as_ref()).to_string();

                match tag_name.as_str() {
                    "node" | "way" | "relation" => {
                        element_type = tag_name.to_string();
                        in_element = true;
                        element_depth = 1;
                        current_element.clear();

                        if import_type == "delta" && !delta_container.is_empty() {
                            current_element.push_str(&format!("<{}>\n", delta_container));
                        }

                        // Build start tag with all attributes
                        current_element.push_str(&format!("<{}", tag_name));
                        for attr in e.attributes() {
                            let attr = attr?;
                            let key = std::str::from_utf8(attr.key.as_ref())?;
                            let value = std::str::from_utf8(&attr.value)?;
                            // Escape XML attribute value
                            let escaped_value = value
                                .replace("&", "&amp;")
                                .replace("\"", "&quot;")
                                .replace("<", "&lt;")
                                .replace(">", "&gt;");
                            current_element.push_str(&format!(" {}=\"{}\"", key, escaped_value));
                        }

                        current_element.push_str(">");
                    }
                    "create" | "modify" | "delete" if import_type == "delta" => {
                        delta_container = tag_name.to_string();
                    }
                    _ => {
                        if in_element {
                            element_depth += 1;

                            // Handle nested elements (nd, tag, member, etc.)
                            current_element.push_str(&format!("<{}", tag_name));
                            for attr in e.attributes() {
                                let attr = attr?;
                                let key = std::str::from_utf8(attr.key.as_ref())?;
                                let value = std::str::from_utf8(&attr.value)?;
                                // Escape XML attribute value
                                let escaped_value = value
                                    .replace("&", "&amp;")
                                    .replace("\"", "&quot;")
                                    .replace("<", "&lt;")
                                    .replace(">", "&gt;");
                                current_element
                                    .push_str(&format!(" {}=\"{}\"", key, escaped_value));
                            }
                            current_element.push_str(">");
                        }
                    }
                }
            }
            Ok(Event::End(ref e)) => {
                let tag_name = String::from_utf8_lossy(e.local_name().as_ref()).to_string();

                match tag_name.as_str() {
                    "node" | "way" | "relation" => {
                        if in_element && element_depth == 1 {
                            current_element.push_str(&format!("</{}>", tag_name));

                            if import_type == "delta" && !delta_container.is_empty() {
                                current_element.push_str(&format!("\n</{}>", delta_container));
                            }

                            current_batches
                                .get_mut(&element_type)
                                .unwrap()
                                .push(current_element.clone());
                            total_elements_processed += 1;

                            // Log progress every 10,000 elements or every 10 seconds
                            let now = std::time::Instant::now();
                            if total_elements_processed % 10000 == 0
                                || now.duration_since(last_log_time).as_secs() >= 10
                            {
                                info!("📊 Progress: {} elements processed (nodes: {}, ways: {}, relations: {})", 
                                    total_elements_processed,
                                    current_batches["node"].len() + batch_counts["node"] * elements_per_batch,
                                    current_batches["way"].len() + batch_counts["way"] * elements_per_batch,
                                    current_batches["relation"].len() + batch_counts["relation"] * elements_per_batch);
                                last_log_time = now;
                            }

                            // Check if batch is full
                            if current_batches[&element_type].len() >= elements_per_batch {
                                write_batch(
                                    &element_type,
                                    &current_batches[&element_type],
                                    batch_counts[&element_type],
                                    import_dir,
                                    input_file,
                                    import_type,
                                    &root_element_info,
                                )
                                .await?;
                                *batch_counts.get_mut(&element_type).unwrap() += 1;
                                current_batches.get_mut(&element_type).unwrap().clear();
                            }

                            in_element = false;
                            element_depth = 0;
                        } else if in_element {
                            // Handle nested element end tags
                            current_element.push_str(&format!("</{}>", tag_name));
                            element_depth -= 1;
                        }
                    }
                    "create" | "modify" | "delete" if import_type == "delta" => {
                        delta_container.clear();
                    }
                    _ => {
                        if in_element && element_depth > 1 {
                            current_element.push_str(&format!("</{}>", tag_name));
                            element_depth -= 1;
                        }
                    }
                }
            }
            Ok(Event::Empty(ref e)) => {
                let tag_name = String::from_utf8_lossy(e.local_name().as_ref()).to_string();

                match tag_name.as_str() {
                    "node" | "way" | "relation" => {
                        // Handle self-closing elements (primarily nodes)
                        element_type = tag_name.to_string();
                        current_element.clear();

                        if import_type == "delta" && !delta_container.is_empty() {
                            current_element.push_str(&format!("<{}>\n", delta_container));
                        }

                        // Build self-closing element with all attributes
                        current_element.push_str(&format!("<{}", tag_name));
                        for attr in e.attributes() {
                            let attr = attr?;
                            let key = std::str::from_utf8(attr.key.as_ref())?;
                            let value = std::str::from_utf8(&attr.value)?;
                            // Escape XML attribute value
                            let escaped_value = value
                                .replace("&", "&amp;")
                                .replace("\"", "&quot;")
                                .replace("<", "&lt;")
                                .replace(">", "&gt;");
                            current_element.push_str(&format!(" {}=\"{}\"", key, escaped_value));
                        }
                        current_element.push_str("/>");

                        if import_type == "delta" && !delta_container.is_empty() {
                            current_element.push_str(&format!("\n</{}>", delta_container));
                        }

                        // Add to batch (same logic as Event::End)
                        current_batches
                            .get_mut(&element_type)
                            .unwrap()
                            .push(current_element.clone());
                        total_elements_processed += 1;

                        // Log progress every 10,000 elements or every 10 seconds
                        let now = std::time::Instant::now();
                        if total_elements_processed % 10000 == 0
                            || now.duration_since(last_log_time).as_secs() >= 10
                        {
                            info!("📊 Progress: {} elements processed (nodes: {}, ways: {}, relations: {})", 
                                total_elements_processed,
                                current_batches["node"].len() + batch_counts["node"] * elements_per_batch,
                                current_batches["way"].len() + batch_counts["way"] * elements_per_batch,
                                current_batches["relation"].len() + batch_counts["relation"] * elements_per_batch);
                            last_log_time = now;
                        }

                        // Check if batch is full
                        if current_batches[&element_type].len() >= elements_per_batch {
                            write_batch(
                                &element_type,
                                &current_batches[&element_type],
                                batch_counts[&element_type],
                                import_dir,
                                input_file,
                                import_type,
                                &root_element_info,
                            )
                            .await?;
                            *batch_counts.get_mut(&element_type).unwrap() += 1;
                            current_batches.get_mut(&element_type).unwrap().clear();
                        }
                    }
                    _ => {
                        // Handle self-closing tags like <nd ref="123"/> when inside an element
                        if in_element {
                            current_element.push_str(&format!("<{}", tag_name));
                            for attr in e.attributes() {
                                let attr = attr?;
                                let key = std::str::from_utf8(attr.key.as_ref())?;
                                let value = std::str::from_utf8(&attr.value)?;
                                // Escape XML attribute value
                                let escaped_value = value
                                    .replace("&", "&amp;")
                                    .replace("\"", "&quot;")
                                    .replace("<", "&lt;")
                                    .replace(">", "&gt;");
                                current_element
                                    .push_str(&format!(" {}=\"{}\"", key, escaped_value));
                            }
                            current_element.push_str("/>");
                        }
                    }
                }
            }
            Ok(Event::Text(e)) => {
                if in_element {
                    let text = std::str::from_utf8(&e)?;
                    // Escape XML text content
                    let escaped_text = text
                        .replace("&", "&amp;")
                        .replace("<", "&lt;")
                        .replace(">", "&gt;");
                    current_element.push_str(&escaped_text);
                }
            }
            Ok(Event::CData(e)) => {
                if in_element {
                    current_element.push_str("<![CDATA[");
                    current_element.push_str(std::str::from_utf8(&e)?);
                    current_element.push_str("]]>");
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => anyhow::bail!("XML parsing error: {}", e),
            _ => {}
        }
        buf.clear();
    }

    info!("🏁 Parsing completed! Writing remaining elements and finalization...");

    // Write remaining elements
    for element_type in &["node", "way", "relation"] {
        let element_key = element_type.to_string();
        if !current_batches[&element_key].is_empty() {
            info!(
                "💾 Writing final batch for {}: {} elements",
                element_type,
                current_batches[&element_key].len()
            );
            write_batch(
                element_type,
                &current_batches[&element_key],
                batch_counts[&element_key],
                import_dir,
                input_file,
                import_type,
                &root_element_info,
            )
            .await?;
            *batch_counts.get_mut(&element_key).unwrap() += 1;
        }

        // Write completion marker
        let input_filename = Path::new(input_file).file_name().unwrap().to_str().unwrap();
        let completion_file = format!(
            "{}/batches/{}/{}.batches_complete",
            import_dir, element_type, input_filename
        );
        let completion_message = format!(
            "wrote {} batches from {}\n",
            batch_counts[&element_key], input_filename
        );
        fs::write(&completion_file, &completion_message).await?;
        info!(
            "✅ {}: {} batches written",
            element_type, batch_counts[&element_key]
        );
    }

    info!("🎉 XML batching completed successfully!");
    info!("📊 Final statistics:");
    for element_type in &["node", "way", "relation"] {
        let element_key = element_type.to_string();
        info!(
            "   {}: {} batches",
            element_type, batch_counts[&element_key]
        );
    }
    info!("   Total elements processed: {}", total_elements_processed);

    Ok(())
}

async fn write_batch(
    element_type: &str,
    elements: &[String],
    batch_number: usize,
    import_dir: &str,
    input_file: &str,
    _import_type: &str,
    root_info: &RootElementInfo,
) -> Result<()> {
    let input_filename = Path::new(input_file).file_name().unwrap().to_str().unwrap();
    let extension = ".xml";
    let batch_filename = format!("{}.batch_{:06}{}", input_filename, batch_number, extension);
    let batch_path = format!("{}/batches/{}/{}", import_dir, element_type, batch_filename);
    let temp_path = format!("{}.temp", batch_path);

    let mut content = String::new();
    content.push_str("<?xml version='1.0' encoding='UTF-8'?>\n");

    // Build root element with preserved attributes
    content.push_str(&format!("<{}", root_info.tag));
    for (key, value) in &root_info.attributes {
        let escaped_value = value
            .replace("&", "&amp;")
            .replace("\"", "&quot;")
            .replace("<", "&lt;")
            .replace(">", "&gt;");
        content.push_str(&format!(" {}=\"{}\"", key, escaped_value));
    }
    content.push_str(">\n");

    // Add elements
    for element in elements {
        content.push_str(element);
        content.push('\n');
    }

    // Close root element
    content.push_str(&format!("</{}>\n", root_info.tag));

    // Write to temp file first
    fs::write(&temp_path, content).await?;

    // Move to final location
    fs::rename(&temp_path, &batch_path).await?;

    Ok(())
}

fn parse_root_element(xml_content: &str) -> Result<RootElementInfo> {
    let mut reader = Reader::from_str(xml_content);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();

    // Find the root element (osm or osmChange)
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let tag_name = String::from_utf8_lossy(e.local_name().as_ref()).to_string();

                if tag_name == "osm" || tag_name == "osmChange" {
                    let mut attributes = std::collections::HashMap::new();

                    for attr in e.attributes() {
                        let attr = attr?;
                        let key = std::str::from_utf8(attr.key.as_ref())?.to_string();
                        let value = std::str::from_utf8(&attr.value)?.to_string();
                        attributes.insert(key, value);
                    }

                    // Add/update generator attribute to include Rust implementation info
                    let current_generator =
                        attributes.get("generator").cloned().unwrap_or_default();
                    attributes.insert(
                        "generator".to_string(),
                        format!("Chaldal osm-import-rust; {}", current_generator),
                    );

                    return Ok(RootElementInfo {
                        tag: tag_name,
                        attributes,
                    });
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => anyhow::bail!("XML parsing error while finding root element: {}", e),
            _ => {}
        }
        buf.clear();
    }

    anyhow::bail!("Could not find root element (osm or osmChange)")
}
