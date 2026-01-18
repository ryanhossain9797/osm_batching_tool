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

    match (
        Path::new(&batch_file_path).exists(),
        tokio::fs::read_to_string(&batch_file_path).await,
    ) {
        (true, Ok(content)) => BatchFileStatus::FileReadSuccessfully(content),
        (true, Err(_)) => {
            error!("Batch file exists but failed to read: {}", batch_file_path);
            BatchFileStatus::FileReadError("Failed to read batch file".to_string())
        }
        (false, _) => {
            if Path::new(&batches_complete_file_path).exists() {
                BatchFileStatus::FileWillNeverExist
            } else {
                BatchFileStatus::FileDoesNotExistYet
            }
        }
    }
}

pub async fn maybe_start_background_processing(import_options: ImportOptions) {
    let import_lock_file = import_options.get_lock_file();
    if !Path::new(&import_lock_file).exists() {
        tokio::spawn(async move {
            info!("🎯 Background task started");
            if let Err(e) = process_osm_import(&import_options).await {
                error!("💥 Background processing failed: {e}");
            } else {
                info!("🎉 Background processing completed successfully");
            }
        });
    }
}

pub async fn process_osm_import(import_options: &ImportOptions) -> Result<()> {
    let import_scope = import_options.get_import_scope();
    let import_dir = import_options.get_import_dir();

    fs::create_dir_all(&import_dir).await?;

    let lock_file_path = import_options.get_lock_file();
    fs::write(&lock_file_path, "locked").await?;

    let result = match import_options.osm_file_type {
        OsmFileType::Full(_) => process_full_import(&import_scope, &import_dir).await,
        OsmFileType::Delta(_) => process_delta_import(&import_scope, &import_dir).await,
    };

    match fs::remove_file(&lock_file_path).await {
        Ok(_) => {}
        Err(e) => warn!("Failed to remove lock file: {}", e),
    }

    result
}

async fn process_full_import(date: &str, import_dir: &str) -> Result<()> {
    let osm_pbf_file = format!("{}/{}.osm.pbf", import_dir, date);
    let osm_xml_file = format!("{}/{}.osm", import_dir, date);

    download_osm_pbf(date, &osm_pbf_file).await?;

    if !Path::new(&osm_xml_file).exists() {
        utils::convert_pbf_to_xml(&osm_pbf_file, &osm_xml_file).await?;
    }

    batch_osm_xml(&osm_xml_file, import_dir, "full", 500).await?;

    Ok(())
}

async fn process_delta_import(abc: &str, import_dir: &str) -> Result<()> {
    let a_b_c = abc.replace("/", "_");
    let osc_gz_file = format!("{}/{}.osc.gz", import_dir, a_b_c);
    let osc_file = format!("{}/{}.osc", import_dir, a_b_c);

    download_osc_gz(abc, &osc_gz_file).await?;

    utils::decompress_gz(&osc_gz_file, &osc_file).await?;

    batch_osm_xml(&osc_file, import_dir, "delta", 1000).await?;

    Ok(())
}

async fn download_osm_pbf(date: &str, output_path: &str) -> Result<()> {
    if Path::new(output_path).exists() {
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
    let batches_dir = format!("{}/batches", import_dir);
    let input_filename = Path::new(input_file).file_name().unwrap().to_str().unwrap();

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
        return Ok(());
    }

    if Path::new(&batches_dir).exists() {
        fs::remove_dir_all(&batches_dir).await?;
    }

    fs::create_dir_all(&batches_dir).await?;
    for element_type in &["node", "way", "relation"] {
        let dir_path = format!("{}/{}", batches_dir, element_type);
        fs::create_dir_all(&dir_path).await?;
    }

    let xml_content = fs::read_to_string(input_file).await?;

    let mut reader = Reader::from_str(&xml_content);
    reader.config_mut().trim_text(true);

    let root_element_info = parse_root_element(&xml_content)?;

    let mut batch_counts = std::collections::HashMap::new();
    let mut current_batches: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();

    for element_type in &["node", "way", "relation"] {
        batch_counts.insert(element_type.to_string(), 0);
        current_batches.insert(element_type.to_string(), Vec::new());
    }

    let mut buf = Vec::new();
    let mut current_element = String::new();
    let mut element_type = String::new();
    let mut in_element = false;
    let mut element_depth = 0;
    let mut delta_container = String::new();
    let mut total_elements_processed = 0;

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

                        current_element.push_str(&format!("<{}", tag_name));
                        for attr in e.attributes() {
                            let attr = attr?;
                            let key = std::str::from_utf8(attr.key.as_ref())?;
                            let value = std::str::from_utf8(&attr.value)?;
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

                            current_element.push_str(&format!("<{}", tag_name));
                            for attr in e.attributes() {
                                let attr = attr?;
                                let key = std::str::from_utf8(attr.key.as_ref())?;
                                let value = std::str::from_utf8(&attr.value)?;
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
                        element_type = tag_name.to_string();
                        current_element.clear();

                        if import_type == "delta" && !delta_container.is_empty() {
                            current_element.push_str(&format!("<{}>\n", delta_container));
                        }

                        current_element.push_str(&format!("<{}", tag_name));
                        for attr in e.attributes() {
                            let attr = attr?;
                            let key = std::str::from_utf8(attr.key.as_ref())?;
                            let value = std::str::from_utf8(&attr.value)?;
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

                        current_batches
                            .get_mut(&element_type)
                            .unwrap()
                            .push(current_element.clone());
                        total_elements_processed += 1;

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
                        if in_element {
                            current_element.push_str(&format!("<{}", tag_name));
                            for attr in e.attributes() {
                                let attr = attr?;
                                let key = std::str::from_utf8(attr.key.as_ref())?;
                                let value = std::str::from_utf8(&attr.value)?;
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

    for element_type in &["node", "way", "relation"] {
        let element_key = element_type.to_string();
        if !current_batches[&element_key].is_empty() {
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
    }

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

    for element in elements {
        content.push_str(element);
        content.push('\n');
    }

    content.push_str(&format!("</{}>\n", root_info.tag));

    fs::write(&temp_path, content).await?;

    fs::rename(&temp_path, &batch_path).await?;

    Ok(())
}

fn parse_root_element(xml_content: &str) -> Result<RootElementInfo> {
    let mut reader = Reader::from_str(xml_content);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();

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
