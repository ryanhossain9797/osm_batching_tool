fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("../OSMImport/proto/osm_import.proto")?;
    Ok(())
}
