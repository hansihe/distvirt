fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::compile_protos(
        &["proto/distvirt/worker/v1/worker_protocol.proto"],
        &["proto/"],
    )?;
    Ok(())
}
