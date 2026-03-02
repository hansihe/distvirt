fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/distvirt/client/v1/client.proto")?;
    Ok(())
}
