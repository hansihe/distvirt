fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/worker_protocol.capnp")
        .run()
        .expect("capnp schema compilation failed");
}
