fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_file = "../../proto/msgbus/v1/msgbus.proto";
    println!("cargo:rerun-if-changed={proto_file}");
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&[proto_file], &["../../proto"])?;
    Ok(())
}
