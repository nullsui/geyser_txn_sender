fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = "../../proto";

    tonic_build::configure()
        .build_server(false) // We only need clients
        .build_client(true)
        .out_dir("src/gen")
        .compile_protos(
            &[format!(
                "{proto_root}/sui/rpc/v2/transaction_execution_service.proto"
            )],
            &[proto_root],
        )?;

    println!("cargo:rerun-if-changed={proto_root}");

    Ok(())
}
