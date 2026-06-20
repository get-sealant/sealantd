fn main() {
    let mut config = prost_build::Config::new();
    // Carry raw bytes as `bytes::Bytes` is optional; default Vec<u8> is fine for our conversions.
    config
        .compile_protos(&["proto/sealant.proto"], &["proto"])
        .expect("compile sealant.proto");
    println!("cargo:rerun-if-changed=proto/sealant.proto");
}
