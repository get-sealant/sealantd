fn main() {
    // Use a vendored protoc so the build needs no system protobuf-compiler on any platform (ADR-0012).
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc binary");
    // SAFETY: build scripts run single-threaded here; this only points prost-build at our protoc.
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }
    prost_build::Config::new()
        .compile_protos(&["proto/sealant.proto"], &["proto"])
        .expect("compile sealant.proto");
    println!("cargo:rerun-if-changed=proto/sealant.proto");
}
