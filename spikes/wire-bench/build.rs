fn main() {
    prost_build::compile_protos(&["envelope.proto"], &["."]).expect("compile protos");
    println!("cargo:rerun-if-changed=envelope.proto");
}
