//! Throwaway spike: compare JSON (base64) vs msgpack (native bytes) vs protobuf for a representative
//! sealantd EventEnvelope. Measures encoded size + encode/decode time, apples-to-apples.

use base64::Engine as _;
use prost::Message as _;
use std::hint::black_box;
use std::time::Instant;

mod pb {
    include!(concat!(env!("OUT_DIR"), "/wirebench.rs"));
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct JsonEnvelope {
    schema_version: u32,
    event_id: String,
    runtime_id: String,
    execution_id: Option<String>,
    process_id: Option<String>,
    sequence: u64,
    observed_at: i64,
    monotonic: u64,
    capture_method: String,
    confidence: String,
    event_type: String,
    stream: String,
    byte_count: u64,
    stream_offset: u64,
    content: String, // base64, as sealantd does today
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct BinEnvelope {
    schema_version: u32,
    event_id: String,
    runtime_id: String,
    execution_id: Option<String>,
    process_id: Option<String>,
    sequence: u64,
    observed_at: i64,
    monotonic: u64,
    capture_method: String,
    confidence: String,
    event_type: String,
    stream: String,
    byte_count: u64,
    stream_offset: u64,
    #[serde(with = "serde_bytes")]
    content: Vec<u8>, // native bytes
}

fn build(content: Vec<u8>) -> (JsonEnvelope, BinEnvelope, pb::Envelope) {
    let b64 = base64::engine::general_purpose::STANDARD.encode(&content);
    let json = JsonEnvelope {
        schema_version: 1,
        event_id: "evt_00965034affc27ca_4f".to_string(),
        runtime_id: "rt_00965034affc27ca".to_string(),
        execution_id: Some("run-7f3a2b10".to_string()),
        process_id: Some("proc_00965034_12".to_string()),
        sequence: 4096,
        observed_at: 1_700_000_000_000_000,
        monotonic: 123_456_789,
        capture_method: "pipe".to_string(),
        confidence: "observed".to_string(),
        event_type: "io.chunk".to_string(),
        stream: "stdout".to_string(),
        byte_count: content.len() as u64,
        stream_offset: 1 << 20,
        content: b64,
    };
    let bin = BinEnvelope {
        schema_version: json.schema_version,
        event_id: json.event_id.clone(),
        runtime_id: json.runtime_id.clone(),
        execution_id: json.execution_id.clone(),
        process_id: json.process_id.clone(),
        sequence: json.sequence,
        observed_at: json.observed_at,
        monotonic: json.monotonic,
        capture_method: json.capture_method.clone(),
        confidence: json.confidence.clone(),
        event_type: json.event_type.clone(),
        stream: json.stream.clone(),
        byte_count: json.byte_count,
        stream_offset: json.stream_offset,
        content: content.clone(),
    };
    let pbe = pb::Envelope {
        schema_version: json.schema_version,
        event_id: json.event_id.clone(),
        runtime_id: json.runtime_id.clone(),
        execution_id: json.execution_id.clone(),
        process_id: json.process_id.clone(),
        sequence: json.sequence,
        observed_at: json.observed_at,
        monotonic: json.monotonic,
        capture_method: json.capture_method.clone(),
        confidence: json.confidence.clone(),
        event_type: json.event_type.clone(),
        stream: json.stream.clone(),
        byte_count: json.byte_count,
        stream_offset: json.stream_offset,
        content,
    };
    (json, bin, pbe)
}

fn time_it(iters: u32, mut f: impl FnMut()) -> f64 {
    for _ in 0..(iters / 10).max(1) {
        f();
    }
    let t = Instant::now();
    for _ in 0..iters {
        f();
    }
    t.elapsed().as_nanos() as f64 / f64::from(iters)
}

fn row(label: &str, content_len: usize, iters: u32) {
    let mut content = vec![0u8; content_len];
    for (i, b) in content.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(31).wrapping_add(7);
    }
    let (json, bin, pbe) = build(content);

    let json_bytes = serde_json::to_vec(&json).unwrap();
    let mp_bytes = rmp_serde::to_vec_named(&bin).unwrap();
    let pb_bytes = pbe.encode_to_vec();

    let json_enc = time_it(iters, || {
        black_box(serde_json::to_vec(&json).unwrap());
    });
    let json_dec = time_it(iters, || {
        black_box(serde_json::from_slice::<JsonEnvelope>(&json_bytes).unwrap());
    });
    let mp_enc = time_it(iters, || {
        black_box(rmp_serde::to_vec_named(&bin).unwrap());
    });
    let mp_dec = time_it(iters, || {
        black_box(rmp_serde::from_slice::<BinEnvelope>(&mp_bytes).unwrap());
    });
    let pb_enc = time_it(iters, || {
        black_box(pbe.encode_to_vec());
    });
    let pb_dec = time_it(iters, || {
        black_box(pb::Envelope::decode(&pb_bytes[..]).unwrap());
    });

    println!("\n== {label}  (content = {content_len} bytes) ==");
    println!("{:<10} {:>9} {:>11} {:>11}", "format", "size(B)", "enc(ns)", "dec(ns)");
    println!("{:<10} {:>9} {:>11.0} {:>11.0}", "json", json_bytes.len(), json_enc, json_dec);
    println!("{:<10} {:>9} {:>11.0} {:>11.0}", "msgpack", mp_bytes.len(), mp_enc, mp_dec);
    println!("{:<10} {:>9} {:>11.0} {:>11.0}", "protobuf", pb_bytes.len(), pb_enc, pb_dec);
    println!(
        "  size vs json:  msgpack {:.0}%   protobuf {:.0}%",
        100.0 * mp_bytes.len() as f64 / json_bytes.len() as f64,
        100.0 * pb_bytes.len() as f64 / json_bytes.len() as f64
    );
}

fn main() {
    println!("wire-format spike: JSON(base64) vs msgpack(native) vs protobuf");
    row("small lifecycle event", 0, 300_000);
    row("typical stdout chunk", 4096, 80_000);
    row("large stdout chunk", 65536, 10_000);
}
