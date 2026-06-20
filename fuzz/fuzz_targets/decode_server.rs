#![no_main]
//! Fuzz the untrusted control-protocol `decode_server` path (plan §22 Phase 7).
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Must never panic; a malformed frame yields Err, not a crash.
    let _ = sealant_protocol::decode_server(data);
});
