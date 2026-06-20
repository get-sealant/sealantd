# sealantd benchmark report

Measured characteristics that informed design decisions and bound runtime cost (plan §22 Phase 8).
These are directional engineering measurements, not marketing numbers; reproduce with the commands
shown.

## Wire format (ADR-0012)

The `spikes/wire-bench/` harness encoded representative control/telemetry messages (an `io.chunk`
with a 4 KiB binary payload, a `process.started`, an `EventEnvelope`) across candidate encodings.

| Encoding | Size vs JSON | Encode speed vs JSON | Binary payloads |
|----------|--------------|----------------------|-----------------|
| JSON (baseline) | 1.00× | 1.00× | base64 (+33% inflation) |
| Protobuf | ~0.66× | ~20–30× faster | native `bytes` |
| MessagePack | ~0.68× | ~20–30× faster | native bytes |

Conclusion: framed JSON was ~33% larger and ~20–30× slower to encode on the high-volume I/O path.
Protobuf was chosen for parity with MessagePack on the wire **plus** typed multi-language SDKs from
one schema via Buf. Reproduce: `cargo run --release --manifest-path spikes/wire-bench/Cargo.toml`.

## Artifact size

Release build, static musl, stripped:

| Target | Linking | Size |
|--------|---------|------|
| `aarch64-unknown-linux-musl` | static (`ldd`: not a dynamic executable) | ~2.7 MiB |

Reproduce: `scripts/build-release.sh`.

## Runtime characteristics (observed)

From `examples/demo.sh` and the e2e suite on a developer machine (debug builds; release is faster):

- Daemon cold start to `healthy` (socket listening, telemetry started): low tens of milliseconds.
- `exec` round trip (request → `execAccepted`) for a trivial process: single-digit milliseconds.
- Telemetry path: bounded queue + broadcast; backpressure drops by priority and is **never silent**
  (every drop is accounted as `telemetry.dropped` + the `droppedEvents` counter).
- I/O capture is binary-safe and chunked at `ioChunkBytes` (default 64 KiB); large/secret content is
  transformed (truncated/redacted) with `transform` metadata, never dropped silently.

## Telemetry durability

With `--spool-dir`, events are CRC32-checked, segment-rotated append records with crash recovery and
replay-on-restart (optimistic broadcast-ack). The spool is bounded by `spoolLimitBytes` and evicts
oldest-first under disk pressure. Covered by `sealant-eventlog` + `sealant-telemetry` tests.
