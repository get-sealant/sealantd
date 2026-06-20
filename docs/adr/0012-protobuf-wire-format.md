# ADR-0012: Protobuf wire format (supersedes ADR-0002 encoding)

## Status

Accepted, 2026-06-20. Supersedes the *encoding* decision of
[ADR-0002](0002-wire-schema-and-framing.md). The length-prefixed framing and the
single-schema-source principle from ADR-0002 are retained. **Not yet implemented**
— scheduled as a dedicated protocol-format migration phase before any external,
non-TypeScript SDK ships (see Migration below). The daemon continues on framed JSON
until then.

## Context

ADR-0002 chose framed JSON for developer inspectability, with an explicit documented
path to a binary encoding. Through Phases 0–4 the protocol has exactly two consumers
(the Rust daemon and the TypeScript SDK) and is not yet public. The product owner has
since stated that **being able to offer the SDK in other languages from day 0 is a
goal**.

To decide on evidence rather than opinion, a benchmark (`spikes/wire-bench/`) encoded a
representative `EventEnvelope` three ways — JSON (base64 bytes, as today), MessagePack
(native bytes), and Protobuf — measuring encoded size and encode/decode time on the
same data, same machine, one process.

| message | format | size (B) | encode (ns) | decode (ns) |
|---|---|---:|---:|---:|
| small lifecycle (0 B content) | json | 365 | 380 | 510 |
| | msgpack | 298 | 389 | 444 |
| | protobuf | **135** | **80** | **278** |
| typical stdout chunk (4 KiB) | json | 5832 | 2128 | 1162 |
| | msgpack | 4397 | 497 | 481 |
| | protobuf | **4237** | **149** | 448 |
| large stdout chunk (64 KiB) | json | 87753 | 25809 | 9911 |
| | msgpack | 65841 | 1346 | **1202** |
| | protobuf | **65679** | **874** | 2429 |

Findings:
- JSON is ~33% larger on the high-volume I/O path (base64 inflation) and ~20–30× slower
  to encode large chunks — the firehose path where it matters most (socket bandwidth,
  spool disk, CPU).
- MessagePack and Protobuf are effectively tied on size/speed for large payloads;
  Protobuf is markedly smaller on small lifecycle events (field tags vs JSON field
  names) and slightly faster to encode overall.
- Therefore the size/speed axis does **not** separate the two binary options. The
  deciding factor is the *tooling/contract* axis: a schema-ful IDL (Protobuf) yields
  typed SDKs in any language for free (`buf generate`); MessagePack is schemaless and
  gives other languages bytes but no typed contract.

## Decision

Adopt **Protobuf** as the wire encoding, managed with **Buf** (lint, breaking-change
detection, codegen, optionally ConnectRPC). Keep the existing length-prefixed framing
(`u32` length + body), now wrapping Protobuf bytes instead of JSON. Preserve developer
inspectability with a **debug-only Protobuf→JSON view** (e.g. `sealantctl` decoding to
JSON) rather than a second wire format.

Rationale: Protobuf matches MessagePack on performance and beats it on small-message
size, and is the only option that delivers the day-0 multi-language SDK capability the
product wants. JSON's sole advantage (raw readability) is recovered by the debug view.

## Consequences

Positive:
- ~25% smaller wire/spool on the high-volume path; ~20–30× faster large-chunk encode.
- `buf generate` produces typed clients for Go/Python/Java/etc. with no protocol rework.
- Buf gives rigorous, automated backward/forward-compatibility checks on the schema.

Negative / costs (one-time):
- A `.proto` schema must model today's serde types; the string-tagged enums (`Command`
  `cmd`, `EventPayload` `eventType`, `ClientMessage`/`ServerMessage` `kind`) become
  Protobuf `oneof`s, and unknown future event types decode to an explicit
  unknown/raw-bytes case rather than passing through transparently.
- Build gains a codegen step (Buf/protoc) where there is none today.
- Loss of plain-text wire/spool readability, mitigated by the debug view.
- The Effect-TS SDK generates types from Protobuf and wraps them to match
  `@sealant/api-contracts` Effect Schema conventions (bounded glue; ADR-0010).
- Best-effort `requestId` salvage from a malformed frame (today's `invalid-json`
  correlated error) is weaker; a decode failure yields a generic decode error.

## Alternatives considered

- **Stay on framed JSON.** Rejected for the eventual external-SDK goal: biggest/slowest
  on the high-volume path and no free typed multi-language SDKs.
- **MessagePack (`rmp-serde`).** Tempting because it keeps the Rust serde types as the
  source of truth and is a near one-line codec swap, with a free JSON debug mode from
  the same types. Rejected as the *primary* choice because it is schemaless and does not
  provide the typed cross-language SDKs the product wants; it remains the fallback if the
  multi-language goal is dropped.
- **FlatBuffers / Cap'n Proto.** FlatBuffers' zero-copy/low-memory niche does not match
  our constraints (large blobs already offloaded to artifacts); Cap'n Proto has
  language-maturity gaps. Both rejected.

## Migration (scheduled, not yet done)

A dedicated phase, run before any external SDK ships and after the in-flight feature
phases, because it is concentrated and mechanical:
1. Author `proto/` schemas mirroring `sealant-protocol`; adopt `buf` (lint + breaking).
2. Generate Rust types (`prost`/`tonic`-style) and replace the encode/decode at the ~5
   call sites (`sealant-control` request/response, `sealant-telemetry` spool
   append/replay). Framing and the spool record codec are unchanged (opaque bytes).
3. Generate TS types via Buf; wrap to Effect Schema; keep shared fixtures for contract
   tests; add the `sealantctl` Protobuf→JSON debug view.
4. Bump `schemaVersion`; update protocol docs and the requirements matrix.

Estimated ~1–2 focused days; isolated to the protocol crate, the encode/decode sites,
the TS packages, and the wire-shape assertions in tests.
