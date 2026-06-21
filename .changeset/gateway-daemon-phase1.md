---
"@sealant/runtime-client": minor
---

sealantd: gateway daemon Phase 1 — reliable byte-conduit channels over the control socket

- §0 enabler: `ChannelId`, `StreamFrame`/`StreamPayload`/`StreamEnd`, `ServerMessage::Stream` + `ClientMessage::Stream` (domain + proto + convert; `StreamPayload::Data` carries raw bytes, never through telemetry redaction), `ConnHandle` + `ControlService::handle_on_connection`, and a per-connection `ChannelId`→sink registry with connection-scoped teardown.
- §1.A: `attachSession`/`detachSession` → a reliable, backpressured per-session PTY output stream (single PTY reader fans out to both the lossy `IoChunk` telemetry and the lossless attach channel), `StreamEnd{exit_code}` on leader exit.
- §1.B: `openForward`/`closeForward` (direct-tcpip) — `TcpStream::connect` from inside the container, two backpressured pumps, gated behind the `networkCollection` feature (`PolicyDenied` on deny).
- §1.C: `openSftp`/`closeSftp` — bridges the standalone in-container `sftp-server` stdio over a channel.
