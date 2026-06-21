---
"@sealant/runtime-client": minor
---

sealantd: eager channel teardown + exec-attach (gateway daemon §1.A)

- BLOCKER fix — eager channel teardown. Previously, when a control connection dropped, an idle `openForward`/`openSftp` whose upstream never wrote left its outbound (far-end→gateway) pump blocked on `read()` forever — it never called `out_tx.send`, so it never observed the closed outbound queue. That leaked the pump task, the socket FD, and the un-reaped `ForwardRuntime`/`SftpRuntime` map entry per disconnect (idle direct-tcpip forwards are the VSCode-Server steady state, so it accumulated unboundedly). The connection now carries a per-`ChannelId` closer registry (`ConnHandle.closers`); each `openForward`/`openSftp`/`attachSession`/exec-attach registers an eager closer that aborts both pumps **and** removes the runtime map entry. On connection teardown the control server drains and invokes every closer, so nothing leaks. PTY attach uses the same eager path.
- exec-attach (`exec{attach:true}` → `ProcessAttached{process_id, channel_id}`). A non-PTY process's combined stdout/stderr is now delivered over a backpressured `StreamFrame` channel exactly like §1.A's session attach — raw bytes (no telemetry redaction/coalescing), a single shared per-channel `seq` across stdout+stderr, terminated by `StreamFrame::End{exit_code}` on process exit. The binding is established atomically at spawn so the initial output burst is never lost. The always-on lossy `IoChunk` telemetry tap keeps running in parallel. This is the reliable path VSCode's non-PTY bootstrap reads from.
