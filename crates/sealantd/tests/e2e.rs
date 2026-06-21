//! End-to-end Phase 1 acceptance tests.
//!
//! `in_process_*` drives the real [`Runtime`] over an in-memory duplex via the control server.
//! `binary_stdio_*` spawns the actual `sealantd` binary in `--stdio` mode and drives it over real
//! pipes, proving the binary wiring and that protocol output never mixes with diagnostics.

use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use sealant_control::{handle_connection, read_frame, write_frame};
use sealant_protocol::{
    AttachMode, AttachSessionArgs, ChannelId, ClientMessage, Command, CommandResult,
    ControlRequest, EventPayload, ExecArgs, Feature, OpenForwardArgs, OpenSessionArgs, RequestId,
    ResponseOutcome, ServerMessage, StreamFrame, StreamKind, StreamPayload,
};
use sealant_runtime_core::{RuntimeConfig, new_runtime_id};
use sealantd::Runtime;
use sealantd::shutdown::ShutdownSignal;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;

const MAX: u32 = 8 * 1024 * 1024;

fn exec_args(executable: &str, args: &[&str]) -> ExecArgs {
    ExecArgs {
        execution_id: None,
        session_id: None,
        executable: executable.to_owned(),
        args: args.iter().map(|s| (*s).to_owned()).collect(),
        cwd: None,
        env: vec![],
        stdin: false,
        timeout_millis: None,
        background: false,
        capture: None,
        graceful_signal: None,
    }
}

async fn send_request<W: AsyncWrite + Unpin>(writer: &mut W, request: ControlRequest) {
    let body = sealant_protocol::encode_client(&ClientMessage::Request(request));
    write_frame(writer, &body, MAX).await.expect("write frame");
}

async fn recv_message<R: AsyncRead + Unpin>(reader: &mut R) -> ServerMessage {
    let body = read_frame(reader, MAX)
        .await
        .expect("read frame")
        .expect("frame present");
    sealant_protocol::decode_server(&body).expect("decode server message")
}

#[tokio::test]
async fn in_process_exec_streams_events_and_reports_exit() {
    let mut config = RuntimeConfig::new(new_runtime_id());
    config.workspace_root = std::env::temp_dir();
    let runtime = Runtime::new(config, Arc::new(ShutdownSignal::new(1000)));
    runtime.mark_healthy();

    let (_sd_tx, sd_rx) = watch::channel(false);
    let (mut client, server) = tokio::io::duplex(1 << 20);
    let (server_read, server_write) = tokio::io::split(server);
    let conn = tokio::spawn(handle_connection(
        runtime.clone(),
        server_read,
        server_write,
        sd_rx,
    ));

    // Health check first.
    send_request(
        &mut client,
        ControlRequest::new(RequestId::new("r1"), Command::RuntimeHealth),
    )
    .await;
    match recv_message(&mut client).await {
        ServerMessage::Response(r) => {
            assert_eq!(r.request_id, RequestId::new("r1"));
            assert!(r.is_ok());
        }
        other => panic!("expected health response, got {other:?}"),
    }

    // Exec a command and stream its lifecycle.
    send_request(
        &mut client,
        ControlRequest::new(
            RequestId::new("r2"),
            Command::Exec(exec_args("/bin/echo", &["hello"])),
        ),
    )
    .await;

    let mut accepted = false;
    let mut stdout = Vec::new();
    let mut exit_code = None;
    let collect = async {
        loop {
            match recv_message(&mut client).await {
                ServerMessage::Response(r) if r.request_id == RequestId::new("r2") => {
                    if let ResponseOutcome::Ok {
                        result: Some(CommandResult::ExecAccepted(_)),
                    } = r.outcome
                    {
                        accepted = true;
                    }
                }
                ServerMessage::Event(e) => match e.payload {
                    EventPayload::IoChunk(chunk) if chunk.stream == StreamKind::Stdout => {
                        if let Some(content) = chunk.content {
                            stdout.extend_from_slice(content.as_slice());
                        }
                    }
                    EventPayload::ProcessExited(p) => {
                        exit_code = p.exit_code;
                        break;
                    }
                    _ => {}
                },
                ServerMessage::Response(_) | ServerMessage::Stream(_) => {}
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(10), collect)
        .await
        .expect("did not hang");

    assert!(accepted, "exec should be acknowledged");
    assert_eq!(stdout, b"hello\n");
    assert_eq!(exit_code, Some(0));

    // Graceful shutdown via command.
    send_request(
        &mut client,
        ControlRequest::new(
            RequestId::new("r3"),
            Command::RuntimeGracefulShutdown {
                grace_millis: Some(200),
            },
        ),
    )
    .await;
    let drain = async {
        loop {
            if let ServerMessage::Response(r) = recv_message(&mut client).await
                && r.request_id == RequestId::new("r3")
            {
                assert!(r.is_ok());
                break;
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), drain)
        .await
        .expect("shutdown ack");

    drop(client);
    let _ = tokio::time::timeout(Duration::from_secs(5), conn).await;
}

#[tokio::test]
async fn binary_stdio_roundtrips_binary_unsafe_output_and_shuts_down() {
    let exe = env!("CARGO_BIN_EXE_sealantd");
    let mut child = tokio::process::Command::new(exe)
        .arg("--stdio")
        .arg("--workspace")
        .arg(std::env::temp_dir())
        .arg("--log-level")
        .arg("off")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sealantd");

    let mut stdin = child.stdin.take().expect("stdin");
    let mut stdout = child.stdout.take().expect("stdout");

    // Emit bytes including NUL and a high byte; assert exact binary round-trip.
    send_request(
        &mut stdin,
        ControlRequest::new(
            RequestId::new("r1"),
            Command::Exec(exec_args("/bin/sh", &["-c", r"printf 'x\000y\377z'"])),
        ),
    )
    .await;

    let mut bytes = Vec::new();
    let mut exit_code = None;
    let collect = async {
        loop {
            match recv_message(&mut stdout).await {
                ServerMessage::Event(e) => match e.payload {
                    EventPayload::IoChunk(chunk) if chunk.stream == StreamKind::Stdout => {
                        if let Some(content) = chunk.content {
                            bytes.extend_from_slice(content.as_slice());
                        }
                    }
                    EventPayload::ProcessExited(p) => {
                        exit_code = p.exit_code;
                        break;
                    }
                    _ => {}
                },
                ServerMessage::Response(_) | ServerMessage::Stream(_) => {}
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(10), collect)
        .await
        .expect("did not hang");

    assert_eq!(bytes, vec![b'x', 0x00, b'y', 0xff, b'z']);
    assert_eq!(exit_code, Some(0));

    // Closing stdin ends the stdio session, which triggers a graceful shutdown.
    drop(stdin);
    let status = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("daemon exits")
        .expect("wait");
    assert!(status.success());
}

async fn read_pty_until<R: AsyncRead + Unpin>(reader: &mut R, needle: &str) -> bool {
    let mut acc = String::new();
    let scan = async {
        loop {
            if let ServerMessage::Event(e) = recv_message(reader).await
                && let EventPayload::IoChunk(c) = &e.payload
                && c.stream == StreamKind::PtyOutput
                && let Some(content) = &c.content
            {
                acc.push_str(&String::from_utf8_lossy(content.as_slice()));
                if acc.contains(needle) {
                    return true;
                }
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(8), scan)
        .await
        .unwrap_or(false)
}

#[tokio::test]
async fn in_process_session_open_write_resize_close() {
    let mut config = RuntimeConfig::new(new_runtime_id());
    config.workspace_root = std::env::temp_dir();
    config.default_shell = "/bin/sh".to_owned();
    let runtime = Runtime::new(config, Arc::new(ShutdownSignal::new(1000)));
    runtime.mark_healthy();

    let (_sd_tx, sd_rx) = watch::channel(false);
    let (mut client, server) = tokio::io::duplex(1 << 20);
    let (server_read, server_write) = tokio::io::split(server);
    let conn = tokio::spawn(handle_connection(
        runtime.clone(),
        server_read,
        server_write,
        sd_rx,
    ));

    // Open an interactive session.
    send_request(
        &mut client,
        ControlRequest::new(
            RequestId::new("s1"),
            Command::OpenSession(sealant_protocol::OpenSessionArgs {
                execution_id: None,
                shell: None,
                args: vec![],
                cwd: None,
                env: vec![],
                cols: 80,
                rows: 24,
                term: None,
            }),
        ),
    )
    .await;
    let session_id = {
        let find = async {
            loop {
                if let ServerMessage::Response(r) = recv_message(&mut client).await
                    && r.request_id == RequestId::new("s1")
                {
                    match r.outcome {
                        ResponseOutcome::Ok {
                            result: Some(CommandResult::SessionOpened(s)),
                        } => return s.session_id,
                        other => panic!("expected SessionOpened, got {other:?}"),
                    }
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(5), find)
            .await
            .expect("session opened")
    };

    let type_line = |id: sealant_protocol::SessionId| {
        Command::WriteStdin(sealant_protocol::WriteStdinArgs {
            process_id: None,
            session_id: Some(id),
            data: sealant_protocol::Base64Bytes::new(b"stty size\n".to_vec()),
        })
    };

    send_request(
        &mut client,
        ControlRequest::new(RequestId::new("s2"), type_line(session_id.clone())),
    )
    .await;
    assert!(read_pty_until(&mut client, "24 80").await, "initial size");

    // Resize and confirm the session sees the new dimensions.
    send_request(
        &mut client,
        ControlRequest::new(
            RequestId::new("s3"),
            Command::ResizePty {
                session_id: session_id.clone(),
                cols: 132,
                rows: 50,
            },
        ),
    )
    .await;
    send_request(
        &mut client,
        ControlRequest::new(RequestId::new("s4"), type_line(session_id.clone())),
    )
    .await;
    assert!(read_pty_until(&mut client, "50 132").await, "resized size");

    // Close the session.
    send_request(
        &mut client,
        ControlRequest::new(
            RequestId::new("s5"),
            Command::CloseSession {
                session_id: session_id.clone(),
            },
        ),
    )
    .await;

    drop(client);
    let _ = tokio::time::timeout(Duration::from_secs(5), conn).await;
}

// ===================== gateway consolidation (§0 / §1.A / §1.B) =====================

/// Send a raw inbound stream frame from the "gateway" to the daemon.
async fn send_stream<W: AsyncWrite + Unpin>(writer: &mut W, frame: StreamFrame) {
    let body = sealant_protocol::encode_client(&ClientMessage::Stream(frame));
    write_frame(writer, &body, MAX)
        .await
        .expect("write stream frame");
}

/// Drive a real Runtime over the control server and return (client, conn join handle).
fn wire_runtime(runtime: Arc<Runtime>) -> (tokio::io::DuplexStream, tokio::task::JoinHandle<()>) {
    let (_sd_tx, sd_rx) = watch::channel(false);
    // Leak the shutdown sender so it lives for the connection (test-only).
    std::mem::forget(_sd_tx);
    let (client, server) = tokio::io::duplex(1 << 20);
    let (server_read, server_write) = tokio::io::split(server);
    let conn = tokio::spawn(handle_connection(runtime, server_read, server_write, sd_rx));
    (client, conn)
}

async fn open_session(
    client: &mut tokio::io::DuplexStream,
    rid: &str,
) -> sealant_protocol::SessionId {
    send_request(
        client,
        ControlRequest::new(
            RequestId::new(rid),
            Command::OpenSession(OpenSessionArgs {
                execution_id: None,
                shell: Some("/bin/sh".to_owned()),
                args: vec![],
                cwd: None,
                env: vec![],
                cols: 80,
                rows: 24,
                term: None,
            }),
        ),
    )
    .await;
    let want = RequestId::new(rid);
    let find = async {
        loop {
            if let ServerMessage::Response(r) = recv_message(client).await
                && r.request_id == want
                && let ResponseOutcome::Ok {
                    result: Some(CommandResult::SessionOpened(s)),
                } = r.outcome
            {
                return s.session_id;
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), find)
        .await
        .expect("session opened")
}

/// §1.A end-to-end over the control socket: openSession → attachSession → reliable StreamFrame::Data
/// carrying PTY output (distinct from the IoChunk telemetry) → StreamEnd on leader exit.
#[tokio::test]
async fn in_process_attach_streams_pty_output_over_channel() {
    let mut config = RuntimeConfig::new(new_runtime_id());
    config.workspace_root = std::env::temp_dir();
    config.default_shell = "/bin/sh".to_owned();
    let runtime = Runtime::new(config, Arc::new(ShutdownSignal::new(1000)));
    runtime.mark_healthy();
    let (mut client, conn) = wire_runtime(runtime.clone());

    let session_id = open_session(&mut client, "o1").await;

    // Attach.
    send_request(
        &mut client,
        ControlRequest::new(
            RequestId::new("a1"),
            Command::AttachSession(AttachSessionArgs {
                session_id: session_id.clone(),
                mode: AttachMode::Interactive,
            }),
        ),
    )
    .await;
    let channel = {
        let find = async {
            loop {
                if let ServerMessage::Response(r) = recv_message(&mut client).await
                    && r.request_id == RequestId::new("a1")
                    && let ResponseOutcome::Ok {
                        result: Some(CommandResult::StreamAttached(s)),
                    } = r.outcome
                {
                    return s.channel_id;
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(5), find)
            .await
            .expect("attached")
    };

    // Type a command into the PTY via writeStdin; its echoed output must arrive on the channel.
    send_request(
        &mut client,
        ControlRequest::new(
            RequestId::new("w1"),
            Command::WriteStdin(sealant_protocol::WriteStdinArgs {
                process_id: None,
                session_id: Some(session_id.clone()),
                data: sealant_protocol::Base64Bytes::new(b"echo CHANNEL_MARKER\n".to_vec()),
            }),
        ),
    )
    .await;

    // Read StreamFrame::Data frames on the attach channel until the marker appears.
    let mut acc = String::new();
    let scan = async {
        loop {
            if let ServerMessage::Stream(frame) = recv_message(&mut client).await
                && frame.channel_id == channel
                && let StreamPayload::Data { data } = frame.payload
            {
                acc.push_str(&String::from_utf8_lossy(data.as_slice()));
                if acc.contains("CHANNEL_MARKER") {
                    return true;
                }
            }
        }
    };
    assert!(
        tokio::time::timeout(Duration::from_secs(8), scan)
            .await
            .unwrap_or(false),
        "attach channel should carry PTY output; got {acc:?}"
    );

    drop(client);
    let _ = tokio::time::timeout(Duration::from_secs(5), conn).await;
}

/// §1.B end-to-end: openForward to a loopback echo server, pump bytes both ways over the channel.
#[tokio::test]
async fn in_process_open_forward_loopback_echo() {
    // Loopback echo server.
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind echo");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut s, _) = listener.accept().await.expect("accept");
        let mut b = [0u8; 1024];
        loop {
            match s.read(&mut b).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if s.write_all(&b[..n]).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    let mut config = RuntimeConfig::new(new_runtime_id());
    config.workspace_root = std::env::temp_dir();
    let runtime = Runtime::new(config, Arc::new(ShutdownSignal::new(1000)));
    runtime.mark_healthy();
    let (mut client, conn) = wire_runtime(runtime.clone());

    // Forwarding is gated behind the networkCollection feature; enable it.
    send_request(
        &mut client,
        ControlRequest::new(
            RequestId::new("f0"),
            Command::SetFeatureState {
                feature: Feature::NetworkCollection,
                enabled: true,
            },
        ),
    )
    .await;
    // Drain the ack.
    loop {
        if let ServerMessage::Response(r) = recv_message(&mut client).await
            && r.request_id == RequestId::new("f0")
        {
            assert!(r.is_ok());
            break;
        }
    }

    // Open the forward.
    send_request(
        &mut client,
        ControlRequest::new(
            RequestId::new("f1"),
            Command::OpenForward(OpenForwardArgs {
                host: "127.0.0.1".to_owned(),
                port: addr.port(),
                execution_id: None,
            }),
        ),
    )
    .await;
    let channel = {
        let find = async {
            loop {
                if let ServerMessage::Response(r) = recv_message(&mut client).await
                    && r.request_id == RequestId::new("f1")
                    && let ResponseOutcome::Ok {
                        result: Some(CommandResult::ForwardOpened(f)),
                    } = r.outcome
                {
                    return f.channel_id;
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(5), find)
            .await
            .expect("forward opened")
    };

    // Send bytes inbound (gateway → socket); the echo must come back as StreamFrame::Data.
    send_stream(
        &mut client,
        StreamFrame::data(channel.clone(), 0, b"PINGPONG".to_vec()),
    )
    .await;

    let mut got = Vec::new();
    let scan = async {
        loop {
            if let ServerMessage::Stream(frame) = recv_message(&mut client).await
                && frame.channel_id == channel
                && let StreamPayload::Data { data } = frame.payload
            {
                got.extend_from_slice(data.as_slice());
                if got.len() >= 8 {
                    return true;
                }
            }
        }
    };
    assert!(
        tokio::time::timeout(Duration::from_secs(8), scan)
            .await
            .unwrap_or(false),
        "forward should echo bytes; got {got:?}"
    );
    assert_eq!(&got, b"PINGPONG");

    drop(client);
    let _ = tokio::time::timeout(Duration::from_secs(5), conn).await;
}

/// §1.B policy gate: openForward must be denied when the networkCollection feature is off.
#[tokio::test]
async fn in_process_open_forward_denied_when_feature_off() {
    let mut config = RuntimeConfig::new(new_runtime_id());
    config.workspace_root = std::env::temp_dir();
    let runtime = Runtime::new(config, Arc::new(ShutdownSignal::new(1000)));
    runtime.mark_healthy();
    let (mut client, conn) = wire_runtime(runtime.clone());

    send_request(
        &mut client,
        ControlRequest::new(
            RequestId::new("d1"),
            Command::OpenForward(OpenForwardArgs {
                host: "127.0.0.1".to_owned(),
                port: 9,
                execution_id: None,
            }),
        ),
    )
    .await;
    let denied = async {
        loop {
            if let ServerMessage::Response(r) = recv_message(&mut client).await
                && r.request_id == RequestId::new("d1")
            {
                return r;
            }
        }
    };
    let r = tokio::time::timeout(Duration::from_secs(5), denied)
        .await
        .expect("response");
    match r.outcome {
        ResponseOutcome::Error { error } => {
            assert_eq!(error.code, sealant_protocol::ControlErrorCode::PolicyDenied);
        }
        other => panic!("expected PolicyDenied, got {other:?}"),
    }

    drop(client);
    let _ = tokio::time::timeout(Duration::from_secs(5), conn).await;
}

/// §0.3 connection-scoped teardown: when the gateway connection drops, the daemon tears down all its
/// channels (the attach reader stops). We assert the session's attachment is cleared after the
/// connection closes — proving channels die with their connection.
#[tokio::test]
async fn in_process_connection_drop_tears_down_attachment() {
    let mut config = RuntimeConfig::new(new_runtime_id());
    config.workspace_root = std::env::temp_dir();
    config.default_shell = "/bin/sh".to_owned();
    let runtime = Runtime::new(config, Arc::new(ShutdownSignal::new(1000)));
    runtime.mark_healthy();
    let (mut client, conn) = wire_runtime(runtime.clone());

    // Open a long-lived session and attach to it.
    let session_id = open_session(&mut client, "t1").await;
    send_request(
        &mut client,
        ControlRequest::new(
            RequestId::new("t2"),
            Command::WriteStdin(sealant_protocol::WriteStdinArgs {
                process_id: None,
                session_id: Some(session_id.clone()),
                data: sealant_protocol::Base64Bytes::new(b"sleep 30\n".to_vec()),
            }),
        ),
    )
    .await;
    send_request(
        &mut client,
        ControlRequest::new(
            RequestId::new("t3"),
            Command::AttachSession(AttachSessionArgs {
                session_id: session_id.clone(),
                mode: AttachMode::Interactive,
            }),
        ),
    )
    .await;
    let _channel: ChannelId = {
        let find = async {
            loop {
                if let ServerMessage::Response(r) = recv_message(&mut client).await
                    && r.request_id == RequestId::new("t3")
                    && let ResponseOutcome::Ok {
                        result: Some(CommandResult::StreamAttached(s)),
                    } = r.outcome
                {
                    return s.channel_id;
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(5), find)
            .await
            .expect("attached")
    };

    // Drop the connection: handle_connection must return, having torn down the connection's
    // channels (its out_tx clones are gone). The capture loop then observes a closed attach sink and
    // clears the attachment on its next chunk; force a chunk by typing into the PTY before drop.
    send_request(
        &mut client,
        ControlRequest::new(
            RequestId::new("t4"),
            Command::WriteStdin(sealant_protocol::WriteStdinArgs {
                process_id: None,
                session_id: Some(session_id.clone()),
                data: sealant_protocol::Base64Bytes::new(b"echo X\n".to_vec()),
            }),
        ),
    )
    .await;
    drop(client);
    tokio::time::timeout(Duration::from_secs(5), conn)
        .await
        .expect("handle_connection returns after gateway disconnect")
        .expect("join");

    // The control server cleared the connection's channel registry on teardown; once the capture
    // loop pushes a chunk to the now-closed attach sink, it clears the session attachment. Drive a
    // little more output and poll for the attachment to clear.
    use sealant_protocol::SessionId;
    let session_id: SessionId = session_id;
    let mut cleared = false;
    for _ in 0..100 {
        // Use the runtime's own session input path to keep producing output.
        let _ = runtime
            .session_runtime()
            .write_input(&session_id, b"echo Y\n")
            .await;
        if runtime.session_runtime().attachment_is_clear(&session_id) {
            cleared = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert!(
        cleared,
        "attachment should clear after the gateway connection drops"
    );
}
