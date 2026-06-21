//! Direct-tcpip forwarding: open a TCP connection from inside the container to `host:port` and pump
//! bytes both ways over a reliable [`ChannelId`] conduit (gateway consolidation §1.B).
//!
//! This is a *raw byte conduit*: payload never touches the telemetry `EventBus`. Outbound (socket →
//! gateway) bytes become [`StreamFrame::Data`] on the connection's backpressured `out_tx`; inbound
//! (gateway → socket) [`StreamPayload`] frames arrive on an mpsc fed by the control reader. Either
//! side's EOF/half-close maps to a [`StreamFrame::End`], mirroring `copy_bidirectional` semantics.

use std::collections::HashMap;
use std::sync::Mutex;

use sealant_protocol::{
    ChannelId, ControlError, ControlErrorCode, ExecutionId, ServerMessage, StreamEnd, StreamFrame,
    StreamPayload,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::sync::mpsc;

/// Read-buffer size for the socket→gateway pump (raw conduit; not a recorded stream).
const READ_BUF: usize = 64 * 1024;

/// A live forward: the two pump tasks driving one TCP connection.
#[derive(Debug)]
struct ForwardEntry {
    socket_to_gateway: tokio::task::JoinHandle<()>,
    gateway_to_socket: tokio::task::JoinHandle<()>,
}

impl ForwardEntry {
    fn abort(&self) {
        self.socket_to_gateway.abort();
        self.gateway_to_socket.abort();
    }
}

/// Registry of live forwards, keyed by channel id. Connection-scoped teardown drops the inbound
/// sinks (closing `gateway_to_socket`); [`ForwardRuntime::close`] aborts a single forward eagerly.
#[derive(Debug, Default)]
pub struct ForwardRuntime {
    inner: Mutex<HashMap<ChannelId, ForwardEntry>>,
}

impl ForwardRuntime {
    /// An empty forward runtime.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Open a forward to `host:port` bound to `channel_id`.
    ///
    /// On success returns the inbound sink (gateway → socket) the caller must register in the
    /// connection's channel registry so reader-routed `Stream` frames reach the socket. On connect
    /// failure returns a [`ControlError`] (the caller may also emit a `StreamFrame::End{error}`).
    ///
    /// # Errors
    /// Returns a [`ControlError`] with [`ControlErrorCode::InternalError`] when the TCP connect
    /// fails.
    pub async fn open(
        &self,
        channel_id: ChannelId,
        host: &str,
        port: u16,
        _execution_id: Option<ExecutionId>,
        out_tx: mpsc::Sender<ServerMessage>,
    ) -> Result<mpsc::Sender<StreamPayload>, ControlError> {
        // The exact connect used by the egress proxy (proxy.rs); resolves inside the container.
        let stream = TcpStream::connect((host, port)).await.map_err(|e| {
            ControlError::new(
                ControlErrorCode::InternalError,
                format!("forward connect to {host}:{port} failed: {e}"),
            )
        })?;
        let _ = stream.set_nodelay(true);
        let (read_half, mut write_half) = stream.into_split();

        // Inbound (gateway → socket): bounded so a slow socket backpressures the gateway.
        let (inbound_tx, mut inbound_rx) = mpsc::channel::<StreamPayload>(64);

        // socket → gateway: read raw bytes, push StreamFrame::Data (awaited = backpressure).
        let s2g_channel = channel_id.clone();
        let s2g_out = out_tx.clone();
        let socket_to_gateway = tokio::spawn(async move {
            pump_socket_to_gateway(read_half, s2g_channel, s2g_out).await;
        });

        // gateway → socket: write inbound Data; an inbound End half-closes the write side.
        let gateway_to_socket = tokio::spawn(async move {
            while let Some(payload) = inbound_rx.recv().await {
                match payload {
                    StreamPayload::Data { data } => {
                        if write_half.write_all(data.as_slice()).await.is_err() {
                            break;
                        }
                    }
                    // Flow-control credits are advisory; the mpsc depth already bounds us.
                    StreamPayload::WindowUpdate { .. } => {}
                    StreamPayload::End(_) => {
                        let _ = write_half.shutdown().await;
                        break;
                    }
                }
            }
            let _ = write_half.shutdown().await;
        });

        self.inner.lock().unwrap_or_else(|e| e.into_inner()).insert(
            channel_id,
            ForwardEntry {
                socket_to_gateway,
                gateway_to_socket,
            },
        );
        Ok(inbound_tx)
    }

    /// Close a forward eagerly, aborting both pumps. Idempotent.
    pub fn close(&self, channel_id: &ChannelId) {
        if let Some(entry) = self
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(channel_id)
        {
            entry.abort();
        }
    }

    /// Number of live forwards.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Whether there are no live forwards.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Pump the socket read half to the gateway. On EOF/error, emit a final `StreamFrame::End` so the
/// gateway half-closes its SSH channel; on error the `End` carries the message.
async fn pump_socket_to_gateway(
    mut read_half: OwnedReadHalf,
    channel_id: ChannelId,
    out_tx: mpsc::Sender<ServerMessage>,
) {
    let mut buf = vec![0u8; READ_BUF];
    let mut seq: u64 = 0;
    let mut error: Option<String> = None;
    loop {
        match read_half.read(&mut buf).await {
            Ok(0) => break, // clean EOF / half-close from the far end
            Ok(n) => {
                let frame = StreamFrame::data(channel_id.clone(), seq, &buf[..n]);
                seq = seq.wrapping_add(1);
                if out_tx.send(ServerMessage::Stream(frame)).await.is_err() {
                    return; // connection gone; no point emitting End
                }
            }
            Err(e) => {
                error = Some(e.to_string());
                break;
            }
        }
    }
    let end = StreamFrame::end(
        channel_id,
        u64::MAX,
        StreamEnd {
            exit_code: None,
            signal: None,
            error,
        },
    );
    let _ = out_tx.send(ServerMessage::Stream(end)).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use sealant_protocol::Base64Bytes;
    use tokio::net::TcpListener;

    /// Loopback echo: open a forward to an echo server, send bytes inbound, read them back as
    /// outbound `StreamFrame::Data`, and confirm a clean `End` on far-end close.
    #[tokio::test]
    async fn forward_loopback_echo_round_trips() {
        // Echo server.
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
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

        let (out_tx, mut out_rx) = mpsc::channel::<ServerMessage>(256);
        let rt = ForwardRuntime::new();
        let channel = ChannelId::new("chan_fwd");
        let inbound = rt
            .open(channel.clone(), "127.0.0.1", addr.port(), None, out_tx)
            .await
            .expect("open forward");
        assert_eq!(rt.len(), 1);

        // Send bytes inbound (gateway → socket).
        inbound
            .send(StreamPayload::data(Base64Bytes::new(b"hello".to_vec())))
            .await
            .expect("inbound send");

        // Read the echoed bytes back as outbound StreamFrame::Data.
        let mut got = Vec::new();
        while got.len() < 5 {
            match out_rx.recv().await.expect("out frame") {
                ServerMessage::Stream(StreamFrame {
                    payload: StreamPayload::Data { data },
                    channel_id,
                    ..
                }) => {
                    assert_eq!(channel_id, channel);
                    got.extend_from_slice(data.as_slice());
                }
                other => panic!("expected data frame, got {other:?}"),
            }
        }
        assert_eq!(&got, b"hello");

        // Half-close inbound → echo server sees EOF → far end closes → outbound End.
        inbound
            .send(StreamPayload::End(StreamEnd::default()))
            .await
            .expect("inbound end");
        loop {
            match out_rx.recv().await.expect("out frame") {
                ServerMessage::Stream(StreamFrame {
                    payload: StreamPayload::End(end),
                    ..
                }) => {
                    assert!(end.error.is_none(), "clean close, got {:?}", end.error);
                    break;
                }
                ServerMessage::Stream(StreamFrame {
                    payload: StreamPayload::Data { .. },
                    ..
                }) => {}
                other => panic!("expected end/data, got {other:?}"),
            }
        }

        rt.close(&channel);
        assert!(rt.is_empty());
    }

    #[tokio::test]
    async fn forward_connect_failure_is_an_error() {
        let (out_tx, _out_rx) = mpsc::channel::<ServerMessage>(8);
        let rt = ForwardRuntime::new();
        // Port 1 on loopback should refuse.
        let result = rt
            .open(ChannelId::new("chan_x"), "127.0.0.1", 1, None, out_tx)
            .await;
        assert!(result.is_err(), "connect to 127.0.0.1:1 should fail");
        assert!(rt.is_empty());
    }
}
