//! An explicit local egress proxy that observes HTTP request metadata and HTTPS `CONNECT` tunnels.
//!
//! Simplifications (honest, documented): plain-HTTP forwarding assumes the origin closes the
//! response (`Connection: close`) and does not stream large request bodies beyond the initial read;
//! these cover the common agent egress shapes. CONNECT bodies are tunneled byte-for-byte and never
//! inspected.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use sealant_protocol::{
    CaptureMethod, Confidence, EventPayload, ExecutionId, NetworkRequest, NetworkScheme,
    NetworkSourceObserved,
};
use sealant_telemetry::{Correlation, EventBus};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const MAX_HEAD_BYTES: usize = 32 * 1024;

/// Shared proxy state: where to publish observations and how to correlate them.
#[derive(Clone)]
pub(crate) struct ProxyContext {
    /// Telemetry bus.
    pub bus: Arc<EventBus>,
    /// Execution to correlate observations with.
    pub execution_id: Option<ExecutionId>,
}

impl ProxyContext {
    fn correlation(&self) -> Correlation {
        Correlation::new().execution(self.execution_id.clone())
    }

    fn emit(&self, payload: EventPayload) {
        self.bus.publish(
            &self.correlation(),
            CaptureMethod::Proxy,
            Confidence::Observed,
            payload,
        );
    }
}

/// Start the proxy on an ephemeral `127.0.0.1` port. Returns the bound address and the accept task.
///
/// # Errors
/// Returns an [`std::io::Error`] if the listener cannot bind.
pub(crate) async fn start_proxy(
    ctx: ProxyContext,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _peer)) => {
                    let ctx = ctx.clone();
                    tokio::spawn(async move {
                        if let Err(error) = handle_connection(stream, &ctx).await {
                            tracing::debug!(%error, "proxy connection ended");
                        }
                    });
                }
                Err(error) => {
                    tracing::warn!(%error, "proxy accept failed");
                    break;
                }
            }
        }
    });
    Ok((addr, handle))
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Read an HTTP message head (up to CRLFCRLF). Returns `(head_without_terminator, leftover_body)`.
async fn read_head(stream: &mut TcpStream) -> std::io::Result<Option<(Vec<u8>, Vec<u8>)>> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        if let Some(pos) = find_subsequence(&buf, b"\r\n\r\n") {
            return Ok(Some((buf[..pos].to_vec(), buf[pos + 4..].to_vec())));
        }
        if buf.len() > MAX_HEAD_BYTES {
            return Ok(None);
        }
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

fn split_host_port(authority: &str, default: u16) -> (String, u16) {
    if let Some((host, port)) = authority.rsplit_once(':')
        && let Ok(port) = port.parse::<u16>()
    {
        return (host.to_owned(), port);
    }
    (authority.to_owned(), default)
}

fn parse_absolute(uri: &str) -> Option<(String, u16, String)> {
    let rest = uri.strip_prefix("http://")?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let (host, port) = split_host_port(authority, 80);
    Some((host, port, path.to_owned()))
}

fn parse_status(head: &[u8]) -> Option<u32> {
    let text = String::from_utf8_lossy(head);
    let first = text.split("\r\n").next()?;
    first.split_whitespace().nth(1)?.parse().ok()
}

fn resolved_ips(stream: &TcpStream) -> Vec<String> {
    stream
        .peer_addr()
        .ok()
        .map(|a| a.ip().to_string())
        .into_iter()
        .collect()
}

async fn handle_connection(mut client: TcpStream, ctx: &ProxyContext) -> std::io::Result<()> {
    let Some((head, leftover)) = read_head(&mut client).await? else {
        return Ok(());
    };
    let head_text = String::from_utf8_lossy(&head);
    let Some(request_line) = head_text.split("\r\n").next() else {
        return Ok(());
    };
    let mut parts = request_line.split_whitespace();
    let (Some(method), Some(target)) = (parts.next(), parts.next()) else {
        return Ok(());
    };

    if method.eq_ignore_ascii_case("CONNECT") {
        handle_connect(client, target, ctx).await
    } else {
        let method = method.to_owned();
        handle_http(client, &method, target, &head, leftover, ctx).await
    }
}

async fn handle_connect(
    mut client: TcpStream,
    target: &str,
    ctx: &ProxyContext,
) -> std::io::Result<()> {
    let (host, port) = split_host_port(target, 443);
    let start = Instant::now();
    let mut server = match TcpStream::connect((host.as_str(), port)).await {
        Ok(stream) => stream,
        Err(error) => {
            let _ = client.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n").await;
            return Err(error);
        }
    };
    let ips = resolved_ips(&server);
    client
        .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
        .await?;

    // copy_bidirectional propagates half-close (shuts down the opposite write half on EOF), so the
    // tunnel terminates cleanly instead of hanging when one side closes.
    let (sent, received) = tokio::io::copy_bidirectional(&mut client, &mut server)
        .await
        .unwrap_or((0, 0));

    ctx.emit(EventPayload::NetworkRequest(NetworkRequest {
        scheme: NetworkScheme::HttpsConnect,
        method: None,
        host: host.clone(),
        port,
        path: None,
        status: None,
        bytes_sent: sent,
        bytes_received: received,
        duration_micros: start.elapsed().as_micros() as u64,
    }));
    ctx.emit(EventPayload::NetworkSourceObserved(NetworkSourceObserved {
        host,
        resolved_ips: ips,
        port,
        scheme: Some(NetworkScheme::HttpsConnect),
        method: None,
        path: None,
        status: None,
    }));
    Ok(())
}

async fn handle_http(
    mut client: TcpStream,
    method: &str,
    target: &str,
    head: &[u8],
    leftover: Vec<u8>,
    ctx: &ProxyContext,
) -> std::io::Result<()> {
    let Some((host, port, path)) = parse_absolute(target) else {
        let _ = client.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n").await;
        return Ok(());
    };
    let start = Instant::now();
    let mut server = match TcpStream::connect((host.as_str(), port)).await {
        Ok(stream) => stream,
        Err(error) => {
            let _ = client.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n").await;
            return Err(error);
        }
    };
    let ips = resolved_ips(&server);

    // Rewrite the proxy (absolute-form) request line to origin-form, keep the headers.
    let head_text = String::from_utf8_lossy(head);
    let headers = head_text.split_once("\r\n").map_or("", |x| x.1);
    let mut request = format!("{method} {path} HTTP/1.1\r\n{headers}\r\n\r\n").into_bytes();
    request.extend_from_slice(&leftover);
    server.write_all(&request).await?;
    let bytes_sent = request.len() as u64;

    let Some((resp_head, resp_leftover)) = read_head(&mut server).await? else {
        return Ok(());
    };
    let status = parse_status(&resp_head);
    client.write_all(&resp_head).await?;
    client.write_all(b"\r\n\r\n").await?;
    client.write_all(&resp_leftover).await?;
    let copied = tokio::io::copy(&mut server, &mut client).await.unwrap_or(0);
    let bytes_received = resp_head.len() as u64 + 4 + resp_leftover.len() as u64 + copied;

    ctx.emit(EventPayload::NetworkRequest(NetworkRequest {
        scheme: NetworkScheme::Http,
        method: Some(method.to_owned()),
        host: host.clone(),
        port,
        path: Some(path.clone()),
        status,
        bytes_sent,
        bytes_received,
        duration_micros: start.elapsed().as_micros() as u64,
    }));
    ctx.emit(EventPayload::NetworkSourceObserved(NetworkSourceObserved {
        host,
        resolved_ips: ips,
        port,
        scheme: Some(NetworkScheme::Http),
        method: Some(method.to_owned()),
        path: Some(path),
        status,
    }));
    Ok(())
}
