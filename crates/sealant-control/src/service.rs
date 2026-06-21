//! The seam between the transport and the runtime.

use std::future::Future;
use std::sync::Arc;

use sealant_protocol::{
    ChannelId, ControlRequest, ControlResponse, EventEnvelope, ServerMessage, StreamPayload,
};
use tokio::sync::{Mutex, broadcast, mpsc, watch};

/// The per-connection registry of open byte conduits.
///
/// Maps each [`ChannelId`] opened on this connection to the sink that pumps inbound
/// [`StreamPayload`] frames (gateway → far end). Owned by `handle_connection` and shared into
/// [`ConnHandle`] so streaming commands can register their inbound sink at open time and so that
/// dropping the connection tears down every channel (connection-scoped lifetime).
pub type ChannelRegistry =
    Arc<Mutex<std::collections::HashMap<ChannelId, mpsc::Sender<StreamPayload>>>>;

/// A per-connection handle the service uses to open and drive reliable byte channels.
///
/// `out_tx` is the *same* backpressured outbound queue used for responses and events
/// (`OUTBOUND_CAPACITY`): awaiting `out_tx.send(...)` is the flow-control mechanism. `channels` is
/// the connection's [`ChannelRegistry`]; `shutdown` lets long-lived pump tasks observe shutdown.
#[derive(Clone)]
pub struct ConnHandle {
    /// The connection's backpressured outbound queue (responses, events, and stream frames).
    pub out_tx: mpsc::Sender<ServerMessage>,
    /// The connection's channel registry (inbound sinks keyed by [`ChannelId`]).
    pub channels: ChannelRegistry,
    /// Observes daemon shutdown so per-channel pump tasks can exit promptly.
    pub shutdown: watch::Receiver<bool>,
}

impl std::fmt::Debug for ConnHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnHandle").finish_non_exhaustive()
    }
}

impl ConnHandle {
    /// Register a freshly opened channel's inbound sink so reader-routed `Stream` frames reach it.
    pub async fn register_channel(
        &self,
        channel_id: ChannelId,
        inbound: mpsc::Sender<StreamPayload>,
    ) {
        self.channels.lock().await.insert(channel_id, inbound);
    }

    /// Deregister (and drop the sink for) a channel; its pump task observes the closed sender.
    pub async fn deregister_channel(&self, channel_id: &ChannelId) {
        self.channels.lock().await.remove(channel_id);
    }
}

/// A handler the control server dispatches to. Implemented by the runtime composition root.
///
/// `handle_on_connection` returns exactly one response per request (the acknowledgement contract,
/// plan §8.6) but additionally receives this connection's [`ConnHandle`] so streaming commands
/// (`attachSession`/`openForward`/`openSftp`) can register a [`ChannelId`] and pump bytes over the
/// backpressured writer. Long-running telemetry still surfaces via [`Self::subscribe_events`].
pub trait ControlService: Send + Sync + 'static {
    /// Handle one control request with access to this connection's backpressured writer and channel
    /// registry, and produce its single response.
    fn handle_on_connection(
        &self,
        request: ControlRequest,
        conn: &ConnHandle,
    ) -> impl Future<Output = ControlResponse> + Send;

    /// Subscribe to the runtime's telemetry event stream. Each connection gets its own receiver.
    fn subscribe_events(&self) -> broadcast::Receiver<EventEnvelope>;

    /// The configured maximum control-frame size.
    fn max_frame_bytes(&self) -> u32;
}
