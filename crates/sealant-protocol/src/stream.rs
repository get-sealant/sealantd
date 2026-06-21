//! Reliable byte-conduit framing for control connections (gateway consolidation §0).
//!
//! A [`StreamFrame`] carries raw bytes (PTY output, TCP-forward payload, SFTP traffic) over the
//! same backpressured outbound queue used for responses and events, addressed by [`ChannelId`].
//! Unlike the lossy telemetry [`crate::IoChunk`] broadcast (which drops on lag), the stream path is
//! ordered and never coalesced/redacted: [`StreamPayload::Data`] is the faithful copy of the bytes.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::bytes::Base64Bytes;
use crate::ids::ChannelId;

/// One frame on a [`ChannelId`] conduit: a chunk of raw bytes, a flow-control credit, or a close.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StreamFrame {
    /// The conduit this frame belongs to.
    pub channel_id: ChannelId,
    /// Per-channel monotonic counter for gap detection only (delivery is already ordered/reliable).
    pub seq: u64,
    /// The frame body.
    pub payload: StreamPayload,
}

/// The body of a [`StreamFrame`].
///
/// The serde representation is a debug-JSON view only; the canonical wire is protobuf (see
/// `convert::stream_frame_to_wire`). `Data` uses a named `data` field (rather than a newtype) so the
/// internally-tagged JSON view can carry the base64 string without serde's newtype-variant limit.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum StreamPayload {
    /// Raw bytes. Never redacted, coalesced, or routed through telemetry transforms.
    Data {
        /// The raw bytes (base64 on the JSON debug view).
        data: Base64Bytes,
    },
    /// Optional credit-based flow control beyond the mpsc depth.
    WindowUpdate {
        /// Additional bytes the peer is willing to receive.
        credits: u64,
    },
    /// Half/full close of the conduit.
    End(StreamEnd),
}

impl StreamPayload {
    /// Construct a [`StreamPayload::Data`] from anything convertible into bytes.
    #[must_use]
    pub fn data(bytes: impl Into<Base64Bytes>) -> Self {
        Self::Data { data: bytes.into() }
    }
}

/// Terminal metadata for a conduit close.
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StreamEnd {
    /// Set when the channel's far end is a process (PTY/sftp/exec) that exited with a code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Set when the far-end process was terminated by a signal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    /// Set on a failure (e.g. a forward connect error) so the peer can surface it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl StreamFrame {
    /// A data frame carrying raw bytes.
    #[must_use]
    pub fn data(channel_id: ChannelId, seq: u64, bytes: impl Into<Base64Bytes>) -> Self {
        Self {
            channel_id,
            seq,
            payload: StreamPayload::data(bytes),
        }
    }

    /// An end-of-stream frame.
    #[must_use]
    pub fn end(channel_id: ChannelId, seq: u64, end: StreamEnd) -> Self {
        Self {
            channel_id,
            seq,
            payload: StreamPayload::End(end),
        }
    }

    /// A window-update (credit) frame.
    #[must_use]
    pub fn window_update(channel_id: ChannelId, seq: u64, credits: u64) -> Self {
        Self {
            channel_id,
            seq,
            payload: StreamPayload::WindowUpdate { credits },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_frame_round_trips_via_serde() {
        let frame = StreamFrame::data(ChannelId::new("chan_1"), 3, vec![0u8, 0xff, b'a']);
        let value = serde_json::to_value(&frame).expect("ser");
        assert_eq!(value["channelId"], "chan_1");
        assert_eq!(value["seq"], 3);
        assert_eq!(value["payload"]["type"], "data");
        let back: StreamFrame = serde_json::from_value(value).expect("de");
        assert_eq!(back, frame);
    }

    #[test]
    fn end_frame_carries_exit_code() {
        let frame = StreamFrame::end(
            ChannelId::new("chan_2"),
            9,
            StreamEnd {
                exit_code: Some(7),
                signal: None,
                error: None,
            },
        );
        let value = serde_json::to_value(&frame).expect("ser");
        assert_eq!(value["payload"]["type"], "end");
        assert_eq!(value["payload"]["exitCode"], 7);
        let back: StreamFrame = serde_json::from_value(value).expect("de");
        assert_eq!(back, frame);
    }
}
