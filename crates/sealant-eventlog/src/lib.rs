//! Append-only durable spool for at-least-once telemetry delivery and crash recovery (plan §16).
//!
//! Events are serialized to opaque bytes by the telemetry layer and appended here as
//! length-prefixed, CRC32-checked [`record`]s, grouped into rotating segment files. On restart the
//! [`Spool`] replays unacknowledged records; a truncated final record (a crash between write and
//! fsync) is discarded deterministically, and a corrupt record is reported as loss rather than
//! silently dropped.
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]
#![forbid(unsafe_code)]

pub mod record;
pub mod spool;

pub use record::{Record, RecordError};
pub use spool::{FsyncPolicy, ReplayStats, Spool, SpoolConfig, SpoolError};
