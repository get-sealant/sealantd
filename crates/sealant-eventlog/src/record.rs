//! On-disk record codec: a length-prefixed, CRC32-checked frame around an opaque payload.
//!
//! Layout (all integers big-endian):
//! ```text
//! magic(4) version(1) flags(1) sequence(8) timestampMicros(8) payloadLen(4) payload(payloadLen) crc32(4)
//! ```
//! The CRC covers the header and payload (everything before the CRC field).

use std::io::{self, Read};

/// Magic marking the start of every record.
pub const MAGIC: [u8; 4] = *b"SLD1";
/// Current record format version.
pub const FORMAT_VERSION: u8 = 1;
/// Fixed header size: magic + version + flags + sequence + timestamp + payloadLen.
pub const HEADER_LEN: usize = 4 + 1 + 1 + 8 + 8 + 4;
const CRC_LEN: usize = 4;

/// A decoded spool record.
#[derive(Clone, PartialEq, Eq)]
pub struct Record {
    /// Execution sequence assigned by the telemetry pipeline.
    pub sequence: u64,
    /// Wall-clock timestamp in microseconds since the Unix epoch.
    pub timestamp_micros: i64,
    /// Opaque serialized event payload.
    pub payload: Vec<u8>,
}

impl core::fmt::Debug for Record {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Record")
            .field("sequence", &self.sequence)
            .field("timestamp_micros", &self.timestamp_micros)
            .field("payload_len", &self.payload.len())
            .finish()
    }
}

/// Total encoded size of a record with the given payload length.
#[must_use]
pub fn encoded_len(payload_len: usize) -> usize {
    HEADER_LEN + payload_len + CRC_LEN
}

/// Errors decoding a record.
#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    /// The record ended before all of its bytes were present (e.g. a crash before fsync).
    #[error("record is truncated")]
    Truncated,
    /// The record did not begin with the expected magic.
    #[error("bad record magic")]
    BadMagic,
    /// The stored CRC did not match the computed CRC.
    #[error("record checksum mismatch")]
    CorruptChecksum,
    /// The declared payload length exceeded the configured maximum.
    #[error("record payload length {len} exceeds maximum {max}")]
    Oversized {
        /// Declared payload length.
        len: u32,
        /// Configured maximum.
        max: u32,
    },
    /// An underlying I/O error.
    #[error(transparent)]
    Io(io::Error),
}

/// Append an encoded record to `out`.
pub fn encode_into(out: &mut Vec<u8>, sequence: u64, timestamp_micros: i64, payload: &[u8]) {
    let start = out.len();
    out.extend_from_slice(&MAGIC);
    out.push(FORMAT_VERSION);
    out.push(0); // flags
    out.extend_from_slice(&sequence.to_be_bytes());
    out.extend_from_slice(&timestamp_micros.to_be_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    let crc = crc32fast::hash(&out[start..]);
    out.extend_from_slice(&crc.to_be_bytes());
}

/// Encode a single record to a fresh buffer.
#[must_use]
pub fn encode(sequence: u64, timestamp_micros: i64, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(encoded_len(payload.len()));
    encode_into(&mut out, sequence, timestamp_micros, payload);
    out
}

/// Read as many bytes as possible into `buf`, returning the count (which is `< buf.len()` only at
/// end of stream).
fn read_full(reader: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        match reader.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(total)
}

/// Read the next record. `Ok(None)` means a clean end of stream at a record boundary.
///
/// # Errors
/// Returns [`RecordError`] for truncated, corrupt, oversized, or mis-magicked records.
pub fn read_record(
    reader: &mut impl Read,
    max_payload_bytes: u32,
) -> Result<Option<Record>, RecordError> {
    let mut header = [0u8; HEADER_LEN];
    let read = read_full(reader, &mut header).map_err(RecordError::Io)?;
    if read == 0 {
        return Ok(None);
    }
    if read < HEADER_LEN {
        return Err(RecordError::Truncated);
    }
    if header[0..4] != MAGIC {
        return Err(RecordError::BadMagic);
    }
    // header[4] = version, header[5] = flags (currently ignored beyond presence).
    let sequence = u64::from_be_bytes(header[6..14].try_into().unwrap_or([0; 8]));
    let timestamp_micros = i64::from_be_bytes(header[14..22].try_into().unwrap_or([0; 8]));
    let payload_len = u32::from_be_bytes(header[22..26].try_into().unwrap_or([0; 4]));
    if payload_len > max_payload_bytes {
        return Err(RecordError::Oversized {
            len: payload_len,
            max: max_payload_bytes,
        });
    }

    let mut payload = vec![0u8; payload_len as usize];
    if read_full(reader, &mut payload).map_err(RecordError::Io)? < payload.len() {
        return Err(RecordError::Truncated);
    }
    let mut crc_buf = [0u8; CRC_LEN];
    if read_full(reader, &mut crc_buf).map_err(RecordError::Io)? < CRC_LEN {
        return Err(RecordError::Truncated);
    }

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&header);
    hasher.update(&payload);
    if hasher.finalize() != u32::from_be_bytes(crc_buf) {
        return Err(RecordError::CorruptChecksum);
    }

    Ok(Some(Record {
        sequence,
        timestamp_micros,
        payload,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        let bytes = encode(42, 1_700_000_000_000_000, b"hello payload");
        assert_eq!(bytes.len(), encoded_len(b"hello payload".len()));
        let mut cursor = io::Cursor::new(bytes);
        let record = read_record(&mut cursor, 4096).expect("ok").expect("some");
        assert_eq!(record.sequence, 42);
        assert_eq!(record.timestamp_micros, 1_700_000_000_000_000);
        assert_eq!(record.payload, b"hello payload");
        // A second read at the boundary is a clean EOF.
        assert!(read_record(&mut cursor, 4096).expect("ok").is_none());
    }

    #[test]
    fn empty_stream_is_clean_eof() {
        let mut cursor = io::Cursor::new(Vec::new());
        assert!(read_record(&mut cursor, 4096).expect("ok").is_none());
    }

    #[test]
    fn truncated_header_is_detected() {
        let bytes = encode(1, 1, b"abc");
        let mut cursor = io::Cursor::new(bytes[..10].to_vec());
        assert!(matches!(
            read_record(&mut cursor, 4096),
            Err(RecordError::Truncated)
        ));
    }

    #[test]
    fn truncated_payload_is_detected() {
        let bytes = encode(1, 1, b"abcdefgh");
        // Drop the final CRC and part of the payload.
        let mut cursor = io::Cursor::new(bytes[..HEADER_LEN + 3].to_vec());
        assert!(matches!(
            read_record(&mut cursor, 4096),
            Err(RecordError::Truncated)
        ));
    }

    #[test]
    fn corrupt_checksum_is_detected() {
        let mut bytes = encode(1, 1, b"abcdefgh");
        // Flip a payload byte (offset within the payload region).
        bytes[HEADER_LEN + 2] ^= 0xff;
        let mut cursor = io::Cursor::new(bytes);
        assert!(matches!(
            read_record(&mut cursor, 4096),
            Err(RecordError::CorruptChecksum)
        ));
    }

    #[test]
    fn oversized_is_rejected_before_alloc() {
        let bytes = encode(1, 1, &vec![0u8; 1000]);
        let mut cursor = io::Cursor::new(bytes);
        assert!(matches!(
            read_record(&mut cursor, 16),
            Err(RecordError::Oversized { max: 16, .. })
        ));
    }

    #[test]
    fn bad_magic_is_detected() {
        let mut bytes = encode(1, 1, b"abc");
        bytes[0] = b'X';
        let mut cursor = io::Cursor::new(bytes);
        assert!(matches!(
            read_record(&mut cursor, 4096),
            Err(RecordError::BadMagic)
        ));
    }
}
