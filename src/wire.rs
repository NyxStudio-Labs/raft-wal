//! Wire format serialization and parsing (no_std compatible).
//!
//! ## Segment format (v1+)
//!
//! ```text
//! [MAGIC "RWAL" 4 bytes][VERSION u8]  ← segment header (5 bytes)
//! [entry]*                             ← zero or more entries
//! ```
//!
//! ## Entry format
//!
//! ```text
//! [u32 crc32c LE][u64 index LE][u32 payload_len LE][payload]
//! ```
//!
//! Segments created before v0.6 (version 0) have no header; the parser
//! auto-detects this by checking the first 4 bytes against [`SEGMENT_MAGIC`].

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use crate::crc::{crc32c, crc32c_append};

/// Entry header size: 4 (CRC) + 8 (index) + 4 (payload length) = 16 bytes.
pub const ENTRY_HEADER_SIZE: usize = 4 + 8 + 4;

/// Magic bytes at the start of a versioned segment file.
pub const SEGMENT_MAGIC: [u8; 4] = *b"RWAL";

/// Current segment format version.
pub const SEGMENT_VERSION: u8 = 1;

/// Total segment header size: 4 (magic) + 1 (version) = 5 bytes.
pub const SEGMENT_HEADER_SIZE: usize = 5;

/// Returns the segment header bytes for the current version.
pub fn segment_header() -> [u8; SEGMENT_HEADER_SIZE] {
    [
        SEGMENT_MAGIC[0],
        SEGMENT_MAGIC[1],
        SEGMENT_MAGIC[2],
        SEGMENT_MAGIC[3],
        SEGMENT_VERSION,
    ]
}

/// Detects whether `data` starts with a segment header and strips it.
///
/// Returns `(version, entry_data)`:
/// - Versioned segment: `(version, &data[5..])`
/// - Legacy segment (v0): `(0, data)` (unchanged)
pub fn strip_segment_header(data: &[u8]) -> (u8, &[u8]) {
    if data.len() >= SEGMENT_HEADER_SIZE && data[..4] == SEGMENT_MAGIC {
        (data[4], &data[SEGMENT_HEADER_SIZE..])
    } else {
        (0, data)
    }
}

/// Serializes a single entry into the buffer.
pub fn serialize_entry(buf: &mut Vec<u8>, index: u64, entry: &[u8]) {
    let idx_bytes = index.to_le_bytes();
    let len_bytes = (entry.len() as u32).to_le_bytes();

    let crc = crc32c_append(
        crc32c_append(crc32c_append(0, &idx_bytes), &len_bytes),
        entry,
    );

    buf.extend_from_slice(&crc.to_le_bytes());
    buf.extend_from_slice(&idx_bytes);
    buf.extend_from_slice(&len_bytes);
    buf.extend_from_slice(entry);
}

/// Parses entries from raw segment bytes.
/// Stops at the first corrupted or incomplete entry.
pub fn parse_entries(data: &[u8]) -> Vec<(u64, Vec<u8>)> {
    let mut entries = Vec::new();
    let mut pos = 0;

    while pos + ENTRY_HEADER_SIZE <= data.len() {
        let crc_stored = u32::from_le_bytes(data[pos..pos + 4].try_into().expect("4 bytes"));
        let index = u64::from_le_bytes(data[pos + 4..pos + 12].try_into().expect("8 bytes"));
        let plen =
            u32::from_le_bytes(data[pos + 12..pos + 16].try_into().expect("4 bytes")) as usize;

        pos += ENTRY_HEADER_SIZE;
        if pos + plen > data.len() {
            break;
        }

        let payload = &data[pos..pos + plen];

        let crc_computed = crc32c(&data[pos - 12..pos + plen]);
        if crc_computed != crc_stored {
            break;
        }

        entries.push((index, payload.to_vec()));
        pos += plen;
    }

    entries
}

/// Parses entries and records each entry's byte offset and total size.
///
/// Returns `(index, payload, byte_offset, total_size)` for each entry,
/// where `byte_offset` is relative to the start of `data` and
/// `total_size = ENTRY_HEADER_SIZE + payload.len()`.
pub fn parse_entries_with_offsets(data: &[u8]) -> Vec<(u64, Vec<u8>, usize, usize)> {
    let mut entries = Vec::new();
    let mut pos = 0;

    while pos + ENTRY_HEADER_SIZE <= data.len() {
        let entry_start = pos;
        let crc_stored = u32::from_le_bytes(data[pos..pos + 4].try_into().expect("4 bytes"));
        let index = u64::from_le_bytes(data[pos + 4..pos + 12].try_into().expect("8 bytes"));
        let plen =
            u32::from_le_bytes(data[pos + 12..pos + 16].try_into().expect("4 bytes")) as usize;

        pos += ENTRY_HEADER_SIZE;
        if pos + plen > data.len() {
            break;
        }

        let payload = &data[pos..pos + plen];

        let crc_computed = crc32c(&data[pos - 12..pos + plen]);
        if crc_computed != crc_stored {
            break;
        }

        let total_size = ENTRY_HEADER_SIZE + plen;
        entries.push((index, payload.to_vec(), entry_start, total_size));
        pos += plen;
    }

    entries
}

/// Reads a single entry by index from raw segment bytes.
/// Returns None if not found or corrupted.
pub fn find_entry_in_data(data: &[u8], target: u64) -> Option<Vec<u8>> {
    let mut pos = 0;
    while pos + ENTRY_HEADER_SIZE <= data.len() {
        let crc_stored = u32::from_le_bytes(data[pos..pos + 4].try_into().expect("4 bytes"));
        let index = u64::from_le_bytes(data[pos + 4..pos + 12].try_into().expect("8 bytes"));
        let plen =
            u32::from_le_bytes(data[pos + 12..pos + 16].try_into().expect("4 bytes")) as usize;

        pos += ENTRY_HEADER_SIZE;
        if pos + plen > data.len() {
            break;
        }

        let crc_computed = crc32c(&data[pos - 12..pos + plen]);
        if crc_computed != crc_stored {
            break;
        }

        if index == target {
            return Some(data[pos..pos + plen].to_vec());
        }
        pos += plen;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let mut buf = Vec::new();
        serialize_entry(&mut buf, 1, b"hello");
        serialize_entry(&mut buf, 2, b"world");

        let entries = parse_entries(&buf);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (1, b"hello".to_vec()));
        assert_eq!(entries[1], (2, b"world".to_vec()));
    }

    #[test]
    fn truncated_data() {
        let mut buf = Vec::new();
        serialize_entry(&mut buf, 1, b"hello");
        serialize_entry(&mut buf, 2, b"world");
        buf.truncate(buf.len() - 3);
        let entries = parse_entries(&buf);
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn corrupted_crc() {
        let mut buf = Vec::new();
        serialize_entry(&mut buf, 1, b"hello");
        serialize_entry(&mut buf, 2, b"world");
        let second_start = ENTRY_HEADER_SIZE + 5;
        buf[second_start] ^= 0xFF;
        let entries = parse_entries(&buf);
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn find_entry() {
        let mut buf = Vec::new();
        serialize_entry(&mut buf, 1, b"hello");
        serialize_entry(&mut buf, 2, b"world");
        assert_eq!(find_entry_in_data(&buf, 2), Some(b"world".to_vec()));
        assert_eq!(find_entry_in_data(&buf, 3), None);
    }

    #[test]
    fn empty_data() {
        assert!(parse_entries(&[]).is_empty());
    }
}
