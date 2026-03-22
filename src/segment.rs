use std::path::{Path, PathBuf};

use crc32c::{crc32c, crc32c_append};

/// Entry wire format: `[u32 crc32c LE][u64 index LE][u32 payload_len LE][payload]`
pub(crate) const ENTRY_HEADER_SIZE: usize = 4 + 8 + 4;

/// Default maximum segment size before rotation (64 MB).
pub(crate) const DEFAULT_MAX_SEGMENT_SIZE: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct SegmentMeta {
    pub path: PathBuf,
    pub first_index: u64,
    pub last_index: u64,
}

/// Generates a segment filename from the first index.
pub(crate) fn segment_path(dir: &Path, first_index: u64) -> PathBuf {
    dir.join(format!("{first_index:020}.seg"))
}

/// Lists all segment files in a directory, sorted by first_index.
pub(crate) fn list_segments(dir: &Path) -> Vec<PathBuf> {
    let mut segs: Vec<PathBuf> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("seg"))
        .collect();
    segs.sort();
    segs
}

/// Serializes a single entry into the buffer.
pub(crate) fn serialize_entry(buf: &mut Vec<u8>, index: u64, entry: &[u8]) {
    let idx_bytes = index.to_le_bytes();
    let len_bytes = (entry.len() as u32).to_le_bytes();

    // CRC covers: index + payload_len + payload (incremental, no alloc)
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
pub(crate) fn parse_entries(data: &[u8]) -> Vec<(u64, Vec<u8>)> {
    let mut entries = Vec::new();
    let mut pos = 0;

    while pos + ENTRY_HEADER_SIZE <= data.len() {
        let crc_stored = u32::from_le_bytes(data[pos..pos + 4].try_into().expect("4 bytes"));
        let index = u64::from_le_bytes(data[pos + 4..pos + 12].try_into().expect("8 bytes"));
        let plen =
            u32::from_le_bytes(data[pos + 12..pos + 16].try_into().expect("4 bytes")) as usize;

        pos += ENTRY_HEADER_SIZE;
        if pos + plen > data.len() {
            break; // incomplete entry (partial write)
        }

        let payload = &data[pos..pos + plen];

        // Verify CRC
        let crc_computed = crc32c(&data[pos - 12..pos + plen]);
        if crc_computed != crc_stored {
            break; // corrupted entry — stop here
        }

        entries.push((index, payload.to_vec()));
        pos += plen;
    }

    entries
}

/// Reads a single entry by index from raw segment bytes.
/// Returns None if not found or corrupted.
pub(crate) fn find_entry_in_data(data: &[u8], target: u64) -> Option<Vec<u8>> {
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

/// Reads a single entry by index from a segment file.
pub(crate) fn read_entry_from_segment(path: &std::path::Path, target: u64) -> Option<Vec<u8>> {
    let data = std::fs::read(path).ok()?;
    find_entry_in_data(&data, target)
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

        // Truncate mid-entry
        buf.truncate(buf.len() - 3);
        let entries = parse_entries(&buf);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, 1);
    }

    #[test]
    fn corrupted_crc() {
        let mut buf = Vec::new();
        serialize_entry(&mut buf, 1, b"hello");
        serialize_entry(&mut buf, 2, b"world");

        // Corrupt the CRC of the second entry
        let second_entry_start = ENTRY_HEADER_SIZE + 5; // first entry size
        buf[second_entry_start] ^= 0xFF;

        let entries = parse_entries(&buf);
        assert_eq!(entries.len(), 1); // only first entry survives
    }

    #[test]
    fn empty_data() {
        assert!(parse_entries(&[]).is_empty());
    }
}
