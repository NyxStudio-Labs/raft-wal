//! Pure logic functions shared by all WAL backends (sync, tokio, `io_uring`).
//!
//! These functions contain ZERO I/O — they take raw data in and return
//! processed buffers out. Each backend reads data (sync or async), calls
//! these functions, then writes the result.

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use crate::state::LogState;
use crate::wire::{
    active_segment_header, decompress_blocks, parse_entries_with_offsets, serialize_entry,
    strip_segment_header, ENTRY_HEADER_SIZE, SEGMENT_VERSION_COMPRESSED,
};

/// Rewrites a segment file, keeping only entries that satisfy `keep(index)`.
///
/// Takes raw segment data (with or without header), returns new segment
/// data (with the active version header) and per-entry offset map.
///
/// When the `zstd` feature is enabled, the output uses v2 format with
/// a single compressed block. Otherwise, v1 uncompressed format is used.
pub(crate) fn rewrite_segment_keeping(
    raw_data: &[u8],
    keep: impl Fn(u64) -> bool,
) -> (Vec<u8>, Vec<(u64, usize, usize)>) {
    let (ver, entry_data) = strip_segment_header(raw_data);
    // For v2 segments, decompress first to get raw entries
    let decompressed;
    let raw_entries = if ver == SEGMENT_VERSION_COMPRESSED {
        decompressed = decompress_blocks(entry_data);
        &decompressed[..]
    } else {
        entry_data
    };
    let entries = parse_entries_with_offsets(raw_entries);

    // Build raw entry buffer first
    let mut raw_buf = Vec::new();
    let mut offsets = Vec::new();
    let hdr = active_segment_header();
    // We track offsets relative to the final file; for v2 segments the
    // offsets are not meaningful for random access (blocks are compressed),
    // but we still record them for API consistency.
    for (idx, payload, _, _) in &entries {
        if keep(*idx) {
            offsets.push((
                *idx,
                hdr.len() + raw_buf.len(),
                ENTRY_HEADER_SIZE + payload.len(),
            ));
            serialize_entry(&mut raw_buf, *idx, payload);
        }
    }

    let mut buf = Vec::from(hdr.as_slice());
    #[cfg(feature = "zstd")]
    if !raw_buf.is_empty() {
        let compressed = crate::wire::compress_block(&raw_buf);
        buf.extend_from_slice(&compressed);
    }
    #[cfg(not(feature = "zstd"))]
    {
        buf.extend_from_slice(&raw_buf);
    }
    (buf, offsets)
}

/// Builds the rewritten active segment buffer, merging in-memory cached
/// entries with on-disk entries (for evicted entries that are only on disk).
///
/// `existing_disk_entries` should be parsed from the current active segment
/// file (and any unflushed `disk_buf`). This ensures evicted entries aren't
/// lost during rewrite.
///
/// Returns `(first_active_index, segment_buffer_with_header)`.
pub(crate) fn build_active_rewrite(
    state: &LogState,
    sealed_last_index: Option<u64>,
    existing_disk_entries: &[(u64, Vec<u8>, usize, usize)],
) -> (u64, Vec<u8>) {
    let first_active = if state.is_empty() {
        1
    } else {
        sealed_last_index.map_or(state.base_index, |i| i + 1)
    };

    let hdr = active_segment_header();
    let mut raw_entries = Vec::new();
    if !state.is_empty() {
        let last = state.base_index + state.entries.len() as u64 - 1;
        for idx in first_active..=last {
            if let Some(data) = state.get(idx) {
                serialize_entry(&mut raw_entries, idx, data);
            } else if let Some((_, payload, _, _)) =
                existing_disk_entries.iter().find(|(i, _, _, _)| *i == idx)
            {
                serialize_entry(&mut raw_entries, idx, payload);
            }
        }
    }

    let mut buf = Vec::from(hdr.as_slice());
    #[cfg(feature = "zstd")]
    if !raw_entries.is_empty() {
        let compressed = crate::wire::compress_block(&raw_entries);
        buf.extend_from_slice(&compressed);
    }
    #[cfg(not(feature = "zstd"))]
    {
        buf.extend_from_slice(&raw_entries);
    }
    (first_active, buf)
}

/// Parses a raw segment file (with optional header) and returns entries
/// with their payload, byte offset, and size.
///
/// This is a convenience wrapper around `strip_segment_header` +
/// `parse_entries_with_offsets` used by all three backends during recovery
/// and segment rewriting.
///
/// For v2 (zstd-compressed) segments, decompresses all blocks first, then
/// parses the resulting raw entry bytes.
#[allow(clippy::type_complexity)]
pub(crate) fn parse_segment(raw: &[u8]) -> (u8, usize, Vec<(u64, Vec<u8>, usize, usize)>) {
    let (ver, entry_data) = strip_segment_header(raw);
    let header_len = raw.len() - entry_data.len();
    if ver == SEGMENT_VERSION_COMPRESSED {
        let decompressed = decompress_blocks(entry_data);
        let entries = parse_entries_with_offsets(&decompressed);
        (ver, header_len, entries)
    } else {
        let entries = parse_entries_with_offsets(entry_data);
        (ver, header_len, entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{segment_header, serialize_entry, SEGMENT_HEADER_SIZE};

    /// Helper to extract `(index, payload)` pairs from a segment buffer,
    /// handling both v1 and v2 (compressed) formats transparently.
    fn parse_output(buf: &[u8]) -> Vec<(u64, Vec<u8>)> {
        let (_ver, _hdr_len, entries) = parse_segment(buf);
        entries
            .into_iter()
            .map(|(idx, payload, _, _)| (idx, payload))
            .collect()
    }

    fn make_segment(entries: &[(u64, &[u8])]) -> Vec<u8> {
        let hdr = segment_header();
        let mut buf = Vec::from(hdr.as_slice());
        for (idx, data) in entries {
            serialize_entry(&mut buf, *idx, data);
        }
        buf
    }

    fn make_legacy_segment(entries: &[(u64, &[u8])]) -> Vec<u8> {
        let mut buf = Vec::new();
        for (idx, data) in entries {
            serialize_entry(&mut buf, *idx, data);
        }
        buf
    }

    // ====== parse_segment ======

    #[test]
    fn parse_segment_with_header() {
        let raw = make_segment(&[(1, b"a"), (2, b"b"), (3, b"c")]);
        let (ver, hdr_len, entries) = parse_segment(&raw);
        assert_eq!(ver, 1);
        assert_eq!(hdr_len, SEGMENT_HEADER_SIZE);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0, 1);
        assert_eq!(entries[2].1, b"c");
    }

    #[test]
    fn parse_segment_legacy_no_header() {
        let raw = make_legacy_segment(&[(10, b"legacy")]);
        let (ver, hdr_len, entries) = parse_segment(&raw);
        assert_eq!(ver, 0);
        assert_eq!(hdr_len, 0);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, 10);
    }

    #[test]
    fn parse_segment_empty() {
        let (ver, hdr_len, entries) = parse_segment(&[]);
        assert_eq!(ver, 0);
        assert_eq!(hdr_len, 0);
        assert!(entries.is_empty());
    }

    #[test]
    fn parse_segment_header_only() {
        let raw = segment_header().to_vec();
        let (ver, hdr_len, entries) = parse_segment(&raw);
        assert_eq!(ver, 1);
        assert_eq!(hdr_len, SEGMENT_HEADER_SIZE);
        assert!(entries.is_empty());
    }

    // ====== rewrite_segment_keeping ======

    #[test]
    fn rewrite_keep_all() {
        let raw = make_segment(&[(1, b"a"), (2, b"b"), (3, b"c")]);
        let (buf, offsets) = rewrite_segment_keeping(&raw, |_| true);
        let entries = parse_output(&buf);
        assert_eq!(entries.len(), 3);
        assert_eq!(offsets.len(), 3);
    }

    #[test]
    fn rewrite_keep_none() {
        let raw = make_segment(&[(1, b"a"), (2, b"b")]);
        let (buf, offsets) = rewrite_segment_keeping(&raw, |_| false);
        assert_eq!(buf.len(), SEGMENT_HEADER_SIZE); // header only
        assert!(offsets.is_empty());
    }

    #[test]
    fn rewrite_keep_partial_compact() {
        let raw = make_segment(&[(1, b"a"), (2, b"b"), (3, b"c"), (4, b"d")]);
        // Compact: keep only entries > 2
        let (buf, offsets) = rewrite_segment_keeping(&raw, |idx| idx > 2);
        let entries = parse_output(&buf);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, 3);
        assert_eq!(entries[1].0, 4);
        assert_eq!(offsets.len(), 2);
        assert_eq!(offsets[0].0, 3); // index
    }

    #[test]
    fn rewrite_keep_partial_truncate() {
        let raw = make_segment(&[(1, b"a"), (2, b"b"), (3, b"c"), (4, b"d")]);
        // Truncate: keep only entries < 3
        let (buf, offsets) = rewrite_segment_keeping(&raw, |idx| idx < 3);
        let entries = parse_output(&buf);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, 1);
        assert_eq!(entries[1].0, 2);
        assert_eq!(offsets.len(), 2);
    }

    #[test]
    fn rewrite_legacy_segment() {
        let raw = make_legacy_segment(&[(1, b"old"), (2, b"format")]);
        let (buf, offsets) = rewrite_segment_keeping(&raw, |_| true);
        // Output always has RWAL magic
        assert_eq!(&buf[..4], b"RWAL");
        let entries = parse_output(&buf);
        assert_eq!(entries.len(), 2);
        assert_eq!(offsets.len(), 2);
    }

    #[test]
    fn rewrite_empty_segment() {
        let raw = make_segment(&[]);
        let (buf, offsets) = rewrite_segment_keeping(&raw, |_| true);
        assert_eq!(buf.len(), SEGMENT_HEADER_SIZE);
        assert!(offsets.is_empty());
    }

    #[test]
    fn rewrite_offset_map_correctness() {
        let raw = make_segment(&[(10, b"hello"), (20, b"world")]);
        let (buf, offsets) = rewrite_segment_keeping(&raw, |_| true);
        // Verify offsets record correct indices and the segment round-trips
        assert_eq!(offsets.len(), 2);
        assert_eq!(offsets[0].0, 10);
        assert_eq!(offsets[1].0, 20);
        let entries = parse_output(&buf);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (10, b"hello".to_vec()));
        assert_eq!(entries[1], (20, b"world".to_vec()));
    }

    // ====== build_active_rewrite ======

    #[test]
    fn build_active_empty_state() {
        let state = LogState::new();
        let (first, buf) = build_active_rewrite(&state, None, &[]);
        assert_eq!(first, 1);
        assert_eq!(buf.len(), SEGMENT_HEADER_SIZE); // header only
    }

    #[test]
    fn build_active_all_cached() {
        let mut state = LogState::new();
        state.insert(5, b"five");
        state.insert(6, b"six");
        state.insert(7, b"seven");

        let (first, buf) = build_active_rewrite(&state, None, &[]);
        assert_eq!(first, 5);
        let entries = parse_output(&buf);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], (5, b"five".to_vec()));
    }

    #[test]
    fn build_active_with_sealed_last() {
        let mut state = LogState::new();
        state.insert(11, b"eleven");
        state.insert(12, b"twelve");

        // Sealed segment ends at 10, so active starts at 11
        let (first, buf) = build_active_rewrite(&state, Some(10), &[]);
        assert_eq!(first, 11);
        let entries = parse_output(&buf);
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn build_active_evicted_entry_from_disk() {
        let mut state = LogState::new();
        state.insert(1, b"will-evict");
        state.insert(2, b"cached");
        state.insert(3, b"cached-too");
        // Evict oldest entry (index 1) — eviction goes front-to-back
        state.max_cache_entries = 2;
        state.evict_if_needed();

        assert!(state.get(1).is_none(), "entry 1 should be evicted");
        assert!(state.get(2).is_some(), "entry 2 should be cached");

        // Entry 1 is evicted (empty sentinel), but we have it on disk
        let disk_entries = vec![(1u64, b"from-disk".to_vec(), 0usize, 0usize)];

        let (first, buf) = build_active_rewrite(&state, None, &disk_entries);
        assert_eq!(first, 1);
        let entries = parse_output(&buf);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], (1, b"from-disk".to_vec()));
        assert_eq!(entries[1], (2, b"cached".to_vec()));
        assert_eq!(entries[2], (3, b"cached-too".to_vec()));
    }

    #[test]
    fn build_active_zero_length_entry_preserved() {
        let mut state = LogState::new();
        state.insert(1, b"");
        state.insert(2, b"nonempty");

        let (first, buf) = build_active_rewrite(&state, None, &[]);
        assert_eq!(first, 1);
        let entries = parse_output(&buf);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (1, b"".to_vec()));
        assert_eq!(entries[1], (2, b"nonempty".to_vec()));
    }
}
