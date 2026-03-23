//! Pure logic functions shared by all WAL backends (sync, tokio, `io_uring`).
//!
//! These functions contain ZERO I/O — they take raw data in and return
//! processed buffers out. Each backend reads data (sync or async), calls
//! these functions, then writes the result.

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use crate::state::LogState;
use crate::wire::{
    parse_entries_with_offsets, segment_header, serialize_entry, strip_segment_header,
    ENTRY_HEADER_SIZE,
};

/// Rewrites a segment file, keeping only entries that satisfy `keep(index)`.
///
/// Takes raw segment data (with or without header), returns new segment
/// data (always with v1 header) and per-entry offset map.
pub(crate) fn rewrite_segment_keeping(
    raw_data: &[u8],
    keep: impl Fn(u64) -> bool,
) -> (Vec<u8>, Vec<(u64, usize, usize)>) {
    let (_ver, entry_data) = strip_segment_header(raw_data);
    let entries = parse_entries_with_offsets(entry_data);
    let hdr = segment_header();
    let mut buf = Vec::from(hdr.as_slice());
    let mut offsets = Vec::new();
    for (idx, payload, _, _) in &entries {
        if keep(*idx) {
            offsets.push((*idx, buf.len(), ENTRY_HEADER_SIZE + payload.len()));
            serialize_entry(&mut buf, *idx, payload);
        }
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
        sealed_last_index
            .map_or(state.base_index, |i| i + 1)
    };

    let hdr = segment_header();
    let mut buf = Vec::from(hdr.as_slice());
    if !state.is_empty() {
        let last = state.base_index + state.entries.len() as u64 - 1;
        for idx in first_active..=last {
            if let Some(data) = state.get(idx) {
                // Entry is in memory cache
                serialize_entry(&mut buf, idx, data);
            } else if let Some((_, payload, _, _)) = existing_disk_entries
                .iter()
                .find(|(i, _, _, _)| *i == idx)
            {
                // Entry was evicted — recover from disk
                serialize_entry(&mut buf, idx, payload);
            }
        }
    }
    (first_active, buf)
}

/// Parses a raw segment file (with optional header) and returns entries
/// with their payload, byte offset, and size.
///
/// This is a convenience wrapper around `strip_segment_header` +
/// `parse_entries_with_offsets` used by all three backends during recovery
/// and segment rewriting.
#[allow(clippy::type_complexity)]
pub(crate) fn parse_segment(raw: &[u8]) -> (u8, usize, Vec<(u64, Vec<u8>, usize, usize)>) {
    let (ver, entry_data) = strip_segment_header(raw);
    let header_len = raw.len() - entry_data.len();
    let entries = parse_entries_with_offsets(entry_data);
    (ver, header_len, entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{parse_entries, segment_header, serialize_entry, SEGMENT_HEADER_SIZE};

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
        let entries = parse_entries(&buf[SEGMENT_HEADER_SIZE..]);
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
        let entries = parse_entries(&buf[SEGMENT_HEADER_SIZE..]);
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
        let entries = parse_entries(&buf[SEGMENT_HEADER_SIZE..]);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, 1);
        assert_eq!(entries[1].0, 2);
        assert_eq!(offsets.len(), 2);
    }

    #[test]
    fn rewrite_legacy_segment() {
        let raw = make_legacy_segment(&[(1, b"old"), (2, b"format")]);
        let (buf, offsets) = rewrite_segment_keeping(&raw, |_| true);
        // Output always has v1 header
        assert_eq!(&buf[..4], b"RWAL");
        let entries = parse_entries(&buf[SEGMENT_HEADER_SIZE..]);
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
        // Verify each offset points to a valid entry
        for (idx, offset, size) in &offsets {
            let entry_data = &buf[*offset..*offset + *size];
            let parsed = parse_entries(entry_data);
            assert_eq!(parsed.len(), 1);
            assert_eq!(parsed[0].0, *idx);
        }
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
        let entries = parse_entries(&buf[SEGMENT_HEADER_SIZE..]);
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
        let entries = parse_entries(&buf[SEGMENT_HEADER_SIZE..]);
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
        let entries = parse_entries(&buf[SEGMENT_HEADER_SIZE..]);
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
        let entries = parse_entries(&buf[SEGMENT_HEADER_SIZE..]);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (1, b"".to_vec()));
        assert_eq!(entries[1], (2, b"nonempty".to_vec()));
    }
}
