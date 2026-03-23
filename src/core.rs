//! Pure logic functions shared by all WAL backends (sync, tokio, io_uring).
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
/// file (and any unflushed disk_buf). This ensures evicted entries aren't
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
            .map(|i| i + 1)
            .unwrap_or(state.base_index)
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
    let (_ver, entry_data) = strip_segment_header(raw);
    let header_len = raw.len() - entry_data.len();
    let entries = parse_entries_with_offsets(entry_data);
    (_ver, header_len, entries)
}
