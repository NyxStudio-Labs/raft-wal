//! Segment file management (std only).
//!
//! Used by `tokio` and `io-uring` backends. May appear unused when
//! only the `std` feature is enabled.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

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

/// Lists all segment files in a directory, sorted by `first_index`.
pub(crate) fn list_segments(dir: &Path) -> Vec<PathBuf> {
    let mut segs: Vec<PathBuf> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .filter_map(std::result::Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("seg"))
        .collect();
    segs.sort();
    segs
}
