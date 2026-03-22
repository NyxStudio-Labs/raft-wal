//! # raft-wal
//!
//! A minimal append-only WAL (Write-Ahead Log) optimized for Raft consensus.
//!
//! - **Segment-based storage** — log is split into segment files; `compact()`
//!   deletes old segments without rewriting.
//! - **CRC32C checksums** — HW-accelerated integrity checks on every entry.
//! - **Raft-correct durability** — metadata (term/vote) is always fsynced;
//!   log entries are buffered with opt-in [`RaftWal::sync`].
//! - **Parallel recovery** — segments are read and verified concurrently.
//! - **openraft integration** — enable `openraft-storage` feature for
//!   [`RaftLogStorage`](openraft::storage::RaftLogStorage) implementation.
//!
//! ## Usage
//!
//! ```rust
//! use raft_wal::RaftWal;
//!
//! # let dir = tempfile::tempdir().unwrap();
//! let mut wal = RaftWal::open(dir.path()).unwrap();
//!
//! wal.append(1, b"entry-1").unwrap();
//! wal.append(2, b"entry-2").unwrap();
//!
//! let entries: Vec<_> = wal.iter().collect();
//! assert_eq!(entries.len(), 2);
//!
//! wal.set_meta("vote", b"node-1").unwrap();
//! assert_eq!(wal.get_meta("vote"), Some(b"node-1".as_slice()));
//! ```

#![deny(missing_docs)]
#![deny(clippy::unwrap_used)]

pub(crate) mod segment;
pub(crate) mod state;

#[cfg(feature = "tokio")]
mod tokio;
#[cfg(feature = "tokio")]
pub use self::tokio::AsyncRaftWal;

pub mod impls;

#[cfg(feature = "openraft-storage")]
pub use impls::openraft::OpenRaftLogStorage;

use std::borrow::Cow;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};

use segment::{
    list_segments, parse_entries, read_entry_from_segment, segment_path, serialize_entry,
    SegmentMeta, DEFAULT_MAX_SEGMENT_SIZE,
};
use state::LogState;

/// Errors returned by WAL operations.
#[derive(Debug)]
pub enum WalError {
    /// An I/O error.
    Io(std::io::Error),
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "WAL I/O: {e}"),
        }
    }
}

impl std::error::Error for WalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
        }
    }
}

impl From<std::io::Error> for WalError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// Result type for WAL operations.
pub type Result<T> = std::result::Result<T, WalError>;

/// A borrowed entry yielded by [`RaftWal::iter`] and [`RaftWal::iter_range`].
pub struct Entry<'a> {
    /// The Raft log index.
    pub index: u64,
    /// The entry payload.
    pub data: &'a [u8],
}

/// An append-only WAL optimized for Raft.
///
/// Maintains an in-memory `VecDeque` (O(1) append/lookup) backed by
/// segment files on disk. Each segment has a maximum size (default 64 MB);
/// when full, a new segment is created.
///
/// **Durability guarantees:**
/// - Metadata writes ([`set_meta`](Self::set_meta)) are always fsynced,
///   ensuring Raft's election safety (term/vote must survive crashes).
/// - Log entry writes are buffered and *not* fsynced by default.
///   Call [`sync`](Self::sync) after append if your Raft implementation
///   requires durable entries before acknowledging `AppendEntries`.
///
/// **Integrity:** Each entry is protected by a CRC32C checksum. Corrupted
/// or partial entries at the tail are silently discarded during recovery.
///
/// **Memory:** All entries are held in memory for O(1) reads. Call
/// [`compact`](Self::compact) periodically after snapshots to free memory.
/// Use [`estimated_memory`](Self::estimated_memory) to monitor usage.
pub struct RaftWal {
    state: LogState,
    sealed: Vec<SegmentMeta>,
    active_writer: BufWriter<File>,
    active_meta: SegmentMeta,
    active_bytes: usize,
    max_segment_size: usize,
    dir_path: PathBuf,
    meta_path: PathBuf,
    write_buf: Vec<u8>,
}

impl RaftWal {
    /// Opens or creates a WAL in the given directory.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let dir = data_dir.as_ref();
        std::fs::create_dir_all(dir)?;

        let meta_path = dir.join("meta.bin");
        let mut state = LogState::new();

        // Recover segments — read + parse + CRC verify in parallel.
        // Use N threads (one per core), each processing a chunk of segments.
        let seg_paths = list_segments(dir);
        let num_threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(seg_paths.len().max(1));
        let chunk_size = (seg_paths.len() + num_threads - 1) / num_threads.max(1);

        type SegResult = std::io::Result<(PathBuf, Vec<(u64, Vec<u8>)>)>;
        let parsed: Vec<SegResult> = std::thread::scope(|s| {
            let handles: Vec<_> = seg_paths
                .chunks(chunk_size.max(1))
                .map(|chunk| {
                    let chunk = chunk.to_vec();
                    s.spawn(move || {
                        let mut results = Vec::with_capacity(chunk.len());
                        for path in chunk {
                            let data = std::fs::read(&path)?;
                            results.push((path, parse_entries(&data)));
                        }
                        Ok::<_, std::io::Error>(results)
                    })
                })
                .collect();
            let mut all = Vec::with_capacity(seg_paths.len());
            for h in handles {
                match h.join().expect("segment reader panicked") {
                    Ok(results) => {
                        for r in results {
                            all.push(Ok(r));
                        }
                    }
                    Err(e) => all.push(Err(e)),
                }
            }
            all
        });

        let mut sealed = Vec::new();
        for result in parsed {
            let (path, entries) = result?;
            if entries.is_empty() {
                continue;
            }
            let first_index = entries[0].0;
            let last_index = entries[entries.len() - 1].0;
            for (index, payload) in &entries {
                state.insert(*index, payload);
            }
            sealed.push(SegmentMeta {
                path,
                first_index,
                last_index,
            });
        }

        // Recover meta
        if meta_path.exists() {
            let data = std::fs::read(&meta_path)?;
            state.recover_meta(&data);
        }

        // Determine active segment
        let next_index = state.last_index().map(|i| i + 1).unwrap_or(1);
        let active_path = segment_path(dir, next_index);
        let active_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active_path)?;
        let active_bytes = active_file.metadata()?.len() as usize;

        Ok(Self {
            state,
            sealed,
            active_writer: BufWriter::with_capacity(64 * 1024, active_file),
            active_meta: SegmentMeta {
                path: active_path,
                first_index: next_index,
                last_index: next_index.saturating_sub(1),
            },
            active_bytes,
            max_segment_size: DEFAULT_MAX_SEGMENT_SIZE,
            dir_path: dir.to_path_buf(),
            meta_path,
            write_buf: Vec::with_capacity(4096),
        })
    }

    /// Appends a single log entry.
    pub fn append(&mut self, index: u64, entry: &[u8]) -> Result<()> {
        self.write_buf.clear();
        serialize_entry(&mut self.write_buf, index, entry);
        self.active_writer.write_all(&self.write_buf)?;
        self.active_bytes += self.write_buf.len();
        self.active_meta.last_index = index;
        self.state.insert(index, entry);
        self.state
            .evict_if_needed_until(self.active_meta.first_index);

        if self.active_bytes >= self.max_segment_size {
            self.rotate_segment()?;
        }
        Ok(())
    }

    /// Appends multiple log entries in a single write.
    ///
    /// Accepts any slice of `(u64, V)` where `V: AsRef<[u8]>`, so both
    /// `&[(u64, &[u8])]` and `&[(u64, Vec<u8>)]` work directly.
    pub fn append_batch<V: AsRef<[u8]>>(&mut self, entries: &[(u64, V)]) -> Result<()> {
        self.write_buf.clear();
        for (index, entry) in entries {
            serialize_entry(&mut self.write_buf, *index, entry.as_ref());
        }
        self.active_writer.write_all(&self.write_buf)?;
        self.active_bytes += self.write_buf.len();

        for (index, entry) in entries {
            self.active_meta.last_index = *index;
            self.state.insert(*index, entry.as_ref());
        }
        self.state
            .evict_if_needed_until(self.active_meta.first_index);

        if self.active_bytes >= self.max_segment_size {
            self.rotate_segment()?;
        }
        Ok(())
    }

    /// Iterates over all entries as borrowed [`Entry`] values.
    pub fn iter(&self) -> impl Iterator<Item = Entry<'_>> {
        self.state.iter()
    }

    /// Iterates over entries in the given index range without cloning.
    pub fn iter_range<R: RangeBounds<u64>>(&self, range: R) -> impl Iterator<Item = Entry<'_>> {
        self.state.iter_range(range)
    }

    /// Returns entries within the given index range as owned pairs.
    ///
    /// For zero-copy iteration, prefer [`RaftWal::iter_range`].
    pub fn read_range<R: RangeBounds<u64>>(&self, range: R) -> Vec<(u64, Vec<u8>)> {
        self.state
            .iter_range(range)
            .map(|e| (e.index, e.data.to_vec()))
            .collect()
    }

    /// Returns the entry at the given index.
    ///
    /// Returns `Cow::Borrowed` for cached (in-memory) entries and
    /// `Cow::Owned` for evicted entries read from disk.
    pub fn get(&self, index: u64) -> Option<Cow<'_, [u8]>> {
        // Fast path: check in-memory cache
        if let Some(data) = self.state.get(index) {
            return Some(Cow::Borrowed(data));
        }
        // Slow path: read from disk segment
        self.read_from_disk(index).map(Cow::Owned)
    }

    /// Returns the entry from memory only (no disk fallback).
    /// This is the zero-copy fast path (~1ns).
    pub fn get_cached(&self, index: u64) -> Option<&[u8]> {
        self.state.get(index)
    }

    /// Returns the first (lowest) index in the log.
    pub fn first_index(&self) -> Option<u64> {
        self.state.first_index()
    }

    /// Returns the last (highest) index in the log.
    pub fn last_index(&self) -> Option<u64> {
        self.state.last_index()
    }

    /// Returns the number of entries in the log.
    pub fn len(&self) -> usize {
        self.state.len()
    }

    /// Returns `true` if the log contains no entries.
    pub fn is_empty(&self) -> bool {
        self.state.is_empty()
    }

    /// Estimated memory usage of in-memory entries and metadata in bytes.
    pub fn estimated_memory(&self) -> usize {
        self.state.estimated_memory()
    }

    /// Sets the maximum number of entries to keep in memory.
    ///
    /// When the cache exceeds this limit, oldest entries are evicted from
    /// memory but remain on disk. `get()` for evicted entries reads from
    /// the segment file (slower). Default is `usize::MAX` (no eviction).
    pub fn set_max_cache_entries(&mut self, max: usize) {
        self.state.max_cache_entries = max;
        self.state.evict_if_needed();
    }

    /// Sets the maximum segment file size in bytes (default 64 MB).
    ///
    /// When the active segment exceeds this size, it is sealed and a new
    /// segment is created.
    pub fn set_max_segment_size(&mut self, size: usize) {
        self.max_segment_size = size;
    }

    /// Discards all entries with index <= `up_to_inclusive`.
    ///
    /// Deletes old segment files entirely. The active segment is rewritten
    /// only if it contained compacted entries.
    /// Typically called after a snapshot to purge old entries and free memory.
    pub fn compact(&mut self, up_to_inclusive: u64) -> Result<()> {
        if !self.state.compact(up_to_inclusive) {
            return Ok(());
        }
        // Delete sealed segments that are entirely compacted
        self.sealed.retain(|seg| {
            if seg.last_index <= up_to_inclusive {
                let _ = std::fs::remove_file(&seg.path);
                false
            } else {
                true
            }
        });
        // Rewrite active segment if it contained compacted entries
        if self.active_meta.first_index <= up_to_inclusive {
            self.rewrite_active_segment()?;
        }
        Ok(())
    }

    /// Discards all entries with index >= `from_inclusive`.
    ///
    /// Typically called during Raft log conflict resolution.
    pub fn truncate(&mut self, from_inclusive: u64) -> Result<()> {
        if !self.state.truncate(from_inclusive) {
            return Ok(());
        }
        // Delete sealed segments entirely past the truncation point
        self.sealed.retain(|seg| {
            if seg.first_index >= from_inclusive {
                let _ = std::fs::remove_file(&seg.path);
                false
            } else {
                true
            }
        });
        // Rewrite the active segment with only surviving entries
        self.rewrite_active_segment()?;
        Ok(())
    }

    /// Stores a metadata key-value pair (e.g. `"vote"`, `"term"`).
    ///
    /// Always fsynced to disk — required for Raft election safety.
    pub fn set_meta(&mut self, key: &str, value: &[u8]) -> Result<()> {
        self.state.meta.insert(key.to_string(), value.to_vec());
        self.save_meta()
    }

    /// Returns the metadata value for the given key.
    pub fn get_meta(&self, key: &str) -> Option<&[u8]> {
        self.state.meta.get(key).map(|v| v.as_slice())
    }

    /// Removes a metadata key.
    pub fn remove_meta(&mut self, key: &str) -> Result<()> {
        self.state.meta.remove(key);
        self.save_meta()
    }

    /// Flushes buffered writes to the OS (without fsync).
    pub fn flush(&mut self) -> Result<()> {
        self.active_writer.flush()?;
        Ok(())
    }

    /// Flushes buffered writes and fsyncs to stable storage.
    ///
    /// Call this after [`append`](Self::append) if your Raft implementation
    /// requires durable log entries before acknowledging `AppendEntries`.
    pub fn sync(&mut self) -> Result<()> {
        self.active_writer.flush()?;
        self.active_writer.get_ref().sync_data()?;
        Ok(())
    }

    fn read_from_disk(&self, index: u64) -> Option<Vec<u8>> {
        // Check sealed segments
        for seg in &self.sealed {
            if index >= seg.first_index && index <= seg.last_index {
                return read_entry_from_segment(&seg.path, index);
            }
        }
        // Check active segment
        if index >= self.active_meta.first_index && index <= self.active_meta.last_index {
            return read_entry_from_segment(&self.active_meta.path, index);
        }
        None
    }

    fn rotate_segment(&mut self) -> Result<()> {
        self.active_writer.flush()?;

        // Seal the current active segment
        let sealed_meta = self.active_meta.clone();

        // Open new segment
        let next_index = self.active_meta.last_index + 1;
        let new_path = segment_path(&self.dir_path, next_index);
        let new_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&new_path)?;

        // Swap the old active file handle so it drops and closes
        let old_writer = std::mem::replace(
            &mut self.active_writer,
            BufWriter::with_capacity(64 * 1024, new_file),
        );
        let _ = old_writer.into_inner();

        self.sealed.push(sealed_meta);
        self.active_meta = SegmentMeta {
            path: new_path,
            first_index: next_index,
            last_index: next_index.saturating_sub(1),
        };
        self.active_bytes = 0;
        Ok(())
    }

    fn rewrite_active_segment(&mut self) -> Result<()> {
        let new_path = if self.state.is_empty() {
            segment_path(&self.dir_path, 1)
        } else {
            let first_active = self
                .sealed
                .last()
                .map(|s| s.last_index + 1)
                .unwrap_or(self.state.base_index);
            segment_path(&self.dir_path, first_active)
        };

        let tmp_path = self.dir_path.join("active.tmp");
        let mut buf = Vec::new();
        if !self.state.is_empty() {
            let first_active = self
                .sealed
                .last()
                .map(|s| s.last_index + 1)
                .unwrap_or(self.state.base_index);
            let last = self.state.base_index + self.state.entries.len() as u64 - 1;
            for idx in first_active..=last {
                if let Some(data) = self.state.get(idx) {
                    serialize_entry(&mut buf, idx, data);
                }
            }
        }
        std::fs::write(&tmp_path, &buf)?;

        // Close old active writer
        let tmp_file = std::fs::OpenOptions::new().append(true).open(&tmp_path)?;
        let old_writer = std::mem::replace(&mut self.active_writer, BufWriter::new(tmp_file));
        let _ = old_writer.into_inner();

        // Remove old active segment if different path
        if self.active_meta.path != new_path {
            let _ = std::fs::remove_file(&self.active_meta.path);
        }
        std::fs::rename(&tmp_path, &new_path)?;

        let file = std::fs::OpenOptions::new().append(true).open(&new_path)?;
        self.active_writer = BufWriter::with_capacity(64 * 1024, file);
        self.active_bytes = buf.len();

        let first_index = if self.state.is_empty() {
            1
        } else {
            self.sealed
                .last()
                .map(|s| s.last_index + 1)
                .unwrap_or(self.state.base_index)
        };
        self.active_meta = SegmentMeta {
            path: new_path,
            first_index,
            last_index: self
                .state
                .last_index()
                .unwrap_or(first_index.saturating_sub(1)),
        };
        Ok(())
    }

    fn save_meta(&self) -> Result<()> {
        let bytes = self.state.serialize_meta();
        let tmp_path = self.meta_path.with_extension("tmp");
        let mut file = File::create(&tmp_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp_path, &self.meta_path)?;
        Ok(())
    }
}

impl Drop for RaftWal {
    fn drop(&mut self) {
        let _ = self.active_writer.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_append_get() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = RaftWal::open(dir.path()).expect("open");
        wal.append(1, b"hello").expect("append");
        assert_eq!(wal.get(1).as_deref(), Some(b"hello".as_slice()));
        assert_eq!(wal.len(), 1);
    }

    #[test]
    fn read_range_works() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = RaftWal::open(dir.path()).expect("open");
        for i in 1..=10 {
            wal.append(i, format!("e{i}").as_bytes()).expect("append");
        }
        let r = wal.read_range(3..=7);
        assert_eq!(r.len(), 5);
        assert_eq!(r[0].0, 3);
    }

    #[test]
    fn iter_range_borrowed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = RaftWal::open(dir.path()).expect("open");
        for i in 1..=5 {
            wal.append(i, format!("e{i}").as_bytes()).expect("append");
        }
        let entries: Vec<_> = wal.iter_range(2..=4).collect();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].index, 2);
        assert_eq!(entries[0].data, b"e2");
        assert_eq!(entries[2].index, 4);
    }

    #[test]
    fn iter_all() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = RaftWal::open(dir.path()).expect("open");
        for i in 1..=3 {
            wal.append(i, format!("e{i}").as_bytes()).expect("append");
        }
        let entries: Vec<_> = wal.iter().collect();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].index, 1);
        assert_eq!(entries[2].data, b"e3");
    }

    #[test]
    fn recovery() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();
        {
            let mut wal = RaftWal::open(&path).expect("open");
            wal.append(1, b"a").expect("a");
            wal.append(2, b"b").expect("b");
            wal.set_meta("vote", b"v1").expect("meta");
        }
        {
            let wal = RaftWal::open(&path).expect("reopen");
            assert_eq!(wal.len(), 2);
            assert_eq!(wal.get(2).as_deref(), Some(b"b".as_slice()));
            assert_eq!(wal.get_meta("vote"), Some(b"v1".as_slice()));
        }
    }

    #[test]
    fn compact_works() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = RaftWal::open(dir.path()).expect("open");
        for i in 1..=5 {
            wal.append(i, b"x").expect("a");
        }
        wal.compact(3).expect("compact");
        assert_eq!(wal.len(), 2);
        assert_eq!(wal.first_index(), Some(4));
    }

    #[test]
    fn truncate_works() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = RaftWal::open(dir.path()).expect("open");
        for i in 1..=5 {
            wal.append(i, b"x").expect("a");
        }
        wal.truncate(3).expect("truncate");
        assert_eq!(wal.len(), 2);
        assert_eq!(wal.last_index(), Some(2));
    }

    #[test]
    fn batch_append_borrowed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = RaftWal::open(dir.path()).expect("open");
        wal.append_batch(&[(1, b"a" as &[u8]), (2, b"b"), (3, b"c")])
            .expect("batch");
        assert_eq!(wal.len(), 3);
        assert_eq!(wal.get(2).as_deref(), Some(b"b".as_slice()));
    }

    #[test]
    fn batch_append_owned() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = RaftWal::open(dir.path()).expect("open");
        let entries = vec![(1u64, vec![1u8, 2, 3]), (2, vec![4, 5, 6])];
        wal.append_batch(&entries).expect("batch owned");
        assert_eq!(wal.len(), 2);
        assert_eq!(wal.get(1).as_deref(), Some([1u8, 2, 3].as_slice()));
    }

    #[test]
    fn meta_operations() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = RaftWal::open(dir.path()).expect("open");
        assert!(wal.get_meta("k").is_none());
        wal.set_meta("k", b"v").expect("set");
        assert_eq!(wal.get_meta("k"), Some(b"v".as_slice()));
        wal.remove_meta("k").expect("rm");
        assert!(wal.get_meta("k").is_none());
    }

    #[test]
    fn empty_wal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = RaftWal::open(dir.path()).expect("open");
        assert!(wal.is_empty());
        assert_eq!(wal.first_index(), None);
        assert_eq!(wal.last_index(), None);
    }

    #[test]
    fn recovery_after_compact() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();
        {
            let mut wal = RaftWal::open(&path).expect("open");
            for i in 1..=5 {
                wal.append(i, format!("e{i}").as_bytes()).expect("a");
            }
            wal.compact(3).expect("compact");
        }
        {
            let wal = RaftWal::open(&path).expect("reopen");
            assert_eq!(wal.len(), 2);
            assert_eq!(wal.first_index(), Some(4));
            assert_eq!(wal.get(4).as_deref(), Some(b"e4".as_slice()));
        }
    }

    #[test]
    fn recovery_after_truncate() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();
        {
            let mut wal = RaftWal::open(&path).expect("open");
            for i in 1..=5 {
                wal.append(i, format!("e{i}").as_bytes()).expect("a");
            }
            wal.truncate(4).expect("truncate");
        }
        {
            let wal = RaftWal::open(&path).expect("reopen");
            assert_eq!(wal.len(), 3);
            assert_eq!(wal.last_index(), Some(3));
            assert!(wal.get(4).is_none());
        }
    }

    #[test]
    fn append_after_compact() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = RaftWal::open(dir.path()).expect("open");
        for i in 1..=5 {
            wal.append(i, b"x").expect("a");
        }
        wal.compact(3).expect("compact");
        wal.append(6, b"new").expect("append after compact");
        assert_eq!(wal.len(), 3);
        assert_eq!(wal.get(6).as_deref(), Some(b"new".as_slice()));
        assert_eq!(wal.first_index(), Some(4));
    }

    #[test]
    fn append_after_truncate() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = RaftWal::open(dir.path()).expect("open");
        for i in 1..=5 {
            wal.append(i, b"old").expect("a");
        }
        wal.truncate(3).expect("truncate");
        wal.append(3, b"new").expect("append replacement");
        assert_eq!(wal.len(), 3);
        assert_eq!(wal.get(3).as_deref(), Some(b"new".as_slice()));
    }

    #[test]
    fn compact_all() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = RaftWal::open(dir.path()).expect("open");
        for i in 1..=3 {
            wal.append(i, b"x").expect("a");
        }
        wal.compact(3).expect("compact all");
        assert!(wal.is_empty());
        assert_eq!(wal.first_index(), None);
    }

    #[test]
    fn truncate_all() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = RaftWal::open(dir.path()).expect("open");
        for i in 1..=3 {
            wal.append(i, b"x").expect("a");
        }
        wal.truncate(1).expect("truncate all");
        assert!(wal.is_empty());
        assert_eq!(wal.last_index(), None);
    }

    #[test]
    fn truncate_noop_on_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = RaftWal::open(dir.path()).expect("open");
        wal.compact(10).expect("noop");
        wal.truncate(1).expect("noop");
        assert!(wal.is_empty());
    }

    #[test]
    fn truncate_out_of_range() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = RaftWal::open(dir.path()).expect("open");
        for i in 5..=10 {
            wal.append(i, b"x").expect("a");
        }
        wal.compact(2).expect("below range");
        assert_eq!(wal.len(), 6);
        wal.truncate(20).expect("above range");
        assert_eq!(wal.len(), 6);
    }

    #[test]
    fn get_out_of_range() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = RaftWal::open(dir.path()).expect("open");
        wal.append(5, b"x").expect("a");
        assert!(wal.get(0).is_none());
        assert!(wal.get(4).is_none());
        assert!(wal.get(6).is_none());
        assert!(wal.get(u64::MAX).is_none());
    }

    #[test]
    fn read_range_empty_result() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = RaftWal::open(dir.path()).expect("open");
        for i in 5..=10 {
            wal.append(i, b"x").expect("a");
        }
        assert!(wal.read_range(1..=4).is_empty());
        assert!(wal.read_range(11..=20).is_empty());
        assert!(wal.read_range(8..8).is_empty());
    }

    #[test]
    fn read_range_partial_overlap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = RaftWal::open(dir.path()).expect("open");
        for i in 5..=10 {
            wal.append(i, format!("e{i}").as_bytes()).expect("a");
        }
        let r = wal.read_range(3..=7);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].0, 5);
        assert_eq!(r[2].0, 7);
    }

    #[test]
    fn large_entry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();
        let big = vec![0xABu8; 1024 * 1024];
        {
            let mut wal = RaftWal::open(&path).expect("open");
            wal.append(1, &big).expect("append big");
        }
        {
            let wal = RaftWal::open(&path).expect("reopen");
            assert_eq!(wal.get(1).expect("get").as_ref(), big.as_slice());
        }
    }

    #[test]
    fn flush_persists() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();
        {
            let mut wal = RaftWal::open(&path).expect("open");
            wal.append(1, b"buffered").expect("a");
            wal.flush().expect("flush");
        }
        {
            let wal = RaftWal::open(&path).expect("reopen");
            assert_eq!(wal.get(1).as_deref(), Some(b"buffered".as_slice()));
        }
    }

    #[test]
    fn sync_persists() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();
        {
            let mut wal = RaftWal::open(&path).expect("open");
            wal.append(1, b"durable").expect("a");
            wal.sync().expect("sync");
        }
        {
            let wal = RaftWal::open(&path).expect("reopen");
            assert_eq!(wal.get(1).as_deref(), Some(b"durable".as_slice()));
        }
    }

    #[test]
    fn meta_survives_crash() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();
        {
            let mut wal = RaftWal::open(&path).expect("open");
            wal.set_meta("term", b"5").expect("set term");
            wal.set_meta("vote", b"node-2").expect("set vote");
        }
        {
            let wal = RaftWal::open(&path).expect("reopen");
            assert_eq!(wal.get_meta("term"), Some(b"5".as_slice()));
            assert_eq!(wal.get_meta("vote"), Some(b"node-2".as_slice()));
        }
    }

    #[test]
    fn error_source_chain() {
        let err = WalError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "gone"));
        assert!(std::error::Error::source(&err).is_some());
    }

    #[test]
    fn estimated_memory_increases() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = RaftWal::open(dir.path()).expect("open");
        let before = wal.estimated_memory();
        for i in 1..=100 {
            wal.append(i, &[0u8; 256]).expect("a");
        }
        assert!(wal.estimated_memory() > before);
    }

    #[test]
    fn segment_rotation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = RaftWal::open(dir.path()).expect("open");
        // Force small segments for testing
        wal.max_segment_size = 200;

        for i in 1..=100 {
            wal.append(i, &[0u8; 32]).expect("a");
        }
        assert!(!wal.sealed.is_empty());

        // All entries still readable
        for i in 1..=100 {
            assert!(wal.get(i).is_some(), "missing index {i}");
        }
    }

    #[test]
    fn segment_rotation_recovery() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();
        {
            let mut wal = RaftWal::open(&path).expect("open");
            wal.max_segment_size = 200;
            for i in 1..=100 {
                wal.append(i, format!("e{i}").as_bytes()).expect("a");
            }
        }
        {
            let wal = RaftWal::open(&path).expect("reopen");
            assert_eq!(wal.len(), 100);
            assert_eq!(wal.get(1).as_deref(), Some(b"e1".as_slice()));
            assert_eq!(wal.get(100).as_deref(), Some(b"e100".as_slice()));
        }
    }

    #[test]
    fn compact_deletes_old_segments() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = RaftWal::open(dir.path()).expect("open");
        wal.max_segment_size = 200;

        for i in 1..=100 {
            wal.append(i, &[0u8; 32]).expect("a");
        }
        let seg_count_before = list_segments(dir.path()).len();

        wal.compact(80).expect("compact");
        let seg_count_after = list_segments(dir.path()).len();

        assert!(seg_count_after < seg_count_before);
        assert_eq!(wal.len(), 20);
        assert_eq!(wal.first_index(), Some(81));
    }
}
