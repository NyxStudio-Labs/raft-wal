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

#![cfg_attr(not(feature = "std"), no_std)]
#![deny(missing_docs)]
#![deny(clippy::unwrap_used)]

#[cfg(not(feature = "std"))]
extern crate alloc;

/// CRC32C functions (hw-accelerated with `std`, software fallback in `no_std`).
pub mod crc;
/// Wire format serialization and parsing (no_std compatible).
pub mod wire;

#[macro_use]
pub(crate) mod macros;
pub(crate) mod core;

#[cfg(feature = "std")]
pub(crate) mod segment;
pub(crate) mod state;

mod storage;
pub use storage::WalStorage;

mod generic;
pub use generic::GenericRaftWal;

#[cfg(feature = "std")]
mod std_storage;
#[cfg(feature = "std")]
pub use std_storage::StdStorage;

#[cfg(feature = "tokio")]
mod tokio;
#[cfg(feature = "tokio")]
pub use self::tokio::AsyncRaftWal;

#[cfg(feature = "io-uring")]
mod uring;
#[cfg(feature = "io-uring")]
pub use uring::UringRaftWal;

#[cfg(feature = "io-uring-bridge")]
mod bridge;
#[cfg(feature = "io-uring-bridge")]
pub use bridge::BridgedUringWal;

#[cfg(feature = "std")]
pub mod impls;

#[cfg(feature = "openraft-storage")]
pub use impls::openraft::OpenRaftLogStorage;

/// Errors returned by WAL operations.
#[cfg(feature = "std")]
#[derive(Debug)]
pub enum WalError {
    /// An I/O error.
    Io(std::io::Error),
}

#[cfg(feature = "std")]
impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "WAL I/O: {e}"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for WalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
        }
    }
}

#[cfg(feature = "std")]
impl From<std::io::Error> for WalError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// Result type for WAL operations.
#[cfg(feature = "std")]
pub type Result<T> = std::result::Result<T, WalError>;

/// A borrowed entry yielded by iterators over WAL entries.
pub struct Entry<'a> {
    /// The Raft log index.
    pub index: u64,
    /// The entry payload.
    pub data: &'a [u8],
}

/// An append-only WAL optimized for Raft, backed by the local filesystem.
///
/// This is a type alias for [`GenericRaftWal`] with [`StdStorage`].
/// See [`GenericRaftWal`] for the full API documentation.
#[cfg(feature = "std")]
pub type RaftWal = GenericRaftWal<StdStorage>;

#[cfg(feature = "std")]
impl RaftWal {
    /// Opens or creates a WAL in the given directory.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created or existing
    /// segments cannot be recovered.
    pub fn open(data_dir: impl AsRef<std::path::Path>) -> Result<Self> {
        let storage = StdStorage::new(data_dir)?;
        GenericRaftWal::new(storage).map_err(WalError::Io)
    }

    /// Returns the entry at the given index.
    ///
    /// Returns `Cow::Borrowed` for cached (in-memory) entries and
    /// `Cow::Owned` for evicted entries read from disk.
    #[must_use]
    pub fn get(&self, index: u64) -> Option<std::borrow::Cow<'_, [u8]>> {
        if let Some(data) = self.get_cached(index) {
            return Some(std::borrow::Cow::Borrowed(data));
        }
        self.get_or_read(index).ok().flatten().map(std::borrow::Cow::Owned)
    }
}

#[cfg(all(test, feature = "std"))]
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
        wal.set_max_segment_size(200);

        for i in 1..=100 {
            wal.append(i, &[0u8; 32]).expect("a");
        }

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
            wal.set_max_segment_size(200);
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
        wal.set_max_segment_size(200);

        for i in 1..=100 {
            wal.append(i, &[0u8; 32]).expect("a");
        }
        let seg_count_before = segment::list_segments(dir.path()).len();

        wal.compact(80).expect("compact");
        let seg_count_after = segment::list_segments(dir.path()).len();

        assert!(seg_count_after < seg_count_before);
        assert_eq!(wal.len(), 20);
        assert_eq!(wal.first_index(), Some(81));
    }
}
