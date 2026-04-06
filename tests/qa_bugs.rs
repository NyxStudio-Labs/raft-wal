#![allow(clippy::pedantic)]
//! Regression tests for identified bugs and edge cases.
//!
//! Each test is tagged with the issue number from the code review:
//!   #1  — eviction leaves empty Vecs consuming memory in VecDeque
//!   #2  — state.rs insert() underflows when index < base_index
//!   #3  — compact silently ignores remove_file errors
//!   #4  — read_from_disk loads entire segment for a single entry
//!   #5  — std_storage sync_file opens a separate fd
//!   #8  — openraft append calls LogFlushed without sync
//!   #10 — wire format has no version header

use raft_wal::{GenericRaftWal, RaftWal, WalStorage};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

fn open(dir: &Path) -> RaftWal {
    RaftWal::open(dir).expect("open")
}

// ========================================================================
// #2 — insert() underflow when index < base_index after compact
// ========================================================================

/// After compact(N), base_index advances past N. Appending an index below
/// base_index causes u64 underflow in state.insert(). Through the public
/// API, compact(11) + append(5, ..) triggers this.
#[test]
#[should_panic]
fn bug02_append_below_base_index_after_compact() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = open(dir.path());

    wal.append(10, b"a").expect("append");
    wal.append(11, b"b").expect("append");
    wal.append(12, b"c").expect("append");
    wal.compact(11).expect("compact"); // base_index → 12

    // This should fail gracefully, but instead causes u64 underflow
    // in state.insert() → (5 - 12) wraps → huge slot → OOM or panic.
    wal.append(5, b"underflow").expect("append");
}

// ========================================================================
// #1 — Eviction leaves empty Vecs in VecDeque, wasting memory
// ========================================================================

/// Eviction replaces entries with empty Vecs. The per-slot overhead (24 bytes
/// each on 64-bit) is reclaimed by compact(), not eviction itself.
/// This test documents the behavior and verifies disk fallback works.
#[test]
fn bug01_eviction_memory_reduced_and_disk_fallback() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = open(dir.path());

    // Small segments to force sealing (eviction only applies to sealed segments)
    wal.set_max_segment_size(512);

    let n = 500u64;
    for i in 1..=n {
        wal.append(i, &[0u8; 32]).expect("append");
    }

    let mem_before = wal.estimated_memory();
    wal.set_max_cache_entries(10).expect("set cache");
    let mem_after = wal.estimated_memory();

    // Memory should drop from eviction (payload freed, slot overhead remains)
    assert!(
        mem_after < mem_before,
        "eviction should reduce memory: before={mem_before} after={mem_after}"
    );

    // Evicted entries return None from cache
    assert!(
        wal.get_cached(1).is_none(),
        "evicted entry should not be in cache"
    );
    assert!(
        wal.get_cached(n).is_some(),
        "recent entry should still be cached"
    );

    // len() reports the full count (evicted entries still in WAL)
    assert_eq!(wal.len(), n as usize);

    // Disk fallback works for evicted entries
    let entry = wal.get(1);
    assert_eq!(entry.as_deref(), Some([0u8; 32].as_slice()));

    // compact() reclaims the empty slot overhead
    wal.compact(400).expect("compact");
    let mem_compacted = wal.estimated_memory();
    assert!(
        mem_compacted < mem_after,
        "compact should reclaim empty slot overhead: after_evict={mem_after} after_compact={mem_compacted}"
    );
}

// ========================================================================
// #3 — compact ignores remove_file errors (via custom WalStorage)
// ========================================================================

/// A WalStorage that fails on remove_file to test error propagation.
struct FailOnRemoveStorage {
    inner: BTreeMap<String, Vec<u8>>,
    fail_remove: bool,
}

impl FailOnRemoveStorage {
    fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
            fail_remove: false,
        }
    }
}

#[derive(Debug)]
struct TestError(#[allow(dead_code)] String);

impl WalStorage for FailOnRemoveStorage {
    type Error = TestError;

    fn read_file(&self, name: &str) -> Result<Vec<u8>, Self::Error> {
        self.inner
            .get(name)
            .cloned()
            .ok_or_else(|| TestError(format!("not found: {name}")))
    }

    fn write_file(&mut self, name: &str, data: &[u8]) -> Result<(), Self::Error> {
        self.inner.insert(name.to_string(), data.to_vec());
        Ok(())
    }

    fn append_file(&mut self, name: &str, data: &[u8]) -> Result<(), Self::Error> {
        self.inner
            .entry(name.to_string())
            .or_default()
            .extend_from_slice(data);
        Ok(())
    }

    fn remove_file(&mut self, name: &str) -> Result<(), Self::Error> {
        if self.fail_remove {
            return Err(TestError(format!("remove failed: {name}")));
        }
        self.inner.remove(name);
        Ok(())
    }

    fn list_files(&self, suffix: &str) -> Result<Vec<String>, Self::Error> {
        let mut names: Vec<String> = self
            .inner
            .keys()
            .filter(|k| k.ends_with(suffix))
            .cloned()
            .collect();
        names.sort();
        Ok(names)
    }

    fn sync_file(&mut self, _name: &str) -> Result<(), Self::Error> {
        Ok(())
    }

    fn rename_file(&mut self, from: &str, to: &str) -> Result<(), Self::Error> {
        if let Some(data) = self.inner.remove(from) {
            self.inner.insert(to.to_string(), data);
        }
        Ok(())
    }

    fn file_size(&self, name: &str) -> Result<u64, Self::Error> {
        self.inner
            .get(name)
            .map(|d| d.len() as u64)
            .ok_or_else(|| TestError(format!("not found: {name}")))
    }

    fn file_exists(&self, name: &str) -> bool {
        self.inner.contains_key(name)
    }
}

/// [FIXED] compact now propagates remove_file errors instead of swallowing them.
#[test]
fn bug03_compact_propagates_remove_file_error() {
    let storage = FailOnRemoveStorage::new();
    let mut wal = GenericRaftWal::new(storage).expect("new");

    wal.set_max_segment_size(64);

    for i in 1..=20 {
        wal.append(i, &[0u8; 32]).expect("append");
    }

    // Enable removal failures
    wal.storage_mut().fail_remove = true;

    // compact should now return an error when remove_file fails
    let result = wal.compact(15);
    assert!(
        result.is_err(),
        "compact should propagate remove_file errors"
    );

    // Despite the error, in-memory state is still consistent
    // (sealed list is updated even if file deletion failed)
    assert_eq!(wal.first_index(), Some(16));
}

/// Test the default WalStorage::read_file_range implementation (fallback).
#[test]
fn default_read_file_range_fallback() {
    // FailOnRemoveStorage uses the DEFAULT read_file_range (not overridden)
    let mut storage = FailOnRemoveStorage::new();
    storage
        .write_file("test.dat", b"hello world")
        .expect("write");

    let result = storage.read_file_range("test.dat", 6, 5).expect("range");
    assert_eq!(&result, b"world");

    // Past-end is clamped by default impl
    let result = storage.read_file_range("test.dat", 9, 100).expect("range");
    assert_eq!(&result, b"ld");

    let result = storage.read_file_range("test.dat", 0, 5).expect("range");
    assert_eq!(&result, b"hello");
}

// ========================================================================
// #4 — read_from_disk loads the entire segment for a single entry lookup
// ========================================================================

/// After cache eviction, get() falls back to read_from_disk which reads
/// the entire sealed segment file even for a single entry.
#[test]
fn bug04_disk_fallback_after_eviction() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = open(dir.path());

    wal.set_max_segment_size(512);

    let entry_count = 200u64;
    for i in 1..=entry_count {
        wal.append(i, format!("payload-{i}").as_bytes())
            .expect("append");
    }
    wal.sync().expect("sync");

    // Evict everything except the last 5
    wal.set_max_cache_entries(5).expect("set cache");

    // Evicted entry readable from disk (entire segment loaded for this)
    let entry = wal.get(1);
    assert_eq!(
        entry.as_deref(),
        Some(b"payload-1".as_slice()),
        "evicted entry should be readable from disk"
    );

    // Mid-range entry from different sealed segment
    let mid = entry_count / 2;
    let entry = wal.get(mid);
    assert_eq!(
        entry.as_deref(),
        Some(format!("payload-{mid}").as_bytes()),
        "mid-range evicted entry should be readable"
    );

    // Recent cached entry
    let entry = wal.get(entry_count);
    assert_eq!(
        entry.as_deref(),
        Some(format!("payload-{entry_count}").as_bytes()),
        "recent entry should still be cached"
    );
}

// ========================================================================
// #5 — std_storage sync_file opens a separate fd
// ========================================================================

/// sync_file opens with File::open (read-only) and calls sync_all() on
/// that new fd, rather than syncing the write fd. Data survives on Linux
/// due to shared page cache, but not guaranteed by POSIX.
#[test]
fn bug05_sync_separate_fd_still_recoverable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = open(dir.path());

    wal.append(1, b"data").expect("append");
    wal.sync().expect("sync");

    drop(wal);
    let wal = open(dir.path());
    assert_eq!(
        wal.get(1).as_deref(),
        Some(b"data".as_slice()),
        "data should survive reopen after sync"
    );
}

// ========================================================================
// #10 — Wire format has no version header
// ========================================================================

/// [FIXED] Segments now have a version header ("RWAL" + version byte).
/// The parser auto-detects the header and strips it before parsing entries.
#[test]
fn bug10_segments_have_version_header() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = open(dir.path());

    wal.append(1, b"test").expect("append");
    wal.sync().expect("sync");
    drop(wal);

    // Read raw segment file
    let seg_files: Vec<_> = fs::read_dir(dir.path())
        .expect("readdir")
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".seg"))
        .collect();
    assert!(!seg_files.is_empty(), "should have at least one segment");

    let data = fs::read(seg_files[0].path()).expect("read seg");
    assert!(!data.is_empty());

    // Verify segment header is present
    assert_eq!(&data[..4], b"RWAL", "segment should start with RWAL magic");
    #[cfg(feature = "zstd")]
    assert_eq!(data[4], 2, "segment version should be 2 (zstd)");
    #[cfg(not(feature = "zstd"))]
    assert_eq!(data[4], 1, "segment version should be 1");

    // strip_segment_header correctly separates header from entries
    let (version, _entry_data) = raft_wal::wire::strip_segment_header(&data);
    #[cfg(feature = "zstd")]
    assert_eq!(version, 2);
    #[cfg(not(feature = "zstd"))]
    assert_eq!(version, 1);

    // Verify entries can be recovered via parse_segment (handles v1 and v2)
    let entries: Vec<_> = {
        let wal = open(dir.path());
        wal.iter().map(|e| (e.index, e.data.to_vec())).collect()
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, 1);
    assert_eq!(entries[0].1, b"test");
}

// ========================================================================
// Compound: compact + eviction + disk read
// ========================================================================

#[test]
fn regression_compact_eviction_disk_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = open(dir.path());
    wal.set_max_segment_size(256);

    for i in 1..=100 {
        wal.append(i, format!("e{i}").as_bytes()).expect("append");
    }
    wal.sync().expect("sync");

    wal.compact(30).expect("compact");
    assert_eq!(wal.first_index(), Some(31));

    wal.set_max_cache_entries(5).expect("set cache");

    // Entry 31: evicted from cache but still on disk in a sealed segment
    let entry = wal.get(31);
    assert_eq!(
        entry.as_deref(),
        Some(b"e31".as_slice()),
        "entry after compact + eviction should be readable from disk"
    );

    // Note: compact removes entries from in-memory state, but get() on
    // RaftWal falls through to get_or_read() which reads from disk.
    // Compacted entries may still be readable from disk if their segment
    // wasn't fully deleted (segment.last_index > compact boundary).
    // This is a separate concern — the in-memory state correctly reports:
    assert_eq!(wal.first_index(), Some(31));
    assert!(wal.get_cached(1).is_none(), "compacted entry not in cache");
    assert!(wal.get_cached(30).is_none(), "compacted entry not in cache");
}

// ========================================================================
// Async regression tests (require tokio feature)
// ========================================================================

#[cfg(feature = "tokio")]
mod async_bugs {
    use raft_wal::AsyncRaftWal;

    /// #8: openraft's append should sync before reporting durability.
    /// Test that sync() guarantees data survives reopen, while entries
    /// without sync may be lost on crash (simulated via mem::forget).
    #[tokio::test]
    async fn bug08_sync_required_for_durability() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();

        // Crash simulation: entries without sync
        {
            let mut wal = AsyncRaftWal::open(&path).await.expect("open");
            wal.append(1, b"unsynced").await.expect("append");
            std::mem::forget(wal); // simulate crash
        }

        // Verify: synced entries survive
        {
            let mut wal = AsyncRaftWal::open(&path).await.expect("open");
            wal.append(10, b"synced").await.expect("append");
            wal.sync().await.expect("sync");
            wal.close().await.expect("close");
        }
        {
            let wal = AsyncRaftWal::open(&path).await.expect("reopen");
            assert_eq!(wal.get(10), Some(b"synced".as_slice()));
        }
    }

    /// #6: async compact/truncate should work correctly with async I/O.
    #[tokio::test]
    async fn bug06_async_compact_removes_segments() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = AsyncRaftWal::open(dir.path()).await.expect("open");
        for i in 1..=100 {
            wal.append(i, &[0u8; 1024]).await.expect("append");
        }
        wal.sync().await.expect("sync");
        wal.compact(80).await.expect("compact");
        assert_eq!(wal.first_index(), Some(81));
    }
}
