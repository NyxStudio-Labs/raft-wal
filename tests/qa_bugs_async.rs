//! Async regression tests for identified bugs.
//!
//! Requires `tokio` feature: cargo test --features tokio

#![cfg(feature = "tokio")]

use raft_wal::AsyncRaftWal;
use std::fs;

// ========================================================================
// #8 — openraft append does not call sync before LogFlushed callback
// ========================================================================
//
// LogFlushed is not publicly constructable, so we test the underlying
// AsyncRaftWal: after append() without sync(), data is not guaranteed
// to be on stable storage.

/// Demonstrates that entries buffered in memory (not yet flushed to OS)
/// are lost if the process terminates without sync. The openraft
/// integration calls `log_io_completed` immediately after append without
/// sync, so openraft believes entries are durable when they may not be.
#[tokio::test]
async fn bug08_append_not_durable_without_sync() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();

    {
        let mut wal = AsyncRaftWal::open(&path).await.expect("open");
        wal.append(1, b"entry-1").await.expect("append");
        wal.append(2, b"entry-2").await.expect("append");
        // Deliberately NOT calling sync() — simulates what openraft's
        // append() does before calling log_io_completed().
        // mem::forget simulates a crash (no Drop, no async cleanup).
        std::mem::forget(wal);
    }

    // Reopen — entries in disk_buf (< FLUSH_THRESHOLD) may be lost.
    let wal = AsyncRaftWal::open(&path).await.expect("reopen");
    let _recovered = wal.len();

    // The entries may or may not be recovered depending on OS buffer state.
    // The point: without fsync, durability is not guaranteed. openraft's
    // LogFlushed callback should only fire AFTER sync().
    //
    // We don't assert on `recovered` count because it's OS-dependent.
    // Instead, verify that sync() DOES guarantee durability:
    drop(wal);

    {
        let mut wal = AsyncRaftWal::open(&path).await.expect("open");
        wal.append(10, b"synced-entry").await.expect("append");
        wal.sync().await.expect("sync");
        wal.close().await.expect("close");
    }
    {
        let wal = AsyncRaftWal::open(&path).await.expect("reopen");
        assert_eq!(
            wal.get(10),
            Some(b"synced-entry".as_slice()),
            "synced entry must survive reopen"
        );
    }
}

/// Verify that close() (which calls sync internally) guarantees durability.
#[tokio::test]
async fn bug08_close_guarantees_durability() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();

    {
        let mut wal = AsyncRaftWal::open(&path).await.expect("open");
        wal.append(1, b"a").await.expect("append");
        wal.append(2, b"b").await.expect("append");
        wal.close().await.expect("close"); // close calls sync
    }
    {
        let wal = AsyncRaftWal::open(&path).await.expect("reopen");
        assert_eq!(wal.len(), 2);
        assert_eq!(wal.get(1), Some(b"a".as_slice()));
        assert_eq!(wal.get(2), Some(b"b".as_slice()));
    }
}

// ========================================================================
// #6 — Blocking I/O in async compact/truncate
// ========================================================================

/// tokio.rs compact() uses std::fs::remove_file inside retain() —
/// a blocking call in async context. This test verifies correctness
/// while documenting the blocking issue.
#[tokio::test]
async fn bug06_async_compact_blocking_remove() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = AsyncRaftWal::open(dir.path()).await.expect("open");

    // Enough data to force segment rotation
    for i in 1..=100 {
        wal.append(i, &[0u8; 1024]).await.expect("append");
    }
    wal.sync().await.expect("sync");

    let seg_count_before = fs::read_dir(dir.path())
        .expect("readdir")
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".seg"))
        .count();

    // compact() uses std::fs::remove_file (blocking!) in .retain() closure
    wal.compact(80).await.expect("compact");

    let seg_count_after = fs::read_dir(dir.path())
        .expect("readdir")
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".seg"))
        .count();

    assert!(
        seg_count_after <= seg_count_before,
        "compact should remove old segments"
    );
    assert_eq!(wal.first_index(), Some(81));
}

/// truncate() also uses std::fs::remove_file in async context.
#[tokio::test]
async fn bug06_async_truncate_blocking_remove() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = AsyncRaftWal::open(dir.path()).await.expect("open");

    for i in 1..=50 {
        wal.append(i, &[0u8; 2048]).await.expect("append");
    }
    wal.sync().await.expect("sync");

    // truncate() uses blocking std::fs::remove_file in .retain()
    wal.truncate(20).await.expect("truncate");

    assert_eq!(wal.last_index(), Some(19));
    assert!(wal.get(20).is_none());
}

// ========================================================================
// Async: recovery after unclean shutdown (no sync/close)
// ========================================================================

/// When AsyncRaftWal is dropped without close/sync, only entries that
/// were flushed to the OS survive. Entries in the in-memory buffer are lost.
#[tokio::test]
async fn regression_async_recovery_without_close() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();

    {
        let mut wal = AsyncRaftWal::open(&path).await.expect("open");
        // Write enough to trigger at least one flush (FLUSH_THRESHOLD = 64KB)
        for i in 1..=100 {
            wal.append(i, &[0u8; 1024]).await.expect("append");
        }
        // Drop without sync — some data may be in memory buffer still
    }

    {
        let wal = AsyncRaftWal::open(&path).await.expect("reopen");
        // We should recover at least the entries that were flushed.
        // With 100 * 1024 bytes > 64KB threshold, at least some flushes happened.
        assert!(
            wal.len() > 0,
            "should recover at least some entries after unclean shutdown"
        );
    }
}
