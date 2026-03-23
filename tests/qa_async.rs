#![cfg(feature = "tokio")]
//! Async-specific QA tests for AsyncRaftWal.
//!
//! Logic tests (append, get, iter, compact, truncate) are covered by
//! the sync tests in qa.rs since they share the same core logic via
//! core.rs. These tests focus on async I/O behavior that ONLY the
//! tokio backend exercises:
//!   - Recovery across close/reopen
//!   - Flush/sync durability
//!   - CRC corruption recovery (async segment reading)
//!   - close() lifecycle
//!   - Segment rotation under async I/O

use raft_wal::AsyncRaftWal;
use std::fs;
use std::path::Path;

fn find_segments(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut segs: Vec<_> = fs::read_dir(dir)
        .expect("readdir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("seg"))
        .collect();
    segs.sort();
    segs
}

// ========================================================================
// Recovery (async file lifecycle: close → reopen)
// ========================================================================

#[tokio::test]
async fn recovery_after_close() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    {
        let mut wal = AsyncRaftWal::open(&path).await.expect("open");
        wal.append(1, b"a").await.expect("a");
        wal.append(2, b"b").await.expect("b");
        wal.set_meta("vote", b"v1").await.expect("meta");
        wal.close().await.expect("close");
    }
    {
        let wal = AsyncRaftWal::open(&path).await.expect("reopen");
        assert_eq!(wal.len(), 2);
        assert_eq!(wal.get(2).as_deref(), Some(b"b".as_slice()));
        assert_eq!(wal.get_meta("vote"), Some(b"v1".as_slice()));
    }
}

#[tokio::test]
async fn recovery_after_compact() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    {
        let mut wal = AsyncRaftWal::open(&path).await.expect("open");
        for i in 1..=10 {
            wal.append(i, format!("e{i}").as_bytes()).await.expect("a");
        }
        wal.compact(7).await.expect("compact");
        wal.close().await.expect("close");
    }
    {
        let wal = AsyncRaftWal::open(&path).await.expect("reopen");
        assert_eq!(wal.len(), 3);
        assert_eq!(wal.first_index(), Some(8));
        assert_eq!(wal.get(8).as_deref(), Some(b"e8".as_slice()));
        assert!(wal.get(7).is_none());
    }
}

#[tokio::test]
async fn meta_persistence_across_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    {
        let mut wal = AsyncRaftWal::open(&path).await.expect("open");
        wal.set_meta("term", b"5").await.expect("set");
        wal.set_meta("vote", b"node-2").await.expect("set");
        wal.close().await.expect("close");
    }
    {
        let wal = AsyncRaftWal::open(&path).await.expect("reopen");
        assert_eq!(wal.get_meta("term"), Some(b"5".as_slice()));
        assert_eq!(wal.get_meta("vote"), Some(b"node-2".as_slice()));
    }
}

// ========================================================================
// Durability (flush/sync/close semantics)
// ========================================================================

#[tokio::test]
async fn flush_persists_on_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    {
        let mut wal = AsyncRaftWal::open(&path).await.expect("open");
        wal.append(1, b"buffered").await.expect("a");
        wal.flush().await.expect("flush");
        // Drop without close — flush should have written to OS
    }
    {
        let wal = AsyncRaftWal::open(&path).await.expect("reopen");
        assert_eq!(wal.get(1).as_deref(), Some(b"buffered".as_slice()));
    }
}

#[tokio::test]
async fn sync_persists_on_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    {
        let mut wal = AsyncRaftWal::open(&path).await.expect("open");
        wal.append(1, b"durable").await.expect("a");
        wal.sync().await.expect("sync");
    }
    {
        let wal = AsyncRaftWal::open(&path).await.expect("reopen");
        assert_eq!(wal.get(1).as_deref(), Some(b"durable".as_slice()));
    }
}

#[tokio::test]
async fn close_guarantees_durability() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    {
        let mut wal = AsyncRaftWal::open(&path).await.expect("open");
        wal.append(1, b"closed").await.expect("a");
        wal.close().await.expect("close");
    }
    {
        let wal = AsyncRaftWal::open(&path).await.expect("reopen");
        assert_eq!(wal.get(1).as_deref(), Some(b"closed".as_slice()));
    }
}

// ========================================================================
// CRC corruption recovery (async segment reading path)
// ========================================================================

#[tokio::test]
async fn corrupt_crc_recovery() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    {
        let mut wal = AsyncRaftWal::open(&path).await.expect("open");
        wal.append(1, b"good").await.expect("a");
        wal.append(2, b"will-corrupt").await.expect("a");
        wal.close().await.expect("close");
    }
    let segs = find_segments(&path);
    let last_seg = &segs[segs.len() - 1];
    let mut data = fs::read(last_seg).expect("read seg");
    let seg_hdr = if data.len() >= 4 && &data[..4] == b"RWAL" { 5 } else { 0 };
    let second_entry_offset = seg_hdr + 20;
    if data.len() > second_entry_offset {
        data[second_entry_offset] ^= 0xFF;
        fs::write(last_seg, &data).expect("write corrupt");
    }
    {
        let wal = AsyncRaftWal::open(&path).await.expect("reopen");
        assert_eq!(wal.get(1).as_deref(), Some(b"good".as_slice()));
        assert!(wal.get(2).is_none());
    }
}

// ========================================================================
// Large entry recovery (tests async segment read for big files)
// ========================================================================

#[tokio::test]
async fn large_entry_recovery() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    let big = vec![0xABu8; 1024 * 1024];
    {
        let mut wal = AsyncRaftWal::open(&path).await.expect("open");
        wal.append(1, &big).await.expect("a");
        wal.close().await.expect("close");
    }
    {
        let wal = AsyncRaftWal::open(&path).await.expect("reopen");
        assert_eq!(wal.get(1).as_deref().map(|s| s.len()), Some(big.len()));
    }
}

// ========================================================================
// Recovery without close (Drop without async cleanup)
// ========================================================================

#[tokio::test]
async fn recovery_without_close() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    {
        let mut wal = AsyncRaftWal::open(&path).await.expect("open");
        // Write enough to trigger at least one flush (64KB threshold)
        for i in 1..=100 {
            wal.append(i, &[0u8; 1024]).await.expect("a");
        }
        // Drop without close/sync
    }
    {
        let wal = AsyncRaftWal::open(&path).await.expect("reopen");
        assert!(wal.len() > 0, "should recover flushed entries");
    }
}

// ========================================================================
// Partial segment compact/truncate recovery (async I/O path)
// ========================================================================

#[tokio::test]
async fn compact_partial_segment_recovery() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    {
        let mut wal = AsyncRaftWal::open(&path).await.expect("open");
        // Force segment rotation with enough data
        for i in 1..=50 {
            wal.append(i, &[0u8; 2048]).await.expect("a");
        }
        wal.sync().await.expect("sync");
        // Compact in the middle of a sealed segment
        wal.compact(25).await.expect("compact");
        wal.close().await.expect("close");
    }
    {
        let wal = AsyncRaftWal::open(&path).await.expect("reopen");
        assert_eq!(wal.first_index(), Some(26));
        assert!(wal.get(25).is_none(), "compacted entry should not resurrect");
        assert!(wal.get(26).is_some(), "entry after compact boundary should exist");
    }
}

#[tokio::test]
async fn truncate_partial_segment_recovery() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    {
        let mut wal = AsyncRaftWal::open(&path).await.expect("open");
        for i in 1..=50 {
            wal.append(i, &[0u8; 2048]).await.expect("a");
        }
        wal.sync().await.expect("sync");
        wal.truncate(25).await.expect("truncate");
        wal.close().await.expect("close");
    }
    {
        let wal = AsyncRaftWal::open(&path).await.expect("reopen");
        assert_eq!(wal.last_index(), Some(24));
        assert!(wal.get(25).is_none(), "truncated entry should not resurrect");
        assert!(wal.get(24).is_some(), "entry before truncate boundary should exist");
    }
}

// ========================================================================
// Rewrite active segment preserves all entries after compact
// ========================================================================

#[tokio::test]
async fn rewrite_active_preserves_entries_after_compact() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    {
        let mut wal = AsyncRaftWal::open(&path).await.expect("open");
        for i in 1..=20 {
            wal.append(i, format!("e{i}").as_bytes()).await.expect("a");
        }
        wal.sync().await.expect("sync");
        wal.compact(10).await.expect("compact");
        wal.close().await.expect("close");
    }
    {
        let wal = AsyncRaftWal::open(&path).await.expect("reopen");
        assert_eq!(wal.first_index(), Some(11));
        for i in 11..=20 {
            assert_eq!(
                wal.get(i).as_deref(),
                Some(format!("e{i}").as_bytes()),
                "entry {i} should survive"
            );
        }
    }
}
