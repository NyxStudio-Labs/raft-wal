#![cfg(feature = "tokio")]
//! QA tests for AsyncRaftWal (tokio).

use raft_wal::AsyncRaftWal;
use std::fs;
use std::path::Path;

fn find_segments(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut segs: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("seg"))
        .collect();
    segs.sort();
    segs
}

// ========================================================================
// Happy Path
// ========================================================================

#[tokio::test]
async fn async_hp01_open_append_get() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = AsyncRaftWal::open(dir.path()).await.unwrap();
    wal.append(1, b"hello").await.unwrap();
    assert_eq!(wal.get(1).as_deref(), Some(b"hello".as_slice()));
    assert_eq!(wal.len(), 1);
}

#[tokio::test]
async fn async_hp02_append_batch_borrowed() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = AsyncRaftWal::open(dir.path()).await.unwrap();
    wal.append_batch(&[(1, b"a" as &[u8]), (2, b"b"), (3, b"c")])
        .await
        .unwrap();
    assert_eq!(wal.len(), 3);
    assert_eq!(wal.get(2).as_deref(), Some(b"b".as_slice()));
}

#[tokio::test]
async fn async_hp03_append_batch_owned() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = AsyncRaftWal::open(dir.path()).await.unwrap();
    let entries = vec![(1u64, vec![1u8, 2]), (2, vec![3, 4])];
    wal.append_batch(&entries).await.unwrap();
    assert_eq!(wal.get(1).as_deref(), Some([1u8, 2].as_slice()));
}

#[tokio::test]
async fn async_hp04_iter() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = AsyncRaftWal::open(dir.path()).await.unwrap();
    for i in 1..=5 {
        wal.append(i, format!("e{i}").as_bytes()).await.unwrap();
    }
    let all: Vec<_> = wal.iter().collect();
    assert_eq!(all.len(), 5);
    assert_eq!(all[0].index, 1);
    assert_eq!(all[4].data, b"e5");
}

#[tokio::test]
async fn async_hp05_iter_range() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = AsyncRaftWal::open(dir.path()).await.unwrap();
    for i in 1..=10 {
        wal.append(i, format!("e{i}").as_bytes()).await.unwrap();
    }
    let r: Vec<_> = wal.iter_range(3..=7).collect();
    assert_eq!(r.len(), 5);
    assert_eq!(r[0].index, 3);
}

#[tokio::test]
async fn async_hp06_read_range() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = AsyncRaftWal::open(dir.path()).await.unwrap();
    for i in 1..=10 {
        wal.append(i, format!("e{i}").as_bytes()).await.unwrap();
    }
    let r = wal.read_range(3..=7);
    assert_eq!(r.len(), 5);
}

#[tokio::test]
async fn async_hp07_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = AsyncRaftWal::open(&path).await.unwrap();
        wal.append(1, b"a").await.unwrap();
        wal.append(2, b"b").await.unwrap();
        wal.close().await.unwrap();
    }
    {
        let wal = AsyncRaftWal::open(&path).await.unwrap();
        assert_eq!(wal.len(), 2);
        assert_eq!(wal.get(2).as_deref(), Some(b"b".as_slice()));
    }
}

#[tokio::test]
async fn async_hp08_meta() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = AsyncRaftWal::open(&path).await.unwrap();
        wal.set_meta("term", b"5").await.unwrap();
        wal.set_meta("vote", b"node-2").await.unwrap();
        wal.close().await.unwrap();
    }
    {
        let wal = AsyncRaftWal::open(&path).await.unwrap();
        assert_eq!(wal.get_meta("term"), Some(b"5".as_slice()));
        assert_eq!(wal.get_meta("vote"), Some(b"node-2".as_slice()));
    }
}

#[tokio::test]
async fn async_hp09_compact() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = AsyncRaftWal::open(dir.path()).await.unwrap();
    for i in 1..=5 {
        wal.append(i, b"x").await.unwrap();
    }
    wal.compact(3).await.unwrap();
    assert_eq!(wal.len(), 2);
    assert_eq!(wal.first_index(), Some(4));
}

#[tokio::test]
async fn async_hp10_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = AsyncRaftWal::open(dir.path()).await.unwrap();
    for i in 1..=5 {
        wal.append(i, b"x").await.unwrap();
    }
    wal.truncate(3).await.unwrap();
    assert_eq!(wal.len(), 2);
    assert_eq!(wal.last_index(), Some(2));
}

#[tokio::test]
async fn async_hp11_compact_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = AsyncRaftWal::open(&path).await.unwrap();
        for i in 1..=5 {
            wal.append(i, format!("e{i}").as_bytes()).await.unwrap();
        }
        wal.compact(3).await.unwrap();
        wal.close().await.unwrap();
    }
    {
        let wal = AsyncRaftWal::open(&path).await.unwrap();
        assert_eq!(wal.len(), 2);
        assert_eq!(wal.first_index(), Some(4));
        assert_eq!(wal.get(4).as_deref(), Some(b"e4".as_slice()));
    }
}

#[tokio::test]
async fn async_hp12_flush() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = AsyncRaftWal::open(&path).await.unwrap();
        wal.append(1, b"buffered").await.unwrap();
        wal.flush().await.unwrap();
    }
    {
        let wal = AsyncRaftWal::open(&path).await.unwrap();
        assert_eq!(wal.get(1).as_deref(), Some(b"buffered".as_slice()));
    }
}

#[tokio::test]
async fn async_hp13_sync() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = AsyncRaftWal::open(&path).await.unwrap();
        wal.append(1, b"durable").await.unwrap();
        wal.sync().await.unwrap();
    }
    {
        let wal = AsyncRaftWal::open(&path).await.unwrap();
        assert_eq!(wal.get(1).as_deref(), Some(b"durable".as_slice()));
    }
}

#[tokio::test]
async fn async_hp14_estimated_memory() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = AsyncRaftWal::open(dir.path()).await.unwrap();
    let before = wal.estimated_memory();
    for i in 1..=100 {
        wal.append(i, &[0u8; 256]).await.unwrap();
    }
    assert!(wal.estimated_memory() > before);
}

#[tokio::test]
async fn async_hp15_close() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = AsyncRaftWal::open(&path).await.unwrap();
        wal.append(1, b"closed").await.unwrap();
        wal.close().await.unwrap();
    }
    {
        let wal = AsyncRaftWal::open(&path).await.unwrap();
        assert_eq!(wal.get(1).as_deref(), Some(b"closed".as_slice()));
    }
}

#[tokio::test]
async fn async_hp16_append_after_compact() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = AsyncRaftWal::open(dir.path()).await.unwrap();
    for i in 1..=5 {
        wal.append(i, b"x").await.unwrap();
    }
    wal.compact(3).await.unwrap();
    wal.append(6, b"new").await.unwrap();
    assert_eq!(wal.len(), 3);
    assert_eq!(wal.get(6).as_deref(), Some(b"new".as_slice()));
}

#[tokio::test]
async fn async_hp17_append_after_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = AsyncRaftWal::open(dir.path()).await.unwrap();
    for i in 1..=5 {
        wal.append(i, b"old").await.unwrap();
    }
    wal.truncate(3).await.unwrap();
    wal.append(3, b"new").await.unwrap();
    assert_eq!(wal.get(3).as_deref(), Some(b"new".as_slice()));
}

#[tokio::test]
async fn async_hp18_large_entry() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let big = vec![0xABu8; 1024 * 1024];
    {
        let mut wal = AsyncRaftWal::open(&path).await.unwrap();
        wal.append(1, &big).await.unwrap();
        wal.close().await.unwrap();
    }
    {
        let wal = AsyncRaftWal::open(&path).await.unwrap();
        assert_eq!(wal.get(1).unwrap().as_ref(), big.as_slice());
    }
}

// ========================================================================
// Reverse QA
// ========================================================================

#[tokio::test]
async fn async_rq01_empty_wal() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = AsyncRaftWal::open(dir.path()).await.unwrap();
    assert!(wal.is_empty());
    assert_eq!(wal.first_index(), None);
    assert_eq!(wal.last_index(), None);
    assert!(wal.get(1).is_none());
    wal.compact(10).await.unwrap();
    wal.truncate(1).await.unwrap();
}

#[tokio::test]
async fn async_rq02_get_out_of_range() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = AsyncRaftWal::open(dir.path()).await.unwrap();
    wal.append(5, b"x").await.unwrap();
    assert!(wal.get(0).is_none());
    assert!(wal.get(4).is_none());
    assert!(wal.get(6).is_none());
}

#[tokio::test]
async fn async_rq03_compact_noop() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = AsyncRaftWal::open(dir.path()).await.unwrap();
    for i in 5..=10 {
        wal.append(i, b"x").await.unwrap();
    }
    wal.compact(2).await.unwrap();
    assert_eq!(wal.len(), 6);
}

#[tokio::test]
async fn async_rq04_truncate_noop() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = AsyncRaftWal::open(dir.path()).await.unwrap();
    for i in 5..=10 {
        wal.append(i, b"x").await.unwrap();
    }
    wal.truncate(20).await.unwrap();
    assert_eq!(wal.len(), 6);
}

#[tokio::test]
async fn async_rq05_double_compact() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = AsyncRaftWal::open(dir.path()).await.unwrap();
    for i in 1..=10 {
        wal.append(i, b"x").await.unwrap();
    }
    wal.compact(5).await.unwrap();
    wal.compact(7).await.unwrap();
    assert_eq!(wal.len(), 3);
    assert_eq!(wal.first_index(), Some(8));
}

#[tokio::test]
async fn async_rq06_double_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = AsyncRaftWal::open(dir.path()).await.unwrap();
    for i in 1..=10 {
        wal.append(i, b"x").await.unwrap();
    }
    wal.truncate(8).await.unwrap();
    wal.truncate(5).await.unwrap();
    assert_eq!(wal.len(), 4);
    assert_eq!(wal.last_index(), Some(4));
}

#[tokio::test]
async fn async_rq07_corrupt_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = AsyncRaftWal::open(&path).await.unwrap();
        wal.append(1, b"good").await.unwrap();
        wal.append(2, b"will-corrupt").await.unwrap();
        wal.close().await.unwrap();
    }
    let segs = find_segments(&path);
    let last_seg = &segs[segs.len() - 1];
    let mut data = fs::read(last_seg).unwrap();
    // Account for segment version header (5 bytes: "RWAL" + version)
    let seg_hdr = if data.len() >= 4 && &data[..4] == b"RWAL" { 5 } else { 0 };
    let second_entry_offset = seg_hdr + 20; // first entry: 16 hdr + 4 payload
    if data.len() > second_entry_offset {
        data[second_entry_offset] ^= 0xFF;
        fs::write(last_seg, &data).unwrap();
    }
    {
        let wal = AsyncRaftWal::open(&path).await.unwrap();
        assert_eq!(wal.get(1).as_deref(), Some(b"good".as_slice()));
        assert!(wal.get(2).is_none());
    }
}

#[tokio::test]
async fn async_rq08_meta_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = AsyncRaftWal::open(&path).await.unwrap();
        wal.set_meta("k", b"v1").await.unwrap();
        wal.set_meta("k", b"v2").await.unwrap();
        wal.close().await.unwrap();
    }
    {
        let wal = AsyncRaftWal::open(&path).await.unwrap();
        assert_eq!(wal.get_meta("k"), Some(b"v2".as_slice()));
    }
}

#[tokio::test]
async fn async_rq09_remove_nonexistent_meta() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = AsyncRaftWal::open(dir.path()).await.unwrap();
    wal.remove_meta("nope").await.unwrap();
    assert!(wal.get_meta("nope").is_none());
}

#[tokio::test]
async fn async_rq10_compact_all_then_append() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = AsyncRaftWal::open(dir.path()).await.unwrap();
    for i in 1..=5 {
        wal.append(i, b"x").await.unwrap();
    }
    wal.compact(5).await.unwrap();
    assert!(wal.is_empty());
    wal.append(6, b"after").await.unwrap();
    assert_eq!(wal.get(6).as_deref(), Some(b"after".as_slice()));
}

#[tokio::test]
async fn async_rq11_truncate_all_then_append() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = AsyncRaftWal::open(dir.path()).await.unwrap();
    for i in 1..=5 {
        wal.append(i, b"x").await.unwrap();
    }
    wal.truncate(1).await.unwrap();
    assert!(wal.is_empty());
    wal.append(1, b"fresh").await.unwrap();
    assert_eq!(wal.get(1).as_deref(), Some(b"fresh".as_slice()));
}

#[tokio::test]
async fn async_rq12_compact_then_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = AsyncRaftWal::open(dir.path()).await.unwrap();
    for i in 1..=10 {
        wal.append(i, b"x").await.unwrap();
    }
    wal.compact(3).await.unwrap();
    wal.truncate(8).await.unwrap();
    assert_eq!(wal.len(), 4);
    assert_eq!(wal.first_index(), Some(4));
    assert_eq!(wal.last_index(), Some(7));
}
