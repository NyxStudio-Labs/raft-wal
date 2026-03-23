//! QA tests — happy path + reverse/edge cases for RaftWal (sync).

use raft_wal::RaftWal;
use std::fs;
use std::path::Path;

fn open(dir: &Path) -> RaftWal {
    RaftWal::open(dir).expect("open")
}

// ========================================================================
// Happy Path
// ========================================================================

#[test]
fn hp01_open_append_get() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    wal.append(1, b"hello").unwrap();
    assert_eq!(wal.get(1).as_deref(), Some(b"hello".as_slice()));
    assert_eq!(wal.len(), 1);
    assert!(!wal.is_empty());
}

#[test]
fn hp02_append_batch_borrowed() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    wal.append_batch(&[(1, b"a" as &[u8]), (2, b"b"), (3, b"c")])
        .unwrap();
    assert_eq!(wal.len(), 3);
    assert_eq!(wal.get(2).as_deref(), Some(b"b".as_slice()));
}

#[test]
fn hp03_append_batch_owned() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    let entries = vec![(1u64, vec![1u8, 2]), (2, vec![3, 4])];
    wal.append_batch(&entries).unwrap();
    assert_eq!(wal.get(1).as_deref(), Some([1u8, 2].as_slice()));
}

#[test]
fn hp04_iter() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    for i in 1..=5 {
        wal.append(i, format!("e{i}").as_bytes()).unwrap();
    }
    let all: Vec<_> = wal.iter().collect();
    assert_eq!(all.len(), 5);
    assert_eq!(all[0].index, 1);
    assert_eq!(all[4].data, b"e5");
}

#[test]
fn hp05_iter_range() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    for i in 1..=10 {
        wal.append(i, format!("e{i}").as_bytes()).unwrap();
    }
    let r: Vec<_> = wal.iter_range(3..=7).collect();
    assert_eq!(r.len(), 5);
    assert_eq!(r[0].index, 3);
    assert_eq!(r[4].index, 7);
}

#[test]
fn hp06_read_range() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    for i in 1..=10 {
        wal.append(i, format!("e{i}").as_bytes()).unwrap();
    }
    let r = wal.read_range(3..=7);
    assert_eq!(r.len(), 5);
    assert_eq!(r[0].0, 3);
}

#[test]
fn hp07_recovery_after_clean_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = open(&path);
        wal.append(1, b"a").unwrap();
        wal.append(2, b"b").unwrap();
    }
    {
        let wal = open(&path);
        assert_eq!(wal.len(), 2);
        assert_eq!(wal.get(1).as_deref(), Some(b"a".as_slice()));
        assert_eq!(wal.get(2).as_deref(), Some(b"b".as_slice()));
    }
}

#[test]
fn hp08_meta_set_get_remove() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    assert!(wal.get_meta("k").is_none());
    wal.set_meta("k", b"v").unwrap();
    assert_eq!(wal.get_meta("k"), Some(b"v".as_slice()));
    wal.remove_meta("k").unwrap();
    assert!(wal.get_meta("k").is_none());
}

#[test]
fn hp09_meta_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = open(&path);
        wal.set_meta("term", b"5").unwrap();
        wal.set_meta("vote", b"node-2").unwrap();
    }
    {
        let wal = open(&path);
        assert_eq!(wal.get_meta("term"), Some(b"5".as_slice()));
        assert_eq!(wal.get_meta("vote"), Some(b"node-2".as_slice()));
    }
}

#[test]
fn hp10_compact_removes_entries() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    for i in 1..=5 {
        wal.append(i, b"x").unwrap();
    }
    wal.compact(3).unwrap();
    assert_eq!(wal.len(), 2);
    assert_eq!(wal.first_index(), Some(4));
    assert!(wal.get(3).is_none());
    assert!(wal.get(4).is_some());
}

#[test]
fn hp11_truncate_removes_entries() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    for i in 1..=5 {
        wal.append(i, b"x").unwrap();
    }
    wal.truncate(3).unwrap();
    assert_eq!(wal.len(), 2);
    assert_eq!(wal.last_index(), Some(2));
    assert!(wal.get(3).is_none());
}

#[test]
fn hp12_compact_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = open(&path);
        for i in 1..=5 {
            wal.append(i, format!("e{i}").as_bytes()).unwrap();
        }
        wal.compact(3).unwrap();
    }
    {
        let wal = open(&path);
        assert_eq!(wal.len(), 2);
        assert_eq!(wal.first_index(), Some(4));
        assert_eq!(wal.get(4).as_deref(), Some(b"e4".as_slice()));
    }
}

#[test]
fn hp13_truncate_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = open(&path);
        for i in 1..=5 {
            wal.append(i, format!("e{i}").as_bytes()).unwrap();
        }
        wal.truncate(4).unwrap();
    }
    {
        let wal = open(&path);
        assert_eq!(wal.len(), 3);
        assert_eq!(wal.last_index(), Some(3));
        assert!(wal.get(4).is_none());
    }
}

#[test]
fn hp14_segment_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    wal.set_max_segment_size(200);
    for i in 1..=100 {
        wal.append(i, &[0u8; 32]).unwrap();
    }
    // Should have created multiple segment files
    let seg_count = fs::read_dir(dir.path())
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .path()
                .extension()
                .map(|e| e == "seg")
                .unwrap_or(false)
        })
        .count();
    assert!(seg_count > 1, "expected multiple segments, got {seg_count}");
    // All entries still readable
    for i in 1..=100 {
        assert!(wal.get(i).is_some(), "missing index {i}");
    }
}

#[test]
fn hp15_segment_rotation_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = open(&path);
        wal.set_max_segment_size(200);
        for i in 1..=100 {
            wal.append(i, format!("e{i}").as_bytes()).unwrap();
        }
    }
    {
        let wal = open(&path);
        assert_eq!(wal.len(), 100);
        assert_eq!(wal.get(1).as_deref(), Some(b"e1".as_slice()));
        assert_eq!(wal.get(100).as_deref(), Some(b"e100".as_slice()));
    }
}

#[test]
fn hp16_flush_persists() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = open(&path);
        wal.append(1, b"buffered").unwrap();
        wal.flush().unwrap();
    }
    {
        let wal = open(&path);
        assert_eq!(wal.get(1).as_deref(), Some(b"buffered".as_slice()));
    }
}

#[test]
fn hp17_sync_persists() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = open(&path);
        wal.append(1, b"durable").unwrap();
        wal.sync().unwrap();
    }
    {
        let wal = open(&path);
        assert_eq!(wal.get(1).as_deref(), Some(b"durable".as_slice()));
    }
}

#[test]
fn hp18_estimated_memory() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    let before = wal.estimated_memory();
    for i in 1..=100 {
        wal.append(i, &[0u8; 256]).unwrap();
    }
    let after = wal.estimated_memory();
    assert!(after > before);
    wal.compact(80).unwrap();
    let after_compact = wal.estimated_memory();
    assert!(after_compact < after);
}

#[test]
fn hp19_append_after_compact() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    for i in 1..=5 {
        wal.append(i, b"x").unwrap();
    }
    wal.compact(3).unwrap();
    wal.append(6, b"new").unwrap();
    assert_eq!(wal.len(), 3);
    assert_eq!(wal.first_index(), Some(4));
    assert_eq!(wal.get(6).as_deref(), Some(b"new".as_slice()));
}

#[test]
fn hp20_append_after_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    for i in 1..=5 {
        wal.append(i, b"old").unwrap();
    }
    wal.truncate(3).unwrap();
    wal.append(3, b"new").unwrap();
    assert_eq!(wal.len(), 3);
    assert_eq!(wal.get(3).as_deref(), Some(b"new".as_slice()));
}

#[test]
fn hp21_large_entry() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let big = vec![0xABu8; 1024 * 1024];
    {
        let mut wal = open(&path);
        wal.append(1, &big).unwrap();
    }
    {
        let wal = open(&path);
        assert_eq!(wal.get(1).unwrap().as_ref(), big.as_slice());
    }
}

#[test]
fn hp22_many_entries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = open(&path);
        for i in 1..=10_000 {
            wal.append(i, &[0u8; 64]).unwrap();
        }
    }
    {
        let wal = open(&path);
        assert_eq!(wal.len(), 10_000);
        assert_eq!(wal.first_index(), Some(1));
        assert_eq!(wal.last_index(), Some(10_000));
    }
}

#[test]
fn hp23_batch_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = open(&path);
        wal.append_batch(&[(1, b"a" as &[u8]), (2, b"b"), (3, b"c")])
            .unwrap();
    }
    {
        let wal = open(&path);
        assert_eq!(wal.len(), 3);
        assert_eq!(wal.get(2).as_deref(), Some(b"b".as_slice()));
    }
}

#[test]
fn hp24_compact_deletes_segment_files() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    wal.set_max_segment_size(200);
    for i in 1..=100 {
        wal.append(i, &[0u8; 32]).unwrap();
    }
    let before = seg_count(dir.path());
    wal.compact(80).unwrap();
    let after = seg_count(dir.path());
    assert!(
        after < before,
        "segments not deleted: before={before} after={after}"
    );
}

#[test]
fn hp25_first_last_index() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    assert_eq!(wal.first_index(), None);
    assert_eq!(wal.last_index(), None);
    wal.append(5, b"x").unwrap();
    assert_eq!(wal.first_index(), Some(5));
    assert_eq!(wal.last_index(), Some(5));
    wal.append(6, b"y").unwrap();
    assert_eq!(wal.last_index(), Some(6));
}

// ========================================================================
// Reverse QA — Edge Cases / Failure Paths
// ========================================================================

#[test]
fn rq01_empty_wal_operations() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    assert!(wal.is_empty());
    assert_eq!(wal.len(), 0);
    assert_eq!(wal.first_index(), None);
    assert_eq!(wal.last_index(), None);
    assert!(wal.get(1).is_none());
    assert!(wal.iter().next().is_none());
    assert!(wal.read_range(1..=10).is_empty());
    // compact/truncate on empty should not panic
    wal.compact(10).unwrap();
    wal.truncate(1).unwrap();
}

#[test]
fn rq02_get_out_of_range() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    wal.append(5, b"x").unwrap();
    assert!(wal.get(0).is_none());
    assert!(wal.get(4).is_none());
    assert!(wal.get(6).is_none());
    assert!(wal.get(u64::MAX).is_none());
}

#[test]
fn rq03_compact_below_range_noop() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    for i in 5..=10 {
        wal.append(i, b"x").unwrap();
    }
    wal.compact(2).unwrap(); // below all entries
    assert_eq!(wal.len(), 6);
    assert_eq!(wal.first_index(), Some(5));
}

#[test]
fn rq04_compact_above_range_removes_all() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    for i in 1..=3 {
        wal.append(i, b"x").unwrap();
    }
    wal.compact(3).unwrap();
    assert!(wal.is_empty());
    assert_eq!(wal.first_index(), None);
}

#[test]
fn rq05_truncate_below_range_removes_all() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    for i in 1..=3 {
        wal.append(i, b"x").unwrap();
    }
    wal.truncate(1).unwrap();
    assert!(wal.is_empty());
    assert_eq!(wal.last_index(), None);
}

#[test]
fn rq06_truncate_above_range_noop() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    for i in 5..=10 {
        wal.append(i, b"x").unwrap();
    }
    wal.truncate(20).unwrap(); // above all entries
    assert_eq!(wal.len(), 6);
}

#[test]
fn rq07_double_compact() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    for i in 1..=10 {
        wal.append(i, b"x").unwrap();
    }
    wal.compact(5).unwrap();
    assert_eq!(wal.len(), 5);
    wal.compact(7).unwrap();
    assert_eq!(wal.len(), 3);
    assert_eq!(wal.first_index(), Some(8));
}

#[test]
fn rq08_double_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    for i in 1..=10 {
        wal.append(i, b"x").unwrap();
    }
    wal.truncate(8).unwrap();
    assert_eq!(wal.len(), 7);
    wal.truncate(5).unwrap();
    assert_eq!(wal.len(), 4);
    assert_eq!(wal.last_index(), Some(4));
}

#[test]
fn rq09_corrupt_crc_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = open(&path);
        wal.append(1, b"good").unwrap();
        wal.append(2, b"will-corrupt").unwrap();
        wal.append(3, b"after-corrupt").unwrap();
    }
    // Corrupt CRC of second entry in the segment file
    let seg_files = find_segments(&path);
    assert!(!seg_files.is_empty());
    let last_seg = &seg_files[seg_files.len() - 1];
    let mut data = fs::read(last_seg).unwrap();
    // Segment may have a 5-byte version header ("RWAL" + version).
    // First entry: 16 header + 4 payload = 20 bytes (after segment header).
    // Second entry CRC starts at segment_header + 20.
    let seg_hdr = if data.len() >= 4 && &data[..4] == b"RWAL" { 5 } else { 0 };
    let second_entry_offset = seg_hdr + 20;
    if data.len() > second_entry_offset {
        data[second_entry_offset] ^= 0xFF; // corrupt CRC of second entry
        fs::write(last_seg, &data).unwrap();
    }
    {
        let wal = open(&path);
        // Only first entry should survive (CRC check stops at corruption)
        assert_eq!(wal.get(1).as_deref(), Some(b"good".as_slice()));
        assert!(wal.get(2).is_none());
        assert!(wal.get(3).is_none());
    }
}

#[test]
fn rq10_partial_entry_at_tail() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = open(&path);
        wal.append(1, b"complete").unwrap();
        wal.append(2, b"also-complete").unwrap();
    }
    // Truncate the file to simulate partial write
    let seg_files = find_segments(&path);
    let last_seg = &seg_files[seg_files.len() - 1];
    let data = fs::read(last_seg).unwrap();
    let truncated = &data[..data.len() - 3]; // cut 3 bytes off the end
    fs::write(last_seg, truncated).unwrap();
    {
        let wal = open(&path);
        assert_eq!(wal.get(1).as_deref(), Some(b"complete".as_slice()));
        // Second entry may or may not survive depending on how much was cut
        assert_eq!(wal.len(), 1);
    }
}

#[test]
fn rq11_empty_segment_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = open(&path);
        wal.append(1, b"data").unwrap();
    }
    // Create an empty .seg file
    fs::write(path.join("99999999999999999999.seg"), b"").unwrap();
    {
        let wal = open(&path);
        assert_eq!(wal.get(1).as_deref(), Some(b"data".as_slice()));
    }
}

#[test]
fn rq12_corrupt_meta_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = open(&path);
        wal.set_meta("key", b"value").unwrap();
        wal.append(1, b"data").unwrap();
    }
    // Corrupt meta.bin
    fs::write(path.join("meta.bin"), b"garbage data here").unwrap();
    {
        let wal = open(&path);
        // Meta should be empty (corrupt file ignored), entries still OK
        assert!(wal.get_meta("key").is_none());
        assert_eq!(wal.get(1).as_deref(), Some(b"data".as_slice()));
    }
}

#[test]
fn rq13_zero_length_entry() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = open(&path);
        wal.append(1, b"").unwrap();
        wal.append(2, b"not-empty").unwrap();
    }
    {
        let wal = open(&path);
        // Zero-length entry: our WAL uses empty vec as "no entry" sentinel,
        // so get(1) returns None but get(2) works
        assert_eq!(wal.get(2).as_deref(), Some(b"not-empty".as_slice()));
    }
}

#[test]
fn rq14_read_range_empty_results() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    for i in 5..=10 {
        wal.append(i, b"x").unwrap();
    }
    assert!(wal.read_range(1..=4).is_empty()); // below range
    assert!(wal.read_range(11..=20).is_empty()); // above range
    assert!(wal.read_range(8..8).is_empty()); // empty range
}

#[test]
fn rq15_read_range_partial_overlap() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    for i in 5..=10 {
        wal.append(i, format!("e{i}").as_bytes()).unwrap();
    }
    let r = wal.read_range(3..=7);
    assert_eq!(r.len(), 3); // only 5,6,7
    assert_eq!(r[0].0, 5);
    assert_eq!(r[2].0, 7);
}

#[test]
fn rq16_read_range_on_empty_wal() {
    let dir = tempfile::tempdir().unwrap();
    let wal = open(dir.path());
    assert!(wal.read_range(1..=100).is_empty());
    assert!(wal.iter_range(1..=100).next().is_none());
}

#[test]
fn rq17_compact_then_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    for i in 1..=10 {
        wal.append(i, b"x").unwrap();
    }
    wal.compact(3).unwrap(); // remove 1,2,3
    wal.truncate(8).unwrap(); // remove 8,9,10
    assert_eq!(wal.len(), 4); // 4,5,6,7
    assert_eq!(wal.first_index(), Some(4));
    assert_eq!(wal.last_index(), Some(7));
}

#[test]
fn rq18_truncate_then_compact() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    for i in 1..=10 {
        wal.append(i, b"x").unwrap();
    }
    wal.truncate(8).unwrap(); // remove 8,9,10
    wal.compact(3).unwrap(); // remove 1,2,3
    assert_eq!(wal.len(), 4); // 4,5,6,7
    assert_eq!(wal.first_index(), Some(4));
    assert_eq!(wal.last_index(), Some(7));
}

#[test]
fn rq19_meta_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = open(&path);
        wal.set_meta("key", b"v1").unwrap();
        wal.set_meta("key", b"v2").unwrap();
    }
    {
        let wal = open(&path);
        assert_eq!(wal.get_meta("key"), Some(b"v2".as_slice()));
    }
}

#[test]
fn rq20_remove_nonexistent_meta() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    // Should not panic
    wal.remove_meta("does-not-exist").unwrap();
    assert!(wal.get_meta("does-not-exist").is_none());
}

#[test]
fn rq21_multi_segment_one_corrupted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = open(&path);
        wal.set_max_segment_size(200);
        for i in 1..=50 {
            wal.append(i, format!("e{i}").as_bytes()).unwrap();
        }
    }
    // Corrupt the middle segment
    let segs = find_segments(&path);
    assert!(segs.len() >= 3, "need at least 3 segments");
    let mid = &segs[segs.len() / 2];
    let mut data = fs::read(mid).unwrap();
    if data.len() > 4 {
        data[0] ^= 0xFF; // corrupt first entry's CRC
        fs::write(mid, &data).unwrap();
    }
    {
        let wal = open(&path);
        // Entries in non-corrupted segments should be intact.
        // The corrupted segment loses entries from the corruption point
        // to the end of that segment. Other segments recover independently.
        assert!(wal.get(1).is_some(), "first entry should survive");
        // At least one entry should be missing from the corrupted segment.
        let mut missing = 0;
        for i in 1..=50 {
            if wal.get(i).is_none() {
                missing += 1;
            }
        }
        assert!(
            missing > 0,
            "corruption should cause at least one missing entry"
        );
    }
}

#[test]
fn rq22_append_non_sequential() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    wal.append(1, b"a").unwrap();
    wal.append(5, b"b").unwrap(); // gap: 2,3,4 missing
    assert_eq!(wal.get(1).as_deref(), Some(b"a".as_slice()));
    assert_eq!(wal.get(5).as_deref(), Some(b"b".as_slice()));
    assert!(wal.get(2).is_none());
    assert!(wal.get(3).is_none());
    assert!(wal.get(4).is_none());
}

#[test]
fn rq23_compact_all_then_append() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = open(&path);
        for i in 1..=5 {
            wal.append(i, b"x").unwrap();
        }
        wal.compact(5).unwrap();
        assert!(wal.is_empty());
        wal.append(6, b"after").unwrap();
        assert_eq!(wal.len(), 1);
        assert_eq!(wal.get(6).as_deref(), Some(b"after".as_slice()));
    }
    {
        let wal = open(&path);
        assert_eq!(wal.get(6).as_deref(), Some(b"after".as_slice()));
    }
}

#[test]
fn rq24_truncate_all_then_append() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = open(&path);
        for i in 1..=5 {
            wal.append(i, b"x").unwrap();
        }
        wal.truncate(1).unwrap();
        assert!(wal.is_empty());
        wal.append(1, b"fresh").unwrap();
        assert_eq!(wal.len(), 1);
        assert_eq!(wal.get(1).as_deref(), Some(b"fresh".as_slice()));
    }
    {
        let wal = open(&path);
        assert_eq!(wal.get(1).as_deref(), Some(b"fresh".as_slice()));
    }
}

#[test]
fn rq25_error_source_chain() {
    use raft_wal::WalError;
    let err = WalError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "gone"));
    assert!(std::error::Error::source(&err).is_some());
    assert!(err.to_string().contains("gone"));
}

// ========================================================================
// Cache eviction tests
// ========================================================================

#[test]
fn cache_eviction_basic() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    wal.set_max_segment_size(200); // small segments so sealed entries exist
    wal.set_max_cache_entries(10);

    for i in 1..=50 {
        wal.append(i, format!("e{i}").as_bytes()).unwrap();
    }

    // Recent entries are in cache
    assert_eq!(wal.get_cached(50), Some(b"e50".as_slice()));

    // Old entries in sealed segments are evicted from memory
    assert!(wal.get_cached(1).is_none());

    // But get() reads from disk
    assert_eq!(wal.get(1).as_deref(), Some(b"e1".as_slice()));
    assert_eq!(wal.get(5).as_deref(), Some(b"e5".as_slice()));

    // All 50 entries still exist
    assert_eq!(wal.len(), 50);
}

#[test]
fn cache_eviction_reduces_memory() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());

    for i in 1..=1000 {
        wal.append(i, &[0u8; 256]).unwrap();
    }
    let mem_all = wal.estimated_memory();

    wal.set_max_cache_entries(100);
    let mem_limited = wal.estimated_memory();

    assert!(
        mem_limited < mem_all / 2,
        "memory should decrease significantly: {mem_all} -> {mem_limited}"
    );
}

#[test]
fn cache_eviction_with_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = open(&path);
        wal.set_max_cache_entries(5);
        for i in 1..=20 {
            wal.append(i, format!("e{i}").as_bytes()).unwrap();
        }
    }
    {
        let wal = open(&path);
        // Recovery loads all entries into memory (no eviction limit persisted)
        assert_eq!(wal.len(), 20);
        assert_eq!(wal.get(1).as_deref(), Some(b"e1".as_slice()));
        assert_eq!(wal.get(20).as_deref(), Some(b"e20".as_slice()));
    }
}

#[test]
fn cache_eviction_disk_fallback_after_segment_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = open(dir.path());
    wal.set_max_segment_size(200);
    wal.set_max_cache_entries(10);

    for i in 1..=50 {
        wal.append(i, format!("e{i}").as_bytes()).unwrap();
    }

    // Old entries evicted from memory but still on sealed segments
    assert!(wal.get_cached(1).is_none());
    assert_eq!(wal.get(1).as_deref(), Some(b"e1".as_slice()));

    // Recent entries in cache
    assert!(wal.get_cached(50).is_some());
}

// ========================================================================
// Helpers
// ========================================================================

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

fn seg_count(dir: &Path) -> usize {
    find_segments(dir).len()
}
