#![allow(clippy::pedantic)]
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use raft_wal::RaftWal;

fn bench_append(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = RaftWal::open(dir.path()).expect("open");
    let payload = vec![0u8; 128];
    let mut idx = 1u64;

    c.bench_function("append_128b", |b| {
        b.iter(|| {
            wal.append(idx, black_box(&payload)).expect("append");
            idx += 1;
        })
    });
}

fn bench_append_sync(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = RaftWal::open(dir.path()).expect("open");
    let payload = vec![0u8; 128];
    let mut idx = 1u64;

    c.bench_function("append+sync_128b", |b| {
        b.iter(|| {
            wal.append(idx, black_box(&payload)).expect("append");
            wal.sync().expect("sync");
            idx += 1;
        })
    });
}

fn bench_append_batch_sync(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = RaftWal::open(dir.path()).expect("open");
    let payload = vec![0u8; 128];
    let mut base = 1u64;

    c.bench_function("append_batch_10+sync", |b| {
        b.iter(|| {
            let entries: Vec<(u64, &[u8])> =
                (0..10).map(|i| (base + i, payload.as_slice())).collect();
            wal.append_batch(black_box(&entries)).expect("batch");
            wal.sync().expect("sync");
            base += 10;
        })
    });
}

fn bench_append_batch(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = RaftWal::open(dir.path()).expect("open");
    let payload = vec![0u8; 128];
    let mut base = 1u64;

    c.bench_function("append_batch_10x128b", |b| {
        b.iter(|| {
            let entries: Vec<(u64, &[u8])> =
                (0..10).map(|i| (base + i, payload.as_slice())).collect();
            wal.append_batch(black_box(&entries)).expect("batch");
            base += 10;
        })
    });
}

fn bench_get(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = RaftWal::open(dir.path()).expect("open");
    for i in 1..=10_000 {
        wal.append(i, &[0u8; 128]).expect("append");
    }

    c.bench_function("get", |b| {
        b.iter(|| {
            let _ = wal.get(black_box(5_000));
        })
    });
}

fn bench_read_range(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = RaftWal::open(dir.path()).expect("open");
    for i in 1..=10_000 {
        wal.append(i, &[0u8; 128]).expect("append");
    }

    c.bench_function("read_range_100", |b| {
        b.iter(|| {
            let _ = wal.read_range(black_box(4_950..=5_050));
        })
    });
}

fn bench_recovery(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    {
        let mut wal = RaftWal::open(&path).expect("open");
        for i in 1..=10_000 {
            wal.append(i, &[0u8; 128]).expect("append");
        }
    }

    c.bench_function("recovery_10k_1seg", |b| {
        b.iter(|| {
            RaftWal::open(black_box(&path)).expect("open");
        })
    });
}

fn bench_recovery_multi_segment(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    {
        let mut wal = RaftWal::open(&path).expect("open");
        // Force small segments to create many files
        wal.set_max_segment_size(4096);
        for i in 1..=10_000 {
            wal.append(i, &[0u8; 128]).expect("append");
        }
    }

    c.bench_function("recovery_10k_multi_seg", |b| {
        b.iter(|| {
            RaftWal::open(black_box(&path)).expect("open");
        })
    });
}

criterion_group!(
    benches,
    bench_append,
    bench_append_sync,
    bench_append_batch_sync,
    bench_append_batch,
    bench_get,
    bench_read_range,
    bench_recovery,
    bench_recovery_multi_segment,
);
criterion_main!(benches);
