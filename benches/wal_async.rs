#![allow(clippy::pedantic)]
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use raft_wal::AsyncRaftWal;

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

fn bench_async_append(c: &mut Criterion) {
    let rt = rt();
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = rt.block_on(AsyncRaftWal::open(dir.path())).expect("open");
    let payload = vec![0u8; 128];
    let mut idx = 1u64;

    c.bench_function("async_append_128b", |b| {
        b.iter(|| {
            rt.block_on(wal.append(idx, black_box(&payload)))
                .expect("append");
            idx += 1;
        })
    });
}

fn bench_async_append_batch(c: &mut Criterion) {
    let rt = rt();
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = rt.block_on(AsyncRaftWal::open(dir.path())).expect("open");
    let payload = vec![0u8; 128];
    let mut base = 1u64;

    c.bench_function("async_append_batch_10x128b", |b| {
        b.iter(|| {
            let entries: Vec<(u64, &[u8])> =
                (0..10).map(|i| (base + i, payload.as_slice())).collect();
            rt.block_on(wal.append_batch(black_box(&entries)))
                .expect("batch");
            base += 10;
        })
    });
}

fn bench_async_get(c: &mut Criterion) {
    let rt = rt();
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = rt.block_on(AsyncRaftWal::open(dir.path())).expect("open");
    for i in 1..=10_000 {
        rt.block_on(wal.append(i, &[0u8; 128])).expect("append");
    }

    c.bench_function("async_get", |b| {
        b.iter(|| {
            let _ = wal.get(black_box(5_000));
        })
    });
}

fn bench_async_recovery(c: &mut Criterion) {
    let rt = rt();
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    {
        let mut wal = rt.block_on(AsyncRaftWal::open(&path)).expect("open");
        for i in 1..=10_000 {
            rt.block_on(wal.append(i, &[0u8; 128])).expect("append");
        }
        rt.block_on(wal.close()).expect("close");
    }

    c.bench_function("async_recovery_10k", |b| {
        b.iter(|| {
            rt.block_on(AsyncRaftWal::open(black_box(&path)))
                .expect("open");
        })
    });
}

criterion_group!(
    benches,
    bench_async_append,
    bench_async_append_batch,
    bench_async_get,
    bench_async_recovery,
);
criterion_main!(benches);
