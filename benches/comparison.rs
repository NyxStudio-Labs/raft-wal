//! Comparison benchmarks: raft-wal vs sled vs redb vs rocksdb
//!
//! All benchmarks simulate Raft WAL usage patterns:
//! - Sequential append with u64 index key
//! - Point get by index
//! - Recovery (open with 10k existing entries)

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};

const PAYLOAD: [u8; 128] = [0u8; 128];

// ========================================================================
// raft-wal
// ========================================================================

fn raft_wal_append(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = raft_wal::RaftWal::open(dir.path()).unwrap();
    let mut idx = 1u64;

    c.bench_function("append/raft-wal", |b| {
        b.iter(|| {
            wal.append(idx, black_box(&PAYLOAD)).unwrap();
            idx += 1;
        })
    });
}

fn raft_wal_append_sync(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = raft_wal::RaftWal::open(dir.path()).unwrap();
    let mut idx = 1u64;

    c.bench_function("append+sync/raft-wal", |b| {
        b.iter(|| {
            wal.append(idx, black_box(&PAYLOAD)).unwrap();
            wal.sync().unwrap();
            idx += 1;
        })
    });
}

fn raft_wal_get(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = raft_wal::RaftWal::open(dir.path()).unwrap();
    for i in 1..=10_000 {
        wal.append(i, &PAYLOAD).unwrap();
    }

    c.bench_function("get/raft-wal", |b| {
        b.iter(|| {
            wal.get(black_box(5_000));
        })
    });
}

fn raft_wal_recovery(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut wal = raft_wal::RaftWal::open(&path).unwrap();
        for i in 1..=10_000 {
            wal.append(i, &PAYLOAD).unwrap();
        }
    }

    c.bench_function("recovery/raft-wal", |b| {
        b.iter(|| {
            raft_wal::RaftWal::open(black_box(&path)).unwrap();
        })
    });
}

// ========================================================================
// sled
// ========================================================================

fn sled_append(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let db = sled::open(dir.path()).unwrap();
    let mut idx = 1u64;

    c.bench_function("append/sled", |b| {
        b.iter(|| {
            db.insert(idx.to_be_bytes(), black_box(&PAYLOAD[..]))
                .unwrap();
            idx += 1;
        })
    });
}

fn sled_get(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let db = sled::open(dir.path()).unwrap();
    for i in 1u64..=10_000 {
        db.insert(i.to_be_bytes(), &PAYLOAD[..]).unwrap();
    }

    c.bench_function("get/sled", |b| {
        b.iter(|| {
            db.get(black_box(5_000u64).to_be_bytes()).unwrap();
        })
    });
}

fn sled_recovery(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = sled::open(&path).unwrap();
        for i in 1u64..=10_000 {
            db.insert(i.to_be_bytes(), &PAYLOAD[..]).unwrap();
        }
        db.flush().unwrap();
    }

    c.bench_function("recovery/sled", |b| {
        b.iter(|| {
            sled::open(black_box(&path)).unwrap();
        })
    });
}

// ========================================================================
// redb
// ========================================================================

fn redb_append(c: &mut Criterion) {
    use redb::TableDefinition;
    const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("wal");

    let dir = tempfile::tempdir().unwrap();
    let db = redb::Database::create(dir.path().join("redb")).unwrap();
    let mut idx = 1u64;

    c.bench_function("append/redb", |b| {
        b.iter(|| {
            let tx = db.begin_write().unwrap();
            {
                let mut table = tx.open_table(TABLE).unwrap();
                table.insert(idx, black_box(&PAYLOAD[..])).unwrap();
            }
            tx.commit().unwrap();
            idx += 1;
        })
    });
}

fn redb_get(c: &mut Criterion) {
    use redb::TableDefinition;
    const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("wal");

    let dir = tempfile::tempdir().unwrap();
    let db = redb::Database::create(dir.path().join("redb")).unwrap();
    {
        let tx = db.begin_write().unwrap();
        {
            let mut table = tx.open_table(TABLE).unwrap();
            for i in 1u64..=10_000 {
                table.insert(i, &PAYLOAD[..]).unwrap();
            }
        }
        tx.commit().unwrap();
    }

    c.bench_function("get/redb", |b| {
        b.iter(|| {
            use redb::ReadableDatabase;
            let tx = db.begin_read().unwrap();
            let table = tx.open_table(TABLE).unwrap();
            table.get(black_box(5_000u64)).unwrap();
        })
    });
}

fn redb_recovery(c: &mut Criterion) {
    use redb::TableDefinition;
    const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("wal");

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("redb");
    {
        let db = redb::Database::create(&path).unwrap();
        let tx = db.begin_write().unwrap();
        {
            let mut table = tx.open_table(TABLE).unwrap();
            for i in 1u64..=10_000 {
                table.insert(i, &PAYLOAD[..]).unwrap();
            }
        }
        tx.commit().unwrap();
    }

    c.bench_function("recovery/redb", |b| {
        b.iter(|| {
            redb::Database::open(black_box(&path)).unwrap();
        })
    });
}

// ========================================================================
// rocksdb
// ========================================================================

fn rocksdb_append(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let db = rocksdb::DB::open_default(dir.path()).unwrap();
    let mut idx = 1u64;

    c.bench_function("append/rocksdb", |b| {
        b.iter(|| {
            db.put(idx.to_be_bytes(), black_box(&PAYLOAD[..])).unwrap();
            idx += 1;
        })
    });
}

fn rocksdb_get(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let db = rocksdb::DB::open_default(dir.path()).unwrap();
    for i in 1u64..=10_000 {
        db.put(i.to_be_bytes(), &PAYLOAD[..]).unwrap();
    }

    c.bench_function("get/rocksdb", |b| {
        b.iter(|| {
            db.get(black_box(5_000u64).to_be_bytes()).unwrap();
        })
    });
}

fn rocksdb_recovery(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = rocksdb::DB::open_default(&path).unwrap();
        for i in 1u64..=10_000 {
            db.put(i.to_be_bytes(), &PAYLOAD[..]).unwrap();
        }
        drop(db);
    }

    c.bench_function("recovery/rocksdb", |b| {
        b.iter(|| {
            let db = rocksdb::DB::open_default(black_box(&path)).unwrap();
            drop(db);
        })
    });
}

// ========================================================================
// Groups
// ========================================================================

criterion_group!(
    benches,
    raft_wal_append,
    raft_wal_append_sync,
    sled_append,
    redb_append,
    rocksdb_append,
    raft_wal_get,
    sled_get,
    redb_get,
    rocksdb_get,
    raft_wal_recovery,
    sled_recovery,
    redb_recovery,
    rocksdb_recovery,
);
criterion_main!(benches);
