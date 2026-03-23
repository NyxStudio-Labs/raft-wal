# raft-wal

[![crates.io](https://img.shields.io/crates/v/raft-wal.svg)](https://crates.io/crates/raft-wal)
[![docs.rs](https://docs.rs/raft-wal/badge.svg)](https://docs.rs/raft-wal)
[![CI](https://github.com/NyxStudio-Labs/raft-wal/actions/workflows/ci.yml/badge.svg)](https://github.com/NyxStudio-Labs/raft-wal/actions/workflows/ci.yml)
[![coverage](https://img.shields.io/badge/coverage-90%25-brightgreen)](https://github.com/NyxStudio-Labs/raft-wal)
[![tests](https://img.shields.io/badge/tests-132%20passed-brightgreen)](https://github.com/NyxStudio-Labs/raft-wal)
[![license](https://img.shields.io/crates/l/raft-wal.svg)](https://github.com/NyxStudio-Labs/raft-wal#license)

A minimal append-only WAL (Write-Ahead Log) optimized for Raft consensus.

General-purpose KV stores like sled or RocksDB carry unnecessary overhead for Raft log storage. raft-wal focuses on four operations: **append**, **range read**, **truncate**, and **metadata** — nothing else.

## Features

- **Fast** — ~210ns append (with HW-accelerated CRC32C), ~1ns get (O(1) via `VecDeque`)
- **Minimal dependencies** — only `crc32c` required; `tokio` and `openraft` are optional
- **Sync & async** — `RaftWal` and `AsyncRaftWal` share the same optimized core
- **Raft-correct durability** — metadata (term/vote) is always fsynced; log entries are buffered with opt-in `sync()`
- **Integrity** — every entry is protected by a CRC32C checksum; corrupted or partial writes are detected on recovery
- **Segment-based storage** — log is split into segment files (default 64 MB); `compact()` deletes old segments without rewriting
- **Parallel recovery** — segment files are read and CRC-verified in parallel across CPU cores
- **openraft integration** — optional `RaftLogStorage` trait implementation
- **Cross-platform** — Linux, macOS, Windows

## Usage

```toml
[dependencies]
raft-wal = "0.4"

# For async support:
# raft-wal = { version = "0.4", features = ["tokio"] }

# For openraft integration:
# raft-wal = { version = "0.4", features = ["openraft-storage"] }
```

```rust
use raft_wal::RaftWal;

let mut wal = RaftWal::open("./my-raft-data").unwrap();

// Append log entries
wal.append(1, b"entry-1").unwrap();
wal.append(2, b"entry-2").unwrap();

// Read entries
assert_eq!(wal.get(1), Some(b"entry-1".as_slice()));

let entries: Vec<_> = wal.iter_range(1..=2).collect();
assert_eq!(entries.len(), 2);

// Store Raft metadata (always fsynced)
wal.set_meta("term", b"5").unwrap();
wal.set_meta("vote", b"node-2").unwrap();

// Snapshot compaction — deletes old segment files
wal.compact(1).unwrap(); // discard index <= 1

// Conflict resolution
wal.truncate(2).unwrap(); // discard index >= 2

// Opt-in durable write
wal.append(3, b"entry-3").unwrap();
wal.sync().unwrap(); // fsync to disk
```

### Async

```rust
use raft_wal::AsyncRaftWal;

let mut wal = AsyncRaftWal::open("./my-raft-data").await.unwrap();

wal.append(1, b"entry-1").await.unwrap();
wal.set_meta("term", b"1").await.unwrap();

// Must call close() — tokio can't flush in Drop
wal.close().await.unwrap();
```

### openraft Integration

Enable `openraft-storage` to get `RaftLogStorage` + `RaftLogReader` implementations:

```toml
raft-wal = { version = "0.4", features = ["openraft-storage"] }
```

```rust,ignore
use raft_wal::OpenRaftLogStorage;

let storage = OpenRaftLogStorage::<MyTypeConfig>::open("./raft-data").await?;
```

`C::Entry`, `VoteOf<C>`, and `LogIdOf<C>` must implement `serde::Serialize + serde::DeserializeOwned`.

## Durability

| Operation | Behavior |
|---|---|
| `set_meta` / `remove_meta` | Always fsynced (Raft election safety) |
| `append` / `append_batch` | Buffered, no fsync |
| `sync()` | Flushes + fsyncs log entries |
| `flush()` | Flushes to OS without fsync |

**Metadata** (term, votedFor) must survive crashes per the Raft paper. `set_meta` writes to a temp file, fsyncs, then atomically renames.

**Log entries** are buffered for performance. Call `sync()` after append if your Raft implementation requires durable entries before acknowledging `AppendEntries`.

## Integrity

Each entry on disk is prefixed with a CRC32C checksum covering the index, payload length, and payload bytes. On recovery, entries with invalid checksums or incomplete writes are silently discarded from the tail — the WAL recovers up to the last good entry.

## Benchmarks

Measured on Linux with 128-byte entries. Lower is better.

### vs alternatives (buffered, no fsync)

| Operation | raft-wal | sled | redb | rocksdb |
|---|---|---|---|---|
| **append** | **209 ns** | 3.54 µs | 604 µs | 1.16 µs |
| **get** | **1.5 ns** | 194 ns | 439 ns | 321 ns |
| **recovery** (10k) | **1.37 ms** | 4.97 ms | 7.00 ms | 26.3 ms |

> **Note on durability:** `append` numbers above are **buffered without fsync**.
> sled and rocksdb also buffer by default. redb fsyncs every transaction.
> Latencies vary with disk cache state — run `cargo bench --bench comparison` on your hardware.

### durable writes (with fdatasync)

| Pattern | Latency | Per-entry cost |
|---|---|---|
| `append` + `sync()` (1 entry) | ~2.4 ms | 2.4 ms |
| `append_batch(10)` + `sync()` | ~5.0 ms | **500 µs** |

> In real Raft, `AppendEntries` RPCs carry multiple entries. Batch + single fdatasync
> is the typical pattern, making the per-entry durable cost ~500µs rather than ~2.4ms.

### raft-wal detailed

| Operation | Latency |
|---|---|
| `append` (buffered) | ~260 ns |
| `append` + `sync()` (durable) | ~2.4 ms |
| `append_batch(10)` + `sync()` | ~5.0 ms |
| `append_batch(10)` (buffered) | ~1.8 µs |
| `get` (cache hit) | ~1.4 ns |
| `read_range` (100 entries) | ~3.3 µs |
| `recovery` (10k entries, 1 segment) | ~1.3 ms |
| `recovery` (10k entries, multi-segment) | ~2.3 ms |

```sh
cargo bench --bench comparison  # vs sled, redb, rocksdb
cargo bench --bench wal         # raft-wal detailed
cargo bench --bench wal_async --features tokio
```

## Design

- **Memory-bounded cache**: Recent entries are kept in a `VecDeque` (O(1) append/lookup). Older entries are evicted from memory but remain on disk. `get()` transparently falls back to disk reads for evicted entries (`Cow::Borrowed` for cache hits, `Cow::Owned` for disk). Use `set_max_cache_entries()` to limit memory. `get_cached()` provides a zero-copy fast path for in-memory entries only.
- **Segment files**: the log is split into segment files (`{index}.seg`). When the active segment exceeds `max_segment_size` (default 64 MB), it is sealed and a new segment begins. `compact()` deletes old segments with a file remove — no rewrite needed.
- **Entry format**: `[u32 crc32c LE][u64 index LE][u32 payload_len LE][payload]` — 16-byte header per entry
- **Buffered writes**: 64 KB `BufWriter` (sync) or userspace buffer (async) — syscalls only when the buffer fills
- **Parallel recovery**: segment files are read and CRC-verified concurrently using one thread per CPU core (`std::thread::scope`) or `tokio::spawn` (async)
- **Atomic metadata**: `set_meta` writes to a temp file, fsyncs, then renames — crash-safe
- **CRC32C**: hardware-accelerated via the `crc32c` crate (SSE4.2 on x86, ARM CRC on aarch64, software fallback elsewhere)

## Status

This crate is in early development and has not been battle-tested in production yet. It is planned for use in [Nyx Studio](https://github.com/NyxStudio-Labs) infrastructure. Use at your own risk.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for commit conventions and guidelines.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT License](LICENSE-MIT) at your option.
