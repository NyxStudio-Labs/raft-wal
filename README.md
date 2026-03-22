# raft-wal

[![crates.io](https://img.shields.io/crates/v/raft-wal.svg)](https://crates.io/crates/raft-wal)
[![docs.rs](https://docs.rs/raft-wal/badge.svg)](https://docs.rs/raft-wal)
[![CI](https://github.com/NyxStudio-Labs/raft-wal/actions/workflows/ci.yml/badge.svg)](https://github.com/NyxStudio-Labs/raft-wal/actions/workflows/ci.yml)
[![coverage](https://img.shields.io/badge/coverage-86%25-green)](https://github.com/NyxStudio-Labs/raft-wal)
[![tests](https://img.shields.io/badge/tests-80%20passed-brightgreen)](https://github.com/NyxStudio-Labs/raft-wal)
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
raft-wal = "0.2"

# For async support:
# raft-wal = { version = "0.2", features = ["tokio"] }

# For openraft integration:
# raft-wal = { version = "0.2", features = ["openraft-storage"] }
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
raft-wal = { version = "0.2", features = ["openraft-storage"] }
```

```rust,ignore
use raft_wal::OpenRaftLogStorage;

let storage = OpenRaftLogStorage::<MyTypeConfig>::open("./raft-data").await?;
```

`C::Entry`, `VoteOf<C>`, and `LogIdOf<C>` must implement `bitcode::Encode + bitcode::DecodeOwned`.

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

Measured on Linux with 128-byte entries:

| Operation | Latency |
|---|---|
| `append` | ~210 ns |
| `append_batch` (10 entries) | ~2.9 µs |
| `get` | ~1 ns |
| `read_range` (100 entries) | ~3.2 µs |
| `recovery` (10k entries, 1 segment) | ~1.2 ms |
| `recovery` (10k entries, multi-segment) | ~2.0 ms |

```sh
cargo bench
cargo bench --bench wal_async --features tokio
```

## Design

- **In-memory index**: `VecDeque<Vec<u8>>` with a base offset — O(1) append and lookup. All entries are held in memory; call `compact()` periodically after snapshots to free memory. Use `estimated_memory()` to monitor usage.
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
