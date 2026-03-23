# raft-wal

[![crates.io](https://img.shields.io/crates/v/raft-wal.svg)](https://crates.io/crates/raft-wal)
[![docs.rs](https://docs.rs/raft-wal/badge.svg)](https://docs.rs/raft-wal)
[![CI](https://github.com/NyxStudio-Labs/raft-wal/actions/workflows/ci.yml/badge.svg)](https://github.com/NyxStudio-Labs/raft-wal/actions/workflows/ci.yml)
[![coverage](https://img.shields.io/badge/coverage-93%25-brightgreen)](https://github.com/NyxStudio-Labs/raft-wal)
[![tests](https://img.shields.io/badge/tests-150%20passed-brightgreen)](https://github.com/NyxStudio-Labs/raft-wal)
[![license](https://img.shields.io/crates/l/raft-wal.svg)](https://github.com/NyxStudio-Labs/raft-wal#license)

A minimal append-only WAL (Write-Ahead Log) optimized for Raft consensus.

General-purpose KV stores like sled or RocksDB carry unnecessary overhead for Raft log storage. raft-wal focuses on four operations: **append**, **range read**, **truncate**, and **metadata** — nothing else.

## Features

- **Fast** — ~314ns append (with HW-accelerated CRC32C), ~2ns get (O(1) via `VecDeque`)
- **no_std support** — `WalStorage` trait for pluggable backends (SPI flash, EEPROM, in-memory). CRC32C has a software fallback for `no_std`.
- **WASM / WASI P2** — builds for `wasm32-wasip2` with full filesystem access
- **io_uring** — optional `UringRaftWal` for Linux kernel-bypass async I/O
- **Sync & async** — `RaftWal`, `AsyncRaftWal`, and `GenericRaftWal<S>` for custom backends
- **Raft-correct durability** — metadata (term/vote) is always fsynced; log entries are buffered with opt-in `sync()`
- **Integrity** — every entry is protected by a CRC32C checksum; corrupted or partial writes are detected on recovery
- **Segment-based storage** — log is split into segment files (default 64 MB); `compact()` deletes old segments without rewriting
- **Parallel recovery** — segment files are read and CRC-verified in parallel across CPU cores
- **Memory-bounded** — `set_max_cache_entries()` limits in-memory entries; evicted entries are read from disk transparently
- **openraft integration** — optional `RaftLogStorage` trait implementation
- **Cross-platform** — Linux, macOS, Windows, WASI

## Usage

```toml
[dependencies]
raft-wal = "0.6"

# Async (tokio):
# raft-wal = { version = "0.6", features = ["tokio"] }

# io_uring (Linux):
# raft-wal = { version = "0.6", features = ["io-uring"] }

# openraft integration:
# raft-wal = { version = "0.6", features = ["openraft-storage"] }

# no_std (custom backend):
# raft-wal = { version = "0.6", default-features = false }
```

### Feature matrix

| Feature | Default | Description |
|---|---|---|
| `std` | yes | Filesystem backend (`RaftWal`) |
| `tokio` | no | Async WAL (`AsyncRaftWal`) |
| `io-uring` | no | Linux io_uring backend (`UringRaftWal`) |
| `openraft-storage` | no | openraft 0.9 `RaftLogStorage` impl |
| (no default features) | — | `no_std` + `alloc`: `GenericRaftWal<S>`, `WalStorage` trait |

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
raft-wal = { version = "0.6", features = ["openraft-storage"] }
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

Each segment file begins with a 5-byte version header (`"RWAL"` magic + version byte), followed by entries. Each entry is prefixed with a CRC32C checksum covering the index, payload length, and payload bytes. On recovery, entries with invalid checksums or incomplete writes are silently discarded from the tail — the WAL recovers up to the last good entry. Legacy segments (v0, without header) are auto-detected and parsed correctly.

## Benchmarks

Measured on Linux with 128-byte entries. Lower is better.

### vs alternatives

| Operation | raft-wal | sled | redb | rocksdb |
|---|---|---|---|---|
| **append** (buffered) | **314 ns** | 3.46 µs | 2.32 ms† | 1.17 µs |
| **append + sync** (durable) | **980 µs** | — | — | — |
| **get** | **2.0 ns** | 192 ns | 444 ns | 300 ns |
| **recovery** (10k) | **1.16 ms** | 11.6 ms | 8.34 ms | 8.82 ms |

> † redb fsyncs every transaction. sled and rocksdb buffer by default like raft-wal's `append`.
> Run `cargo bench --bench comparison` on your hardware.

### durable writes (with fdatasync)

| Pattern | Latency | Per-entry cost |
|---|---|---|
| `append` + `sync()` (1 entry) | ~980 µs | 980 µs |
| `append_batch(10)` + `sync()` | ~4.2 ms | **420 µs** |

> In real Raft, `AppendEntries` RPCs carry multiple entries. Batch + single fdatasync
> is the typical pattern, making the per-entry durable cost ~420µs.

### raft-wal detailed

| Operation | Latency |
|---|---|
| `append` (buffered) | ~314 ns |
| `append` + `sync()` (durable) | ~980 µs |
| `append_batch(10)` + `sync()` | ~4.2 ms |
| `append_batch(10)` (buffered) | ~3.1 µs |
| `get` (cache hit) | ~2.0 ns |
| `read_range` (100 entries) | ~3.1 µs |
| `recovery` (10k entries, 1 segment) | ~1.0 ms |
| `recovery` (10k entries, multi-segment) | ~1.9 ms |

```sh
cargo bench --bench comparison  # vs sled, redb, rocksdb
cargo bench --bench wal         # raft-wal detailed
cargo bench --bench wal_async --features tokio
```

## Design

- **Memory-bounded cache**: Recent entries are kept in a `VecDeque` (O(1) append/lookup). Older entries are evicted from memory but remain on disk. `get()` transparently falls back to disk reads for evicted entries (`Cow::Borrowed` for cache hits, `Cow::Owned` for disk). Use `set_max_cache_entries()` to limit memory. `get_cached()` provides a zero-copy fast path for in-memory entries only.
- **Segment files**: the log is split into segment files (`{index}.seg`). When the active segment exceeds `max_segment_size` (default 64 MB), it is sealed and a new segment begins. `compact()` deletes old segments with a file remove — no rewrite needed.
- **Segment format** (v1): `[RWAL magic 4B][version u8]` header, then entries: `[u32 crc32c LE][u64 index LE][u32 payload_len LE][payload]` — 16-byte header per entry
- **Buffered writes**: 64 KB `BufWriter` (sync) or userspace buffer (async) — syscalls only when the buffer fills
- **Parallel recovery**: segment files are read and CRC-verified concurrently using one thread per CPU core (`std::thread::scope`) or `tokio::spawn` (async)
- **Atomic metadata**: `set_meta` writes to a temp file, fsyncs, then renames — crash-safe
- **CRC32C**: hardware-accelerated via the `crc32c` crate (SSE4.2 on x86, ARM CRC on aarch64); software lookup-table fallback in `no_std`
- **Pluggable storage**: `WalStorage` trait abstracts file I/O. `StdStorage` (default) uses `std::fs`. Implement your own for embedded, WASM, or custom storage backends.
- **Platform targets**: `x86_64`, `aarch64`, `wasm32-wasip2` verified. `io_uring` for Linux kernel-bypass I/O.

## Status

This crate is in early development and has not been battle-tested in production yet. It is planned for use in [Nyx Studio](https://github.com/NyxStudio-Labs) infrastructure. Use at your own risk.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for commit conventions and guidelines.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT License](LICENSE-MIT) at your option.
