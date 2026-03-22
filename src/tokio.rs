//! Async WAL backed by Tokio.

use std::ops::RangeBounds;
use std::path::{Path, PathBuf};

use ::tokio::io::AsyncWriteExt;

use crate::segment::{
    list_segments, parse_entries, segment_path, serialize_entry, SegmentMeta,
    DEFAULT_MAX_SEGMENT_SIZE,
};
use crate::state::LogState;
use crate::{Entry, Result};

const FLUSH_THRESHOLD: usize = 64 * 1024;

/// Async version of [`crate::RaftWal`] backed by Tokio.
///
/// Same segment-based design as [`crate::RaftWal`] but all I/O goes
/// through `tokio::fs`.
///
/// **Durability guarantees:**
/// - Metadata writes ([`set_meta`](Self::set_meta)) are always fsynced.
/// - Log entry writes are buffered and *not* fsynced by default.
///   Call [`sync`](Self::sync) if your Raft implementation requires
///   durable entries before acknowledging `AppendEntries`.
///
/// **Integrity:** Each entry is protected by a CRC32C checksum.
///
/// Read-only accessors (`get`, `iter`, `len`, etc.) are synchronous
/// since they only touch the in-memory state.
pub struct AsyncRaftWal {
    state: LogState,
    sealed: Vec<SegmentMeta>,
    wal_file: ::tokio::fs::File,
    active_meta: SegmentMeta,
    active_bytes: usize,
    disk_buf: Vec<u8>,
    max_segment_size: usize,
    dir_path: PathBuf,
    meta_path: PathBuf,
    write_buf: Vec<u8>,
}

impl AsyncRaftWal {
    /// Opens or creates a WAL in the given directory.
    pub async fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let dir = data_dir.as_ref();
        ::tokio::fs::create_dir_all(dir).await?;

        let meta_path = dir.join("meta.bin");
        let mut state = LogState::new();

        // Recover segments — read + parse + CRC verify concurrently
        let seg_paths = list_segments(dir);
        type SegResult = std::io::Result<(PathBuf, Vec<(u64, Vec<u8>)>)>;
        let mut handles: Vec<::tokio::task::JoinHandle<SegResult>> =
            Vec::with_capacity(seg_paths.len());
        for path in seg_paths {
            handles.push(::tokio::spawn(async move {
                let data = ::tokio::fs::read(&path).await?;
                Ok((path, parse_entries(&data)))
            }));
        }

        let mut sealed = Vec::new();
        for handle in handles {
            let (path, entries) = handle.await.expect("segment reader panicked")?;
            if entries.is_empty() {
                continue;
            }
            let first_index = entries[0].0;
            let last_index = entries[entries.len() - 1].0;
            for (index, payload) in &entries {
                state.insert(*index, payload);
            }
            sealed.push(SegmentMeta {
                path,
                first_index,
                last_index,
            });
        }

        if meta_path.exists() {
            let data = ::tokio::fs::read(&meta_path).await?;
            state.recover_meta(&data);
        }

        let next_index = state.last_index().map(|i| i + 1).unwrap_or(1);
        let active_path = segment_path(dir, next_index);
        let wal_file = ::tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active_path)
            .await?;
        let active_bytes = wal_file.metadata().await?.len() as usize;

        Ok(Self {
            state,
            sealed,
            wal_file,
            active_meta: SegmentMeta {
                path: active_path,
                first_index: next_index,
                last_index: next_index.saturating_sub(1),
            },
            active_bytes,
            disk_buf: Vec::with_capacity(FLUSH_THRESHOLD * 2),
            max_segment_size: DEFAULT_MAX_SEGMENT_SIZE,
            dir_path: dir.to_path_buf(),
            meta_path,
            write_buf: Vec::with_capacity(4096),
        })
    }

    /// Appends a single log entry.
    pub async fn append(&mut self, index: u64, entry: &[u8]) -> Result<()> {
        self.write_buf.clear();
        serialize_entry(&mut self.write_buf, index, entry);
        self.disk_buf.extend_from_slice(&self.write_buf);
        self.active_bytes += self.write_buf.len();
        self.active_meta.last_index = index;
        self.state.insert(index, entry);

        if self.disk_buf.len() >= FLUSH_THRESHOLD {
            self.flush_buf().await?;
        }
        if self.active_bytes >= self.max_segment_size {
            self.rotate_segment().await?;
        }
        Ok(())
    }

    /// Appends multiple log entries.
    pub async fn append_batch<V: AsRef<[u8]>>(&mut self, entries: &[(u64, V)]) -> Result<()> {
        self.write_buf.clear();
        for (index, entry) in entries {
            serialize_entry(&mut self.write_buf, *index, entry.as_ref());
        }
        self.disk_buf.extend_from_slice(&self.write_buf);
        self.active_bytes += self.write_buf.len();

        for (index, entry) in entries {
            self.active_meta.last_index = *index;
            self.state.insert(*index, entry.as_ref());
        }

        if self.disk_buf.len() >= FLUSH_THRESHOLD {
            self.flush_buf().await?;
        }
        if self.active_bytes >= self.max_segment_size {
            self.rotate_segment().await?;
        }
        Ok(())
    }

    /// Iterates over all entries as borrowed [`Entry`] values.
    pub fn iter(&self) -> impl Iterator<Item = Entry<'_>> {
        self.state.iter()
    }

    /// Iterates over entries in the given index range without cloning.
    pub fn iter_range<R: RangeBounds<u64>>(&self, range: R) -> impl Iterator<Item = Entry<'_>> {
        self.state.iter_range(range)
    }

    /// Returns entries within the given index range as owned pairs.
    pub fn read_range<R: RangeBounds<u64>>(&self, range: R) -> Vec<(u64, Vec<u8>)> {
        self.state
            .iter_range(range)
            .map(|e| (e.index, e.data.to_vec()))
            .collect()
    }

    /// Returns the entry at the given index.
    pub fn get(&self, index: u64) -> Option<&[u8]> {
        self.state.get(index)
    }

    /// Returns the first (lowest) index in the log.
    pub fn first_index(&self) -> Option<u64> {
        self.state.first_index()
    }

    /// Returns the last (highest) index in the log.
    pub fn last_index(&self) -> Option<u64> {
        self.state.last_index()
    }

    /// Returns the number of entries in the log.
    pub fn len(&self) -> usize {
        self.state.len()
    }

    /// Returns `true` if the log contains no entries.
    pub fn is_empty(&self) -> bool {
        self.state.is_empty()
    }

    /// Estimated memory usage of in-memory entries and metadata in bytes.
    pub fn estimated_memory(&self) -> usize {
        self.state.estimated_memory()
    }

    /// Discards all entries with index <= `up_to_inclusive`.
    pub async fn compact(&mut self, up_to_inclusive: u64) -> Result<()> {
        if !self.state.compact(up_to_inclusive) {
            return Ok(());
        }
        self.sealed.retain(|seg| {
            if seg.last_index <= up_to_inclusive {
                let _ = std::fs::remove_file(&seg.path);
                false
            } else {
                true
            }
        });
        if self.active_meta.first_index <= up_to_inclusive {
            self.rewrite_active_segment().await?;
        }
        Ok(())
    }

    /// Discards all entries with index >= `from_inclusive`.
    pub async fn truncate(&mut self, from_inclusive: u64) -> Result<()> {
        if !self.state.truncate(from_inclusive) {
            return Ok(());
        }
        self.sealed.retain(|seg| {
            if seg.first_index >= from_inclusive {
                let _ = std::fs::remove_file(&seg.path);
                false
            } else {
                true
            }
        });
        self.rewrite_active_segment().await?;
        Ok(())
    }

    /// Stores a metadata key-value pair. Always fsynced.
    pub async fn set_meta(&mut self, key: &str, value: &[u8]) -> Result<()> {
        self.state.meta.insert(key.to_string(), value.to_vec());
        self.save_meta().await
    }

    /// Returns the metadata value for the given key.
    pub fn get_meta(&self, key: &str) -> Option<&[u8]> {
        self.state.meta.get(key).map(|v| v.as_slice())
    }

    /// Removes a metadata key.
    pub async fn remove_meta(&mut self, key: &str) -> Result<()> {
        self.state.meta.remove(key);
        self.save_meta().await
    }

    /// Flushes buffered writes to the OS (without fsync).
    pub async fn flush(&mut self) -> Result<()> {
        self.flush_buf().await?;
        self.wal_file.flush().await?;
        Ok(())
    }

    /// Flushes buffered writes and fsyncs data to stable storage.
    pub async fn sync(&mut self) -> Result<()> {
        self.flush_buf().await?;
        self.wal_file.flush().await?;
        self.wal_file.sync_data().await?;
        Ok(())
    }

    /// Flushes and shuts down the WAL writer.
    pub async fn close(mut self) -> Result<()> {
        self.sync().await?;
        self.wal_file.shutdown().await?;
        Ok(())
    }

    async fn flush_buf(&mut self) -> Result<()> {
        if !self.disk_buf.is_empty() {
            self.wal_file.write_all(&self.disk_buf).await?;
            self.disk_buf.clear();
        }
        Ok(())
    }

    async fn rotate_segment(&mut self) -> Result<()> {
        self.flush_buf().await?;
        self.wal_file.flush().await?;

        let sealed_meta = self.active_meta.clone();
        let next_index = self.active_meta.last_index + 1;
        let new_path = segment_path(&self.dir_path, next_index);

        self.wal_file.shutdown().await?;
        self.wal_file = ::tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&new_path)
            .await?;

        self.sealed.push(sealed_meta);
        self.active_meta = SegmentMeta {
            path: new_path,
            first_index: next_index,
            last_index: next_index.saturating_sub(1),
        };
        self.active_bytes = 0;
        Ok(())
    }

    async fn rewrite_active_segment(&mut self) -> Result<()> {
        self.disk_buf.clear();

        let first_active = if self.state.is_empty() {
            1
        } else {
            self.sealed
                .last()
                .map(|s| s.last_index + 1)
                .unwrap_or(self.state.base_index)
        };
        let new_path = segment_path(&self.dir_path, first_active);
        let tmp_path = self.dir_path.join("active.tmp");

        let mut buf = Vec::new();
        if !self.state.is_empty() {
            let last = self.state.base_index + self.state.entries.len() as u64 - 1;
            for idx in first_active..=last {
                if let Some(data) = self.state.get(idx) {
                    serialize_entry(&mut buf, idx, data);
                }
            }
        }
        ::tokio::fs::write(&tmp_path, &buf).await?;

        self.wal_file.flush().await?;
        self.wal_file.shutdown().await?;

        if self.active_meta.path != new_path {
            let _ = ::tokio::fs::remove_file(&self.active_meta.path).await;
        }
        ::tokio::fs::rename(&tmp_path, &new_path).await?;

        self.wal_file = ::tokio::fs::OpenOptions::new()
            .append(true)
            .open(&new_path)
            .await?;
        self.active_bytes = buf.len();
        self.active_meta = SegmentMeta {
            path: new_path,
            first_index: first_active,
            last_index: self
                .state
                .last_index()
                .unwrap_or(first_active.saturating_sub(1)),
        };
        Ok(())
    }

    async fn save_meta(&self) -> Result<()> {
        let bytes = self.state.serialize_meta();
        let tmp_path = self.meta_path.with_extension("tmp");
        let mut file = ::tokio::fs::File::create(&tmp_path).await?;
        file.write_all(&bytes).await?;
        file.sync_all().await?;
        drop(file);
        ::tokio::fs::rename(&tmp_path, &self.meta_path).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[::tokio::test]
    async fn open_append_get() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = AsyncRaftWal::open(dir.path()).await.expect("open");
        wal.append(1, b"hello").await.expect("append");
        assert_eq!(wal.get(1), Some(b"hello".as_slice()));
        assert_eq!(wal.len(), 1);
    }

    #[::tokio::test]
    async fn recovery() {
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
            assert_eq!(wal.get(2), Some(b"b".as_slice()));
            assert_eq!(wal.get_meta("vote"), Some(b"v1".as_slice()));
        }
    }

    #[::tokio::test]
    async fn compact_works() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = AsyncRaftWal::open(dir.path()).await.expect("open");
        for i in 1..=5 {
            wal.append(i, b"x").await.expect("a");
        }
        wal.compact(3).await.expect("compact");
        assert_eq!(wal.len(), 2);
        assert_eq!(wal.first_index(), Some(4));
    }

    #[::tokio::test]
    async fn truncate_works() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = AsyncRaftWal::open(dir.path()).await.expect("open");
        for i in 1..=5 {
            wal.append(i, b"x").await.expect("a");
        }
        wal.truncate(3).await.expect("truncate");
        assert_eq!(wal.len(), 2);
        assert_eq!(wal.last_index(), Some(2));
    }

    #[::tokio::test]
    async fn batch_append() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = AsyncRaftWal::open(dir.path()).await.expect("open");
        wal.append_batch(&[(1, b"a" as &[u8]), (2, b"b"), (3, b"c")])
            .await
            .expect("batch");
        assert_eq!(wal.len(), 3);
        assert_eq!(wal.get(2), Some(b"b".as_slice()));
    }

    #[::tokio::test]
    async fn meta_operations() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = AsyncRaftWal::open(dir.path()).await.expect("open");
        assert!(wal.get_meta("k").is_none());
        wal.set_meta("k", b"v").await.expect("set");
        assert_eq!(wal.get_meta("k"), Some(b"v".as_slice()));
        wal.remove_meta("k").await.expect("rm");
        assert!(wal.get_meta("k").is_none());
    }

    #[::tokio::test]
    async fn empty_wal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = AsyncRaftWal::open(dir.path()).await.expect("open");
        assert!(wal.is_empty());
        assert_eq!(wal.first_index(), None);
        assert_eq!(wal.last_index(), None);
    }

    #[::tokio::test]
    async fn flush_persists() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();
        {
            let mut wal = AsyncRaftWal::open(&path).await.expect("open");
            wal.append(1, b"buffered").await.expect("a");
            wal.flush().await.expect("flush");
        }
        {
            let wal = AsyncRaftWal::open(&path).await.expect("reopen");
            assert_eq!(wal.get(1), Some(b"buffered".as_slice()));
        }
    }

    #[::tokio::test]
    async fn recovery_after_compact() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();
        {
            let mut wal = AsyncRaftWal::open(&path).await.expect("open");
            for i in 1..=5 {
                wal.append(i, format!("e{i}").as_bytes()).await.expect("a");
            }
            wal.compact(3).await.expect("compact");
        }
        {
            let wal = AsyncRaftWal::open(&path).await.expect("reopen");
            assert_eq!(wal.len(), 2);
            assert_eq!(wal.first_index(), Some(4));
            assert_eq!(wal.get(4), Some(b"e4".as_slice()));
        }
    }
}
