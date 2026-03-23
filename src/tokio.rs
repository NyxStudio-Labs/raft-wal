//! Async WAL backed by Tokio.

use std::path::{Path, PathBuf};

use ::tokio::io::AsyncWriteExt;

use crate::core::{build_active_rewrite, parse_segment, rewrite_segment_keeping};
use crate::segment::{list_segments, segment_path, SegmentMeta, DEFAULT_MAX_SEGMENT_SIZE};
use crate::wire::segment_header;
use crate::state::LogState;
use crate::Result;

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
                let raw = ::tokio::fs::read(&path).await?;
                let (_ver, _hdr_len, parsed) = parse_segment(&raw);
                let entries: Vec<(u64, Vec<u8>)> = parsed
                    .into_iter()
                    .map(|(idx, payload, _, _)| (idx, payload))
                    .collect();
                Ok((path, entries))
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
        let is_new = !active_path.exists();
        let mut wal_file = ::tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active_path)
            .await?;
        let active_bytes = if is_new {
            // Write version header to new segment
            let hdr = segment_header();
            wal_file.write_all(&hdr).await?;
            wal_file.flush().await?;
            hdr.len()
        } else {
            wal_file.metadata().await?.len() as usize
        };

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
        append_to_buf!(self, index, entry);
        self.active_meta.last_index = index;

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
        append_batch_to_buf!(self, entries);
        if let Some((idx, _)) = entries.last() {
            self.active_meta.last_index = *idx;
        }

        if self.disk_buf.len() >= FLUSH_THRESHOLD {
            self.flush_buf().await?;
        }
        if self.active_bytes >= self.max_segment_size {
            self.rotate_segment().await?;
        }
        Ok(())
    }

    impl_wal_accessors!();

    /// Returns the entry at the given index.
    pub fn get(&self, index: u64) -> Option<&[u8]> {
        self.state.get(index)
    }

    /// Returns the directory path this WAL is stored in.
    pub fn dir_path(&self) -> &Path {
        &self.dir_path
    }

    /// Discards all entries with index <= `up_to_inclusive`.
    pub async fn compact(&mut self, up_to_inclusive: u64) -> Result<()> {
        if !self.state.compact(up_to_inclusive) {
            return Ok(());
        }
        let mut first_err: Option<std::io::Error> = None;

        // Remove fully-covered segments
        let to_remove: Vec<std::path::PathBuf> = self
            .sealed
            .iter()
            .filter(|seg| seg.last_index <= up_to_inclusive)
            .map(|seg| seg.path.clone())
            .collect();
        for path in &to_remove {
            if let Err(e) = ::tokio::fs::remove_file(path).await {
                if e.kind() != std::io::ErrorKind::NotFound && first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }

        // Rewrite partially-overlapping sealed segments
        for seg in &mut self.sealed {
            if seg.first_index <= up_to_inclusive && seg.last_index > up_to_inclusive {
                let raw = ::tokio::fs::read(&seg.path).await?;
                let (buf, _offsets) =
                    rewrite_segment_keeping(&raw, |idx| idx > up_to_inclusive);
                let tmp = self.dir_path.join("compact_rewrite.tmp");
                ::tokio::fs::write(&tmp, &buf).await?;
                {
                    let f = ::tokio::fs::File::open(&tmp).await?;
                    f.sync_all().await?;
                }
                ::tokio::fs::rename(&tmp, &seg.path).await?;
                seg.first_index = up_to_inclusive + 1;
            }
        }

        self.sealed.retain(|seg| seg.last_index > up_to_inclusive);

        if self.active_meta.first_index <= up_to_inclusive {
            self.rewrite_active_segment().await?;
        }
        if let Some(e) = first_err {
            return Err(e.into());
        }
        Ok(())
    }

    /// Discards all entries with index >= `from_inclusive`.
    pub async fn truncate(&mut self, from_inclusive: u64) -> Result<()> {
        if !self.state.truncate(from_inclusive) {
            return Ok(());
        }
        let mut first_err: Option<std::io::Error> = None;

        let to_remove: Vec<std::path::PathBuf> = self
            .sealed
            .iter()
            .filter(|seg| seg.first_index >= from_inclusive)
            .map(|seg| seg.path.clone())
            .collect();
        for path in &to_remove {
            if let Err(e) = ::tokio::fs::remove_file(path).await {
                if e.kind() != std::io::ErrorKind::NotFound && first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }

        // Rewrite partially-overlapping sealed segments
        for seg in &mut self.sealed {
            if seg.last_index >= from_inclusive && seg.first_index < from_inclusive {
                let raw = ::tokio::fs::read(&seg.path).await?;
                let (buf, _offsets) =
                    rewrite_segment_keeping(&raw, |idx| idx < from_inclusive);
                let tmp = self.dir_path.join("truncate_rewrite.tmp");
                ::tokio::fs::write(&tmp, &buf).await?;
                {
                    let f = ::tokio::fs::File::open(&tmp).await?;
                    f.sync_all().await?;
                }
                ::tokio::fs::rename(&tmp, &seg.path).await?;
                seg.last_index = from_inclusive - 1;
            }
        }

        self.sealed.retain(|seg| seg.first_index < from_inclusive);

        self.rewrite_active_segment().await?;
        if let Some(e) = first_err {
            return Err(e.into());
        }
        Ok(())
    }

    /// Stores a metadata key-value pair. Always fsynced.
    pub async fn set_meta(&mut self, key: &str, value: &[u8]) -> Result<()> {
        self.state.meta.insert(key.to_string(), value.to_vec());
        self.save_meta().await
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
        // Sync active segment to disk before sealing
        self.wal_file.sync_data().await?;

        let sealed_meta = self.active_meta.clone();
        let next_index = self.active_meta.last_index + 1;
        let new_path = segment_path(&self.dir_path, next_index);

        self.wal_file.shutdown().await?;
        self.wal_file = ::tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&new_path)
            .await?;

        // Write version header to new segment
        let hdr = segment_header();
        self.wal_file.write_all(&hdr).await?;
        self.wal_file.flush().await?;

        self.sealed.push(sealed_meta);
        self.active_meta = SegmentMeta {
            path: new_path,
            first_index: next_index,
            last_index: next_index.saturating_sub(1),
        };
        self.active_bytes = hdr.len();
        Ok(())
    }

    async fn rewrite_active_segment(&mut self) -> Result<()> {
        self.disk_buf.clear();

        // Read existing entries from disk so evicted entries aren't lost
        let existing_on_disk = if self.active_meta.path.exists() {
            let raw = ::tokio::fs::read(&self.active_meta.path).await?;
            let (_ver, _hdr_len, entries) = parse_segment(&raw);
            entries
        } else {
            Vec::new()
        };

        let sealed_last = self.sealed.last().map(|s| s.last_index);
        let (first_active, buf) =
            build_active_rewrite(&self.state, sealed_last, &existing_on_disk);

        let new_path = segment_path(&self.dir_path, first_active);
        let tmp_path = self.dir_path.join("active.tmp");

        ::tokio::fs::write(&tmp_path, &buf).await?;
        // Sync tmp file before rename for crash safety
        {
            let f = ::tokio::fs::File::open(&tmp_path).await?;
            f.sync_all().await?;
        }

        self.wal_file.flush().await?;
        self.wal_file.shutdown().await?;

        if self.active_meta.path != new_path {
            let _ = ::tokio::fs::remove_file(&self.active_meta.path).await;
        }
        ::tokio::fs::rename(&tmp_path, &new_path).await?;
        // Sync directory for durable rename
        {
            let dir = ::tokio::fs::File::open(&self.dir_path).await?;
            dir.sync_all().await?;
        }

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
        // Sync directory to ensure rename is durable on crash
        let dir = ::tokio::fs::File::open(&self.dir_path).await?;
        dir.sync_all().await?;
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

    #[::tokio::test]
    async fn segment_rotation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = AsyncRaftWal::open(dir.path()).await.expect("open");
        // Force rotation with many entries
        for i in 1..=200 {
            wal.append(i, &[0u8; 512]).await.expect("a");
        }
        wal.sync().await.expect("sync");
        assert_eq!(wal.len(), 200);
        assert_eq!(wal.get(1).as_deref(), Some([0u8; 512].as_slice()));
        assert_eq!(wal.get(200).as_deref(), Some([0u8; 512].as_slice()));
    }

    #[::tokio::test]
    async fn compact_partial_sealed_segment() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = AsyncRaftWal::open(dir.path()).await.expect("open");
        for i in 1..=100 {
            wal.append(i, &[0u8; 1024]).await.expect("a");
        }
        wal.sync().await.expect("sync");
        wal.compact(50).await.expect("compact");
        assert_eq!(wal.first_index(), Some(51));
        assert!(wal.get(50).is_none());
        assert!(wal.get(51).is_some());
    }

    #[::tokio::test]
    async fn truncate_partial_sealed_segment() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = AsyncRaftWal::open(dir.path()).await.expect("open");
        for i in 1..=100 {
            wal.append(i, &[0u8; 1024]).await.expect("a");
        }
        wal.sync().await.expect("sync");
        wal.truncate(50).await.expect("truncate");
        assert_eq!(wal.last_index(), Some(49));
        assert!(wal.get(50).is_none());
        assert!(wal.get(49).is_some());
    }

    #[::tokio::test]
    async fn remove_meta() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = AsyncRaftWal::open(dir.path()).await.expect("open");
        wal.set_meta("k", b"v").await.expect("set");
        assert_eq!(wal.get_meta("k"), Some(b"v".as_slice()));
        wal.remove_meta("k").await.expect("rm");
        assert!(wal.get_meta("k").is_none());
    }
}
