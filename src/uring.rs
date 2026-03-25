//! Async WAL backed by `io_uring` via `tokio-uring`.
//!
//! Linux-only. Enable with the `io-uring` feature flag.

#[cfg(target_os = "linux")]
mod inner {
    use std::path::{Path, PathBuf};

    use crate::core::{build_active_rewrite, parse_segment, rewrite_segment_keeping};
    use crate::segment::{list_segments, segment_path, SegmentMeta, DEFAULT_MAX_SEGMENT_SIZE};
    use crate::state::LogState;
    use crate::wire::active_segment_header;
    use crate::Result;

    const FLUSH_THRESHOLD: usize = 64 * 1024;

    /// Async WAL backed by `io_uring` via [`tokio_uring`].
    ///
    /// Same segment-based design as [`crate::AsyncRaftWal`] but all file I/O
    /// goes through the `io_uring` submission queue instead of a thread pool,
    /// reducing syscall overhead on Linux 5.1+.
    ///
    /// **Durability guarantees** are identical to [`crate::AsyncRaftWal`]:
    /// - Metadata writes are always fsynced.
    /// - Log entry writes are buffered; call [`sync`](Self::sync) for durability.
    pub struct UringRaftWal {
        state: LogState,
        sealed: Vec<SegmentMeta>,
        wal_file: tokio_uring::fs::File,
        active_meta: SegmentMeta,
        /// Total bytes written to the active segment (flushed + buffered).
        active_bytes: usize,
        /// Bytes already flushed to disk in the active segment.
        flushed_bytes: u64,
        disk_buf: Vec<u8>,
        max_segment_size: usize,
        dir_path: PathBuf,
        meta_path: PathBuf,
        write_buf: Vec<u8>,
    }

    impl UringRaftWal {
        /// Opens or creates a WAL in the given directory.
        ///
        /// # Errors
        ///
        /// Returns an error if the directory cannot be created or existing
        /// segments cannot be recovered.
        pub async fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
            let dir = data_dir.as_ref();
            std::fs::create_dir_all(dir)?;

            let meta_path = dir.join("meta.bin");
            let mut state = LogState::new();

            // Recover segments sequentially (no tokio::spawn in tokio-uring)
            let seg_paths = list_segments(dir);
            let mut sealed = Vec::new();
            for path in seg_paths {
                let raw = std::fs::read(&path)?;
                let (_ver, _hdr_len, parsed) = parse_segment(&raw);
                let entries: Vec<(u64, Vec<u8>)> = parsed
                    .into_iter()
                    .map(|(idx, payload, _, _)| (idx, payload))
                    .collect();
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
                let data = std::fs::read(&meta_path)?;
                state.recover_meta(&data);
            }

            let next_index = state.last_index().map_or(1, |i| i + 1);
            let active_path = segment_path(dir, next_index);
            let is_new = !active_path.exists();
            let wal_file = tokio_uring::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(&active_path)
                .await?;
            let active_bytes = if is_new {
                let hdr = active_segment_header();
                let hdr_len = hdr.len();
                let (res, _) = wal_file.write_all_at(hdr.to_vec(), 0).await;
                res?;
                hdr_len
            } else {
                #[allow(clippy::cast_possible_truncation)]
                {
                    std::fs::metadata(&active_path)
                        .map(|m| m.len() as usize)
                        .unwrap_or(0)
                }
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
                #[allow(clippy::cast_possible_truncation)]
                flushed_bytes: active_bytes as u64,
                disk_buf: Vec::with_capacity(FLUSH_THRESHOLD * 2),
                max_segment_size: DEFAULT_MAX_SEGMENT_SIZE,
                dir_path: dir.to_path_buf(),
                meta_path,
                write_buf: Vec::with_capacity(4096),
            })
        }

        /// Appends a single log entry.
        ///
        /// # Errors
        ///
        /// Returns an error if the underlying I/O operations fail.
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
        ///
        /// # Errors
        ///
        /// Returns an error if the underlying I/O operations fail.
        pub async fn append_batch<V: AsRef<[u8]>>(
            &mut self,
            entries: &[(u64, V)],
        ) -> Result<()> {
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
        #[must_use]
        pub fn get(&self, index: u64) -> Option<&[u8]> {
            self.state.get(index)
        }

        /// Discards all entries with index <= `up_to_inclusive`.
        ///
        /// # Errors
        ///
        /// Returns an error if the underlying I/O operations fail.
        pub async fn compact(&mut self, up_to_inclusive: u64) -> Result<()> {
            if !self.state.compact(up_to_inclusive) {
                return Ok(());
            }
            let mut first_err: Option<std::io::Error> = None;

            let to_remove: Vec<std::path::PathBuf> = self
                .sealed
                .iter()
                .filter(|seg| seg.last_index <= up_to_inclusive)
                .map(|seg| seg.path.clone())
                .collect();
            for path in &to_remove {
                if let Err(e) = std::fs::remove_file(path) {
                    if e.kind() != std::io::ErrorKind::NotFound && first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }

            // Rewrite partially-overlapping sealed segments
            for seg in &mut self.sealed {
                if seg.first_index <= up_to_inclusive && seg.last_index > up_to_inclusive {
                    let raw = std::fs::read(&seg.path)?;
                    let (buf, _offsets) =
                        rewrite_segment_keeping(&raw, |idx| idx > up_to_inclusive);
                    let tmp = self.dir_path.join("compact_rewrite.tmp");
                    std::fs::write(&tmp, &buf)?;
                    {
                        let f = std::fs::File::open(&tmp)?;
                        f.sync_all()?;
                    }
                    std::fs::rename(&tmp, &seg.path)?;
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
        ///
        /// # Errors
        ///
        /// Returns an error if the underlying I/O operations fail.
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
                if let Err(e) = std::fs::remove_file(path) {
                    if e.kind() != std::io::ErrorKind::NotFound && first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }

            // Rewrite partially-overlapping sealed segments
            for seg in &mut self.sealed {
                if seg.last_index >= from_inclusive && seg.first_index < from_inclusive {
                    let raw = std::fs::read(&seg.path)?;
                    let (buf, _offsets) =
                        rewrite_segment_keeping(&raw, |idx| idx < from_inclusive);
                    let tmp = self.dir_path.join("truncate_rewrite.tmp");
                    std::fs::write(&tmp, &buf)?;
                    {
                        let f = std::fs::File::open(&tmp)?;
                        f.sync_all()?;
                    }
                    std::fs::rename(&tmp, &seg.path)?;
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
        ///
        /// # Errors
        ///
        /// Returns an error if persisting the metadata fails.
        pub async fn set_meta(&mut self, key: &str, value: &[u8]) -> Result<()> {
            self.state.meta.insert(key.to_string(), value.to_vec());
            self.save_meta().await
        }

        /// Removes a metadata key.
        ///
        /// # Errors
        ///
        /// Returns an error if persisting the metadata fails.
        pub async fn remove_meta(&mut self, key: &str) -> Result<()> {
            self.state.meta.remove(key);
            self.save_meta().await
        }

        /// Flushes buffered writes and fsyncs data to stable storage.
        ///
        /// # Errors
        ///
        /// Returns an error if the flush or sync operation fails.
        pub async fn sync(&mut self) -> Result<()> {
            self.flush_buf().await?;
            self.wal_file.sync_data().await?;
            Ok(())
        }

        /// Flushes and closes the WAL.
        ///
        /// # Errors
        ///
        /// Returns an error if the flush, sync, or close operation fails.
        pub async fn close(mut self) -> Result<()> {
            self.sync().await?;
            self.wal_file.close().await?;
            Ok(())
        }

        async fn flush_buf(&mut self) -> Result<()> {
            if !self.disk_buf.is_empty() {
                let offset = self.flushed_bytes;
                #[cfg(feature = "zstd")]
                {
                    let compressed = crate::wire::compress_block(&self.disk_buf);
                    let len = compressed.len();
                    let (res, _) = self.wal_file.write_all_at(compressed, offset).await;
                    res?;
                    self.flushed_bytes += len as u64;
                    self.disk_buf.clear();
                }
                #[cfg(not(feature = "zstd"))]
                {
                    let buf = std::mem::take(&mut self.disk_buf);
                    let len = buf.len();
                    let (res, returned_buf) = self.wal_file.write_all_at(buf, offset).await;
                    res?;
                    self.flushed_bytes += len as u64;
                    self.disk_buf = returned_buf;
                    self.disk_buf.clear();
                }
            }
            Ok(())
        }

        async fn replace_wal_file(
            &mut self,
            new_file: tokio_uring::fs::File,
        ) -> Result<()> {
            let old = std::mem::replace(&mut self.wal_file, new_file);
            old.close().await?;
            Ok(())
        }

        async fn rotate_segment(&mut self) -> Result<()> {
            self.flush_buf().await?;
            // Sync active segment to disk before sealing
            self.wal_file.sync_data().await?;

            let sealed_meta = self.active_meta.clone();
            let next_index = self.active_meta.last_index + 1;
            let new_path = segment_path(&self.dir_path, next_index);

            let new_file = tokio_uring::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(&new_path)
                .await?;

            // Write version header to new segment
            let hdr = active_segment_header();
            let hdr_len = hdr.len();
            let (res, _) = new_file.write_all_at(hdr.to_vec(), 0).await;
            res?;

            self.replace_wal_file(new_file).await?;

            self.sealed.push(sealed_meta);
            self.active_meta = SegmentMeta {
                path: new_path,
                first_index: next_index,
                last_index: next_index.saturating_sub(1),
            };
            self.active_bytes = hdr_len;
            self.flushed_bytes = hdr_len as u64;
            Ok(())
        }

        async fn rewrite_active_segment(&mut self) -> Result<()> {
            self.disk_buf.clear();

            // Read existing entries from disk so evicted entries aren't lost
            let existing_on_disk = if self.active_meta.path.exists() {
                let raw = std::fs::read(&self.active_meta.path)?;
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

            let tmp_file = tokio_uring::fs::File::create(&tmp_path).await?;
            let buf_len = buf.len();
            let (res, _) = tmp_file.write_all_at(buf, 0).await;
            res?;
            tmp_file.sync_all().await?;
            tmp_file.close().await?;

            if self.active_meta.path != new_path {
                let _ = std::fs::remove_file(&self.active_meta.path);
            }
            std::fs::rename(&tmp_path, &new_path)?;
            // Directory fsync for durable rename
            {
                let dir = std::fs::File::open(&self.dir_path)?;
                dir.sync_all()?;
            }

            let new_file = tokio_uring::fs::OpenOptions::new()
                .write(true)
                .open(&new_path)
                .await?;
            self.replace_wal_file(new_file).await?;
            self.active_bytes = buf_len;
            self.flushed_bytes = buf_len as u64;
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
            let file = tokio_uring::fs::File::create(&tmp_path).await?;
            let (res, _) = file.write_all_at(bytes, 0).await;
            res?;
            file.sync_all().await?;
            file.close().await?;
            std::fs::rename(&tmp_path, &self.meta_path)?;
            // Directory fsync for durable rename
            {
                let dir = std::fs::File::open(&self.dir_path)?;
                dir.sync_all()?;
            }
            Ok(())
        }
    }

    // Note: UringRaftWal is intentionally !Send because tokio_uring::fs::File
    // uses Rc internally. io_uring is single-threaded by design — all I/O
    // must happen on the thread that owns the uring instance.
}

#[cfg(target_os = "linux")]
pub use inner::UringRaftWal;
