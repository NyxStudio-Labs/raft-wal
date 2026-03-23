//! Async WAL backed by io_uring via `tokio-uring`.
//!
//! Linux-only. Enable with the `io-uring` feature flag.

#[cfg(target_os = "linux")]
mod inner {
    use std::ops::RangeBounds;
    use std::path::{Path, PathBuf};

    use crate::segment::{
        list_segments, parse_entries, segment_path, serialize_entry, SegmentMeta,
        DEFAULT_MAX_SEGMENT_SIZE,
    };
    use crate::state::LogState;
    use crate::{Entry, Result};

    const FLUSH_THRESHOLD: usize = 64 * 1024;

    /// Async WAL backed by `io_uring` via [`tokio_uring`].
    ///
    /// Same segment-based design as [`crate::AsyncRaftWal`] but all file I/O
    /// goes through the io_uring submission queue instead of a thread pool,
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
        pub async fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
            let dir = data_dir.as_ref();
            std::fs::create_dir_all(dir)?;

            let meta_path = dir.join("meta.bin");
            let mut state = LogState::new();

            // Recover segments sequentially (no tokio::spawn in tokio-uring)
            let seg_paths = list_segments(dir);
            let mut sealed = Vec::new();
            for path in seg_paths {
                let data = std::fs::read(&path)?;
                let entries = parse_entries(&data);
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

            let next_index = state.last_index().map(|i| i + 1).unwrap_or(1);
            let active_path = segment_path(dir, next_index);
            let wal_file = tokio_uring::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(&active_path)
                .await?;
            let active_bytes = std::fs::metadata(&active_path)
                .map(|m| m.len() as usize)
                .unwrap_or(0);

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
                flushed_bytes: active_bytes as u64,
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
        pub async fn append_batch<V: AsRef<[u8]>>(
            &mut self,
            entries: &[(u64, V)],
        ) -> Result<()> {
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
        pub fn iter_range<R: RangeBounds<u64>>(
            &self,
            range: R,
        ) -> impl Iterator<Item = Entry<'_>> {
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

        /// Flushes buffered writes and fsyncs data to stable storage.
        pub async fn sync(&mut self) -> Result<()> {
            self.flush_buf().await?;
            self.wal_file.sync_data().await?;
            Ok(())
        }

        /// Flushes and closes the WAL.
        pub async fn close(mut self) -> Result<()> {
            self.sync().await?;
            self.wal_file.close().await?;
            Ok(())
        }

        async fn flush_buf(&mut self) -> Result<()> {
            if !self.disk_buf.is_empty() {
                let offset = self.flushed_bytes;
                let buf = std::mem::take(&mut self.disk_buf);
                let len = buf.len();
                let (res, returned_buf) = self.wal_file.write_all_at(buf, offset).await;
                res?;
                self.flushed_bytes += len as u64;
                self.disk_buf = returned_buf;
                self.disk_buf.clear();
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

            let sealed_meta = self.active_meta.clone();
            let next_index = self.active_meta.last_index + 1;
            let new_path = segment_path(&self.dir_path, next_index);

            let new_file = tokio_uring::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(&new_path)
                .await?;
            self.replace_wal_file(new_file).await?;

            self.sealed.push(sealed_meta);
            self.active_meta = SegmentMeta {
                path: new_path,
                first_index: next_index,
                last_index: next_index.saturating_sub(1),
            };
            self.active_bytes = 0;
            self.flushed_bytes = 0;
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
            std::fs::write(&tmp_path, &buf)?;

            if self.active_meta.path != new_path {
                let _ = std::fs::remove_file(&self.active_meta.path);
            }
            std::fs::rename(&tmp_path, &new_path)?;

            let new_file = tokio_uring::fs::OpenOptions::new()
                .write(true)
                .open(&new_path)
                .await?;
            self.replace_wal_file(new_file).await?;
            self.active_bytes = buf.len();
            self.flushed_bytes = buf.len() as u64;
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
            Ok(())
        }
    }

    // Note: UringRaftWal is intentionally !Send because tokio_uring::fs::File
    // uses Rc internally. io_uring is single-threaded by design — all I/O
    // must happen on the thread that owns the uring instance.
}

#[cfg(target_os = "linux")]
pub use inner::UringRaftWal;
