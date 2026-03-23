//! Generic WAL implementation parameterized over a [`WalStorage`] backend.

#[cfg(not(feature = "std"))]
use alloc::string::{String, ToString};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use crate::state::LogState;
use crate::storage::WalStorage;
use crate::wire::{parse_entries, serialize_entry};
use crate::Entry;

/// Default maximum segment size before rotation (64 MB).
const DEFAULT_MAX_SEGMENT_SIZE: usize = 64 * 1024 * 1024;

/// Threshold for flushing the write buffer.
const FLUSH_THRESHOLD: usize = 64 * 1024;

/// Metadata about a sealed segment (uses string names, not paths).
#[derive(Clone, Debug)]
struct SegmentInfo {
    name: String,
    first_index: u64,
    last_index: u64,
}

/// Generates a segment filename from the first index.
fn segment_name(first_index: u64) -> String {
    #[cfg(feature = "std")]
    {
        format!("{first_index:020}.seg")
    }
    #[cfg(not(feature = "std"))]
    {
        use core::fmt::Write;
        let mut s = String::new();
        let _ = write!(s, "{first_index:020}.seg");
        s
    }
}

/// A generic append-only WAL optimized for Raft, parameterized over the
/// storage backend.
///
/// This struct contains the core WAL logic and is available in `no_std`
/// environments (with `alloc`). Use [`WalStorage`] implementations to
/// provide the underlying I/O.
///
/// For the filesystem-backed convenience type, see
/// [`RaftWal`](crate::RaftWal) (requires `std` feature).
pub struct GenericRaftWal<S: WalStorage> {
    state: LogState,
    storage: S,
    sealed: Vec<SegmentInfo>,
    active_name: String,
    active_first_index: u64,
    active_last_index: u64,
    active_bytes: usize,
    disk_buf: Vec<u8>,
    /// Maximum segment size before rotation.
    pub(crate) max_segment_size: usize,
    write_buf: Vec<u8>,
}

impl<S: WalStorage> GenericRaftWal<S> {
    /// Opens or creates a WAL using the given storage backend.
    ///
    /// Recovers existing segments and metadata from storage.
    pub fn new(mut storage: S) -> Result<Self, S::Error> {
        let mut state = LogState::new();

        // Recover segments
        let seg_names = storage.list_files(".seg")?;
        let mut sealed = Vec::new();

        for name in &seg_names {
            let data = storage.read_file(name)?;
            let entries = parse_entries(&data);
            if entries.is_empty() {
                continue;
            }
            let first_index = entries[0].0;
            let last_index = entries[entries.len() - 1].0;
            for (index, payload) in &entries {
                state.insert(*index, payload);
            }
            sealed.push(SegmentInfo {
                name: name.clone(),
                first_index,
                last_index,
            });
        }

        // Recover metadata
        if storage.file_exists("meta.bin") {
            let data = storage.read_file("meta.bin")?;
            state.recover_meta(&data);
        }

        // Create or open active segment
        let next_index = state.last_index().map(|i| i + 1).unwrap_or(1);
        let active_name = segment_name(next_index);
        let active_bytes = if storage.file_exists(&active_name) {
            storage.file_size(&active_name)? as usize
        } else {
            // Touch the file so it exists
            storage.write_file(&active_name, &[])?;
            0
        };

        Ok(Self {
            state,
            storage,
            sealed,
            active_name,
            active_first_index: next_index,
            active_last_index: next_index.saturating_sub(1),
            active_bytes,
            disk_buf: Vec::with_capacity(FLUSH_THRESHOLD * 2),
            max_segment_size: DEFAULT_MAX_SEGMENT_SIZE,
            write_buf: Vec::with_capacity(4096),
        })
    }

    /// Appends a single log entry.
    pub fn append(&mut self, index: u64, entry: &[u8]) -> Result<(), S::Error> {
        self.write_buf.clear();
        serialize_entry(&mut self.write_buf, index, entry);
        self.disk_buf.extend_from_slice(&self.write_buf);
        self.active_bytes += self.write_buf.len();
        self.active_last_index = index;
        self.state.insert(index, entry);

        self.maybe_flush_and_rotate()?;
        Ok(())
    }

    /// Appends multiple log entries.
    pub fn append_batch<V: AsRef<[u8]>>(&mut self, entries: &[(u64, V)]) -> Result<(), S::Error> {
        self.write_buf.clear();
        for (index, entry) in entries {
            serialize_entry(&mut self.write_buf, *index, entry.as_ref());
        }
        self.disk_buf.extend_from_slice(&self.write_buf);
        self.active_bytes += self.write_buf.len();

        for (index, entry) in entries {
            self.active_last_index = *index;
            self.state.insert(*index, entry.as_ref());
        }

        self.maybe_flush_and_rotate()?;
        Ok(())
    }

    /// Iterates over all entries as borrowed [`Entry`] values.
    pub fn iter(&self) -> impl Iterator<Item = Entry<'_>> {
        self.state.iter()
    }

    /// Iterates over entries in the given index range without cloning.
    pub fn iter_range<R: core::ops::RangeBounds<u64>>(
        &self,
        range: R,
    ) -> impl Iterator<Item = Entry<'_>> {
        self.state.iter_range(range)
    }

    /// Returns entries within the given index range as owned pairs.
    pub fn read_range<R: core::ops::RangeBounds<u64>>(
        &self,
        range: R,
    ) -> Vec<(u64, Vec<u8>)> {
        self.state
            .iter_range(range)
            .map(|e| (e.index, e.data.to_vec()))
            .collect()
    }

    /// Returns the cached entry at the given index, or `None` if evicted
    /// or out of range.
    pub fn get_cached(&self, index: u64) -> Option<&[u8]> {
        self.state.get(index)
    }

    /// Reads the entry at the given index, falling back to disk if not
    /// in cache.
    pub fn get_or_read(&self, index: u64) -> Result<Option<Vec<u8>>, S::Error> {
        if let Some(data) = self.state.get(index) {
            return Ok(Some(data.to_vec()));
        }
        self.read_from_disk(index)
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

    /// Sets the maximum number of entries to keep in the in-memory cache.
    ///
    /// Flushes any buffered writes first so evicted entries are on disk.
    pub fn set_max_cache_entries(&mut self, max: usize) -> Result<(), S::Error> {
        self.state.max_cache_entries = max;
        // Flush so all entries are on disk before evicting
        self.flush_buf()?;
        self.state.evict_if_needed();
        Ok(())
    }

    /// Sets the maximum segment size (in bytes) before rotation.
    pub fn set_max_segment_size(&mut self, size: usize) {
        self.max_segment_size = size;
    }

    /// Discards all entries with index <= `up_to_inclusive`.
    pub fn compact(&mut self, up_to_inclusive: u64) -> Result<(), S::Error> {
        if !self.state.compact(up_to_inclusive) {
            return Ok(());
        }
        // Remove sealed segments that are fully compacted
        let storage = &mut self.storage;
        self.sealed.retain(|seg| {
            if seg.last_index <= up_to_inclusive {
                let _ = storage.remove_file(&seg.name);
                false
            } else {
                true
            }
        });
        // If the active segment's first index was compacted, rewrite it
        if self.active_first_index <= up_to_inclusive {
            self.rewrite_active_segment()?;
        }
        Ok(())
    }

    /// Discards all entries with index >= `from_inclusive`.
    pub fn truncate(&mut self, from_inclusive: u64) -> Result<(), S::Error> {
        if !self.state.truncate(from_inclusive) {
            return Ok(());
        }
        let storage = &mut self.storage;
        self.sealed.retain(|seg| {
            if seg.first_index >= from_inclusive {
                let _ = storage.remove_file(&seg.name);
                false
            } else {
                true
            }
        });
        self.rewrite_active_segment()?;
        Ok(())
    }

    /// Stores a metadata key-value pair. Always synced to storage.
    pub fn set_meta(&mut self, key: &str, value: &[u8]) -> Result<(), S::Error> {
        self.state.meta.insert(key.to_string(), value.to_vec());
        self.save_meta()
    }

    /// Returns the metadata value for the given key.
    pub fn get_meta(&self, key: &str) -> Option<&[u8]> {
        self.state.meta.get(key).map(|v| v.as_slice())
    }

    /// Removes a metadata key.
    pub fn remove_meta(&mut self, key: &str) -> Result<(), S::Error> {
        self.state.meta.remove(key);
        self.save_meta()
    }

    /// Flushes buffered writes to storage (without sync).
    pub fn flush(&mut self) -> Result<(), S::Error> {
        self.flush_buf()
    }

    /// Flushes buffered writes and syncs to durable storage.
    pub fn sync(&mut self) -> Result<(), S::Error> {
        self.flush_buf()?;
        self.storage.sync_file(&self.active_name)
    }

    /// Returns a reference to the underlying storage.
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Returns a mutable reference to the underlying storage.
    pub fn storage_mut(&mut self) -> &mut S {
        &mut self.storage
    }

    // --- Internal helpers ---

    fn flush_buf(&mut self) -> Result<(), S::Error> {
        if !self.disk_buf.is_empty() {
            self.storage.append_file(&self.active_name, &self.disk_buf)?;
            self.disk_buf.clear();
        }
        Ok(())
    }

    fn maybe_flush_and_rotate(&mut self) -> Result<(), S::Error> {
        if self.disk_buf.len() >= FLUSH_THRESHOLD {
            self.flush_buf()?;
        }
        if self.active_bytes >= self.max_segment_size {
            self.rotate_segment()?;
        }
        Ok(())
    }

    fn rotate_segment(&mut self) -> Result<(), S::Error> {
        self.flush_buf()?;

        let sealed = SegmentInfo {
            name: self.active_name.clone(),
            first_index: self.active_first_index,
            last_index: self.active_last_index,
        };

        let next_index = self.active_last_index + 1;
        let new_name = segment_name(next_index);
        // Create the new segment file
        self.storage.write_file(&new_name, &[])?;

        self.sealed.push(sealed);
        self.active_name = new_name;
        self.active_first_index = next_index;
        self.active_last_index = next_index.saturating_sub(1);
        self.active_bytes = 0;

        // Now that entries are sealed, evict if needed
        self.state.evict_if_needed_until(self.active_first_index);

        Ok(())
    }

    fn rewrite_active_segment(&mut self) -> Result<(), S::Error> {
        self.disk_buf.clear();

        let first_active = if self.state.is_empty() {
            1
        } else {
            self.sealed
                .last()
                .map(|s| s.last_index + 1)
                .unwrap_or(self.state.base_index)
        };

        let new_name = segment_name(first_active);
        let tmp_name = "active.tmp";

        let mut buf = Vec::new();
        if !self.state.is_empty() {
            let last = self.state.base_index + self.state.entries.len() as u64 - 1;
            for idx in first_active..=last {
                if let Some(data) = self.state.get(idx) {
                    serialize_entry(&mut buf, idx, data);
                }
            }
        }
        self.storage.write_file(tmp_name, &buf)?;

        // Remove old active segment if name changed
        if self.active_name != new_name {
            let _ = self.storage.remove_file(&self.active_name);
        }
        self.storage.rename_file(tmp_name, &new_name)?;

        self.active_bytes = buf.len();
        self.active_name = new_name;
        self.active_first_index = first_active;
        self.active_last_index = self
            .state
            .last_index()
            .unwrap_or(first_active.saturating_sub(1));

        Ok(())
    }

    fn save_meta(&mut self) -> Result<(), S::Error> {
        let bytes = self.state.serialize_meta();
        let tmp_name = "meta.tmp";
        self.storage.write_file(tmp_name, &bytes)?;
        self.storage.sync_file(tmp_name)?;
        self.storage.rename_file(tmp_name, "meta.bin")?;
        Ok(())
    }

    fn read_from_disk(&self, index: u64) -> Result<Option<Vec<u8>>, S::Error> {
        // Check sealed segments in reverse (most recent first)
        for seg in self.sealed.iter().rev() {
            if index >= seg.first_index && index <= seg.last_index {
                let data = self.storage.read_file(&seg.name)?;
                return Ok(crate::wire::find_entry_in_data(&data, index));
            }
        }
        // Check active segment
        if index >= self.active_first_index && index <= self.active_last_index {
            // Flush pending data first? We can't because &self.
            // Read what's on disk + check disk_buf
            if self.storage.file_exists(&self.active_name) {
                let data = self.storage.read_file(&self.active_name)?;
                if let Some(entry) = crate::wire::find_entry_in_data(&data, index) {
                    return Ok(Some(entry));
                }
            }
            // Also search the in-memory disk_buf that hasn't been flushed
            if !self.disk_buf.is_empty() {
                if let Some(entry) = crate::wire::find_entry_in_data(&self.disk_buf, index) {
                    return Ok(Some(entry));
                }
            }
        }
        Ok(None)
    }
}

impl<S: WalStorage> Drop for GenericRaftWal<S> {
    fn drop(&mut self) {
        let _ = self.flush_buf();
    }
}
