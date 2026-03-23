//! Generic WAL implementation parameterized over a [`WalStorage`] backend.

#[cfg(not(feature = "std"))]
use alloc::string::{String, ToString};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use crate::state::LogState;
use crate::storage::WalStorage;
use crate::wire::{parse_entries_with_offsets, segment_header, strip_segment_header};

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
    /// Per-entry byte offsets within the segment (after header).
    /// `entry_offsets[i]` = `(byte_offset, total_entry_size)` relative to
    /// the start of the *file* (including any segment header).
    entry_offsets: Vec<(u64, usize, usize)>,
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
            let raw = storage.read_file(name)?;
            let (_ver, entry_data) = strip_segment_header(&raw);
            let header_len = raw.len() - entry_data.len();
            let entries = parse_entries_with_offsets(entry_data);
            if entries.is_empty() {
                continue;
            }
            let first_index = entries[0].0;
            let last_index = entries[entries.len() - 1].0;
            let entry_offsets: Vec<(u64, usize, usize)> = entries
                .iter()
                .map(|(idx, _, off, sz)| (*idx, off + header_len, *sz))
                .collect();
            for (index, payload, _, _) in &entries {
                state.insert(*index, payload);
            }
            sealed.push(SegmentInfo {
                name: name.clone(),
                first_index,
                last_index,
                entry_offsets,
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
            // Create with version header
            let hdr = segment_header();
            storage.write_file(&active_name, &hdr)?;
            hdr.len()
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
        append_to_buf!(self, index, entry);
        self.active_last_index = index;
        self.maybe_flush_and_rotate()?;
        Ok(())
    }

    /// Appends multiple log entries.
    pub fn append_batch<V: AsRef<[u8]>>(&mut self, entries: &[(u64, V)]) -> Result<(), S::Error> {
        append_batch_to_buf!(self, entries);
        if let Some((idx, _)) = entries.last() {
            self.active_last_index = *idx;
        }
        self.maybe_flush_and_rotate()?;
        Ok(())
    }

    impl_wal_accessors!();

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
        // Collect segment files to delete, then remove them with error propagation.
        let to_remove: Vec<String> = self
            .sealed
            .iter()
            .filter(|seg| seg.last_index <= up_to_inclusive)
            .map(|seg| seg.name.clone())
            .collect();

        let mut first_err: Option<S::Error> = None;
        for name in &to_remove {
            if let Err(e) = self.storage.remove_file(name) {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
        // Always update the in-memory list even if some deletes failed,
        // so the WAL state stays consistent. Orphan files on disk are
        // harmless and will be cleaned up on next compact.
        self.sealed.retain(|seg| seg.last_index > up_to_inclusive);

        if self.active_first_index <= up_to_inclusive {
            self.rewrite_active_segment()?;
        }
        if let Some(e) = first_err {
            return Err(e);
        }
        Ok(())
    }

    /// Discards all entries with index >= `from_inclusive`.
    pub fn truncate(&mut self, from_inclusive: u64) -> Result<(), S::Error> {
        if !self.state.truncate(from_inclusive) {
            return Ok(());
        }
        let to_remove: Vec<String> = self
            .sealed
            .iter()
            .filter(|seg| seg.first_index >= from_inclusive)
            .map(|seg| seg.name.clone())
            .collect();

        let mut first_err: Option<S::Error> = None;
        for name in &to_remove {
            if let Err(e) = self.storage.remove_file(name) {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
        self.sealed.retain(|seg| seg.first_index < from_inclusive);

        self.rewrite_active_segment()?;
        if let Some(e) = first_err {
            return Err(e);
        }
        Ok(())
    }

    /// Stores a metadata key-value pair. Always synced to storage.
    pub fn set_meta(&mut self, key: &str, value: &[u8]) -> Result<(), S::Error> {
        self.state.meta.insert(key.to_string(), value.to_vec());
        self.save_meta()
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

        // Build offset map for the segment we're about to seal
        let raw = self.storage.read_file(&self.active_name)?;
        let (_ver, entry_data) = strip_segment_header(&raw);
        let header_len = raw.len() - entry_data.len();
        let parsed = parse_entries_with_offsets(entry_data);
        let entry_offsets: Vec<(u64, usize, usize)> = parsed
            .iter()
            .map(|(idx, _, off, sz)| (*idx, off + header_len, *sz))
            .collect();

        let sealed = SegmentInfo {
            name: self.active_name.clone(),
            first_index: self.active_first_index,
            last_index: self.active_last_index,
            entry_offsets,
        };

        let next_index = self.active_last_index + 1;
        let new_name = segment_name(next_index);
        // Create the new segment file with version header
        let hdr = segment_header();
        self.storage.write_file(&new_name, &hdr)?;

        self.sealed.push(sealed);
        self.active_name = new_name;
        self.active_first_index = next_index;
        self.active_last_index = next_index.saturating_sub(1);
        self.active_bytes = hdr.len();

        // Now that entries are sealed, evict if needed
        self.state.evict_if_needed_until(self.active_first_index);

        Ok(())
    }

    fn rewrite_active_segment(&mut self) -> Result<(), S::Error> {
        self.disk_buf.clear();

        let sealed_last = self.sealed.last().map(|s| s.last_index);
        let (first_active, buf) = crate::macros::build_rewrite_buf(&self.state, sealed_last);

        let new_name = segment_name(first_active);
        let tmp_name = "active.tmp";

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
        // Check sealed segments in reverse (most recent first).
        // Use offset map for O(1) lookup when available, falling back to
        // full-segment read + linear scan for legacy segments.
        for seg in self.sealed.iter().rev() {
            if index >= seg.first_index && index <= seg.last_index {
                // Try offset map first (O(1) lookup via read_file_range)
                if let Some((_, offset, size)) = seg
                    .entry_offsets
                    .iter()
                    .find(|(idx, _, _)| *idx == index)
                {
                    let data = self.storage.read_file_range(&seg.name, *offset, *size)?;
                    let entries = crate::wire::parse_entries(&data);
                    if let Some((_idx, payload)) = entries.into_iter().next() {
                        return Ok(Some(payload));
                    }
                }
                // Fallback: read entire segment (legacy or offset map miss)
                let raw = self.storage.read_file(&seg.name)?;
                let (_ver, entry_data) = strip_segment_header(&raw);
                return Ok(crate::wire::find_entry_in_data(entry_data, index));
            }
        }
        // Check active segment
        if index >= self.active_first_index && index <= self.active_last_index {
            if self.storage.file_exists(&self.active_name) {
                let raw = self.storage.read_file(&self.active_name)?;
                let (_ver, entry_data) = strip_segment_header(&raw);
                if let Some(entry) = crate::wire::find_entry_in_data(entry_data, index) {
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
