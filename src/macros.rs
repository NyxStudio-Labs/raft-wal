//! Shared macros and helper functions for WAL implementations.
//!
//! Eliminates code duplication across sync (`GenericRaftWal`),
//! tokio (`AsyncRaftWal`), and io_uring (`UringRaftWal`) backends.

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use crate::state::LogState;
use crate::wire::{segment_header, serialize_entry};

/// Builds the buffer for rewriting the active segment.
///
/// Returns `(first_active_index, buf)` where `buf` includes the segment
/// version header followed by serialized entries.
pub(crate) fn build_rewrite_buf(
    state: &LogState,
    sealed_last_index: Option<u64>,
) -> (u64, Vec<u8>) {
    let first_active = if state.is_empty() {
        1
    } else {
        sealed_last_index
            .map(|i| i + 1)
            .unwrap_or(state.base_index)
    };

    let hdr = segment_header();
    let mut buf = Vec::from(hdr.as_slice());
    if !state.is_empty() {
        let last = state.base_index + state.entries.len() as u64 - 1;
        for idx in first_active..=last {
            if let Some(data) = state.get(idx) {
                serialize_entry(&mut buf, idx, data);
            }
        }
    }
    (first_active, buf)
}

/// Generates read-only accessor methods that delegate to `self.state`.
///
/// All WAL backends share these methods identically. The `get` / `get_cached`
/// method is NOT included because its signature differs between backends
/// (sync returns `Cow`, async returns `Option<&[u8]>`).
macro_rules! impl_wal_accessors {
    () => {
        /// Iterates over all entries as borrowed [`Entry`] values.
        pub fn iter(&self) -> impl Iterator<Item = $crate::Entry<'_>> {
            self.state.iter()
        }

        /// Iterates over entries in the given index range without cloning.
        pub fn iter_range<R: core::ops::RangeBounds<u64>>(
            &self,
            range: R,
        ) -> impl Iterator<Item = $crate::Entry<'_>> {
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

        /// Returns the metadata value for the given key.
        pub fn get_meta(&self, key: &str) -> Option<&[u8]> {
            self.state.meta.get(key).map(|v| v.as_slice())
        }
    };
}

/// Serializes a single entry into the write buffer and appends to disk_buf.
///
/// Shared by all three backends. After this macro, the caller must handle
/// flush/rotate (sync or async).
macro_rules! append_to_buf {
    ($self:expr, $index:expr, $entry:expr) => {{
        $self.write_buf.clear();
        $crate::wire::serialize_entry(&mut $self.write_buf, $index, $entry);
        $self.disk_buf.extend_from_slice(&$self.write_buf);
        $self.active_bytes += $self.write_buf.len();
        $self.state.insert($index, $entry);
    }};
}

/// Serializes a batch of entries into the write buffer and appends to disk_buf.
macro_rules! append_batch_to_buf {
    ($self:expr, $entries:expr) => {{
        $self.write_buf.clear();
        for (index, entry) in $entries {
            $crate::wire::serialize_entry(&mut $self.write_buf, *index, entry.as_ref());
        }
        $self.disk_buf.extend_from_slice(&$self.write_buf);
        $self.active_bytes += $self.write_buf.len();
        for (index, entry) in $entries {
            $self.state.insert(*index, entry.as_ref());
        }
    }};
}

pub(crate) use append_batch_to_buf;
pub(crate) use append_to_buf;
pub(crate) use impl_wal_accessors;
