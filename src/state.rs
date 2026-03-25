#[cfg(not(feature = "std"))]
use alloc::collections::{BTreeMap, VecDeque};
#[cfg(not(feature = "std"))]
use alloc::string::{String, ToString};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
#[cfg(not(feature = "std"))]
use core::ops::{Bound, RangeBounds};

#[cfg(feature = "std")]
use std::collections::{BTreeMap, VecDeque};
#[cfg(feature = "std")]
use std::ops::{Bound, RangeBounds};

use crate::Entry;

/// Returns true if a Vec represents an evicted (sentinel) entry.
/// Evicted entries use `Vec::new()` (capacity 0, length 0).
/// Genuinely empty payloads are stored with capacity > 0.
#[inline]
fn is_evicted(v: &Vec<u8>) -> bool {
    v.is_empty() && v.capacity() == 0
}

/// Shared in-memory state used by both sync and async WAL implementations.
#[cfg_attr(not(feature = "std"), allow(dead_code))]
pub(crate) struct LogState {
    pub entries: VecDeque<Vec<u8>>,
    pub base_index: u64,
    /// Index of the first entry that's actually cached in memory.
    /// Entries between `base_index` and `cache_start_index` are evicted
    /// (empty Vec in the deque) but still on disk.
    pub cache_start_index: u64,
    pub meta: BTreeMap<String, Vec<u8>>,
    pub max_cache_entries: usize,
}

#[cfg_attr(not(feature = "std"), allow(dead_code))]
impl LogState {
    pub fn new() -> Self {
        Self {
            entries: VecDeque::new(),
            base_index: 0,
            cache_start_index: 0,
            meta: BTreeMap::new(),
            max_cache_entries: 1024,
        }
    }

    /// Inserts an entry into the in-memory log.
    ///
    /// # Panics (debug builds)
    ///
    /// Panics if `index` is below `base_index` (after compact). In release
    /// builds the insert is silently ignored to prevent OOM from u64 underflow.
    pub fn insert(&mut self, index: u64, entry: &[u8]) {
        if self.entries.is_empty() {
            self.base_index = index;
            self.cache_start_index = index;
        }
        debug_assert!(
            index >= self.base_index,
            "insert index {index} is below base_index {base}; \
             this is a caller bug (e.g. append after compact with stale index)",
            base = self.base_index
        );
        if index < self.base_index {
            return;
        }
        // Index offsets are bounded by entries.len() which fits in usize
        #[allow(clippy::cast_possible_truncation)]
        let slot = (index - self.base_index) as usize;
        if slot >= self.entries.len() {
            self.entries.resize(slot + 1, Vec::new());
        }
        // Store the entry. For zero-length payloads, allocate 1 byte of
        // capacity so they're distinguishable from evicted entries (which
        // use Vec::new() with capacity 0).
        if entry.is_empty() {
            let mut v = Vec::with_capacity(1);
            v.extend_from_slice(entry);
            self.entries[slot] = v;
        } else {
            self.entries[slot] = entry.to_vec();
        }
    }

    /// Evicts oldest cached entries to stay within `max_cache_entries`.
    /// Evicted entries are replaced with empty `Vec`s (still on disk).
    /// `protect_from` is the first index of the active segment — entries
    /// at or after this index are never evicted (they may not be flushed yet).
    #[allow(clippy::cast_possible_truncation)]
    pub fn evict_if_needed_until(&mut self, protect_from: u64) {
        if self.max_cache_entries == usize::MAX {
            return;
        }
        let cached = self.entries.len();
        if cached <= self.max_cache_entries {
            return;
        }
        let to_evict = cached - self.max_cache_entries;
        for i in 0..to_evict {
            let abs_idx = self.base_index + i as u64;
            if abs_idx >= protect_from {
                break; // don't evict active segment entries
            }
            if abs_idx >= self.cache_start_index {
                let slot = (abs_idx - self.base_index) as usize;
                if slot < self.entries.len() {
                    self.entries[slot] = Vec::new();
                }
            }
        }
        let evicted_up_to = (self.base_index + to_evict as u64).min(protect_from);
        if evicted_up_to > self.cache_start_index {
            self.cache_start_index = evicted_up_to;
        }
        // Note: evicted entries remain as empty Vec slots in the VecDeque
        // (24 bytes each on 64-bit). This overhead is reclaimed by compact(),
        // which drains the front of the deque. In normal Raft usage, periodic
        // snapshots trigger compact, keeping the overhead bounded.
    }

    /// Simple eviction without protection (used when all data is flushed).
    pub fn evict_if_needed(&mut self) {
        self.evict_if_needed_until(u64::MAX);
    }


    pub fn get(&self, index: u64) -> Option<&[u8]> {
        if index < self.base_index {
            return None;
        }
        // Index offsets are bounded by entries.len() which fits in usize
        #[allow(clippy::cast_possible_truncation)]
        let slot = (index - self.base_index) as usize;
        self.entries.get(slot).and_then(|v| {
            if is_evicted(v) {
                None
            } else {
                Some(v.as_slice())
            }
        })
    }


    pub fn first_index(&self) -> Option<u64> {
        if self.entries.is_empty() {
            None
        } else {
            Some(self.base_index)
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn last_index(&self) -> Option<u64> {
        if self.entries.is_empty() {
            None
        } else {
            Some(self.base_index + self.entries.len() as u64 - 1)
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns true if entries were actually removed.
    #[allow(clippy::cast_possible_truncation)]
    pub fn compact(&mut self, up_to_inclusive: u64) -> bool {
        if self.entries.is_empty() || up_to_inclusive < self.base_index {
            return false;
        }
        let n = ((up_to_inclusive - self.base_index) as usize + 1).min(self.entries.len());
        self.entries.drain(..n);
        self.base_index += n as u64;
        true
    }

    /// Returns true if entries were actually removed.
    #[allow(clippy::cast_possible_truncation)]
    pub fn truncate(&mut self, from_inclusive: u64) -> bool {
        if self.entries.is_empty()
            || from_inclusive > self.base_index + self.entries.len() as u64 - 1
        {
            return false;
        }
        let keep = (from_inclusive - self.base_index) as usize;
        self.entries.truncate(keep);
        true
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn resolve_range<R: RangeBounds<u64>>(&self, range: &R) -> (usize, usize) {
        if self.entries.is_empty() {
            return (1, 0);
        }

        let lo = match range.start_bound() {
            Bound::Included(&v) => v.max(self.base_index),
            Bound::Excluded(&v) => (v + 1).max(self.base_index),
            Bound::Unbounded => self.base_index,
        };
        let hi = match range.end_bound() {
            Bound::Included(&v) => v,
            Bound::Excluded(&v) => v.saturating_sub(1),
            Bound::Unbounded => self.base_index + self.entries.len() as u64 - 1,
        };
        let last = self.base_index + self.entries.len() as u64 - 1;
        let hi = hi.min(last);

        if lo > hi {
            return (1, 0);
        }

        let start = (lo - self.base_index) as usize;
        let end = (hi - self.base_index) as usize;
        (start, end)
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn iter(&self) -> impl Iterator<Item = Entry<'_>> {
        self.entries.iter().enumerate().filter_map(|(i, data)| {
            if is_evicted(data) {
                None
            } else {
                Some(Entry {
                    index: self.base_index + i as u64,
                    data,
                })
            }
        })
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn iter_range<R: RangeBounds<u64>>(&self, range: R) -> impl Iterator<Item = Entry<'_>> {
        let (start, end) = self.resolve_range(&range);
        let base = self.base_index;
        (start..=end).filter_map(move |i| {
            let data = &self.entries[i];
            if is_evicted(data) {
                None
            } else {
                Some(Entry {
                    index: base + i as u64,
                    data,
                })
            }
        })
    }

    /// Estimated memory usage in bytes.
    pub fn estimated_memory(&self) -> usize {
        self.entries
            .iter()
            .map(|e| e.capacity() + core::mem::size_of::<Vec<u8>>())
            .sum::<usize>()
            + self
                .meta
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()
    }

    /// Parses metadata bytes.
    ///
    /// Format: `[u32 count][[u32 key_len][key bytes][u32 val_len][val bytes]]*`
    #[allow(clippy::cast_possible_truncation)]
    pub fn recover_meta(&mut self, data: &[u8]) {
        let mut pos = 0;
        if pos + 4 > data.len() {
            return;
        }
        let count = u32::from_le_bytes(data[pos..pos + 4].try_into().expect("4 bytes")) as usize;
        pos += 4;

        for _ in 0..count {
            if pos + 4 > data.len() {
                return;
            }
            let klen = u32::from_le_bytes(data[pos..pos + 4].try_into().expect("4 bytes")) as usize;
            pos += 4;
            if pos + klen > data.len() {
                return;
            }
            let key = match core::str::from_utf8(&data[pos..pos + klen]) {
                Ok(s) => s.to_string(),
                Err(_) => return,
            };
            pos += klen;

            if pos + 4 > data.len() {
                return;
            }
            let vlen = u32::from_le_bytes(data[pos..pos + 4].try_into().expect("4 bytes")) as usize;
            pos += 4;
            if pos + vlen > data.len() {
                return;
            }
            let val = data[pos..pos + vlen].to_vec();
            pos += vlen;

            self.meta.insert(key, val);
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn serialize_meta(&self) -> Vec<u8> {
        let total: usize = 4 + self
            .meta
            .iter()
            .map(|(k, v)| 4 + k.len() + 4 + v.len())
            .sum::<usize>();
        let mut buf = Vec::with_capacity(total);
        buf.extend_from_slice(&(self.meta.len() as u32).to_le_bytes());
        for (key, val) in &self.meta {
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key.as_bytes());
            buf.extend_from_slice(&(val.len() as u32).to_le_bytes());
            buf.extend_from_slice(val);
        }
        buf
    }
}
