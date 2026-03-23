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

/// Shared in-memory state used by both sync and async WAL implementations.
#[cfg_attr(not(feature = "std"), allow(dead_code))]
pub(crate) struct LogState {
    pub entries: VecDeque<Vec<u8>>,
    pub base_index: u64,
    /// The first logical index in the WAL (may differ from base_index
    /// after eviction drains leading empty slots).
    pub first_logical_index: Option<u64>,
    /// Index of the first entry that's actually cached in memory.
    /// Entries between base_index and cache_start_index are evicted
    /// (empty Vec in the deque) but still on disk.
    pub cache_start_index: u64,
    pub meta: BTreeMap<String, Vec<u8>>,
    pub max_cache_entries: usize,
    /// Number of leading entries drained during eviction. Used to
    /// maintain correct `len()` accounting.
    pub drained_count: u64,
}

#[cfg_attr(not(feature = "std"), allow(dead_code))]
impl LogState {
    pub fn new() -> Self {
        Self {
            entries: VecDeque::new(),
            base_index: 0,
            first_logical_index: None,
            cache_start_index: 0,
            meta: BTreeMap::new(),
            max_cache_entries: usize::MAX,
            drained_count: 0,
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
            if self.first_logical_index.is_none() {
                self.first_logical_index = Some(index);
            }
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
        let slot = (index - self.base_index) as usize;
        if slot >= self.entries.len() {
            self.entries.resize(slot + 1, Vec::new());
        }
        self.entries[slot] = entry.to_vec();
    }

    /// Evicts oldest cached entries to stay within max_cache_entries.
    /// Evicted entries are replaced with empty Vecs (still on disk).
    /// `protect_from` is the first index of the active segment — entries
    /// at or after this index are never evicted (they may not be flushed yet).
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
        // Drain leading empty slots to reclaim memory. Without this,
        // long-running WALs accumulate empty Vec<u8> slots (24 bytes each)
        // in the VecDeque for every evicted entry.
        self.drain_evicted_prefix();
    }

    /// Drains contiguous empty entries from the front of the VecDeque,
    /// advancing `base_index` accordingly. This reclaims the per-slot
    /// `Vec<u8>` overhead (24 bytes each on 64-bit) for evicted entries.
    ///
    /// After draining, `first_index()` may advance past entries that still
    /// exist on disk. Callers should fall back to disk reads for indices
    /// between the old and new `first_index()`.
    fn drain_evicted_prefix(&mut self) {
        let mut drain_count = 0;
        for entry in self.entries.iter() {
            if entry.is_empty() {
                drain_count += 1;
            } else {
                break;
            }
        }
        if drain_count > 0 {
            self.entries.drain(..drain_count);
            self.base_index += drain_count as u64;
            self.drained_count += drain_count as u64;
        }
    }

    /// Simple eviction without protection (used when all data is flushed).
    pub fn evict_if_needed(&mut self) {
        self.evict_if_needed_until(u64::MAX);
    }


    pub fn get(&self, index: u64) -> Option<&[u8]> {
        if index < self.base_index {
            return None;
        }
        let slot = (index - self.base_index) as usize;
        self.entries.get(slot).and_then(|v| {
            if v.is_empty() {
                None
            } else {
                Some(v.as_slice())
            }
        })
    }

    pub fn first_index(&self) -> Option<u64> {
        if self.entries.is_empty() && self.drained_count == 0 {
            self.first_logical_index
        } else if self.entries.is_empty() {
            None
        } else {
            // Return the original logical first index, accounting for
            // entries drained during eviction.
            Some(self.base_index - self.drained_count)
        }
    }

    pub fn last_index(&self) -> Option<u64> {
        if self.entries.is_empty() {
            None
        } else {
            Some(self.base_index + self.entries.len() as u64 - 1)
        }
    }

    pub fn len(&self) -> usize {
        // Include drained entries in the count so len() reflects the
        // total logical entry count (not just VecDeque size).
        self.entries.len() + self.drained_count as usize
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns true if entries were actually removed.
    pub fn compact(&mut self, up_to_inclusive: u64) -> bool {
        let logical_base = self.base_index - self.drained_count;
        if self.entries.is_empty() || up_to_inclusive < logical_base {
            return false;
        }
        // Account for already-drained entries
        let effective_up_to = if up_to_inclusive < self.base_index {
            // Compacting entries that were already drained from VecDeque
            0
        } else {
            ((up_to_inclusive - self.base_index) as usize + 1).min(self.entries.len())
        };
        if effective_up_to > 0 {
            self.entries.drain(..effective_up_to);
            self.base_index += effective_up_to as u64;
        }
        // Reset drained count — compact is the "real" deletion
        self.drained_count = 0;
        self.first_logical_index = if self.entries.is_empty() {
            None
        } else {
            Some(self.base_index)
        };
        true
    }

    /// Returns true if entries were actually removed.
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

    pub fn iter(&self) -> impl Iterator<Item = Entry<'_>> {
        self.entries.iter().enumerate().filter_map(|(i, data)| {
            if data.is_empty() {
                None
            } else {
                Some(Entry {
                    index: self.base_index + i as u64,
                    data,
                })
            }
        })
    }

    pub fn iter_range<R: RangeBounds<u64>>(&self, range: R) -> impl Iterator<Item = Entry<'_>> {
        let (start, end) = self.resolve_range(&range);
        let base = self.base_index;
        (start..=end).filter_map(move |i| {
            let data = &self.entries[i];
            if data.is_empty() {
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
