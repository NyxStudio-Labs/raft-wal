use std::collections::{BTreeMap, VecDeque};
use std::ops::{Bound, RangeBounds};

use crate::Entry;

/// Shared in-memory state used by both sync and async WAL implementations.
pub(crate) struct LogState {
    pub entries: VecDeque<Vec<u8>>,
    pub base_index: u64,
    pub meta: BTreeMap<String, Vec<u8>>,
}

impl LogState {
    pub fn new() -> Self {
        Self {
            entries: VecDeque::new(),
            base_index: 0,
            meta: BTreeMap::new(),
        }
    }

    /// Inserts an entry into the in-memory log.
    pub fn insert(&mut self, index: u64, entry: &[u8]) {
        if self.entries.is_empty() {
            self.base_index = index;
        }
        let slot = (index - self.base_index) as usize;
        if slot >= self.entries.len() {
            self.entries.resize(slot + 1, Vec::new());
        }
        self.entries[slot] = entry.to_vec();
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
        if self.entries.is_empty() {
            None
        } else {
            Some(self.base_index)
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
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns true if entries were actually removed.
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
            .map(|e| e.capacity() + std::mem::size_of::<Vec<u8>>())
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
            let key = match std::str::from_utf8(&data[pos..pos + klen]) {
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
