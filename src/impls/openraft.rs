//! `openraft` crate `RaftLogStorage` + `RaftLogReader` trait implementation.
//!
//! Enable with `features = ["openraft-storage"]`.

use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;

use openraft::entry::RaftEntry;
use openraft::storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::type_config::alias::{LogIdOf, VoteOf};
use openraft::{OptionalSend, RaftTypeConfig};

use crate::AsyncRaftWal;

const META_VOTE: &str = "openraft:vote";
const META_COMMITTED: &str = "openraft:committed";
const META_PURGED: &str = "openraft:purged";

fn ser<T: bitcode::Encode>(v: &T) -> Vec<u8> {
    bitcode::encode(v)
}

fn de<T: bitcode::DecodeOwned>(bytes: &[u8]) -> Option<T> {
    bitcode::decode::<T>(bytes).ok()
}

fn io_err(e: crate::WalError) -> io::Error {
    match e {
        crate::WalError::Io(io) => io,
    }
}

/// A wrapper around [`AsyncRaftWal`] that implements openraft's
/// [`RaftLogStorage`] and [`RaftLogReader`] traits.
///
/// Entries are stored as bitcode-serialized bytes. Vote, committed log ID,
/// and purged log ID are persisted via WAL metadata (always fsynced).
///
/// `C::Entry`, `VoteOf<C>`, and `LogIdOf<C>` must implement
/// `bitcode::Encode + bitcode::DecodeOwned`.
pub struct OpenRaftLogStorage<C: RaftTypeConfig> {
    wal: AsyncRaftWal,
    _phantom: std::marker::PhantomData<C>,
}

impl<C: RaftTypeConfig> OpenRaftLogStorage<C> {
    /// Creates a new storage backed by the given [`AsyncRaftWal`].
    pub fn new(wal: AsyncRaftWal) -> Self {
        Self {
            wal,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Opens or creates storage in the given directory.
    pub async fn open(data_dir: impl AsRef<std::path::Path>) -> crate::Result<Self> {
        Ok(Self::new(AsyncRaftWal::open(data_dir).await?))
    }

    /// Returns a mutable reference to the underlying [`AsyncRaftWal`].
    pub fn wal_mut(&mut self) -> &mut AsyncRaftWal {
        &mut self.wal
    }
}

impl<C: RaftTypeConfig> RaftLogReader<C> for OpenRaftLogStorage<C>
where
    C::Entry: bitcode::Encode + bitcode::DecodeOwned,
    VoteOf<C>: bitcode::Encode + bitcode::DecodeOwned,
{
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, io::Error>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        let entries: Vec<C::Entry> = self
            .wal
            .iter_range(range)
            .filter_map(|e| de(e.data))
            .collect();
        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, io::Error> {
        Ok(self.wal.get_meta(META_VOTE).and_then(de))
    }
}

impl<C: RaftTypeConfig> RaftLogStorage<C> for OpenRaftLogStorage<C>
where
    C::Entry: bitcode::Encode + bitcode::DecodeOwned,
    VoteOf<C>: bitcode::Encode + bitcode::DecodeOwned,
    LogIdOf<C>: bitcode::Encode + bitcode::DecodeOwned + Copy,
{
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, io::Error> {
        let purged: Option<LogIdOf<C>> = self.wal.get_meta(META_PURGED).and_then(de);

        let last = self.wal.last_index().and_then(|idx| {
            self.wal
                .get(idx)
                .and_then(de::<C::Entry>)
                .map(|e| e.log_id())
        });

        Ok(LogState {
            last_purged_log_id: purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        panic!(
            "OpenRaftLogStorage does not support separate log readers. \
             Wrap in Arc<Mutex<>> if concurrent readers are needed."
        )
    }

    async fn save_vote(&mut self, vote: &VoteOf<C>) -> Result<(), io::Error> {
        self.wal
            .set_meta(META_VOTE, &ser(vote))
            .await
            .map_err(io_err)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<C>>,
    ) -> Result<(), io::Error> {
        match committed {
            Some(log_id) => self
                .wal
                .set_meta(META_COMMITTED, &ser(&log_id))
                .await
                .map_err(io_err),
            None => Ok(()),
        }
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<C>>, io::Error> {
        Ok(self.wal.get_meta(META_COMMITTED).and_then(de))
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<C>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        for entry in entries {
            let index = entry.log_id().index();
            let bytes = ser(&entry);
            self.wal.append(index, &bytes).await.map_err(io_err)?;
        }
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<C>>,
    ) -> Result<(), io::Error> {
        match last_log_id {
            Some(log_id) => {
                let index = log_id.index() + 1;
                self.wal.truncate(index).await.map_err(io_err)
            }
            None => {
                if let Some(first) = self.wal.first_index() {
                    self.wal.truncate(first).await.map_err(io_err)
                } else {
                    Ok(())
                }
            }
        }
    }

    async fn purge(&mut self, log_id: LogIdOf<C>) -> Result<(), io::Error> {
        self.wal
            .set_meta(META_PURGED, &ser(&log_id))
            .await
            .map_err(io_err)?;
        self.wal.compact(log_id.index()).await.map_err(io_err)
    }
}
