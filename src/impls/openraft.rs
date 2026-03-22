//! `openraft` crate `RaftLogStorage` + `RaftLogReader` trait implementation.
//!
//! Enable with `features = ["openraft-storage"]`.
//! Requires openraft 0.9 with `storage-v2` + `serde` features.

use std::fmt::Debug;
use std::ops::RangeBounds;

use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use openraft::{
    LogId, LogState, OptionalSend, RaftLogId, RaftLogReader, RaftTypeConfig, StorageError, Vote,
};

use crate::AsyncRaftWal;

const META_VOTE: &str = "openraft:vote";
const META_COMMITTED: &str = "openraft:committed";
const META_PURGED: &str = "openraft:purged";

fn ser<T: serde::Serialize>(v: &T) -> Vec<u8> {
    bincode::serde::encode_to_vec(v, bincode::config::standard()).expect("serialization failed")
}

fn de<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Option<T> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .ok()
        .map(|(v, _)| v)
}

fn to_storage_err<NID: openraft::NodeId>(e: crate::WalError) -> StorageError<NID> {
    match e {
        crate::WalError::Io(io) => StorageError::IO {
            source: openraft::StorageIOError::write(&io),
        },
    }
}

/// A wrapper around [`AsyncRaftWal`] that implements openraft 0.9's
/// [`RaftLogStorage`] and [`RaftLogReader`] traits.
///
/// Entries are stored as bincode-serialized bytes. Vote, committed log ID,
/// and purged log ID are persisted via WAL metadata (always fsynced).
///
/// `C::Entry`, `Vote<C::NodeId>`, and `LogId<C::NodeId>` must implement
/// `serde::Serialize + serde::DeserializeOwned`.
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
    C::Entry: serde::Serialize + serde::de::DeserializeOwned,
{
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>>
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
}

impl<C: RaftTypeConfig> RaftLogStorage<C> for OpenRaftLogStorage<C>
where
    C::Entry: serde::Serialize + serde::de::DeserializeOwned,
    C::NodeId: serde::Serialize + serde::de::DeserializeOwned,
{
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>> {
        let purged: Option<LogId<C::NodeId>> = self.wal.get_meta(META_PURGED).and_then(de);

        let last = self.wal.last_index().and_then(|idx| {
            self.wal
                .get(idx)
                .and_then(de::<C::Entry>)
                .map(|e| e.get_log_id().clone())
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

    async fn save_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        self.wal
            .set_meta(META_VOTE, &ser(vote))
            .await
            .map_err(to_storage_err)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
        Ok(self.wal.get_meta(META_VOTE).and_then(de))
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<C::NodeId>>,
    ) -> Result<(), StorageError<C::NodeId>> {
        match committed {
            Some(log_id) => self
                .wal
                .set_meta(META_COMMITTED, &ser(&log_id))
                .await
                .map_err(to_storage_err),
            None => Ok(()),
        }
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<C::NodeId>>, StorageError<C::NodeId>> {
        Ok(self.wal.get_meta(META_COMMITTED).and_then(de))
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<C>,
    ) -> Result<(), StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        use openraft::RaftLogId;
        for entry in entries {
            let index = entry.get_log_id().index;
            let bytes = ser(&entry);
            self.wal
                .append(index, &bytes)
                .await
                .map_err(to_storage_err)?;
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        self.wal
            .truncate(log_id.index)
            .await
            .map_err(to_storage_err)
    }

    async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        self.wal
            .set_meta(META_PURGED, &ser(&log_id))
            .await
            .map_err(to_storage_err)?;
        self.wal.compact(log_id.index).await.map_err(to_storage_err)
    }
}
