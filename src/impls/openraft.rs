//! `openraft` crate `RaftLogStorage` + `RaftLogReader` trait implementation.
//!
//! Enable with `features = ["openraft-storage"]`.
//! Requires openraft 0.9 with `storage-v2` + `serde` features.
//!
//! # Shared-state design
//!
//! `OpenRaftLogStorage` wraps `AsyncRaftWal` in `Arc<tokio::sync::RwLock<_>>`
//! so that `get_log_reader()` returns a cheap clone sharing the same in-memory
//! state. This avoids the bug where a reader opened from disk cannot see
//! entries that the writer has appended but not yet flushed.

use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use openraft::{
    LogId, LogState, OptionalSend, RaftLogId, RaftLogReader, RaftTypeConfig, StorageError, Vote,
};
use tokio::sync::{RwLock, RwLockWriteGuard};

use crate::AsyncRaftWal;

const META_VOTE: &str = "openraft:vote";
const META_COMMITTED: &str = "openraft:committed";
const META_PURGED: &str = "openraft:purged";

/// Serializes a value with bitcode. This should never fail for well-formed
/// serde types; the expect is a safety net for broken Serialize impls.
#[allow(clippy::unwrap_used)]
fn ser<T: serde::Serialize>(v: &T) -> Vec<u8> {
    bitcode::serialize(v).expect("bitcode serialization of serde type should not fail")
}

fn de<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Option<T> {
    bitcode::deserialize(bytes).ok()
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
/// Entries are stored as bitcode-serialized bytes. Vote, committed log ID,
/// and purged log ID are persisted via WAL metadata (always fsynced).
///
/// `C::Entry`, `Vote<C::NodeId>`, and `LogId<C::NodeId>` must implement
/// `serde::Serialize + serde::DeserializeOwned`.
///
/// The inner WAL is wrapped in `Arc<RwLock<AsyncRaftWal>>`, so cloning this
/// struct is cheap and all clones share the same in-memory state.
/// [`get_log_reader`](RaftLogStorage::get_log_reader) returns a clone,
/// ensuring readers always see the latest appended entries.
pub struct OpenRaftLogStorage<C: RaftTypeConfig> {
    wal: Arc<RwLock<AsyncRaftWal>>,
    _phantom: std::marker::PhantomData<C>,
}

impl<C: RaftTypeConfig> Clone for OpenRaftLogStorage<C> {
    fn clone(&self) -> Self {
        Self {
            wal: Arc::clone(&self.wal),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<C: RaftTypeConfig> OpenRaftLogStorage<C> {
    /// Creates a new storage backed by the given [`AsyncRaftWal`].
    #[must_use]
    pub fn new(wal: AsyncRaftWal) -> Self {
        Self {
            wal: Arc::new(RwLock::new(wal)),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Opens or creates storage in the given directory.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL cannot be opened or created.
    pub async fn open(data_dir: impl AsRef<std::path::Path>) -> crate::Result<Self> {
        Ok(Self::new(AsyncRaftWal::open(data_dir).await?))
    }

    /// Returns a write-lock guard to the underlying [`AsyncRaftWal`].
    ///
    /// Use this for direct WAL access (e.g. compaction, shutdown).
    pub async fn wal_mut(&self) -> RwLockWriteGuard<'_, AsyncRaftWal> {
        self.wal.write().await
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
        let wal = self.wal.read().await;
        let entries: Vec<C::Entry> = wal.iter_range(range).filter_map(|e| de(e.data)).collect();
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
        let wal = self.wal.read().await;
        let purged: Option<LogId<C::NodeId>> = wal.get_meta(META_PURGED).and_then(de);

        let last = wal.last_index().and_then(|idx| {
            wal.get(idx)
                .and_then(de::<C::Entry>)
                .map(|e| e.get_log_id().clone())
        });

        Ok(LogState {
            last_purged_log_id: purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        self.wal
            .write()
            .await
            .set_meta(META_VOTE, &ser(vote))
            .await
            .map_err(to_storage_err)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
        Ok(self.wal.read().await.get_meta(META_VOTE).and_then(de))
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<C::NodeId>>,
    ) -> Result<(), StorageError<C::NodeId>> {
        match committed {
            Some(log_id) => self
                .wal
                .write()
                .await
                .set_meta(META_COMMITTED, &ser(&log_id))
                .await
                .map_err(to_storage_err),
            None => Ok(()),
        }
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<C::NodeId>>, StorageError<C::NodeId>> {
        Ok(self.wal.read().await.get_meta(META_COMMITTED).and_then(de))
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
        let mut wal = self.wal.write().await;
        for entry in entries {
            let index = entry.get_log_id().index;
            let bytes = ser(&entry);
            wal.append(index, &bytes).await.map_err(to_storage_err)?;
        }
        // Flush + fsync before signaling durability to openraft.
        // Without this, entries may be in the in-memory buffer and lost on crash,
        // even though openraft considers them committed.
        wal.sync().await.map_err(to_storage_err)?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        self.wal
            .write()
            .await
            .truncate(log_id.index)
            .await
            .map_err(to_storage_err)
    }

    async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        let mut wal = self.wal.write().await;
        wal.set_meta(META_PURGED, &ser(&log_id))
            .await
            .map_err(to_storage_err)?;
        wal.compact(log_id.index).await.map_err(to_storage_err)
    }
}
