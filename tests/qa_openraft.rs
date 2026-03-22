//! Compile-time QA for OpenRaftLogStorage (openraft 0.9).
//!
//! Verifies that OpenRaftLogStorage correctly implements RaftLogStorage
//! and RaftLogReader traits. Runtime integration tests require openraft's
//! internal LogFlushed callback which is not publicly constructable.

#![cfg(feature = "openraft-storage")]

use openraft::storage::RaftLogStorage;
use openraft::RaftLogReader;
use raft_wal::OpenRaftLogStorage;

openraft::declare_raft_types!(
    TC:
        D = String,
        R = String,
        NodeId = u64,
        Node = openraft::BasicNode,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

/// Compile-time assertion that OpenRaftLogStorage implements the required traits.
fn _assert_impls() {
    fn assert_log_reader<T: RaftLogReader<TC>>() {}
    fn assert_log_storage<T: RaftLogStorage<TC>>() {}

    assert_log_reader::<OpenRaftLogStorage<TC>>();
    assert_log_storage::<OpenRaftLogStorage<TC>>();
}

#[tokio::test]
async fn open_and_basic_meta() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = OpenRaftLogStorage::<TC>::open(dir.path()).await.unwrap();

    // read_vote is on RaftLogStorage, not RaftLogReader in 0.9
    let vote = s.read_vote().await.unwrap();
    assert!(vote.is_none());

    let v = openraft::Vote::new(1, 0);
    s.save_vote(&v).await.unwrap();

    let vote = s.read_vote().await.unwrap();
    assert_eq!(vote, Some(v));
}

#[tokio::test]
async fn save_read_committed() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = OpenRaftLogStorage::<TC>::open(dir.path()).await.unwrap();

    assert!(s.read_committed().await.unwrap().is_none());

    let log_id = openraft::LogId::new(openraft::CommittedLeaderId::new(1, 0), 5);
    s.save_committed(Some(log_id)).await.unwrap();
    assert_eq!(s.read_committed().await.unwrap(), Some(log_id));
}

#[tokio::test]
async fn get_log_state_empty() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = OpenRaftLogStorage::<TC>::open(dir.path()).await.unwrap();

    let state = s.get_log_state().await.unwrap();
    assert!(state.last_log_id.is_none());
    assert!(state.last_purged_log_id.is_none());
}
