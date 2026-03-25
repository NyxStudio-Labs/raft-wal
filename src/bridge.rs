//! Bridged `io_uring` WAL for use from standard Tokio runtimes.
//!
//! Linux-only. Enable with the `io-uring-bridge` feature flag.
//!
//! [`BridgedUringWal`] spawns a dedicated thread running its own
//! `tokio_uring` runtime and communicates via lock-free channels,
//! allowing callers on a normal `#[tokio::main]` runtime to benefit
//! from `io_uring` without switching runtimes.

#[cfg(target_os = "linux")]
mod inner {
    use std::path::{Path, PathBuf};
    use std::thread::JoinHandle;

    use crossbeam_channel::{Receiver, Sender};

    use crate::Result;

    // ------------------------------------------------------------------
    // Request / Response types
    // ------------------------------------------------------------------

    type Reply<T> = ::tokio::sync::oneshot::Sender<T>;

    enum Request {
        Append {
            index: u64,
            data: Vec<u8>,
            reply: Reply<Result<()>>,
        },
        AppendBatch {
            entries: Vec<(u64, Vec<u8>)>,
            reply: Reply<Result<()>>,
        },
        Sync {
            reply: Reply<Result<()>>,
        },
        Flush {
            reply: Reply<Result<()>>,
        },
        Compact {
            up_to: u64,
            reply: Reply<Result<()>>,
        },
        Truncate {
            from: u64,
            reply: Reply<Result<()>>,
        },
        SetMeta {
            key: String,
            value: Vec<u8>,
            reply: Reply<Result<()>>,
        },
        RemoveMeta {
            key: String,
            reply: Reply<Result<()>>,
        },
        GetMeta {
            key: String,
            reply: Reply<Option<Vec<u8>>>,
        },
        Get {
            index: u64,
            reply: Reply<Option<Vec<u8>>>,
        },
        ReadRange {
            start: u64,
            end_inclusive: u64,
            reply: Reply<Vec<(u64, Vec<u8>)>>,
        },
        FirstIndex {
            reply: Reply<Option<u64>>,
        },
        LastIndex {
            reply: Reply<Option<u64>>,
        },
        Len {
            reply: Reply<usize>,
        },
        IsEmpty {
            reply: Reply<bool>,
        },
        Shutdown {
            reply: Reply<Result<()>>,
        },
    }

    /// Bridged `io_uring` WAL usable from a standard Tokio runtime.
    ///
    /// Internally spawns a dedicated OS thread running a `tokio_uring`
    /// event loop. All WAL operations are dispatched to that thread via
    /// a lock-free [`crossbeam_channel`] and results are returned through
    /// [`tokio::sync::oneshot`] channels.
    ///
    /// # Drop behaviour
    ///
    /// Dropping a `BridgedUringWal` sends a shutdown request and joins the
    /// background thread. Use [`close`](Self::close) for explicit async
    /// shutdown with error handling.
    pub struct BridgedUringWal {
        tx: Option<Sender<Request>>,
        handle: Option<JoinHandle<()>>,
    }

    impl BridgedUringWal {
        /// Opens or creates a WAL in the given directory.
        ///
        /// Spawns a background thread with its own `tokio_uring` runtime.
        ///
        /// # Errors
        ///
        /// Returns an error if the WAL cannot be opened or the background
        /// thread fails to start.
        pub async fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
            let dir = data_dir.as_ref().to_path_buf();
            let (tx, rx): (Sender<Request>, Receiver<Request>) =
                crossbeam_channel::unbounded();

            // Use a oneshot to receive the open result from the uring thread
            let (open_tx, open_rx) = ::tokio::sync::oneshot::channel::<Result<()>>();

            let handle = std::thread::Builder::new()
                .name("raft-wal-uring".into())
                .spawn(move || {
                    uring_thread_main(dir, rx, open_tx);
                })
                .map_err(|e| crate::WalError::Io(
                    std::io::Error::other(format!("failed to spawn uring thread: {e}")),
                ))?;

            // Wait for the WAL to be opened on the uring thread
            let result = open_rx.await.map_err(|_| {
                crate::WalError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "uring thread exited before open completed",
                ))
            })?;
            result?;

            Ok(Self {
                tx: Some(tx),
                handle: Some(handle),
            })
        }

        /// Appends a single log entry.
        ///
        /// # Errors
        ///
        /// Returns an error if the underlying I/O operations fail.
        pub async fn append(&self, index: u64, data: &[u8]) -> Result<()> {
            let (reply, rx) = ::tokio::sync::oneshot::channel();
            self.send(Request::Append {
                index,
                data: data.to_vec(),
                reply,
            })?;
            recv(rx).await
        }

        /// Appends multiple log entries.
        ///
        /// # Errors
        ///
        /// Returns an error if the underlying I/O operations fail.
        pub async fn append_batch(&self, entries: &[(u64, Vec<u8>)]) -> Result<()> {
            let (reply, rx) = ::tokio::sync::oneshot::channel();
            self.send(Request::AppendBatch {
                entries: entries.to_vec(),
                reply,
            })?;
            recv(rx).await
        }

        /// Flushes buffered writes and fsyncs data to stable storage.
        ///
        /// # Errors
        ///
        /// Returns an error if the flush or sync operation fails.
        pub async fn sync(&self) -> Result<()> {
            let (reply, rx) = ::tokio::sync::oneshot::channel();
            self.send(Request::Sync { reply })?;
            recv(rx).await
        }

        /// Flushes buffered writes to the OS (without fsync).
        ///
        /// # Errors
        ///
        /// Returns an error if the flush operation fails.
        pub async fn flush(&self) -> Result<()> {
            let (reply, rx) = ::tokio::sync::oneshot::channel();
            self.send(Request::Flush { reply })?;
            recv(rx).await
        }

        /// Discards all entries with index <= `up_to_inclusive`.
        ///
        /// # Errors
        ///
        /// Returns an error if the underlying I/O operations fail.
        pub async fn compact(&self, up_to_inclusive: u64) -> Result<()> {
            let (reply, rx) = ::tokio::sync::oneshot::channel();
            self.send(Request::Compact {
                up_to: up_to_inclusive,
                reply,
            })?;
            recv(rx).await
        }

        /// Discards all entries with index >= `from_inclusive`.
        ///
        /// # Errors
        ///
        /// Returns an error if the underlying I/O operations fail.
        pub async fn truncate(&self, from_inclusive: u64) -> Result<()> {
            let (reply, rx) = ::tokio::sync::oneshot::channel();
            self.send(Request::Truncate {
                from: from_inclusive,
                reply,
            })?;
            recv(rx).await
        }

        /// Stores a metadata key-value pair. Always fsynced.
        ///
        /// # Errors
        ///
        /// Returns an error if persisting the metadata fails.
        pub async fn set_meta(&self, key: &str, value: &[u8]) -> Result<()> {
            let (reply, rx) = ::tokio::sync::oneshot::channel();
            self.send(Request::SetMeta {
                key: key.to_string(),
                value: value.to_vec(),
                reply,
            })?;
            recv(rx).await
        }

        /// Removes a metadata key.
        ///
        /// # Errors
        ///
        /// Returns an error if persisting the metadata fails.
        pub async fn remove_meta(&self, key: &str) -> Result<()> {
            let (reply, rx) = ::tokio::sync::oneshot::channel();
            self.send(Request::RemoveMeta {
                key: key.to_string(),
                reply,
            })?;
            recv(rx).await
        }

        /// Returns the metadata value for the given key (owned copy).
        ///
        /// # Errors
        ///
        /// Returns an error if the bridge channel is broken.
        pub async fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>> {
            let (reply, rx) = ::tokio::sync::oneshot::channel();
            self.send(Request::GetMeta {
                key: key.to_string(),
                reply,
            })?;
            recv_infallible(rx).await
        }

        /// Returns the entry at the given index (owned copy).
        ///
        /// # Errors
        ///
        /// Returns an error if the bridge channel is broken.
        pub async fn get(&self, index: u64) -> Result<Option<Vec<u8>>> {
            let (reply, rx) = ::tokio::sync::oneshot::channel();
            self.send(Request::Get { index, reply })?;
            recv_infallible(rx).await
        }

        /// Returns entries within the given index range as owned pairs.
        ///
        /// # Errors
        ///
        /// Returns an error if the bridge channel is broken.
        pub async fn read_range(
            &self,
            start: u64,
            end_inclusive: u64,
        ) -> Result<Vec<(u64, Vec<u8>)>> {
            let (reply, rx) = ::tokio::sync::oneshot::channel();
            self.send(Request::ReadRange {
                start,
                end_inclusive,
                reply,
            })?;
            recv_infallible(rx).await
        }

        /// Returns the first (lowest) index in the log.
        ///
        /// # Errors
        ///
        /// Returns an error if the bridge channel is broken.
        pub async fn first_index(&self) -> Result<Option<u64>> {
            let (reply, rx) = ::tokio::sync::oneshot::channel();
            self.send(Request::FirstIndex { reply })?;
            recv_infallible(rx).await
        }

        /// Returns the last (highest) index in the log.
        ///
        /// # Errors
        ///
        /// Returns an error if the bridge channel is broken.
        pub async fn last_index(&self) -> Result<Option<u64>> {
            let (reply, rx) = ::tokio::sync::oneshot::channel();
            self.send(Request::LastIndex { reply })?;
            recv_infallible(rx).await
        }

        /// Returns the number of entries in the log.
        ///
        /// # Errors
        ///
        /// Returns an error if the bridge channel is broken.
        pub async fn len(&self) -> Result<usize> {
            let (reply, rx) = ::tokio::sync::oneshot::channel();
            self.send(Request::Len { reply })?;
            recv_infallible(rx).await
        }

        /// Returns `true` if the log contains no entries.
        ///
        /// # Errors
        ///
        /// Returns an error if the bridge channel is broken.
        pub async fn is_empty(&self) -> Result<bool> {
            let (reply, rx) = ::tokio::sync::oneshot::channel();
            self.send(Request::IsEmpty { reply })?;
            recv_infallible(rx).await
        }

        /// Flushes, syncs, and shuts down the WAL and its background thread.
        ///
        /// # Errors
        ///
        /// Returns an error if the flush, sync, or close operation fails.
        pub async fn close(mut self) -> Result<()> {
            self.shutdown_inner().await
        }

        async fn shutdown_inner(&mut self) -> Result<()> {
            if let Some(tx) = self.tx.take() {
                let (reply, rx) = ::tokio::sync::oneshot::channel();
                let _ = tx.send(Request::Shutdown { reply });
                let result = recv(rx).await;
                if let Some(handle) = self.handle.take() {
                    let _ = handle.join();
                }
                return result;
            }
            Ok(())
        }

        fn send(&self, req: Request) -> Result<()> {
            self.tx
                .as_ref()
                .ok_or_else(|| {
                    crate::WalError::Io(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "uring bridge is shut down",
                    ))
                })?
                .send(req)
                .map_err(|_| {
                    crate::WalError::Io(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "uring thread exited unexpectedly",
                    ))
                })
        }
    }

    impl Drop for BridgedUringWal {
        fn drop(&mut self) {
            if let Some(tx) = self.tx.take() {
                let (reply, _rx) = ::tokio::sync::oneshot::channel();
                let _ = tx.send(Request::Shutdown { reply });
                drop(tx); // close the channel so the thread exits
                if let Some(handle) = self.handle.take() {
                    let _ = handle.join();
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Response helpers
    // ------------------------------------------------------------------

    async fn recv(rx: ::tokio::sync::oneshot::Receiver<Result<()>>) -> Result<()> {
        rx.await.map_err(|_| {
            crate::WalError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "uring thread dropped reply",
            ))
        })?
    }

    async fn recv_infallible<T>(rx: ::tokio::sync::oneshot::Receiver<T>) -> Result<T> {
        rx.await.map_err(|_| {
            crate::WalError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "uring thread dropped reply",
            ))
        })
    }

    // ------------------------------------------------------------------
    // Background uring thread
    // ------------------------------------------------------------------

    fn uring_thread_main(
        dir: PathBuf,
        rx: Receiver<Request>,
        open_tx: ::tokio::sync::oneshot::Sender<Result<()>>,
    ) {
        tokio_uring::start(async move {
            let wal = crate::UringRaftWal::open(&dir).await;
            let mut wal = match wal {
                Ok(w) => {
                    let _ = open_tx.send(Ok(()));
                    w
                }
                Err(e) => {
                    let _ = open_tx.send(Err(e));
                    return;
                }
            };

            loop {
                // Block the uring thread waiting for the next request.
                // crossbeam recv is blocking, which is fine because this
                // thread is dedicated to WAL I/O.
                let req = match rx.recv() {
                    Ok(r) => r,
                    Err(_) => break, // channel closed, exit
                };

                match req {
                    Request::Append { index, data, reply } => {
                        let _ = reply.send(wal.append(index, &data).await);
                    }
                    Request::AppendBatch { entries, reply } => {
                        let _ = reply.send(wal.append_batch(&entries).await);
                    }
                    Request::Sync { reply } => {
                        let _ = reply.send(wal.sync().await);
                    }
                    Request::Flush { reply } => {
                        // UringRaftWal doesn't have a separate flush, use sync
                        let _ = reply.send(wal.sync().await);
                    }
                    Request::Compact { up_to, reply } => {
                        let _ = reply.send(wal.compact(up_to).await);
                    }
                    Request::Truncate { from, reply } => {
                        let _ = reply.send(wal.truncate(from).await);
                    }
                    Request::SetMeta { key, value, reply } => {
                        let _ = reply.send(wal.set_meta(&key, &value).await);
                    }
                    Request::RemoveMeta { key, reply } => {
                        let _ = reply.send(wal.remove_meta(&key).await);
                    }
                    Request::GetMeta { key, reply } => {
                        let val = wal.get_meta(&key).map(|v| v.to_vec());
                        let _ = reply.send(val);
                    }
                    Request::Get { index, reply } => {
                        let val = wal.get(index).map(|v| v.to_vec());
                        let _ = reply.send(val);
                    }
                    Request::ReadRange {
                        start,
                        end_inclusive,
                        reply,
                    } => {
                        let entries: Vec<(u64, Vec<u8>)> = wal
                            .iter_range(start..=end_inclusive)
                            .map(|e| (e.index, e.data.to_vec()))
                            .collect();
                        let _ = reply.send(entries);
                    }
                    Request::FirstIndex { reply } => {
                        let _ = reply.send(wal.first_index());
                    }
                    Request::LastIndex { reply } => {
                        let _ = reply.send(wal.last_index());
                    }
                    Request::Len { reply } => {
                        let _ = reply.send(wal.len());
                    }
                    Request::IsEmpty { reply } => {
                        let _ = reply.send(wal.is_empty());
                    }
                    Request::Shutdown { reply } => {
                        let _ = reply.send(wal.close().await);
                        break;
                    }
                }
            }
        });
    }
}

#[cfg(target_os = "linux")]
pub use inner::BridgedUringWal;
