//! Storage trait for pluggable WAL backends.

#[cfg(not(feature = "std"))]
use alloc::string::String;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// Trait abstracting file-like I/O for the WAL.
///
/// Implement this trait to back a [`GenericRaftWal`](crate::GenericRaftWal)
/// with custom storage (SPI flash, EEPROM, in-memory buffer, etc.).
///
/// File names are simple strings (e.g. `"00000000000000000001.seg"`,
/// `"meta.bin"`). The storage implementation decides how to map these
/// to the underlying medium.
pub trait WalStorage {
    /// Error type returned by storage operations.
    type Error: core::fmt::Debug;

    /// Reads the entire contents of the named file.
    ///
    /// Returns an error if the file does not exist.
    fn read_file(&self, name: &str) -> Result<Vec<u8>, Self::Error>;

    /// Creates or overwrites a file with the given contents.
    fn write_file(&mut self, name: &str, data: &[u8]) -> Result<(), Self::Error>;

    /// Appends data to an existing file (or creates it).
    fn append_file(&mut self, name: &str, data: &[u8]) -> Result<(), Self::Error>;

    /// Removes a file. Implementations should not error if the file
    /// does not exist.
    fn remove_file(&mut self, name: &str) -> Result<(), Self::Error>;

    /// Lists all file names that end with `suffix`, sorted.
    fn list_files(&self, suffix: &str) -> Result<Vec<String>, Self::Error>;

    /// Ensures buffered data for the named file is durably persisted.
    fn sync_file(&mut self, name: &str) -> Result<(), Self::Error>;

    /// Atomically replaces `to` with `from`.
    fn rename_file(&mut self, from: &str, to: &str) -> Result<(), Self::Error>;

    /// Returns the size of the named file in bytes.
    fn file_size(&self, name: &str) -> Result<u64, Self::Error>;

    /// Returns true if the named file exists.
    fn file_exists(&self, name: &str) -> bool;
}
