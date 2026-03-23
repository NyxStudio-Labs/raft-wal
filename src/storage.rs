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
    /// # Errors
    ///
    /// Returns an error if the file does not exist or cannot be read.
    fn read_file(&self, name: &str) -> Result<Vec<u8>, Self::Error>;

    /// Creates or overwrites a file with the given contents.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be written.
    fn write_file(&mut self, name: &str, data: &[u8]) -> Result<(), Self::Error>;

    /// Appends data to an existing file (or creates it).
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or written to.
    fn append_file(&mut self, name: &str, data: &[u8]) -> Result<(), Self::Error>;

    /// Removes a file. Implementations should not error if the file
    /// does not exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be removed.
    fn remove_file(&mut self, name: &str) -> Result<(), Self::Error>;

    /// Lists all file names that end with `suffix`, sorted.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying directory cannot be read.
    fn list_files(&self, suffix: &str) -> Result<Vec<String>, Self::Error>;

    /// Ensures buffered data for the named file is durably persisted.
    ///
    /// # Errors
    ///
    /// Returns an error if the sync operation fails.
    fn sync_file(&mut self, name: &str) -> Result<(), Self::Error>;

    /// Atomically replaces `to` with `from`.
    ///
    /// # Errors
    ///
    /// Returns an error if the rename operation fails.
    fn rename_file(&mut self, from: &str, to: &str) -> Result<(), Self::Error>;

    /// Returns the size of the named file in bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the file does not exist or its metadata cannot be read.
    fn file_size(&self, name: &str) -> Result<u64, Self::Error>;

    /// Returns true if the named file exists.
    fn file_exists(&self, name: &str) -> bool;

    /// Reads a byte range from the named file.
    ///
    /// Default implementation reads the entire file and slices. Override
    /// for backends that support efficient random access (e.g. seek+read).
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read.
    fn read_file_range(&self, name: &str, offset: usize, len: usize) -> Result<Vec<u8>, Self::Error> {
        let data = self.read_file(name)?;
        let end = (offset + len).min(data.len());
        let start = offset.min(end);
        Ok(data[start..end].to_vec())
    }
}
