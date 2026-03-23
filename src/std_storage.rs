//! Filesystem-backed [`WalStorage`] implementation using `std::fs`.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::storage::WalStorage;

/// A [`WalStorage`] backend that maps file names to a directory on the
/// local filesystem.
pub struct StdStorage {
    dir: PathBuf,
}

impl StdStorage {
    /// Creates a new `StdStorage` rooted at the given directory.
    ///
    /// The directory is created if it does not exist.
    pub fn new(dir: impl AsRef<Path>) -> std::io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    /// Returns the directory path this storage is rooted at.
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

impl WalStorage for StdStorage {
    type Error = std::io::Error;

    fn read_file(&self, name: &str) -> Result<Vec<u8>, Self::Error> {
        fs::read(self.path(name))
    }

    fn write_file(&mut self, name: &str, data: &[u8]) -> Result<(), Self::Error> {
        fs::write(self.path(name), data)
    }

    fn append_file(&mut self, name: &str, data: &[u8]) -> Result<(), Self::Error> {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.path(name))?;
        file.write_all(data)
    }

    fn remove_file(&mut self, name: &str) -> Result<(), Self::Error> {
        match fs::remove_file(self.path(name)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn list_files(&self, suffix: &str) -> Result<Vec<String>, Self::Error> {
        let mut names: Vec<String> = fs::read_dir(&self.dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().into_owned();
                if name.ends_with(suffix) {
                    Some(name)
                } else {
                    None
                }
            })
            .collect();
        names.sort();
        Ok(names)
    }

    fn sync_file(&mut self, name: &str) -> Result<(), Self::Error> {
        // Open in write mode so we sync the same fd semantics as the writer.
        // Opening read-only and calling sync_all() works on Linux (shared page
        // cache) but is not guaranteed by POSIX.
        let file = fs::OpenOptions::new().write(true).open(self.path(name))?;
        file.sync_data()
    }

    fn rename_file(&mut self, from: &str, to: &str) -> Result<(), Self::Error> {
        fs::rename(self.path(from), self.path(to))?;
        // Sync the directory to ensure the rename is durable on crash.
        // Without this, ext4 with data=ordered may lose the rename.
        let dir = fs::File::open(&self.dir)?;
        dir.sync_all()?;
        Ok(())
    }

    fn file_size(&self, name: &str) -> Result<u64, Self::Error> {
        Ok(fs::metadata(self.path(name))?.len())
    }

    fn file_exists(&self, name: &str) -> bool {
        self.path(name).exists()
    }

    fn read_file_range(&self, name: &str, offset: usize, len: usize) -> Result<Vec<u8>, Self::Error> {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = fs::File::open(self.path(name))?;
        file.seek(SeekFrom::Start(offset as u64))?;
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf)?;
        Ok(buf)
    }
}
