use std::fs::File;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Abstraction over the persistence layer for snapshot storage.
///
/// Implementations are responsible for durably storing and retrieving
/// opaque byte blobs (serialized store snapshots). The trait is
/// object-safe so that `Store` can hold a `Box<dyn StorageBackend>`.
pub trait StorageBackend: Send + Sync {
    /// Persist `data` to the backend, replacing any previous content.
    fn save(&self, data: &[u8]) -> io::Result<()>;

    /// Load the most recently saved data, or `NotFound` if nothing has
    /// been persisted yet.
    fn load(&self) -> io::Result<Vec<u8>>;

    /// Return `true` if a previous save exists.
    fn exists(&self) -> bool;
}

// ---------------------------------------------------------------------------
// FileBackend
// ---------------------------------------------------------------------------

/// File-based persistence backend using atomic write + fsync.
///
/// Writes go to a `.tmp` sibling file first, which is fsynced and then
/// renamed over the target path. This prevents half-written snapshots on
/// crash.
#[derive(Debug, Clone)]
pub struct FileBackend {
    path: PathBuf,
}

impl FileBackend {
    /// Create a new `FileBackend` that persists to `path`.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Return the path this backend writes to.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl StorageBackend for FileBackend {
    fn save(&self, data: &[u8]) -> io::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Atomic write: temp file -> fsync -> rename.
        let tmp_path = self.path.with_extension("tmp");
        let mut file = File::create(&tmp_path)?;
        file.write_all(data)?;
        file.sync_all()?;
        std::fs::rename(&tmp_path, &self.path)?;
        Ok(())
    }

    fn load(&self) -> io::Result<Vec<u8>> {
        std::fs::read(&self.path)
    }

    fn exists(&self) -> bool {
        self.path.exists()
    }
}

// ---------------------------------------------------------------------------
// MemoryBackend
// ---------------------------------------------------------------------------

/// In-memory persistence backend, useful for testing.
///
/// Data is stored in a `Mutex<Option<Vec<u8>>>`. The `Arc`-wrapped inner
/// state allows cloning the backend to inspect saved data from test code.
#[derive(Debug, Clone, Default)]
pub struct MemoryBackend {
    inner: Arc<Mutex<Option<Vec<u8>>>>,
}

impl MemoryBackend {
    /// Create a new, empty `MemoryBackend`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a clone of the stored data, if any.
    pub fn data(&self) -> Option<Vec<u8>> {
        self.inner.lock().unwrap().clone()
    }
}

impl StorageBackend for MemoryBackend {
    fn save(&self, data: &[u8]) -> io::Result<()> {
        *self.inner.lock().unwrap() = Some(data.to_vec());
        Ok(())
    }

    fn load(&self) -> io::Result<Vec<u8>> {
        self.inner
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no data saved"))
    }

    fn exists(&self) -> bool {
        self.inner.lock().unwrap().is_some()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------------------
    // FileBackend
    // ---------------------------------------------------------------

    #[test]
    fn file_backend_save_and_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");
        let backend = FileBackend::new(&path);

        assert!(!backend.exists());

        backend.save(b"hello world").unwrap();
        assert!(backend.exists());

        let data = backend.load().unwrap();
        assert_eq!(data, b"hello world");
    }

    #[test]
    fn file_backend_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");
        let backend = FileBackend::new(&path);

        backend.save(b"first").unwrap();
        backend.save(b"second").unwrap();

        let data = backend.load().unwrap();
        assert_eq!(data, b"second");
    }

    #[test]
    fn file_backend_load_missing_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.dat");
        let backend = FileBackend::new(&path);

        let err = backend.load().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn file_backend_creates_parent_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a").join("b").join("test.dat");
        let backend = FileBackend::new(&path);

        backend.save(b"nested").unwrap();
        assert!(path.exists());
    }

    #[test]
    fn file_backend_no_tmp_after_success() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");
        let tmp_path = dir.path().join("test.tmp");
        let backend = FileBackend::new(&path);

        backend.save(b"data").unwrap();
        assert!(path.exists());
        assert!(!tmp_path.exists());
    }

    // ---------------------------------------------------------------
    // MemoryBackend
    // ---------------------------------------------------------------

    #[test]
    fn memory_backend_save_and_load() {
        let backend = MemoryBackend::new();

        assert!(!backend.exists());

        backend.save(b"hello").unwrap();
        assert!(backend.exists());

        let data = backend.load().unwrap();
        assert_eq!(data, b"hello");
    }

    #[test]
    fn memory_backend_overwrite() {
        let backend = MemoryBackend::new();

        backend.save(b"first").unwrap();
        backend.save(b"second").unwrap();

        let data = backend.load().unwrap();
        assert_eq!(data, b"second");
    }

    #[test]
    fn memory_backend_load_empty_returns_error() {
        let backend = MemoryBackend::new();

        let err = backend.load().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn memory_backend_data_accessor() {
        let backend = MemoryBackend::new();
        assert!(backend.data().is_none());

        backend.save(b"peek").unwrap();
        assert_eq!(backend.data(), Some(b"peek".to_vec()));
    }

    #[test]
    fn memory_backend_clone_shares_state() {
        let backend = MemoryBackend::new();
        let clone = backend.clone();

        backend.save(b"shared").unwrap();
        assert_eq!(clone.load().unwrap(), b"shared");
    }
}
