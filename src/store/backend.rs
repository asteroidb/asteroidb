use std::collections::BTreeMap;
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

// ===========================================================================
// KvBackend — per-key operations for embedded storage
// ===========================================================================

/// Per-key storage abstraction for embedded persistent backends.
///
/// Unlike [`StorageBackend`] (which saves/loads opaque blobs for snapshot
/// persistence), `KvBackend` exposes fine-grained key-value operations
/// suitable for write-ahead or direct-persistence approaches.
pub trait KvBackend: Send + Sync {
    /// Return the value associated with `key`, or `None` if absent.
    fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>>;

    /// Insert or overwrite the value for `key`.
    fn put(&self, key: &str, value: &[u8]) -> io::Result<()>;

    /// Remove `key` from the store. No-op if absent.
    fn delete(&self, key: &str) -> io::Result<()>;

    /// Return all entries whose key starts with `prefix`, sorted by key.
    fn scan_prefix(&self, prefix: &str) -> io::Result<Vec<(String, Vec<u8>)>>;

    /// Return all entries whose key is lexicographically >= `frontier`,
    /// sorted by key. Useful for delta-sync catchup.
    fn entries_since(&self, frontier: &[u8]) -> io::Result<Vec<(String, Vec<u8>)>>;
}

// ---------------------------------------------------------------------------
// InMemoryKvBackend
// ---------------------------------------------------------------------------

/// In-memory `KvBackend` backed by a `BTreeMap`.
///
/// Useful for testing and as a drop-in when persistence is not required.
#[derive(Debug, Clone, Default)]
pub struct InMemoryKvBackend {
    inner: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
}

impl InMemoryKvBackend {
    /// Create a new, empty backend.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the number of stored entries (for test assertions).
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    /// Return `true` if the store contains no entries.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }
}

impl KvBackend for InMemoryKvBackend {
    fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        Ok(self.inner.lock().unwrap().get(key).cloned())
    }

    fn put(&self, key: &str, value: &[u8]) -> io::Result<()> {
        self.inner
            .lock()
            .unwrap()
            .insert(key.to_owned(), value.to_vec());
        Ok(())
    }

    fn delete(&self, key: &str) -> io::Result<()> {
        self.inner.lock().unwrap().remove(key);
        Ok(())
    }

    fn scan_prefix(&self, prefix: &str) -> io::Result<Vec<(String, Vec<u8>)>> {
        let guard = self.inner.lock().unwrap();
        let results: Vec<(String, Vec<u8>)> = guard
            .range(prefix.to_owned()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Ok(results)
    }

    fn entries_since(&self, frontier: &[u8]) -> io::Result<Vec<(String, Vec<u8>)>> {
        let frontier_str = String::from_utf8_lossy(frontier);
        let guard = self.inner.lock().unwrap();
        let results: Vec<(String, Vec<u8>)> = guard
            .range(frontier_str.as_ref().to_owned()..)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Ok(results)
    }
}

// ---------------------------------------------------------------------------
// RedbBackend
// ---------------------------------------------------------------------------

#[cfg(feature = "native-storage")]
/// Persistent `KvBackend` backed by [redb](https://docs.rs/redb).
///
/// Uses a single B+tree table for all key-value pairs. Provides ACID
/// transactions. Requires the `native-storage` feature (uses libc for
/// mmap/file I/O, not available on `wasm32-unknown-unknown`).
pub struct RedbBackend {
    db: redb::Database,
}

#[cfg(feature = "native-storage")]
const TABLE: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new("kv");

#[cfg(feature = "native-storage")]
impl RedbBackend {
    /// Open (or create) a redb database at `path`.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let db = redb::Database::create(path.as_ref()).map_err(io::Error::other)?;
        Ok(Self { db })
    }
}

#[cfg(feature = "native-storage")]
impl KvBackend for RedbBackend {
    fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        let tx = self.db.begin_read().map_err(io::Error::other)?;
        let table = match tx.open_table(TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(io::Error::other(e)),
        };
        match table.get(key) {
            Ok(Some(val)) => Ok(Some(val.value().to_vec())),
            Ok(None) => Ok(None),
            Err(e) => Err(io::Error::other(e)),
        }
    }

    fn put(&self, key: &str, value: &[u8]) -> io::Result<()> {
        let tx = self.db.begin_write().map_err(io::Error::other)?;
        {
            let mut table = tx.open_table(TABLE).map_err(io::Error::other)?;
            table.insert(key, value).map_err(io::Error::other)?;
        }
        tx.commit().map_err(io::Error::other)?;
        Ok(())
    }

    fn delete(&self, key: &str) -> io::Result<()> {
        let tx = self.db.begin_write().map_err(io::Error::other)?;
        {
            let mut table = tx.open_table(TABLE).map_err(io::Error::other)?;
            table.remove(key).map_err(io::Error::other)?;
        }
        tx.commit().map_err(io::Error::other)?;
        Ok(())
    }

    fn scan_prefix(&self, prefix: &str) -> io::Result<Vec<(String, Vec<u8>)>> {
        let tx = self.db.begin_read().map_err(io::Error::other)?;
        let table = match tx.open_table(TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(io::Error::other(e)),
        };
        let iter = table.range(prefix..).map_err(io::Error::other)?;
        let mut results = Vec::new();
        for entry in iter {
            let entry = entry.map_err(io::Error::other)?;
            let k = entry.0.value().to_owned();
            if !k.starts_with(prefix) {
                break;
            }
            results.push((k, entry.1.value().to_vec()));
        }
        Ok(results)
    }

    fn entries_since(&self, frontier: &[u8]) -> io::Result<Vec<(String, Vec<u8>)>> {
        let frontier_str = String::from_utf8_lossy(frontier);
        let tx = self.db.begin_read().map_err(io::Error::other)?;
        let table = match tx.open_table(TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(io::Error::other(e)),
        };
        let iter = table
            .range(frontier_str.as_ref()..)
            .map_err(io::Error::other)?;
        let mut results = Vec::new();
        for entry in iter {
            let entry = entry.map_err(io::Error::other)?;
            results.push((entry.0.value().to_owned(), entry.1.value().to_vec()));
        }
        Ok(results)
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

    // ---------------------------------------------------------------
    // KvBackend — shared conformance suite
    // ---------------------------------------------------------------

    /// Run the standard KvBackend conformance suite against any impl.
    fn kv_backend_conformance(b: &dyn KvBackend) {
        // get on missing key
        assert!(b.get("missing").unwrap().is_none());

        // put + get
        b.put("key1", b"val1").unwrap();
        assert_eq!(b.get("key1").unwrap(), Some(b"val1".to_vec()));

        // overwrite
        b.put("key1", b"val1-updated").unwrap();
        assert_eq!(b.get("key1").unwrap(), Some(b"val1-updated".to_vec()));

        // delete
        b.delete("key1").unwrap();
        assert!(b.get("key1").unwrap().is_none());

        // delete non-existent is no-op
        b.delete("no-such-key").unwrap();

        // scan_prefix
        b.put("user:1", b"alice").unwrap();
        b.put("user:2", b"bob").unwrap();
        b.put("user:3", b"carol").unwrap();
        b.put("order:1", b"o1").unwrap();

        let users = b.scan_prefix("user:").unwrap();
        assert_eq!(users.len(), 3);
        assert_eq!(users[0].0, "user:1");
        assert_eq!(users[1].0, "user:2");
        assert_eq!(users[2].0, "user:3");

        let orders = b.scan_prefix("order:").unwrap();
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].0, "order:1");

        // empty prefix scan
        let none = b.scan_prefix("zzz:").unwrap();
        assert!(none.is_empty());

        // entries_since
        let since = b.entries_since(b"user:2").unwrap();
        assert!(since.len() >= 2); // user:2, user:3
        assert_eq!(since[0].0, "user:2");
        assert_eq!(since[1].0, "user:3");
    }

    // ---------------------------------------------------------------
    // InMemoryKvBackend
    // ---------------------------------------------------------------

    #[test]
    fn in_memory_kv_conformance() {
        let b = InMemoryKvBackend::new();
        kv_backend_conformance(&b);
    }

    #[test]
    fn in_memory_kv_len_and_is_empty() {
        let b = InMemoryKvBackend::new();
        assert!(b.is_empty());
        assert_eq!(b.len(), 0);

        b.put("a", b"1").unwrap();
        assert!(!b.is_empty());
        assert_eq!(b.len(), 1);

        b.delete("a").unwrap();
        assert!(b.is_empty());
    }

    // ---------------------------------------------------------------
    // RedbBackend
    // ---------------------------------------------------------------

    #[cfg(feature = "native-storage")]
    #[test]
    fn redb_kv_conformance() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.redb");
        let b = RedbBackend::open(&path).unwrap();
        kv_backend_conformance(&b);
    }

    #[cfg(feature = "native-storage")]
    #[test]
    fn redb_persistence_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("reopen.redb");

        {
            let b = RedbBackend::open(&path).unwrap();
            b.put("persist", b"data").unwrap();
        }

        // Re-open the same database file and verify data survives.
        let b = RedbBackend::open(&path).unwrap();
        assert_eq!(b.get("persist").unwrap(), Some(b"data".to_vec()));
    }

    #[cfg(feature = "native-storage")]
    #[test]
    fn redb_delete_and_scan_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("del.redb");
        let b = RedbBackend::open(&path).unwrap();

        b.put("x", b"1").unwrap();
        b.delete("x").unwrap();
        assert!(b.get("x").unwrap().is_none());

        let all = b.scan_prefix("").unwrap();
        assert!(all.is_empty());
    }
}
