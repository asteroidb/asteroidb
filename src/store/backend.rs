use std::collections::BTreeMap;
use std::io;
#[cfg(not(target_arch = "wasm32"))]
use std::io::Write;
#[cfg(not(target_arch = "wasm32"))]
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
// FileBackend (not available on wasm32)
// ---------------------------------------------------------------------------

/// Fsync the directory containing `path`-like entries so that file
/// creations, renames, and deletions inside it are themselves durable.
///
/// Without this, a crash after `rename()` can lose the rename (the ALICE
/// study's most common durability defect): the file data is on disk but
/// the directory entry pointing at it is not.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn fsync_dir(dir: &Path) -> io::Result<()> {
    // A bare relative filename has an empty parent; that means "current dir".
    let dir = if dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        dir
    };
    let handle = std::fs::File::open(dir)?;
    handle.sync_all()
}

/// File-based persistence backend using atomic write + fsync.
///
/// Writes go to a `.tmp` sibling file first, which is fsynced and then
/// renamed over the target path. This prevents half-written snapshots on
/// crash.
///
/// Not available on `wasm32-unknown-unknown` (no filesystem access).
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
pub struct FileBackend {
    path: PathBuf,
}

#[cfg(not(target_arch = "wasm32"))]
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

#[cfg(not(target_arch = "wasm32"))]
impl StorageBackend for FileBackend {
    fn save(&self, data: &[u8]) -> io::Result<()> {
        use std::fs::File;

        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Atomic write: temp file -> fsync -> rename.
        // Use `<filename>.<pid>.<seq>.tmp` rather than `with_extension("tmp")`
        // so that paths that already end in `.tmp` (where `with_extension` is
        // a no-op) still get a distinct temporary path. The per-process
        // sequence number makes concurrent saves of the SAME target path
        // (e.g. the periodic checkpoint ticker racing the shutdown-path
        // final checkpoint) use distinct tmp files — sharing one tmp file
        // would interleave their writes and could rename a garbled snapshot
        // into place.
        static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let tmp_path = self.path.with_file_name(format!(
            "{}.{}.{}.tmp",
            self.path.file_name().unwrap_or_default().to_string_lossy(),
            std::process::id(),
            TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        ));
        let mut file = File::create(&tmp_path)?;
        file.write_all(data)?;
        file.sync_all()?;
        std::fs::rename(&tmp_path, &self.path)?;
        // The rename only becomes durable once the parent directory itself
        // is flushed; without it a crash can revert to the previous file
        // (or to nothing, for a first save).
        if let Some(parent) = self.path.parent() {
            fsync_dir(parent)?;
        }
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
        let backend = FileBackend::new(&path);

        backend.save(b"data").unwrap();
        assert!(path.exists());

        // The tmp file is named `<filename>.<pid>.tmp` (not simply `test.tmp`).
        // After a successful save the rename removes it; verify no `*.tmp` file
        // remains in the directory.
        let leftover_tmp = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| e.file_name().to_string_lossy().ends_with(".tmp"));
        assert!(
            !leftover_tmp,
            "no .tmp file should remain after a successful save"
        );
    }

    /// Concurrent saves to the same target must never install an
    /// interleaving of two payloads: each save uses its own tmp file, so
    /// the final content is exactly one writer's payload.
    #[test]
    fn concurrent_saves_to_same_path_stay_atomic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap.bin");
        let backend = FileBackend::new(&path);
        let payload_a = vec![0xAA_u8; 4096];
        let payload_b = vec![0xBB_u8; 8192];

        let t1 = std::thread::spawn({
            let backend = backend.clone();
            let payload = payload_a.clone();
            move || {
                for _ in 0..50 {
                    backend.save(&payload).unwrap();
                }
            }
        });
        let t2 = std::thread::spawn({
            let backend = backend.clone();
            let payload = payload_b.clone();
            move || {
                for _ in 0..50 {
                    backend.save(&payload).unwrap();
                }
            }
        });
        t1.join().unwrap();
        t2.join().unwrap();

        let data = backend.load().unwrap();
        assert!(
            data == payload_a || data == payload_b,
            "snapshot must be exactly one writer's payload, never a mix (len={})",
            data.len()
        );
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
