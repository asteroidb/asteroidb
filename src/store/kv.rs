use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::crdt::lww_register::LwwRegister;
use crate::crdt::or_map::OrMap;
use crate::crdt::or_set::OrSet;
use crate::crdt::pn_counter::PnCounter;
use crate::error::CrdtError;
use crate::hlc::HlcTimestamp;
#[cfg(not(target_arch = "wasm32"))]
use crate::store::backend::FileBackend;
use crate::store::backend::StorageBackend;
use crate::store::migration;

/// Current persistence format version written by this code.
///
/// v3: added the session-guarantee fields (`applied_origins`,
/// `merge_failed_keys`, `pruned_floor`, `visible_origins`) to [`Store`].
/// v1/v2 snapshots contain only `data` + `timestamps` and are migrated on
/// load (JSON via the migration registry, bincode via [`StoreV2Layout`]).
pub const CURRENT_FORMAT_VERSION: u32 = 3;

/// Versioned envelope for persisted store data.
///
/// All snapshots written by this code include a `format_version` field.
/// On load, the version is checked and migrations are applied as needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedStore {
    format_version: u32,
    store: serde_json::Value,
}

/// A CRDT value stored in the KVS.
///
/// Wraps all supported CRDT types so the store can hold heterogeneous
/// values while preserving type-safe merge semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CrdtValue {
    Counter(PnCounter),
    Set(OrSet<String>),
    Map(OrMap<String, String>),
    Register(LwwRegister<String>),
}

impl CrdtValue {
    /// Returns a human-readable type name for error reporting.
    pub fn type_name(&self) -> &'static str {
        match self {
            CrdtValue::Counter(_) => "Counter",
            CrdtValue::Set(_) => "Set",
            CrdtValue::Map(_) => "Map",
            CrdtValue::Register(_) => "Register",
        }
    }

    /// Extract changes since the given frontier timestamp.
    ///
    /// Delegates to the underlying CRDT type's `delta_since` method.
    /// Returns `None` when there is nothing to send (e.g., the value
    /// has not been modified since the frontier).
    pub fn delta_since(&self, frontier: &HlcTimestamp) -> Option<Self> {
        match self {
            CrdtValue::Counter(c) => c.delta_since(frontier).map(CrdtValue::Counter),
            CrdtValue::Set(s) => s.delta_since(frontier).map(CrdtValue::Set),
            CrdtValue::Map(m) => m.delta_since(frontier).map(CrdtValue::Map),
            CrdtValue::Register(r) => r.delta_since(frontier).map(CrdtValue::Register),
        }
    }

    /// Merge a delta into this CRDT value.
    ///
    /// Returns `Err` if the delta type does not match the existing value type.
    pub fn merge_delta(&mut self, delta: &CrdtValue) -> Result<(), CrdtError> {
        match (self, delta) {
            (CrdtValue::Counter(a), CrdtValue::Counter(b)) => {
                a.merge_delta(b);
                Ok(())
            }
            (CrdtValue::Set(a), CrdtValue::Set(b)) => {
                a.merge_delta(b);
                Ok(())
            }
            (CrdtValue::Map(a), CrdtValue::Map(b)) => {
                a.merge_delta(b);
                Ok(())
            }
            (CrdtValue::Register(a), CrdtValue::Register(b)) => {
                a.merge_delta(b);
                Ok(())
            }
            (existing, incoming) => Err(CrdtError::TypeMismatch {
                expected: existing.type_name().to_string(),
                actual: incoming.type_name().to_string(),
            }),
        }
    }

    /// Feed this value's canonical byte representation into `hasher`
    /// (digest-based anti-entropy, see [`crate::store::digest`]).
    ///
    /// Dispatches to the per-CRDT `digest_into` methods, each of which
    /// prefixes its own type tag (0x01 Register, 0x02 Counter, 0x03 Set,
    /// 0x04 Map) and emits a deterministic, sorted canonical stream.
    /// NEVER hash raw bincode/JSON output of a `CrdtValue` instead: the
    /// inner `HashMap`/`HashSet` iteration order is non-deterministic.
    ///
    /// # MAINTAINER CONTRACT
    /// Adding a `CrdtValue` variant REQUIRES a new type tag here plus a
    /// `DIGEST_SCHEME_VERSION` bump (see `crate::store::digest`).
    pub fn canonical_digest_into(&self, hasher: &mut sha2::Sha256) {
        match self {
            CrdtValue::Counter(c) => c.digest_into(hasher),
            CrdtValue::Set(s) => s.digest_into(hasher),
            CrdtValue::Map(m) => m.digest_into(hasher),
            CrdtValue::Register(r) => r.digest_into(hasher),
        }
    }
}

/// Key-value store backed by CRDT values (FR-001).
///
/// Provides basic CRUD operations, prefix-based key space partitioning,
/// and CRDT-aware value merging with type checking. Supports HLC-based
/// change tracking for delta sync.
///
/// The data map uses a `BTreeMap` for efficient O(log n + m) prefix range
/// scans (see [`keys_with_prefix`](Self::keys_with_prefix)).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Store {
    data: BTreeMap<String, CrdtValue>,
    /// Per-key HLC timestamp of the last modification, used for delta sync.
    #[serde(default)]
    timestamps: HashMap<String, HlcTimestamp>,
    /// Per-origin applied frontier used for session guarantees
    /// (read-your-writes / monotonic reads).
    ///
    /// Invariant: `applied_origins[o] = h` means this store contains the
    /// effects of ALL writes by origin node `o` with HLC `<= h` (or a CRDT
    /// state that dominates them), except for keys in `merge_failed_keys`.
    /// The map is max-monotone and is deliberately NOT touched by
    /// `prune_timestamps_before` or `delete`, so old session tokens remain
    /// satisfiable after compaction.
    #[serde(default)]
    applied_origins: HashMap<String, HlcTimestamp>,
    /// Keys permanently poisoned by a failed remote merge (type mismatch).
    ///
    /// For these keys the `applied_origins` invariant does not hold — a
    /// remote contribution was dropped — so session checks must not rely
    /// on path A for them. Persisted together with `applied_origins`:
    /// losing the poison set while keeping the frontier would produce
    /// false session successes after restart.
    #[serde(default)]
    merge_failed_keys: HashSet<String>,
    /// Highest frontier ever passed to `prune_timestamps_before` that
    /// actually pruned entries. Used ONLY as a guard for delta-sync
    /// frontier adoption (a receiver may adopt this sender's
    /// `applied_origins` only if it requested a delta from a frontier at
    /// or above this floor); never consulted for session satisfaction.
    #[serde(default)]
    pruned_floor: Option<HlcTimestamp>,
    /// Per-origin max HLC of ANY contribution merged into visible state —
    /// claimed or not. Always a superset (per-origin max) of
    /// `applied_origins`.
    ///
    /// Used ONLY to build response session tokens: the token must cover
    /// everything the client may have observed, including contributions
    /// that arrived through possibly-incomplete (unclaimed) deltas.
    /// Over-covering is safe (a later replica merely answers 412 more
    /// often — the false-negative direction); under-covering would let a
    /// stale replica satisfy the token while serving an older value (a
    /// monotonic-reads lie). Never consulted by `is_satisfied`.
    #[serde(default)]
    visible_origins: HashMap<String, HlcTimestamp>,
}

/// Structural layout of `Store` persisted by format versions 1 and 2
/// (bincode is positional and non-self-describing, so old snapshots must
/// be decoded with the exact old field layout — `#[serde(default)]` on
/// the current struct cannot rescue missing trailing fields).
#[derive(Debug, Deserialize)]
struct StoreV2Layout {
    data: BTreeMap<String, CrdtValue>,
    #[serde(default)]
    timestamps: HashMap<String, HlcTimestamp>,
}

impl From<StoreV2Layout> for Store {
    fn from(old: StoreV2Layout) -> Self {
        let mut store = Store {
            data: old.data,
            timestamps: old.timestamps,
            applied_origins: HashMap::new(),
            merge_failed_keys: HashSet::new(),
            pruned_floor: None,
            visible_origins: HashMap::new(),
        };
        store.rebuild_visible_origins();
        store
    }
}

/// Max-merge `hlc` into a per-origin frontier map.
///
/// Entries with an empty node id (e.g. zero sentinel frontiers) are
/// ignored: they name no origin, and a session-token entry with an empty
/// node id would be rejected on parse.
fn max_merge_origin(map: &mut HashMap<String, HlcTimestamp>, hlc: &HlcTimestamp) {
    if hlc.node_id.is_empty() {
        return;
    }
    match map.get_mut(&hlc.node_id) {
        Some(existing) => {
            if *hlc > *existing {
                *existing = hlc.clone();
            }
        }
        None => {
            map.insert(hlc.node_id.clone(), hlc.clone());
        }
    }
}

/// Compute the exclusive upper bound for a BTreeMap range scan.
///
/// Increments the last byte of `prefix` to produce a string that is
/// lexicographically just past all strings that start with `prefix`.
/// Returns `None` if the prefix consists entirely of `0xFF` bytes.
fn prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut bytes = prefix.as_bytes().to_vec();
    while let Some(last) = bytes.last_mut() {
        if *last < 0xFF {
            *last += 1;
            return Some(String::from_utf8_lossy(&bytes).into_owned());
        }
        bytes.pop();
    }
    None
}

impl Store {
    /// Create a new, empty store.
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            timestamps: HashMap::new(),
            applied_origins: HashMap::new(),
            merge_failed_keys: HashSet::new(),
            pruned_floor: None,
            visible_origins: HashMap::new(),
        }
    }

    /// Get a reference to the value associated with `key`.
    pub fn get(&self, key: &str) -> Option<&CrdtValue> {
        self.data.get(key)
    }

    /// Get a mutable reference to the value associated with `key`.
    pub fn get_mut(&mut self, key: &str) -> Option<&mut CrdtValue> {
        self.data.get_mut(key)
    }

    /// Insert or replace a value for the given key.
    ///
    /// # Warning
    /// This method does NOT update the `timestamps` map. Callers that hold
    /// an `HlcTimestamp` MUST use `put_with_timestamp` instead — skipping it
    /// causes the entry to be invisible to `delta_sync` / `entries_since`.
    /// When no HLC is available, call [`record_change`](Self::record_change)
    /// immediately after this method.
    pub fn put(&mut self, key: String, value: CrdtValue) {
        self.data.insert(key, value);
    }

    /// Insert or replace a value for the given key, atomically recording the
    /// HLC timestamp used for delta sync.
    ///
    /// This is the preferred variant when the caller already holds an
    /// `HlcTimestamp` — it guarantees that the `timestamps` map is always
    /// up-to-date after the write, so `delta_sync` will immediately see the
    /// new entry without a separate `record_change` call.
    pub fn put_with_timestamp(&mut self, key: String, value: CrdtValue, hlc: HlcTimestamp) {
        self.data.insert(key.clone(), value);
        self.timestamps.insert(key, hlc);
    }

    /// Remove and return the value for the given key.
    ///
    /// Also removes the corresponding change-tracking timestamp so that
    /// orphaned entries never accumulate in `self.timestamps`.
    ///
    /// # MAINTAINER WARNING
    /// Physical deletion is NOT recorded in the write-ahead log. Wiring
    /// this into an eventual write path would make deletes resurrect on
    /// WAL over-replay (an earlier `UpsertApplied` record re-inserts the
    /// key). Logical deletion must go through the CRDT tombstone
    /// operations (`OrSet::remove` / `OrMap::delete`) instead.
    pub fn delete(&mut self, key: &str) -> Option<CrdtValue> {
        self.timestamps.remove(key);
        self.data.remove(key)
    }

    /// Return all keys in the store.
    pub fn keys(&self) -> Vec<&String> {
        self.data.keys().collect()
    }

    /// Return keys that start with the given prefix (FR-001 key space partitioning).
    ///
    /// Uses O(log n + m) BTreeMap range scan where m is the number of
    /// matching keys, instead of O(n) full iteration.
    pub fn keys_with_prefix(&self, prefix: &str) -> Vec<&String> {
        if prefix.is_empty() {
            return self.data.keys().collect();
        }
        if let Some(end) = prefix_upper_bound(prefix) {
            self.data
                .range::<String, _>(prefix.to_string()..end)
                .map(|(k, _)| k)
                .collect()
        } else {
            // Fallback: prefix is all 0xFF bytes -- scan from prefix to end.
            self.data
                .range::<String, _>(prefix.to_string()..)
                .take_while(|(k, _)| k.starts_with(prefix))
                .map(|(k, _)| k)
                .collect()
        }
    }

    /// Check whether the store contains a value for `key`.
    pub fn contains_key(&self, key: &str) -> bool {
        self.data.contains_key(key)
    }

    /// Return the number of entries in the store.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check whether the store is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Return all key-value pairs as an iterator.
    pub fn all_entries(&self) -> impl Iterator<Item = (&String, &CrdtValue)> {
        self.data.iter()
    }

    /// Return all key-value pairs with their last-modification HLC timestamp.
    pub fn all_entries_with_hlc(
        &self,
    ) -> impl Iterator<Item = (&String, &CrdtValue, &HlcTimestamp)> {
        self.data
            .iter()
            .filter_map(|(k, v)| self.timestamps.get(k).map(|ts| (k, v, ts)))
    }

    /// Save the store as a versioned JSON snapshot to the given path.
    ///
    /// Uses a [`FileBackend`] internally for atomic write (write to `.tmp`
    /// then rename) to prevent corruption on crash. The snapshot includes
    /// a `format_version` field for forward compatibility.
    ///
    /// Not available on `wasm32-unknown-unknown` (no filesystem access).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn save_snapshot(&self, path: &std::path::Path) -> io::Result<()> {
        let backend = FileBackend::new(path);
        self.save_to_backend(&backend)
    }

    /// Save the store to an arbitrary [`StorageBackend`].
    pub fn save_to_backend(&self, backend: &dyn StorageBackend) -> io::Result<()> {
        let store_value = serde_json::to_value(self)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let envelope = PersistedStore {
            format_version: CURRENT_FORMAT_VERSION,
            store: store_value,
        };
        let json = serde_json::to_string(&envelope)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        backend.save(json.as_bytes())
    }

    /// Save the store as a bincode-encoded snapshot to the given path.
    ///
    /// Uses bincode for faster serialization compared to JSON (~2-4x speedup).
    /// The snapshot includes a 4-byte format version prefix for forward
    /// compatibility detection.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn save_snapshot_bincode(&self, path: &Path) -> io::Result<()> {
        let backend = FileBackend::new(path);
        self.save_to_backend_bincode(&backend)
    }

    /// Save the store to an arbitrary [`StorageBackend`] using bincode.
    pub fn save_to_backend_bincode(&self, backend: &dyn StorageBackend) -> io::Result<()> {
        let mut buf = Vec::new();
        // Write format version as a 4-byte LE prefix.
        buf.extend_from_slice(&CURRENT_FORMAT_VERSION.to_le_bytes());
        let encoded = bincode::serde::encode_to_vec(self, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        buf.extend_from_slice(&encoded);
        backend.save(&buf)
    }

    /// Load a store from a bincode-encoded snapshot at the given path.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn load_snapshot_bincode(path: &Path) -> io::Result<Self> {
        let backend = FileBackend::new(path);
        Self::load_from_backend_bincode(&backend)
    }

    /// Load a store from a bincode-encoded snapshot, falling back to an
    /// empty store only when the file is missing.
    ///
    /// Any other error (corruption, incompatible version, I/O failure) is
    /// propagated so a damaged snapshot can never be silently replaced by
    /// an empty store (data-loss direction).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn load_snapshot_bincode_or_default(path: &Path) -> io::Result<Self> {
        match Self::load_snapshot_bincode(path) {
            Ok(store) => Ok(store),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e),
        }
    }

    /// Load a store from an arbitrary [`StorageBackend`] using bincode.
    pub fn load_from_backend_bincode(backend: &dyn StorageBackend) -> io::Result<Self> {
        let bytes = backend.load()?;
        if bytes.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bincode snapshot too short",
            ));
        }
        let version = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        if version > CURRENT_FORMAT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                crate::error::CrdtError::IncompatibleVersion {
                    data_version: version,
                    code_version: CURRENT_FORMAT_VERSION,
                },
            ));
        }
        // MAINTAINER WARNING — bincode is positional and non-self-describing:
        // `#[serde(default)]` cannot rescue fields missing from an old
        // snapshot. Every persisted format version whose STRUCT LAYOUT
        // differs from the current one needs its own versioned decode type
        // below (see `StoreV2Layout` for v1/v2). When you add a field to
        // `Store`, bump `CURRENT_FORMAT_VERSION` and add a decode arm here.
        let mut store: Self = match version {
            // v1 and v2 share the {data, timestamps} layout; the session
            // fields introduced in v3 are filled with defaults.
            1 | 2 => {
                let (old, _len): (StoreV2Layout, _) =
                    bincode::serde::decode_from_slice(&bytes[4..], bincode::config::standard())
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                old.into()
            }
            _ => {
                let (store, _len): (Self, _) =
                    bincode::serde::decode_from_slice(&bytes[4..], bincode::config::standard())
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                store
            }
        };
        store.rebuild_visible_origins();
        Ok(store)
    }

    /// Load a store from a versioned JSON snapshot at the given path.
    ///
    /// Uses a [`FileBackend`] internally. Detects the format version and
    /// applies migrations if the data was written by an older version.
    /// Returns an error if the data version is newer than what this code
    /// supports (forward incompatibility).
    ///
    /// Not available on `wasm32-unknown-unknown` (no filesystem access).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn load_snapshot(path: &std::path::Path) -> io::Result<Self> {
        let backend = FileBackend::new(path);
        Self::load_from_backend(&backend)
    }

    /// Load a store from an arbitrary [`StorageBackend`].
    pub fn load_from_backend(backend: &dyn StorageBackend) -> io::Result<Self> {
        let bytes = backend.load()?;
        let raw =
            String::from_utf8(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Self::deserialize_snapshot(&raw)
    }

    /// Deserialize a snapshot from a JSON string, applying migrations as needed.
    fn deserialize_snapshot(raw: &str) -> io::Result<Self> {
        let parsed: serde_json::Value =
            serde_json::from_str(raw).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // Detect version: if format_version is missing, treat as v1 (legacy).
        let data_version = parsed
            .get("format_version")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32)
            .unwrap_or(1);

        if data_version > CURRENT_FORMAT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                CrdtError::IncompatibleVersion {
                    data_version,
                    code_version: CURRENT_FORMAT_VERSION,
                },
            ));
        }

        // Extract the store data from the envelope.
        let store_data = if parsed.get("format_version").is_some() {
            if let Some(store_field) = parsed.get("store") {
                // New versioned format: store data is in the "store" field.
                store_field.clone()
            } else {
                // Legacy versioned format (flatten): strip format_version to get raw store data.
                let mut obj = parsed;
                if let Some(map) = obj.as_object_mut() {
                    map.remove("format_version");
                }
                obj
            }
        } else {
            // Legacy format (v1): the entire JSON is the store.
            parsed
        };

        // Apply migrations if needed.
        let registry = migration::default_registry();
        let migrated = registry
            .apply_migrations(store_data, data_version, CURRENT_FORMAT_VERSION)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let mut store: Self = serde_json::from_value(migrated)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        // Pre-v3 snapshots did not persist visible_origins; restore the
        // `visible ⊇ applied` invariant conservatively.
        store.rebuild_visible_origins();
        Ok(store)
    }

    /// Load a store from a snapshot, falling back to an empty store only when
    /// the file is missing.
    ///
    /// Returns an error for incompatible versions or other I/O failures to
    /// prevent silent data loss.
    ///
    /// Not available on `wasm32-unknown-unknown` (no filesystem access).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn load_snapshot_or_default(path: &std::path::Path) -> io::Result<Self> {
        match Self::load_snapshot(path) {
            Ok(store) => Ok(store),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e),
        }
    }

    /// Load a store from a backend, falling back to an empty store when
    /// no data has been saved yet.
    pub fn load_from_backend_or_default(backend: &dyn StorageBackend) -> io::Result<Self> {
        match Self::load_from_backend(backend) {
            Ok(store) => Ok(store),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e),
        }
    }

    /// Merge a CRDT value into an existing entry.
    ///
    /// If the key does not exist, the value is inserted directly.
    /// If the key exists but the CRDT types differ, returns `CrdtError::TypeMismatch`.
    pub fn merge_value(&mut self, key: String, value: &CrdtValue) -> Result<(), CrdtError> {
        if let Some(existing) = self.data.get_mut(&key) {
            match (existing, value) {
                (CrdtValue::Counter(a), CrdtValue::Counter(b)) => {
                    a.merge(b);
                }
                (CrdtValue::Set(a), CrdtValue::Set(b)) => {
                    a.merge(b);
                }
                (CrdtValue::Map(a), CrdtValue::Map(b)) => {
                    a.merge(b);
                }
                (CrdtValue::Register(a), CrdtValue::Register(b)) => {
                    a.merge(b);
                }
                (existing, incoming) => {
                    return Err(CrdtError::TypeMismatch {
                        expected: existing.type_name().to_string(),
                        actual: incoming.type_name().to_string(),
                    });
                }
            }
        } else {
            self.data.insert(key, value.clone());
        }
        Ok(())
    }

    /// Merge a delta CRDT value into an existing entry.
    ///
    /// If the key does not exist, the delta is inserted directly (it becomes
    /// the full state). If the key exists, delegates to `CrdtValue::merge_delta`.
    pub fn merge_delta_value(&mut self, key: String, delta: &CrdtValue) -> Result<(), CrdtError> {
        if let Some(existing) = self.data.get_mut(&key) {
            existing.merge_delta(delta)
        } else {
            self.data.insert(key, delta.clone());
            Ok(())
        }
    }

    // ---------------------------------------------------------------
    // HLC-tracked operations for delta sync
    // ---------------------------------------------------------------

    /// Record a change timestamp for the given key.
    ///
    /// Called after any mutation to enable delta sync tracking.
    pub fn record_change(&mut self, key: &str, hlc: HlcTimestamp) {
        self.timestamps.insert(key.to_string(), hlc);
    }

    /// Record a change timestamp for the given key, keeping the maximum of
    /// the incoming HLC and any existing per-key timestamp.
    ///
    /// Unlike [`record_change`](Self::record_change) (overwrite semantics)
    /// this never moves a per-key timestamp backwards, so merges replayed
    /// out of order (remote deltas, WAL recovery) cannot silently drop an
    /// entry from the delta-sync change log.
    pub fn record_change_max(&mut self, key: &str, hlc: HlcTimestamp) {
        match self.timestamps.get_mut(key) {
            Some(existing) => {
                if hlc > *existing {
                    *existing = hlc;
                }
            }
            None => {
                self.timestamps.insert(key.to_string(), hlc);
            }
        }
    }

    /// Return the highest HLC timestamp known to this store across the
    /// per-key change log, the applied frontier, and the visible frontier.
    ///
    /// Used after crash recovery to re-seed the node's HLC clock: new
    /// timestamps issued after a restart must be strictly greater than
    /// anything already persisted, or LWW resolution and delta sync break.
    pub fn max_known_hlc(&self) -> Option<HlcTimestamp> {
        self.timestamps
            .values()
            .chain(self.applied_origins.values())
            .chain(self.visible_origins.values())
            .max()
            .cloned()
    }

    /// Return entries modified strictly after the given frontier timestamp.
    ///
    /// Returns `(key, value, last_modified)` triples sorted by HLC timestamp.
    pub fn entries_since(&self, frontier: &HlcTimestamp) -> Vec<(String, CrdtValue, HlcTimestamp)> {
        let mut result: Vec<(String, CrdtValue, HlcTimestamp)> = self
            .timestamps
            .iter()
            .filter(|(_, ts)| *ts > frontier)
            .filter_map(|(key, ts)| {
                self.data
                    .get(key)
                    .map(|v| (key.clone(), v.clone(), ts.clone()))
            })
            .collect();
        result.sort_unstable_by(|a, b| a.2.cmp(&b.2));
        result
    }

    /// Return the highest HLC timestamp across all tracked entries.
    ///
    /// Returns `None` if no entries have been tracked yet.
    pub fn current_frontier(&self) -> Option<HlcTimestamp> {
        self.timestamps.values().max().cloned()
    }

    /// Return the HLC timestamp for a specific key, if tracked.
    pub fn timestamp_for(&self, key: &str) -> Option<&HlcTimestamp> {
        self.timestamps.get(key)
    }

    /// Remove change-tracking timestamps that are at or before the given
    /// frontier for keys matching the given prefix.
    ///
    /// This is the "log deletion" step of compaction: once a checkpoint has
    /// been created and confirmed by a majority of authorities, the
    /// per-key timestamps used for delta sync are no longer needed for
    /// entries older than the checkpoint. Removing them bounds the memory
    /// used by the change-tracking metadata.
    ///
    /// Returns the number of timestamp entries pruned.
    pub fn prune_timestamps_before(&mut self, prefix: &str, frontier: &HlcTimestamp) -> usize {
        // Use the BTreeMap's efficient prefix range scan to find candidate
        // keys instead of scanning the entire timestamps HashMap.
        let candidate_keys: Vec<String> = if prefix.is_empty() {
            self.data.keys().cloned().collect()
        } else if let Some(end) = prefix_upper_bound(prefix) {
            self.data
                .range::<String, _>(prefix.to_string()..end)
                .map(|(k, _)| k.clone())
                .collect()
        } else {
            self.data
                .range::<String, _>(prefix.to_string()..)
                .take_while(|(k, _)| k.starts_with(prefix))
                .map(|(k, _)| k.clone())
                .collect()
        };

        let mut count = 0;
        for key in candidate_keys {
            if let Some(ts) = self.timestamps.get(&key)
                && ts <= frontier
            {
                self.timestamps.remove(&key);
                count += 1;
            }
        }

        // Record the pruned floor for delta-sync frontier adoption. The
        // session-guarantee metadata (applied_origins / merge_failed_keys)
        // is deliberately left untouched: it is max-monotone, so old
        // session tokens stay satisfiable after compaction.
        if count > 0
            && self
                .pruned_floor
                .as_ref()
                .is_none_or(|floor| frontier > floor)
        {
            self.pruned_floor = Some(frontier.clone());
        }

        count
    }

    // ---------------------------------------------------------------
    // Session-guarantee metadata (read-your-writes / monotonic reads)
    // ---------------------------------------------------------------

    /// Advance the applied frontier for the origin of `hlc` (max-monotone).
    ///
    /// Callers must only invoke this when the store now contains the full
    /// write prefix of `hlc.node_id` up to `hlc` (see the invariant on
    /// [`Store::applied_origins`]): a local mutation, a complete delta
    /// pull entry, or a full-state merge stamped with the local node id.
    pub fn note_applied(&mut self, hlc: &HlcTimestamp) {
        max_merge_origin(&mut self.applied_origins, hlc);
        // Invariant: visible_origins is a per-origin superset of
        // applied_origins (anything claimed is also visible).
        max_merge_origin(&mut self.visible_origins, hlc);
    }

    /// Record that a contribution stamped `hlc` is now part of the visible
    /// state WITHOUT claiming the origin's write prefix (max-monotone).
    ///
    /// Called for every successful remote merge that carries an origin
    /// HLC, including possibly-incomplete (unclaimed) deltas. Response
    /// session tokens are built from this map so they always cover what a
    /// reader may have observed (see [`Store::visible_origins`]).
    pub fn note_visible(&mut self, hlc: &HlcTimestamp) {
        max_merge_origin(&mut self.visible_origins, hlc);
    }

    /// Return the per-origin visible frontier map (superset of
    /// [`applied_origins`](Self::applied_origins); response-token input).
    pub fn visible_origins(&self) -> &HashMap<String, HlcTimestamp> {
        &self.visible_origins
    }

    /// Max-merge a remote visible-origins snapshot into the local map.
    ///
    /// Unlike [`merge_applied_origins`](Self::merge_applied_origins) this
    /// needs NO completeness precondition: a delta entry's CRDT value can
    /// embed contributions from origins the entry HLC does not name, so
    /// after merging ANY entries from a sender the local visible state
    /// may reflect anything the sender could see. Over-covering the
    /// visible frontier is always safe — it only widens response tokens
    /// (false-negative direction); it never fabricates an applied claim.
    pub fn merge_visible_origins(&mut self, remote: &HashMap<String, HlcTimestamp>) {
        for hlc in remote.values() {
            max_merge_origin(&mut self.visible_origins, hlc);
        }
    }

    /// Rebuild `visible_origins` conservatively from persisted state.
    ///
    /// Called after loading a snapshot: pre-v3 snapshots did not persist
    /// `visible_origins`, so it is reconstructed as the per-origin max of
    /// `applied_origins` and the per-key change timestamps. This may still
    /// under-cover contributions whose per-key timestamp was later
    /// superseded before the upgrade, but it restores the
    /// `visible ⊇ applied` invariant and covers everything the change log
    /// still knows about.
    fn rebuild_visible_origins(&mut self) {
        let applied: Vec<HlcTimestamp> = self.applied_origins.values().cloned().collect();
        for hlc in &applied {
            max_merge_origin(&mut self.visible_origins, hlc);
        }
        let tracked: Vec<HlcTimestamp> = self.timestamps.values().cloned().collect();
        for hlc in &tracked {
            max_merge_origin(&mut self.visible_origins, hlc);
        }
    }

    /// Return the per-origin applied frontier map.
    pub fn applied_origins(&self) -> &HashMap<String, HlcTimestamp> {
        &self.applied_origins
    }

    /// Return the applied frontier for a single origin node, if tracked.
    pub fn applied_origin(&self, node_id: &str) -> Option<&HlcTimestamp> {
        self.applied_origins.get(node_id)
    }

    /// Max-merge a remote applied-origins snapshot into the local map
    /// (frontier adoption).
    ///
    /// Sound only when the caller has verified the completeness condition:
    /// a full dump was applied (unconditionally complete), or a delta was
    /// requested from a frontier at or above the sender's
    /// [`pruned_floor`](Self::pruned_floor).
    pub fn merge_applied_origins(&mut self, remote: &HashMap<String, HlcTimestamp>) {
        for hlc in remote.values() {
            self.note_applied(hlc);
        }
    }

    /// Mark a key as poisoned by a failed remote merge (type mismatch).
    ///
    /// Poisoning is permanent: a later successful merge is not guaranteed
    /// to carry the contribution that was dropped, so clearing the mark
    /// would be unsound for session checks.
    pub fn note_merge_failed(&mut self, key: &str) {
        self.merge_failed_keys.insert(key.to_string());
        if self.merge_failed_keys.len() > 10_000 {
            tracing::warn!(
                poisoned_keys = self.merge_failed_keys.len(),
                "merge_failed_keys is growing large; check for systematic CRDT type conflicts"
            );
        }
    }

    /// Check whether a key is poisoned by a failed remote merge.
    pub fn merge_failed_contains(&self, key: &str) -> bool {
        self.merge_failed_keys.contains(key)
    }

    /// Return the set of keys poisoned by failed remote merges.
    pub fn merge_failed_keys(&self) -> &HashSet<String> {
        &self.merge_failed_keys
    }

    /// Union a set of poisoned keys into the local set (frontier adoption:
    /// the sender's dropped contributions must not be claimed as present here).
    pub fn merge_failed_extend<I: IntoIterator<Item = String>>(&mut self, keys: I) {
        for key in keys {
            self.note_merge_failed(&key);
        }
    }

    /// Return the highest frontier that pruning has been performed at.
    pub fn pruned_floor(&self) -> Option<&HlcTimestamp> {
        self.pruned_floor.as_ref()
    }

    /// Return the number of change-tracking timestamps currently stored.
    pub fn timestamp_count(&self) -> usize {
        self.timestamps.len()
    }

    /// Return delta entries modified strictly after the given frontier.
    ///
    /// Unlike `entries_since` which returns the full CRDT state for each
    /// changed key, this method calls `delta_since` on each value to
    /// extract only the changed portion. Falls back to the full state
    /// when the per-CRDT delta extraction returns `None`.
    ///
    /// Returns `(key, delta_value, last_modified)` triples sorted by HLC.
    pub fn delta_entries_since(
        &self,
        frontier: &HlcTimestamp,
    ) -> Vec<(String, CrdtValue, HlcTimestamp)> {
        let mut result: Vec<(String, CrdtValue, HlcTimestamp)> = self
            .timestamps
            .iter()
            .filter(|(_, ts)| *ts > frontier)
            .filter_map(|(key, ts)| {
                self.data.get(key).map(|v| {
                    // Try per-CRDT delta; fall back to full state.
                    let delta = v.delta_since(frontier).unwrap_or_else(|| v.clone());
                    (key.clone(), delta, ts.clone())
                })
            })
            .collect();
        result.sort_unstable_by(|a, b| a.2.cmp(&b.2));
        result
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::HlcTimestamp;
    use crate::types::NodeId;

    fn node(name: &str) -> NodeId {
        NodeId(name.into())
    }

    fn ts(physical: u64, logical: u32, node: &str) -> HlcTimestamp {
        HlcTimestamp {
            physical,
            logical,
            node_id: node.into(),
        }
    }

    // ---------------------------------------------------------------
    // Empty store
    // ---------------------------------------------------------------

    #[test]
    fn new_store_is_empty() {
        let store = Store::new();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
        assert!(store.keys().is_empty());
    }

    #[test]
    fn default_store_is_empty() {
        let store = Store::default();
        assert!(store.is_empty());
    }

    // ---------------------------------------------------------------
    // Basic CRUD
    // ---------------------------------------------------------------

    #[test]
    fn put_and_get_counter() {
        let mut store = Store::new();
        let mut counter = PnCounter::new();
        counter.increment(&node("A"));

        store.put("hits".into(), CrdtValue::Counter(counter));
        assert!(store.contains_key("hits"));
        assert_eq!(store.len(), 1);

        match store.get("hits") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn put_and_get_set() {
        let mut store = Store::new();
        let mut set = OrSet::new();
        set.add("alice".to_string(), &node("A"));

        store.put("users".into(), CrdtValue::Set(set));

        match store.get("users") {
            Some(CrdtValue::Set(s)) => assert!(s.contains(&"alice".to_string())),
            other => panic!("expected Set, got {:?}", other),
        }
    }

    #[test]
    fn put_and_get_map() {
        let mut store = Store::new();
        let mut map = OrMap::new();
        map.set(
            "name".to_string(),
            "AsteroidDB".to_string(),
            ts(100, 0, "A"),
            &node("A"),
        );

        store.put("config".into(), CrdtValue::Map(map));

        match store.get("config") {
            Some(CrdtValue::Map(m)) => {
                assert_eq!(m.get(&"name".to_string()), Some(&"AsteroidDB".to_string()))
            }
            other => panic!("expected Map, got {:?}", other),
        }
    }

    #[test]
    fn put_and_get_register() {
        let mut store = Store::new();
        let mut reg = LwwRegister::new();
        reg.set("hello".to_string(), ts(100, 0, "A"));

        store.put("greeting".into(), CrdtValue::Register(reg));

        match store.get("greeting") {
            Some(CrdtValue::Register(r)) => {
                assert_eq!(r.get(), Some(&"hello".to_string()))
            }
            other => panic!("expected Register, got {:?}", other),
        }
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let store = Store::new();
        assert!(store.get("missing").is_none());
        assert!(!store.contains_key("missing"));
    }

    #[test]
    fn put_overwrites_existing() {
        let mut store = Store::new();

        let mut c1 = PnCounter::new();
        c1.increment(&node("A"));
        store.put("x".into(), CrdtValue::Counter(c1));

        let mut c2 = PnCounter::new();
        c2.increment(&node("A"));
        c2.increment(&node("A"));
        store.put("x".into(), CrdtValue::Counter(c2));

        assert_eq!(store.len(), 1);
        match store.get("x") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 2),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn delete_existing_key() {
        let mut store = Store::new();
        store.put("k".into(), CrdtValue::Counter(PnCounter::new()));
        assert!(store.contains_key("k"));

        let removed = store.delete("k");
        assert!(removed.is_some());
        assert!(!store.contains_key("k"));
        assert!(store.is_empty());
    }

    #[test]
    fn delete_nonexistent_returns_none() {
        let mut store = Store::new();
        assert!(store.delete("ghost").is_none());
    }

    #[test]
    fn delete_also_removes_timestamp() {
        let mut store = Store::new();
        store.put("k".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("k", ts(1, 0, "n1"));
        assert!(store.timestamp_for("k").is_some());

        store.delete("k");
        assert!(
            store.timestamp_for("k").is_none(),
            "timestamp should be removed when key is deleted"
        );
    }

    // ---------------------------------------------------------------
    // Keys and prefix filtering
    // ---------------------------------------------------------------

    #[test]
    fn keys_returns_all_keys() {
        let mut store = Store::new();
        store.put("a".into(), CrdtValue::Counter(PnCounter::new()));
        store.put("b".into(), CrdtValue::Counter(PnCounter::new()));
        store.put("c".into(), CrdtValue::Counter(PnCounter::new()));

        let mut keys: Vec<&String> = store.keys();
        keys.sort();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn keys_with_prefix_filters_correctly() {
        let mut store = Store::new();
        store.put("user/alice".into(), CrdtValue::Counter(PnCounter::new()));
        store.put("user/bob".into(), CrdtValue::Counter(PnCounter::new()));
        store.put("config/db".into(), CrdtValue::Counter(PnCounter::new()));
        store.put("config/net".into(), CrdtValue::Counter(PnCounter::new()));
        store.put("log/2024".into(), CrdtValue::Counter(PnCounter::new()));

        let mut user_keys: Vec<&String> = store.keys_with_prefix("user/");
        user_keys.sort();
        assert_eq!(user_keys, vec!["user/alice", "user/bob"]);

        let mut config_keys: Vec<&String> = store.keys_with_prefix("config/");
        config_keys.sort();
        assert_eq!(config_keys, vec!["config/db", "config/net"]);

        let log_keys = store.keys_with_prefix("log/");
        assert_eq!(log_keys.len(), 1);
    }

    #[test]
    fn keys_with_prefix_no_match() {
        let mut store = Store::new();
        store.put("user/alice".into(), CrdtValue::Counter(PnCounter::new()));

        let keys = store.keys_with_prefix("config/");
        assert!(keys.is_empty());
    }

    #[test]
    fn keys_with_prefix_empty_prefix_returns_all() {
        let mut store = Store::new();
        store.put("a".into(), CrdtValue::Counter(PnCounter::new()));
        store.put("b".into(), CrdtValue::Counter(PnCounter::new()));

        let keys = store.keys_with_prefix("");
        assert_eq!(keys.len(), 2);
    }

    // ---------------------------------------------------------------
    // merge_value — matching types
    // ---------------------------------------------------------------

    #[test]
    fn merge_counter_into_existing() {
        let mut store = Store::new();
        let mut c1 = PnCounter::new();
        c1.increment(&node("A"));
        c1.increment(&node("A"));
        store.put("hits".into(), CrdtValue::Counter(c1));

        let mut c2 = PnCounter::new();
        c2.increment(&node("B"));
        store
            .merge_value("hits".into(), &CrdtValue::Counter(c2))
            .unwrap();

        match store.get("hits") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 3),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn merge_set_into_existing() {
        let mut store = Store::new();
        let mut s1 = OrSet::new();
        s1.add("alice".to_string(), &node("A"));
        store.put("users".into(), CrdtValue::Set(s1));

        let mut s2 = OrSet::new();
        s2.add("bob".to_string(), &node("B"));
        store
            .merge_value("users".into(), &CrdtValue::Set(s2))
            .unwrap();

        match store.get("users") {
            Some(CrdtValue::Set(s)) => {
                assert!(s.contains(&"alice".to_string()));
                assert!(s.contains(&"bob".to_string()));
                assert_eq!(s.len(), 2);
            }
            other => panic!("expected Set, got {:?}", other),
        }
    }

    #[test]
    fn merge_map_into_existing() {
        let mut store = Store::new();
        let mut m1 = OrMap::new();
        m1.set(
            "k1".to_string(),
            "v1".to_string(),
            ts(100, 0, "A"),
            &node("A"),
        );
        store.put("data".into(), CrdtValue::Map(m1));

        let mut m2 = OrMap::new();
        m2.set(
            "k2".to_string(),
            "v2".to_string(),
            ts(200, 0, "B"),
            &node("B"),
        );
        store
            .merge_value("data".into(), &CrdtValue::Map(m2))
            .unwrap();

        match store.get("data") {
            Some(CrdtValue::Map(m)) => {
                assert_eq!(m.get(&"k1".to_string()), Some(&"v1".to_string()));
                assert_eq!(m.get(&"k2".to_string()), Some(&"v2".to_string()));
            }
            other => panic!("expected Map, got {:?}", other),
        }
    }

    #[test]
    fn merge_register_into_existing() {
        let mut store = Store::new();
        let mut r1 = LwwRegister::new();
        r1.set("old".to_string(), ts(100, 0, "A"));
        store.put("val".into(), CrdtValue::Register(r1));

        let mut r2 = LwwRegister::new();
        r2.set("new".to_string(), ts(200, 0, "B"));
        store
            .merge_value("val".into(), &CrdtValue::Register(r2))
            .unwrap();

        match store.get("val") {
            Some(CrdtValue::Register(r)) => {
                assert_eq!(r.get(), Some(&"new".to_string()));
            }
            other => panic!("expected Register, got {:?}", other),
        }
    }

    #[test]
    fn merge_into_nonexistent_key_inserts() {
        let mut store = Store::new();
        let mut counter = PnCounter::new();
        counter.increment(&node("A"));

        store
            .merge_value("new_key".into(), &CrdtValue::Counter(counter))
            .unwrap();

        assert!(store.contains_key("new_key"));
        match store.get("new_key") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    // ---------------------------------------------------------------
    // merge_value — type mismatch
    // ---------------------------------------------------------------

    #[test]
    fn merge_type_mismatch_counter_vs_set() {
        let mut store = Store::new();
        store.put("k".into(), CrdtValue::Counter(PnCounter::new()));

        let err = store
            .merge_value("k".into(), &CrdtValue::Set(OrSet::new()))
            .unwrap_err();

        assert_eq!(
            err,
            CrdtError::TypeMismatch {
                expected: "Counter".into(),
                actual: "Set".into(),
            }
        );
    }

    #[test]
    fn merge_type_mismatch_set_vs_register() {
        let mut store = Store::new();
        store.put("k".into(), CrdtValue::Set(OrSet::new()));

        let err = store
            .merge_value("k".into(), &CrdtValue::Register(LwwRegister::new()))
            .unwrap_err();

        assert_eq!(
            err,
            CrdtError::TypeMismatch {
                expected: "Set".into(),
                actual: "Register".into(),
            }
        );
    }

    #[test]
    fn merge_type_mismatch_map_vs_counter() {
        let mut store = Store::new();
        store.put("k".into(), CrdtValue::Map(OrMap::new()));

        let err = store
            .merge_value("k".into(), &CrdtValue::Counter(PnCounter::new()))
            .unwrap_err();

        assert_eq!(
            err,
            CrdtError::TypeMismatch {
                expected: "Map".into(),
                actual: "Counter".into(),
            }
        );
    }

    #[test]
    fn merge_type_mismatch_register_vs_map() {
        let mut store = Store::new();
        store.put("k".into(), CrdtValue::Register(LwwRegister::new()));

        let err = store
            .merge_value("k".into(), &CrdtValue::Map(OrMap::new()))
            .unwrap_err();

        assert_eq!(
            err,
            CrdtError::TypeMismatch {
                expected: "Register".into(),
                actual: "Map".into(),
            }
        );
    }

    // ---------------------------------------------------------------
    // Multiple entries
    // ---------------------------------------------------------------

    #[test]
    fn multiple_entries_different_types() {
        let mut store = Store::new();

        let mut counter = PnCounter::new();
        counter.increment(&node("A"));
        store.put("counter".into(), CrdtValue::Counter(counter));

        let mut set = OrSet::new();
        set.add("x".to_string(), &node("A"));
        store.put("set".into(), CrdtValue::Set(set));

        let mut reg = LwwRegister::new();
        reg.set("val".to_string(), ts(100, 0, "A"));
        store.put("register".into(), CrdtValue::Register(reg));

        assert_eq!(store.len(), 3);
        assert!(store.contains_key("counter"));
        assert!(store.contains_key("set"));
        assert!(store.contains_key("register"));
    }

    #[test]
    fn len_and_is_empty_consistency() {
        let mut store = Store::new();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);

        store.put("a".into(), CrdtValue::Counter(PnCounter::new()));
        assert!(!store.is_empty());
        assert_eq!(store.len(), 1);

        store.put("b".into(), CrdtValue::Counter(PnCounter::new()));
        assert_eq!(store.len(), 2);

        store.delete("a");
        assert_eq!(store.len(), 1);

        store.delete("b");
        assert!(store.is_empty());
    }

    // ---------------------------------------------------------------
    // Snapshot persistence
    // ---------------------------------------------------------------

    #[test]
    fn save_and_load_snapshot_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.json");

        let mut store = Store::new();

        // Counter
        let mut counter = PnCounter::new();
        counter.increment(&node("A"));
        counter.increment(&node("A"));
        counter.decrement(&node("B"));
        store.put("hits".into(), CrdtValue::Counter(counter));

        // Set
        let mut set = OrSet::new();
        set.add("alice".to_string(), &node("A"));
        set.add("bob".to_string(), &node("B"));
        store.put("users".into(), CrdtValue::Set(set));

        // Map
        let mut map = OrMap::new();
        map.set(
            "name".to_string(),
            "AsteroidDB".to_string(),
            ts(100, 0, "A"),
            &node("A"),
        );
        store.put("config".into(), CrdtValue::Map(map));

        // Register
        let mut reg = LwwRegister::new();
        reg.set("hello".to_string(), ts(200, 0, "A"));
        store.put("greeting".into(), CrdtValue::Register(reg));

        // Save
        store.save_snapshot(&path).unwrap();

        // Load
        let loaded = Store::load_snapshot(&path).unwrap();

        assert_eq!(loaded.len(), 4);
        assert!(loaded.contains_key("hits"));
        assert!(loaded.contains_key("users"));
        assert!(loaded.contains_key("config"));
        assert!(loaded.contains_key("greeting"));

        // Verify counter value
        match loaded.get("hits") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1), // 2 inc - 1 dec
            other => panic!("expected Counter, got {:?}", other),
        }

        // Verify set values
        match loaded.get("users") {
            Some(CrdtValue::Set(s)) => {
                assert!(s.contains(&"alice".to_string()));
                assert!(s.contains(&"bob".to_string()));
            }
            other => panic!("expected Set, got {:?}", other),
        }

        // Verify map values
        match loaded.get("config") {
            Some(CrdtValue::Map(m)) => {
                assert_eq!(m.get(&"name".to_string()), Some(&"AsteroidDB".to_string()));
            }
            other => panic!("expected Map, got {:?}", other),
        }

        // Verify register value
        match loaded.get("greeting") {
            Some(CrdtValue::Register(r)) => {
                assert_eq!(r.get(), Some(&"hello".to_string()));
            }
            other => panic!("expected Register, got {:?}", other),
        }
    }

    #[test]
    fn load_snapshot_missing_file_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");

        let result = Store::load_snapshot(&path);
        assert!(result.is_err());
    }

    #[test]
    fn load_snapshot_or_default_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");

        let store = Store::load_snapshot_or_default(&path).unwrap();
        assert!(store.is_empty());
    }

    #[test]
    fn load_snapshot_or_default_corrupt_file_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt.json");
        std::fs::write(&path, "not valid json {{{").unwrap();

        let result = Store::load_snapshot_or_default(&path);
        assert!(result.is_err());
    }

    #[test]
    fn load_snapshot_or_default_incompatible_version_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("future.json");

        let json = serde_json::json!({
            "format_version": 99,
            "store": { "data": {}, "timestamps": {} }
        });
        std::fs::write(&path, serde_json::to_string(&json).unwrap()).unwrap();

        let result = Store::load_snapshot_or_default(&path);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("incompatible"));
    }

    #[test]
    fn save_snapshot_creates_parent_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("dir").join("store.json");

        let store = Store::new();
        store.save_snapshot(&path).unwrap();

        assert!(path.exists());
    }

    #[test]
    fn empty_store_snapshot_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.json");

        let store = Store::new();
        store.save_snapshot(&path).unwrap();

        let loaded = Store::load_snapshot(&path).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn save_overwrite_and_reload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.json");

        // First save
        let mut store = Store::new();
        store.put("a".into(), CrdtValue::Counter(PnCounter::new()));
        store.save_snapshot(&path).unwrap();

        // Overwrite with different data
        let mut store2 = Store::new();
        let mut reg = LwwRegister::new();
        reg.set("val".to_string(), ts(100, 0, "A"));
        store2.put("b".into(), CrdtValue::Register(reg));
        store2.save_snapshot(&path).unwrap();

        // Should load the second version
        let loaded = Store::load_snapshot(&path).unwrap();
        assert_eq!(loaded.len(), 1);
        assert!(!loaded.contains_key("a"));
        assert!(loaded.contains_key("b"));
    }

    // ---------------------------------------------------------------
    // Delta sync: record_change, entries_since, current_frontier
    // ---------------------------------------------------------------

    #[test]
    fn record_change_and_current_frontier() {
        let mut store = Store::new();
        assert!(store.current_frontier().is_none());

        store.put("a".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("a", ts(100, 0, "N1"));

        assert_eq!(store.current_frontier(), Some(ts(100, 0, "N1")));

        store.put("b".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("b", ts(200, 0, "N1"));

        assert_eq!(store.current_frontier(), Some(ts(200, 0, "N1")));
    }

    #[test]
    fn entries_since_returns_only_newer() {
        let mut store = Store::new();

        store.put("old".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("old", ts(100, 0, "N1"));

        store.put("mid".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("mid", ts(200, 0, "N1"));

        store.put("new".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("new", ts(300, 0, "N1"));

        // Everything after ts(150, 0, "N1")
        let frontier = ts(150, 0, "N1");
        let entries = store.entries_since(&frontier);

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, "mid");
        assert_eq!(entries[1].0, "new");
    }

    #[test]
    fn entries_since_returns_empty_when_nothing_newer() {
        let mut store = Store::new();

        store.put("a".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("a", ts(100, 0, "N1"));

        let frontier = ts(200, 0, "N1");
        let entries = store.entries_since(&frontier);

        assert!(entries.is_empty());
    }

    #[test]
    fn entries_since_empty_store() {
        let store = Store::new();
        let frontier = ts(0, 0, "");
        let entries = store.entries_since(&frontier);

        assert!(entries.is_empty());
    }

    #[test]
    fn entries_since_sorted_by_hlc() {
        let mut store = Store::new();

        store.put("c".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("c", ts(300, 0, "N1"));

        store.put("a".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("a", ts(100, 0, "N1"));

        store.put("b".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("b", ts(200, 0, "N1"));

        let frontier = ts(0, 0, "");
        let entries = store.entries_since(&frontier);

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0, "a");
        assert_eq!(entries[1].0, "b");
        assert_eq!(entries[2].0, "c");
    }

    #[test]
    fn timestamp_for_returns_correct_value() {
        let mut store = Store::new();

        store.put("k".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("k", ts(100, 5, "N1"));

        assert_eq!(store.timestamp_for("k"), Some(&ts(100, 5, "N1")));
        assert_eq!(store.timestamp_for("missing"), None);
    }

    #[test]
    fn record_change_updates_timestamp() {
        let mut store = Store::new();

        store.put("k".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("k", ts(100, 0, "N1"));
        assert_eq!(store.timestamp_for("k"), Some(&ts(100, 0, "N1")));

        store.record_change("k", ts(200, 0, "N1"));
        assert_eq!(store.timestamp_for("k"), Some(&ts(200, 0, "N1")));
    }

    #[test]
    fn snapshot_preserves_timestamps() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.json");

        let mut store = Store::new();
        store.put("k".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("k", ts(100, 0, "N1"));

        store.save_snapshot(&path).unwrap();
        let loaded = Store::load_snapshot(&path).unwrap();

        assert_eq!(loaded.timestamp_for("k"), Some(&ts(100, 0, "N1")));
        assert_eq!(loaded.current_frontier(), Some(ts(100, 0, "N1")));
    }

    // ---------------------------------------------------------------
    // Versioned persistence format
    // ---------------------------------------------------------------

    #[test]
    fn save_writes_format_version_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.json");

        let store = Store::new();
        store.save_snapshot(&path).unwrap();

        let raw = std::fs::read_to_string(&path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(
            parsed.get("format_version").and_then(|v| v.as_u64()),
            Some(super::CURRENT_FORMAT_VERSION as u64)
        );
    }

    #[test]
    fn versioned_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.json");

        let mut store = Store::new();
        let mut counter = PnCounter::new();
        counter.increment(&node("A"));
        store.put("key".into(), CrdtValue::Counter(counter));

        store.save_snapshot(&path).unwrap();
        let loaded = Store::load_snapshot(&path).unwrap();

        assert_eq!(loaded.len(), 1);
        match loaded.get("key") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn load_legacy_v1_format_without_version_field() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.json");

        // Write a legacy format (no format_version) — just a raw Store.
        let mut store = Store::new();
        let mut counter = PnCounter::new();
        counter.increment(&node("A"));
        store.put("k".into(), CrdtValue::Counter(counter));

        let json = serde_json::to_string(&store).unwrap();
        std::fs::write(&path, json).unwrap();

        let loaded = Store::load_snapshot(&path).unwrap();
        assert_eq!(loaded.len(), 1);
        match loaded.get("k") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn load_future_version_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("future.json");

        let json = serde_json::json!({
            "format_version": 99,
            "data": {},
            "timestamps": {}
        });
        std::fs::write(&path, serde_json::to_string(&json).unwrap()).unwrap();

        let result = Store::load_snapshot(&path);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("incompatible"));
    }

    #[test]
    fn atomic_write_no_tmp_file_on_success() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.json");

        let store = Store::new();
        store.save_snapshot(&path).unwrap();

        assert!(path.exists(), "final file should exist");

        // The tmp file is named `<filename>.<pid>.tmp` (not simply `store.tmp`).
        // After a successful rename no `*.tmp` file should remain in the directory.
        let leftover_tmp = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| e.file_name().to_string_lossy().ends_with(".tmp"));
        assert!(!leftover_tmp, "temp file should not persist on success");
    }

    // ---------------------------------------------------------------
    // Batch frontier computation (#193)
    // ---------------------------------------------------------------

    /// Verify that the max HLC of a batch from entries_since corresponds to
    /// the last element (entries are sorted by HLC).  This is the property
    /// relied upon by the delta push fix: we advance the peer frontier to
    /// the batch max, not the store's current_frontier().
    #[test]
    fn entries_since_batch_max_hlc_equals_last_entry() {
        let mut store = Store::new();

        store.put("a".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("a", ts(100, 0, "N1"));

        store.put("b".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("b", ts(200, 0, "N1"));

        store.put("c".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("c", ts(300, 0, "N1"));

        let frontier = ts(0, 0, "");
        let entries = store.entries_since(&frontier);

        // The last entry should have the max HLC of the batch.
        let batch_max_hlc = entries.last().map(|(_, _, hlc)| hlc.clone());
        assert_eq!(batch_max_hlc, Some(ts(300, 0, "N1")));

        // This batch max is NOT necessarily equal to current_frontier()
        // if new writes occur concurrently. Simulate that:
        store.put("d".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("d", ts(400, 0, "N1"));

        // current_frontier now is ts(400), but our batch max was ts(300).
        assert_eq!(store.current_frontier(), Some(ts(400, 0, "N1")));
        // The batch we already captured still has max ts(300).
        assert_eq!(entries.last().unwrap().2, ts(300, 0, "N1"));
    }

    /// Verify that on a partial push (only first N entries succeed),
    /// the correct frontier is the HLC of entry at index N-1.
    #[test]
    fn entries_since_partial_batch_frontier() {
        let mut store = Store::new();

        store.put("a".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("a", ts(100, 0, "N1"));

        store.put("b".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("b", ts(200, 0, "N1"));

        store.put("c".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("c", ts(300, 0, "N1"));

        store.put("d".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("d", ts(400, 0, "N1"));

        let frontier = ts(0, 0, "");
        let entries = store.entries_since(&frontier);
        assert_eq!(entries.len(), 4);

        // If only 2 entries were pushed successfully, the frontier
        // should advance to the HLC of entry at index 1 (0-based).
        let pushed = 2;
        let partial_frontier = &entries[pushed - 1].2;
        assert_eq!(*partial_frontier, ts(200, 0, "N1"));

        // Entries after this frontier should include the unpushed ones.
        let remaining = store.entries_since(partial_frontier);
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].0, "c");
        assert_eq!(remaining[1].0, "d");
    }

    /// Concurrent writes during a push window must not be skipped.
    /// The batch captured before the push should have a max HLC that
    /// does NOT cover writes that occur during the push.
    #[test]
    fn concurrent_writes_during_push_not_skipped() {
        let mut store = Store::new();

        // Pre-existing entries (the "batch" to push).
        store.put("x".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("x", ts(100, 0, "N1"));

        store.put("y".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("y", ts(200, 0, "N1"));

        // Capture the batch.
        let frontier = ts(0, 0, "");
        let batch = store.entries_since(&frontier);
        let batch_max = batch.last().unwrap().2.clone();
        assert_eq!(batch_max, ts(200, 0, "N1"));

        // Simulate a concurrent write that occurs DURING the push.
        store.put("z".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("z", ts(250, 0, "N1"));

        // If we advance the frontier to batch_max (200), the concurrent
        // write (250) will be picked up in the next cycle.
        let next_batch = store.entries_since(&batch_max);
        assert_eq!(next_batch.len(), 1);
        assert_eq!(next_batch[0].0, "z");

        // But if we had used current_frontier (250), we'd skip "z" forever!
        let bad_frontier = store.current_frontier().unwrap();
        assert_eq!(bad_frontier, ts(250, 0, "N1"));
        let skipped = store.entries_since(&bad_frontier);
        assert!(
            skipped.is_empty(),
            "using current_frontier would skip the concurrent write"
        );
    }

    #[test]
    fn migration_chain_v1_to_current_applied_on_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v1.json");

        // Write a v1-format file (with explicit version 1).
        let mut store = Store::new();
        let mut counter = PnCounter::new();
        counter.increment(&node("X"));
        store.put("test".into(), CrdtValue::Counter(counter));

        let mut value = serde_json::to_value(&store).unwrap();
        if let Some(obj) = value.as_object_mut() {
            obj.insert(
                "format_version".to_string(),
                serde_json::Value::Number(1.into()),
            );
        }
        std::fs::write(&path, serde_json::to_string(&value).unwrap()).unwrap();

        // Loading should apply migration v1->v2 and succeed.
        let loaded = Store::load_snapshot(&path).unwrap();
        assert_eq!(loaded.len(), 1);
        match loaded.get("test") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    // ---------------------------------------------------------------
    // StorageBackend integration (#251)
    // ---------------------------------------------------------------

    #[test]
    fn save_and_load_via_memory_backend() {
        use crate::store::backend::MemoryBackend;

        let backend = MemoryBackend::new();

        let mut store = Store::new();
        let mut counter = PnCounter::new();
        counter.increment(&node("A"));
        store.put("hits".into(), CrdtValue::Counter(counter));
        store.record_change("hits", ts(100, 0, "N1"));

        store.save_to_backend(&backend).unwrap();
        assert!(backend.exists());

        let loaded = Store::load_from_backend(&backend).unwrap();
        assert_eq!(loaded.len(), 1);
        match loaded.get("hits") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1),
            other => panic!("expected Counter, got {:?}", other),
        }
        assert_eq!(loaded.timestamp_for("hits"), Some(&ts(100, 0, "N1")));
    }

    #[test]
    fn load_from_backend_or_default_empty() {
        use crate::store::backend::MemoryBackend;

        let backend = MemoryBackend::new();
        let store = Store::load_from_backend_or_default(&backend).unwrap();
        assert!(store.is_empty());
    }

    #[test]
    fn save_to_file_backend_matches_save_snapshot() {
        use crate::store::backend::FileBackend;

        let dir = tempfile::tempdir().unwrap();

        // Save via save_snapshot (legacy API).
        let path_a = dir.path().join("a.json");
        let mut store = Store::new();
        store.put("k".into(), CrdtValue::Counter(PnCounter::new()));
        store.save_snapshot(&path_a).unwrap();

        // Save via save_to_backend with FileBackend.
        let path_b = dir.path().join("b.json");
        let backend = FileBackend::new(&path_b);
        store.save_to_backend(&backend).unwrap();

        // Both should produce loadable stores.
        let loaded_a = Store::load_snapshot(&path_a).unwrap();
        let loaded_b = Store::load_from_backend(&backend).unwrap();
        assert_eq!(loaded_a.len(), loaded_b.len());
    }

    // ---------------------------------------------------------------
    // BTreeMap prefix scan optimization (#255)
    // ---------------------------------------------------------------

    #[test]
    fn btree_keys_with_prefix_returns_sorted() {
        let mut store = Store::new();
        store.put("user/zara".into(), CrdtValue::Counter(PnCounter::new()));
        store.put("user/alice".into(), CrdtValue::Counter(PnCounter::new()));
        store.put("user/bob".into(), CrdtValue::Counter(PnCounter::new()));
        store.put("config/db".into(), CrdtValue::Counter(PnCounter::new()));

        // BTreeMap range scan returns keys in sorted order.
        let user_keys: Vec<&String> = store.keys_with_prefix("user/");
        assert_eq!(user_keys, vec!["user/alice", "user/bob", "user/zara"]);
    }

    #[test]
    fn prefix_upper_bound_helper() {
        assert_eq!(super::prefix_upper_bound("abc"), Some("abd".to_string()));
        assert_eq!(super::prefix_upper_bound("a"), Some("b".to_string()));
        assert_eq!(
            super::prefix_upper_bound("user/"),
            Some("user0".to_string())
        );
        // Trailing 0x7E ('~') increments to 0x7F
        assert_eq!(super::prefix_upper_bound("~"), Some("\x7F".to_string()));
        // Empty prefix => None (no bytes to increment)
        assert_eq!(super::prefix_upper_bound(""), None);
    }

    #[test]
    fn keys_with_prefix_boundary_keys() {
        let mut store = Store::new();
        // Keys that are exactly at prefix boundaries.
        store.put("abc".into(), CrdtValue::Counter(PnCounter::new()));
        store.put("abd".into(), CrdtValue::Counter(PnCounter::new()));
        store.put("ab".into(), CrdtValue::Counter(PnCounter::new()));
        store.put("abcdef".into(), CrdtValue::Counter(PnCounter::new()));

        let keys = store.keys_with_prefix("abc");
        assert_eq!(keys, vec!["abc", "abcdef"]);
    }

    // ---------------------------------------------------------------
    // prune_timestamps_before (#253)
    // ---------------------------------------------------------------

    #[test]
    fn prune_timestamps_removes_old_entries() {
        let mut store = Store::new();
        store.put("user/a".into(), CrdtValue::Counter(PnCounter::new()));
        store.put("user/b".into(), CrdtValue::Counter(PnCounter::new()));
        store.put("user/c".into(), CrdtValue::Counter(PnCounter::new()));
        store.put("order/x".into(), CrdtValue::Counter(PnCounter::new()));

        store.record_change("user/a", ts(50, 0, "n"));
        store.record_change("user/b", ts(100, 0, "n"));
        store.record_change("user/c", ts(200, 0, "n"));
        store.record_change("order/x", ts(50, 0, "n"));

        assert_eq!(store.timestamp_count(), 4);

        // Prune user/ entries at or before ts=100.
        let pruned = store.prune_timestamps_before("user/", &ts(100, 0, "n"));
        assert_eq!(pruned, 2); // user/a (50) and user/b (100)
        assert_eq!(store.timestamp_count(), 2); // user/c and order/x remain
        assert!(store.timestamp_for("user/a").is_none());
        assert!(store.timestamp_for("user/b").is_none());
        assert!(store.timestamp_for("user/c").is_some());
        assert!(store.timestamp_for("order/x").is_some());
    }

    #[test]
    fn prune_timestamps_respects_prefix() {
        let mut store = Store::new();
        store.put("user/a".into(), CrdtValue::Counter(PnCounter::new()));
        store.put("order/a".into(), CrdtValue::Counter(PnCounter::new()));

        store.record_change("user/a", ts(50, 0, "n"));
        store.record_change("order/a", ts(50, 0, "n"));

        // Only prune "order/" prefix.
        let pruned = store.prune_timestamps_before("order/", &ts(100, 0, "n"));
        assert_eq!(pruned, 1);
        assert!(store.timestamp_for("user/a").is_some());
        assert!(store.timestamp_for("order/a").is_none());
    }

    #[test]
    fn prune_timestamps_empty_store() {
        let mut store = Store::new();
        let pruned = store.prune_timestamps_before("user/", &ts(100, 0, "n"));
        assert_eq!(pruned, 0);
    }

    #[test]
    fn prune_timestamps_nothing_to_prune() {
        let mut store = Store::new();
        store.put("user/a".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("user/a", ts(200, 0, "n"));

        // Frontier is before the only entry.
        let pruned = store.prune_timestamps_before("user/", &ts(100, 0, "n"));
        assert_eq!(pruned, 0);
        assert_eq!(store.timestamp_count(), 1);
    }

    #[test]
    fn timestamp_count_reflects_state() {
        let mut store = Store::new();
        assert_eq!(store.timestamp_count(), 0);

        store.put("k".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("k", ts(100, 0, "n"));
        assert_eq!(store.timestamp_count(), 1);
    }

    // ---------------------------------------------------------------
    // put_with_timestamp — atomic write + timestamp update
    // ---------------------------------------------------------------

    #[test]
    fn put_with_timestamp_stores_value_and_timestamp() {
        let mut store = Store::new();
        store.put_with_timestamp(
            "k".into(),
            CrdtValue::Counter(PnCounter::new()),
            ts(100, 0, "n1"),
        );

        assert!(store.contains_key("k"));
        assert_eq!(store.timestamp_for("k"), Some(&ts(100, 0, "n1")));
    }

    #[test]
    fn put_with_timestamp_visible_to_entries_since() {
        let mut store = Store::new();
        store.put_with_timestamp(
            "key".into(),
            CrdtValue::Counter(PnCounter::new()),
            ts(200, 0, "n1"),
        );

        // Should be visible to delta sync without calling record_change.
        let frontier = ts(0, 0, "");
        let entries = store.entries_since(&frontier);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "key");
        assert_eq!(entries[0].2, ts(200, 0, "n1"));
    }

    #[test]
    fn put_without_timestamp_not_visible_to_entries_since() {
        let mut store = Store::new();
        // Plain put() does NOT update timestamps; delta sync cannot see this entry.
        store.put("key".into(), CrdtValue::Counter(PnCounter::new()));

        let frontier = ts(0, 0, "");
        let entries = store.entries_since(&frontier);
        assert!(
            entries.is_empty(),
            "put() without a timestamp must not appear in delta sync"
        );
    }

    #[test]
    fn put_with_timestamp_overwrites_existing_timestamp() {
        let mut store = Store::new();
        store.put_with_timestamp(
            "k".into(),
            CrdtValue::Counter(PnCounter::new()),
            ts(100, 0, "n1"),
        );
        store.put_with_timestamp(
            "k".into(),
            CrdtValue::Counter(PnCounter::new()),
            ts(200, 0, "n1"),
        );

        assert_eq!(store.timestamp_for("k"), Some(&ts(200, 0, "n1")));
        assert_eq!(store.timestamp_count(), 1);
    }

    // ---------------------------------------------------------------
    // Bincode snapshot tests (#306)
    // ---------------------------------------------------------------

    #[test]
    fn bincode_snapshot_roundtrip() {
        let mut store = Store::new();
        let n = node("bench-node");

        let mut counter = PnCounter::new();
        counter.increment(&n);
        counter.increment(&n);
        store.put("key-a".into(), CrdtValue::Counter(counter));
        store.record_change("key-a", ts(100, 0, "bench-node"));

        let tmp_dir = tempfile::TempDir::new().unwrap();
        let path = tmp_dir.path().join("test-bincode.bin");

        store.save_snapshot_bincode(&path).unwrap();
        let loaded = Store::load_snapshot_bincode(&path).unwrap();

        assert_eq!(loaded.len(), 1);
        assert!(loaded.contains_key("key-a"));
    }

    #[test]
    fn bincode_snapshot_preserves_timestamps() {
        let mut store = Store::new();
        let n = node("bench-node");

        let mut counter = PnCounter::new();
        counter.increment(&n);
        store.put("key-a".into(), CrdtValue::Counter(counter));
        store.record_change("key-a", ts(200, 5, "bench-node"));

        let tmp_dir = tempfile::TempDir::new().unwrap();
        let path = tmp_dir.path().join("test-bincode.bin");

        store.save_snapshot_bincode(&path).unwrap();
        let loaded = Store::load_snapshot_bincode(&path).unwrap();

        let loaded_ts = loaded.timestamp_for("key-a").unwrap();
        assert_eq!(loaded_ts.physical, 200);
        assert_eq!(loaded_ts.logical, 5);
    }

    #[test]
    fn bincode_snapshot_multiple_crdt_types() {
        use crate::crdt::lww_register::LwwRegister;
        use crate::crdt::or_set::OrSet;

        let mut store = Store::new();
        let n = node("n1");

        let mut counter = PnCounter::new();
        counter.increment(&n);
        store.put("counter".into(), CrdtValue::Counter(counter));

        let mut set = OrSet::new();
        set.add("x".to_string(), &n);
        store.put("set".into(), CrdtValue::Set(set));

        let mut reg = LwwRegister::new();
        reg.set("hello".to_string(), ts(100, 0, "n1"));
        store.put("reg".into(), CrdtValue::Register(reg));

        let tmp_dir = tempfile::TempDir::new().unwrap();
        let path = tmp_dir.path().join("multi-type.bin");

        store.save_snapshot_bincode(&path).unwrap();
        let loaded = Store::load_snapshot_bincode(&path).unwrap();

        assert_eq!(loaded.len(), 3);
        assert!(loaded.contains_key("counter"));
        assert!(loaded.contains_key("set"));
        assert!(loaded.contains_key("reg"));
    }

    /// Serialisable mirror of the exact struct layout persisted by format
    /// versions 1 and 2 ({data, timestamps} only), used to craft genuine
    /// old-format snapshots in tests.
    #[derive(serde::Serialize)]
    struct OldStoreLayoutV2 {
        data: BTreeMap<String, CrdtValue>,
        timestamps: HashMap<String, HlcTimestamp>,
    }

    fn old_layout_bincode_snapshot(version: u32) -> Vec<u8> {
        let mut counter = PnCounter::new();
        counter.increment(&node("A"));
        counter.increment(&node("A"));
        let mut set = OrSet::new();
        set.add("alice".to_string(), &node("A"));

        let mut data = BTreeMap::new();
        data.insert("hits".to_string(), CrdtValue::Counter(counter));
        data.insert("users".to_string(), CrdtValue::Set(set));
        let mut timestamps = HashMap::new();
        timestamps.insert("hits".to_string(), ts(42, 0, "A"));

        let old = OldStoreLayoutV2 { data, timestamps };
        let payload = bincode::serde::encode_to_vec(&old, bincode::config::standard())
            .expect("bincode encode failed");

        let mut bytes = Vec::with_capacity(4 + payload.len());
        bytes.extend_from_slice(&version.to_le_bytes());
        bytes.extend_from_slice(&payload);
        bytes
    }

    /// v1/v2 bincode snapshots use the old {data, timestamps} layout;
    /// loading them must decode via the versioned layout (bincode is
    /// positional — serde defaults cannot rescue missing trailing fields)
    /// and fill the v3 session fields with defaults.
    #[test]
    fn bincode_v1_and_v2_snapshots_migrate_on_load() {
        use crate::store::backend::MemoryBackend;

        for version in [1u32, 2] {
            let backend = MemoryBackend::new();
            backend.save(&old_layout_bincode_snapshot(version)).unwrap();

            let loaded = Store::load_from_backend_bincode(&backend).unwrap_or_else(|e| {
                panic!("v{version} bincode snapshot should migrate and load: {e}")
            });

            assert_eq!(loaded.len(), 2, "all keys must survive migration");
            match loaded.get("hits") {
                Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 2),
                other => panic!("expected Counter, got {:?}", other),
            }
            match loaded.get("users") {
                Some(CrdtValue::Set(s)) => assert!(s.contains(&"alice".to_string())),
                other => panic!("expected Set, got {:?}", other),
            }
            assert_eq!(
                loaded.timestamp_for("hits"),
                Some(&ts(42, 0, "A")),
                "timestamp must survive migration"
            );

            // Session fields default; visible_origins is rebuilt from the
            // change timestamps so response tokens keep covering the data.
            assert!(loaded.applied_origins().is_empty());
            assert!(loaded.merge_failed_keys().is_empty());
            assert!(loaded.pruned_floor().is_none());
            assert_eq!(loaded.visible_origins().get("A"), Some(&ts(42, 0, "A")));
        }
    }

    /// Round-trip at the CURRENT version must preserve the session fields
    /// verbatim in bincode form.
    #[test]
    fn bincode_snapshot_preserves_session_metadata() {
        use crate::store::backend::MemoryBackend;

        let mut store = Store::new();
        store.put("k".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("k", ts(100, 0, "a"));
        store.note_applied(&ts(100, 0, "a"));
        store.note_visible(&ts(200, 0, "b"));
        store.note_merge_failed("bad");
        store.prune_timestamps_before("", &ts(150, 0, "a"));

        let backend = MemoryBackend::new();
        store.save_to_backend_bincode(&backend).unwrap();
        let loaded = Store::load_from_backend_bincode(&backend).unwrap();

        assert_eq!(loaded.applied_origin("a"), Some(&ts(100, 0, "a")));
        assert_eq!(loaded.visible_origins().get("b"), Some(&ts(200, 0, "b")));
        assert!(loaded.merge_failed_contains("bad"));
        assert_eq!(loaded.pruned_floor(), Some(&ts(150, 0, "a")));
    }

    #[test]
    fn bincode_load_future_version_returns_error() {
        use crate::store::backend::MemoryBackend;
        // Mirror of load_future_version_returns_error for the JSON path: a
        // bincode snapshot with version > CURRENT_FORMAT_VERSION must be
        // rejected with InvalidData so future schema incompatibilities are
        // caught rather than silently decoded as the current layout.
        let backend = MemoryBackend::new();
        let future_version: u32 = 99;
        let bytes = future_version.to_le_bytes().to_vec();

        backend.save(&bytes).unwrap();

        let result = Store::load_from_backend_bincode(&backend);
        assert!(result.is_err(), "future-version bincode must be rejected");
        let err = result.unwrap_err();
        assert_eq!(
            err.kind(),
            std::io::ErrorKind::InvalidData,
            "error kind must be InvalidData"
        );
        assert!(
            err.to_string().contains("incompatible"),
            "error message must mention 'incompatible'; got: {err}"
        );
    }

    // ---------------------------------------------------------------
    // Session-guarantee metadata (applied_origins / merge_failed_keys /
    // pruned_floor)
    // ---------------------------------------------------------------

    #[test]
    fn note_applied_is_max_monotone() {
        let mut store = Store::new();
        store.note_applied(&ts(100, 0, "a"));
        assert_eq!(store.applied_origin("a"), Some(&ts(100, 0, "a")));

        // Older timestamp must not regress the frontier.
        store.note_applied(&ts(50, 9, "a"));
        assert_eq!(store.applied_origin("a"), Some(&ts(100, 0, "a")));

        // Newer timestamp advances it; other origins are independent.
        store.note_applied(&ts(200, 0, "a"));
        store.note_applied(&ts(10, 0, "b"));
        assert_eq!(store.applied_origin("a"), Some(&ts(200, 0, "a")));
        assert_eq!(store.applied_origin("b"), Some(&ts(10, 0, "b")));
    }

    /// visible_origins is a per-origin superset of applied_origins:
    /// note_applied feeds it, and note_visible advances it WITHOUT
    /// advancing applied_origins (unclaimed merges must never claim).
    #[test]
    fn visible_origins_superset_of_applied() {
        let mut store = Store::new();
        store.note_applied(&ts(100, 0, "a"));
        assert_eq!(store.visible_origins().get("a"), Some(&ts(100, 0, "a")));

        store.note_visible(&ts(200, 0, "b"));
        assert_eq!(store.visible_origins().get("b"), Some(&ts(200, 0, "b")));
        assert!(
            store.applied_origin("b").is_none(),
            "note_visible must not claim the origin"
        );

        // Max-monotone; empty node ids are ignored (zero sentinels).
        store.note_visible(&ts(150, 0, "b"));
        assert_eq!(store.visible_origins().get("b"), Some(&ts(200, 0, "b")));
        store.note_visible(&ts(999, 0, ""));
        assert!(!store.visible_origins().contains_key(""));
    }

    #[test]
    fn merge_applied_origins_is_max_merge() {
        let mut store = Store::new();
        store.note_applied(&ts(100, 0, "a"));
        store.note_applied(&ts(300, 0, "b"));

        let mut remote = HashMap::new();
        remote.insert("a".to_string(), ts(200, 0, "a")); // newer — adopts
        remote.insert("b".to_string(), ts(100, 0, "b")); // older — keeps 300
        remote.insert("c".to_string(), ts(50, 0, "c")); // new origin
        store.merge_applied_origins(&remote);

        assert_eq!(store.applied_origin("a"), Some(&ts(200, 0, "a")));
        assert_eq!(store.applied_origin("b"), Some(&ts(300, 0, "b")));
        assert_eq!(store.applied_origin("c"), Some(&ts(50, 0, "c")));
    }

    #[test]
    fn merge_failed_keys_tracking() {
        let mut store = Store::new();
        assert!(!store.merge_failed_contains("k"));
        store.note_merge_failed("k");
        assert!(store.merge_failed_contains("k"));

        store.merge_failed_extend(vec!["x".to_string(), "y".to_string()]);
        assert!(store.merge_failed_contains("x"));
        assert!(store.merge_failed_contains("y"));
        assert_eq!(store.merge_failed_keys().len(), 3);
    }

    #[test]
    fn prune_preserves_session_metadata_and_updates_floor() {
        let mut store = Store::new();
        store.put("k1".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("k1", ts(100, 0, "a"));
        store.note_applied(&ts(100, 0, "a"));
        store.note_merge_failed("poisoned");

        let pruned = store.prune_timestamps_before("", &ts(150, 0, "a"));
        assert_eq!(pruned, 1);
        assert!(store.timestamp_for("k1").is_none(), "per-key ts pruned");

        // Session metadata must be untouched (old tokens stay satisfiable).
        assert_eq!(store.applied_origin("a"), Some(&ts(100, 0, "a")));
        assert!(store.merge_failed_contains("poisoned"));
        // The pruned floor advanced to the prune frontier.
        assert_eq!(store.pruned_floor(), Some(&ts(150, 0, "a")));

        // A lower later prune must not regress the floor; a no-op prune
        // (nothing removed) must not advance it.
        let pruned = store.prune_timestamps_before("", &ts(120, 0, "a"));
        assert_eq!(pruned, 0);
        assert_eq!(store.pruned_floor(), Some(&ts(150, 0, "a")));
    }

    #[test]
    fn delete_preserves_session_metadata() {
        let mut store = Store::new();
        store.put("k".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("k", ts(100, 0, "a"));
        store.note_applied(&ts(100, 0, "a"));

        store.delete("k");
        assert_eq!(
            store.applied_origin("a"),
            Some(&ts(100, 0, "a")),
            "applied_origins must survive delete"
        );
    }

    #[test]
    fn snapshot_preserves_session_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.json");

        let mut store = Store::new();
        store.put("k".into(), CrdtValue::Counter(PnCounter::new()));
        store.record_change("k", ts(100, 0, "a"));
        store.note_applied(&ts(100, 0, "a"));
        store.note_merge_failed("bad");
        store.prune_timestamps_before("", &ts(200, 0, "a"));

        store.save_snapshot(&path).unwrap();
        let loaded = Store::load_snapshot(&path).unwrap();

        assert_eq!(loaded.applied_origin("a"), Some(&ts(100, 0, "a")));
        assert!(loaded.merge_failed_contains("bad"));
        assert_eq!(loaded.pruned_floor(), Some(&ts(200, 0, "a")));
    }

    #[test]
    fn snapshot_without_session_fields_loads_with_defaults() {
        // Snapshots written before the session-guarantee fields existed
        // must load with empty defaults.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("old.json");

        let json = serde_json::json!({
            "format_version": 2,
            "store": { "data": {}, "timestamps": {} }
        });
        std::fs::write(&path, serde_json::to_string(&json).unwrap()).unwrap();

        let loaded = Store::load_snapshot(&path).unwrap();
        assert!(loaded.applied_origins().is_empty());
        assert!(loaded.merge_failed_keys().is_empty());
        assert!(loaded.pruned_floor().is_none());
        assert!(loaded.visible_origins().is_empty());
    }

    /// Pre-v3 snapshots have no visible_origins; loading must rebuild it
    /// conservatively from applied_origins and the change timestamps so
    /// response tokens keep covering persisted data after an upgrade.
    #[test]
    fn snapshot_load_rebuilds_visible_origins() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v2-with-data.json");

        let json = serde_json::json!({
            "format_version": 2,
            "store": {
                "data": {},
                "timestamps": { "k": { "physical": 70, "logical": 0, "node_id": "c" } },
                "applied_origins": { "a": { "physical": 50, "logical": 0, "node_id": "a" } }
            }
        });
        std::fs::write(&path, serde_json::to_string(&json).unwrap()).unwrap();

        let loaded = Store::load_snapshot(&path).unwrap();
        assert_eq!(loaded.visible_origins().get("a"), Some(&ts(50, 0, "a")));
        assert_eq!(loaded.visible_origins().get("c"), Some(&ts(70, 0, "c")));
    }
}
