use std::collections::HashMap;
use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::crdt::lww_register::LwwRegister;
use crate::crdt::or_map::OrMap;
use crate::crdt::or_set::OrSet;
use crate::crdt::pn_counter::PnCounter;
use crate::error::CrdtError;
use crate::hlc::HlcTimestamp;

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
}

/// Key-value store backed by CRDT values (FR-001).
///
/// Provides basic CRUD operations, prefix-based key space partitioning,
/// and CRDT-aware value merging with type checking. Supports HLC-based
/// change tracking for delta sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Store {
    data: HashMap<String, CrdtValue>,
    /// Per-key HLC timestamp of the last modification, used for delta sync.
    #[serde(default)]
    timestamps: HashMap<String, HlcTimestamp>,
}

impl Store {
    /// Create a new, empty store.
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            timestamps: HashMap::new(),
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
    pub fn put(&mut self, key: String, value: CrdtValue) {
        self.data.insert(key, value);
    }

    /// Remove and return the value for the given key.
    pub fn delete(&mut self, key: &str) -> Option<CrdtValue> {
        self.data.remove(key)
    }

    /// Return all keys in the store.
    pub fn keys(&self) -> Vec<&String> {
        self.data.keys().collect()
    }

    /// Return keys that start with the given prefix (FR-001 key space partitioning).
    pub fn keys_with_prefix(&self, prefix: &str) -> Vec<&String> {
        self.data.keys().filter(|k| k.starts_with(prefix)).collect()
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

    /// Save the store as a JSON snapshot to the given path.
    pub fn save_snapshot(&self, path: &Path) -> io::Result<()> {
        let json = serde_json::to_string(self)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, json)
    }

    /// Load a store from a JSON snapshot at the given path.
    ///
    /// Returns an `io::Error` if the file cannot be read or parsed.
    pub fn load_snapshot(path: &Path) -> io::Result<Self> {
        let data = std::fs::read_to_string(path)?;
        serde_json::from_str(&data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Load a store from a snapshot, falling back to an empty store on any error.
    ///
    /// This is the recommended way to load at startup: if the snapshot file
    /// is missing or corrupted, the store starts fresh.
    pub fn load_snapshot_or_default(path: &Path) -> Self {
        Self::load_snapshot(path).unwrap_or_default()
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

    // ---------------------------------------------------------------
    // HLC-tracked operations for delta sync
    // ---------------------------------------------------------------

    /// Record a change timestamp for the given key.
    ///
    /// Called after any mutation to enable delta sync tracking.
    pub fn record_change(&mut self, key: &str, hlc: HlcTimestamp) {
        self.timestamps.insert(key.to_string(), hlc);
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
        result.sort_by(|a, b| a.2.cmp(&b.2));
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

        let store = Store::load_snapshot_or_default(&path);
        assert!(store.is_empty());
    }

    #[test]
    fn load_snapshot_or_default_corrupt_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt.json");
        std::fs::write(&path, "not valid json {{{").unwrap();

        let store = Store::load_snapshot_or_default(&path);
        assert!(store.is_empty());
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
}
