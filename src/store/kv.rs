use std::collections::HashMap;

use crate::crdt::lww_register::LwwRegister;
use crate::crdt::or_map::OrMap;
use crate::crdt::or_set::OrSet;
use crate::crdt::pn_counter::PnCounter;
use crate::error::CrdtError;

/// A CRDT value stored in the KVS.
///
/// Wraps all supported CRDT types so the store can hold heterogeneous
/// values while preserving type-safe merge semantics.
#[derive(Debug, Clone)]
pub enum CrdtValue {
    Counter(PnCounter),
    Set(OrSet<String>),
    Map(OrMap<String, String>),
    Register(LwwRegister<String>),
}

impl CrdtValue {
    /// Returns a human-readable type name for error reporting.
    fn type_name(&self) -> &'static str {
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
/// and CRDT-aware value merging with type checking.
#[derive(Debug, Clone)]
pub struct Store {
    data: HashMap<String, CrdtValue>,
}

impl Store {
    /// Create a new, empty store.
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Get a reference to the value associated with `key`.
    pub fn get(&self, key: &str) -> Option<&CrdtValue> {
        self.data.get(key)
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
}
