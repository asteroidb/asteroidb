use crate::error::CrdtError;
use crate::hlc::{Hlc, HlcTimestamp};
use crate::store::kv::{CrdtValue, Store};
use crate::types::NodeId;

use crate::crdt::lww_register::LwwRegister;
use crate::crdt::or_map::OrMap;
use crate::crdt::or_set::OrSet;
use crate::crdt::pn_counter::PnCounter;

/// Eventual consistency API (FR-002, FR-004).
///
/// Reads and writes are local-first: writes are accepted immediately
/// and propagated asynchronously. Reads return the local CRDT state,
/// which converges across replicas via merge.
pub struct EventualApi {
    store: Store,
    clock: Hlc,
    node_id: NodeId,
}

impl EventualApi {
    /// Create a new EventualApi for the given node.
    pub fn new(node_id: NodeId) -> Self {
        let clock = Hlc::new(node_id.0.clone());
        Self {
            store: Store::new(),
            clock,
            node_id,
        }
    }

    /// Read the local CRDT value for a key (FR-002).
    ///
    /// Returns `None` if the key does not exist.
    pub fn get_eventual(&self, key: &str) -> Option<&CrdtValue> {
        self.store.get(key)
    }

    /// Write a CRDT value locally (FR-004).
    ///
    /// The value is accepted immediately and will be propagated
    /// to other nodes asynchronously. Records the HLC timestamp
    /// for delta sync tracking.
    pub fn eventual_write(&mut self, key: String, value: CrdtValue) {
        let ts = self.clock.now();
        self.store.put(key.clone(), value);
        self.store.record_change(&key, ts);
    }

    /// Increment a PN-Counter at the given key.
    ///
    /// Creates the counter if the key does not exist.
    /// Returns `TypeMismatch` if the key exists with a different CRDT type.
    pub fn eventual_counter_inc(&mut self, key: &str) -> Result<(), CrdtError> {
        match self.store.get(key) {
            Some(CrdtValue::Counter(_)) => {}
            Some(other) => {
                return Err(CrdtError::TypeMismatch {
                    expected: "Counter".into(),
                    actual: other.type_name().into(),
                });
            }
            None => {
                self.store
                    .put(key.to_string(), CrdtValue::Counter(PnCounter::new()));
            }
        }
        // Safe: we just ensured a Counter exists at this key.
        if let Some(CrdtValue::Counter(c)) = self.store.get_mut(key) {
            c.increment(&self.node_id);
        }
        let ts = self.clock.now();
        self.store.record_change(key, ts);
        Ok(())
    }

    /// Decrement a PN-Counter at the given key.
    ///
    /// Creates the counter if the key does not exist.
    /// Returns `TypeMismatch` if the key exists with a different CRDT type.
    pub fn eventual_counter_dec(&mut self, key: &str) -> Result<(), CrdtError> {
        match self.store.get(key) {
            Some(CrdtValue::Counter(_)) => {}
            Some(other) => {
                return Err(CrdtError::TypeMismatch {
                    expected: "Counter".into(),
                    actual: other.type_name().into(),
                });
            }
            None => {
                self.store
                    .put(key.to_string(), CrdtValue::Counter(PnCounter::new()));
            }
        }
        if let Some(CrdtValue::Counter(c)) = self.store.get_mut(key) {
            c.decrement(&self.node_id);
        }
        let ts = self.clock.now();
        self.store.record_change(key, ts);
        Ok(())
    }

    /// Add an element to an OR-Set at the given key.
    ///
    /// Creates the set if the key does not exist.
    /// Returns `TypeMismatch` if the key exists with a different CRDT type.
    pub fn eventual_set_add(&mut self, key: &str, element: String) -> Result<(), CrdtError> {
        match self.store.get(key) {
            Some(CrdtValue::Set(_)) => {}
            Some(other) => {
                return Err(CrdtError::TypeMismatch {
                    expected: "Set".into(),
                    actual: other.type_name().into(),
                });
            }
            None => {
                self.store
                    .put(key.to_string(), CrdtValue::Set(OrSet::new()));
            }
        }
        if let Some(CrdtValue::Set(s)) = self.store.get_mut(key) {
            s.add(element, &self.node_id);
        }
        let ts = self.clock.now();
        self.store.record_change(key, ts);
        Ok(())
    }

    /// Remove an element from an OR-Set at the given key.
    ///
    /// Returns `TypeMismatch` if the key exists with a different CRDT type.
    /// Returns `KeyNotFound` if the key does not exist.
    pub fn eventual_set_remove(&mut self, key: &str, element: &str) -> Result<(), CrdtError> {
        match self.store.get(key) {
            Some(CrdtValue::Set(_)) => {}
            Some(other) => {
                return Err(CrdtError::TypeMismatch {
                    expected: "Set".into(),
                    actual: other.type_name().into(),
                });
            }
            None => {
                return Err(CrdtError::KeyNotFound(key.to_string()));
            }
        }
        if let Some(CrdtValue::Set(s)) = self.store.get_mut(key) {
            s.remove(&element.to_string());
        }
        let ts = self.clock.now();
        self.store.record_change(key, ts);
        Ok(())
    }

    /// Set a key-value pair in an OR-Map at the given key.
    ///
    /// Creates the map if the key does not exist.
    /// Returns `TypeMismatch` if the key exists with a different CRDT type.
    pub fn eventual_map_set(
        &mut self,
        key: &str,
        map_key: String,
        map_value: String,
    ) -> Result<(), CrdtError> {
        match self.store.get(key) {
            Some(CrdtValue::Map(_)) => {}
            Some(other) => {
                return Err(CrdtError::TypeMismatch {
                    expected: "Map".into(),
                    actual: other.type_name().into(),
                });
            }
            None => {
                self.store
                    .put(key.to_string(), CrdtValue::Map(OrMap::new()));
            }
        }
        let ts = self.clock.now();
        if let Some(CrdtValue::Map(m)) = self.store.get_mut(key) {
            m.set(map_key, map_value, ts, &self.node_id);
        }
        let change_ts = self.clock.now();
        self.store.record_change(key, change_ts);
        Ok(())
    }

    /// Delete a key from an OR-Map at the given key.
    ///
    /// Returns `TypeMismatch` if the key exists with a different CRDT type.
    /// Returns `KeyNotFound` if the key does not exist.
    pub fn eventual_map_delete(&mut self, key: &str, map_key: &str) -> Result<(), CrdtError> {
        match self.store.get(key) {
            Some(CrdtValue::Map(_)) => {}
            Some(other) => {
                return Err(CrdtError::TypeMismatch {
                    expected: "Map".into(),
                    actual: other.type_name().into(),
                });
            }
            None => {
                return Err(CrdtError::KeyNotFound(key.to_string()));
            }
        }
        if let Some(CrdtValue::Map(m)) = self.store.get_mut(key) {
            m.delete(&map_key.to_string());
        }
        let ts = self.clock.now();
        self.store.record_change(key, ts);
        Ok(())
    }

    /// Set a LWW-Register value at the given key.
    ///
    /// Creates the register if the key does not exist.
    /// Returns `TypeMismatch` if the key exists with a different CRDT type.
    pub fn eventual_register_set(&mut self, key: &str, value: String) -> Result<(), CrdtError> {
        match self.store.get(key) {
            Some(CrdtValue::Register(_)) => {}
            Some(other) => {
                return Err(CrdtError::TypeMismatch {
                    expected: "Register".into(),
                    actual: other.type_name().into(),
                });
            }
            None => {
                self.store
                    .put(key.to_string(), CrdtValue::Register(LwwRegister::new()));
            }
        }
        let ts = self.clock.now();
        if let Some(CrdtValue::Register(r)) = self.store.get_mut(key) {
            r.set(value, ts);
        }
        let change_ts = self.clock.now();
        self.store.record_change(key, change_ts);
        Ok(())
    }

    /// Merge a CRDT value received from a remote node.
    ///
    /// Delegates to `Store::merge_value`, which handles type checking
    /// and CRDT-specific merge semantics. Records the HLC timestamp
    /// for delta sync tracking.
    pub fn merge_remote(&mut self, key: String, remote_value: &CrdtValue) -> Result<(), CrdtError> {
        self.store.merge_value(key.clone(), remote_value)?;
        let ts = self.clock.now();
        self.store.record_change(&key, ts);
        Ok(())
    }

    /// Merge a CRDT value received from a remote node with a pre-assigned HLC.
    ///
    /// Used by delta sync to preserve the original modification timestamp.
    /// Only updates the change timestamp if the incoming HLC is newer than
    /// the existing one for that key, preventing an older remote timestamp
    /// from overwriting a newer local one.
    pub fn merge_remote_with_hlc(
        &mut self,
        key: String,
        remote_value: &CrdtValue,
        hlc: HlcTimestamp,
    ) -> Result<(), CrdtError> {
        self.clock.update(&hlc);
        self.store.merge_value(key.clone(), remote_value)?;
        // Always record the change using the maximum of the incoming HLC
        // and any existing timestamp for this key. This ensures that
        // merges are never silently dropped from the change log, which
        // would cause delta-sync peers to miss updates.
        let record_hlc = match self.store.timestamp_for(&key) {
            Some(existing) if *existing > hlc => existing.clone(),
            _ => hlc,
        };
        self.store.record_change(&key, record_hlc);
        Ok(())
    }

    /// Return all keys in the store.
    pub fn keys(&self) -> Vec<&String> {
        self.store.keys()
    }

    /// Return keys that start with the given prefix.
    pub fn keys_with_prefix(&self, prefix: &str) -> Vec<&String> {
        self.store.keys_with_prefix(prefix)
    }

    /// Return a reference to the underlying store.
    ///
    /// Used by the anti-entropy sync layer to read all entries for
    /// push-based replication.
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Return a mutable reference to the underlying `Store`.
    ///
    /// Needed by tombstone GC and compaction to modify CRDT deferred sets
    /// and prune change-tracking timestamps in-place.
    pub fn store_mut(&mut self) -> &mut Store {
        &mut self.store
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str) -> NodeId {
        NodeId(name.into())
    }

    // ---------------------------------------------------------------
    // get_eventual
    // ---------------------------------------------------------------

    #[test]
    fn get_eventual_empty_store_returns_none() {
        let api = EventualApi::new(node("node-a"));
        assert!(api.get_eventual("missing").is_none());
    }

    // ---------------------------------------------------------------
    // eventual_write + get_eventual round-trip
    // ---------------------------------------------------------------

    #[test]
    fn eventual_write_and_get_round_trip() {
        let mut api = EventualApi::new(node("node-a"));

        let mut counter = PnCounter::new();
        counter.increment(&node("node-a"));
        api.eventual_write("hits".into(), CrdtValue::Counter(counter));

        match api.get_eventual("hits") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn eventual_write_overwrites() {
        let mut api = EventualApi::new(node("node-a"));

        let mut c1 = PnCounter::new();
        c1.increment(&node("node-a"));
        api.eventual_write("k".into(), CrdtValue::Counter(c1));

        let mut c2 = PnCounter::new();
        c2.increment(&node("node-a"));
        c2.increment(&node("node-a"));
        api.eventual_write("k".into(), CrdtValue::Counter(c2));

        match api.get_eventual("k") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 2),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    // ---------------------------------------------------------------
    // Counter inc/dec
    // ---------------------------------------------------------------

    #[test]
    fn counter_inc_creates_and_increments() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_counter_inc("count").unwrap();
        api.eventual_counter_inc("count").unwrap();
        api.eventual_counter_inc("count").unwrap();

        match api.get_eventual("count") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 3),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn counter_dec_creates_and_decrements() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_counter_dec("count").unwrap();
        api.eventual_counter_dec("count").unwrap();

        match api.get_eventual("count") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), -2),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn counter_inc_and_dec_combined() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_counter_inc("count").unwrap();
        api.eventual_counter_inc("count").unwrap();
        api.eventual_counter_inc("count").unwrap();
        api.eventual_counter_dec("count").unwrap();

        match api.get_eventual("count") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 2),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn counter_inc_type_mismatch() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_write("k".into(), CrdtValue::Set(OrSet::new()));

        let err = api.eventual_counter_inc("k").unwrap_err();
        assert_eq!(
            err,
            CrdtError::TypeMismatch {
                expected: "Counter".into(),
                actual: "Set".into(),
            }
        );
    }

    #[test]
    fn counter_dec_type_mismatch() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_write("k".into(), CrdtValue::Register(LwwRegister::new()));

        let err = api.eventual_counter_dec("k").unwrap_err();
        assert_eq!(
            err,
            CrdtError::TypeMismatch {
                expected: "Counter".into(),
                actual: "Register".into(),
            }
        );
    }

    // ---------------------------------------------------------------
    // Set add/remove
    // ---------------------------------------------------------------

    #[test]
    fn set_add_creates_and_adds() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_set_add("users", "alice".into()).unwrap();
        api.eventual_set_add("users", "bob".into()).unwrap();

        match api.get_eventual("users") {
            Some(CrdtValue::Set(s)) => {
                assert!(s.contains(&"alice".to_string()));
                assert!(s.contains(&"bob".to_string()));
                assert_eq!(s.len(), 2);
            }
            other => panic!("expected Set, got {:?}", other),
        }
    }

    #[test]
    fn set_remove_removes_element() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_set_add("users", "alice".into()).unwrap();
        api.eventual_set_add("users", "bob".into()).unwrap();
        api.eventual_set_remove("users", "alice").unwrap();

        match api.get_eventual("users") {
            Some(CrdtValue::Set(s)) => {
                assert!(!s.contains(&"alice".to_string()));
                assert!(s.contains(&"bob".to_string()));
                assert_eq!(s.len(), 1);
            }
            other => panic!("expected Set, got {:?}", other),
        }
    }

    #[test]
    fn set_remove_nonexistent_key_returns_key_not_found() {
        let mut api = EventualApi::new(node("node-a"));
        let err = api.eventual_set_remove("missing", "x").unwrap_err();
        assert_eq!(err, CrdtError::KeyNotFound("missing".into()));
    }

    #[test]
    fn set_add_type_mismatch() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_write("k".into(), CrdtValue::Counter(PnCounter::new()));

        let err = api.eventual_set_add("k", "x".into()).unwrap_err();
        assert_eq!(
            err,
            CrdtError::TypeMismatch {
                expected: "Set".into(),
                actual: "Counter".into(),
            }
        );
    }

    // ---------------------------------------------------------------
    // Map set/delete
    // ---------------------------------------------------------------

    #[test]
    fn map_set_creates_and_sets() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_map_set("config", "name".into(), "AsteroidDB".into())
            .unwrap();

        match api.get_eventual("config") {
            Some(CrdtValue::Map(m)) => {
                assert_eq!(m.get(&"name".to_string()), Some(&"AsteroidDB".to_string()));
            }
            other => panic!("expected Map, got {:?}", other),
        }
    }

    #[test]
    fn map_set_overwrites_value() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_map_set("config", "name".into(), "old".into())
            .unwrap();
        api.eventual_map_set("config", "name".into(), "new".into())
            .unwrap();

        match api.get_eventual("config") {
            Some(CrdtValue::Map(m)) => {
                assert_eq!(m.get(&"name".to_string()), Some(&"new".to_string()));
            }
            other => panic!("expected Map, got {:?}", other),
        }
    }

    #[test]
    fn map_delete_removes_entry() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_map_set("config", "name".into(), "AsteroidDB".into())
            .unwrap();
        api.eventual_map_set("config", "version".into(), "1.0".into())
            .unwrap();
        api.eventual_map_delete("config", "name").unwrap();

        match api.get_eventual("config") {
            Some(CrdtValue::Map(m)) => {
                assert!(!m.contains_key(&"name".to_string()));
                assert_eq!(m.get(&"version".to_string()), Some(&"1.0".to_string()));
            }
            other => panic!("expected Map, got {:?}", other),
        }
    }

    #[test]
    fn map_delete_nonexistent_key_returns_key_not_found() {
        let mut api = EventualApi::new(node("node-a"));
        let err = api.eventual_map_delete("missing", "k").unwrap_err();
        assert_eq!(err, CrdtError::KeyNotFound("missing".into()));
    }

    #[test]
    fn map_set_type_mismatch() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_write("k".into(), CrdtValue::Set(OrSet::new()));

        let err = api
            .eventual_map_set("k", "key".into(), "val".into())
            .unwrap_err();
        assert_eq!(
            err,
            CrdtError::TypeMismatch {
                expected: "Map".into(),
                actual: "Set".into(),
            }
        );
    }

    // ---------------------------------------------------------------
    // Register set
    // ---------------------------------------------------------------

    #[test]
    fn register_set_creates_and_sets() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_register_set("greeting", "hello".into())
            .unwrap();

        match api.get_eventual("greeting") {
            Some(CrdtValue::Register(r)) => {
                assert_eq!(r.get(), Some(&"hello".to_string()));
            }
            other => panic!("expected Register, got {:?}", other),
        }
    }

    #[test]
    fn register_set_overwrites_value() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_register_set("greeting", "hello".into())
            .unwrap();
        api.eventual_register_set("greeting", "world".into())
            .unwrap();

        match api.get_eventual("greeting") {
            Some(CrdtValue::Register(r)) => {
                assert_eq!(r.get(), Some(&"world".to_string()));
            }
            other => panic!("expected Register, got {:?}", other),
        }
    }

    #[test]
    fn register_set_type_mismatch() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_write("k".into(), CrdtValue::Counter(PnCounter::new()));

        let err = api.eventual_register_set("k", "val".into()).unwrap_err();
        assert_eq!(
            err,
            CrdtError::TypeMismatch {
                expected: "Register".into(),
                actual: "Counter".into(),
            }
        );
    }

    // ---------------------------------------------------------------
    // merge_remote
    // ---------------------------------------------------------------

    #[test]
    fn merge_remote_matching_types() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_counter_inc("count").unwrap();
        api.eventual_counter_inc("count").unwrap();

        let mut remote = PnCounter::new();
        remote.increment(&node("node-b"));
        remote.increment(&node("node-b"));
        remote.increment(&node("node-b"));

        api.merge_remote("count".into(), &CrdtValue::Counter(remote))
            .unwrap();

        match api.get_eventual("count") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 5),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn merge_remote_into_nonexistent_key() {
        let mut api = EventualApi::new(node("node-a"));

        let mut remote = PnCounter::new();
        remote.increment(&node("node-b"));

        api.merge_remote("new_key".into(), &CrdtValue::Counter(remote))
            .unwrap();

        match api.get_eventual("new_key") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn merge_remote_type_mismatch() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_write("k".into(), CrdtValue::Counter(PnCounter::new()));

        let err = api
            .merge_remote("k".into(), &CrdtValue::Set(OrSet::new()))
            .unwrap_err();
        assert_eq!(
            err,
            CrdtError::TypeMismatch {
                expected: "Counter".into(),
                actual: "Set".into(),
            }
        );
    }

    // ---------------------------------------------------------------
    // keys / keys_with_prefix
    // ---------------------------------------------------------------

    #[test]
    fn keys_returns_all_keys() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_counter_inc("a").unwrap();
        api.eventual_counter_inc("b").unwrap();
        api.eventual_counter_inc("c").unwrap();

        let mut keys = api.keys();
        keys.sort();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn keys_with_prefix_filters() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_counter_inc("user/alice").unwrap();
        api.eventual_counter_inc("user/bob").unwrap();
        api.eventual_counter_inc("config/db").unwrap();

        let mut user_keys = api.keys_with_prefix("user/");
        user_keys.sort();
        assert_eq!(user_keys, vec!["user/alice", "user/bob"]);

        let config_keys = api.keys_with_prefix("config/");
        assert_eq!(config_keys.len(), 1);
    }

    #[test]
    fn keys_with_prefix_no_match() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_counter_inc("user/alice").unwrap();

        let keys = api.keys_with_prefix("log/");
        assert!(keys.is_empty());
    }
}
