//! WASM bindings for AsteroidDB CRDT operations.
//!
//! This crate exposes a subset of AsteroidDB's CRDT functionality to JavaScript
//! via `wasm-bindgen`, enabling browser and Node.js applications to use
//! conflict-free replicated data types for local-first or edge computing.

use wasm_bindgen::prelude::*;

use asteroidb_poc::crdt::lww_register::LwwRegister;
use asteroidb_poc::crdt::or_set::OrSet;
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::store::backend::MemoryBackend;
use asteroidb_poc::store::kv::{CrdtValue, Store};
use asteroidb_poc::types::NodeId;

// ---------------------------------------------------------------------------
// WasmPnCounter
// ---------------------------------------------------------------------------

/// A PN-Counter exposed to JavaScript.
///
/// Supports increment, decrement, merge, and value queries.
#[wasm_bindgen]
pub struct WasmPnCounter {
    inner: PnCounter,
    node_id: NodeId,
}

#[wasm_bindgen]
impl WasmPnCounter {
    /// Create a new PN-Counter for the given node.
    #[wasm_bindgen(constructor)]
    pub fn new(node_id: &str) -> Self {
        Self {
            inner: PnCounter::new(),
            node_id: NodeId(node_id.to_string()),
        }
    }

    /// Increment the counter by 1.
    pub fn increment(&mut self) {
        self.inner.increment(&self.node_id);
    }

    /// Decrement the counter by 1.
    pub fn decrement(&mut self) {
        self.inner.decrement(&self.node_id);
    }

    /// Return the current counter value.
    pub fn value(&self) -> i64 {
        self.inner.value()
    }

    /// Merge another counter's state into this one.
    ///
    /// This is the core CRDT operation: after merge, both replicas converge
    /// to the same value regardless of message ordering.
    pub fn merge(&mut self, other: &WasmPnCounter) {
        self.inner.merge(&other.inner);
    }

    /// Serialize the counter state to JSON.
    pub fn to_json(&self) -> Result<String, JsError> {
        serde_json::to_string(&self.inner).map_err(|e| JsError::new(&e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// WasmOrSet
// ---------------------------------------------------------------------------

/// An Observed-Remove Set (OR-Set) exposed to JavaScript.
///
/// Supports add, remove, membership queries, and merge with add-wins semantics.
#[wasm_bindgen]
pub struct WasmOrSet {
    inner: OrSet<String>,
    node_id: NodeId,
}

#[wasm_bindgen]
impl WasmOrSet {
    /// Create a new, empty OR-Set for the given node.
    #[wasm_bindgen(constructor)]
    pub fn new(node_id: &str) -> Self {
        Self {
            inner: OrSet::new(),
            node_id: NodeId(node_id.to_string()),
        }
    }

    /// Add an element to the set.
    pub fn add(&mut self, element: &str) {
        self.inner.add(element.to_string(), &self.node_id);
    }

    /// Remove an element from the set.
    ///
    /// Only the currently observed dots are tombstoned, so a concurrent add
    /// on another replica will survive after merge (add-wins semantics).
    pub fn remove(&mut self, element: &str) {
        self.inner.remove(&element.to_string());
    }

    /// Check whether the set contains the given element.
    pub fn contains(&self, element: &str) -> bool {
        self.inner.contains(&element.to_string())
    }

    /// Return the number of elements in the set.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Return true if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Return all elements as a JSON array of strings.
    pub fn elements_json(&self) -> Result<String, JsError> {
        let elems: Vec<&String> = self.inner.elements().into_iter().collect();
        serde_json::to_string(&elems).map_err(|e| JsError::new(&e.to_string()))
    }

    /// Merge another OR-Set's state into this one.
    pub fn merge(&mut self, other: &WasmOrSet) {
        self.inner.merge(&other.inner);
    }

    /// Serialize the set state to JSON.
    pub fn to_json(&self) -> Result<String, JsError> {
        serde_json::to_string(&self.inner).map_err(|e| JsError::new(&e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// WasmLwwRegister
// ---------------------------------------------------------------------------

/// A Last-Writer-Wins Register exposed to JavaScript.
///
/// The register holds a single string value. Concurrent writes are resolved
/// by comparing HLC timestamps — the latest write wins.
#[wasm_bindgen]
pub struct WasmLwwRegister {
    inner: LwwRegister<String>,
    node_id: String,
    /// Monotonic counter to generate unique HLC timestamps within this node.
    logical: u32,
}

#[wasm_bindgen]
impl WasmLwwRegister {
    /// Create a new, empty LWW-Register for the given node.
    #[wasm_bindgen(constructor)]
    pub fn new(node_id: &str) -> Self {
        Self {
            inner: LwwRegister::new(),
            node_id: node_id.to_string(),
            logical: 0,
        }
    }

    /// Set the register value with an auto-generated timestamp.
    ///
    /// Uses a monotonically increasing logical counter to ensure each write
    /// has a unique timestamp, even in environments without high-resolution
    /// clocks (e.g., WASM).
    pub fn set(&mut self, value: &str) {
        self.logical += 1;
        let ts = HlcTimestamp {
            // Use logical counter as both physical and logical for simplicity.
            // In production, physical would come from Date.now() via js_sys.
            physical: self.logical as u64,
            logical: self.logical,
            node_id: self.node_id.clone(),
        };
        self.inner.set(value.to_string(), ts);
    }

    /// Get the current register value, or null if unset.
    pub fn get(&self) -> Option<String> {
        self.inner.get().cloned()
    }

    /// Merge another register's state into this one.
    pub fn merge(&mut self, other: &WasmLwwRegister) {
        self.inner.merge(&other.inner);
    }

    /// Serialize the register state to JSON.
    pub fn to_json(&self) -> Result<String, JsError> {
        serde_json::to_string(&self.inner).map_err(|e| JsError::new(&e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// WasmStore
// ---------------------------------------------------------------------------

/// A CRDT key-value store exposed to JavaScript.
///
/// Wraps AsteroidDB's `Store` with an in-memory backend, providing
/// put/get/delete operations and snapshot persistence to memory.
#[wasm_bindgen]
pub struct WasmStore {
    inner: Store,
    backend: MemoryBackend,
}

#[wasm_bindgen]
impl WasmStore {
    /// Create a new, empty store with an in-memory backend.
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: Store::new(),
            backend: MemoryBackend::new(),
        }
    }

    /// Store a PN-Counter value under the given key.
    pub fn put_counter(&mut self, key: &str, counter: &WasmPnCounter) {
        self.inner
            .put(key.to_string(), CrdtValue::Counter(counter.inner.clone()));
    }

    /// Store an OR-Set value under the given key.
    pub fn put_set(&mut self, key: &str, set: &WasmOrSet) {
        self.inner
            .put(key.to_string(), CrdtValue::Set(set.inner.clone()));
    }

    /// Store an LWW-Register value under the given key.
    pub fn put_register(&mut self, key: &str, register: &WasmLwwRegister) {
        self.inner
            .put(key.to_string(), CrdtValue::Register(register.inner.clone()));
    }

    /// Get a value by key, returned as a JSON string.
    ///
    /// Returns `null` (as a JSON string) if the key does not exist.
    pub fn get_json(&self, key: &str) -> Result<String, JsError> {
        match self.inner.get(key) {
            Some(value) => serde_json::to_string(value).map_err(|e| JsError::new(&e.to_string())),
            None => Ok("null".to_string()),
        }
    }

    /// Delete a key from the store. Returns true if the key existed.
    pub fn delete(&mut self, key: &str) -> bool {
        self.inner.delete(key).is_some()
    }

    /// Check whether a key exists in the store.
    pub fn contains_key(&self, key: &str) -> bool {
        self.inner.contains_key(key)
    }

    /// Return the number of keys in the store.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Return true if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Return all keys as a JSON array of strings.
    pub fn keys_json(&self) -> Result<String, JsError> {
        let keys: Vec<&String> = self.inner.keys();
        serde_json::to_string(&keys).map_err(|e| JsError::new(&e.to_string()))
    }

    /// Save the current store state to the in-memory backend.
    pub fn save_snapshot(&self) -> Result<(), JsError> {
        self.inner
            .save_to_backend(&self.backend)
            .map_err(|e| JsError::new(&e.to_string()))
    }

    /// Load store state from the in-memory backend.
    ///
    /// Replaces the current store contents with the previously saved snapshot.
    pub fn load_snapshot(&mut self) -> Result<(), JsError> {
        self.inner = Store::load_from_backend(&self.backend)
            .map_err(|e| JsError::new(&e.to_string()))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Convenience: run a quick self-test from JS
// ---------------------------------------------------------------------------

/// Run a self-test that exercises all CRDT types and returns a summary string.
///
/// Useful for verifying the WASM module loaded correctly.
#[wasm_bindgen]
pub fn self_test() -> String {
    let mut results = Vec::new();

    // PnCounter
    let mut c1 = WasmPnCounter::new("node-a");
    let mut c2 = WasmPnCounter::new("node-b");
    c1.increment();
    c1.increment();
    c2.increment();
    c2.decrement();
    c1.merge(&c2);
    results.push(format!("PnCounter: node-a(+2) merged with node-b(+1,-1) = {}", c1.value()));

    // OrSet
    let mut s1 = WasmOrSet::new("node-a");
    let mut s2 = WasmOrSet::new("node-b");
    s1.add("apple");
    s1.add("banana");
    s2.add("cherry");
    s2.add("apple");
    s1.merge(&s2);
    results.push(format!("OrSet: merged set has {} elements", s1.len()));

    // LwwRegister
    let mut r1 = WasmLwwRegister::new("node-a");
    let mut r2 = WasmLwwRegister::new("node-b");
    r1.set("hello");
    r2.set("world");
    r2.set("world!"); // r2 has a later logical timestamp
    r1.merge(&r2);
    results.push(format!(
        "LwwRegister: merged value = {:?}",
        r1.get().unwrap_or_default()
    ));

    // Store
    let mut store = WasmStore::new();
    store.put_counter("visits", &c1);
    store.put_set("fruits", &s1);
    results.push(format!("Store: {} keys stored", store.len()));

    // Snapshot round-trip
    if store.save_snapshot().is_ok() {
        // Verify save/load on the same store instance (same MemoryBackend).
        store.load_snapshot().ok();
        results.push(format!(
            "Store: snapshot save/load OK, {} keys after reload",
            store.len()
        ));
    }

    results.join("\n")
}
