//! WASM-compatible StorageBackend verification tests.
//!
//! These tests exercise `MemoryBackend` and `InMemoryKvBackend` — the two
//! storage backends available on `wasm32-unknown-unknown`. They run on both
//! native and WASM targets to ensure the backends work identically in both
//! environments.
//!
//! The tests verify:
//! - Basic CRUD operations
//! - Data persistence across operations (within the same runtime)
//! - Concurrent access via `Arc`-shared handles
//! - Edge cases (empty values, large payloads, Unicode keys)
//!
//! Related: #297

use std::io;
use std::sync::Arc;
use std::thread;

use asteroidb_poc::store::backend::{InMemoryKvBackend, KvBackend, MemoryBackend, StorageBackend};

// ===========================================================================
// MemoryBackend — StorageBackend trait tests
// ===========================================================================

#[test]
fn memory_backend_initial_state_is_empty() {
    let backend = MemoryBackend::new();
    assert!(!backend.exists());
    assert!(backend.data().is_none());
}

#[test]
fn memory_backend_save_then_load() {
    let backend = MemoryBackend::new();
    backend.save(b"hello wasm").unwrap();
    assert!(backend.exists());

    let loaded = backend.load().unwrap();
    assert_eq!(loaded, b"hello wasm");
}

#[test]
fn memory_backend_overwrite_replaces_data() {
    let backend = MemoryBackend::new();
    backend.save(b"version-1").unwrap();
    backend.save(b"version-2").unwrap();

    assert_eq!(backend.load().unwrap(), b"version-2");
}

#[test]
fn memory_backend_load_before_save_returns_not_found() {
    let backend = MemoryBackend::new();
    let err = backend.load().unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
}

#[test]
fn memory_backend_empty_data_is_valid() {
    let backend = MemoryBackend::new();
    backend.save(b"").unwrap();
    assert!(backend.exists());

    let loaded = backend.load().unwrap();
    assert!(loaded.is_empty());
}

#[test]
fn memory_backend_large_payload() {
    let backend = MemoryBackend::new();
    // 1 MiB payload — verifies no artificial size limits.
    let payload = vec![0xABu8; 1024 * 1024];
    backend.save(&payload).unwrap();
    assert_eq!(backend.load().unwrap(), payload);
}

#[test]
fn memory_backend_binary_data() {
    let backend = MemoryBackend::new();
    // Full byte range including null bytes.
    let payload: Vec<u8> = (0..=255).collect();
    backend.save(&payload).unwrap();
    assert_eq!(backend.load().unwrap(), payload);
}

#[test]
fn memory_backend_clone_shares_state() {
    let backend = MemoryBackend::new();
    let clone = backend.clone();

    backend.save(b"shared-state").unwrap();
    assert_eq!(clone.load().unwrap(), b"shared-state");
    assert!(clone.exists());
}

#[test]
fn memory_backend_concurrent_access() {
    let backend = Arc::new(MemoryBackend::new());
    let mut handles = Vec::new();

    // Spawn 10 threads that each save then load.
    for i in 0..10u8 {
        let b = Arc::clone(&backend);
        handles.push(thread::spawn(move || {
            let data = vec![i; 64];
            b.save(&data).unwrap();
            // load should succeed — might see our data or another thread's.
            let loaded = b.load().unwrap();
            assert_eq!(loaded.len(), 64);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // After all threads, backend should have some data.
    assert!(backend.exists());
    assert_eq!(backend.load().unwrap().len(), 64);
}

#[test]
fn memory_backend_data_accessor_reflects_saves() {
    let backend = MemoryBackend::new();
    assert!(backend.data().is_none());

    backend.save(b"snapshot-1").unwrap();
    assert_eq!(backend.data(), Some(b"snapshot-1".to_vec()));

    backend.save(b"snapshot-2").unwrap();
    assert_eq!(backend.data(), Some(b"snapshot-2".to_vec()));
}

#[test]
fn memory_backend_persistence_across_operations() {
    let backend = MemoryBackend::new();

    // Simulate multiple save/load cycles (within same runtime).
    for round in 0..5u32 {
        let payload = format!("round-{round}").into_bytes();
        backend.save(&payload).unwrap();
        assert_eq!(backend.load().unwrap(), payload);
    }

    // Final state should be last written value.
    assert_eq!(backend.load().unwrap(), b"round-4");
}

// ===========================================================================
// InMemoryKvBackend — KvBackend trait tests
// ===========================================================================

#[test]
fn kv_backend_initial_state_is_empty() {
    let kv = InMemoryKvBackend::new();
    assert!(kv.is_empty());
    assert_eq!(kv.len(), 0);
}

#[test]
fn kv_backend_put_and_get() {
    let kv = InMemoryKvBackend::new();
    kv.put("key1", b"value1").unwrap();
    assert_eq!(kv.get("key1").unwrap(), Some(b"value1".to_vec()));
}

#[test]
fn kv_backend_get_missing_returns_none() {
    let kv = InMemoryKvBackend::new();
    assert!(kv.get("nonexistent").unwrap().is_none());
}

#[test]
fn kv_backend_put_overwrites() {
    let kv = InMemoryKvBackend::new();
    kv.put("key", b"old").unwrap();
    kv.put("key", b"new").unwrap();
    assert_eq!(kv.get("key").unwrap(), Some(b"new".to_vec()));
    assert_eq!(kv.len(), 1);
}

#[test]
fn kv_backend_delete_existing() {
    let kv = InMemoryKvBackend::new();
    kv.put("key", b"val").unwrap();
    kv.delete("key").unwrap();
    assert!(kv.get("key").unwrap().is_none());
    assert!(kv.is_empty());
}

#[test]
fn kv_backend_delete_nonexistent_is_noop() {
    let kv = InMemoryKvBackend::new();
    // Should not panic or return error.
    kv.delete("ghost").unwrap();
    assert!(kv.is_empty());
}

#[test]
fn kv_backend_empty_key_and_value() {
    let kv = InMemoryKvBackend::new();
    kv.put("", b"").unwrap();
    assert_eq!(kv.get("").unwrap(), Some(Vec::new()));
    assert_eq!(kv.len(), 1);
}

#[test]
fn kv_backend_unicode_keys() {
    let kv = InMemoryKvBackend::new();
    kv.put("日本語キー", b"value").unwrap();
    kv.put("clé-française", b"valeur").unwrap();
    kv.put("emoji-🚀", b"rocket").unwrap();

    assert_eq!(kv.get("日本語キー").unwrap(), Some(b"value".to_vec()));
    assert_eq!(kv.get("clé-française").unwrap(), Some(b"valeur".to_vec()));
    assert_eq!(kv.get("emoji-🚀").unwrap(), Some(b"rocket".to_vec()));
    assert_eq!(kv.len(), 3);
}

#[test]
fn kv_backend_binary_values() {
    let kv = InMemoryKvBackend::new();
    let binary: Vec<u8> = (0..=255).collect();
    kv.put("bin", &binary).unwrap();
    assert_eq!(kv.get("bin").unwrap(), Some(binary));
}

#[test]
fn kv_backend_large_value() {
    let kv = InMemoryKvBackend::new();
    let large = vec![0xFFu8; 512 * 1024]; // 512 KiB
    kv.put("big", &large).unwrap();
    assert_eq!(kv.get("big").unwrap(), Some(large));
}

#[test]
fn kv_backend_scan_prefix_basic() {
    let kv = InMemoryKvBackend::new();
    kv.put("user:1", b"alice").unwrap();
    kv.put("user:2", b"bob").unwrap();
    kv.put("user:3", b"carol").unwrap();
    kv.put("order:1", b"order-data").unwrap();
    kv.put("order:2", b"order-data-2").unwrap();

    let users = kv.scan_prefix("user:").unwrap();
    assert_eq!(users.len(), 3);
    assert_eq!(users[0].0, "user:1");
    assert_eq!(users[1].0, "user:2");
    assert_eq!(users[2].0, "user:3");

    let orders = kv.scan_prefix("order:").unwrap();
    assert_eq!(orders.len(), 2);
}

#[test]
fn kv_backend_scan_prefix_no_matches() {
    let kv = InMemoryKvBackend::new();
    kv.put("a:1", b"x").unwrap();
    let results = kv.scan_prefix("z:").unwrap();
    assert!(results.is_empty());
}

#[test]
fn kv_backend_scan_prefix_empty_prefix_returns_all() {
    let kv = InMemoryKvBackend::new();
    kv.put("a", b"1").unwrap();
    kv.put("b", b"2").unwrap();
    kv.put("c", b"3").unwrap();

    let all = kv.scan_prefix("").unwrap();
    assert_eq!(all.len(), 3);
}

#[test]
fn kv_backend_scan_prefix_results_are_sorted() {
    let kv = InMemoryKvBackend::new();
    // Insert in reverse order.
    kv.put("ns:z", b"last").unwrap();
    kv.put("ns:a", b"first").unwrap();
    kv.put("ns:m", b"middle").unwrap();

    let results = kv.scan_prefix("ns:").unwrap();
    assert_eq!(results[0].0, "ns:a");
    assert_eq!(results[1].0, "ns:m");
    assert_eq!(results[2].0, "ns:z");
}

#[test]
fn kv_backend_entries_since() {
    let kv = InMemoryKvBackend::new();
    kv.put("a", b"1").unwrap();
    kv.put("b", b"2").unwrap();
    kv.put("c", b"3").unwrap();
    kv.put("d", b"4").unwrap();

    let since_b = kv.entries_since(b"b").unwrap();
    assert_eq!(since_b.len(), 3); // b, c, d
    assert_eq!(since_b[0].0, "b");
    assert_eq!(since_b[1].0, "c");
    assert_eq!(since_b[2].0, "d");
}

#[test]
fn kv_backend_entries_since_beyond_last_key() {
    let kv = InMemoryKvBackend::new();
    kv.put("a", b"1").unwrap();
    kv.put("b", b"2").unwrap();

    let since = kv.entries_since(b"z").unwrap();
    assert!(since.is_empty());
}

#[test]
fn kv_backend_entries_since_empty_frontier() {
    let kv = InMemoryKvBackend::new();
    kv.put("x", b"1").unwrap();
    kv.put("y", b"2").unwrap();

    let since = kv.entries_since(b"").unwrap();
    assert_eq!(since.len(), 2);
}

#[test]
fn kv_backend_persistence_across_operations() {
    let kv = InMemoryKvBackend::new();

    // Write phase.
    for i in 0..100u32 {
        let key = format!("key:{i:04}");
        let val = format!("value-{i}").into_bytes();
        kv.put(&key, &val).unwrap();
    }
    assert_eq!(kv.len(), 100);

    // Delete every other key.
    for i in (0..100u32).step_by(2) {
        let key = format!("key:{i:04}");
        kv.delete(&key).unwrap();
    }
    assert_eq!(kv.len(), 50);

    // Verify remaining keys.
    for i in (1..100u32).step_by(2) {
        let key = format!("key:{i:04}");
        let expected = format!("value-{i}").into_bytes();
        assert_eq!(kv.get(&key).unwrap(), Some(expected));
    }

    // Verify deleted keys are gone.
    for i in (0..100u32).step_by(2) {
        let key = format!("key:{i:04}");
        assert!(kv.get(&key).unwrap().is_none());
    }
}

#[test]
fn kv_backend_concurrent_reads_and_writes() {
    let kv = Arc::new(InMemoryKvBackend::new());
    let mut handles = Vec::new();

    // Writer threads.
    for i in 0..5u32 {
        let kv = Arc::clone(&kv);
        handles.push(thread::spawn(move || {
            for j in 0..20u32 {
                let key = format!("t{i}:k{j}");
                let val = format!("v{i}-{j}").into_bytes();
                kv.put(&key, &val).unwrap();
            }
        }));
    }

    // Reader threads.
    for _ in 0..3 {
        let kv = Arc::clone(&kv);
        handles.push(thread::spawn(move || {
            for _ in 0..50 {
                // scan_prefix should never fail, though results vary.
                let _ = kv.scan_prefix("t").unwrap();
                let _ = kv.entries_since(b"t0:").unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // All writer threads wrote 20 keys each.
    assert_eq!(kv.len(), 100);
}

#[test]
fn kv_backend_clone_shares_state() {
    let kv = InMemoryKvBackend::new();
    let clone = kv.clone();

    kv.put("shared-key", b"shared-value").unwrap();
    assert_eq!(
        clone.get("shared-key").unwrap(),
        Some(b"shared-value".to_vec())
    );

    clone.delete("shared-key").unwrap();
    assert!(kv.get("shared-key").unwrap().is_none());
}

// ===========================================================================
// WASM compatibility verification (compile-time assertions)
// ===========================================================================

/// Verify that `MemoryBackend` satisfies `Send + Sync` (required by
/// `StorageBackend` and essential for WASM runtimes that may move
/// backends across tasks).
#[test]
fn memory_backend_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<MemoryBackend>();
}

/// Verify that `InMemoryKvBackend` satisfies `Send + Sync`.
#[test]
fn in_memory_kv_backend_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<InMemoryKvBackend>();
}

/// Verify that the traits themselves are object-safe and can be used
/// behind `Box<dyn ...>`.
#[test]
fn trait_object_safety() {
    let _storage: Box<dyn StorageBackend> = Box::new(MemoryBackend::new());
    let _kv: Box<dyn KvBackend> = Box::new(InMemoryKvBackend::new());
}

/// Verify that both backends can be wrapped in `Arc` for shared ownership
/// (common pattern in WASM runtimes).
#[test]
fn arc_wrapping() {
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    storage.save(b"arc-test").unwrap();
    assert_eq!(storage.load().unwrap(), b"arc-test");

    let kv: Arc<dyn KvBackend> = Arc::new(InMemoryKvBackend::new());
    kv.put("arc-key", b"arc-val").unwrap();
    assert_eq!(kv.get("arc-key").unwrap(), Some(b"arc-val".to_vec()));
}

// ===========================================================================
// Interaction between MemoryBackend and InMemoryKvBackend
// ===========================================================================

/// Demonstrates that MemoryBackend (blob storage) and InMemoryKvBackend
/// (per-key storage) are independent but both WASM-compatible.
#[test]
fn both_backends_coexist() {
    let blob = MemoryBackend::new();
    let kv = InMemoryKvBackend::new();

    // Use KvBackend for structured data.
    kv.put("user:1", b"alice").unwrap();
    kv.put("user:2", b"bob").unwrap();

    // Serialize KvBackend state to a blob snapshot.
    let snapshot: Vec<(String, Vec<u8>)> = kv.scan_prefix("").unwrap();
    let snapshot_bytes = format!("{snapshot:?}").into_bytes();

    // Store the blob via MemoryBackend.
    blob.save(&snapshot_bytes).unwrap();
    assert!(blob.exists());

    // Both remain usable.
    kv.put("user:3", b"carol").unwrap();
    assert_eq!(kv.len(), 3);
    assert!(!blob.load().unwrap().is_empty());
}
