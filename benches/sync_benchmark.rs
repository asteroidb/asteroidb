//! Benchmark comparing full sync vs delta sync communication costs (#120).
//!
//! Measures serialized payload size (bytes) and elapsed time for:
//! - Full sync: sending all 1000 entries
//! - Delta sync: sending only 10 changed entries out of 1000
//!
//! Run with: `cargo bench --bench sync_benchmark`

use std::collections::HashMap;
use std::time::Instant;

use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::Hlc;
use asteroidb_poc::network::sync::{DeltaEntry, DeltaSyncResponse, SyncRequest};
use asteroidb_poc::store::kv::{CrdtValue, Store};
use asteroidb_poc::types::NodeId;

fn node_id(s: &str) -> NodeId {
    NodeId(s.into())
}

fn main() {
    let total_entries = 1000;
    let changed_entries = 10;

    println!("=== Sync Benchmark: Full vs Delta ===");
    println!("Total entries: {total_entries}, Changed entries: {changed_entries}");
    println!();

    // Build a store with `total_entries` counters.
    let mut store = Store::new();
    let mut clock = Hlc::new("bench-node".into());

    for i in 0..total_entries {
        let key = format!("key-{i:04}");
        let mut counter = PnCounter::new();
        counter.increment(&node_id("bench-node"));
        let ts = clock.now();
        store.put(key.clone(), CrdtValue::Counter(counter));
        store.record_change(&key, ts);
    }

    // Record frontier after initial population.
    let frontier_after_init = store.current_frontier().unwrap();

    // Modify `changed_entries` keys (simulate small batch of updates).
    for i in 0..changed_entries {
        let key = format!("key-{i:04}");
        if let Some(CrdtValue::Counter(c)) = store.get_mut(&key) {
            c.increment(&node_id("bench-node"));
        }
        let ts = clock.now();
        store.record_change(&key, ts);
    }

    // ---------------------------------------------------------------
    // Full sync: serialize all entries
    // ---------------------------------------------------------------

    let full_entries: HashMap<String, CrdtValue> = store
        .all_entries()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    let full_request = SyncRequest {
        sender: "bench-node".into(),
        entries: full_entries,
    };

    let start = Instant::now();
    let full_json = serde_json::to_vec(&full_request).unwrap();
    let full_serialize_time = start.elapsed();
    let full_bytes = full_json.len();

    println!("Full sync:");
    println!(
        "  Payload size: {full_bytes} bytes ({:.1} KB)",
        full_bytes as f64 / 1024.0
    );
    println!("  Serialize time: {full_serialize_time:?}");

    // ---------------------------------------------------------------
    // Delta sync: serialize only changed entries
    // ---------------------------------------------------------------

    let delta_entries: Vec<DeltaEntry> = store
        .entries_since(&frontier_after_init)
        .into_iter()
        .map(|(key, value, hlc)| DeltaEntry { key, value, hlc })
        .collect();

    let delta_response = DeltaSyncResponse {
        entries: delta_entries,
        sender_frontier: store.current_frontier(),
    };

    let start = Instant::now();
    let delta_json = serde_json::to_vec(&delta_response).unwrap();
    let delta_serialize_time = start.elapsed();
    let delta_bytes = delta_json.len();

    println!();
    println!("Delta sync:");
    println!(
        "  Payload size: {delta_bytes} bytes ({:.1} KB)",
        delta_bytes as f64 / 1024.0
    );
    println!("  Serialize time: {delta_serialize_time:?}");
    println!("  Entries sent: {}", delta_response.entries.len());

    // ---------------------------------------------------------------
    // Comparison
    // ---------------------------------------------------------------

    let savings_pct = if full_bytes > 0 {
        (1.0 - delta_bytes as f64 / full_bytes as f64) * 100.0
    } else {
        0.0
    };

    println!();
    println!("=== Results ===");
    println!("  Bandwidth savings: {savings_pct:.1}%");
    println!(
        "  Full/Delta ratio: {:.1}x",
        full_bytes as f64 / delta_bytes.max(1) as f64
    );

    // Assertions: delta should be significantly smaller.
    assert!(
        delta_bytes < full_bytes,
        "delta ({delta_bytes}B) should be smaller than full ({full_bytes}B)"
    );
    assert!(
        savings_pct > 80.0,
        "expected >80% savings, got {savings_pct:.1}%"
    );

    println!();
    println!("All assertions passed.");
}
