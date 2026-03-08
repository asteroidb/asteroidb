//! Criterion benchmarks for the Store layer: put, get, entries_since,
//! save_snapshot, and load_snapshot.

use std::path::Path;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use tempfile::TempDir;

use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::{Hlc, HlcTimestamp};
use asteroidb_poc::store::kv::{CrdtValue, Store};
use asteroidb_poc::types::NodeId;

fn node(name: &str) -> NodeId {
    NodeId(name.into())
}

/// Build a store pre-populated with `n` counter entries and HLC timestamps.
fn build_store(n: usize) -> (Store, Option<HlcTimestamp>) {
    let mut store = Store::new();
    let mut clock = Hlc::new("bench-node".into());
    let nid = node("bench-node");

    for i in 0..n {
        let key = format!("key-{i:06}");
        let mut counter = PnCounter::new();
        counter.increment(&nid);
        let ts = clock.now();
        store.put(key.clone(), CrdtValue::Counter(counter));
        store.record_change(&key, ts);
    }

    let frontier = store.current_frontier();
    (store, frontier)
}

// ---------------------------------------------------------------------------
// put + get benchmarks
// ---------------------------------------------------------------------------

fn bench_store_put(c: &mut Criterion) {
    let nid = node("bench-node");

    c.bench_function("store/put_1000", |b| {
        b.iter(|| {
            let mut store = Store::new();
            for i in 0..1000 {
                let key = format!("key-{i:06}");
                let mut counter = PnCounter::new();
                counter.increment(&nid);
                store.put(key, CrdtValue::Counter(counter));
            }
            store
        });
    });
}

fn bench_store_get(c: &mut Criterion) {
    let (store, _) = build_store(1000);

    c.bench_function("store/get_existing", |b| {
        b.iter(|| {
            // Access 100 keys spread across the keyspace.
            for i in (0..1000).step_by(10) {
                let key = format!("key-{i:06}");
                std::hint::black_box(store.get(&key));
            }
        });
    });
}

fn bench_store_get_missing(c: &mut Criterion) {
    let (store, _) = build_store(1000);

    c.bench_function("store/get_missing", |b| {
        b.iter(|| {
            for i in 0..100 {
                let key = format!("nonexistent-{i}");
                std::hint::black_box(store.get(&key));
            }
        });
    });
}

// ---------------------------------------------------------------------------
// entries_since benchmarks
// ---------------------------------------------------------------------------

fn bench_entries_since(c: &mut Criterion) {
    let mut group = c.benchmark_group("store/entries_since");

    for total in [100, 1000, 5000] {
        group.bench_with_input(BenchmarkId::from_parameter(total), &total, |b, &total| {
            let (mut store, frontier) = build_store(total);

            // Record changes for the last 10% of keys (simulate delta).
            let mut clock = Hlc::new("bench-node".into());
            let changed = total / 10;
            for i in 0..changed {
                let key = format!("key-{i:06}");
                let ts = clock.now();
                store.record_change(&key, ts);
            }

            let frontier = frontier.unwrap();
            b.iter(|| {
                let entries = store.entries_since(&frontier);
                std::hint::black_box(entries.len());
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Snapshot benchmarks
// ---------------------------------------------------------------------------

fn bench_save_snapshot(c: &mut Criterion) {
    let (store, _) = build_store(1000);
    let tmp_dir = TempDir::new().expect("create temp dir");
    let path = tmp_dir.path().join("bench-snapshot.json");

    c.bench_function("store/save_snapshot_1000", |b| {
        b.iter(|| {
            store.save_snapshot(Path::new(&path)).unwrap();
        });
    });
}

fn bench_load_snapshot(c: &mut Criterion) {
    let (store, _) = build_store(1000);
    let tmp_dir = TempDir::new().expect("create temp dir");
    let path = tmp_dir.path().join("bench-snapshot.json");
    store.save_snapshot(Path::new(&path)).unwrap();

    c.bench_function("store/load_snapshot_1000", |b| {
        b.iter(|| {
            let loaded = Store::load_snapshot(Path::new(&path)).unwrap();
            std::hint::black_box(loaded.len());
        });
    });
}

criterion_group!(
    benches,
    bench_store_put,
    bench_store_get,
    bench_store_get_missing,
    bench_entries_since,
    bench_save_snapshot,
    bench_load_snapshot,
);
criterion_main!(benches);
