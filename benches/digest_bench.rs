//! Digest-computation benchmarks (M-7).
//!
//! Compares the pre-M-7 per-exchange cost (full store clone + full
//! SHA-256 pass — what `snapshot_store_digest` and the digest handler
//! paid on EVERY probe/answer) against the incremental `Store::digest()`
//! cache: steady state (dirty = 0, the M-6 RR-gate regime) and bursts of
//! d dirty keys.

use asteroidb_poc::crdt::lww_register::LwwRegister;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::store::digest::compute_store_digest;
use asteroidb_poc::store::kv::{CrdtValue, Store};
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use std::collections::BTreeMap;

fn ts(physical: u64) -> HlcTimestamp {
    HlcTimestamp {
        physical,
        logical: 0,
        node_id: "bench-node".into(),
    }
}

fn key(i: usize) -> String {
    format!("bench/key-{i:06}")
}

fn build_store(n: usize) -> Store {
    let mut store = Store::new();
    for i in 0..n {
        let mut reg = LwwRegister::new();
        reg.set(format!("value-{i}"), ts(i as u64 + 1));
        store.put_with_timestamp(key(i), CrdtValue::Register(reg), ts(i as u64 + 1));
    }
    store
}

fn bench_digest(c: &mut Criterion) {
    for &n in &[1_000usize, 10_000, 100_000] {
        let mut warm = build_store(n);
        let _ = warm.digest(); // warm the cache once
        let data: BTreeMap<String, CrdtValue> = warm
            .all_entries()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let mut group = c.benchmark_group(format!("digest/N={n}"));
        group.sample_size(10);

        // Pre-M-7 per-exchange cost: deep clone (under the store lock)
        // plus a full hashing pass.
        group.bench_function("legacy_clone_plus_full_hash", |b| {
            b.iter(|| {
                let snapshot: BTreeMap<String, CrdtValue> = warm
                    .all_entries()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                compute_store_digest(&snapshot)
            })
        });

        // Clone-free full pass (isolates hash cost from clone cost).
        group.bench_function("full_hash_only", |b| b.iter(|| compute_store_digest(&data)));

        // M-7 steady state: warm cache, dirty = 0 (the converged
        // cluster regime under the M-6 RR gate).
        group.bench_function("cached_dirty=0", |b| b.iter(|| warm.digest()));

        // M-7 incremental refresh with d dirty keys (d = 1024 exceeds
        // REFRESH_INLINE_MAX and exercises the inline safety net).
        for &d in &[1usize, 64, 1024] {
            group.bench_function(BenchmarkId::new("cached_dirty", d), |b| {
                // iter_batched_ref: the (expensive) drop of the cloned
                // store stays OUTSIDE the measurement.
                b.iter_batched_ref(
                    || {
                        let mut store = warm.clone();
                        for i in 0..d.min(n) {
                            if let Some(CrdtValue::Register(reg)) = store.get_mut(&key(i)) {
                                reg.set(format!("updated-{i}"), ts(n as u64 + i as u64 + 1));
                            }
                        }
                        store
                    },
                    |store| store.digest(),
                    BatchSize::LargeInput,
                )
            });
        }

        group.finish();
    }
}

criterion_group!(benches, bench_digest);
criterion_main!(benches);
