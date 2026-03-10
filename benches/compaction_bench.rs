//! Criterion benchmarks for the compaction engine and tombstone GC.
//!
//! Covers:
//! - CompactionEngine: record_op, should_checkpoint, create_checkpoint,
//!   is_compactable, run_compaction
//! - TombstoneGc: gc_tombstones at various store sizes
//! - AdaptiveCompactionConfig: tune cycle

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use asteroidb_poc::authority::ack_frontier::{AckFrontier, AckFrontierSet};
use asteroidb_poc::compaction::{AdaptiveCompactionConfig, CompactionConfig, CompactionEngine};
use asteroidb_poc::crdt::gc::TombstoneGc;
use asteroidb_poc::crdt::or_set::OrSet;
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::store::kv::{CrdtValue, Store};
use asteroidb_poc::types::{KeyRange, NodeId, PolicyVersion};

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

fn key_range(prefix: &str) -> KeyRange {
    KeyRange {
        prefix: prefix.into(),
    }
}

fn make_frontier(authority: &str, physical: u64, prefix: &str) -> AckFrontier {
    AckFrontier {
        authority_id: NodeId(authority.into()),
        frontier_hlc: ts(physical, 0, authority),
        key_range: key_range(prefix),
        policy_version: PolicyVersion(1),
        digest_hash: format!("{authority}-{physical}"),
    }
}

// ---------------------------------------------------------------------------
// CompactionEngine benchmarks
// ---------------------------------------------------------------------------

fn bench_record_op(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction/record_op");

    for n in [100, 1000, 10000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let mut engine = CompactionEngine::with_defaults();
                let kr = key_range("user/");
                for i in 0..n {
                    engine.record_op_at(&kr, 1000 + i as u64);
                }
                std::hint::black_box(&engine);
            });
        });
    }
    group.finish();
}

fn bench_should_checkpoint(c: &mut Criterion) {
    c.bench_function("compaction/should_checkpoint", |b| {
        let mut engine = CompactionEngine::with_defaults();
        let kr = key_range("user/");

        // Record some ops below threshold.
        for i in 0..5000 {
            engine.record_op_at(&kr, 1000 + i);
        }

        let now = ts(50_000, 0, "node-a");
        b.iter(|| {
            let result = engine.should_checkpoint(&kr, &now);
            std::hint::black_box(result);
        });
    });
}

fn bench_create_checkpoint(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction/create_checkpoint");

    for n in [10, 100, 500] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let mut engine = CompactionEngine::with_defaults();
                    let kr = key_range("user/");
                    for i in 0..1000 {
                        engine.record_op_at(&kr, 1000 + i);
                    }
                    engine
                },
                |mut engine| {
                    for i in 0..n {
                        engine.create_checkpoint(
                            key_range("user/"),
                            ts(10_000 + i * 1_000, 0, "node-a"),
                            format!("digest-{i}"),
                            PolicyVersion(1),
                        );
                    }
                    std::hint::black_box(&engine);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_is_compactable(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction/is_compactable");

    for n_authorities in [3, 5, 9] {
        let mut engine = CompactionEngine::with_defaults();
        let kr = key_range("user/");

        engine.create_checkpoint(
            kr.clone(),
            ts(100, 0, "node-a"),
            "hash".into(),
            PolicyVersion(1),
        );

        let mut frontiers = AckFrontierSet::new();
        for i in 0..n_authorities {
            frontiers.update(make_frontier(
                &format!("auth-{i}"),
                200 + i as u64 * 100,
                "user/",
            ));
        }

        group.bench_with_input(
            BenchmarkId::from_parameter(n_authorities),
            &n_authorities,
            |b, &n| {
                b.iter(|| {
                    let result = engine.is_compactable("user/", &frontiers, n);
                    std::hint::black_box(result);
                });
            },
        );
    }
    group.finish();
}

fn bench_run_compaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction/run_compaction");

    for n_keys in [100, 500, 1000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(n_keys),
            &n_keys,
            |b, &n_keys| {
                b.iter_batched(
                    || {
                        let mut engine = CompactionEngine::new(CompactionConfig {
                            time_threshold_ms: 30_000,
                            ops_threshold: 3,
                        });
                        let kr = key_range("user/");

                        // Record enough ops to trigger checkpoint.
                        for _ in 0..5 {
                            engine.record_op(&kr);
                        }

                        // Build a store with timestamps.
                        let nid = node("bench-node");
                        let mut store = Store::new();
                        for i in 0..n_keys {
                            let mut counter = PnCounter::new();
                            counter.increment(&nid);
                            store.put(format!("user/key-{i:06}"), CrdtValue::Counter(counter));
                            // Half before checkpoint, half after.
                            let phys = if i < n_keys / 2 { 50 } else { 200 };
                            store.record_change(
                                &format!("user/key-{i:06}"),
                                ts(phys, i as u32, "bench-node"),
                            );
                        }

                        // Build frontiers: all 3 authorities past t=100.
                        let mut frontiers = AckFrontierSet::new();
                        frontiers.update(make_frontier("auth-1", 200, "user/"));
                        frontiers.update(make_frontier("auth-2", 300, "user/"));
                        frontiers.update(make_frontier("auth-3", 150, "user/"));

                        (engine, store, frontiers)
                    },
                    |(mut engine, mut store, frontiers)| {
                        let pruned = engine.run_compaction(
                            &key_range("user/"),
                            ts(100, 0, "node-a"),
                            "digest-100".into(),
                            PolicyVersion(1),
                            &frontiers,
                            3,
                            &mut store,
                        );
                        std::hint::black_box(pruned);
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Adaptive compaction tuning benchmarks
// ---------------------------------------------------------------------------

fn bench_adaptive_tune(c: &mut Criterion) {
    c.bench_function("compaction/adaptive_tune", |b| {
        b.iter_batched(
            || {
                let base = CompactionConfig {
                    time_threshold_ms: 30_000,
                    ops_threshold: 10_000,
                };
                let mut adaptive = AdaptiveCompactionConfig::with_write_rate_window(base, 10_000);
                adaptive.set_tuning_interval_ms(0);
                // Record realistic write rate data.
                for i in 0..100 {
                    adaptive.record_ops("user/", 1_000 + i * 10, 50);
                }
                adaptive
            },
            |mut adaptive| {
                let changed = adaptive.tune(2_000, Some(5_000));
                std::hint::black_box(changed);
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_write_rate_tracker(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction/write_rate_tracker");

    for n_records in [100, 1000, 5000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(n_records),
            &n_records,
            |b, &n| {
                b.iter_batched(
                    || {
                        let base = CompactionConfig::default();
                        let mut adaptive =
                            AdaptiveCompactionConfig::with_write_rate_window(base, 60_000);
                        for i in 0..n {
                            adaptive.record_ops("user/", 1_000 + i as u64 * 10, 1);
                        }
                        adaptive
                    },
                    |adaptive| {
                        let rate = adaptive.write_rate("user/", 60_000);
                        std::hint::black_box(rate);
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// TombstoneGc benchmarks
// ---------------------------------------------------------------------------

fn bench_gc_tombstones(c: &mut Criterion) {
    let mut group = c.benchmark_group("gc/tombstones");

    for n_sets in [10, 50, 200] {
        group.bench_with_input(BenchmarkId::from_parameter(n_sets), &n_sets, |b, &n| {
            b.iter_batched(
                || {
                    let gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(0));
                    let mut store = Store::new();

                    // Create n OrSets, each with tombstones.
                    for i in 0..n {
                        let nid = node(&format!("node-{i}"));
                        let mut set = OrSet::new();
                        // Add, remove, then add again to advance counter.
                        set.add(format!("elem-{i}"), &nid);
                        set.remove(&format!("elem-{i}"));
                        set.add(format!("elem-{i}-new"), &nid);
                        store.put(format!("set-{i:04}"), CrdtValue::Set(set));
                    }
                    (gc, store)
                },
                |(mut gc, mut store)| {
                    let collected = gc.gc_tombstones(&mut store, 1_000);
                    std::hint::black_box(collected);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_gc_mixed_store(c: &mut Criterion) {
    c.bench_function("gc/mixed_store_100", |b| {
        b.iter_batched(
            || {
                let gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(0));
                let mut store = Store::new();
                let nid = node("bench-node");

                // 50 OrSets with tombstones.
                for i in 0..50 {
                    let mut set = OrSet::new();
                    set.add(format!("x-{i}"), &nid);
                    set.remove(&format!("x-{i}"));
                    set.add(format!("y-{i}"), &nid);
                    store.put(format!("set-{i:04}"), CrdtValue::Set(set));
                }

                // 50 counters (no tombstones — GC should skip these).
                for i in 0..50 {
                    let mut counter = PnCounter::new();
                    counter.increment(&nid);
                    store.put(format!("cnt-{i:04}"), CrdtValue::Counter(counter));
                }

                (gc, store)
            },
            |(mut gc, mut store)| {
                let collected = gc.gc_tombstones(&mut store, 1_000);
                std::hint::black_box(collected);
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_gc_with_version_floor(c: &mut Criterion) {
    c.bench_function("gc/with_version_floor_50", |b| {
        b.iter_batched(
            || {
                let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(0));
                let mut store = Store::new();

                // Create 50 OrSets with tombstones from different nodes.
                for i in 0..50 {
                    let nid = node(&format!("node-{i}"));
                    let mut set = OrSet::new();
                    set.add(format!("elem-{i}"), &nid);
                    set.remove(&format!("elem-{i}"));
                    set.add(format!("elem-{i}-new"), &nid);
                    store.put(format!("set-{i:04}"), CrdtValue::Set(set));
                    // Set version floor for each node.
                    gc.set_floor(&nid, 2);
                }

                (gc, store)
            },
            |(mut gc, mut store)| {
                let collected = gc.gc_tombstones(&mut store, 1_000);
                std::hint::black_box(collected);
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    benches,
    bench_record_op,
    bench_should_checkpoint,
    bench_create_checkpoint,
    bench_is_compactable,
    bench_run_compaction,
    bench_adaptive_tune,
    bench_write_rate_tracker,
    bench_gc_tombstones,
    bench_gc_mixed_store,
    bench_gc_with_version_floor,
);
criterion_main!(benches);
