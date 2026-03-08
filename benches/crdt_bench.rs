//! Criterion benchmarks for CRDT types: PnCounter, OrSet, OrMap, LwwRegister.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use asteroidb_poc::crdt::lww_register::LwwRegister;
use asteroidb_poc::crdt::or_map::OrMap;
use asteroidb_poc::crdt::or_set::OrSet;
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::types::NodeId;

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

// ---------------------------------------------------------------------------
// PnCounter benchmarks
// ---------------------------------------------------------------------------

fn bench_pn_counter_increment(c: &mut Criterion) {
    let node_a = node("node-a");
    c.bench_function("pn_counter/increment", |b| {
        b.iter(|| {
            let mut counter = PnCounter::new();
            for _ in 0..100 {
                counter.increment(&node_a);
            }
            counter
        });
    });
}

fn bench_pn_counter_merge(c: &mut Criterion) {
    let node_a = node("node-a");
    let node_b = node("node-b");

    let mut counter_a = PnCounter::new();
    for _ in 0..1000 {
        counter_a.increment(&node_a);
    }

    let mut counter_b = PnCounter::new();
    for _ in 0..1000 {
        counter_b.increment(&node_b);
    }
    for _ in 0..500 {
        counter_b.decrement(&node_b);
    }

    c.bench_function("pn_counter/merge_2_replicas", |b| {
        b.iter(|| {
            let mut a = counter_a.clone();
            a.merge(&counter_b);
            a
        });
    });
}

// ---------------------------------------------------------------------------
// OrSet benchmarks
// ---------------------------------------------------------------------------

fn bench_or_set_add_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("or_set/add_merge");

    for size in [10, 100, 1000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let node_a = node("node-a");
            let node_b = node("node-b");

            // Build two sets with `size` elements each (disjoint).
            let mut set_a = OrSet::new();
            for i in 0..size {
                set_a.add(format!("a-{i}"), &node_a);
            }

            let mut set_b = OrSet::new();
            for i in 0..size {
                set_b.add(format!("b-{i}"), &node_b);
            }

            b.iter(|| {
                let mut a = set_a.clone();
                a.merge(&set_b);
                a
            });
        });
    }
    group.finish();
}

fn bench_or_set_add(c: &mut Criterion) {
    let mut group = c.benchmark_group("or_set/add");

    for size in [10, 100, 1000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let node_a = node("node-a");
            b.iter(|| {
                let mut set = OrSet::new();
                for i in 0..size {
                    set.add(format!("elem-{i}"), &node_a);
                }
                set
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// OrMap benchmarks
// ---------------------------------------------------------------------------

fn bench_or_map_put_merge(c: &mut Criterion) {
    let node_a = node("node-a");
    let node_b = node("node-b");

    let mut map_a: OrMap<String, String> = OrMap::new();
    for i in 0..100 {
        map_a.set(
            format!("key-{i}"),
            format!("val-a-{i}"),
            ts(1000 + i, 0, "node-a"),
            &node_a,
        );
    }

    let mut map_b: OrMap<String, String> = OrMap::new();
    for i in 0..100 {
        map_b.set(
            format!("key-{i}"),
            format!("val-b-{i}"),
            ts(2000 + i, 0, "node-b"),
            &node_b,
        );
    }

    c.bench_function("or_map/put_100_merge", |b| {
        b.iter(|| {
            let mut a = map_a.clone();
            a.merge(&map_b);
            a
        });
    });
}

// ---------------------------------------------------------------------------
// LwwRegister benchmarks
// ---------------------------------------------------------------------------

fn bench_lww_register_set_merge(c: &mut Criterion) {
    let mut reg_a = LwwRegister::new();
    reg_a.set("value-a".to_string(), ts(100, 0, "node-a"));

    let mut reg_b = LwwRegister::new();
    reg_b.set("value-b".to_string(), ts(200, 0, "node-b"));

    c.bench_function("lww_register/set_and_merge", |b| {
        b.iter(|| {
            let mut a = reg_a.clone();
            a.merge(&reg_b);
            a
        });
    });
}

fn bench_lww_register_set(c: &mut Criterion) {
    c.bench_function("lww_register/set_100", |b| {
        b.iter(|| {
            let mut reg = LwwRegister::new();
            for i in 0u64..100 {
                reg.set(format!("value-{i}"), ts(i, 0, "node-a"));
            }
            reg
        });
    });
}

criterion_group!(
    benches,
    bench_pn_counter_increment,
    bench_pn_counter_merge,
    bench_or_set_add,
    bench_or_set_add_merge,
    bench_or_map_put_merge,
    bench_lww_register_set,
    bench_lww_register_set_merge,
);
criterion_main!(benches);
