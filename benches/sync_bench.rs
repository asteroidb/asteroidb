//! Criterion benchmarks for delta sync serialization and deserialization.
//!
//! Covers:
//! - SyncRequest / DeltaSyncResponse serialization (JSON vs bincode)
//! - Deserialization round-trips at various payload sizes
//! - entries_since (delta extraction from Store)

use std::collections::HashMap;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::{Hlc, HlcTimestamp};
use asteroidb_poc::http::codec::{
    CONTENT_TYPE_BINCODE, CONTENT_TYPE_JSON, deserialize_internal, serialize_internal,
};
use asteroidb_poc::network::sync::{DeltaEntry, DeltaSyncResponse, SyncRequest};
use asteroidb_poc::store::kv::{CrdtValue, Store};
use asteroidb_poc::types::NodeId;

fn node(name: &str) -> NodeId {
    NodeId(name.into())
}

/// Build a SyncRequest with `n` counter entries.
fn build_sync_request(n: usize) -> SyncRequest {
    let nid = node("bench-node");
    let mut entries = HashMap::new();
    for i in 0..n {
        let key = format!("key-{i:06}");
        let mut counter = PnCounter::new();
        counter.increment(&nid);
        entries.insert(key, CrdtValue::Counter(counter));
    }
    SyncRequest {
        sender: "bench-node".into(),
        entries,
    }
}

/// Build a DeltaSyncResponse with `n` delta entries.
fn build_delta_response(n: usize) -> DeltaSyncResponse {
    let nid = node("bench-node");
    let mut entries = Vec::with_capacity(n);
    for i in 0..n {
        let mut counter = PnCounter::new();
        counter.increment(&nid);
        entries.push(DeltaEntry {
            key: format!("key-{i:06}"),
            value: CrdtValue::Counter(counter),
            hlc: HlcTimestamp {
                physical: 1_700_000_000_000 + i as u64,
                logical: 0,
                node_id: "bench-node".into(),
            },
        });
    }
    DeltaSyncResponse {
        entries,
        sender_frontier: Some(HlcTimestamp {
            physical: 1_700_000_000_000 + n as u64,
            logical: 0,
            node_id: "bench-node".into(),
        }),
    }
}

// ---------------------------------------------------------------------------
// SyncRequest serialization benchmarks (JSON vs bincode)
// ---------------------------------------------------------------------------

fn bench_sync_request_serialize_json(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync/serialize_json");

    for n in [10, 100, 1000] {
        let req = build_sync_request(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let (bytes, _) = serialize_internal(&req, Some(CONTENT_TYPE_JSON)).unwrap();
                std::hint::black_box(bytes.len());
            });
        });
    }
    group.finish();
}

fn bench_sync_request_serialize_bincode(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync/serialize_bincode");

    for n in [10, 100, 1000] {
        let req = build_sync_request(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let (bytes, _) = serialize_internal(&req, Some(CONTENT_TYPE_BINCODE)).unwrap();
                std::hint::black_box(bytes.len());
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// SyncRequest deserialization benchmarks (JSON vs bincode)
// ---------------------------------------------------------------------------

fn bench_sync_request_deserialize_json(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync/deserialize_json");

    for n in [10, 100, 1000] {
        let req = build_sync_request(n);
        let (bytes, _) = serialize_internal(&req, Some(CONTENT_TYPE_JSON)).unwrap();
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let decoded: SyncRequest =
                    deserialize_internal(&bytes, Some(CONTENT_TYPE_JSON)).unwrap();
                std::hint::black_box(decoded.entries.len());
            });
        });
    }
    group.finish();
}

fn bench_sync_request_deserialize_bincode(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync/deserialize_bincode");

    for n in [10, 100, 1000] {
        let req = build_sync_request(n);
        let (bytes, _) = serialize_internal(&req, Some(CONTENT_TYPE_BINCODE)).unwrap();
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let decoded: SyncRequest =
                    deserialize_internal(&bytes, Some(CONTENT_TYPE_BINCODE)).unwrap();
                std::hint::black_box(decoded.entries.len());
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// DeltaSyncResponse serialization benchmarks (JSON vs bincode)
// ---------------------------------------------------------------------------

fn bench_delta_response_serialize_json(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync/delta_serialize_json");

    for n in [10, 100, 500] {
        let resp = build_delta_response(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let (bytes, _) = serialize_internal(&resp, Some(CONTENT_TYPE_JSON)).unwrap();
                std::hint::black_box(bytes.len());
            });
        });
    }
    group.finish();
}

fn bench_delta_response_serialize_bincode(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync/delta_serialize_bincode");

    for n in [10, 100, 500] {
        let resp = build_delta_response(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let (bytes, _) = serialize_internal(&resp, Some(CONTENT_TYPE_BINCODE)).unwrap();
                std::hint::black_box(bytes.len());
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// DeltaSyncResponse deserialization benchmarks
// ---------------------------------------------------------------------------

fn bench_delta_response_deserialize_json(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync/delta_deserialize_json");

    for n in [10, 100, 500] {
        let resp = build_delta_response(n);
        let (bytes, _) = serialize_internal(&resp, Some(CONTENT_TYPE_JSON)).unwrap();
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let decoded: DeltaSyncResponse =
                    deserialize_internal(&bytes, Some(CONTENT_TYPE_JSON)).unwrap();
                std::hint::black_box(decoded.entries.len());
            });
        });
    }
    group.finish();
}

fn bench_delta_response_deserialize_bincode(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync/delta_deserialize_bincode");

    for n in [10, 100, 500] {
        let resp = build_delta_response(n);
        let (bytes, _) = serialize_internal(&resp, Some(CONTENT_TYPE_BINCODE)).unwrap();
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let decoded: DeltaSyncResponse =
                    deserialize_internal(&bytes, Some(CONTENT_TYPE_BINCODE)).unwrap();
                std::hint::black_box(decoded.entries.len());
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// entries_since (delta extraction from store)
// ---------------------------------------------------------------------------

fn bench_entries_since_delta(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync/entries_since");

    for (total, changed_pct) in [(1000, 1), (1000, 10), (5000, 5)] {
        let nid = node("bench-node");
        let mut store = Store::new();
        let mut clock = Hlc::new("bench-node".into());

        for i in 0..total {
            let key = format!("key-{i:06}");
            let mut counter = PnCounter::new();
            counter.increment(&nid);
            let ts = clock.now();
            store.put(key.clone(), CrdtValue::Counter(counter));
            store.record_change(&key, ts);
        }

        let frontier = store.current_frontier().unwrap();

        // Modify changed_pct% of keys.
        let changed = total * changed_pct / 100;
        for i in 0..changed {
            let key = format!("key-{i:06}");
            if let Some(CrdtValue::Counter(c)) = store.get_mut(&key) {
                c.increment(&nid);
            }
            let ts = clock.now();
            store.record_change(&key, ts);
        }

        let label = format!("{total}_keys_{changed_pct}pct");
        group.bench_with_input(BenchmarkId::from_parameter(&label), &label, |b, _| {
            b.iter(|| {
                let entries = store.entries_since(&frontier);
                std::hint::black_box(entries.len());
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Payload size comparison (JSON vs bincode)
// ---------------------------------------------------------------------------

fn bench_payload_size_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync/payload_size");

    for n in [10, 100, 1000] {
        let req = build_sync_request(n);

        let (json_bytes, _) = serialize_internal(&req, Some(CONTENT_TYPE_JSON)).unwrap();
        let (bincode_bytes, _) = serialize_internal(&req, Some(CONTENT_TYPE_BINCODE)).unwrap();

        // Benchmark both encode paths; the iteration cost reveals allocation overhead.
        group.bench_with_input(BenchmarkId::new("json", n), &req, |b, req| {
            b.iter(|| {
                let (bytes, _) = serialize_internal(req, Some(CONTENT_TYPE_JSON)).unwrap();
                std::hint::black_box(bytes.len());
            });
        });
        group.bench_with_input(BenchmarkId::new("bincode", n), &req, |b, req| {
            b.iter(|| {
                let (bytes, _) = serialize_internal(req, Some(CONTENT_TYPE_BINCODE)).unwrap();
                std::hint::black_box(bytes.len());
            });
        });

        // Print sizes for the profiling report.
        println!(
            "n={n}: JSON={} bytes, bincode={} bytes, ratio={:.2}x",
            json_bytes.len(),
            bincode_bytes.len(),
            json_bytes.len() as f64 / bincode_bytes.len() as f64
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_sync_request_serialize_json,
    bench_sync_request_serialize_bincode,
    bench_sync_request_deserialize_json,
    bench_sync_request_deserialize_bincode,
    bench_delta_response_serialize_json,
    bench_delta_response_serialize_bincode,
    bench_delta_response_deserialize_json,
    bench_delta_response_deserialize_bincode,
    bench_entries_since_delta,
    bench_payload_size_comparison,
);
criterion_main!(benches);
