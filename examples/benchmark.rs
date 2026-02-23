/// AsteroidDB Benchmark Suite
///
/// Measures three key performance indicators:
/// 1. Eventual write latency (PN-Counter increment)
/// 2. Certified write confirmation time (write -> certified via majority)
/// 3. Partition recovery convergence time (divergent -> converged via CRDT merge)
///
/// Run: `cargo run --example benchmark`
/// JSON output: `cargo run --example benchmark 2>/dev/null`
use std::time::Instant;

use std::sync::{Arc, RwLock};

use asteroidb_poc::api::certified::{CertifiedApi, OnTimeout};
use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::authority::ack_frontier::AckFrontier;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::ops::metrics::{BenchmarkResult, collect_latencies, csv_header, to_csv_row};
use asteroidb_poc::store::kv::CrdtValue;
use asteroidb_poc::types::{CertificationStatus, KeyRange, NodeId, PolicyVersion};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn node(name: &str) -> NodeId {
    NodeId(name.into())
}

fn kr(prefix: &str) -> KeyRange {
    KeyRange {
        prefix: prefix.into(),
    }
}

fn make_frontier(authority: &str, physical: u64, prefix: &str) -> AckFrontier {
    AckFrontier {
        authority_id: NodeId(authority.into()),
        frontier_hlc: HlcTimestamp {
            physical,
            logical: 0,
            node_id: authority.into(),
        },
        key_range: KeyRange {
            prefix: prefix.into(),
        },
        policy_version: PolicyVersion(1),
        digest_hash: format!("{authority}-{physical}"),
    }
}

fn default_namespace() -> Arc<RwLock<SystemNamespace>> {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: kr(""),
        authority_nodes: vec![node("auth-1"), node("auth-2"), node("auth-3")],
    });
    Arc::new(RwLock::new(ns))
}

fn counter_value(n: i64) -> CrdtValue {
    let mut counter = PnCounter::new();
    for _ in 0..n {
        counter.increment(&node("writer"));
    }
    CrdtValue::Counter(counter)
}

// ---------------------------------------------------------------------------
// Benchmark 1: Eventual write latency
// ---------------------------------------------------------------------------

fn bench_eventual_write_latency(iterations: usize) -> BenchmarkResult {
    eprintln!("[1/3] Measuring eventual write latency ({iterations} iterations)...");

    let mut api = EventualApi::new(node("bench-node"));
    let mut durations = Vec::with_capacity(iterations);

    for i in 0..iterations {
        let key = format!("bench/counter/{i}");
        let start = Instant::now();
        api.eventual_counter_inc(&key).unwrap();
        durations.push(start.elapsed());
    }

    let result = collect_latencies("eventual_write_latency", &durations);
    eprintln!(
        "       mean={:.2}us  p50={:.2}us  p95={:.2}us  p99={:.2}us",
        result.mean_us, result.p50_us, result.p95_us, result.p99_us
    );
    result
}

// ---------------------------------------------------------------------------
// Benchmark 2: Certified write -> certified confirmation time
// ---------------------------------------------------------------------------

fn bench_certified_confirmation_time(iterations: usize) -> BenchmarkResult {
    eprintln!("[2/3] Measuring certified confirmation time ({iterations} iterations)...");

    let mut durations = Vec::with_capacity(iterations);

    for i in 0..iterations {
        let mut api = CertifiedApi::new(node("bench-node"), default_namespace());

        let key = format!("bench/cert/{i}");

        // Start timing: certified_write + frontier updates + process_certifications
        let start = Instant::now();

        api.certified_write(key.clone(), counter_value(1), OnTimeout::Pending)
            .unwrap();

        let write_ts = api.pending_writes()[0].timestamp.physical;

        // Simulate majority of authorities acknowledging the write.
        api.update_frontier(make_frontier("auth-1", write_ts + 100, ""));
        api.update_frontier(make_frontier("auth-2", write_ts + 200, ""));

        api.process_certifications();

        let elapsed = start.elapsed();

        // Verify certification succeeded.
        assert_eq!(
            api.get_certification_status(&key),
            CertificationStatus::Certified,
            "write should be certified after majority frontier advancement"
        );

        durations.push(elapsed);
    }

    let result = collect_latencies("certified_confirmation_time", &durations);
    eprintln!(
        "       mean={:.2}us  p50={:.2}us  p95={:.2}us  p99={:.2}us",
        result.mean_us, result.p50_us, result.p95_us, result.p99_us
    );
    result
}

// ---------------------------------------------------------------------------
// Benchmark 3: Partition recovery convergence time
// ---------------------------------------------------------------------------

fn bench_recovery_convergence_time(iterations: usize) -> BenchmarkResult {
    eprintln!("[3/3] Measuring partition recovery convergence time ({iterations} iterations)...");

    let mut durations = Vec::with_capacity(iterations);

    for _ in 0..iterations {
        let node_a = node("node-A");
        let node_b = node("node-B");
        let node_c = node("node-C");

        let mut api_a = EventualApi::new(node_a);
        let mut api_b = EventualApi::new(node_b);
        let mut api_c = EventualApi::new(node_c);

        let key = "bench/recovery";

        // Phase 1: Pre-partition writes and full replication.
        for _ in 0..10 {
            api_a.eventual_counter_inc(key).unwrap();
        }
        for _ in 0..7 {
            api_b.eventual_counter_inc(key).unwrap();
        }
        for _ in 0..5 {
            api_c.eventual_counter_inc(key).unwrap();
        }

        // Full sync before partition.
        let val_a = api_a.get_eventual(key).unwrap().clone();
        let val_b = api_b.get_eventual(key).unwrap().clone();
        let val_c = api_c.get_eventual(key).unwrap().clone();
        api_a.merge_remote(key.into(), &val_b).unwrap();
        api_a.merge_remote(key.into(), &val_c).unwrap();
        api_b.merge_remote(key.into(), &val_a).unwrap();
        api_b.merge_remote(key.into(), &val_c).unwrap();
        api_c.merge_remote(key.into(), &val_a).unwrap();
        api_c.merge_remote(key.into(), &val_b).unwrap();

        // Phase 2: Partition -- node-C is isolated.
        for _ in 0..20 {
            api_a.eventual_counter_inc(key).unwrap();
        }
        for _ in 0..15 {
            api_b.eventual_counter_inc(key).unwrap();
        }
        for _ in 0..8 {
            api_c.eventual_counter_inc(key).unwrap();
        }

        // A and B sync with each other (but not C).
        let val_a = api_a.get_eventual(key).unwrap().clone();
        let val_b = api_b.get_eventual(key).unwrap().clone();
        api_a.merge_remote(key.into(), &val_b).unwrap();
        api_b.merge_remote(key.into(), &val_a).unwrap();

        // State is now divergent: A and B have (10+7+5+20+15)=57, C has (10+7+5+8)=30.

        // Phase 3: Recovery -- measure convergence time.
        let start = Instant::now();

        let val_a = api_a.get_eventual(key).unwrap().clone();
        let val_b = api_b.get_eventual(key).unwrap().clone();
        let val_c = api_c.get_eventual(key).unwrap().clone();

        api_a.merge_remote(key.into(), &val_c).unwrap();
        api_b.merge_remote(key.into(), &val_c).unwrap();
        api_c.merge_remote(key.into(), &val_a).unwrap();
        api_c.merge_remote(key.into(), &val_b).unwrap();

        // Also sync A <-> B to ensure they pick up C's partition writes.
        let val_a_final = api_a.get_eventual(key).unwrap().clone();
        let val_b_final = api_b.get_eventual(key).unwrap().clone();
        api_a.merge_remote(key.into(), &val_b_final).unwrap();
        api_b.merge_remote(key.into(), &val_a_final).unwrap();

        let elapsed = start.elapsed();

        // Verify all nodes converged to the same value.
        let expected = 10 + 7 + 5 + 20 + 15 + 8; // 65
        let get_val = |api: &EventualApi| match api.get_eventual(key).unwrap() {
            CrdtValue::Counter(c) => c.value(),
            _ => panic!("expected Counter"),
        };
        assert_eq!(get_val(&api_a), expected, "node-A should converge");
        assert_eq!(get_val(&api_b), expected, "node-B should converge");
        assert_eq!(get_val(&api_c), expected, "node-C should converge");

        durations.push(elapsed);
    }

    let result = collect_latencies("recovery_convergence_time", &durations);
    eprintln!(
        "       mean={:.2}us  p50={:.2}us  p95={:.2}us  p99={:.2}us",
        result.mean_us, result.p50_us, result.p95_us, result.p99_us
    );
    result
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    eprintln!("======================================================================");
    eprintln!("AsteroidDB Benchmark Suite");
    eprintln!("======================================================================");
    eprintln!();

    let results = vec![
        bench_eventual_write_latency(1000),
        bench_certified_confirmation_time(100),
        bench_recovery_convergence_time(100),
    ];

    eprintln!();
    eprintln!("----------------------------------------------------------------------");
    eprintln!("Results (CSV)");
    eprintln!("----------------------------------------------------------------------");
    eprintln!("{}", csv_header());
    for r in &results {
        eprintln!("{}", to_csv_row(r));
    }

    // JSON output to stdout for programmatic consumption.
    let json = serde_json::to_string_pretty(&results).expect("JSON serialization failed");
    println!("{json}");

    eprintln!();
    eprintln!("JSON results printed to stdout.");
    eprintln!("======================================================================");
}
