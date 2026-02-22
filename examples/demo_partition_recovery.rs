/// Demo: Network Partition -> Recovery -> Certified Confirmation
///
/// Simulates a 3-node AsteroidDB cluster experiencing network partition
/// and recovery, demonstrating CRDT convergence and certified confirmation.
///
/// Run: `cargo run --example demo_partition_recovery`
use asteroidb_poc::api::certified::{CertifiedApi, OnTimeout};
use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::authority::ack_frontier::AckFrontier;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::store::kv::CrdtValue;
use asteroidb_poc::types::{CertificationStatus, KeyRange, NodeId, PolicyVersion};

fn separator() {
    println!("{}", "=".repeat(70));
}

fn sub_separator() {
    println!("{}", "-".repeat(50));
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

fn print_counter(label: &str, value: Option<&CrdtValue>) {
    match value {
        Some(CrdtValue::Counter(c)) => println!("  {label}: counter = {}", c.value()),
        None => println!("  {label}: (not found)"),
        Some(other) => println!("  {label}: unexpected type: {}", other.type_name()),
    }
}

fn assert_counter_value(api: &EventualApi, key: &str, expected: i64, node_name: &str) {
    match api.get_eventual(key) {
        Some(CrdtValue::Counter(c)) => {
            assert_eq!(
                c.value(),
                expected,
                "{node_name}: expected counter = {expected}, got {}",
                c.value()
            );
        }
        other => panic!("{node_name}: expected Counter, got {other:?}"),
    }
}

fn main() {
    separator();
    println!("AsteroidDB Demo: Partition -> Recovery -> Certified Confirmation");
    println!("Scenario: 3-node cluster with PN-Counter CRDT");
    separator();
    println!();

    // Step 1: Initialize 3-node cluster
    println!("STEP 1: Initialize 3-node cluster");
    sub_separator();
    let node_a = NodeId("node-A".into());
    let node_b = NodeId("node-B".into());
    let node_c = NodeId("node-C".into());
    let mut api_a = EventualApi::new(node_a.clone());
    let mut api_b = EventualApi::new(node_b.clone());
    let mut api_c = EventualApi::new(node_c.clone());
    let mut namespace = SystemNamespace::new();
    namespace.set_authority_definition(AuthorityDefinition {
        key_range: KeyRange {
            prefix: String::new(),
        },
        authority_nodes: vec![node_a.clone(), node_b.clone(), node_c.clone()],
    });
    let mut certified_api = CertifiedApi::new(node_a.clone(), namespace);
    println!("  Nodes: node-A, node-B, node-C");
    println!("  Authority set: all 3 nodes (majority = 2)");
    println!("  Key: \"sensor/temperature\" (PN-Counter)");
    println!();
    println!("  [Expected] All nodes start with empty stores.");
    println!();

    // Step 2: Eventual writes across all nodes (pre-partition)
    println!("STEP 2: Eventual writes across all nodes (pre-partition)");
    sub_separator();
    api_a.eventual_counter_inc("sensor/temperature").unwrap();
    api_a.eventual_counter_inc("sensor/temperature").unwrap();
    api_a.eventual_counter_inc("sensor/temperature").unwrap();
    println!("  node-A: incremented counter 3 times");
    api_b.eventual_counter_inc("sensor/temperature").unwrap();
    api_b.eventual_counter_inc("sensor/temperature").unwrap();
    println!("  node-B: incremented counter 2 times");
    api_c.eventual_counter_inc("sensor/temperature").unwrap();
    println!("  node-C: incremented counter 1 time");
    println!();
    println!("  Local state BEFORE merge propagation:");
    print_counter("node-A", api_a.get_eventual("sensor/temperature"));
    print_counter("node-B", api_b.get_eventual("sensor/temperature"));
    print_counter("node-C", api_c.get_eventual("sensor/temperature"));
    let val_a = api_a.get_eventual("sensor/temperature").unwrap().clone();
    let val_b = api_b.get_eventual("sensor/temperature").unwrap().clone();
    let val_c = api_c.get_eventual("sensor/temperature").unwrap().clone();
    api_a
        .merge_remote("sensor/temperature".into(), &val_b)
        .unwrap();
    api_a
        .merge_remote("sensor/temperature".into(), &val_c)
        .unwrap();
    api_b
        .merge_remote("sensor/temperature".into(), &val_a)
        .unwrap();
    api_b
        .merge_remote("sensor/temperature".into(), &val_c)
        .unwrap();
    api_c
        .merge_remote("sensor/temperature".into(), &val_a)
        .unwrap();
    api_c
        .merge_remote("sensor/temperature".into(), &val_b)
        .unwrap();
    println!();
    println!("  After merge propagation (full replication):");
    print_counter("node-A", api_a.get_eventual("sensor/temperature"));
    print_counter("node-B", api_b.get_eventual("sensor/temperature"));
    print_counter("node-C", api_c.get_eventual("sensor/temperature"));
    println!();
    println!("  [Expected] All nodes converge to counter = 6 (3 + 2 + 1).");
    assert_counter_value(&api_a, "sensor/temperature", 6, "node-A");
    assert_counter_value(&api_b, "sensor/temperature", 6, "node-B");
    assert_counter_value(&api_c, "sensor/temperature", 6, "node-C");
    println!("  [OK] All nodes converged to 6.");
    println!();

    // Step 3: Network partition -- isolate node-C
    println!("STEP 3: Network partition -- isolate node-C");
    sub_separator();
    println!("  node-C is now PARTITIONED (no merge propagation).");
    println!("  node-A and node-B remain connected.");
    println!();

    // Step 4: Continued writes during partition
    println!("STEP 4: Continued writes during partition");
    sub_separator();
    for _ in 0..5 {
        api_a.eventual_counter_inc("sensor/temperature").unwrap();
    }
    println!("  node-A: incremented counter 5 more times");
    for _ in 0..3 {
        api_b.eventual_counter_inc("sensor/temperature").unwrap();
    }
    println!("  node-B: incremented counter 3 more times");
    api_c.eventual_counter_inc("sensor/temperature").unwrap();
    api_c.eventual_counter_inc("sensor/temperature").unwrap();
    println!("  node-C (partitioned): incremented counter 2 more times");
    let val_a = api_a.get_eventual("sensor/temperature").unwrap().clone();
    let val_b = api_b.get_eventual("sensor/temperature").unwrap().clone();
    api_a
        .merge_remote("sensor/temperature".into(), &val_b)
        .unwrap();
    api_b
        .merge_remote("sensor/temperature".into(), &val_a)
        .unwrap();
    println!();
    println!("  State during partition:");
    print_counter(
        "node-A (connected)",
        api_a.get_eventual("sensor/temperature"),
    );
    print_counter(
        "node-B (connected)",
        api_b.get_eventual("sensor/temperature"),
    );
    print_counter(
        "node-C (partitioned)",
        api_c.get_eventual("sensor/temperature"),
    );
    println!();
    println!("  [Expected] node-A and node-B see 6 + 5 + 3 = 14.");
    println!("  [Expected] node-C sees only its own updates: 6 + 2 = 8.");
    println!("  [Expected] State is DIVERGENT -- node-C is behind.");
    assert_counter_value(&api_a, "sensor/temperature", 14, "node-A");
    assert_counter_value(&api_b, "sensor/temperature", 14, "node-B");
    assert_counter_value(&api_c, "sensor/temperature", 8, "node-C");
    println!("  [OK] Divergent state confirmed: A=14, B=14, C=8.");
    println!();

    // Step 5: Partition recovery -- CRDT merge convergence
    println!("STEP 5: Partition recovery -- CRDT merge convergence");
    sub_separator();
    println!("  node-C reconnects. Merge propagation resumes.");
    println!();
    let val_a = api_a.get_eventual("sensor/temperature").unwrap().clone();
    let val_b = api_b.get_eventual("sensor/temperature").unwrap().clone();
    let val_c = api_c.get_eventual("sensor/temperature").unwrap().clone();
    api_a
        .merge_remote("sensor/temperature".into(), &val_b)
        .unwrap();
    api_a
        .merge_remote("sensor/temperature".into(), &val_c)
        .unwrap();
    api_b
        .merge_remote("sensor/temperature".into(), &val_a)
        .unwrap();
    api_b
        .merge_remote("sensor/temperature".into(), &val_c)
        .unwrap();
    api_c
        .merge_remote("sensor/temperature".into(), &val_a)
        .unwrap();
    api_c
        .merge_remote("sensor/temperature".into(), &val_b)
        .unwrap();
    println!("  After recovery merge propagation:");
    print_counter("node-A", api_a.get_eventual("sensor/temperature"));
    print_counter("node-B", api_b.get_eventual("sensor/temperature"));
    print_counter("node-C", api_c.get_eventual("sensor/temperature"));
    println!();
    println!("  [Expected] All nodes converge to 16 (3+2+1 + 5+3+2).");
    println!("  CRDT PN-Counter merge is commutative, associative, idempotent.");
    assert_counter_value(&api_a, "sensor/temperature", 16, "node-A");
    assert_counter_value(&api_b, "sensor/temperature", 16, "node-B");
    assert_counter_value(&api_c, "sensor/temperature", 16, "node-C");
    println!("  [OK] All nodes converged to 16. CRDT convergence confirmed.");
    println!();

    // Step 6: Authority consensus -> certified confirmation
    println!("STEP 6: Authority consensus -> certified confirmation");
    sub_separator();
    let mut counter_for_cert = PnCounter::new();
    for _ in 0..16 {
        counter_for_cert.increment(&node_a);
    }
    let cert_result = certified_api.certified_write(
        "sensor/temperature".into(),
        CrdtValue::Counter(counter_for_cert),
        OnTimeout::Pending,
    );
    match &cert_result {
        Ok(status) => println!("  certified_write issued. Status: {status:?}"),
        Err(e) => println!("  certified_write error: {e}"),
    }
    let write_ts = certified_api.pending_writes()[0].timestamp.physical;
    println!("  Write timestamp (physical): {write_ts}");
    println!();
    println!("  Simulating Authority ack_frontier updates...");

    // 1 of 3 authorities -- not enough for majority
    certified_api.update_frontier(make_frontier("node-A", write_ts + 100, ""));
    certified_api.process_certifications();
    let status_1 = certified_api.get_certification_status("sensor/temperature");
    println!("  After 1/3 authority acks: status = {status_1:?}");
    println!("  [Expected] Status = Pending (majority not yet reached).");
    assert_eq!(status_1, CertificationStatus::Pending);
    println!("  [OK] Status is Pending.");

    // 2 of 3 authorities -- majority reached
    certified_api.update_frontier(make_frontier("node-B", write_ts + 200, ""));
    certified_api.process_certifications();
    let status_2 = certified_api.get_certification_status("sensor/temperature");
    println!();
    println!("  After 2/3 authority acks: status = {status_2:?}");
    println!("  [Expected] Status = Certified (majority reached: 2/3).");
    assert_eq!(status_2, CertificationStatus::Certified);
    println!("  [OK] Status is Certified!");

    // 3 of 3 authorities -- all confirmed
    certified_api.update_frontier(make_frontier("node-C", write_ts + 300, ""));
    certified_api.process_certifications();
    let status_3 = certified_api.get_certification_status("sensor/temperature");
    println!();
    println!("  After 3/3 authority acks: status = {status_3:?}");
    println!("  [Expected] Status = Certified (all authorities confirmed).");
    assert_eq!(status_3, CertificationStatus::Certified);
    println!("  [OK] Status remains Certified.");

    // Certified read verification
    let certified_read = certified_api.get_certified("sensor/temperature");
    println!();
    println!("  Certified read result:");
    println!("    value present: {}", certified_read.value.is_some());
    println!("    status: {:?}", certified_read.status);
    println!(
        "    frontier: {:?}",
        certified_read.frontier.as_ref().map(|f| f.physical)
    );
    println!("  [Expected] value present, status = Certified, frontier is Some.");
    assert!(certified_read.value.is_some());
    assert_eq!(certified_read.status, CertificationStatus::Certified);
    assert!(certified_read.frontier.is_some());
    println!("  [OK] Certified read confirmed.");
    println!();

    // Step 7: Summary
    separator();
    println!("SUMMARY");
    separator();
    println!("  1. 3-node cluster initialized with PN-Counter CRDT.");
    println!("  2. Pre-partition writes: all nodes converged to 6.");
    println!("  3. node-C partitioned (merge propagation stopped).");
    println!("  4. Writes during partition: A=14, B=14, C=8 (divergent).");
    println!("  5. Partition recovered: all nodes converged to 16 via CRDT merge.");
    println!("  6. Authority majority consensus reached: status = Certified.");
    println!();
    println!("  AsteroidDB guarantees:");
    println!("  - Eventual consistency: CRDT merge ensures convergence after partition.");
    println!("  - Certified consistency: Authority majority confirms data integrity.");
    println!("  - No data loss: all writes (including during partition) are preserved.");
    separator();
    println!("Demo completed successfully.");
}
