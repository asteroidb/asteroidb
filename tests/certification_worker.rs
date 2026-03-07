//! Certification worker E2E tests (Issue #80).
//!
//! Validates that the automatic certification pipeline in [`NodeRunner`]
//! drives pending writes to completion without manual intervention:
//!
//! 1. **3-Authority auto-certification**: a `certified_write` on an authority
//!    node is automatically certified by the frontier auto-report pipeline.
//! 2. **Timeout detection**: stale pending writes are promoted to `Timeout`
//!    during the certification tick.
//! 3. **Rejected transition**: writes can be explicitly rejected and the
//!    status is observable.
//! 4. **Status tracking**: certification status transitions are observable
//!    through the `CertifiedApi` and `CertificationTracker` APIs.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use asteroidb_poc::api::certified::{CertifiedApi, OnTimeout, RetentionPolicy};
use asteroidb_poc::api::status::{CertificationTracker, WriteId};
use asteroidb_poc::authority::ack_frontier::AckFrontier;
use asteroidb_poc::authority::frontier_reporter::FrontierReporter;
use asteroidb_poc::compaction::CompactionEngine;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::ops::metrics::RuntimeMetrics;
use asteroidb_poc::runtime::{NodeRunner, NodeRunnerConfig};
use asteroidb_poc::store::kv::CrdtValue;
use asteroidb_poc::types::{CertificationStatus, KeyRange, NodeId, PolicyVersion};
use tokio::sync::Mutex;

fn wrap_ns(ns: SystemNamespace) -> Arc<RwLock<SystemNamespace>> {
    Arc::new(RwLock::new(ns))
}

fn node_id(s: &str) -> NodeId {
    NodeId(s.into())
}

fn kr(prefix: &str) -> KeyRange {
    KeyRange {
        prefix: prefix.into(),
    }
}

fn counter_value(n: i64) -> CrdtValue {
    let mut counter = PnCounter::new();
    for _ in 0..n {
        counter.increment(&node_id("writer"));
    }
    CrdtValue::Counter(counter)
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

/// Build a 3-authority namespace with a catch-all key range.
fn three_authority_namespace() -> SystemNamespace {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: kr(""),
        authority_nodes: vec![node_id("auth-1"), node_id("auth-2"), node_id("auth-3")],
        auto_generated: false,
    });
    ns
}

fn three_authority_namespace_shared() -> Arc<RwLock<SystemNamespace>> {
    wrap_ns(three_authority_namespace())
}

/// Fast runner config with short intervals for testing.
fn fast_config() -> NodeRunnerConfig {
    NodeRunnerConfig {
        certification_interval: Duration::from_millis(10),
        cleanup_interval: Duration::from_secs(60),
        compaction_check_interval: Duration::from_secs(60),
        frontier_report_interval: Duration::from_millis(10),
        sync_interval: None,
    }
}

// ---------------------------------------------------------------
// Test 1: 3 Authority auto-certification E2E
// ---------------------------------------------------------------

/// In a 3-authority system, a `certified_write` should reach `Certified`
/// once a majority (2 of 3) of authority frontier reporters advance past
/// the write timestamp. This test simulates 3 authorities generating
/// frontier reports via `FrontierReporter`, feeds them to a client node
/// holding the pending write, and verifies automatic certification.
#[tokio::test]
async fn three_authority_auto_certification() {
    let ns = three_authority_namespace();

    // Create frontier reporters for all 3 authorities.
    let reporter1 = FrontierReporter::new(node_id("auth-1"), &ns);
    let reporter2 = FrontierReporter::new(node_id("auth-2"), &ns);
    let reporter3 = FrontierReporter::new(node_id("auth-3"), &ns);

    assert!(reporter1.is_authority());
    assert!(reporter2.is_authority());
    assert!(reporter3.is_authority());

    // Client node: writes a pending entry.
    let mut client_api = CertifiedApi::new(node_id("client"), wrap_ns(ns));
    client_api
        .certified_write("sensor/temp".into(), counter_value(42), OnTimeout::Pending)
        .unwrap();
    assert_eq!(
        client_api.get_certification_status("sensor/temp"),
        CertificationStatus::Pending
    );

    let write_ts = &client_api.pending_writes()[0].timestamp;

    // Each authority generates a frontier report at a timestamp guaranteed
    // to be after the write. We use the write timestamp + 100ms to ensure
    // the frontier strictly covers the write.
    let frontier_ts = HlcTimestamp {
        physical: write_ts.physical + 100,
        logical: 0,
        node_id: "frontier-gen".into(),
    };

    let frontiers1 = reporter1.report_frontiers_at(&frontier_ts);
    let frontiers2 = reporter2.report_frontiers_at(&frontier_ts);
    let frontiers3 = reporter3.report_frontiers_at(&frontier_ts);

    // Apply frontiers from all 3 authorities to the client.
    for f in frontiers1 {
        client_api.update_frontier(f);
    }
    for f in frontiers2 {
        client_api.update_frontier(f);
    }
    for f in frontiers3 {
        client_api.update_frontier(f);
    }

    // Process certifications on the client side.
    client_api.process_certifications();

    // The write should now be certified because all 3 authorities
    // have reported frontiers past the write timestamp.
    assert_eq!(
        client_api.get_certification_status("sensor/temp"),
        CertificationStatus::Certified,
        "certified_write should auto-certify when 3-authority frontiers have advanced"
    );
}

/// Full NodeRunner-based E2E: an authority node running a NodeRunner
/// with its own frontier reporter auto-certifies a pending write.
/// Uses 3-authority namespace but only one authority node runs locally.
/// The other 2 authorities' frontiers are injected manually to simulate
/// the network sync that would deliver them.
#[tokio::test]
async fn three_authority_node_runner_certification() {
    let ns = three_authority_namespace_shared();

    // auth-1 is an authority node that also has the pending write.
    let mut api = CertifiedApi::new(node_id("auth-1"), ns);
    api.certified_write("data/val".into(), counter_value(5), OnTimeout::Pending)
        .unwrap();
    let write_ts = api.pending_writes()[0].timestamp.physical;

    // Inject frontiers for auth-2 and auth-3 (simulating network sync).
    // These are past the write timestamp.
    api.update_frontier(make_frontier("auth-2", write_ts + 500, ""));
    api.update_frontier(make_frontier("auth-3", write_ts + 600, ""));

    // auth-1's own frontier will be auto-reported by the NodeRunner.
    let shared_api = Arc::new(Mutex::new(api));
    let engine = CompactionEngine::with_defaults();
    let mut runner = NodeRunner::new(
        node_id("auth-1"),
        shared_api.clone(),
        engine,
        fast_config(),
        Arc::new(RuntimeMetrics::default()),
    )
    .await;
    assert!(runner.is_authority());

    let handle = runner.shutdown_handle();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = handle.send(true);
    });

    runner.run().await;

    // auth-1 frontier should have been auto-reported, giving 3/3 authorities.
    // But only 2/3 are needed for majority. The write should be certified.
    let api = shared_api.lock().await;
    assert_eq!(
        api.pending_writes()[0].status,
        CertificationStatus::Certified,
        "write should auto-certify in 3-authority NodeRunner setup"
    );
}

// ---------------------------------------------------------------
// Test 2: Single authority node auto-certifies its own write
// ---------------------------------------------------------------

/// When an authority node writes locally and runs its own frontier
/// reporter, the write should auto-certify without external intervention.
/// This uses a 1-authority setup for simplicity.
#[tokio::test]
async fn single_authority_self_certification() {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: kr(""),
        authority_nodes: vec![node_id("auth-1")],
        auto_generated: false,
    });

    let mut api = CertifiedApi::new(node_id("auth-1"), wrap_ns(ns));
    api.certified_write("key1".into(), counter_value(10), OnTimeout::Pending)
        .unwrap();
    assert_eq!(
        api.get_certification_status("key1"),
        CertificationStatus::Pending
    );

    let shared_api = Arc::new(Mutex::new(api));
    let engine = CompactionEngine::with_defaults();
    let mut runner = NodeRunner::new(
        node_id("auth-1"),
        shared_api.clone(),
        engine,
        fast_config(),
        Arc::new(RuntimeMetrics::default()),
    )
    .await;
    let handle = runner.shutdown_handle();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = handle.send(true);
    });

    let stats = runner.run().await;

    assert!(
        stats.frontier_report_ticks >= 1,
        "authority should have reported frontiers"
    );
    assert!(
        stats.certification_ticks >= 1,
        "certification ticks should have fired"
    );

    let api = shared_api.lock().await;
    assert_eq!(
        api.pending_writes()[0].status,
        CertificationStatus::Certified,
        "write should auto-certify on authority node"
    );
}

// ---------------------------------------------------------------
// Test 3: Timeout auto-detection during certification tick
// ---------------------------------------------------------------

/// Pending writes that exceed the retention policy's `max_age_ms` should
/// be automatically marked as `Timeout` during the certification tick.
#[tokio::test]
async fn timeout_auto_detection() {
    let retention = RetentionPolicy {
        max_age_ms: 10, // Very short TTL for test.
        max_entries: 10_000,
    };
    let ns = three_authority_namespace_shared();
    let mut api = CertifiedApi::with_retention(node_id("node-1"), ns, retention);

    api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
        .unwrap();
    assert_eq!(
        api.get_certification_status("key1"),
        CertificationStatus::Pending
    );

    let shared_api = Arc::new(Mutex::new(api));
    let engine = CompactionEngine::with_defaults();
    let config = NodeRunnerConfig {
        certification_interval: Duration::from_millis(10),
        cleanup_interval: Duration::from_secs(60),
        compaction_check_interval: Duration::from_secs(60),
        frontier_report_interval: Duration::from_secs(60),
        sync_interval: None,
    };

    let mut runner = NodeRunner::new(
        node_id("node-1"),
        shared_api.clone(),
        engine,
        config,
        Arc::new(RuntimeMetrics::default()),
    )
    .await;
    let handle = runner.shutdown_handle();

    // Run long enough for the write to age past max_age_ms (10ms).
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(80)).await;
        let _ = handle.send(true);
    });

    runner.run().await;

    // The write should be marked as Timeout because the certification tick
    // detected that it exceeded max_age_ms.
    let api = shared_api.lock().await;
    assert_eq!(
        api.pending_writes()[0].status,
        CertificationStatus::Timeout,
        "stale write should auto-transition to Timeout"
    );
}

// ---------------------------------------------------------------
// Test 4: Rejected transition
// ---------------------------------------------------------------

/// Writes can be explicitly rejected via `reject_write`, and this
/// status is observable through the API.
#[tokio::test]
async fn rejected_transition_observable() {
    let ns = three_authority_namespace_shared();
    let mut api = CertifiedApi::new(node_id("node-1"), Arc::clone(&ns));

    api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
        .unwrap();
    assert_eq!(
        api.get_certification_status("key1"),
        CertificationStatus::Pending
    );

    // Reject the write.
    let rejected = api.reject_write("key1");
    assert!(
        rejected,
        "reject_write should return true for pending write"
    );

    assert_eq!(
        api.get_certification_status("key1"),
        CertificationStatus::Rejected,
        "write status should be Rejected after reject_write"
    );

    // Reject on already-rejected write should be a no-op.
    let rejected_again = api.reject_write("key1");
    assert!(
        !rejected_again,
        "reject_write on non-pending should return false"
    );
}

// ---------------------------------------------------------------
// Test 5: Status API tracks transitions via CertificationTracker
// ---------------------------------------------------------------

/// The `CertificationTracker` mirrors the certification flow and
/// transitions are observable: Pending -> Certified, Pending -> Timeout,
/// Pending -> Rejected.
#[tokio::test]
async fn status_tracker_mirrors_certification_flow() {
    let mut tracker = CertificationTracker::with_timeout(100); // 100ms timeout

    // Register a write.
    let wid1 = WriteId {
        key: "key1".into(),
        timestamp: HlcTimestamp {
            physical: 1000,
            logical: 0,
            node_id: "node-1".into(),
        },
    };
    tracker.register_write(wid1.clone(), 2, wid1.timestamp.clone());

    // Initially pending.
    assert_eq!(
        tracker.get_status(&wid1),
        Some(CertificationStatus::Pending)
    );
    assert_eq!(tracker.pending_count(), 1);

    // Record ack from auth-1.
    tracker.record_ack(
        &wid1,
        node_id("auth-1"),
        HlcTimestamp {
            physical: 1001,
            logical: 0,
            node_id: "auth-1".into(),
        },
    );
    assert_eq!(
        tracker.get_status(&wid1),
        Some(CertificationStatus::Pending),
        "1 ack out of 2 required: still pending"
    );

    // Record ack from auth-2 -> certified.
    tracker.record_ack(
        &wid1,
        node_id("auth-2"),
        HlcTimestamp {
            physical: 1002,
            logical: 0,
            node_id: "auth-2".into(),
        },
    );
    assert_eq!(
        tracker.get_status(&wid1),
        Some(CertificationStatus::Certified),
        "2 acks out of 2 required: certified"
    );

    // Register a second write that will time out.
    let wid2 = WriteId {
        key: "key2".into(),
        timestamp: HlcTimestamp {
            physical: 2000,
            logical: 0,
            node_id: "node-1".into(),
        },
    };
    tracker.register_write(wid2.clone(), 3, wid2.timestamp.clone());

    // Check timeouts at a time > created_at + 100ms.
    tracker.check_timeouts(&HlcTimestamp {
        physical: 2200,
        logical: 0,
        node_id: "node-1".into(),
    });
    assert_eq!(
        tracker.get_status(&wid2),
        Some(CertificationStatus::Timeout),
        "write should time out after 100ms"
    );

    // Register a third write that will be rejected.
    let wid3 = WriteId {
        key: "key3".into(),
        timestamp: HlcTimestamp {
            physical: 3000,
            logical: 0,
            node_id: "node-1".into(),
        },
    };
    tracker.register_write(wid3.clone(), 2, wid3.timestamp.clone());
    tracker.reject(
        &wid3,
        HlcTimestamp {
            physical: 3001,
            logical: 0,
            node_id: "node-1".into(),
        },
    );
    assert_eq!(
        tracker.get_status(&wid3),
        Some(CertificationStatus::Rejected),
        "write should be rejected"
    );
}

// ---------------------------------------------------------------
// Test 6: 3-Authority with manual frontier injection
// ---------------------------------------------------------------

/// Simulates the full flow without NodeRunner: write -> inject 2/3
/// authority frontiers -> process_certifications -> Certified.
/// Verifies the majority condition precisely.
#[tokio::test]
async fn majority_certification_requires_two_of_three() {
    let ns = three_authority_namespace_shared();
    let mut api = CertifiedApi::new(node_id("node-1"), ns);

    api.certified_write("data/x".into(), counter_value(1), OnTimeout::Pending)
        .unwrap();
    let write_ts = api.pending_writes()[0].timestamp.physical;

    // 1 of 3: not enough.
    api.update_frontier(make_frontier("auth-1", write_ts + 100, ""));
    api.process_certifications();
    assert_eq!(
        api.get_certification_status("data/x"),
        CertificationStatus::Pending,
        "1/3 authorities: should stay pending"
    );

    // 2 of 3: majority reached.
    api.update_frontier(make_frontier("auth-2", write_ts + 200, ""));
    api.process_certifications();
    assert_eq!(
        api.get_certification_status("data/x"),
        CertificationStatus::Certified,
        "2/3 authorities: should be certified"
    );
}

// ---------------------------------------------------------------
// Test 7: Multiple writes with mixed outcomes
// ---------------------------------------------------------------

/// Multiple writes in flight: one gets certified, one times out,
/// one gets rejected. All transitions are observable.
/// Uses separate key ranges so that frontier advancement for one range
/// does not inadvertently certify writes in another range.
#[tokio::test]
async fn mixed_outcomes_all_observable() {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: kr("cert/"),
        authority_nodes: vec![node_id("auth-1"), node_id("auth-2"), node_id("auth-3")],
        auto_generated: false,
    });
    ns.set_authority_definition(AuthorityDefinition {
        key_range: kr("stale/"),
        authority_nodes: vec![node_id("auth-s1"), node_id("auth-s2"), node_id("auth-s3")],
        auto_generated: false,
    });
    ns.set_authority_definition(AuthorityDefinition {
        key_range: kr("rej/"),
        authority_nodes: vec![node_id("auth-r1"), node_id("auth-r2"), node_id("auth-r3")],
        auto_generated: false,
    });

    let retention = RetentionPolicy {
        max_age_ms: 5_000,
        max_entries: 10_000,
    };
    let mut api = CertifiedApi::with_retention(node_id("node-1"), wrap_ns(ns), retention);

    // Write 3 keys to different ranges.
    api.certified_write("cert/key".into(), counter_value(1), OnTimeout::Pending)
        .unwrap();
    let ts_cert = api.pending_writes()[0].timestamp.physical;

    api.certified_write("stale/key".into(), counter_value(2), OnTimeout::Pending)
        .unwrap();

    api.certified_write("rej/key".into(), counter_value(3), OnTimeout::Pending)
        .unwrap();

    // Certify cert/key via frontier advancement (cert/ range authorities).
    api.update_frontier(make_frontier("auth-1", ts_cert + 100, "cert/"));
    api.update_frontier(make_frontier("auth-2", ts_cert + 200, "cert/"));

    // Reject rej/key explicitly.
    api.reject_write("rej/key");

    // Process with a timestamp far enough for stale/key to expire.
    api.process_certifications_with_timeout(ts_cert + 10_000);

    assert_eq!(
        api.get_certification_status("cert/key"),
        CertificationStatus::Certified
    );
    assert_eq!(
        api.get_certification_status("stale/key"),
        CertificationStatus::Timeout
    );
    assert_eq!(
        api.get_certification_status("rej/key"),
        CertificationStatus::Rejected
    );
}

// ---------------------------------------------------------------
// Test 8: Cleanup removes completed entries after certification
// ---------------------------------------------------------------

/// After auto-certification, cleanup should remove resolved entries
/// and leave only pending ones.
/// Uses separate key ranges so that frontier advancement for one range
/// does not certify writes in the other.
#[tokio::test]
async fn cleanup_after_auto_certification() {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: kr("a/"),
        authority_nodes: vec![node_id("auth-1"), node_id("auth-2"), node_id("auth-3")],
        auto_generated: false,
    });
    ns.set_authority_definition(AuthorityDefinition {
        key_range: kr("b/"),
        authority_nodes: vec![node_id("auth-b1"), node_id("auth-b2"), node_id("auth-b3")],
        auto_generated: false,
    });

    let mut api = CertifiedApi::new(node_id("node-1"), wrap_ns(ns));

    api.certified_write("a/key1".into(), counter_value(1), OnTimeout::Pending)
        .unwrap();
    api.certified_write("b/key2".into(), counter_value(2), OnTimeout::Pending)
        .unwrap();

    let ts = api.pending_writes()[0].timestamp.physical;

    // Certify a/key1 only (advance a/ range authorities).
    api.update_frontier(make_frontier("auth-1", ts + 100, "a/"));
    api.update_frontier(make_frontier("auth-2", ts + 100, "a/"));
    api.process_certifications();

    assert_eq!(api.pending_writes().len(), 2);
    assert_eq!(
        api.get_certification_status("a/key1"),
        CertificationStatus::Certified
    );
    assert_eq!(
        api.get_certification_status("b/key2"),
        CertificationStatus::Pending
    );

    // Full cleanup removes certified entries.
    api.cleanup(ts + 100);

    // Only b/key2 (still pending) should remain.
    let remaining: Vec<&str> = api
        .pending_writes()
        .iter()
        .map(|pw| pw.key.as_str())
        .collect();
    assert!(
        remaining.contains(&"b/key2"),
        "b/key2 should still be pending"
    );
    assert!(
        !remaining.contains(&"a/key1"),
        "a/key1 should have been cleaned up"
    );
}

// ---------------------------------------------------------------
// Test 9: Status tracker persistence across certification
// ---------------------------------------------------------------

/// The CertificationTracker can be serialized/deserialized while
/// certification is in progress, and the restored tracker continues
/// correctly.
#[tokio::test]
async fn tracker_persistence_during_certification() {
    let mut tracker = CertificationTracker::new();

    let wid = WriteId {
        key: "persistent-key".into(),
        timestamp: HlcTimestamp {
            physical: 5000,
            logical: 0,
            node_id: "node-1".into(),
        },
    };
    tracker.register_write(wid.clone(), 2, wid.timestamp.clone());

    // Partial ack.
    tracker.record_ack(
        &wid,
        node_id("auth-1"),
        HlcTimestamp {
            physical: 5001,
            logical: 0,
            node_id: "auth-1".into(),
        },
    );
    assert_eq!(tracker.get_status(&wid), Some(CertificationStatus::Pending));

    // Serialize.
    let json = tracker.to_json().expect("serialize");

    // Restore.
    let mut restored = CertificationTracker::from_json(&json).expect("deserialize");

    // Verify state survived.
    assert_eq!(
        restored.get_status(&wid),
        Some(CertificationStatus::Pending)
    );
    let entry = restored.get_entry(&wid).unwrap();
    assert_eq!(entry.acked_by.len(), 1);

    // Complete certification on restored tracker.
    let status = restored.record_ack(
        &wid,
        node_id("auth-2"),
        HlcTimestamp {
            physical: 5002,
            logical: 0,
            node_id: "auth-2".into(),
        },
    );
    assert_eq!(status, Some(CertificationStatus::Certified));
}
