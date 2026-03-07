//! End-to-end crash recovery tests (Issue #62).
//!
//! Validates that all persistent components survive a simulated crash
//! (save to disk then reload) with their data intact.  Each test uses
//! a `tempfile::TempDir` so no leftover artefacts remain on disk.

use asteroidb_poc::api::status::{CertificationTracker, WriteId};
use asteroidb_poc::authority::ack_frontier::{AckFrontier, AckFrontierSet, FrontierScope};
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::crdt::lww_register::LwwRegister;
use asteroidb_poc::crdt::or_map::OrMap;
use asteroidb_poc::crdt::or_set::OrSet;
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::placement::PlacementPolicy;
use asteroidb_poc::store::kv::{CrdtValue, Store};
use asteroidb_poc::types::{CertificationStatus, KeyRange, NodeId, PolicyVersion};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

fn kr(prefix: &str) -> KeyRange {
    KeyRange {
        prefix: prefix.into(),
    }
}

fn pv(v: u64) -> PolicyVersion {
    PolicyVersion(v)
}

fn write_id(key: &str, physical: u64) -> WriteId {
    WriteId {
        key: key.into(),
        timestamp: ts(physical, 0, "node-a"),
    }
}

fn make_frontier(authority: &str, physical: u64, logical: u32, prefix: &str) -> AckFrontier {
    AckFrontier {
        authority_id: NodeId(authority.into()),
        frontier_hlc: ts(physical, logical, authority),
        key_range: kr(prefix),
        policy_version: PolicyVersion(1),
        digest_hash: format!("{authority}-{physical}-{logical}"),
    }
}

fn make_policy(prefix: &str) -> PlacementPolicy {
    PlacementPolicy::new(PolicyVersion(1), kr(prefix), 3)
}

fn make_authority_def(prefix: &str, nodes: &[&str]) -> AuthorityDefinition {
    AuthorityDefinition {
        key_range: kr(prefix),
        authority_nodes: nodes.iter().map(|s| NodeId((*s).into())).collect(),
        auto_generated: false,
    }
}

// ===================================================================
// 1. Store: eventual write -> save -> load -> data survives
// ===================================================================

#[test]
fn store_eventual_write_survives_crash() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.json");

    // --- Pre-crash phase ---
    {
        let mut store = Store::new();

        // Counter
        let mut counter = PnCounter::new();
        counter.increment(&node("A"));
        counter.increment(&node("A"));
        counter.decrement(&node("B"));
        store.put("hits".into(), CrdtValue::Counter(counter));

        // OR-Set
        let mut set = OrSet::new();
        set.add("alice".to_string(), &node("A"));
        set.add("bob".to_string(), &node("B"));
        store.put("users".into(), CrdtValue::Set(set));

        // OR-Map
        let mut map = OrMap::new();
        map.set(
            "name".to_string(),
            "AsteroidDB".to_string(),
            ts(100, 0, "A"),
            &node("A"),
        );
        map.set(
            "version".to_string(),
            "0.1".to_string(),
            ts(101, 0, "A"),
            &node("A"),
        );
        store.put("config".into(), CrdtValue::Map(map));

        // LWW-Register
        let mut reg = LwwRegister::new();
        reg.set("hello-world".to_string(), ts(200, 0, "A"));
        store.put("greeting".into(), CrdtValue::Register(reg));

        store.save_snapshot(&path).unwrap();
    }
    // store is dropped here, simulating a crash

    // --- Post-recovery phase ---
    let recovered = Store::load_snapshot(&path).unwrap();

    assert_eq!(recovered.len(), 4);

    // Verify counter
    match recovered.get("hits") {
        Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1), // 2 - 1
        other => panic!("expected Counter, got {other:?}"),
    }

    // Verify set
    match recovered.get("users") {
        Some(CrdtValue::Set(s)) => {
            assert!(s.contains(&"alice".to_string()));
            assert!(s.contains(&"bob".to_string()));
            assert_eq!(s.len(), 2);
        }
        other => panic!("expected Set, got {other:?}"),
    }

    // Verify map
    match recovered.get("config") {
        Some(CrdtValue::Map(m)) => {
            assert_eq!(m.get(&"name".to_string()), Some(&"AsteroidDB".to_string()));
            assert_eq!(m.get(&"version".to_string()), Some(&"0.1".to_string()));
        }
        other => panic!("expected Map, got {other:?}"),
    }

    // Verify register
    match recovered.get("greeting") {
        Some(CrdtValue::Register(r)) => {
            assert_eq!(r.get(), Some(&"hello-world".to_string()));
        }
        other => panic!("expected Register, got {other:?}"),
    }
}

// ===================================================================
// 2. SystemNamespace: policy add -> save -> load -> policy restored
// ===================================================================

#[test]
fn system_namespace_policy_survives_crash() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ns.json");

    // --- Pre-crash phase ---
    {
        let mut ns = SystemNamespace::new();

        // Add placement policies
        ns.set_placement_policy(make_policy("user/"));
        ns.set_placement_policy(
            PlacementPolicy::new(PolicyVersion(2), kr("order/"), 5).with_certified(true),
        );

        // Add authority definitions
        ns.set_authority_definition(make_authority_def("user/", &["n1", "n2", "n3"]));
        ns.set_authority_definition(make_authority_def("order/", &["n4", "n5", "n6", "n7"]));

        ns.save(&path).unwrap();
    }

    // --- Post-recovery phase ---
    let recovered = SystemNamespace::load(&path).unwrap().unwrap();

    // Version should reflect all mutations (new=1, +4 mutations = 5)
    assert_eq!(*recovered.version(), PolicyVersion(5));

    // Placement policies
    let user_policy = recovered.get_placement_policy("user/").unwrap();
    assert_eq!(user_policy.replica_count, 3);
    assert!(!user_policy.certified);

    let order_policy = recovered.get_placement_policy("order/").unwrap();
    assert_eq!(order_policy.replica_count, 5);
    assert!(order_policy.certified);

    // Authority definitions
    let user_auth = recovered.get_authority_definition("user/").unwrap();
    assert_eq!(user_auth.authority_nodes.len(), 3);

    let order_auth = recovered.get_authority_definition("order/").unwrap();
    assert_eq!(order_auth.authority_nodes.len(), 4);

    // Version history
    assert_eq!(
        recovered.version_history(),
        &[
            PolicyVersion(1),
            PolicyVersion(2),
            PolicyVersion(3),
            PolicyVersion(4),
            PolicyVersion(5),
        ]
    );
}

#[test]
fn system_namespace_version_continues_after_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ns.json");

    // Save at version 3 (new=1 + 2 mutations)
    {
        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(make_policy("a/"));
        ns.set_placement_policy(make_policy("b/"));
        ns.save(&path).unwrap();
    }

    // Recover and continue mutating
    let mut recovered = SystemNamespace::load(&path).unwrap().unwrap();
    assert_eq!(*recovered.version(), PolicyVersion(3));

    recovered.set_placement_policy(make_policy("c/"));
    assert_eq!(*recovered.version(), PolicyVersion(4));
}

// ===================================================================
// 3. AckFrontierSet: frontier add -> save -> load -> frontier restored
// ===================================================================

#[test]
fn ack_frontier_set_survives_crash() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("frontiers.json");

    // --- Pre-crash phase ---
    {
        let mut set = AckFrontierSet::new();

        // Multiple authorities across different key ranges
        set.update(make_frontier("auth-1", 100, 0, "user/"));
        set.update(make_frontier("auth-2", 200, 0, "user/"));
        set.update(make_frontier("auth-3", 150, 0, "user/"));

        set.update(make_frontier("auth-1", 500, 0, "order/"));
        set.update(make_frontier("auth-2", 600, 0, "order/"));

        set.save(&path).unwrap();
    }

    // --- Post-recovery phase ---
    let recovered = AckFrontierSet::load(&path).unwrap();

    assert_eq!(recovered.all().len(), 5);

    // Verify scoped lookup
    let scope_u1 = FrontierScope::new(kr("user/"), pv(1), node("auth-1"));
    let scope_u2 = FrontierScope::new(kr("user/"), pv(1), node("auth-2"));
    let scope_u3 = FrontierScope::new(kr("user/"), pv(1), node("auth-3"));
    let scope_o1 = FrontierScope::new(kr("order/"), pv(1), node("auth-1"));
    let scope_o2 = FrontierScope::new(kr("order/"), pv(1), node("auth-2"));

    assert_eq!(
        recovered
            .get_scoped(&scope_u1)
            .unwrap()
            .frontier_hlc
            .physical,
        100
    );
    assert_eq!(
        recovered
            .get_scoped(&scope_u2)
            .unwrap()
            .frontier_hlc
            .physical,
        200
    );
    assert_eq!(
        recovered
            .get_scoped(&scope_u3)
            .unwrap()
            .frontier_hlc
            .physical,
        150
    );
    assert_eq!(
        recovered
            .get_scoped(&scope_o1)
            .unwrap()
            .frontier_hlc
            .physical,
        500
    );
    assert_eq!(
        recovered
            .get_scoped(&scope_o2)
            .unwrap()
            .frontier_hlc
            .physical,
        600
    );

    // Verify derived queries still work after recovery
    let mf_user = recovered
        .majority_frontier_for_scope(&kr("user/"), &pv(1), 3)
        .unwrap();
    assert_eq!(mf_user.physical, 150);
}

#[test]
fn ack_frontier_set_scope_isolation_survives_crash() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("frontiers_scoped.json");

    // --- Pre-crash phase ---
    {
        let mut set = AckFrontierSet::new();

        // Same authority, different policy versions
        set.update(AckFrontier {
            authority_id: node("auth-1"),
            frontier_hlc: ts(100, 0, "auth-1"),
            key_range: kr("user/"),
            policy_version: pv(1),
            digest_hash: "v1-hash".into(),
        });
        set.update(AckFrontier {
            authority_id: node("auth-1"),
            frontier_hlc: ts(999, 0, "auth-1"),
            key_range: kr("user/"),
            policy_version: pv(2),
            digest_hash: "v2-hash".into(),
        });

        set.save(&path).unwrap();
    }

    // --- Post-recovery phase ---
    let recovered = AckFrontierSet::load(&path).unwrap();

    assert_eq!(recovered.all().len(), 2);

    // v1 frontier must remain at 100, not contaminated by v2
    let scope_v1 = FrontierScope::new(kr("user/"), pv(1), node("auth-1"));
    assert_eq!(
        recovered
            .get_scoped(&scope_v1)
            .unwrap()
            .frontier_hlc
            .physical,
        100
    );

    // v2 frontier at 999
    let scope_v2 = FrontierScope::new(kr("user/"), pv(2), node("auth-1"));
    assert_eq!(
        recovered
            .get_scoped(&scope_v2)
            .unwrap()
            .frontier_hlc
            .physical,
        999
    );
}

// ===================================================================
// 4. CertificationTracker: status add -> save -> load -> status restored
// ===================================================================

#[test]
fn certification_tracker_survives_crash() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tracker.json");

    let wid_pending = write_id("pending-key", 1000);
    let wid_certified = write_id("certified-key", 2000);
    let wid_rejected = write_id("rejected-key", 3000);

    // --- Pre-crash phase ---
    {
        let mut tracker = CertificationTracker::with_timeout(30_000);

        tracker.register_write(wid_pending.clone(), 3, ts(1000, 0, "node-a"));
        tracker.register_write(wid_certified.clone(), 2, ts(2000, 0, "node-a"));
        tracker.register_write(wid_rejected.clone(), 3, ts(3000, 0, "node-a"));

        // Certify one
        tracker.record_ack(&wid_certified, node("auth-1"), ts(2001, 0, "auth-1"));
        tracker.record_ack(&wid_certified, node("auth-2"), ts(2002, 0, "auth-2"));

        // Partially ack the pending one
        tracker.record_ack(&wid_pending, node("auth-1"), ts(1001, 0, "auth-1"));

        // Reject one
        tracker.reject(&wid_rejected, ts(3001, 0, "auth-1"));

        tracker.save(&path).unwrap();
    }

    // --- Post-recovery phase ---
    let recovered = CertificationTracker::load(&path).unwrap();

    assert_eq!(recovered.total_count(), 3);

    // Verify statuses
    assert_eq!(
        recovered.get_status(&wid_pending),
        Some(CertificationStatus::Pending)
    );
    assert_eq!(
        recovered.get_status(&wid_certified),
        Some(CertificationStatus::Certified)
    );
    assert_eq!(
        recovered.get_status(&wid_rejected),
        Some(CertificationStatus::Rejected)
    );

    // Verify partial acks survived
    let entry = recovered.get_entry(&wid_pending).unwrap();
    assert_eq!(entry.acked_by.len(), 1);
    assert!(entry.acked_by.contains(&node("auth-1")));
    assert_eq!(entry.acks_required, 3);
}

#[test]
fn certification_tracker_continues_after_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tracker.json");

    let wid = write_id("key-1", 1000);

    // Save with partial acks
    {
        let mut tracker = CertificationTracker::new();
        tracker.register_write(wid.clone(), 2, ts(1000, 0, "node-a"));
        tracker.record_ack(&wid, node("auth-1"), ts(1001, 0, "auth-1"));
        tracker.save(&path).unwrap();
    }

    // Recover and continue certification
    let mut recovered = CertificationTracker::load(&path).unwrap();
    assert_eq!(
        recovered.get_status(&wid),
        Some(CertificationStatus::Pending)
    );

    // Second ack should complete certification
    let status = recovered.record_ack(&wid, node("auth-2"), ts(1002, 0, "auth-2"));
    assert_eq!(status, Some(CertificationStatus::Certified));
}

// ===================================================================
// 5. Composite scenario: all components save -> load -> consistency
// ===================================================================

#[test]
fn composite_all_components_crash_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let store_path = dir.path().join("store.json");
    let ns_path = dir.path().join("namespace.json");
    let frontier_path = dir.path().join("frontiers.json");
    let tracker_path = dir.path().join("tracker.json");

    let wid = write_id("user/alice/balance", 1000);

    // --- Pre-crash phase: coordinated state across all components ---
    {
        // 1. Store: write data
        let mut store = Store::new();
        let mut counter = PnCounter::new();
        counter.increment(&node("node-a"));
        counter.increment(&node("node-a"));
        counter.increment(&node("node-a"));
        store.put("user/alice/balance".into(), CrdtValue::Counter(counter));

        let mut set = OrSet::new();
        set.add("alice".to_string(), &node("node-a"));
        store.put("user/index".into(), CrdtValue::Set(set));

        store.save_snapshot(&store_path).unwrap();

        // 2. SystemNamespace: configure policies
        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(
            PlacementPolicy::new(PolicyVersion(1), kr("user/"), 3).with_certified(true),
        );
        ns.set_authority_definition(make_authority_def("user/", &["auth-1", "auth-2", "auth-3"]));
        ns.save(&ns_path).unwrap();

        // 3. AckFrontierSet: track authority progress
        let mut frontiers = AckFrontierSet::new();
        frontiers.update(make_frontier("auth-1", 1500, 0, "user/"));
        frontiers.update(make_frontier("auth-2", 1200, 0, "user/"));
        frontiers.update(make_frontier("auth-3", 1000, 0, "user/"));
        frontiers.save(&frontier_path).unwrap();

        // 4. CertificationTracker: track write status
        let mut tracker = CertificationTracker::new();
        tracker.register_write(wid.clone(), 2, ts(1000, 0, "node-a"));
        tracker.record_ack(&wid, node("auth-1"), ts(1001, 0, "auth-1"));
        tracker.save(&tracker_path).unwrap();
    }
    // All components are dropped, simulating a crash.

    // --- Post-recovery phase: verify cross-component consistency ---

    // 1. Recover store
    let store = Store::load_snapshot(&store_path).unwrap();
    assert_eq!(store.len(), 2);
    match store.get("user/alice/balance") {
        Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 3),
        other => panic!("expected Counter, got {other:?}"),
    }
    match store.get("user/index") {
        Some(CrdtValue::Set(s)) => assert!(s.contains(&"alice".to_string())),
        other => panic!("expected Set, got {other:?}"),
    }

    // 2. Recover namespace
    let ns = SystemNamespace::load(&ns_path).unwrap().unwrap();
    let policy = ns.get_placement_policy("user/").unwrap();
    assert!(policy.certified);
    assert_eq!(policy.replica_count, 3);
    let auth_def = ns.get_authority_definition("user/").unwrap();
    assert_eq!(auth_def.authority_nodes.len(), 3);

    // 3. Recover frontiers
    let frontiers = AckFrontierSet::load(&frontier_path).unwrap();
    assert_eq!(frontiers.all().len(), 3);
    // Majority frontier for user/ scope (3 auths): sorted [1000, 1200, 1500],
    // majority=2, idx=1 -> 1200
    let mf = frontiers
        .majority_frontier_for_scope(&kr("user/"), &pv(1), 3)
        .unwrap();
    assert_eq!(mf.physical, 1200);
    // The write at ts=1000 is below majority frontier -> would be certifiable
    assert!(frontiers.is_certified_at_for_scope(&ts(1000, 0, "node-a"), &kr("user/"), &pv(1), 3));

    // 4. Recover tracker and complete certification
    let mut tracker = CertificationTracker::load(&tracker_path).unwrap();
    assert_eq!(tracker.get_status(&wid), Some(CertificationStatus::Pending));
    // Complete certification with second ack
    let status = tracker.record_ack(&wid, node("auth-2"), ts(1002, 0, "auth-2"));
    assert_eq!(status, Some(CertificationStatus::Certified));
}

// ===================================================================
// 6. Pending/timeout state persistence and recovery
// ===================================================================

#[test]
fn pending_state_persists_and_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tracker.json");

    let wid1 = write_id("key-1", 1000);
    let wid2 = write_id("key-2", 2000);
    let wid3 = write_id("key-3", 3000);

    // Save with multiple pending writes at different ack levels
    {
        let mut tracker = CertificationTracker::new();
        // No acks
        tracker.register_write(wid1.clone(), 3, ts(1000, 0, "node-a"));
        // 1 ack (of 3 required)
        tracker.register_write(wid2.clone(), 3, ts(2000, 0, "node-a"));
        tracker.record_ack(&wid2, node("auth-1"), ts(2001, 0, "auth-1"));
        // 2 acks (of 3 required)
        tracker.register_write(wid3.clone(), 3, ts(3000, 0, "node-a"));
        tracker.record_ack(&wid3, node("auth-1"), ts(3001, 0, "auth-1"));
        tracker.record_ack(&wid3, node("auth-2"), ts(3002, 0, "auth-2"));

        tracker.save(&path).unwrap();
    }

    // Recover
    let recovered = CertificationTracker::load(&path).unwrap();
    assert_eq!(recovered.pending_count(), 3);

    // Verify each write's ack count
    let e1 = recovered.get_entry(&wid1).unwrap();
    assert_eq!(e1.acked_by.len(), 0);
    assert_eq!(e1.acks_required, 3);

    let e2 = recovered.get_entry(&wid2).unwrap();
    assert_eq!(e2.acked_by.len(), 1);
    assert!(e2.acked_by.contains(&node("auth-1")));

    let e3 = recovered.get_entry(&wid3).unwrap();
    assert_eq!(e3.acked_by.len(), 2);
    assert!(e3.acked_by.contains(&node("auth-1")));
    assert!(e3.acked_by.contains(&node("auth-2")));
}

#[test]
fn timeout_state_persists_and_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tracker.json");

    let wid_timeout = write_id("will-timeout", 1000);
    let wid_pending = write_id("still-pending", 5000);

    // Create a tracker, timeout one entry, save
    {
        let mut tracker = CertificationTracker::with_timeout(3000);
        tracker.register_write(wid_timeout.clone(), 3, ts(1000, 0, "node-a"));
        tracker.register_write(wid_pending.clone(), 3, ts(5000, 0, "node-a"));

        // Advance time so will-timeout exceeds the 3000ms threshold
        // 1000 + 3000 = 4000, check at 4000
        tracker.check_timeouts(&ts(4000, 0, "node-a"));

        tracker.save(&path).unwrap();
    }

    // Recover
    let recovered = CertificationTracker::load(&path).unwrap();

    assert_eq!(
        recovered.get_status(&wid_timeout),
        Some(CertificationStatus::Timeout)
    );
    assert_eq!(
        recovered.get_status(&wid_pending),
        Some(CertificationStatus::Pending)
    );
}

#[test]
fn all_certification_statuses_persist() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tracker.json");

    let wid_pending = write_id("pending", 10000);
    let wid_certified = write_id("certified", 2000);
    let wid_rejected = write_id("rejected", 3000);
    let wid_timeout = write_id("timeout", 100);

    {
        let mut tracker = CertificationTracker::with_timeout(500);

        tracker.register_write(wid_pending.clone(), 3, ts(10000, 0, "node-a"));
        tracker.register_write(wid_certified.clone(), 1, ts(2000, 0, "node-a"));
        tracker.register_write(wid_rejected.clone(), 3, ts(3000, 0, "node-a"));
        tracker.register_write(wid_timeout.clone(), 3, ts(100, 0, "node-a"));

        // Certify
        tracker.record_ack(&wid_certified, node("auth-1"), ts(2001, 0, "auth-1"));
        // Reject
        tracker.reject(&wid_rejected, ts(3001, 0, "auth-1"));
        // Timeout (100 + 500 = 600, check at 10000 >> 600)
        tracker.check_timeouts(&ts(10000, 0, "node-a"));

        tracker.save(&path).unwrap();
    }

    let recovered = CertificationTracker::load(&path).unwrap();

    assert_eq!(
        recovered.get_status(&wid_pending),
        Some(CertificationStatus::Pending)
    );
    assert_eq!(
        recovered.get_status(&wid_certified),
        Some(CertificationStatus::Certified)
    );
    assert_eq!(
        recovered.get_status(&wid_rejected),
        Some(CertificationStatus::Rejected)
    );
    assert_eq!(
        recovered.get_status(&wid_timeout),
        Some(CertificationStatus::Timeout)
    );
}

#[test]
fn timeout_detection_works_on_recovered_pending() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tracker.json");

    let wid = write_id("key-1", 1000);

    // Save while still pending
    {
        let mut tracker = CertificationTracker::with_timeout(5000);
        tracker.register_write(wid.clone(), 3, ts(1000, 0, "node-a"));
        tracker.save(&path).unwrap();
    }

    // Recover and apply timeout detection
    let mut recovered = CertificationTracker::load(&path).unwrap();
    assert_eq!(
        recovered.get_status(&wid),
        Some(CertificationStatus::Pending)
    );

    // Now enough time has passed for timeout (1000 + 5000 = 6000)
    recovered.check_timeouts(&ts(6000, 0, "node-a"));
    assert_eq!(
        recovered.get_status(&wid),
        Some(CertificationStatus::Timeout)
    );
}

// ===================================================================
// Edge cases
// ===================================================================

#[test]
fn store_load_snapshot_or_default_missing_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nonexistent.json");
    let store = Store::load_snapshot_or_default(&path).unwrap();
    assert!(store.is_empty());
}

#[test]
fn store_load_snapshot_or_default_corrupt_file_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("corrupt.json");
    std::fs::write(&path, "not valid json {{{").unwrap();
    let result = Store::load_snapshot_or_default(&path);
    assert!(result.is_err());
}

#[test]
fn system_namespace_load_missing_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nonexistent.json");
    let result = SystemNamespace::load(&path).unwrap();
    assert!(result.is_none());
}

#[test]
fn empty_components_survive_crash() {
    let dir = tempfile::tempdir().unwrap();

    // Empty store
    let store_path = dir.path().join("store.json");
    Store::new().save_snapshot(&store_path).unwrap();
    let store = Store::load_snapshot(&store_path).unwrap();
    assert!(store.is_empty());

    // Empty namespace
    let ns_path = dir.path().join("ns.json");
    SystemNamespace::new().save(&ns_path).unwrap();
    let ns = SystemNamespace::load(&ns_path).unwrap().unwrap();
    assert!(ns.all_placement_policies().is_empty());
    assert!(ns.all_authority_definitions().is_empty());

    // Empty frontier set
    let frontier_path = dir.path().join("frontiers.json");
    AckFrontierSet::new().save(&frontier_path).unwrap();
    let frontiers = AckFrontierSet::load(&frontier_path).unwrap();
    assert!(frontiers.all().is_empty());

    // Empty tracker
    let tracker_path = dir.path().join("tracker.json");
    CertificationTracker::new().save(&tracker_path).unwrap();
    let tracker = CertificationTracker::load(&tracker_path).unwrap();
    assert_eq!(tracker.total_count(), 0);
}
