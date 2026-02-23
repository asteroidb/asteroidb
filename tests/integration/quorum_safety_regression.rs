//! Regression test suite for frontier scope and duplicate-quorum safety.
//!
//! Validates fixes from:
//! - #39: AckFrontierSet scoped by FrontierScope {key_range, policy_version, authority_id}
//! - #41: ControlPlaneConsensus duplicate approval deduplication
//! - #42: MajorityCertificate and CertificationTracker unique-authority quorum
//! - #43: CertifiedApi pending_writes retention/cleanup

use std::sync::{Arc, RwLock};

use asteroidb_poc::api::certified::{CertifiedApi, OnTimeout, RetentionPolicy};
use asteroidb_poc::api::status::{CertificationTracker, WriteId};
use asteroidb_poc::authority::ack_frontier::{AckFrontier, AckFrontierSet, FrontierScope};
use asteroidb_poc::authority::certificate::{
    AuthoritySignature, KeysetVersion, MajorityCertificate, create_certificate_message,
    sign_message,
};
use asteroidb_poc::compaction::{CompactionConfig, CompactionEngine};
use asteroidb_poc::control_plane::consensus::ControlPlaneConsensus;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::placement::PlacementPolicy;
use asteroidb_poc::store::kv::CrdtValue;
use asteroidb_poc::types::{CertificationStatus, KeyRange, NodeId, PolicyVersion};

use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn node(name: &str) -> NodeId {
    NodeId(name.into())
}

fn key_range(prefix: &str) -> KeyRange {
    KeyRange {
        prefix: prefix.into(),
    }
}

fn pv(v: u64) -> PolicyVersion {
    PolicyVersion(v)
}

fn ts(physical: u64, logical: u32, node_id: &str) -> HlcTimestamp {
    HlcTimestamp {
        physical,
        logical,
        node_id: node_id.into(),
    }
}

fn make_frontier(authority: &str, physical: u64, logical: u32, prefix: &str) -> AckFrontier {
    AckFrontier {
        authority_id: node(authority),
        frontier_hlc: ts(physical, logical, authority),
        key_range: key_range(prefix),
        policy_version: PolicyVersion(1),
        digest_hash: format!("{authority}-{physical}-{logical}"),
    }
}

fn make_frontier_v(
    authority: &str,
    physical: u64,
    logical: u32,
    prefix: &str,
    version: u64,
) -> AckFrontier {
    AckFrontier {
        authority_id: node(authority),
        frontier_hlc: ts(physical, logical, authority),
        key_range: key_range(prefix),
        policy_version: PolicyVersion(version),
        digest_hash: format!("{authority}-{physical}-{logical}-v{version}"),
    }
}

fn counter_value(n: i64) -> CrdtValue {
    let mut counter = PnCounter::new();
    let writer = node("writer");
    for _ in 0..n {
        counter.increment(&writer);
    }
    CrdtValue::Counter(counter)
}

fn wrap_ns(ns: SystemNamespace) -> Arc<RwLock<SystemNamespace>> {
    Arc::new(RwLock::new(ns))
}

/// Create a default SystemNamespace with a single authority definition
/// covering the empty prefix (all keys), with 3 authority nodes.
fn default_namespace() -> Arc<RwLock<SystemNamespace>> {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: key_range(""),
        authority_nodes: vec![node("auth-1"), node("auth-2"), node("auth-3")],
    });
    wrap_ns(ns)
}

fn make_key_pair() -> (SigningKey, ed25519_dalek::VerifyingKey) {
    let sk = SigningKey::generate(&mut OsRng);
    let vk = sk.verifying_key();
    (sk, vk)
}

// ===========================================================================
// 1. Cross-range frontier contamination tests (#39)
// ===========================================================================

/// Updating a frontier in key_range "user/" must not overwrite or alter
/// a frontier in key_range "order/", even for the same authority.
#[test]
fn cross_range_frontier_does_not_contaminate() {
    let mut set = AckFrontierSet::new();

    // auth-1 reports frontier for "user/" at 100
    set.update(make_frontier("auth-1", 100, 0, "user/"));
    // auth-1 reports frontier for "order/" at 9999
    set.update(make_frontier("auth-1", 9999, 0, "order/"));

    // user/ frontier must remain at 100
    let scope_user = FrontierScope::new(key_range("user/"), pv(1), node("auth-1"));
    assert_eq!(
        set.get_scoped(&scope_user).unwrap().frontier_hlc.physical,
        100,
        "order/ update contaminated user/ frontier"
    );
}

/// Majority frontier for one key range must not include entries from a
/// different key range.
#[test]
fn cross_range_majority_frontier_independent() {
    let mut set = AckFrontierSet::new();

    // "user/": 2 of 3 authorities report (physical 100, 150)
    set.update(make_frontier("auth-1", 100, 0, "user/"));
    set.update(make_frontier("auth-2", 150, 0, "user/"));
    // "order/": 2 of 3 authorities report at much higher values
    set.update(make_frontier("auth-1", 5000, 0, "order/"));
    set.update(make_frontier("auth-2", 6000, 0, "order/"));

    // user/ majority should be 100 (sorted [100,150], idx=0), not inflated
    let mf_user = set
        .majority_frontier_for_scope(&key_range("user/"), &pv(1), 3)
        .unwrap();
    assert_eq!(mf_user.physical, 100);

    // order/ majority should be 5000 (sorted [5000,6000], idx=0)
    let mf_order = set
        .majority_frontier_for_scope(&key_range("order/"), &pv(1), 3)
        .unwrap();
    assert_eq!(mf_order.physical, 5000);
}

/// is_certified_at_for_scope in one key range does not depend on
/// frontiers in another.
#[test]
fn cross_range_certification_isolated() {
    let mut set = AckFrontierSet::new();

    // Only "order/" has majority; "user/" has 0 frontiers
    set.update(make_frontier("auth-1", 500, 0, "order/"));
    set.update(make_frontier("auth-2", 600, 0, "order/"));

    let check_ts = ts(200, 0, "client");

    // user/ must NOT be certified even though order/ has strong frontiers
    assert!(
        !set.is_certified_at_for_scope(&check_ts, &key_range("user/"), &pv(1), 3),
        "user/ certified despite having no frontier entries"
    );

    // order/ IS certified at 200
    assert!(set.is_certified_at_for_scope(&check_ts, &key_range("order/"), &pv(1), 3));
}

/// Multiple key ranges updated in interleaved order still track independently.
#[test]
fn cross_range_interleaved_updates_independent() {
    let mut set = AckFrontierSet::new();

    // Interleave updates across three key ranges
    set.update(make_frontier("auth-1", 100, 0, "a/"));
    set.update(make_frontier("auth-1", 200, 0, "b/"));
    set.update(make_frontier("auth-1", 300, 0, "c/"));
    set.update(make_frontier("auth-2", 110, 0, "a/"));
    set.update(make_frontier("auth-2", 210, 0, "b/"));
    set.update(make_frontier("auth-2", 310, 0, "c/"));

    // Each range has exactly 2 entries
    assert_eq!(set.all_for_scope(&key_range("a/"), &pv(1)).len(), 2);
    assert_eq!(set.all_for_scope(&key_range("b/"), &pv(1)).len(), 2);
    assert_eq!(set.all_for_scope(&key_range("c/"), &pv(1)).len(), 2);

    // min_frontier per scope is independent
    assert_eq!(
        set.min_frontier_for_scope(&key_range("a/"), &pv(1))
            .unwrap()
            .physical,
        100
    );
    assert_eq!(
        set.min_frontier_for_scope(&key_range("b/"), &pv(1))
            .unwrap()
            .physical,
        200
    );
    assert_eq!(
        set.min_frontier_for_scope(&key_range("c/"), &pv(1))
            .unwrap()
            .physical,
        300
    );
}

// ===========================================================================
// 2. Policy-version transition certification tests (#39)
// ===========================================================================

/// Different policy versions in the same key range must maintain separate
/// frontiers.
#[test]
fn policy_version_transition_frontiers_isolated() {
    let mut set = AckFrontierSet::new();

    // v1: all 3 authorities at high values
    set.update(make_frontier_v("auth-1", 1000, 0, "user/", 1));
    set.update(make_frontier_v("auth-2", 1100, 0, "user/", 1));
    set.update(make_frontier_v("auth-3", 1200, 0, "user/", 1));

    // v2: all 3 authorities at low values (fresh epoch)
    set.update(make_frontier_v("auth-1", 10, 0, "user/", 2));
    set.update(make_frontier_v("auth-2", 20, 0, "user/", 2));
    set.update(make_frontier_v("auth-3", 30, 0, "user/", 2));

    // v1 certification at ts=900 should succeed (majority frontier = 1100)
    assert!(set.is_certified_at_for_scope(&ts(900, 0, "c"), &key_range("user/"), &pv(1), 3));

    // v2 certification at ts=900 should fail (majority frontier = 20)
    assert!(!set.is_certified_at_for_scope(&ts(900, 0, "c"), &key_range("user/"), &pv(2), 3));
}

/// Adding v2 frontiers does not advance v1 majority frontier.
#[test]
fn policy_version_upgrade_does_not_advance_old_version() {
    let mut set = AckFrontierSet::new();

    // v1: only 1 of 3 reported
    set.update(make_frontier_v("auth-1", 100, 0, "user/", 1));

    // v2: all 3 reported at high values
    set.update(make_frontier_v("auth-1", 5000, 0, "user/", 2));
    set.update(make_frontier_v("auth-2", 6000, 0, "user/", 2));
    set.update(make_frontier_v("auth-3", 7000, 0, "user/", 2));

    // v1 still has no majority (only 1 of 3)
    assert!(
        set.majority_frontier_for_scope(&key_range("user/"), &pv(1), 3)
            .is_none(),
        "v2 entries inflated v1 majority"
    );

    // v2 has full majority
    assert!(
        set.majority_frontier_for_scope(&key_range("user/"), &pv(2), 3)
            .is_some()
    );
}

/// Certification correctness during a rolling policy version upgrade:
/// some authorities on v1, others migrated to v2.
#[test]
fn policy_version_rolling_upgrade_correctness() {
    let mut set = AckFrontierSet::new();

    // auth-1 and auth-2 still on v1
    set.update(make_frontier_v("auth-1", 500, 0, "data/", 1));
    set.update(make_frontier_v("auth-2", 600, 0, "data/", 1));
    // auth-3 migrated to v2
    set.update(make_frontier_v("auth-3", 1000, 0, "data/", 2));

    // v1 has 2 of 3 → majority
    let mf_v1 = set
        .majority_frontier_for_scope(&key_range("data/"), &pv(1), 3)
        .unwrap();
    assert_eq!(mf_v1.physical, 500);

    // v2 has only 1 of 3 → no majority
    assert!(
        set.majority_frontier_for_scope(&key_range("data/"), &pv(2), 3)
            .is_none()
    );
}

// ===========================================================================
// 3. Duplicate approval/signature/ack attack-style tests (#41, #42)
// ===========================================================================

// --- 3a. ControlPlaneConsensus duplicate approvals (#41) ---

/// Duplicate approvals from the same node must not inflate quorum in
/// control-plane consensus.
#[test]
fn duplicate_approval_does_not_inflate_control_plane_quorum() {
    let consensus = ControlPlaneConsensus::new(vec![node("n1"), node("n2"), node("n3")]);

    // n1 approves 10 times — should still count as 1 unique approval
    let approvals: Vec<NodeId> = (0..10).map(|_| node("n1")).collect();
    assert!(
        !consensus.has_majority(&approvals),
        "duplicate approvals inflated quorum to majority"
    );
}

/// Non-authority approvals must not count toward quorum even when combined
/// with duplicates.
#[test]
fn non_authority_plus_duplicate_does_not_reach_quorum() {
    let consensus = ControlPlaneConsensus::new(vec![node("n1"), node("n2"), node("n3")]);

    // n1 (authority) + n99 (non-authority) repeated
    let approvals = vec![
        node("n1"),
        node("n99"),
        node("n99"),
        node("n99"),
        node("n1"),
    ];
    assert!(
        !consensus.has_majority(&approvals),
        "non-authority approvals + duplicates inflated quorum"
    );
}

/// Policy update must be rejected when approvals are all from the same node.
#[test]
fn duplicate_approval_policy_update_rejected() {
    let mut consensus = ControlPlaneConsensus::new(vec![node("n1"), node("n2"), node("n3")]);

    let policy = PlacementPolicy::new(pv(1), key_range("user/"), 3);
    let result = consensus.propose_policy_update(policy, &[node("n1"), node("n1"), node("n1")]);

    assert!(
        result.is_err(),
        "policy update should require unique majority"
    );
    assert!(
        consensus
            .namespace()
            .get_placement_policy("user/")
            .is_none(),
        "rejected proposal mutated namespace"
    );
}

/// Authority update must be rejected when approvals are duplicated.
#[test]
fn duplicate_approval_authority_update_rejected() {
    let mut consensus = ControlPlaneConsensus::new(vec![node("n1"), node("n2"), node("n3")]);

    let def = AuthorityDefinition {
        key_range: key_range("order/"),
        authority_nodes: vec![node("a1"), node("a2")],
    };
    let result = consensus.propose_authority_update(def, &[node("n2"), node("n2")]);

    assert!(
        result.is_err(),
        "authority update should require unique majority"
    );
}

// --- 3b. MajorityCertificate duplicate signatures (#42) ---

/// Duplicate signatures from the same authority must not inflate the
/// certificate's majority count.
#[test]
fn duplicate_signature_does_not_inflate_certificate_majority() {
    let kr = key_range("user/");
    let hlc = ts(1_000_000, 0, "auth-1");
    let p = pv(1);
    let message = create_certificate_message(&kr, &hlc, &p);

    let mut cert = MajorityCertificate::new(kr, hlc, p, KeysetVersion(1));

    // Add 5 signatures all from the same authority
    for _ in 0..5 {
        let (sk, vk) = make_key_pair();
        let sig = sign_message(&sk, &message);
        cert.add_signature(AuthoritySignature {
            authority_id: node("auth-1"),
            public_key: vk,
            signature: sig,
        });
    }

    assert_eq!(
        cert.signature_count(),
        1,
        "duplicate signatures were counted"
    );
    assert!(
        !cert.has_majority(3),
        "duplicate signatures inflated majority"
    );
}

/// Even with different keys, the same authority_id must be deduplicated.
#[test]
fn duplicate_signature_different_keys_same_authority_deduplicated() {
    let kr = key_range("data/");
    let hlc = ts(2_000_000, 0, "auth-1");
    let p = pv(1);
    let message = create_certificate_message(&kr, &hlc, &p);

    let mut cert = MajorityCertificate::new(kr, hlc, p, KeysetVersion(1));

    // Two different signing keys, but same authority_id
    let (sk1, vk1) = make_key_pair();
    let (sk2, vk2) = make_key_pair();

    cert.add_signature(AuthoritySignature {
        authority_id: node("auth-A"),
        public_key: vk1,
        signature: sign_message(&sk1, &message),
    });
    cert.add_signature(AuthoritySignature {
        authority_id: node("auth-A"),
        public_key: vk2,
        signature: sign_message(&sk2, &message),
    });

    assert_eq!(cert.signature_count(), 1);
    assert!(!cert.has_majority(3));
}

/// A certificate reaches majority only with genuinely unique authorities
/// after discarding duplicates.
#[test]
fn certificate_majority_requires_unique_authorities() {
    let kr = key_range("test/");
    let hlc = ts(3_000_000, 0, "auth-1");
    let p = pv(1);
    let message = create_certificate_message(&kr, &hlc, &p);

    let mut cert = MajorityCertificate::new(kr, hlc, p, KeysetVersion(1));

    // auth-1 signs 3 times (duplicates)
    for _ in 0..3 {
        let (sk, vk) = make_key_pair();
        cert.add_signature(AuthoritySignature {
            authority_id: node("auth-1"),
            public_key: vk,
            signature: sign_message(&sk, &message),
        });
    }
    assert!(!cert.has_majority(3));

    // Now add auth-2 → 2 unique signers = majority of 3
    let (sk2, vk2) = make_key_pair();
    cert.add_signature(AuthoritySignature {
        authority_id: node("auth-2"),
        public_key: vk2,
        signature: sign_message(&sk2, &message),
    });

    assert_eq!(cert.signature_count(), 2);
    assert!(cert.has_majority(3));
}

// --- 3c. CertificationTracker duplicate acks (#42) ---

/// Duplicate acks from the same authority must not inflate the ack count
/// and must not prematurely promote a pending write.
#[test]
fn duplicate_ack_does_not_promote_pending_write() {
    let mut tracker = CertificationTracker::new();
    let wid = WriteId {
        key: "victim-key".into(),
        timestamp: ts(1000, 0, "node-a"),
    };
    // Require 2 unique acks
    tracker.register_write(wid.clone(), 2, ts(1000, 0, "node-a"));

    // Same authority acks 10 times
    for i in 0..10 {
        tracker.record_ack(&wid, node("auth-1"), ts(1001 + i, 0, "auth-1"));
    }

    assert_eq!(
        tracker.get_status(&wid),
        Some(CertificationStatus::Pending),
        "duplicate acks promoted write prematurely"
    );

    let entry = tracker.get_entry(&wid).unwrap();
    assert_eq!(
        entry.acked_by.len(),
        1,
        "duplicate acks inflated acked_by set"
    );
}

/// A write requires unique authorities to reach the ack threshold.
#[test]
fn certification_tracker_requires_unique_authorities_for_promotion() {
    let mut tracker = CertificationTracker::new();
    let wid = WriteId {
        key: "protected-key".into(),
        timestamp: ts(2000, 0, "node-a"),
    };
    // Require 3 unique acks (majority of 5)
    tracker.register_write(wid.clone(), 3, ts(2000, 0, "node-a"));

    // auth-1 acks 5 times
    for i in 0..5 {
        tracker.record_ack(&wid, node("auth-1"), ts(2001 + i, 0, "auth-1"));
    }
    assert_eq!(tracker.get_status(&wid), Some(CertificationStatus::Pending));

    // auth-2 acks 5 times
    for i in 0..5 {
        tracker.record_ack(&wid, node("auth-2"), ts(3001 + i, 0, "auth-2"));
    }
    assert_eq!(tracker.get_status(&wid), Some(CertificationStatus::Pending));

    // auth-3 acks once → 3 unique authorities → certified
    let status = tracker.record_ack(&wid, node("auth-3"), ts(4000, 0, "auth-3"));
    assert_eq!(status, Some(CertificationStatus::Certified));
}

/// Mixed duplicates and unique acks: only unique ones count.
#[test]
fn mixed_duplicate_and_unique_acks_counted_correctly() {
    let mut tracker = CertificationTracker::new();
    let wid = WriteId {
        key: "mixed-key".into(),
        timestamp: ts(5000, 0, "node-a"),
    };
    tracker.register_write(wid.clone(), 2, ts(5000, 0, "node-a"));

    // Interleave: auth-1, auth-1, auth-2, auth-1
    tracker.record_ack(&wid, node("auth-1"), ts(5001, 0, "auth-1"));
    let s1 = tracker.record_ack(&wid, node("auth-1"), ts(5002, 0, "auth-1"));
    assert_eq!(s1, Some(CertificationStatus::Pending)); // still only 1 unique

    let s2 = tracker.record_ack(&wid, node("auth-2"), ts(5003, 0, "auth-2"));
    assert_eq!(s2, Some(CertificationStatus::Certified)); // 2 unique → threshold

    // Trailing duplicate after certification
    let s3 = tracker.record_ack(&wid, node("auth-1"), ts(5004, 0, "auth-1"));
    assert_eq!(s3, Some(CertificationStatus::Certified)); // unchanged
}

// ===========================================================================
// 4. Compaction eligibility checks under scoped frontiers (#39, FR-010)
// ===========================================================================

/// Compaction eligibility uses scoped majority frontier (#51),
/// so a checkpoint is only compactable when authorities in the *same
/// key_range and policy_version scope* have consumed past it.
/// Frontiers from other key ranges must NOT contribute.
#[test]
fn compaction_eligibility_uses_scoped_frontier() {
    let mut engine = CompactionEngine::new(CompactionConfig {
        time_threshold_ms: 30_000,
        ops_threshold: 10_000,
    });

    let kr = key_range("user/");
    engine.create_checkpoint(kr.clone(), ts(100, 0, "node-a"), "hash1".into(), pv(1));

    // Frontiers: only "order/" range has majority → should NOT help "user/" compaction
    let mut frontiers = AckFrontierSet::new();
    frontiers.update(make_frontier("auth-1", 500, 0, "order/"));
    frontiers.update(make_frontier("auth-2", 600, 0, "order/"));

    // is_compactable uses scoped is_certified_at_for_scope which only counts
    // entries matching the checkpoint's key_range + policy_version.
    // No "user/" frontiers exist, so this must NOT be compactable:
    assert!(!engine.is_compactable("user/", &frontiers, 3));

    // With only 1 frontier entry (also wrong scope): still NOT compactable
    let mut frontiers_insufficient = AckFrontierSet::new();
    frontiers_insufficient.update(make_frontier("auth-1", 500, 0, "order/"));
    assert!(!engine.is_compactable("user/", &frontiers_insufficient, 3));
}

/// Compaction with scoped frontiers: when all authorities are in the correct
/// scope, the checkpoint is compactable.
#[test]
fn compaction_eligible_with_scoped_frontiers() {
    let mut engine = CompactionEngine::new(CompactionConfig {
        time_threshold_ms: 30_000,
        ops_threshold: 10_000,
    });

    let kr = key_range("sensor/");
    engine.create_checkpoint(
        kr.clone(),
        ts(200, 0, "node-a"),
        "hash-sensor".into(),
        pv(1),
    );

    // All 3 authorities in "sensor/" scope past checkpoint
    let mut frontiers = AckFrontierSet::new();
    frontiers.update(make_frontier("auth-1", 300, 0, "sensor/"));
    frontiers.update(make_frontier("auth-2", 400, 0, "sensor/"));
    frontiers.update(make_frontier("auth-3", 250, 0, "sensor/"));

    assert!(engine.is_compactable("sensor/", &frontiers, 3));
}

/// Compaction must not be eligible when frontier majority is below checkpoint.
#[test]
fn compaction_ineligible_when_frontiers_behind_checkpoint() {
    let mut engine = CompactionEngine::new(CompactionConfig {
        time_threshold_ms: 30_000,
        ops_threshold: 10_000,
    });

    let kr = key_range("log/");
    // Checkpoint at a high timestamp
    engine.create_checkpoint(
        kr.clone(),
        ts(10_000, 0, "node-a"),
        "hash-log".into(),
        pv(1),
    );

    // Authorities mostly behind the checkpoint
    let mut frontiers = AckFrontierSet::new();
    frontiers.update(make_frontier("auth-1", 500, 0, "log/"));
    frontiers.update(make_frontier("auth-2", 600, 0, "log/"));
    frontiers.update(make_frontier("auth-3", 15_000, 0, "log/"));

    // majority frontier = sorted [500, 600, 15000], idx=1 → 600 < 10000
    assert!(
        !engine.is_compactable("log/", &frontiers, 3),
        "compaction allowed with majority behind checkpoint"
    );
}

/// Scoped min_frontier is correct after interleaving updates from
/// multiple key ranges.
#[test]
fn scoped_min_frontier_after_interleaved_updates() {
    let mut set = AckFrontierSet::new();

    // "fast/" range: authorities at high values
    set.update(make_frontier("auth-1", 10_000, 0, "fast/"));
    set.update(make_frontier("auth-2", 11_000, 0, "fast/"));
    set.update(make_frontier("auth-3", 12_000, 0, "fast/"));

    // "slow/" range: authorities at low values
    set.update(make_frontier("auth-1", 100, 0, "slow/"));
    set.update(make_frontier("auth-2", 200, 0, "slow/"));
    set.update(make_frontier("auth-3", 150, 0, "slow/"));

    // Scoped min for "fast/" should not be dragged down by "slow/"
    assert_eq!(
        set.min_frontier_for_scope(&key_range("fast/"), &pv(1))
            .unwrap()
            .physical,
        10_000
    );
    assert_eq!(
        set.min_frontier_for_scope(&key_range("slow/"), &pv(1))
            .unwrap()
            .physical,
        100
    );

    // Global min IS the lowest across all scopes
    assert_eq!(set.min_frontier().unwrap().physical, 100);
}

// ===========================================================================
// 5. CertifiedApi retention/cleanup safety (#43)
// ===========================================================================

/// Auto-cleanup triggered at capacity removes completed entries,
/// keeping the list bounded.
#[test]
fn retention_auto_cleanup_removes_completed_entries() {
    let policy = RetentionPolicy {
        max_age_ms: 60_000,
        max_entries: 5,
    };
    let mut api = CertifiedApi::with_retention(node("node-1"), default_namespace(), policy);

    // Write 5 entries (hitting capacity)
    for i in 0..5 {
        api.certified_write(format!("key-{i}"), counter_value(1), OnTimeout::Pending)
            .unwrap();
    }

    let ts_first = api.pending_writes()[0].timestamp.physical;

    // Certify all entries by advancing frontier well past all timestamps
    api.update_frontier(make_frontier("auth-1", ts_first + 1, 0, ""));
    api.update_frontier(make_frontier("auth-2", ts_first + 1, 0, ""));
    api.process_certifications();

    // All 5 should be certified now
    let certified_before = api
        .pending_writes()
        .iter()
        .filter(|pw| pw.status == CertificationStatus::Certified)
        .count();
    assert!(certified_before > 0, "no entries were certified");
    assert_eq!(api.pending_writes().len(), 5);

    // 6th write triggers auto-cleanup (len >= max_entries → cleanup_completed)
    // Auto-cleanup removes all certified entries, then adds the new write.
    api.certified_write("key-5".into(), counter_value(1), OnTimeout::Pending)
        .unwrap();

    // After auto-cleanup: the old certified entries (key-0..key-4) are removed.
    // Only key-5 (and possibly other new writes) remain.
    assert!(
        api.pending_writes().len() < 5,
        "auto-cleanup did not remove completed entries: {} entries remain",
        api.pending_writes().len()
    );

    // key-5 should be present
    assert!(
        api.pending_writes().iter().any(|pw| pw.key == "key-5"),
        "new write key-5 is missing after auto-cleanup"
    );
}

/// Expired pending writes are marked Timeout and removed by cleanup.
#[test]
fn retention_expired_writes_marked_timeout_and_removed() {
    let policy = RetentionPolicy {
        max_age_ms: 1_000,
        max_entries: 10_000,
    };
    let mut api = CertifiedApi::with_retention(node("node-1"), default_namespace(), policy);

    api.certified_write("ephemeral".into(), counter_value(1), OnTimeout::Pending)
        .unwrap();
    let write_ts = api.pending_writes()[0].timestamp.physical;

    // Not yet expired
    api.cleanup_expired(write_ts + 999);
    assert_eq!(api.pending_writes().len(), 1);

    // Now expired
    api.cleanup_expired(write_ts + 1_000);
    assert_eq!(
        api.pending_writes().len(),
        0,
        "expired pending write was not cleaned up"
    );
}

/// cleanup() performs full lifecycle: expire + remove completed.
#[test]
fn retention_full_cleanup_lifecycle() {
    let policy = RetentionPolicy {
        max_age_ms: 5_000,
        max_entries: 100,
    };
    let mut api = CertifiedApi::with_retention(node("node-1"), default_namespace(), policy);

    // Write entry A (will be certified)
    api.certified_write("cert-me".into(), counter_value(1), OnTimeout::Pending)
        .unwrap();
    let ts_a = api.pending_writes()[0].timestamp.physical;

    // Write entry B (will expire)
    api.certified_write("expire-me".into(), counter_value(2), OnTimeout::Pending)
        .unwrap();

    // Certify A
    api.update_frontier(make_frontier("auth-1", ts_a + 1, 0, ""));
    api.update_frontier(make_frontier("auth-2", ts_a + 1, 0, ""));
    api.process_certifications();

    // Full cleanup at time well past max_age_ms
    let ts_b = api.pending_writes()[1].timestamp.physical;
    api.cleanup(ts_b + 5_000);

    assert_eq!(
        api.pending_writes().len(),
        0,
        "entries remained after full cleanup"
    );
}

/// Bounded growth: under sustained writes with periodic certification,
/// the pending_writes list stays bounded.
#[test]
fn retention_bounded_growth_under_sustained_writes() {
    let policy = RetentionPolicy {
        max_age_ms: 60_000,
        max_entries: 10,
    };
    let mut api = CertifiedApi::with_retention(node("node-1"), default_namespace(), policy);

    for i in 0..100u64 {
        api.certified_write(format!("key-{i}"), counter_value(1), OnTimeout::Pending)
            .unwrap();

        // Certify every 3rd write
        if i % 3 == 0 {
            let last_ts = api.pending_writes().last().unwrap().timestamp.physical;
            api.update_frontier(make_frontier("auth-1", last_ts + 100, 0, ""));
            api.update_frontier(make_frontier("auth-2", last_ts + 100, 0, ""));
            api.process_certifications();
        }
    }

    // Must be bounded at or below max_entries + small overhead
    assert!(
        api.pending_writes().len() <= 20,
        "unbounded growth: {} entries",
        api.pending_writes().len()
    );
}

// ===========================================================================
// 6. Combined integration: scoped frontier + duplicate safety end-to-end
// ===========================================================================

/// End-to-end test combining:
/// - Scoped frontiers for two key ranges
/// - Duplicate ack resistance in CertificationTracker
/// - Certificate with unique-authority enforcement
#[test]
fn end_to_end_scoped_frontier_with_duplicate_safety() {
    // --- Setup: two key ranges tracked independently ---
    let mut frontier_set = AckFrontierSet::new();
    let mut tracker = CertificationTracker::new();

    let wid_user = WriteId {
        key: "user/alice".into(),
        timestamp: ts(1000, 0, "node-1"),
    };
    let wid_order = WriteId {
        key: "order/123".into(),
        timestamp: ts(1000, 0, "node-1"),
    };

    tracker.register_write(wid_user.clone(), 2, ts(1000, 0, "node-1"));
    tracker.register_write(wid_order.clone(), 2, ts(1000, 0, "node-1"));

    // --- auth-1 acks both writes ---
    frontier_set.update(make_frontier("auth-1", 2000, 0, "user/"));
    frontier_set.update(make_frontier("auth-1", 2000, 0, "order/"));
    tracker.record_ack(&wid_user, node("auth-1"), ts(2000, 0, "auth-1"));
    tracker.record_ack(&wid_order, node("auth-1"), ts(2000, 0, "auth-1"));

    // --- auth-1 duplicates (should be ignored) ---
    tracker.record_ack(&wid_user, node("auth-1"), ts(2001, 0, "auth-1"));
    tracker.record_ack(&wid_order, node("auth-1"), ts(2001, 0, "auth-1"));

    // Both still pending (only 1 unique ack each)
    assert_eq!(
        tracker.get_status(&wid_user),
        Some(CertificationStatus::Pending)
    );
    assert_eq!(
        tracker.get_status(&wid_order),
        Some(CertificationStatus::Pending)
    );

    // --- auth-2 acks user/ only ---
    frontier_set.update(make_frontier("auth-2", 2500, 0, "user/"));
    let s = tracker.record_ack(&wid_user, node("auth-2"), ts(2500, 0, "auth-2"));
    assert_eq!(s, Some(CertificationStatus::Certified));

    // user/ certified, order/ still pending
    assert_eq!(
        tracker.get_status(&wid_user),
        Some(CertificationStatus::Certified)
    );
    assert_eq!(
        tracker.get_status(&wid_order),
        Some(CertificationStatus::Pending)
    );

    // --- Verify scoped frontier independence ---
    // user/: 2 of 3 → majority at 2000
    let mf_user = frontier_set
        .majority_frontier_for_scope(&key_range("user/"), &pv(1), 3)
        .unwrap();
    assert_eq!(mf_user.physical, 2000);

    // order/: only 1 of 3 → no majority
    assert!(
        frontier_set
            .majority_frontier_for_scope(&key_range("order/"), &pv(1), 3)
            .is_none()
    );

    // --- Verify certificate with duplicate protection ---
    let kr = key_range("user/");
    let hlc = ts(2000, 0, "auth-1");
    let p = pv(1);
    let message = create_certificate_message(&kr, &hlc, &p);

    let mut cert = MajorityCertificate::new(kr, hlc, p, KeysetVersion(1));

    let (sk1, vk1) = make_key_pair();
    cert.add_signature(AuthoritySignature {
        authority_id: node("auth-1"),
        public_key: vk1,
        signature: sign_message(&sk1, &message),
    });
    // Duplicate from auth-1
    let (sk1b, vk1b) = make_key_pair();
    cert.add_signature(AuthoritySignature {
        authority_id: node("auth-1"),
        public_key: vk1b,
        signature: sign_message(&sk1b, &message),
    });

    assert_eq!(
        cert.signature_count(),
        1,
        "duplicate auth-1 signature counted"
    );

    let (sk2, vk2) = make_key_pair();
    cert.add_signature(AuthoritySignature {
        authority_id: node("auth-2"),
        public_key: vk2,
        signature: sign_message(&sk2, &message),
    });

    assert!(cert.has_majority(3));
    assert!(cert.verify_signatures(&message).is_ok());
}
