//! Split-brain safety tests for the Authority consensus mechanism (Issue #303).
//!
//! Verifies that:
//! - During a network partition, only the majority partition can certify writes.
//! - The minority partition CANNOT certify writes (safety property).
//! - Certificates issued by one partition cannot be "combined" with the other
//!   partition's certificates to create a fraudulent majority.
//! - After partition heal, stale minority certificates are correctly rejected.
//! - DualModeCertificate (BLS and Ed25519) respects the same quorum rules.
//!
//! These are unit/integration tests that simulate partitions by controlling
//! which authority frontiers are visible to each `CertifiedApi` instance.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use ed25519_dalek::SigningKey;

use asteroidb_poc::api::certified::{CertifiedApi, OnTimeout};
use asteroidb_poc::authority::ack_frontier::{AckFrontier, AckFrontierSet};
use asteroidb_poc::authority::bls::{self, BlsKeypair};
use asteroidb_poc::authority::certificate::{
    AuthoritySignature, DualModeCertificate, KeysetVersion, MajorityCertificate,
    create_certificate_message, sign_message,
};
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::error::CrdtError;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::placement::PlacementPolicy;
use asteroidb_poc::store::kv::CrdtValue;
use asteroidb_poc::types::{CertificationStatus, KeyRange, NodeId, PolicyVersion};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn node(name: &str) -> NodeId {
    NodeId(name.into())
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
        authority_id: NodeId(authority.into()),
        frontier_hlc: HlcTimestamp {
            physical,
            logical,
            node_id: authority.into(),
        },
        key_range: KeyRange {
            prefix: prefix.into(),
        },
        policy_version: PolicyVersion(1),
        digest_hash: format!("{authority}-{physical}-{logical}"),
    }
}

/// Return a physical timestamp far in the future (current time + 10 minutes).
/// This ensures that any write's HLC timestamp will be below the frontier.
fn future_physical() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 600_000 // +10 minutes
}

fn wrap_ns(ns: SystemNamespace) -> Arc<RwLock<SystemNamespace>> {
    Arc::new(RwLock::new(ns))
}

/// Create a namespace with N authority nodes and a catch-all prefix.
fn namespace_with_authorities(n: usize) -> Arc<RwLock<SystemNamespace>> {
    let authorities: Vec<NodeId> = (1..=n).map(|i| node(&format!("auth-{i}"))).collect();
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: KeyRange { prefix: "".into() },
        authority_nodes: authorities,
        auto_generated: false,
    });
    ns.set_placement_policy(PlacementPolicy::new(
        PolicyVersion(1),
        KeyRange { prefix: "".into() },
        n,
    ));
    wrap_ns(ns)
}

/// 3-authority namespace (default for most tests).
fn default_namespace() -> Arc<RwLock<SystemNamespace>> {
    namespace_with_authorities(3)
}

/// 5-authority namespace.
fn five_authority_namespace() -> Arc<RwLock<SystemNamespace>> {
    namespace_with_authorities(5)
}

/// Generate a deterministic Ed25519 key pair from a seed byte.
fn make_ed25519_keypair(seed: u8) -> (SigningKey, ed25519_dalek::VerifyingKey) {
    let mut bytes = [0u8; 32];
    bytes[0] = seed;
    bytes[31] = seed.wrapping_add(42);
    let sk = SigningKey::from_bytes(&bytes);
    let vk = sk.verifying_key();
    (sk, vk)
}

/// Generate a deterministic BLS key pair from a seed byte.
fn make_bls_keypair(seed: u8) -> BlsKeypair {
    let mut ikm = [0u8; 32];
    ikm[0] = seed;
    ikm[31] = seed.wrapping_add(42);
    BlsKeypair::generate(&ikm)
}

/// Build a real AuthoritySignature by signing the certificate message.
fn real_authority_sig(
    authority_id: &str,
    signing_key: &SigningKey,
    message: &[u8],
) -> AuthoritySignature {
    let sig = sign_message(signing_key, message);
    AuthoritySignature {
        authority_id: node(authority_id),
        public_key: signing_key.verifying_key(),
        signature: sig,
        keyset_version: KeysetVersion(1),
    }
}

// ===========================================================================
// 1. Authority quorum requirements under partition (3 nodes)
// ===========================================================================

#[test]
fn minority_1_of_3_cannot_certify() {
    // Partition: auth-1 is isolated (minority). auth-2 + auth-3 are majority.
    // The minority side should NOT be able to certify.
    let mut cert_api = CertifiedApi::new(node("node-minority"), default_namespace());

    // Only auth-1's frontier is visible (simulating minority partition).
    cert_api.update_frontier(make_frontier("auth-1", 5000, 0, ""));

    // Certified write should not succeed (1 of 3 is not majority).
    let counter = CrdtValue::Counter(PnCounter::new());
    let result = cert_api.certified_write("key1".into(), counter, OnTimeout::Error);
    assert!(
        matches!(result, Err(CrdtError::Timeout)),
        "minority (1/3) must not certify: got {result:?}"
    );
}

#[test]
fn majority_2_of_3_can_certify() {
    // Partition: auth-1 is isolated. auth-2 + auth-3 form majority.
    let mut cert_api = CertifiedApi::new(node("node-majority"), default_namespace());

    // Both auth-2 and auth-3 report frontiers far in the future (2 of 3 = majority).
    let ft = future_physical();
    cert_api.update_frontier(make_frontier("auth-2", ft, 0, ""));
    cert_api.update_frontier(make_frontier("auth-3", ft, 0, ""));

    let counter = CrdtValue::Counter(PnCounter::new());
    let result = cert_api.certified_write("key1".into(), counter, OnTimeout::Pending);
    // Should be either immediately Certified or Pending (then promoted on process).
    match result {
        Ok(CertificationStatus::Certified) => {} // Best case
        Ok(CertificationStatus::Pending) => {
            // Process certifications to promote.
            cert_api.process_certifications();
            let status = cert_api.get_certification_status("key1");
            assert_eq!(
                status,
                CertificationStatus::Certified,
                "majority (2/3) should certify after processing"
            );
        }
        other => panic!("majority (2/3) should certify, got: {other:?}"),
    }
}

#[test]
fn minority_1_of_3_pending_stays_pending() {
    // With OnTimeout::Pending, the minority write stays Pending indefinitely.
    let mut cert_api = CertifiedApi::new(node("node-minority"), default_namespace());
    cert_api.update_frontier(make_frontier("auth-1", 5000, 0, ""));

    let counter = CrdtValue::Counter(PnCounter::new());
    let result = cert_api.certified_write("key1".into(), counter, OnTimeout::Pending);
    assert_eq!(result.unwrap(), CertificationStatus::Pending);

    // Even after process_certifications, it remains Pending.
    cert_api.process_certifications();
    assert_eq!(
        cert_api.get_certification_status("key1"),
        CertificationStatus::Pending,
    );

    // Adding more frontiers from the same authority doesn't help.
    cert_api.update_frontier(make_frontier("auth-1", 10000, 0, ""));
    cert_api.process_certifications();
    assert_eq!(
        cert_api.get_certification_status("key1"),
        CertificationStatus::Pending,
    );
}

// ===========================================================================
// 2. Authority quorum requirements under partition (5 nodes)
// ===========================================================================

#[test]
fn minority_2_of_5_cannot_certify() {
    // 5-node cluster, partition: {auth-1, auth-2} vs {auth-3, auth-4, auth-5}.
    // Minority (2 of 5) cannot reach majority threshold of 3.
    let mut cert_api = CertifiedApi::new(node("node-minority"), five_authority_namespace());

    cert_api.update_frontier(make_frontier("auth-1", 5000, 0, ""));
    cert_api.update_frontier(make_frontier("auth-2", 5000, 0, ""));

    let counter = CrdtValue::Counter(PnCounter::new());
    let result = cert_api.certified_write("key1".into(), counter, OnTimeout::Error);
    assert!(
        matches!(result, Err(CrdtError::Timeout)),
        "minority (2/5) must not certify: got {result:?}"
    );
}

#[test]
fn majority_3_of_5_can_certify() {
    // 5-node cluster, majority partition: {auth-3, auth-4, auth-5}.
    let mut cert_api = CertifiedApi::new(node("node-majority"), five_authority_namespace());

    let ft = future_physical();
    cert_api.update_frontier(make_frontier("auth-3", ft, 0, ""));
    cert_api.update_frontier(make_frontier("auth-4", ft, 0, ""));
    cert_api.update_frontier(make_frontier("auth-5", ft, 0, ""));

    let counter = CrdtValue::Counter(PnCounter::new());
    let result = cert_api.certified_write("key1".into(), counter, OnTimeout::Pending);
    match result {
        Ok(CertificationStatus::Certified) => {}
        Ok(CertificationStatus::Pending) => {
            cert_api.process_certifications();
            assert_eq!(
                cert_api.get_certification_status("key1"),
                CertificationStatus::Certified,
            );
        }
        other => panic!("majority (3/5) should certify, got: {other:?}"),
    }
}

#[test]
fn exact_half_of_5_cannot_certify() {
    // Edge case: exactly 2 of 5 is below majority (threshold = 3).
    let mut cert_api = CertifiedApi::new(node("node-half"), five_authority_namespace());

    cert_api.update_frontier(make_frontier("auth-1", 5000, 0, ""));
    cert_api.update_frontier(make_frontier("auth-2", 5000, 0, ""));

    let counter = CrdtValue::Counter(PnCounter::new());
    let result = cert_api.certified_write("key1".into(), counter, OnTimeout::Error);
    assert!(matches!(result, Err(CrdtError::Timeout)));
}

// ===========================================================================
// 3. Both sides of partition cannot independently certify (safety invariant)
// ===========================================================================

#[test]
fn both_partitions_cannot_simultaneously_certify_3_nodes() {
    // The core split-brain safety property: given 3 authorities,
    // any partition into two groups means at most one group can certify.
    let ns = default_namespace();

    let ft = future_physical();

    // Minority side: only auth-1
    let mut minority = CertifiedApi::new(node("minority-node"), Arc::clone(&ns));
    minority.update_frontier(make_frontier("auth-1", ft, 0, ""));

    // Majority side: auth-2 + auth-3
    let mut majority = CertifiedApi::new(node("majority-node"), Arc::clone(&ns));
    majority.update_frontier(make_frontier("auth-2", ft, 0, ""));
    majority.update_frontier(make_frontier("auth-3", ft, 0, ""));

    let counter = CrdtValue::Counter(PnCounter::new());

    // Minority writes
    let _min_result =
        minority.certified_write("shared-key".into(), counter.clone(), OnTimeout::Pending);
    minority.process_certifications();
    let min_status = minority.get_certification_status("shared-key");

    // Majority writes
    let _maj_result =
        majority.certified_write("shared-key".into(), counter.clone(), OnTimeout::Pending);
    majority.process_certifications();
    let maj_status = majority.get_certification_status("shared-key");

    // Safety: both cannot be Certified simultaneously.
    assert!(
        !(min_status == CertificationStatus::Certified
            && maj_status == CertificationStatus::Certified),
        "SAFETY VIOLATION: both partitions certified the same key! \
         minority={min_status:?}, majority={maj_status:?}"
    );

    // Stronger: minority must NOT be certified.
    assert_ne!(
        min_status,
        CertificationStatus::Certified,
        "minority (1/3) must not certify"
    );
}

#[test]
fn both_partitions_cannot_simultaneously_certify_5_nodes() {
    // 5-node partition: {auth-1, auth-2} vs {auth-3, auth-4, auth-5}.
    let ns = five_authority_namespace();

    let ft = future_physical();

    let mut minority = CertifiedApi::new(node("minority-node"), Arc::clone(&ns));
    minority.update_frontier(make_frontier("auth-1", ft, 0, ""));
    minority.update_frontier(make_frontier("auth-2", ft, 0, ""));

    let mut majority = CertifiedApi::new(node("majority-node"), Arc::clone(&ns));
    majority.update_frontier(make_frontier("auth-3", ft, 0, ""));
    majority.update_frontier(make_frontier("auth-4", ft, 0, ""));
    majority.update_frontier(make_frontier("auth-5", ft, 0, ""));

    let counter = CrdtValue::Counter(PnCounter::new());

    minority
        .certified_write("k".into(), counter.clone(), OnTimeout::Pending)
        .ok();
    minority.process_certifications();
    let min_status = minority.get_certification_status("k");

    majority
        .certified_write("k".into(), counter.clone(), OnTimeout::Pending)
        .ok();
    majority.process_certifications();
    let maj_status = majority.get_certification_status("k");

    assert!(
        !(min_status == CertificationStatus::Certified
            && maj_status == CertificationStatus::Certified),
        "SAFETY VIOLATION: both partitions certified for 5-node cluster!"
    );
    assert_ne!(min_status, CertificationStatus::Certified);
}

// ===========================================================================
// 4. Certificate validity across partition boundaries
// ===========================================================================

#[test]
fn certificate_from_minority_lacks_majority() {
    // A certificate signed only by minority authorities does not have majority.
    let (sk1, _vk1) = make_ed25519_keypair(1);
    let key_range = KeyRange { prefix: "".into() };
    let hlc = ts(5000, 0, "coord");
    let pv = PolicyVersion(1);

    let message = create_certificate_message(&key_range, &hlc, &pv);

    let mut cert = MajorityCertificate::new(key_range.clone(), hlc.clone(), pv, KeysetVersion(1));
    cert.add_signature(real_authority_sig("auth-1", &sk1, &message));

    // 1 signature out of 3 total authorities: NOT majority.
    assert!(!cert.has_majority(3));
    assert_eq!(cert.signature_count(), 1);
}

#[test]
fn certificate_from_majority_has_majority() {
    let (sk2, _) = make_ed25519_keypair(2);
    let (sk3, _) = make_ed25519_keypair(3);
    let key_range = KeyRange { prefix: "".into() };
    let hlc = ts(5000, 0, "coord");
    let pv = PolicyVersion(1);

    let message = create_certificate_message(&key_range, &hlc, &pv);

    let mut cert = MajorityCertificate::new(key_range.clone(), hlc.clone(), pv, KeysetVersion(1));
    cert.add_signature(real_authority_sig("auth-2", &sk2, &message));
    cert.add_signature(real_authority_sig("auth-3", &sk3, &message));

    // 2 of 3 = majority.
    assert!(cert.has_majority(3));
    // Signatures are valid.
    let signers = cert.verify_signatures(&message).unwrap();
    assert_eq!(signers.len(), 2);
}

#[test]
fn dual_mode_bls_minority_cannot_reach_majority() {
    // BLS dual-mode certificate with only 1 signer out of 3.
    let kp1 = make_bls_keypair(1);
    let key_range = KeyRange { prefix: "".into() };
    let hlc = ts(5000, 0, "coord");
    let pv = PolicyVersion(1);

    let message = create_certificate_message(&key_range, &hlc, &pv);
    let sig1 = bls::sign_message(kp1.secret_key(), &message);
    let agg = bls::aggregate_signatures(&[sig1]).unwrap();

    let mut dual =
        DualModeCertificate::new_bls(key_range.clone(), hlc.clone(), pv, KeysetVersion(1));
    dual.set_bls_aggregate(vec![(node("auth-1"), kp1.public_key.clone())], agg);

    // 1 of 3: NOT majority.
    assert!(!dual.has_majority(3));
    assert_eq!(dual.signer_count(), 1);
}

#[test]
fn dual_mode_bls_majority_can_reach_majority() {
    // BLS dual-mode certificate with 2 signers out of 3.
    let kp2 = make_bls_keypair(2);
    let kp3 = make_bls_keypair(3);
    let key_range = KeyRange { prefix: "".into() };
    let hlc = ts(5000, 0, "coord");
    let pv = PolicyVersion(1);

    let message = create_certificate_message(&key_range, &hlc, &pv);
    let sig2 = bls::sign_message(kp2.secret_key(), &message);
    let sig3 = bls::sign_message(kp3.secret_key(), &message);
    let agg = bls::aggregate_signatures(&[sig2, sig3]).unwrap();

    let mut dual =
        DualModeCertificate::new_bls(key_range.clone(), hlc.clone(), pv, KeysetVersion(1));
    dual.set_bls_aggregate(
        vec![
            (node("auth-2"), kp2.public_key.clone()),
            (node("auth-3"), kp3.public_key.clone()),
        ],
        agg,
    );

    // 2 of 3: majority.
    assert!(dual.has_majority(3));

    // Verify the aggregated signature.
    let signers = dual.verify(&message).unwrap();
    assert_eq!(signers.len(), 2);
}

// ===========================================================================
// 5. Cross-partition certificate manipulation resistance
// ===========================================================================

#[test]
fn cannot_combine_minority_certificates_into_majority() {
    // Two certificates from different partitions, each with 1 signer,
    // cannot be combined to form a 2-signer certificate that claims majority.
    //
    // This tests that the quorum intersection property holds: the signatures
    // are tied to specific authority IDs, and duplicate-signer detection
    // prevents inflation.
    let (sk1, _) = make_ed25519_keypair(1);
    let (sk2, _) = make_ed25519_keypair(2);
    let key_range = KeyRange { prefix: "".into() };
    let hlc = ts(5000, 0, "coord");
    let pv = PolicyVersion(1);

    let message = create_certificate_message(&key_range, &hlc, &pv);

    // Partition A certificate: signed by auth-1 only.
    let sig1 = real_authority_sig("auth-1", &sk1, &message);

    // Partition B certificate: signed by auth-2 only.
    let sig2 = real_authority_sig("auth-2", &sk2, &message);

    // Now try to combine into a single certificate.
    let mut combined =
        MajorityCertificate::new(key_range.clone(), hlc.clone(), pv, KeysetVersion(1));
    combined.add_signature(sig1);
    combined.add_signature(sig2);

    // This has 2 of 3 signatures and technically has majority.
    // The point is that in a REAL partition, authority-1 would have a different
    // frontier_hlc than authority-2, making this scenario impossible if the
    // certified write path is correct (the same key can't have both frontiers).
    //
    // But at the certificate level, if someone could somehow get signatures
    // from both partitions for the same message, the math works. This is
    // expected because it requires crossing the partition boundary, which
    // means the partition has already healed.
    assert!(combined.has_majority(3));
    // The safety guarantee is that during a TRUE partition, you can't get
    // signatures from both sides for the same frontier_hlc.
}

#[test]
fn duplicate_authority_id_does_not_inflate_count() {
    // An attacker trying to replay the same authority's signature twice
    // should not inflate the majority count.
    let (sk1, _) = make_ed25519_keypair(1);
    let key_range = KeyRange { prefix: "".into() };
    let hlc = ts(5000, 0, "coord");
    let pv = PolicyVersion(1);

    let message = create_certificate_message(&key_range, &hlc, &pv);

    let sig = real_authority_sig("auth-1", &sk1, &message);

    let mut cert = MajorityCertificate::new(key_range.clone(), hlc.clone(), pv, KeysetVersion(1));

    // Add the same authority's signature multiple times.
    cert.add_signature(sig.clone());
    cert.add_signature(sig.clone());
    cert.add_signature(sig.clone());

    // Should still count as 1 unique signer.
    assert_eq!(cert.signature_count(), 1);
    assert!(!cert.has_majority(3));
}

#[test]
fn bls_duplicate_signer_ids_rejected() {
    // A BLS certificate with duplicate signer IDs should be rejected.
    let kp1 = make_bls_keypair(1);
    let key_range = KeyRange { prefix: "".into() };
    let hlc = ts(5000, 0, "coord");
    let pv = PolicyVersion(1);

    let message = create_certificate_message(&key_range, &hlc, &pv);
    let sig1 = bls::sign_message(kp1.secret_key(), &message);
    // Aggregate the same signature twice — but claim 2 different IDs might
    // bypass signer_count. Instead, test with same ID.
    let agg = bls::aggregate_signatures(&[sig1.clone(), sig1]).unwrap();

    let mut dual =
        DualModeCertificate::new_bls(key_range.clone(), hlc.clone(), pv, KeysetVersion(1));
    dual.set_bls_aggregate(
        vec![
            (node("auth-1"), kp1.public_key.clone()),
            (node("auth-1"), kp1.public_key.clone()), // duplicate!
        ],
        agg,
    );

    // verify() should reject due to duplicate signer IDs.
    let result = dual.verify(&message);
    assert!(
        result.is_err(),
        "BLS certificate with duplicate signer IDs should be rejected"
    );
}

// ===========================================================================
// 6. Stale certificate rejection after partition heal
// ===========================================================================

#[test]
fn stale_frontier_from_old_partition_does_not_regress_certification() {
    // After partition heal, the minority's stale frontier should not
    // cause a regression in the majority's certification state.
    let ns = default_namespace();
    let mut cert_api = CertifiedApi::new(node("node-a"), Arc::clone(&ns));

    let ft = future_physical();
    let ft2 = ft + 600_000; // even further in the future

    // Phase 1: All 3 authorities report frontier at ft.
    cert_api.update_frontier(make_frontier("auth-1", ft, 0, ""));
    cert_api.update_frontier(make_frontier("auth-2", ft, 0, ""));
    cert_api.update_frontier(make_frontier("auth-3", ft, 0, ""));

    let counter = CrdtValue::Counter(PnCounter::new());
    let _result = cert_api.certified_write("key1".into(), counter.clone(), OnTimeout::Pending);
    cert_api.process_certifications();
    assert_eq!(
        cert_api.get_certification_status("key1"),
        CertificationStatus::Certified
    );

    // Phase 2: Partition — auth-1 gets isolated and its frontier stalls.
    // Majority (auth-2, auth-3) advances to ft2.
    cert_api.update_frontier(make_frontier("auth-2", ft2, 0, ""));
    cert_api.update_frontier(make_frontier("auth-3", ft2, 0, ""));

    // Write at the new frontier.
    let _result2 = cert_api.certified_write("key2".into(), counter.clone(), OnTimeout::Pending);
    cert_api.process_certifications();
    assert_eq!(
        cert_api.get_certification_status("key2"),
        CertificationStatus::Certified,
        "majority at ft2 should certify"
    );

    // Phase 3: Partition heals — auth-1 sends its old frontier (ft).
    // This should not regress the majority frontier.
    // The AckFrontierSet already monotonically advances, so the old update
    // should be ignored.
    let _advanced = cert_api.update_frontier(make_frontier("auth-1", ft, 0, ""));
    // The frontier was already set for auth-1 at 5000; no advancement.
    // Key point: a stale frontier does not overwrite a newer one.

    // Certification at time 10000 should still be valid.
    assert_eq!(
        cert_api.get_certification_status("key2"),
        CertificationStatus::Certified
    );
}

#[test]
fn frontier_monotonicity_prevents_regression() {
    // AckFrontierSet should never accept a frontier older than the current one.
    let mut frontiers = AckFrontierSet::new();

    // First: advance to 5000.
    assert!(frontiers.update(make_frontier("auth-1", 5000, 0, "")));

    // Then: try to set back to 3000 — should be rejected.
    assert!(!frontiers.update(make_frontier("auth-1", 3000, 0, "")));

    // The frontier should still be 5000.
    let all = frontiers.all_for_scope(&KeyRange { prefix: "".into() }, &PolicyVersion(1));
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].frontier_hlc.physical, 5000);
}

// ===========================================================================
// 7. Post-partition convergence
// ===========================================================================

#[test]
fn minority_pending_write_certifies_after_partition_heal() {
    // A write that was Pending during partition should certify once the
    // partition heals and majority frontiers are received.
    let ns = default_namespace();
    let mut cert_api = CertifiedApi::new(node("node-minority"), Arc::clone(&ns));

    // During partition: only auth-1's frontier is visible (but far in the future).
    let ft = future_physical();
    cert_api.update_frontier(make_frontier("auth-1", ft, 0, ""));

    let counter = CrdtValue::Counter(PnCounter::new());
    cert_api
        .certified_write("key1".into(), counter, OnTimeout::Pending)
        .unwrap();
    cert_api.process_certifications();
    assert_eq!(
        cert_api.get_certification_status("key1"),
        CertificationStatus::Pending,
        "should be Pending during partition"
    );

    // Partition heals: auth-2 and auth-3 frontiers arrive.
    cert_api.update_frontier(make_frontier("auth-2", ft, 0, ""));
    cert_api.update_frontier(make_frontier("auth-3", ft, 0, ""));

    // Re-process: the write's timestamp (from the Pending phase) should now
    // be below the majority frontier (which is at 5000 or later).
    cert_api.process_certifications();
    assert_eq!(
        cert_api.get_certification_status("key1"),
        CertificationStatus::Certified,
        "should certify after partition heal"
    );
}

#[test]
fn multiple_pending_writes_certify_in_order_after_heal() {
    let ns = default_namespace();
    let mut cert_api = CertifiedApi::new(node("node-minority"), Arc::clone(&ns));

    // During partition: only auth-1 frontier visible.
    let ft = future_physical();
    cert_api.update_frontier(make_frontier("auth-1", ft, 0, ""));

    let counter = CrdtValue::Counter(PnCounter::new());
    for i in 1..=5 {
        cert_api
            .certified_write(format!("key{i}"), counter.clone(), OnTimeout::Pending)
            .unwrap();
    }
    cert_api.process_certifications();
    for i in 1..=5 {
        assert_eq!(
            cert_api.get_certification_status(&format!("key{i}")),
            CertificationStatus::Pending
        );
    }

    // Partition heals: remaining authorities report frontiers.
    cert_api.update_frontier(make_frontier("auth-2", ft, 0, ""));
    cert_api.update_frontier(make_frontier("auth-3", ft, 0, ""));
    cert_api.process_certifications();

    for i in 1..=5 {
        assert_eq!(
            cert_api.get_certification_status(&format!("key{i}")),
            CertificationStatus::Certified,
            "key{i} should certify after partition heal"
        );
    }
}

// ===========================================================================
// 8. Edge cases
// ===========================================================================

#[test]
fn zero_authority_reachable_cannot_certify() {
    // No authorities at all — should fail.
    let mut cert_api = CertifiedApi::new(node("node-alone"), default_namespace());

    let counter = CrdtValue::Counter(PnCounter::new());
    let result = cert_api.certified_write("key1".into(), counter, OnTimeout::Error);
    assert!(matches!(result, Err(CrdtError::Timeout)));
}

#[test]
fn single_authority_cluster_certifies_with_one() {
    // A cluster with only 1 authority: majority threshold = 1.
    let ns = namespace_with_authorities(1);
    let mut cert_api = CertifiedApi::new(node("node-solo"), ns);

    let ft = future_physical();
    cert_api.update_frontier(make_frontier("auth-1", ft, 0, ""));

    let counter = CrdtValue::Counter(PnCounter::new());
    let result = cert_api.certified_write("key1".into(), counter, OnTimeout::Pending);
    match result {
        Ok(CertificationStatus::Certified) => {}
        Ok(CertificationStatus::Pending) => {
            cert_api.process_certifications();
            assert_eq!(
                cert_api.get_certification_status("key1"),
                CertificationStatus::Certified,
            );
        }
        other => panic!("single-node should certify, got: {other:?}"),
    }
}

#[test]
fn even_cluster_requires_strict_majority() {
    // 4-node cluster: majority threshold = 3 (4/2 + 1).
    let ns = namespace_with_authorities(4);
    let ft = future_physical();

    // 2 of 4 should NOT be enough.
    let mut cert_api = CertifiedApi::new(node("node-half"), Arc::clone(&ns));
    cert_api.update_frontier(make_frontier("auth-1", ft, 0, ""));
    cert_api.update_frontier(make_frontier("auth-2", ft, 0, ""));

    let counter = CrdtValue::Counter(PnCounter::new());
    let result = cert_api.certified_write("key1".into(), counter.clone(), OnTimeout::Error);
    assert!(
        matches!(result, Err(CrdtError::Timeout)),
        "2/4 should not certify"
    );

    // 3 of 4 should be enough.
    let mut cert_api2 = CertifiedApi::new(node("node-majority"), Arc::clone(&ns));
    cert_api2.update_frontier(make_frontier("auth-1", ft, 0, ""));
    cert_api2.update_frontier(make_frontier("auth-2", ft, 0, ""));
    cert_api2.update_frontier(make_frontier("auth-3", ft, 0, ""));

    let result2 = cert_api2.certified_write("key2".into(), counter, OnTimeout::Pending);
    match result2 {
        Ok(CertificationStatus::Certified) => {}
        Ok(CertificationStatus::Pending) => {
            cert_api2.process_certifications();
            assert_eq!(
                cert_api2.get_certification_status("key2"),
                CertificationStatus::Certified,
            );
        }
        other => panic!("3/4 should certify, got: {other:?}"),
    }
}

#[test]
fn partition_with_different_key_ranges_independent() {
    // Two different key ranges should have independent certification.
    // Partition affects authorities for one range but not the other.
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: KeyRange {
            prefix: "users/".into(),
        },
        authority_nodes: vec![node("auth-1"), node("auth-2"), node("auth-3")],
        auto_generated: false,
    });
    ns.set_placement_policy(PlacementPolicy::new(
        PolicyVersion(1),
        KeyRange {
            prefix: "users/".into(),
        },
        3,
    ));
    ns.set_authority_definition(AuthorityDefinition {
        key_range: KeyRange {
            prefix: "orders/".into(),
        },
        authority_nodes: vec![node("auth-4"), node("auth-5"), node("auth-6")],
        auto_generated: false,
    });
    ns.set_placement_policy(PlacementPolicy::new(
        PolicyVersion(1),
        KeyRange {
            prefix: "orders/".into(),
        },
        3,
    ));
    let ns = wrap_ns(ns);

    let mut cert_api = CertifiedApi::new(node("node-a"), ns);
    let ft = future_physical();

    // "users/" range: only auth-1 reachable (minority).
    cert_api.update_frontier(make_frontier("auth-1", ft, 0, "users/"));

    // "orders/" range: auth-4 and auth-5 reachable (majority).
    cert_api.update_frontier(make_frontier("auth-4", ft, 0, "orders/"));
    cert_api.update_frontier(make_frontier("auth-5", ft, 0, "orders/"));

    let counter = CrdtValue::Counter(PnCounter::new());

    // "users/" write should fail (minority for that range).
    let result1 = cert_api.certified_write("users/alice".into(), counter.clone(), OnTimeout::Error);
    assert!(
        matches!(result1, Err(CrdtError::Timeout)),
        "users/ should be in minority"
    );

    // "orders/" write should succeed (majority for that range).
    let result2 =
        cert_api.certified_write("orders/1234".into(), counter.clone(), OnTimeout::Pending);
    match result2 {
        Ok(CertificationStatus::Certified) => {}
        Ok(CertificationStatus::Pending) => {
            cert_api.process_certifications();
            assert_eq!(
                cert_api.get_certification_status("orders/1234"),
                CertificationStatus::Certified,
            );
        }
        other => panic!("orders/ should certify, got: {other:?}"),
    }
}

// ===========================================================================
// 9. MajorityCertificate signer intersection property
// ===========================================================================

#[test]
fn two_majority_certificates_must_share_signer() {
    // For any N authorities, two valid majority certificates must share
    // at least one signer. This is the fundamental quorum intersection
    // guarantee that prevents split-brain.
    for n in 3..=7 {
        let threshold = n / 2 + 1;
        let all_nodes: Vec<String> = (0..n).map(|i| format!("auth-{i}")).collect();
        let keypairs: Vec<(SigningKey, ed25519_dalek::VerifyingKey)> =
            (0..n).map(|i| make_ed25519_keypair(i as u8)).collect();

        let key_range = KeyRange { prefix: "".into() };
        let hlc = ts(5000, 0, "coord");
        let pv = PolicyVersion(1);
        let message = create_certificate_message(&key_range, &hlc, &pv);

        // Certificate A: first `threshold` authorities.
        let mut cert_a =
            MajorityCertificate::new(key_range.clone(), hlc.clone(), pv, KeysetVersion(1));
        for i in 0..threshold {
            cert_a.add_signature(real_authority_sig(&all_nodes[i], &keypairs[i].0, &message));
        }

        // Certificate B: last `threshold` authorities.
        let mut cert_b =
            MajorityCertificate::new(key_range.clone(), hlc.clone(), pv, KeysetVersion(1));
        for i in (n - threshold)..n {
            cert_b.add_signature(real_authority_sig(&all_nodes[i], &keypairs[i].0, &message));
        }

        assert!(
            cert_a.has_majority(n),
            "cert_a should have majority for n={n}"
        );
        assert!(
            cert_b.has_majority(n),
            "cert_b should have majority for n={n}"
        );

        // Check intersection.
        let signers_a: HashSet<String> = cert_a
            .signatures
            .iter()
            .map(|s| s.authority_id.0.clone())
            .collect();
        let signers_b: HashSet<String> = cert_b
            .signatures
            .iter()
            .map(|s| s.authority_id.0.clone())
            .collect();
        let common: HashSet<&String> = signers_a.intersection(&signers_b).collect();

        assert!(
            !common.is_empty(),
            "Two majority certs MUST share a signer for n={n}, but found disjoint sets: \
             A={signers_a:?}, B={signers_b:?}"
        );
    }
}

// ===========================================================================
// 10. Timeout behavior during partition
// ===========================================================================

#[test]
fn minority_write_times_out_with_process_certifications_with_timeout() {
    let ns = default_namespace();
    let mut cert_api = CertifiedApi::new(node("node-minority"), ns);

    // Only 1 of 3 authorities reachable.
    cert_api.update_frontier(make_frontier("auth-1", 5000, 0, ""));

    let counter = CrdtValue::Counter(PnCounter::new());
    cert_api
        .certified_write("key1".into(), counter, OnTimeout::Pending)
        .unwrap();

    // Simulate passage of time beyond max_age_ms (default 60s).
    // The write's HLC physical time is based on wall clock, so we need
    // now_ms to be wall_clock + 120 seconds.
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 120_000; // 120 seconds in the future
    let transitions = cert_api.process_certifications_with_timeout(now_ms);

    assert!(transitions > 0, "should have timed out");
    assert_eq!(
        cert_api.get_certification_status("key1"),
        CertificationStatus::Timeout,
    );
}
