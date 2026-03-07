//! Property-based tests for quorum safety guarantees.
//!
//! Verifies that:
//! - For any partition of N nodes, at most one group can have majority.
//! - Two valid MajorityCertificates for the same key_range and policy_version
//!   must share at least one common signer.

use std::collections::HashSet;

use ed25519_dalek::Signer;
use proptest::prelude::*;

use asteroidb_poc::authority::certificate::{
    AuthoritySignature, KeysetVersion, MajorityCertificate,
};
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::types::{KeyRange, NodeId, PolicyVersion};

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------

fn ts(physical: u64, logical: u32, node_id: &str) -> HlcTimestamp {
    HlcTimestamp {
        physical,
        logical,
        node_id: node_id.into(),
    }
}

/// Create a dummy AuthoritySignature for testing (signature bytes don't matter
/// for the quorum intersection property — we're testing the counting logic).
fn dummy_sig(authority_id: &str) -> AuthoritySignature {
    use ed25519_dalek::SigningKey;
    let key = SigningKey::from_bytes(&[0u8; 32]);
    let verifying = key.verifying_key();
    let signature = key.sign(b"dummy");
    AuthoritySignature {
        authority_id: NodeId(authority_id.into()),
        public_key: verifying,
        signature,
        keyset_version: KeysetVersion(1),
    }
}

/// Majority threshold: floor(n/2) + 1
fn majority(n: usize) -> usize {
    n / 2 + 1
}

// ---------------------------------------------------------------
// Quorum intersection property
// ---------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// For any partition of N nodes into two disjoint groups,
    /// at most one group can reach majority (N/2 + 1).
    #[test]
    fn quorum_intersection_partition(
        n in 3..10usize,
        partition_bits in prop::collection::vec(prop::bool::ANY, 9)
    ) {
        let threshold = majority(n);

        // Assign each node to group A (true) or group B (false).
        let group_a_size = partition_bits.iter().take(n).filter(|&&b| b).count();
        let group_b_size = n - group_a_size;

        // At most one group can reach majority.
        let a_has_majority = group_a_size >= threshold;
        let b_has_majority = group_b_size >= threshold;

        prop_assert!(
            !(a_has_majority && b_has_majority),
            "Both groups cannot have majority: n={}, threshold={}, A={}, B={}",
            n, threshold, group_a_size, group_b_size
        );
    }

    /// Two majority subsets of the same N-node set must intersect.
    #[test]
    fn two_majorities_intersect(
        n in 3..10usize,
        indices_a in prop::collection::vec(any::<usize>(), 1..10),
        indices_b in prop::collection::vec(any::<usize>(), 1..10),
    ) {
        let threshold = majority(n);

        // Build majority subsets deterministically from random indices
        let mut set_a: Vec<usize> = indices_a.iter()
            .map(|i| i % n)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        // Pad to majority if needed
        let mut next = 0;
        while set_a.len() < threshold {
            if !set_a.contains(&next) {
                set_a.push(next);
            }
            next += 1;
        }
        let set_a: HashSet<usize> = set_a.into_iter().collect();

        let mut set_b: Vec<usize> = indices_b.iter()
            .map(|i| i % n)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let mut next = 0;
        while set_b.len() < threshold {
            if !set_b.contains(&next) {
                set_b.push(next);
            }
            next += 1;
        }
        let set_b: HashSet<usize> = set_b.into_iter().collect();

        // Both are guaranteed majority — property must hold
        prop_assert!(set_a.len() >= threshold);
        prop_assert!(set_b.len() >= threshold);
        prop_assert!(
            !set_a.is_disjoint(&set_b),
            "Two majority subsets must intersect: n={}, A={:?}, B={:?}, threshold={}",
            n, set_a, set_b, threshold
        );
    }

    /// Two valid MajorityCertificates for the same key_range and policy_version
    /// must have at least one common signer.
    #[test]
    fn majority_certificate_signer_overlap(
        n in 3..10usize,
        indices_a in prop::collection::vec(any::<usize>(), 1..10),
        indices_b in prop::collection::vec(any::<usize>(), 1..10),
    ) {
        let threshold = majority(n);
        let all_nodes: Vec<String> = (0..n).map(|i| format!("auth-{i}")).collect();

        // Build guaranteed-majority signer sets from random indices
        let mut picks_a: Vec<usize> = indices_a.iter()
            .map(|i| i % n)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let mut next = 0;
        while picks_a.len() < threshold {
            if !picks_a.contains(&next) {
                picks_a.push(next);
            }
            next += 1;
        }
        let signers_a: Vec<&str> = picks_a.iter().map(|&i| all_nodes[i].as_str()).collect();

        let mut picks_b: Vec<usize> = indices_b.iter()
            .map(|i| i % n)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let mut next = 0;
        while picks_b.len() < threshold {
            if !picks_b.contains(&next) {
                picks_b.push(next);
            }
            next += 1;
        }
        let signers_b: Vec<&str> = picks_b.iter().map(|&i| all_nodes[i].as_str()).collect();

        // Both signer sets are guaranteed to have majority
        let range = KeyRange { prefix: "test/".into() };
        let policy = PolicyVersion(1);

        let mut cert_a = MajorityCertificate::new(
            range.clone(),
            ts(100, 0, "coord"),
            policy,
            KeysetVersion(1),
        );
        for s in &signers_a {
            cert_a.add_signature(dummy_sig(s));
        }

        let mut cert_b = MajorityCertificate::new(
            range,
            ts(200, 0, "coord"),
            policy,
            KeysetVersion(1),
        );
        for s in &signers_b {
            cert_b.add_signature(dummy_sig(s));
        }

        prop_assert!(cert_a.has_majority(n), "cert_a should have majority");
        prop_assert!(cert_b.has_majority(n), "cert_b should have majority");

        // Extract signer sets and check intersection
        let ids_a: HashSet<String> = cert_a.signatures.iter()
            .map(|s| s.authority_id.0.clone())
            .collect();
        let ids_b: HashSet<String> = cert_b.signatures.iter()
            .map(|s| s.authority_id.0.clone())
            .collect();

        let common: HashSet<&String> = ids_a.intersection(&ids_b).collect();
        prop_assert!(
            !common.is_empty(),
            "Two majority certs must share a signer: n={}, |A|={}, |B|={}",
            n, ids_a.len(), ids_b.len()
        );
    }
}
