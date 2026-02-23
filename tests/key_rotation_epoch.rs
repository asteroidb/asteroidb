//! Integration tests: Key rotation and epoch management for proof verification.
//!
//! Validates that keyset rotation, epoch grace periods, and mixed-version
//! signature verification work correctly in end-to-end scenarios (FR-008).

use asteroidb_poc::api::certified::ProofBundle;
use asteroidb_poc::authority::certificate::{
    AuthoritySignature, CertError, EpochConfig, EpochManager, KeysetRegistry, KeysetVersion,
    MajorityCertificate, create_certificate_message, sign_message,
};
use asteroidb_poc::authority::verifier::{
    verify_proof, verify_proof_with_registry, verify_proof_with_registry_detailed,
};
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::types::{KeyRange, NodeId, PolicyVersion};
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_key_pair() -> (SigningKey, ed25519_dalek::VerifyingKey) {
    let sk = SigningKey::generate(&mut OsRng);
    let vk = sk.verifying_key();
    (sk, vk)
}

fn sample_kr() -> KeyRange {
    KeyRange {
        prefix: "data/".into(),
    }
}

fn sample_hlc() -> HlcTimestamp {
    HlcTimestamp {
        physical: 1_700_000_000_000,
        logical: 42,
        node_id: "node-1".into(),
    }
}

fn sample_pv() -> PolicyVersion {
    PolicyVersion(1)
}

// ---------------------------------------------------------------------------
// Scenario 1: Key rotation mid-operation — old and new keys coexist
// ---------------------------------------------------------------------------

#[test]
fn key_rotation_old_and_new_keys_coexist_during_grace() {
    let kr = sample_kr();
    let hlc = sample_hlc();
    let pv = sample_pv();
    let message = create_certificate_message(&kr, &hlc, &pv);

    // Set up epoch manager with 24h epochs and 7-epoch grace.
    let config = EpochConfig {
        duration_secs: 86400,
        grace_epochs: 7,
    };
    let base_secs = 1_700_000_000;
    let mut manager = EpochManager::new(config.clone(), base_secs);

    // Rotate to version 1 at epoch 0.
    let (sk_a1, vk_a1) = make_key_pair();
    let (_sk_b1, vk_b1) = make_key_pair();
    let (_sk_c1, vk_c1) = make_key_pair();
    let id_a = NodeId("auth-a".into());
    let id_b = NodeId("auth-b".into());
    let id_c = NodeId("auth-c".into());

    manager
        .rotate_keyset(
            base_secs,
            vec![
                (id_a.clone(), vk_a1),
                (id_b.clone(), vk_b1),
                (id_c.clone(), vk_c1),
            ],
        )
        .unwrap();

    // Rotate to version 2 at epoch 3 (3 days later).
    let epoch3_secs = base_secs + 86400 * 3;
    let (_sk_a2, vk_a2) = make_key_pair();
    let (sk_b2, vk_b2) = make_key_pair();
    let (sk_c2, vk_c2) = make_key_pair();

    manager
        .rotate_keyset(
            epoch3_secs,
            vec![
                (id_a.clone(), vk_a2),
                (id_b.clone(), vk_b2),
                (id_c.clone(), vk_c2),
            ],
        )
        .unwrap();

    // Build a certificate with mixed versions:
    // auth-a signs with version 1 (hasn't rotated yet)
    // auth-b signs with version 2 (already rotated)
    // auth-c signs with version 2 (already rotated)
    let mut cert = MajorityCertificate::new(kr.clone(), hlc.clone(), pv, KeysetVersion(2));

    let sig_a = sign_message(&sk_a1, &message);
    cert.add_signature(AuthoritySignature {
        authority_id: id_a.clone(),
        public_key: vk_a1,
        signature: sig_a,
        keyset_version: KeysetVersion(1),
    });

    let sig_b = sign_message(&sk_b2, &message);
    cert.add_signature(AuthoritySignature {
        authority_id: id_b.clone(),
        public_key: vk_b2,
        signature: sig_b,
        keyset_version: KeysetVersion(2),
    });

    let sig_c = sign_message(&sk_c2, &message);
    cert.add_signature(AuthoritySignature {
        authority_id: id_c.clone(),
        public_key: vk_c2,
        signature: sig_c,
        keyset_version: KeysetVersion(2),
    });

    let bundle = ProofBundle {
        key_range: kr,
        frontier_hlc: hlc,
        policy_version: pv,
        contributing_authorities: vec![id_a.clone(), id_b.clone(), id_c.clone()],
        total_authorities: 5,
        certificate: Some(cert),
    };

    // At epoch 5, version 1 (registered at epoch 0, grace 7) is still valid.
    let result = verify_proof_with_registry(
        &bundle,
        manager.registry(),
        manager.current_epoch(base_secs + 86400 * 5),
        manager.config(),
    );
    assert!(
        result.valid,
        "mixed version proof should be valid during grace period"
    );
    assert!(result.has_majority);
    assert_eq!(result.signatures_valid, Some(true));
}

// ---------------------------------------------------------------------------
// Scenario 2: Grace period expiry rejects old keys
// ---------------------------------------------------------------------------

#[test]
fn expired_keyset_rejected_after_grace_period() {
    let kr = sample_kr();
    let hlc = sample_hlc();
    let pv = sample_pv();
    let message = create_certificate_message(&kr, &hlc, &pv);

    let config = EpochConfig {
        duration_secs: 86400,
        grace_epochs: 3,
    };
    let base_secs = 1_700_000_000;
    let mut manager = EpochManager::new(config.clone(), base_secs);

    // Version 1 at epoch 0.
    let (sk_a, vk_a) = make_key_pair();
    let id_a = NodeId("auth-a".into());
    manager
        .rotate_keyset(base_secs, vec![(id_a.clone(), vk_a)])
        .unwrap();

    // Version 2 at epoch 2.
    let (_, vk_new) = make_key_pair();
    manager
        .rotate_keyset(
            base_secs + 86400 * 2,
            vec![(NodeId("auth-b".into()), vk_new)],
        )
        .unwrap();

    // Build a proof signed with version 1 only.
    let mut cert = MajorityCertificate::new(kr.clone(), hlc.clone(), pv, KeysetVersion(1));
    let sig_a = sign_message(&sk_a, &message);
    cert.add_signature(AuthoritySignature {
        authority_id: id_a.clone(),
        public_key: vk_a,
        signature: sig_a,
        keyset_version: KeysetVersion(1),
    });

    let bundle = ProofBundle {
        key_range: kr,
        frontier_hlc: hlc,
        policy_version: pv,
        contributing_authorities: vec![id_a],
        total_authorities: 1,
        certificate: Some(cert),
    };

    // At epoch 3, version 1 (registered at 0, grace 3) is still valid (3 <= 0+3).
    let result = verify_proof_with_registry(&bundle, manager.registry(), 3, manager.config());
    assert!(result.valid, "should be valid at boundary of grace period");

    // At epoch 4, version 1 is expired (4 > 0+3).
    let result = verify_proof_with_registry(&bundle, manager.registry(), 4, manager.config());
    assert!(!result.valid, "should be invalid after grace period expiry");

    // Detailed error should show ExpiredKeyset.
    let err = verify_proof_with_registry_detailed(&bundle, manager.registry(), 4, manager.config())
        .unwrap_err();
    assert!(
        matches!(
            err,
            CertError::ExpiredKeyset {
                version: 1,
                keyset_epoch: 0,
                current_epoch: 4
            }
        ),
        "expected ExpiredKeyset, got: {err:?}"
    );
}

// ---------------------------------------------------------------------------
// Scenario 3: Tampered signature detection with registry
// ---------------------------------------------------------------------------

#[test]
fn tampered_signature_detected_with_registry() {
    let kr = sample_kr();
    let hlc = sample_hlc();
    let pv = sample_pv();
    let message = create_certificate_message(&kr, &hlc, &pv);

    let mut registry = KeysetRegistry::new();

    let (sk_a, vk_a) = make_key_pair();
    let (sk_b, vk_b) = make_key_pair();
    let (_sk_c, vk_c) = make_key_pair();
    let id_a = NodeId("auth-a".into());
    let id_b = NodeId("auth-b".into());
    let id_c = NodeId("auth-c".into());

    registry
        .register_keyset(
            KeysetVersion(1),
            0,
            vec![
                (id_a.clone(), vk_a),
                (id_b.clone(), vk_b),
                (id_c.clone(), vk_c),
            ],
        )
        .unwrap();

    let mut cert = MajorityCertificate::new(kr.clone(), hlc.clone(), pv, KeysetVersion(1));

    // Valid signature from auth-a.
    let sig_a = sign_message(&sk_a, &message);
    cert.add_signature(AuthoritySignature {
        authority_id: id_a.clone(),
        public_key: vk_a,
        signature: sig_a,
        keyset_version: KeysetVersion(1),
    });

    // Valid signature from auth-b.
    let sig_b = sign_message(&sk_b, &message);
    cert.add_signature(AuthoritySignature {
        authority_id: id_b.clone(),
        public_key: vk_b,
        signature: sig_b,
        keyset_version: KeysetVersion(1),
    });

    // TAMPERED: auth-c's signature is from a different key.
    let (sk_fake, _) = make_key_pair();
    let sig_c_fake = sign_message(&sk_fake, &message);
    cert.add_signature(AuthoritySignature {
        authority_id: id_c.clone(),
        public_key: vk_c, // registry will use the real key → mismatch
        signature: sig_c_fake,
        keyset_version: KeysetVersion(1),
    });

    let bundle = ProofBundle {
        key_range: kr,
        frontier_hlc: hlc,
        policy_version: pv,
        contributing_authorities: vec![id_a, id_b, id_c],
        total_authorities: 5,
        certificate: Some(cert),
    };

    let config = EpochConfig::default();
    let result = verify_proof_with_registry(&bundle, &registry, 0, &config);
    assert!(!result.valid, "tampered signature should be detected");
    assert_eq!(result.signatures_valid, Some(false));
}

// ---------------------------------------------------------------------------
// Scenario 4: Full epoch lifecycle — rotate, verify, expire
// ---------------------------------------------------------------------------

#[test]
fn full_epoch_lifecycle() {
    let kr = sample_kr();
    let hlc = sample_hlc();
    let pv = sample_pv();
    let message = create_certificate_message(&kr, &hlc, &pv);

    let config = EpochConfig {
        duration_secs: 3600, // 1h epochs for testing
        grace_epochs: 2,
    };
    let base_secs = 1_000_000;
    let mut manager = EpochManager::new(config.clone(), base_secs);

    // Phase 1: Initial keyset at epoch 0.
    let (sk1, vk1) = make_key_pair();
    let id = NodeId("sole-authority".into());
    manager
        .rotate_keyset(base_secs, vec![(id.clone(), vk1)])
        .unwrap();

    // Sign with version 1.
    let sig1 = sign_message(&sk1, &message);
    let make_bundle = |cert: MajorityCertificate| -> ProofBundle {
        ProofBundle {
            key_range: kr.clone(),
            frontier_hlc: hlc.clone(),
            policy_version: pv,
            contributing_authorities: vec![id.clone()],
            total_authorities: 1,
            certificate: Some(cert),
        }
    };

    let mut cert1 = MajorityCertificate::new(kr.clone(), hlc.clone(), pv, KeysetVersion(1));
    cert1.add_signature(AuthoritySignature {
        authority_id: id.clone(),
        public_key: vk1,
        signature: sig1,
        keyset_version: KeysetVersion(1),
    });
    let bundle1 = make_bundle(cert1);

    // Verify at epoch 0 → valid.
    let r = verify_proof_with_registry(&bundle1, manager.registry(), 0, manager.config());
    assert!(r.valid, "version 1 at epoch 0 should be valid");

    // Phase 2: Rotate to version 2 at epoch 3.
    let (sk2, vk2) = make_key_pair();
    manager
        .rotate_keyset(base_secs + 3600 * 3, vec![(id.clone(), vk2)])
        .unwrap();

    // Version 1 at epoch 3 → still valid (3 <= 0+2? no, 3 > 2 → expired!)
    // Wait, grace_epochs is 2, so version 1 (registered at epoch 0) is valid up to epoch 2.
    let r = verify_proof_with_registry(&bundle1, manager.registry(), 3, manager.config());
    assert!(!r.valid, "version 1 at epoch 3 should be expired (grace=2)");

    // Version 1 at epoch 2 → still valid (2 <= 0+2).
    let r = verify_proof_with_registry(&bundle1, manager.registry(), 2, manager.config());
    assert!(r.valid, "version 1 at epoch 2 should still be valid");

    // Phase 3: Sign with version 2 and verify.
    let sig2 = sign_message(&sk2, &message);
    let mut cert2 = MajorityCertificate::new(kr.clone(), hlc.clone(), pv, KeysetVersion(2));
    cert2.add_signature(AuthoritySignature {
        authority_id: id.clone(),
        public_key: vk2,
        signature: sig2,
        keyset_version: KeysetVersion(2),
    });
    let bundle2 = make_bundle(cert2);

    // Version 2 at epoch 100 → current version, always valid.
    let r = verify_proof_with_registry(&bundle2, manager.registry(), 100, manager.config());
    assert!(r.valid, "current version should always be valid");
}

// ---------------------------------------------------------------------------
// Scenario 5: Backward compatibility — verify_proof works without registry
// ---------------------------------------------------------------------------

#[test]
fn backward_compatibility_verify_proof_without_registry() {
    let kr = sample_kr();
    let hlc = sample_hlc();
    let pv = sample_pv();
    let message = create_certificate_message(&kr, &hlc, &pv);

    let mut cert = MajorityCertificate::new(kr.clone(), hlc.clone(), pv, KeysetVersion(1));

    for i in 0..3 {
        let (sk, vk) = make_key_pair();
        let sig = sign_message(&sk, &message);
        cert.add_signature(AuthoritySignature {
            authority_id: NodeId(format!("auth-{i}")),
            public_key: vk,
            signature: sig,
            keyset_version: KeysetVersion(1),
        });
    }

    let bundle = ProofBundle {
        key_range: kr,
        frontier_hlc: hlc,
        policy_version: pv,
        contributing_authorities: (0..3).map(|i| NodeId(format!("auth-{i}"))).collect(),
        total_authorities: 5,
        certificate: Some(cert),
    };

    // verify_proof (non-registry) should still work.
    let result = verify_proof(&bundle);
    assert!(result.valid);
    assert!(result.has_majority);
    assert_eq!(result.signatures_valid, Some(true));
}

// ---------------------------------------------------------------------------
// Scenario 6: EpochManager tracks multiple rotations
// ---------------------------------------------------------------------------

#[test]
fn epoch_manager_multiple_rotations() {
    let config = EpochConfig {
        duration_secs: 86400,
        grace_epochs: 2,
    };
    let base = 1_000_000;
    let mut manager = EpochManager::new(config, base);

    // Rotate 5 times across different epochs.
    for i in 1..=5u64 {
        let (_, vk) = make_key_pair();
        let time = base + 86400 * (i - 1) * 2; // every 2 epochs
        let v = manager
            .rotate_keyset(time, vec![(NodeId(format!("auth-{i}")), vk)])
            .unwrap();
        assert_eq!(v, KeysetVersion(i));
    }

    assert_eq!(manager.registry().current_version(), KeysetVersion(5));

    // Version 4 was registered at epoch 6 (base + 86400*6).
    // At epoch 8, grace 2 → 8 <= 6+2 → valid.
    assert!(
        manager
            .validate_keyset_version(&KeysetVersion(4), base + 86400 * 8)
            .is_ok()
    );

    // At epoch 9 → 9 > 6+2 → expired.
    assert!(
        manager
            .validate_keyset_version(&KeysetVersion(4), base + 86400 * 9)
            .is_err()
    );
}
