use asteroidb_poc::api::certified::{CertifiedApi, OnTimeout};
use asteroidb_poc::api::status::{CertificationTracker, WriteId};
use asteroidb_poc::authority::ack_frontier::{AckFrontier, AckFrontierSet};
use asteroidb_poc::authority::certificate::{
    AuthoritySignature, EpochConfig, KeysetVersion, MajorityCertificate,
    create_certificate_message, sign_message,
};
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::{Hlc, HlcTimestamp};
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

fn make_frontier(authority: &str, physical: u64, logical: u32, prefix: &str) -> AckFrontier {
    AckFrontier {
        authority_id: node(authority),
        frontier_hlc: HlcTimestamp {
            physical,
            logical,
            node_id: authority.into(),
        },
        key_range: key_range(prefix),
        policy_version: PolicyVersion(1),
        digest_hash: format!("{authority}-{physical}-{logical}"),
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

fn make_key_pair() -> (SigningKey, ed25519_dalek::VerifyingKey) {
    let sk = SigningKey::generate(&mut OsRng);
    let vk = sk.verifying_key();
    (sk, vk)
}

// ---------------------------------------------------------------------------
// Scenario 1: 3 Authority のうち 2 つが ack → majority_frontier が更新される
// ---------------------------------------------------------------------------

#[test]
fn two_of_three_authorities_ack_updates_majority_frontier() {
    let mut frontiers = AckFrontierSet::new();

    // Authority 1 reports frontier at physical=1000
    frontiers.update(make_frontier("auth-1", 1000, 0, ""));
    // Only 1 of 3 → no majority yet
    assert!(frontiers.majority_frontier(3).is_none());

    // Authority 2 reports frontier at physical=1500
    frontiers.update(make_frontier("auth-2", 1500, 0, ""));
    // 2 of 3 → majority reached
    let mf = frontiers.majority_frontier(3).unwrap();
    // Sorted: [1000, 1500], majority=2, index = 2-2 = 0 → 1000
    assert_eq!(mf.physical, 1000);

    // Authority 3 reports frontier at physical=1200
    frontiers.update(make_frontier("auth-3", 1200, 0, ""));
    // Sorted: [1000, 1200, 1500], majority=2, index = 3-2 = 1 → 1200
    let mf = frontiers.majority_frontier(3).unwrap();
    assert_eq!(mf.physical, 1200);
}

// ---------------------------------------------------------------------------
// Scenario 2: majority_frontier 到達後に get_certified が Certified 値を返す
// ---------------------------------------------------------------------------

#[test]
fn get_certified_returns_certified_after_majority_frontier_reached() {
    let mut api = CertifiedApi::new(node("node-1"), 3);

    // Write a value (pending because no frontiers yet)
    let status = api
        .certified_write("user/alice".into(), counter_value(10), OnTimeout::Pending)
        .unwrap();
    assert_eq!(status, CertificationStatus::Pending);

    let write_ts = api.pending_writes()[0].timestamp.physical;

    // Advance 2 of 3 authorities past the write timestamp
    api.update_frontier(make_frontier("auth-1", write_ts + 100, 0, ""));
    api.update_frontier(make_frontier("auth-2", write_ts + 200, 0, ""));

    // Process certifications to promote pending → certified
    api.process_certifications();

    // Now get_certified should return Certified status
    let read = api.get_certified("user/alice");
    assert!(read.value.is_some());
    assert_eq!(read.status, CertificationStatus::Certified);
    assert!(read.frontier.is_some());

    // Verify the actual value
    match read.value.unwrap() {
        CrdtValue::Counter(c) => assert_eq!(c.value(), 10),
        other => panic!("expected Counter, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Scenario 3: majority 未到達時は Pending が返る
// ---------------------------------------------------------------------------

#[test]
fn pending_status_when_majority_not_reached() {
    let mut api = CertifiedApi::new(node("node-1"), 3);

    // Write a value
    api.certified_write("key1".into(), counter_value(5), OnTimeout::Pending)
        .unwrap();

    let write_ts = api.pending_writes()[0].timestamp.physical;

    // Only 1 of 3 authorities reports (not enough for majority)
    api.update_frontier(make_frontier("auth-1", write_ts + 100, 0, ""));
    api.process_certifications();

    // Status should still be Pending
    assert_eq!(
        api.get_certification_status("key1"),
        CertificationStatus::Pending
    );

    let read = api.get_certified("key1");
    assert_eq!(read.status, CertificationStatus::Pending);
    // No majority frontier since only 1 of 3 reported
    assert!(read.frontier.is_none());
}

// ---------------------------------------------------------------------------
// Scenario 4: Authority 1 ノード障害時でも 2/3 majority で certification 成功
// ---------------------------------------------------------------------------

#[test]
fn certification_succeeds_with_one_authority_failure() {
    let mut api = CertifiedApi::new(node("node-1"), 3);

    // Write a value
    let status = api
        .certified_write("data/sensor".into(), counter_value(42), OnTimeout::Pending)
        .unwrap();
    assert_eq!(status, CertificationStatus::Pending);

    let write_ts = api.pending_writes()[0].timestamp.physical;

    // Authority 1 is DOWN (never sends frontier)
    // Authority 2 and 3 report successfully
    api.update_frontier(make_frontier("auth-2", write_ts + 500, 0, ""));
    api.update_frontier(make_frontier("auth-3", write_ts + 300, 0, ""));

    api.process_certifications();

    // 2 of 3 is majority → should be certified
    let read = api.get_certified("data/sensor");
    assert_eq!(read.status, CertificationStatus::Certified);
    assert!(read.frontier.is_some());
    match read.value.unwrap() {
        CrdtValue::Counter(c) => assert_eq!(c.value(), 42),
        other => panic!("expected Counter, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Scenario 5: MajorityCertificate 署名検証 → AckFrontierSet → CertifiedApi
//             の一気通貫フロー
// ---------------------------------------------------------------------------

#[test]
fn end_to_end_certificate_signing_to_certified_api() {
    // Step 1: Generate signing keys for 3 authorities
    let (sk1, vk1) = make_key_pair();
    let (sk2, vk2) = make_key_pair();
    let (_sk3, _vk3) = make_key_pair(); // Authority 3 is slow/down

    let kr = key_range("user/");
    let frontier_hlc = HlcTimestamp {
        physical: 2_000_000_000_000,
        logical: 0,
        node_id: "auth-1".into(),
    };
    let pv = PolicyVersion(1);

    // Step 2: Create certificate message and sign with 2 authorities (majority)
    let message = create_certificate_message(&kr, &frontier_hlc, &pv);

    let mut cert = MajorityCertificate::new(kr.clone(), frontier_hlc.clone(), pv, KeysetVersion(1));

    let sig1 = sign_message(&sk1, &message);
    cert.add_signature(AuthoritySignature {
        authority_id: node("auth-1"),
        public_key: vk1,
        signature: sig1,
    });

    let sig2 = sign_message(&sk2, &message);
    cert.add_signature(AuthoritySignature {
        authority_id: node("auth-2"),
        public_key: vk2,
        signature: sig2,
    });

    // Step 3: Verify the certificate has majority and signatures are valid
    assert!(cert.has_majority(3)); // 2 >= 3/2+1 = 2
    let valid_signers = cert.verify_signatures(&message).unwrap();
    assert_eq!(valid_signers.len(), 2);
    assert_eq!(valid_signers[0], node("auth-1"));
    assert_eq!(valid_signers[1], node("auth-2"));

    // Step 4: Use the certified frontier to update AckFrontierSet
    let mut frontier_set = AckFrontierSet::new();
    for signer in &valid_signers {
        frontier_set.update(AckFrontier {
            authority_id: signer.clone(),
            frontier_hlc: cert.frontier_hlc.clone(),
            key_range: cert.key_range.clone(),
            policy_version: pv,
            digest_hash: format!("{}-certified", signer.0),
        });
    }

    // Verify majority frontier is available
    assert!(frontier_set.majority_frontier(3).is_some());

    // Step 5: Feed into CertifiedApi
    let mut api = CertifiedApi::new(node("client-1"), 3);

    // Write a value with a timestamp that should be <= the certified frontier
    api.certified_write("user/data".into(), counter_value(100), OnTimeout::Pending)
        .unwrap();

    // Update frontiers from the certificate verification
    for signer in &valid_signers {
        api.update_frontier(AckFrontier {
            authority_id: signer.clone(),
            frontier_hlc: cert.frontier_hlc.clone(),
            key_range: cert.key_range.clone(),
            policy_version: pv,
            digest_hash: format!("{}-certified", signer.0),
        });
    }

    api.process_certifications();

    // The write should now be certified (its timestamp is well below the frontier)
    let read = api.get_certified("user/data");
    assert_eq!(read.status, CertificationStatus::Certified);
    assert!(read.value.is_some());
}

// ---------------------------------------------------------------------------
// Scenario 6: certified_write → ack_frontier 更新 → certificate 発行 →
//             get_certified の完全フロー
// ---------------------------------------------------------------------------

#[test]
fn full_flow_write_frontier_certificate_read() {
    let mut api = CertifiedApi::new(node("node-1"), 3);

    // Phase 1: certified_write → Pending
    let status = api
        .certified_write("sensor/temp".into(), counter_value(25), OnTimeout::Pending)
        .unwrap();
    assert_eq!(status, CertificationStatus::Pending);

    let write_ts = api.pending_writes()[0].timestamp.clone();

    // Phase 2: Simulate authority ack_frontier updates
    // Each authority processes the write and advances their frontier
    let frontier_physical = write_ts.physical + 1000;

    api.update_frontier(make_frontier("auth-1", frontier_physical, 0, ""));
    api.update_frontier(make_frontier("auth-2", frontier_physical + 500, 0, ""));
    // auth-3 not yet reported

    // Phase 3: Meanwhile, create a MajorityCertificate for verification
    let (sk1, vk1) = make_key_pair();
    let (sk2, vk2) = make_key_pair();

    let kr = key_range("sensor/");
    let cert_frontier = HlcTimestamp {
        physical: frontier_physical,
        logical: 0,
        node_id: "auth-1".into(),
    };
    let pv = PolicyVersion(1);
    let message = create_certificate_message(&kr, &cert_frontier, &pv);

    let mut cert = MajorityCertificate::new(kr, cert_frontier, pv, KeysetVersion(1));

    cert.add_signature(AuthoritySignature {
        authority_id: node("auth-1"),
        public_key: vk1,
        signature: sign_message(&sk1, &message),
    });
    cert.add_signature(AuthoritySignature {
        authority_id: node("auth-2"),
        public_key: vk2,
        signature: sign_message(&sk2, &message),
    });

    // Verify certificate
    assert!(cert.has_majority(3));
    assert!(cert.verify_signatures(&message).is_ok());

    // Phase 4: Process certifications
    api.process_certifications();

    // Phase 5: get_certified should return Certified
    let read = api.get_certified("sensor/temp");
    assert_eq!(read.status, CertificationStatus::Certified);
    assert!(read.value.is_some());
    assert!(read.frontier.is_some());

    match read.value.unwrap() {
        CrdtValue::Counter(c) => assert_eq!(c.value(), 25),
        other => panic!("expected Counter, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Scenario 7: epoch 境界をまたぐ certification フロー
// ---------------------------------------------------------------------------

#[test]
fn certification_across_epoch_boundary() {
    let epoch_config = EpochConfig::default();
    assert_eq!(epoch_config.duration_secs, 86400); // 24h
    assert_eq!(epoch_config.grace_epochs, 7);

    // Simulate writes happening across epoch boundaries
    let epoch_ms = epoch_config.duration_secs * 1000;

    // Epoch 1: Write occurs at the end of epoch 1
    let mut api = CertifiedApi::new(node("node-1"), 3);
    api.certified_write(
        "cross-epoch/data".into(),
        counter_value(1),
        OnTimeout::Pending,
    )
    .unwrap();

    let write_ts = api.pending_writes()[0].timestamp.physical;

    // Epoch 2: Authority frontiers advance into the next epoch
    let next_epoch_ts = write_ts + epoch_ms;

    api.update_frontier(make_frontier("auth-1", next_epoch_ts, 0, ""));
    api.update_frontier(make_frontier("auth-2", next_epoch_ts + 1000, 0, ""));

    api.process_certifications();

    // Even though we crossed an epoch boundary, the write should be certified
    // because the frontier has advanced past the write timestamp
    let read = api.get_certified("cross-epoch/data");
    assert_eq!(read.status, CertificationStatus::Certified);

    // Verify keyset versioning across epochs
    let (sk_epoch1, vk_epoch1) = make_key_pair();
    let (sk_epoch2, vk_epoch2) = make_key_pair();

    let kr = key_range("cross-epoch/");
    let pv = PolicyVersion(1);

    // Certificate from epoch 1
    let frontier_epoch1 = HlcTimestamp {
        physical: write_ts + 500,
        logical: 0,
        node_id: "auth-1".into(),
    };
    let msg1 = create_certificate_message(&kr, &frontier_epoch1, &pv);
    let mut cert_epoch1 =
        MajorityCertificate::new(kr.clone(), frontier_epoch1, pv, KeysetVersion(1));

    cert_epoch1.add_signature(AuthoritySignature {
        authority_id: node("auth-1"),
        public_key: vk_epoch1,
        signature: sign_message(&sk_epoch1, &msg1),
    });

    // Certificate from epoch 2 with new keyset version
    let frontier_epoch2 = HlcTimestamp {
        physical: next_epoch_ts,
        logical: 0,
        node_id: "auth-2".into(),
    };
    let msg2 = create_certificate_message(&kr, &frontier_epoch2, &pv);
    let mut cert_epoch2 = MajorityCertificate::new(kr, frontier_epoch2, pv, KeysetVersion(2));

    cert_epoch2.add_signature(AuthoritySignature {
        authority_id: node("auth-2"),
        public_key: vk_epoch2,
        signature: sign_message(&sk_epoch2, &msg2),
    });

    // Both certificates should be independently verifiable
    assert!(cert_epoch1.verify_signatures(&msg1).is_ok());
    assert!(cert_epoch2.verify_signatures(&msg2).is_ok());

    // KeysetVersion should be monotonically increasing
    assert!(cert_epoch1.keyset_version < cert_epoch2.keyset_version);
}

// ---------------------------------------------------------------------------
// Additional integration scenarios
// ---------------------------------------------------------------------------

/// Multiple writes with progressive certification
#[test]
fn progressive_certification_of_multiple_writes() {
    let mut api = CertifiedApi::new(node("node-1"), 3);

    // Write three values at different times
    api.certified_write("key-a".into(), counter_value(1), OnTimeout::Pending)
        .unwrap();
    let ts_a = api.pending_writes()[0].timestamp.physical;

    api.certified_write("key-b".into(), counter_value(2), OnTimeout::Pending)
        .unwrap();
    let ts_b = api.pending_writes()[1].timestamp.physical;

    api.certified_write("key-c".into(), counter_value(3), OnTimeout::Pending)
        .unwrap();

    // All three should be pending
    assert_eq!(
        api.get_certification_status("key-a"),
        CertificationStatus::Pending
    );
    assert_eq!(
        api.get_certification_status("key-b"),
        CertificationStatus::Pending
    );
    assert_eq!(
        api.get_certification_status("key-c"),
        CertificationStatus::Pending
    );

    // Advance frontiers to certify only key-a (frontier between ts_a and ts_b)
    let frontier_between = ts_a + 1;
    // Only advance if ts_b is actually after ts_a + 1
    // Due to HLC monotonicity, ts_b >= ts_a
    api.update_frontier(make_frontier("auth-1", frontier_between, 0, ""));
    api.update_frontier(make_frontier("auth-2", frontier_between, 0, ""));
    api.process_certifications();

    assert_eq!(
        api.get_certification_status("key-a"),
        CertificationStatus::Certified
    );
    // key-b and key-c may or may not be certified depending on timestamp ordering
    // (they could share the same physical timestamp due to logical counter)

    // Now advance frontier past all writes
    let far_future = ts_b + 100_000;
    api.update_frontier(make_frontier("auth-1", far_future, 0, ""));
    api.update_frontier(make_frontier("auth-2", far_future, 0, ""));
    api.update_frontier(make_frontier("auth-3", far_future, 0, ""));
    api.process_certifications();

    // All should be certified now
    assert_eq!(
        api.get_certification_status("key-a"),
        CertificationStatus::Certified
    );
    assert_eq!(
        api.get_certification_status("key-b"),
        CertificationStatus::Certified
    );
    assert_eq!(
        api.get_certification_status("key-c"),
        CertificationStatus::Certified
    );
}

/// OnTimeout::Error returns error but write is still tracked
#[test]
fn on_timeout_error_still_tracks_write() {
    let mut api = CertifiedApi::new(node("node-1"), 3);

    let result = api.certified_write("key1".into(), counter_value(7), OnTimeout::Error);
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err(),
        asteroidb_poc::error::CrdtError::Timeout
    );

    // Despite error, write is in the store and tracked as pending
    let read = api.get_certified("key1");
    assert!(read.value.is_some());
    assert_eq!(read.status, CertificationStatus::Pending);
    assert_eq!(api.pending_writes().len(), 1);

    // Can still be certified later via frontier updates
    let write_ts = api.pending_writes()[0].timestamp.physical;
    api.update_frontier(make_frontier("auth-1", write_ts + 100, 0, ""));
    api.update_frontier(make_frontier("auth-2", write_ts + 200, 0, ""));
    api.process_certifications();

    assert_eq!(
        api.get_certification_status("key1"),
        CertificationStatus::Certified
    );
}

/// CertificationTracker + AckFrontierSet combined flow
#[test]
fn certification_tracker_with_frontier_set() {
    let mut tracker = CertificationTracker::new();
    let mut frontier_set = AckFrontierSet::new();

    let write_ts = HlcTimestamp {
        physical: 5000,
        logical: 0,
        node_id: "node-1".into(),
    };
    let write_id = WriteId {
        key: "tracked-key".into(),
        timestamp: write_ts.clone(),
    };

    // Register a write requiring majority of 3 (threshold = 2)
    tracker.register_write(write_id.clone(), 2, write_ts.clone());
    assert_eq!(
        tracker.get_status(&write_id),
        Some(CertificationStatus::Pending)
    );

    // Authority 1 acks
    frontier_set.update(make_frontier("auth-1", 6000, 0, ""));
    let ack1_ts = HlcTimestamp {
        physical: 6000,
        logical: 0,
        node_id: "auth-1".into(),
    };
    tracker.record_ack(&write_id, ack1_ts);

    // Check: 1 ack, not enough
    assert_eq!(
        tracker.get_status(&write_id),
        Some(CertificationStatus::Pending)
    );
    assert!(!frontier_set.is_certified_at(&write_ts, 3));

    // Authority 2 acks
    frontier_set.update(make_frontier("auth-2", 6500, 0, ""));
    let ack2_ts = HlcTimestamp {
        physical: 6500,
        logical: 0,
        node_id: "auth-2".into(),
    };
    let status = tracker.record_ack(&write_id, ack2_ts);

    // Now both tracker and frontier_set agree: certified
    assert_eq!(status, Some(CertificationStatus::Certified));
    assert!(frontier_set.is_certified_at(&write_ts, 3));
}

/// Store write is visible immediately even before certification
#[test]
fn store_value_visible_before_certification() {
    let mut api = CertifiedApi::new(node("node-1"), 3);

    api.certified_write("immediate".into(), counter_value(99), OnTimeout::Pending)
        .unwrap();

    // Value should be readable even before certification (eventual consistency)
    let read = api.get_certified("immediate");
    assert!(read.value.is_some());
    match read.value.unwrap() {
        CrdtValue::Counter(c) => assert_eq!(c.value(), 99),
        other => panic!("expected Counter, got {:?}", other),
    }
    // But status should be pending
    assert_eq!(read.status, CertificationStatus::Pending);
}

/// Frontier regression is prevented
#[test]
fn frontier_regression_prevented_in_certified_flow() {
    let mut api = CertifiedApi::new(node("node-1"), 3);

    api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
        .unwrap();
    let write_ts = api.pending_writes()[0].timestamp.physical;

    // Advance auth-1 to a high frontier
    api.update_frontier(make_frontier("auth-1", write_ts + 1000, 0, ""));
    api.update_frontier(make_frontier("auth-2", write_ts + 500, 0, ""));
    api.process_certifications();
    assert_eq!(
        api.get_certification_status("key1"),
        CertificationStatus::Certified
    );

    // Try to regress auth-1's frontier (should be ignored)
    api.update_frontier(make_frontier("auth-1", write_ts - 1000, 0, ""));

    // Write a new value
    api.certified_write("key2".into(), counter_value(2), OnTimeout::Pending)
        .unwrap();
    let write_ts2 = api.pending_writes().last().unwrap().timestamp.physical;

    // The regression should not have affected auth-1's frontier
    // So we only need auth-2 to advance to certify key2
    api.update_frontier(make_frontier("auth-2", write_ts2 + 100, 0, ""));
    api.process_certifications();

    // key2 should be certified because auth-1 still has its high frontier
    assert_eq!(
        api.get_certification_status("key2"),
        CertificationStatus::Certified
    );
}

/// MajorityCertificate with invalid signature is detected
#[test]
fn invalid_signature_detected_in_certificate() {
    let (sk1, vk1) = make_key_pair();
    let (sk2, _vk2) = make_key_pair();
    let (_sk3, vk3) = make_key_pair(); // mismatched key

    let kr = key_range("secure/");
    let frontier = HlcTimestamp {
        physical: 1_000_000,
        logical: 0,
        node_id: "auth-1".into(),
    };
    let pv = PolicyVersion(1);
    let message = create_certificate_message(&kr, &frontier, &pv);

    let mut cert = MajorityCertificate::new(kr, frontier, pv, KeysetVersion(1));

    // Valid signature from auth-1
    cert.add_signature(AuthoritySignature {
        authority_id: node("auth-1"),
        public_key: vk1,
        signature: sign_message(&sk1, &message),
    });

    // Invalid signature: signed by sk2 but presented with vk3
    cert.add_signature(AuthoritySignature {
        authority_id: node("auth-2"),
        public_key: vk3,
        signature: sign_message(&sk2, &message),
    });

    // Has 2 signatures (majority) but verification should fail
    assert!(cert.has_majority(3));
    let result = cert.verify_signatures(&message);
    assert!(result.is_err());
}

/// Five-authority cluster: need 3 for majority
#[test]
fn five_authority_majority_certification() {
    let mut api = CertifiedApi::new(node("node-1"), 5);

    api.certified_write("five-auth".into(), counter_value(50), OnTimeout::Pending)
        .unwrap();
    let write_ts = api.pending_writes()[0].timestamp.physical;

    // 2 of 5 → not enough
    api.update_frontier(make_frontier("auth-1", write_ts + 100, 0, ""));
    api.update_frontier(make_frontier("auth-2", write_ts + 200, 0, ""));
    api.process_certifications();
    assert_eq!(
        api.get_certification_status("five-auth"),
        CertificationStatus::Pending
    );

    // 3 of 5 → majority
    api.update_frontier(make_frontier("auth-3", write_ts + 150, 0, ""));
    api.process_certifications();
    assert_eq!(
        api.get_certification_status("five-auth"),
        CertificationStatus::Certified
    );
}

/// HLC timestamp ordering in the certification flow
#[test]
fn hlc_ordering_in_certification() {
    let mut clock = Hlc::new("node-1".into());

    // Generate ordered timestamps
    let t1 = clock.now();
    let t2 = clock.now();
    let t3 = clock.now();

    // Verify ordering
    assert!(t1 < t2);
    assert!(t2 < t3);

    // Use these in a certification flow
    let mut api = CertifiedApi::new(node("node-1"), 3);

    api.certified_write("ordered/first".into(), counter_value(1), OnTimeout::Pending)
        .unwrap();
    api.certified_write(
        "ordered/second".into(),
        counter_value(2),
        OnTimeout::Pending,
    )
    .unwrap();

    let ts_first = api.pending_writes()[0].timestamp.clone();
    let ts_second = api.pending_writes()[1].timestamp.clone();

    // First write should have earlier timestamp
    assert!(ts_first < ts_second);

    // Advance frontier between the two writes
    // Only first write should be certified
    api.update_frontier(AckFrontier {
        authority_id: node("auth-1"),
        frontier_hlc: ts_first.clone(),
        key_range: key_range(""),
        policy_version: PolicyVersion(1),
        digest_hash: "auth-1-frontier".into(),
    });
    api.update_frontier(AckFrontier {
        authority_id: node("auth-2"),
        frontier_hlc: ts_first.clone(),
        key_range: key_range(""),
        policy_version: PolicyVersion(1),
        digest_hash: "auth-2-frontier".into(),
    });

    api.process_certifications();

    assert_eq!(
        api.get_certification_status("ordered/first"),
        CertificationStatus::Certified
    );
    assert_eq!(
        api.get_certification_status("ordered/second"),
        CertificationStatus::Pending
    );
}

/// CertificationTracker timeout flow
#[test]
fn certification_tracker_timeout_flow() {
    let mut tracker = CertificationTracker::with_timeout(5000);

    let write_ts = HlcTimestamp {
        physical: 1000,
        logical: 0,
        node_id: "node-1".into(),
    };
    let wid = WriteId {
        key: "timeout-test".into(),
        timestamp: write_ts.clone(),
    };

    tracker.register_write(wid.clone(), 2, write_ts);

    // Before timeout: still pending
    let before = HlcTimestamp {
        physical: 5999,
        logical: 0,
        node_id: "node-1".into(),
    };
    tracker.check_timeouts(&before);
    assert_eq!(tracker.get_status(&wid), Some(CertificationStatus::Pending));

    // At timeout boundary: should timeout
    let at_timeout = HlcTimestamp {
        physical: 6000,
        logical: 0,
        node_id: "node-1".into(),
    };
    tracker.check_timeouts(&at_timeout);
    assert_eq!(tracker.get_status(&wid), Some(CertificationStatus::Timeout));
}

/// Overwriting a key and certifying the latest write
#[test]
fn overwrite_and_certify_latest() {
    let mut api = CertifiedApi::new(node("node-1"), 3);

    // Write first version
    api.certified_write("mutable".into(), counter_value(1), OnTimeout::Pending)
        .unwrap();

    // Overwrite with second version
    api.certified_write("mutable".into(), counter_value(100), OnTimeout::Pending)
        .unwrap();

    // The store should have the latest value
    let read = api.get_certified("mutable");
    match read.value.unwrap() {
        CrdtValue::Counter(c) => assert_eq!(c.value(), 100),
        other => panic!("expected Counter, got {:?}", other),
    }

    // Certify by advancing frontier past both writes
    let latest_ts = api.pending_writes().last().unwrap().timestamp.physical;
    api.update_frontier(make_frontier("auth-1", latest_ts + 100, 0, ""));
    api.update_frontier(make_frontier("auth-2", latest_ts + 200, 0, ""));
    api.process_certifications();

    // get_certified returns status of the latest write for that key
    let read = api.get_certified("mutable");
    assert_eq!(read.status, CertificationStatus::Certified);
}
