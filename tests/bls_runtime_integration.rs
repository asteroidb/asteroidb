#![cfg(feature = "native-crypto")]
//! Integration tests: BLS signature and epoch manager integration in runtime (#208).
//!
//! Validates that:
//! 1. `EpochManager` rotates epochs correctly when embedded in `NodeRunner`.
//! 2. When BLS keys are registered, certificates use BLS mode.
//! 3. Backward compat: no BLS config results in Ed25519 certificates only.
//! 4. The runtime epoch check tick fires and drives rotation.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use asteroidb_poc::api::certified::{CertifiedApi, OnTimeout};
use asteroidb_poc::authority::ack_frontier::AckFrontier;
use asteroidb_poc::authority::bls::BlsKeypair;
use asteroidb_poc::authority::certificate::{
    CertificateMode, DualModeCertificate, EpochConfig, EpochManager, KeysetVersion,
    create_certificate_message,
};
use asteroidb_poc::compaction::CompactionEngine;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::ops::metrics::RuntimeMetrics;
use asteroidb_poc::placement::PlacementPolicy;
use asteroidb_poc::runtime::{BlsConfig, NodeRunner, NodeRunnerConfig};
use asteroidb_poc::store::kv::CrdtValue;
use asteroidb_poc::types::{CertificationStatus, KeyRange, NodeId, PolicyVersion};
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

fn three_authority_namespace() -> SystemNamespace {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: kr(""),
        authority_nodes: vec![node_id("auth-1"), node_id("auth-2"), node_id("auth-3")],
        auto_generated: false,
    });
    ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr(""), 3));
    ns
}

fn wrap_ns(ns: SystemNamespace) -> Arc<RwLock<SystemNamespace>> {
    Arc::new(RwLock::new(ns))
}

fn fast_config() -> NodeRunnerConfig {
    NodeRunnerConfig {
        certification_interval: Duration::from_millis(10),
        cleanup_interval: Duration::from_secs(60),
        compaction_check_interval: Duration::from_secs(60),
        frontier_report_interval: Duration::from_millis(10),
        sync_interval: None,
        ping_interval: None,
        epoch_check_interval: Duration::from_millis(10),
        gc_interval: Duration::from_secs(60),
        epoch_config: EpochConfig::default(),
        bls_config: None,
        ..Default::default()
    }
}

fn fast_config_with_bls(seed: u8) -> NodeRunnerConfig {
    let mut seed_bytes = [0u8; 32];
    seed_bytes[0] = seed;
    seed_bytes[31] = seed.wrapping_add(42);
    NodeRunnerConfig {
        bls_config: Some(BlsConfig { seed: seed_bytes }),
        ..fast_config()
    }
}

// ---------------------------------------------------------------------------
// Test 1: EpochManager rotates epochs correctly in runtime context
// ---------------------------------------------------------------------------

#[test]
fn epoch_manager_rotates_when_epoch_boundary_crossed() {
    // Use short 10-second epochs for testing.
    let config = EpochConfig {
        duration_secs: 10,
        grace_epochs: 2,
    };
    let base_secs = 1_000_000;
    let mut manager = EpochManager::new(config, base_secs);

    // Stage keys for first rotation.
    let keys_v1 = vec![
        (
            node_id("auth-1"),
            ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng).verifying_key(),
        ),
        (
            node_id("auth-2"),
            ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng).verifying_key(),
        ),
    ];
    manager.stage_keys(keys_v1);

    // At epoch 0 (base_secs), check_and_rotate should trigger first rotation
    // since no rotation has happened yet.
    let result = manager.check_and_rotate(base_secs * 1000);
    assert!(result.is_some(), "first rotation should happen at epoch 0");
    let event = result.unwrap();
    assert_eq!(event.new_version, KeysetVersion(1));
    assert_eq!(event.epoch, 0);
    assert_eq!(manager.rotation_count(), 1);

    // Stage keys for second rotation.
    let keys_v2 = vec![(
        node_id("auth-1"),
        ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng).verifying_key(),
    )];
    manager.stage_keys(keys_v2);

    // Still in epoch 0: no rotation should happen.
    let result = manager.check_and_rotate(base_secs * 1000 + 5000);
    assert!(result.is_none(), "no rotation within same epoch");
    assert_eq!(manager.rotation_count(), 1);

    // Advance to epoch 1 (10 seconds later).
    let epoch1_ms = (base_secs + 10) * 1000;
    let result = manager.check_and_rotate(epoch1_ms);
    assert!(result.is_some(), "rotation at epoch boundary");
    let event = result.unwrap();
    assert_eq!(event.new_version, KeysetVersion(2));
    assert_eq!(event.epoch, 1);
    assert_eq!(manager.rotation_count(), 2);
}

#[test]
fn epoch_manager_cleanup_stale_keysets() {
    let config = EpochConfig {
        duration_secs: 10,
        grace_epochs: 2,
    };
    let base_secs = 1_000_000;
    let mut manager = EpochManager::new(config, base_secs);

    // Rotate at epoch 0.
    let keys = vec![(
        node_id("auth-1"),
        ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng).verifying_key(),
    )];
    manager.stage_keys(keys);
    manager.check_and_rotate(base_secs * 1000);
    assert_eq!(manager.rotation_count(), 1);

    // Rotate at epoch 1.
    let keys = vec![(
        node_id("auth-2"),
        ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng).verifying_key(),
    )];
    manager.stage_keys(keys);
    manager.check_and_rotate((base_secs + 10) * 1000);
    assert_eq!(manager.rotation_count(), 2);

    // Rotate at epoch 5 (well past grace period for version 1 which was epoch 0).
    let keys = vec![(
        node_id("auth-3"),
        ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng).verifying_key(),
    )];
    manager.stage_keys(keys);
    let result = manager.check_and_rotate((base_secs + 50) * 1000);
    assert!(result.is_some());
    let event = result.unwrap();
    // Version 1 (epoch 0) should be cleaned up: epoch 5 > 0 + 2 (grace).
    assert!(
        event.cleaned_versions.contains(&1),
        "version 1 should be cleaned up at epoch 5 with grace_epochs=2"
    );
}

// ---------------------------------------------------------------------------
// Test 2: BLS keys registered → certificates use BLS mode
// ---------------------------------------------------------------------------

#[test]
fn bls_certificate_mode_when_keys_registered() {
    let msg = b"certified-data";

    // Generate 3 BLS keypairs.
    let kp1 = BlsKeypair::generate(&[1u8; 32]);
    let kp2 = BlsKeypair::generate(&[2u8; 32]);
    let kp3 = BlsKeypair::generate(&[3u8; 32]);

    let kr = KeyRange {
        prefix: "test/".into(),
    };
    let hlc = HlcTimestamp {
        physical: 1_700_000_000_000,
        logical: 0,
        node_id: "node-1".into(),
    };
    let pv = PolicyVersion(1);

    // Create a BLS-mode DualModeCertificate.
    let mut cert = DualModeCertificate::new_bls(kr.clone(), hlc.clone(), pv, KeysetVersion(1));

    // Sign with all three.
    let sig1 = asteroidb_poc::authority::bls::sign_message(kp1.secret_key(), msg);
    let sig2 = asteroidb_poc::authority::bls::sign_message(kp2.secret_key(), msg);
    let sig3 = asteroidb_poc::authority::bls::sign_message(kp3.secret_key(), msg);

    let agg = asteroidb_poc::authority::bls::aggregate_signatures(&[sig1, sig2, sig3]).unwrap();

    cert.set_bls_aggregate(
        vec![
            (node_id("auth-1"), kp1.public_key.clone()),
            (node_id("auth-2"), kp2.public_key.clone()),
            (node_id("auth-3"), kp3.public_key.clone()),
        ],
        agg,
    );

    assert_eq!(cert.mode, CertificateMode::Bls);
    assert_eq!(cert.signer_count(), 3);
    assert!(cert.has_majority(3));

    // Verify the certificate.
    let valid_signers = cert.verify(msg).unwrap();
    assert_eq!(valid_signers.len(), 3);
}

#[test]
fn bls_certificate_with_registry_verification() {
    let msg = b"registry-verified";

    let kp1 = BlsKeypair::generate(&[10u8; 32]);
    let kp2 = BlsKeypair::generate(&[11u8; 32]);

    let config = EpochConfig {
        duration_secs: 86400,
        grace_epochs: 7,
    };
    let mut manager = EpochManager::new(config, 1_700_000_000);

    // Register Ed25519 keys (required for keyset creation).
    let vk1 = ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng).verifying_key();
    let vk2 = ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng).verifying_key();
    manager
        .rotate_keyset(
            1_700_000_000,
            vec![(node_id("auth-1"), vk1), (node_id("auth-2"), vk2)],
        )
        .unwrap();

    // Register BLS keys for the same keyset version.
    let version = manager.registry().current_version();
    manager
        .registry_mut()
        .register_bls_keys(
            &version,
            vec![
                ("auth-1".into(), kp1.public_key.clone()),
                ("auth-2".into(), kp2.public_key.clone()),
            ],
        )
        .unwrap();

    // Create and populate a BLS certificate.
    let kr = KeyRange {
        prefix: "data/".into(),
    };
    let hlc = HlcTimestamp {
        physical: 1_700_000_000_000,
        logical: 0,
        node_id: "node-1".into(),
    };
    let pv = PolicyVersion(1);

    let mut cert = DualModeCertificate::new_bls(kr.clone(), hlc.clone(), pv, version.clone());
    let sig1 = asteroidb_poc::authority::bls::sign_message(kp1.secret_key(), msg);
    let sig2 = asteroidb_poc::authority::bls::sign_message(kp2.secret_key(), msg);
    let agg = asteroidb_poc::authority::bls::aggregate_signatures(&[sig1, sig2]).unwrap();
    cert.set_bls_aggregate(
        vec![
            (node_id("auth-1"), kp1.public_key.clone()),
            (node_id("auth-2"), kp2.public_key.clone()),
        ],
        agg,
    );

    // Verify with registry.
    let valid = cert
        .verify_with_registry(msg, manager.registry(), 0, manager.config())
        .unwrap();
    assert_eq!(valid.len(), 2);
}

// ---------------------------------------------------------------------------
// Test 3: Backward compat — no BLS config results in Ed25519 only
// ---------------------------------------------------------------------------

#[tokio::test]
async fn no_bls_config_uses_ed25519_mode() {
    let ns = wrap_ns(three_authority_namespace());
    let api = CertifiedApi::new(node_id("auth-1"), ns);
    let shared_api = Arc::new(Mutex::new(api));
    let metrics = Arc::new(RuntimeMetrics::default());
    let engine = CompactionEngine::with_defaults();

    let config = fast_config(); // No BLS config.
    let runner = NodeRunner::new(node_id("auth-1"), shared_api, engine, config, metrics).await;

    assert!(!runner.has_bls_keys(), "no BLS keys without config");
    assert_eq!(
        runner.certificate_mode(),
        CertificateMode::Ed25519,
        "default mode should be Ed25519"
    );
}

#[tokio::test]
async fn bls_config_enables_bls_keypair() {
    let ns = wrap_ns(three_authority_namespace());
    let api = CertifiedApi::new(node_id("auth-1"), ns);
    let shared_api = Arc::new(Mutex::new(api));
    let metrics = Arc::new(RuntimeMetrics::default());
    let engine = CompactionEngine::with_defaults();

    let config = fast_config_with_bls(42);
    let runner = NodeRunner::new(node_id("auth-1"), shared_api, engine, config, metrics).await;

    assert!(runner.has_bls_keys(), "BLS keys should be present");
    // Without registered keys in the epoch manager's registry, mode is still Ed25519.
    assert_eq!(
        runner.certificate_mode(),
        CertificateMode::Ed25519,
        "mode is Ed25519 until BLS keys are registered in keyset registry"
    );
}

#[tokio::test]
async fn bls_mode_after_registry_registration() {
    let ns = wrap_ns(three_authority_namespace());
    let api = CertifiedApi::new(node_id("auth-1"), ns);
    let shared_api = Arc::new(Mutex::new(api));
    let metrics = Arc::new(RuntimeMetrics::default());
    let engine = CompactionEngine::with_defaults();

    let config = fast_config_with_bls(99);
    let mut runner = NodeRunner::new(node_id("auth-1"), shared_api, engine, config, metrics).await;

    // Register Ed25519 keys to create a keyset version.
    let vk = ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng).verifying_key();
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    runner
        .epoch_manager_mut()
        .rotate_keyset(now_secs, vec![(node_id("auth-1"), vk)])
        .unwrap();

    // Register BLS key for this node.
    let bls_pk = runner.bls_keypair().unwrap().public_key.clone();
    let version = runner.epoch_manager().registry().current_version();
    runner
        .epoch_manager_mut()
        .registry_mut()
        .register_bls_keys(&version, vec![("auth-1".into(), bls_pk)])
        .unwrap();

    assert_eq!(
        runner.certificate_mode(),
        CertificateMode::Bls,
        "mode should be BLS after registering keys"
    );
}

// ---------------------------------------------------------------------------
// Test 4: Runtime epoch check tick fires and updates epoch manager
// ---------------------------------------------------------------------------

#[tokio::test]
async fn epoch_check_tick_fires_in_run_loop() {
    let ns = wrap_ns(three_authority_namespace());
    let api = CertifiedApi::new(node_id("auth-1"), ns);
    let shared_api = Arc::new(Mutex::new(api));
    let metrics = Arc::new(RuntimeMetrics::default());
    let engine = CompactionEngine::with_defaults();

    let config = NodeRunnerConfig {
        certification_interval: Duration::from_millis(10),
        cleanup_interval: Duration::from_secs(60),
        compaction_check_interval: Duration::from_secs(60),
        frontier_report_interval: Duration::from_millis(10),
        sync_interval: None,
        ping_interval: None,
        epoch_check_interval: Duration::from_millis(10),
        gc_interval: Duration::from_secs(60),
        epoch_config: EpochConfig {
            duration_secs: 86400,
            grace_epochs: 7,
        },
        bls_config: None,
        ..Default::default()
    };

    let mut runner = NodeRunner::new(
        node_id("auth-1"),
        shared_api.clone(),
        engine,
        config,
        metrics,
    )
    .await;

    let handle = runner.shutdown_handle();

    // Run for a short time and verify epoch check ticks fire.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = handle.send(true);
    });

    let stats = runner.run().await;
    assert!(
        stats.epoch_check_ticks > 0,
        "epoch check ticks should have fired: got {}",
        stats.epoch_check_ticks
    );
}

// ---------------------------------------------------------------------------
// Test 5: E2E: certification still works with default Ed25519 (backward compat)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn certification_works_with_ed25519_default() {
    let ns = wrap_ns(three_authority_namespace());
    let mut api = CertifiedApi::new(node_id("auth-1"), ns.clone());

    // Write a certified entry.
    api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
        .unwrap();
    let write_ts = api.pending_writes()[0].timestamp.physical;

    // Advance 2 of 3 authorities past the write timestamp (majority).
    api.update_frontier(make_frontier("auth-1", write_ts + 100, ""));
    api.update_frontier(make_frontier("auth-2", write_ts + 200, ""));

    let shared_api = Arc::new(Mutex::new(api));
    let metrics = Arc::new(RuntimeMetrics::default());
    let engine = CompactionEngine::with_defaults();

    // Use default config (no BLS).
    let config = fast_config();
    let mut runner = NodeRunner::new(
        node_id("auth-1"),
        shared_api.clone(),
        engine,
        config,
        metrics,
    )
    .await;

    let handle = runner.shutdown_handle();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = handle.send(true);
    });

    let _stats = runner.run().await;

    // Verify the write was certified.
    let api = shared_api.lock().await;
    assert_eq!(
        api.get_certification_status("key1"),
        CertificationStatus::Certified,
        "write should be certified with Ed25519 default"
    );
}

// ---------------------------------------------------------------------------
// Test 6: DualModeCertificate Ed25519 fallback when BLS not available
// ---------------------------------------------------------------------------

#[test]
fn dual_mode_ed25519_fallback_works() {
    let kr = KeyRange {
        prefix: "test/".into(),
    };
    let hlc = HlcTimestamp {
        physical: 1_700_000_000_000,
        logical: 0,
        node_id: "node-1".into(),
    };
    let pv = PolicyVersion(1);
    let message = create_certificate_message(&kr, &hlc, &pv);

    // Create Ed25519 certificate (the fallback mode).
    let mut cert = DualModeCertificate::new_ed25519(kr, hlc, pv, KeysetVersion(1));

    let sk = ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng);
    let vk = sk.verifying_key();
    let sig = asteroidb_poc::authority::certificate::sign_message(&sk, &message);

    cert.add_ed25519_signature(asteroidb_poc::authority::certificate::AuthoritySignature {
        authority_id: node_id("auth-1"),
        public_key: vk,
        signature: sig,
        keyset_version: KeysetVersion(1),
    });

    assert_eq!(cert.mode, CertificateMode::Ed25519);
    assert_eq!(cert.signer_count(), 1);

    let valid = cert.verify(&message).unwrap();
    assert_eq!(valid.len(), 1);
    assert_eq!(valid[0], node_id("auth-1"));
}

// ---------------------------------------------------------------------------
// Test 7: Mixed scenario — BLS signer count matches majority
// ---------------------------------------------------------------------------

#[test]
fn bls_majority_threshold_with_5_authorities() {
    let msg = b"majority-test";

    // 5 authorities, need 3 for majority.
    let keypairs: Vec<BlsKeypair> = (0..5)
        .map(|i| {
            let mut seed = [0u8; 32];
            seed[0] = i;
            seed[31] = 42;
            BlsKeypair::generate(&seed)
        })
        .collect();

    let kr = KeyRange { prefix: "".into() };
    let hlc = HlcTimestamp {
        physical: 1_700_000_000_000,
        logical: 0,
        node_id: "node-1".into(),
    };
    let pv = PolicyVersion(1);

    // Sign with only 3 of 5.
    let sigs: Vec<_> = keypairs[0..3]
        .iter()
        .map(|kp| asteroidb_poc::authority::bls::sign_message(kp.secret_key(), msg))
        .collect();
    let agg = asteroidb_poc::authority::bls::aggregate_signatures(&sigs).unwrap();

    let mut cert = DualModeCertificate::new_bls(kr, hlc, pv, KeysetVersion(1));
    let signers: Vec<_> = (0..3)
        .map(|i| {
            (
                node_id(&format!("auth-{i}")),
                keypairs[i].public_key.clone(),
            )
        })
        .collect();
    cert.set_bls_aggregate(signers, agg);

    assert!(cert.has_majority(5), "3/5 is majority");
    assert!(!cert.has_majority(7), "3/7 is not majority");

    let valid = cert.verify(msg).unwrap();
    assert_eq!(valid.len(), 3);
}
