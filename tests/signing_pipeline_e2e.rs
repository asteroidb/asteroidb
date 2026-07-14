//! End-to-end tests for the certificate signing pipeline (FR-008).
//!
//! Runs 3 in-process HTTP nodes whose keyset registries contain all three
//! authorities' public keys (the equivalent of distributing them via
//! `ASTEROIDB_AUTHORITY_KEYS`). Authorities sign their frontier reports and
//! push them over HTTP (Bearer-authenticated) using `FrontierSyncClient`;
//! each node verifies the signatures, assembles a `MajorityCertificate`,
//! attaches it to certified read proofs, and `POST /api/certified/verify`
//! validates the proof end-to-end.

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use asteroidb_poc::api::certified::{CertifiedApi, OnTimeout};
use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::authority::ack_frontier::AckFrontier;
use asteroidb_poc::authority::certificate::{KeysetRegistry, KeysetVersion};
use asteroidb_poc::authority::frontier_sig::{FrontierSignature, NodeSigner};
use asteroidb_poc::control_plane::consensus::ControlPlaneConsensus;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::http::handlers::AppState;
use asteroidb_poc::http::routes::router;
use asteroidb_poc::network::frontier_sync::FrontierSyncClient;
use asteroidb_poc::ops::metrics::RuntimeMetrics;
use asteroidb_poc::placement::PlacementPolicy;
use asteroidb_poc::store::kv::CrdtValue;
use asteroidb_poc::types::{CertificationStatus, KeyRange, NodeId, PolicyVersion};

use tokio::sync::Mutex;
use tokio::task::JoinHandle;

const TOKEN: &str = "e2e-secret";

fn node_id(s: &str) -> NodeId {
    NodeId(s.into())
}

#[cfg(feature = "native-crypto")]
fn make_signer(name: &str, byte: u8) -> NodeSigner {
    let mut seed = [0u8; 32];
    seed[0] = byte;
    NodeSigner::from_seed(node_id(name), &seed, true)
}

#[cfg(not(feature = "native-crypto"))]
fn make_signer(name: &str, byte: u8) -> NodeSigner {
    let mut seed = [0u8; 32];
    seed[0] = byte;
    NodeSigner::from_seed(node_id(name), &seed)
}

/// Build a registry containing all signers' public keys at keyset version 1,
/// mirroring what `ASTEROIDB_AUTHORITY_KEYS` distribution produces.
fn full_registry(signers: &[&NodeSigner]) -> KeysetRegistry {
    let mut registry = KeysetRegistry::new();
    registry
        .register_keyset(
            KeysetVersion(1),
            0,
            signers
                .iter()
                .map(|s| (s.node_id().clone(), s.verifying_key()))
                .collect(),
        )
        .unwrap();
    #[cfg(feature = "native-crypto")]
    {
        let bls_keys: Vec<(
            String,
            asteroidb_poc::authority::bls::BlsPublicKey,
            asteroidb_poc::authority::bls::BlsProofOfPossession,
        )> = signers
            .iter()
            .filter_map(|s| {
                s.bls_public_key()
                    .zip(s.bls_proof_of_possession())
                    .map(|(pk, pop)| (s.node_id().0.clone(), pk, pop))
            })
            .collect();
        if !bls_keys.is_empty() {
            registry
                .register_bls_keys(&KeysetVersion(1), bls_keys)
                .unwrap();
        }
    }
    registry
}

fn default_namespace() -> SystemNamespace {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: KeyRange {
            prefix: String::new(),
        },
        authority_nodes: vec![node_id("auth-1"), node_id("auth-2"), node_id("auth-3")],
        auto_generated: false,
    });
    ns.set_placement_policy(PlacementPolicy::new(
        PolicyVersion(1),
        KeyRange {
            prefix: String::new(),
        },
        3,
    ))
    .unwrap();
    ns
}

/// Spawn an HTTP node with a populated keyset registry and internal token.
async fn spawn_node(
    name: &str,
    registry: KeysetRegistry,
    require_signed: bool,
) -> (Arc<AppState>, SocketAddr, JoinHandle<()>) {
    let nid = node_id(name);
    let namespace = Arc::new(RwLock::new(default_namespace()));
    let state = Arc::new(AppState {
        eventual: Arc::new(Mutex::new(EventualApi::new(nid.clone()))),
        certified: Arc::new(Mutex::new(CertifiedApi::new(nid, Arc::clone(&namespace)))),
        namespace,
        metrics: Arc::new(RuntimeMetrics::default()),
        peers: None,
        peer_persist_path: None,
        namespace_persist_path: None,
        consensus: Arc::new(Mutex::new(ControlPlaneConsensus::new(vec![]))),
        internal_token: Some(TOKEN.to_string()),
        self_node_id: None,
        self_addr: None,
        latency_model: None,
        cluster_nodes: None,
        slo_tracker: Arc::new(asteroidb_poc::ops::slo::SloTracker::new()),
        keyset_registry: Some(Arc::new(RwLock::new(registry))),
        epoch_config: asteroidb_poc::authority::certificate::EpochConfig::default(),
        current_epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        require_signed_frontiers: require_signed,
    });

    let app = router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (state, addr, handle)
}

fn counter_value(n: i64) -> CrdtValue {
    let mut counter = PnCounter::new();
    for _ in 0..n {
        counter.increment(&node_id("writer"));
    }
    CrdtValue::Counter(counter)
}

/// Build a signed frontier report from one authority at a timestamp.
fn signed_report(signer: &NodeSigner, physical: u64) -> (AckFrontier, Option<FrontierSignature>) {
    let frontier = AckFrontier {
        authority_id: signer.node_id().clone(),
        frontier_hlc: HlcTimestamp {
            physical,
            logical: 0,
            node_id: signer.node_id().0.clone(),
        },
        key_range: KeyRange {
            prefix: String::new(),
        },
        policy_version: PolicyVersion(1),
        digest_hash: format!("{}-{physical}", signer.node_id().0),
    };
    let sig = signer.sign_frontier(&frontier, KeysetVersion(1));
    (frontier, Some(sig))
}

/// Certified-write a key directly on a node's API and return the write HLC.
async fn certified_write(state: &Arc<AppState>, key: &str) -> u64 {
    let mut api = state.certified.lock().await;
    api.certified_write(key.to_string(), counter_value(1), OnTimeout::Pending)
        .unwrap();
    api.pending_writes()
        .iter()
        .rev()
        .find(|pw| pw.key == key)
        .unwrap()
        .timestamp
        .physical
}

#[tokio::test]
async fn signed_frontiers_flow_through_cluster_and_verify() {
    let s1 = make_signer("auth-1", 1);
    let s2 = make_signer("auth-2", 2);
    let s3 = make_signer("auth-3", 3);

    let mut nodes = Vec::new();
    for name in ["node-1", "node-2", "node-3"] {
        nodes.push(spawn_node(name, full_registry(&[&s1, &s2, &s3]), false).await);
    }

    // A certified write lands on every node (each node tracks its own writes).
    let mut write_ts_max = 0u64;
    for (state, _, _) in &nodes {
        let ts = certified_write(state, "user/alice").await;
        write_ts_max = write_ts_max.max(ts);
    }

    // All 3 authorities push signed frontier reports (past the next
    // checkpoint boundary) to every node via the real sync client.
    let client = FrontierSyncClient::with_token(TOKEN.to_string());
    let report_ts = (write_ts_max / 1000 + 1) * 1000 + 100;
    for signer in [&s1, &s2, &s3] {
        let (frontier, sig) = signed_report(signer, report_ts);
        for (_, addr, _) in &nodes {
            let resp = client
                .push_signed_frontiers(&addr.to_string(), vec![frontier.clone()], vec![sig.clone()])
                .await
                .expect("signed frontier push must succeed");
            assert_eq!(resp.accepted, 1, "signed frontier must be accepted");
        }
    }

    let http = reqwest::Client::new();
    for (state, addr, _) in &nodes {
        // Drive certification (normally the NodeRunner tick does this).
        state.certified.lock().await.process_certifications();

        // Certified read must carry a verifiable certificate.
        let read: serde_json::Value = http
            .get(format!("http://{addr}/api/certified/user%2Falice"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(read["status"], "Certified", "on node {addr}: {read}");
        let proof = read["proof"].clone();
        assert!(
            proof["certificate"].is_object(),
            "proof on {addr} must carry a certificate: {proof}"
        );

        // Round-trip the proof into the verify endpoint (Ed25519).
        let mut ed_payload = proof.clone();
        ed_payload["signature_algorithm"] = serde_json::json!("Ed25519");
        let result: serde_json::Value = http
            .post(format!("http://{addr}/api/certified/verify"))
            .json(&ed_payload)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(result["valid"], true, "Ed25519 verify on {addr}: {result}");

        // BLS aggregate path (native-crypto builds).
        #[cfg(feature = "native-crypto")]
        {
            assert_eq!(proof["signature_algorithm"], "Bls12_381");
            let result: serde_json::Value = http
                .post(format!("http://{addr}/api/certified/verify"))
                .json(&proof)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(result["valid"], true, "BLS verify on {addr}: {result}");
        }
    }

    for (_, _, handle) in nodes {
        handle.abort();
    }
}

#[tokio::test]
async fn forged_frontiers_cannot_advance_certification() {
    let s1 = make_signer("auth-1", 11);
    let s2 = make_signer("auth-2", 12);
    let s3 = make_signer("auth-3", 13);
    // The rogue holds its own keys, not the registered authorities' keys.
    let rogue2 = make_signer("auth-2", 42);
    let rogue3 = make_signer("auth-3", 43);

    let (state, addr, handle) = spawn_node("node-1", full_registry(&[&s1, &s2, &s3]), true).await;

    let write_ts = certified_write(&state, "user/bob").await;
    let report_ts = (write_ts / 1000 + 1) * 1000 + 100;

    let client = FrontierSyncClient::with_token(TOKEN.to_string());

    // A malicious peer impersonates auth-2/auth-3 with its own keys.
    for rogue in [&rogue2, &rogue3] {
        let (frontier, sig) = signed_report(rogue, report_ts);
        let resp = client
            .push_signed_frontiers(&addr.to_string(), vec![frontier], vec![sig])
            .await
            .unwrap();
        assert_eq!(resp.accepted, 0, "forged signature must be rejected");
    }

    // Unsigned pushes are also rejected in strict mode.
    let (frontier, _) = signed_report(&rogue2, report_ts);
    let resp = client
        .push_signed_frontiers(&addr.to_string(), vec![frontier], vec![None])
        .await
        .unwrap();
    assert_eq!(
        resp.accepted, 0,
        "unsigned frontier rejected in strict mode"
    );

    // Unknown keyset versions are rejected as well.
    let frontier = AckFrontier {
        authority_id: node_id("auth-1"),
        frontier_hlc: HlcTimestamp {
            physical: report_ts,
            logical: 0,
            node_id: "auth-1".into(),
        },
        key_range: KeyRange {
            prefix: String::new(),
        },
        policy_version: PolicyVersion(1),
        digest_hash: format!("auth-1-{report_ts}"),
    };
    let bad_keyset_sig = s1.sign_frontier(&frontier, KeysetVersion(99));
    let resp = client
        .push_signed_frontiers(
            &addr.to_string(),
            vec![frontier],
            vec![Some(bad_keyset_sig)],
        )
        .await
        .unwrap();
    assert_eq!(resp.accepted, 0, "unknown keyset must be rejected");

    // None of the rejected pushes may have advanced certification.
    {
        let mut api = state.certified.lock().await;
        api.process_certifications();
        assert_eq!(
            api.get_certification_status("user/bob"),
            CertificationStatus::Pending,
            "forged frontiers must not certify writes"
        );
    }

    // A single genuine authority is below the 2-of-3 majority.
    let (frontier, sig) = signed_report(&s1, report_ts);
    let resp = client
        .push_signed_frontiers(&addr.to_string(), vec![frontier], vec![sig])
        .await
        .unwrap();
    assert_eq!(resp.accepted, 1);
    {
        let mut api = state.certified.lock().await;
        api.process_certifications();
        assert_eq!(
            api.get_certification_status("user/bob"),
            CertificationStatus::Pending,
            "1 of 3 authorities is not a majority"
        );
    }

    // A second genuine authority completes the majority.
    let (frontier, sig) = signed_report(&s2, report_ts);
    client
        .push_signed_frontiers(&addr.to_string(), vec![frontier], vec![sig])
        .await
        .unwrap();
    {
        let mut api = state.certified.lock().await;
        api.process_certifications();
        assert_eq!(
            api.get_certification_status("user/bob"),
            CertificationStatus::Certified
        );
        let proof = api.get_certified("user/bob").proof.unwrap();
        assert!(proof.certificate.is_some());
        let verification = asteroidb_poc::authority::verifier::verify_proof(&proof, None, 0);
        assert!(verification.valid);
    }

    handle.abort();
}

#[tokio::test]
async fn frontier_push_requires_bearer_token() {
    let s1 = make_signer("auth-1", 21);
    let (_, addr, handle) = spawn_node("node-1", full_registry(&[&s1]), false).await;

    // A client without the token must be rejected by the auth middleware.
    let unauthenticated = FrontierSyncClient::new();
    let (frontier, sig) = signed_report(&s1, 10_500);
    let result = unauthenticated
        .push_signed_frontiers(&addr.to_string(), vec![frontier], vec![sig])
        .await;
    assert!(result.is_err(), "push without Bearer token must fail");

    handle.abort();
}
