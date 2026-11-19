//! Criterion benchmarks for the certified write path:
//! certified_write, process_certifications, and verify_proof.

use std::sync::{Arc, RwLock};

use criterion::{Criterion, criterion_group, criterion_main};
use ed25519_dalek::{SigningKey, VerifyingKey};
use rand::rngs::OsRng;

use asteroidb_poc::api::certified::{CertifiedApi, OnTimeout, ProofBundle};
use asteroidb_poc::authority::ack_frontier::AckFrontier;
use asteroidb_poc::authority::certificate::{
    AuthoritySignature, KeysetVersion, MajorityCertificate, create_certificate_message,
    sign_message,
};
use asteroidb_poc::authority::verifier::verify_proof;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::store::kv::CrdtValue;
use asteroidb_poc::types::{KeyRange, NodeId, PolicyVersion};

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

/// Build a CertifiedApi with 3 authorities for "user/" key range.
fn build_certified_api() -> CertifiedApi {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: KeyRange {
            prefix: "user/".into(),
        },
        authority_nodes: vec![node("auth-0"), node("auth-1"), node("auth-2")],
        auto_generated: false,
    });
    let ns = Arc::new(RwLock::new(ns));
    CertifiedApi::new(node("node-a"), ns)
}

/// Build a signed ProofBundle with `n_signers` out of `total` authorities.
fn make_signed_proof(n_signers: usize, total: usize) -> (ProofBundle, Vec<(NodeId, VerifyingKey)>) {
    let kr = KeyRange {
        prefix: "user/".into(),
    };
    let hlc = ts(1_700_000_000_000, 42, "node-1");
    let pv = PolicyVersion(1);

    let message = create_certificate_message(&kr, &hlc, &pv);
    let mut cert = MajorityCertificate::new(kr.clone(), hlc.clone(), pv, KeysetVersion(1));

    let mut authorities = Vec::with_capacity(n_signers);
    let mut trusted_keys = Vec::with_capacity(total);
    for i in 0..total {
        let authority = node(&format!("auth-{i}"));
        let signing_key = SigningKey::generate(&mut OsRng);
        let verifying_key = signing_key.verifying_key();
        if i < n_signers {
            authorities.push(authority.clone());
            cert.add_signature(AuthoritySignature {
                authority_id: authority.clone(),
                public_key: verifying_key,
                signature: sign_message(&signing_key, &message),
                keyset_version: KeysetVersion(1),
            });
        }
        trusted_keys.push((authority, verifying_key));
    }

    let proof = ProofBundle {
        key_range: kr,
        frontier_hlc: hlc,
        policy_version: pv,
        contributing_authorities: authorities,
        total_authorities: total,
        certificate: Some(cert),
        bls_certificate: None,
    };
    (proof, trusted_keys)
}

// ---------------------------------------------------------------------------
// certified_write benchmark
// ---------------------------------------------------------------------------

fn bench_certified_write(c: &mut Criterion) {
    c.bench_function("certified/write_pending", |b| {
        b.iter_batched(
            build_certified_api,
            |mut api| {
                for i in 0..100 {
                    let key = format!("user/key-{i}");
                    let mut counter = PnCounter::new();
                    counter.increment(&node("node-a"));
                    let _ =
                        api.certified_write(key, CrdtValue::Counter(counter), OnTimeout::Pending);
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

// ---------------------------------------------------------------------------
// process_certifications benchmark
// ---------------------------------------------------------------------------

fn bench_process_certifications(c: &mut Criterion) {
    c.bench_function("certified/process_certifications", |b| {
        b.iter_batched(
            || {
                let mut api = build_certified_api();
                // Write 100 pending entries.
                for i in 0..100 {
                    let key = format!("user/key-{i}");
                    let mut counter = PnCounter::new();
                    counter.increment(&node("node-a"));
                    let _ =
                        api.certified_write(key, CrdtValue::Counter(counter), OnTimeout::Pending);
                }
                // Advance frontiers so some writes become certifiable.
                let frontier_hlc = ts(u64::MAX - 1, u32::MAX, "zzz");
                let kr = KeyRange {
                    prefix: "user/".into(),
                };
                let pv = PolicyVersion(1);
                for i in 0..2 {
                    api.update_frontier(AckFrontier {
                        authority_id: node(&format!("auth-{i}")),
                        frontier_hlc: frontier_hlc.clone(),
                        key_range: kr.clone(),
                        policy_version: pv,
                        digest_hash: String::new(),
                    });
                }
                api
            },
            |mut api| {
                api.process_certifications();
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

// ---------------------------------------------------------------------------
// verify_proof benchmark
// ---------------------------------------------------------------------------

fn bench_verify_proof(c: &mut Criterion) {
    // 3-of-5 signed proof.
    let (proof, trusted_keys) = make_signed_proof(3, 5);

    c.bench_function("certified/verify_proof_3of5", |b| {
        b.iter(|| {
            let result = verify_proof(&proof, &trusted_keys, None, 0);
            std::hint::black_box(result.valid);
        });
    });
}

fn bench_verify_proof_large(c: &mut Criterion) {
    // 5-of-9 signed proof for heavier workload.
    let (proof, trusted_keys) = make_signed_proof(5, 9);

    c.bench_function("certified/verify_proof_5of9", |b| {
        b.iter(|| {
            let result = verify_proof(&proof, &trusted_keys, None, 0);
            std::hint::black_box(result.valid);
        });
    });
}

criterion_group!(
    benches,
    bench_certified_write,
    bench_process_certifications,
    bench_verify_proof,
    bench_verify_proof_large,
);
criterion_main!(benches);
