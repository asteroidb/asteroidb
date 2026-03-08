//! Criterion benchmarks comparing BLS12-381 and Ed25519 signature operations.
//!
//! Covers:
//! - Key generation
//! - Single sign / verify
//! - BLS aggregate sign / verify (3, 5, 10 signers)
//! - Ed25519 multi-signature verification (N individual verifies)
//! - DualModeCertificate create + verify in both modes

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;

use asteroidb_poc::authority::bls::{
    self, BlsKeypair, BlsPublicKey, BlsSignature, aggregate_signatures, aggregate_verify,
    sign_message as bls_sign, verify_signature as bls_verify,
};
use asteroidb_poc::authority::certificate::{
    AuthoritySignature, DualModeCertificate, KeysetVersion, create_certificate_message,
    sign_message as ed25519_sign,
};
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::types::{KeyRange, NodeId, PolicyVersion};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn sample_message() -> Vec<u8> {
    let kr = KeyRange {
        prefix: "user/".into(),
    };
    let hlc = HlcTimestamp {
        physical: 1_700_000_000_000,
        logical: 42,
        node_id: "bench-node".into(),
    };
    let pv = PolicyVersion(1);
    create_certificate_message(&kr, &hlc, &pv)
}

fn make_bls_keypair(seed: u8) -> BlsKeypair {
    let mut ikm = [0u8; 32];
    ikm[0] = seed;
    ikm[31] = seed.wrapping_add(42);
    BlsKeypair::generate(&ikm)
}

// ---------------------------------------------------------------------------
// BLS benchmarks
// ---------------------------------------------------------------------------

fn bench_bls_keygen(c: &mut Criterion) {
    c.bench_function("signature/bls/keygen", |b| {
        let mut seed = 0u8;
        b.iter(|| {
            let kp = make_bls_keypair(seed);
            seed = seed.wrapping_add(1);
            std::hint::black_box(kp.public_key);
        });
    });
}

fn bench_bls_sign(c: &mut Criterion) {
    let kp = make_bls_keypair(1);
    let msg = sample_message();

    c.bench_function("signature/bls/sign", |b| {
        b.iter(|| {
            let sig = bls_sign(kp.secret_key(), &msg);
            std::hint::black_box(sig);
        });
    });
}

fn bench_bls_verify(c: &mut Criterion) {
    let kp = make_bls_keypair(2);
    let msg = sample_message();
    let sig = bls_sign(kp.secret_key(), &msg);

    c.bench_function("signature/bls/verify", |b| {
        b.iter(|| {
            let ok = bls_verify(&kp.public_key, &msg, &sig);
            std::hint::black_box(ok);
        });
    });
}

fn bench_bls_aggregate_sign(c: &mut Criterion) {
    let mut group = c.benchmark_group("signature/bls/aggregate_sign");

    for n in [3, 5, 10] {
        let msg = sample_message();
        let keypairs: Vec<BlsKeypair> = (0..n).map(|i| make_bls_keypair(10 + i as u8)).collect();
        let sigs: Vec<BlsSignature> = keypairs
            .iter()
            .map(|kp| bls_sign(kp.secret_key(), &msg))
            .collect();

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let agg = aggregate_signatures(&sigs).unwrap();
                std::hint::black_box(agg);
            });
        });
    }
    group.finish();
}

fn bench_bls_aggregate_verify(c: &mut Criterion) {
    let mut group = c.benchmark_group("signature/bls/aggregate_verify");

    for n in [3, 5, 10] {
        let msg = sample_message();
        let keypairs: Vec<BlsKeypair> = (0..n).map(|i| make_bls_keypair(50 + i as u8)).collect();
        let sigs: Vec<BlsSignature> = keypairs
            .iter()
            .map(|kp| bls_sign(kp.secret_key(), &msg))
            .collect();
        let agg = aggregate_signatures(&sigs).unwrap();
        let pks: Vec<BlsPublicKey> = keypairs.iter().map(|kp| kp.public_key.clone()).collect();

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let ok = aggregate_verify(&pks, &msg, &agg);
                std::hint::black_box(ok);
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Ed25519 benchmarks
// ---------------------------------------------------------------------------

fn bench_ed25519_keygen(c: &mut Criterion) {
    c.bench_function("signature/ed25519/keygen", |b| {
        b.iter(|| {
            let sk = SigningKey::generate(&mut OsRng);
            std::hint::black_box(sk.verifying_key());
        });
    });
}

fn bench_ed25519_sign(c: &mut Criterion) {
    let sk = SigningKey::generate(&mut OsRng);
    let msg = sample_message();

    c.bench_function("signature/ed25519/sign", |b| {
        b.iter(|| {
            let sig = ed25519_sign(&sk, &msg);
            std::hint::black_box(sig);
        });
    });
}

fn bench_ed25519_verify(c: &mut Criterion) {
    let sk = SigningKey::generate(&mut OsRng);
    let vk = sk.verifying_key();
    let msg = sample_message();
    let sig = ed25519_sign(&sk, &msg);

    c.bench_function("signature/ed25519/verify", |b| {
        b.iter(|| {
            use ed25519_dalek::Verifier;
            let ok = vk.verify(&msg, &sig);
            std::hint::black_box(ok);
        });
    });
}

fn bench_ed25519_multi_verify(c: &mut Criterion) {
    let mut group = c.benchmark_group("signature/ed25519/multi_verify");

    for n in [3usize, 5, 10] {
        let msg = sample_message();
        let keys: Vec<(SigningKey, ed25519_dalek::VerifyingKey)> = (0..n)
            .map(|_| {
                let sk = SigningKey::generate(&mut OsRng);
                let vk = sk.verifying_key();
                (sk, vk)
            })
            .collect();
        let sigs: Vec<ed25519_dalek::Signature> =
            keys.iter().map(|(sk, _)| ed25519_sign(sk, &msg)).collect();

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                use ed25519_dalek::Verifier;
                for (i, (_, vk)) in keys.iter().enumerate() {
                    let ok = vk.verify(&msg, &sigs[i]);
                    std::hint::black_box(ok);
                }
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// DualModeCertificate benchmarks
// ---------------------------------------------------------------------------

fn bench_dual_create_ed25519(c: &mut Criterion) {
    let msg = sample_message();
    let keys: Vec<(SigningKey, ed25519_dalek::VerifyingKey, NodeId)> = (0..3)
        .map(|i| {
            let sk = SigningKey::generate(&mut OsRng);
            let vk = sk.verifying_key();
            (sk, vk, NodeId(format!("auth-{i}")))
        })
        .collect();

    c.bench_function("signature/dual/create_ed25519", |b| {
        b.iter(|| {
            let kr = KeyRange {
                prefix: "user/".into(),
            };
            let hlc = HlcTimestamp {
                physical: 1_700_000_000_000,
                logical: 42,
                node_id: "bench-node".into(),
            };
            let mut cert =
                DualModeCertificate::new_ed25519(kr, hlc, PolicyVersion(1), KeysetVersion(1));

            for (sk, vk, nid) in &keys {
                let sig = ed25519_sign(sk, &msg);
                cert.add_ed25519_signature(AuthoritySignature {
                    authority_id: nid.clone(),
                    public_key: *vk,
                    signature: sig,
                    keyset_version: KeysetVersion(1),
                });
            }
            std::hint::black_box(cert);
        });
    });
}

fn bench_dual_create_bls(c: &mut Criterion) {
    let msg = sample_message();
    let keypairs: Vec<(BlsKeypair, NodeId)> = (0..3)
        .map(|i| (make_bls_keypair(80 + i), NodeId(format!("auth-{i}"))))
        .collect();

    c.bench_function("signature/dual/create_bls", |b| {
        b.iter(|| {
            let kr = KeyRange {
                prefix: "user/".into(),
            };
            let hlc = HlcTimestamp {
                physical: 1_700_000_000_000,
                logical: 42,
                node_id: "bench-node".into(),
            };
            let mut cert =
                DualModeCertificate::new_bls(kr, hlc, PolicyVersion(1), KeysetVersion(1));

            let sigs: Vec<BlsSignature> = keypairs
                .iter()
                .map(|(kp, _)| bls_sign(kp.secret_key(), &msg))
                .collect();
            let agg = aggregate_signatures(&sigs).unwrap();

            let signers: Vec<(NodeId, BlsPublicKey)> = keypairs
                .iter()
                .map(|(kp, nid)| (nid.clone(), kp.public_key.clone()))
                .collect();
            cert.set_bls_aggregate(signers, agg);
            std::hint::black_box(cert);
        });
    });
}

fn bench_dual_verify_ed25519(c: &mut Criterion) {
    let msg = sample_message();
    let keys: Vec<(SigningKey, ed25519_dalek::VerifyingKey, NodeId)> = (0..3)
        .map(|i| {
            let sk = SigningKey::generate(&mut OsRng);
            let vk = sk.verifying_key();
            (sk, vk, NodeId(format!("auth-{i}")))
        })
        .collect();

    let kr = KeyRange {
        prefix: "user/".into(),
    };
    let hlc = HlcTimestamp {
        physical: 1_700_000_000_000,
        logical: 42,
        node_id: "bench-node".into(),
    };
    let mut cert = DualModeCertificate::new_ed25519(kr, hlc, PolicyVersion(1), KeysetVersion(1));
    for (sk, vk, nid) in &keys {
        let sig = ed25519_sign(sk, &msg);
        cert.add_ed25519_signature(AuthoritySignature {
            authority_id: nid.clone(),
            public_key: *vk,
            signature: sig,
            keyset_version: KeysetVersion(1),
        });
    }

    c.bench_function("signature/dual/verify_ed25519", |b| {
        b.iter(|| {
            let result = cert.verify(&msg);
            std::hint::black_box(result);
        });
    });
}

fn bench_dual_verify_bls(c: &mut Criterion) {
    let msg = sample_message();
    let keypairs: Vec<(BlsKeypair, NodeId)> = (0..3)
        .map(|i| (make_bls_keypair(90 + i), NodeId(format!("auth-{i}"))))
        .collect();

    let kr = KeyRange {
        prefix: "user/".into(),
    };
    let hlc = HlcTimestamp {
        physical: 1_700_000_000_000,
        logical: 42,
        node_id: "bench-node".into(),
    };
    let mut cert = DualModeCertificate::new_bls(kr, hlc, PolicyVersion(1), KeysetVersion(1));

    let sigs: Vec<BlsSignature> = keypairs
        .iter()
        .map(|(kp, _)| bls_sign(kp.secret_key(), &msg))
        .collect();
    let agg = aggregate_signatures(&sigs).unwrap();

    let signers: Vec<(NodeId, BlsPublicKey)> = keypairs
        .iter()
        .map(|(kp, nid)| (nid.clone(), kp.public_key.clone()))
        .collect();
    cert.set_bls_aggregate(signers, agg);

    c.bench_function("signature/dual/verify_bls", |b| {
        b.iter(|| {
            let result = cert.verify(&msg);
            std::hint::black_box(result);
        });
    });
}

criterion_group!(
    benches,
    // BLS
    bench_bls_keygen,
    bench_bls_sign,
    bench_bls_verify,
    bench_bls_aggregate_sign,
    bench_bls_aggregate_verify,
    // Ed25519
    bench_ed25519_keygen,
    bench_ed25519_sign,
    bench_ed25519_verify,
    bench_ed25519_multi_verify,
    // DualModeCertificate
    bench_dual_create_ed25519,
    bench_dual_create_bls,
    bench_dual_verify_ed25519,
    bench_dual_verify_bls,
);
criterion_main!(benches);
