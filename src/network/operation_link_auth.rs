//! Pinned adjacent identities for routed operation custody HTTP messages.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::operation_neighbors::OperationNeighborManifest;
use crate::types::NodeId;

pub const SENDER: &str = "x-asteroidb-link-sender";
pub const RECEIVER: &str = "x-asteroidb-link-receiver";
pub const SIGNATURE: &str = "x-asteroidb-link-signature";
pub const REQUEST_DIGEST: &str = "x-asteroidb-link-request-digest";

#[derive(Clone)]
pub struct OperationLinkAuth {
    self_id: NodeId,
    signer: Arc<SigningKey>,
    peers: Arc<BTreeMap<NodeId, VerifyingKey>>,
    addresses: Arc<BTreeMap<String, NodeId>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PinnedKey {
    node_id: NodeId,
    public_key: String,
}

impl OperationLinkAuth {
    pub fn from_manifest(
        self_id: NodeId,
        seed_hex: &str,
        keys_json: &str,
        neighbors: &OperationNeighborManifest,
    ) -> Result<Self, String> {
        fn valid_id(id: &NodeId) -> bool {
            !id.0.is_empty()
                && id
                    .0
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        }
        if !valid_id(&self_id) {
            return Err("invalid local operation link node ID".into());
        }
        let seed = decode_fixed::<32>(seed_hex)?;
        let signer = SigningKey::from_bytes(&seed);
        let rows: Vec<PinnedKey> = serde_json::from_str(keys_json).map_err(|e| e.to_string())?;
        let mut peers = BTreeMap::new();
        let mut addresses = BTreeMap::new();
        for neighbor in neighbors.neighbors() {
            addresses.insert(neighbor.addr.clone(), neighbor.node_id.clone());
        }
        for row in rows {
            if !valid_id(&row.node_id) {
                return Err("invalid operation link peer node ID".into());
            }
            if !addresses.values().any(|id| id == &row.node_id) {
                return Err(format!("link key for non-neighbor {}", row.node_id.0));
            }
            let key = VerifyingKey::from_bytes(&decode_fixed::<32>(&row.public_key)?)
                .map_err(|e| e.to_string())?;
            if key == signer.verifying_key() || peers.values().any(|existing| *existing == key) {
                return Err("operation link public keys must be distinct per node".into());
            }
            if peers.insert(row.node_id.clone(), key).is_some() {
                return Err(format!("duplicate link key for {}", row.node_id.0));
            }
        }
        let expected: BTreeSet<_> = addresses.values().cloned().collect();
        if expected != peers.keys().cloned().collect() {
            return Err("link public keys must exactly cover adjacent peers".into());
        }
        Ok(Self {
            self_id,
            signer: Arc::new(signer),
            peers: Arc::new(peers),
            addresses: Arc::new(addresses),
        })
    }

    pub fn self_id(&self) -> &NodeId {
        &self.self_id
    }
    pub fn peer_for_addr(&self, addr: &str) -> Option<&NodeId> {
        self.addresses.get(addr)
    }
    pub fn is_peer(&self, id: &NodeId) -> bool {
        self.peers.contains_key(id)
    }

    pub fn manifest_digest(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"asteroidb/operation-link-manifest/v1");
        put(&mut hash, self.self_id.0.as_bytes());
        hash.update(self.signer.verifying_key().as_bytes());
        for (id, key) in self.peers.iter() {
            put(&mut hash, id.0.as_bytes());
            hash.update(key.as_bytes());
        }
        for (addr, id) in self.addresses.iter() {
            put(&mut hash, addr.as_bytes());
            put(&mut hash, id.0.as_bytes());
        }
        hash.finalize().into()
    }

    pub fn request_digest(
        sender: &NodeId,
        receiver: &NodeId,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"asteroidb/operation-link-request/v1");
        put(&mut hash, sender.0.as_bytes());
        put(&mut hash, receiver.0.as_bytes());
        put(&mut hash, method.as_bytes());
        put(&mut hash, path.as_bytes());
        hash.update(Sha256::digest(body));
        hash.finalize().into()
    }

    pub fn sign_request(
        &self,
        receiver: &NodeId,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> ([u8; 32], String) {
        let digest = Self::request_digest(&self.self_id, receiver, method, path, body);
        (digest, hex::encode(self.signer.sign(&digest).to_bytes()))
    }

    pub fn verify_request(
        &self,
        sender: &NodeId,
        receiver: &NodeId,
        method: &str,
        path: &str,
        body: &[u8],
        signature: &str,
    ) -> Option<[u8; 32]> {
        if receiver != &self.self_id {
            return None;
        }
        let digest = Self::request_digest(sender, receiver, method, path, body);
        self.peers
            .get(sender)?
            .verify(
                &digest,
                &Signature::from_bytes(&decode_fixed::<64>(signature).ok()?),
            )
            .ok()?;
        Some(digest)
    }

    fn response_digest(
        sender: &NodeId,
        receiver: &NodeId,
        method: &str,
        path: &str,
        request: &[u8; 32],
        status: u16,
        body: &[u8],
    ) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"asteroidb/operation-link-response/v1");
        put(&mut hash, sender.0.as_bytes());
        put(&mut hash, receiver.0.as_bytes());
        put(&mut hash, method.as_bytes());
        put(&mut hash, path.as_bytes());
        hash.update(request);
        hash.update(status.to_be_bytes());
        hash.update(Sha256::digest(body));
        hash.finalize().into()
    }

    pub fn sign_response(
        &self,
        receiver: &NodeId,
        method: &str,
        path: &str,
        request: &[u8; 32],
        status: u16,
        body: &[u8],
    ) -> String {
        let digest =
            Self::response_digest(&self.self_id, receiver, method, path, request, status, body);
        hex::encode(self.signer.sign(&digest).to_bytes())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn verify_response(
        &self,
        sender: &NodeId,
        receiver: &NodeId,
        method: &str,
        path: &str,
        request: &[u8; 32],
        status: u16,
        body: &[u8],
        signature: &str,
    ) -> bool {
        if receiver != &self.self_id {
            return false;
        }
        let Some(key) = self.peers.get(sender) else {
            return false;
        };
        let Ok(bytes) = decode_fixed::<64>(signature) else {
            return false;
        };
        let digest = Self::response_digest(sender, receiver, method, path, request, status, body);
        key.verify(&digest, &Signature::from_bytes(&bytes)).is_ok()
    }
}

fn put(hash: &mut Sha256, bytes: &[u8]) {
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
}

fn decode_fixed<const N: usize>(value: &str) -> Result<[u8; N], String> {
    let bytes = hex::decode(value).map_err(|e| e.to_string())?;
    bytes
        .try_into()
        .map_err(|_| format!("expected {N}-byte hex value"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    fn link(id: &str, peer: &str, seed: u8, peer_seed: u8) -> OperationLinkAuth {
        let neighbors = OperationNeighborManifest::from_json(
            &format!(r#"[{{"node_id":"{peer}","addr":"127.0.0.1:9999","key_prefixes":[""]}}]"#),
            &NodeId(id.into()),
        )
        .unwrap();
        let public = SigningKey::from_bytes(&[peer_seed; 32]).verifying_key();
        OperationLinkAuth::from_manifest(
            NodeId(id.into()),
            &hex::encode([seed; 32]),
            &serde_json::json!([{"node_id":peer,"public_key":hex::encode(public.as_bytes())}])
                .to_string(),
            &neighbors,
        )
        .unwrap()
    }

    #[test]
    fn request_and_receipt_bind_peer_path_and_exact_body() {
        let a = link("a", "b", 1, 2);
        let b = link("b", "a", 2, 1);
        let (digest, signature) =
            a.sign_request(&NodeId("b".into()), "POST", "/transfer", b"first");
        assert_eq!(
            b.verify_request(
                &NodeId("a".into()),
                &NodeId("b".into()),
                "POST",
                "/transfer",
                b"first",
                &signature
            ),
            Some(digest)
        );
        assert!(
            b.verify_request(
                &NodeId("a".into()),
                &NodeId("b".into()),
                "POST",
                "/completion",
                b"first",
                &signature
            )
            .is_none()
        );
        assert!(
            b.verify_request(
                &NodeId("a".into()),
                &NodeId("b".into()),
                "POST",
                "/transfer",
                b"second",
                &signature
            )
            .is_none()
        );
        let receipt = b.sign_response(
            &NodeId("a".into()),
            "POST",
            "/transfer",
            &digest,
            200,
            b"ack",
        );
        assert!(a.verify_response(
            &NodeId("b".into()),
            &NodeId("a".into()),
            "POST",
            "/transfer",
            &digest,
            200,
            b"ack",
            &receipt
        ));
        assert!(!a.verify_response(
            &NodeId("b".into()),
            &NodeId("a".into()),
            "POST",
            "/transfer",
            &digest,
            200,
            b"forged",
            &receipt
        ));
        assert!(!a.verify_response(
            &NodeId("b".into()),
            &NodeId("a".into()),
            "POST",
            "/transfer",
            &[9; 32],
            200,
            b"ack",
            &receipt
        ));
    }

    #[test]
    fn link_manifest_pin_changes_with_public_key_and_address() {
        let a = link("a", "b", 1, 2);
        let other_key = link("a", "b", 1, 3);
        assert_ne!(a.manifest_digest(), other_key.manifest_digest());
    }
}
