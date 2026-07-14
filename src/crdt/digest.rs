//! Canonical byte-stream helpers for CRDT state digests.
//!
//! The digest-based anti-entropy protocol compares SHA-256 digests of CRDT
//! state across replicas, so the byte stream fed into the hasher MUST be
//! deterministic: identical CRDT states must produce identical bytes on
//! every replica, regardless of insertion order, merge order, process
//! hash-map seeding, or serde round-trips. Hashing raw bincode/JSON output
//! is therefore forbidden — `HashMap`/`HashSet` iteration order is
//! non-deterministic. Instead each CRDT type emits a *canonical* stream:
//! all unordered collections are sorted into a total order first.
//!
//! Primitive encoding (scheme version 1, see
//! [`crate::store::digest::DIGEST_SCHEME_VERSION`]):
//! - integers: fixed-width big-endian (`u64` / `u32`)
//! - `str(s)` = `u32BE(len)` ‖ UTF-8 bytes
//! - `hlc(t)` = `u64BE(physical)` ‖ `u32BE(logical)` ‖ `str(node_id)`
//! - `dot(d)` = `str(node_id)` ‖ `u64BE(counter)`, sorted by
//!   `(node_id bytes, counter)`
//! - `Option`: `0x00` for `None`, `0x01` ‖ payload for `Some`
//!
//! # MAINTAINER CONTRACT
//! Any change to these helpers — or to the per-type `digest_into`
//! implementations that use them — changes the wire digest. Such a change
//! MUST bump `DIGEST_SCHEME_VERSION` (mixed-version clusters detect the
//! mismatch and fall back to full sync) and update the golden digest test
//! in `crate::store::digest`. Forgetting to do so makes "digest matched"
//! a lie, which corrupts session-guarantee claims.

use std::collections::HashMap;

use sha2::{Digest, Sha256};

use crate::hlc::HlcTimestamp;
use crate::types::NodeId;

/// Feed a big-endian `u32` into the hasher.
pub(crate) fn write_u32(hasher: &mut Sha256, v: u32) {
    hasher.update(v.to_be_bytes());
}

/// Feed a big-endian `u64` into the hasher.
pub(crate) fn write_u64(hasher: &mut Sha256, v: u64) {
    hasher.update(v.to_be_bytes());
}

/// Feed a length-prefixed UTF-8 string into the hasher.
pub(crate) fn write_str(hasher: &mut Sha256, s: &str) {
    write_u32(hasher, s.len() as u32);
    hasher.update(s.as_bytes());
}

/// Feed an `Option<&str>` into the hasher (`0x00` = None, `0x01` ‖ str = Some).
pub(crate) fn write_opt_str(hasher: &mut Sha256, v: Option<&str>) {
    match v {
        None => hasher.update([0x00]),
        Some(s) => {
            hasher.update([0x01]);
            write_str(hasher, s);
        }
    }
}

/// Feed an HLC timestamp into the hasher.
///
/// The `node_id` is included deliberately: two states holding the same
/// value written by different nodes are different states and must not
/// collide.
pub(crate) fn write_hlc(hasher: &mut Sha256, ts: &HlcTimestamp) {
    write_u64(hasher, ts.physical);
    write_u32(hasher, ts.logical);
    write_str(hasher, &ts.node_id);
}

/// Feed a per-node counter map in canonical (node-id byte) order.
pub(crate) fn write_counters(hasher: &mut Sha256, counters: &HashMap<NodeId, u64>) {
    let mut items: Vec<(&str, u64)> = counters.iter().map(|(id, &v)| (id.0.as_str(), v)).collect();
    items.sort_unstable();
    write_u32(hasher, items.len() as u32);
    for (id, v) in items {
        write_str(hasher, id);
        write_u64(hasher, v);
    }
}

/// Feed a dot collection in canonical `(node_id bytes, counter)` order.
///
/// Takes `(node_id, counter)` pairs so that both `OrSet::Dot` and the
/// private `OrMap` dot type can share this helper.
pub(crate) fn write_dots<'a, I>(hasher: &mut Sha256, dots: I)
where
    I: IntoIterator<Item = (&'a str, u64)>,
{
    let mut items: Vec<(&str, u64)> = dots.into_iter().collect();
    items.sort_unstable();
    write_u32(hasher, items.len() as u32);
    for (id, counter) in items {
        write_str(hasher, id);
        write_u64(hasher, counter);
    }
}
