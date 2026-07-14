//! Client session guarantees: read-your-writes and monotonic reads.
//!
//! Implements a stateless session-token scheme over the eventual store.
//! Writes return an opaque token encoding the HLC position of the write;
//! reads may present a token, and the server only answers when the local
//! replica provably contains all writes the token covers (per origin
//! node). When the replica has not caught up, the read fails with
//! `SessionNotSatisfied` (HTTP 412) instead of returning a stale value —
//! a false success is never returned (false negatives are acceptable).
//!
//! Token wire format (v1, opaque to clients):
//!
//! ```text
//! token   := "v1:" [ entry ("," entry)* ]
//! entry   := physical-hex "." logical-hex "." nodeid-hex
//! ```
//!
//! `"v1:"` with no entries is a valid EMPTY token (no precondition);
//! servers may issue it when no origin is visible yet.
//!
//! `physical` is a hex-encoded `u64`, `logical` a hex-encoded `u32`, and
//! `nodeid-hex` the hex encoding of the origin node id's UTF-8 bytes.
//! The character set (`[0-9a-f.,:v]`) requires no URL encoding.

use std::collections::HashMap;

use crate::error::CrdtError;
use crate::hlc::{HlcTimestamp, MAX_CLOCK_SKEW_MS};
use crate::store::kv::{CrdtValue, Store};

/// Maximum number of `(origin, HLC)` entries a token may carry.
///
/// Tokens larger than this are rejected on parse; response tokens are
/// thinned to this cap (request-derived entries are kept preferentially,
/// see [`SessionToken::merge_frontiers`]; [`SessionToken::encode`]
/// enforces it again as a final guard). Thinning only weakens the
/// guarantee to the retained origins — it can never produce a false
/// success.
pub const MAX_TOKEN_ENTRIES: usize = 64;

/// Maximum accepted token length in bytes (encoded form).
///
/// [`SessionToken::encode`] also thins the emitted token to this limit
/// (dropping oldest-HLC entries first) so a server-issued token always
/// round-trips through [`SessionToken::parse`].
pub const MAX_TOKEN_BYTES: usize = 8192;

/// Maximum decoded node-id length in bytes for a single token entry.
pub const MAX_NODE_ID_BYTES: usize = 128;

/// A session token: a set of per-origin HLC positions.
///
/// Semantics: "the client has observed (written or read) all writes of
/// origin `node_id` up to and including the entry's HLC". Entries are
/// unique per `node_id`; `encode` emits them sorted by `node_id` for a
/// stable representation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionToken {
    /// Invariant: at most one entry per `node_id`.
    entries: Vec<HlcTimestamp>,
}

impl SessionToken {
    /// Create a token covering a single write position.
    pub fn from_hlc(ts: &HlcTimestamp) -> Self {
        Self {
            entries: vec![ts.clone()],
        }
    }

    /// Return the token entries (one per origin node).
    pub fn entries(&self) -> &[HlcTimestamp] {
        &self.entries
    }

    /// Parse a token from its `v1:` wire form.
    ///
    /// Returns `CrdtError::InvalidArgument` on any syntactic or limit
    /// violation (unknown version, non-hex fields, overflow, empty or
    /// oversized node id, too many entries, oversized input). Duplicate
    /// node ids are collapsed to the maximum HLC.
    pub fn parse(s: &str) -> Result<Self, CrdtError> {
        if s.len() > MAX_TOKEN_BYTES {
            return Err(CrdtError::InvalidArgument(format!(
                "session token too long: {} bytes (max {MAX_TOKEN_BYTES})",
                s.len()
            )));
        }
        let rest = s.strip_prefix("v1:").ok_or_else(|| {
            CrdtError::InvalidArgument("session token must start with \"v1:\"".into())
        })?;
        // "v1:" with no entries is the encoding of an EMPTY token (issued
        // e.g. by a replica with no visible origins yet). It must parse —
        // clients carry server-issued tokens back verbatim — and imposes
        // no precondition.
        if rest.is_empty() {
            return Ok(SessionToken::default());
        }

        let mut token = SessionToken::default();
        for (i, part) in rest.split(',').enumerate() {
            if i >= MAX_TOKEN_ENTRIES {
                return Err(CrdtError::InvalidArgument(format!(
                    "session token has too many entries (max {MAX_TOKEN_ENTRIES})"
                )));
            }
            let mut fields = part.split('.');
            let (Some(phys), Some(log), Some(nid), None) =
                (fields.next(), fields.next(), fields.next(), fields.next())
            else {
                return Err(CrdtError::InvalidArgument(format!(
                    "session token entry {i} must have exactly 3 dot-separated fields"
                )));
            };
            let physical = u64::from_str_radix(phys, 16).map_err(|e| {
                CrdtError::InvalidArgument(format!(
                    "session token entry {i}: invalid physical timestamp: {e}"
                ))
            })?;
            let logical = u32::from_str_radix(log, 16).map_err(|e| {
                CrdtError::InvalidArgument(format!(
                    "session token entry {i}: invalid logical counter: {e}"
                ))
            })?;
            let node_bytes = hex::decode(nid).map_err(|e| {
                CrdtError::InvalidArgument(format!(
                    "session token entry {i}: invalid node id hex: {e}"
                ))
            })?;
            if node_bytes.is_empty() {
                return Err(CrdtError::InvalidArgument(format!(
                    "session token entry {i}: node id must not be empty"
                )));
            }
            if node_bytes.len() > MAX_NODE_ID_BYTES {
                return Err(CrdtError::InvalidArgument(format!(
                    "session token entry {i}: node id too long: {} bytes (max {MAX_NODE_ID_BYTES})",
                    node_bytes.len()
                )));
            }
            let node_id = String::from_utf8(node_bytes).map_err(|e| {
                CrdtError::InvalidArgument(format!(
                    "session token entry {i}: node id is not valid UTF-8: {e}"
                ))
            })?;
            // Duplicate node ids collapse to the max side.
            token.merge_hlc(&HlcTimestamp {
                physical,
                logical,
                node_id,
            });
        }
        Ok(token)
    }

    /// Encode the token into its stable `v1:` wire form (entries sorted
    /// by `node_id`).
    ///
    /// Enforces the parse-side limits so a server-issued token always
    /// round-trips: when the encoded form would exceed
    /// [`MAX_TOKEN_ENTRIES`] or [`MAX_TOKEN_BYTES`] (possible with many
    /// origins and long node ids), entries with the OLDEST HLC positions
    /// are dropped first. Dropping an entry only weakens the guarantee to
    /// the retained origins (false-negative direction) — it never
    /// fabricates coverage. An empty token encodes as `"v1:"`, which
    /// parses back to an empty token.
    pub fn encode(&self) -> String {
        let mut entries: Vec<&HlcTimestamp> = self.entries.iter().collect();
        // Drop oldest positions first when over either cap. Sorting
        // newest-first and truncating keeps the freshest origins.
        entries.sort_unstable_by(|a, b| b.cmp(a));
        entries.truncate(MAX_TOKEN_ENTRIES);

        let mut encoded: Vec<(String, &HlcTimestamp)> = entries
            .iter()
            .map(|e| {
                (
                    format!(
                        "{:x}.{:x}.{}",
                        e.physical,
                        e.logical,
                        hex::encode(e.node_id.as_bytes())
                    ),
                    *e,
                )
            })
            .collect();

        // Byte budget: "v1:" + entries + separators.
        let mut total = 3
            + encoded.iter().map(|(s, _)| s.len()).sum::<usize>()
            + encoded.len().saturating_sub(1);
        while total > MAX_TOKEN_BYTES {
            // entries are sorted newest-first, so the oldest is last.
            let Some((dropped, _)) = encoded.pop() else {
                break;
            };
            total -= dropped.len();
            if !encoded.is_empty() {
                total -= 1; // separator
            }
        }

        encoded.sort_unstable_by(|(_, a), (_, b)| a.node_id.cmp(&b.node_id));
        let body: Vec<String> = encoded.into_iter().map(|(s, _)| s).collect();
        format!("v1:{}", body.join(","))
    }

    /// Merge a single HLC position into the token (per-node-id max).
    ///
    /// Never regresses an existing entry; adds a new entry when the
    /// origin is not yet covered (no cap applied — used for the
    /// single-entry write path and parse-time dedup).
    pub fn merge_hlc(&mut self, ts: &HlcTimestamp) {
        match self.entries.iter_mut().find(|e| e.node_id == ts.node_id) {
            Some(existing) => {
                if *ts > *existing {
                    *existing = ts.clone();
                }
            }
            None => self.entries.push(ts.clone()),
        }
    }

    /// Max-merge an applied-origins snapshot into the token, applying the
    /// [`MAX_TOKEN_ENTRIES`] cap.
    ///
    /// Entries already present in the token (request-derived) are always
    /// kept and only ever advanced; new origins from `frontiers` fill the
    /// remaining slots in descending `(physical, logical)` order. Dropping
    /// entries only weakens the monotonic-reads guarantee to the retained
    /// origins — it never fabricates coverage.
    pub fn merge_frontiers(&mut self, frontiers: &HashMap<String, HlcTimestamp>) {
        let mut additions: Vec<&HlcTimestamp> = Vec::new();
        for (node_id, ts) in frontiers {
            match self.entries.iter_mut().find(|e| &e.node_id == node_id) {
                Some(existing) => {
                    if *ts > *existing {
                        *existing = ts.clone();
                    }
                }
                None => additions.push(ts),
            }
        }
        // Fill remaining capacity with the freshest new origins first.
        additions.sort_unstable_by(|a, b| b.cmp(a));
        for ts in additions {
            if self.entries.len() >= MAX_TOKEN_ENTRIES {
                break;
            }
            self.entries.push(ts.clone());
        }
    }

    /// Reject tokens whose physical component lies too far in the future.
    ///
    /// This is the out-of-bounds defence against clock-advance attacks:
    /// a token is client-supplied input and must never be fed into
    /// `Hlc::update`, but even for pure comparison a far-future physical
    /// would make the token permanently unsatisfiable while looking
    /// legitimate, so it is rejected up front with `InvalidArgument`.
    pub fn validate_bounds(&self, wall_ms: u64) -> Result<(), CrdtError> {
        let limit = wall_ms.saturating_add(MAX_CLOCK_SKEW_MS);
        for e in &self.entries {
            if e.physical > limit {
                return Err(CrdtError::InvalidArgument(format!(
                    "session token entry for origin {} is too far in the future \
                     (physical={}, wall={wall_ms}, max_skew_ms={MAX_CLOCK_SKEW_MS})",
                    e.node_id, e.physical
                )));
            }
        }
        Ok(())
    }

    /// Check whether the local store provably contains all writes covered
    /// by this token for `key`.
    ///
    /// Two sound evidence paths, per token entry:
    ///
    /// - **Path A (applied origins)**: `store.applied_origins()[origin] >=
    ///   entry`. Sound because `applied_origins` only advances when the
    ///   full write prefix of that origin has been applied (local
    ///   mutation, complete delta pull, or frontier adoption). Disabled
    ///   for keys poisoned by a failed merge (`merge_failed_keys`), whose
    ///   contributions may be missing even though the origin frontier
    ///   advanced on other keys.
    /// - **Path B (register value evidence)**: for LWW registers only,
    ///   the register's internal timestamp `>= entry` proves the write
    ///   would not change the visible value even if merged (LWW
    ///   dominance). Value-level, hence sound even for poisoned keys.
    ///
    /// Deliberately NOT used: per-key `timestamp_for` comparison (a
    /// concurrent write by another origin advances it without containing
    /// the token's write — a false success) and push-path peer frontiers
    /// (advance on push success, which proves nothing about local state).
    pub fn is_satisfied(&self, store: &Store, key: &str) -> bool {
        let poisoned = store.merge_failed_contains(key);
        let register_ts = match store.get(key) {
            Some(CrdtValue::Register(r)) => Some(r.timestamp()),
            _ => None,
        };
        self.entries.iter().all(|entry| {
            let applied_ok = !poisoned
                && store
                    .applied_origin(&entry.node_id)
                    .is_some_and(|applied| applied >= entry);
            let register_ok = register_ts.is_some_and(|ts| ts >= entry);
            applied_ok || register_ok
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::wall_clock_ms;

    fn ts(physical: u64, logical: u32, node: &str) -> HlcTimestamp {
        HlcTimestamp {
            physical,
            logical,
            node_id: node.into(),
        }
    }

    // ---------------------------------------------------------------
    // encode / parse round-trip
    // ---------------------------------------------------------------

    #[test]
    fn encode_parse_round_trip() {
        let mut token = SessionToken::from_hlc(&ts(0x19704a1b2c3, 2, "node-a"));
        token.merge_hlc(&ts(0x19704b00000, 0, "node-b"));

        let encoded = token.encode();
        let back = SessionToken::parse(&encoded).unwrap();
        assert_eq!(token.entries().len(), back.entries().len());
        for e in token.entries() {
            assert!(back.entries().contains(e), "missing entry {e:?}");
        }
    }

    #[test]
    fn round_trip_with_special_node_ids() {
        // node ids containing '-', '.', and non-ASCII UTF-8.
        for node in ["node-a", "node.with.dots", "ノードa", "n,1:v"] {
            let token = SessionToken::from_hlc(&ts(1000, 5, node));
            let back = SessionToken::parse(&token.encode()).unwrap();
            assert_eq!(back.entries()[0].node_id, node);
            assert_eq!(back.entries()[0].physical, 1000);
            assert_eq!(back.entries()[0].logical, 5);
        }
    }

    #[test]
    fn encode_is_stable_and_sorted_by_node_id() {
        let mut t1 = SessionToken::from_hlc(&ts(2, 0, "zeta"));
        t1.merge_hlc(&ts(1, 0, "alpha"));

        let mut t2 = SessionToken::from_hlc(&ts(1, 0, "alpha"));
        t2.merge_hlc(&ts(2, 0, "zeta"));

        assert_eq!(t1.encode(), t2.encode(), "encode must be order-insensitive");
        let encoded = t1.encode();
        let alpha_pos = encoded.find(&hex::encode("alpha")).unwrap();
        let zeta_pos = encoded.find(&hex::encode("zeta")).unwrap();
        assert!(alpha_pos < zeta_pos, "entries must be sorted by node_id");
    }

    #[test]
    fn encode_uses_url_safe_charset() {
        let token = SessionToken::from_hlc(&ts(u64::MAX, u32::MAX, "日本語/ノード?=&"));
        let encoded = token.encode();
        assert!(
            encoded.chars().all(|c| c.is_ascii_hexdigit()
                || c == '.'
                || c == ','
                || c == ':'
                || c == 'v'),
            "unexpected character in {encoded}"
        );
    }

    /// A server that has no visible origins yet issues an EMPTY token;
    /// it must round-trip (the client carries it back verbatim) and
    /// impose no precondition.
    #[test]
    fn empty_token_round_trips() {
        let token = SessionToken::default();
        let encoded = token.encode();
        assert_eq!(encoded, "v1:");
        let back = SessionToken::parse(&encoded).unwrap();
        assert!(back.entries().is_empty());
        assert!(back.is_satisfied(&Store::new(), "any-key"));
    }

    /// encode() must enforce the byte cap so server-issued tokens always
    /// round-trip: 64 origins with long node ids would exceed
    /// MAX_TOKEN_BYTES without thinning. The freshest entries are kept.
    #[test]
    fn encode_thins_to_byte_cap_and_round_trips() {
        let mut token = SessionToken::default();
        // 64 origins × 120-byte node ids with realistic timestamps —
        // untrimmed this encodes to far more than MAX_TOKEN_BYTES.
        for i in 0..MAX_TOKEN_ENTRIES {
            let node = format!("{:0>120}", format!("origin-{i}"));
            token.merge_hlc(&ts(1_770_000_000_000 + i as u64, 0, &node));
        }

        let encoded = token.encode();
        assert!(
            encoded.len() <= MAX_TOKEN_BYTES,
            "encoded token must fit the parse limit, got {} bytes",
            encoded.len()
        );
        let back = SessionToken::parse(&encoded).unwrap();
        assert!(!back.entries().is_empty());
        assert!(
            back.entries().len() < MAX_TOKEN_ENTRIES,
            "must have thinned"
        );

        // Thinning drops the OLDEST entries: the freshest origin survives.
        let freshest = format!("{:0>120}", format!("origin-{}", MAX_TOKEN_ENTRIES - 1));
        assert!(back.entries().iter().any(|e| e.node_id == freshest));
        // The oldest origin was dropped.
        let oldest = format!("{:0>120}", "origin-0");
        assert!(back.entries().iter().all(|e| e.node_id != oldest));
    }

    /// encode() also enforces the entry-count cap (merge_hlc has no cap;
    /// the read-key coverage path can push a token past 64 entries).
    #[test]
    fn encode_thins_to_entry_cap() {
        let mut token = SessionToken::default();
        for i in 0..(MAX_TOKEN_ENTRIES + 5) {
            token.merge_hlc(&ts(1_000 + i as u64, 0, &format!("n{i}")));
        }
        let back = SessionToken::parse(&token.encode()).unwrap();
        assert_eq!(back.entries().len(), MAX_TOKEN_ENTRIES);
        // Freshest survive.
        let freshest = format!("n{}", MAX_TOKEN_ENTRIES + 4);
        assert!(back.entries().iter().any(|e| e.node_id == freshest));
    }

    // ---------------------------------------------------------------
    // parse rejections
    // ---------------------------------------------------------------

    #[test]
    fn parse_rejects_invalid_inputs() {
        let cases: Vec<String> = vec![
            "".into(),                              // empty
            "v2:1.0.61".into(),                     // unknown version
            "v1:xyz.0.61".into(),                   // non-hex physical
            "v1:1.zz.61".into(),                    // non-hex logical
            "v1:1.0.6g".into(),                     // non-hex node id
            "v1:1.0".into(),                        // missing field
            "v1:1.0.61.99".into(),                  // extra field
            "v1:1.0.".into(),                       // empty node id
            format!("v1:1ffffffffffffffff.0.61"),   // u64 overflow (17 hex digits)
            format!("v1:1.1ffffffff.61"),           // u32 overflow
            format!("v1:1.0.{}", "61".repeat(129)), // node id 129 bytes
            format!("v1:1.0.ff"),                   // node id not UTF-8
        ];
        for case in cases {
            assert!(
                matches!(
                    SessionToken::parse(&case),
                    Err(CrdtError::InvalidArgument(_))
                ),
                "expected InvalidArgument for {case:?}"
            );
        }
    }

    #[test]
    fn parse_rejects_too_many_entries() {
        let body: Vec<String> = (0..65)
            .map(|i| format!("1.0.{}", hex::encode(format!("n{i}"))))
            .collect();
        let s = format!("v1:{}", body.join(","));
        assert!(matches!(
            SessionToken::parse(&s),
            Err(CrdtError::InvalidArgument(_))
        ));
    }

    #[test]
    fn parse_accepts_max_entries() {
        let body: Vec<String> = (0..MAX_TOKEN_ENTRIES)
            .map(|i| format!("1.0.{}", hex::encode(format!("n{i}"))))
            .collect();
        let s = format!("v1:{}", body.join(","));
        let token = SessionToken::parse(&s).unwrap();
        assert_eq!(token.entries().len(), MAX_TOKEN_ENTRIES);
    }

    #[test]
    fn parse_rejects_oversized_input() {
        let s = format!("v1:1.0.{}", "61".repeat(MAX_TOKEN_BYTES));
        assert!(s.len() > MAX_TOKEN_BYTES);
        assert!(matches!(
            SessionToken::parse(&s),
            Err(CrdtError::InvalidArgument(_))
        ));
    }

    #[test]
    fn parse_collapses_duplicate_node_ids_to_max() {
        let nid = hex::encode("node-a");
        let s = format!("v1:64.0.{nid},c8.5.{nid},32.0.{nid}");
        let token = SessionToken::parse(&s).unwrap();
        assert_eq!(token.entries().len(), 1);
        assert_eq!(token.entries()[0], ts(0xc8, 5, "node-a"));
    }

    // ---------------------------------------------------------------
    // validate_bounds
    // ---------------------------------------------------------------

    #[test]
    fn validate_bounds_allows_boundary_rejects_beyond() {
        let wall = wall_clock_ms();
        let at_boundary = SessionToken::from_hlc(&ts(wall + MAX_CLOCK_SKEW_MS, 0, "n"));
        assert!(at_boundary.validate_bounds(wall).is_ok());

        let beyond = SessionToken::from_hlc(&ts(wall + MAX_CLOCK_SKEW_MS + 1, 0, "n"));
        assert!(matches!(
            beyond.validate_bounds(wall),
            Err(CrdtError::InvalidArgument(_))
        ));
    }

    // ---------------------------------------------------------------
    // merge_hlc / merge_frontiers
    // ---------------------------------------------------------------

    #[test]
    fn merge_hlc_takes_max_per_node_and_never_regresses() {
        let mut token = SessionToken::from_hlc(&ts(100, 0, "a"));
        token.merge_hlc(&ts(50, 0, "a")); // older — ignored
        assert_eq!(token.entries()[0], ts(100, 0, "a"));

        token.merge_hlc(&ts(200, 0, "a")); // newer — advances
        assert_eq!(token.entries()[0], ts(200, 0, "a"));

        token.merge_hlc(&ts(10, 0, "b")); // new origin — added
        assert_eq!(token.entries().len(), 2);
    }

    #[test]
    fn merge_frontiers_max_merges_and_never_regresses() {
        let mut token = SessionToken::from_hlc(&ts(100, 0, "a"));
        let mut frontiers = HashMap::new();
        frontiers.insert("a".to_string(), ts(50, 0, "a")); // older — kept at 100
        frontiers.insert("b".to_string(), ts(300, 0, "b")); // new origin
        token.merge_frontiers(&frontiers);

        assert!(token.entries().contains(&ts(100, 0, "a")));
        assert!(token.entries().contains(&ts(300, 0, "b")));
        assert_eq!(token.entries().len(), 2);
    }

    #[test]
    fn merge_frontiers_cap_keeps_request_entries() {
        // Token starts with 2 request-derived entries; frontier snapshot
        // offers many more origins than the cap allows.
        let mut token = SessionToken::from_hlc(&ts(1, 0, "req-old"));
        token.merge_hlc(&ts(2, 0, "req-old2"));

        let mut frontiers = HashMap::new();
        for i in 0..(MAX_TOKEN_ENTRIES + 20) {
            // All fresher than the request entries.
            frontiers.insert(
                format!("srv-{i}"),
                ts(1_000 + i as u64, 0, &format!("srv-{i}")),
            );
        }
        token.merge_frontiers(&frontiers);

        assert_eq!(token.entries().len(), MAX_TOKEN_ENTRIES);
        assert!(
            token.entries().iter().any(|e| e.node_id == "req-old"),
            "request-derived entry must survive the cap"
        );
        assert!(
            token.entries().iter().any(|e| e.node_id == "req-old2"),
            "request-derived entry must survive the cap"
        );
        // Fill order is freshest-first: the newest server origin must be present.
        let freshest = format!("srv-{}", MAX_TOKEN_ENTRIES + 19);
        assert!(token.entries().iter().any(|e| e.node_id == freshest));
    }

    // ---------------------------------------------------------------
    // is_satisfied (basic; store interplay tested in api/eventual.rs)
    // ---------------------------------------------------------------

    #[test]
    fn empty_token_is_always_satisfied() {
        let store = Store::new();
        let token = SessionToken::default();
        assert!(token.is_satisfied(&store, "any-key"));
    }

    #[test]
    fn unknown_origin_is_not_satisfied() {
        let store = Store::new();
        let token = SessionToken::from_hlc(&ts(100, 0, "elsewhere"));
        assert!(!token.is_satisfied(&store, "k"));
    }

    #[test]
    fn applied_origin_satisfies_at_or_above_entry() {
        let mut store = Store::new();
        store.note_applied(&ts(100, 0, "a"));

        assert!(SessionToken::from_hlc(&ts(100, 0, "a")).is_satisfied(&store, "k"));
        assert!(SessionToken::from_hlc(&ts(99, 5, "a")).is_satisfied(&store, "k"));
        assert!(!SessionToken::from_hlc(&ts(100, 1, "a")).is_satisfied(&store, "k"));
    }

    #[test]
    fn all_entries_must_be_satisfied() {
        let mut store = Store::new();
        store.note_applied(&ts(100, 0, "a"));

        let mut token = SessionToken::from_hlc(&ts(50, 0, "a"));
        token.merge_hlc(&ts(50, 0, "b")); // origin "b" unknown to store
        assert!(!token.is_satisfied(&store, "k"));

        store.note_applied(&ts(50, 0, "b"));
        assert!(token.is_satisfied(&store, "k"));
    }
}
