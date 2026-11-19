pub(crate) mod digest;
pub mod gc;
pub mod lww_register;
pub mod or_map;
pub mod or_set;
pub mod pn_counter;

use std::collections::{HashMap, HashSet};

use crate::types::NodeId;

/// A writer has no unused `u64` dot left. Reusing a wrapped dot would merge
/// unrelated updates and can revive a removed value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("CRDT dot counter exhausted")]
pub struct DotCounterExhausted;

/// Effects of one CRDT merge that involved the per-value compaction floor
/// (see [`or_set::OrSet::merge`] / [`or_map::OrMap::merge`]).
///
/// The `rejected_*` / `killed_*` counters are diagnostics, not part of
/// CRDT state: `merge` remains a pure join on the (elements, counters,
/// deferred, floor) lattice. Callers that do not care may ignore the
/// return value (deliberately not `#[must_use]`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MergeEffects {
    /// Incoming deferred (tombstone) dots that were NOT adopted because
    /// they are covered by the merged compaction floor — the tombstone's
    /// information is already carried by "floor + absence". A steady
    /// increase during a rolling upgrade indicates v1 peers re-offering
    /// stale tombstones (the old GC-rollback pressure, now absorbed).
    pub rejected_covered_deferred: u64,
    /// Local live dots killed by the floor clause (covered by the other
    /// side's floor and absent from its complete live set): a remove this
    /// replica never saw as a tombstone, learned through the floor.
    pub killed_by_floor: u64,
    /// Incoming live dots rejected as stale (covered by our floor and not
    /// already held live here): certified-removed dots re-offered by a
    /// lagging or previously-unknown replica.
    pub rejected_stale_live: u64,
    /// NOT a diagnostic: `true` iff this merge strictly inflated the local
    /// physical state (`pre != post` over ALL components — elements/dots,
    /// deferred, counters, compaction floor, register value+timestamp).
    /// `false` GUARANTEES `pre == post`, which is what the RR gate
    /// (redundant-relay suppression, M-6) relies on to skip re-stamping.
    ///
    /// Invariants: `killed_by_floor > 0 ⇒ changed` (a floor kill always
    /// removes a dot). The `rejected_*` counters are NON-adoption events
    /// and must NEVER imply `changed` — counting them would let a lagging
    /// peer's stale re-offers keep the receiver permanently "dirty" and
    /// resurrect the ping-pong this flag exists to stop.
    pub changed: bool,
}

impl MergeEffects {
    /// Accumulate another merge's effects into this total.
    ///
    /// `changed` aggregates by OR: the total reads "at least one absorbed
    /// merge inflated local state" (harmless for the diagnostics-only
    /// consumers of the accumulated counters, e.g. `Store::floor_effects`).
    pub fn absorb(&mut self, other: MergeEffects) {
        self.rejected_covered_deferred += other.rejected_covered_deferred;
        self.killed_by_floor += other.killed_by_floor;
        self.rejected_stale_live += other.rejected_stale_live;
        self.changed |= other.changed;
    }
}

/// Outcome of one certified sweep over a single CRDT value
/// (see [`or_set::OrSet::compact_deferred_certified`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepOutcome {
    /// Tombstone dots physically removed (they are now represented by the
    /// compaction floor).
    pub collected: u64,
    /// Per-node floor walks stopped by a hole: a dot that is neither live
    /// nor deferred below the node's counter (legacy pre-floor sweeps).
    /// The public GC path stops at these holes.
    pub stalled_holes: u64,
    /// Per-node floor walks stopped by a tombstone outside the marked
    /// candidate set (created after the mark; next cycle covers it).
    pub stalled_uncandidated: u64,
}

impl SweepOutcome {
    /// Accumulate another value's sweep outcome into this total.
    pub fn absorb(&mut self, other: SweepOutcome) {
        self.collected += other.collected;
        self.stalled_holes += other.stalled_holes;
        self.stalled_uncandidated += other.stalled_uncandidated;
    }
}

/// Is dot `(node_id, counter)` covered by (at or below) the compaction
/// floor? An absent floor entry means 0, and dot counters start at 1, so
/// an empty floor never covers anything.
pub(crate) fn covered(floor: &HashMap<NodeId, u64>, node_id: &NodeId, counter: u64) -> bool {
    counter <= floor.get(node_id).copied().unwrap_or(0)
}

/// Advance the per-node compaction floor over the known dot landscape —
/// the shared walk behind `OrSet::compact_deferred_certified` and
/// `OrMap::compact_deferred_certified` (kept in one place so the two
/// independent CRDT implementations cannot diverge).
///
/// For each node the walk starts at `floor[n] + 1` and advances while the
/// dot's fate is decided: live (present in `live_dots`), a MARKED
/// tombstone (`deferred_dots` ∩ `candidates`), or a hole authorised by
/// `hole_jump_ceilings` (Stage 2: only holes at or below the node's
/// counter AS SNAPSHOTTED AT MARK TIME may be crossed — a hole minted
/// AFTER the mark by an inbound partial delta, e.g. counters riding a
/// delta whose old-origin-timestamp entry was filtered out, is not
/// covered by "complete pull since the mark" evidence and MUST stall
/// the walk. Public GC always supplies `None` because dynamic peer
/// eviction leaves the remaining inbound evidence incomplete. `None`, or a
/// missing/lower ceiling entry, is fail-closed: the walk stalls at the
/// first hole (Stage 1).
///
/// Hole spans are crossed as INTERVALS between consecutive known
/// counters, not counter-by-counter: the walk is `O(S log S)` in the
/// number of live + deferred dots `S`, independent of `counters[n]`
/// (lifetime add counts can be orders of magnitude larger than the
/// resident state, and the sweep runs under the store lock).
///
/// Returns the stall tallies; `collected` is left at 0 for the caller
/// (physical tombstone deletion stays type-specific).
pub(crate) fn advance_compaction_floor(
    live_dots: &HashSet<(&NodeId, u64)>,
    deferred_dots: &HashSet<(&NodeId, u64)>,
    counters: &HashMap<NodeId, u64>,
    compaction_floor: &mut HashMap<NodeId, u64>,
    candidates: &HashSet<(NodeId, u64)>,
    mark_counter_caps: Option<&HashMap<NodeId, u64>>,
    hole_jump_ceilings: Option<&HashMap<NodeId, u64>>,
) -> SweepOutcome {
    /// Try to cross the hole span `[from, to]` (`from <= to`). Holes at
    /// or below `ceiling` are jumpable; `next` is left one past the last
    /// crossed hole. Returns `true` when the whole span was crossed.
    fn jump_hole_span(from: u128, to: u128, ceiling: u128, next: &mut u128) -> bool {
        if ceiling >= to {
            *next = to + 1;
            true
        } else {
            if ceiling >= from {
                // Partial jump: the floor may advance to the ceiling, but
                // the first post-mark hole stalls the walk (fail-closed).
                *next = ceiling + 1;
            }
            false
        }
    }

    let mut outcome = SweepOutcome::default();

    let mut nodes: HashSet<NodeId> = counters.keys().cloned().collect();
    nodes.extend(deferred_dots.iter().map(|(n, _)| (*n).clone()));
    nodes.extend(compaction_floor.keys().cloned());

    // Sorted known (live or deferred) counters per node: the walk jumps
    // between them instead of probing every counter in between.
    let mut known: HashMap<&NodeId, Vec<u64>> = HashMap::new();
    for (n, c) in live_dots.iter().chain(deferred_dots.iter()) {
        known.entry(n).or_default().push(*c);
    }
    for counters_of_node in known.values_mut() {
        counters_of_node.sort_unstable();
        counters_of_node.dedup();
    }

    for node in nodes {
        let current_counter = counters.get(&node).copied().unwrap_or(0);
        // A saved mark may be swept after later local writes or a merge.
        // Neither post-mark live dots nor post-mark holes may advance this
        // sweep's floor beyond the writer counter captured with the mark.
        let max_counter = mark_counter_caps
            .map(|caps| current_counter.min(caps.get(&node).copied().unwrap_or(0)))
            .unwrap_or(current_counter);
        let start = compaction_floor.get(&node).copied().unwrap_or(0);
        if start >= max_counter {
            continue;
        }
        let ceiling = hole_jump_ceilings
            .map(|m| m.get(&node).copied().unwrap_or(0))
            .unwrap_or(0);
        // The cursor may need to stand one past `u64::MAX` after deciding
        // the last possible dot. Keep it wider than stored dot counters.
        let mut next = u128::from(start) + 1;
        let mut stalled_on_hole = false;
        let mut stalled_on_uncandidated = false;

        let known_of_node = known.get(&node).map(Vec::as_slice).unwrap_or(&[]);
        let first = known_of_node.partition_point(|&k| k <= start);
        for &k in &known_of_node[first..] {
            if k > max_counter {
                // Defensive: never walk past counters[n] (INV-CTR).
                break;
            }
            // Hole span between the previous known counter and this one.
            if u128::from(k) > next
                && !jump_hole_span(next, u128::from(k) - 1, u128::from(ceiling), &mut next)
            {
                stalled_on_hole = true;
                break;
            }
            if live_dots.contains(&(&node, k)) {
                next = u128::from(k) + 1; // fate decided: live
            } else if candidates.contains(&(node.clone(), k)) {
                next = u128::from(k) + 1; // fate decided: removed (gated)
            } else {
                // A tombstone created after the mark; next cycle covers it.
                stalled_on_uncandidated = true;
                break;
            }
        }
        // Tail hole span up to counters[n].
        if !stalled_on_hole
            && !stalled_on_uncandidated
            && next <= u128::from(max_counter)
            && !jump_hole_span(
                next,
                u128::from(max_counter),
                u128::from(ceiling),
                &mut next,
            )
        {
            stalled_on_hole = true;
        }

        if stalled_on_hole {
            outcome.stalled_holes += 1;
        }
        if stalled_on_uncandidated {
            outcome.stalled_uncandidated += 1;
        }
        let new_floor = u64::try_from(next - 1).expect("floor cannot exceed the writer counter");
        if new_floor > start {
            compaction_floor.insert(node, new_floor);
        }
    }

    outcome
}

#[cfg(test)]
mod floor_walk_tests {
    use super::*;

    /// Exhaust the small, well-formed dot landscapes against the direct
    /// Stage 1 rule: cross exactly the contiguous live or marked-deleted
    /// prefix, stopping at the first hole or unmarked tombstone. This checks
    /// the interval implementation's known-counter construction as well as
    /// its mark-time cap.
    #[test]
    fn interval_floor_walk_matches_contiguous_reference() {
        let node = NodeId("writer".into());
        for max_counter in 0..=4u64 {
            for start in 0..=max_counter {
                for cap in start..=max_counter {
                    for mut layout in 0..3u32.pow(max_counter as u32) {
                        let mut kinds = vec![0u8; max_counter as usize + 1];
                        for counter in 1..=max_counter {
                            kinds[counter as usize] = (layout % 3) as u8;
                            layout /= 3;
                        }
                        for mask in 0..(1u32 << max_counter) {
                            let mut live = HashSet::new();
                            let mut removed = HashSet::new();
                            let mut marked = HashSet::new();
                            for counter in 1..=max_counter {
                                match kinds[counter as usize] {
                                    1 => {
                                        live.insert((&node, counter));
                                    }
                                    2 => {
                                        removed.insert((&node, counter));
                                        if mask & (1 << (counter - 1)) != 0 {
                                            marked.insert((node.clone(), counter));
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            let mut expected = start;
                            for counter in (start + 1)..=cap {
                                let decided = kinds[counter as usize] == 1
                                    || (kinds[counter as usize] == 2
                                        && marked.contains(&(node.clone(), counter)));
                                if !decided {
                                    break;
                                }
                                expected = counter;
                            }

                            let counters = HashMap::from([(node.clone(), max_counter)]);
                            let caps = HashMap::from([(node.clone(), cap)]);
                            let mut floor = HashMap::from([(node.clone(), start)]);
                            let _outcome = advance_compaction_floor(
                                &live,
                                &removed,
                                &counters,
                                &mut floor,
                                &marked,
                                Some(&caps),
                                None,
                            );
                            assert_eq!(
                                floor.get(&node).copied().unwrap_or(0),
                                expected,
                                "max={max_counter} start={start} cap={cap} kinds={kinds:?} mask={mask}"
                            );
                        }
                    }
                }
            }
        }
    }
}
