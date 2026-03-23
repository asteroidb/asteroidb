# Performance Profiling Results

Baseline measurements taken on Linux (x86_64), Rust 1.93, release builds.

## 1. CRDT Merge Operations

| Benchmark | Median | Notes |
|-----------|--------|-------|
| pn_counter/increment (100 ops) | 6.75 us | 67 ns/op |
| pn_counter/merge_2_replicas (1000 each) | 273 ns | Extremely fast (HashMap merge) |
| or_set/add/10 | 4.75 us | 475 ns/op |
| or_set/add/100 | 75.8 us | 758 ns/op |
| or_set/add/1000 | 743 us | 743 ns/op |
| or_set/add_merge/10 | 8.67 us | |
| or_set/add_merge/100 | 104 us | |
| or_set/add_merge/1000 | 1.36 ms | **Hottest CRDT path** |
| or_map/put_100_merge | 95.6 us | |
| lww_register/set_100 | 10.2 us | |
| lww_register/set_and_merge | 158 ns | |

### Bottleneck Analysis: CRDT Merge

- **OrSet merge at 1000 elements is the dominant CRDT cost** (1.36 ms). This is
  allocation-bound: each merge clones the entire set, allocates new `HashSet<Dot>`
  entries, and iterates both the element map and the deferred (tombstone) set.
- PnCounter merge is near-zero cost (HashMap max-merge over node counters).
- LwwRegister merge is trivial (single timestamp comparison).
- OrMap merge at 100 entries costs ~96 us, which is reasonable for its complexity
  (LWW per-key merge + dot-based tombstones).

### Improvement Strategies

1. **OrSet merge: reduce clone overhead**. The merge currently clones elements from
   the other set. A `merge_into` variant that takes `other` by value (consuming it)
   would eliminate most allocations.
2. **OrSet merge: batch deferred lookups**. The `deferred.contains()` call is O(1)
   per dot, but the iteration pattern causes cache misses across the two HashSets.
   Pre-sorting or using a BTreeSet for the deferred set could improve locality.
3. **OrSet merge: shrink Dot size**. Each `Dot` contains a `NodeId(String)` + `u64`.
   Interning node IDs (e.g., `u32` index into a global registry) would reduce per-dot
   memory from ~56 bytes to ~12 bytes, cutting allocation pressure by ~4x.

## 2. Delta Sync Serialization / Deserialization

### Serialization (encode)

| Benchmark | JSON | bincode | Speedup |
|-----------|------|---------|---------|
| SyncRequest/10 entries | 1.63 us | 1.27 us | 1.3x |
| SyncRequest/100 entries | 12.6 us | 6.69 us | 1.9x |
| SyncRequest/1000 entries | 157 us | 92.8 us | 1.7x |
| DeltaSyncResponse/10 | 4.03 us | 1.44 us | 2.8x |
| DeltaSyncResponse/100 | 33.0 us | 9.43 us | 3.5x |
| DeltaSyncResponse/500 | 167 us | 44.8 us | 3.7x |

### Deserialization (decode)

| Benchmark | JSON | bincode | Speedup |
|-----------|------|---------|---------|
| SyncRequest/10 entries | 7.90 us | 5.10 us | 1.5x |
| SyncRequest/100 entries | 87.6 us | 54.8 us | 1.6x |
| SyncRequest/1000 entries | 913 us | 552 us | 1.7x |
| DeltaSyncResponse/10 | 11.4 us | 6.29 us | 1.8x |
| DeltaSyncResponse/100 | 122 us | 61.4 us | 2.0x |
| DeltaSyncResponse/500 | 586 us | 314 us | 1.9x |

### Payload Sizes

| Entries | JSON | bincode | Compression ratio |
|---------|------|---------|-------------------|
| 10 | 585 B | 272 B | 2.15x |
| 100 | 5,535 B | 2,612 B | 2.12x |
| 1,000 | 55,035 B | 26,014 B | 2.12x |

### Bottleneck Analysis: Serialization

- **JSON deserialization is the single most expensive sync operation**: decoding
  1000 entries takes 913 us. This is CPU-bound (parsing + UTF-8 validation +
  number conversion).
- **bincode deserialization at 1000 entries still costs 552 us**. The cost is
  dominated by per-entry `CrdtValue` enum decoding and `HashMap<String, CrdtValue>`
  allocation.
- Bincode provides consistent 1.5-3.7x improvement over JSON for both encode and
  decode, plus 2.1x smaller payloads.

### Improvement Strategies

1. **Default to bincode for all internal traffic** (already supported via
   `Accept: application/octet-stream`). Ensure all node-to-node paths use it.
2. **Pre-allocate deserialization buffers**. The `HashMap` deserialization creates
   default-capacity maps that rehash multiple times. Providing size hints via a
   custom deserializer could reduce allocations.
3. **Streaming deserialization**. For large delta payloads (>500 entries), process
   entries as they are decoded rather than collecting into a Vec/HashMap first.
4. **Consider `rkyv` for zero-copy deserialization**. For the sync hot path,
   zero-copy deserialization would eliminate the decode step entirely at the cost
   of a more complex API.

## 3. Delta Extraction (entries_since)

| Benchmark | Median |
|-----------|--------|
| entries_since/100 total | 549 ns |
| entries_since/1000 total (10% changed) | 13.2 us |
| entries_since/5000 total (10% changed) | 208 us |
| entries_since/1000_keys_1pct changed | 12.6 us |
| entries_since/1000_keys_10pct changed | 73.0 us |
| entries_since/5000_keys_5pct changed | 239 us |

### Bottleneck Analysis: Delta Extraction

- The `entries_since` implementation scans the full `BTreeMap<String, HlcTimestamp>`
  changelog, making it O(n) in the total number of keys rather than O(delta).
- At 5000 keys with 5% changes, extraction costs 239 us -- this becomes significant
  when sync runs every few seconds on a busy cluster.

### Improvement Strategies

1. **Maintain a sorted change index by HLC**. Replace the current full-scan with
   a `BTreeMap<HlcTimestamp, Vec<String>>` that allows O(log n + k) extraction
   where k is the number of changed keys.
2. **Skip-list or interval tree**. For very large key spaces (>100k), a more
   specialized data structure would keep extraction sub-linear.

## 4. Certified Write Path

| Benchmark | Median |
|-----------|--------|
| certified/write_pending (100 writes) | 39.6 us |
| certified/process_certifications | 952 ns |
| certified/verify_proof_3of5 | 271 us |
| certified/verify_proof_5of9 | 437 us |

### Bottleneck Analysis: Certification

- **Ed25519 proof verification is the bottleneck** in the certified path.
  Verifying 3-of-5 takes 271 us (90 us per signature), and 5-of-9 takes 437 us
  (87 us per signature). This is CPU-bound (elliptic curve math).
- `certified_write` itself is cheap (0.4 us per write for pending queue insertion).
- `process_certifications` is extremely cheap (952 ns) -- frontier comparison and
  certificate state transition are lightweight.

### Signature Cost Comparison (Ed25519 vs BLS)

| Operation | Ed25519 | BLS12-381 | Winner |
|-----------|---------|-----------|--------|
| keygen | 31.0 us | 159 us | Ed25519 (5x) |
| sign | 30.9 us | 609 us | Ed25519 (20x) |
| single verify | 61.0 us | 1.68 ms | Ed25519 (28x) |
| verify 3-of-5 | 187 us | 1.61 ms | Ed25519 (8.6x) |
| verify 5-of-9 | 315 us | 1.57 ms | BLS (5.0x at N>15) |
| verify 7-of-11 | 434 us | 1.58 ms | BLS (3.6x at N>15) |
| cert size (N=3) | 1,116 B | 823 B | BLS (1.4x) |
| cert size (N=10) | 2,992 B | 1,579 B | BLS (1.9x) |

**Crossover point**: BLS aggregate verification has constant cost (~1.6 ms) while
Ed25519 scales linearly. BLS becomes faster at roughly N > 26 signers (1.6 ms /
61 us = 26).

### Improvement Strategies

1. **Batch signature verification**. ed25519-dalek supports batch verification
   which uses a single multi-scalar multiplication, reducing per-signature cost
   by ~40%.
2. **Parallel BLS aggregation**. BLS signing (609 us per signer) can be
   parallelized across authority nodes since each signs independently.
3. **Cache verified certificates**. Once a proof is verified, cache the result
   keyed by certificate hash to avoid re-verification on repeated reads.

## 5. Store Operations

| Benchmark | Median |
|-----------|--------|
| store/put_1000 | 669 us |
| store/get_existing (100 lookups) | 25.1 us |
| store/get_missing (100 lookups) | 21.8 us |
| store/save_snapshot_1000 | 12.1 ms |
| store/load_snapshot_1000 | 5.89 ms |

### Bottleneck Analysis: Store

- **Snapshot save is the most expensive store operation** (12.1 ms for 1000 entries).
  This is I/O-bound: JSON serialization + file write.
- **Snapshot load** costs 5.89 ms (JSON deserialization + file read).
- `put` costs 669 ns/entry, which is reasonable (HashMap insert + clone).
- `get` is essentially free at 251 ns per lookup.

### Improvement Strategies

1. **Use bincode for snapshots**. Switching from JSON to bincode would reduce both
   save and load times by ~2x based on the serialization benchmarks above.
2. **Incremental snapshots**. Instead of saving the full store, write only changed
   entries since the last snapshot (delta-based persistence).
3. **mmap-backed store**. For large stores (>100k entries), memory-mapped I/O would
   avoid the serialization step entirely.

## 6. Compaction & Tombstone GC

| Benchmark | Median |
|-----------|--------|
| compaction/record_op/100 | 6.54 us |
| compaction/record_op/1000 | 65.9 us |
| compaction/record_op/10000 | 642 us |
| compaction/should_checkpoint | 35.7 ns |
| compaction/create_checkpoint/10 | 8.86 us |
| compaction/create_checkpoint/100 | 83.0 us |
| compaction/create_checkpoint/500 | 420 us |
| compaction/is_compactable/3 auth | 158 ns |
| compaction/is_compactable/5 auth | 274 ns |
| compaction/is_compactable/9 auth | 445 ns |
| compaction/run_compaction/100 keys | 29.0 us |
| compaction/run_compaction/500 keys | 233 us |
| compaction/run_compaction/1000 keys | 494 us |
| compaction/adaptive_tune | 427 ns |
| compaction/write_rate_tracker/100 | 381 ns |
| compaction/write_rate_tracker/1000 | 1.78 us |
| compaction/write_rate_tracker/5000 | 7.83 us |
| gc/tombstones/10 sets | 7.12 us |
| gc/tombstones/50 sets | 47.7 us |
| gc/tombstones/200 sets | 214 us |
| gc/mixed_store_100 | 64.6 us |
| gc/with_version_floor_50 | 51.3 us |

### Bottleneck Analysis: Compaction

- **`run_compaction` at 1000 keys costs 494 us**. The bottleneck is `prune_timestamps_before`,
  which iterates the full changelog BTreeMap filtering by prefix.
- **`record_op` at 10000 ops costs 642 us** (64 ns/op). The cost is from
  `HashMap::entry().or_insert()` + the adaptive write rate tracker recording.
- **`create_checkpoint` at 500 costs 420 us** due to cloning the checkpoint into
  the history VecDeque and the HashMap insert.
- **`should_checkpoint` and `is_compactable` are negligible** (36-445 ns).
- **Adaptive tuning is extremely cheap** (427 ns) -- the write rate aggregation
  loop is O(num_prefixes).

### Tombstone GC Analysis

- GC costs scale linearly with the number of sets: 7 us for 10 sets, 214 us
  for 200 sets (~1 us per set).
- The `gc_tombstones` method iterates all store keys, calling `compact_deferred()`
  on each OrSet/OrMap. The per-set cost is dominated by building the `live_dots`
  HashSet from all element dot sets.
- With version floor enabled, GC cost is similar (51 us for 50 sets vs 48 us without),
  indicating the floor check adds minimal overhead.

### Improvement Strategies

1. **Index changelog by prefix**. `prune_timestamps_before` currently scans the
   full changelog. Maintaining a per-prefix index would make pruning O(k) where k
   is the number of entries in that prefix.
2. **Amortize record_op**. Batch multiple operations into a single `record_ops(n)`
   call to reduce HashMap lookup overhead.
3. **Lazy GC**. Instead of iterating all store keys, maintain a dirty-set of keys
   that have had removes since the last GC, and only compact those.

## 7. Summary: Top Bottlenecks

Ranked by impact on production workloads:

| Rank | Bottleneck | Median Cost | Type | Mitigation |
|------|-----------|-------------|------|------------|
| 1 | BLS verify (single) | 1.68 ms | CPU | Batch verify, cache results |
| 2 | OrSet merge (1000 elems) | 1.36 ms | Alloc | Consume-merge, intern NodeId |
| 3 | JSON deserialize (1000 entries) | 913 us | CPU | Default to bincode |
| 4 | Store snapshot save | 12.1 ms | I/O | Bincode format, incremental |
| 5 | Store snapshot load | 5.89 ms | I/O | Bincode format |
| 6 | run_compaction (1000 keys) | 494 us | CPU | Per-prefix changelog index |
| 7 | entries_since (5000 keys) | 208-239 us | CPU | HLC-sorted change index |
| 8 | GC tombstones (200 sets) | 214 us | Alloc | Lazy dirty-set tracking |

### Quick Wins (Low Effort, High Impact)

1. **Switch internal sync to bincode by default** -- 1.5-3.7x speedup on the sync
   hot path with no API changes.
2. **Use bincode for store snapshots** -- ~2x improvement on save/load.
3. **Enable ed25519 batch verification** -- ~40% reduction in proof verification cost.
4. **Consume-merge for OrSet** -- eliminate clones on the hottest CRDT path.
