# Long Partition + Reconnection Convergence Test

## Methodology

This test validates AsteroidDB's ability to recover from extended network
partitions while maintaining CRDT correctness guarantees. It exercises the
delta sync protocol, full sync fallback, and tombstone GC under realistic
long-duration partition scenarios.

### Test Environment

- **Cluster**: 3 Docker containers (node-1, node-2, node-3) on a single bridge network
- **Partition method**: iptables INPUT/OUTPUT rules on node-3 (bidirectional block)
- **Partition topology**: 2-1 split (majority: node-1 + node-2, minority: node-3)

### Partition Durations

| Duration | Rationale |
|----------|-----------|
| 30s      | Baseline; exceeds delta sync interval (~2s), tests normal delta recovery |
| 60s      | Medium partition; delta buffer may accumulate significant state |
| 120s     | Extended partition; tests delta buffer limits and potential fallback trigger |
| 300s     | Long partition; likely triggers full sync fallback path |

### Write Pattern

During each partition, both sides receive concurrent writes:

- **Majority side (node-1)**: N counter increments + M OR-Set additions (items prefixed `majority-item-`)
- **Minority side (node-3)**: N counter increments + M OR-Set additions (items prefixed `minority-item-`)

Default: N=20 counter increments per side, M=10 set items per side.

### Verification Criteria

After the partition heals, the test verifies:

1. **Convergence**: All 3 nodes reach identical state within the timeout window
2. **Counter correctness**: PN-Counter value = 2N on all nodes (sum of both sides)
3. **Set correctness**: OR-Set contains the union of all items (2M items total) on all nodes
4. **Convergence time**: Milliseconds from partition heal to full convergence

### Measurements

For each partition duration, the test records:

| Metric | Description |
|--------|-------------|
| `partition_duration_secs` | Configured partition duration |
| `actual_partition_ms` | Actual measured partition duration |
| `convergence_time_ms` | Time from heal to all-node convergence |
| `converged` | Whether convergence succeeded within timeout |
| `data_correct` | Whether CRDT merge produced correct results |

### Output Format

Results are written to `target/long-partition-results.json` and printed as both
CSV and JSON to stdout.

**CSV columns**: `partition_secs,actual_partition_ms,convergence_ms,converged,data_correct`

**JSON structure**:
```json
{
  "test": "long-partition-recovery",
  "run_id": "lp-<pid>",
  "total_duration_secs": 600,
  "passed": 4,
  "failed": 0,
  "scenarios": [
    {
      "partition_duration_secs": 30,
      "actual_partition_ms": 30123,
      "convergence_time_ms": 4500,
      "converged": true,
      "data_correct": true,
      "counter_writes_per_side": 20,
      "set_items_per_side": 10,
      "expected_counter_total": 40,
      "expected_set_items": 20
    }
  ]
}
```

## Expected Behavior

### Delta Sync (short partitions, < ~60s)

For partitions shorter than the delta buffer retention window, delta sync
should handle recovery efficiently. Expected convergence time: a few seconds
after partition heal (1-2 sync intervals).

### Full Sync Fallback (long partitions, > ~120s)

For partitions exceeding the delta buffer window, the full sync fallback
mechanism (#262) should trigger automatically. This path transfers the
complete state snapshot instead of incremental deltas. Expected convergence
time: proportional to total state size, but still within seconds for the
test workload.

### Tombstone GC

The AckFrontier-based GC (#263) must correctly track that node-3 has not
acknowledged recent operations during the partition. Tombstones must not be
prematurely collected while node-3 is unreachable.

## Running

```bash
# Default: test 30s, 60s, 120s, 300s partitions
./scripts/test-long-partition.sh

# Quick test with shorter durations
./scripts/test-long-partition.sh --durations "30 60"

# Custom parameters
./scripts/test-long-partition.sh \
  --durations "30 60 120 300" \
  --writes-per-side 50 \
  --set-items-per-side 20 \
  --convergence-timeout 300

# Skip Docker build (use existing image)
./scripts/test-long-partition.sh --skip-build --durations "30"
```

## Relation to Existing Tests

| Test | Scope | Partition Duration |
|------|-------|--------------------|
| `test-netem-light.sh` | Quick CI smoke test | 3s |
| `bench-partition-recovery.sh` | Recovery benchmark | 10s (configurable) |
| `fault-inject/runner.sh` | Fault injection suite | 3-5s per scenario |
| **`test-long-partition.sh`** | **Long partition convergence** | **30s - 300s** |

This test fills the gap between the existing short-duration fault injection
tests and real-world scenarios where partitions can last minutes to hours
(e.g., satellite link interruptions, disaster recovery).
